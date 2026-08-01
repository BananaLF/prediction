from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.catalog.sync import SyncMarketTask
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.repositories import (
    CatalogRepository,
    SystemEventRepository,
)
from predmarket.persistence.writer import DatabaseWriter
from predmarket.polymarket.gateway import MAPPING_VERSION, MarketSnapshot


def _event(
    market_ids: tuple[str, ...],
    *,
    title: str = "Event",
    generation: str = "sdk-generation",
    neg_risk_complete: bool = False,
) -> Event:
    return Event(
        id="event-1",
        title=title,
        status=MarketStatus.ACTIVE,
        market_ids=market_ids,
        sync_generation=generation,
        sync_generation_complete=True,
        neg_risk=neg_risk_complete,
        neg_risk_complete=neg_risk_complete,
        neg_risk_conversion_supported=neg_risk_complete,
        created_at=10,
        updated_at=10,
    )


def _snapshot(
    market_id: str,
    *,
    question: str | None = None,
    generation: str = "sdk-generation",
    neg_risk_member_complete: bool = False,
) -> MarketSnapshot:
    suffix = market_id.rsplit("-", 1)[-1]
    market = Market(
        id=market_id,
        event_id="event-1",
        condition_id=f"condition-{suffix}",
        question=question or f"Question {suffix}?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation=generation,
        sync_generation_complete=True,
        neg_risk=neg_risk_member_complete,
        neg_risk_outcome_position=0 if neg_risk_member_complete else None,
        neg_risk_member_complete=neg_risk_member_complete,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
        created_at=10,
        updated_at=10,
    )
    tokens = tuple(
        Token(
            id=f"{market_id}-token-{position}",
            market_id=market_id,
            outcome=outcome,
            position=position,
            sync_generation=generation,
            sync_generation_complete=True,
            created_at=10,
            updated_at=10,
        )
        for position, outcome in enumerate(("YES", "NO"))
    )
    return MarketSnapshot(
        market=market,
        tokens=tokens,
        mapping_version=MAPPING_VERSION,
    )


@dataclass
class _FakeGateway:
    events: Any
    markets: Any
    event_calls: int = 0
    market_calls: int = 0

    async def list_active_events(self) -> tuple[Event, ...]:
        self.event_calls += 1
        if isinstance(self.events, BaseException):
            raise self.events
        return tuple(self.events)

    async def list_active_markets(self) -> tuple[MarketSnapshot, ...]:
        self.market_calls += 1
        if isinstance(self.markets, BaseException):
            raise self.markets
        return tuple(self.markets)


class _RecordingQueue:
    def __init__(self, repository: CatalogRepository | None = None) -> None:
        self.items: list[MarketChange] = []
        self._repository = repository

    async def put(self, change: MarketChange) -> bool:
        if self._repository is not None and change.market_id is not None:
            assert await self._repository.get_market(change.market_id) is not None
        self.items.append(change)
        return True


class _FailingCatalog:
    def __init__(self, delegate: CatalogRepository) -> None:
        self._delegate = delegate

    async def load_catalog(self) -> object:
        return await self._delegate.load_catalog()

    async def save_catalog(self, **_: object) -> None:
        raise RuntimeError("forced catalog rollback")


@pytest.fixture
async def catalog_runtime(tmp_path: Path):
    database_path = tmp_path / "catalog.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    system_events = SystemEventRepository(database_path, writer)
    try:
        yield catalog, system_events
    finally:
        await writer.close()


async def _seed(
    catalog: CatalogRepository,
    market_ids: tuple[str, ...],
) -> None:
    snapshots = tuple(_snapshot(market_id, generation="old") for market_id in market_ids)
    await catalog.save_catalog(
        events=(_event(market_ids, generation="old"),),
        markets=tuple(snapshot.market for snapshot in snapshots),
        tokens=tuple(token for snapshot in snapshots for token in snapshot.tokens),
    )


async def test_complete_generation_drains_gateway_and_commits_before_publish(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    gateway = _FakeGateway(
        events=(_event(("market-1", "market-2")),),
        markets=(_snapshot("market-1"), _snapshot("market-2")),
    )
    queue = _RecordingQueue(catalog)
    task = SyncMarketTask(
        gateway=gateway,
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 100,
        generation_factory=lambda: "sync-1",
    )

    result = await task.run_once()

    assert result.complete is True
    assert result.sync_generation == "sync-1"
    assert (result.events_seen, result.markets_seen, result.tokens_seen) == (1, 2, 4)
    assert gateway.event_calls == gateway.market_calls == 1
    assert [change.change_type for change in queue.items] == [
        MarketChangeType.MARKET_ADDED,
        MarketChangeType.MARKET_ADDED,
    ]
    stored = await catalog.load_catalog()
    assert all(event.sync_generation == "sync-1" for event in stored.events)
    assert all(event.sync_generation_complete for event in stored.events)
    assert all(market.sync_generation_complete for market in stored.markets)
    assert all(token.sync_generation_complete for token in stored.tokens)


async def test_complete_generation_deactivates_only_missing_market(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1", "market-2"))
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(_snapshot("market-1"),),
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 200,
        generation_factory=lambda: "sync-2",
    )

    result = await task.run_once()

    assert result.complete is True
    assert (await catalog.get_market("market-1")).active is True  # type: ignore[union-attr]
    missing = await catalog.get_market("market-2")
    assert missing is not None
    assert missing.status is MarketStatus.CLOSED
    assert missing.active is False
    assert missing.accepting_orders is False
    assert missing.enable_orderbook is False
    assert [change.change_type for change in queue.items] == [
        MarketChangeType.MARKET_DEACTIVATED
    ]
    assert queue.items[0].market_id == "market-2"


async def test_complete_generation_rejects_unknown_declared_event_member(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1", "market-never-parsed")),),
            markets=(_snapshot("market-1"),),
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 250,
        generation_factory=lambda: "sync-unknown-member",
    )

    result = await task.run_once()

    assert result.complete is False
    assert "market-never-parsed" in result.error
    stored = await catalog.get_market("market-1")
    assert stored is not None
    assert stored.sync_generation_complete is False
    assert queue.items == []


async def test_incomplete_generation_never_creates_complete_neg_risk_proof(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(
                _event(("market-1",), neg_risk_complete=True),
            ),
            markets=(
                _snapshot("market-1", neg_risk_member_complete=True),
                object(),
            ),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 275,
        generation_factory=lambda: "sync-incomplete-neg-risk",
    )

    result = await task.run_once()

    assert result.complete is False
    event = await catalog.get_event("event-1")
    market = await catalog.get_market("market-1")
    assert event is not None
    assert event.neg_risk_complete is False
    assert event.neg_risk_conversion_supported is False
    assert market is not None
    assert market.neg_risk_member_complete is False


async def test_incomplete_generation_ignores_changed_token_identity(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1",))
    changed = _snapshot("market-1")
    changed = MarketSnapshot(
        market=changed.market,
        tokens=tuple(
            replace(token, id=f"replacement-{token.position}")
            for token in changed.tokens
        ),
        mapping_version=changed.mapping_version,
    )
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(changed, object()),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 290,
        generation_factory=lambda: "sync-bad-token-identity",
    )

    result = await task.run_once()

    assert result.complete is False
    snapshot = await catalog.load_catalog()
    assert tuple(token.id for token in snapshot.tokens) == (
        "market-1-token-0",
        "market-1-token-1",
    )


@pytest.mark.parametrize(
    ("events", "markets", "expected_error"),
    [
        (RuntimeError("event request failed"), (), "event request failed"),
        (
            (_event(("market-1",)),),
            RuntimeError("paginator stopped halfway"),
            "paginator stopped halfway",
        ),
        (
            (_event(("market-1",)),),
            (_snapshot("market-1"), object()),
            "required market snapshot",
        ),
    ],
)
async def test_incomplete_generation_preserves_existing_active_state_and_emits_no_diff(
    catalog_runtime,
    events: object,
    markets: object,
    expected_error: str,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1", "market-2"))
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(events=events, markets=markets),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 300,
        generation_factory=lambda: "sync-incomplete",
    )

    result = await task.run_once()

    assert result.complete is False
    assert expected_error in result.error
    for market_id in ("market-1", "market-2"):
        market = await catalog.get_market(market_id)
        assert market is not None
        assert market.status is MarketStatus.ACTIVE
        assert market.active is True
        assert market.accepting_orders is True
        assert market.enable_orderbook is True
    assert queue.items == []
    events_after = await system_events.read_after(0)
    assert events_after[-1]["event_type"] == "SYNC_GENERATION_INCOMPLETE"
    assert events_after[-1]["details"]["sync_generation"] == "sync-incomplete"


async def test_repeated_complete_generation_is_an_idempotent_upsert(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    gateway = _FakeGateway(
        events=(_event(("market-1",)),),
        markets=(_snapshot("market-1"),),
    )
    queue = _RecordingQueue()
    generations = iter(("sync-1", "sync-2"))
    task = SyncMarketTask(
        gateway=gateway,
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 400,
        generation_factory=lambda: next(generations),
    )

    first = await task.run_once()
    second = await task.run_once()

    assert first.complete is second.complete is True
    assert [change.change_type for change in queue.items] == [
        MarketChangeType.MARKET_ADDED
    ]
    stored = await catalog.load_catalog()
    assert len(stored.events) == 1
    assert len(stored.markets) == 1
    assert len(stored.tokens) == 2
    assert stored.markets[0].sync_generation == "sync-2"


async def test_catalog_rollback_never_publishes_market_changes(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(_snapshot("market-1"),),
        ),
        catalog=_FailingCatalog(catalog),
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 500,
        generation_factory=lambda: "sync-rollback",
    )

    with pytest.raises(RuntimeError, match="forced catalog rollback"):
        await task.run_once()

    assert queue.items == []
    assert await catalog.get_market("market-1") is None
