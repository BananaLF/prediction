from __future__ import annotations

import asyncio
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
    refreshed: dict[str, Any] | None = None
    refresh_delay: float = 0
    event_calls: int = 0
    market_calls: int = 0
    refresh_calls: list[str] | None = None

    def __post_init__(self) -> None:
        self.refresh_calls = []

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

    async def refresh_market(self, market_id: str) -> MarketSnapshot:
        assert self.refresh_calls is not None
        self.refresh_calls.append(market_id)
        if self.refresh_delay:
            await asyncio.sleep(self.refresh_delay)
        value = None if self.refreshed is None else self.refreshed.get(market_id)
        if isinstance(value, BaseException):
            raise value
        if value is None:
            raise RuntimeError(f"no refresh fixture for {market_id}")
        return value


class _RecordingQueue:
    def __init__(
        self,
        repository: CatalogRepository | None = None,
        *,
        admit: bool = True,
    ) -> None:
        self.items: list[MarketChange] = []
        self._repository = repository
        self._admit = admit

    async def put(self, change: MarketChange) -> bool:
        if self._repository is not None and change.market_id is not None:
            assert await self._repository.get_market(change.market_id) is not None
        self.items.append(change)
        return self._admit


class _FailingCatalog:
    def __init__(self, delegate: CatalogRepository) -> None:
        self._delegate = delegate

    async def load_catalog(self) -> object:
        return await self._delegate.load_catalog()

    async def save_catalog(self, **_: object) -> None:
        raise RuntimeError("forced catalog rollback")


class _MarkerFailingSystemEvents:
    def __init__(self, delegate: SystemEventRepository) -> None:
        self._delegate = delegate
        self._failed = False

    def __getattr__(self, name: str) -> object:
        return getattr(self._delegate, name)

    async def record_market_change_published(
        self,
        change: MarketChange,
        *,
        market_ids: tuple[str, ...] | None = None,
    ) -> int:
        if not self._failed:
            self._failed = True
            raise RuntimeError("forced marker failure")
        return await self._delegate.record_market_change_published(
            change,
            market_ids=market_ids,
        )


class _BlockingDegradedSystemEvents(_MarkerFailingSystemEvents):
    def __init__(self, delegate: SystemEventRepository) -> None:
        super().__init__(delegate)
        self.active_reports = 0
        self.cancelled_reports = 0

    async def append(self, **values: Any) -> int:
        if values.get("event_type") != "SYSTEM_DEGRADED":
            return await self._delegate.append(**values)
        self.active_reports += 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled_reports += 1
            raise
        finally:
            self.active_reports -= 1
        raise AssertionError("unreachable")


class _CursorFailingSystemEvents:
    def __init__(self, delegate: SystemEventRepository) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> object:
        return getattr(self._delegate, name)

    async def record_settlement_refresh_cursor(self, **_: object) -> int:
        raise RuntimeError("forced cursor failure")


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


async def test_all_authoritatively_resolved_event_markets_publish_event_settled(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1", "market-2"))
    resolved = {
        market_id: MarketSnapshot(
            market=replace(
                _snapshot(market_id).market,
                status=MarketStatus.RESOLVED,
                active=False,
                accepting_orders=False,
                enable_orderbook=False,
                resolved_at=600,
            ),
            tokens=_snapshot(market_id).tokens,
            mapping_version=MAPPING_VERSION,
        )
        for market_id in ("market-1", "market-2")
    }
    gateway = _FakeGateway(events=(), markets=(), refreshed=resolved)
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=gateway,
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 610,
        generation_factory=lambda: "sync-settled",
    )

    result = await task.run_once()

    assert result.complete is True
    event = await catalog.get_event("event-1")
    assert event is not None
    assert event.status is MarketStatus.RESOLVED
    assert event.resolved_at == 600
    assert gateway.refresh_calls == ["market-1", "market-2"]
    assert [change.change_type for change in queue.items] == [
        MarketChangeType.EVENT_SETTLED
    ]


@pytest.mark.parametrize(
    "second_refresh",
    [
        RuntimeError("refresh failed"),
        MarketSnapshot(
            market=replace(
                _snapshot("market-2").market,
                status=MarketStatus.CLOSED,
                active=False,
                accepting_orders=False,
                enable_orderbook=False,
            ),
            tokens=_snapshot("market-2").tokens,
            mapping_version=MAPPING_VERSION,
        ),
    ],
)
async def test_missing_event_is_not_guessed_settled_without_all_resolved_proof(
    catalog_runtime,
    second_refresh: object,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1", "market-2"))
    first = _snapshot("market-1")
    resolved_first = MarketSnapshot(
        market=replace(
            first.market,
            status=MarketStatus.RESOLVED,
            active=False,
            accepting_orders=False,
            enable_orderbook=False,
            resolved_at=620,
        ),
        tokens=first.tokens,
        mapping_version=MAPPING_VERSION,
    )
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(),
            markets=(),
            refreshed={
                "market-1": resolved_first,
                "market-2": second_refresh,
            },
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 630,
        generation_factory=lambda: "sync-not-settled",
    )

    result = await task.run_once()

    assert result.complete is True
    event = await catalog.get_event("event-1")
    assert event is not None
    assert event.status is MarketStatus.CLOSED
    assert event.resolved_at is None
    assert MarketChangeType.EVENT_SETTLED not in {
        change.change_type for change in queue.items
    }
    assert all(
        change.change_type is MarketChangeType.MARKET_DEACTIVATED
        for change in queue.items
    )


async def test_missing_market_refresh_still_active_makes_generation_incomplete(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1",))
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(),
            markets=(),
            refreshed={"market-1": _snapshot("market-1")},
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 640,
        generation_factory=lambda: "sync-active-refresh-race",
    )

    result = await task.run_once()

    assert result.complete is False
    market = await catalog.get_market("market-1")
    assert market is not None
    assert market.status is MarketStatus.ACTIVE
    assert market.active is True
    assert queue.items == []


async def test_closed_unresolved_market_is_refreshed_until_later_resolution(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1",))
    first = SyncMarketTask(
        gateway=_FakeGateway(
            events=(),
            markets=(),
            refreshed={"market-1": RuntimeError("not resolved yet")},
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 650,
        generation_factory=lambda: "sync-first-closed",
    )
    assert (await first.run_once()).complete is True
    assert (await catalog.get_market("market-1")).status is MarketStatus.CLOSED  # type: ignore[union-attr]

    snapshot = _snapshot("market-1")
    resolved = MarketSnapshot(
        market=replace(
            snapshot.market,
            status=MarketStatus.RESOLVED,
            active=False,
            accepting_orders=False,
            enable_orderbook=False,
            resolved_at=660,
        ),
        tokens=snapshot.tokens,
        mapping_version=MAPPING_VERSION,
    )
    queue = _RecordingQueue()
    gateway = _FakeGateway(
        events=(),
        markets=(),
        refreshed={"market-1": resolved},
    )
    second = SyncMarketTask(
        gateway=gateway,
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 670,
        generation_factory=lambda: "sync-later-resolved",
    )

    assert (await second.run_once()).complete is True
    assert gateway.refresh_calls == ["market-1"]
    assert [item.change_type for item in queue.items] == [
        MarketChangeType.EVENT_SETTLED
    ]


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


async def test_complete_after_new_incomplete_replays_added_after_restart(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    snapshot = _snapshot("market-1")
    incomplete = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot, object()),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 410,
        generation_factory=lambda: "sync-incomplete-new",
    )
    assert (await incomplete.run_once()).complete is False

    queue = _RecordingQueue()
    recovered = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot,),
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 420,
        generation_factory=lambda: "sync-complete-new",
    )

    assert (await recovered.run_once()).complete is True
    assert [(item.change_type, item.market_id) for item in queue.items] == [
        (MarketChangeType.MARKET_ADDED, "market-1")
    ]


async def test_complete_after_existing_incomplete_replays_critical_update(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    original = _snapshot("market-1", question="Original?")
    first_queue = _RecordingQueue()
    first = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",), title="Original event"),),
            markets=(original,),
        ),
        catalog=catalog,
        changes=first_queue,
        system_events=system_events,
        clock_ms=lambda: 430,
        generation_factory=lambda: "sync-first-complete",
    )
    assert (await first.run_once()).complete is True
    assert first_queue.items[0].change_type is MarketChangeType.MARKET_ADDED

    changed = _snapshot("market-1", question="Changed?")
    incomplete = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",), title="Changed event"),),
            markets=(changed, object()),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 440,
        generation_factory=lambda: "sync-existing-incomplete",
    )
    assert (await incomplete.run_once()).complete is False

    queue = _RecordingQueue()
    recovered = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",), title="Changed event"),),
            markets=(changed,),
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 450,
        generation_factory=lambda: "sync-existing-recovered",
    )

    assert (await recovered.run_once()).complete is True
    assert len(queue.items) == 1
    assert queue.items[0].change_type is MarketChangeType.MARKET_UPDATED
    assert queue.items[0].critical is True


async def test_dropped_added_is_not_recorded_as_publication_baseline(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    snapshot = _snapshot("market-1")
    dropped = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot,),
        ),
        catalog=catalog,
        changes=_RecordingQueue(admit=False),
        system_events=system_events,
        clock_ms=lambda: 460,
        generation_factory=lambda: "sync-dropped",
    )
    result = await dropped.run_once()
    assert result.changes_dropped == 1

    incomplete = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot, object()),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 470,
        generation_factory=lambda: "sync-after-drop-incomplete",
    )
    assert (await incomplete.run_once()).complete is False

    queue = _RecordingQueue()
    recovered = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot,),
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 480,
        generation_factory=lambda: "sync-after-drop-complete",
    )
    await recovered.run_once()

    assert queue.items[0].change_type is MarketChangeType.MARKET_ADDED


async def test_publication_marker_is_idempotent_by_change_identity(
    catalog_runtime,
) -> None:
    _, system_events = catalog_runtime
    change = MarketChange(
        change_id="sync-1:MARKET_ADDED:market-1",
        change_type=MarketChangeType.MARKET_ADDED,
        event_id="event-1",
        market_id="market-1",
        token_ids=("token-1", "token-2"),
        occurred_at=490,
    )

    first_id = await system_events.record_market_change_published(change)
    second_id = await system_events.record_market_change_published(change)

    assert second_id == first_id
    rows = await system_events.read_after(0)
    assert [row["event_type"] for row in rows] == ["MARKET_CHANGE_PUBLISHED"]


async def test_marker_failure_degrades_but_does_not_skip_later_critical_control(
    catalog_runtime,
) -> None:
    catalog, real_system_events = catalog_runtime
    await _seed(catalog, ("market-2",))
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(_snapshot("market-1"),),
            refreshed={"market-2": RuntimeError("not resolved")},
        ),
        catalog=catalog,
        changes=queue,
        system_events=_MarkerFailingSystemEvents(real_system_events),  # type: ignore[arg-type]
        clock_ms=lambda: 700,
        generation_factory=lambda: "sync-marker-failure",
    )

    result = await task.run_once()

    assert result.complete is True
    assert result.degraded is True
    assert result.publication_marker_failures == 1
    assert [item.change_type for item in queue.items] == [
        MarketChangeType.MARKET_ADDED,
        MarketChangeType.MARKET_DEACTIVATED,
    ]
    events = await real_system_events.read_after(0)
    degraded = [event for event in events if event["event_type"] == "SYSTEM_DEGRADED"]
    assert len(degraded) == 1
    assert degraded[0]["details"]["failed_change_id"].endswith("market-1")


async def test_blocked_degraded_report_cannot_delay_later_critical_control(
    catalog_runtime,
) -> None:
    catalog, real_system_events = catalog_runtime
    await _seed(catalog, ("market-2",))
    queue = _RecordingQueue()
    blocking_events = _BlockingDegradedSystemEvents(real_system_events)
    task = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(_snapshot("market-1"),),
            refreshed={"market-2": RuntimeError("not resolved")},
        ),
        catalog=catalog,
        changes=queue,
        system_events=blocking_events,  # type: ignore[arg-type]
        clock_ms=lambda: 705,
        generation_factory=lambda: "sync-blocked-degraded-report",
    )

    result = await asyncio.wait_for(task.run_once(), timeout=0.2)

    assert [item.change_type for item in queue.items] == [
        MarketChangeType.MARKET_ADDED,
        MarketChangeType.MARKET_DEACTIVATED,
    ]
    assert result.degraded is True
    assert task.degraded is True
    assert blocking_events.cancelled_reports == 1
    assert blocking_events.active_reports == 0


async def test_deactivated_publication_baseline_reactivation_replays_added(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    snapshot = _snapshot("market-1")
    initial = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot,),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 710,
        generation_factory=lambda: "sync-active",
    )
    await initial.run_once()
    deactivated = SyncMarketTask(
        gateway=_FakeGateway(
            events=(),
            markets=(),
            refreshed={"market-1": RuntimeError("not resolved")},
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 720,
        generation_factory=lambda: "sync-deactivated",
    )
    await deactivated.run_once()
    incomplete = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot, object()),
        ),
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=system_events,
        clock_ms=lambda: 730,
        generation_factory=lambda: "sync-reactivation-incomplete",
    )
    assert (await incomplete.run_once()).complete is False

    queue = _RecordingQueue()
    recovered = SyncMarketTask(
        gateway=_FakeGateway(
            events=(_event(("market-1",)),),
            markets=(snapshot,),
        ),
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 740,
        generation_factory=lambda: "sync-reactivated",
    )
    await recovered.run_once()

    assert queue.items[0].change_type is MarketChangeType.MARKET_ADDED


async def test_settlement_refresh_budget_uses_persistent_fair_cursor(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    market_ids = tuple(f"market-{index}" for index in range(1, 6))
    await _seed(catalog, market_ids)
    observed: list[tuple[str, ...]] = []
    generations = iter(("refresh-1", "refresh-2", "refresh-3"))
    for occurred_at in (750, 760, 770):
        gateway = _FakeGateway(
            events=(),
            markets=(),
            refreshed={
                market_id: RuntimeError("not resolved")
                for market_id in market_ids
            },
        )
        task = SyncMarketTask(
            gateway=gateway,
            catalog=catalog,
            changes=_RecordingQueue(),
            system_events=system_events,
            clock_ms=lambda occurred_at=occurred_at: occurred_at,
            generation_factory=lambda: next(generations),
            settlement_refresh_budget=2,
            settlement_refresh_timeout_seconds=0.05,
        )
        await task.run_once()
        assert gateway.refresh_calls is not None
        observed.append(tuple(gateway.refresh_calls))

    assert observed == [
        ("market-1", "market-2"),
        ("market-3", "market-4"),
        ("market-5", "market-1"),
    ]


async def test_settlement_refresh_timeout_is_fail_closed_and_bounded(
    catalog_runtime,
) -> None:
    catalog, system_events = catalog_runtime
    await _seed(catalog, ("market-1", "market-2"))
    gateway = _FakeGateway(
        events=(),
        markets=(),
        refreshed={
            "market-1": _snapshot("market-1"),
            "market-2": _snapshot("market-2"),
        },
        refresh_delay=0.2,
    )
    queue = _RecordingQueue()
    task = SyncMarketTask(
        gateway=gateway,
        catalog=catalog,
        changes=queue,
        system_events=system_events,
        clock_ms=lambda: 780,
        generation_factory=lambda: "refresh-timeout",
        settlement_refresh_budget=1,
        settlement_refresh_timeout_seconds=0.005,
    )

    result = await asyncio.wait_for(task.run_once(), timeout=0.1)

    assert result.complete is True
    assert gateway.refresh_calls == ["market-1"]
    assert (await catalog.get_event("event-1")).status is MarketStatus.CLOSED  # type: ignore[union-attr]
    assert all(
        item.change_type is MarketChangeType.MARKET_DEACTIVATED
        for item in queue.items
    )


async def test_cursor_failure_degrades_and_keeps_in_process_fair_progress(
    catalog_runtime,
) -> None:
    catalog, real_system_events = catalog_runtime
    await _seed(catalog, ("market-1", "market-2", "market-3"))
    gateway = _FakeGateway(
        events=(),
        markets=(),
        refreshed={
            market_id: RuntimeError("not resolved")
            for market_id in ("market-1", "market-2", "market-3")
        },
    )
    generations = iter(("cursor-failure-1", "cursor-failure-2"))
    task = SyncMarketTask(
        gateway=gateway,
        catalog=catalog,
        changes=_RecordingQueue(),
        system_events=_CursorFailingSystemEvents(real_system_events),  # type: ignore[arg-type]
        clock_ms=lambda: 785,
        generation_factory=lambda: next(generations),
        settlement_refresh_budget=1,
    )

    first = await task.run_once()
    second = await task.run_once()

    assert gateway.refresh_calls == ["market-1", "market-2"]
    assert first.degraded is True
    assert first.cursor_persistence_failed is True
    assert second.degraded is True
    assert second.cursor_persistence_failed is True
    assert task.degraded is True
    assert await real_system_events.get_settlement_refresh_cursor() is None


@pytest.mark.parametrize(
    "timeout",
    (float("nan"), float("inf"), float("-inf"), 0.0, -1.0),
)
async def test_settlement_refresh_timeout_must_be_finite_and_positive(
    catalog_runtime,
    timeout: float,
) -> None:
    catalog, system_events = catalog_runtime

    with pytest.raises(ValueError, match="finite positive"):
        SyncMarketTask(
            gateway=_FakeGateway(events=(), markets=()),
            catalog=catalog,
            changes=_RecordingQueue(),
            system_events=system_events,
            clock_ms=lambda: 790,
            settlement_refresh_timeout_seconds=timeout,
        )


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
