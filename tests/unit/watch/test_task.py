from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import DecisionReason, NotEvaluable
from predmarket.persistence.repositories import CatalogSnapshot
from predmarket.polymarket.gateway import MarketStreamEvent, MarketStreamInvalidated
from predmarket.watch.cache import CacheState
from predmarket.watch.task import EvaluationTarget, WatchTask


def _market(market_id: str, event_id: str = "event-1", *, active: bool = True) -> Market:
    return Market(
        id=market_id,
        event_id=event_id,
        condition_id=f"condition-{market_id}",
        question=f"Question {market_id}",
        status=MarketStatus.ACTIVE if active else MarketStatus.CLOSED,
        active=active,
        accepting_orders=active,
        enable_orderbook=active,
        sync_generation="sync-1",
        sync_generation_complete=True,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def _token(token_id: str, market_id: str, position: int = 0) -> Token:
    return Token(
        id=token_id,
        market_id=market_id,
        outcome=f"outcome-{position}",
        position=position,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )


def _event(market_ids: tuple[str, ...], *, event_id: str = "event-1") -> Event:
    return Event(
        id=event_id,
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=market_ids,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )


def _catalog(*, second_market: bool = False, first_active: bool = True) -> CatalogSnapshot:
    markets = [_market("market-1", active=first_active)]
    tokens = [_token("token-1", "market-1", 0), _token("token-2", "market-1", 1)]
    if second_market:
        markets.append(_market("market-2"))
        tokens.extend((_token("token-3", "market-2", 0), _token("token-4", "market-2", 1)))
    return CatalogSnapshot(
        events=(_event(tuple(market.id for market in markets)),),
        markets=tuple(markets),
        tokens=tuple(tokens),
    )


def _book(token_id: str, generation: int, *, market_id: str | None = None) -> OrderBook:
    return OrderBook(
        market_id=market_id or ("market-2" if token_id in {"token-3", "token-4"} else "market-1"),
        token_id=token_id,
        bids=(OrderBookLevel(Decimal("0.40"), Decimal("3")),),
        asks=(OrderBookLevel(Decimal("0.50"), Decimal("4")),),
        subscription_generation=generation,
        book_hash=f"hash-{generation}-{token_id}",
        exchange_timestamp=100,
        received_timestamp=101,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


class FakeSubscription:
    def __init__(self, generation: int) -> None:
        self.subscription_generation = generation
        self.items: asyncio.Queue[Any] = asyncio.Queue()
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.items.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        self.closed = True


class FakeGateway:
    def __init__(self) -> None:
        self.generations = 0
        self.requests: list[tuple[str, ...]] = []
        self.subscriptions: list[FakeSubscription] = []
        self.recovery_gate: asyncio.Event | None = None

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        self.requests.append(normalized)
        subscription = FakeSubscription(generation)
        self.subscriptions.append(subscription)
        if self.recovery_gate is not None:
            await self.recovery_gate.wait()
        return SimpleNamespace(
            order_books=tuple(_book(token_id, generation) for token_id in normalized),
            subscription=subscription,
            subscription_generation=generation,
        )


class FakeCatalog:
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        self.snapshot = snapshot

    async def load_catalog(self) -> CatalogSnapshot:
        return self.snapshot


class FakeChanges:
    def __init__(self) -> None:
        self.items: asyncio.Queue[MarketChange] = asyncio.Queue()
        self.done = 0

    async def get(self) -> MarketChange:
        return await self.items.get()

    def task_done(self) -> None:
        self.done += 1


class FakeContextSource:
    def contexts_for(
        self,
        changed_token_id: str,
        orderbooks: tuple[OrderBook, ...],
    ) -> tuple[EvaluationTarget, ...]:
        return (
            EvaluationTarget(
                context=SimpleNamespace(
                    changed_token_id=changed_token_id,
                    orderbooks=orderbooks,
                ),  # type: ignore[arg-type]
                opportunity_key=f"opportunity:{changed_token_id}",
                expected_revision=None,
            ),
        )


class FakeStrategy:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    def evaluate(self, context: Any) -> NotEvaluable:
        self.calls.append(context)
        return NotEvaluable(
            DecisionReason.INPUT_METADATA_MISSING,
            {"changed_token_id": context.changed_token_id},
        )


class FakeSignals:
    def __init__(self) -> None:
        self.applied: list[tuple[Any, str, int | None]] = []
        self.closed: list[tuple[tuple[str, ...], NotEvaluable]] = []

    async def apply(
        self,
        decision: Any,
        opportunity_key: str,
        expected_revision: int | None,
    ) -> str:
        self.applied.append((decision, opportunity_key, expected_revision))
        return f"signal-{len(self.applied)}"

    async def close_for_tokens(
        self,
        token_ids: tuple[str, ...],
        decision: NotEvaluable,
    ) -> None:
        self.closed.append((token_ids, decision))


def _watch(
    *,
    gateway: FakeGateway | None = None,
    catalog: FakeCatalog | None = None,
    changes: FakeChanges | None = None,
    strategy: FakeStrategy | None = None,
    signals: FakeSignals | None = None,
) -> tuple[WatchTask, FakeGateway, FakeCatalog, FakeChanges, FakeStrategy, FakeSignals]:
    gateway = gateway or FakeGateway()
    catalog = catalog or FakeCatalog(_catalog())
    changes = changes or FakeChanges()
    strategy = strategy or FakeStrategy()
    signals = signals or FakeSignals()
    return (
        WatchTask(
            gateway=gateway,
            catalog=catalog,
            changes=changes,
            strategy_engine=strategy,
            signal_manager=signals,
            context_source=FakeContextSource(),
        ),
        gateway,
        catalog,
        changes,
        strategy,
        signals,
    )


async def _cancel(task: asyncio.Task[Any]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_run_subscribes_all_initial_watchable_tokens_and_evaluates_after_rest() -> None:
    # Catches startup analyzing before the complete REST recovery baseline exists.
    gateway = FakeGateway()
    gateway.recovery_gate = asyncio.Event()
    watch, _, _, _, strategy, _ = _watch(gateway=gateway)

    task = asyncio.create_task(watch.run())
    await asyncio.sleep(0)

    assert gateway.requests == [("token-1", "token-2")]
    assert strategy.calls == []
    assert watch.cache.state is CacheState.INVALID
    gateway.recovery_gate.set()
    for _ in range(10):
        await asyncio.sleep(0)
        if watch.cache.state is CacheState.VALID:
            break

    assert watch.cache.state is CacheState.VALID
    assert {call.changed_token_id for call in strategy.calls} == {"token-1", "token-2"}
    await _cancel(task)
    assert gateway.subscriptions[0].closed is True


async def test_market_add_rebuilds_subscription_and_closes_old_generation() -> None:
    # Catches dynamic catalog additions being absent until process restart.
    watch, gateway, catalog, _, _, signals = _watch()
    await watch.start()
    first = gateway.subscriptions[0]
    catalog.snapshot = _catalog(second_market=True)

    await watch.handle_market_change(
        MarketChange(
            change_id="change-1",
            change_type=MarketChangeType.MARKET_ADDED,
            event_id="event-1",
            market_id="market-2",
            token_ids=("token-3", "token-4"),
            occurred_at=200,
        )
    )

    assert first.closed is True
    assert gateway.requests[-1] == ("token-1", "token-2", "token-3", "token-4")
    assert signals.closed[-1][0] == ("token-1", "token-2")
    assert signals.closed[-1][1].reason_code is DecisionReason.ORDERBOOK_INVALID
    await watch.close()


async def test_deactivated_market_unsubscribes_and_closes_with_market_reason() -> None:
    # Catches inactive market signals surviving after its subscription is removed.
    watch, gateway, catalog, _, _, signals = _watch()
    await watch.start()
    catalog.snapshot = _catalog(first_active=False, second_market=True)

    await watch.handle_market_change(
        MarketChange(
            change_id="change-2",
            change_type=MarketChangeType.MARKET_DEACTIVATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=201,
            critical=True,
        )
    )

    assert gateway.requests[-1] == ("token-3", "token-4")
    assert any(
        token_ids == ("token-1", "token-2")
        and decision.reason_code is DecisionReason.MARKET_CLOSED
        for token_ids, decision in signals.closed
    )
    await watch.close()


async def test_old_generation_stream_message_is_ignored() -> None:
    # Catches a late event from a closed SDK handle triggering evaluation.
    watch, _, catalog, _, strategy, _ = _watch()
    await watch.start()
    catalog.snapshot = _catalog(second_market=True)
    await watch.handle_market_change(
        MarketChange(
            change_id="rotate",
            change_type=MarketChangeType.MARKET_ADDED,
            event_id="event-1",
            market_id="market-2",
            token_ids=("token-3", "token-4"),
            occurred_at=105,
        )
    )
    before = len(strategy.calls)

    await watch.handle_stream_message(
        MarketStreamEvent(
            event_type="price_change",
            market_id="market-1",
            payload={
                "timestamp": 110,
                "price_changes": [
                    {
                        "token_id": "token-1",
                        "side": "BUY",
                        "price": "0.41",
                        "size": "9",
                        "hash": "late-hash",
                    }
                ],
            },
            received_timestamp=111,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    assert len(strategy.calls) == before
    assert watch.cache.state is CacheState.VALID
    await watch.close()


async def test_sdk_invalidation_closes_before_rest_recovery_and_never_evaluates_invalid() -> None:
    # Catches OPEN signals surviving a disconnect or strategy crossing the REST barrier.
    gateway = FakeGateway()
    watch, _, _, _, strategy, signals = _watch(gateway=gateway)
    await watch.start()
    before = len(strategy.calls)
    gateway.recovery_gate = asyncio.Event()
    invalidation = MarketStreamInvalidated(
        reason="connection_lost",
        token_ids=("token-1", "token-2"),
        received_timestamp=120,
        subscription_generation=1,
        mapping_version="mapping-v1",
    )

    recovery = asyncio.create_task(watch.handle_stream_message(invalidation))
    for _ in range(10):
        await asyncio.sleep(0)
        if signals.closed:
            break

    assert watch.cache.state is CacheState.INVALID
    assert len(strategy.calls) == before
    assert signals.closed[-1][1].reason_code is DecisionReason.SDK_DISCONNECTED
    gateway.recovery_gate.set()
    await recovery

    assert watch.cache.state is CacheState.VALID
    assert len(strategy.calls) > before
    await watch.close()


async def test_price_change_uses_local_arrival_sequence_and_evaluates_changed_token() -> None:
    # Catches valid canonical deltas failing to reach affected strategy routing.
    watch, _, _, _, strategy, _ = _watch()
    await watch.start()
    before = len(strategy.calls)

    await watch.handle_stream_message(
        MarketStreamEvent(
            event_type="price_change",
            market_id="market-1",
            payload={
                "timestamp": 110,
                "price_changes": [
                    {
                        "token_id": "token-1",
                        "side": "BUY",
                        "price": "0.41",
                        "size": "9",
                        "hash": "post-hash",
                    }
                ],
            },
            received_timestamp=111,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    assert watch.cache.last_sequence == 1
    assert len(strategy.calls) == before + 1
    assert strategy.calls[-1].changed_token_id == "token-1"
    await watch.close()


async def test_event_settled_removes_tokens_and_closes_with_settlement_reason() -> None:
    # Catches a stream settlement remaining open until the next catalog poll.
    watch, gateway, _, _, _, signals = _watch()
    await watch.start()

    await watch.handle_stream_message(
        MarketStreamEvent(
            event_type="market_resolved",
            market_id="market-1",
            payload={"token_ids": ["token-1", "token-2"], "timestamp": 120},
            received_timestamp=121,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    assert signals.closed[-1][1].reason_code is DecisionReason.EVENT_SETTLED
    assert watch.active_token_ids == ()
    assert gateway.subscriptions[0].closed is True
    await watch.close()
