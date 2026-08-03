from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from decimal import Decimal
import logging
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
from predmarket.watch.task import (
    EvaluationTarget,
    WatchCleanupError,
    WatchTask,
    _timestamp_ms,
)


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


class FailOnceCloseSubscription(FakeSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        if self.subscription_generation == 2 and self.close_calls == 1:
            raise RuntimeError("late close failed")
        self.closed = True


class SelfCancellingCloseSubscription(FakeSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        if self.subscription_generation == 2 and self.close_calls == 1:
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            await asyncio.sleep(0)
        self.closed = True


class ActiveFailOnceCloseSubscription(FakeSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            raise RuntimeError("active close failed")
        self.closed = True


class ActiveSelfCancellingCloseSubscription(FakeSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            await asyncio.sleep(0)
        self.closed = True


class BlockingFailOnceCloseSubscription(ActiveFailOnceCloseSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.close_started = asyncio.Event()
        self.release_close = asyncio.Event()

    async def close(self) -> None:
        self.close_calls += 1
        if self.close_calls == 1:
            self.close_started.set()
            await self.release_close.wait()
            raise RuntimeError("active close failed")
        self.closed = True


class BlockingCloseSubscription(FakeSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.close_started = asyncio.Event()
        self.release_close = asyncio.Event()

    async def close(self) -> None:
        self.close_started.set()
        await self.release_close.wait()
        self.closed = True


class CancellationDelayedSubscription(FakeSubscription):
    def __init__(self, generation: int) -> None:
        super().__init__(generation)
        self.read_cancelled = asyncio.Event()
        self.release_reader = asyncio.Event()
        self.reader_finished = asyncio.Event()

    async def __anext__(self):
        try:
            await self.items.get()
            raise AssertionError("no stream item expected")
        except asyncio.CancelledError:
            self.read_cancelled.set()
            await self.release_reader.wait()
            self.reader_finished.set()
            raise


class FakeGateway:
    def __init__(self) -> None:
        self.generations = 0
        self.requests: list[tuple[str, ...]] = []
        self.subscriptions: list[FakeSubscription] = []
        self.recovery_gate: asyncio.Event | None = None
        self.ignore_recovery_cancellation = False
        self.ignore_recovery_cancellation_count = 0
        self.recovery_cancellations = 0
        self.recovery_cancelled = asyncio.Event()
        self.subscription_factory = FakeSubscription

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        self.requests.append(normalized)
        subscription = self.subscription_factory(generation)
        self.subscriptions.append(subscription)
        if self.recovery_gate is not None:
            remaining = self.ignore_recovery_cancellation_count
            if self.ignore_recovery_cancellation and remaining == 0:
                remaining = 1
            while not self.recovery_gate.is_set():
                try:
                    await self.recovery_gate.wait()
                except asyncio.CancelledError:
                    self.recovery_cancellations += 1
                    self.recovery_cancelled.set()
                    if remaining == 0:
                        raise
                    remaining -= 1
        return SimpleNamespace(
            order_books=tuple(_book(token_id, generation) for token_id in normalized),
            subscription=subscription,
            subscription_generation=generation,
        )


class FailSecondRecoveryGateway(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.recovery_calls = 0

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.recovery_calls += 1
        if self.recovery_calls == 2:
            raise RuntimeError("recovery unavailable")
        return await super().recover_market_session(token_ids)


class FakeCatalog:
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        self.snapshot = snapshot

    async def load_catalog(self) -> CatalogSnapshot:
        return self.snapshot


class FakeChanges:
    def __init__(self) -> None:
        self.items: asyncio.Queue[MarketChange] = asyncio.Queue()
        self.done = 0
        self.joined = asyncio.Event()

    async def get(self) -> MarketChange:
        return await self.items.get()

    def task_done(self) -> None:
        self.done += 1
        self.joined.set()

    async def join(self) -> None:
        await self.joined.wait()


class CancelOnReturnChanges(FakeChanges):
    def __init__(self) -> None:
        super().__init__()
        self.outer_task: asyncio.Task[Any] | None = None

    async def get(self) -> MarketChange:
        change = await super().get()
        assert self.outer_task is not None
        asyncio.get_running_loop().call_soon(self.outer_task.cancel)
        return change


class ReturnChangeOnCancellation(FakeChanges):
    def __init__(self, change: MarketChange) -> None:
        super().__init__()
        self.change = change
        self.get_started = asyncio.Event()

    async def get(self) -> MarketChange:
        self.get_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return self.change


class DelayedReturnChangeOnCancellation(ReturnChangeOnCancellation):
    def __init__(self, change: MarketChange) -> None:
        super().__init__(change)
        self.cancel_caught = asyncio.Event()
        self.release_result = asyncio.Event()

    async def get(self) -> MarketChange:
        self.get_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancel_caught.set()
            await self.release_result.wait()
            return self.change


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
        self.close_entered = asyncio.Event()
        self.close_gate: asyncio.Event | None = None

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
        self.close_entered.set()
        if self.close_gate is not None:
            await self.close_gate.wait()


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
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.requests:
            break

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


async def test_close_wakes_run_with_no_active_tokens() -> None:
    # Catches a no-subscription run loop remaining blocked forever in changes.get().
    catalog = FakeCatalog(_catalog(first_active=False))
    watch, _, _, _, _, _ = _watch(catalog=catalog)
    running = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if watch.active_token_ids == ():
            break

    await watch.close()

    await asyncio.wait_for(asyncio.shield(running), timeout=0.1)
    assert running.done() is True


async def test_start_and_close_allow_an_empty_catalog_without_recovery() -> None:
    watch, gateway, _, _, _, _ = _watch(
        catalog=FakeCatalog(CatalogSnapshot(events=(), markets=(), tokens=()))
    )

    await watch.start()
    assert watch.active_token_ids == ()
    assert gateway.requests == []

    running = asyncio.create_task(watch.run())
    await asyncio.sleep(0)
    await watch.close()
    await asyncio.wait_for(asyncio.shield(running), timeout=0.1)
    assert running.done() is True


async def test_acquired_change_is_acknowledged_once_when_reader_cleanup_is_cancelled() -> None:
    # Catches cancellation between successful queue get and pending-reader cleanup.
    gateway = FakeGateway()
    gateway.subscription_factory = CancellationDelayedSubscription
    changes = FakeChanges()
    watch, _, _, _, _, _ = _watch(gateway=gateway, changes=changes)
    running = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.subscriptions:
            break
    subscription = gateway.subscriptions[0]
    assert isinstance(subscription, CancellationDelayedSubscription)
    await changes.items.put(
        MarketChange(
            change_id="ack-change",
            change_type=MarketChangeType.MARKET_UPDATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=200,
        )
    )
    await subscription.read_cancelled.wait()

    running.cancel()
    await asyncio.sleep(0)
    assert changes.done == 0

    subscription.release_reader.set()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert subscription.reader_finished.is_set()
    assert changes.done == 1
    await asyncio.wait_for(changes.join(), timeout=0.1)


async def test_change_result_is_claimed_when_outer_wait_is_simultaneously_cancelled() -> None:
    # Catches a completed get result bypassing acknowledgement via wait cancellation.
    changes = CancelOnReturnChanges()
    catalog = FakeCatalog(_catalog(first_active=False))
    watch, _, _, _, _, _ = _watch(catalog=catalog, changes=changes)
    running = asyncio.create_task(watch.run())
    changes.outer_task = running
    await changes.items.put(
        MarketChange(
            change_id="simultaneous-cancel",
            change_type=MarketChangeType.MARKET_UPDATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=200,
        )
    )

    with pytest.raises(asyncio.CancelledError):
        await running

    assert changes.done == 1
    await asyncio.wait_for(changes.join(), timeout=0.1)


async def test_change_returned_during_stop_reader_drain_is_claimed_once() -> None:
    # Catches get producing its owned item while stop cancels pending readers.
    change = MarketChange(
        change_id="return-during-drain",
        change_type=MarketChangeType.MARKET_UPDATED,
        event_id="event-1",
        market_id="market-1",
        token_ids=("token-1", "token-2"),
        occurred_at=200,
    )
    changes = ReturnChangeOnCancellation(change)
    catalog = FakeCatalog(_catalog(first_active=False))
    watch, _, _, _, _, _ = _watch(catalog=catalog, changes=changes)
    running = asyncio.create_task(watch.run())
    await changes.get_started.wait()

    await watch.close()
    await running

    assert changes.done == 1
    await asyncio.wait_for(changes.join(), timeout=0.1)


async def test_double_cancel_during_stop_drain_still_claims_change_once() -> None:
    # Catches repeated run cancellation skipping the post-drain result claim.
    change = MarketChange(
        change_id="double-cancel-drain",
        change_type=MarketChangeType.MARKET_UPDATED,
        event_id="event-1",
        market_id="market-1",
        token_ids=("token-1", "token-2"),
        occurred_at=200,
    )
    changes = DelayedReturnChangeOnCancellation(change)
    catalog = FakeCatalog(_catalog(first_active=False))
    watch, _, _, _, _, _ = _watch(catalog=catalog, changes=changes)
    running = asyncio.create_task(watch.run())
    await changes.get_started.wait()
    await watch.close()
    await changes.cancel_caught.wait()

    running.cancel()
    running.cancel()
    changes.release_result.set()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert changes.done == 1
    await asyncio.wait_for(changes.join(), timeout=0.1)


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


async def test_start_and_rotation_log_unique_subscribed_market_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    watch, _, catalog, _, _, _ = _watch()
    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()
        catalog.snapshot = _catalog(second_market=True)
        await watch.handle_market_change(
            MarketChange(
                change_id="change-log-count",
                change_type=MarketChangeType.MARKET_ADDED,
                event_id="event-1",
                market_id="market-2",
                token_ids=("token-3", "token-4"),
                occurred_at=200,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert messages[0].startswith("watch_subscribed ")
    assert "markets=1" in messages[0]
    assert "tokens=2" in messages[0]
    assert "generation=1" in messages[0]
    assert messages[-1].startswith("watch_subscribed ")
    assert "markets=2" in messages[-1]
    assert "tokens=4" in messages[-1]
    assert "generation=2" in messages[-1]
    await watch.close()


async def test_failed_rotation_does_not_log_successful_subscription(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway = FailSecondRecoveryGateway()
    watch, _, catalog, _, _, _ = _watch(gateway=gateway)
    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()
        catalog.snapshot = _catalog(second_market=True)
        with pytest.raises(RuntimeError, match="recovery unavailable"):
            await watch.handle_market_change(
                MarketChange(
                    change_id="change-log-failure",
                    change_type=MarketChangeType.MARKET_ADDED,
                    event_id="event-1",
                    market_id="market-2",
                    token_ids=("token-3", "token-4"),
                    occurred_at=200,
                )
            )

    messages = [record.getMessage() for record in caplog.records]
    assert [message for message in messages if message.startswith("watch_subscribed ")] == [
        "watch_subscribed markets=1 tokens=2 generation=1"
    ]
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


async def test_future_generation_fails_closed_and_recovers_without_applying_message() -> None:
    # Catches an ownership violation being silently treated as a late old message.
    watch, gateway, _, _, _, signals = _watch()
    await watch.start()

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
                        "hash": "future-hash",
                    }
                ],
            },
            received_timestamp=111,
            subscription_generation=2,
            mapping_version="mapping-v1",
        )
    )

    assert gateway.subscriptions[0].closed is True
    assert watch.cache.generation == 2
    assert watch.cache.state is CacheState.VALID
    assert signals.closed[-1][1].reason_code is DecisionReason.ORDERBOOK_INVALID
    book = watch.cache.get("token-1")
    assert book is not None
    assert tuple((level.price, level.size) for level in book.bids) == (
        (Decimal("0.40"), Decimal("3")),
    )
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


async def test_close_cancels_recovery_and_closes_late_session_without_evaluation() -> None:
    # Catches close returning while a non-cooperative recovery later installs a handle.
    gateway = FakeGateway()
    watch, _, _, _, strategy, _ = _watch(gateway=gateway)
    await watch.start()
    baseline_calls = len(strategy.calls)
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation = True
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    closing = asyncio.create_task(watch.close())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancelled.is_set():
            break

    assert closing.done() is False
    assert gateway.recovery_cancelled.is_set()
    gateway.recovery_gate.set()
    await closing
    await recovering

    assert gateway.subscriptions[1].closed is True
    assert watch.cache.state is CacheState.INVALID
    assert len(strategy.calls) == baseline_calls


async def test_cancelled_close_waits_for_owned_recovery_and_handle_cleanup() -> None:
    # Catches caller cancellation orphaning the late SDK recovery session.
    gateway = FakeGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation = True
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    closing = asyncio.create_task(watch.close())
    await asyncio.sleep(0)
    closing.cancel()
    await asyncio.sleep(0)
    assert closing.done() is False

    gateway.recovery_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    await recovering

    assert gateway.subscriptions[1].closed is True
    assert all(subscription.closed for subscription in gateway.subscriptions)


async def test_cancelled_recovery_handler_closes_noncooperative_late_session() -> None:
    # Catches direct handler cancellation losing a session returned after cancellation.
    gateway = FakeGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation = True
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    recovering.cancel()
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancelled.is_set():
            break
    gateway.recovery_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await recovering

    assert gateway.subscriptions[1].closed is True
    await watch.close()


async def test_close_during_signal_closure_forbids_late_rotation_recovery() -> None:
    # Catches rotation starting recovery after close cleanup already observed none.
    gateway = FakeGateway()
    catalog = FakeCatalog(_catalog())
    signals = FakeSignals()
    signals.close_gate = asyncio.Event()
    watch, _, _, _, _, _ = _watch(
        gateway=gateway,
        catalog=catalog,
        signals=signals,
    )
    await watch.start()
    catalog.snapshot = _catalog(second_market=True)
    gateway.recovery_gate = asyncio.Event()
    rotating = asyncio.create_task(
        watch.handle_market_change(
            MarketChange(
                change_id="late-rotation",
                change_type=MarketChangeType.MARKET_ADDED,
                event_id="event-1",
                market_id="market-2",
                token_ids=("token-3", "token-4"),
                occurred_at=200,
            )
        )
    )
    await signals.close_entered.wait()

    closing = asyncio.create_task(watch.close())
    await asyncio.sleep(0)
    signals.close_gate.set()
    for _ in range(20):
        await asyncio.sleep(0)
        if rotating.done() or len(gateway.subscriptions) > 1:
            break
    late_recovery_started = len(gateway.subscriptions) > 1
    gateway.recovery_gate.set()
    await rotating
    await closing

    assert late_recovery_started is False
    assert len(gateway.subscriptions) == 1
    assert gateway.subscriptions[0].closed is True


async def test_double_cancelled_recovery_drains_and_closes_late_handle() -> None:
    # Catches a second caller cancellation killing the owned recovery cleanup.
    gateway = FakeGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation_count = 2
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    recovering.cancel()
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancellations == 1:
            break
    recovering.cancel()
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancellations == 2:
            break
    gateway.recovery_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await recovering

    assert gateway.recovery_cancellations == 1
    assert gateway.subscriptions[1].closed is True
    await watch.close()


async def test_handler_cancel_and_concurrent_close_cancel_gateway_only_once() -> None:
    # Catches handler and close each propagating cancellation to one recovery target.
    gateway = FakeGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation_count = 1
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    recovering.cancel()
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancellations == 1:
            break
    closing = asyncio.create_task(watch.close())
    for _ in range(20):
        await asyncio.sleep(0)
    gateway.recovery_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await recovering
    await closing

    assert gateway.recovery_cancellations == 1
    assert gateway.subscriptions[1].closed is True


async def test_multiple_cancelled_close_waiters_share_one_terminal_cleanup() -> None:
    # Catches multiple close waiters caching cancellation or cancelling recovery twice.
    gateway = FakeGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation_count = 1
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    first = asyncio.create_task(watch.close())
    second = asyncio.create_task(watch.close())
    await asyncio.sleep(0)
    first.cancel()
    second.cancel()
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancellations:
            break
    gateway.recovery_gate.set()
    for waiter in (first, second):
        with pytest.raises(asyncio.CancelledError):
            await waiter
    await recovering
    await watch.close()

    assert gateway.recovery_cancellations == 1
    assert gateway.subscriptions[1].closed is True


async def test_late_recovery_close_failure_reaches_handler_and_shared_close_waiters_then_retries() -> None:
    # Catches a failed late-handle close being reported as successful and forgotten.
    gateway = FakeGateway()
    gateway.subscription_factory = FailOnceCloseSubscription
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation_count = 1
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    first_close = asyncio.create_task(watch.close())
    second_close = asyncio.create_task(watch.close())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancellations == 1:
            break
    gateway.recovery_gate.set()

    results = await asyncio.gather(
        recovering,
        first_close,
        second_close,
        return_exceptions=True,
    )
    assert [type(result) for result in results] == [RuntimeError] * 3
    assert [str(result) for result in results] == ["late close failed"] * 3
    late = gateway.subscriptions[1]
    assert isinstance(late, FailOnceCloseSubscription)
    assert late.closed is False
    assert late.close_calls == 1
    assert watch._recovery_owner is not None

    await watch.close()

    assert late.closed is True
    assert late.close_calls == 2
    assert watch._recovery_owner is None
    assert gateway.recovery_cancellations == 1


async def test_late_recovery_self_cancel_is_normalized_and_retryable() -> None:
    # Catches an SDK close self-cancellation being cached as caller cancellation.
    gateway = FakeGateway()
    gateway.subscription_factory = SelfCancellingCloseSubscription
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    gateway.recovery_gate = asyncio.Event()
    gateway.ignore_recovery_cancellation_count = 1
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(20):
        await asyncio.sleep(0)
        if len(gateway.subscriptions) == 2:
            break

    closing = asyncio.create_task(watch.close())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.recovery_cancellations == 1:
            break
    gateway.recovery_gate.set()

    handler_result, close_result = await asyncio.gather(
        recovering,
        closing,
        return_exceptions=True,
    )
    assert isinstance(handler_result, RuntimeError)
    assert isinstance(close_result, RuntimeError)
    assert not isinstance(handler_result, asyncio.CancelledError)
    assert not isinstance(close_result, asyncio.CancelledError)
    assert str(handler_result) == "SDK subscription cleanup cancelled internally"
    assert str(close_result) == "SDK subscription cleanup cancelled internally"
    owner = watch._recovery_owner
    assert owner is not None
    assert owner.cleanup_task is not None
    assert owner.cleanup_task.cancelled() is False
    assert not isinstance(owner.cleanup_task.result().error, asyncio.CancelledError)
    late = gateway.subscriptions[1]
    assert isinstance(late, SelfCancellingCloseSubscription)
    assert late.closed is False
    assert late.close_calls == 1

    await watch.close()

    assert late.closed is True
    assert late.close_calls == 2
    assert watch._recovery_owner is None
    assert gateway.recovery_cancellations == 1


async def test_active_subscription_close_failure_is_retained_and_retryable() -> None:
    # Catches a failed active-handle close losing the only retryable reference.
    gateway = FakeGateway()
    gateway.subscription_factory = ActiveFailOnceCloseSubscription
    watch, _, _, _, strategy, _ = _watch(gateway=gateway)
    await watch.start()
    active = gateway.subscriptions[0]
    assert isinstance(active, ActiveFailOnceCloseSubscription)
    baseline_calls = len(strategy.calls)

    with pytest.raises(RuntimeError, match="active close failed"):
        await watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    assert watch._subscription is active
    assert active.closed is False
    assert active.close_calls == 1
    assert gateway.requests == [("token-1", "token-2")]
    assert len(strategy.calls) == baseline_calls

    await watch.close()

    assert active.closed is True
    assert active.close_calls == 2
    assert watch._subscription is None


async def test_active_subscription_self_cancel_is_normalized_and_retryable() -> None:
    # Catches SDK self-cancellation becoming a cached caller cancellation or lost handle.
    gateway = FakeGateway()
    gateway.subscription_factory = ActiveSelfCancellingCloseSubscription
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    active = gateway.subscriptions[0]
    assert isinstance(active, ActiveSelfCancellingCloseSubscription)

    with pytest.raises(
        WatchCleanupError,
        match="SDK subscription cleanup cancelled internally",
    ):
        await watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    assert watch._subscription is active
    assert active.closed is False
    assert active.close_calls == 1

    await watch.close()

    assert active.closed is True
    assert active.close_calls == 2
    assert watch._subscription is None


async def test_concurrent_rotation_and_close_share_one_active_close_attempt() -> None:
    # Catches close retrying or forgetting an active handle while rotation still owns it.
    gateway = FakeGateway()
    gateway.subscription_factory = BlockingFailOnceCloseSubscription
    catalog = FakeCatalog(_catalog())
    watch, _, _, _, strategy, _ = _watch(gateway=gateway, catalog=catalog)
    await watch.start()
    active = gateway.subscriptions[0]
    assert isinstance(active, BlockingFailOnceCloseSubscription)
    baseline_calls = len(strategy.calls)
    catalog.snapshot = _catalog(second_market=True)
    rotating = asyncio.create_task(
        watch.handle_market_change(
            MarketChange(
                change_id="concurrent-active-close",
                change_type=MarketChangeType.MARKET_ADDED,
                event_id="event-1",
                market_id="market-2",
                token_ids=("token-3", "token-4"),
                occurred_at=200,
            )
        )
    )
    await active.close_started.wait()

    closing = asyncio.create_task(watch.close())
    await asyncio.sleep(0)
    active.release_close.set()
    results = await asyncio.gather(rotating, closing, return_exceptions=True)

    assert [type(result) for result in results] == [RuntimeError, RuntimeError]
    assert [str(result) for result in results] == [
        "active close failed",
        "active close failed",
    ]
    assert active.close_calls == 1
    assert active.closed is False
    assert watch._subscription is active
    assert gateway.requests == [("token-1", "token-2")]
    assert len(strategy.calls) == baseline_calls

    await watch.close()

    assert active.close_calls == 2
    assert active.closed is True
    assert watch._subscription is None


async def test_active_close_success_uses_identity_compare_and_clear() -> None:
    # Catches an old close completion clearing a newer installed subscription.
    gateway = FakeGateway()
    gateway.subscription_factory = BlockingCloseSubscription
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    await watch.start()
    active = gateway.subscriptions[0]
    assert isinstance(active, BlockingCloseSubscription)
    closing = asyncio.create_task(watch._close_current_subscription())
    await active.close_started.wait()
    retained_while_pending = watch._subscription is active
    replacement = FakeSubscription(99)
    watch._subscription = replacement
    active.release_close.set()

    await closing

    assert retained_while_pending is True
    assert active.closed is True
    assert watch._subscription is replacement
    await watch.close()
    assert replacement.closed is True


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


@pytest.mark.parametrize(
    "timestamp",
    [
        None,
        "malformed",
        "2026-08-01T00:00:00",
        "0001-01-01T00:00:00+14:00",
        "9999-12-31T23:59:59.999-14:00",
        -1,
        253_402_300_800_000,
    ],
)
async def test_invalid_exchange_timestamp_fails_closed_without_received_fallback(
    timestamp: object,
) -> None:
    # Catches unknown/stale exchange time being forged from local receipt time.
    watch, gateway, _, _, _, signals = _watch()
    await watch.start()

    await watch.handle_stream_message(
        MarketStreamEvent(
            event_type="price_change",
            market_id="market-1",
            payload={
                "timestamp": timestamp,
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
            received_timestamp=999_999,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    assert gateway.subscriptions[0].closed is True
    assert signals.closed[-1][1].reason_code is DecisionReason.ORDERBOOK_INVALID
    assert watch.cache.generation == 2
    book = watch.cache.get("token-1")
    assert book is not None
    assert book.exchange_timestamp == 100
    assert book.exchange_timestamp != 999_999
    await watch.close()


def test_nonfinite_exchange_timestamp_is_rejected_by_parser() -> None:
    # Catches non-finite numeric timestamps reaching freshness calculations.
    with pytest.raises(ValueError, match="exchange timestamp"):
        _timestamp_ms(float("inf"))


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
