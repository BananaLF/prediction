from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from decimal import Decimal
import logging
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.config import StrategyConfig
from predmarket.domain.fees import FeeModel, FeeSchedule
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import (
    DecisionReason,
    NotEvaluable,
    StrategyContext,
    StrategyType,
)
from predmarket.persistence.repositories import CatalogSnapshot
from predmarket.polymarket.gateway import (
    MarketRecoveryInvalidatedError,
    MarketRecoveryTransientError,
    MarketStreamEvent,
    MarketStreamInvalidated,
)
from predmarket.signals.manager import SubscriptionGenerationChanged
from predmarket.polymarket.gateway import MarketSnapshot
from predmarket.watch.cache import CacheState
from predmarket.watch.task import (
    EvaluationTarget,
    WatchCleanupError,
    WatchTask,
    _format_decimal_for_log,
    _watchable_subscription,
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
    inferred_market_id = {
        "token-3": "market-2",
        "token-4": "market-2",
        "token-5": "market-3",
        "token-6": "market-3",
    }.get(token_id, "market-1")
    return OrderBook(
        market_id=market_id or inferred_market_id,
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
        self.reader_tasks: list[asyncio.Task[Any]] = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        reader_task = asyncio.current_task()
        assert reader_task is not None
        if reader_task not in self.reader_tasks:
            self.reader_tasks.append(reader_task)
        item = await self.items.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self) -> None:
        self.closed = True


class CloseOnReadCancellationSubscription(FakeSubscription):
    """Mirror the production subscription's close-on-cancel read contract."""

    async def __anext__(self):
        if self.closed:
            raise StopAsyncIteration
        try:
            return await super().__anext__()
        except asyncio.CancelledError:
            await self.close()
            raise


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
        self.hydrated_market_ids: list[tuple[str, ...]] = []

    def hydrate_market_identities(
        self,
        markets: tuple[Market, ...],
        tokens: tuple[Token, ...],
        market_ids: tuple[str, ...],
    ) -> None:
        self.hydrated_market_ids.append(tuple(market_ids))

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


class TransientRecoveryGateway(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.recovery_calls = 0

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.recovery_calls += 1
        if self.recovery_calls == 1:
            raise MarketRecoveryInvalidatedError("connection_lost")
        return await super().recover_market_session(token_ids)


class TransientRequestRecoveryGateway(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.recovery_calls = 0

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.recovery_calls += 1
        if self.recovery_calls == 1:
            raise MarketRecoveryTransientError(
                "request_rejected",
                status=502,
                retry_after=0.001,
            )
        return await super().recover_market_session(token_ids)


class InvalidSnapshotOnceGateway(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.recovery_calls = 0

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.recovery_calls += 1
        if self.recovery_calls != 1:
            return await super().recover_market_session(token_ids)
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        self.requests.append(normalized)
        subscription = self.subscription_factory(generation)
        self.subscriptions.append(subscription)
        books = tuple(_book(token_id, generation) for token_id in normalized)
        books = (
            replace(
                books[0],
                bids=(OrderBookLevel(Decimal("0.60"), Decimal("3")),),
                asks=(OrderBookLevel(Decimal("0.50"), Decimal("4")),),
            ),
            *books[1:],
        )
        return SimpleNamespace(
            order_books=books,
            subscription=subscription,
            subscription_generation=generation,
        )


class NonRetryableRecoveryGateway(FakeGateway):
    async def recover_market_session(self, token_ids: tuple[str, ...]):
        raise MarketRecoveryInvalidatedError("sdk_version_changed")


class AlwaysInvalidatedRecoveryGateway(FakeGateway):
    def __init__(self) -> None:
        super().__init__()
        self.recovery_called = asyncio.Event()

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.recovery_called.set()
        raise MarketRecoveryInvalidatedError("connection_lost")


class PruningRecoveryGateway(FakeGateway):
    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        effective = tuple(
            token_id for token_id in normalized if token_id in {"token-1", "token-2"}
        )
        self.requests.append(normalized)
        subscription = self.subscription_factory(generation)
        self.subscriptions.append(subscription)
        return SimpleNamespace(
            token_ids=effective,
            order_books=tuple(_book(token_id, generation) for token_id in effective),
            subscription=subscription,
            subscription_generation=generation,
        )


class PrunesOneMarketOnceGateway(FakeGateway):
    async def recover_market_session(self, token_ids: tuple[str, ...]):
        if self.requests:
            return await super().recover_market_session(token_ids)
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        effective = tuple(
            token_id for token_id in normalized if token_id not in {"token-3", "token-4"}
        )
        self.requests.append(normalized)
        subscription = self.subscription_factory(generation)
        self.subscriptions.append(subscription)
        return SimpleNamespace(
            token_ids=effective,
            order_books=tuple(_book(token_id, generation) for token_id in effective),
            subscription=subscription,
            subscription_generation=generation,
        )


class PrunesAllMarketsGateway(FakeGateway):
    async def recover_market_session(self, token_ids: tuple[str, ...]):
        if not token_ids:
            raise AssertionError("empty recovery scope must not reach the gateway")
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        self.requests.append(normalized)
        subscription = self.subscription_factory(generation)
        self.subscriptions.append(subscription)
        return SimpleNamespace(
            token_ids=(),
            order_books=(),
            subscription=subscription,
            subscription_generation=generation,
        )


class FakeCatalog:
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        self.snapshot = snapshot
        self.load_calls = 0

    async def load_catalog(self) -> CatalogSnapshot:
        self.load_calls += 1
        return self.snapshot


class StartupRefreshingGateway(FakeGateway):
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        super().__init__()
        self._snapshot = snapshot
        self.refreshed_market_ids: list[str] = []

    async def refresh_market(self, market_id: str) -> MarketSnapshot:
        self.refreshed_market_ids.append(market_id)
        market = next(item for item in self._snapshot.markets if item.id == market_id)
        fee_schedule = FeeSchedule(
            model=FeeModel.ZERO,
            enabled=False,
            source="startup-refresh-test",
            parameters={},
            updated_at=200,
        )
        return MarketSnapshot(
            market=replace(
                market,
                sync_generation="sync-refresh",
                updated_at=200,
            ),
            tokens=tuple(
                replace(
                    token,
                    sync_generation="sync-refresh",
                    fee_schedule=fee_schedule,
                    fee_updated_at=200,
                    updated_at=200,
                )
                for token in self._snapshot.tokens
                if token.market_id == market_id
            ),
            mapping_version="startup-refresh-test",
        )


class BlockingRecoveryRefillGateway(StartupRefreshingGateway):
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        super().__init__(snapshot)
        self.refill_refresh_started = asyncio.Event()
        self.release_refill_refresh = asyncio.Event()

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        if self.requests:
            return await super().recover_market_session(token_ids)
        self.generations += 1
        generation = self.generations
        normalized = tuple(token_ids)
        effective = tuple(
            token_id
            for token_id in normalized
            if token_id not in {"token-3", "token-4"}
        )
        self.requests.append(normalized)
        subscription = self.subscription_factory(generation)
        self.subscriptions.append(subscription)
        return SimpleNamespace(
            token_ids=effective,
            order_books=tuple(_book(token_id, generation) for token_id in effective),
            subscription=subscription,
            subscription_generation=generation,
        )

    async def refresh_market(self, market_id: str) -> MarketSnapshot:
        if market_id == "market-3":
            self.refill_refresh_started.set()
            await self.release_refill_refresh.wait()
        return await super().refresh_market(market_id)


class FailingStartupRefreshingGateway(FakeGateway):
    async def refresh_market(self, market_id: str) -> MarketSnapshot:
        raise RuntimeError("upstream response " + "x" * 2_000)


class StartupPersistingCatalog(FakeCatalog):
    def __init__(self, snapshot: CatalogSnapshot) -> None:
        super().__init__(snapshot)
        self.saved_market_ids: list[tuple[str, ...]] = []

    async def save_catalog(
        self,
        *,
        events: tuple[Event, ...],
        markets: tuple[Market, ...],
        tokens: tuple[Token, ...],
    ) -> None:
        assert events == ()
        self.saved_market_ids.append(tuple(market.id for market in markets))
        market_by_id = {market.id: market for market in self.snapshot.markets}
        market_by_id.update((market.id, market) for market in markets)
        token_by_id = {token.id: token for token in self.snapshot.tokens}
        token_by_id.update((token.id, token) for token in tokens)
        self.snapshot = CatalogSnapshot(
            events=self.snapshot.events,
            markets=tuple(market_by_id.values()),
            tokens=tuple(token_by_id.values()),
        )


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


class TrackingChanges(FakeChanges):
    def __init__(self) -> None:
        super().__init__()
        self.get_calls = 0
        self.get_cancellations = 0

    async def get(self) -> MarketChange:
        self.get_calls += 1
        try:
            return await super().get()
        except asyncio.CancelledError:
            self.get_cancellations += 1
            raise


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


class SharedOpportunityContextSource(FakeContextSource):
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
                opportunity_key="opportunity:market-1",
                expected_revision=None,
            ),
        )


class BatchContextSource(FakeContextSource):
    def __init__(self) -> None:
        self.batch_calls: list[tuple[str, ...]] = []

    def contexts_for(
        self,
        changed_token_id: str,
        orderbooks: tuple[OrderBook, ...],
    ) -> tuple[EvaluationTarget, ...]:
        raise AssertionError("per-token context path should not be used")

    def contexts_for_batch(
        self,
        changed_token_ids: tuple[str, ...],
        orderbooks: tuple[OrderBook, ...],
    ) -> dict[str, tuple[EvaluationTarget, ...]]:
        self.batch_calls.append(changed_token_ids)
        return {
            token_id: FakeContextSource.contexts_for(self, token_id, orderbooks)
            for token_id in changed_token_ids
        }


class CatalogAwareContextSource(FakeContextSource):
    def __init__(self) -> None:
        self.snapshots: list[CatalogSnapshot] = []

    def use_catalog_snapshot(self, snapshot: CatalogSnapshot) -> None:
        self.snapshots.append(snapshot)


class FakeStrategy:
    def __init__(self) -> None:
        self.calls: list[Any] = []

    def evaluate(self, context: Any) -> NotEvaluable:
        self.calls.append(context)
        return NotEvaluable(
            DecisionReason.INPUT_METADATA_MISSING,
            {"changed_token_id": context.changed_token_id},
        )


class CausalityDiagnosticStrategy(FakeStrategy):
    def evaluate(self, context: Any) -> NotEvaluable:
        self.calls.append(context)
        skew_ms = 150 if context.changed_token_id == "token-1" else 240
        return NotEvaluable(
            DecisionReason.ORDERBOOK_INVALID,
            {
                "changed_token_id": context.changed_token_id,
                "detail": "orderbook_timestamp_causality_invalid",
                "token_id": context.changed_token_id,
                "exchange_timestamp": 1_000 + skew_ms,
                "received_timestamp": 1_000,
                "exchange_clock_skew_ms": skew_ms,
                "maximum_exchange_clock_skew_ms": 100,
            },
        )


class AdvancingClockStrategy(FakeStrategy):
    def __init__(self, clock: list[int]) -> None:
        super().__init__()
        self._clock = clock
        self.advance_to: int | None = None

    def evaluate(self, context: Any) -> NotEvaluable:
        self.calls.append(context)
        if self.advance_to is not None:
            self._clock[0] = self.advance_to
        return NotEvaluable(
            DecisionReason.INPUT_METADATA_MISSING,
            {"changed_token_id": context.changed_token_id},
        )


class BlockingStreamStrategy(FakeStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.stream_evaluation_started = asyncio.Event()
        self.release_stream_evaluation = asyncio.Event()

    async def evaluate(self, context: Any) -> NotEvaluable:
        self.calls.append(context)
        if len(self.calls) > 2:
            self.stream_evaluation_started.set()
            await self.release_stream_evaluation.wait()
        return NotEvaluable(
            DecisionReason.INPUT_METADATA_MISSING,
            {"changed_token_id": context.changed_token_id},
        )


class BlockingSyncStreamStrategy(FakeStrategy):
    def __init__(self) -> None:
        super().__init__()
        self.stream_evaluation_started = threading.Event()
        self.release_stream_evaluation = threading.Event()
        self.stream_evaluation_finished = threading.Event()

    def evaluate(self, context: Any) -> NotEvaluable:
        self.calls.append(context)
        if len(self.calls) > 2:
            self.stream_evaluation_started.set()
            self.release_stream_evaluation.wait(timeout=1)
            self.stream_evaluation_finished.set()
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


class NoPersistSignals(FakeSignals):
    async def apply(
        self,
        decision: Any,
        opportunity_key: str,
        expected_revision: int | None,
    ) -> None:
        self.applied.append((decision, opportunity_key, expected_revision))
        return None


class GenerationChangingSignals(FakeSignals):
    def __init__(self) -> None:
        super().__init__()
        self.reject_apply = False

    async def apply(
        self,
        decision: Any,
        opportunity_key: str,
        expected_revision: int | None,
    ) -> str:
        if self.reject_apply:
            raise SubscriptionGenerationChanged(
                "subscription generation is unavailable for 'token-1'"
            )
        return await super().apply(decision, opportunity_key, expected_revision)


def _watch(
    *,
    gateway: FakeGateway | None = None,
    catalog: FakeCatalog | None = None,
    changes: FakeChanges | None = None,
    strategy: FakeStrategy | None = None,
    signals: FakeSignals | None = None,
    context_source: FakeContextSource | None = None,
    clock_ms: Any | None = None,
    market_limit: int = 100,
    minimum_end_horizon_seconds: int = 1_800,
) -> tuple[WatchTask, FakeGateway, FakeCatalog, FakeChanges, FakeStrategy, FakeSignals]:
    gateway = gateway or FakeGateway()
    catalog = catalog or FakeCatalog(_catalog())
    changes = changes or FakeChanges()
    strategy = strategy or FakeStrategy()
    signals = signals or FakeSignals()
    context_source = context_source or FakeContextSource()
    return (
        WatchTask(
            gateway=gateway,
            catalog=catalog,
            changes=changes,
            strategy_engine=strategy,
            signal_manager=signals,
            context_source=context_source,
            clock_ms=clock_ms,
            market_limit=market_limit,
            minimum_end_horizon_seconds=minimum_end_horizon_seconds,
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


def test_decimal_diagnostics_are_bounded_to_eight_fractional_digits() -> None:
    value = Decimal(
        "-0.0156850585913468873500765304866945227795085315497546595008538932"
    )

    assert _format_decimal_for_log(value) == "-0.01568506"
    assert _format_decimal_for_log(None) == "none"


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
    assert gateway.hydrated_market_ids == [("market-1",)]
    assert strategy.calls == []
    assert watch.cache.state is CacheState.INVALID
    gateway.recovery_gate.set()
    try:
        async with asyncio.timeout(1):
            while {
                call.changed_token_id for call in strategy.calls
            } != {"token-1", "token-2"}:
                await asyncio.sleep(0.001)

        assert watch.cache.state is CacheState.VALID
    finally:
        await _cancel(task)
    assert gateway.subscriptions[0].closed is True


async def test_evaluation_batch_deduplicates_shared_opportunity_keys() -> None:
    # Both token updates for one market must evaluate the shared opportunity once.
    context_source = SharedOpportunityContextSource()
    watch, gateway, _, _, strategy, signals = _watch(context_source=context_source)

    await watch.start()

    assert gateway.requests == [("token-1", "token-2")]
    assert len(strategy.calls) == 1
    assert len(signals.applied) == 1
    assert signals.applied[0][1] == "opportunity:market-1"
    await watch.close()


async def test_evaluation_summary_logs_stage_timings(
    caplog: pytest.LogCaptureFixture,
) -> None:
    watch, _, _, _, _, _ = _watch()

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    summary = next(
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("watch_evaluation_summary ")
    )
    assert "context_ms=" in summary
    assert "strategy_ms=" in summary
    assert "signal_apply_ms=" in summary
    assert "elapsed_ms=" in summary
    await watch.close()


async def test_evaluation_summary_logs_largest_exchange_clock_skew(
    caplog: pytest.LogCaptureFixture,
) -> None:
    watch, _, _, _, _, _ = _watch(strategy=CausalityDiagnosticStrategy())

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    summary = next(
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("watch_evaluation_summary ")
    )
    assert "exchange_clock_skew_ms=240" in summary
    assert "exchange_clock_skew_limit_ms=100" in summary
    assert "exchange_clock_skew_token_id=token-2" in summary
    assert "exchange_timestamp=1240" in summary
    assert "received_timestamp=1000" in summary
    await watch.close()


async def test_evaluation_summary_rate_limits_ordinary_live_batches(
    caplog: pytest.LogCaptureFixture,
) -> None:
    signals = NoPersistSignals()
    watch, _, _, _, _, _ = _watch(signals=signals)

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()
        await watch._evaluate_tokens(("token-1",))  # noqa: SLF001
        await watch._evaluate_tokens(("token-2",))  # noqa: SLF001

    summaries = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("watch_evaluation_summary ")
    ]
    assert len(summaries) == 1
    await watch.close()


async def test_evaluation_batch_completion_rate_limits_ordinary_batches(
    caplog: pytest.LogCaptureFixture,
) -> None:
    signals = NoPersistSignals()
    watch, _, _, _, _, _ = _watch(signals=signals)
    await watch.start()
    worker = asyncio.create_task(watch._run_deferred_evaluations())  # noqa: SLF001

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        for expected_batch in (1, 2):
            watch._queue_evaluation(("token-1",))  # noqa: SLF001
            for _ in range(20):
                await asyncio.sleep(0)
                if watch._evaluation_batch_count >= expected_batch:  # noqa: SLF001
                    break

    completions = [
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("watch_evaluation_batch_completed ")
    ]
    assert len(completions) == 1
    await _cancel(worker)
    await watch.close()


async def test_evaluation_prefers_batch_context_materialization() -> None:
    context_source = BatchContextSource()
    watch, _, _, _, _, _ = _watch(context_source=context_source)

    await watch.start()

    assert context_source.batch_calls == [("token-1", "token-2")]
    await watch.close()


async def test_evaluation_rejects_book_that_becomes_stale_while_strategy_runs() -> None:
    clock = [100]
    configuration = StrategyConfig(
        bankroll=Decimal("1000"),
        minimum_return_rate=Decimal("0.0075"),
        maximum_risk_rate=Decimal("1"),
        maximum_unhedged_notional=Decimal("1000"),
        safety_buffer_rate=Decimal("0"),
        conversion_cost=Decimal("0"),
        maximum_book_age_ms=1_000,
        maximum_exchange_clock_skew_ms=100,
        maximum_leg_skew_ms=250,
    )

    class ContextSource(FakeContextSource):
        def contexts_for(
            self,
            changed_token_id: str,
            orderbooks: tuple[OrderBook, ...],
        ) -> tuple[EvaluationTarget, ...]:
            return (
                EvaluationTarget(
                    context=StrategyContext(
                        strategy_type=StrategyType.BINARY_UNDERPRICED,
                        changed_token_id=changed_token_id,
                        markets=(_market("market-1"),),
                        tokens=(
                            _token("token-1", "market-1", 0),
                            _token("token-2", "market-1", 1),
                        ),
                        approved_implication_relation=None,
                        orderbooks=orderbooks,
                        fee_schedules={},
                        evaluated_at=1,
                        configuration=configuration,
                    ),
                    opportunity_key=f"opportunity:{changed_token_id}",
                    expected_revision=None,
                ),
            )

    strategy = AdvancingClockStrategy(clock)
    signals = FakeSignals()
    watch, _, _, _, _, _ = _watch(
        strategy=strategy,
        signals=signals,
        context_source=ContextSource(),
        clock_ms=lambda: clock[0],
    )
    await watch.start()
    strategy.calls.clear()
    signals.applied.clear()
    clock[0] = 100
    watch._last_orderbook_observed_at_ms = 100  # noqa: SLF001
    strategy.advance_to = 1_101

    await watch._evaluate_tokens(("token-1",))  # noqa: SLF001

    assert strategy.calls[0].evaluated_at == 100
    assert signals.applied[0][0].reason_code is DecisionReason.ORDERBOOK_STALE
    await watch.close()


async def test_run_continues_consuming_stream_while_strategy_evaluation_is_slow() -> None:
    strategy = BlockingStreamStrategy()
    watch, gateway, _, _, _, _ = _watch(strategy=strategy)
    task = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.subscriptions:
            break

    subscription = gateway.subscriptions[0]
    for timestamp, size, book_hash in (
        (110, "9", "stream-hash-1"),
        (111, "8", "stream-hash-2"),
    ):
        await subscription.items.put(
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
                            "size": size,
                            "hash": book_hash,
                        }
                    ],
                },
                received_timestamp=timestamp + 1,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    await asyncio.wait_for(strategy.stream_evaluation_started.wait(), timeout=1)
    for _ in range(20):
        book = watch.cache.get("token-1")
        if book is not None and book.book_hash == "stream-hash-2":
            break
        await asyncio.sleep(0)

    book = watch.cache.get("token-1")
    assert book is not None
    assert book.book_hash == "stream-hash-2"
    strategy.release_stream_evaluation.set()
    await _cancel(task)


async def test_stream_messages_reuse_pending_change_reader() -> None:
    # A busy stream must not recreate and cancel the catalog reader per message.
    changes = TrackingChanges()
    watch, gateway, _, _, _, _ = _watch(changes=changes)
    task = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.subscriptions and changes.get_calls:
            break

    subscription = gateway.subscriptions[0]
    for timestamp, size in ((110, "9"), (111, "8")):
        await subscription.items.put(
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
                            "size": size,
                            "hash": f"stream-hash-{timestamp}",
                        }
                    ],
                },
                received_timestamp=timestamp + 1,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    for _ in range(40):
        await asyncio.sleep(0)
        book = watch.cache.get("token-1")
        if book is not None and book.book_hash == "stream-hash-111":
            break

    assert changes.get_calls == 1
    assert changes.get_cancellations == 0
    await _cancel(task)


async def test_stream_messages_reuse_one_stream_reader_task() -> None:
    # A busy stream must not create an outer asyncio task for every message.
    watch, gateway, _, _, _, _ = _watch()
    task = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.subscriptions:
            break

    subscription = gateway.subscriptions[0]
    for timestamp in (110, 111):
        await subscription.items.put(
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
                            "hash": f"stream-hash-{timestamp}",
                        }
                    ],
                },
                received_timestamp=timestamp + 1,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    for _ in range(40):
        await asyncio.sleep(0)
        book = watch.cache.get("token-1")
        if book is not None and book.book_hash == "stream-hash-111":
            break

    assert len(subscription.reader_tasks) == 1
    await _cancel(task)


async def test_deferred_evaluation_does_not_apply_after_subscription_generation_changes() -> None:
    strategy = BlockingStreamStrategy()
    signals = FakeSignals()
    watch, gateway, _, _, _, _ = _watch(strategy=strategy, signals=signals)
    task = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if gateway.subscriptions:
            break

    await gateway.subscriptions[0].items.put(
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
                        "hash": "stream-hash",
                    }
                ],
            },
            received_timestamp=111,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )
    await asyncio.wait_for(strategy.stream_evaluation_started.wait(), timeout=1)
    applied_before_generation_change = len(signals.applied)

    watch.cache.invalidate(generation=1, reason="test_generation_change")
    watch.cache.begin_resync(generation=2, token_ids=("token-1", "token-2"))
    watch.cache.apply_snapshot((_book("token-1", 2), _book("token-2", 2)))
    strategy.release_stream_evaluation.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert len(signals.applied) == applied_before_generation_change
    await _cancel(task)


async def test_generation_change_during_signal_apply_aborts_evaluation_without_crashing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    signals = GenerationChangingSignals()
    watch, _, _, _, _, _ = _watch(signals=signals)
    await watch.start()
    signals.reject_apply = True

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch._evaluate_tokens(("token-1",))  # noqa: SLF001

    assert "watch_evaluation_aborted" in caplog.text
    assert "stage=signal_apply_generation_changed" in caplog.text
    await watch.close()


async def test_sync_strategy_evaluation_does_not_block_stream_event_loop() -> None:
    strategy = BlockingSyncStreamStrategy()
    watch, _, _, _, _, _ = _watch(strategy=strategy)
    await watch.start()
    evaluation: asyncio.Task[Any] | None = None
    try:
        evaluation = asyncio.create_task(
            watch.handle_stream_message(
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
        )
        for _ in range(20):
            await asyncio.sleep(0)
            if strategy.stream_evaluation_started.is_set():
                break

        assert strategy.stream_evaluation_started.is_set() is True
        assert strategy.stream_evaluation_finished.is_set() is False
        strategy.release_stream_evaluation.set()
        await asyncio.wait_for(evaluation, timeout=1)
    finally:
        strategy.release_stream_evaluation.set()
        if evaluation is not None and not evaluation.done():
            await asyncio.gather(evaluation, return_exceptions=True)
        await watch.close()


async def test_start_prepares_context_source_with_loaded_catalog_snapshot() -> None:
    context_source = CatalogAwareContextSource()
    catalog = FakeCatalog(_catalog())
    watch, _, _, _, _, _ = _watch(
        catalog=catalog,
        context_source=context_source,
    )

    await watch.start()

    assert context_source.snapshots == [catalog.snapshot]
    await watch.close()


async def test_start_refreshes_selected_market_metadata_before_context_and_recovery() -> None:
    original = _catalog()
    gateway = StartupRefreshingGateway(original)
    catalog = StartupPersistingCatalog(original)
    context_source = CatalogAwareContextSource()
    watch, _, _, _, _, _ = _watch(
        gateway=gateway,
        catalog=catalog,
        context_source=context_source,
    )

    await watch.start()

    assert gateway.refreshed_market_ids == ["market-1"]
    assert catalog.saved_market_ids == [("market-1",)]
    assert context_source.snapshots == [catalog.snapshot]
    assert all(
        token.fee_updated_at == 200
        for token in context_source.snapshots[0].tokens
    )
    assert gateway.requests == [("token-1", "token-2")]
    await watch.close()


async def test_start_bounds_partial_refresh_failure_samples(caplog) -> None:
    catalog = StartupPersistingCatalog(_catalog())
    watch, _, _, _, _, _ = _watch(
        gateway=FailingStartupRefreshingGateway(),
        catalog=catalog,
    )

    with caplog.at_level(logging.WARNING, logger="predmarket.watch.task"):
        await watch.start()

    message = next(
        record.getMessage()
        for record in caplog.records
        if record.getMessage().startswith("watch_catalog_refresh_partial_failure")
    )
    assert "market-1:RuntimeError:upstream response" in message
    assert "x" * 129 not in message
    assert len(message) < 500
    await watch.close()


async def test_run_refreshes_active_market_metadata_periodically() -> None:
    original = _catalog()
    gateway = StartupRefreshingGateway(original)
    catalog = StartupPersistingCatalog(original)
    context_source = CatalogAwareContextSource()
    watch = WatchTask(
        gateway=gateway,
        catalog=catalog,
        changes=FakeChanges(),
        strategy_engine=FakeStrategy(),
        signal_manager=FakeSignals(),
        context_source=context_source,
        market_metadata_refresh_interval_seconds=1,
    )

    task = asyncio.create_task(watch.run())
    try:
        async with asyncio.timeout(2):
            while len(gateway.refreshed_market_ids) < 2:
                await asyncio.sleep(0.01)

        assert gateway.refreshed_market_ids == ["market-1", "market-1"]
        assert catalog.saved_market_ids == [("market-1",), ("market-1",)]
        assert context_source.snapshots[-1] == catalog.snapshot
        assert catalog.load_calls == 1
    finally:
        await watch.close()
        await task


async def test_market_update_refreshes_context_catalog_before_scope_shortcut() -> None:
    context_source = CatalogAwareContextSource()
    catalog = FakeCatalog(_catalog())
    watch, _, _, _, _, _ = _watch(
        catalog=catalog,
        context_source=context_source,
    )
    await watch.start()
    original = catalog.snapshot
    updated_market = replace(original.markets[0], question="Updated question")
    catalog.snapshot = CatalogSnapshot(
        events=original.events,
        markets=(updated_market,),
        tokens=original.tokens,
    )

    await watch.handle_market_change(
        MarketChange(
            change_id="change-context-refresh",
            change_type=MarketChangeType.MARKET_UPDATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=200,
        )
    )

    assert context_source.snapshots == [original, catalog.snapshot]
    await watch.close()


async def test_same_sync_generation_prepares_catalog_once_and_keeps_controls() -> None:
    context_source = CatalogAwareContextSource()
    catalog = FakeCatalog(_catalog())
    signals = FakeSignals()
    watch, _, _, _, _, _ = _watch(
        catalog=catalog,
        context_source=context_source,
        signals=signals,
    )
    await watch.start()
    baseline_load_calls = catalog.load_calls
    baseline_closures = len(signals.closed)

    await watch.handle_market_change(
        MarketChange(
            change_id="sync-generation:MARKET_UPDATED:market-1",
            change_type=MarketChangeType.MARKET_UPDATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=200,
        )
    )
    await watch.handle_market_change(
        MarketChange(
            change_id="sync-generation:MARKET_DEACTIVATED:market-2",
            change_type=MarketChangeType.MARKET_DEACTIVATED,
            event_id="event-1",
            market_id="market-2",
            token_ids=("token-3", "token-4"),
            occurred_at=200,
            critical=True,
        )
    )
    await watch.handle_market_change(
        MarketChange(
            change_id="sync-generation:MARKET_DEACTIVATED:market-1",
            change_type=MarketChangeType.MARKET_DEACTIVATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=200,
            critical=True,
        )
    )

    assert catalog.load_calls == baseline_load_calls + 1
    assert context_source.snapshots == [catalog.snapshot, catalog.snapshot]
    assert signals.closed[baseline_closures][0] == ("token-3", "token-4")
    assert len(signals.closed) == baseline_closures + 1
    await watch.close()


async def test_start_adopts_gateway_pruned_recovery_scope() -> None:
    gateway = PruningRecoveryGateway()
    signals = FakeSignals()
    watch, _, _, _, strategy, _ = _watch(
        gateway=gateway,
        catalog=FakeCatalog(_catalog(second_market=True)),
        signals=signals,
    )

    await watch.start()

    assert watch.active_token_ids == ("token-1", "token-2")
    assert watch.cache.state is CacheState.VALID
    assert tuple(book.token_id for book in watch.cache.view()) == (
        "token-1",
        "token-2",
    )
    assert {call.changed_token_id for call in strategy.calls} == {
        "token-1",
        "token-2",
    }
    assert signals.closed[0][0] == ("token-3", "token-4")
    assert signals.closed[0][1].reason_code is DecisionReason.ORDERBOOK_INVALID
    await watch.close()


async def test_start_refills_market_removed_from_recovery_scope(caplog) -> None:
    markets = tuple(_market(f"market-{index}") for index in range(1, 4))
    tokens = tuple(
        _token(f"token-{(index - 1) * 2 + position + 1}", market.id, position)
        for index, market in enumerate(markets, start=1)
        for position in (0, 1)
    )
    snapshot = CatalogSnapshot(
        events=(_event(tuple(market.id for market in markets)),),
        markets=markets,
        tokens=tokens,
    )
    gateway = PrunesOneMarketOnceGateway()
    catalog = FakeCatalog(snapshot)
    watch, _, _, _, _, _ = _watch(
        gateway=gateway,
        catalog=catalog,
        market_limit=2,
    )

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    assert gateway.requests == [
        ("token-1", "token-2", "token-3", "token-4"),
        ("token-1", "token-2", "token-5", "token-6"),
    ]
    assert gateway.subscriptions[0].closed is True
    assert watch.active_token_ids == (
        "token-1",
        "token-2",
        "token-5",
        "token-6",
    )
    assert tuple(book.token_id for book in watch.cache.view()) == watch.active_token_ids
    assert catalog.load_calls == 1
    assert any(
        record.getMessage().startswith("watch_recovery_refill_prepared")
        for record in caplog.records
    )
    await watch.close()


async def test_recovery_refill_does_not_overwrite_newer_catalog_context() -> None:
    markets = tuple(_market(f"market-{index}") for index in range(1, 4))
    tokens = tuple(
        _token(f"token-{(index - 1) * 2 + position + 1}", market.id, position)
        for index, market in enumerate(markets, start=1)
        for position in (0, 1)
    )
    snapshot = CatalogSnapshot(
        events=(_event(tuple(market.id for market in markets)),),
        markets=markets,
        tokens=tokens,
    )
    gateway = BlockingRecoveryRefillGateway(snapshot)
    catalog = StartupPersistingCatalog(snapshot)
    context_source = CatalogAwareContextSource()
    watch, _, _, _, _, _ = _watch(
        gateway=gateway,
        catalog=catalog,
        context_source=context_source,
        market_limit=2,
    )
    start_task = asyncio.create_task(watch.start())
    try:
        await asyncio.wait_for(gateway.refill_refresh_started.wait(), timeout=1)
        current = context_source.snapshots[-1]
        newer = replace(
            current,
            events=(replace(current.events[0], title="Newer catalog"),),
        )
        await watch._prepare_context_catalog(newer)
        gateway.release_refill_refresh.set()
        await asyncio.wait_for(start_task, timeout=1)

        assert context_source.snapshots[-1].events[0].title == "Newer catalog"
    finally:
        gateway.release_refill_refresh.set()
        await asyncio.gather(start_task, return_exceptions=True)
        await watch.close()


async def test_start_stops_recovery_when_pruning_leaves_no_watchable_market(
    caplog,
) -> None:
    gateway = PrunesAllMarketsGateway()
    watch, _, _, _, _, _ = _watch(
        gateway=gateway,
        catalog=FakeCatalog(_catalog()),
    )

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    assert gateway.requests == [("token-1", "token-2")]
    assert gateway.subscriptions[0].closed is True
    assert watch.active_token_ids == ()
    assert any(
        record.getMessage().startswith("watch_recovery_refill_unavailable")
        for record in caplog.records
    )
    await watch.close()


def test_watchable_subscription_selects_complete_unexpired_markets_with_limit() -> None:
    future_early = replace(_market("market-b"), end_at=2_000)
    future_late = replace(_market("market-a"), end_at=3_000)
    expired = replace(_market("market-expired"), end_at=999)
    incomplete = replace(
        _market("market-incomplete"),
        sync_generation_complete=False,
    )
    markets = (future_late, expired, incomplete, future_early)
    tokens = tuple(
        _token(f"token-{market.id}-{position}", market.id, position)
        for market in markets
        for position in (0, 1)
    )
    snapshot = CatalogSnapshot(events=(), markets=markets, tokens=tokens)

    token_ids, market_ids = _watchable_subscription(
        snapshot,
        now_ms=1_000,
        market_limit=1,
    )

    assert market_ids == ("market-b",)
    assert token_ids == ("token-market-b-0", "token-market-b-1")


def test_watchable_subscription_excludes_markets_inside_minimum_end_horizon() -> None:
    near = replace(_market("market-near"), end_at=1_801_000)
    far = replace(_market("market-far"), end_at=1_801_001)
    tokens = tuple(
        _token(f"token-{market.id}-{position}", market.id, position)
        for market in (near, far)
        for position in (0, 1)
    )

    token_ids, market_ids = _watchable_subscription(
        CatalogSnapshot(events=(), markets=(near, far), tokens=tokens),
        now_ms=1_000,
        market_limit=2,
        minimum_end_horizon_ms=1_800_000,
    )

    assert market_ids == ("market-far",)
    assert token_ids == ("token-market-far-0", "token-market-far-1")


def test_watchable_subscription_rejects_market_with_incomplete_token_generation() -> None:
    market = _market("market-1")
    tokens = (
        _token("token-1", market.id, 0),
        replace(_token("token-2", market.id, 1), sync_generation_complete=False),
    )

    assert _watchable_subscription(
        CatalogSnapshot(events=(), markets=(market,), tokens=tokens),
        now_ms=1_000,
        market_limit=100,
    ) == ((), ())


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
    assert gateway.hydrated_market_ids == []

    running = asyncio.create_task(watch.run())
    await asyncio.sleep(0)
    await watch.close()
    await asyncio.wait_for(asyncio.shield(running), timeout=0.1)
    assert running.done() is True


async def test_unchanged_market_update_keeps_pending_subscription_open() -> None:
    gateway = FakeGateway()
    gateway.subscription_factory = CloseOnReadCancellationSubscription
    changes = FakeChanges()
    watch, _, _, _, _, _ = _watch(gateway=gateway, changes=changes)
    running = asyncio.create_task(watch.run())
    try:
        async with asyncio.timeout(1):
            while not gateway.subscriptions or not gateway.subscriptions[0].reader_tasks:
                await asyncio.sleep(0)
        first = gateway.subscriptions[0]

        await changes.items.put(
            MarketChange(
                change_id="unchanged-market-update",
                change_type=MarketChangeType.MARKET_UPDATED,
                event_id="event-1",
                market_id="market-1",
                token_ids=("token-1", "token-2"),
                occurred_at=200,
            )
        )
        await asyncio.wait_for(changes.join(), timeout=1)
        await asyncio.sleep(0)

        assert first.closed is False
        assert gateway.requests == [("token-1", "token-2")]
    finally:
        await watch.close()
        await running


async def test_acquired_change_is_acknowledged_once_during_reader_cleanup() -> None:
    # Catches terminal reader cleanup acknowledging an already handled change twice.
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
    await asyncio.wait_for(changes.join(), timeout=1)

    running.cancel()
    await subscription.read_cancelled.wait()
    running.cancel()
    await asyncio.sleep(0)
    assert changes.done == 1

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


async def test_market_add_refreshes_newly_selected_market_before_recovery() -> None:
    original = _catalog()
    expanded = _catalog(second_market=True)
    gateway = StartupRefreshingGateway(expanded)
    catalog = StartupPersistingCatalog(original)
    context_source = CatalogAwareContextSource()
    watch, _, _, _, _, _ = _watch(
        gateway=gateway,
        catalog=catalog,
        context_source=context_source,
    )
    await watch.start()
    refreshed = catalog.snapshot
    catalog.snapshot = CatalogSnapshot(
        events=expanded.events,
        markets=(refreshed.markets[0], expanded.markets[1]),
        tokens=refreshed.tokens + expanded.tokens[2:],
    )

    await watch.handle_market_change(
        MarketChange(
            change_id="change-refresh-added-market",
            change_type=MarketChangeType.MARKET_ADDED,
            event_id="event-1",
            market_id="market-2",
            token_ids=("token-3", "token-4"),
            occurred_at=200,
        )
    )

    assert gateway.refreshed_market_ids == ["market-1", "market-2"]
    assert catalog.saved_market_ids == [("market-1",), ("market-2",)]
    assert gateway.requests[-1] == (
        "token-1",
        "token-2",
        "token-3",
        "token-4",
    )
    latest = context_source.snapshots[-1]
    assert all(
        token.fee_updated_at == 200
        for token in latest.tokens
        if token.market_id == "market-2"
    )
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
    subscribed = [
        message for message in messages if message.startswith("watch_subscribed ")
    ]
    assert "markets=1" in subscribed[0]
    assert "tokens=2" in subscribed[0]
    assert "generation=1" in subscribed[0]
    assert "markets=2" in subscribed[-1]
    assert "tokens=4" in subscribed[-1]
    assert "generation=2" in subscribed[-1]
    await watch.close()


async def test_start_logs_catalog_subscription_and_recovery_phase_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    watch, _, _, _, _, _ = _watch()

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    messages = [record.getMessage() for record in caplog.records]
    assert "watch_catalog_load_started" in messages
    assert any(
        message.startswith("watch_catalog_loaded ")
        and "events=1" in message
        and "markets=1" in message
        and "tokens=2" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert any(
        message.startswith("watch_subscription_prepared ")
        and "markets=1" in message
        and "tokens=2" in message
        and "token_id_bytes=14" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert any(
        message.startswith("watch_recovery_started ")
        and "tokens=2" in message
        for message in messages
    )
    assert any(
        message.startswith("watch_recovery_baseline_received ")
        and "books=2" in message
        and "generation=1" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert any(
        message.startswith("watch_evaluation_completed ")
        and "tokens=2" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    await watch.close()


async def test_first_accepted_stream_message_logs_listener_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    watch, _, _, _, _, _ = _watch()
    await watch.start()

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.handle_stream_message(
            MarketStreamEvent(
                event_type="last_trade_price",
                market_id="market-1",
                payload={},
                received_timestamp=111,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    assert any(
        record.getMessage().startswith("watch_stream_progress ")
        and "messages=1" in record.getMessage()
        and "event_type=last_trade_price" in record.getMessage()
        and "generation=1" in record.getMessage()
        for record in caplog.records
    )
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


async def test_transient_recovery_invalidation_retries_without_exiting_watch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway = TransientRecoveryGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    watch._recovery_retry_initial_seconds = 0.001

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    assert gateway.recovery_calls == 2
    assert watch.cache.state is CacheState.VALID
    assert any(
        record.getMessage().startswith("watch_recovery_retry_scheduled ")
        and "attempt=1" in record.getMessage()
        and "reason=connection_lost" in record.getMessage()
        for record in caplog.records
    )
    await watch.close()


async def test_transient_recovery_request_retries_without_exiting_watch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway = TransientRequestRecoveryGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    watch._recovery_retry_initial_seconds = 0.001

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    assert gateway.recovery_calls == 2
    assert watch.cache.state is CacheState.VALID
    assert any(
        record.getMessage().startswith("watch_recovery_retry_scheduled ")
        and "reason=request_rejected" in record.getMessage()
        and "status=502" in record.getMessage()
        for record in caplog.records
    )
    await watch.close()


async def test_invalid_recovery_snapshot_closes_subscription_and_retries(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway = InvalidSnapshotOnceGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    watch._recovery_retry_initial_seconds = 0.001

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
        await watch.start()

    assert gateway.recovery_calls == 2
    assert gateway.subscriptions[0].closed is True
    assert gateway.subscriptions[1].closed is False
    assert watch.cache.state is CacheState.VALID
    assert any(
        record.getMessage().startswith("watch_recovery_retry_scheduled ")
        and "reason=cache_snapshot_invalid:best bid must be below best ask"
        in record.getMessage()
        for record in caplog.records
    )
    await watch.close()


async def test_non_retryable_recovery_invalidation_exits_immediately() -> None:
    watch, _, _, _, _, _ = _watch(gateway=NonRetryableRecoveryGateway())

    with pytest.raises(
        MarketRecoveryInvalidatedError,
        match="sdk_version_changed",
    ):
        await watch.start()

    await watch.close()


async def test_close_interrupts_recovery_retry_backoff() -> None:
    gateway = AlwaysInvalidatedRecoveryGateway()
    watch, _, _, _, _, _ = _watch(gateway=gateway)
    watch._recovery_retry_initial_seconds = 60
    start_task = asyncio.create_task(watch.start())

    await asyncio.wait_for(gateway.recovery_called.wait(), timeout=1)
    await asyncio.sleep(0)
    await asyncio.wait_for(watch.close(), timeout=1)
    await asyncio.wait_for(start_task, timeout=1)


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


async def test_stale_deactivation_does_not_close_active_catalog_tokens() -> None:
    """A stale sync control must not override the committed catalog state."""
    watch, gateway, _, _, _, signals = _watch()
    await watch.start()
    original_subscription = gateway.subscriptions[0]
    baseline_closures = len(signals.closed)

    await watch.handle_market_change(
        MarketChange(
            change_id="stale-sync:MARKET_DEACTIVATED:market-1",
            change_type=MarketChangeType.MARKET_DEACTIVATED,
            event_id="event-1",
            market_id="market-1",
            token_ids=("token-1", "token-2"),
            occurred_at=202,
            critical=True,
        )
    )

    assert gateway.requests == [("token-1", "token-2")]
    assert original_subscription.closed is False
    assert len(signals.closed) == baseline_closures
    assert watch.active_token_ids == ("token-1", "token-2")
    await watch.close()


async def test_deactivated_market_outside_subscription_does_not_rotate() -> None:
    watch, gateway, catalog, _, _, signals = _watch()
    await watch.start()
    original_subscription = gateway.subscriptions[0]
    catalog.snapshot = CatalogSnapshot(
        events=(_event(("market-1", "market-2")),),
        markets=(_market("market-1"), _market("market-2", active=False)),
        tokens=(
            _token("token-1", "market-1", 0),
            _token("token-2", "market-1", 1),
            _token("token-3", "market-2", 0),
            _token("token-4", "market-2", 1),
        ),
    )

    await watch.handle_market_change(
        MarketChange(
            change_id="change-inactive-unsubscribed",
            change_type=MarketChangeType.MARKET_DEACTIVATED,
            event_id="event-1",
            market_id="market-2",
            token_ids=("token-3", "token-4"),
            occurred_at=202,
            critical=True,
        )
    )

    assert gateway.requests == [("token-1", "token-2")]
    assert original_subscription.closed is False
    assert any(
        token_ids == ("token-3", "token-4")
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


async def test_price_change_uses_local_arrival_sequence_and_evaluates_changed_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches valid canonical deltas failing to reach affected strategy routing.
    watch, _, _, _, strategy, _ = _watch()
    await watch.start()
    before = len(strategy.calls)

    with caplog.at_level(logging.INFO, logger="predmarket.watch.task"):
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
    assert "watch_price_change_progress messages=1" in caplog.text
    assert "parse_ms_per_message=" in caplog.text
    assert "cache_ms_per_message=" in caplog.text
    assert "book_levels_per_message=" in caplog.text
    await watch.close()


async def test_crossed_price_change_logs_bounded_server_and_cache_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    watch, _, _, _, _, _ = _watch()
    await watch.start()

    with caplog.at_level(logging.WARNING, logger="predmarket.watch.task"):
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
                            "price": "0.55",
                            "size": "9",
                            "hash": "crossed-hash",
                            "best_bid": "0.55",
                            "best_ask": "0.50",
                        }
                    ],
                },
                received_timestamp=111,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

    assert "watch_price_change_invalid" in caplog.text
    assert "market_id=market-1 generation=1" in caplog.text
    assert "exchange_timestamp=110 received_timestamp=111" in caplog.text
    assert "token_id=token-1 side=BUY price=0.55 size=9" in caplog.text
    assert "server_best_bid=0.55 server_best_ask=0.50" in caplog.text
    assert "cache_best_bid=0.40 cache_best_ask=0.50" in caplog.text
    await watch.close()


async def test_price_change_reconciles_stale_top_with_server_best_prices() -> None:
    watch, gateway, _, _, _, _ = _watch()
    await watch.start()
    baseline = replace(
        _book("token-1", 1),
        asks=(
            OrderBookLevel(Decimal("0.41"), Decimal("2")),
            OrderBookLevel(Decimal("0.50"), Decimal("4")),
        ),
        exchange_timestamp=105,
    )
    assert watch.cache.apply_book(baseline) is True

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
                        "best_bid": "0.41",
                        "best_ask": "0.5",
                    }
                ],
            },
            received_timestamp=111,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    book = watch.cache.get("token-1")
    assert book is not None
    assert book.bids[0].price == Decimal("0.41")
    assert book.asks[0].price == Decimal("0.50")
    assert len(gateway.requests) == 1
    await watch.close()


async def test_full_stream_book_replaces_rest_baseline_without_recovery() -> None:
    # Catches the initial WebSocket book snapshot causing an endless resync loop.
    watch, gateway, _, _, strategy, _ = _watch()
    await watch.start()
    before = len(strategy.calls)

    await watch.handle_stream_message(
        MarketStreamEvent(
            event_type="book",
            market_id="market-1",
            payload={
                "token_id": "token-1",
                "timestamp": 110,
                "bids": [{"price": "0.41", "size": "9"}],
                "asks": [{"price": "0.51", "size": "8"}],
                "hash": "stream-hash",
                "tick_size": "0.01",
            },
            received_timestamp=111,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    assert gateway.requests == [("token-1", "token-2")]
    assert gateway.subscriptions[0].closed is False
    assert len(strategy.calls) == before + 1
    book = watch.cache.get("token-1")
    assert book is not None
    assert book.book_hash == "stream-hash"
    assert book.exchange_timestamp == 110
    assert book.minimum_order_size == Decimal("1")
    await watch.close()


async def test_stale_full_stream_book_is_ignored_without_recovery() -> None:
    # Catches a book buffered before REST completion rolling state backward.
    watch, gateway, _, _, strategy, _ = _watch()
    await watch.start()
    before_calls = len(strategy.calls)
    before_book = watch.cache.get("token-1")

    await watch.handle_stream_message(
        MarketStreamEvent(
            event_type="book",
            market_id="market-1",
            payload={
                "token_id": "token-1",
                "timestamp": 99,
                "bids": [{"price": "0.41", "size": "9"}],
                "asks": [{"price": "0.51", "size": "8"}],
                "hash": "stale-stream-hash",
                "tick_size": "0.01",
                "min_order_size": "1",
            },
            received_timestamp=111,
            subscription_generation=1,
            mapping_version="mapping-v1",
        )
    )

    assert gateway.requests == [("token-1", "token-2")]
    assert gateway.subscriptions[0].closed is False
    assert len(strategy.calls) == before_calls
    assert watch.cache.get("token-1") == before_book
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
