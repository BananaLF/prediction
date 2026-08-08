from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from io import StringIO
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest

from predmarket.app import _SignalManagerRouter
from predmarket.catalog.changes import (
    MarketChange,
    MarketChangeQueue,
    MarketChangeType,
)
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import (
    Action,
    ExecutionMode,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
    StrategyType,
)
from predmarket.notification.notifier import Notifier
from predmarket.persistence.repositories import (
    CatalogRepository,
    CatalogSnapshot,
    SignalRepository,
)
from predmarket.persistence.writer import DatabaseWriter
from predmarket.polymarket.gateway import MarketStreamEvent, MarketStreamInvalidated
from predmarket.watch.cache import CacheState
from predmarket.watch.task import EvaluationTarget, WatchTask


def _book(
    token_id: str,
    generation: int,
    *,
    exchange_timestamp: int | None = None,
    received_timestamp: int | None = None,
) -> OrderBook:
    return OrderBook(
        market_id="market-1",
        token_id=token_id,
        bids=(OrderBookLevel(Decimal("0.40"), Decimal("3")),),
        asks=(OrderBookLevel(Decimal("0.50"), Decimal("4")),),
        subscription_generation=generation,
        book_hash=f"hash-{generation}-{token_id}",
        exchange_timestamp=(
            100 + generation if exchange_timestamp is None else exchange_timestamp
        ),
        received_timestamp=(
            101 + generation if received_timestamp is None else received_timestamp
        ),
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


class _Subscription:
    def __init__(self, generation: int) -> None:
        self.subscription_generation = generation
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.Event().wait()
        raise StopAsyncIteration

    async def close(self) -> None:
        self.closed = True


class _Gateway:
    def __init__(self) -> None:
        self.generation = 0
        self.block_next: asyncio.Event | None = None
        self.subscriptions: list[_Subscription] = []

    def hydrate_market_identities(
        self,
        markets: tuple[Market, ...],
        tokens: tuple[Token, ...],
        market_ids: tuple[str, ...],
    ) -> None:
        return None

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.generation += 1
        generation = self.generation
        subscription = _Subscription(generation)
        self.subscriptions.append(subscription)
        if self.block_next is not None:
            await self.block_next.wait()
        return SimpleNamespace(
            order_books=tuple(_book(token_id, generation) for token_id in token_ids),
            subscription=subscription,
            subscription_generation=generation,
        )


class _Catalog:
    async def load_catalog(self) -> CatalogSnapshot:
        event = Event(
            id="event-1",
            title="Event",
            status=MarketStatus.ACTIVE,
            market_ids=("market-1",),
            sync_generation="sync-1",
            sync_generation_complete=True,
        )
        market = Market(
            id="market-1",
            event_id="event-1",
            condition_id="condition-1",
            question="Question",
            status=MarketStatus.ACTIVE,
            active=True,
            accepting_orders=True,
            enable_orderbook=True,
            sync_generation="sync-1",
            sync_generation_complete=True,
            tick_size=Decimal("0.01"),
            minimum_order_size=Decimal("1"),
        )
        tokens = tuple(
            Token(
                id=f"token-{position + 1}",
                market_id="market-1",
                outcome=outcome,
                position=position,
                sync_generation="sync-1",
                sync_generation_complete=True,
            )
            for position, outcome in enumerate(("Yes", "No"))
        )
        return CatalogSnapshot(events=(event,), markets=(market,), tokens=tokens)


class _Changes:
    async def get(self):
        await asyncio.Event().wait()

    def task_done(self) -> None:
        raise AssertionError("no catalog change expected")


class _Contexts:
    def contexts_for(
        self,
        changed_token_id: str,
        orderbooks: tuple[OrderBook, ...],
    ) -> tuple[EvaluationTarget, ...]:
        if changed_token_id != "token-1":
            return ()
        return (
            EvaluationTarget(
                context=SimpleNamespace(
                    changed_token_id=changed_token_id,
                    orderbooks=orderbooks,
                ),  # type: ignore[arg-type]
                opportunity_key="same-opportunity",
                expected_revision=None,
            ),
        )


class _PersistedContexts:
    def __init__(self, signals: SignalRepository) -> None:
        self._signals = signals

    async def contexts_for(
        self,
        changed_token_id: str,
        orderbooks: tuple[OrderBook, ...],
    ) -> tuple[EvaluationTarget, ...]:
        if changed_token_id != "token-1":
            return ()
        opportunity_key = "BINARY_UNDERPRICED:market-1"
        revisions = await self._signals.find_open_revisions((opportunity_key,))
        return (
            EvaluationTarget(
                context=SimpleNamespace(
                    changed_token_id=changed_token_id,
                    orderbooks=orderbooks,
                ),  # type: ignore[arg-type]
                opportunity_key=opportunity_key,
                expected_revision=revisions.get(opportunity_key),
            ),
        )


class _ProfitableStrategy:
    def evaluate(self, context: Any) -> OpportunityPresent:
        book = next(
            candidate for candidate in context.orderbooks if candidate.token_id == "token-1"
        )
        return OpportunityPresent(
            calculation=OpportunityCalculation(
                quantity=Decimal("1"),
                total_capital=Decimal("0.5"),
                expected_profit=Decimal("0.1"),
                return_rate=Decimal("0.2"),
                worst_case_loss=Decimal("0"),
                risk_rate=Decimal("0"),
                unhedged_notional=Decimal("0"),
            ),
            legs=(
                SignalLeg(
                    position=0,
                    market_id="market-1",
                    token_id="token-1",
                    action=Action.BUY,
                    quantity=Decimal("1"),
                    average_price=Decimal("0.5"),
                    worst_price=Decimal("0.5"),
                    gross_amount=Decimal("0.5"),
                    fee_amount=Decimal("0"),
                ),
            ),
            evidence=(book,),
        )


class _LifecycleSignals:
    def __init__(self) -> None:
        self.next_id = 1
        self.open_id: str | None = None
        self.opened_ids: list[str] = []
        self.closed_ids: list[str] = []

    async def apply(
        self,
        decision: Any,
        opportunity_key: str,
        expected_revision: int | None,
        *,
        observed_at: int,
    ) -> str | None:
        assert opportunity_key == "same-opportunity"
        assert expected_revision is None
        if not isinstance(decision, OpportunityPresent):
            return None
        if self.open_id is None:
            self.open_id = f"signal-{self.next_id}"
            self.next_id += 1
            self.opened_ids.append(self.open_id)
        return self.open_id

    async def close_for_tokens(
        self,
        token_ids: tuple[str, ...],
        decision: Any,
        *,
        observed_at: int,
    ) -> None:
        assert token_ids == ("token-1", "token-2")
        if self.open_id is not None:
            self.closed_ids.append(self.open_id)
            self.open_id = None


async def _ignore_market_change_report(_: object) -> None:
    return None


def _persisted_open_signal() -> tuple[Event, Market, Token, OpportunityPresent]:
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    market = Market(
        id="market-1",
        event_id=event.id,
        condition_id="condition-1",
        question="Question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    present = OpportunityPresent(
        calculation=OpportunityCalculation(
            quantity=Decimal("2"),
            total_capital=Decimal("0.80"),
            expected_profit=Decimal("0.20"),
            return_rate=Decimal("0.25"),
            worst_case_loss=Decimal("0.40"),
            risk_rate=Decimal("0.5"),
            unhedged_notional=Decimal("0.40"),
        ),
        legs=(
            SignalLeg(
                position=0,
                market_id=market.id,
                token_id=token.id,
                action=Action.BUY,
                quantity=Decimal("2"),
                average_price=Decimal("0.4"),
                worst_price=Decimal("0.4"),
                gross_amount=Decimal("0.80"),
                fee_amount=Decimal("0"),
            ),
        ),
        evidence=(
            OrderBook(
                market_id=market.id,
                token_id=token.id,
                bids=(OrderBookLevel(Decimal("0.3"), Decimal("2")),),
                asks=(OrderBookLevel(Decimal("0.4"), Decimal("2")),),
                subscription_generation=1,
                book_hash="hash-token-1",
                exchange_timestamp=10,
                received_timestamp=11,
                tick_size=Decimal("0.01"),
                minimum_order_size=Decimal("1"),
            ),
        ),
    )
    return event, market, token, present


@pytest.mark.parametrize(
    "change_type",
    (MarketChangeType.MARKET_DEACTIVATED, MarketChangeType.EVENT_SETTLED),
)
async def test_queued_control_change_without_market_watermark_does_not_reclose_signal(
    tmp_path: Path,
    change_type: MarketChangeType,
    caplog: pytest.LogCaptureFixture,
) -> None:
    database_path = tmp_path / "signals.sqlite3"
    writer = DatabaseWriter(database_path)
    await writer.start()
    try:
        catalog = CatalogRepository(database_path, writer)
        signals = SignalRepository(database_path, writer)
        event, market, token, present = _persisted_open_signal()
        await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))
        await signals.open_signal(
            signal_id="persisted-signal",
            opportunity_key="BINARY_UNDERPRICED:market-1",
            strategy_type=StrategyType.BINARY_UNDERPRICED,
            market_ids=(market.id,),
            relation_id=None,
            execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
            observed_at=1,
            decision=present,
        )
        await catalog.save_catalog(
            events=(event,),
            markets=(
                replace(
                    market,
                    status=MarketStatus.CLOSED,
                    active=False,
                    accepting_orders=False,
                    enable_orderbook=False,
                ),
            ),
            tokens=(token,),
        )
        changes = MarketChangeQueue(
            1,
            record_system_event=_ignore_market_change_report,
            notify=_ignore_market_change_report,
        )
        watch = WatchTask(
            gateway=object(),
            catalog=catalog,
            changes=changes,
            strategy_engine=object(),
            signal_manager=_SignalManagerRouter(
                signals,
                Notifier(terminal=StringIO()),
            ),
            context_source=object(),
        )

        await watch.start()
        assert watch.active_token_ids == ()
        await changes.put(
            MarketChange(
                change_id=f"queued-{change_type.value}",
                change_type=change_type,
                event_id=event.id,
                market_id=None
                if change_type is MarketChangeType.EVENT_SETTLED
                else market.id,
                token_ids=(token.id,),
                occurred_at=2,
                critical=True,
            )
        )

        running = asyncio.create_task(watch.run())
        try:
            await asyncio.wait_for(changes.join(), timeout=0.1)
            assert await signals.find_open_signal_id(
                "BINARY_UNDERPRICED:market-1"
            ) is None
            with sqlite3.connect(database_path) as connection:
                signal_row = connection.execute(
                    "SELECT status, latest_revision, close_reason, closed_at "
                    "FROM arbitrage_signals WHERE id = 'persisted-signal'"
                ).fetchone()
                observed_times = connection.execute(
                    "SELECT observed_at FROM signal_revisions "
                    "WHERE signal_id = 'persisted-signal' ORDER BY revision"
                ).fetchall()
            assert signal_row == ("CLOSED", 2, "MARKET_CLOSED", 1)
            assert observed_times == [(1,), (1,)]
            assert any(
                record.getMessage().startswith("watch_signal_mutation_skipped ")
                and "operation=close_for_tokens" in record.getMessage()
                and "generation=0" in record.getMessage()
                and "market_time=None" in record.getMessage()
                for record in caplog.records
            )
        finally:
            await watch.close()
            await asyncio.wait_for(running, timeout=0.1)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_start_without_market_watermark_reuses_latest_signal_time(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A restart cannot invent a business timestamp when no market is watchable."""

    database_path = tmp_path / "signals.sqlite3"
    writer = DatabaseWriter(database_path)
    await writer.start()
    try:
        catalog = CatalogRepository(database_path, writer)
        signals = SignalRepository(database_path, writer)
        event, market, token, present = _persisted_open_signal()
        await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))
        await signals.open_signal(
            signal_id="persisted-signal",
            opportunity_key="BINARY_UNDERPRICED:market-1",
            strategy_type=StrategyType.BINARY_UNDERPRICED,
            market_ids=(market.id,),
            relation_id=None,
            execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
            observed_at=1,
            decision=present,
        )
        # Deliberately omit MarketChangeQueue.put: this is the crash window.
        await catalog.save_catalog(
            events=(event,),
            markets=(
                replace(
                    market,
                    status=MarketStatus.CLOSED,
                    active=False,
                    accepting_orders=False,
                    enable_orderbook=False,
                ),
            ),
            tokens=(token,),
        )
        watch = WatchTask(
            gateway=object(),
            catalog=catalog,
            changes=_Changes(),
            strategy_engine=object(),
            signal_manager=_SignalManagerRouter(
                signals,
                Notifier(terminal=StringIO()),
            ),
            context_source=object(),
        )

        await watch.start()

        assert watch.active_token_ids == ()
        assert await signals.find_open_signal_id(
            "BINARY_UNDERPRICED:market-1"
        ) is None
        with sqlite3.connect(database_path) as connection:
            signal_row = connection.execute(
                "SELECT status, latest_revision, close_reason, closed_at "
                "FROM arbitrage_signals WHERE id = 'persisted-signal'"
            ).fetchone()
            observed_times = connection.execute(
                "SELECT observed_at FROM signal_revisions "
                "WHERE signal_id = 'persisted-signal' ORDER BY revision"
            ).fetchall()
        assert signal_row == ("CLOSED", 2, "MARKET_CLOSED", 1)
        assert observed_times == [(1,), (1,)]
        assert any(
            record.getMessage().startswith(
                "watch_signal_reconciliation_using_latest_signal_time "
            )
            and "operation=reconcile_open_signals" in record.getMessage()
            and "generation=0" in record.getMessage()
            and "market_time=None" in record.getMessage()
            for record in caplog.records
        )
    finally:
        await writer.close()


async def test_recovery_closes_old_signal_and_reopens_with_new_signal_id() -> None:
    # Catches a CLOSED signal ID being reused after a generation recovery barrier.
    gateway = _Gateway()
    signals = _LifecycleSignals()
    watch = WatchTask(
        gateway=gateway,
        catalog=_Catalog(),
        changes=_Changes(),
        strategy_engine=_ProfitableStrategy(),
        signal_manager=signals,
        context_source=_Contexts(),
    )
    await watch.start()
    assert signals.opened_ids == ["signal-1"]

    gateway.block_next = asyncio.Event()
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
    for _ in range(10):
        await asyncio.sleep(0)
        if signals.closed_ids:
            break

    assert signals.closed_ids == ["signal-1"]
    assert signals.open_id is None
    assert watch.cache.state is CacheState.INVALID
    assert signals.opened_ids == ["signal-1"]

    gateway.block_next.set()
    await recovering

    assert watch.cache.state is CacheState.VALID
    assert signals.opened_ids == ["signal-1", "signal-2"]
    assert signals.open_id == "signal-2"
    await watch.close()


@pytest.mark.asyncio
async def test_sqlite_signal_times_follow_each_market_generation_across_recovery(
    tmp_path: Path,
) -> None:
    class MarketTimeGateway(_Gateway):
        async def recover_market_session(self, token_ids: tuple[str, ...]):
            self.generation += 1
            generation = self.generation
            subscription = _Subscription(generation)
            self.subscriptions.append(subscription)
            if self.block_next is not None:
                await self.block_next.wait()
            timestamps = {
                1: (20_000, 20_020),
                2: (5_000, 5_020),
            }[generation]
            return SimpleNamespace(
                order_books=tuple(
                    _book(
                        token_id,
                        generation,
                        exchange_timestamp=timestamps[index],
                        received_timestamp=10_000,
                    )
                    for index, token_id in enumerate(token_ids)
                ),
                subscription=subscription,
                subscription_generation=generation,
            )

    database_path = tmp_path / "signals.sqlite3"
    writer = DatabaseWriter(database_path)
    await writer.start()
    gateway = MarketTimeGateway()
    watch: WatchTask | None = None
    try:
        catalog = CatalogRepository(database_path, writer)
        signals = SignalRepository(database_path, writer)
        snapshot = await _Catalog().load_catalog()
        await catalog.save_catalog(
            events=snapshot.events,
            markets=snapshot.markets,
            tokens=snapshot.tokens,
        )
        watch = WatchTask(
            gateway=gateway,
            catalog=catalog,
            changes=_Changes(),
            strategy_engine=_ProfitableStrategy(),
            signal_manager=_SignalManagerRouter(
                signals,
                Notifier(terminal=StringIO()),
            ),
            context_source=_PersistedContexts(signals),
            clock_ms=lambda: 10_000,
        )

        await watch.start()
        await watch.handle_stream_message(
            MarketStreamEvent(
                event_type="book",
                market_id="market-1",
                payload={
                    "token_id": "token-1",
                    "timestamp": 20_100,
                    "bids": [{"price": "0.41", "size": "3"}],
                    "asks": [{"price": "0.51", "size": "4"}],
                    "hash": "stream-generation-1",
                    "tick_size": "0.01",
                },
                received_timestamp=10_000,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

        gateway.block_next = asyncio.Event()
        recovering = asyncio.create_task(
            watch.handle_stream_message(
                MarketStreamInvalidated(
                    reason="connection_lost",
                    token_ids=("token-1", "token-2"),
                    received_timestamp=10_000,
                    subscription_generation=1,
                    mapping_version="mapping-v1",
                )
            )
        )
        for _ in range(50):
            if await signals.find_open_signal_id(
                "BINARY_UNDERPRICED:market-1"
            ) is None:
                break
            await asyncio.sleep(0)

        with sqlite3.connect(database_path) as connection:
            assert connection.execute(
                """
                SELECT event_type, observed_at
                FROM signal_revisions
                ORDER BY rowid
                """
            ).fetchall() == [
                ("OPENED", 20_020),
                ("UPDATED", 20_100),
                ("CLOSED", 20_100),
            ]

        gateway.block_next.set()
        await recovering

        with sqlite3.connect(database_path) as connection:
            revisions_before_old_event = connection.execute(
                """
                SELECT event_type, observed_at
                FROM signal_revisions
                ORDER BY rowid
                """
            ).fetchall()
            assert revisions_before_old_event == [
                ("OPENED", 20_020),
                ("UPDATED", 20_100),
                ("CLOSED", 20_100),
                ("OPENED", 5_020),
            ]
            assert connection.execute(
                """
                SELECT status, opened_at, updated_at, closed_at
                FROM arbitrage_signals
                ORDER BY opened_at DESC
                """
            ).fetchall() == [
                ("CLOSED", 20_020, 20_100, 20_100),
                ("OPEN", 5_020, 5_020, None),
            ]

        await watch.handle_stream_message(
            MarketStreamEvent(
                event_type="book",
                market_id="market-1",
                payload={
                    "token_id": "token-1",
                    "timestamp": 99_999,
                    "bids": [{"price": "0.42", "size": "3"}],
                    "asks": [{"price": "0.52", "size": "4"}],
                    "hash": "stale-generation-1",
                    "tick_size": "0.01",
                },
                received_timestamp=10_000,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )

        assert watch.cache.generation == 2
        assert watch.cache.get("token-1").exchange_timestamp == 5_000
        with sqlite3.connect(database_path) as connection:
            assert connection.execute(
                """
                SELECT event_type, observed_at
                FROM signal_revisions
                ORDER BY rowid
                """
            ).fetchall() == revisions_before_old_event
    finally:
        if watch is not None:
            await watch.close()
        await writer.close()
