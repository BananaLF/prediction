from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from io import StringIO
from pathlib import Path

import pytest

from predmarket.app import Supervisor, _SignalManagerRouter
from predmarket.config import AppConfig, DatabaseConfig, NotificationConfig
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import (
    Action,
    DecisionReason,
    ExecutionMode,
    NotEvaluable,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
    StrategyType,
)
from predmarket.notification.notifier import Notifier
from predmarket.persistence.repositories import CatalogRepository, SignalRepository
from predmarket.persistence.writer import DatabaseWriter


class _Events:
    def __init__(self) -> None:
        self.entries: list[dict[str, object]] = []

    async def append(self, **entry: object) -> int:
        self.entries.append(entry)
        return len(self.entries)


class _Sync:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def run_once(self):
        self.calls.append("sync")
        return type("Result", (), {"complete": True})()


class _FailingSync:
    async def run_once(self):
        raise RuntimeError("initial sync failed")


class _Watch:
    def __init__(self, calls: list[str], *, crash: bool) -> None:
        self.calls = calls
        self.crash = crash

    async def start(self) -> None:
        self.calls.append("watch-start")

    async def run(self) -> None:
        self.calls.append("watch-run")
        if self.crash:
            raise RuntimeError("watch crashed")
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.calls.append("watch-close")


async def _wait_for_cancellation(_: float) -> None:
    await asyncio.Event().wait()


def _config(tmp_path: Path) -> AppConfig:
    base = AppConfig.load(Path("config/default.yaml"))
    return replace(
        base,
        database=DatabaseConfig(
            path=tmp_path / "signals.sqlite3",
            busy_timeout_ms=base.database.busy_timeout_ms,
            writer_queue_capacity=base.database.writer_queue_capacity,
        ),
        notification=NotificationConfig(
            terminal_enabled=base.notification.terminal_enabled,
            desktop_enabled=False,
        ),
    )


class _FailingNotifier:
    async def notify(self, **_: object) -> None:
        raise RuntimeError("notifier unavailable")


@pytest.mark.asyncio
async def test_supervisor_syncs_before_watch_and_terminates_after_watch_crash(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    events = _Events()
    output = StringIO()
    notifier = Notifier(terminal=output, system_events=events, clock_ms=lambda: 1)
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        notifier=notifier,
        sync_task_factory=lambda **_: _Sync(calls),
        watch_task_factory=lambda **_: _Watch(calls, crash=True),
        sleep=_wait_for_cancellation,
    )

    assert await supervisor.run() == 1

    assert calls == ["sync", "watch-start", "watch-run", "watch-close"]
    assert "RUNTIME_TASK_EXITED" in output.getvalue()
    assert events.entries == []


@pytest.mark.asyncio
async def test_supervisor_notifies_startup_failure_with_constructed_default_notifier(
    tmp_path: Path,
) -> None:
    output = StringIO()
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        terminal=output,
        sync_task_factory=lambda **_: _FailingSync(),
        watch_task_factory=lambda **_: _Watch([], crash=False),
    )

    assert await supervisor.run() == 1

    assert "RUNTIME_STARTUP_FAILED: Signal service startup failed: initial sync failed" in output.getvalue()


@pytest.mark.asyncio
async def test_supervisor_returns_failure_when_startup_failure_notification_fails(
    tmp_path: Path,
) -> None:
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        notifier=_FailingNotifier(),  # type: ignore[arg-type]
        sync_task_factory=lambda **_: _FailingSync(),
        watch_task_factory=lambda **_: _Watch([], crash=False),
    )

    assert await supervisor.run() == 1


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


@pytest.mark.asyncio
async def test_router_closes_persisted_signal_after_restart_without_evaluation_context(
    tmp_path: Path,
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
        restarted_router = _SignalManagerRouter(
            signals,
            Notifier(terminal=StringIO()),
            lambda: 2,
        )

        await restarted_router.close_for_tokens(
            (token.id,),
            NotEvaluable(
                reason_code=DecisionReason.MARKET_CLOSED,
                context={"detail": "initial sync made every token unwatchable"},
            ),
        )

        assert await signals.find_open_signal_id("BINARY_UNDERPRICED:market-1") is None
    finally:
        await writer.close()
