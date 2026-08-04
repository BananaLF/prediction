from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from io import StringIO
import logging
from pathlib import Path

import pytest

from predmarket.app import (
    Supervisor,
    _ApplicationContextSource,
    _SignalManagerRouter,
    _SubscriptionGenerationSource,
)
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
from predmarket.persistence.repositories import (
    CatalogRepository,
    RelationRepository,
    SignalRepository,
)
from predmarket.persistence.schema import initialize_database
from predmarket.persistence.writer import DatabaseWriter
from predmarket.watch.cache import OrderBookCache


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


class _BlockingInitialSync:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run_once(self):
        self.started.set()
        await self.release.wait()
        return type("Result", (), {"complete": True})()


class _SerialSync:
    def __init__(self) -> None:
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()
        self.second_started = asyncio.Event()

    async def run_once(self):
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.calls == 1:
                self.first_started.set()
                await self.release_first.wait()
            elif self.calls == 2:
                self.second_started.set()
            return type("Result", (), {"complete": True})()
        finally:
            self.active -= 1


class _GateSleep:
    def __init__(self) -> None:
        self.called = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, _: float) -> None:
        self.called.set()
        await self.release.wait()
        self.release.clear()


class _SkippedInitialSync:
    async def run_once(self):
        return type(
            "Result",
            (),
            {
                "complete": True,
                "sync_generation": "sync-initial-skipped",
                "skipped_market_ids": ("market-2278824",),
                "warnings": ("events must contain exactly one event reference",),
            },
        )()


class _FailingSync:
    async def run_once(self):
        raise RuntimeError("initial sync failed")


class _IncompletePeriodicSync:
    def __init__(self) -> None:
        self.called = asyncio.Event()

    async def run_once(self):
        self.called.set()
        return type(
            "Result",
            (),
            {
                "complete": False,
                "error": 'market request failed; api_response={"id":"200"}',
                "sync_generation": "sync-periodic",
            },
        )()


class _IncompleteWatchableSync:
    async def run_once(self):
        return type(
            "Result",
            (),
            {
                "complete": False,
                "error": "event request failed",
                "sync_generation": "sync-initial-incomplete",
            },
        )()


class _SkippedPeriodicSync:
    def __init__(self) -> None:
        self.called = asyncio.Event()

    async def run_once(self):
        self.called.set()
        return type(
            "Result",
            (),
            {
                "complete": True,
                "sync_generation": "sync-periodic-skipped",
                "skipped_market_ids": ("market-2278824",),
                "warnings": ("events must contain exactly one event reference",),
            },
        )()


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


@pytest.mark.asyncio
async def test_application_context_source_materializes_async_targets(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    initialize_database(config.database.path)
    writer = DatabaseWriter(config.database.path)
    await writer.start()
    try:
        catalog = CatalogRepository(config.database.path, writer)
        market = Market(
            id="market-context",
            event_id="event-context",
            condition_id="condition-context",
            question="Context market?",
            status=MarketStatus.ACTIVE,
            active=True,
            accepting_orders=True,
            enable_orderbook=True,
            sync_generation="sync-context",
            sync_generation_complete=True,
            tick_size=Decimal("0.01"),
            minimum_order_size=Decimal("1"),
        )
        tokens = (
            Token(
                id="token-context-yes",
                market_id=market.id,
                outcome="YES",
                position=0,
                sync_generation="sync-context",
                sync_generation_complete=True,
            ),
            Token(
                id="token-context-no",
                market_id=market.id,
                outcome="NO",
                position=1,
                sync_generation="sync-context",
                sync_generation_complete=True,
            ),
        )
        await catalog.save_catalog(
            events=(
                Event(
                    id="event-context",
                    title="Context event",
                    status=MarketStatus.ACTIVE,
                    market_ids=(market.id,),
                    sync_generation="sync-context",
                    sync_generation_complete=True,
                ),
            ),
            markets=(market,),
            tokens=tokens,
        )
        source = _ApplicationContextSource(
            config=config,
            catalog=catalog,
            relations=RelationRepository(config.database.path, writer),
            signals=SignalRepository(config.database.path, writer),
            clock_ms=lambda: 1_000,
        )
        books = tuple(
            OrderBook(
                market_id=market.id,
                token_id=token.id,
                bids=(OrderBookLevel(Decimal("0.40"), Decimal("3")),),
                asks=(OrderBookLevel(Decimal("0.50"), Decimal("4")),),
                subscription_generation=1,
                book_hash=f"hash-{token.id}",
                exchange_timestamp=900,
                received_timestamp=950,
                tick_size=Decimal("0.01"),
                minimum_order_size=Decimal("1"),
            )
            for token in tokens
        )

        targets_by_token = await source.contexts_for_batch(
            tuple(token.id for token in tokens),
            books,
        )
        targets = targets_by_token[tokens[0].id]

        assert tuple(target.opportunity_key for target in targets) == (
            "BINARY_UNDERPRICED:market-context",
            "BINARY_OVERPRICED:market-context",
        )
        assert tuple(target.opportunity_key for target in targets_by_token[tokens[1].id]) == (
            "BINARY_UNDERPRICED:market-context",
            "BINARY_OVERPRICED:market-context",
        )
    finally:
        await writer.close()


class _FailingNotifier:
    async def notify(self, **_: object) -> None:
        raise RuntimeError("notifier unavailable")


@pytest.mark.asyncio
async def test_supervisor_syncs_before_watch_and_terminates_after_watch_crash(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
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

    with caplog.at_level(logging.INFO, logger="predmarket.app"):
        assert await supervisor.run() == 1

    assert calls[:2] == ["watch-start", "watch-run"]
    assert calls[-1] == "watch-close"
    assert output.getvalue() == ""
    assert "runtime_starting" in caplog.text
    assert "runtime_build_started" in caplog.text
    assert "runtime_built elapsed_ms=" in caplog.text
    assert "component_initialized component=database" in caplog.text
    assert "component_initialized component=watch_task" in caplog.text
    assert "component_starting component=watch_bootstrap" in caplog.text
    assert "component_started component=watch_bootstrap elapsed_ms=" in caplog.text
    assert "component_started component=watch_task" in caplog.text
    assert "component_started component=sync_task" in caplog.text
    assert "runtime_started" in caplog.text
    assert "runtime_task_exited" in caplog.text
    assert "watch crashed" in caplog.text
    assert "runtime_stopped" in caplog.text
    assert events.entries == []


@pytest.mark.asyncio
async def test_supervisor_starts_watch_before_initial_sync_completes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    sync = _BlockingInitialSync()
    calls: list[str] = []
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        sync_task_factory=lambda **_: sync,
        watch_task_factory=lambda **_: _Watch(calls, crash=False),
        sleep=_wait_for_cancellation,
    )
    running = asyncio.create_task(supervisor.run())

    try:
        with caplog.at_level(logging.INFO, logger="predmarket.app"):
            await asyncio.wait_for(sync.started.wait(), timeout=1)
            assert calls[:2] == ["watch-start", "watch-run"]
            assert "component_started component=watch_task" in caplog.text
            assert "component_started component=sync_task" in caplog.text
            assert "runtime_started" in caplog.text
    finally:
        sync.release.set()
        running.cancel()
        assert await running == 0

    assert calls[-1] == "watch-close"


@pytest.mark.asyncio
async def test_sync_forever_runs_immediately_and_serially(tmp_path: Path) -> None:
    sync = _SerialSync()
    sleep = _GateSleep()
    supervisor = Supervisor(_config(tmp_path), gateway=object(), sleep=sleep)
    task = asyncio.create_task(supervisor._sync_forever(sync, Notifier(terminal=StringIO())))

    try:
        await asyncio.wait_for(sync.first_started.wait(), timeout=1)
        assert not sleep.called.is_set()
        sync.release_first.set()
        await asyncio.wait_for(sleep.called.wait(), timeout=1)
        sleep.release.set()
        await asyncio.wait_for(sync.second_started.wait(), timeout=1)
        assert sync.max_active == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_supervisor_notifies_when_initial_generation_skips_malformed_markets(
    tmp_path: Path,
) -> None:
    output = StringIO()
    notifier = Notifier(terminal=output)
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        notifier=notifier,
        sync_task_factory=lambda **_: _SkippedInitialSync(),
        watch_task_factory=lambda **_: _Watch([], crash=True),
        sleep=_wait_for_cancellation,
    )

    assert await supervisor.run() == 1

    assert output.getvalue() == ""


@pytest.mark.asyncio
async def test_supervisor_starts_watch_from_existing_catalog_during_incomplete_initial_sync(
    tmp_path: Path,
) -> None:
    database_path = _config(tmp_path).database.path
    initialize_database(database_path)
    writer = DatabaseWriter(database_path)
    await writer.start()
    try:
        catalog = CatalogRepository(database_path, writer)
        market = Market(
            id="market-orphan",
            event_id=None,
            condition_id="condition-orphan",
            question="Orphan?",
            status=MarketStatus.ACTIVE,
            active=True,
            accepting_orders=True,
            enable_orderbook=True,
            sync_generation="sync-existing",
            sync_generation_complete=True,
        )
        await catalog.save_catalog(
            events=(),
            markets=(market,),
            tokens=(
                Token(
                    id="token-orphan",
                    market_id=market.id,
                    outcome="YES",
                    position=0,
                    sync_generation="sync-existing",
                    sync_generation_complete=True,
                ),
            ),
        )
    finally:
        await writer.close()

    calls: list[str] = []
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        terminal=StringIO(),
        sync_task_factory=lambda **_: _IncompleteWatchableSync(),
        watch_task_factory=lambda **_: _Watch(calls, crash=True),
        sleep=_wait_for_cancellation,
    )

    assert await supervisor.run() == 1
    assert calls == ["watch-start", "watch-run", "watch-close"]


@pytest.mark.asyncio
async def test_supervisor_treats_cancellation_as_normal_shutdown(tmp_path: Path) -> None:
    calls: list[str] = []
    sync = _IncompletePeriodicSync()
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        sync_task_factory=lambda **_: sync,
        watch_task_factory=lambda **_: _Watch(calls, crash=False),
        sleep=_wait_for_cancellation,
    )
    running = asyncio.create_task(supervisor.run())

    try:
        await asyncio.wait_for(sync.called.wait(), timeout=1)
        running.cancel()
        assert await running == 0
    finally:
        if not running.done():
            running.cancel()
            await running

    assert calls == ["watch-start", "watch-run", "watch-close"]


@pytest.mark.asyncio
async def test_supervisor_reports_background_sync_failure_with_constructed_default_notifier(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
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

    assert output.getvalue() == ""
    assert "runtime_task_exited" in caplog.text
    assert "initial sync failed" in caplog.text


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


@pytest.mark.asyncio
async def test_periodic_sync_notifies_when_generation_is_incomplete(
    tmp_path: Path,
) -> None:
    output = StringIO()
    notifier = Notifier(terminal=output)
    sync = _IncompletePeriodicSync()
    supervisor = Supervisor(
        _config(tmp_path),
        notifier=notifier,
        sleep=lambda _: asyncio.sleep(0),
    )
    task = asyncio.create_task(supervisor._sync_forever(sync, notifier))

    try:
        await asyncio.wait_for(sync.called.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert output.getvalue() == ""


@pytest.mark.asyncio
async def test_periodic_sync_notifies_when_generation_skips_malformed_markets(
    tmp_path: Path,
) -> None:
    output = StringIO()
    notifier = Notifier(terminal=output)
    sync = _SkippedPeriodicSync()
    supervisor = Supervisor(
        _config(tmp_path),
        notifier=notifier,
        sleep=lambda _: asyncio.sleep(0),
    )
    task = asyncio.create_task(supervisor._sync_forever(sync, notifier))

    try:
        await asyncio.wait_for(sync.called.wait(), timeout=1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert output.getvalue() == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ("initialize", "startup", "writer"))
async def test_supervisor_reports_pre_notifier_database_failures_to_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    output = StringIO()

    if stage == "initialize":
        def fail_initialize(_: Path) -> None:
            raise RuntimeError("database initialize failed")

        monkeypatch.setattr("predmarket.app.initialize_database", fail_initialize)
    elif stage == "startup":
        def fail_startup(_: Path) -> None:
            raise RuntimeError("database startup failed")

        monkeypatch.setattr("predmarket.app.check_database_startup", fail_startup)
    else:
        async def fail_writer_start(_: DatabaseWriter) -> None:
            raise RuntimeError("database writer failed")

        monkeypatch.setattr("predmarket.app.DatabaseWriter.start", fail_writer_start)
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        terminal=output,
        sync_task_factory=lambda **_: _Sync([]),
        watch_task_factory=lambda **_: _Watch([], crash=False),
    )

    assert await supervisor.run() == 1
    assert output.getvalue() == ""
    assert "runtime_startup_failed" in caplog.text
    assert f"database {stage} failed" in caplog.text


@pytest.mark.asyncio
async def test_router_revalidates_generation_source_before_database_commit(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.sqlite3"
    writer = DatabaseWriter(database_path)
    await writer.start()
    try:
        source = _SubscriptionGenerationSource()
        router = _SignalManagerRouter(
            SignalRepository(database_path, writer),
            Notifier(terminal=StringIO()),
            lambda: 1,
            subscription_generation=source,
        )
        decision = _persisted_open_signal()[3]
        with pytest.raises(ValueError, match="subscription generation is unavailable"):
            await router.apply(decision, "BINARY_UNDERPRICED:market-1", None)
        cache = OrderBookCache()
        cache.begin_resync(generation=2, token_ids=("token-1",))
        cache.apply_snapshot((replace(decision.evidence[0], subscription_generation=2),))
        source.bind(cache)
        with pytest.raises(ValueError, match="stale subscription generation"):
            await router.apply(decision, "BINARY_UNDERPRICED:market-1", None)
    finally:
        await writer.close()


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
