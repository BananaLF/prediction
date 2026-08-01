from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sqlite3

import pytest

from predmarket.domain.market import MarketStatus
from predmarket.domain.signal import ExecutionMode, StrategyType
from predmarket.persistence.repositories import SignalRepository
from predmarket.signals.manager import SignalManager
from tests.unit.signals.test_manager import _catalog, _open_manager, _present


@pytest.mark.asyncio
async def test_market_deactivation_queued_before_open_is_revalidated_in_transaction(
    tmp_path: Path,
) -> None:
    writer, catalog, _signals, manager = await _open_manager(tmp_path)
    try:
        _event, market, _token = _catalog()
        deactivation = asyncio.create_task(
            catalog.save_market(
                replace(
                    market,
                    status=MarketStatus.CLOSED,
                    active=False,
                    accepting_orders=False,
                )
            )
        )
        await asyncio.sleep(0)
        with pytest.raises(ValueError, match="watchable"):
            await manager.apply(_present(), "opportunity-race", None)
        await deactivation
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        assert connection.execute("SELECT COUNT(*) FROM arbitrage_signals").fetchone() == (0,)


@pytest.mark.asyncio
async def test_subscription_generation_is_revalidated_after_waiting_for_writer(
    tmp_path: Path,
) -> None:
    writer, _catalog_repo, _signals, _manager = await _open_manager(tmp_path)
    generations = {"token-1": 1}
    manager = SignalManager(
        SignalRepository(tmp_path / "signals.db", writer),
        strategy_type=StrategyType.BINARY_UNDERPRICED,
        execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
        subscription_generation=generations,
        clock=lambda: 101,
    )
    release = asyncio.Event()
    entered = asyncio.Event()

    async def block_writer(_connection) -> None:
        entered.set()
        await release.wait()

    try:
        blocker = asyncio.create_task(writer.execute(block_writer))
        await entered.wait()
        open_task = asyncio.create_task(manager.apply(_present(), "opportunity-generation", None))
        await asyncio.sleep(0)
        generations["token-1"] = 2
        release.set()
        await blocker
        with pytest.raises(ValueError, match="stale subscription generation"):
            await open_task
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        assert connection.execute("SELECT COUNT(*) FROM arbitrage_signals").fetchone() == (0,)


@pytest.mark.asyncio
async def test_two_managers_retry_same_revision_without_duplicate_revision(tmp_path: Path) -> None:
    writer, _catalog_repo, signals, first = await _open_manager(tmp_path)
    second = SignalManager(
        SignalRepository(tmp_path / "signals.db", writer),
        strategy_type=StrategyType.BINARY_UNDERPRICED,
        execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
        clock=lambda: 101,
    )
    try:
        signal_id = await first.apply(_present(), "opportunity-race", None)
        assert signal_id is not None
        results = await asyncio.gather(
            first.apply(_present(expected_profit="0.24"), "opportunity-race", 1),
            second.apply(_present(expected_profit="0.24"), "opportunity-race", 1),
        )
        assert results == [signal_id, signal_id]
        assert await signals.get_latest_revision(signal_id) == 2
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        revisions = connection.execute(
            "SELECT revision, event_type FROM signal_revisions WHERE signal_id = ? ORDER BY revision",
            (signal_id,),
        ).fetchall()
    assert revisions == [(1, "OPENED"), (2, "UPDATED")]


@pytest.mark.asyncio
async def test_revision_payload_failure_rolls_back_main_signal_and_all_evidence(tmp_path: Path) -> None:
    writer, _catalog_repo, _signals, manager = await _open_manager(tmp_path)
    try:
        signal_id = await manager.apply(_present(), "opportunity-atomic", None)
        assert signal_id is not None

        async def install_failure(connection) -> None:
            await connection.execute(
                """
                CREATE TRIGGER fail_revision_two BEFORE INSERT ON orderbook_levels
                WHEN NEW.snapshot_id LIKE '%:2:%'
                BEGIN SELECT RAISE(ABORT, 'injected evidence failure'); END
                """
            )

        await writer.execute(install_failure)
        with pytest.raises(sqlite3.IntegrityError, match="injected evidence failure"):
            await manager.apply(_present(expected_profit="0.24"), "opportunity-atomic", 1)
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        latest = connection.execute(
            "SELECT latest_revision FROM arbitrage_signals WHERE id = ?", (signal_id,)
        ).fetchone()
        revisions = connection.execute(
            "SELECT revision FROM signal_revisions WHERE signal_id = ?", (signal_id,)
        ).fetchall()
        snapshots = connection.execute(
            "SELECT revision FROM orderbook_snapshots WHERE signal_id = ?", (signal_id,)
        ).fetchall()
    assert latest == (1,)
    assert revisions == [(1,)]
    assert snapshots == [(1,)]
