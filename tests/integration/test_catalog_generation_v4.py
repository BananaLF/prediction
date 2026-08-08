from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sqlite3
from typing import Any

import aiosqlite
import pytest

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.catalog_generations import (
    CatalogFaultPoint,
    CatalogGenerationCoordinator,
    GenerationInput,
    catalog_input_digest,
    write_runtime_catalog,
)
from predmarket.persistence.repositories import (
    CatalogRepository,
    PendingCatalogReconciliation,
    SystemEventRepository,
)
from predmarket.persistence.schema import create_v4_database
from predmarket.persistence.writer import CheckpointMode, CheckpointResult


class _InjectedCatalogCrash(RuntimeError):
    pass


class _CrashOnce:
    def __init__(self, target: CatalogFaultPoint) -> None:
        self.target = target
        self.calls: list[CatalogFaultPoint] = []
        self.crashed = False

    async def __call__(self, point: CatalogFaultPoint) -> None:
        self.calls.append(point)
        if point is self.target and not self.crashed:
            self.crashed = True
            raise _InjectedCatalogCrash(point.value)


class _V4Writer:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def execute(self, command: Any) -> Any:
        async with aiosqlite.connect(self.path, isolation_level=None) as connection:
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("BEGIN IMMEDIATE")
            try:
                result = await command(connection)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
            return result

    async def checkpoint(
        self,
        mode: CheckpointMode = CheckpointMode.PASSIVE,
    ) -> CheckpointResult:
        return CheckpointResult(
            mode=mode,
            busy=0,
            log_pages=0,
            checkpointed_pages=0,
            wal_bytes=0,
        )


def _input(generation: str, *, title: str) -> GenerationInput:
    event = Event(
        id="event-1",
        slug="event-slug",
        title=title,
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    market = Market(
        id="market-1",
        event_id=event.id,
        condition_id="condition-1",
        slug="market-slug",
        question=f"{title}?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    return GenerationInput(
        sync_generation=generation,
        updated_at=10,
        events=(event,),
        markets=(market,),
        tokens=(token,),
        input_digest=catalog_input_digest(
            sync_generation=generation,
            updated_at=10,
            events=(event,),
            markets=(market,),
            tokens=(token,),
        ),
    )


async def test_reader_sees_only_complete_generation_across_activation(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_V4Writer(database_path))
    old = await coordinator.stage(_input("sync-old", title="Old"))
    await coordinator.activate(await coordinator.validate_candidate(old), None)
    new = await coordinator.stage(_input("sync-new", title="New"))
    validation = await coordinator.validate_candidate(new)

    async with aiosqlite.connect(database_path, isolation_level=None) as reader:
        await reader.execute("BEGIN")
        old_event_generation = (
            await (await reader.execute("SELECT sync_generation FROM events")).fetchone()
        )[0]
        await coordinator.activate(validation, None)
        old_market_generation = (
            await (await reader.execute("SELECT sync_generation FROM markets")).fetchone()
        )[0]
        old_token_generation = (
            await (await reader.execute("SELECT sync_generation FROM tokens")).fetchone()
        )[0]
        await reader.execute("ROLLBACK")

        await reader.execute("BEGIN")
        visible_generations: list[str] = []
        for table in ("events", "markets", "tokens"):
            cursor = await reader.execute(f"SELECT sync_generation FROM {table}")
            row = await cursor.fetchone()
            assert row is not None
            visible_generations.append(str(row[0]))
        new_generations = tuple(visible_generations)
        await reader.execute("ROLLBACK")

    assert (
        old_event_generation,
        old_market_generation,
        old_token_generation,
    ) == ("sync-old", "sync-old", "sync-old")
    assert new_generations == ("sync-new", "sync-new", "sync-new")


async def test_v4_repository_runs_stage_validate_activate_and_unique_outbox(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    writer = _V4Writer(database_path)
    catalog = CatalogRepository(database_path, writer)  # type: ignore[arg-type]
    system_events = SystemEventRepository(database_path, writer)  # type: ignore[arg-type]
    value = _input("sync-repository", title="Repository")
    change = MarketChange(
        change_id="sync-repository:CATALOG_RECONCILED:catalog",
        change_type=MarketChangeType.CATALOG_RECONCILED,
        event_id=None,
        market_id=None,
        token_ids=(),
        occurred_at=10,
        critical=True,
    )

    await catalog.save_complete_catalog(
        generation=value.sync_generation,
        updated_at=value.updated_at,
        events=value.events,
        markets=value.markets,
        tokens=value.tokens,
        reconciliation_change=change,
        reconciliation_market_ids=("market-1",),
    )
    await catalog.save_complete_catalog(
        generation=value.sync_generation,
        updated_at=value.updated_at,
        events=value.events,
        markets=value.markets,
        tokens=value.tokens,
        reconciliation_change=change,
        reconciliation_market_ids=("market-1",),
    )

    snapshot = await catalog.load_catalog()
    pending = await system_events.list_pending_catalog_reconciliations()
    assert snapshot.events == value.events
    assert snapshot.markets == value.markets
    assert snapshot.tokens == value.tokens
    assert len(pending) == 1
    assert pending[0].change == change
    assert pending[0].market_ids == ("market-1",)


async def test_effective_candidate_inherits_unchanged_committed_payloads(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_V4Writer(database_path))
    first_value = _input("sync-first", title="First")
    first = await coordinator.stage(first_value)
    await coordinator.activate(await coordinator.validate_candidate(first), None)
    changed_event = replace(
        first_value.events[0],
        title="Second",
        sync_generation="sync-second",
    )
    second_value = GenerationInput(
        sync_generation="sync-second",
        updated_at=10,
        events=(changed_event,),
        markets=(),
        tokens=(),
        input_digest="digest-sync-second",
    )

    second = await coordinator.stage(second_value)
    validation = await coordinator.validate_candidate(second)
    await coordinator.activate(validation, None)

    snapshot = await CatalogRepository(
        database_path,
        _V4Writer(database_path),  # type: ignore[arg-type]
    ).load_catalog()
    assert validation.event_count == 1
    assert validation.market_count == 1
    assert validation.token_count == 1
    assert snapshot.events[0].title == "Second"
    assert snapshot.markets[0].sync_generation == "sync-second"
    assert snapshot.tokens[0].sync_generation == "sync-second"


@pytest.mark.parametrize(
    "fault_point",
    (CatalogFaultPoint.AFTER_BATCH_COMMIT, CatalogFaultPoint.AFTER_CHECKPOINT),
)
async def test_staging_resumes_after_committed_batch_or_checkpoint_crash(
    tmp_path: Path,
    fault_point: CatalogFaultPoint,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    value = _input("sync-restart", title="Restart")
    crashing = _CrashOnce(fault_point)

    with pytest.raises(_InjectedCatalogCrash, match=fault_point.value):
        await CatalogGenerationCoordinator(
            _V4Writer(database_path), fault_hook=crashing
        ).stage(value)

    restarted = CatalogGenerationCoordinator(_V4Writer(database_path))
    staged = await restarted.stage(value)
    await restarted.activate(await restarted.validate_candidate(staged), None)
    snapshot = await CatalogRepository(
        database_path,
        _V4Writer(database_path),  # type: ignore[arg-type]
    ).load_catalog()
    assert snapshot.events == value.events
    assert snapshot.markets == value.markets
    assert snapshot.tokens == value.tokens
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_generations WHERE status = 'STAGING'"
        ).fetchone() == (0,)


async def test_frozen_validation_survives_restart(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    value = _input("sync-validated", title="Validated")
    crashing = _CrashOnce(CatalogFaultPoint.AFTER_VALIDATION)
    coordinator = CatalogGenerationCoordinator(
        _V4Writer(database_path), fault_hook=crashing
    )
    staged = await coordinator.stage(value)

    with pytest.raises(_InjectedCatalogCrash, match="AFTER_VALIDATION"):
        await coordinator.validate_candidate(staged)

    restarted = CatalogGenerationCoordinator(_V4Writer(database_path))
    resumed = await restarted.stage(value)
    validation = await restarted.validate_candidate(resumed)
    await restarted.activate(validation, None)
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM catalog_generations WHERE id = ?", (staged.id,)
        ).fetchone() == ("COMMITTED",)


@pytest.mark.parametrize(
    "fault_point",
    (
        CatalogFaultPoint.BEFORE_ACTIVATION_COMMIT,
        CatalogFaultPoint.AFTER_ACTIVATION_COMMIT,
    ),
)
async def test_activation_crash_is_atomic_and_outbox_is_at_most_once(
    tmp_path: Path,
    fault_point: CatalogFaultPoint,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    value = _input("sync-activation", title="Activation")
    change = MarketChange(
        change_id="sync-activation:CATALOG_RECONCILED:catalog",
        change_type=MarketChangeType.CATALOG_RECONCILED,
        event_id=None,
        market_id=None,
        token_ids=(),
        occurred_at=10,
        critical=True,
    )
    reconciliation = PendingCatalogReconciliation(change, ("market-1",))
    crashing = _CrashOnce(fault_point)
    coordinator = CatalogGenerationCoordinator(
        _V4Writer(database_path), fault_hook=crashing
    )
    staged = await coordinator.stage(value)
    validation = await coordinator.validate_candidate(staged)

    with pytest.raises(_InjectedCatalogCrash, match=fault_point.value):
        await coordinator.activate(validation, reconciliation)

    restarted = CatalogGenerationCoordinator(_V4Writer(database_path))
    if fault_point is CatalogFaultPoint.BEFORE_ACTIVATION_COMMIT:
        await restarted.activate(validation, reconciliation)
    else:
        await CatalogRepository(
            database_path,
            _V4Writer(database_path),  # type: ignore[arg-type]
        ).save_complete_catalog(
            generation=value.sync_generation,
            updated_at=value.updated_at,
            events=value.events,
            markets=value.markets,
            tokens=value.tokens,
            reconciliation_change=change,
            reconciliation_market_ids=("market-1",),
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM system_events "
            "WHERE event_type = 'CATALOG_RECONCILIATION_READY'"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT sync_generation FROM events WHERE id = 'event-1'"
        ).fetchone() == (value.sync_generation,)


async def test_rebase_commit_survives_restart_and_preserves_newer_watch_value(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    old_value = _input("sync-old", title="Old")
    setup = CatalogGenerationCoordinator(_V4Writer(database_path))
    old = await setup.stage(old_value)
    await setup.activate(await setup.validate_candidate(old), None)

    new_value = _input("sync-new", title="Full sync")
    crashing = _CrashOnce(CatalogFaultPoint.AFTER_REBASE)
    coordinator = CatalogGenerationCoordinator(
        _V4Writer(database_path), fault_hook=crashing
    )
    staged = await coordinator.stage(new_value)
    validation = await coordinator.validate_candidate(staged)
    watch_event = replace(
        old_value.events[0], title="Watch wins", updated_at=30
    )
    await _V4Writer(database_path).execute(
        lambda connection: write_runtime_catalog(
            connection, events=(watch_event,), changed_at=30
        )
    )

    with pytest.raises(_InjectedCatalogCrash, match="AFTER_REBASE"):
        await coordinator.activate(validation, None)

    await CatalogGenerationCoordinator(_V4Writer(database_path)).activate(
        validation, None
    )
    snapshot = await CatalogRepository(
        database_path,
        _V4Writer(database_path),  # type: ignore[arg-type]
    ).load_catalog()
    assert snapshot.events[0].title == "Watch wins"
    assert snapshot.events[0].updated_at == 30


async def test_cleanup_resumes_after_committed_batch_crash(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    setup = CatalogGenerationCoordinator(_V4Writer(database_path))
    old = await setup.stage(_input("sync-cleanup-old", title="Old"))
    await setup.activate(await setup.validate_candidate(old), None)
    new = await setup.stage(_input("sync-cleanup-new", title="New"))
    await setup.activate(await setup.validate_candidate(new), None)

    crashing = _CrashOnce(CatalogFaultPoint.AFTER_CLEANUP_BATCH)
    with pytest.raises(_InjectedCatalogCrash, match="AFTER_CLEANUP_BATCH"):
        await CatalogGenerationCoordinator(
            _V4Writer(database_path), fault_hook=crashing
        ).cleanup(max_rows=1)

    restarted = CatalogGenerationCoordinator(_V4Writer(database_path))
    while True:
        result = await restarted.cleanup(max_rows=1)
        if result.remaining_versions == 0 and result.remaining_journal_rows == 0:
            break

    snapshot = await CatalogRepository(
        database_path,
        _V4Writer(database_path),  # type: ignore[arg-type]
    ).load_catalog()
    assert snapshot.events[0].title == "New"
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_generations WHERE status = 'STAGING'"
        ).fetchone() == (0,)


async def test_catalog_queries_preserve_results_and_index_correlated_lookups(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    value = _input("sync-query-plan", title="Query plan")
    coordinator = CatalogGenerationCoordinator(_V4Writer(database_path))
    staged = await coordinator.stage(value)
    await coordinator.activate(await coordinator.validate_candidate(staged), None)

    queries = (
        ("SELECT id FROM events ORDER BY id", ()),
        ("SELECT id FROM events WHERE id = ?", ("event-1",)),
        ("SELECT id FROM events WHERE slug = ?", ("event-slug",)),
        ("SELECT id FROM markets WHERE id = ?", ("market-1",)),
        ("SELECT id FROM markets WHERE slug = ?", ("market-slug",)),
        ("SELECT id FROM markets WHERE condition_id = ?", ("condition-1",)),
        ("SELECT id FROM tokens WHERE id = ?", ("token-1",)),
        (
            "SELECT markets.id, tokens.id FROM markets "
            "JOIN tokens ON tokens.market_id = markets.id "
            "WHERE markets.status = 'ACTIVE' AND markets.active = 1",
            (),
        ),
        (
            "SELECT events.id, markets.id, tokens.id FROM events "
            "JOIN markets ON markets.event_id = events.id "
            "JOIN tokens ON tokens.market_id = markets.id "
            "WHERE events.id = ?",
            ("event-1",),
        ),
    )
    with sqlite3.connect(database_path) as connection:
        for statement, parameters in queries:
            rows = connection.execute(statement, parameters).fetchall()
            assert rows
            plan = connection.execute(
                f"EXPLAIN QUERY PLAN {statement}", parameters
            ).fetchall()
            details = tuple(str(row[3]) for row in plan)
            assert not any("SCAN candidate" in detail for detail in details), details
            assert all(
                "USING" in detail
                for detail in details
                if "SEARCH candidate" in detail
            ), details
