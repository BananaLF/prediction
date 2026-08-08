from __future__ import annotations

from dataclasses import replace
import sqlite3
from pathlib import Path
from typing import Any

import aiosqlite
import pytest

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.catalog_generations import (
    CandidateValidation,
    CatalogActivationConflict,
    CatalogBatchLimits,
    CatalogConstraintError,
    CatalogGenerationCoordinator,
    CatalogSyncAborted,
    GenerationInput,
)
from predmarket.persistence.schema import GenerationStatus, create_v4_database
from predmarket.persistence.repositories import PendingCatalogReconciliation
from predmarket.persistence.wal import WalPolicy
from predmarket.persistence.writer import CheckpointMode, CheckpointResult


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _insert_generation(
    connection: sqlite3.Connection,
    *,
    sync_generation: str,
    status: GenerationStatus,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO catalog_generations (
            sync_generation, input_digest, base_generation_id,
            base_runtime_revision, status, created_at
        ) VALUES (?, ?, 1, 0, ?, 1)
        """,
        (sync_generation, f"digest-{sync_generation}", status.value),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _insert_event_version(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    generation_id: int,
    title: str,
) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO catalog_event_ids (id) VALUES (?)",
        (event_id,),
    )
    connection.execute(
        """
        INSERT INTO event_versions (
            entity_id, generation_id, title, status, neg_risk,
            neg_risk_complete, neg_risk_conversion_supported,
            market_ids_json, created_at, updated_at
        ) VALUES (?, ?, ?, 'ACTIVE', 0, 0, 0, '[]', 1, 1)
        """,
        (event_id, generation_id, title),
    )


class _CatalogTestWriter:
    def __init__(
        self,
        path: Path,
        *,
        fail_transaction: int | None = None,
        checkpoint_wal_bytes: tuple[int, ...] = (),
    ) -> None:
        self.path = path
        self.fail_transaction = fail_transaction
        self.checkpoint_wal_bytes = list(checkpoint_wal_bytes)
        self.transaction_count = 0
        self.written_totals: list[int] = []

    async def execute(self, command: Any) -> Any:
        self.transaction_count += 1
        if self.transaction_count == self.fail_transaction:
            raise RuntimeError("injected writer failure")
        async with aiosqlite.connect(self.path, isolation_level=None) as connection:
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("BEGIN IMMEDIATE")
            try:
                result = await command(connection)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
            cursor = await connection.execute(
                """
                SELECT COALESCE(MAX(
                    written_event_count + written_market_count + written_token_count
                ), 0)
                FROM catalog_generations
                """
            )
            self.written_totals.append(int((await cursor.fetchone())[0]))
            return result

    async def checkpoint(
        self,
        mode: CheckpointMode = CheckpointMode.PASSIVE,
    ) -> CheckpointResult:
        wal_bytes = (
            self.checkpoint_wal_bytes.pop(0)
            if self.checkpoint_wal_bytes
            else 0
        )
        return CheckpointResult(
            mode=mode,
            busy=0,
            log_pages=0,
            checkpointed_pages=0,
            wal_bytes=wal_bytes,
        )


def _generation_input(
    *,
    generation: str,
    event_count: int,
    input_digest: str,
) -> GenerationInput:
    events = tuple(
        Event(
            id=f"event-{index:05d}",
            title=f"Event {index}",
            status=MarketStatus.ACTIVE,
            market_ids=(),
            sync_generation=generation,
            sync_generation_complete=True,
            updated_at=10,
        )
        for index in range(event_count)
    )
    return GenerationInput(
        sync_generation=generation,
        updated_at=10,
        events=events,
        markets=(),
        tokens=(),
        input_digest=input_digest,
    )


def test_catalog_views_select_latest_visible_committed_version(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)

    with _connect(database_path) as connection:
        committed_id = _insert_generation(
            connection,
            sync_generation="sync-committed",
            status=GenerationStatus.COMMITTED,
        )
        staging_id = _insert_generation(
            connection,
            sync_generation="sync-staging",
            status=GenerationStatus.STAGING,
        )
        aborted_id = _insert_generation(
            connection,
            sync_generation="sync-aborted",
            status=GenerationStatus.ABORTED,
        )
        _insert_event_version(
            connection,
            event_id="event-1",
            generation_id=1,
            title="Bootstrap payload",
        )
        _insert_event_version(
            connection,
            event_id="event-1",
            generation_id=committed_id,
            title="Committed payload",
        )
        _insert_event_version(
            connection,
            event_id="event-1",
            generation_id=staging_id,
            title="Staging payload",
        )
        _insert_event_version(
            connection,
            event_id="event-1",
            generation_id=aborted_id,
            title="Aborted payload",
        )
        connection.execute(
            "UPDATE catalog_state SET active_generation_id = ? WHERE id = 1",
            (committed_id,),
        )

        assert connection.execute(
            """
            SELECT title, sync_generation, sync_generation_complete
            FROM events WHERE id = 'event-1'
            """
        ).fetchone() == ("Committed payload", "sync-committed", 1)


def test_catalog_schema_allows_only_one_staging_generation(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)

    with _connect(database_path) as connection:
        _insert_generation(
            connection,
            sync_generation="sync-staging-1",
            status=GenerationStatus.STAGING,
        )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_generation(
                connection,
                sync_generation="sync-staging-2",
                status=GenerationStatus.STAGING,
            )


def test_catalog_views_keep_unchanged_entities_and_hide_staging_only_entities(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)

    with _connect(database_path) as connection:
        old_generation_id = _insert_generation(
            connection,
            sync_generation="sync-old",
            status=GenerationStatus.COMMITTED,
        )
        active_generation_id = _insert_generation(
            connection,
            sync_generation="sync-active",
            status=GenerationStatus.COMMITTED,
        )
        staging_generation_id = _insert_generation(
            connection,
            sync_generation="sync-staging",
            status=GenerationStatus.STAGING,
        )
        _insert_event_version(
            connection,
            event_id="event-unchanged",
            generation_id=old_generation_id,
            title="Unchanged",
        )
        _insert_event_version(
            connection,
            event_id="event-staging-only",
            generation_id=staging_generation_id,
            title="Invisible",
        )
        connection.execute(
            "UPDATE catalog_state SET active_generation_id = ? WHERE id = 1",
            (active_generation_id,),
        )

        assert connection.execute(
            "SELECT id, sync_generation FROM events ORDER BY id"
        ).fetchall() == [("event-unchanged", "sync-active")]
        with pytest.raises(
            sqlite3.OperationalError,
            match="cannot modify markets because it is a view",
        ):
            connection.execute(
                """
                INSERT INTO markets (
                    id, condition_id, question, status, active, accepting_orders,
                    enable_orderbook, neg_risk, neg_risk_member_complete,
                    sync_generation, sync_generation_complete, created_at, updated_at
                ) VALUES (
                    'market-1', 'condition-1', 'Question?', 'ACTIVE', 1, 1,
                    1, 0, 0, 'sync-active', 1, 1, 1
                )
                """
            )


async def test_stage_splits_large_inputs_and_commits_progress_with_each_batch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    writer = _CatalogTestWriter(database_path)
    coordinator = CatalogGenerationCoordinator(writer)

    generation = await coordinator.stage(
        _generation_input(
            generation="sync-large",
            event_count=8_001,
            input_digest="digest-large",
        )
    )

    assert writer.transaction_count == 3  # create plus two payload batches
    assert writer.written_totals == [0, 8_000, 8_001]
    with _connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT status, planned_event_count, written_event_count,
                   event_cursor, planned_event_digest, written_event_digest
            FROM catalog_generations WHERE id = ?
            """,
            (generation.id,),
        ).fetchone()
        version_count = connection.execute(
            "SELECT COUNT(*) FROM event_versions WHERE generation_id = ?",
            (generation.id,),
        ).fetchone()[0]
    assert row[:4] == ("STAGING", 8_001, 8_001, "event-08000")
    assert row[4] == row[5]
    assert version_count == 8_001


async def test_stage_resumes_matching_digest_without_duplicate_versions(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    value = _generation_input(
        generation="sync-resume",
        event_count=5,
        input_digest="digest-resume",
    )
    failing_writer = _CatalogTestWriter(database_path, fail_transaction=3)
    coordinator = CatalogGenerationCoordinator(
        failing_writer,
        batch_limits=CatalogBatchLimits(max_rows=2, max_payload_bytes=1_000_000),
    )

    with pytest.raises(RuntimeError, match="injected writer failure"):
        await coordinator.stage(value)

    resumed = await CatalogGenerationCoordinator(
        _CatalogTestWriter(database_path),
        batch_limits=CatalogBatchLimits(max_rows=2, max_payload_bytes=1_000_000),
    ).stage(value)

    with _connect(database_path) as connection:
        generations = connection.execute(
            "SELECT id, status, written_event_count FROM catalog_generations "
            "WHERE sync_generation = 'sync-resume'"
        ).fetchall()
        version_count = connection.execute(
            "SELECT COUNT(*) FROM event_versions WHERE generation_id = ?",
            (resumed.id,),
        ).fetchone()[0]
    assert generations == [(resumed.id, "STAGING", 5)]
    assert version_count == 5


async def test_stage_aborts_stale_digest_before_creating_replacement(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    first = _generation_input(
        generation="sync-old-input",
        event_count=3,
        input_digest="digest-old",
    )
    failing_writer = _CatalogTestWriter(database_path, fail_transaction=3)
    limits = CatalogBatchLimits(max_rows=1, max_payload_bytes=1_000_000)
    with pytest.raises(RuntimeError, match="injected writer failure"):
        await CatalogGenerationCoordinator(
            failing_writer,
            batch_limits=limits,
        ).stage(first)

    replacement = await CatalogGenerationCoordinator(
        _CatalogTestWriter(database_path),
        batch_limits=limits,
    ).stage(
        replace(
            first,
            sync_generation="sync-new-input",
            input_digest="digest-new",
            events=tuple(
                replace(event, sync_generation="sync-new-input")
                for event in first.events
            ),
        )
    )

    with _connect(database_path) as connection:
        rows = connection.execute(
            """
            SELECT sync_generation, status, failure_reason
            FROM catalog_generations
            WHERE sync_generation != 'bootstrap-v4'
            ORDER BY id
            """
        ).fetchall()
        active_generation_id = connection.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        ).fetchone()[0]
    assert rows == [
        ("sync-old-input", "ABORTED", "superseded by different input digest"),
        ("sync-new-input", "STAGING", None),
    ]
    assert replacement.id != active_generation_id


async def test_stage_aborts_when_preflight_wal_cannot_be_reclaimed(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    writer = _CatalogTestWriter(
        database_path,
        checkpoint_wal_bytes=(16 * 1024 * 1024,),
    )

    with pytest.raises(CatalogSyncAborted, match="preflight"):
        await CatalogGenerationCoordinator(writer).stage(
            _generation_input(
                generation="sync-blocked",
                event_count=1,
                input_digest="digest-blocked",
            )
        )

    assert writer.transaction_count == 0
    with _connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_generations WHERE status = 'STAGING'"
        ).fetchone()[0] == 0


async def test_stage_commits_oversized_payload_alone_and_preserves_dependencies(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    generation = "sync-dependencies"
    event = Event(
        id="event-1",
        title="E" * 512,
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
        question="Question?",
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
        outcome="Yes",
        position=0,
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    writer = _CatalogTestWriter(database_path)

    staged = await CatalogGenerationCoordinator(
        writer,
        batch_limits=CatalogBatchLimits(max_rows=100, max_payload_bytes=128),
    ).stage(
        GenerationInput(
            sync_generation=generation,
            updated_at=10,
            events=(event,),
            markets=(market,),
            tokens=(token,),
            input_digest="digest-dependencies",
        )
    )

    assert writer.written_totals == [0, 1, 2, 3]
    with _connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM event_versions WHERE generation_id = ?",
            (staged.id,),
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT event_id FROM catalog_market_ids WHERE id = 'market-1'"
        ).fetchone() == ("event-1",)
        assert connection.execute(
            "SELECT market_id FROM catalog_token_ids WHERE id = 'token-1'"
        ).fetchone() == ("market-1",)


async def test_stage_shrinks_the_next_batch_when_wal_projection_is_unsafe(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    writer = _CatalogTestWriter(
        database_path,
        checkpoint_wal_bytes=(0, 1_900, 0, 0, 0),
    )
    policy = WalPolicy(
        preflight_bytes=100,
        passive_bytes=1_000,
        pause_bytes=2_000,
        abort_bytes=2_800,
        reserve_bytes=1_000,
        hard_limit_bytes=3_800,
    )

    await CatalogGenerationCoordinator(
        writer,
        batch_limits=CatalogBatchLimits(max_rows=4, max_payload_bytes=1_000_000),
        wal_policy=policy,
        initial_wal_amplification=0.1,
    ).stage(
        _generation_input(
            generation="sync-shrink",
            event_count=6,
            input_digest="digest-shrink",
        )
    )

    assert writer.written_totals == [0, 4, 5, 6]


async def test_stage_aborts_generation_when_post_batch_wal_reaches_abort_line(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    abort_bytes = 96 * 1024 * 1024
    writer = _CatalogTestWriter(
        database_path,
        checkpoint_wal_bytes=(0, abort_bytes),
    )

    with pytest.raises(CatalogSyncAborted, match="abort waterline"):
        await CatalogGenerationCoordinator(
            writer,
            batch_limits=CatalogBatchLimits(max_rows=1),
        ).stage(
            _generation_input(
                generation="sync-abort",
                event_count=2,
                input_digest="digest-abort",
            )
        )

    with _connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT status, written_event_count
            FROM catalog_generations WHERE sync_generation = 'sync-abort'
            """
        ).fetchone()
        active_generation_id = connection.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        ).fetchone()[0]
    assert row == ("ABORTED", 1)
    assert active_generation_id == 1


def _related_generation_input(
    *,
    generation: str,
    event_slug: str | None = "event-slug",
    market_slug: str | None = "market-slug",
) -> GenerationInput:
    event = Event(
        id="event-1",
        slug=event_slug,
        title="Event",
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
        slug=market_slug,
        question="Question?",
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
        input_digest=f"digest-{generation}",
    )


def _pending_reconciliation(generation: str) -> PendingCatalogReconciliation:
    return PendingCatalogReconciliation(
        change=MarketChange(
            change_id=f"{generation}:CATALOG_RECONCILED:catalog",
            change_type=MarketChangeType.CATALOG_RECONCILED,
            event_id=None,
            market_id=None,
            token_ids=(),
            occurred_at=10,
            critical=True,
        ),
        market_ids=("market-1",),
    )


async def test_validate_candidate_freezes_effective_snapshot(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_CatalogTestWriter(database_path))
    staged = await coordinator.stage(
        _related_generation_input(generation="sync-valid")
    )

    validation = await coordinator.validate_candidate(staged)

    assert validation == CandidateValidation(
        generation_id=staged.id,
        event_count=1,
        market_count=1,
        token_count=1,
        snapshot_digest=validation.snapshot_digest,
        validated_runtime_revision=0,
    )
    assert len(validation.snapshot_digest) == 64
    with _connect(database_path) as connection:
        row = connection.execute(
            """
            SELECT candidate_event_count, candidate_market_count,
                   candidate_token_count, candidate_snapshot_digest, validated_at
            FROM catalog_generations WHERE id = ?
            """,
            (staged.id,),
        ).fetchone()
    assert row[:4] == (1, 1, 1, validation.snapshot_digest)
    assert row[4] is not None


@pytest.mark.parametrize(
    ("violation", "message"),
    (
        ("event_slug", "event slug"),
        ("market_slug", "market slug"),
        ("condition", "condition"),
        ("position", "position"),
        ("outcome", "outcome"),
        ("missing_parent", "parent"),
    ),
)
async def test_validate_candidate_aborts_catalog_constraint_violations(
    tmp_path: Path,
    violation: str,
    message: str,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    generation = "sync-invalid"
    value = _related_generation_input(generation=generation)
    event = value.events[0]
    market = value.markets[0]
    token = value.tokens[0]
    events = value.events
    markets = value.markets
    tokens = value.tokens
    if violation == "event_slug":
        events += (replace(event, id="event-2", market_ids=()),)
    elif violation == "market_slug":
        markets += (
            replace(
                market,
                id="market-2",
                condition_id="condition-2",
            ),
        )
    elif violation == "condition":
        markets += (replace(market, id="market-2", slug="market-2"),)
    elif violation == "position":
        tokens += (replace(token, id="token-2", outcome="NO"),)
    elif violation == "outcome":
        tokens += (replace(token, id="token-2", position=1),)
    else:
        with _connect(database_path) as connection:
            connection.execute(
                "INSERT INTO catalog_event_ids (id) VALUES ('missing-event')"
            )
        markets = (replace(market, event_id="missing-event"),)
    value = replace(
        value,
        events=events,
        markets=markets,
        tokens=tokens,
        input_digest=f"digest-{violation}",
    )
    coordinator = CatalogGenerationCoordinator(_CatalogTestWriter(database_path))
    staged = await coordinator.stage(value)

    with pytest.raises(CatalogConstraintError, match=message):
        await coordinator.validate_candidate(staged)

    with _connect(database_path) as connection:
        row = connection.execute(
            "SELECT status, failure_reason FROM catalog_generations WHERE id = ?",
            (staged.id,),
        ).fetchone()
        active_generation_id = connection.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        ).fetchone()[0]
    assert row[0] == "ABORTED"
    assert message in row[1]
    assert active_generation_id == 1


async def test_validate_candidate_aborts_count_or_digest_mismatch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_CatalogTestWriter(database_path))
    staged = await coordinator.stage(
        _related_generation_input(generation="sync-corrupt")
    )
    with _connect(database_path) as connection:
        connection.execute(
            """
            UPDATE catalog_generations
            SET written_market_count = written_market_count + 1
            WHERE id = ?
            """,
            (staged.id,),
        )

    with pytest.raises(CatalogConstraintError, match="count or digest"):
        await coordinator.validate_candidate(staged)

    with _connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM catalog_generations WHERE id = ?",
            (staged.id,),
        ).fetchone() == ("ABORTED",)


async def test_activate_rolls_back_pointer_status_and_outbox_then_retries(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_CatalogTestWriter(database_path))
    staged = await coordinator.stage(
        _related_generation_input(generation="sync-activate")
    )
    validation = await coordinator.validate_candidate(staged)
    with _connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER fail_catalog_ready
            BEFORE INSERT ON system_events
            WHEN NEW.event_type = 'CATALOG_RECONCILIATION_READY'
            BEGIN
                SELECT RAISE(ABORT, 'injected outbox failure');
            END
            """
        )
    reconciliation = _pending_reconciliation("sync-activate")

    with pytest.raises(sqlite3.IntegrityError, match="injected outbox failure"):
        await coordinator.activate(validation, reconciliation)

    with _connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM catalog_generations WHERE id = ?",
            (staged.id,),
        ).fetchone() == ("STAGING",)
        assert connection.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM system_events"
        ).fetchone() == (0,)
        connection.execute("DROP TRIGGER fail_catalog_ready")

    await coordinator.activate(validation, reconciliation)

    with _connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM catalog_generations WHERE id = ?",
            (staged.id,),
        ).fetchone() == ("COMMITTED",)
        assert connection.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        ).fetchone() == (staged.id,)
        assert connection.execute(
            """
            SELECT COUNT(*) FROM system_events
            WHERE event_type = 'CATALOG_RECONCILIATION_READY'
            """
        ).fetchone() == (1,)


async def test_activate_rejects_runtime_revision_change(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_CatalogTestWriter(database_path))
    staged = await coordinator.stage(
        _related_generation_input(generation="sync-conflict")
    )
    validation = await coordinator.validate_candidate(staged)
    with _connect(database_path) as connection:
        connection.execute(
            "UPDATE catalog_state SET runtime_revision = 1 WHERE id = 1"
        )

    with pytest.raises(CatalogActivationConflict, match="runtime revision"):
        await coordinator.activate(validation, None)

    with _connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM catalog_generations WHERE id = ?",
            (staged.id,),
        ).fetchone() == ("STAGING",)
        assert connection.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        ).fetchone() == (1,)
