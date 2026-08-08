from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from predmarket.persistence.schema import GenerationStatus, create_v4_database


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
