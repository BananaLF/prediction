from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from predmarket.persistence.migration import migrate_database
from predmarket.persistence.schema import SCHEMA_V1


def _create_v1_database(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            "BEGIN IMMEDIATE;\n"
            + SCHEMA_V1
            + "\nPRAGMA user_version = 1;\nCOMMIT;\n"
        )
        connection.execute(
            """
            INSERT INTO events (
                id, title, status, neg_risk, neg_risk_complete,
                neg_risk_conversion_supported, market_ids_json,
                sync_generation, sync_generation_complete, created_at, updated_at
            ) VALUES ('event-1', 'Event', 'ACTIVE', 0, 0, 0, '[]', 'sync-1', 1, 1, 1)
            """
        )
        connection.execute(
            """
            INSERT INTO markets (
                id, event_id, condition_id, question, status, active,
                accepting_orders, enable_orderbook, neg_risk,
                neg_risk_member_complete, sync_generation,
                sync_generation_complete, created_at, updated_at
            ) VALUES ('market-1', 'event-1', 'condition-1', 'Question?',
                      'ACTIVE', 1, 1, 1, 0, 0, 'sync-1', 1, 1, 1)
            """
        )


def test_migrate_v1_to_v2_keeps_backup_and_allows_orphan_markets(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    backup_path = tmp_path / "market-v1-backup.db"
    _create_v1_database(database_path)

    migrate_database(database_path, backup_path, target_version=2)

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute(
            "SELECT event_id FROM markets WHERE id = 'market-1'"
        ).fetchone() == ("event-1",)
        connection.execute(
            """
            INSERT INTO markets (
                id, event_id, condition_id, question, status, active,
                accepting_orders, enable_orderbook, neg_risk,
                neg_risk_member_complete, sync_generation,
                sync_generation_complete, created_at, updated_at
            ) VALUES ('market-2', NULL, 'condition-2', 'Orphan?',
                      'ACTIVE', 1, 1, 1, 0, 0, 'sync-1', 1, 1, 1)
            """
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    with sqlite3.connect(backup_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("PRAGMA table_info(markets)").fetchall()[1][3] == 1


def test_migrate_rejects_non_v1_without_mutating_database(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    backup_path = tmp_path / "market-v1-backup.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO unrelated VALUES (1)")
        connection.execute("PRAGMA user_version = 7")
    original_bytes = database_path.read_bytes()

    with pytest.raises(ValueError, match="schema version 7"):
        migrate_database(database_path, backup_path, target_version=2)

    assert database_path.read_bytes() == original_bytes
    assert not backup_path.exists()
