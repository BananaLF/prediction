from __future__ import annotations

import fcntl
from pathlib import Path
import sqlite3

import pytest

import predmarket.persistence.catalog_migration as catalog_migration
from predmarket.persistence.catalog_migration import (
    MigrationStage,
    migrate_v3_to_v4,
    recover_v4_switch,
)
from predmarket.persistence.schema import SCHEMA_V3


TABLES = (
    "events",
    "markets",
    "tokens",
    "relations",
    "arbitrage_signals",
    "signal_revisions",
    "signal_legs",
    "orderbook_snapshots",
    "orderbook_levels",
    "system_events",
)


def _create_v3(path: Path, *, seeded: bool = False) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            "BEGIN IMMEDIATE;\n"
            + SCHEMA_V3
            + "\nPRAGMA user_version = 3;\nCOMMIT;\n"
        )
        if not seeded:
            return
        connection.executescript(
            """
            INSERT INTO events (
                id, title, status, neg_risk, neg_risk_complete,
                neg_risk_conversion_supported, market_ids_json,
                sync_generation, sync_generation_complete, created_at, updated_at
            ) VALUES ('event-1', 'Event', 'ACTIVE', 0, 0, 0,
                      '["market-1"]', 'sync-1', 1, 1, 2);
            INSERT INTO markets (
                id, event_id, condition_id, question, status, active,
                accepting_orders, enable_orderbook, neg_risk,
                neg_risk_member_complete, sync_generation,
                sync_generation_complete, created_at, updated_at
            ) VALUES
                ('market-1', 'event-1', 'condition-1', 'Question 1?', 'ACTIVE',
                 1, 1, 1, 0, 0, 'sync-1', 1, 1, 2),
                ('market-2', NULL, 'condition-2', 'Question 2?', 'ACTIVE',
                 1, 1, 1, 0, 0, 'sync-1', 1, 1, 1);
            INSERT INTO tokens (
                id, market_id, outcome, position, sync_generation,
                sync_generation_complete, created_at, updated_at
            ) VALUES
                ('token-1', 'market-1', 'YES', 0, 'sync-1', 1, 1, 2),
                ('token-2', 'market-2', 'NO', 0, 'sync-1', 1, 1, 1);
            INSERT INTO relations (
                id, market_a_id, market_b_id, status, discovery_source,
                created_at, updated_at
            ) VALUES ('relation-1', 'market-1', 'market-2', 'APPROVED', 'MANUAL', 2, 2);
            INSERT INTO arbitrage_signals (
                id, opportunity_key, strategy_type, market_ids_json, relation_id,
                execution_mode, status, opened_at, updated_at, latest_revision
            ) VALUES ('signal-1', 'logical-1', 'LOGICAL_IMPLICATION',
                      '["market-1","market-2"]', 'relation-1',
                      'HOLD_TO_RESOLUTION', 'OPEN', 2, 2, 1);
            INSERT INTO signal_revisions (
                signal_id, revision, event_type, observed_at, quantity,
                total_capital, expected_profit, return_rate, worst_case_loss,
                risk_rate, unhedged_notional, risk_flags_json, calculation_json
            ) VALUES ('signal-1', 1, 'OPENED', 2, '1', '0.4', '0.1', '0.25',
                      '0', '0', '0', '[]', '{"source":"test"}');
            INSERT INTO signal_legs (
                signal_id, revision, position, market_id, token_id, action,
                side, quantity, average_price, worst_price, gross_amount, fee_amount
            ) VALUES ('signal-1', 1, 0, 'market-1', 'token-1', 'BUY', 'BUY',
                      '1', '0.4', '0.4', '0.4', '0');
            INSERT INTO orderbook_snapshots (
                id, signal_id, revision, market_id, token_id,
                subscription_generation, book_hash, exchange_timestamp,
                received_timestamp, tick_size, minimum_order_size
            ) VALUES ('snapshot-1', 'signal-1', 1, 'market-1', 'token-1',
                      1, 'hash', 2, 2, '0.01', '1');
            INSERT INTO orderbook_levels (
                snapshot_id, side, position, price, size
            ) VALUES ('snapshot-1', 'ASK', 0, '0.4', '1');
            INSERT INTO system_events (
                component, severity, event_type, message, details_json, occurred_at
            ) VALUES ('DATABASE', 'INFO', 'SEEDED', 'Seeded', '{"ok":true}', 2);
            """
        )


def _snapshot(path: Path) -> dict[str, list[tuple[object, ...]]]:
    with sqlite3.connect(path) as connection:
        snapshots: dict[str, list[tuple[object, ...]]] = {}
        for table in TABLES:
            columns = [
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            ]
            order = ", ".join(f'CAST("{column}" AS BLOB)' for column in columns)
            snapshots[table] = connection.execute(
                f'SELECT * FROM "{table}" ORDER BY {order}'
            ).fetchall()
        return snapshots


def _version(path: Path) -> int:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])


def test_migrate_v3_to_v4_preserves_catalog_and_all_downstream_rows(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    _create_v3(database_path, seeded=True)
    expected = _snapshot(database_path)

    result = migrate_v3_to_v4(database_path)

    assert result.database == database_path
    assert result.source_version == 3
    assert result.target_version == 4
    assert result.backup.exists()
    assert list(tmp_path.glob("catalog.*.pre-v4.sqlite3")) == [result.backup]
    assert _version(result.backup) == 3
    assert _version(database_path) == 4
    assert _snapshot(database_path) == expected
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute(
            """
            SELECT relations.id, markets.question
            FROM relations JOIN markets ON markets.id = relations.market_a_id
            """
        ).fetchall() == [("relation-1", "Question 1?")]


@pytest.mark.parametrize(
    "stage",
    [
        MigrationStage.BUILDING,
        MigrationStage.VALIDATED,
        MigrationStage.V3_BACKED_UP,
        MigrationStage.V4_INSTALLED,
    ],
)
def test_recovery_resolves_every_persisted_switch_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: MigrationStage,
) -> None:
    database_path = tmp_path / f"catalog-{stage.value}.sqlite3"
    _create_v3(database_path)

    def crash(current: MigrationStage) -> None:
        if current == stage:
            raise RuntimeError(f"crash at {stage.value}")

    monkeypatch.setattr(catalog_migration, "_migration_stage_hook", crash)
    with pytest.raises(RuntimeError, match=f"crash at {stage.value}"):
        migrate_v3_to_v4(database_path)

    monkeypatch.setattr(catalog_migration, "_migration_stage_hook", lambda _: None)
    recover_v4_switch(database_path)

    assert database_path.exists()
    assert _version(database_path) in {3, 4}
    if stage is not MigrationStage.BUILDING:
        assert _version(database_path) == 4
        assert len(list(tmp_path.glob(f"catalog-{stage.value}.*.pre-v4.sqlite3"))) == 1


def test_preflight_and_existing_marker_fail_without_mutating_v3(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    _create_v3(database_path)
    original = database_path.read_bytes()

    monkeypatch.setattr(catalog_migration, "_available_bytes", lambda _: 0)
    with pytest.raises(OSError, match="free space"):
        migrate_v3_to_v4(database_path)
    assert database_path.read_bytes() == original

    monkeypatch.undo()
    marker = catalog_migration.migration_marker_path(database_path)
    marker.write_text('{"stage":"BUILDING"}')
    with pytest.raises(RuntimeError, match="migration marker"):
        migrate_v3_to_v4(database_path)
    assert database_path.read_bytes() == original


def test_process_lock_failure_does_not_mutate_v3(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    _create_v3(database_path)
    original = database_path.read_bytes()
    lock_path = catalog_migration.migration_lock_path(database_path)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="migration lock"):
            migrate_v3_to_v4(database_path)
    assert database_path.read_bytes() == original


def test_non_v3_input_is_rejected_without_mutation(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO unrelated VALUES (1)")
        connection.execute("PRAGMA user_version = 4")
    original = database_path.read_bytes()

    with pytest.raises(ValueError, match="schema version 4"):
        migrate_v3_to_v4(database_path)

    assert database_path.read_bytes() == original


def test_domain_invalid_catalog_is_rejected_before_v4_is_installed(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalog.sqlite3"
    _create_v3(database_path, seeded=True)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE events SET market_ids_json = '[1]' WHERE id = 'event-1'"
        )
    original = database_path.read_bytes()

    with pytest.raises(ValueError, match="market_ids"):
        migrate_v3_to_v4(database_path)

    assert database_path.read_bytes() == original
    assert list(tmp_path.glob("catalog.*.pre-v4.sqlite3")) == []
