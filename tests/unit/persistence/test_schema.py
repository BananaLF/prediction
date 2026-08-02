from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import predmarket.persistence.schema as schema_module
from predmarket.domain.signal import DecisionReason
from predmarket.persistence.schema import initialize_database


PROJECT_TABLES = {
    "arbitrage_signals",
    "events",
    "markets",
    "orderbook_levels",
    "orderbook_snapshots",
    "relations",
    "signal_legs",
    "signal_revisions",
    "system_events",
    "tokens",
}


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _insert_catalog(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO events (
            id, title, status, neg_risk, neg_risk_complete,
            neg_risk_conversion_supported, market_ids_json, sync_generation,
            sync_generation_complete, created_at, updated_at
        ) VALUES ('event-1', 'Event', 'ACTIVE', 0, 0, 0, '["market-1","market-2"]',
                  'sync-1', 1, 1, 1)
        """
    )
    for market_id, condition_id in (
        ("market-1", "condition-1"),
        ("market-2", "condition-2"),
    ):
        connection.execute(
            """
            INSERT INTO markets (
                id, event_id, condition_id, question, status, active,
                accepting_orders, enable_orderbook, neg_risk,
                neg_risk_member_complete, sync_generation,
                sync_generation_complete, created_at, updated_at
            ) VALUES (?, 'event-1', ?, 'Question?', 'ACTIVE', 1, 1, 1, 0, 0,
                      'sync-1', 1, 1, 1)
            """,
            (market_id, condition_id),
        )
    connection.execute(
        """
        INSERT INTO tokens (
            id, market_id, outcome, position, sync_generation,
            sync_generation_complete, created_at, updated_at
        ) VALUES ('token-1', 'market-1', 'YES', 0, 'sync-1', 1, 1, 1)
        """
    )


def _insert_signal(
    connection: sqlite3.Connection,
    *,
    signal_id: str = "signal-1",
    opportunity_key: str = "opportunity-1",
    status: str = "OPEN",
) -> None:
    closed_at = None if status == "OPEN" else 2
    close_reason = None if status == "OPEN" else "MARKET_CLOSED"
    connection.execute(
        """
        INSERT INTO arbitrage_signals (
            id, opportunity_key, strategy_type, market_ids_json, relation_id,
            execution_mode, status, opened_at, updated_at, closed_at,
            close_reason, latest_revision
        ) VALUES (?, ?, 'BINARY_UNDERPRICED', '["market-1"]', NULL,
                  'IMMEDIATE_CONVERSION', ?, 1, 1, ?, ?, 1)
        """,
        (signal_id, opportunity_key, status, closed_at, close_reason),
    )


def _insert_revision(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    economic: bool,
    closure_context: bool,
) -> None:
    values = (
        "2",
        "1.6",
        "0.4",
        "0.25",
        "0.8",
        "0.5",
        "0.6",
    ) if economic else (None,) * 7
    connection.execute(
        """
        INSERT INTO signal_revisions (
            signal_id, revision, event_type, observed_at, quantity,
            total_capital, expected_profit, return_rate, worst_case_loss,
            risk_rate, unhedged_notional, risk_flags_json, calculation_json,
            closure_context_json
        ) VALUES ('signal-1', 1, ?, 1, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?)
        """,
        (
            event_type,
            *values,
            '{"source":"test"}' if economic else None,
            '{"reason_code":"MARKET_CLOSED"}' if closure_context else None,
        ),
    )


def test_initialize_database_creates_exact_schema_v2_and_wal(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"

    initialize_database(database_path)

    with _connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        assert tables == PROJECT_TABLES
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)


def test_schema_accepts_every_decision_reason_as_a_close_reason(tmp_path: Path) -> None:
    # Catches domain/schema enum drift before SignalManager persists a closure.
    database_path = tmp_path / "market.db"
    initialize_database(database_path)

    with _connect(database_path) as connection:
        _insert_catalog(connection)
        for index, reason in enumerate(DecisionReason):
            signal_id = f"signal-{index}"
            _insert_signal(
                connection,
                signal_id=signal_id,
                opportunity_key=f"opportunity-{index}",
            )
            connection.execute(
                """
                UPDATE arbitrage_signals
                SET status = 'CLOSED', closed_at = 2, close_reason = ?
                WHERE id = ?
                """,
                (reason.value, signal_id),
            )
        persisted = {
            row[0]
            for row in connection.execute(
                "SELECT close_reason FROM arbitrage_signals ORDER BY id"
            )
        }
        assert persisted == {reason.value for reason in DecisionReason}


def test_initialize_database_rejects_nonempty_unknown_schema_without_mutation(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "legacy.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE legacy_v7 (payload TEXT NOT NULL)")
        connection.execute("INSERT INTO legacy_v7 VALUES ('keep-me')")
        connection.execute("PRAGMA user_version = 7")
    original_bytes = database_path.read_bytes()

    with pytest.raises(ValueError, match="schema version 7"):
        initialize_database(database_path)

    assert database_path.read_bytes() == original_bytes


def test_initialize_database_accepts_an_existing_schema_v2(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)

    initialize_database(database_path)

    with _connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)


def test_initialize_database_rolls_back_a_partially_failing_schema_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "market.db"
    monkeypatch.setattr(
        schema_module,
        "SCHEMA_V2",
        """
        CREATE TABLE partial_table (id INTEGER PRIMARY KEY);
        CREATE TABLE broken_table (;
        """,
    )

    with pytest.raises(sqlite3.DatabaseError):
        initialize_database(database_path)

    with sqlite3.connect(database_path) as connection:
        tables = connection.execute(
            """
            SELECT name FROM sqlite_schema
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
        assert tables == []


def test_relation_and_signal_strategy_constraints_are_enforced(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    with _connect(database_path) as connection:
        _insert_catalog(connection)
        connection.execute(
            """
            INSERT INTO relations (
                id, market_a_id, market_b_id, status, discovery_source,
                created_at, updated_at
            ) VALUES ('relation-1', 'market-1', 'market-2', 'APPROVED', 'MANUAL', 1, 1)
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO relations (
                    id, market_a_id, market_b_id, status, discovery_source,
                    created_at, updated_at
                ) VALUES ('bad', 'market-1', 'market-1', 'APPROVED', 'RULE', 1, 1)
                """
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO arbitrage_signals (
                    id, opportunity_key, strategy_type, market_ids_json,
                    relation_id, execution_mode, status, opened_at, updated_at,
                    latest_revision
                ) VALUES ('bad-signal', 'bad', 'LOGICAL_IMPLICATION',
                          '["market-1","market-2"]', NULL,
                          'IMMEDIATE_CONVERSION', 'OPEN', 1, 1, 1)
                """
            )
        connection.execute(
            """
            INSERT INTO arbitrage_signals (
                id, opportunity_key, strategy_type, market_ids_json,
                relation_id, execution_mode, status, opened_at, updated_at,
                latest_revision
            ) VALUES ('logical', 'logical-opportunity', 'LOGICAL_IMPLICATION',
                      '["market-1","market-2"]', 'relation-1',
                      'HOLD_TO_RESOLUTION', 'OPEN', 1, 1, 1)
            """
        )


def test_signal_status_and_partial_open_identity_constraints_are_enforced(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    with _connect(database_path) as connection:
        _insert_catalog(connection)
        _insert_signal(connection)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_signal(
                connection,
                signal_id="signal-2",
                opportunity_key="opportunity-1",
            )
        connection.execute(
            """
            UPDATE arbitrage_signals
            SET status = 'CLOSED', closed_at = 2, close_reason = 'MARKET_CLOSED'
            WHERE id = 'signal-1'
            """
        )
        _insert_signal(
            connection,
            signal_id="signal-2",
            opportunity_key="opportunity-1",
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE arbitrage_signals SET closed_at = 2 WHERE id = 'signal-2'"
            )


@pytest.mark.parametrize(
    ("event_type", "economic", "closure_context", "valid"),
    [
        ("OPENED", True, False, True),
        ("UPDATED", True, False, True),
        ("OPENED", False, False, False),
        ("CLOSED", True, False, True),
        ("CLOSED", False, True, True),
        ("CLOSED", False, False, False),
        ("CLOSED", True, True, False),
    ],
)
def test_revision_payload_shape_constraints(
    tmp_path: Path,
    event_type: str,
    economic: bool,
    closure_context: bool,
    valid: bool,
) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    with _connect(database_path) as connection:
        _insert_catalog(connection)
        _insert_signal(connection)
        if valid:
            _insert_revision(
                connection,
                event_type=event_type,
                economic=economic,
                closure_context=closure_context,
            )
        else:
            with pytest.raises(sqlite3.IntegrityError):
                _insert_revision(
                    connection,
                    event_type=event_type,
                    economic=economic,
                    closure_context=closure_context,
                )


def test_trade_and_snapshot_token_foreign_keys_include_market_id(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    with _connect(database_path) as connection:
        _insert_catalog(connection)
        _insert_signal(connection)
        _insert_revision(
            connection,
            event_type="OPENED",
            economic=True,
            closure_context=False,
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO signal_legs (
                    signal_id, revision, position, market_id, token_id, action,
                    side, quantity, average_price, worst_price, gross_amount,
                    fee_amount
                ) VALUES ('signal-1', 1, 0, 'market-2', 'token-1', 'BUY',
                          'BUY', '1', '0.4', '0.4', '0.4', '0')
                """
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO orderbook_snapshots (
                    id, signal_id, revision, market_id, token_id,
                    subscription_generation, book_hash, exchange_timestamp,
                    received_timestamp, tick_size, minimum_order_size
                ) VALUES ('snapshot-1', 'signal-1', 1, 'market-2', 'token-1',
                          1, 'hash', 1, 1, '0.01', '1')
                """
            )
