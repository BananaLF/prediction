from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from predmarket.persistence.integrity import (
    DatabaseIntegrityError,
    check_database_integrity,
)
from predmarket.persistence.schema import initialize_database


def _seed_valid_database(path: Path) -> None:
    initialize_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO events (
                id, title, status, neg_risk, neg_risk_complete,
                neg_risk_conversion_supported, market_ids_json,
                sync_generation, sync_generation_complete, created_at, updated_at
            ) VALUES ('event-1', 'Event', 'ACTIVE', 0, 0, 0,
                      '["market-1","market-2"]', 'sync-1', 1, 1, 1)
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
                    sync_generation_complete, tick_size, minimum_order_size,
                    created_at, updated_at
                ) VALUES (?, 'event-1', ?, 'Question?', 'ACTIVE', 1, 1, 1, 0, 0,
                          'sync-1', 1, '0.01', '1', 1, 1)
                """,
                (market_id, condition_id),
            )
        connection.execute(
            """
            INSERT INTO tokens (
                id, market_id, outcome, position, fee_schedule_json,
                fee_updated_at, sync_generation, sync_generation_complete,
                created_at, updated_at
            ) VALUES ('token-1', 'market-1', 'YES', 0,
                      '{"enabled":true,"model":"FLAT","parameters":{"rate":"0.01"},"source":"sdk","updated_at":1}',
                      1, 'sync-1', 1, 1, 1)
            """
        )
        connection.execute(
            """
            INSERT INTO relations (
                id, market_a_id, market_b_id, status, discovery_source,
                llm_confidence, created_at, updated_at
            ) VALUES ('relation-1', 'market-1', 'market-2',
                      'NO_LLM_APPROVE', 'RULE', '0.9', 1, 1)
            """
        )
        connection.execute(
            """
            INSERT INTO arbitrage_signals (
                id, opportunity_key, strategy_type, market_ids_json,
                execution_mode, status, opened_at, updated_at, latest_revision
            ) VALUES ('signal-1', 'opportunity-1', 'BINARY_UNDERPRICED',
                      '["market-1","market-2"]', 'IMMEDIATE_CONVERSION',
                      'OPEN', 1, 1, 1)
            """
        )
        connection.execute(
            """
            INSERT INTO signal_revisions (
                signal_id, revision, event_type, observed_at, quantity,
                total_capital, expected_profit, return_rate, worst_case_loss,
                risk_rate, unhedged_notional, risk_flags_json, calculation_json
            ) VALUES ('signal-1', 1, 'OPENED', 1, '2', '1.6', '0.4', '0.25',
                      '0.8', '0.5', '0.6', '["PARTIAL_FILL"]',
                      '{"source":"test"}')
            """
        )
        connection.execute(
            """
            INSERT INTO signal_legs (
                signal_id, revision, position, market_id, token_id, action,
                side, quantity, average_price, worst_price, gross_amount,
                fee_amount
            ) VALUES ('signal-1', 1, 0, 'market-1', 'token-1', 'BUY', 'BUY',
                      '2', '0.4', '0.4', '0.8', '0')
            """
        )
        connection.execute(
            """
            INSERT INTO orderbook_snapshots (
                id, signal_id, revision, market_id, token_id,
                subscription_generation, book_hash, exchange_timestamp,
                received_timestamp, tick_size, minimum_order_size
            ) VALUES ('snapshot-1', 'signal-1', 1, 'market-1', 'token-1',
                      1, 'hash', 1, 1, '0.01', '1')
            """
        )
        connection.executemany(
            """
            INSERT INTO orderbook_levels (
                snapshot_id, side, position, price, size
            ) VALUES ('snapshot-1', ?, 0, ?, '2')
            """,
            (("BID", "0.4"), ("ASK", "0.6")),
        )


def _corrupt(
    path: Path,
    sql: str,
    parameters: tuple[object, ...] = (),
    *,
    ignore_checks: bool = False,
    foreign_keys: bool = True,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(f"PRAGMA foreign_keys = {int(foreign_keys)}")
        if ignore_checks:
            connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(sql, parameters)


def _assert_violation(path: Path, code: str) -> None:
    with pytest.raises(DatabaseIntegrityError) as captured:
        check_database_integrity(path)
    assert code in captured.value.violations


def test_integrity_accepts_a_valid_schema_v1_database(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)

    check_database_integrity(database_path)


def test_integrity_reports_stable_error_for_incomplete_schema_v1(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 1")

    _assert_violation(database_path, "SCHEMA_INVALID")


@pytest.mark.parametrize("table", ["events", "arbitrage_signals"])
@pytest.mark.parametrize(
    "invalid_json",
    [
        '"market-1"',
        "[]",
        '["market-1",1]',
        '["market-1","market-1"]',
        '["market-2","market-1"]',
    ],
)
def test_integrity_rejects_invalid_id_arrays(
    tmp_path: Path,
    table: str,
    invalid_json: str,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        f"UPDATE {table} SET market_ids_json = ? WHERE id = ?",
        (invalid_json, "event-1" if table == "events" else "signal-1"),
        ignore_checks=True,
    )

    _assert_violation(
        database_path,
        "EVENT_MARKET_IDS_INVALID"
        if table == "events"
        else "SIGNAL_MARKET_IDS_INVALID",
    )


def test_integrity_rejects_event_market_dual_write_mismatch(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        "UPDATE events SET market_ids_json = '[\"market-1\"]' WHERE id = 'event-1'",
    )

    _assert_violation(database_path, "EVENT_MARKETS_MISMATCH")


def test_integrity_rejects_dangling_signal_market_id(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE arbitrage_signals
        SET market_ids_json = '["market-1","missing"]'
        WHERE id = 'signal-1'
        """,
    )

    _assert_violation(database_path, "SIGNAL_MARKET_MISSING")


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        (
            "UPDATE markets SET tick_size = '0.010' WHERE id = 'market-1'",
            "DECIMAL_INVALID",
        ),
        (
            "UPDATE signal_revisions SET quantity = '2.0' WHERE signal_id = 'signal-1'",
            "DECIMAL_INVALID",
        ),
        (
            "UPDATE tokens SET fee_schedule_json = "
            "'{\"enabled\":true,\"model\":\"FLAT\",\"parameters\":{\"rate\":\"1e-2\"},"
            "\"source\":\"sdk\",\"updated_at\":1}' WHERE id = 'token-1'",
            "DECIMAL_INVALID",
        ),
    ],
)
def test_integrity_rejects_noncanonical_decimal(
    tmp_path: Path,
    sql: str,
    code: str,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(database_path, sql)

    _assert_violation(database_path, code)


def test_integrity_rejects_bad_risk_formula(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE signal_revisions
        SET risk_rate = '0.6'
        WHERE signal_id = 'signal-1'
        """,
    )

    _assert_violation(database_path, "RISK_FORMULA_INVALID")


def test_integrity_rejects_stale_latest_revision(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE arbitrage_signals
        SET latest_revision = 2
        WHERE id = 'signal-1'
        """,
    )

    _assert_violation(database_path, "LATEST_REVISION_MISMATCH")


def test_integrity_rejects_cross_market_token_reference(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE signal_legs
        SET market_id = 'market-2'
        WHERE signal_id = 'signal-1'
        """,
        foreign_keys=False,
    )

    _assert_violation(database_path, "FOREIGN_KEY_VIOLATION")


def test_integrity_rejects_evidence_for_a_different_token_than_trade_leg(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        INSERT INTO tokens (
            id, market_id, outcome, position, sync_generation,
            sync_generation_complete, created_at, updated_at
        ) VALUES ('token-2', 'market-1', 'NO', 1, 'sync-1', 1, 1, 1)
        """
    )
    _corrupt(
        database_path,
        """
        UPDATE orderbook_snapshots
        SET token_id = 'token-2'
        WHERE id = 'snapshot-1'
        """
    )

    _assert_violation(database_path, "EVIDENCE_IDENTITY_MISMATCH")


def test_integrity_accepts_duplicate_trade_identity_with_one_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        INSERT INTO signal_legs (
            signal_id, revision, position, market_id, token_id, action,
            side, quantity, average_price, worst_price, gross_amount,
            fee_amount
        ) VALUES ('signal-1', 1, 1, 'market-1', 'token-1', 'BUY', 'BUY',
                  '1', '0.4', '0.4', '0.4', '0')
        """
    )

    check_database_integrity(database_path)


def test_integrity_accepts_multiple_matching_trade_and_snapshot_identities(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        INSERT INTO tokens (
            id, market_id, outcome, position, sync_generation,
            sync_generation_complete, created_at, updated_at
        ) VALUES ('token-2', 'market-1', 'NO', 1, 'sync-1', 1, 1, 1)
        """
    )
    _corrupt(
        database_path,
        """
        INSERT INTO signal_legs (
            signal_id, revision, position, market_id, token_id, action,
            side, quantity, average_price, worst_price, gross_amount,
            fee_amount
        ) VALUES ('signal-1', 1, 1, 'market-1', 'token-2', 'SELL', 'SELL',
                  '1', '0.6', '0.6', '0.6', '0')
        """
    )
    _corrupt(
        database_path,
        """
        INSERT INTO orderbook_snapshots (
            id, signal_id, revision, market_id, token_id,
            subscription_generation, book_hash, exchange_timestamp,
            received_timestamp, tick_size, minimum_order_size
        ) VALUES ('snapshot-2', 'signal-1', 1, 'market-1', 'token-2',
                  1, 'hash-2', 1, 1, '0.01', '1')
        """
    )

    check_database_integrity(database_path)


def test_integrity_rejects_economic_revision_without_legs_or_evidence(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        "DELETE FROM signal_legs WHERE signal_id = 'signal-1'",
    )

    _assert_violation(database_path, "REVISION_PAYLOAD_INVALID")


def test_integrity_rejects_not_evaluable_revision_with_evidence(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE signal_revisions
        SET event_type = 'CLOSED',
            quantity = NULL,
            total_capital = NULL,
            expected_profit = NULL,
            return_rate = NULL,
            worst_case_loss = NULL,
            risk_rate = NULL,
            unhedged_notional = NULL,
            calculation_json = NULL,
            closure_context_json = '{"reason_code":"ORDERBOOK_INVALID"}'
        WHERE signal_id = 'signal-1'
        """,
        ignore_checks=True,
    )

    _assert_violation(database_path, "REVISION_PAYLOAD_INVALID")
