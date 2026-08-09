from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from predmarket.persistence import integrity
from predmarket.persistence.integrity import (
    DatabaseIntegrityError,
    check_database_integrity,
    check_database_startup,
    run_database_doctor,
)
from predmarket.persistence.schema import initialize_database


def _insert_event(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    market_ids_json: str = "[]",
) -> None:
    connection.execute(
        "INSERT INTO catalog_event_ids (id) VALUES (?)",
        (event_id,),
    )
    connection.execute(
        """
        INSERT INTO event_versions (
            entity_id, generation_id, title, status, neg_risk,
            neg_risk_complete, neg_risk_conversion_supported,
            market_ids_json, created_at, updated_at
        ) VALUES (?, 1, 'Event', 'ACTIVE', 0, 0, 0, ?, 1, 1)
        """,
        (event_id, market_ids_json),
    )


def _insert_market(
    connection: sqlite3.Connection,
    *,
    market_id: str,
    condition_id: str,
    event_id: str | None,
) -> None:
    connection.execute(
        "INSERT INTO catalog_market_ids (id, event_id) VALUES (?, ?)",
        (market_id, event_id),
    )
    connection.execute(
        """
        INSERT INTO market_versions (
            entity_id, generation_id, condition_id, question, status,
            active, accepting_orders, enable_orderbook, neg_risk,
            neg_risk_member_complete, tick_size, minimum_order_size,
            created_at, updated_at
        ) VALUES (?, 1, ?, 'Question?', 'ACTIVE', 1, 1, 1, 0, 0,
                  '0.01', '1', 1, 1)
        """,
        (market_id, condition_id),
    )


def _insert_token(
    connection: sqlite3.Connection,
    *,
    token_id: str,
    market_id: str,
    position: int,
    fee_schedule_json: str | None = None,
) -> None:
    connection.execute(
        "INSERT INTO catalog_token_ids (id, market_id) VALUES (?, ?)",
        (token_id, market_id),
    )
    connection.execute(
        """
        INSERT INTO token_versions (
            entity_id, generation_id, outcome, position, fee_schedule_json,
            fee_updated_at, created_at, updated_at
        ) VALUES (?, 1, ?, ?, ?, ?, 1, 1)
        """,
        (
            token_id,
            "YES" if position == 0 else "NO",
            position,
            fee_schedule_json,
            1 if fee_schedule_json is not None else None,
        ),
    )


def _seed_valid_database(path: Path) -> None:
    initialize_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        _insert_event(
            connection,
            event_id="event-1",
            market_ids_json='["market-1","market-2"]',
        )
        for market_id, condition_id in (
            ("market-1", "condition-1"),
            ("market-2", "condition-2"),
        ):
            _insert_market(
                connection,
                market_id=market_id,
                condition_id=condition_id,
                event_id="event-1",
            )
        _insert_token(
            connection,
            token_id="token-1",
            market_id="market-1",
            position=0,
            fee_schedule_json=(
                '{"enabled":true,"model":"FLAT","parameters":'
                '{"rate":"0.01"},"source":"sdk","updated_at":1}'
            ),
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


def test_integrity_accepts_a_valid_schema_v4_database(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)

    check_database_integrity(database_path)


def test_startup_check_skips_full_semantic_scans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)

    for name in (
        "_check_catalog_generations",
        "_check_id_arrays",
        "_check_json_payloads",
        "_check_decimals",
        "_check_latest_revisions",
        "_check_revision_payloads",
    ):
        monkeypatch.setattr(
            integrity,
            name,
            lambda *_args, _name=name: pytest.fail(
                f"startup check ran semantic scan {_name}"
            ),
        )

    check_database_startup(database_path)


def test_doctor_reports_a_healthy_database(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)

    report = run_database_doctor(database_path)
    payload = report.to_payload()

    assert report.exit_code == 0
    assert payload["status"] == "ok"
    assert payload["summary"] == {"errors": 0, "warnings": 0}
    assert payload["findings"] == []


def test_doctor_reports_v4_generation_and_cleanup_anomalies(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        committed = connection.execute(
            """
            INSERT INTO catalog_generations (
                sync_generation, input_digest, base_generation_id,
                base_runtime_revision, status, created_at, activated_at
            ) VALUES ('committed-2', 'digest-2', 1, 0, 'COMMITTED', 2, 2)
            """
        ).lastrowid
        assert committed is not None
        connection.execute(
            """
            INSERT INTO event_versions (
                entity_id, generation_id, title, status, neg_risk,
                neg_risk_complete, neg_risk_conversion_supported,
                market_ids_json, created_at, updated_at
            ) VALUES ('event-1', ?, 'New event', 'ACTIVE', 0, 0, 0,
                      '["market-1","market-2"]', 1, 2)
            """,
            (committed,),
        )
        staging = connection.execute(
            """
            INSERT INTO catalog_generations (
                sync_generation, input_digest, base_generation_id,
                base_runtime_revision, planned_event_count,
                written_event_count, candidate_event_count,
                status, created_at
            ) VALUES ('staging-3', 'digest-3', ?, 1, 0, 1, 1,
                      'STAGING', 3)
            """,
            (committed,),
        ).lastrowid
        assert staging is not None
        connection.execute(
            "UPDATE catalog_state SET active_generation_id = ?, runtime_revision = 1 "
            "WHERE id = 1",
            (committed,),
        )
        connection.execute(
            """
            INSERT INTO catalog_runtime_changes (
                runtime_revision, entity_type, entity_id, generation_id,
                updated_at, changed_at
            ) VALUES (1, 'EVENT', 'event-1', ?, 2, 2)
            """,
            (committed,),
        )

    marker = database_path.with_name(f".{database_path.name}.v4-migration.json")
    marker.write_text('{"stage":"BUILDING"}')

    payload = run_database_doctor(database_path).to_payload()
    findings = {finding["code"]: finding for finding in payload["findings"]}

    assert {
        "CATALOG_CANDIDATE_INVALID",
        "CATALOG_CLEANUP_BACKLOG",
        "CATALOG_JOURNAL_BACKLOG",
        "MIGRATION_MARKER_INCOMPLETE",
    }.issubset(findings)
    cleanup = findings["CATALOG_CLEANUP_BACKLOG"]["records"][0]
    assert cleanup["logical_reclaimable_versions"] >= 1
    assert cleanup["page_count"] >= 1
    assert cleanup["freelist_count"] >= 0


def test_doctor_reports_an_invalid_active_generation_pointer(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        "UPDATE catalog_state SET active_generation_id = 999 WHERE id = 1",
        foreign_keys=False,
    )

    payload = run_database_doctor(database_path).to_payload()

    assert "CATALOG_ACTIVE_GENERATION_INVALID" in {
        finding["code"] for finding in payload["findings"]
    }


def test_doctor_reports_multiple_staging_generations(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP INDEX catalog_generations_one_staging_idx")
        connection.executemany(
            """
            INSERT INTO catalog_generations (
                sync_generation, input_digest, base_generation_id,
                base_runtime_revision, status, created_at
            ) VALUES (?, ?, 1, 0, 'STAGING', ?)
            """,
            (("staging-2", "digest-2", 2), ("staging-3", "digest-3", 3)),
        )

    payload = run_database_doctor(database_path).to_payload()

    assert "CATALOG_MULTIPLE_STAGING" in {
        finding["code"] for finding in payload["findings"]
    }


def test_doctor_orders_findings_and_json_safe_records() -> None:
    report = integrity.DatabaseDoctorReport(
        database=Path("market.db"),
        status="issues",
        findings=(
            integrity._IntegrityFinding(
                code="JSON_PAYLOAD_INVALID",
                category="json_payloads",
                severity="error",
                records=({"id": "b"}, {"id": b"a"}),
            ),
            integrity._IntegrityFinding(
                code="EVENT_MARKETS_MISMATCH",
                category="id_arrays",
                severity="error",
                records=({"id": "z"},),
            ),
        ),
    )

    payload = report.to_payload()

    assert [finding["code"] for finding in payload["findings"]] == [
        "EVENT_MARKETS_MISMATCH",
        "JSON_PAYLOAD_INVALID",
    ]
    assert payload["findings"][1]["records"] == [
        {"id": "b"},
        {"id": {"type": "bytes", "hex": "61"}},
    ]
    json.dumps(payload)


def test_doctor_reports_structural_findings_after_version_mismatch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA user_version = 1")
        connection.execute("DROP VIEW events")

    payload = run_database_doctor(database_path).to_payload()

    assert {finding["code"] for finding in payload["findings"]} >= {
        "SCHEMA_VERSION_MISMATCH",
        "SCHEMA_INVALID",
    }


def test_doctor_reports_affected_records_for_semantic_findings(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        "UPDATE event_versions SET market_ids_json = '[\"market-1\"]' "
        "WHERE entity_id = 'event-1' AND generation_id = 1",
    )

    report = run_database_doctor(database_path)
    finding = next(
        item
        for item in report.to_payload()["findings"]
        if item["code"] == "EVENT_MARKETS_MISMATCH"
    )

    assert report.exit_code == 1
    assert finding["category"] == "id_arrays"
    assert finding["severity"] == "error"
    assert finding["records"] == [
        {"field": "market_ids_json", "id": "event-1", "table": "events"}
    ]


def test_doctor_returns_unavailable_for_a_missing_database(tmp_path: Path) -> None:
    report = run_database_doctor(tmp_path / "missing.sqlite3")
    payload = report.to_payload()

    assert report.exit_code == 2
    assert payload["status"] == "unavailable"
    assert payload["error"]["code"] == "DATABASE_UNAVAILABLE"


def test_integrity_reports_stable_error_for_incomplete_schema_v4(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
        connection.execute("PRAGMA user_version = 4")

    _assert_violation(database_path, "SCHEMA_INVALID")


@pytest.mark.parametrize("table", ["events", "arbitrage_signals"])
@pytest.mark.parametrize(
    "invalid_json",
    [
        '"market-1"',
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
        (
            "UPDATE event_versions SET market_ids_json = ? "
            "WHERE entity_id = ? AND generation_id = 1"
            if table == "events"
            else f"UPDATE {table} SET market_ids_json = ? WHERE id = ?"
        ),
        (invalid_json, "event-1" if table == "events" else "signal-1"),
        ignore_checks=True,
    )

    _assert_violation(
        database_path,
        "EVENT_MARKET_IDS_INVALID"
        if table == "events"
        else "SIGNAL_MARKET_IDS_INVALID",
    )


def test_integrity_accepts_an_event_without_markets(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        _insert_event(connection, event_id="event-empty")

    check_database_integrity(database_path)


def test_integrity_rejects_event_market_dual_write_mismatch(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        "UPDATE event_versions SET market_ids_json = '[\"market-1\"]' "
        "WHERE entity_id = 'event-1' AND generation_id = 1",
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
            "UPDATE market_versions SET tick_size = '0.010' "
            "WHERE entity_id = 'market-1' AND generation_id = 1",
            "DECIMAL_INVALID",
        ),
        (
            "UPDATE signal_revisions SET quantity = '2.0' WHERE signal_id = 'signal-1'",
            "DECIMAL_INVALID",
        ),
        (
            "UPDATE token_versions SET fee_schedule_json = "
            "'{\"enabled\":true,\"model\":\"FLAT\",\"parameters\":{\"rate\":\"1e-2\"},"
            "\"source\":\"sdk\",\"updated_at\":1}' "
            "WHERE entity_id = 'token-1' AND generation_id = 1",
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
    _corrupt(database_path, sql, ignore_checks=True)

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


def test_integrity_accepts_risk_formula_at_persisted_precision(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE signal_revisions
        SET total_capital = '3',
            worst_case_loss = '1',
            risk_rate = '0.3333333333333333333333333333333333333333'
        WHERE signal_id = 'signal-1'
        """,
    )

    check_database_integrity(database_path)


def test_integrity_rejects_risk_formula_mismatch_at_persisted_precision(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        """
        UPDATE signal_revisions
        SET total_capital = '3',
            worst_case_loss = '1',
            risk_rate = '0.3333333333333333333333333333333333333334'
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
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        _insert_token(
            connection,
            token_id="token-2",
            market_id="market-1",
            position=1,
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
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        _insert_token(
            connection,
            token_id="token-2",
            market_id="market-1",
            position=1,
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
