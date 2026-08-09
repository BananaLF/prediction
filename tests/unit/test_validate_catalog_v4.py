from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable

import pytest

from predmarket.persistence.schema import create_v4_database
from scripts import validate_catalog_v4 as module


class FakeClock:
    def __init__(self, value: str) -> None:
        self.value = datetime.fromisoformat(value)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        from datetime import timedelta

        self.value += timedelta(seconds=seconds)


REPO_ROOT = Path(__file__).resolve().parents[2]


def make_v4_database(path: Path, *, generation: str) -> Path:
    create_v4_database(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE catalog_generations SET sync_generation = ? WHERE id = 1",
            (generation,),
        )
        connection.commit()
    return path


def run_single(database: Path, *, wal_bytes: int = 0) -> dict[str, Any]:
    return module.run_probe(
        module.ProbeOptions(database, 0, 30, None),
        clock=FakeClock("2026-08-09T00:00:00+00:00"),
        sleep=lambda _: pytest.fail("single sample must not sleep"),
        wal_reader=lambda _: wal_bytes,
    )


def failure_codes(report: dict[str, Any]) -> set[str]:
    return {
        check["code"]
        for check in report["checks"]
        if check["status"] == "fail"
    }


def codes(sample: dict[str, Any]) -> set[str]:
    return {check["code"] for check in sample["checks"]}


def set_user_version(version: int) -> Callable[[Path], None]:
    def mutate(database: Path) -> None:
        with sqlite3.connect(database) as connection:
            connection.execute(f"PRAGMA user_version = {version}")
            connection.commit()

    return mutate


def break_active_generation() -> Callable[[Path], None]:
    def mutate(database: Path) -> None:
        with sqlite3.connect(database) as connection:
            connection.execute(
                "UPDATE catalog_generations SET status = 'ABORTED' WHERE id = 1"
            )
            connection.commit()

    return mutate


def leave_migration_marker_incomplete() -> Callable[[Path], None]:
    def mutate(database: Path) -> None:
        marker = database.with_name(f".{database.name}.v4-migration.json")
        marker.write_text(json.dumps({"stage": "COPYING"}), encoding="utf-8")

    return mutate


def _seed_catalog_rows(connection: sqlite3.Connection) -> None:
    timestamp = 1_786_233_600
    connection.execute(
        "INSERT INTO catalog_event_ids (id, created_at) VALUES ('event-1', ?)",
        (timestamp,),
    )
    connection.execute(
        "INSERT INTO catalog_market_ids (id, event_id, created_at) VALUES ('market-1', 'event-1', ?)",
        (timestamp,),
    )
    connection.execute(
        "INSERT INTO catalog_token_ids (id, market_id, created_at) VALUES ('token-1', 'market-1', ?)",
        (timestamp,),
    )
    connection.execute(
        """
        INSERT INTO event_versions (
            entity_id, generation_id, title, status, neg_risk,
            neg_risk_complete, neg_risk_conversion_supported,
            market_ids_json, created_at, updated_at
        ) VALUES ('event-1', 1, 'Event', 'ACTIVE', 0, 0, 0,
                  '[\"market-1\"]', ?, ?)
        """,
        (timestamp, timestamp),
    )
    connection.execute(
        """
        INSERT INTO market_versions (
            entity_id, generation_id, condition_id, question, status,
            active, accepting_orders, enable_orderbook, neg_risk,
            neg_risk_member_complete, tick_size, minimum_order_size,
            created_at, updated_at
        ) VALUES ('market-1', 1, 'condition-1', 'Question?', 'ACTIVE',
                  1, 1, 1, 0, 0, '0.01', '1', ?, ?)
        """,
        (timestamp, timestamp),
    )
    connection.execute(
        """
        INSERT INTO token_versions (
            entity_id, generation_id, outcome, position, fee_schedule_json,
            created_at, updated_at
        ) VALUES ('token-1', 1, 'YES', 0,
                  '{"model":"ZERO","enabled":false,"source":"fixture","parameters":{}}',
                  ?, ?)
        """,
        (timestamp, timestamp),
    )


def _seed_signal(connection: sqlite3.Connection, *, with_orderbook: bool) -> None:
    timestamp = 1_786_233_600
    connection.execute(
        """
        INSERT INTO arbitrage_signals (
            id, opportunity_key, strategy_type, market_ids_json,
            execution_mode, status, opened_at, updated_at, latest_revision
        ) VALUES ('signal-1', 'opportunity-1', 'BINARY_UNDERPRICED',
                  '[\"market-1\"]', 'IMMEDIATE_CONVERSION', 'OPEN', ?, ?, 1)
        """,
        (timestamp, timestamp),
    )
    connection.execute(
        """
        INSERT INTO signal_revisions (
            signal_id, revision, event_type, observed_at, quantity,
            total_capital, expected_profit, return_rate, worst_case_loss,
            risk_rate, unhedged_notional, risk_flags_json, calculation_json
        ) VALUES ('signal-1', 1, 'OPENED', ?, '1', '0.5', '0.1', '0.2',
                  '0', '0', '0', '[]', '{}')
        """,
        (timestamp,),
    )
    connection.execute(
        """
        INSERT INTO signal_legs (
            signal_id, revision, position, market_id, token_id, action, side,
            quantity, average_price, worst_price, gross_amount, fee_amount
        ) VALUES ('signal-1', 1, 0, 'market-1', 'token-1', 'BUY', 'BUY',
                  '1', '0.4', '0.45', '0.4', '0.01')
        """
    )
    connection.execute(
        """
        INSERT INTO signal_legs (
            signal_id, revision, position, market_id, action, quantity,
            gross_amount, fee_amount
        ) VALUES ('signal-1', 1, 1, 'market-1', 'MERGE', '1', '0.5', '0')
        """
    )
    if with_orderbook:
        connection.execute(
            """
            INSERT INTO orderbook_snapshots (
                id, signal_id, revision, market_id, token_id,
                subscription_generation, book_hash, exchange_timestamp,
                received_timestamp, tick_size, minimum_order_size
            ) VALUES ('snapshot-1', 'signal-1', 1, 'market-1', 'token-1',
                      1, 'book-hash', ?, ?, '0.01', '1')
            """,
            (timestamp, timestamp),
        )
        connection.execute(
            """
            INSERT INTO orderbook_levels
                (snapshot_id, side, position, price, size)
            VALUES ('snapshot-1', 'BID', 0, '0.4', '10'),
                   ('snapshot-1', 'ASK', 0, '0.45', '10')
            """
        )
    connection.execute(
        """
        INSERT INTO system_events (
            id, component, severity, event_type, message, details_json, occurred_at
        ) VALUES (1, 'STRATEGY', 'INFO', 'SIGNAL_OPENED', 'signal opened',
                  '{\"signal_id\": \"signal-1\"}', ?)
        """,
        (timestamp,),
    )


def seed_signal_revision_legs_orderbook_and_event(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        _seed_catalog_rows(connection)
        _seed_signal(connection, with_orderbook=True)
        connection.commit()


def seed_signal_without_orderbook(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        _seed_catalog_rows(connection)
        _seed_signal(connection, with_orderbook=False)
        connection.commit()


def test_parse_args_requires_database_and_rejects_non_positive_interval() -> None:
    with pytest.raises(module.ProbeUsageError):
        module.parse_args(["--interval-seconds", "0"])


def test_cli_help_documents_exit_codes(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        module.parse_args(["--help"])

    assert error.value.code == 0
    assert "exit codes" in capsys.readouterr().out.lower()


def test_open_read_only_does_not_create_missing_database(tmp_path: Path) -> None:
    database = tmp_path / "missing.sqlite3"
    with pytest.raises(module.ProbeUnavailableError):
        module.open_read_only(database)
    assert not database.exists()


def test_open_read_only_resolves_symlink_before_reading_wal(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "target.sqlite3", generation="g1")
    symlink = tmp_path / "catalog.sqlite3"
    symlink.symlink_to(database)
    wal_path = Path(f"{database}-wal")
    wal_path.write_bytes(b"wal")

    assert module._read_wal_size(symlink) == 3


def test_output_cannot_overwrite_database_artifacts(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "catalog.sqlite3", generation="g1")
    marker = database.with_name(f".{database.name}.v4-migration.json")
    marker.write_text('{"stage":"COMPLETE"}\n', encoding="utf-8")
    output_paths = [
        database,
        Path(f"{database}-wal"),
        Path(f"{database}-shm"),
        marker,
    ]

    for output_path in output_paths:
        if not output_path.exists():
            output_path.write_bytes(b"sentinel")
        before = output_path.read_bytes()
        assert module.main(
            [
                "--database",
                str(database),
                "--duration-seconds",
                "0",
                "--output",
                str(output_path),
            ]
        ) == 2
        assert output_path.read_bytes() == before


def test_sample_queries_run_in_one_read_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = make_v4_database(tmp_path / "snapshot.sqlite3", generation="g1")
    transaction_states: list[bool] = []
    original = module.collect_catalog_sample

    def wrapped(*args: Any, **kwargs: Any) -> dict[str, Any]:
        transaction_states.append(args[0].in_transaction)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "collect_catalog_sample", wrapped)
    run_single(database)

    assert transaction_states == [True]


def test_duration_zero_produces_one_sample_without_output_side_effect(tmp_path: Path) -> None:
    database = tmp_path / "catalog.sqlite3"
    create_v4_database(database)
    keeper = sqlite3.connect(database)
    keeper.execute("PRAGMA journal_mode = WAL")
    before = {
        path.name: path.read_bytes()
        for path in tmp_path.glob("catalog.sqlite3-*")
    }

    report = module.run_probe(
        module.ProbeOptions(database, 0, 30, None),
        clock=FakeClock("2026-08-09T00:00:00+00:00"),
        sleep=lambda _: pytest.fail("single sample must not sleep"),
        wal_reader=lambda _: 0,
    )

    assert len(report["samples"]) == 1
    after = {
        path.name: path.read_bytes()
        for path in tmp_path.glob("catalog.sqlite3-*")
    }
    keeper.close()
    assert after == before


def test_open_read_only_does_not_create_sidecars_for_checkpointed_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "checkpointed.sqlite3"
    create_v4_database(database)
    before = {
        path.name: path.read_bytes()
        for path in tmp_path.iterdir()
        if path.is_file()
    }

    run_single(database)

    after = {
        path.name: path.read_bytes()
        for path in tmp_path.iterdir()
        if path.is_file()
    }
    assert after == before


def test_healthy_v4_sample_reports_schema_integrity_generation_and_wal(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "healthy.sqlite3", generation="g1")
    with sqlite3.connect(database) as connection:
        _seed_catalog_rows(connection)
        connection.commit()
    sample = run_single(database, wal_bytes=4096)["samples"][0]

    assert codes(sample) >= {
        "SCHEMA_VERSION",
        "SQLITE_INTEGRITY_CHECK",
        "FOREIGN_KEY_CHECK",
        "CATALOG_ACTIVE_GENERATION",
        "WAL_SIZE",
    }
    assert all(check["status"] == "pass" for check in sample["checks"])


def test_empty_catalog_views_fail_generation_validation(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "empty.sqlite3", generation="g1")

    report = run_single(database)

    assert "CATALOG_GENERATION_EMPTY" in failure_codes(report)
    assert report["exit_code"] == 1


def test_missing_schema_objects_are_reported_without_query_error(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "missing-table.sqlite3", generation="g1")
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TABLE system_events")
        connection.commit()

    report = run_single(database)

    assert "SCHEMA_OBJECT_MISSING" in failure_codes(report)
    assert report["exit_code"] == 1


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (set_user_version(3), "SCHEMA_VERSION_MISMATCH"),
        (break_active_generation(), "CATALOG_ACTIVE_GENERATION_INVALID"),
        (leave_migration_marker_incomplete(), "MIGRATION_MARKER_INCOMPLETE"),
    ],
)
def test_invalid_v4_fixtures_have_stable_failure_codes(
    tmp_path: Path, mutate: Callable[[Path], None], expected_code: str
) -> None:
    database = make_v4_database(tmp_path / "invalid.sqlite3", generation="g1")
    mutate(database)

    report = run_single(database)

    assert expected_code in failure_codes(report)
    assert report["exit_code"] == 1


def test_wal_limit_is_reported_without_checkpoint_or_mutation(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "wal.sqlite3", generation="g1")
    before = database.read_bytes()

    report = run_single(database, wal_bytes=128 * 1024 * 1024 + 1)

    assert "WAL_LIMIT_EXCEEDED" in failure_codes(report)
    assert report["exit_code"] == 1
    assert database.read_bytes() == before


def test_signal_report_links_revision_legs_orderbook_and_system_event(
    tmp_path: Path,
) -> None:
    database = make_v4_database(tmp_path / "evidence.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)

    report = run_single(database)
    signal = report["samples"][0]["signals"][0]

    assert signal["signal_id"] == "signal-1"
    assert signal["latest_revision"] == 1
    assert {leg["leg_id"] for leg in signal["legs"]} == {
        "signal-1:1:0",
        "signal-1:1:1",
    }
    assert signal["orderbook"]["snapshot_ids"] == ["snapshot-1"]
    assert signal["evidence_class"] == "orderbook_checked_estimate"
    assert report["samples"][0]["runtime"]["system_events"][0]["event_id"] == 1


def test_missing_orderbook_metadata_is_not_reported_as_executable(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "missing-book.sqlite3", generation="g1")
    seed_signal_without_orderbook(database)

    report = run_single(database)
    signal = report["samples"][0]["signals"][0]

    assert signal["evidence_class"] == "theoretical_estimate"
    assert signal["orderbook"]["status"] == "not_observed"
    assert "ORDERBOOK_METADATA_UNAVAILABLE" in failure_codes(report)


def test_orderbook_depth_failure_downgrades_estimate(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "shallow-book.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE orderbook_levels SET size = '0.5' WHERE snapshot_id = 'snapshot-1' AND side = 'ASK'"
        )
        connection.commit()

    report = run_single(database)
    signal = report["samples"][0]["signals"][0]

    assert signal["evidence_class"] == "theoretical_estimate"
    assert signal["orderbook"]["executable"]["status"] == "fail"
    assert "ORDERBOOK_EXECUTION_CHECK_FAILED" in failure_codes(report)


def test_realized_is_always_unsupported_without_fill_source(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "realized.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)

    report = run_single(database)

    assert report["aggregates"]["returns"]["realized"] == "unsupported"


def test_fixed_clock_generates_deterministic_window_and_wal_peak(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "window.sqlite3", generation="g1")
    clock = FakeClock("2026-08-09T00:00:00+00:00")
    wal_values = iter([100, 300, 200])

    report = module.run_probe(
        module.ProbeOptions(database, 60, 30, None),
        clock=clock,
        sleep=clock.advance,
        wal_reader=lambda _: next(wal_values),
    )

    assert len(report["samples"]) == 3
    assert report["aggregates"]["wal"]["peak_bytes"] == 300
    assert report["duration_seconds"] == 60


def test_window_evidence_is_not_counted_again_at_each_sample(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "window-evidence.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)
    clock = FakeClock("2026-08-09T00:00:00+00:00")

    report = module.run_probe(
        module.ProbeOptions(database, 60, 30, None),
        clock=clock,
        sleep=clock.advance,
        wal_reader=lambda _: 0,
    )

    assert [len(sample["runtime"]["system_events"]) for sample in report["samples"]] == [1, 0, 0]
    assert [sample["runtime"]["signal_revision_count"] for sample in report["samples"]] == [1, 0, 0]
    assert report["aggregates"]["runtime"]["system_event_count"] == 1


def test_runtime_event_counts_include_records_beyond_report_limit(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "many-events.sqlite3", generation="g1")
    timestamp = 1_786_233_600
    with sqlite3.connect(database) as connection:
        connection.executemany(
            """
            INSERT INTO system_events (
                component, severity, event_type, message, details_json, occurred_at
            ) VALUES ('SYNC', 'WARNING', 'RETRY', 'retry', NULL, ?)
            """,
            [(timestamp,) for _ in range(module.MAX_RECORDS + 1)],
        )
        connection.commit()

    report = run_single(database)
    runtime = report["samples"][0]["runtime"]

    assert len(runtime["system_events"]) == module.MAX_RECORDS
    assert runtime["event_counts"] == [
        {
            "component": "SYNC",
            "severity": "WARNING",
            "event_type": "RETRY",
            "count": module.MAX_RECORDS + 1,
        }
    ]
    assert runtime["alerts_total"] == module.MAX_RECORDS + 1
    assert runtime["alerts_truncated"] is True
    assert report["aggregates"]["runtime"]["system_event_count"] == module.MAX_RECORDS + 1


def test_signal_evidence_uses_revision_and_orderbook_inside_window(
    tmp_path: Path,
) -> None:
    database = make_v4_database(tmp_path / "revision-window.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO signal_revisions (
                signal_id, revision, event_type, observed_at, quantity,
                total_capital, expected_profit, return_rate, worst_case_loss,
                risk_rate, unhedged_notional, risk_flags_json, calculation_json
            ) VALUES ('signal-1', 2, 'UPDATED', ?, '1', '0.5', '0.2', '0.4',
                      '0', '0', '0', '[]', '{}')
            """,
            (1_786_233_600 + 120,),
        )
        connection.execute(
            "UPDATE arbitrage_signals SET latest_revision = 2 WHERE id = 'signal-1'"
        )
        connection.commit()

    signal = run_single(database)["samples"][0]["signals"][0]

    assert signal["latest_revision"] == 1
    assert signal["revision"]["observed_at"] == 1_786_233_600
    assert signal["orderbook"]["snapshot_ids"] == ["snapshot-1"]


def test_report_has_stable_top_level_keys_and_bounded_summaries(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "report.sqlite3", generation="g1")
    with sqlite3.connect(database) as connection:
        _seed_catalog_rows(connection)
        connection.commit()
    report_path = tmp_path / "reports" / "catalog.json"
    report_path.parent.mkdir()

    assert module.main(
        [
            "--database",
            str(database),
            "--duration-seconds",
            "0",
            "--output",
            str(report_path),
        ]
    ) == 0
    report = json.loads(report_path.read_text())

    assert list(report) == [
        "schema_version",
        "tool_version",
        "database",
        "started_at",
        "ended_at",
        "duration_seconds",
        "status",
        "exit_code",
        "checks",
        "samples",
        "aggregates",
        "limitations",
    ]
    assert "realized" in report["aggregates"]["returns"]


@pytest.mark.parametrize(
    "path",
    ["README.md", "docs/OPERATIONS.md", "docs/VERIFICATION.md", "docs/PROJECT-GUIDE.md"],
)
def test_documentation_describes_v4_validation_contract(path: str) -> None:
    text = (REPO_ROOT / path).read_text()
    assert "migrate --to 4 --database" in text
    assert "validate_catalog_v4.py" in text
    assert "128 MiB" in text or "128 * 1024 * 1024" in text
    assert "realized" in text and "unsupported" in text
    assert "不自动" in text or "不触发" in text or "not trigger" in text
