"""Read-only deployment and observation probe for the SQLite catalog v4 database."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any, Callable, Mapping, Sequence


REPORT_SCHEMA_VERSION = 1
TOOL_VERSION = "1.0.0"
SCHEMA_VERSION = 4
WAL_LIMIT_BYTES = 128 * 1024 * 1024
MAX_RECORDS = 100

CATALOG_REQUIRED_TABLES = {
    "arbitrage_signals",
    "catalog_event_ids",
    "catalog_generations",
    "catalog_market_ids",
    "catalog_runtime_changes",
    "catalog_state",
    "catalog_token_ids",
    "event_versions",
    "market_versions",
    "orderbook_levels",
    "orderbook_snapshots",
    "relations",
    "signal_legs",
    "signal_revisions",
    "system_events",
    "token_versions",
}
CATALOG_REQUIRED_VIEWS = {"events", "markets", "tokens"}


class ProbeUsageError(ValueError):
    """The command line arguments do not describe a valid probe run."""


class ProbeUnavailableError(RuntimeError):
    """The target database cannot be opened read-only."""


class ProbeOutputError(RuntimeError):
    """The requested report output path cannot be used."""


@dataclass(frozen=True)
class ProbeOptions:
    database: Path
    duration_seconds: int = 1800
    interval_seconds: int = 30
    output: Path | None = None


def parse_args(argv: Sequence[str]) -> ProbeOptions:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Exit codes: 0 means the validation passed, 1 means the report "
            "contains failed checks, and 2 means the probe could not run."
        ),
    )
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--duration-seconds", default=1800, type=int)
    parser.add_argument("--interval-seconds", default=30, type=int)
    parser.add_argument("--output", type=Path)
    try:
        args = parser.parse_args(list(argv))
    except SystemExit as error:
        if error.code == 0:
            raise
        raise ProbeUsageError("invalid command line arguments") from error
    if args.duration_seconds < 0:
        raise ProbeUsageError("--duration-seconds must be non-negative")
    if args.interval_seconds <= 0:
        raise ProbeUsageError("--interval-seconds must be positive")
    return ProbeOptions(
        database=args.database,
        duration_seconds=args.duration_seconds,
        interval_seconds=args.interval_seconds,
        output=args.output,
    )


def _resolved_database_path(database: Path) -> Path:
    try:
        return Path(database).expanduser().resolve(strict=False)
    except OSError as error:
        raise ProbeUnavailableError(f"cannot resolve database: {error}") from error


def open_read_only(database: Path) -> sqlite3.Connection:
    database_path = _resolved_database_path(database)
    if not database_path.is_file():
        raise ProbeUnavailableError(f"database does not exist: {database_path}")
    try:
        wal_path = database_path.with_name(database_path.name + "-wal")
        shm_path = database_path.with_name(database_path.name + "-shm")
        immutable = not wal_path.exists() and not shm_path.exists()
        query = "mode=ro" + ("&immutable=1" if immutable else "")
        connection = sqlite3.connect(f"file:{database_path}?{query}", uri=True)
        if immutable and (wal_path.exists() or shm_path.exists()):
            connection.close()
            connection = sqlite3.connect(
                f"file:{database_path}?mode=ro",
                uri=True,
            )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection
    except (OSError, sqlite3.DatabaseError) as error:
        if "connection" in locals():
            connection.close()
        raise ProbeUnavailableError(str(error)) from error


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read_wal_size(database: Path) -> int:
    database_path = _resolved_database_path(database)
    wal_path = database_path.with_name(database_path.name + "-wal")
    try:
        return wal_path.stat().st_size if wal_path.exists() else 0
    except OSError as error:
        raise ProbeUnavailableError(f"cannot inspect WAL: {error}") from error


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _epoch(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp())


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def _check(
    code: str,
    status: str,
    observed: Any,
    expected: Any,
    records: Sequence[Mapping[str, Any]] = (),
    *,
    severity: str = "error",
) -> dict[str, Any]:
    return {
        "code": code,
        "status": status,
        "severity": severity,
        "observed": _json_value(observed),
        "expected": _json_value(expected),
        "records": [_json_value(record) for record in records][:MAX_RECORDS],
    }


def _pass_or_fail(
    healthy_code: str,
    failure_code: str,
    ok: bool,
    observed: Any,
    expected: Any,
    records: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return _check(
        healthy_code if ok else failure_code,
        "pass" if ok else "fail",
        observed,
        expected,
        records,
    )


def _table_objects(connection: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        "table": {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table'"
            )
        },
        "view": {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'view'"
            )
        },
    }


def _catalog_checks(
    connection: sqlite3.Connection,
    database: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    catalog: dict[str, Any] = {}
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    checks.append(
        _pass_or_fail(
            "SCHEMA_VERSION",
            "SCHEMA_VERSION_MISMATCH",
            version == SCHEMA_VERSION,
            version,
            SCHEMA_VERSION,
        )
    )

    integrity_rows = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
    checks.append(
        _pass_or_fail(
            "SQLITE_INTEGRITY_CHECK",
            "SQLITE_INTEGRITY_CHECK_FAILED",
            integrity_rows == ["ok"],
            integrity_rows,
            ["ok"],
        )
    )
    foreign_key_rows = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]
    checks.append(
        _pass_or_fail(
            "FOREIGN_KEY_CHECK",
            "FOREIGN_KEY_VIOLATION",
            not foreign_key_rows,
            foreign_key_rows,
            [],
        )
    )

    objects = _table_objects(connection)
    missing_tables = sorted(CATALOG_REQUIRED_TABLES - objects["table"])
    missing_views = sorted(CATALOG_REQUIRED_VIEWS - objects["view"])
    checks.append(
        _check(
            "SCHEMA_OBJECTS",
            "pass" if not missing_tables and not missing_views else "fail",
            {"missing_tables": missing_tables, "missing_views": missing_views},
            {"missing_tables": [], "missing_views": []},
        )
        if not missing_tables and not missing_views
        else _check(
            "SCHEMA_OBJECT_MISSING",
            "fail",
            {"missing_tables": missing_tables, "missing_views": missing_views},
            {"missing_tables": [], "missing_views": []},
        )
    )
    catalog["schema_objects_available"] = not missing_tables and not missing_views
    if not catalog["schema_objects_available"]:
        return checks, catalog

    state_row = connection.execute(
        "SELECT * FROM catalog_state WHERE id = 1"
    ).fetchone()
    active_row = None
    if state_row is not None:
        active_row = connection.execute(
            "SELECT * FROM catalog_generations WHERE id = ?",
            (state_row["active_generation_id"],),
        ).fetchone()
    active_ok = bool(
        state_row is not None
        and active_row is not None
        and active_row["status"] == "COMMITTED"
    )
    catalog["state"] = dict(state_row) if state_row is not None else None
    catalog["active_generation"] = dict(active_row) if active_row is not None else None
    checks.append(
        _pass_or_fail(
            "CATALOG_ACTIVE_GENERATION",
            "CATALOG_ACTIVE_GENERATION_INVALID",
            active_ok,
            {
                "state": dict(state_row) if state_row is not None else None,
                "generation": dict(active_row) if active_row is not None else None,
            },
            {"state_id": 1, "generation_status": "COMMITTED"},
        )
    )

    staging_rows = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM catalog_generations WHERE status = 'STAGING' ORDER BY id"
        )
    ]
    catalog["staging_generations"] = staging_rows[:MAX_RECORDS]
    checks.append(
        _check(
            "CATALOG_STAGING",
            "pass" if not staging_rows else "fail",
            {"count": len(staging_rows)},
            {"count": 0},
            staging_rows,
        )
        if not staging_rows
        else _check(
            "CATALOG_STAGING_BACKLOG",
            "fail",
            {"count": len(staging_rows)},
            {"count": 0},
            staging_rows,
        )
    )

    generation_values: set[str] = set()
    for view in sorted(CATALOG_REQUIRED_VIEWS):
        if view not in objects["view"]:
            continue
        generation_values.update(
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT sync_generation FROM {view}"
            )
            if row[0] is not None
        )
    catalog["visible_generations"] = sorted(generation_values)
    active_sync_generation = (
        str(active_row["sync_generation"])
        if active_row is not None
        else None
    )
    if not generation_values:
        checks.append(
            _check(
                "CATALOG_GENERATION_EMPTY",
                "fail",
                [],
                {"active_generation": active_sync_generation},
            )
        )
    else:
        checks.append(
            _pass_or_fail(
                "CATALOG_GENERATION_CONSISTENT",
                "CATALOG_GENERATION_INCONSISTENT",
                active_sync_generation is not None
                and generation_values == {active_sync_generation},
                sorted(generation_values),
                {"active_generation": active_sync_generation},
            )
        )

    candidate_records: list[dict[str, Any]] = []
    invalid_candidate_records: list[dict[str, Any]] = []
    for row in staging_rows:
        invalid_fields: list[str] = []
        progress: dict[str, Any] = {}
        for kind in ("event", "market", "token"):
            planned = int(row[f"planned_{kind}_count"])
            written = int(row[f"written_{kind}_count"])
            written_digest = row[f"written_{kind}_digest"]
            cursor = row[f"{kind}_cursor"]
            progress[kind] = {
                "planned": planned,
                "written": written,
                "cursor": cursor,
                "written_digest": written_digest,
            }
            if written > planned:
                invalid_fields.append(f"written_{kind}_count")
            if written == 0 and cursor is not None:
                invalid_fields.append(f"{kind}_cursor")
            if written > 0 and (cursor is None or written_digest is None):
                invalid_fields.append(f"written_{kind}_progress")
            if written == planned and written_digest != row[f"planned_{kind}_digest"]:
                invalid_fields.append(f"written_{kind}_digest")

        candidate_values = (
            row["candidate_event_count"],
            row["candidate_market_count"],
            row["candidate_token_count"],
            row["candidate_snapshot_digest"],
        )
        candidate_present = [value is not None for value in candidate_values]
        if any(candidate_present) != all(candidate_present):
            invalid_fields.append("candidate_fields")
        elif all(candidate_present):
            digest = str(row["candidate_snapshot_digest"])
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                invalid_fields.append("candidate_snapshot_digest")
            if row["validated_at"] is None:
                invalid_fields.append("validated_at")
            for kind, table in (
                ("event", "event_versions"),
                ("market", "market_versions"),
                ("token", "token_versions"),
            ):
                actual = int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*) FROM (
                            SELECT entity_id FROM {table}
                            WHERE generation_id = ?
                            UNION
                            SELECT versions.entity_id
                            FROM {table} AS versions
                            JOIN catalog_generations AS committed
                              ON committed.id = versions.generation_id
                             AND committed.status = 'COMMITTED'
                            WHERE versions.generation_id <= ?
                        )
                        """,
                        (row["id"], row["base_generation_id"]),
                    ).fetchone()[0]
                )
                if actual != row[f"candidate_{kind}_count"]:
                    invalid_fields.append(f"candidate_{kind}_count")

        record = {
            "generation_id": row["id"],
            "planned": {
                "events": row["planned_event_count"],
                "markets": row["planned_market_count"],
                "tokens": row["planned_token_count"],
            },
            "written": {
                "events": row["written_event_count"],
                "markets": row["written_market_count"],
                "tokens": row["written_token_count"],
            },
            "progress": progress,
            "candidate": {
                "events": row["candidate_event_count"],
                "markets": row["candidate_market_count"],
                "tokens": row["candidate_token_count"],
                "snapshot_digest": row["candidate_snapshot_digest"],
            },
            "invalid_fields": sorted(set(invalid_fields)),
        }
        candidate_records.append(record)
        if invalid_fields:
            invalid_candidate_records.append(record)
    checks.append(
        _check(
            "CATALOG_CANDIDATE",
            "pass" if not invalid_candidate_records else "fail",
            {"staging_candidates": candidate_records},
            "staging candidates satisfy doctor validation rules",
            candidate_records,
        )
    )
    if invalid_candidate_records:
        checks[-1]["code"] = "CATALOG_CANDIDATE_INVALID"

    runtime_revision = int(state_row["runtime_revision"]) if state_row else 0
    journal_boundary = int(
        connection.execute(
            f"""
            SELECT COALESCE(MIN(base_runtime_revision), ?)
            FROM catalog_generations WHERE status = 'STAGING'
            """,
            (runtime_revision,),
        ).fetchone()[0]
    )
    journal_rows = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM catalog_runtime_changes WHERE runtime_revision <= ? ORDER BY runtime_revision LIMIT ?",
            (journal_boundary, MAX_RECORDS),
        )
    ]
    catalog["runtime_revision"] = runtime_revision
    catalog["journal_boundary"] = journal_boundary
    catalog["journal_backlog"] = journal_rows
    checks.append(
        _check(
            "CATALOG_JOURNAL",
            "pass" if not journal_rows else "fail",
            {"count": len(journal_rows), "through_revision": journal_boundary},
            {"count": 0},
            journal_rows,
        )
        if not journal_rows
        else _check(
            "CATALOG_JOURNAL_BACKLOG",
            "fail",
            {"count": len(journal_rows), "through_revision": journal_boundary},
            {"count": 0},
            journal_rows,
            severity="warning",
        )
    )

    reclaimable_versions = 0
    if state_row is not None:
        for table in ("event_versions", "market_versions", "token_versions"):
            reclaimable_versions += int(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table} AS versions
                    JOIN catalog_generations AS generation
                      ON generation.id = versions.generation_id
                    WHERE generation.status = 'ABORTED'
                       OR (
                            generation.status = 'COMMITTED'
                            AND versions.generation_id <= ?
                            AND versions.generation_id < (
                                SELECT MAX(candidate.generation_id)
                                FROM {table} AS candidate
                                JOIN catalog_generations AS candidate_generation
                                  ON candidate_generation.id = candidate.generation_id
                                 AND candidate_generation.status = 'COMMITTED'
                                WHERE candidate.entity_id = versions.entity_id
                                  AND candidate.generation_id <= ?
                            )
                       )
                    """,
                    (int(state_row["active_generation_id"]), int(state_row["active_generation_id"])),
                ).fetchone()[0]
            )
    cleanup_pending = reclaimable_versions > 0
    cleanup_observed = {
        "logical_reclaimable_versions": reclaimable_versions,
        "cleanup_generation_id": state_row["cleanup_generation_id"] if state_row else None,
        "cleanup_entity_type": state_row["cleanup_entity_type"] if state_row else None,
        "cleanup_entity_id": state_row["cleanup_entity_id"] if state_row else None,
    }
    if cleanup_pending:
        cleanup_observed["page_count"] = int(
            connection.execute("PRAGMA page_count").fetchone()[0]
        )
        cleanup_observed["freelist_count"] = int(
            connection.execute("PRAGMA freelist_count").fetchone()[0]
        )
    checks.append(
        _check(
            "CATALOG_CLEANUP",
            "pass" if not cleanup_pending else "fail",
            cleanup_observed,
            "no reclaimable old or aborted version rows",
        )
        if not cleanup_pending
        else _check(
            "CATALOG_CLEANUP_BACKLOG",
            "fail",
            cleanup_observed,
            "no reclaimable old or aborted version rows",
            severity="warning",
        )
    )

    database_path = _resolved_database_path(database)
    marker = database_path.with_name(f".{database_path.name}.v4-migration.json")
    marker_payload: Any = None
    marker_ok = True
    if marker.exists():
        try:
            marker_payload = json.loads(marker.read_text(encoding="utf-8"))
            marker_ok = isinstance(marker_payload, dict) and marker_payload.get("stage") == "COMPLETE"
        except (OSError, json.JSONDecodeError):
            marker_ok = False
    catalog["migration_marker"] = marker_payload
    checks.append(
        _check(
            "MIGRATION_MARKER",
            "pass" if marker_ok else "fail",
            marker_payload,
            "absent or stage COMPLETE",
        )
        if marker_ok
        else _check(
            "MIGRATION_MARKER_INCOMPLETE",
            "fail",
            marker_payload,
            "absent or stage COMPLETE",
        )
    )
    return checks, catalog


def _json_payload(value: Any) -> Any:
    if value is None:
        return None
    try:
        return _json_value(json.loads(str(value)))
    except (TypeError, json.JSONDecodeError):
        return None


def _details(value: Any) -> dict[str, Any] | None:
    parsed = _json_payload(value)
    return parsed if isinstance(parsed, dict) else None


def classify_return_evidence(
    signal: Mapping[str, Any],
    orderbook: Mapping[str, Any] | None,
) -> str:
    calculation = signal.get("calculation")
    if isinstance(calculation, Mapping) and (
        calculation.get("evidence_class") == "simulated"
        or calculation.get("simulated") is True
    ):
        return "simulated"
    if orderbook is not None and (
        orderbook.get("status") == "observed"
        and orderbook.get("executable", {}).get("status") in {"pass", "not_applicable"}
    ):
        return "orderbook_checked_estimate"
    return "theoretical_estimate"


def _select_signal_ids(
    connection: sqlite3.Connection,
    start_epoch: int,
    end_epoch: int,
    *,
    include_start: bool,
) -> list[str]:
    start_operator = ">=" if include_start else ">"
    rows = connection.execute(
        f"""
        SELECT id
        FROM arbitrage_signals
        WHERE opened_at {start_operator} ? AND opened_at <= ?
           OR updated_at {start_operator} ? AND updated_at <= ?
           OR closed_at {start_operator} ? AND closed_at <= ?
           OR EXISTS (
                SELECT 1 FROM signal_revisions AS revisions
                WHERE revisions.signal_id = arbitrage_signals.id
                  AND revisions.observed_at {start_operator} ?
                  AND revisions.observed_at <= ?
           )
        ORDER BY opened_at, id
        LIMIT ?
        """,
        (
            start_epoch,
            end_epoch,
            start_epoch,
            end_epoch,
            start_epoch,
            end_epoch,
            start_epoch,
            end_epoch,
            MAX_RECORDS,
        ),
    )
    return [str(row[0]) for row in rows]


def _collect_orderbook(
    connection: sqlite3.Connection,
    signal_id: str,
    revision: int,
    start_epoch: int,
    end_epoch: int,
    *,
    include_start: bool,
    legs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    start_operator = ">=" if include_start else ">"
    rows = [
        dict(row)
        for row in connection.execute(
            f"""
            SELECT orderbook_snapshots.id, orderbook_snapshots.market_id,
                   orderbook_snapshots.token_id,
                   orderbook_snapshots.subscription_generation,
                   orderbook_snapshots.book_hash,
                   orderbook_snapshots.exchange_timestamp,
                   orderbook_snapshots.received_timestamp,
                   orderbook_snapshots.tick_size,
                   orderbook_snapshots.minimum_order_size,
                   tokens.fee_schedule_json
            FROM orderbook_snapshots
            LEFT JOIN tokens
              ON tokens.id = orderbook_snapshots.token_id
             AND tokens.market_id = orderbook_snapshots.market_id
            WHERE orderbook_snapshots.signal_id = ?
              AND orderbook_snapshots.revision = ?
              AND orderbook_snapshots.received_timestamp {start_operator} ?
              AND orderbook_snapshots.received_timestamp <= ?
            ORDER BY orderbook_snapshots.id
            LIMIT ?
            """,
            (signal_id, revision, start_epoch, end_epoch, MAX_RECORDS),
        )
    ]
    snapshots: list[dict[str, Any]] = []
    level_rows_by_snapshot: dict[str, list[dict[str, Any]]] = {}
    valid = bool(rows)
    for row in rows:
        level_rows = [
            dict(level)
            for level in connection.execute(
                """
                SELECT side, position, price, size
                FROM orderbook_levels
                WHERE snapshot_id = ?
                ORDER BY side, position
                """,
                (row["id"],),
            )
        ]
        level_rows_by_snapshot[str(row["id"])] = level_rows
        levels = [
            dict(level)
            for level in connection.execute(
                """
                SELECT side, COUNT(*) AS level_count,
                       COALESCE(SUM(CAST(size AS REAL)), 0) AS total_size
                FROM orderbook_levels
                WHERE snapshot_id = ?
                GROUP BY side
                ORDER BY side
                """,
                (row["id"],),
            )
        ]
        sides = {str(level["side"]): level for level in levels}
        fee_schedule = _details(row["fee_schedule_json"])
        valid = valid and all(row.get(key) is not None for key in (
            "subscription_generation",
            "book_hash",
            "exchange_timestamp",
            "received_timestamp",
            "tick_size",
            "minimum_order_size",
        )) and fee_schedule is not None and {"BID", "ASK"}.issubset(sides)
        snapshots.append(
            {
                "snapshot_id": row["id"],
                "market_id": row["market_id"],
                "token_id": row["token_id"],
                "subscription_generation": row["subscription_generation"],
                "book_hash": row["book_hash"],
                "exchange_timestamp": row["exchange_timestamp"],
                "received_timestamp": row["received_timestamp"],
                "tick_size": row["tick_size"],
                "minimum_order_size": row["minimum_order_size"],
                "fee_schedule": fee_schedule,
                "depth": {
                    side: {
                        "levels": int(value["level_count"]),
                        "total_size": value["total_size"],
                    }
                    for side, value in sides.items()
                },
            }
        )
    if not rows:
        return {"status": "not_observed", "snapshot_ids": [], "snapshots": []}
    executable_checks: list[dict[str, Any]] = []
    executable_legs = [
        leg for leg in legs if leg.get("action") in {"BUY", "SELL"}
    ]
    for leg in executable_legs:
        matching_snapshots = [
            (snapshot, level_rows_by_snapshot[str(snapshot["snapshot_id"])])
            for snapshot in snapshots
            if snapshot["market_id"] == leg.get("market_id")
            and snapshot["token_id"] == leg.get("token_id")
        ]
        if not matching_snapshots:
            executable_checks.append(
                {
                    "leg_id": leg.get("leg_id"),
                    "status": "fail",
                    "reason": "snapshot_missing_for_leg",
                }
            )
            continue
        snapshot, level_rows = max(
            matching_snapshots,
            key=lambda item: int(item[0]["received_timestamp"]),
        )
        side = "ASK" if leg["action"] == "BUY" else "BID"
        side_rows = [row for row in level_rows if row["side"] == side]
        try:
            quantity = Decimal(str(leg["quantity"]))
            minimum_order_size = Decimal(str(snapshot["minimum_order_size"]))
            worst_price = Decimal(str(leg["worst_price"]))
            available = sum(Decimal(str(row["size"])) for row in side_rows)
            ordered_rows = sorted(
                side_rows,
                key=lambda row: Decimal(str(row["price"])),
                reverse=leg["action"] == "SELL",
            )
            remaining = quantity
            price_within_worst = True
            for row in ordered_rows:
                price = Decimal(str(row["price"]))
                if (leg["action"] == "BUY" and price > worst_price) or (
                    leg["action"] == "SELL" and price < worst_price
                ):
                    price_within_worst = False
                    break
                remaining -= min(remaining, Decimal(str(row["size"])))
                if remaining <= 0:
                    break
            reasons: list[str] = []
            if quantity < minimum_order_size:
                reasons.append("below_minimum_order_size")
            if available < quantity:
                reasons.append("insufficient_depth")
            if remaining > 0:
                reasons.append("quantity_not_fillable")
            if not price_within_worst:
                reasons.append("outside_worst_price")
        except (InvalidOperation, TypeError, ValueError):
            reasons = ["invalid_decimal_evidence"]
        executable_checks.append(
            {
                "leg_id": leg.get("leg_id"),
                "status": "pass" if not reasons else "fail",
                "side": side,
                "reasons": reasons,
                "snapshot_id": snapshot["snapshot_id"],
            }
        )
    executable_status = (
        "not_applicable"
        if not executable_legs
        else "pass"
        if all(check["status"] == "pass" for check in executable_checks)
        else "fail"
    )
    executable = {"status": executable_status, "checks": executable_checks}
    return {
        "status": "observed" if valid else "incomplete",
        "snapshot_ids": [row["id"] for row in rows],
        "snapshots": snapshots,
        "executable": executable,
    }


def collect_signal_evidence(
    connection: sqlite3.Connection,
    signal_ids: Sequence[str],
    window_start: datetime,
    observed_at: datetime,
    *,
    include_start: bool = True,
) -> list[dict[str, Any]]:
    start_epoch = _epoch(window_start)
    end_epoch = _epoch(observed_at)
    start_operator = ">=" if include_start else ">"
    signals: list[dict[str, Any]] = []
    for signal_id in signal_ids[:MAX_RECORDS]:
        signal_row = connection.execute(
            """
            SELECT id, opportunity_key, strategy_type, market_ids_json,
                   execution_mode, status, opened_at, updated_at, closed_at,
                   close_reason, latest_revision
            FROM arbitrage_signals WHERE id = ?
            """,
            (signal_id,),
        ).fetchone()
        if signal_row is None:
            continue
        current_revision_number = int(signal_row["latest_revision"])
        revision_row = connection.execute(
            f"""
            SELECT revision, event_type, observed_at, quantity, total_capital,
                   expected_profit, return_rate, worst_case_loss, risk_rate,
                   unhedged_notional, risk_flags_json, calculation_json,
                   closure_context_json
            FROM signal_revisions
            WHERE signal_id = ?
              AND observed_at {start_operator} ?
              AND observed_at <= ?
            ORDER BY revision DESC
            LIMIT 1
            """,
            (signal_id, start_epoch, end_epoch),
        ).fetchone()
        revision_number = (
            int(revision_row["revision"])
            if revision_row is not None
            else current_revision_number
        )
        if revision_row is None:
            revision: dict[str, Any] = {"status": "not_observed", "revision": revision_number}
        else:
            revision = {
                "status": "observed",
                "revision": revision_row["revision"],
                "event_type": revision_row["event_type"],
                "observed_at": revision_row["observed_at"],
                "quantity": revision_row["quantity"],
                "total_capital": revision_row["total_capital"],
                "expected_profit": revision_row["expected_profit"],
                "return_rate": revision_row["return_rate"],
                "worst_case_loss": revision_row["worst_case_loss"],
                "risk_rate": revision_row["risk_rate"],
                "unhedged_notional": revision_row["unhedged_notional"],
                "risk_flags": _json_payload(revision_row["risk_flags_json"]),
                "calculation": _details(revision_row["calculation_json"]),
                "closure_context": _details(revision_row["closure_context_json"]),
            }
        legs = [
            {
                "leg_id": f"{signal_id}:{revision_number}:{row['position']}",
                "position": row["position"],
                "market_id": row["market_id"],
                "token_id": row["token_id"],
                "action": row["action"],
                "side": row["side"],
                "quantity": row["quantity"],
                "average_price": row["average_price"],
                "worst_price": row["worst_price"],
                "gross_amount": row["gross_amount"],
                "fee_amount": row["fee_amount"],
            }
            for row in connection.execute(
                """
                SELECT position, market_id, token_id, action, side, quantity,
                       average_price, worst_price, gross_amount, fee_amount
                FROM signal_legs
                WHERE signal_id = ? AND revision = ?
                ORDER BY position LIMIT ?
                """,
                (signal_id, revision_number, MAX_RECORDS),
            )
        ] if revision_row is not None else []
        orderbook = (
            _collect_orderbook(
                connection,
                signal_id,
                revision_number,
                start_epoch,
                end_epoch,
                include_start=include_start,
                legs=legs,
            )
            if revision_row is not None
            else {"status": "not_observed", "snapshot_ids": [], "snapshots": []}
        )
        signal = {
            "signal_id": signal_row["id"],
            "opportunity_key": signal_row["opportunity_key"],
            "strategy_type": signal_row["strategy_type"],
            "market_ids": _json_payload(signal_row["market_ids_json"]),
            "execution_mode": signal_row["execution_mode"],
            "status": signal_row["status"],
            "opened_at": signal_row["opened_at"],
            "updated_at": signal_row["updated_at"],
            "closed_at": signal_row["closed_at"],
            "close_reason": signal_row["close_reason"],
            "latest_revision": revision_number,
            "revision": revision,
            "legs": legs,
            "orderbook": orderbook,
        }
        signal["evidence_class"] = classify_return_evidence(
            signal.get("revision", {}), orderbook
        )
        signals.append(signal)
    return signals


def collect_runtime_evidence(
    connection: sqlite3.Connection,
    window_start: datetime,
    observed_at: datetime,
    *,
    include_start: bool = True,
) -> dict[str, Any]:
    start_epoch = _epoch(window_start)
    end_epoch = _epoch(observed_at)
    start_operator = ">=" if include_start else ">"
    events = [
        {
            "event_id": row["id"],
            "component": row["component"],
            "severity": row["severity"],
            "event_type": row["event_type"],
            "message": row["message"],
            "details": _details(row["details_json"]),
            "occurred_at": row["occurred_at"],
        }
        for row in connection.execute(
            f"""
            SELECT id, component, severity, event_type, message, details_json, occurred_at
            FROM system_events
            WHERE occurred_at {start_operator} ? AND occurred_at <= ?
            ORDER BY occurred_at, id LIMIT ?
            """,
            (start_epoch, end_epoch, MAX_RECORDS),
        )
    ]
    event_count_rows = connection.execute(
        f"""
        SELECT component, severity, event_type, COUNT(*) AS count
        FROM system_events
        WHERE occurred_at {start_operator} ? AND occurred_at <= ?
        GROUP BY component, severity, event_type
        ORDER BY component, severity, event_type
        """,
        (start_epoch, end_epoch),
    )
    alert_total = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM system_events
            WHERE occurred_at {start_operator} ? AND occurred_at <= ?
              AND severity IN ('WARNING', 'ERROR', 'FATAL')
            """,
            (start_epoch, end_epoch),
        ).fetchone()[0]
    )
    alerts = [
        event for event in events if event["severity"] in {"WARNING", "ERROR", "FATAL"}
    ]
    orderbook_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM orderbook_snapshots
            WHERE received_timestamp {start_operator} ? AND received_timestamp <= ?
            """,
            (start_epoch, end_epoch),
        ).fetchone()[0]
    )
    revision_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM signal_revisions
            WHERE observed_at {start_operator} ? AND observed_at <= ?
            """,
            (start_epoch, end_epoch),
        ).fetchone()[0]
    )
    runtime_change_count = int(
        connection.execute(
            f"""
            SELECT COUNT(*) FROM catalog_runtime_changes
            WHERE changed_at {start_operator} ? AND changed_at <= ?
            """,
            (start_epoch, end_epoch),
        ).fetchone()[0]
    )
    signal_ids = _select_signal_ids(
        connection,
        start_epoch,
        end_epoch,
        include_start=include_start,
    )
    return {
        "window": {"start": _iso(window_start), "end": _iso(observed_at)},
        "system_events": events,
        "event_counts": [
            {
                "component": row["component"],
                "severity": row["severity"],
                "event_type": row["event_type"],
                "count": row["count"],
            }
            for row in event_count_rows
        ],
        "alerts": alerts,
        "alerts_total": alert_total,
        "alerts_truncated": alert_total > len(alerts),
        "signal_revision_count": revision_count,
        "orderbook_snapshot_count": orderbook_count,
        "catalog_runtime_change_count": runtime_change_count,
        "signal_ids": signal_ids,
    }


def collect_catalog_sample(
    connection: sqlite3.Connection,
    database: Path,
    observed_at: datetime,
    wal_bytes: int,
    *,
    window_start: datetime,
    include_start: bool = True,
) -> dict[str, Any]:
    checks, catalog = _catalog_checks(connection, database)
    if catalog.get("schema_objects_available") is False:
        runtime = {
            "window": {"start": _iso(window_start), "end": _iso(observed_at)},
            "system_events": [],
            "event_counts": [],
            "alerts": [],
            "alerts_total": 0,
            "alerts_truncated": False,
            "signal_revision_count": 0,
            "orderbook_snapshot_count": 0,
            "catalog_runtime_change_count": 0,
            "signal_ids": [],
        }
        signals: list[dict[str, Any]] = []
    else:
        runtime = collect_runtime_evidence(
            connection,
            window_start,
            observed_at,
            include_start=include_start,
        )
        signals = collect_signal_evidence(
            connection,
            runtime["signal_ids"],
            window_start,
            observed_at,
            include_start=include_start,
        )
    if signals:
        for signal in signals:
            if signal["orderbook"]["status"] != "observed":
                checks.append(
                    _check(
                        "ORDERBOOK_METADATA_UNAVAILABLE",
                        "fail",
                        {
                            "signal_id": signal["signal_id"],
                            "orderbook_status": signal["orderbook"]["status"],
                        },
                        "observed orderbook metadata and BID/ASK levels",
                    )
                )
            elif signal["orderbook"]["executable"]["status"] == "fail":
                checks.append(
                    _check(
                        "ORDERBOOK_EXECUTION_CHECK_FAILED",
                        "fail",
                        {
                            "signal_id": signal["signal_id"],
                            "checks": signal["orderbook"]["executable"]["checks"],
                        },
                        "all executable legs satisfy local quantity, depth, side, and worst-price checks",
                    )
                )
    checks.append(
        _pass_or_fail(
            "WAL_SIZE",
            "WAL_LIMIT_EXCEEDED",
            wal_bytes <= WAL_LIMIT_BYTES,
            wal_bytes,
            WAL_LIMIT_BYTES,
        )
    )
    return {
        "observed_at": _iso(observed_at),
        "database": {"path": str(database), "wal_bytes": wal_bytes},
        "checks": checks,
        "catalog": catalog,
        "runtime": runtime,
        "signals": signals,
    }


def _aggregate_checks(samples: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_code: dict[str, dict[str, Any]] = {}
    for sample in samples:
        for check in sample.get("checks", []):
            code = str(check["code"])
            current = by_code.get(code)
            if current is None or (
                current["status"] != "fail" and check["status"] == "fail"
            ):
                by_code[code] = dict(check)
    return [by_code[code] for code in sorted(by_code)]


def build_report(
    database: Path,
    started_at: datetime,
    ended_at: datetime,
    samples: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    checks = _aggregate_checks(samples)
    failed = any(check["status"] == "fail" for check in checks)
    wal_values = [
        int(sample["database"]["wal_bytes"])
        for sample in samples
        if "database" in sample and "wal_bytes" in sample["database"]
    ]
    signal_classes = Counter(
        signal.get("evidence_class", "theoretical_estimate")
        for sample in samples
        for signal in sample.get("signals", [])
    )
    runtime_events = sum(
        int(event_count["count"])
        for sample in samples
        for event_count in sample.get("runtime", {}).get("event_counts", [])
    )
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "tool_version": TOOL_VERSION,
        "database": str(database),
        "started_at": _iso(started_at),
        "ended_at": _iso(ended_at),
        "duration_seconds": max(0, int((ended_at - started_at).total_seconds())),
        "status": "failed" if failed else "healthy",
        "exit_code": 1 if failed else 0,
        "checks": checks,
        "samples": list(samples),
        "aggregates": {
            "wal": {
                "current_bytes": wal_values[-1] if wal_values else 0,
                "peak_bytes": max(wal_values, default=0),
                "limit_bytes": WAL_LIMIT_BYTES,
            },
            "runtime": {"system_event_count": runtime_events},
            "signals": {
                "count": sum(len(sample.get("signals", [])) for sample in samples),
                "evidence_classes": dict(sorted(signal_classes.items())),
            },
            "returns": {
                "theoretical_estimate": signal_classes.get("theoretical_estimate", 0),
                "orderbook_checked_estimate": signal_classes.get("orderbook_checked_estimate", 0),
                "simulated": signal_classes.get("simulated", 0),
                "realized": "unsupported",
            },
        },
        "limitations": [
            "The probe is read-only and does not checkpoint, delete, migrate, reset, or control production processes.",
            "Expected profit and return rate are estimates; no fill source is observed, so realized returns are unsupported.",
            "WAL is measured only; the probe does not control the writer or checkpoint policy.",
        ],
    }


def sample_window(
    options: ProbeOptions,
    *,
    clock: Callable[[], datetime],
    sleep: Callable[[float], None],
    wal_reader: Callable[[Path], int],
) -> tuple[datetime, datetime, list[dict[str, Any]]]:
    database = Path(options.database)
    started_at = clock().astimezone(timezone.utc)
    deadline = started_at.timestamp() + options.duration_seconds
    samples: list[dict[str, Any]] = []
    window_start = started_at
    include_start = True
    while True:
        observed_at = clock().astimezone(timezone.utc)
        with open_read_only(database) as connection:
            connection.execute("BEGIN")
            try:
                samples.append(
                    collect_catalog_sample(
                        connection,
                        database,
                        observed_at,
                        wal_reader(database),
                        window_start=window_start,
                        include_start=include_start,
                    )
                )
            finally:
                connection.rollback()
        if options.duration_seconds == 0 or observed_at.timestamp() >= deadline:
            break
        window_start = observed_at
        include_start = False
        sleep(min(options.interval_seconds, max(0, deadline - observed_at.timestamp())))
    return started_at, clock().astimezone(timezone.utc), samples


def run_probe(
    options: ProbeOptions,
    *,
    clock: Callable[[], datetime] = _now,
    sleep: Callable[[float], None] = time.sleep,
    wal_reader: Callable[[Path], int] = _read_wal_size,
) -> dict[str, Any]:
    if options.duration_seconds < 0:
        raise ProbeUsageError("duration_seconds must be non-negative")
    if options.interval_seconds <= 0:
        raise ProbeUsageError("interval_seconds must be positive")
    try:
        started_at, ended_at, samples = sample_window(
            options,
            clock=clock,
            sleep=sleep,
            wal_reader=wal_reader,
        )
    except (OSError, sqlite3.DatabaseError) as error:
        raise ProbeUnavailableError(str(error)) from error
    return build_report(Path(options.database), started_at, ended_at, samples)


def write_report(path: Path, report: Mapping[str, Any]) -> None:
    output_path = Path(path)
    if not output_path.parent.is_dir():
        raise ProbeOutputError(f"output directory does not exist: {output_path.parent}")
    try:
        output_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as error:
        raise ProbeOutputError(str(error)) from error


def _validate_output_path(output: Path, database: Path) -> None:
    output_path = Path(output).expanduser().resolve(strict=False)
    database_path = _resolved_database_path(database)
    protected_paths = {
        database_path,
        database_path.with_name(database_path.name + "-wal"),
        database_path.with_name(database_path.name + "-shm"),
        database_path.with_name(f".{database_path.name}.v4-migration.json"),
    }
    if output_path in protected_paths:
        raise ProbeOutputError(
            f"output path must not overwrite database artifacts: {output}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        options = parse_args(sys.argv[1:] if argv is None else argv)
        if options.output is not None:
            _validate_output_path(options.output, options.database)
        report = run_probe(options)
        if options.output is not None:
            write_report(options.output, report)
    except (
        ProbeUsageError,
        ProbeUnavailableError,
        ProbeOutputError,
        OSError,
        sqlite3.DatabaseError,
    ) as error:
        print(f"catalog-v4 validation error: {error}", file=sys.stderr)
        return 2
    print(
        f"catalog-v4 status={report['status']} samples={len(report['samples'])} "
        f"exit_code={report['exit_code']}"
    )
    return int(report["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
