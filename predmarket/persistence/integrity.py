"""Read-only startup integrity checks beyond SQLite's structural checks."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from typing import Any

from predmarket.domain.decimal import parse_decimal
from predmarket.domain.fees import FeeSchedule
from predmarket.persistence.schema import SCHEMA_VERSION


_PROJECT_TABLES = {
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


class DatabaseIntegrityError(RuntimeError):
    def __init__(self, violations: tuple[str, ...]) -> None:
        self.violations = violations
        super().__init__(
            "database integrity check failed: " + ", ".join(violations)
        )


def check_database_integrity(path: Path) -> None:
    """Raise with stable violation codes when a schema-v1 database is unsafe."""
    database_path = Path(path)
    connection = sqlite3.connect(
        f"file:{database_path}?mode=ro",
        uri=True,
    )
    connection.row_factory = sqlite3.Row
    violations: list[str] = []
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != SCHEMA_VERSION:
            _add(violations, "SCHEMA_VERSION_MISMATCH")
            raise DatabaseIntegrityError(tuple(violations))

        integrity_rows = connection.execute("PRAGMA integrity_check").fetchall()
        if [row[0] for row in integrity_rows] != ["ok"]:
            _add(violations, "SQLITE_INTEGRITY_CHECK_FAILED")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            _add(violations, "FOREIGN_KEY_VIOLATION")

        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        if tables != _PROJECT_TABLES:
            _add(violations, "SCHEMA_INVALID")
        else:
            try:
                _check_id_arrays(connection, violations)
                _check_json_payloads(connection, violations)
                _check_decimals(connection, violations)
                _check_latest_revisions(connection, violations)
                _check_revision_payloads(connection, violations)
            except sqlite3.DatabaseError:
                _add(violations, "SCHEMA_INVALID")
    finally:
        connection.close()

    if violations:
        raise DatabaseIntegrityError(tuple(violations))


def _check_id_arrays(
    connection: sqlite3.Connection,
    violations: list[str],
) -> None:
    for row in connection.execute(
        "SELECT id, market_ids_json FROM events ORDER BY CAST(id AS BLOB)"
    ):
        market_ids = _canonical_id_array(row["market_ids_json"])
        if market_ids is None:
            _add(violations, "EVENT_MARKET_IDS_INVALID")
            continue
        actual = tuple(
            market_row[0]
            for market_row in connection.execute(
                """
                SELECT id FROM markets
                WHERE event_id = ?
                ORDER BY CAST(id AS BLOB)
                """,
                (row["id"],),
            )
        )
        if market_ids != actual:
            _add(violations, "EVENT_MARKETS_MISMATCH")

    known_market_ids = {
        row[0] for row in connection.execute("SELECT id FROM markets")
    }
    for row in connection.execute(
        """
        SELECT id, market_ids_json FROM arbitrage_signals
        ORDER BY CAST(id AS BLOB)
        """
    ):
        market_ids = _canonical_id_array(row["market_ids_json"])
        if market_ids is None:
            _add(violations, "SIGNAL_MARKET_IDS_INVALID")
            continue
        if any(market_id not in known_market_ids for market_id in market_ids):
            _add(violations, "SIGNAL_MARKET_MISSING")


def _check_json_payloads(
    connection: sqlite3.Connection,
    violations: list[str],
) -> None:
    object_columns = (
        ("events", "neg_risk_metadata_json"),
        ("relations", "llm_analysis_json"),
        ("signal_revisions", "calculation_json"),
        ("signal_revisions", "closure_context_json"),
    )
    for table, column in object_columns:
        for row in connection.execute(
            f"SELECT {column} AS payload FROM {table} WHERE {column} IS NOT NULL"
        ):
            try:
                payload = json.loads(row["payload"])
            except (TypeError, json.JSONDecodeError):
                _add(violations, "JSON_PAYLOAD_INVALID")
                continue
            if not isinstance(payload, dict):
                _add(violations, "JSON_PAYLOAD_INVALID")

    for row in connection.execute(
        "SELECT risk_flags_json FROM signal_revisions"
    ):
        try:
            flags = json.loads(row["risk_flags_json"])
        except (TypeError, json.JSONDecodeError):
            _add(violations, "JSON_PAYLOAD_INVALID")
            continue
        if not isinstance(flags, list) or any(
            not isinstance(flag, str) or not flag for flag in flags
        ):
            _add(violations, "JSON_PAYLOAD_INVALID")


def _check_decimals(
    connection: sqlite3.Connection,
    violations: list[str],
) -> None:
    positive = lambda value: value > 0
    nonnegative = lambda value: value >= 0
    price = lambda value: Decimal("0") < value < Decimal("1")
    tick = lambda value: Decimal("0") < value <= Decimal("1")
    confidence = lambda value: Decimal("0") <= value <= Decimal("1")

    _check_decimal_columns(
        connection,
        violations,
        "markets",
        {
            "tick_size": tick,
            "minimum_order_size": positive,
        },
    )
    _check_decimal_columns(
        connection,
        violations,
        "relations",
        {"llm_confidence": confidence},
    )
    _check_decimal_columns(
        connection,
        violations,
        "signal_revisions",
        {
            "quantity": positive,
            "total_capital": positive,
            "expected_profit": lambda value: value.is_finite(),
            "return_rate": lambda value: value.is_finite(),
            "worst_case_loss": nonnegative,
            "risk_rate": nonnegative,
            "unhedged_notional": nonnegative,
        },
    )
    _check_decimal_columns(
        connection,
        violations,
        "signal_legs",
        {
            "quantity": positive,
            "average_price": price,
            "worst_price": price,
            "gross_amount": nonnegative,
            "fee_amount": nonnegative,
        },
    )
    _check_decimal_columns(
        connection,
        violations,
        "orderbook_snapshots",
        {
            "tick_size": tick,
            "minimum_order_size": positive,
        },
    )
    _check_decimal_columns(
        connection,
        violations,
        "orderbook_levels",
        {"price": price, "size": positive},
    )

    for row in connection.execute(
        "SELECT fee_schedule_json FROM tokens WHERE fee_schedule_json IS NOT NULL"
    ):
        try:
            FeeSchedule.from_json(json.loads(row["fee_schedule_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            _add(violations, "DECIMAL_INVALID")

    for row in connection.execute(
        """
        SELECT total_capital, worst_case_loss, risk_rate
        FROM signal_revisions
        WHERE total_capital IS NOT NULL
          AND worst_case_loss IS NOT NULL
          AND risk_rate IS NOT NULL
        """
    ):
        try:
            total_capital = parse_decimal(row["total_capital"])
            worst_case_loss = parse_decimal(row["worst_case_loss"])
            risk_rate = parse_decimal(row["risk_rate"])
        except ValueError:
            continue
        if total_capital == 0 or risk_rate != worst_case_loss / total_capital:
            _add(violations, "RISK_FORMULA_INVALID")


def _check_decimal_columns(
    connection: sqlite3.Connection,
    violations: list[str],
    table: str,
    columns: dict[str, Callable[[Decimal], bool]],
) -> None:
    selected = ", ".join(columns)
    for row in connection.execute(f"SELECT {selected} FROM {table}"):
        for column, predicate in columns.items():
            encoded = row[column]
            if encoded is None:
                continue
            try:
                value = parse_decimal(encoded)
            except ValueError:
                _add(violations, "DECIMAL_INVALID")
                continue
            if not predicate(value):
                _add(violations, "DECIMAL_INVALID")


def _check_latest_revisions(
    connection: sqlite3.Connection,
    violations: list[str],
) -> None:
    for row in connection.execute(
        """
        SELECT signals.id, signals.latest_revision, MAX(revisions.revision) AS actual
        FROM arbitrage_signals AS signals
        LEFT JOIN signal_revisions AS revisions ON revisions.signal_id = signals.id
        GROUP BY signals.id, signals.latest_revision
        """
    ):
        if row["actual"] is None or row["latest_revision"] != row["actual"]:
            _add(violations, "LATEST_REVISION_MISMATCH")


def _check_revision_payloads(
    connection: sqlite3.Connection,
    violations: list[str],
) -> None:
    economic_columns = (
        "quantity",
        "total_capital",
        "expected_profit",
        "return_rate",
        "worst_case_loss",
        "risk_rate",
        "unhedged_notional",
    )
    for row in connection.execute(
        """
        SELECT revisions.*,
               (SELECT COUNT(*) FROM signal_legs AS legs
                WHERE legs.signal_id = revisions.signal_id
                  AND legs.revision = revisions.revision) AS leg_count,
               (SELECT COUNT(*) FROM orderbook_snapshots AS snapshots
                WHERE snapshots.signal_id = revisions.signal_id
                  AND snapshots.revision = revisions.revision) AS snapshot_count
        FROM signal_revisions AS revisions
        """
    ):
        present = [row[column] is not None for column in economic_columns]
        all_economic = all(present)
        no_economic = not any(present)
        calculation = row["calculation_json"] is not None
        closure = row["closure_context_json"] is not None
        has_payload = row["leg_count"] > 0 and row["snapshot_count"] > 0

        valid = False
        if row["event_type"] in {"OPENED", "UPDATED"}:
            valid = all_economic and calculation and not closure and has_payload
        elif row["event_type"] == "CLOSED":
            valid = (
                all_economic and calculation and not closure and has_payload
            ) or (
                no_economic
                and not calculation
                and closure
                and row["leg_count"] == 0
                and row["snapshot_count"] == 0
            )
        if not valid:
            _add(violations, "REVISION_PAYLOAD_INVALID")

    for row in connection.execute(
        """
        SELECT signals.status, revisions.event_type
        FROM arbitrage_signals AS signals
        JOIN signal_revisions AS revisions
          ON revisions.signal_id = signals.id
         AND revisions.revision = signals.latest_revision
        """
    ):
        if (row["status"] == "OPEN") != (row["event_type"] != "CLOSED"):
            _add(violations, "REVISION_PAYLOAD_INVALID")


def _canonical_id_array(encoded: Any) -> tuple[str, ...] | None:
    if not isinstance(encoded, str):
        return None
    try:
        values = json.loads(encoded)
    except json.JSONDecodeError:
        return None
    if not isinstance(values, list) or not values:
        return None
    if any(not isinstance(value, str) or not value for value in values):
        return None
    if len(values) != len(set(values)):
        return None
    canonical = sorted(values, key=lambda value: value.encode("utf-8"))
    if values != canonical:
        return None
    canonical_encoding = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    if encoded != canonical_encoding:
        return None
    return tuple(values)


def _add(violations: list[str], code: str) -> None:
    if code not in violations:
        violations.append(code)
