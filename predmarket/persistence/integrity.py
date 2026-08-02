"""Read-only startup integrity checks beyond SQLite's structural checks."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from typing import Any, Literal

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

_CATEGORY_NAMES = (
    "schema",
    "id_arrays",
    "json_payloads",
    "decimals",
    "revisions",
)
_CODE_CATEGORIES = {
    "SCHEMA_VERSION_MISMATCH": "schema",
    "SQLITE_INTEGRITY_CHECK_FAILED": "schema",
    "FOREIGN_KEY_VIOLATION": "schema",
    "SCHEMA_INVALID": "schema",
    "EVENT_MARKET_IDS_INVALID": "id_arrays",
    "EVENT_MARKETS_MISMATCH": "id_arrays",
    "SIGNAL_MARKET_IDS_INVALID": "id_arrays",
    "SIGNAL_MARKET_MISSING": "id_arrays",
    "JSON_PAYLOAD_INVALID": "json_payloads",
    "DECIMAL_INVALID": "decimals",
    "RISK_FORMULA_INVALID": "decimals",
    "LATEST_REVISION_MISMATCH": "revisions",
    "REVISION_PAYLOAD_INVALID": "revisions",
    "EVIDENCE_IDENTITY_MISMATCH": "revisions",
}
_DECIMAL_KEY_COLUMNS = {
    "markets": ("id",),
    "relations": ("id",),
    "signal_revisions": ("signal_id", "revision"),
    "signal_legs": ("signal_id", "revision", "position"),
    "orderbook_snapshots": ("id",),
    "orderbook_levels": ("snapshot_id", "side", "position"),
}


@dataclass(frozen=True)
class _IntegrityFinding:
    code: str
    category: str
    severity: Literal["error", "warning"]
    records: tuple[dict[str, object], ...]

    def to_payload(self) -> dict[str, object]:
        return {
            "category": self.category,
            "code": self.code,
            "severity": self.severity,
            "records": [
                _json_safe(record)
                for record in sorted(self.records, key=_record_sort_key)
            ],
        }


class _IntegrityCollector:
    def __init__(self) -> None:
        self._findings: dict[str, _IntegrityFinding] = {}

    @property
    def violations(self) -> tuple[str, ...]:
        return tuple(self._findings)

    @property
    def findings(self) -> tuple[_IntegrityFinding, ...]:
        return tuple(self._findings.values())

    def add(
        self,
        code: str,
        *,
        record: dict[str, object] | None = None,
    ) -> None:
        category = _CODE_CATEGORIES[code]
        finding = self._findings.get(code)
        if finding is None:
            self._findings[code] = _IntegrityFinding(
                code=code,
                category=category,
                severity="error",
                records=() if record is None else (dict(record),),
            )
            return
        if record is not None and record not in finding.records:
            self._findings[code] = _IntegrityFinding(
                code=finding.code,
                category=finding.category,
                severity=finding.severity,
                records=finding.records + (dict(record),),
            )


@dataclass(frozen=True)
class DatabaseDoctorReport:
    database: Path
    status: Literal["ok", "issues", "unavailable"]
    findings: tuple[_IntegrityFinding, ...] = ()
    error: dict[str, str] | None = None

    @property
    def exit_code(self) -> int:
        return 2 if self.status == "unavailable" else int(self.status == "issues")

    def to_payload(self) -> dict[str, object]:
        findings = tuple(
            sorted(
                self.findings,
                key=lambda finding: (
                    _CATEGORY_NAMES.index(finding.category),
                    finding.code,
                ),
            )
        )
        errors = sum(
            finding.severity == "error" for finding in findings
        )
        warnings = sum(
            finding.severity == "warning" for finding in findings
        )
        categories = {
            category: {
                "errors": sum(
                    finding.category == category and finding.severity == "error"
                    for finding in findings
                ),
                "warnings": sum(
                    finding.category == category and finding.severity == "warning"
                    for finding in findings
                ),
            }
            for category in _CATEGORY_NAMES
        }
        payload: dict[str, object] = {
            "database": str(self.database),
            "status": self.status,
            "summary": {"errors": errors, "warnings": warnings},
            "categories": categories,
            "findings": [finding.to_payload() for finding in findings],
        }
        if self.error is not None:
            payload["error"] = dict(self.error)
        return payload


class DatabaseIntegrityError(RuntimeError):
    def __init__(self, violations: tuple[str, ...]) -> None:
        self.violations = violations
        super().__init__(
            "database integrity check failed: " + ", ".join(violations)
        )


def check_database_integrity(path: Path) -> None:
    """Raise with stable violation codes when a schema-v1 database is unsafe."""
    collector = _collect_database_findings(path, include_semantic=True)
    if collector.violations:
        raise DatabaseIntegrityError(collector.violations)


def check_database_startup(path: Path) -> None:
    """Run only the fast structural checks needed before starting the service."""
    collector = _collect_database_findings(path, include_semantic=False)
    if collector.violations:
        raise DatabaseIntegrityError(collector.violations)


def run_database_doctor(path: Path) -> DatabaseDoctorReport:
    """Return a read-only report for structural and semantic database checks."""
    database_path = Path(path)
    try:
        collector = _collect_database_findings(
            database_path,
            include_semantic=True,
        )
    except (OSError, sqlite3.DatabaseError) as error:
        return DatabaseDoctorReport(
            database=database_path,
            status="unavailable",
            error={
                "code": "DATABASE_UNAVAILABLE",
                "message": str(error),
            },
        )
    return DatabaseDoctorReport(
        database=database_path,
        status="issues" if collector.findings else "ok",
        findings=collector.findings,
    )


def _collect_database_findings(
    path: Path,
    *,
    include_semantic: bool,
) -> _IntegrityCollector:
    database_path = Path(path)
    connection = _connect_read_only(database_path)
    connection.row_factory = sqlite3.Row
    collector = _IntegrityCollector()
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        schema_version_valid = version == SCHEMA_VERSION
        if not schema_version_valid:
            _add(collector, "SCHEMA_VERSION_MISMATCH")

        integrity_rows = connection.execute("PRAGMA integrity_check").fetchall()
        if [row[0] for row in integrity_rows] != ["ok"]:
            _add(collector, "SQLITE_INTEGRITY_CHECK_FAILED")
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            for row in foreign_key_rows:
                _add(
                    collector,
                    "FOREIGN_KEY_VIOLATION",
                    record={
                        "table": row[0],
                        "rowid": row[1],
                        "parent": row[2],
                        "foreign_key": row[3],
                    },
                )

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
            _add(collector, "SCHEMA_INVALID")
        elif include_semantic and schema_version_valid:
            try:
                _check_id_arrays(connection, collector)
                _check_json_payloads(connection, collector)
                _check_decimals(connection, collector)
                _check_latest_revisions(connection, collector)
                _check_revision_payloads(connection, collector)
            except sqlite3.DatabaseError:
                _add(collector, "SCHEMA_INVALID")
    finally:
        connection.close()
    return collector


def _connect_read_only(path: Path) -> sqlite3.Connection:
    """Open read-only without creating WAL sidecars for a quiet database."""
    database_path = Path(path)
    query = "mode=ro"
    sidecars = _database_sidecars(database_path)
    if not any(path.exists() for path in sidecars):
        query += "&immutable=1"
    connection = sqlite3.connect(f"file:{database_path}?{query}", uri=True)
    if "immutable=1" in query and any(path.exists() for path in sidecars):
        connection.close()
        return sqlite3.connect(
            f"file:{database_path}?mode=ro",
            uri=True,
        )
    return connection


def _database_sidecars(path: Path) -> tuple[Path, Path]:
    return Path(f"{path}-wal"), Path(f"{path}-shm")


def _check_id_arrays(
    connection: sqlite3.Connection,
    violations: list[str] | _IntegrityCollector,
) -> None:
    for row in connection.execute(
        "SELECT id, market_ids_json FROM events ORDER BY CAST(id AS BLOB)"
    ):
        market_ids = _canonical_id_array(row["market_ids_json"])
        if market_ids is None:
            _add(
                violations,
                "EVENT_MARKET_IDS_INVALID",
                record=_record("events", row, ("id",), "market_ids_json"),
            )
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
            _add(
                violations,
                "EVENT_MARKETS_MISMATCH",
                record=_record("events", row, ("id",), "market_ids_json"),
            )

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
            _add(
                violations,
                "SIGNAL_MARKET_IDS_INVALID",
                record=_record(
                    "arbitrage_signals", row, ("id",), "market_ids_json"
                ),
            )
            continue
        if any(market_id not in known_market_ids for market_id in market_ids):
            _add(
                violations,
                "SIGNAL_MARKET_MISSING",
                record=_record(
                    "arbitrage_signals", row, ("id",), "market_ids_json"
                ),
            )


def _check_json_payloads(
    connection: sqlite3.Connection,
    violations: list[str] | _IntegrityCollector,
) -> None:
    object_columns = (
        ("events", ("id",), "neg_risk_metadata_json"),
        ("relations", ("id",), "llm_analysis_json"),
        (
            "signal_revisions",
            ("signal_id", "revision"),
            "calculation_json",
        ),
        (
            "signal_revisions",
            ("signal_id", "revision"),
            "closure_context_json",
        ),
    )
    for table, key_columns, column in object_columns:
        for row in connection.execute(
            f"SELECT {', '.join((*key_columns, column))} FROM {table} "
            f"WHERE {column} IS NOT NULL"
        ):
            try:
                payload = json.loads(row[column])
            except (TypeError, json.JSONDecodeError):
                _add(
                    violations,
                    "JSON_PAYLOAD_INVALID",
                    record=_record(table, row, key_columns, column),
                )
                continue
            if not isinstance(payload, dict):
                _add(
                    violations,
                    "JSON_PAYLOAD_INVALID",
                    record=_record(table, row, key_columns, column),
                )

    for row in connection.execute(
        "SELECT signal_id, revision, risk_flags_json FROM signal_revisions"
    ):
        try:
            flags = json.loads(row["risk_flags_json"])
        except (TypeError, json.JSONDecodeError):
            _add(
                violations,
                "JSON_PAYLOAD_INVALID",
                record=_record(
                    "signal_revisions",
                    row,
                    ("signal_id", "revision"),
                    "risk_flags_json",
                ),
            )
            continue
        if not isinstance(flags, list) or any(
            not isinstance(flag, str) or not flag for flag in flags
        ):
            _add(
                violations,
                "JSON_PAYLOAD_INVALID",
                record=_record(
                    "signal_revisions",
                    row,
                    ("signal_id", "revision"),
                    "risk_flags_json",
                ),
            )


def _check_decimals(
    connection: sqlite3.Connection,
    violations: list[str] | _IntegrityCollector,
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
        "SELECT id, fee_schedule_json FROM tokens "
        "WHERE fee_schedule_json IS NOT NULL"
    ):
        try:
            FeeSchedule.from_json(json.loads(row["fee_schedule_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            _add(
                violations,
                "DECIMAL_INVALID",
                record=_record("tokens", row, ("id",), "fee_schedule_json"),
            )

    for row in connection.execute(
        """
        SELECT signal_id, revision, total_capital, worst_case_loss, risk_rate
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
            _add(
                violations,
                "RISK_FORMULA_INVALID",
                record=_record(
                    "signal_revisions",
                    row,
                    ("signal_id", "revision"),
                    "risk_rate",
                ),
            )


def _check_decimal_columns(
    connection: sqlite3.Connection,
    violations: list[str] | _IntegrityCollector,
    table: str,
    columns: dict[str, Callable[[Decimal], bool]],
) -> None:
    key_columns = _DECIMAL_KEY_COLUMNS[table]
    selected = ", ".join((*key_columns, *columns))
    for row in connection.execute(f"SELECT {selected} FROM {table}"):
        for column, predicate in columns.items():
            encoded = row[column]
            if encoded is None:
                continue
            try:
                value = parse_decimal(encoded)
            except ValueError:
                _add(
                    violations,
                    "DECIMAL_INVALID",
                    record=_record(table, row, key_columns, column),
                )
                continue
            if not predicate(value):
                _add(
                    violations,
                    "DECIMAL_INVALID",
                    record=_record(table, row, key_columns, column),
                )


def _check_latest_revisions(
    connection: sqlite3.Connection,
    violations: list[str] | _IntegrityCollector,
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
            _add(
                violations,
                "LATEST_REVISION_MISMATCH",
                record={
                    "table": "arbitrage_signals",
                    "id": row["id"],
                    "field": "latest_revision",
                },
            )


def _check_revision_payloads(
    connection: sqlite3.Connection,
    violations: list[str] | _IntegrityCollector,
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
            _add(
                violations,
                "REVISION_PAYLOAD_INVALID",
                record={
                    "table": "signal_revisions",
                    "signal_id": row["signal_id"],
                    "revision": row["revision"],
                    "field": "revision_payload",
                },
            )
        trade_identities = {
            (identity["market_id"], identity["token_id"])
            for identity in connection.execute(
                """
                SELECT market_id, token_id
                FROM signal_legs
                WHERE signal_id = ?
                  AND revision = ?
                  AND action IN ('BUY', 'SELL')
                """,
                (row["signal_id"], row["revision"]),
            )
        }
        evidence_identities = {
            (identity["market_id"], identity["token_id"])
            for identity in connection.execute(
                """
                SELECT market_id, token_id
                FROM orderbook_snapshots
                WHERE signal_id = ? AND revision = ?
                """,
                (row["signal_id"], row["revision"]),
            )
        }
        if trade_identities != evidence_identities:
            _add(
                violations,
                "EVIDENCE_IDENTITY_MISMATCH",
                record={
                    "table": "signal_revisions",
                    "signal_id": row["signal_id"],
                    "revision": row["revision"],
                    "field": "evidence_identity",
                },
            )

    for row in connection.execute(
        """
        SELECT signals.id, signals.status, revisions.event_type
        FROM arbitrage_signals AS signals
        JOIN signal_revisions AS revisions
          ON revisions.signal_id = signals.id
         AND revisions.revision = signals.latest_revision
        """
    ):
        if (row["status"] == "OPEN") != (row["event_type"] != "CLOSED"):
            _add(
                violations,
                "REVISION_PAYLOAD_INVALID",
                record={
                    "table": "arbitrage_signals",
                    "id": row["id"],
                    "field": "status_latest_revision",
                },
            )


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


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"type": "bytes", "hex": value.hex()}
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _record_sort_key(record: dict[str, object]) -> str:
    return json.dumps(
        _json_safe(record),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _record(
    table: str,
    row: sqlite3.Row,
    key_columns: tuple[str, ...],
    field: str,
) -> dict[str, object]:
    return {
        "table": table,
        **{column: row[column] for column in key_columns},
        "field": field,
    }


def _add(
    violations: list[str] | _IntegrityCollector,
    code: str,
    *,
    record: dict[str, object] | None = None,
) -> None:
    if isinstance(violations, _IntegrityCollector):
        violations.add(code, record=record)
    elif code not in violations:
        violations.append(code)
