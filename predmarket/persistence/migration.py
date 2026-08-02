"""Explicit, transactional migrations for the on-disk SQLite schema."""

from __future__ import annotations

from pathlib import Path
import sqlite3

_TARGET_VERSION = 2
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
_MARKET_COLUMNS = (
    "id",
    "event_id",
    "condition_id",
    "slug",
    "question",
    "description",
    "status",
    "active",
    "accepting_orders",
    "enable_orderbook",
    "neg_risk",
    "neg_risk_outcome_position",
    "neg_risk_member_complete",
    "sync_generation",
    "sync_generation_complete",
    "tick_size",
    "minimum_order_size",
    "end_at",
    "resolved_at",
    "source_updated_at",
    "created_at",
    "updated_at",
)
_MARKET_COLUMN_SQL = ", ".join(_MARKET_COLUMNS)
_MARKETS_TABLE_V2 = """
CREATE TABLE markets_v2 (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    event_id TEXT REFERENCES events(id),
    condition_id TEXT NOT NULL UNIQUE CHECK (length(condition_id) > 0),
    slug TEXT UNIQUE,
    question TEXT NOT NULL CHECK (length(trim(question)) > 0),
    description TEXT,
    status TEXT NOT NULL
        CHECK (status IN ('ACTIVE', 'CLOSED', 'RESOLVED', 'ARCHIVED')),
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    accepting_orders INTEGER NOT NULL CHECK (accepting_orders IN (0, 1)),
    enable_orderbook INTEGER NOT NULL CHECK (enable_orderbook IN (0, 1)),
    neg_risk INTEGER NOT NULL CHECK (neg_risk IN (0, 1)),
    neg_risk_outcome_position INTEGER
        CHECK (neg_risk_outcome_position IS NULL OR neg_risk_outcome_position >= 0),
    neg_risk_member_complete INTEGER NOT NULL
        CHECK (neg_risk_member_complete IN (0, 1)),
    sync_generation TEXT NOT NULL CHECK (length(sync_generation) > 0),
    sync_generation_complete INTEGER NOT NULL
        CHECK (sync_generation_complete IN (0, 1)),
    tick_size TEXT
        CHECK (
            tick_size IS NULL OR (
                length(tick_size) > 0
                AND CAST(tick_size AS NUMERIC) > 0
                AND CAST(tick_size AS NUMERIC) <= 1
            )
        ),
    minimum_order_size TEXT
        CHECK (
            minimum_order_size IS NULL OR (
                length(minimum_order_size) > 0
                AND CAST(minimum_order_size AS NUMERIC) > 0
            )
        ),
    end_at INTEGER CHECK (end_at IS NULL OR end_at >= 0),
    resolved_at INTEGER CHECK (resolved_at IS NULL OR resolved_at >= 0),
    source_updated_at INTEGER CHECK (source_updated_at IS NULL OR source_updated_at >= 0),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);
"""


def migrate_database(
    database_path: Path,
    backup_path: Path,
    *,
    target_version: int,
) -> None:
    """Migrate schema v1 to v2 after creating an immutable backup copy."""
    if target_version != _TARGET_VERSION:
        raise ValueError(f"unsupported migration target {target_version}; expected 2")

    source_path = Path(database_path)
    destination_path = Path(backup_path)
    if not source_path.exists() or source_path.stat().st_size == 0:
        raise ValueError(f"database does not exist or is empty: {source_path}")
    if source_path.resolve() == destination_path.resolve():
        raise ValueError("backup path must differ from database path")
    if destination_path.exists():
        raise FileExistsError(f"backup already exists: {destination_path}")

    _validate_v1(source_path)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    _backup(source_path, destination_path)

    connection = sqlite3.connect(source_path, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(_MARKETS_TABLE_V2)
            connection.execute(
                f"INSERT INTO markets_v2 ({_MARKET_COLUMN_SQL}) "
                f"SELECT {_MARKET_COLUMN_SQL} FROM markets"
            )
            connection.execute("DROP TABLE markets")
            connection.execute("ALTER TABLE markets_v2 RENAME TO markets")
            connection.execute(
                "CREATE INDEX markets_watch_state_idx "
                "ON markets(status, active, accepting_orders, enable_orderbook)"
            )
            connection.execute("CREATE INDEX markets_event_id_idx ON markets(event_id)")
            connection.execute(f"PRAGMA user_version = {_TARGET_VERSION}")
            _check_sqlite_integrity(connection)
        except BaseException:
            connection.rollback()
            raise
        connection.commit()
        connection.execute("PRAGMA foreign_keys = ON")
        _check_sqlite_integrity(connection)
    finally:
        connection.close()


def _validate_v1(path: Path) -> None:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version != 1:
            raise ValueError(f"unsupported database schema version {version}; expected 1")
        _check_sqlite_integrity(connection)
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
            raise ValueError("database does not have the expected schema v1 tables")
        market_event = next(
            row
            for row in connection.execute("PRAGMA table_info(markets)")
            if row["name"] == "event_id"
        )
        if int(market_event["notnull"]) != 1:
            raise ValueError("database markets.event_id is not schema v1")
    finally:
        connection.close()


def _backup(source_path: Path, destination_path: Path) -> None:
    source = sqlite3.connect(source_path)
    destination = sqlite3.connect(destination_path)
    try:
        source.backup(destination)
        destination.commit()
    except BaseException:
        destination_path.unlink(missing_ok=True)
        raise
    finally:
        source.close()
        destination.close()


def _check_sqlite_integrity(connection: sqlite3.Connection) -> None:
    integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
    if integrity != ["ok"]:
        raise sqlite3.DatabaseError(
            "sqlite integrity check failed: " + ", ".join(map(str, integrity))
        )
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_keys:
        raise sqlite3.DatabaseError("foreign key check failed")
