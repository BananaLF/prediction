"""Creation and version validation for the greenfield SQLite schema."""

from __future__ import annotations

import json
from pathlib import Path
import re
import sqlite3

from predmarket.domain.decimal import decode_decimal, encode_decimal
from predmarket.domain.fees import FeeSchedule

SCHEMA_VERSION = 3

SCHEMA_V1 = """
CREATE TABLE events (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    slug TEXT UNIQUE,
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    description TEXT,
    status TEXT NOT NULL
        CHECK (status IN ('ACTIVE', 'CLOSED', 'RESOLVED', 'ARCHIVED')),
    neg_risk INTEGER NOT NULL CHECK (neg_risk IN (0, 1)),
    neg_risk_id TEXT,
    neg_risk_type TEXT,
    neg_risk_complete INTEGER NOT NULL CHECK (neg_risk_complete IN (0, 1)),
    neg_risk_conversion_supported INTEGER NOT NULL
        CHECK (neg_risk_conversion_supported IN (0, 1)),
    neg_risk_metadata_json TEXT
        CHECK (
            neg_risk_metadata_json IS NULL OR (
                json_valid(neg_risk_metadata_json)
                AND json_type(neg_risk_metadata_json) = 'object'
            )
        ),
    neg_risk_synced_at INTEGER CHECK (neg_risk_synced_at IS NULL OR neg_risk_synced_at >= 0),
    market_ids_json TEXT NOT NULL
        CHECK (json_valid(market_ids_json) AND json_type(market_ids_json) = 'array'),
    sync_generation TEXT NOT NULL CHECK (length(sync_generation) > 0),
    sync_generation_complete INTEGER NOT NULL
        CHECK (sync_generation_complete IN (0, 1)),
    start_at INTEGER CHECK (start_at IS NULL OR start_at >= 0),
    end_at INTEGER CHECK (end_at IS NULL OR end_at >= 0),
    resolved_at INTEGER CHECK (resolved_at IS NULL OR resolved_at >= 0),
    source_updated_at INTEGER CHECK (source_updated_at IS NULL OR source_updated_at >= 0),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= 0)
);

CREATE TABLE markets (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    event_id TEXT NOT NULL REFERENCES events(id),
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

CREATE TABLE tokens (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    market_id TEXT NOT NULL REFERENCES markets(id),
    outcome TEXT NOT NULL CHECK (length(outcome) > 0),
    position INTEGER NOT NULL CHECK (position >= 0),
    fee_schedule_json TEXT
        CHECK (
            fee_schedule_json IS NULL OR (
                json_valid(fee_schedule_json)
                AND json_type(fee_schedule_json) = 'object'
            )
        ),
    fee_updated_at INTEGER CHECK (fee_updated_at IS NULL OR fee_updated_at >= 0),
    sync_generation TEXT NOT NULL CHECK (length(sync_generation) > 0),
    sync_generation_complete INTEGER NOT NULL
        CHECK (sync_generation_complete IN (0, 1)),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
    UNIQUE (market_id, position),
    UNIQUE (market_id, outcome),
    UNIQUE (market_id, id)
);

CREATE TABLE relations (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    market_a_id TEXT NOT NULL REFERENCES markets(id),
    market_b_id TEXT NOT NULL REFERENCES markets(id),
    status TEXT NOT NULL
        CHECK (status IN ('NO_LLM_APPROVE', 'LLM_APPROVE', 'APPROVED')),
    discovery_source TEXT NOT NULL
        CHECK (discovery_source IN ('RULE', 'MANUAL')),
    llm_confidence TEXT
        CHECK (
            llm_confidence IS NULL OR (
                length(llm_confidence) > 0
                AND CAST(llm_confidence AS NUMERIC) >= 0
                AND CAST(llm_confidence AS NUMERIC) <= 1
            )
        ),
    llm_analysis_json TEXT
        CHECK (llm_analysis_json IS NULL OR json_valid(llm_analysis_json)),
    created_at INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= created_at),
    CHECK (market_a_id <> market_b_id),
    UNIQUE (market_a_id, market_b_id)
);

CREATE TABLE arbitrage_signals (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    opportunity_key TEXT NOT NULL CHECK (length(opportunity_key) > 0),
    strategy_type TEXT NOT NULL
        CHECK (
            strategy_type IN (
                'BINARY_UNDERPRICED',
                'BINARY_OVERPRICED',
                'LOGICAL_IMPLICATION',
                'NEG_RISK_COMPLETE_SET'
            )
        ),
    market_ids_json TEXT NOT NULL
        CHECK (json_valid(market_ids_json) AND json_type(market_ids_json) = 'array'),
    relation_id TEXT REFERENCES relations(id),
    execution_mode TEXT NOT NULL
        CHECK (execution_mode IN ('IMMEDIATE_CONVERSION', 'HOLD_TO_RESOLUTION')),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at INTEGER NOT NULL CHECK (opened_at >= 0),
    updated_at INTEGER NOT NULL CHECK (updated_at >= opened_at),
    closed_at INTEGER CHECK (closed_at IS NULL OR closed_at >= opened_at),
    close_reason TEXT
        CHECK (
            close_reason IS NULL OR close_reason IN (
                'PROFIT_BELOW_THRESHOLD',
                'RISK_ABOVE_THRESHOLD',
                'INSUFFICIENT_DEPTH',
                'QUANTITY_BELOW_MINIMUM',
                'INSUFFICIENT_CAPITAL',
                'MARKET_CLOSED',
                'EVENT_SETTLED',
                'ORDERBOOK_INVALID',
                'ORDERBOOK_STALE',
                'LEG_SKEW_EXCEEDED',
                'SDK_DISCONNECTED',
                'INPUT_METADATA_MISSING',
                'FEE_SCHEDULE_UNKNOWN',
                'FEE_SCHEDULE_STALE',
                'SYNC_GENERATION_INCOMPLETE',
                'RELATION_NOT_APPROVED'
            )
        ),
    latest_revision INTEGER NOT NULL CHECK (latest_revision >= 1),
    CHECK (
        (status = 'OPEN' AND closed_at IS NULL AND close_reason IS NULL)
        OR
        (status = 'CLOSED' AND closed_at IS NOT NULL AND close_reason IS NOT NULL)
    ),
    CHECK (
        (
            strategy_type = 'LOGICAL_IMPLICATION'
            AND relation_id IS NOT NULL
            AND execution_mode = 'HOLD_TO_RESOLUTION'
        )
        OR
        (
            strategy_type IN (
                'BINARY_UNDERPRICED',
                'BINARY_OVERPRICED',
                'NEG_RISK_COMPLETE_SET'
            )
            AND relation_id IS NULL
            AND execution_mode = 'IMMEDIATE_CONVERSION'
        )
    )
);

CREATE TABLE signal_revisions (
    signal_id TEXT NOT NULL REFERENCES arbitrage_signals(id),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    event_type TEXT NOT NULL CHECK (event_type IN ('OPENED', 'UPDATED', 'CLOSED')),
    observed_at INTEGER NOT NULL CHECK (observed_at >= 0),
    quantity TEXT CHECK (quantity IS NULL OR (length(quantity) > 0 AND CAST(quantity AS NUMERIC) > 0)),
    total_capital TEXT
        CHECK (total_capital IS NULL OR (length(total_capital) > 0 AND CAST(total_capital AS NUMERIC) > 0)),
    expected_profit TEXT CHECK (expected_profit IS NULL OR length(expected_profit) > 0),
    return_rate TEXT CHECK (return_rate IS NULL OR length(return_rate) > 0),
    worst_case_loss TEXT
        CHECK (
            worst_case_loss IS NULL OR (
                length(worst_case_loss) > 0
                AND CAST(worst_case_loss AS NUMERIC) >= 0
            )
        ),
    risk_rate TEXT CHECK (risk_rate IS NULL OR length(risk_rate) > 0),
    unhedged_notional TEXT
        CHECK (
            unhedged_notional IS NULL OR (
                length(unhedged_notional) > 0
                AND CAST(unhedged_notional AS NUMERIC) >= 0
            )
        ),
    risk_flags_json TEXT NOT NULL
        CHECK (json_valid(risk_flags_json) AND json_type(risk_flags_json) = 'array'),
    calculation_json TEXT
        CHECK (
            calculation_json IS NULL OR (
                json_valid(calculation_json)
                AND json_type(calculation_json) = 'object'
            )
        ),
    closure_context_json TEXT
        CHECK (
            closure_context_json IS NULL OR (
                json_valid(closure_context_json)
                AND json_type(closure_context_json) = 'object'
            )
        ),
    PRIMARY KEY (signal_id, revision),
    CHECK (
        (
            event_type IN ('OPENED', 'UPDATED')
            AND quantity IS NOT NULL
            AND total_capital IS NOT NULL
            AND expected_profit IS NOT NULL
            AND return_rate IS NOT NULL
            AND worst_case_loss IS NOT NULL
            AND risk_rate IS NOT NULL
            AND unhedged_notional IS NOT NULL
            AND calculation_json IS NOT NULL
            AND closure_context_json IS NULL
        )
        OR
        (
            event_type = 'CLOSED'
            AND (
                (
                    quantity IS NOT NULL
                    AND total_capital IS NOT NULL
                    AND expected_profit IS NOT NULL
                    AND return_rate IS NOT NULL
                    AND worst_case_loss IS NOT NULL
                    AND risk_rate IS NOT NULL
                    AND unhedged_notional IS NOT NULL
                    AND calculation_json IS NOT NULL
                    AND closure_context_json IS NULL
                )
                OR
                (
                    quantity IS NULL
                    AND total_capital IS NULL
                    AND expected_profit IS NULL
                    AND return_rate IS NULL
                    AND worst_case_loss IS NULL
                    AND risk_rate IS NULL
                    AND unhedged_notional IS NULL
                    AND calculation_json IS NULL
                    AND closure_context_json IS NOT NULL
                )
            )
        )
    )
);

CREATE TABLE signal_legs (
    signal_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    market_id TEXT NOT NULL REFERENCES markets(id),
    token_id TEXT,
    action TEXT NOT NULL
        CHECK (action IN ('BUY', 'SELL', 'MERGE', 'SPLIT', 'REDEEM', 'NEG_RISK_CONVERT')),
    side TEXT CHECK (side IS NULL OR side IN ('BUY', 'SELL')),
    quantity TEXT NOT NULL
        CHECK (length(quantity) > 0 AND CAST(quantity AS NUMERIC) > 0),
    average_price TEXT
        CHECK (
            average_price IS NULL OR (
                length(average_price) > 0
                AND CAST(average_price AS NUMERIC) > 0
                AND CAST(average_price AS NUMERIC) < 1
            )
        ),
    worst_price TEXT
        CHECK (
            worst_price IS NULL OR (
                length(worst_price) > 0
                AND CAST(worst_price AS NUMERIC) > 0
                AND CAST(worst_price AS NUMERIC) < 1
            )
        ),
    gross_amount TEXT NOT NULL
        CHECK (length(gross_amount) > 0 AND CAST(gross_amount AS NUMERIC) >= 0),
    fee_amount TEXT NOT NULL
        CHECK (length(fee_amount) > 0 AND CAST(fee_amount AS NUMERIC) >= 0),
    PRIMARY KEY (signal_id, revision, position),
    FOREIGN KEY (signal_id, revision)
        REFERENCES signal_revisions(signal_id, revision),
    FOREIGN KEY (market_id, token_id)
        REFERENCES tokens(market_id, id),
    CHECK (
        (
            action IN ('BUY', 'SELL')
            AND token_id IS NOT NULL
            AND side = action
            AND average_price IS NOT NULL
            AND worst_price IS NOT NULL
        )
        OR
        (
            action IN ('MERGE', 'SPLIT', 'REDEEM', 'NEG_RISK_CONVERT')
            AND token_id IS NULL
            AND side IS NULL
            AND average_price IS NULL
            AND worst_price IS NULL
        )
    )
);

CREATE TABLE orderbook_snapshots (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    signal_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    market_id TEXT NOT NULL REFERENCES markets(id),
    token_id TEXT NOT NULL,
    subscription_generation INTEGER NOT NULL CHECK (subscription_generation >= 1),
    book_hash TEXT NOT NULL CHECK (length(book_hash) > 0),
    exchange_timestamp INTEGER NOT NULL CHECK (exchange_timestamp >= 0),
    received_timestamp INTEGER NOT NULL CHECK (received_timestamp >= 0),
    tick_size TEXT NOT NULL
        CHECK (
            length(tick_size) > 0
            AND CAST(tick_size AS NUMERIC) > 0
            AND CAST(tick_size AS NUMERIC) <= 1
        ),
    minimum_order_size TEXT NOT NULL
        CHECK (
            length(minimum_order_size) > 0
            AND CAST(minimum_order_size AS NUMERIC) > 0
        ),
    FOREIGN KEY (signal_id, revision)
        REFERENCES signal_revisions(signal_id, revision),
    FOREIGN KEY (market_id, token_id)
        REFERENCES tokens(market_id, id),
    UNIQUE (signal_id, revision, token_id)
);

CREATE TABLE orderbook_levels (
    snapshot_id TEXT NOT NULL
        REFERENCES orderbook_snapshots(id) ON DELETE CASCADE,
    side TEXT NOT NULL CHECK (side IN ('BID', 'ASK')),
    position INTEGER NOT NULL CHECK (position >= 0),
    price TEXT NOT NULL
        CHECK (
            length(price) > 0
            AND CAST(price AS NUMERIC) > 0
            AND CAST(price AS NUMERIC) < 1
        ),
    size TEXT NOT NULL CHECK (length(size) > 0 AND CAST(size AS NUMERIC) > 0),
    PRIMARY KEY (snapshot_id, side, position)
);

CREATE TABLE system_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL
        CHECK (
            component IN (
                'SUPERVISOR',
                'DATABASE',
                'SYNC',
                'WATCH',
                'STRATEGY',
                'SIGNAL',
                'NOTIFIER'
            )
        ),
    severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'FATAL')),
    event_type TEXT NOT NULL CHECK (length(event_type) > 0),
    message TEXT NOT NULL CHECK (length(message) > 0),
    details_json TEXT CHECK (details_json IS NULL OR json_valid(details_json)),
    occurred_at INTEGER NOT NULL CHECK (occurred_at >= 0)
);

CREATE UNIQUE INDEX one_open_signal_per_opportunity
    ON arbitrage_signals(opportunity_key)
    WHERE status = 'OPEN';
CREATE INDEX events_status_idx ON events(status);
CREATE INDEX markets_watch_state_idx
    ON markets(status, active, accepting_orders, enable_orderbook);
CREATE INDEX markets_event_id_idx ON markets(event_id);
CREATE INDEX tokens_market_id_idx ON tokens(market_id);
CREATE INDEX relations_status_idx ON relations(status);
CREATE INDEX relations_market_a_id_idx ON relations(market_a_id);
CREATE INDEX relations_market_b_id_idx ON relations(market_b_id);
CREATE INDEX arbitrage_signals_status_updated_at_idx
    ON arbitrage_signals(status, updated_at);
CREATE INDEX arbitrage_signals_relation_id_idx ON arbitrage_signals(relation_id);
CREATE INDEX signal_revisions_observed_at_idx ON signal_revisions(observed_at);
CREATE INDEX system_events_occurred_at_idx ON system_events(occurred_at);
CREATE INDEX system_events_severity_occurred_at_idx
    ON system_events(severity, occurred_at);
"""

# v1 is retained as the source schema for the explicit on-disk migration.
# The only v2 structural change is that a market may temporarily have no event.
SCHEMA_V2 = SCHEMA_V1.replace(
    "event_id TEXT NOT NULL REFERENCES events(id)",
    "event_id TEXT REFERENCES events(id)",
)

_SCHEMA_TABLES = (
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

_DECIMAL_COLUMNS = {
    "markets": {
        "tick_size": "unit_interval_positive_or_one",
        "minimum_order_size": "positive",
    },
    "relations": {"llm_confidence": "unit_interval"},
    "signal_revisions": {
        "quantity": "positive",
        "total_capital": "positive",
        "expected_profit": "any",
        "return_rate": "any",
        "worst_case_loss": "nonnegative",
        "risk_rate": "any",
        "unhedged_notional": "nonnegative",
    },
    "signal_legs": {
        "quantity": "positive",
        "average_price": "unit_interval_positive",
        "worst_price": "unit_interval_positive",
        "gross_amount": "nonnegative",
        "fee_amount": "nonnegative",
    },
    "orderbook_snapshots": {
        "tick_size": "unit_interval_positive_or_one",
        "minimum_order_size": "positive",
    },
    "orderbook_levels": {
        "price": "unit_interval_positive",
        "size": "positive",
    },
}


def _canonical_decimal_check(column: str) -> str:
    magnitude = (
        f"CASE WHEN substr({column}, 1, 1) = '-' "
        f"THEN substr({column}, 2) ELSE {column} END"
    )
    return (
        f"typeof({column}) = 'text'"
        f" AND length({column}) > 0"
        f" AND substr({magnitude}, 1, 1) GLOB '[0-9]'"
        f" AND {magnitude} NOT GLOB '*[^0-9.]*'"
        f" AND {magnitude} NOT GLOB '*.*.*'"
        f" AND {magnitude} NOT GLOB '0[0-9]*'"
        f" AND NOT ({column} GLOB '-*' AND {magnitude} = '0')"
        f" AND substr({magnitude}, -1, 1) <> '.'"
        f" AND NOT ({magnitude} LIKE '%.%' AND substr({column}, -1, 1) = '0')"
    )


def _decimal_range_check(column: str, range_name: str) -> str:
    nonnegative = f"substr({column}, 1, 1) <> '-'"
    positive = f"{nonnegative} AND {column} <> '0'"
    if range_name == "any":
        return "1"
    if range_name == "positive":
        return positive
    if range_name == "nonnegative":
        return nonnegative
    if range_name == "unit_interval":
        return f"{nonnegative} AND ({column} IN ('0', '1') OR {column} LIKE '0.%')"
    if range_name == "unit_interval_positive":
        return f"{nonnegative} AND {column} LIKE '0.%'"
    if range_name == "unit_interval_positive_or_one":
        return f"{nonnegative} AND ({column} = '1' OR {column} LIKE '0.%')"
    raise ValueError(f"unknown Decimal range {range_name!r}")


def _decimal_definition(column: str, range_name: str, *, nullable: bool) -> str:
    checks = f"{_canonical_decimal_check(column)} AND {_decimal_range_check(column, range_name)}"
    if nullable:
        checks = f"{column} IS NULL OR ({checks})"
        nullability = ""
    else:
        nullability = " NOT NULL"
    return f"    {column} TEXT{nullability}\n        CHECK ({checks}),\n"


def _build_schema_v3() -> str:
    """Build the v3 schema by replacing every Decimal declaration in v2."""

    schema = SCHEMA_V2
    for table, columns in _DECIMAL_COLUMNS.items():
        table_match = re.search(
            rf"(?ms)^CREATE TABLE {re.escape(table)} \(.*?^\);",
            schema,
        )
        if table_match is None:
            raise RuntimeError(f"schema table {table!r} is missing")

        table_sql = table_match.group(0)
        for column, range_name in columns.items():
            column_match = re.search(
                rf"(?ms)^    {re.escape(column)} TEXT(?P<not_null> NOT NULL)?\s.*?"
                r"(?=^    [A-Za-z_][A-Za-z0-9_]*|^\);)",
                table_sql,
            )
            if column_match is None:
                raise RuntimeError(f"schema Decimal column {table}.{column} is missing")
            replacement = _decimal_definition(
                column,
                range_name,
                nullable=column_match.group("not_null") is None,
            )
            table_sql = (
                table_sql[: column_match.start()]
                + replacement
                + table_sql[column_match.end() :]
            )

        schema = schema[: table_match.start()] + table_sql + schema[table_match.end() :]
    return schema


SCHEMA_V3 = _build_schema_v3()


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _execute_sql_script(connection: sqlite3.Connection, script: str) -> None:
    """Execute a schema script without the implicit commit of executescript()."""

    statement_lines: list[str] = []
    for line in script.splitlines():
        statement_lines.append(line)
        statement = "\n".join(statement_lines)
        if sqlite3.complete_statement(statement):
            connection.execute(statement)
            statement_lines = []
    if any(line.strip() for line in statement_lines):
        raise sqlite3.OperationalError("incomplete schema SQL")


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})")
    ]


def _normalize_fee_schedule_json(value: str) -> str:
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("fee schedule must be a JSON object")
    parameters = payload.get("parameters")
    if not isinstance(parameters, dict):
        raise ValueError("fee parameters must be a JSON object")
    normalized = dict(payload)
    normalized_parameters = dict(parameters)
    for key, parameter in normalized_parameters.items():
        if isinstance(parameter, str):
            normalized_parameters[key] = encode_decimal(decode_decimal(parameter))
    normalized["parameters"] = normalized_parameters
    FeeSchedule.from_json(normalized)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _migrate_v2_to_v3(database_path: Path) -> None:
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("BEGIN IMMEDIATE")
        saved_indexes = connection.execute(
            "SELECT name, sql FROM sqlite_schema "
            "WHERE type = 'index' AND sql IS NOT NULL "
            "AND tbl_name IN (" + ",".join("?" for _ in _SCHEMA_TABLES) + ")",
            _SCHEMA_TABLES,
        ).fetchall()
        for index_name, _ in saved_indexes:
            connection.execute(f"DROP INDEX {_quote_identifier(str(index_name))}")

        temporary_tables = {
            table: f"__predmarket_v2_{table}" for table in _SCHEMA_TABLES
        }
        for table in _SCHEMA_TABLES:
            connection.execute(
                f"ALTER TABLE {_quote_identifier(table)} "
                f"RENAME TO {_quote_identifier(temporary_tables[table])}"
            )

        _execute_sql_script(connection, SCHEMA_V3)

        for table in _SCHEMA_TABLES:
            source_table = temporary_tables[table]
            source_columns = _table_columns(connection, source_table)
            target_columns = _table_columns(connection, table)
            source_column_set = set(source_columns)
            columns = [column for column in target_columns if column in source_column_set]
            quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
            rows = connection.execute(
                f"SELECT {quoted_columns} FROM {_quote_identifier(source_table)}"
            ).fetchall()
            decimal_columns = _DECIMAL_COLUMNS.get(table, {})
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = (
                f"INSERT INTO {_quote_identifier(table)} ({quoted_columns}) "
                f"VALUES ({placeholders})"
            )
            column_positions = {column: index for index, column in enumerate(columns)}
            for row in rows:
                values = list(row)
                for column in decimal_columns:
                    position = column_positions.get(column)
                    if position is None or values[position] is None:
                        continue
                    try:
                        values[position] = encode_decimal(decode_decimal(values[position]))
                    except ValueError as error:
                        raise ValueError(
                            f"invalid Decimal value in {table}.{column}"
                        ) from error
                fee_position = column_positions.get("fee_schedule_json")
                if fee_position is not None and values[fee_position] is not None:
                    try:
                        values[fee_position] = _normalize_fee_schedule_json(
                            values[fee_position]
                        )
                    except (TypeError, ValueError, json.JSONDecodeError) as error:
                        raise ValueError(
                            f"invalid fee schedule JSON in {table}.fee_schedule_json"
                        ) from error
                connection.execute(insert_sql, values)

        for table in reversed(_SCHEMA_TABLES):
            connection.execute(f"DROP TABLE {_quote_identifier(temporary_tables[table])}")

        existing_indexes = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'index'"
            )
        }
        for index_name, index_sql in saved_indexes:
            if str(index_name) in existing_indexes:
                continue
            assert isinstance(index_sql, str)
            connection.execute(index_sql)

        connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.close()


def initialize_database(path: Path) -> None:
    """Create schema v3 or migrate an existing schema v2 database to v3."""
    database_path = Path(path)
    if database_path.exists() and database_path.stat().st_size > 0:
        version = _read_existing_version(database_path)
        if version == SCHEMA_VERSION:
            return
        if version == 2:
            _migrate_v2_to_v3(database_path)
            return
        raise ValueError(
            f"unsupported database schema version {version}; expected {SCHEMA_VERSION}"
        )

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        try:
            connection.execute("BEGIN IMMEDIATE")
            _execute_sql_script(connection, SCHEMA_V3)
            connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    finally:
        connection.close()


def _read_existing_version(path: Path) -> int:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = connection.execute("PRAGMA user_version").fetchone()
        assert row is not None
        return int(row[0])
    finally:
        connection.close()
