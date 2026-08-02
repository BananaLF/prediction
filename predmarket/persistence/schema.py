"""Creation and version validation for the greenfield SQLite schema."""

from __future__ import annotations

from pathlib import Path
import sqlite3


SCHEMA_VERSION = 2

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


def initialize_database(path: Path) -> None:
    """Create schema v2 or validate that an existing database is schema v2."""
    database_path = Path(path)
    if database_path.exists() and database_path.stat().st_size > 0:
        version = _read_existing_version(database_path)
        if version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported database schema version {version}; expected {SCHEMA_VERSION}"
            )
        return

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        try:
            connection.executescript(
                "BEGIN IMMEDIATE;\n"
                + SCHEMA_V2
                + f"\nPRAGMA user_version = {SCHEMA_VERSION};\n"
                + "COMMIT;\n"
            )
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
