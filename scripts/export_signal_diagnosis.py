#!/usr/bin/env python3
"""Export a deterministic, reviewable signal-diagnosis evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path


SIGNAL_IDS = (
    "1d1ff4e0cab8489e838fd6a9d53b7213",
    "cbec6442236146b5bc303e22665c2acd",
)
EXECUTION_TABLE_NAMES = ("orders", "fills", "trades", "wallet_transactions", "realized_pnl")


def _rows(connection: sqlite3.Connection, query: str, parameters: tuple = ()) -> list[dict]:
    return [dict(row) for row in connection.execute(query, parameters)]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def export(database: Path) -> dict:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in SIGNAL_IDS)
    tables = tuple(
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
    )
    bundle = {
        "format_version": 1,
        "source": {
            "filename": database.name,
            "sha256": _sha256(database),
        },
        "capability_boundary": {
            "available_tables": tables,
            "execution_tables_checked": EXECUTION_TABLE_NAMES,
            "execution_tables_present": sorted(set(tables) & set(EXECUTION_TABLE_NAMES)),
        },
        "signals": _rows(
            connection,
            f"""SELECT id, opportunity_key, strategy_type, execution_mode, status,
                       close_reason, opened_at, updated_at, closed_at, latest_revision
                  FROM arbitrage_signals WHERE id IN ({placeholders}) ORDER BY id""",
            SIGNAL_IDS,
        ),
        "revisions": _rows(
            connection,
            f"""SELECT signal_id, revision, event_type, observed_at, quantity,
                       total_capital, expected_profit, return_rate, worst_case_loss,
                       risk_rate, unhedged_notional, risk_flags_json
                  FROM signal_revisions WHERE signal_id IN ({placeholders})
              ORDER BY signal_id, revision""",
            SIGNAL_IDS,
        ),
        "legs": _rows(
            connection,
            f"""SELECT signal_id, revision, position, market_id, token_id, action,
                       side, quantity, average_price, worst_price, gross_amount, fee_amount
                  FROM signal_legs WHERE signal_id IN ({placeholders})
              ORDER BY signal_id, revision, position""",
            SIGNAL_IDS,
        ),
        "token_fee_schedules": _rows(
            connection,
            f"""SELECT DISTINCT t.id AS token_id, t.market_id,
                       t.fee_schedule_json, t.fee_updated_at
                  FROM tokens t JOIN signal_legs l ON l.token_id = t.id
                 WHERE l.signal_id IN ({placeholders})
              ORDER BY t.id""",
            SIGNAL_IDS,
        ),
        "snapshots": _rows(
            connection,
            f"""SELECT id, signal_id, revision, market_id, token_id,
                       subscription_generation, book_hash, exchange_timestamp,
                       received_timestamp, tick_size, minimum_order_size
                  FROM orderbook_snapshots WHERE signal_id IN ({placeholders})
              ORDER BY signal_id, revision, token_id""",
            SIGNAL_IDS,
        ),
        "levels": _rows(
            connection,
            f"""SELECT l.snapshot_id, l.side, l.position, l.price, l.size
                  FROM orderbook_levels l JOIN orderbook_snapshots s ON s.id = l.snapshot_id
                 WHERE s.signal_id IN ({placeholders})
              ORDER BY l.snapshot_id, l.side, l.position""",
            SIGNAL_IDS,
        ),
        "system_event_types": _rows(
            connection,
            """SELECT component, event_type, COUNT(*) AS count
                 FROM system_events GROUP BY component, event_type
             ORDER BY component, event_type""",
        ),
    }
    connection.close()
    return bundle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    rendered = json.dumps(export(arguments.database), indent=2, sort_keys=True) + "\n"
    if arguments.output:
        arguments.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
