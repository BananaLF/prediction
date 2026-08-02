from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import sqlite3

import yaml

from predmarket.cli import _build_parser, main
from predmarket.persistence.schema import initialize_database


def _config(tmp_path: Path, database_path: Path) -> Path:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw["database"]["path"] = str(database_path)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def _seed_signal(path: Path) -> None:
    initialize_database(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            INSERT INTO arbitrage_signals (
                id, opportunity_key, strategy_type, market_ids_json, relation_id,
                execution_mode, status, opened_at, updated_at, closed_at,
                close_reason, latest_revision
            ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?)
            """,
            (
                "signal-1",
                "binary:market-a",
                "BINARY_UNDERPRICED",
                '["market-a","market-b"]',
                "IMMEDIATE_CONVERSION",
                "OPEN",
                10,
                11,
                1,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def test_cli_has_service_inspection_migration_and_relation_commands() -> None:
    parser = _build_parser()
    action = next(item for item in parser._actions if item.dest == "command")

    assert set(action.choices) == {
        "run",
        "status",
        "signals",
        "relations",
        "migrate",
        "doctor",
    }
    assert {"trade", "order", "wallet", "auth", "login"}.isdisjoint(
        action.choices
    )


def test_signals_list_and_show_include_canonical_market_ids(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.sqlite3"
    config_path = _config(tmp_path, database_path)
    _seed_signal(database_path)

    listed = StringIO()
    assert main(["--config", str(config_path), "signals", "list"], stdout=listed) == 0
    shown = StringIO()
    assert main(
        ["--config", str(config_path), "signals", "show", "signal-1"],
        stdout=shown,
    ) == 0

    assert json.loads(listed.getvalue()) == [
        {
            "close_reason": None,
            "closed_at": None,
            "execution_mode": "IMMEDIATE_CONVERSION",
            "id": "signal-1",
            "latest_revision": 1,
            "market_ids": ["market-a", "market-b"],
            "opened_at": 10,
            "opportunity_key": "binary:market-a",
            "relation_id": None,
            "status": "OPEN",
            "strategy_type": "BINARY_UNDERPRICED",
            "updated_at": 11,
        }
    ]
    assert json.loads(shown.getvalue())["market_ids"] == ["market-a", "market-b"]


def test_doctor_reports_legal_orphans_without_failing(tmp_path: Path) -> None:
    database_path = tmp_path / "doctor.sqlite3"
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO markets (
                id, event_id, condition_id, question, status, active,
                accepting_orders, enable_orderbook, neg_risk,
                neg_risk_member_complete, sync_generation,
                sync_generation_complete, created_at, updated_at
            ) VALUES ('market-orphan', NULL, 'condition-orphan', 'Orphan?',
                      'ACTIVE', 1, 1, 1, 0, 0, 'sync-1', 1, 1, 1)
            """
        )

    output = StringIO()
    assert main(
        ["doctor", "--database", str(database_path)],
        stdout=output,
    ) == 0
    report = json.loads(output.getvalue())
    assert report["schema_version"] == 2
    assert report["orphan_markets"] == 1
    assert report["violations"] == []
