from __future__ import annotations

from io import StringIO
import json
import logging
from pathlib import Path
import sqlite3

import yaml

from predmarket.cli import _build_parser, main
from predmarket.persistence.schema import initialize_database
from predmarket.runtime_logging import configure_runtime_logging


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


def test_cli_has_only_service_and_relation_command_families() -> None:
    parser = _build_parser()
    action = next(item for item in parser._actions if item.dest == "command")

    assert set(action.choices) == {"run", "status", "signals", "relations"}
    assert {"trade", "order", "wallet", "auth", "login"}.isdisjoint(
        action.choices
    )


def test_runtime_logging_targets_cli_output_without_duplicate_handlers() -> None:
    runtime_logger = logging.getLogger("predmarket")
    child_logger = logging.getLogger("predmarket.test_cli")
    third_party_logger = logging.getLogger("third_party.test_cli")
    previous_handlers = runtime_logger.handlers[:]
    previous_level = runtime_logger.level
    previous_propagate = runtime_logger.propagate
    previous_third_party_level = third_party_logger.level
    try:
        first_output = StringIO()
        configure_runtime_logging(first_output)
        child_logger.info("first message")
        assert "INFO predmarket.test_cli first message" in first_output.getvalue()

        second_output = StringIO()
        configure_runtime_logging(second_output)
        child_logger.info("second message")

        runtime_handlers = [
            handler
            for handler in runtime_logger.handlers
            if getattr(handler, "_predmarket_runtime_handler", False)
        ]
        assert len(runtime_handlers) == 1
        assert "second message" not in first_output.getvalue()
        assert "INFO predmarket.test_cli second message" in second_output.getvalue()
        assert third_party_logger.level == previous_third_party_level
    finally:
        for handler in runtime_logger.handlers:
            handler.close()
        runtime_logger.handlers = previous_handlers
        runtime_logger.setLevel(previous_level)
        runtime_logger.propagate = previous_propagate


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
