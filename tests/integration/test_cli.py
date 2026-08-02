from __future__ import annotations

from io import StringIO
import json
import logging
from pathlib import Path
import re
import sqlite3

import pytest
import yaml

from predmarket.cli import _build_parser, _configure_logging, main
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


def test_cli_has_only_service_and_relation_command_families() -> None:
    parser = _build_parser()
    action = next(item for item in parser._actions if item.dest == "command")

    assert set(action.choices) == {"run", "status", "signals", "relations"}
    assert {"trade", "order", "wallet", "auth", "login"}.isdisjoint(
        action.choices
    )


def test_run_parser_accepts_case_insensitive_log_level() -> None:
    arguments = _build_parser().parse_args(["run", "--log-level", "debug"])

    assert arguments.log_level == "DEBUG"


def test_run_parser_defaults_to_info() -> None:
    arguments = _build_parser().parse_args(["run"])

    assert arguments.log_level == "INFO"


def test_run_parser_rejects_unknown_log_level() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["run", "--log-level", "TRACE"])


def test_configure_logging_writes_formatted_records_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    application_logger = logging.getLogger("predmarket")
    original_handlers = list(application_logger.handlers)
    original_level = application_logger.level
    original_propagate = application_logger.propagate

    try:
        _configure_logging("DEBUG", terminal_enabled=True)
        logging.getLogger("predmarket.cli").info("cli test")
        captured = capsys.readouterr()
    finally:
        for handler in list(application_logger.handlers):
            if handler not in original_handlers:
                application_logger.removeHandler(handler)
                handler.close()
        application_logger.setLevel(original_level)
        application_logger.propagate = original_propagate

    assert captured.out == ""
    assert re.search(
        r"\d{4}-\d{2}-\d{2} .* INFO predmarket\.cli - cli test\n",
        captured.err,
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
