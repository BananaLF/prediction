from __future__ import annotations

import re
import shlex
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest

from predmarket.cli import _build_parser, _normalize_config_position
from predmarket.operator_reset import (
    ResetRefused,
    execute_reset,
    prepare_reset,
    running_predmarket_processes,
)


DOCUMENTS = (
    Path("README.md"),
    Path("SECURITY.md"),
    Path("STRATEGY.md"),
    Path("docs/PROJECT-GUIDE.md"),
    Path("docs/TUTORIAL.md"),
    Path("docs/OPERATIONS.md"),
    Path("docs/VERIFICATION.md"),
)


def _documented_predmarket_commands() -> list[list[str]]:
    commands: list[list[str]] = []
    for document in DOCUMENTS:
        for line in document.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("python -m predmarket"):
                commands.append(shlex.split(stripped)[3:])
    return commands


def test_documented_local_commands_parse_without_runtime_side_effects() -> None:
    commands = _documented_predmarket_commands()

    assert {tuple(command) for command in commands} >= {
        ("--help",),
        ("status", "--config", "config/default.yaml"),
        ("run", "--config", "config/default.yaml"),
        ("signals", "list", "--config", "config/default.yaml"),
        ("relations", "list", "--config", "config/default.yaml"),
    }

    parser = _build_parser()
    for command in commands:
        if command == ["--help"]:
            with pytest.raises(SystemExit) as error:
                parser.parse_args(_normalize_config_position(command))
            assert error.value.code == 0
            continue
        parser.parse_args(_normalize_config_position(command))


def test_documented_help_command_exits_without_network_or_database_access() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "predmarket", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert re.search(r"^usage:", result.stdout, re.MULTILINE)


def _reset_config(tmp_path: Path, database_path: Path) -> Path:
    config = tmp_path / "reset.yaml"
    config.write_text(
        Path("config/default.yaml")
        .read_text()
        .replace("data/predmarket-v1.sqlite3", str(database_path))
    )
    return config


def test_documented_reset_helper_only_removes_exact_temporary_sqlite_files(
    tmp_path: Path,
) -> None:
    database = tmp_path / "reset.sqlite3"
    siblings = (database, Path(f"{database}-wal"), Path(f"{database}-shm"))
    for path in siblings:
        path.write_text("test database content")

    plan = prepare_reset(_reset_config(tmp_path, database))

    assert plan.main_path == database
    assert execute_reset(plan, running_processes=()) == siblings
    assert all(not path.exists() for path in siblings)


def test_reset_helper_refuses_a_configured_directory(tmp_path: Path) -> None:
    with pytest.raises(ResetRefused, match="not a directory"):
        prepare_reset(_reset_config(tmp_path, tmp_path))
    assert tmp_path.exists()


def test_reset_helper_refuses_when_predmarket_is_running(tmp_path: Path) -> None:
    database = tmp_path / "reset.sqlite3"
    database.write_text("test database content")
    plan = prepare_reset(_reset_config(tmp_path, database))

    with pytest.raises(ResetRefused, match="process is running"):
        execute_reset(plan, running_processes=(12345,))

    assert database.exists()


def test_reset_helper_refuses_parent_directory_replaced_by_symlink(
    tmp_path: Path,
) -> None:
    approved_parent = tmp_path / "approved"
    neighbor_parent = tmp_path / "neighbor"
    approved_parent.mkdir()
    neighbor_parent.mkdir()
    database = approved_parent / "reset.sqlite3"
    for path in (database, Path(f"{database}-wal"), Path(f"{database}-shm")):
        path.write_text("approved database")
    plan = prepare_reset(_reset_config(tmp_path, database))

    for name in ("reset.sqlite3", "reset.sqlite3-wal", "reset.sqlite3-shm"):
        (neighbor_parent / name).write_text("neighbor must survive")
    approved_parent.rename(tmp_path / "original-approved")
    approved_parent.symlink_to(neighbor_parent, target_is_directory=True)

    with pytest.raises(ResetRefused, match="parent directory"):
        execute_reset(plan, running_processes=())

    assert [
        (neighbor_parent / name).read_text()
        for name in ("reset.sqlite3", "reset.sqlite3-wal", "reset.sqlite3-shm")
    ] == ["neighbor must survive"] * 3


def test_reset_helper_detects_a_running_predmarket_process(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "predmarket.operator_reset.subprocess.run",
        Mock(
            return_value=subprocess.CompletedProcess(
                args=["ps"],
                returncode=0,
                stdout="4242 python -m predmarket run --config config/default.yaml\n",
            )
        ),
    )

    assert running_predmarket_processes() == (4242,)


def test_reset_helper_detects_console_script_but_not_ordinary_predmarket_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "predmarket.operator_reset.subprocess.run",
        Mock(
            return_value=subprocess.CompletedProcess(
                args=["ps"],
                returncode=0,
                stdout=(
                    "4242 /venv/bin/predmarket run --config config/default.yaml\n"
                    "4243 python -m pytest tests/predmarket/test_reset.py\n"
                    "4244 /tmp/predmarket-notes.txt\n"
                ),
            )
        ),
    )

    assert running_predmarket_processes() == (4242,)
