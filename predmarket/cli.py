"""Command-line interface for the Greenfield signal service."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Mapping, Sequence
import json
from pathlib import Path
import sqlite3
import sys
import time
from typing import TextIO

from predmarket.catalog.relations import (
    RelationAnalyzer,
    RelationCliStore,
    RelationWorkflow,
)
from predmarket.config import AppConfig
from predmarket.domain.decimal import encode_decimal
from predmarket.domain.relation import Relation


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO | None = None,
    analyzer: RelationAnalyzer | None = None,
    now_ms: Callable[[], int] | None = None,
) -> int:
    parser = _build_parser()
    arguments = parser.parse_args(argv)
    config = AppConfig.load(arguments.config)
    output = sys.stdout if stdout is None else stdout
    clock = now_ms or (lambda: time.time_ns() // 1_000_000)

    if arguments.command == "run":
        from predmarket.app import Supervisor

        return asyncio.run(Supervisor(config, terminal=output).run())

    if arguments.command == "status":
        _write_json(output, _ReadOnlyCliStore(config.database.path).status())
        return 0

    if arguments.command == "signals":
        store = _ReadOnlyCliStore(config.database.path)
        if arguments.signals_command == "list":
            _write_json(output, store.list_signals())
            return 0
        if arguments.signals_command == "show":
            signal = store.get_signal(arguments.signal_id)
            if signal is None:
                raise ValueError(f"signal {arguments.signal_id!r} does not exist")
            _write_json(output, signal)
            return 0
        parser.error("a signals command is required")

    if arguments.command != "relations":
        parser.error("a command is required")

    store = RelationCliStore(
        config.database.path,
        busy_timeout_ms=config.database.busy_timeout_ms,
    )

    if arguments.relations_command == "list":
        _write_json(output, [_relation_payload(relation) for relation in store.list()])
        return 0
    if arguments.relations_command == "show":
        relation = asyncio.run(store.get(arguments.relation_id))
        if relation is None:
            raise ValueError(f"relation {arguments.relation_id!r} does not exist")
        _write_json(output, _relation_payload(relation))
        return 0
    if arguments.relations_command == "analyze":
        if not config.relations.llm_enabled:
            raise ValueError("relation LLM analysis is disabled")
        if analyzer is None:
            raise ValueError("relation analyzer is not configured")
        workflow = RelationWorkflow(store, analyzer, llm_enabled=True)
        relation = asyncio.run(
            workflow.analyze(arguments.relation_id, updated_at=clock())
        )
        _write_json(output, _relation_payload(relation))
        return 0
    if arguments.relations_command == "approve":
        relation = store.approve_manual(
            arguments.relation_id,
            occurred_at=clock(),
        )
        _write_json(output, _relation_payload(relation))
        return 0
    parser.error("a relations command is required")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket signal service")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/default.yaml"),
        help="configuration YAML path",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="run the read-only market signal service")
    commands.add_parser("status", help="show local service database status")
    signals = commands.add_parser("signals", help="inspect persisted signals")
    signal_commands = signals.add_subparsers(dest="signals_command", required=True)
    signal_commands.add_parser("list", help="list persisted signals")
    signal_show = signal_commands.add_parser("show", help="show one signal")
    signal_show.add_argument("signal_id")
    relations = commands.add_parser(
        "relations",
        help="inspect, analyze, and manually approve implication relations",
    )
    relation_commands = relations.add_subparsers(
        dest="relations_command",
        required=True,
    )
    relation_commands.add_parser("list", help="list all relations")
    show = relation_commands.add_parser("show", help="show one relation")
    show.add_argument("relation_id")
    analyze = relation_commands.add_parser(
        "analyze",
        help="run the configured analyzer for one relation",
    )
    analyze.add_argument("relation_id")
    approve = relation_commands.add_parser(
        "approve",
        help="manually approve an LLM-recommended relation",
    )
    approve.add_argument("relation_id")
    return parser


def _relation_payload(relation: Relation) -> dict[str, object]:
    return {
        "id": relation.id,
        "market_a_id": relation.market_a_id,
        "market_b_id": relation.market_b_id,
        "status": relation.status.value,
        "discovery_source": relation.discovery_source.value,
        "llm_confidence": (
            None
            if relation.llm_confidence is None
            else encode_decimal(relation.llm_confidence)
        ),
        "llm_analysis": (
            None
            if relation.llm_analysis is None
            else _thaw_json(relation.llm_analysis)
        ),
        "created_at": relation.created_at,
        "updated_at": relation.updated_at,
    }


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _write_json(output: TextIO, payload: object) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        file=output,
    )


class _ReadOnlyCliStore:
    """The CLI's dedicated read-only SQLite boundary."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    def status(self) -> dict[str, object]:
        with self._connect() as connection:
            signals = int(
                connection.execute("SELECT COUNT(*) FROM arbitrage_signals").fetchone()[0]
            )
            events = int(
                connection.execute("SELECT COUNT(*) FROM system_events").fetchone()[0]
            )
        return {"database": str(self._path), "signals": signals, "system_events": events}

    def list_signals(self) -> list[dict[str, object]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM arbitrage_signals ORDER BY opened_at DESC, CAST(id AS BLOB)"
            ).fetchall()
        return [_signal_payload(row) for row in rows]

    def get_signal(self, signal_id: str) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM arbitrage_signals WHERE id = ?", (signal_id,)
            ).fetchone()
        return None if row is None else _signal_payload(row)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"file:{self._path}?mode=ro", uri=True, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection


def _signal_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": str(row["id"]),
        "opportunity_key": str(row["opportunity_key"]),
        "strategy_type": str(row["strategy_type"]),
        "market_ids": list(json.loads(str(row["market_ids_json"]))),
        "relation_id": row["relation_id"],
        "execution_mode": str(row["execution_mode"]),
        "status": str(row["status"]),
        "opened_at": int(row["opened_at"]),
        "updated_at": int(row["updated_at"]),
        "closed_at": row["closed_at"],
        "close_reason": row["close_reason"],
        "latest_revision": int(row["latest_revision"]),
    }
