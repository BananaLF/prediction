"""Command-line interface for the Greenfield signal service."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Mapping, Sequence
import json
from pathlib import Path
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
    if arguments.command != "relations":
        parser.error("a command is required")

    config = AppConfig.load(arguments.config)
    store = RelationCliStore(
        config.database.path,
        busy_timeout_ms=config.database.busy_timeout_ms,
    )
    output = sys.stdout if stdout is None else stdout
    clock = now_ms or (lambda: time.time_ns() // 1_000_000)

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
