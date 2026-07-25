"""Read-only command line interface."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Sequence
from decimal import Decimal
import json
import sys

from predmarket.commands import dispatch


DESCRIPTION = (
    "read-only prediction-market structural-arbitrage scanner; no orders, "
    "wallets, or guaranteed profit. Return threshold semantics: 0.75% = 0.0075."
)


def _positive(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("--config", default="config/default.yaml")
    parser.add_argument("--json", action="store_true", dest="json_output")
    commands = parser.add_subparsers(dest="command", required=True)

    sync = commands.add_parser("sync-markets", help="summarize public market catalog")
    sync.add_argument("--limit", type=_positive, default=100)
    sync.add_argument("--max-pages", type=_positive, default=10)
    sync.add_argument("--max-markets", type=_positive, default=1000)
    sync.add_argument("--rules-dir", default="rules")

    scan = commands.add_parser("scan-once", help="confirm candidates with REST")
    scan.add_argument("--limit", type=_positive, default=100)
    scan.add_argument("--condition")
    scan.add_argument("--yes-token")
    scan.add_argument("--no-token")
    scan.add_argument("--rules-dir", default="rules")
    scan.add_argument("--relation-id")

    watch = commands.add_parser("watch", help="discover via public WebSocket")
    watch.add_argument("--max-connections", type=_positive, default=10)
    watch.add_argument("--max-events", type=_positive)
    watch.add_argument("--rules-dir", default="rules")
    watch.add_argument("--relation-id")

    relations = commands.add_parser("relations", help="manage audited rule files")
    relations.add_argument("--rules-dir", default="rules")
    relation_commands = relations.add_subparsers(
        dest="relation_command", required=True
    )
    relation_commands.add_parser("list")
    validate = relation_commands.add_parser("validate")
    validate.add_argument("path")
    import_command = relation_commands.add_parser("import")
    import_command.add_argument("path")

    replay = commands.add_parser("replay", help="replay immutable evidence")
    replay.add_argument("opportunity_id", nargs="?")
    replay.add_argument("--bundle-id")
    report = commands.add_parser("report", help="bounded evidence summary")
    report.add_argument("--limit", type=_positive, default=100)
    return parser


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def main(
    argv: Sequence[str] | None = None,
    *,
    dispatcher: Callable[[argparse.Namespace], Awaitable[object]] = dispatch,
) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code)
    try:
        result = asyncio.run(dispatcher(args))
    except (FileNotFoundError, ValueError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 1
    except Exception as exc:
        print(f"operational error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    if args.json_output:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=_json_default))
    elif result is not None:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=_json_default, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
