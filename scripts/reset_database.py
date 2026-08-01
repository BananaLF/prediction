"""Safely remove one configured SQLite database for an operator-approved reset."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

# Direct script execution places only ``scripts/`` on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from predmarket.operator_reset import ResetRefused, execute_reset, prepare_reset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="delete the validated main SQLite file and its exact -wal/-shm siblings",
    )
    arguments = parser.parse_args(argv)
    try:
        plan = prepare_reset(arguments.config)
        print(f"configured SQLite main path: {plan.main_path}")
        print("reset targets:")
        for target in plan.targets:
            print(target)
        if not arguments.execute:
            print("dry run only; pass --execute after stopping predmarket")
            return 0
        for target in execute_reset(plan):
            print(f"deleted: {target}")
        return 0
    except ResetRefused as error:
        print(f"refusing database reset: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
