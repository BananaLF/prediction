#!/usr/bin/env python3
"""Replay persisted signal-leg economics without network or source database."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path


def replay(bundle: dict) -> list[dict[str, str | int]]:
    revisions = {
        (row["signal_id"], row["revision"]): row for row in bundle["revisions"]
    }
    grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for leg in bundle["legs"]:
        grouped[(leg["signal_id"], leg["revision"])].append(leg)
    snapshots = {
        (row["signal_id"], row["revision"], row["token_id"]): row
        for row in bundle["snapshots"]
    }
    levels: dict[str, list[dict]] = defaultdict(list)
    for row in bundle["levels"]:
        levels[row["snapshot_id"]].append(row)
    schedules = {
        row["token_id"]: json.loads(row["fee_schedule_json"])
        for row in bundle["token_fee_schedules"]
    }

    results = []
    for key, legs in sorted(grouped.items()):
        revision = revisions[key]
        capital = Decimal(revision["total_capital"])
        buys = [leg for leg in legs if leg["action"] == "BUY"]
        sells = [leg for leg in legs if leg["action"] == "SELL"]
        conversions = [
            leg for leg in legs if leg["action"] in {"MERGE", "REDEEM", "NEG_RISK_CONVERT"}
        ]
        for leg in (*buys, *sells):
            snapshot = snapshots[(key[0], key[1], leg["token_id"])]
            side = "ASK" if leg["action"] == "BUY" else "BID"
            fill_levels = [row for row in levels[snapshot["id"]] if row["side"] == side]
            gross, average, worst = _walk_depth(fill_levels, Decimal(leg["quantity"]))
            _equal(leg, "gross_amount", gross, key)
            _equal(leg, "average_price", average, key)
            _equal(leg, "worst_price", worst, key)
            fee = _fee(schedules[leg["token_id"]], average, Decimal(leg["quantity"]))
            _equal(leg, "fee_amount", fee, key)
        if buys:
            trade_cost = sum(
                (Decimal(leg["gross_amount"]) + Decimal(leg["fee_amount"]) for leg in buys),
                Decimal(0),
            )
            safety_buffer = capital - trade_cost
            if safety_buffer < 0:
                raise ValueError(f"negative safety buffer for {key}")
            proceeds = sum((Decimal(leg["gross_amount"]) for leg in conversions), Decimal(0))
            replayed_profit = proceeds - capital
        elif sells:
            proceeds = sum(
                (Decimal(leg["gross_amount"]) - Decimal(leg["fee_amount"]) for leg in sells),
                Decimal(0),
            )
            split_capital = sum(
                (Decimal(leg["gross_amount"]) for leg in legs if leg["action"] == "SPLIT"),
                Decimal(0),
            )
            safety_buffer = capital - split_capital
            if safety_buffer < 0:
                raise ValueError(f"negative safety buffer for {key}")
            replayed_profit = proceeds - capital
        else:
            raise ValueError(f"unsupported leg set for {key}")
        persisted_profit = Decimal(revision["expected_profit"])
        if replayed_profit != persisted_profit:
            raise ValueError(f"profit mismatch for {key}: {replayed_profit} != {persisted_profit}")
        results.append(
            {
                "signal_id": key[0],
                "revision": key[1],
                "replayed_expected_profit": str(replayed_profit),
                "persisted_expected_profit": str(persisted_profit),
                "classification": "orderbook_checked_estimate",
                "inferred_safety_buffer": str(safety_buffer),
            }
        )
    return results


def _walk_depth(levels: list[dict], quantity: Decimal) -> tuple[Decimal, Decimal, Decimal]:
    remaining = quantity
    gross = Decimal(0)
    worst = None
    for level in levels:
        if remaining <= 0:
            break
        taken = min(remaining, Decimal(level["size"]))
        price = Decimal(level["price"])
        gross += taken * price
        remaining -= taken
        worst = price
    if remaining or worst is None:
        raise ValueError(f"insufficient depth: missing {remaining}")
    return gross, gross / quantity, worst


def _fee(schedule: dict, price: Decimal, quantity: Decimal) -> Decimal:
    if not schedule["enabled"] or schedule["model"] == "ZERO":
        return Decimal(0)
    parameters = schedule["parameters"]
    if schedule["model"] == "FLAT":
        return price * quantity * Decimal(parameters["rate"])
    if schedule["model"] == "CURVE":
        amount = quantity * Decimal(parameters["rate"]) * (
            price * (Decimal(1) - price)
        ) ** Decimal(parameters["exponent"])
        return amount.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    raise ValueError(f"unknown fee model: {schedule['model']}")


def _equal(leg: dict, field: str, replayed: Decimal, key: tuple[str, int]) -> None:
    persisted = Decimal(leg[field])
    if replayed != persisted:
        raise ValueError(f"{field} mismatch for {key}: {replayed} != {persisted}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    arguments = parser.parse_args()
    bundle = json.loads(arguments.evidence.read_text(encoding="utf-8"))
    print(json.dumps(replay(bundle), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
