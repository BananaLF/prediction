import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from predmarket.commands import targeted_binary_market
from predmarket.config import Settings
from predmarket.engine import (
    EngineDependencies,
    FeeConfirmation,
    StructuralArbitrageEngine,
)
from predmarket.fees import FeeSchedule
from predmarket.polymarket.clob import ClobRestClient


class Store:
    def __init__(self):
        self.items = []

    async def save(self, bundle):
        self.items.append(bundle)
        return True

    async def claim_notification(self, *_args):
        return False

    async def record_notification_attempt(self, *_args):
        return None


class Notice:
    async def notify(self, _result):
        raise AssertionError("fixture is not executable")


class Fees:
    async def confirm(self, condition_id, token_ids):
        schedule = FeeSchedule(Decimal("0"), 1, True, 1)
        return FeeConfirmation(
            condition_id, token_ids,
            {token: schedule for token in token_ids}, True, "fixture",
        )


def settings() -> Settings:
    return Settings(
        bankroll=Decimal("1000"),
        minimum_return=Decimal("0.0075"),
        safety_buffer_rate=Decimal("0"),
        conversion_cost=Decimal("0"),
        max_leg_failure_loss=Decimal("1000"),
        max_unhedged_notional=Decimal("1000"),
        maximum_book_age_ms=10_000,
        maximum_leg_skew_ms=10_000,
        maximum_processing_latency_ms=10_000,
        default_simulation_quantity=Decimal("10"),
        reconcile_interval_seconds=30,
        queue_capacity=100,
        database_path="ignored.sqlite3",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("gamma_market_id", ["target:condition-101", "101"])
async def test_actual_clob_condition_id_seam_accepts_targeted_and_catalog_ids(
    gamma_market_id,
):
    payload = json.loads(
        Path("tests/fixtures/polymarket/clob_books.json").read_text()
    )

    async def handler(request):
        assert request.url.path == "/books"
        return httpx.Response(200, json=payload)

    discovery = ClobRestClient(
        transport=httpx.MockTransport(handler),
        wall_clock_ms=lambda: 1_760_000_000_010,
        monotonic=lambda: 1.0,
    )
    confirmation = ClobRestClient(
        transport=httpx.MockTransport(handler),
        wall_clock_ms=lambda: 1_760_000_000_020,
        monotonic=lambda: 2.0,
    )
    market = targeted_binary_market(
        "condition-101", "yes-101", "no-101"
    )
    market = replace(market, market_id=gamma_market_id)
    store = Store()
    engine = StructuralArbitrageEngine(
        EngineDependencies(
            discovery, confirmation, Fees(), store, Notice(), settings(),
            lambda: 1_760_000_000_030, lambda: 2.1,
            lambda _market: "opp-seam", lambda: gamma_market_id.replace(":", "-"),
            "test",
        )
    )
    try:
        result = await engine.scan_binary(market)
    finally:
        await discovery.close()
        await confirmation.close()
    assert result.reason not in {"invalid_discovery", "invalid_confirmation"}


@pytest.mark.asyncio
async def test_actual_clob_condition_mismatch_rejects():
    payload = json.loads(
        Path("tests/fixtures/polymarket/clob_books.json").read_text()
    )
    payload[0]["market"] = "wrong-condition"

    async def handler(_request):
        return httpx.Response(200, json=payload)

    first = ClobRestClient(
        transport=httpx.MockTransport(handler),
        wall_clock_ms=lambda: 1_760_000_000_010,
        monotonic=lambda: 1.0,
    )
    second = ClobRestClient(
        transport=httpx.MockTransport(handler),
        wall_clock_ms=lambda: 1_760_000_000_020,
        monotonic=lambda: 2.0,
    )
    market = targeted_binary_market("condition-101", "yes-101", "no-101")
    store = Store()
    engine = StructuralArbitrageEngine(
        EngineDependencies(
            first, second, Fees(), store, Notice(), settings(),
            lambda: 1_760_000_000_030, lambda: 2.1,
            lambda _market: "opp-mismatch", lambda: "run-mismatch", "test",
        )
    )
    try:
        result = await engine.scan_binary(market)
    finally:
        await first.close()
        await second.close()
    assert result.reason == "invalid_discovery"
