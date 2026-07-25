from decimal import Decimal
import json
from pathlib import Path

import httpx
import pytest

from predmarket.polymarket import AdapterHTTPError, AdapterInvariantError, AdapterPayloadError
from predmarket.polymarket.clob import ClobRestClient


FIXTURES = Path(__file__).parents[1] / "fixtures" / "polymarket"


def fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.asyncio
async def test_books_request_and_full_depth_are_exact_and_input_ordered():
    seen = []

    def handler(request: httpx.Request):
        seen.append(request)
        return httpx.Response(200, json=list(reversed(fixture("clob_books.json"))))

    wall = iter([1760000000100])
    mono = iter([123.25])
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        records = await ClobRestClient(
            http, "https://clob.test", wall_clock_ms=lambda: next(wall), monotonic=lambda: next(mono)
        ).books(["yes-101", "no-101"])

    assert [r.token_id for r in records] == ["yes-101", "no-101"]
    assert records[0].book.asks[0].price == Decimal("0.49")
    assert len(records[0].book.bids) == 2
    assert records[0].received_at_ms == 1760000000100
    assert records[0].received_monotonic == 123.25
    assert seen[0].method == "POST" and seen[0].url.path == "/books"
    assert json.loads(seen[0].content) == [{"token_id": "yes-101"}, {"token_id": "no-101"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("tokens", [[], ["x", "x"], [""], [1], ["x"] * 501])
async def test_book_batch_contract_is_strict(tokens):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None)) as http:
        with pytest.raises((TypeError, ValueError)):
            await ClobRestClient(http, "https://clob.test").books(tokens)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate", "float", "crossed", "iso_time"])
async def test_book_response_invariants_fail_closed(mutation):
    payload = fixture("clob_books.json")
    if mutation == "missing":
        payload.pop()
    elif mutation == "extra":
        payload.append({**payload[0], "asset_id": "extra", "hash": "extra"})
    elif mutation == "duplicate":
        payload[1] = {**payload[0]}
    elif mutation == "float":
        payload[0]["asks"][0]["price"] = 0.49
    elif mutation == "crossed":
        payload[0]["bids"][0]["price"] = "0.50"
    else:
        payload[0]["timestamp"] = "2026-01-01T00:00:00Z"
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises((AdapterPayloadError, AdapterInvariantError)):
            await ClobRestClient(http, "https://clob.test").books(["yes-101", "no-101"])


@pytest.mark.asyncio
async def test_fee_zero_is_safe_but_nonzero_does_not_invent_exponent():
    calls = 0

    def handler(_: httpx.Request):
        nonlocal calls
        calls += 1
        name = "clob_fee_zero.json" if calls == 1 else "clob_fee_nonzero.json"
        return httpx.Response(200, json=fixture(name))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ClobRestClient(http, "https://clob.test", wall_clock_ms=lambda: 99, monotonic=lambda: 1.5)
        zero = await client.fee_rate("yes-101")
        nonzero = await client.fee_rate("no-101")
    assert zero.base_fee_bps == 0 and zero.schedule is not None
    assert zero.schedule.rate == Decimal("0")
    assert nonzero.base_fee_bps == 500 and nonzero.schedule is None
    assert nonzero.rate == Decimal("0.05")
    assert nonzero.provenance == "GET /fee-rate"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [True, -1, 1.5, "500"])
async def test_fee_base_fee_is_strict_integer(value):
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"base_fee": value}))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(AdapterPayloadError):
            await ClobRestClient(http, "https://clob.test").fee_rate("x")


@pytest.mark.asyncio
async def test_clob_http_error_is_typed_and_exact_query_is_used():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(AdapterHTTPError):
            await ClobRestClient(http, "https://clob.test").fee_rate("a/+")
    assert seen[0].method == "GET" and seen[0].url.path == "/fee-rate"
    assert seen[0].url.params["token_id"] == "a/+"


@pytest.mark.asyncio
async def test_owned_client_context_lifecycle():
    client = ClobRestClient(base_url="https://clob.test", transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"base_fee": 0})))
    async with client:
        assert (await client.fee_rate("x")).base_fee_bps == 0
    with pytest.raises(RuntimeError, match="closed"):
        await client.fee_rate("x")
