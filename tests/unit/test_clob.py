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
    assert zero.base_fee_bps == 0 and zero.rate == Decimal("0")
    assert nonzero.base_fee_bps == 500
    assert not hasattr(zero, "schedule") and not hasattr(nonzero, "schedule")
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


@pytest.mark.asyncio
async def test_market_info_binds_tokens_and_exact_complete_fee_schedule():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=fixture("clob_market_info.json"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        info = await ClobRestClient(
            http, "https://clob.test", wall_clock_ms=lambda: 100, monotonic=lambda: 2
        ).market_info("condition/101")
    assert info.condition_id == "condition/101"
    assert [(token.token_id, token.outcome) for token in info.tokens] == [
        ("yes-101", "Yes"),
        ("no-101", "No"),
    ]
    assert info.fee_schedule.rate == Decimal("0.02")
    assert info.fee_schedule.exponent == 2
    assert info.fee_schedule.taker_only is True
    assert info.minimum_order_size == Decimal("5")
    assert info.tick_size == Decimal("0.01")
    assert info.maker_base_fee_bps == 0 and info.taker_base_fee_bps == 0
    assert dict(info.bound_fee_schedules()) == {
        "yes-101": info.fee_schedule,
        "no-101": info.fee_schedule,
    }
    assert b"condition%2F101" in seen[0].url.raw_path


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "duplicate_token",
        "duplicate_outcome",
        "empty_token",
        "float_exponent",
        "bool_exponent",
        "float_from_caller",
        "bad_taker",
        "negative_base_fee",
        "bad_tick",
    ],
)
async def test_market_info_malformed_evidence_fails_closed(mutation):
    payload = fixture("clob_market_info.json")
    if mutation == "duplicate_token":
        payload["t"][1]["t"] = payload["t"][0]["t"]
    elif mutation == "duplicate_outcome":
        payload["t"][1]["o"] = payload["t"][0]["o"]
    elif mutation == "empty_token":
        payload["t"][0]["t"] = ""
    elif mutation == "float_exponent":
        payload["fd"]["e"] = 2.0
    elif mutation == "bool_exponent":
        payload["fd"]["e"] = True
    elif mutation == "float_from_caller":
        # A programmatic MockTransport payload becomes binary float JSON. The
        # adapter must parse response text back into Decimal before domain use.
        payload["fd"]["r"] = 0.1
    elif mutation == "bad_taker":
        payload["fd"]["to"] = 1
    elif mutation == "negative_base_fee":
        payload["tbf"] = -1
    else:
        payload["mts"] = 2
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        if mutation == "float_from_caller":
            info = await ClobRestClient(http, "https://clob.test").market_info("condition")
            assert info.fee_schedule.rate == Decimal("0.1")
        else:
            with pytest.raises((AdapterPayloadError, AdapterInvariantError)):
                await ClobRestClient(http, "https://clob.test").market_info("condition")


@pytest.mark.asyncio
async def test_market_fee_curve_and_token_fee_rate_are_independent_evidence():
    payload = fixture("clob_market_info.json")

    def handler(request):
        if request.url.path == "/fee-rate":
            return httpx.Response(200, json={"base_fee": 500})
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ClobRestClient(http, "https://clob.test")
        info = await client.market_info("condition-101")
        yes = await client.fee_rate("yes-101")
    assert info.taker_base_fee_bps == 0
    assert info.fee_schedule.rate == Decimal("0.02")
    assert yes.base_fee_bps == 500 and yes.rate == Decimal("0.05")
    assert yes.provenance == "GET /fee-rate"
    assert dict(info.bound_fee_schedules())["yes-101"].rate == Decimal("0.02")


@pytest.mark.asyncio
async def test_market_info_zero_fee_is_evidenced_without_special_inference():
    payload = fixture("clob_market_info.json")
    payload["fd"]["r"] = 0
    payload["tbf"] = 0
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        client = ClobRestClient(http, "https://clob.test")
        info = await client.market_info("condition-101")
    assert info.fee_schedule.rate == Decimal("0")
    assert info.fee_schedule.exponent == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["condition", "tokens", "fee_missing", "outcome_case"])
async def test_market_info_binding_mismatches_fail_closed(mismatch):
    payload = fixture("clob_market_info.json")
    expected = None
    if mismatch == "condition":
        payload["condition_id"] = "different-condition"
    elif mismatch == "tokens":
        expected = ["yes-101", "different-token"]
    elif mismatch == "fee_missing":
        del payload["fd"]["e"]
    else:
        payload["t"][1]["o"] = "YES"
        payload["t"][0]["o"] = "Yes"
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises((AdapterPayloadError, AdapterInvariantError)):
            await ClobRestClient(http, "https://clob.test").market_info(
                "condition-101", expected_token_ids=expected
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("condition_id", ["", " ", "..", "bad\nid"])
async def test_market_info_rejects_unsafe_condition_id(condition_id):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None)) as http:
        with pytest.raises((TypeError, ValueError)):
            await ClobRestClient(http, "https://clob.test").market_info(condition_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", ["authorization", "cookie", "poly", "auth"])
@pytest.mark.parametrize("adapter_kind", ["gamma", "clob"])
async def test_mutated_injected_credentials_are_rejected_before_send(credential, adapter_kind):
    calls = 0

    def handler(_):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"markets": [], "next_cursor": ""})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        if adapter_kind == "gamma":
            from predmarket.polymarket.gamma import GammaClient

            adapter = GammaClient(http, "https://gamma.test")
        else:
            adapter = ClobRestClient(http, "https://clob.test")
        if credential == "authorization":
            http.headers["Authorization"] = "secret"
        elif credential == "cookie":
            http.cookies.set("session", "secret")
        elif credential == "poly":
            http.headers["POLY_API_KEY"] = "secret"
        else:
            http._auth = httpx.BasicAuth("name", "secret")
        with pytest.raises(AdapterInvariantError, match="credential|authentication"):
            if adapter_kind == "gamma":
                await adapter.active_markets()
            else:
                await adapter.fee_rate("token")
    assert calls == 0
