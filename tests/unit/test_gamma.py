import json
from pathlib import Path

import httpx
import pytest

from predmarket.polymarket import (
    AdapterHTTPError,
    AdapterInvariantError,
    AdapterPayloadError,
    AdapterTransportError,
)
from predmarket.polymarket.gamma import GammaClient


FIXTURES = Path(__file__).parents[1] / "fixtures" / "polymarket"


def fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.asyncio
async def test_keyset_pagination_preserves_cursor_and_normalizes_binary_tokens():
    requests = []

    def handler(request: httpx.Request):
        requests.append(request)
        cursor = request.url.params.get("after_cursor")
        payload = fixture("gamma_page_1.json") if cursor is None else fixture("gamma_page_2.json")
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await GammaClient(http, "https://gamma.test").active_markets(limit=100)

    assert [market.market_id for market in result] == ["101", "102"]
    assert result[0].yes_token_id == "yes-101"
    assert result[0].event is not None
    assert result[0].event.event_id == "event-10"
    assert result[0].event.slug == "example-event"
    assert result[0].event.title == "Example Event"
    assert result[0].fee_schedule_source == "feeSchedule"
    assert json.loads(result[0].fee_schedule_source_json) == {
        "baseFee": 0,
        "source": "gamma",
    }
    assert result[1].yes_token_id == "yes-102"
    assert result[1].no_token_id == "no-102"
    assert result.diagnostics == ()
    assert requests[0].method == "GET"
    assert requests[0].url.path == "/markets/keyset"
    assert dict(requests[0].url.params) == {"limit": "100", "closed": "false"}
    assert requests[1].url.params["after_cursor"] == "next/+=="
    assert not any(k.lower() in {"authorization", "x-api-key", "poly-signature"} for k in requests[0].headers)


@pytest.mark.asyncio
async def test_last_page_may_omit_cursor_and_preserves_nested_event_and_fee_evidence():
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, json=fixture("gamma_last_page_no_cursor.json"))
    )
    async with httpx.AsyncClient(transport=transport) as http:
        result = await GammaClient(http, "https://gamma.test").active_markets()
    assert [market.market_id for market in result] == ["103"]
    assert result[0].event is not None
    assert result[0].event.event_id == "event-103"
    assert json.loads(result[0].event.source_metadata_json)["ticker"] == "EVENT103"
    assert result[0].fee_schedule_source == "feeSchedule"


@pytest.mark.asyncio
async def test_malformed_market_is_diagnosed_not_silently_invented():
    payload = fixture("gamma_page_1.json")
    payload["next_cursor"] = ""
    payload["markets"].append(
        {"id": "bad", "conditionId": "c", "question": "bad", "outcomes": ["Yes"], "clobTokenIds": ["x"]}
    )
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        result = await GammaClient(http, "https://gamma.test").active_markets(max_pages=1)
    assert [m.market_id for m in result] == ["101"]
    assert len(result.diagnostics) == 1
    assert result.diagnostics[0].market_id == "bad"


@pytest.mark.asyncio
async def test_explicitly_untradeable_market_is_skipped_with_diagnostic():
    payload = fixture("gamma_page_1.json")
    payload["next_cursor"] = ""
    payload["markets"][0]["acceptingOrders"] = False
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        result = await GammaClient(http, "https://gamma.test").active_markets()
    assert result.markets == ()
    assert result.diagnostics[0].reason == "market is not tradeable"


@pytest.mark.asyncio
async def test_zero_and_multiple_nested_events_are_deterministic():
    payload = fixture("gamma_page_1.json")
    payload["next_cursor"] = ""
    payload["markets"][0]["events"] = []
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        zero = await GammaClient(http, "https://gamma.test").active_markets()
    assert zero[0].events == () and zero[0].event is None

    payload["markets"][0]["events"] = [
        {"id": "e1", "title": "One"},
        {"id": "e2", "slug": "two"},
    ]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as http:
        multiple = await GammaClient(http, "https://gamma.test").active_markets()
    assert [event.event_id for event in multiple[0].events] == ["e1", "e2"]
    assert multiple[0].event is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["bad_events", "duplicate_events", "both_event_aliases", "bad_fee", "both_fee_aliases"],
)
async def test_malformed_nested_relations_and_fee_schedule_are_diagnostics(mutation):
    payload = fixture("gamma_page_1.json")
    payload["next_cursor"] = ""
    market = payload["markets"][0]
    if mutation == "bad_events":
        market["events"] = {"id": "not-an-array"}
    elif mutation == "duplicate_events":
        market["events"] = [{"id": "same"}, {"id": "same"}]
    elif mutation == "both_event_aliases":
        market["Events"] = market["events"]
    elif mutation == "bad_fee":
        market["feeSchedule"] = 500
    else:
        market["fee_schedule"] = market["feeSchedule"]
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        result = await GammaClient(http, "https://gamma.test").active_markets()
    assert result.markets == ()
    assert result.diagnostics[0].market_id == "101"


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 101, True, 1.0])
async def test_limit_is_strict(limit):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None)) as http:
        with pytest.raises((TypeError, ValueError)):
            await GammaClient(http, "https://gamma.test").active_markets(limit=limit)


@pytest.mark.asyncio
async def test_pagination_repetition_and_bounds_fail_closed():
    payload = {"markets": [], "next_cursor": "same"}
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(AdapterInvariantError, match="cursor"):
            await GammaClient(http, "https://gamma.test").active_markets()
        with pytest.raises(AdapterInvariantError, match="max_pages"):
            await GammaClient(http, "https://gamma.test").active_markets(max_pages=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "error"),
    [
        (httpx.Response(503, text="no"), AdapterHTTPError),
        (httpx.Response(200, text="{"), AdapterPayloadError),
        (httpx.Response(200, json=[]), AdapterPayloadError),
        (httpx.Response(200, json={"markets": "bad"}), AdapterPayloadError),
    ],
)
async def test_response_failures_are_typed(response, error):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response)) as http:
        with pytest.raises(error):
            await GammaClient(http, "https://gamma.test").active_markets()


@pytest.mark.asyncio
async def test_transport_failures_are_typed():
    def handler(_: httpx.Request):
        raise httpx.ConnectError("offline")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(AdapterTransportError):
            await GammaClient(http, "https://gamma.test").active_markets()


@pytest.mark.asyncio
async def test_base_url_and_auth_defaults_are_rejected():
    async with httpx.AsyncClient(headers={"Authorization": "secret"}) as http:
        with pytest.raises(ValueError, match="credential"):
            GammaClient(http, "https://gamma.test")
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValueError):
            GammaClient(http, "https://user:pass@gamma.test/path")
