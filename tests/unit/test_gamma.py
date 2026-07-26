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
        result = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets(limit=100)

    assert [market.market_id for market in result] == ["101", "102"]
    assert result[0].yes_token_id == "yes-101"
    assert result[0].event is not None
    assert result[0].event.event_id == "event-10"
    assert result[0].event.slug == "example-event"
    assert result[0].event.title == "Example Event"
    assert result[0].fee_schedule_source == "feeSchedule"
    fee_schedule = json.loads(result[0].fee_schedule_source_json)
    assert fee_schedule == {
        "exponent": 2,
        "rate": 0.02,
        "takerOnly": True,
        "rebateRate": 0.2,
    }
    assert type(fee_schedule["exponent"]) is int
    assert type(fee_schedule["rate"]) is float
    assert type(fee_schedule["takerOnly"]) is bool
    assert type(fee_schedule["rebateRate"]) is float
    assert result[1].fee_schedule_source is None
    assert result[1].fee_schedule_source_json is None
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
        result = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
    assert [market.market_id for market in result] == ["103"]
    assert result[0].event is not None
    assert result[0].event.event_id == "event-103"
    assert json.loads(result[0].event.source_metadata_json)["ticker"] == "EVENT103"
    assert result[0].fee_schedule_source == "feeSchedule"
    assert set(json.loads(result[0].fee_schedule_source_json)) == {
        "exponent",
        "rate",
        "takerOnly",
        "rebateRate",
    }


@pytest.mark.asyncio
async def test_malformed_market_is_diagnosed_not_silently_invented():
    payload = fixture("gamma_page_1.json")
    payload["next_cursor"] = ""
    payload["markets"].append(
        {"id": "bad", "conditionId": "c", "question": "bad", "outcomes": ["Yes"], "clobTokenIds": ["x"]}
    )
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        result = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets(max_pages=1)
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
        result = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
    assert result.markets == ()
    assert result.diagnostics[0].reason == "market is not tradeable"


@pytest.mark.asyncio
async def test_zero_and_multiple_nested_events_are_deterministic():
    payload = fixture("gamma_page_1.json")
    payload["next_cursor"] = ""
    payload["markets"][0]["events"] = []
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        zero = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
    assert zero[0].events == () and zero[0].event is None

    payload["markets"][0]["events"] = [
        {"id": "e1", "title": "One"},
        {"id": "e2", "slug": "two"},
    ]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    ) as http:
        multiple = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
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
        result = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
    assert result.markets == ()
    assert result.diagnostics[0].market_id == "101"


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 101, True, 1.0])
async def test_limit_is_strict(limit):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None)) as http:
        with pytest.raises((TypeError, ValueError)):
            await GammaClient(http, "https://gamma-api.polymarket.com").active_markets(limit=limit)


@pytest.mark.asyncio
async def test_pagination_repetition_and_bounds_fail_closed():
    payload = {"markets": [], "next_cursor": "same"}
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        with pytest.raises(AdapterInvariantError, match="cursor"):
            await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
        with pytest.raises(AdapterInvariantError, match="max_pages"):
            await GammaClient(http, "https://gamma-api.polymarket.com").active_markets(max_pages=1)


@pytest.mark.asyncio
async def test_intentional_page_bound_returns_explicit_partial_discovery():
    payload = fixture("gamma_page_1.json")
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as http:
        result = await GammaClient(http, "https://gamma-api.polymarket.com").active_markets(
            max_pages=1, max_markets=100, allow_partial=True
        )
    assert [market.market_id for market in result.markets] == ["101"]
    assert result.complete is False
    assert result.next_cursor == "next/+=="
    assert result.termination == "max_pages"
    assert result.diagnostics[-1].reason == "catalog truncated by max_pages"


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
            await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()


@pytest.mark.asyncio
async def test_transport_failures_are_typed():
    def handler(_: httpx.Request):
        raise httpx.ConnectError("offline")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(AdapterTransportError):
            await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()


@pytest.mark.asyncio
async def test_base_url_and_auth_defaults_are_rejected():
    async with httpx.AsyncClient(headers={"Authorization": "secret"}) as http:
        with pytest.raises(ValueError, match="credential"):
            GammaClient(http, "https://gamma-api.polymarket.com")
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValueError):
            GammaClient(http, "https://user:pass@gamma.test/path")


@pytest.mark.parametrize(
    "origin",
    [
        "https://gamma.evil.example",
        "http://gamma-api.polymarket.com",
        "https://gamma-api.polymarket.com:443",
        "https://gamma-api.polymarket.com/path",
        "https://user@gamma-api.polymarket.com",
    ],
)
def test_gamma_rejects_every_nonofficial_origin(origin):
    with pytest.raises(ValueError, match="official public origin"):
        GammaClient(base_url=origin, transport=httpx.MockTransport(lambda _: None))


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ["market", "condition", "token"])
async def test_identifiers_must_be_unique_across_keyset_pages(collision):
    first = fixture("gamma_page_1.json")
    second = fixture("gamma_page_2.json")
    market = second["markets"][0]
    if collision == "market":
        market["id"] = first["markets"][0]["id"]
    elif collision == "condition":
        market["conditionId"] = first["markets"][0]["conditionId"]
    else:
        market["clobTokenIds"][0] = json.loads(first["markets"][0]["clobTokenIds"])[0]

    def handler(request):
        payload = first if request.url.params.get("after_cursor") is None else second
        return httpx.Response(200, json=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(AdapterInvariantError, match="duplicate"):
            await GammaClient(http, "https://gamma-api.polymarket.com").active_markets()
