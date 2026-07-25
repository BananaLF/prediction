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
    assert result[1].yes_token_id == "yes-102"
    assert result[1].no_token_id == "no-102"
    assert result.diagnostics == ()
    assert requests[0].method == "GET"
    assert requests[0].url.path == "/markets/keyset"
    assert dict(requests[0].url.params) == {"limit": "100", "closed": "false"}
    assert requests[1].url.params["after_cursor"] == "next/+=="
    assert not any(k.lower() in {"authorization", "x-api-key", "poly-signature"} for k in requests[0].headers)


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
