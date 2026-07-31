from __future__ import annotations

import os

import pytest

from predmarket.polymarket.gateway import PolymarketGateway


pytestmark = pytest.mark.skipif(
    os.environ.get("POLYMARKET_LIVE_READONLY") != "1",
    reason="set POLYMARKET_LIVE_READONLY=1 to enable the read-only SDK smoke test",
)


async def test_live_official_sdk_public_surface_is_readable() -> None:
    gateway = PolymarketGateway()
    try:
        events = await gateway.list_active_events()
        markets = await gateway.list_active_markets()
        assert events
        assert markets
        token_ids = tuple(
            token.id
            for snapshot in markets
            for token in snapshot.tokens
        )[:2]
        assert token_ids
        assert await gateway.get_order_books(token_ids)
    finally:
        await gateway.close()
