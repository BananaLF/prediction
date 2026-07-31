from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest

from predmarket.domain.fees import FeeModel
from predmarket.domain.market import Event, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook
from predmarket.polymarket.gateway import (
    MAPPING_VERSION,
    GatewayMappingError,
    MarketSnapshot,
    MarketStreamEvent,
    PolymarketGateway,
)


FIXTURES = Path("tests/fixtures/sdk")
DECIMAL_FIELDS = {
    "price",
    "size",
    "minimum_order_size",
    "minimum_tick_size",
    "rate",
    "rebate_rate",
    "min_order_size",
    "tick_size",
    "last_trade_price",
    "best_bid",
    "best_ask",
    "spread",
}
DATETIME_FIELDS = {
    "created_at",
    "updated_at",
    "published_at",
    "start_date",
    "creation_date",
    "end_date",
    "closed_time",
    "start_time",
    "finished_at",
    "game_start_time",
    "timestamp",
}


class FixtureModel(SimpleNamespace):
    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        assert mode in {"python", "json"}
        return _dump_fixture(vars(self), json_mode=mode == "json")


def _dump_fixture(value: Any, *, json_mode: bool) -> Any:
    if isinstance(value, FixtureModel):
        return {
            key: _dump_fixture(item, json_mode=json_mode)
            for key, item in vars(value).items()
        }
    if isinstance(value, dict):
        return {
            key: _dump_fixture(item, json_mode=json_mode)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return [_dump_fixture(item, json_mode=json_mode) for item in value]
    if json_mode and isinstance(value, Decimal):
        return str(value)
    if json_mode and isinstance(value, datetime):
        return value.isoformat()
    return value


def _fixture_model(value: Any, *, field_name: str | None = None) -> Any:
    if isinstance(value, dict):
        return FixtureModel(
            **{
                key: _fixture_model(item, field_name=key)
                for key, item in value.items()
            }
        )
    if isinstance(value, list):
        return tuple(_fixture_model(item, field_name=field_name) for item in value)
    if isinstance(value, str) and field_name in DATETIME_FIELDS:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(value, str) and field_name in DECIMAL_FIELDS:
        return Decimal(value)
    return value


class FakePaginator:
    def __init__(self, pages: tuple[tuple[Any, ...], ...]) -> None:
        self.pages = pages
        self.pages_yielded = 0

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for items in self.pages:
            self.pages_yielded += 1
            yield SimpleNamespace(items=items)


class FakeSubscriptionHandle:
    def __init__(self, events: tuple[Any, ...] = ()) -> None:
        self._events = iter(events)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._events)
        except StopIteration as error:
            raise StopAsyncIteration from error

    async def close(self) -> None:
        self.closed = True


class FakePublicClient:
    def __init__(
        self,
        *,
        events: tuple[Any, ...],
        markets: tuple[Any, ...],
        books: tuple[Any, ...],
        stream_events: tuple[Any, ...] = (),
    ) -> None:
        self.event_paginator = FakePaginator(((events[0],), (events[1],)))
        self.market_paginator = FakePaginator(((markets[0],), (markets[1],)))
        self.markets = markets
        self.books = books
        self.event_kwargs: dict[str, Any] | None = None
        self.market_kwargs: dict[str, Any] | None = None
        self.book_token_ids: tuple[str, ...] | None = None
        self.refreshed_market_id: str | None = None
        self.subscription_spec: Any = None
        self.subscription_handle = FakeSubscriptionHandle(stream_events)
        self.closed = False

    def list_events(self, **kwargs: Any) -> FakePaginator:
        self.event_kwargs = kwargs
        return self.event_paginator

    def list_markets(self, **kwargs: Any) -> FakePaginator:
        self.market_kwargs = kwargs
        return self.market_paginator

    async def get_market(self, *, id: str) -> Any:
        self.refreshed_market_id = id
        return next(market for market in self.markets if market.id == id)

    async def get_order_books(self, *, token_ids: tuple[str, ...]) -> tuple[Any, ...]:
        self.book_token_ids = token_ids
        return tuple(book for book in self.books if book.token_id in token_ids)

    async def subscribe(self, spec: Any) -> FakeSubscriptionHandle:
        self.subscription_spec = spec
        return self.subscription_handle

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def sdk_fixture() -> dict[str, tuple[Any, ...]]:
    events_payload = json.loads((FIXTURES / "events.json").read_text())
    books_payload = json.loads((FIXTURES / "books.json").read_text())
    return {
        "events": tuple(_fixture_model(item) for item in events_payload["events"]),
        "markets": tuple(_fixture_model(item) for item in events_payload["markets"]),
        "books": tuple(_fixture_model(item) for item in books_payload["books"]),
    }


@pytest.fixture
def fake_client(sdk_fixture: dict[str, tuple[Any, ...]]) -> FakePublicClient:
    return FakePublicClient(**sdk_fixture)


@pytest.fixture
def gateway(fake_client: FakePublicClient) -> PolymarketGateway:
    return PolymarketGateway(
        client=fake_client,
        clock_ms=lambda: 1_785_405_970_000,
        page_size=1,
    )


async def test_list_active_events_drains_every_sdk_page_and_maps_explicit_neg_risk(
    gateway: PolymarketGateway,
    fake_client: FakePublicClient,
) -> None:
    # Catches stopping after the first SDK page or inventing NegRisk completeness.
    events = await gateway.list_active_events()

    assert tuple(event.id for event in events) == ("100", "101")
    assert fake_client.event_paginator.pages_yielded == 2
    assert fake_client.event_kwargs == {"closed": False, "page_size": 1}
    event = events[0]
    assert isinstance(event, Event)
    assert event.status is MarketStatus.ACTIVE
    assert event.market_ids == ("200",)
    assert event.start_at == 1_785_405_600_000
    assert event.end_at == 1_788_084_000_000
    assert event.source_updated_at == 1_785_405_900_000
    assert event.neg_risk is True
    assert event.neg_risk_id == "neg-risk-100"
    assert event.neg_risk_type is None
    assert event.neg_risk_complete is False
    assert event.neg_risk_conversion_supported is False
    assert event.neg_risk_metadata == {
        "mapping_version": MAPPING_VERSION,
        "enable_neg_risk": True,
        "neg_risk_augmented": False,
        "cumulative_markets": False,
        "neg_risk_fee_bips": "25",
    }
    assert isinstance(event.neg_risk_metadata, MappingProxyType)


async def test_list_active_events_excludes_defensive_inactive_sdk_results(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches a closed=False SDK response leaking inactive catalog members.
    payload = deepcopy(sdk_fixture)
    payload["events"][1].state.active = False
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)

    events = await gateway.list_active_events()

    assert tuple(event.id for event in events) == ("100",)
    assert client.event_paginator.pages_yielded == 2


async def test_list_active_markets_maps_tokens_and_authoritative_fee_schedules(
    gateway: PolymarketGateway,
    fake_client: FakePublicClient,
) -> None:
    # Catches token loss, outcome reordering, or silently treating enabled fees as zero.
    snapshots = await gateway.list_active_markets()

    assert tuple(snapshot.market.id for snapshot in snapshots) == ("200", "201")
    assert fake_client.market_paginator.pages_yielded == 2
    assert fake_client.market_kwargs == {"closed": False, "page_size": 1}
    first = snapshots[0]
    assert isinstance(first, MarketSnapshot)
    assert first.mapping_version == MAPPING_VERSION
    assert first.market.event_id == "100"
    assert first.market.condition_id.endswith("1" * 64)
    assert first.market.neg_risk is True
    assert first.market.neg_risk_outcome_position is None
    assert first.market.neg_risk_member_complete is False
    assert first.market.tick_size == Decimal("0.01")
    assert first.market.minimum_order_size == Decimal("5")
    assert tuple(
        (token.id, token.outcome, token.position)
        for token in first.tokens
    ) == (("1001", "Yes", 0), ("1002", "No", 1))
    assert all(isinstance(token, Token) for token in first.tokens)
    fee = first.tokens[0].fee_schedule
    assert fee is not None
    assert fee.model is FeeModel.FLAT
    assert fee.enabled is True
    assert fee.parameters == {"rate": Decimal("0.02")}
    assert fee.updated_at == 1_785_405_970_000
    zero_fee = snapshots[1].tokens[0].fee_schedule
    assert zero_fee is not None
    assert zero_fee.model is FeeModel.ZERO
    assert zero_fee.enabled is False


async def test_get_order_books_preserves_complete_l2_and_request_coverage(
    gateway: PolymarketGateway,
    fake_client: FakePublicClient,
) -> None:
    # Catches top-of-book truncation, side reversal, or accepting partial batch responses.
    await gateway.list_active_markets()
    books = await gateway.get_order_books(("1002", "1001"))

    assert fake_client.book_token_ids == ("1002", "1001")
    assert tuple(book.token_id for book in books) == ("1002", "1001")
    assert all(isinstance(book, OrderBook) for book in books)
    first = books[1]
    assert first.market_id == "200"
    assert first.book_hash == "book-hash-1001"
    assert first.exchange_timestamp == 1_785_405_960_000
    assert first.received_timestamp == 1_785_405_970_000
    assert first.subscription_generation == 1
    assert tuple((level.price, level.size) for level in first.bids) == (
        (Decimal("0.41"), Decimal("3")),
        (Decimal("0.40"), Decimal("7")),
        (Decimal("0.39"), Decimal("11")),
    )
    assert tuple((level.price, level.size) for level in first.asks) == (
        (Decimal("0.43"), Decimal("2")),
        (Decimal("0.44"), Decimal("5")),
        (Decimal("0.45"), Decimal("13")),
    )


async def test_refresh_market_returns_fresh_immutable_market_snapshot(
    gateway: PolymarketGateway,
    fake_client: FakePublicClient,
) -> None:
    # Catches refresh bypassing the SDK get_market operation or leaking SDK models.
    snapshot = await gateway.refresh_market("200")

    assert fake_client.refreshed_market_id == "200"
    assert isinstance(snapshot, MarketSnapshot)
    assert snapshot.market.id == "200"
    assert snapshot.tokens[0].id == "1001"


async def test_subscribe_markets_uses_public_market_spec_and_maps_stream_models(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches direct WebSocket use or leaking a mutable SDK stream model downstream.
    stream_event = FixtureModel(
        type="price_change",
        payload=FixtureModel(
            market=sdk_fixture["markets"][0].condition_id,
            price_changes=(
                FixtureModel(
                    asset_id="1001",
                    price=Decimal("0.42"),
                    size=Decimal("3"),
                    side="BUY",
                    hash="delta-hash",
                    best_bid=Decimal("0.41"),
                    best_ask=Decimal("0.43"),
                ),
            ),
            timestamp=datetime.fromisoformat("2026-07-30T10:06:02+00:00"),
        ),
    )
    client = FakePublicClient(**sdk_fixture, stream_events=(stream_event,))
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001", "1002"))
    mapped = await anext(subscription)

    assert client.subscription_spec.token_ids == ("1001", "1002")
    assert client.subscription_spec.custom_feature_enabled is True
    assert isinstance(mapped, MarketStreamEvent)
    assert mapped.mapping_version == MAPPING_VERSION
    assert mapped.event_type == "price_change"
    assert mapped.market_id == "200"
    assert mapped.received_timestamp == 1_785_405_970_000
    assert mapped.payload["price_changes"][0]["price"] == "0.42"
    assert isinstance(mapped.payload, MappingProxyType)
    await subscription.close()
    assert client.subscription_handle.closed is True


@pytest.mark.parametrize(
    ("entity", "mutation", "match"),
    [
        ("event", lambda item: setattr(item, "title", None), r"event 100.*title"),
        (
            "market",
            lambda item: setattr(item.outcomes.yes, "token_id", None),
            r"market 200.*token",
        ),
        (
            "market",
            lambda item: setattr(item, "condition_id", None),
            r"market 200.*condition",
        ),
    ],
)
async def test_malformed_sdk_entities_fail_closed_with_context(
    sdk_fixture: dict[str, tuple[Any, ...]],
    entity: str,
    mutation: Any,
    match: str,
) -> None:
    # Catches partial entities being persisted with fabricated defaults.
    payload = deepcopy(sdk_fixture)
    target = payload["events"][0] if entity == "event" else payload["markets"][0]
    mutation(target)
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)

    with pytest.raises(GatewayMappingError, match=match):
        if entity == "event":
            await gateway.list_active_events()
        else:
            await gateway.list_active_markets()


async def test_incomplete_order_book_batch_is_rejected(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches a partial batch masquerading as complete recovery evidence.
    client = FakePublicClient(
        events=sdk_fixture["events"],
        markets=sdk_fixture["markets"],
        books=(sdk_fixture["books"][0],),
    )
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with pytest.raises(GatewayMappingError, match=r"order books.*missing.*1002"):
        await gateway.get_order_books(("1001", "1002"))


async def test_malformed_order_book_timestamp_is_rejected(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches accepting an L2 snapshot that cannot participate in recovery ordering.
    payload = deepcopy(sdk_fixture)
    payload["books"][0].timestamp = None
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with pytest.raises(GatewayMappingError, match=r"order book 1001.*timestamp"):
        await gateway.get_order_books(("1001",))


async def test_close_delegates_to_public_sdk_client(
    gateway: PolymarketGateway,
    fake_client: FakePublicClient,
) -> None:
    # Catches leaked HTTP/WebSocket transports during supervisor shutdown.
    await gateway.close()

    assert fake_client.closed is True
