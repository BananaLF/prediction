from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import importlib.metadata
import json
import logging
from pathlib import Path
import re
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest
from polymarket.errors import RequestRejectedError

from predmarket.polymarket import gateway as gateway_module
from predmarket.domain.fees import FeeModel
from predmarket.domain.market import Event, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook
from predmarket.persistence.repositories import CatalogSnapshot
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


def _real_sdk_market_event(
    event_type: str,
    *,
    condition_id: str,
    token_id: str = "1001",
    additional_token_ids: tuple[str, ...] = (),
) -> Any:
    common = {
        "event_type": event_type,
        "market": condition_id,
        "timestamp": "1785405962000",
    }
    variants: dict[str, dict[str, Any]] = {
        "book": {
            **common,
            "asset_id": token_id,
            "bids": [{"price": "0.41", "size": "3"}],
            "asks": [{"price": "0.43", "size": "2"}],
            "hash": "stream-book-hash",
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": True,
        },
        "price_change": {
            **common,
            "price_changes": [
                {
                    "asset_id": change_token_id,
                    "price": "0.42",
                    "size": "3",
                    "side": "BUY",
                    "hash": "delta-hash",
                    "best_bid": "0.41",
                    "best_ask": "0.43",
                }
                for change_token_id in (token_id, *additional_token_ids)
            ],
        },
        "last_trade_price": {
            **common,
            "asset_id": token_id,
            "price": "0.42",
            "size": "3",
            "side": "BUY",
            "fee_rate_bps": "25",
            "transaction_hash": "0xabc",
        },
        "tick_size_change": {
            **common,
            "asset_id": token_id,
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
        },
        "best_bid_ask": {
            **common,
            "asset_id": token_id,
            "best_bid": "0.41",
            "best_ask": "0.43",
            "spread": "0.02",
        },
        "market_resolved": {
            **common,
            "id": "200",
            "assets_ids": ["1001", "1002"],
            "winning_asset_id": "1001",
            "winning_outcome": "Yes",
            "tags": [],
        },
        "new_market": {
            **common,
            "id": "999",
            "question": "A newly announced market",
            "assets_ids": ["9001", "9002"],
            "condition_id": condition_id,
            "active": True,
            "fees_enabled": False,
        },
    }
    return gateway_module._parse_pinned_market_event(variants[event_type])


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
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1024)
        self._ended = False
        self._dropped = 0
        self.closed = False

    @property
    def dropped(self) -> int:
        return self._dropped

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._events)
        except StopIteration as error:
            raise StopAsyncIteration from error

    async def close(self) -> None:
        self.closed = True


class BlockingSubscriptionHandle(FakeSubscriptionHandle):
    def __init__(self) -> None:
        super().__init__()
        self._release = asyncio.Event()
        self.event_wait_cancelled = False

    async def __anext__(self):
        try:
            await self._release.wait()
            raise StopAsyncIteration
        except asyncio.CancelledError:
            self.event_wait_cancelled = True
            raise


class FailingCloseSubscriptionHandle(FakeSubscriptionHandle):
    def __init__(self, error_type: type[BaseException]) -> None:
        super().__init__()
        self.error_type = error_type
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        raise self.error_type("SDK close failed")


class FakeConnection:
    def __init__(self) -> None:
        self._socket: object | None = object()
        self.reader_callback: Any = None

    async def _read_loop(self, socket: object, on_message: Any) -> None:
        if self.reader_callback is not None:
            self.reader_callback()


class FakeMarketManager:
    def __init__(self) -> None:
        self._connection = FakeConnection()
        self._queue_size = 1024
        self._dropped_events = 0
        self.open_state_override: object | None = None
        self.connection_losses: list[tuple[int, str]] = []
        self.received_raw_messages: list[object] = []

    def _on_message(self, raw: object) -> None:
        self.received_raw_messages.append(raw)
        items = raw if isinstance(raw, list) else [raw]
        for item in items:
            try:
                gateway_module._parse_pinned_market_event(item)
            except Exception:
                self._dropped_events += 1

    def _on_socket_connection_lost(self, code: int, reason: str) -> None:
        self.connection_losses.append((code, reason))

    @property
    def is_open(self) -> object:
        if self.open_state_override is not None:
            return self.open_state_override
        return self._connection._socket is not None

    @property
    def dropped_events(self) -> int:
        return self._dropped_events


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
        self._market_manager = FakeMarketManager()
        self.connection_lost_callback: Any = None
        self.queue_size_at_subscribe: int | None = None
        self.operations: list[str] = []
        self.closed = False

    def _get_market_manager(self) -> FakeMarketManager:
        return self._market_manager

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
        self.operations.append("get_order_books")
        self.book_token_ids = token_ids
        return tuple(book for book in self.books if book.token_id in token_ids)

    async def subscribe(self, spec: Any) -> FakeSubscriptionHandle:
        self.operations.append("subscribe")
        self.queue_size_at_subscribe = self._market_manager._queue_size
        self.subscription_spec = spec
        self.connection_lost_callback = (
            self._market_manager._on_socket_connection_lost
        )
        return self.subscription_handle

    async def close(self) -> None:
        self.closed = True


class RotatingSubscriptionClient(FakePublicClient):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.subscription_handles: list[FakeSubscriptionHandle] = []

    async def subscribe(self, spec: Any) -> FakeSubscriptionHandle:
        self.subscription_handle = BlockingSubscriptionHandle()
        self.subscription_handles.append(self.subscription_handle)
        return await super().subscribe(spec)


class BlockingOrderBookClient(FakePublicClient):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.books_started = asyncio.Event()
        self.books_release = asyncio.Event()
        self.books_completed = asyncio.Event()
        self.books_cancelled = False

    async def get_order_books(self, *, token_ids: tuple[str, ...]) -> tuple[Any, ...]:
        self.operations.append("get_order_books")
        self.book_token_ids = token_ids
        self.books_started.set()
        try:
            await self.books_release.wait()
        except asyncio.CancelledError:
            self.books_cancelled = True
            raise
        self.books_completed.set()
        return tuple(book for book in self.books if book.token_id in token_ids)


class RejectingOrderBookClient(FakePublicClient):
    def __init__(self, *, rejection_status: int = 502, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.rejection_status = rejection_status

    async def get_order_books(self, *, token_ids: tuple[str, ...]) -> tuple[Any, ...]:
        self.operations.append("get_order_books")
        raise RequestRejectedError(
            "Cloudflare upstream failure",
            status=self.rejection_status,
            retry_after=1.5,
        )


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


async def test_list_active_events_logs_periodic_page_progress(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakePublicClient(**sdk_fixture)
    client.event_paginator = FakePaginator(
        tuple((sdk_fixture["events"][0],) for _ in range(25))
    )
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)

    with caplog.at_level(logging.INFO, logger=gateway_module.__name__):
        events = await gateway.list_active_events()

    assert len(events) == 25
    assert "catalog_events_fetch_progress pages=25 active_events=25" in caplog.text
    assert "catalog_events_fetch_completed pages=25 active_events=25" in caplog.text


async def test_hydrate_market_identities_supports_recovery_from_persisted_catalog(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    source = PolymarketGateway(
        client=FakePublicClient(**sdk_fixture),
        clock_ms=lambda: 1_785_405_970_000,
    )
    snapshots = await source.list_active_markets()
    selected = snapshots[0]
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
    )
    catalog = CatalogSnapshot(
        events=(),
        markets=(selected.market,),
        tokens=selected.tokens,
    )

    gateway.hydrate_market_identities(
        catalog.markets,
        catalog.tokens,
        (selected.market.id,),
    )
    books = await gateway.get_order_books(tuple(token.id for token in selected.tokens))

    assert {book.market_id for book in books} == {selected.market.id}
    assert {book.token_id for book in books} == {token.id for token in selected.tokens}


async def test_pinned_sdk_private_lifecycle_shape_is_exactly_supported() -> None:
    # Catches a pinned SDK upgrade or private lifecycle shape drift going unnoticed.
    assert importlib.metadata.version("polymarket-client") == "0.3.0b1"

    shape = await gateway_module.probe_pinned_sdk_lifecycle_shape()

    assert shape == {
        "version": "0.3.0b1",
        "client_manager_attribute": "_market_manager",
        "manager_connection_attribute": "_connection",
        "connection_socket_attribute": "_socket",
        "manager_open_property": "is_open",
        "manager_dropped_property": "dropped_events",
        "handle_dropped_property": "dropped",
        "handle_queue_attribute": "_queue",
        "handle_queue_maxsize": 1,
        "handle_ended_attribute": "_ended",
        "initial_manager_open": False,
        "initial_socket_is_none": True,
        "initial_manager_dropped": 0,
        "initial_handle_dropped": 0,
        "initial_handle_ended": False,
    }


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
    assert first.market.status is MarketStatus.ACTIVE
    assert first.market.active is True
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
    assert fee.model is FeeModel.CURVE
    assert fee.enabled is True
    assert fee.parameters == {
        "rate": Decimal("0.04"),
        "exponent": Decimal("1"),
        "rebate_rate": Decimal("0.25"),
    }
    assert fee.taker_only is True
    assert fee.updated_at == 1_785_405_970_000
    zero_fee = snapshots[1].tokens[0].fee_schedule
    assert zero_fee is not None
    assert zero_fee.model is FeeModel.ZERO
    assert zero_fee.enabled is False


async def test_list_active_markets_logs_periodic_page_progress(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakePublicClient(**sdk_fixture)
    client.market_paginator = FakePaginator(
        tuple((sdk_fixture["markets"][0],) for _ in range(25))
    )
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)

    with caplog.at_level(logging.INFO, logger=gateway_module.__name__):
        snapshots = await gateway.list_active_markets()

    assert len(snapshots) == 25
    assert (
        "catalog_markets_fetch_progress pages=25 active_markets=25 warnings=0"
        in caplog.text
    )
    assert (
        "catalog_markets_fetch_completed pages=25 active_markets=25 warnings=0"
        in caplog.text
    )


async def test_list_active_markets_accepts_market_without_event_reference(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    payload = deepcopy(sdk_fixture)
    payload["markets"][0].events = ()
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        page_size=1,
    )

    snapshots = await gateway.list_active_markets()

    assert tuple(snapshot.market.id for snapshot in snapshots) == ("200", "201")
    assert gateway.market_mapping_warnings == ()
    assert snapshots[0].market.event_id is None


async def test_list_active_markets_skips_market_with_multiple_event_references(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    payload = deepcopy(sdk_fixture)
    payload["markets"][0].events = (
        payload["markets"][0].events[0],
        payload["markets"][1].events[0],
    )
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        page_size=1,
    )

    snapshots = await gateway.list_active_markets()

    assert tuple(snapshot.market.id for snapshot in snapshots) == ("201",)
    warning = gateway.market_mapping_warnings[0]
    assert warning.market_id == "200"
    assert "events must contain at most one event reference" in warning.error
    assert '"events":[' in warning.error
    assert len(warning.error.rsplit("api_response=", 1)[1]) <= 8_192


async def test_market_mapping_error_includes_bounded_api_response(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    payload = deepcopy(sdk_fixture)
    payload["markets"][0].trading.fee_schedule = None
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        page_size=1,
    )

    snapshots = await gateway.list_active_markets()

    assert tuple(snapshot.market.id for snapshot in snapshots) == ("201",)
    warning = gateway.market_mapping_warnings[0]
    assert warning.market_id == "200"
    assert "enabled fees require a fee schedule" in warning.error
    assert '"id":"200"' in warning.error
    assert len(warning.error.rsplit("api_response=", 1)[1]) <= 8_192


@pytest.mark.parametrize("contradictory_flag", ["closed", "archived"])
async def test_contradictory_active_market_state_is_rejected(
    sdk_fixture: dict[str, tuple[Any, ...]],
    contradictory_flag: str,
) -> None:
    # Catches closed/archived SDK results entering the active watch catalog.
    payload = deepcopy(sdk_fixture)
    setattr(payload["markets"][0].state, contradictory_flag, True)
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)

    snapshots = await gateway.list_active_markets()

    assert tuple(snapshot.market.id for snapshot in snapshots) == ("201",)
    warning = gateway.market_mapping_warnings[0]
    assert warning.market_id == "200"
    assert "contradictory" in warning.error


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


async def test_get_order_books_records_received_time_after_response(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches an exchange timestamp advancing while the REST request is in flight.
    clock = [1_785_405_970_000]

    class ResponseClockClient(FakePublicClient):
        async def get_order_books(
            self,
            *,
            token_ids: tuple[str, ...],
        ) -> tuple[Any, ...]:
            clock[0] = 1_785_405_980_000
            return await super().get_order_books(token_ids=token_ids)

    client = ResponseClockClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: clock[0])
    await gateway.list_active_markets()

    books = await gateway.get_order_books(("1001", "1002"))

    assert {book.received_timestamp for book in books} == {1_785_405_980_000}


async def test_order_book_token_condition_identity_mismatch_is_rejected(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches a known token being silently attached to another known market.
    payload = deepcopy(sdk_fixture)
    payload["books"][0].condition_id = payload["markets"][1].condition_id
    client = FakePublicClient(**payload)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with pytest.raises(
        GatewayMappingError,
        match=r"order book 1001.*identity.*condition",
    ):
        await gateway.get_order_books(("1001",))


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


async def test_recover_market_session_shares_one_new_generation(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches a REST recovery baseline and its new stream using different generations.
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with caplog.at_level(logging.INFO, logger="predmarket.polymarket.gateway"):
        session = await gateway.recover_market_session(("1001", "1002"))

    assert session.subscription_generation == 1
    assert tuple(
        book.subscription_generation for book in session.order_books
    ) == (1, 1)
    assert session.subscription.subscription_generation == 1
    assert client.operations == ["subscribe", "get_order_books"]
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        message.startswith("market_recovery_started ")
        and "generation=1" in message
        and "tokens=2" in message
        and "token_id_bytes=8" in message
        for message in messages
    )
    assert any(
        message.startswith("market_stream_subscribed ")
        and "generation=1" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert any(
        message.startswith("market_recovery_books_received ")
        and "books=2" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    assert any(
        message.startswith("market_recovery_completed ")
        and "generation=1" in message
        and "elapsed_ms=" in message
        for message in messages
    )
    await session.subscription.close()


async def test_recovery_normalizes_retryable_server_rejection_and_closes_stream(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    client = RejectingOrderBookClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with pytest.raises(gateway_module.MarketRecoveryTransientError) as caught:
        await gateway.recover_market_session(("1001", "1002"))

    assert caught.value.reason == "request_rejected"
    assert caught.value.status == 502
    assert caught.value.retry_after == 1.5
    assert client.subscription_handle.closed is True


async def test_recovery_does_not_normalize_non_retryable_client_rejection(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    client = RejectingOrderBookClient(rejection_status=400, **sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with pytest.raises(RequestRejectedError) as caught:
        await gateway.recover_market_session(("1001", "1002"))

    assert caught.value.status == 400
    assert client.subscription_handle.closed is True


async def test_recovery_prunes_whole_market_when_rest_books_disappear(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches one stale resolving market aborting recovery for every valid market.
    client = RotatingSubscriptionClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    snapshots = await gateway.list_active_markets()
    requested = tuple(
        token.id for snapshot in snapshots for token in snapshot.tokens
    )
    expected = tuple(token.id for token in snapshots[0].tokens)

    with caplog.at_level(logging.INFO, logger="predmarket.polymarket.gateway"):
        session = await gateway.recover_market_session(requested)

    assert session.token_ids == expected
    assert session.subscription.token_ids == expected
    assert tuple(book.token_id for book in session.order_books) == expected
    assert client.operations == [
        "subscribe",
        "get_order_books",
        "subscribe",
        "get_order_books",
    ]
    assert client.subscription_handles[0].closed is True
    assert client.subscription_handles[1].closed is False
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        message.startswith("market_recovery_books_missing ")
        and "missing_tokens=2" in message
        and "removed_markets=1" in message
        and "removed_tokens=2" in message
        and "remaining_tokens=2" in message
        for message in messages
    )
    await session.subscription.close()


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        ("disconnect", "connection_lost"),
        ("replacement", "connection_replaced"),
        ("manager_drop", "sdk_event_dropped"),
        ("handle_drop", "subscription_event_dropped"),
        ("version", "sdk_version_changed"),
        ("shape", "sdk_lifecycle_shape_changed"),
        ("handle_end", "sdk_handle_ended"),
    ],
)
async def test_recovery_monitors_real_handle_while_rest_baseline_is_blocked(
    sdk_fixture: dict[str, tuple[Any, ...]],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    reason: str,
) -> None:
    # Catches recovery returning or hanging after its stream barrier is invalid.
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
    )
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    client = BlockingOrderBookClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    recovery_task = asyncio.create_task(
        gateway.recover_market_session(("1001", "1002"))
    )
    await asyncio.wait_for(client.books_started.wait(), timeout=0.2)

    if failure == "disconnect":
        client._market_manager._connection._socket = None
    elif failure == "replacement":
        client._market_manager._connection._socket = object()
    elif failure == "manager_drop":
        client._market_manager._dropped_events += 1
    elif failure == "handle_drop":
        handle._push(sdk_event)
        handle._push(sdk_event)
        handle._push(sdk_event)
        assert handle.dropped > 0
    elif failure == "version":
        monkeypatch.setattr(
            gateway_module.importlib.metadata,
            "version",
            lambda _distribution: "0.3.0b2",
        )
    elif failure == "shape":
        del client._market_manager._connection
    else:
        assert failure == "handle_end"
        handle._end()

    with pytest.raises(
        gateway_module.GatewayLifecycleError,
        match=reason,
    ):
        await asyncio.wait_for(recovery_task, timeout=0.2)

    assert client.books_cancelled is False
    assert client.books_completed.is_set() is False
    client.books_release.set()
    await asyncio.wait_for(client.books_completed.wait(), timeout=0.2)
    assert handle._ended is True


async def test_recovery_replays_events_buffered_by_its_lifecycle_monitor(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches the recovery monitor consuming a healthy event without replaying it.
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
    )
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    client = BlockingOrderBookClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    recovery_task = asyncio.create_task(
        gateway.recover_market_session(("1001", "1002"))
    )
    await asyncio.wait_for(client.books_started.wait(), timeout=0.2)

    handle._push(sdk_event)
    for _ in range(20):
        if handle._queue.empty():
            break
        await asyncio.sleep(0)
    assert handle._queue.empty() is True
    client.books_release.set()

    session = await asyncio.wait_for(recovery_task, timeout=0.2)
    mapped = await asyncio.wait_for(anext(session.subscription), timeout=0.2)

    assert isinstance(mapped, MarketStreamEvent)
    assert mapped.event_type == "price_change"
    assert mapped.subscription_generation == session.subscription_generation
    await session.subscription.close()


async def test_recovery_replay_rechecks_lifecycle_before_each_buffered_event(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches old-generation buffered events leaking after a post-recovery drop.
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
    )
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=2)
    client = BlockingOrderBookClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    recovery_task = asyncio.create_task(
        gateway.recover_market_session(("1001", "1002"))
    )
    await asyncio.wait_for(client.books_started.wait(), timeout=0.2)

    for _ in range(2):
        handle._push(sdk_event)
        for _ in range(20):
            if handle._queue.empty():
                break
            await asyncio.sleep(0)
        assert handle._queue.empty() is True
    client.books_release.set()
    session = await asyncio.wait_for(recovery_task, timeout=0.2)

    first = await asyncio.wait_for(anext(session.subscription), timeout=0.2)
    assert isinstance(first, MarketStreamEvent)

    for _ in range(3):
        handle._push(sdk_event)
    assert handle.dropped == 1

    invalid = await asyncio.wait_for(anext(session.subscription), timeout=0.2)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == "subscription_event_dropped"
    assert invalid.subscription_generation == session.subscription_generation
    assert handle._ended is True
    with pytest.raises(StopAsyncIteration):
        await anext(session.subscription)


async def test_recovery_replay_rejects_unexpected_real_sdk_handle_end(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches an ended generation replaying events buffered before SDK termination.
    sdk_client = gateway_module.AsyncPublicClient()
    close_manager = sdk_client._get_market_manager()
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=2)
    sdk_subscription = gateway_module.MarketSpec(
        token_ids=("1001", "1002"),
        custom_feature_enabled=True,
    )
    close_manager._registry.add(
        sub=sdk_subscription,
        matcher=lambda _event: True,
        handle=handle,
    )
    handle._bind_close(close_manager._on_handle_close)
    client = BlockingOrderBookClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    try:
        condition_id = sdk_fixture["markets"][0].condition_id
        sdk_event = _real_sdk_market_event(
            "price_change",
            condition_id=condition_id,
        )
        recovery_task = asyncio.create_task(
            gateway.recover_market_session(("1001", "1002"))
        )
        await asyncio.wait_for(client.books_started.wait(), timeout=0.2)

        for _ in range(2):
            handle._push(sdk_event)
            for _ in range(20):
                if handle._queue.empty():
                    break
                await asyncio.sleep(0)
            assert handle._queue.empty() is True
        client.books_release.set()
        session = await asyncio.wait_for(recovery_task, timeout=0.2)
        assert len(session.subscription._buffered_events) == 2

        handle._end()
        assert handle._ended is True
        assert handle.dropped == 0

        invalid = await asyncio.wait_for(
            anext(session.subscription),
            timeout=0.2,
        )

        assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
        assert invalid.reason == "sdk_handle_ended"
        assert invalid.subscription_generation == session.subscription_generation
        assert len(session.subscription._buffered_events) == 0
        assert close_manager._registry.is_empty is True
        with pytest.raises(StopAsyncIteration):
            await anext(session.subscription)
    finally:
        await sdk_client.close()


async def test_recovery_buffer_is_bounded_by_real_sdk_handle_queue(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches the monitor draining a bounded SDK queue into unbounded local memory.
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
    )
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    client = BlockingOrderBookClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    recovery_task = asyncio.create_task(
        gateway.recover_market_session(("1001", "1002"))
    )
    await asyncio.wait_for(client.books_started.wait(), timeout=0.2)

    injected = 0
    for _ in range(2_000):
        handle._push(sdk_event)
        injected += 1
        if not handle._ended:
            for _ in range(10):
                await asyncio.sleep(0)

    with pytest.raises(
        gateway_module.GatewayLifecycleError,
        match="recovery_buffer_overflow",
    ):
        await asyncio.wait_for(recovery_task, timeout=0.2)

    assert injected == 2_000
    assert handle._ended is True
    assert client.books_cancelled is False
    client.books_release.set()
    await asyncio.wait_for(client.books_completed.wait(), timeout=0.2)


async def test_recovery_overflow_detaches_rest_before_blocked_sdk_close(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches blocked SDK cleanup delaying invalidation or cancelling HTTP/2 REST.
    sdk_client = gateway_module.AsyncPublicClient()
    close_manager = sdk_client._get_market_manager()
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    sdk_subscription = gateway_module.MarketSpec(
        token_ids=("1001",),
        custom_feature_enabled=True,
    )
    close_manager._registry.add(
        sub=sdk_subscription,
        matcher=lambda _event: True,
        handle=handle,
    )
    handle._bind_close(close_manager._on_handle_close)
    client = BlockingOrderBookClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
    )
    await close_manager._send_lock.acquire()
    recovery_task: asyncio.Task[Any] | None = None
    try:
        recovery_task = asyncio.create_task(
            gateway.recover_market_session(("1001", "1002"))
        )
        await asyncio.wait_for(client.books_started.wait(), timeout=0.2)

        handle._push(sdk_event)
        for _ in range(20):
            await asyncio.sleep(0)
            if handle._queue.empty():
                break
        handle._push(sdk_event)
        for _ in range(20):
            await asyncio.sleep(0)
            if handle._closing is not None:
                break

        assert handle._closing is not None
        assert client.books_cancelled is False
        assert client.books_completed.is_set() is False
        assert close_manager._registry.is_empty is False

        client.books_release.set()
        await asyncio.wait_for(client.books_completed.wait(), timeout=0.2)
        close_manager._send_lock.release()
        with pytest.raises(
            gateway_module.GatewayLifecycleError,
            match="recovery_buffer_overflow",
        ):
            await asyncio.wait_for(recovery_task, timeout=0.2)

        assert handle._ended is True
        assert close_manager._registry.is_empty is True
    finally:
        if close_manager._send_lock.locked():
            close_manager._send_lock.release()
        if recovery_task is not None and not recovery_task.done():
            recovery_task.cancel()
        if recovery_task is not None:
            await asyncio.gather(recovery_task, return_exceptions=True)
        await sdk_client.close()


async def test_subscribe_markets_uses_public_market_spec_and_maps_stream_models(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches direct WebSocket use or leaking a mutable SDK stream model downstream.
    stream_event = FixtureModel(
        type="price_change",
        payload=FixtureModel(
            market=sdk_fixture["markets"][0].condition_id,
            price_changes=(
                FixtureModel(
                    token_id="1001",
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
    with caplog.at_level(logging.INFO, logger=gateway_module.__name__):
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
    assert "market_stream_consumer_progress generation=1" in caplog.text
    assert "events_total=1" in caplog.text
    assert "queue_size=0 queue_capacity=1024" in caplog.text
    assert "market_stream_pump_progress generation=1" in caplog.text
    assert "sdk_events_total=1" in caplog.text
    assert "mapping_ms_per_event=" in caplog.text
    assert "handoff_wait_ms_per_event=" in caplog.text
    await subscription.close()
    assert client.subscription_handle.closed is True


async def test_market_stream_connection_lost_logs_sdk_close_details(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches the SDK close code/reason being discarded before reaching our logs.
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)

    subscription = await gateway.subscribe_markets(("1001", "1002"))
    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        client.connection_lost_callback(
            1009,
            "message too big",
        )

    assert "market_stream_connection_lost close_code=1009" in caplog.text
    assert "close_reason='message too big'" in caplog.text
    assert client._market_manager.connection_losses == [(1009, "message too big")]
    await subscription.close()


async def test_market_stream_connection_lost_logs_reader_exception_and_heartbeat(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches the pinned SDK swallowing ConnectionClosed before on_error runs.
    client = FakePublicClient(**sdk_fixture)
    connection = client._market_manager._connection
    client._market_manager._heartbeat = SimpleNamespace(
        _clock=lambda: 110.0,
        _last_pong=100.0,
    )
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    subscription = await gateway.subscribe_markets(("1001", "1002"))
    socket = SimpleNamespace(
        state=SimpleNamespace(name="CLOSED"),
        close_code=1006,
        close_reason="",
        recv_exc=ConnectionResetError(54, "Connection reset by peer"),
        latency=0.125,
        transport=SimpleNamespace(is_closing=lambda: True),
        protocol=SimpleNamespace(parser_exc=EOFError("stream ended")),
    )
    connection.reader_callback = lambda: client.connection_lost_callback(1006, "")

    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        await connection._read_loop(socket, lambda raw: None)

    assert "market_stream_connection_lost close_code=1006" in caplog.text
    assert "socket_state=CLOSED" in caplog.text
    assert "reader_exception_type=ConnectionResetError" in caplog.text
    assert "reader_exception='[Errno 54] Connection reset by peer'" in caplog.text
    assert "parser_exception_type=EOFError" in caplog.text
    assert "heartbeat_age_seconds=10.000" in caplog.text
    assert "websocket_latency_seconds=0.125" in caplog.text
    assert "transport_closing=True" in caplog.text
    await subscription.close()


async def test_closed_socket_waits_for_sdk_connection_lost_callback(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches lifecycle polling closing the SDK socket before its reader can
    # report the peer's close code and reason.
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    subscription = await gateway.subscribe_markets(("1001", "1002"))
    invalidation = asyncio.create_task(anext(subscription))

    client._market_manager.open_state_override = False
    await asyncio.sleep(0.01)

    assert invalidation.done() is False

    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        client._market_manager._connection._socket = None
        client.connection_lost_callback(1001, "server shutdown")
        invalid = await asyncio.wait_for(invalidation, timeout=0.2)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == "connection_lost"
    assert "market_stream_connection_lost close_code=1001" in caplog.text
    assert "close_reason='server shutdown'" in caplog.text


async def test_market_stream_malformed_event_logs_validation_at_sdk_callback(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches the SDK's dropped counter hiding the exact invalid wire field.
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    subscription = await gateway.subscribe_markets(("1001", "1002"))
    malformed = {
        "event_type": "price_change",
        "market": "condition-1",
        "timestamp": "1785405962000",
        "price_changes": [
            {
                "asset_id": "1001",
                "price": "0.42",
                "size": "3",
                "side": "UNKNOWN",
            }
        ],
    }

    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        client._market_manager._on_message(malformed)

    assert "market_stream_event_malformed event_type=price_change" in caplog.text
    assert "price_changes.0.side" in caplog.text
    assert '"side":"UNKNOWN"' in caplog.text
    assert client._market_manager.received_raw_messages == [malformed]
    assert client._market_manager.dropped_events == 1
    await subscription.close()


async def test_iso_game_start_new_market_is_normalized_before_sdk_callback(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The live API uses ISO-8601 here while the pinned SDK requires epoch-ms.
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    subscription = await gateway.subscribe_markets(("1001", "1002"))
    new_market = {
        "event_type": "new_market",
        "id": "999",
        "market": "condition-unknown",
        "timestamp": "1785405962000",
        "game_start_time": "2026-08-08 14:00:00+00",
    }

    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        client._market_manager._on_message(new_market)

    assert "market_stream_event_malformed event_type=new_market" not in caplog.text
    assert client._market_manager.received_raw_messages == [
        {
            **new_market,
            "game_start_time": "1786197600000",
        }
    ]
    assert client._market_manager.dropped_events == 0
    assert subscription._lifecycle_probe is not None
    assert subscription._lifecycle_probe.check() is None
    await subscription.close()


async def test_malformed_unscoped_new_market_logs_are_bounded_and_rate_limited(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    subscription = await gateway.subscribe_markets(("1001", "1002"))

    def malformed(index: int) -> dict[str, object]:
        return {
            "event_type": "new_market",
            "id": str(index),
            "market": f"condition-{index}",
            "timestamp": "1785405962000",
            "game_start_time": "not-a-timestamp",
            "description": "x" * 5_000,
        }

    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        client._market_manager._on_message([malformed(1), malformed(2)])
        client._market_manager._on_message([malformed(index) for index in range(3, 100)])
        client._market_manager._on_message(malformed(100))

    messages = [
        record.getMessage()
        for record in caplog.records
        if "market_stream_event_malformed event_type=new_market" in record.getMessage()
    ]
    assert len(messages) == 2
    assert "ignored_count=2 ignored_total=2" in messages[0]
    assert "ignored_count=1 ignored_total=100" in messages[1]
    assert all(len(message) < 1_500 for message in messages)
    assert client._market_manager.received_raw_messages == []
    assert client._market_manager.dropped_events == 0
    await subscription.close()


async def test_subscribe_configures_sdk_queue_before_creating_handle(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The SDK copies manager._queue_size into each new bounded handle.
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        market_stream_queue_capacity=65_536,
    )

    with caplog.at_level(logging.INFO, logger=gateway_module.__name__):
        subscription = await gateway.subscribe_markets(("1001", "1002"))

    assert client.queue_size_at_subscribe == 65_536
    assert "market_stream_queue_configured capacity=65536 previous_capacity=1024" in caplog.text
    await subscription.close()


@pytest.mark.parametrize(
    "event_type",
    [
        "book",
        "price_change",
        "last_trade_price",
        "tick_size_change",
        "best_bid_ask",
        "market_resolved",
    ],
)
async def test_every_token_scoped_public_market_event_variant_is_consumable(
    sdk_fixture: dict[str, tuple[Any, ...]],
    event_type: str,
) -> None:
    # Catches a documented SDK MarketEvent variant being dropped by the gateway.
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        event_type,
        condition_id=condition_id,
    )
    client = FakePublicClient(**sdk_fixture, stream_events=(sdk_event,))
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001", "1002"))
    mapped = await anext(subscription)

    assert isinstance(mapped, MarketStreamEvent)
    assert mapped.event_type == event_type
    assert mapped.market_id == "200"
    await subscription.close()


async def test_unscoped_new_market_variant_is_filtered_without_killing_consumer(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches the custom-feature global new_market event terminating a token watch.
    unknown_condition = "0x" + "9" * 64
    known_condition = sdk_fixture["markets"][0].condition_id
    new_market = _real_sdk_market_event(
        "new_market",
        condition_id=unknown_condition,
        token_id="9001",
    )
    next_price = _real_sdk_market_event(
        "price_change",
        condition_id=known_condition,
    )
    client = FakePublicClient(
        **sdk_fixture,
        stream_events=(new_market, next_price),
    )
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001",))
    mapped = await anext(subscription)

    assert isinstance(mapped, MarketStreamEvent)
    assert mapped.event_type == "price_change"
    assert mapped.market_id == "200"
    await subscription.close()


async def test_real_sdk_handle_drop_before_capture_immediately_invalidates(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches a new SDK handle treating pre-capture queue loss as its baseline.
    condition_id = sdk_fixture["markets"][0].condition_id
    first_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
    )
    second_event = _real_sdk_market_event(
        "last_trade_price",
        condition_id=condition_id,
    )
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    handle._push(first_event)
    handle._push(second_event)
    assert handle.dropped == 1
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001",))
    invalid = await anext(subscription)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == "subscription_event_dropped"
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


async def test_stream_token_condition_identity_mismatch_invalidates_generation(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches an SDK stream token being accepted under another known condition.
    wrong_condition = sdk_fixture["markets"][1].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=wrong_condition,
        token_id="1001",
    )
    client = FakePublicClient(**sdk_fixture, stream_events=(sdk_event,))
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001",))
    invalid = await anext(subscription)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == "sdk_event_invalid"
    assert client.subscription_handle.closed is True
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


async def test_price_change_filters_unsubscribed_tokens_after_identity_validation(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches batched price changes leaking tokens outside the requested watch.
    condition_id = sdk_fixture["markets"][0].condition_id
    sdk_event = _real_sdk_market_event(
        "price_change",
        condition_id=condition_id,
        token_id="1001",
        additional_token_ids=("1002",),
    )
    client = FakePublicClient(**sdk_fixture, stream_events=(sdk_event,))
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001",))
    mapped = await anext(subscription)

    assert isinstance(mapped, MarketStreamEvent)
    assert tuple(
        change["token_id"] for change in mapped.payload["price_changes"]
    ) == ("1001",)
    await subscription.close()


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (
            lambda client: setattr(
                client._market_manager._connection,
                "_socket",
                None,
            ),
            "connection_lost",
        ),
        (
            lambda client: setattr(
                client._market_manager._connection,
                "_socket",
                object(),
            ),
            "connection_replaced",
        ),
        (
            lambda client: setattr(
                client._market_manager,
                "_dropped_events",
                1,
            ),
            "sdk_event_dropped",
        ),
        (
            lambda client: setattr(client.subscription_handle, "_dropped", 1),
            "subscription_event_dropped",
        ),
        (
            lambda client: setattr(
                client.subscription_handle,
                "_queue",
                asyncio.Queue(maxsize=1),
            ),
            "sdk_lifecycle_shape_changed",
        ),
    ],
)
async def test_subscription_lifecycle_change_emits_invalid_then_stops(
    sdk_fixture: dict[str, tuple[Any, ...]],
    mutation: Any,
    reason: str,
) -> None:
    # Catches a transparent reconnect or SDK drop continuing the old generation.
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()
    subscription = await gateway.subscribe_markets(("1001", "1002"))

    mutation(client)
    invalid = await asyncio.wait_for(anext(subscription), timeout=0.2)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == reason
    assert invalid.token_ids == ("1001", "1002")
    assert invalid.subscription_generation == 1
    assert client.subscription_handle.closed is True
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


async def test_subscription_drop_logs_sdk_queue_diagnostics(
    sdk_fixture: dict[str, tuple[Any, ...]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()
    subscription = await gateway.subscribe_markets(("1001", "1002"))

    client.subscription_handle._dropped = 3
    with caplog.at_level(logging.WARNING, logger=gateway_module.__name__):
        invalid = await asyncio.wait_for(anext(subscription), timeout=0.2)

    assert invalid.reason == "subscription_event_dropped"
    assert (
        "market_stream_subscription_drop_detected "
        "handle_dropped=3 previous_handle_dropped=0 drop_delta=3 "
        "queue_size=0 queue_maxsize=1024 manager_dropped=0 "
        "previous_manager_dropped=0"
    ) in caplog.text


@pytest.mark.parametrize(
    ("break_shape", "reason"),
    [
        (
            lambda client: delattr(client._market_manager, "_connection"),
            "sdk_lifecycle_shape_changed",
        ),
        (
            lambda client: setattr(
                client._market_manager,
                "open_state_override",
                "UNKNOWN",
            ),
            "sdk_lifecycle_state_unknown",
        ),
        (
            lambda client: delattr(client.subscription_handle, "_ended"),
            "sdk_lifecycle_shape_changed",
        ),
        (
            lambda client: setattr(
                client.subscription_handle,
                "_ended",
                "UNKNOWN",
            ),
            "sdk_lifecycle_state_unknown",
        ),
    ],
)
async def test_unknown_sdk_lifecycle_shape_or_state_fails_closed(
    sdk_fixture: dict[str, tuple[Any, ...]],
    break_shape: Any,
    reason: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Catches SDK private state drift being silently interpreted as healthy.
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    break_shape(client)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    with caplog.at_level(logging.WARNING, logger="predmarket.polymarket.gateway"):
        subscription = await gateway.subscribe_markets(("1001",))
    invalid = await asyncio.wait_for(anext(subscription), timeout=0.2)

    assert invalid.reason == reason
    if not hasattr(client._market_manager, "_connection"):
        assert "market_stream_connection_diagnostics_unavailable" in caplog.text
    assert client.subscription_handle.closed is True
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


async def test_unexpected_sdk_handle_end_is_an_invalid_generation(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches an ended SDK handle looking like a normal graceful watch shutdown.
    client = FakePublicClient(**sdk_fixture)
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001",))
    invalid = await anext(subscription)

    assert invalid.reason == "sdk_handle_ended"
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


async def test_real_sdk_handle_error_end_is_an_invalid_generation(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches an SDK end error bypassing invalidation and cleanup as a raw exception.
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    subscription = await gateway.subscribe_markets(("1001",))
    read_task = asyncio.create_task(anext(subscription))
    for _ in range(20):
        await asyncio.sleep(0)
        if handle._queue._getters:
            break
    assert handle._queue._getters

    handle._end(RuntimeError("SDK stream failed"))
    invalid = await asyncio.wait_for(read_task, timeout=0.2)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == "sdk_handle_ended"
    assert handle._ended is True
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)


async def test_market_subscription_reuses_lifecycle_monitor_between_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Polling SDK lifecycle state must not create a second task per event.
    lifecycle_starts = 0
    release_lifecycle = asyncio.Event()

    async def track_lifecycle(
        _subscription: gateway_module.MarketSubscription,
    ) -> gateway_module._InvalidReason:
        nonlocal lifecycle_starts
        lifecycle_starts += 1
        await release_lifecycle.wait()
        return gateway_module._InvalidReason.CONNECTION_LOST

    monkeypatch.setattr(
        gateway_module.MarketSubscription,
        "_wait_for_invalidation",
        track_lifecycle,
    )
    events = (object(), object())
    handle = FakeSubscriptionHandle(events)
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda event: event,
        lifecycle_probe=SimpleNamespace(
            handle_queue_maxsize=handle._queue.maxsize,
            check=lambda: None,
        ),
        initial_invalid_reason=None,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )

    assert await anext(subscription) is events[0]
    assert await anext(subscription) is events[1]
    assert lifecycle_starts == 1
    await subscription.close()


async def test_market_subscription_reuses_event_pump_between_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The hot path must not create and destroy an SDK-read task per market event.
    release_lifecycle = asyncio.Event()

    async def wait_for_lifecycle(
        _subscription: gateway_module.MarketSubscription,
    ) -> gateway_module._InvalidReason:
        await release_lifecycle.wait()
        return gateway_module._InvalidReason.CONNECTION_LOST

    monkeypatch.setattr(
        gateway_module.MarketSubscription,
        "_wait_for_invalidation",
        wait_for_lifecycle,
    )
    events = (object(), object())
    handle = FakeSubscriptionHandle(events)
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda event: event,
        lifecycle_probe=SimpleNamespace(
            handle_queue_maxsize=handle._queue.maxsize,
            check=lambda: None,
        ),
        initial_invalid_reason=None,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )

    assert await anext(subscription) is events[0]
    event_pump = subscription._event_pump_task
    assert event_pump is not None
    assert await anext(subscription) is events[1]
    assert subscription._event_pump_task is event_pump
    await subscription.close()


async def test_market_subscription_prefetches_a_burst_without_per_event_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_lifecycle = asyncio.Event()

    async def wait_for_lifecycle(
        _subscription: gateway_module.MarketSubscription,
    ) -> gateway_module._InvalidReason:
        await release_lifecycle.wait()
        return gateway_module._InvalidReason.CONNECTION_LOST

    monkeypatch.setattr(
        gateway_module.MarketSubscription,
        "_wait_for_invalidation",
        wait_for_lifecycle,
    )
    events = (object(), object(), object(), object())
    handle = FakeSubscriptionHandle(events)
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda event: event,
        lifecycle_probe=SimpleNamespace(
            handle_queue_maxsize=handle._queue.maxsize,
            check=lambda: None,
        ),
        initial_invalid_reason=None,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )

    assert await anext(subscription) is events[0]
    await asyncio.sleep(0)

    assert subscription._live_items.qsize() >= 2
    assert await anext(subscription) is events[1]
    assert await anext(subscription) is events[2]
    assert await anext(subscription) is events[3]
    await subscription.close()


async def test_recovery_rejects_handle_end_behind_prefetched_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catches a terminal SDK marker queued behind prefetched events allowing a
    # simultaneously completed recovery baseline to be installed.
    never_invalidated = asyncio.Event()

    async def wait_for_lifecycle(
        _subscription: gateway_module.MarketSubscription,
    ) -> gateway_module._InvalidReason:
        await never_invalidated.wait()
        return gateway_module._InvalidReason.CONNECTION_LOST

    monkeypatch.setattr(
        gateway_module.MarketSubscription,
        "_wait_for_invalidation",
        wait_for_lifecycle,
    )
    handle = FakeSubscriptionHandle((object(), object(), object()))
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda event: event,
        lifecycle_probe=SimpleNamespace(
            handle_queue_maxsize=handle._queue.maxsize,
            check=lambda: None,
        ),
        initial_invalid_reason=None,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )
    subscription._ensure_live_tasks()
    assert subscription._event_pump_task is not None
    await subscription._event_pump_task

    try:
        with pytest.raises(
            gateway_module.GatewayLifecycleError,
            match="sdk_handle_ended",
        ):
            await subscription._guard_awaitable(asyncio.sleep(0, result="baseline"))
    finally:
        await subscription.close()


async def test_concurrent_handle_error_and_lifecycle_loss_still_invalidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catches a simultaneously completed SDK read error escaping the lifecycle branch.
    release = asyncio.Event()

    class ConcurrentEndingHandle(FakeSubscriptionHandle):
        async def __anext__(self) -> Any:
            await release.wait()
            raise RuntimeError("SDK stream failed")

    async def concurrent_invalidation(
        _subscription: gateway_module.MarketSubscription,
    ) -> gateway_module._InvalidReason:
        await release.wait()
        return gateway_module._InvalidReason.SDK_HANDLE_ENDED

    monkeypatch.setattr(
        gateway_module.MarketSubscription,
        "_wait_for_invalidation",
        concurrent_invalidation,
    )
    handle = ConcurrentEndingHandle()
    lifecycle_probe = SimpleNamespace(
        handle_queue_maxsize=handle._queue.maxsize,
        check=lambda: None,
    )
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda _event: None,
        lifecycle_probe=lifecycle_probe,
        initial_invalid_reason=None,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    read_task = asyncio.create_task(anext(subscription))
    await asyncio.sleep(0)
    release.set()

    invalid = await asyncio.wait_for(read_task, timeout=0.2)

    assert isinstance(invalid, gateway_module.MarketStreamInvalidated)
    assert invalid.reason == "sdk_handle_ended"
    assert handle.closed is True


async def test_explicit_subscription_close_is_not_reported_as_sdk_handle_end(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches wrapper-owned normal close being misreported as stream invalidation.
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = handle
    gateway = PolymarketGateway(
        client=client,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.001,
    )
    await gateway.list_active_markets()
    subscription = await gateway.subscribe_markets(("1001",))
    read_task = asyncio.create_task(anext(subscription))
    for _ in range(20):
        await asyncio.sleep(0)
        if handle._queue._getters:
            break
    assert handle._queue._getters

    await subscription.close()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(read_task, timeout=0.2)
    assert subscription._invalid_reason is None


async def test_cancelled_subscription_read_cleans_up_internal_event_wait(
    sdk_fixture: dict[str, tuple[Any, ...]],
) -> None:
    # Catches a cancelled watcher leaving a child task that can consume later events.
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()
    subscription = await gateway.subscribe_markets(("1001",))

    read_task = asyncio.create_task(anext(subscription))
    await asyncio.sleep(0)
    read_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await read_task
    await asyncio.sleep(0)

    assert client.subscription_handle.event_wait_cancelled is True
    await subscription.close()


async def test_cancelled_invalidation_close_cleans_real_sdk_registry() -> None:
    # Catches caller cancellation killing the SDK close task after it claimed cleanup.
    sdk_client = gateway_module.AsyncPublicClient()
    manager = sdk_client._get_market_manager()
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    sdk_subscription = gateway_module.MarketSpec(
        token_ids=("1001",),
        custom_feature_enabled=True,
    )
    manager._registry.add(
        sub=sdk_subscription,
        matcher=lambda _event: True,
        handle=handle,
    )
    handle._bind_close(manager._on_handle_close)
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda _event: None,
        lifecycle_probe=None,
        initial_invalid_reason=gateway_module._InvalidReason.CONNECTION_LOST,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )
    await manager._send_lock.acquire()
    invalid_task: asyncio.Task[Any] | None = None
    followup_close: asyncio.Task[Any] | None = None
    try:
        invalid_task = asyncio.create_task(anext(subscription))
        for _ in range(10):
            await asyncio.sleep(0)
            if handle._closing is not None:
                break
        assert handle._closing is not None
        assert manager._registry.is_empty is False

        invalid_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await invalid_task
        followup_close = asyncio.create_task(subscription.close())
        await asyncio.sleep(0)

        assert handle._closing.cancelled() is False
        assert followup_close.done() is False

        manager._send_lock.release()
        await followup_close

        assert handle._ended is True
        assert manager._registry.is_empty is True
        with pytest.raises(StopAsyncIteration):
            await anext(subscription)
    finally:
        if manager._send_lock.locked():
            manager._send_lock.release()
        if invalid_task is not None and not invalid_task.done():
            invalid_task.cancel()
        if followup_close is not None and not followup_close.done():
            followup_close.cancel()
        await asyncio.gather(
            *(task for task in (invalid_task, followup_close) if task is not None),
            return_exceptions=True,
        )
        await sdk_client.close()


async def test_cancelled_owned_close_task_cleans_real_sdk_registry() -> None:
    # Catches wrapper-task cancellation propagating into the real SDK close task.
    sdk_client = gateway_module.AsyncPublicClient()
    manager = sdk_client._get_market_manager()
    handle: Any = gateway_module.AsyncSubscriptionHandle(queue_size=1)
    sdk_subscription = gateway_module.MarketSpec(
        token_ids=("1001",),
        custom_feature_enabled=True,
    )
    manager._registry.add(
        sub=sdk_subscription,
        matcher=lambda _event: True,
        handle=handle,
    )
    handle._bind_close(manager._on_handle_close)
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda _event: None,
        lifecycle_probe=None,
        initial_invalid_reason=gateway_module._InvalidReason.CONNECTION_LOST,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )
    await manager._send_lock.acquire()
    first_close: asyncio.Task[Any] | None = None
    followup_close: asyncio.Task[Any] | None = None
    try:
        first_close = asyncio.create_task(subscription.close())
        for _ in range(10):
            await asyncio.sleep(0)
            if handle._closing is not None:
                break
        assert handle._closing is not None
        assert subscription._close_task is not None

        subscription._close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_close
        followup_close = asyncio.create_task(subscription.close())
        await asyncio.sleep(0)

        assert handle._closing.cancelled() is False
        assert followup_close.done() is False
        assert manager._registry.is_empty is False

        manager._send_lock.release()
        await followup_close

        assert handle._ended is True
        assert manager._registry.is_empty is True
        await subscription.close()
    finally:
        if manager._send_lock.locked():
            manager._send_lock.release()
        for task in (first_close, followup_close):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (first_close, followup_close) if task is not None),
            return_exceptions=True,
        )
        await sdk_client.close()


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_sdk_close_error_or_cancellation_keeps_wrapper_fail_closed(
    error_type: type[BaseException],
) -> None:
    # Catches a failed SDK close letting the wrapper resume or fake completion.
    handle = FailingCloseSubscriptionHandle(error_type)
    subscription = gateway_module.MarketSubscription(
        handle,
        mapper=lambda _event: None,
        lifecycle_probe=None,
        initial_invalid_reason=gateway_module._InvalidReason.CONNECTION_LOST,
        token_ids=("1001",),
        subscription_generation=1,
        clock_ms=lambda: 1_785_405_970_000,
        lifecycle_poll_interval=0.01,
    )
    error_match = "SDK close failed" if error_type is RuntimeError else None

    with pytest.raises(error_type, match=error_match):
        await anext(subscription)
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)
    with pytest.raises(error_type, match=error_match):
        await subscription.close()

    assert handle.close_calls == 1
    assert subscription._closed is False
    assert subscription._terminal is False


async def test_sdk_version_drift_fails_closed_as_invalid_generation(
    sdk_fixture: dict[str, tuple[Any, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Catches private state inspection continuing after an unreviewed SDK upgrade.
    client = FakePublicClient(**sdk_fixture)
    client.subscription_handle = BlockingSubscriptionHandle()
    monkeypatch.setattr(
        gateway_module.importlib.metadata,
        "version",
        lambda _distribution: "0.3.0b2",
    )
    gateway = PolymarketGateway(client=client, clock_ms=lambda: 1_785_405_970_000)
    await gateway.list_active_markets()

    subscription = await gateway.subscribe_markets(("1001",))
    invalid = await asyncio.wait_for(anext(subscription), timeout=0.2)

    assert invalid.reason == "sdk_version_changed"
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

    if entity == "event":
        with pytest.raises(GatewayMappingError, match=match):
            await gateway.list_active_events()
    else:
        snapshots = await gateway.list_active_markets()
        assert tuple(snapshot.market.id for snapshot in snapshots) == ("201",)
        assert gateway.market_mapping_warnings[0].market_id == "200"
        assert re.search(match, gateway.market_mapping_warnings[0].error)


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
