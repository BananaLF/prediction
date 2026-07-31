"""The sole boundary for Polymarket SDK imports."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
import importlib.metadata
import time
from typing import Any

from polymarket import AsyncPublicClient
from polymarket._internal.streams.handle import AsyncSubscriptionHandle
from polymarket.models.clob.market_events import (
    parse_market_event as _parse_pinned_market_event,
)
from polymarket.streams import MarketSpec

from predmarket.domain.decimal import encode_decimal
from predmarket.domain.fees import FeeModel, FeeSchedule
from predmarket.domain.json import freeze_json_object
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel


MAPPING_VERSION = "polymarket-client-0.3.0b1:v1"
PINNED_SDK_VERSION = "0.3.0b1"


class GatewayMappingError(ValueError):
    """The SDK returned an entity that cannot satisfy the domain contract."""


class GatewayLifecycleError(RuntimeError):
    """The pinned SDK lifecycle contract is absent or has changed."""


async def probe_pinned_sdk_lifecycle_shape() -> Mapping[str, Any]:
    """Validate the minimum private SDK shape used for fail-closed streaming."""
    version = importlib.metadata.version("polymarket-client")
    if version != PINNED_SDK_VERSION:
        raise GatewayLifecycleError(
            f"unsupported polymarket-client version: {version}"
        )
    client = AsyncPublicClient()
    try:
        if "_market_manager" not in vars(client):
            raise GatewayLifecycleError("SDK client has no _market_manager attribute")
        manager = client._get_market_manager()
        if "_connection" not in vars(manager):
            raise GatewayLifecycleError(
                "SDK market manager has no _connection attribute"
            )
        connection = manager._connection
        if "_socket" not in vars(connection):
            raise GatewayLifecycleError("SDK connection has no _socket attribute")
        handle: AsyncSubscriptionHandle[Any] = AsyncSubscriptionHandle(queue_size=1)
        return {
            "version": version,
            "client_manager_attribute": "_market_manager",
            "manager_connection_attribute": "_connection",
            "connection_socket_attribute": "_socket",
            "manager_open_property": "is_open",
            "manager_dropped_property": "dropped_events",
            "handle_dropped_property": "dropped",
            "initial_manager_open": manager.is_open,
            "initial_socket_is_none": connection._socket is None,
            "initial_manager_dropped": manager.dropped_events,
            "initial_handle_dropped": handle.dropped,
        }
    finally:
        await client.close()


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    market: Market
    tokens: tuple[Token, ...]
    mapping_version: str

    def __post_init__(self) -> None:
        if not isinstance(self.market, Market):
            raise ValueError("market must be a Market")
        tokens = tuple(self.tokens)
        if len(tokens) != 2 or any(not isinstance(token, Token) for token in tokens):
            raise ValueError("tokens must contain exactly two Token values")
        if any(token.market_id != self.market.id for token in tokens):
            raise ValueError("tokens must belong to market")
        if not isinstance(self.mapping_version, str) or not self.mapping_version:
            raise ValueError("mapping_version must be a non-empty string")
        object.__setattr__(self, "tokens", tokens)


@dataclass(frozen=True, slots=True)
class MarketStreamEvent:
    event_type: str
    market_id: str
    payload: Mapping[str, Any]
    received_timestamp: int
    subscription_generation: int
    mapping_version: str

    def __post_init__(self) -> None:
        _require_string(self.event_type, "stream event type")
        _require_string(self.market_id, "stream market id")
        if type(self.received_timestamp) is not int or self.received_timestamp < 0:
            raise ValueError("received_timestamp must be a non-negative integer")
        if (
            type(self.subscription_generation) is not int
            or self.subscription_generation < 1
        ):
            raise ValueError("subscription_generation must be at least one")
        _require_string(self.mapping_version, "mapping_version")
        object.__setattr__(
            self,
            "payload",
            freeze_json_object(self.payload, field_name="stream payload"),
        )


class _InvalidReason(str, Enum):
    CONNECTION_LOST = "connection_lost"
    CONNECTION_REPLACED = "connection_replaced"
    SDK_EVENT_DROPPED = "sdk_event_dropped"
    SUBSCRIPTION_EVENT_DROPPED = "subscription_event_dropped"
    SDK_LIFECYCLE_SHAPE_CHANGED = "sdk_lifecycle_shape_changed"
    SDK_LIFECYCLE_STATE_UNKNOWN = "sdk_lifecycle_state_unknown"
    SDK_VERSION_CHANGED = "sdk_version_changed"
    SDK_HANDLE_ENDED = "sdk_handle_ended"
    SDK_EVENT_INVALID = "sdk_event_invalid"


@dataclass(frozen=True, slots=True)
class MarketStreamInvalidated:
    reason: str
    token_ids: tuple[str, ...]
    received_timestamp: int
    subscription_generation: int
    mapping_version: str

    def __post_init__(self) -> None:
        if self.reason not in {reason.value for reason in _InvalidReason}:
            raise ValueError("unknown stream invalidation reason")
        object.__setattr__(self, "token_ids", _token_ids(self.token_ids))
        if type(self.received_timestamp) is not int or self.received_timestamp < 0:
            raise ValueError("received_timestamp must be a non-negative integer")
        if (
            type(self.subscription_generation) is not int
            or self.subscription_generation < 1
        ):
            raise ValueError("subscription_generation must be at least one")
        _require_string(self.mapping_version, "mapping_version")


@dataclass(slots=True)
class _SdkLifecycleProbe:
    client: Any
    handle: Any
    manager: Any
    connection: Any
    socket: Any
    manager_dropped: int
    handle_dropped: int

    @classmethod
    def capture(
        cls,
        *,
        client: Any,
        handle: Any,
    ) -> tuple["_SdkLifecycleProbe | None", _InvalidReason | None]:
        if importlib.metadata.version("polymarket-client") != PINNED_SDK_VERSION:
            return None, _InvalidReason.SDK_VERSION_CHANGED
        try:
            manager = vars(client)["_market_manager"]
            connection = vars(manager)["_connection"]
            socket = vars(connection)["_socket"]
            manager_open = manager.is_open
            manager_dropped = manager.dropped_events
            handle_dropped = handle.dropped
        except (AttributeError, KeyError, TypeError):
            return None, _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if (
            type(manager_open) is not bool
            or type(manager_dropped) is not int
            or manager_dropped < 0
            or type(handle_dropped) is not int
            or handle_dropped < 0
        ):
            return None, _InvalidReason.SDK_LIFECYCLE_STATE_UNKNOWN
        if handle_dropped != 0:
            return None, _InvalidReason.SUBSCRIPTION_EVENT_DROPPED
        if not manager_open or socket is None:
            return None, _InvalidReason.CONNECTION_LOST
        return (
            cls(
                client=client,
                handle=handle,
                manager=manager,
                connection=connection,
                socket=socket,
                manager_dropped=manager_dropped,
                handle_dropped=handle_dropped,
            ),
            None,
        )

    def check(self) -> _InvalidReason | None:
        if importlib.metadata.version("polymarket-client") != PINNED_SDK_VERSION:
            return _InvalidReason.SDK_VERSION_CHANGED
        try:
            manager = vars(self.client)["_market_manager"]
            connection = vars(manager)["_connection"]
            socket = vars(connection)["_socket"]
            manager_open = manager.is_open
            manager_dropped = manager.dropped_events
            handle_dropped = self.handle.dropped
        except (AttributeError, KeyError, TypeError):
            return _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if manager is not self.manager or connection is not self.connection:
            return _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if (
            type(manager_open) is not bool
            or type(manager_dropped) is not int
            or manager_dropped < self.manager_dropped
            or type(handle_dropped) is not int
            or handle_dropped < self.handle_dropped
        ):
            return _InvalidReason.SDK_LIFECYCLE_STATE_UNKNOWN
        if manager_dropped > self.manager_dropped:
            return _InvalidReason.SDK_EVENT_DROPPED
        if handle_dropped > self.handle_dropped:
            return _InvalidReason.SUBSCRIPTION_EVENT_DROPPED
        if not manager_open or socket is None:
            return _InvalidReason.CONNECTION_LOST
        if socket is not self.socket:
            return _InvalidReason.CONNECTION_REPLACED
        return None


class MarketSubscription(
    AsyncIterator[MarketStreamEvent | MarketStreamInvalidated]
):
    def __init__(
        self,
        handle: Any,
        *,
        mapper: Callable[[Any], MarketStreamEvent | None],
        lifecycle_probe: _SdkLifecycleProbe | None,
        initial_invalid_reason: _InvalidReason | None,
        token_ids: tuple[str, ...],
        subscription_generation: int,
        clock_ms: Callable[[], int],
        lifecycle_poll_interval: float,
    ) -> None:
        self._handle = handle
        self._iterator = handle.__aiter__()
        self._mapper = mapper
        self._lifecycle_probe = lifecycle_probe
        self._initial_invalid_reason = initial_invalid_reason
        self._token_ids = token_ids
        self._subscription_generation = subscription_generation
        self._clock_ms = clock_ms
        self._lifecycle_poll_interval = lifecycle_poll_interval
        self._closed = False
        self._terminal = False
        self._close_task: asyncio.Task[None] | None = None
        self._close_error: BaseException | None = None
        self._invalid_reason: _InvalidReason | None = None
        self._buffered_events: deque[MarketStreamEvent] = deque()

    def __aiter__(self) -> "MarketSubscription":
        return self

    @property
    def subscription_generation(self) -> int:
        return self._subscription_generation

    async def __anext__(self) -> MarketStreamEvent | MarketStreamInvalidated:
        if self._close_task is not None or self._closed or self._terminal:
            raise StopAsyncIteration
        if self._buffered_events:
            return self._buffered_events.popleft()
        return await self._next_live()

    async def _next_live(self) -> MarketStreamEvent | MarketStreamInvalidated:
        if self._initial_invalid_reason is not None:
            reason = self._initial_invalid_reason
            self._initial_invalid_reason = None
            return await self._invalidate(reason)

        while True:
            assert self._lifecycle_probe is not None
            reason = self._lifecycle_probe.check()
            if reason is not None:
                return await self._invalidate(reason)

            event_task = asyncio.create_task(self._iterator.__anext__())
            lifecycle_task = asyncio.create_task(self._wait_for_invalidation())
            try:
                done, _pending = await asyncio.wait(
                    (event_task, lifecycle_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except BaseException:
                event_task.cancel()
                lifecycle_task.cancel()
                await asyncio.gather(
                    event_task,
                    lifecycle_task,
                    return_exceptions=True,
                )
                raise
            if lifecycle_task in done:
                event_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await event_task
                return await self._invalidate(lifecycle_task.result())

            lifecycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await lifecycle_task
            try:
                sdk_event = event_task.result()
            except StopAsyncIteration:
                return await self._invalidate(_InvalidReason.SDK_HANDLE_ENDED)

            reason = self._lifecycle_probe.check()
            if reason is not None:
                return await self._invalidate(reason)
            try:
                mapped = self._mapper(sdk_event)
            except GatewayMappingError:
                return await self._invalidate(_InvalidReason.SDK_EVENT_INVALID)
            if mapped is not None:
                return mapped

    async def _guard_awaitable(self, awaitable: Awaitable[Any]) -> Any:
        """Buffer stream events while rejecting an invalid recovery baseline."""
        operation_task = asyncio.ensure_future(awaitable)
        live_task: asyncio.Task[
            MarketStreamEvent | MarketStreamInvalidated
        ] | None = None
        buffered: list[MarketStreamEvent] = []
        try:
            while True:
                live_task = asyncio.create_task(self._next_live())
                done, _pending = await asyncio.wait(
                    (operation_task, live_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if live_task not in done:
                    await asyncio.sleep(0)
                if live_task.done():
                    item = live_task.result()
                    live_task = None
                    if isinstance(item, MarketStreamInvalidated):
                        raise GatewayLifecycleError(
                            f"recovery stream invalidated: {item.reason}"
                        )
                    buffered.append(item)
                    if not operation_task.done():
                        continue
                else:
                    live_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await live_task
                    live_task = None

                result = operation_task.result()
                reason = self._current_invalid_reason()
                if reason is not None:
                    await self._invalidate(reason)
                    raise GatewayLifecycleError(
                        f"recovery stream invalidated: {reason.value}"
                    )
                self._buffered_events.extend(buffered)
                return result
        except BaseException:
            for task in (live_task, operation_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(
                *(task for task in (live_task, operation_task) if task is not None),
                return_exceptions=True,
            )
            raise

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._finish_close())
        await asyncio.shield(self._close_task)
        if self._close_error is not None:
            raise self._close_error

    async def _finish_close(self) -> None:
        try:
            await self._handle.close()
        except BaseException as error:
            self._close_error = error
        else:
            self._closed = True
            self._terminal = True

    async def _wait_for_invalidation(self) -> _InvalidReason:
        assert self._lifecycle_probe is not None
        while True:
            await asyncio.sleep(self._lifecycle_poll_interval)
            reason = self._lifecycle_probe.check()
            if reason is not None:
                return reason

    def _current_invalid_reason(self) -> _InvalidReason | None:
        if self._invalid_reason is not None:
            return self._invalid_reason
        if self._initial_invalid_reason is not None:
            return self._initial_invalid_reason
        if self._lifecycle_probe is None:
            return _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        return self._lifecycle_probe.check()

    async def _invalidate(
        self,
        reason: _InvalidReason,
    ) -> MarketStreamInvalidated:
        self._invalid_reason = reason
        await self.close()
        return MarketStreamInvalidated(
            reason=reason.value,
            token_ids=self._token_ids,
            received_timestamp=self._clock_ms(),
            subscription_generation=self._subscription_generation,
            mapping_version=MAPPING_VERSION,
        )


@dataclass(frozen=True, slots=True)
class MarketRecoverySession:
    order_books: tuple[OrderBook, ...]
    subscription: MarketSubscription
    subscription_generation: int

    def __post_init__(self) -> None:
        books = tuple(self.order_books)
        if not books or any(not isinstance(book, OrderBook) for book in books):
            raise ValueError("order_books must contain OrderBook values")
        if not isinstance(self.subscription, MarketSubscription):
            raise ValueError("subscription must be a MarketSubscription")
        if (
            type(self.subscription_generation) is not int
            or self.subscription_generation < 1
        ):
            raise ValueError("subscription_generation must be at least one")
        if any(
            book.subscription_generation != self.subscription_generation
            for book in books
        ):
            raise ValueError("order books must share the session generation")
        if self.subscription.subscription_generation != self.subscription_generation:
            raise ValueError("subscription must share the session generation")
        object.__setattr__(self, "order_books", books)


class PolymarketGateway:
    def __init__(
        self,
        client: Any | None = None,
        *,
        clock_ms: Callable[[], int] | None = None,
        page_size: int = 100,
        lifecycle_poll_interval: float = 0.01,
    ) -> None:
        if type(page_size) is not int or page_size < 1:
            raise ValueError("page_size must be a positive integer")
        if (
            isinstance(lifecycle_poll_interval, bool)
            or not isinstance(lifecycle_poll_interval, (int, float))
            or lifecycle_poll_interval <= 0
        ):
            raise ValueError("lifecycle_poll_interval must be positive")
        self._client = client if client is not None else AsyncPublicClient()
        self._clock_ms = clock_ms or _system_clock_ms
        self._page_size = page_size
        self._lifecycle_poll_interval = float(lifecycle_poll_interval)
        self._sync_counter = 0
        self._sync_generation: str | None = None
        self._subscription_generation = 0
        self._market_id_by_condition_id: dict[str, str] = {}
        self._condition_id_by_market_id: dict[str, str] = {}
        self._token_identity_by_id: dict[str, tuple[str, str]] = {}
        self._token_ids_by_market_id: dict[str, frozenset[str]] = {}
        self._closed = False

    async def list_active_events(self) -> tuple[Event, ...]:
        received_at = self._now()
        generation = self._start_sync_generation(received_at)
        paginator = self._client.list_events(closed=False, page_size=self._page_size)
        events: list[Event] = []
        async for page in paginator:
            for sdk_event in page.items:
                event = _map_event(
                    sdk_event,
                    received_at=received_at,
                    sync_generation=generation,
                )
                if event.status is MarketStatus.ACTIVE:
                    events.append(event)
        return tuple(events)

    async def list_active_markets(self) -> tuple[MarketSnapshot, ...]:
        received_at = self._now()
        generation = self._current_sync_generation(received_at)
        paginator = self._client.list_markets(closed=False, page_size=self._page_size)
        snapshots: list[MarketSnapshot] = []
        async for page in paginator:
            for sdk_market in page.items:
                snapshot = _map_market(
                    sdk_market,
                    received_at=received_at,
                    sync_generation=generation,
                )
                if (
                    snapshot.market.status is MarketStatus.ACTIVE
                    and snapshot.market.active
                ):
                    self._remember_market(snapshot)
                    snapshots.append(snapshot)
        return tuple(snapshots)

    async def get_order_books(self, token_ids: Sequence[str]) -> tuple[OrderBook, ...]:
        requested = _token_ids(token_ids)
        generation = max(1, self._subscription_generation)
        return await self._get_order_books_for_generation(
            requested,
            subscription_generation=generation,
        )

    async def recover_market_session(
        self,
        token_ids: Sequence[str],
    ) -> MarketRecoverySession:
        normalized = _token_ids(token_ids)
        self._subscription_generation += 1
        generation = self._subscription_generation
        subscription = await self._subscribe_markets_for_generation(
            normalized,
            subscription_generation=generation,
        )
        try:
            books = await subscription._guard_awaitable(
                self._get_order_books_for_generation(
                    normalized,
                    subscription_generation=generation,
                )
            )
        except BaseException:
            await subscription.close()
            raise
        return MarketRecoverySession(
            order_books=books,
            subscription=subscription,
            subscription_generation=generation,
        )

    async def _get_order_books_for_generation(
        self,
        requested: tuple[str, ...],
        *,
        subscription_generation: int,
    ) -> tuple[OrderBook, ...]:
        received_at = self._now()
        sdk_books = await self._client.get_order_books(token_ids=requested)
        mapped: dict[str, OrderBook] = {}
        for sdk_book in sdk_books:
            token_id = _entity_identifier(sdk_book, "token_id", fallback="unknown")
            if token_id in mapped:
                raise GatewayMappingError(
                    f"order books contain duplicate token {token_id}"
                )
            book = self._map_order_book(
                sdk_book,
                received_at=received_at,
                subscription_generation=subscription_generation,
            )
            mapped[book.token_id] = book

        requested_set = set(requested)
        returned_set = set(mapped)
        unexpected = returned_set - requested_set
        if unexpected:
            joined = ", ".join(sorted(unexpected))
            raise GatewayMappingError(
                f"order books contain unexpected tokens: {joined}"
            )
        missing = requested_set - returned_set
        if missing:
            joined = ", ".join(sorted(missing))
            raise GatewayMappingError(
                f"order books are missing requested tokens: {joined}"
            )
        return tuple(mapped[token_id] for token_id in requested)

    async def subscribe_markets(self, token_ids: Sequence[str]) -> MarketSubscription:
        normalized = _token_ids(token_ids)
        self._subscription_generation += 1
        generation = self._subscription_generation
        return await self._subscribe_markets_for_generation(
            normalized,
            subscription_generation=generation,
        )

    async def _subscribe_markets_for_generation(
        self,
        normalized: tuple[str, ...],
        *,
        subscription_generation: int,
    ) -> MarketSubscription:
        handle = await self._client.subscribe(
            MarketSpec(
                token_ids=normalized,
                custom_feature_enabled=True,
            )
        )
        lifecycle_probe, initial_invalid_reason = _SdkLifecycleProbe.capture(
            client=self._client,
            handle=handle,
        )
        return MarketSubscription(
            handle,
            mapper=lambda event: self._map_stream_event(
                event,
                subscription_generation=subscription_generation,
                subscribed_token_ids=normalized,
            ),
            lifecycle_probe=lifecycle_probe,
            initial_invalid_reason=initial_invalid_reason,
            token_ids=normalized,
            subscription_generation=subscription_generation,
            clock_ms=self._now,
            lifecycle_poll_interval=self._lifecycle_poll_interval,
        )

    async def refresh_market(self, market_id: str) -> MarketSnapshot:
        market_id = _require_string(market_id, "market id")
        received_at = self._now()
        generation = self._current_sync_generation(received_at)
        sdk_market = await self._client.get_market(id=market_id)
        snapshot = _map_market(
            sdk_market,
            received_at=received_at,
            sync_generation=generation,
        )
        if snapshot.market.id != market_id:
            raise GatewayMappingError(
                f"market refresh requested {market_id} "
                f"but SDK returned {snapshot.market.id}"
            )
        self._remember_market(snapshot)
        return snapshot

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.close()

    def _now(self) -> int:
        value = self._clock_ms()
        if type(value) is not int or value < 0:
            raise ValueError("clock_ms must return a non-negative integer")
        return value

    def _start_sync_generation(self, received_at: int) -> str:
        self._sync_counter += 1
        self._sync_generation = f"{MAPPING_VERSION}:{received_at}:{self._sync_counter}"
        return self._sync_generation

    def _current_sync_generation(self, received_at: int) -> str:
        if self._sync_generation is None:
            return self._start_sync_generation(received_at)
        return self._sync_generation

    def _remember_market(self, snapshot: MarketSnapshot) -> None:
        market = snapshot.market
        existing = self._market_id_by_condition_id.get(market.condition_id)
        if existing is not None and existing != market.id:
            raise GatewayMappingError(
                f"condition {market.condition_id} maps to both "
                f"{existing} and {market.id}"
            )
        new_token_ids = frozenset(token.id for token in snapshot.tokens)
        for token_id in new_token_ids:
            identity = self._token_identity_by_id.get(token_id)
            if identity is not None and identity[0] != market.id:
                raise GatewayMappingError(
                    f"token {token_id} maps to both market "
                    f"{identity[0]} and {market.id}"
                )

        previous_condition = self._condition_id_by_market_id.get(market.id)
        if (
            previous_condition is not None
            and previous_condition != market.condition_id
            and self._market_id_by_condition_id.get(previous_condition) == market.id
        ):
            del self._market_id_by_condition_id[previous_condition]
        for stale_token_id in (
            self._token_ids_by_market_id.get(market.id, frozenset()) - new_token_ids
        ):
            stale_identity = self._token_identity_by_id.get(stale_token_id)
            if stale_identity is not None and stale_identity[0] == market.id:
                del self._token_identity_by_id[stale_token_id]

        self._market_id_by_condition_id[market.condition_id] = market.id
        self._condition_id_by_market_id[market.id] = market.condition_id
        self._token_ids_by_market_id[market.id] = new_token_ids
        for token_id in new_token_ids:
            self._token_identity_by_id[token_id] = (market.id, market.condition_id)

    def _map_order_book(
        self,
        sdk_book: Any,
        *,
        received_at: int,
        subscription_generation: int,
    ) -> OrderBook:
        token_id = _entity_identifier(sdk_book, "token_id", fallback="unknown")
        try:
            condition_id = _require_string(
                getattr(sdk_book, "condition_id"),
                "condition id",
            )
            expected_identity = self._token_identity_by_id.get(token_id)
            if expected_identity is None:
                raise GatewayMappingError(
                    f"order book {token_id} has no mapped token identity"
                )
            if expected_identity[1] != condition_id:
                raise GatewayMappingError(
                    f"order book {token_id} identity condition "
                    f"{expected_identity[1]} does not match {condition_id}"
                )
            try:
                market_id = self._market_id_by_condition_id[condition_id]
            except KeyError as error:
                raise GatewayMappingError(
                    f"condition {condition_id} has no mapped SDK market id"
                ) from error
            if expected_identity[0] != market_id:
                raise GatewayMappingError(
                    f"order book {token_id} identity market "
                    f"{expected_identity[0]} does not match {market_id}"
                )
            timestamp = _timestamp_ms(
                getattr(sdk_book, "timestamp"),
                "timestamp",
                required=True,
            )
            assert timestamp is not None
            return OrderBook(
                market_id=market_id,
                token_id=_require_string(getattr(sdk_book, "token_id"), "token id"),
                bids=tuple(
                    _map_order_book_level(level, "bid")
                    for level in getattr(sdk_book, "bids")
                ),
                asks=tuple(
                    _map_order_book_level(level, "ask")
                    for level in getattr(sdk_book, "asks")
                ),
                subscription_generation=subscription_generation,
                book_hash=_require_string(getattr(sdk_book, "hash"), "book hash"),
                exchange_timestamp=timestamp,
                received_timestamp=received_at,
                tick_size=_decimal(getattr(sdk_book, "tick_size"), "tick size"),
                minimum_order_size=_decimal(
                    getattr(sdk_book, "min_order_size"),
                    "minimum order size",
                ),
            )
        except GatewayMappingError:
            raise
        except (AttributeError, TypeError, ValueError) as error:
            raise GatewayMappingError(f"order book {token_id}: {error}") from error

    def _map_stream_event(
        self,
        sdk_event: Any,
        *,
        subscription_generation: int,
        subscribed_token_ids: tuple[str, ...],
    ) -> MarketStreamEvent | None:
        event_type = _entity_identifier(sdk_event, "type", fallback="unknown")
        if event_type == "new_market":
            return None
        try:
            payload_model = getattr(sdk_event, "payload")
            condition_id = _require_string(
                getattr(payload_model, "market"),
                "stream condition id",
            )
            try:
                market_id = self._market_id_by_condition_id[condition_id]
            except KeyError as error:
                raise GatewayMappingError(
                    f"stream condition {condition_id} has no mapped SDK market id"
                ) from error
            payload = _json_mapping(payload_model.model_dump(mode="json"))
            event_token_ids: tuple[str, ...]
            if event_type == "price_change":
                changes = getattr(payload_model, "price_changes")
                event_token_ids = tuple(
                    _require_string(getattr(change, "token_id"), "stream token id")
                    for change in changes
                )
            elif event_type == "market_resolved":
                event_token_ids = tuple(
                    _require_string(token_id, "stream token id")
                    for token_id in getattr(payload_model, "token_ids")
                )
                resolved_market_id = _require_string(
                    getattr(payload_model, "id"),
                    "resolved market id",
                )
                if resolved_market_id != market_id:
                    raise GatewayMappingError(
                        f"stream market identity {resolved_market_id} "
                        f"does not match {market_id}"
                    )
            elif event_type in {
                "book",
                "last_trade_price",
                "tick_size_change",
                "best_bid_ask",
            }:
                event_token_ids = (
                    _require_string(
                        getattr(payload_model, "token_id"),
                        "stream token id",
                    ),
                )
            else:
                raise GatewayMappingError(
                    f"unsupported stream event type {event_type}"
                )

            for token_id in event_token_ids:
                identity = self._token_identity_by_id.get(token_id)
                if identity is None:
                    raise GatewayMappingError(
                        f"stream token {token_id} has no mapped identity"
                    )
                if identity != (market_id, condition_id):
                    raise GatewayMappingError(
                        f"stream token {token_id} identity {identity} "
                        f"does not match {(market_id, condition_id)}"
                    )

            subscribed = frozenset(subscribed_token_ids)
            if event_type == "price_change":
                payload["price_changes"] = [
                    change
                    for change in payload["price_changes"]
                    if change["token_id"] in subscribed
                ]
                if not payload["price_changes"]:
                    return None
            elif event_type != "market_resolved" and not subscribed.intersection(
                event_token_ids
            ):
                raise GatewayMappingError(
                    f"stream event {event_type} has no subscribed token"
                )
            return MarketStreamEvent(
                event_type=_require_string(event_type, "stream event type"),
                market_id=market_id,
                payload=payload,
                received_timestamp=self._now(),
                subscription_generation=subscription_generation,
                mapping_version=MAPPING_VERSION,
            )
        except GatewayMappingError:
            raise
        except (AttributeError, TypeError, ValueError) as error:
            raise GatewayMappingError(f"stream event {event_type}: {error}") from error


def _map_event(
    sdk_event: Any,
    *,
    received_at: int,
    sync_generation: str,
) -> Event:
    event_id = _entity_identifier(sdk_event, "id", fallback="unknown")
    try:
        state = getattr(sdk_event, "state")
        schedule = getattr(sdk_event, "schedule")
        trading = getattr(sdk_event, "trading")
        market_ids = tuple(
            _require_string(getattr(market, "id"), "market id")
            for market in getattr(sdk_event, "markets")
        )
        neg_risk = getattr(trading, "neg_risk") is True
        neg_risk_metadata = {
            "mapping_version": MAPPING_VERSION,
            "enable_neg_risk": getattr(trading, "enable_neg_risk"),
            "neg_risk_augmented": getattr(trading, "neg_risk_augmented"),
            "cumulative_markets": getattr(trading, "cumulative_markets"),
            "neg_risk_fee_bips": _optional_number_string(
                getattr(trading, "neg_risk_fee_bips")
            ),
        }
        created_at = _timestamp_ms(
            getattr(sdk_event, "created_at"),
            "created_at",
            required=False,
        )
        updated_at = _timestamp_ms(
            getattr(sdk_event, "updated_at"),
            "updated_at",
            required=False,
        )
        return Event(
            id=_require_string(getattr(sdk_event, "id"), "event id"),
            title=_require_string(getattr(sdk_event, "title"), "title"),
            status=_event_status(state),
            market_ids=market_ids,
            sync_generation=sync_generation,
            sync_generation_complete=True,
            slug=_optional_string(getattr(sdk_event, "slug"), "slug"),
            description=_optional_string(
                getattr(sdk_event, "description"),
                "description",
            ),
            neg_risk=neg_risk,
            neg_risk_id=(
                _optional_string(
                    getattr(trading, "neg_risk_market_id"),
                    "neg_risk_market_id",
                )
                if neg_risk
                else None
            ),
            # 0.3.0b1 exposes flags but no authoritative NegRisk type,
            # member ordering, conversion capability, or completeness proof.
            neg_risk_type=None,
            neg_risk_complete=False,
            neg_risk_conversion_supported=False,
            neg_risk_metadata=neg_risk_metadata,
            neg_risk_synced_at=received_at,
            start_at=_timestamp_ms(
                getattr(schedule, "start_date"),
                "start_date",
                required=False,
            ),
            end_at=_timestamp_ms(
                getattr(schedule, "end_date"),
                "end_date",
                required=False,
            ),
            resolved_at=_timestamp_ms(
                getattr(schedule, "closed_time"),
                "closed_time",
                required=False,
            ),
            source_updated_at=updated_at,
            created_at=created_at if created_at is not None else received_at,
            updated_at=updated_at if updated_at is not None else received_at,
        )
    except GatewayMappingError:
        raise
    except (AttributeError, TypeError, ValueError) as error:
        raise GatewayMappingError(f"event {event_id}: {error}") from error


def _map_market(
    sdk_market: Any,
    *,
    received_at: int,
    sync_generation: str,
) -> MarketSnapshot:
    market_id = _entity_identifier(sdk_market, "id", fallback="unknown")
    try:
        state = getattr(sdk_market, "state")
        outcomes = getattr(sdk_market, "outcomes")
        trading = getattr(sdk_market, "trading")
        events = tuple(getattr(sdk_market, "events"))
        if len(events) != 1:
            raise ValueError("events must contain exactly one event reference")
        event_id = _require_string(getattr(events[0], "id"), "event id")
        condition_id = _require_string(
            getattr(sdk_market, "condition_id"),
            "condition id",
        )
        fee_schedule = _map_fee_schedule(trading, received_at=received_at)
        market_status = _market_status(sdk_market)
        token_models = (getattr(outcomes, "yes"), getattr(outcomes, "no"))
        tokens = tuple(
            Token(
                id=_require_string(getattr(outcome, "token_id"), "token id"),
                market_id=_require_string(getattr(sdk_market, "id"), "market id"),
                outcome=_require_string(getattr(outcome, "label"), "outcome label"),
                position=position,
                sync_generation=sync_generation,
                sync_generation_complete=True,
                fee_schedule=fee_schedule,
                fee_updated_at=(
                    None if fee_schedule is None else fee_schedule.updated_at
                ),
                created_at=received_at,
                updated_at=received_at,
            )
            for position, outcome in enumerate(token_models)
        )
        market = Market(
            id=_require_string(getattr(sdk_market, "id"), "market id"),
            event_id=event_id,
            condition_id=condition_id,
            question=_require_string(getattr(sdk_market, "question"), "question"),
            status=market_status,
            active=market_status is MarketStatus.ACTIVE,
            accepting_orders=getattr(state, "accepting_orders") is True,
            enable_orderbook=getattr(state, "enable_order_book") is True,
            sync_generation=sync_generation,
            sync_generation_complete=True,
            slug=_optional_string(getattr(sdk_market, "slug"), "slug"),
            description=_optional_string(
                getattr(sdk_market, "description"),
                "description",
            ),
            neg_risk=getattr(state, "neg_risk") is True,
            # The pinned public model has no authoritative member position.
            neg_risk_outcome_position=None,
            neg_risk_member_complete=False,
            tick_size=_optional_decimal(
                getattr(trading, "minimum_tick_size"),
                "minimum tick size",
            ),
            minimum_order_size=_optional_decimal(
                getattr(trading, "minimum_order_size"),
                "minimum order size",
            ),
            end_at=_timestamp_ms(
                getattr(state, "end_date"),
                "end_date",
                required=False,
            ),
            resolved_at=_timestamp_ms(
                getattr(state, "closed_time"),
                "closed_time",
                required=False,
            ),
            source_updated_at=None,
            created_at=received_at,
            updated_at=received_at,
        )
        return MarketSnapshot(
            market=market,
            tokens=tokens,
            mapping_version=MAPPING_VERSION,
        )
    except GatewayMappingError:
        raise
    except (AttributeError, TypeError, ValueError) as error:
        raise GatewayMappingError(f"market {market_id}: {error}") from error


def _map_fee_schedule(trading: Any, *, received_at: int) -> FeeSchedule | None:
    enabled = getattr(trading, "fees_enabled")
    source = f"{MAPPING_VERSION}:Market.trading"
    if enabled is False:
        return FeeSchedule(
            model=FeeModel.ZERO,
            enabled=False,
            source=source,
            parameters={},
            updated_at=received_at,
        )
    if enabled is None:
        return None
    if enabled is not True:
        raise ValueError("fees_enabled must be a boolean or None")
    fee_type = _require_string(getattr(trading, "fee_type"), "fee type").lower()
    sdk_schedule = getattr(trading, "fee_schedule")
    if sdk_schedule is None:
        raise ValueError("enabled fees require a fee schedule")
    exponent = getattr(sdk_schedule, "exponent")
    rebate_rate = _decimal(getattr(sdk_schedule, "rebate_rate"), "fee rebate rate")
    if fee_type != "flat" or exponent != 0 or rebate_rate != Decimal("0"):
        raise ValueError("SDK fee schedule cannot be represented by the FLAT domain model")
    return FeeSchedule(
        model=FeeModel.FLAT,
        enabled=True,
        source=source,
        parameters={"rate": _decimal(getattr(sdk_schedule, "rate"), "fee rate")},
        updated_at=received_at,
    )


def _map_order_book_level(sdk_level: Any, side: str) -> OrderBookLevel:
    try:
        return OrderBookLevel(
            price=_decimal(getattr(sdk_level, "price"), f"{side} price"),
            size=_decimal(getattr(sdk_level, "size"), f"{side} size"),
        )
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"malformed {side} level: {error}") from error


def _event_status(state: Any) -> MarketStatus:
    if getattr(state, "archived") is True:
        return MarketStatus.ARCHIVED
    if getattr(state, "closed") is True or getattr(state, "ended") is True:
        return MarketStatus.CLOSED
    if getattr(state, "active") is True:
        return MarketStatus.ACTIVE
    return MarketStatus.CLOSED


def _market_status(sdk_market: Any) -> MarketStatus:
    state = getattr(sdk_market, "state")
    active = getattr(state, "active")
    closed = getattr(state, "closed")
    archived = getattr(state, "archived")
    for field_name, value in (
        ("active", active),
        ("closed", closed),
        ("archived", archived),
    ):
        if type(value) is not bool:
            raise ValueError(f"market state {field_name} must be a boolean")
    if active and (closed or archived):
        raise ValueError("contradictory active and closed/archived market state")
    if closed and archived:
        raise ValueError("contradictory closed and archived market state")
    if archived:
        return MarketStatus.ARCHIVED
    if closed:
        resolution_status = getattr(
            getattr(sdk_market, "resolution"),
            "uma_resolution_status",
        )
        value = (
            resolution_status.value
            if isinstance(resolution_status, Enum)
            else resolution_status
        )
        if value in {"resolved", "settled"}:
            return MarketStatus.RESOLVED
        return MarketStatus.CLOSED
    if active:
        return MarketStatus.ACTIVE
    return MarketStatus.CLOSED


def _token_ids(token_ids: Sequence[str]) -> tuple[str, ...]:
    if isinstance(token_ids, (str, bytes)):
        raise ValueError("token_ids must be a sequence of token ids")
    try:
        normalized = tuple(_require_string(value, "token id") for value in token_ids)
    except TypeError as error:
        raise ValueError("token_ids must be a sequence of token ids") from error
    if not normalized:
        raise ValueError("token_ids must not be empty")
    if len(normalized) != len(set(normalized)):
        raise ValueError("token_ids must not contain duplicates")
    return normalized


def _require_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _optional_string(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field_name)


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)  # type: ignore[arg-type]
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be a finite decimal") from error
    if not result.is_finite():
        raise ValueError(f"{field_name} must be a finite decimal")
    return result


def _optional_decimal(value: object, field_name: str) -> Decimal | None:
    return None if value is None else _decimal(value, field_name)


def _timestamp_ms(
    value: object,
    field_name: str,
    *,
    required: bool,
) -> int | None:
    if value is None:
        if required:
            raise ValueError(f"{field_name} must be present")
        return None
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    utc_value = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc_value - epoch
    return (
        delta.days * 86_400_000
        + delta.seconds * 1_000
        + delta.microseconds // 1_000
    )


def _optional_number_string(value: object) -> str | None:
    if value is None:
        return None
    return encode_decimal(_decimal(str(value), "number"))


def _entity_identifier(entity: Any, field_name: str, *, fallback: str) -> str:
    value = getattr(entity, field_name, None)
    return value if isinstance(value, str) and value else fallback


def _json_mapping(value: object) -> dict[str, Any]:
    converted = _json_value(value)
    if not isinstance(converted, dict):
        raise ValueError("SDK payload must be a JSON object")
    return converted


def _json_value(value: object) -> Any:
    if value is None or type(value) in (str, int, bool):
        return value
    if isinstance(value, Decimal):
        return encode_decimal(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return encode_decimal(_decimal(str(value), "JSON number"))
    if isinstance(value, Mapping):
        return {
            _require_string(key, "JSON key"): _json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise ValueError(f"unsupported SDK payload value: {type(value).__name__}")


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000
