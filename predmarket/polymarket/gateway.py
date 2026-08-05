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
import json
import logging
import time
from typing import Any

from pydantic import ValidationError
from polymarket import AsyncPublicClient
from polymarket.errors import (
    RequestRejectedError,
    TimeoutError as PolymarketTimeoutError,
    TransportError,
)
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
_MAX_MAPPING_RESPONSE_CHARS = 8_192
_MAX_MALFORMED_EVENT_SAMPLE_CHARS = 1_024
_MALFORMED_NEW_MARKET_LOG_INTERVAL = 100
_MARKET_STREAM_PROGRESS_INTERVAL_SECONDS = 10.0
_MARKET_STREAM_HANDOFF_QUEUE_CAPACITY = 4_096
_MARKET_STREAM_CLOSED = object()
_LOGGER = logging.getLogger(__name__)


class GatewayMappingError(ValueError):
    """The SDK returned an entity that cannot satisfy the domain contract."""

    def __init__(self, message: str, *, market_id: str | None = None) -> None:
        super().__init__(message)
        self.market_id = market_id


class MissingOrderBooksError(GatewayMappingError):
    """The CLOB no longer has books for a requested token subset."""

    def __init__(self, token_ids: Sequence[str]) -> None:
        normalized = tuple(sorted(set(token_ids), key=lambda value: value.encode("utf-8")))
        self.token_ids = normalized
        super().__init__(
            "order books are missing requested tokens: " + ", ".join(normalized)
        )


@dataclass(frozen=True, slots=True)
class MarketMappingWarning:
    """A malformed individual market omitted from one sync response."""

    market_id: str
    error: str

    def __post_init__(self) -> None:
        _require_string(self.market_id, "market mapping warning market id")
        _require_string(self.error, "market mapping warning error")


class GatewayLifecycleError(RuntimeError):
    """The pinned SDK lifecycle contract is absent or has changed."""


class MarketRecoveryInvalidatedError(GatewayLifecycleError):
    """A recovery baseline lost its live-stream integrity barrier."""

    def __init__(self, reason: str) -> None:
        _require_string(reason, "recovery invalidation reason")
        self.reason = reason
        self.retryable = reason not in {
            _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED.value,
            _InvalidReason.SDK_LIFECYCLE_STATE_UNKNOWN.value,
            _InvalidReason.SDK_VERSION_CHANGED.value,
        }
        super().__init__(f"recovery stream invalidated: {reason}")


class MarketRecoveryTransientError(GatewayLifecycleError):
    """A temporary transport failure prevented a recovery baseline."""

    def __init__(
        self,
        reason: str,
        *,
        status: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        _require_string(reason, "recovery transient reason")
        self.reason = reason
        self.status = status
        self.retry_after = retry_after
        detail = f" status={status}" if status is not None else ""
        super().__init__(f"transient market recovery failure: {reason}{detail}")


def _market_recovery_transient_error(
    error: Exception,
) -> MarketRecoveryTransientError | None:
    if isinstance(error, RequestRejectedError):
        status = error.status
        if status in {408, 425, 429} or 500 <= status <= 599:
            return MarketRecoveryTransientError(
                "request_rejected",
                status=status,
                retry_after=error.retry_after,
            )
        return None
    if isinstance(error, PolymarketTimeoutError | TimeoutError):
        return MarketRecoveryTransientError("timeout")
    if isinstance(error, TransportError | ConnectionError | OSError):
        return MarketRecoveryTransientError("transport_error")
    return None


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
        if "_queue" not in vars(handle):
            raise GatewayLifecycleError("SDK handle has no _queue attribute")
        if "_ended" not in vars(handle):
            raise GatewayLifecycleError("SDK handle has no _ended attribute")
        queue_maxsize = handle._queue.maxsize
        if type(queue_maxsize) is not int or queue_maxsize < 1:
            raise GatewayLifecycleError("SDK handle queue maxsize is invalid")
        handle_ended = handle._ended
        if type(handle_ended) is not bool:
            raise GatewayLifecycleError("SDK handle ended state is invalid")
        return {
            "version": version,
            "client_manager_attribute": "_market_manager",
            "manager_connection_attribute": "_connection",
            "connection_socket_attribute": "_socket",
            "manager_open_property": "is_open",
            "manager_dropped_property": "dropped_events",
            "handle_dropped_property": "dropped",
            "handle_queue_attribute": "_queue",
            "handle_queue_maxsize": queue_maxsize,
            "handle_ended_attribute": "_ended",
            "initial_manager_open": manager.is_open,
            "initial_socket_is_none": connection._socket is None,
            "initial_manager_dropped": manager.dropped_events,
            "initial_handle_dropped": handle.dropped,
            "initial_handle_ended": handle_ended,
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
    RECOVERY_BUFFER_OVERFLOW = "recovery_buffer_overflow"


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
    handle_queue: Any
    handle_queue_maxsize: int
    subscription_drop_logged: bool = False

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
            handle_state = vars(handle)
            handle_queue = handle_state["_queue"]
            handle_queue_maxsize = handle_queue.maxsize
            handle_ended = handle_state["_ended"]
        except (AttributeError, KeyError, TypeError):
            return None, _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if (
            type(manager_open) is not bool
            or type(manager_dropped) is not int
            or manager_dropped < 0
            or type(handle_dropped) is not int
            or handle_dropped < 0
            or type(handle_queue_maxsize) is not int
            or handle_queue_maxsize < 1
            or type(handle_ended) is not bool
        ):
            return None, _InvalidReason.SDK_LIFECYCLE_STATE_UNKNOWN
        if handle_ended:
            return None, _InvalidReason.SDK_HANDLE_ENDED
        if handle_dropped != 0:
            return None, _InvalidReason.SUBSCRIPTION_EVENT_DROPPED
        # A peer close changes the socket state before the SDK reader claims
        # the socket and invokes on_connection_lost. Treating !is_open as
        # terminal here races that callback: subscription.close() can claim
        # the still-present socket first and make the SDK suppress the peer's
        # close code/reason as a user-initiated close. The pinned SDK clears
        # _socket atomically when its reader owns cleanup, so wait for None.
        if socket is None:
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
                handle_queue=handle_queue,
                handle_queue_maxsize=handle_queue_maxsize,
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
            handle_state = vars(self.handle)
            handle_queue = handle_state["_queue"]
            handle_queue_maxsize = handle_queue.maxsize
            handle_ended = handle_state["_ended"]
        except (AttributeError, KeyError, TypeError):
            return _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if manager is not self.manager or connection is not self.connection:
            return _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if (
            handle_queue is not self.handle_queue
            or handle_queue_maxsize != self.handle_queue_maxsize
        ):
            return _InvalidReason.SDK_LIFECYCLE_SHAPE_CHANGED
        if (
            type(manager_open) is not bool
            or type(manager_dropped) is not int
            or manager_dropped < self.manager_dropped
            or type(handle_dropped) is not int
            or handle_dropped < self.handle_dropped
            or type(handle_queue_maxsize) is not int
            or handle_queue_maxsize < 1
            or type(handle_ended) is not bool
        ):
            return _InvalidReason.SDK_LIFECYCLE_STATE_UNKNOWN
        if handle_ended:
            return _InvalidReason.SDK_HANDLE_ENDED
        if manager_dropped > self.manager_dropped:
            return _InvalidReason.SDK_EVENT_DROPPED
        if handle_dropped > self.handle_dropped:
            if not self.subscription_drop_logged:
                self.subscription_drop_logged = True
                try:
                    queue_size = handle_queue.qsize()
                except (AttributeError, TypeError):
                    queue_size = -1
                _LOGGER.warning(
                    "market_stream_subscription_drop_detected "
                    "handle_dropped=%d previous_handle_dropped=%d drop_delta=%d "
                    "queue_size=%d queue_maxsize=%d manager_dropped=%d "
                    "previous_manager_dropped=%d",
                    handle_dropped,
                    self.handle_dropped,
                    handle_dropped - self.handle_dropped,
                    queue_size,
                    handle_queue_maxsize,
                    manager_dropped,
                    self.manager_dropped,
                )
            return _InvalidReason.SUBSCRIPTION_EVENT_DROPPED
        if socket is None:
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
        detach_recovery_operation: Callable[
            [asyncio.Future[Any], int, str], None
        ]
        | None = None,
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
        self._detach_recovery_operation_callback = detach_recovery_operation
        self._closed = False
        self._terminal = False
        self._normal_close_requested = False
        self._close_task: asyncio.Task[None] | None = None
        self._sdk_close_task: asyncio.Task[None] | None = None
        self._lifecycle_task: asyncio.Task[None] | None = None
        self._event_pump_task: asyncio.Task[None] | None = None
        self._recovery_buffer_capacity = (
            lifecycle_probe.handle_queue_maxsize
            if lifecycle_probe is not None
            else 0
        )
        handoff_queue_capacity = max(
            1,
            min(
                self._recovery_buffer_capacity,
                _MARKET_STREAM_HANDOFF_QUEUE_CAPACITY,
            ),
        )
        self._live_items: asyncio.Queue[
            MarketStreamEvent | _InvalidReason | object
        ] = asyncio.Queue(maxsize=handoff_queue_capacity)
        self._invalid_reason: _InvalidReason | None = None
        self._event_pump_invalid_reason: _InvalidReason | None = None
        self._buffered_events: deque[MarketStreamEvent] = deque(
            maxlen=self._recovery_buffer_capacity
        )
        self._stream_events_total = 0
        self._stream_progress_last_total = 0
        self._stream_progress_last_at = time.monotonic()
        self._pump_sdk_events_total = 0
        self._pump_mapped_events_total = 0
        self._pump_ignored_events_total = 0
        self._pump_mapping_seconds = 0.0
        self._pump_handoff_seconds = 0.0
        self._pump_progress_last_sdk_total = 0
        self._pump_progress_last_mapped_total = 0
        self._pump_progress_last_ignored_total = 0
        self._pump_progress_last_mapping_seconds = 0.0
        self._pump_progress_last_handoff_seconds = 0.0
        self._pump_progress_last_at = time.monotonic()

    def __aiter__(self) -> "MarketSubscription":
        return self

    @property
    def subscription_generation(self) -> int:
        return self._subscription_generation

    @property
    def token_ids(self) -> tuple[str, ...]:
        return self._token_ids

    async def __anext__(self) -> MarketStreamEvent | MarketStreamInvalidated:
        try:
            if self._close_task is not None or self._closed or self._terminal:
                raise StopAsyncIteration
            if self._buffered_events:
                reason = self._current_invalid_reason()
                if reason is not None:
                    self._buffered_events.clear()
                    return await self._invalidate(reason)
                return self._buffered_events.popleft()
            return await self._next_live()
        except asyncio.CancelledError:
            if self._close_task is None:
                await self.close()
            raise

    async def _next_live(self) -> MarketStreamEvent | MarketStreamInvalidated:
        if self._initial_invalid_reason is not None:
            reason = self._initial_invalid_reason
            self._initial_invalid_reason = None
            return await self._invalidate(reason)

        assert self._lifecycle_probe is not None
        reason = self._lifecycle_probe.check()
        if reason is not None:
            return await self._invalidate(reason)

        self._ensure_live_tasks()
        item = await self._live_items.get()
        if self._normal_close_requested or item is _MARKET_STREAM_CLOSED:
            raise StopAsyncIteration
        reason = self._current_invalid_reason()
        if reason is not None:
            return await self._invalidate(reason)
        if isinstance(item, _InvalidReason):
            return await self._invalidate(item)
        self._stream_events_total += 1
        self._log_consumer_progress()
        return item  # type: ignore[return-value]

    def _ensure_live_tasks(self) -> None:
        if self._event_pump_task is None:
            self._event_pump_task = asyncio.create_task(
                self._pump_live_events(),
                name=(
                    "polymarket:market-stream-events:"
                    f"{self._subscription_generation}"
                ),
            )
        if self._lifecycle_task is None:
            self._lifecycle_task = asyncio.create_task(
                self._monitor_lifecycle(),
                name=(
                    "polymarket:market-stream-lifecycle:"
                    f"{self._subscription_generation}"
                ),
            )

    async def _pump_live_events(self) -> None:
        while True:
            try:
                sdk_event = await self._iterator.__anext__()
            except asyncio.CancelledError:
                raise
            except StopAsyncIteration:
                await self._publish_event_pump_invalidation(
                    _InvalidReason.SDK_HANDLE_ENDED
                )
                return
            except Exception:
                await self._publish_event_pump_invalidation(
                    _InvalidReason.SDK_HANDLE_ENDED
                )
                return

            self._pump_sdk_events_total += 1
            mapping_started_at = time.perf_counter()
            try:
                mapped = self._mapper(sdk_event)
            except GatewayMappingError:
                self._pump_mapping_seconds += time.perf_counter() - mapping_started_at
                await self._publish_event_pump_invalidation(
                    _InvalidReason.SDK_EVENT_INVALID
                )
                return
            self._pump_mapping_seconds += time.perf_counter() - mapping_started_at
            if mapped is None:
                self._pump_ignored_events_total += 1
                self._log_pump_progress()
                continue
            handoff_started_at = time.perf_counter()
            await self._live_items.put(mapped)
            self._pump_handoff_seconds += time.perf_counter() - handoff_started_at
            self._pump_mapped_events_total += 1
            self._log_pump_progress()

    def _log_pump_progress(self) -> None:
        now = time.monotonic()
        elapsed = now - self._pump_progress_last_at
        if self._pump_sdk_events_total != 1 and elapsed < _MARKET_STREAM_PROGRESS_INTERVAL_SECONDS:
            return
        sdk_events_delta = (
            self._pump_sdk_events_total - self._pump_progress_last_sdk_total
        )
        mapped_events_delta = (
            self._pump_mapped_events_total - self._pump_progress_last_mapped_total
        )
        ignored_events_delta = (
            self._pump_ignored_events_total - self._pump_progress_last_ignored_total
        )
        mapping_seconds_delta = (
            self._pump_mapping_seconds
            - self._pump_progress_last_mapping_seconds
        )
        handoff_seconds_delta = (
            self._pump_handoff_seconds
            - self._pump_progress_last_handoff_seconds
        )
        sdk_rate = sdk_events_delta / elapsed if elapsed > 0 else 0.0
        mapping_ms = (
            mapping_seconds_delta * 1_000 / sdk_events_delta
            if sdk_events_delta
            else 0.0
        )
        handoff_ms = (
            handoff_seconds_delta * 1_000 / mapped_events_delta
            if mapped_events_delta
            else 0.0
        )
        _LOGGER.info(
            "market_stream_pump_progress generation=%d sdk_events_total=%d "
            "sdk_events_delta=%d sdk_rate_per_second=%.1f mapped_delta=%d "
            "ignored_delta=%d mapping_ms_per_event=%.3f "
            "handoff_wait_ms_per_event=%.3f handoff_queue_size=%d "
            "handoff_queue_capacity=%d",
            self._subscription_generation,
            self._pump_sdk_events_total,
            sdk_events_delta,
            sdk_rate,
            mapped_events_delta,
            ignored_events_delta,
            mapping_ms,
            handoff_ms,
            self._live_items.qsize(),
            self._live_items.maxsize,
        )
        self._pump_progress_last_sdk_total = self._pump_sdk_events_total
        self._pump_progress_last_mapped_total = self._pump_mapped_events_total
        self._pump_progress_last_ignored_total = self._pump_ignored_events_total
        self._pump_progress_last_mapping_seconds = self._pump_mapping_seconds
        self._pump_progress_last_handoff_seconds = self._pump_handoff_seconds
        self._pump_progress_last_at = now

    async def _monitor_lifecycle(self) -> None:
        reason = await self._wait_for_invalidation()
        if self._normal_close_requested:
            return
        self._invalid_reason = reason
        self._replace_live_item(reason)

    async def _publish_event_pump_invalidation(
        self,
        reason: _InvalidReason,
    ) -> None:
        if self._normal_close_requested:
            return
        self._event_pump_invalid_reason = reason
        await self._live_items.put(reason)

    def _replace_live_item(
        self,
        item: MarketStreamEvent | _InvalidReason | object,
    ) -> None:
        with contextlib.suppress(asyncio.QueueEmpty):
            self._live_items.get_nowait()
        self._live_items.put_nowait(item)

    def _log_consumer_progress(self) -> None:
        now = time.monotonic()
        elapsed = now - self._stream_progress_last_at
        if (
            self._stream_events_total != 1
            and elapsed < _MARKET_STREAM_PROGRESS_INTERVAL_SECONDS
        ):
            return
        probe = self._lifecycle_probe
        queue_size = -1
        queue_capacity = -1
        handle_dropped = -1
        manager_dropped = -1
        if probe is not None:
            queue_capacity = probe.handle_queue_maxsize
            try:
                queue_size = probe.handle_queue.qsize()
                handle_dropped = probe.handle.dropped
                manager_dropped = probe.manager.dropped_events
            except (AttributeError, TypeError):
                pass
        events_delta = self._stream_events_total - self._stream_progress_last_total
        rate = events_delta / elapsed if elapsed > 0 else 0.0
        utilization = (
            queue_size * 100 / queue_capacity
            if queue_size >= 0 and queue_capacity > 0
            else -1.0
        )
        _LOGGER.info(
            "market_stream_consumer_progress generation=%d events_total=%d "
            "events_delta=%d rate_per_second=%.1f queue_size=%d "
            "queue_capacity=%d queue_utilization_pct=%.1f "
            "handle_dropped=%d manager_dropped=%d",
            self._subscription_generation,
            self._stream_events_total,
            events_delta,
            rate,
            queue_size,
            queue_capacity,
            utilization,
            handle_dropped,
            manager_dropped,
        )
        self._stream_progress_last_total = self._stream_events_total
        self._stream_progress_last_at = now

    async def _guard_awaitable(self, awaitable: Awaitable[Any]) -> Any:
        """Buffer stream events while rejecting an invalid recovery baseline."""
        operation_task: asyncio.Future[Any] | None = asyncio.ensure_future(awaitable)
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
                    # A persistent event pump adds one hand-off scheduling step.
                    # Give an already-consumed SDK event a bounded chance to
                    # enter the recovery buffer before installing the baseline.
                    for _ in range(3):
                        await asyncio.sleep(0)
                        if live_task.done():
                            break
                if live_task.done():
                    item = live_task.result()
                    live_task = None
                    if isinstance(item, MarketStreamInvalidated):
                        if not operation_task.done():
                            self._detach_recovery_operation(
                                operation_task,
                                item.reason,
                            )
                            operation_task = None
                        raise MarketRecoveryInvalidatedError(item.reason)
                    if len(buffered) >= self._recovery_buffer_capacity:
                        reason = _InvalidReason.RECOVERY_BUFFER_OVERFLOW
                        if not operation_task.done():
                            self._detach_recovery_operation(
                                operation_task,
                                reason.value,
                            )
                            operation_task = None
                        await self._invalidate(reason)
                        raise MarketRecoveryInvalidatedError(reason.value)
                    buffered.append(item)
                    if not operation_task.done():
                        continue
                else:
                    live_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await live_task
                    live_task = None

                assert operation_task is not None
                # The pump may already have removed one more event from the SDK
                # queue while the baseline operation completed. Move that
                # hand-off item behind the same baseline barrier instead of
                # exposing it as a post-recovery event.
                await asyncio.sleep(0)
                with contextlib.suppress(asyncio.QueueEmpty):
                    handoff_item = self._live_items.get_nowait()
                    if handoff_item is _MARKET_STREAM_CLOSED:
                        raise StopAsyncIteration
                    if isinstance(handoff_item, _InvalidReason):
                        await self._invalidate(handoff_item)
                        raise MarketRecoveryInvalidatedError(handoff_item.value)
                    if len(buffered) >= self._recovery_buffer_capacity:
                        reason = _InvalidReason.RECOVERY_BUFFER_OVERFLOW
                        await self._invalidate(reason)
                        raise MarketRecoveryInvalidatedError(reason.value)
                    buffered.append(handoff_item)  # type: ignore[arg-type]
                result = operation_task.result()
                reason = (
                    self._event_pump_invalid_reason
                    or self._current_invalid_reason()
                )
                if reason is not None:
                    await self._invalidate(reason)
                    raise GatewayLifecycleError(
                        f"recovery stream invalidated: {reason.value}"
                    )
                if (
                    len(self._buffered_events) + len(buffered)
                    > self._recovery_buffer_capacity
                ):
                    reason = _InvalidReason.RECOVERY_BUFFER_OVERFLOW
                    if not operation_task.done():
                        self._detach_recovery_operation(
                            operation_task,
                            reason.value,
                        )
                        operation_task = None
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

    def _detach_recovery_operation(
        self,
        operation: asyncio.Future[Any],
        reason: str,
    ) -> None:
        callback = self._detach_recovery_operation_callback
        if callback is not None:
            callback(operation, self._subscription_generation, reason)
            return

        # Directly constructed test/integration subscriptions have no gateway
        # owner. Keep the operation alive and consume its terminal result.
        operation.add_done_callback(_consume_future_result)

    async def close(self) -> None:
        if self._invalid_reason is None:
            self._normal_close_requested = True
        if self._close_task is None or self._close_task.cancelled():
            self._close_task = asyncio.create_task(self._finish_close())
        await asyncio.shield(self._close_task)

    async def _finish_close(self) -> None:
        if self._normal_close_requested:
            self._replace_live_item(_MARKET_STREAM_CLOSED)
        try:
            if self._sdk_close_task is None:
                self._sdk_close_task = asyncio.create_task(self._handle.close())
            await asyncio.shield(self._sdk_close_task)
        finally:
            live_tasks = tuple(
                task
                for task in (self._event_pump_task, self._lifecycle_task)
                if task is not None
            )
            for task in live_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*live_tasks, return_exceptions=True)
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
        if self._normal_close_requested:
            self._buffered_events.clear()
            raise StopAsyncIteration
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
    token_ids: tuple[str, ...]

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
        token_ids = _token_ids(self.token_ids)
        if tuple(book.token_id for book in books) != token_ids:
            raise ValueError("order books must match the session token ids")
        if self.subscription.token_ids != token_ids:
            raise ValueError("subscription must match the session token ids")
        object.__setattr__(self, "order_books", books)
        object.__setattr__(self, "token_ids", token_ids)


class PolymarketGateway:
    def __init__(
        self,
        client: Any | None = None,
        *,
        clock_ms: Callable[[], int] | None = None,
        page_size: int = 100,
        lifecycle_poll_interval: float = 0.01,
        market_stream_queue_capacity: int = 65_536,
    ) -> None:
        if type(page_size) is not int or page_size < 1:
            raise ValueError("page_size must be a positive integer")
        if (
            isinstance(lifecycle_poll_interval, bool)
            or not isinstance(lifecycle_poll_interval, (int, float))
            or lifecycle_poll_interval <= 0
        ):
            raise ValueError("lifecycle_poll_interval must be positive")
        if (
            type(market_stream_queue_capacity) is not int
            or market_stream_queue_capacity < 1
        ):
            raise ValueError("market_stream_queue_capacity must be a positive integer")
        self._client = client if client is not None else AsyncPublicClient()
        self._clock_ms = clock_ms or _system_clock_ms
        self._page_size = page_size
        self._lifecycle_poll_interval = float(lifecycle_poll_interval)
        self._market_stream_queue_capacity = market_stream_queue_capacity
        self._sync_counter = 0
        self._sync_generation: str | None = None
        self._subscription_generation = 0
        self._market_id_by_condition_id: dict[str, str] = {}
        self._condition_id_by_market_id: dict[str, str] = {}
        self._token_identity_by_id: dict[str, tuple[str, str]] = {}
        self._token_ids_by_market_id: dict[str, frozenset[str]] = {}
        self._market_mapping_warnings: tuple[MarketMappingWarning, ...] = ()
        self._connection_lost_logging_manager: Any | None = None
        self._malformed_event_logging_manager: Any | None = None
        self._queue_configured_manager: Any | None = None
        self._detached_recovery_operations: set[asyncio.Future[Any]] = set()
        self._closed = False

    @property
    def market_mapping_warnings(self) -> tuple[MarketMappingWarning, ...]:
        return self._market_mapping_warnings

    async def list_active_events(self) -> tuple[Event, ...]:
        started_at = time.monotonic()
        received_at = self._now()
        generation = self._start_sync_generation(received_at)
        paginator = self._client.list_events(closed=False, page_size=self._page_size)
        events: list[Event] = []
        page_count = 0
        async for page in paginator:
            page_count += 1
            for sdk_event in page.items:
                event = _map_event(
                    sdk_event,
                    received_at=received_at,
                    sync_generation=generation,
                )
                if event.status is MarketStatus.ACTIVE:
                    events.append(event)
            if page_count % 25 == 0:
                _LOGGER.info(
                    "catalog_events_fetch_progress pages=%d active_events=%d "
                    "elapsed_ms=%d",
                    page_count,
                    len(events),
                    int((time.monotonic() - started_at) * 1_000),
                )
        _LOGGER.info(
            "catalog_events_fetch_completed pages=%d active_events=%d elapsed_ms=%d",
            page_count,
            len(events),
            int((time.monotonic() - started_at) * 1_000),
        )
        return tuple(events)

    async def list_active_markets(self) -> tuple[MarketSnapshot, ...]:
        started_at = time.monotonic()
        received_at = self._now()
        generation = self._current_sync_generation(received_at)
        paginator = self._client.list_markets(closed=False, page_size=self._page_size)
        snapshots: list[MarketSnapshot] = []
        warnings: list[MarketMappingWarning] = []
        page_count = 0
        self._market_mapping_warnings = ()
        async for page in paginator:
            page_count += 1
            for sdk_market in page.items:
                try:
                    snapshot = _map_market(
                        sdk_market,
                        received_at=received_at,
                        sync_generation=generation,
                    )
                except GatewayMappingError as error:
                    if error.market_id is None:
                        raise
                    warnings.append(
                        MarketMappingWarning(
                            market_id=error.market_id,
                            error=str(error),
                        )
                    )
                    continue
                if (
                    snapshot.market.status is MarketStatus.ACTIVE
                    and snapshot.market.active
                ):
                    self._remember_market(snapshot)
                    snapshots.append(snapshot)
            if page_count % 25 == 0:
                _LOGGER.info(
                    "catalog_markets_fetch_progress pages=%d active_markets=%d "
                    "warnings=%d elapsed_ms=%d",
                    page_count,
                    len(snapshots),
                    len(warnings),
                    int((time.monotonic() - started_at) * 1_000),
                )
        self._market_mapping_warnings = tuple(warnings)
        _LOGGER.info(
            "catalog_markets_fetch_completed pages=%d active_markets=%d warnings=%d "
            "elapsed_ms=%d",
            page_count,
            len(snapshots),
            len(warnings),
            int((time.monotonic() - started_at) * 1_000),
        )
        return tuple(snapshots)

    async def get_order_books(self, token_ids: Sequence[str]) -> tuple[OrderBook, ...]:
        requested = _token_ids(token_ids)
        generation = max(1, self._subscription_generation)
        return await self._get_order_books_for_generation(
            requested,
            subscription_generation=generation,
        )

    def hydrate_market_identities(
        self,
        markets: Sequence[Market],
        tokens: Sequence[Token],
        market_ids: Sequence[str],
    ) -> None:
        requested = frozenset(_token_ids(market_ids))
        market_by_id = {market.id: market for market in markets}
        tokens_by_market: dict[str, list[Token]] = {}
        for token in tokens:
            if token.market_id in requested:
                tokens_by_market.setdefault(token.market_id, []).append(token)
        for market_id in sorted(
            requested,
            key=lambda value: value.encode("utf-8"),
        ):
            try:
                market = market_by_id[market_id]
            except KeyError as error:
                raise GatewayMappingError(
                    f"catalog has no selected market {market_id}"
                ) from error
            market_tokens = tuple(tokens_by_market.get(market_id, ()))
            self._remember_market(
                MarketSnapshot(
                    market=market,
                    tokens=market_tokens,
                    mapping_version=MAPPING_VERSION,
                )
            )
        _LOGGER.info("market_identities_hydrated markets=%d", len(requested))

    async def recover_market_session(
        self,
        token_ids: Sequence[str],
    ) -> MarketRecoverySession:
        normalized = _token_ids(token_ids)
        self._subscription_generation += 1
        generation = self._subscription_generation
        recovery_started_at = time.monotonic()
        _LOGGER.info(
            "market_recovery_started generation=%d tokens=%d token_id_bytes=%d",
            generation,
            len(normalized),
            sum(len(token_id.encode("utf-8")) for token_id in normalized),
        )
        subscribe_started_at = time.monotonic()
        try:
            subscription = await self._subscribe_markets_for_generation(
                normalized,
                subscription_generation=generation,
            )
        except Exception as error:
            _LOGGER.exception(
                "market_stream_subscribe_failed generation=%d tokens=%d "
                "elapsed_ms=%d error=%s",
                generation,
                len(normalized),
                int((time.monotonic() - subscribe_started_at) * 1_000),
                error,
            )
            transient = _market_recovery_transient_error(error)
            if transient is not None:
                raise transient from error
            raise
        _LOGGER.info(
            "market_stream_subscribed generation=%d tokens=%d elapsed_ms=%d",
            generation,
            len(normalized),
            int((time.monotonic() - subscribe_started_at) * 1_000),
        )
        books_started_at = time.monotonic()
        _LOGGER.info(
            "market_recovery_books_started generation=%d tokens=%d",
            generation,
            len(normalized),
        )
        try:
            books = await subscription._guard_awaitable(
                self._get_order_books_for_generation(
                    normalized,
                    subscription_generation=generation,
                )
            )
        except MissingOrderBooksError as error:
            await subscription.close()
            remaining, removed_market_ids, removed_token_ids = (
                self._prune_missing_order_book_markets(normalized, error.token_ids)
            )
            if not removed_market_ids or not remaining:
                _LOGGER.error(
                    "market_recovery_books_failed generation=%d tokens=%d "
                    "elapsed_ms=%d error=%s",
                    generation,
                    len(normalized),
                    int((time.monotonic() - books_started_at) * 1_000),
                    error,
                )
                raise
            _LOGGER.warning(
                "market_recovery_books_missing generation=%d missing_tokens=%d "
                "removed_markets=%d removed_tokens=%d remaining_tokens=%d "
                "elapsed_ms=%d",
                generation,
                len(error.token_ids),
                len(removed_market_ids),
                len(removed_token_ids),
                len(remaining),
                int((time.monotonic() - books_started_at) * 1_000),
            )
            return await self.recover_market_session(remaining)
        except BaseException as error:
            if isinstance(error, Exception):
                _LOGGER.error(
                    "market_recovery_books_failed generation=%d tokens=%d "
                    "elapsed_ms=%d error=%s",
                    generation,
                    len(normalized),
                    int((time.monotonic() - books_started_at) * 1_000),
                    error,
                )
            await subscription.close()
            if isinstance(error, Exception):
                transient = _market_recovery_transient_error(error)
                if transient is not None:
                    raise transient from error
            raise
        _LOGGER.info(
            "market_recovery_books_received generation=%d tokens=%d books=%d "
            "elapsed_ms=%d",
            generation,
            len(normalized),
            len(books),
            int((time.monotonic() - books_started_at) * 1_000),
        )
        _LOGGER.info(
            "market_recovery_completed generation=%d tokens=%d elapsed_ms=%d",
            generation,
            len(normalized),
            int((time.monotonic() - recovery_started_at) * 1_000),
        )
        return MarketRecoverySession(
            order_books=books,
            subscription=subscription,
            subscription_generation=generation,
            token_ids=normalized,
        )

    def _prune_missing_order_book_markets(
        self,
        requested: tuple[str, ...],
        missing_token_ids: tuple[str, ...],
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
        missing_market_ids: set[str] = set()
        for token_id in missing_token_ids:
            identity = self._token_identity_by_id.get(token_id)
            if identity is None:
                return requested, (), ()
            missing_market_ids.add(identity[0])
        removed_token_set = {
            token_id
            for market_id in missing_market_ids
            for token_id in self._token_ids_by_market_id.get(market_id, ())
            if token_id in requested
        }
        remaining = tuple(
            token_id for token_id in requested if token_id not in removed_token_set
        )
        removed_market_ids = tuple(
            sorted(missing_market_ids, key=lambda value: value.encode("utf-8"))
        )
        removed_token_ids = tuple(
            token_id for token_id in requested if token_id in removed_token_set
        )
        return remaining, removed_market_ids, removed_token_ids

    async def _get_order_books_for_generation(
        self,
        requested: tuple[str, ...],
        *,
        subscription_generation: int,
    ) -> tuple[OrderBook, ...]:
        sdk_books = await self._client.get_order_books(token_ids=requested)
        received_at = self._now()
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
            raise MissingOrderBooksError(missing)
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
        self._configure_market_stream_queue()
        self._ensure_malformed_event_logging()
        self._ensure_connection_lost_logging()
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
            detach_recovery_operation=self._track_detached_recovery_operation,
        )

    def _track_detached_recovery_operation(
        self,
        operation: asyncio.Future[Any],
        generation: int,
        reason: str,
    ) -> None:
        started_at = time.monotonic()
        self._detached_recovery_operations.add(operation)
        _LOGGER.warning(
            "market_recovery_books_detached generation=%d reason=%s "
            "pending_operations=%d action=drain_without_cancellation",
            generation,
            reason,
            len(self._detached_recovery_operations),
        )

        def completed(done: asyncio.Future[Any]) -> None:
            self._detached_recovery_operations.discard(done)
            elapsed_ms = int((time.monotonic() - started_at) * 1_000)
            try:
                done.result()
            except asyncio.CancelledError:
                _LOGGER.warning(
                    "market_recovery_books_detached_completed generation=%d "
                    "reason=%s outcome=cancelled elapsed_ms=%d "
                    "pending_operations=%d",
                    generation,
                    reason,
                    elapsed_ms,
                    len(self._detached_recovery_operations),
                )
            except Exception as error:
                _LOGGER.warning(
                    "market_recovery_books_detached_completed generation=%d "
                    "reason=%s outcome=error elapsed_ms=%d "
                    "pending_operations=%d error=%s",
                    generation,
                    reason,
                    elapsed_ms,
                    len(self._detached_recovery_operations),
                    error,
                )
            else:
                _LOGGER.info(
                    "market_recovery_books_detached_completed generation=%d "
                    "reason=%s outcome=success elapsed_ms=%d "
                    "pending_operations=%d",
                    generation,
                    reason,
                    elapsed_ms,
                    len(self._detached_recovery_operations),
                )

        operation.add_done_callback(completed)

    def _configure_market_stream_queue(self) -> None:
        manager = self._client._get_market_manager()
        if manager is self._queue_configured_manager:
            return
        try:
            previous_capacity = vars(manager)["_queue_size"]
        except (KeyError, TypeError) as error:
            raise GatewayLifecycleError(
                "SDK market manager has no _queue_size attribute"
            ) from error
        if type(previous_capacity) is not int or previous_capacity < 1:
            raise GatewayLifecycleError("SDK market manager queue size is invalid")
        manager._queue_size = self._market_stream_queue_capacity
        self._queue_configured_manager = manager
        _LOGGER.info(
            "market_stream_queue_configured capacity=%d previous_capacity=%d",
            self._market_stream_queue_capacity,
            previous_capacity,
        )

    def _ensure_malformed_event_logging(self) -> None:
        manager = self._client._get_market_manager()
        if manager is self._malformed_event_logging_manager:
            return
        try:
            on_message = manager._on_message
            initial_dropped = manager.dropped_events
        except AttributeError as error:
            raise GatewayLifecycleError(
                "SDK market manager has no message callback or dropped counter"
            ) from error
        if type(initial_dropped) is not int or initial_dropped < 0:
            raise GatewayLifecycleError("SDK market manager dropped counter is invalid")
        ignored_new_market_total = 0

        def logged_message(raw: object) -> None:
            nonlocal ignored_new_market_total
            raw_items = raw if isinstance(raw, list) else [raw]
            forwarded_items: list[object] = []
            ignored = 0
            normalized = False
            ignored_sample: object | None = None
            ignored_error: ValidationError | None = None
            for item in raw_items:
                if _raw_market_event_type(item) != "new_market":
                    forwarded_items.append(item)
                    continue
                normalized_item = _normalize_new_market_game_start_time(item)
                normalized = normalized or normalized_item is not item
                try:
                    _parse_pinned_market_event(normalized_item)
                except ValidationError as error:
                    ignored += 1
                    if ignored_sample is None:
                        ignored_sample = item
                        ignored_error = error
                    continue
                forwarded_items.append(normalized_item)

            if ignored:
                previous_ignored_total = ignored_new_market_total
                ignored_new_market_total += ignored
                if (
                    previous_ignored_total == 0
                    or previous_ignored_total // _MALFORMED_NEW_MARKET_LOG_INTERVAL
                    < ignored_new_market_total // _MALFORMED_NEW_MARKET_LOG_INTERVAL
                ):
                    assert ignored_sample is not None
                    assert ignored_error is not None
                    _LOGGER.warning(
                        "market_stream_event_malformed event_type=new_market "
                        "action=ignored_unscoped_control_event "
                        "ignored_count=%d ignored_total=%d "
                        "validation_error=%s sample_raw=%s",
                        ignored,
                        ignored_new_market_total,
                        _validation_error_summary(ignored_error),
                        _api_response_summary(
                            ignored_sample,
                            max_chars=_MAX_MALFORMED_EVENT_SAMPLE_CHARS,
                        ),
                    )

                if not forwarded_items:
                    return
            if ignored or normalized:
                forwarded = forwarded_items if isinstance(raw, list) else forwarded_items[0]
            else:
                forwarded = raw

            dropped_before = manager.dropped_events
            on_message(forwarded)
            dropped_after = manager.dropped_events
            if dropped_after <= dropped_before:
                return
            items = forwarded if isinstance(forwarded, list) else (forwarded,)
            logged = 0
            for item in items:
                try:
                    _parse_pinned_market_event(item)
                except ValidationError as error:
                    logged += 1
                    _LOGGER.warning(
                        "market_stream_event_malformed event_type=%s "
                        "validation_error=%s raw=%s",
                        _raw_market_event_type(item),
                        _validation_error_summary(error),
                        _api_response_summary(item),
                    )
            unexplained = dropped_after - dropped_before - logged
            if unexplained > 0:
                _LOGGER.warning(
                    "market_stream_event_drop_unexplained dropped=%d raw=%s",
                    unexplained,
                    _api_response_summary(forwarded),
                )

        manager._on_message = logged_message
        self._malformed_event_logging_manager = manager

    def _ensure_connection_lost_logging(self) -> None:
        manager = self._client._get_market_manager()
        if manager is self._connection_lost_logging_manager:
            return
        try:
            on_connection_lost = manager._on_socket_connection_lost
            connection = manager._connection
        except AttributeError:
            _LOGGER.warning(
                "market_stream_connection_diagnostics_unavailable "
                "manager_type=%s",
                type(manager).__name__,
            )
            self._connection_lost_logging_manager = manager
            return

        reader_socket: Any | None = None
        read_loop = getattr(connection, "_read_loop", None)
        if callable(read_loop):

            async def logged_read_loop(socket: Any, on_message: Any) -> None:
                nonlocal reader_socket
                reader_socket = socket
                try:
                    await read_loop(socket, on_message)
                finally:
                    reader_socket = None

            connection._read_loop = logged_read_loop

        def logged_connection_lost(code: int, reason: str) -> None:
            try:
                socket = reader_socket
                state = getattr(socket, "state", None)
                socket_state = getattr(state, "name", state)
                reader_error = getattr(socket, "recv_exc", None)
                protocol = getattr(socket, "protocol", None)
                parser_error = getattr(protocol, "parser_exc", None)
                heartbeat = getattr(manager, "_heartbeat", None)
                heartbeat_clock = getattr(heartbeat, "_clock", None)
                heartbeat_last_pong = getattr(heartbeat, "_last_pong", None)
                if callable(heartbeat_clock) and isinstance(
                    heartbeat_last_pong, int | float
                ):
                    heartbeat_age = (
                        f"{max(0.0, float(heartbeat_clock()) - heartbeat_last_pong):.3f}"
                    )
                else:
                    heartbeat_age = "unavailable"
                latency = getattr(socket, "latency", None)
                websocket_latency = (
                    f"{float(latency):.3f}"
                    if isinstance(latency, int | float)
                    else "unavailable"
                )
                transport = getattr(socket, "transport", None)
                is_closing = getattr(transport, "is_closing", None)
                transport_closing = is_closing() if callable(is_closing) else "unavailable"
                _LOGGER.warning(
                    "market_stream_connection_lost close_code=%s close_reason=%r "
                    "socket_state=%s reader_exception_type=%s reader_exception=%r "
                    "parser_exception_type=%s parser_exception=%r "
                    "heartbeat_age_seconds=%s websocket_latency_seconds=%s "
                    "transport_closing=%s",
                    code,
                    reason,
                    socket_state if socket_state is not None else "unavailable",
                    type(reader_error).__name__ if reader_error is not None else "none",
                    str(reader_error) if reader_error is not None else "",
                    type(parser_error).__name__ if parser_error is not None else "none",
                    str(parser_error) if parser_error is not None else "",
                    heartbeat_age,
                    websocket_latency,
                    transport_closing,
                )
            finally:
                on_connection_lost(code, reason)

        manager._on_socket_connection_lost = logged_connection_lost
        self._connection_lost_logging_manager = manager

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
        if len(events) > 1:
            raise ValueError("events must contain at most one event reference")
        event_id = (
            None
            if not events
            else _require_string(getattr(events[0], "id"), "event id")
        )
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
        raise GatewayMappingError(
            f"market {market_id}: {error}; "
            f"api_response={_api_response_summary(sdk_market)}",
            market_id=market_id,
        ) from error


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
    exponent = _decimal(getattr(sdk_schedule, "exponent"), "fee exponent")
    rate = _decimal(getattr(sdk_schedule, "rate"), "fee rate")
    rebate_rate = _decimal(getattr(sdk_schedule, "rebate_rate"), "fee rebate rate")
    taker_only = getattr(sdk_schedule, "taker_only")
    if type(taker_only) is not bool:
        raise ValueError("fee taker_only must be a boolean")
    if fee_type == "flat" and exponent == 0 and rebate_rate == Decimal("0"):
        return FeeSchedule(
            model=FeeModel.FLAT,
            enabled=True,
            source=source,
            parameters={"rate": rate},
            updated_at=received_at,
            taker_only=taker_only,
        )
    return FeeSchedule(
        model=FeeModel.CURVE,
        enabled=True,
        source=source,
        parameters={
            "rate": rate,
            "exponent": exponent,
            "rebate_rate": rebate_rate,
        },
        updated_at=received_at,
        taker_only=taker_only,
    )


def _api_response_summary(
    value: Any,
    *,
    max_chars: int = _MAX_MAPPING_RESPONSE_CHARS,
) -> str:
    try:
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            payload = model_dump(mode="json")
        elif isinstance(value, (Mapping, list, tuple)):
            payload = value
        else:
            payload = vars(value)
        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    except Exception:
        serialized = repr(value)
    if len(serialized) <= max_chars:
        return serialized
    suffix = "...<truncated>"
    return serialized[: max_chars - len(suffix)] + suffix


def _raw_market_event_type(value: object) -> str:
    if not isinstance(value, Mapping):
        return type(value).__name__
    event_type = value.get("event_type", value.get("type", "missing"))
    return str(event_type)


def _normalize_new_market_game_start_time(value: object) -> object:
    """Bridge the live API's ISO timestamp to the pinned SDK's epoch-ms field."""
    if not isinstance(value, dict):
        return value
    game_start_time = value.get("game_start_time")
    if not isinstance(game_start_time, str):
        return value
    try:
        parsed = datetime.fromisoformat(game_start_time.replace("Z", "+00:00"))
    except ValueError:
        return value
    if parsed.utcoffset() is None:
        return value
    normalized = dict(value)
    normalized["game_start_time"] = str(int(parsed.timestamp() * 1_000))
    return normalized


def _validation_error_summary(error: ValidationError) -> str:
    details: list[str] = []
    for item in error.errors(include_url=False, include_input=False):
        location = ".".join(str(part) for part in item.get("loc", ())) or "root"
        details.append(
            f"{location}:{item.get('msg', 'validation failed')}"
            f"({item.get('type', 'unknown')})"
        )
    return ";".join(details) or str(error).replace("\n", " ")


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


def _consume_future_result(future: asyncio.Future[Any]) -> None:
    with contextlib.suppress(asyncio.CancelledError, Exception):
        future.result()
