"""Fail-closed public market WebSocket discovery.

WebSocket books are deliberately local hints only.  A candidate callback receives
identifiers and must perform the authoritative REST confirmation itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from collections import deque
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import inspect
import json
import math
from types import MappingProxyType
from typing import Any

from predmarket.epochs import EpochBook, EpochState
from predmarket.polymarket.clob import BookSnapshot


MARKET_CHANNEL_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MAX_SUBSCRIPTION_TOKENS = 500
MAX_MESSAGE_BYTES = 1_000_000
LATENCY_SAMPLE_CAPACITY = 1024


class WsProtocolError(ValueError):
    """A malformed market-channel message."""

    def __init__(self, reason: str, token_id: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.token_id = token_id


class WatchOperationalError(RuntimeError):
    """All bounded connection attempts produced no accepted domain event."""


@dataclass(frozen=True)
class ReceivedMessage:
    raw: str
    payload: Mapping[str, object]
    event_type: str
    token_id: str | None
    condition_id: str | None
    exchange_ts_ms: int
    received_wall_ms: int
    received_monotonic: float
    book_hash: str | None


@dataclass(frozen=True)
class BookDepth:
    bids: tuple[tuple[str, str], ...] = ()
    asks: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class BookMetadata:
    condition_id: str
    tick_size: Decimal
    minimum_order_size: Decimal

    def __post_init__(self) -> None:
        _identifier(self.condition_id, "condition_id")
        for name in ("tick_size", "minimum_order_size"):
            value = getattr(self, name)
            if type(value) is not Decimal:
                raise TypeError(f"{name} must be Decimal")
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.tick_size >= 1:
            raise ValueError("tick_size must be less than one")


@dataclass(frozen=True)
class WsMetrics:
    received: int = 0
    dropped: int = 0
    malformed: int = 0
    unknown: int = 0
    heartbeats: int = 0
    disconnects: int = 0
    reconnects: int = 0
    resyncs: int = 0
    overflows: int = 0
    callback_failures: int = 0
    reconciliation_attempts: int = 0
    reconciliation_successes: int = 0
    reconciliation_failures: int = 0
    queue_high_water: int = 0
    processing_latencies_ms: tuple[float, ...] = ()
    processing_latency_count: int = 0
    processing_latency_sum_ms: float = 0.0
    processing_latency_min_ms: float | None = None
    processing_latency_max_ms: float | None = None
    processing_latency_sample_truncated: bool = False


def _identifier(value: object, name: str) -> str:
    if type(value) is not str or not value or value.strip() != value:
        raise WsProtocolError(f"{name} must be a nonempty trimmed string")
    return value


def _timestamp(value: object) -> int:
    if type(value) is not str or not value.isascii() or not value.isdigit():
        raise WsProtocolError("timestamp must be a nonnegative integer decimal string")
    return int(value)


def _decimal(value: object, name: str, *, allow_zero: bool) -> Decimal:
    if type(value) is not str:
        raise WsProtocolError(f"{name} must be an exact decimal string")
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise WsProtocolError(f"{name} must be an exact decimal string") from error
    if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
        raise WsProtocolError(f"{name} is out of range")
    return number


def _freeze(value: object) -> object:
    if type(value) is dict:
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if type(value) is list:
        return tuple(_freeze(item) for item in value)
    return value


def _canonical_decimal(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    if rendered.startswith("."):
        rendered = f"0{rendered}"
    return rendered or "0"


class MarketWebSocket:
    """Owns bounded ingestion and per-token epoch state for discovery."""

    def __init__(
        self,
        token_conditions: Mapping[str, str],
        *,
        queue_capacity: int,
        wall_clock_ms: Callable[[], int],
        monotonic: Callable[[], float],
        candidate_callback: Callable[
            [tuple[str, ...], str], Awaitable[None] | None
        ]
        | None = None,
        event_callback: Callable[[ReceivedMessage], Awaitable[None] | None] | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        heartbeat_interval_seconds: int | float = 10,
        heartbeat_timeout_seconds: int | float = 3,
        book_metadata: Mapping[str, BookMetadata] | None = None,
    ) -> None:
        if not isinstance(token_conditions, Mapping) or not token_conditions:
            raise ValueError("token_conditions must be a nonempty mapping")
        if len(token_conditions) > MAX_SUBSCRIPTION_TOKENS:
            raise ValueError("subscription exceeds maximum token count")
        copied: dict[str, str] = {}
        for token, condition in token_conditions.items():
            token_value = _identifier(token, "token_id")
            condition_value = _identifier(condition, "condition_id")
            if token_value in copied:
                raise ValueError("token IDs must be unique")
            copied[token_value] = condition_value
        if type(queue_capacity) is not int or queue_capacity <= 0:
            raise ValueError("queue_capacity must be a positive integer")
        if not callable(wall_clock_ms) or not callable(monotonic):
            raise TypeError("clocks must be callable")
        if candidate_callback is not None and not callable(candidate_callback):
            raise TypeError("candidate_callback must be callable")
        if book_metadata is None:
            metadata_copy: dict[str, BookMetadata] = {}
        elif not isinstance(book_metadata, Mapping):
            raise TypeError("book_metadata must be a mapping")
        else:
            metadata_copy = dict(book_metadata)
        if not set(metadata_copy).issubset(copied):
            raise ValueError("book_metadata contains unsubscribed tokens")
        for token, metadata in metadata_copy.items():
            if not isinstance(metadata, BookMetadata):
                raise TypeError("book_metadata values must be BookMetadata")
            if metadata.condition_id != copied[token]:
                raise ValueError("book metadata condition binding mismatch")
        if not callable(sleeper):
            raise TypeError("sleeper must be callable")
        for name, value in (
            ("heartbeat_interval_seconds", heartbeat_interval_seconds),
            ("heartbeat_timeout_seconds", heartbeat_timeout_seconds),
        ):
            if (
                isinstance(value, bool)
                or type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be finite and positive")

        self._conditions = MappingProxyType(copied)
        self._book_metadata = MappingProxyType(metadata_copy)
        self.epochs = {token: EpochBook(token) for token in copied}
        self._depth = {token: BookDepth() for token in copied}
        self._queue: asyncio.Queue[ReceivedMessage] = asyncio.Queue(queue_capacity)
        self._wall_clock_ms = wall_clock_ms
        self._monotonic = monotonic
        self._callback = candidate_callback
        self._event_callback = event_callback
        self._sleeper = sleeper
        self._heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self._heartbeat_timeout_seconds = float(heartbeat_timeout_seconds)
        self._metrics = WsMetrics()
        self._processing_latencies = deque(maxlen=LATENCY_SAMPLE_CAPACITY)
        self._trigger_keys: set[tuple[tuple[str, str | None], ...]] = set()
        self._tick_sizes: dict[str, Decimal] = {}
        self._state_lock = asyncio.Lock()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def subscription_payload(self) -> dict[str, object]:
        return {
            "assets_ids": sorted(self._conditions),
            "type": "market",
            "custom_feature_enabled": True,
        }

    def metrics(self) -> WsMetrics:
        return replace(
            self._metrics,
            processing_latencies_ms=tuple(self._processing_latencies),
            processing_latency_sample_truncated=(
                self._metrics.processing_latency_count
                > LATENCY_SAMPLE_CAPACITY
            ),
        )

    def depth(self, token_id: str) -> BookDepth:
        return self._depth[token_id]

    def _clock(self) -> tuple[int, float]:
        wall, mono = self._wall_clock_ms(), self._monotonic()
        if type(wall) is not int or wall < 0:
            raise ValueError("wall clock must return nonnegative integer milliseconds")
        if (
            isinstance(mono, bool)
            or type(mono) not in (int, float)
            or not math.isfinite(mono)
            or mono < 0
        ):
            raise ValueError("monotonic clock must return finite nonnegative seconds")
        return wall, float(mono)

    async def ingest(
        self, raw: str | bytes, *, max_accepted: int | None = None
    ) -> bool:
        if max_accepted is not None and (
            type(max_accepted) is not int or max_accepted <= 0
        ):
            raise ValueError("max_accepted must be a positive integer or None")
        if isinstance(raw, bytes):
            if len(raw) > MAX_MESSAGE_BYTES:
                self._unknown_corruption("message_too_large")
                return False
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                self._unknown_corruption("invalid_utf8")
                return False
        if type(raw) is not str:
            raise TypeError("raw message must be str or bytes")
        if len(raw.encode("utf-8")) > MAX_MESSAGE_BYTES:
            self._unknown_corruption("message_too_large")
            return False
        if raw == "PONG":
            self._metrics = replace(
                self._metrics, heartbeats=self._metrics.heartbeats + 1
            )
            return True
        try:
            decoded = json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
        except (json.JSONDecodeError, ValueError):
            self._unknown_corruption("invalid_json")
            return False
        items = decoded if type(decoded) is list else [decoded]
        if not items or any(type(item) is not dict for item in items):
            self._unknown_corruption("message_root")
            return False

        messages: list[ReceivedMessage] = []
        for payload in items:
            wall, mono = self._clock()
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            event_type = payload.get("event_type")
            if type(event_type) is not str or not event_type:
                self._unknown_corruption("missing_event_type")
                return False
            token = payload.get("asset_id")
            token_id = token if type(token) is str else None
            condition = payload.get("market")
            condition_id = condition if type(condition) is str else None
            try:
                timestamp = _timestamp(payload.get("timestamp"))
            except WsProtocolError:
                self._metrics = replace(
                    self._metrics, malformed=self._metrics.malformed + 1
                )
                if token_id in self.epochs:
                    self._fail_scope(token_id, "malformed_timestamp")
                else:
                    self._invalidate_all("malformed_timestamp")
                return False
            book_hash = payload.get("hash")
            if book_hash is not None and (type(book_hash) is not str or not book_hash):
                self._metrics = replace(
                    self._metrics, malformed=self._metrics.malformed + 1
                )
                if token_id in self.epochs:
                    self._fail_scope(token_id, "malformed_hash")
                else:
                    self._invalidate_all("malformed_hash")
                return False
            messages.append(
                ReceivedMessage(
                    raw=canonical,
                    payload=_freeze(payload),
                    event_type=event_type,
                    token_id=token_id,
                    condition_id=condition_id,
                    exchange_ts_ms=timestamp,
                    received_wall_ms=wall,
                    received_monotonic=mono,
                    book_hash=book_hash,
                )
            )

        if max_accepted is not None and len(messages) > max_accepted:
            remainder = len(messages) - max_accepted
            messages = messages[:max_accepted]
            self._metrics = replace(
                self._metrics, dropped=self._metrics.dropped + remainder
            )
        if self._queue.qsize() + len(messages) > self._queue.maxsize:
            dropped = len(messages)
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()
                dropped += 1
            self._metrics = replace(
                self._metrics,
                dropped=self._metrics.dropped + dropped,
                overflows=self._metrics.overflows + 1,
            )
            self._invalidate_all("queue_overflow")
            return False
        for message in messages:
            self._queue.put_nowait(message)
            self._metrics = replace(
                self._metrics,
                received=self._metrics.received + 1,
                queue_high_water=max(
                    self._metrics.queue_high_water, self._queue.qsize()
                ),
            )
        return True

    async def process_one(self) -> ReceivedMessage:
        message = await self._queue.get()
        try:
            async with self._state_lock:
                await self._process(message)
            latency = (self._monotonic() - message.received_monotonic) * 1000
            if not math.isfinite(latency) or latency < 0:
                raise ValueError("processing clock regressed")
            self._processing_latencies.append(latency)
            self._metrics = replace(
                self._metrics,
                processing_latency_count=(
                    self._metrics.processing_latency_count + 1
                ),
                processing_latency_sum_ms=(
                    self._metrics.processing_latency_sum_ms + latency
                ),
                processing_latency_min_ms=(
                    latency if self._metrics.processing_latency_min_ms is None
                    else min(self._metrics.processing_latency_min_ms, latency)
                ),
                processing_latency_max_ms=(
                    latency if self._metrics.processing_latency_max_ms is None
                    else max(self._metrics.processing_latency_max_ms, latency)
                ),
            )
            if self._event_callback is not None:
                result = self._event_callback(message)
                if inspect.isawaitable(result):
                    await result
            return message
        except WsProtocolError as error:
            self._metrics = replace(
                self._metrics, malformed=self._metrics.malformed + 1
            )
            self._fail_scope(error.token_id or message.token_id, error.reason)
            raise
        finally:
            self._queue.task_done()

    async def reconcile_rest(
        self, snapshots: tuple[BookSnapshot, ...]
    ) -> bool:
        """Atomically replace local hint books from a complete REST batch."""
        self._metrics = replace(
            self._metrics,
            reconciliation_attempts=self._metrics.reconciliation_attempts + 1,
        )
        try:
            if (
                type(snapshots) is not tuple
                or any(not isinstance(item, BookSnapshot) for item in snapshots)
            ):
                raise ValueError("REST reconciliation requires BookSnapshot tuple")
            by_token = {item.token_id: item for item in snapshots}
            if len(by_token) != len(snapshots) or set(by_token) != set(self.epochs):
                raise ValueError("REST reconciliation token coverage mismatch")
            staged: dict[str, BookDepth] = {}
            for token, item in by_token.items():
                metadata = self._book_metadata[token]
                if item.condition_id != self._conditions[token]:
                    raise ValueError("REST reconciliation condition mismatch")
                book = item.book
                if (
                    book.tick_size != metadata.tick_size
                    or book.minimum_order_size != metadata.minimum_order_size
                ):
                    raise ValueError("REST reconciliation metadata mismatch")
                previous = self.epochs[token].exchange_ts_ms
                if previous is not None and book.exchange_ts_ms < previous:
                    raise ValueError("REST reconciliation timestamp regression")
                staged[token] = BookDepth(
                    tuple(
                        (_canonical_decimal(level.price), _canonical_decimal(level.size))
                        for level in book.bids
                    ),
                    tuple(
                        (_canonical_decimal(level.price), _canonical_decimal(level.size))
                        for level in book.asks
                    ),
                )
            async with self._state_lock:
                for token, item in by_token.items():
                    if not self.epochs[token].replace_snapshot(
                        item.book.book_hash, item.book.exchange_ts_ms
                    ):
                        raise ValueError("REST reconciliation commit regression")
                for token, depth in staged.items():
                    self._depth[token] = depth
                    self._tick_sizes[token] = self._book_metadata[token].tick_size
                self._trigger_keys.clear()
            self._metrics = replace(
                self._metrics,
                reconciliation_successes=self._metrics.reconciliation_successes + 1,
            )
            for condition in sorted(set(self._conditions.values())):
                await self._maybe_trigger(condition)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self._invalidate_all("rest_reconciliation_failure")
            self._metrics = replace(
                self._metrics,
                reconciliation_failures=self._metrics.reconciliation_failures + 1,
            )
            return False

    async def _process(self, message: ReceivedMessage) -> None:
        if message.event_type == "book":
            await self._snapshot(message)
        elif message.event_type == "price_change":
            await self._changes(message)
        elif message.event_type == "tick_size_change":
            token = self._validate_scope(
                message.payload.get("asset_id"), message.payload.get("market")
            )
            if token is not None:
                self._fail_scope(token, "tick_size_change")
        elif message.event_type == "market_resolved":
            self._invalidate_condition(message.condition_id, "market_resolved")
        elif message.event_type in {"last_trade_price", "best_bid_ask", "new_market"}:
            if "asset_id" in message.payload:
                self._validate_scope(
                    message.payload.get("asset_id"), message.payload.get("market")
                )
            return
        else:
            self._invalidate_all("unknown_event_type")

    def _validate_scope(self, token: object, condition: object) -> str | None:
        if type(token) is not str or not token:
            raise WsProtocolError("missing_token")
        if token not in self._conditions:
            self._metrics = replace(
                self._metrics, unknown=self._metrics.unknown + 1
            )
            self._invalidate_all("unknown_token")
            return None
        if type(condition) is not str or condition != self._conditions[token]:
            raise WsProtocolError("condition_mismatch", token)
        return token

    @staticmethod
    def _levels(value: object, side: str) -> tuple[tuple[str, str], ...]:
        if type(value) not in (list, tuple):
            raise WsProtocolError(f"{side} levels must be a list")
        parsed: dict[Decimal, tuple[str, str]] = {}
        for level in value:
            if not isinstance(level, Mapping):
                raise WsProtocolError("level must be an object")
            price = _decimal(level.get("price"), "price", allow_zero=False)
            size = _decimal(level.get("size"), "size", allow_zero=False)
            if price >= 1:
                raise WsProtocolError("price is out of range")
            if price in parsed:
                raise WsProtocolError("duplicate equivalent price levels")
            parsed[price] = (
                _canonical_decimal(price),
                _canonical_decimal(size),
            )
        reverse = side == "bids"
        return tuple(parsed[key] for key in sorted(parsed, reverse=reverse))

    async def _snapshot(self, message: ReceivedMessage) -> None:
        token = self._validate_scope(message.payload.get("asset_id"), message.payload.get("market"))
        if token is None:
            return
        if message.book_hash is None:
            raise WsProtocolError("missing_hash", token)
        bids = self._levels(message.payload.get("bids"), "bids")
        asks = self._levels(message.payload.get("asks"), "asks")
        metadata = self._book_metadata.get(token)
        if metadata is None:
            raise WsProtocolError("missing_authoritative_book_metadata", token)
        tick = metadata.tick_size
        minimum = metadata.minimum_order_size
        for field, authoritative in (
            ("tick_size", tick),
            ("min_order_size", minimum),
        ):
            supplied = message.payload.get(field)
            if supplied is not None and _decimal(
                supplied, field, allow_zero=False
            ) != authoritative:
                raise WsProtocolError("book_metadata_mismatch", token)
        if any(Decimal(price) % tick for price, _ in bids + asks):
            raise WsProtocolError("price_not_tick_aligned", token)
        if bids and asks and Decimal(bids[0][0]) >= Decimal(asks[0][0]):
            raise WsProtocolError("crossed_book", token)
        if not self.epochs[token].replace_snapshot(message.book_hash, message.exchange_ts_ms):
            return
        self._tick_sizes[token] = tick
        self._depth[token] = BookDepth(bids=bids, asks=asks)
        await self._maybe_trigger(self._conditions[token])

    async def _changes(self, message: ReceivedMessage) -> None:
        if type(message.condition_id) is not str:
            raise WsProtocolError("missing_condition")
        changes = message.payload.get("price_changes")
        if type(changes) not in (list, tuple) or not changes:
            raise WsProtocolError("price_changes must be a nonempty list")
        validated: list[tuple[str, str, str, str, str]] = []
        affected: set[str] = set()
        try:
            for change in changes:
                if not isinstance(change, Mapping):
                    raise WsProtocolError("change must be an object")
                token = self._validate_scope(change.get("asset_id"), message.condition_id)
                if token is None:
                    continue
                affected.add(token)
                price = change.get("price")
                size = change.get("size")
                decimal_price = _decimal(price, "price", allow_zero=False)
                _decimal(size, "size", allow_zero=True)
                if decimal_price >= 1:
                    raise WsProtocolError("price is out of range", token)
                tick = self._tick_sizes.get(token)
                if (
                    self.epochs[token].state is EpochState.LIVE
                    and (tick is None or decimal_price % tick)
                ):
                    raise WsProtocolError("price_not_tick_aligned", token)
                side = change.get("side")
                if side not in {"BUY", "SELL"} or type(side) is not str:
                    raise WsProtocolError("side must be BUY or SELL", token)
                change_hash = change.get("hash")
                if type(change_hash) is not str or not change_hash:
                    raise WsProtocolError("missing_hash", token)
                validated.append(
                    (
                        token,
                        _canonical_decimal(decimal_price),
                        _canonical_decimal(_decimal(size, "size", allow_zero=True)),
                        side,
                        change_hash,
                    )
                )
        except WsProtocolError as error:
            self._invalidate_condition(message.condition_id, error.reason)
            raise

        for token in affected:
            hashes = {item[4] for item in validated if item[0] == token}
            if len(hashes) != 1:
                self._invalidate_condition(message.condition_id, "inconsistent_hash")
                raise WsProtocolError("inconsistent_hash", token)
            epoch_ts = self.epochs[token].exchange_ts_ms
            if (
                self.epochs[token].state is EpochState.LIVE
                and epoch_ts is not None
                and message.exchange_ts_ms < epoch_ts
            ):
                self._invalidate_condition(message.condition_id, "timestamp_regression")
                return

        staged = {token: self._depth[token] for token in affected}
        for token, price, size, side, _ in validated:
            if self.epochs[token].state is not EpochState.LIVE:
                continue
            levels = dict(staged[token].bids if side == "BUY" else staged[token].asks)
            if Decimal(size) == 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            ordered = tuple(
                sorted(levels.items(), key=lambda item: Decimal(item[0]), reverse=side == "BUY")
            )
            staged[token] = (
                BookDepth(ordered, staged[token].asks)
                if side == "BUY"
                else BookDepth(staged[token].bids, ordered)
            )

        for token in affected:
            depth = staged[token]
            if (
                depth.bids
                and depth.asks
                and Decimal(depth.bids[0][0]) >= Decimal(depth.asks[0][0])
            ):
                self._invalidate_condition(message.condition_id, "crossed_book")
                raise WsProtocolError("crossed_book", token)

        for token in affected:
            if self.epochs[token].state is not EpochState.LIVE:
                continue
            token_changes = [item for item in validated if item[0] == token]
            new_hash = token_changes[-1][4]
            if not self.epochs[token].apply_delta(
                token_changes[-1][1], token_changes[-1][2],
                token_changes[-1][3], message.exchange_ts_ms,
            ):
                continue
            self.epochs[token].replace_snapshot(new_hash, message.exchange_ts_ms)
            self._depth[token] = staged[token]
        for condition in {
            self._conditions[token]
            for token in affected
            if self.epochs[token].state is EpochState.LIVE
        }:
            await self._maybe_trigger(condition)

    async def _maybe_trigger(self, condition: str) -> None:
        tokens = tuple(sorted(token for token, value in self._conditions.items() if value == condition))
        if len(tokens) != 2 or self._callback is None:
            return
        if any(self.epochs[token].state is not EpochState.LIVE for token in tokens):
            return
        has_underpriced_asks = all(self._depth[token].asks for token in tokens) and (
            sum(
                (Decimal(self._depth[token].asks[0][0]) for token in tokens),
                Decimal(0),
            )
            < 1
        )
        has_overpriced_bids = all(self._depth[token].bids for token in tokens) and (
            sum(
                (Decimal(self._depth[token].bids[0][0]) for token in tokens),
                Decimal(0),
            )
            > 1
        )
        if not (has_underpriced_asks or has_overpriced_bids):
            return
        key = tuple((token, self.epochs[token].snapshot_hash) for token in tokens)
        if key in self._trigger_keys:
            return
        self._trigger_keys.add(key)
        try:
            result = self._callback(tokens, condition)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            self._metrics = replace(
                self._metrics,
                callback_failures=self._metrics.callback_failures + 1,
            )
            self._trigger_keys.discard(key)

    def _fail_scope(self, token: str | None, reason: str) -> None:
        if token in self.epochs:
            self.epochs[token].invalidate(reason)
            self._trigger_keys.clear()

    def _invalidate_condition(self, condition: str | None, reason: str) -> None:
        if type(condition) is not str:
            self._invalidate_all(reason)
            return
        matched = False
        for token, expected in self._conditions.items():
            if expected == condition:
                matched = True
                self.epochs[token].invalidate(reason)
        if not matched:
            self._invalidate_all(reason)
            return
        self._trigger_keys.clear()

    def _invalidate_all(self, reason: str) -> None:
        for epoch in self.epochs.values():
            epoch.invalidate(reason)
        self._trigger_keys.clear()
        self._metrics = replace(
            self._metrics, resyncs=self._metrics.resyncs + 1
        )

    def _unknown_corruption(self, reason: str) -> None:
        self._metrics = replace(
            self._metrics, malformed=self._metrics.malformed + 1
        )
        self._invalidate_all(reason)

    def on_disconnect(self, reason: str = "disconnect") -> None:
        _identifier(reason, "reason")
        self._metrics = replace(
            self._metrics, disconnects=self._metrics.disconnects + 1
        )
        self._invalidate_all(reason)

    def on_subscription_change(self) -> None:
        self._invalidate_all("subscription_change")

    async def serve_connection(
        self,
        connection: object,
        *,
        max_messages: int | None = None,
    ) -> None:
        """Serve one connection with independent receiver, processor and timer."""
        if max_messages is not None and (
            type(max_messages) is not int or max_messages <= 0
        ):
            raise ValueError("max_messages must be a positive integer or None")
        send = getattr(connection, "send", None)
        recv = getattr(connection, "recv", None)
        close = getattr(connection, "close", None)
        if not all(callable(value) for value in (send, recv, close)):
            raise TypeError("connection must provide async send, recv, and close")

        subscription = json.dumps(
            self.subscription_payload(), separators=(",", ":"), sort_keys=True
        )
        stop = asyncio.Event()

        async def receive_messages() -> None:
            accepted = 0
            while max_messages is None or accepted < max_messages:
                raw = await recv()
                remaining = (
                    None if max_messages is None
                    else max_messages - accepted
                )
                before = self._metrics.received
                await self.ingest(raw, max_accepted=remaining)
                accepted += self._metrics.received - before

        async def process_messages() -> None:
            while True:
                await self.process_one()

        async def heartbeat() -> None:
            while not stop.is_set():
                await self._sleeper(self._heartbeat_interval_seconds)
                if stop.is_set():
                    return
                pong_count = self._metrics.heartbeats
                await send("PING")
                await self._sleeper(self._heartbeat_timeout_seconds)
                if self._metrics.heartbeats == pong_count:
                    raise TimeoutError("heartbeat_timeout")

        tasks: set[asyncio.Task[None]] = set()
        try:
            await send(subscription)
            receiver_task = asyncio.create_task(receive_messages())
            processor_task = asyncio.create_task(process_messages())
            heartbeat_task = asyncio.create_task(heartbeat())
            tasks = {receiver_task, processor_task, heartbeat_task}
            completed, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in completed:
                error = task.exception()
                if error is not None:
                    raise error
            if receiver_task.done():
                await self._queue.join()
        except asyncio.CancelledError:
            raise
        except (EOFError, ConnectionError, TimeoutError):
            pass
        finally:
            stop.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()
            try:
                await close()
            finally:
                self.on_disconnect("connection_closed")

    async def run(
        self,
        connector: Callable[[str], Awaitable[object]],
        *,
        max_attempts: int,
        sleeper: Callable[[float], Awaitable[None]],
        base_backoff: float,
        max_backoff: float,
        max_messages: int | None = None,
    ) -> None:
        """Run a finite reconnect loop; callers choose whether to invoke again."""
        if not callable(connector) or not callable(sleeper):
            raise TypeError("connector and sleeper must be callable")
        if type(max_attempts) is not int or max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if max_messages is not None and (
            type(max_messages) is not int or max_messages <= 0
        ):
            raise ValueError("max_messages must be a positive integer or None")
        if (
            isinstance(base_backoff, bool)
            or isinstance(max_backoff, bool)
            or type(base_backoff) not in (int, float)
            or type(max_backoff) not in (int, float)
            or not math.isfinite(base_backoff)
            or not math.isfinite(max_backoff)
            or base_backoff < 0
            or max_backoff < base_backoff
        ):
            raise ValueError("backoff values must be finite and ordered")

        starting_received = self._metrics.received
        for attempt in range(max_attempts):
            remaining = (
                None if max_messages is None
                else max_messages - self._metrics.received
            )
            if remaining is not None and remaining <= 0:
                break
            try:
                connection = await connector(MARKET_CHANNEL_URL)
                await self.serve_connection(
                    connection, max_messages=remaining
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                self.on_disconnect("connection_error")
            if (
                max_messages is not None
                and self._metrics.received >= max_messages
            ):
                break
            if attempt + 1 < max_attempts:
                self._metrics = replace(
                    self._metrics, reconnects=self._metrics.reconnects + 1
                )
                await sleeper(min(max_backoff, base_backoff * (2**attempt)))
        if self._metrics.received == starting_received:
            self._invalidate_all("watch_attempts_exhausted")
            raise WatchOperationalError(
                "watch attempts exhausted without an accepted market event"
            )
