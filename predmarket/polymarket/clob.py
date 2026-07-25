from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
import time
from typing import Any, Callable

import httpx

from predmarket.domain import BookLevel
from predmarket.fees import FeeSchedule
from predmarket.orderbook import OrderBook
from predmarket.polymarket import (
    AdapterHTTPError,
    AdapterInvariantError,
    AdapterPayloadError,
    AdapterTransportError,
)
from predmarket.polymarket.gamma import _reject_credential_headers, _validate_base_url


def _string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value:
        raise AdapterPayloadError(f"{key} must be a non-empty string")
    return value


def _decimal_string(raw: Any, name: str, *, allow_zero: bool = False) -> Decimal:
    if not isinstance(raw, str):
        raise AdapterPayloadError(f"{name} must be a decimal string")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise AdapterPayloadError(f"{name} is not a decimal") from exc
    if not value.is_finite() or value < 0 or (not allow_zero and value == 0):
        raise AdapterPayloadError(f"{name} must be finite and {'nonnegative' if allow_zero else 'positive'}")
    return value


def _clock_values(wall_clock_ms: Callable[[], int], monotonic: Callable[[], float]) -> tuple[int, float]:
    wall = wall_clock_ms()
    mono = monotonic()
    if isinstance(wall, bool) or not isinstance(wall, int) or wall < 0:
        raise AdapterInvariantError("wall clock must return a nonnegative integer millisecond timestamp")
    if isinstance(mono, bool) or not isinstance(mono, (int, float)) or not math.isfinite(mono) or mono < 0:
        raise AdapterInvariantError("monotonic clock must return a finite nonnegative number")
    return wall, float(mono)


@dataclass(frozen=True)
class BookSnapshot:
    book: OrderBook
    market_id: str
    neg_risk: bool
    last_trade_price: Decimal | None
    received_at_ms: int
    received_monotonic: float

    @property
    def token_id(self) -> str:
        return self.book.token_id


@dataclass(frozen=True)
class FeeRateEvidence:
    token_id: str
    base_fee_bps: int
    rate: Decimal
    schedule: FeeSchedule | None
    received_at_ms: int
    received_monotonic: float
    provenance: str
    raw_json: str

    def with_exponent(self, exponent: int, *, taker_only: bool = True) -> FeeSchedule:
        return FeeSchedule(self.rate, exponent, taker_only, self.received_at_ms)


class ClobRestClient:
    """Strict adapter for public batch books and token fee rates."""

    def __init__(
        self,
        http: httpx.AsyncClient | None = None,
        base_url: str = "https://clob.polymarket.com",
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
        max_response_bytes: int = 16_000_000,
        wall_clock_ms: Callable[[], int] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.base_url = _validate_base_url(base_url)
        if isinstance(max_response_bytes, bool) or not isinstance(max_response_bytes, int):
            raise TypeError("max_response_bytes must be an integer")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        if http is not None and transport is not None:
            raise ValueError("transport may only be supplied for an owned HTTP client")
        self._owned = http is None
        self.http = http or httpx.AsyncClient(transport=transport, timeout=timeout)
        _reject_credential_headers(self.http)
        self.max_response_bytes = max_response_bytes
        self.wall_clock_ms = wall_clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.monotonic = monotonic or time.monotonic
        self._closed = False

    async def __aenter__(self) -> ClobRestClient:
        if self._closed:
            raise RuntimeError("client is closed")
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owned and not self._closed:
            await self.http.aclose()
        self._closed = True

    async def _request(self, method: str, path: str, **kwargs: Any) -> tuple[Any, int, float]:
        if self._closed:
            raise RuntimeError("client is closed")
        try:
            response = await self.http.request(method, f"{self.base_url}{path}", **kwargs)
        except httpx.HTTPError as exc:
            raise AdapterTransportError("CLOB request failed") from exc
        received_at_ms, received_monotonic = _clock_values(
            self.wall_clock_ms, self.monotonic
        )
        if not 200 <= response.status_code < 300:
            raise AdapterHTTPError(f"CLOB returned HTTP {response.status_code}")
        if len(response.content) > self.max_response_bytes:
            raise AdapterPayloadError("CLOB response exceeds configured size limit")
        try:
            return response.json(), received_at_ms, received_monotonic
        except (ValueError, UnicodeError) as exc:
            raise AdapterPayloadError("CLOB returned invalid JSON") from exc

    @staticmethod
    def _validate_tokens(token_ids: list[str] | tuple[str, ...], *, maximum: int = 500) -> tuple[str, ...]:
        if not isinstance(token_ids, (list, tuple)):
            raise TypeError("token_ids must be a list or tuple")
        if not 1 <= len(token_ids) <= maximum:
            raise ValueError(f"token_ids batch must contain 1..{maximum} values")
        if any(not isinstance(token, str) or not token for token in token_ids):
            raise TypeError("token IDs must be non-empty strings")
        if len(set(token_ids)) != len(token_ids):
            raise ValueError("token IDs must be unique")
        return tuple(token_ids)

    @staticmethod
    def _levels(raw: Any, name: str) -> tuple[BookLevel, ...]:
        if not isinstance(raw, list):
            raise AdapterPayloadError(f"{name} must be an array")
        levels: list[BookLevel] = []
        for item in raw:
            if not isinstance(item, dict):
                raise AdapterPayloadError(f"{name} levels must be objects")
            try:
                levels.append(
                    BookLevel(
                        _decimal_string(item.get("price"), f"{name}.price"),
                        _decimal_string(item.get("size"), f"{name}.size"),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise AdapterPayloadError(f"invalid {name} level: {exc}") from exc
        reverse = name == "bids"
        return tuple(sorted(levels, key=lambda level: level.price, reverse=reverse))

    def _book(self, raw: Any, received_at_ms: int, received_monotonic: float) -> BookSnapshot:
        if not isinstance(raw, dict):
            raise AdapterPayloadError("book must be an object")
        timestamp = _string(raw, "timestamp")
        if not timestamp.isascii() or not timestamp.isdigit():
            raise AdapterPayloadError("timestamp must be an epoch-millisecond digit string")
        neg_risk = raw.get("neg_risk")
        if not isinstance(neg_risk, bool):
            raise AdapterPayloadError("neg_risk must be bool")
        last_raw = raw.get("last_trade_price")
        last_trade = None if last_raw is None else _decimal_string(last_raw, "last_trade_price")
        if last_trade is not None and not Decimal("0") < last_trade < Decimal("1"):
            raise AdapterPayloadError("last_trade_price must be strictly between 0 and 1")
        try:
            book = OrderBook(
                token_id=_string(raw, "asset_id"),
                bids=self._levels(raw.get("bids"), "bids"),
                asks=self._levels(raw.get("asks"), "asks"),
                tick_size=_decimal_string(raw.get("tick_size"), "tick_size"),
                minimum_order_size=_decimal_string(raw.get("min_order_size"), "min_order_size"),
                exchange_ts_ms=int(timestamp),
                book_hash=_string(raw, "hash"),
            )
        except AdapterPayloadError:
            raise
        except (TypeError, ValueError) as exc:
            raise AdapterInvariantError(f"invalid order book: {exc}") from exc
        return BookSnapshot(
            book=book,
            market_id=_string(raw, "market"),
            neg_risk=neg_risk,
            last_trade_price=last_trade,
            received_at_ms=received_at_ms,
            received_monotonic=received_monotonic,
        )

    async def books(self, token_ids: list[str] | tuple[str, ...]) -> tuple[BookSnapshot, ...]:
        requested = self._validate_tokens(token_ids)
        payload, received_at_ms, received_monotonic = await self._request(
            "POST", "/books", json=[{"token_id": token} for token in requested]
        )
        if not isinstance(payload, list):
            raise AdapterPayloadError("books response must be an array")
        records = [self._book(raw, received_at_ms, received_monotonic) for raw in payload]
        actual = [record.token_id for record in records]
        if len(actual) != len(set(actual)):
            raise AdapterInvariantError("books response contains duplicate tokens")
        if set(actual) != set(requested) or len(actual) != len(requested):
            raise AdapterInvariantError("books response must cover requested tokens exactly once")
        by_token = {record.token_id: record for record in records}
        return tuple(by_token[token] for token in requested)

    async def fee_rate(self, token_id: str) -> FeeRateEvidence:
        token = self._validate_tokens([token_id], maximum=1)[0]
        payload, received_at_ms, received_monotonic = await self._request(
            "GET", "/fee-rate", params={"token_id": token}
        )
        if not isinstance(payload, dict) or set(payload) != {"base_fee"}:
            raise AdapterPayloadError("fee response must contain only base_fee")
        base_fee = payload["base_fee"]
        if isinstance(base_fee, bool) or not isinstance(base_fee, int) or base_fee < 0:
            raise AdapterPayloadError("base_fee must be a nonnegative integer basis-point value")
        rate = Decimal(base_fee) / Decimal("10000")
        schedule = FeeSchedule(Decimal("0"), 1, True, received_at_ms) if base_fee == 0 else None
        import json

        return FeeRateEvidence(
            token_id=token,
            base_fee_bps=base_fee,
            rate=rate,
            schedule=schedule,
            received_at_ms=received_at_ms,
            received_monotonic=received_monotonic,
            provenance="GET /fee-rate",
            raw_json=json.dumps(payload, sort_keys=True, separators=(",", ":")),
        )

    async def fee_rates(self, token_ids: list[str] | tuple[str, ...]) -> tuple[FeeRateEvidence, ...]:
        requested = self._validate_tokens(token_ids)
        return tuple([await self.fee_rate(token) for token in requested])


__all__ = ["BookSnapshot", "ClobRestClient", "FeeRateEvidence"]
