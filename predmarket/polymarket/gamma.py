from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
import json
from typing import Any
from urllib.parse import urlsplit

import httpx

from predmarket.polymarket import (
    AdapterHTTPError,
    AdapterInvariantError,
    AdapterPayloadError,
    AdapterSecurityError,
    AdapterTransportError,
)


_CREDENTIAL_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "poly-api-key",
        "poly-signature",
        "poly-passphrase",
    }
)
GAMMA_PUBLIC_ORIGIN = "https://gamma-api.polymarket.com"


def _validate_base_url(value: str, official_origin: str) -> str:
    if not isinstance(value, str):
        raise TypeError("base_url must be a string")
    if value != official_origin:
        raise ValueError(f"base_url must be the official public origin {official_origin}")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"base_url must be the official public origin {official_origin}")
    return value


def _reject_credential_headers(http: httpx.AsyncClient) -> None:
    names = {name.lower() for name in http.headers}
    if names & _CREDENTIAL_HEADERS or any(
        name.startswith(("poly_", "poly-")) for name in names
    ):
        raise AdapterSecurityError(
            "credential headers are not allowed on public read-only clients"
        )
    if len(http.cookies):
        raise AdapterSecurityError(
            "credential cookies are not allowed on public read-only clients"
        )
    if getattr(http, "_auth", None) is not None:
        raise AdapterSecurityError(
            "HTTP authentication is not allowed on public read-only clients"
        )


def _clear_response_cookies(
    http: httpx.AsyncClient, response: httpx.Response
) -> None:
    client_cookie_keys = {
        (cookie.name, cookie.domain, cookie.path)
        for cookie in http.cookies.jar
    }
    for cookie in response.cookies.jar:
        key = (cookie.name, cookie.domain, cookie.path)
        if key not in client_cookie_keys:
            continue
        http.cookies.delete(
            cookie.name,
            domain=cookie.domain,
            path=cookie.path,
        )


def _required_string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AdapterPayloadError(f"{key} must be a non-empty string")
    return value


def _optional_string(raw: dict[str, Any], key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise AdapterPayloadError(f"{key} must be a non-empty string when present")
    return value


def _optional_bool(raw: dict[str, Any], key: str) -> bool | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise AdapterPayloadError(f"{key} must be bool when present")
    return value


def _string_array(raw: dict[str, Any], key: str) -> tuple[str, ...]:
    value = raw.get(key)
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise AdapterPayloadError(f"{key} is not a valid JSON array") from exc
    if not isinstance(value, list) or not value:
        raise AdapterPayloadError(f"{key} must be a non-empty array")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise AdapterPayloadError(f"{key} must contain non-empty strings")
    return tuple(value)


@dataclass(frozen=True)
class TokenMetadata:
    token_id: str
    outcome: str


@dataclass(frozen=True)
class EventMetadata:
    event_id: str
    slug: str | None
    title: str | None
    source_metadata_json: str


@dataclass(frozen=True)
class MarketMetadata:
    market_id: str
    condition_id: str
    question: str
    slug: str | None
    events: tuple[EventMetadata, ...]
    tokens: tuple[TokenMetadata, TokenMetadata]
    active: bool | None
    closed: bool | None
    archived: bool | None
    accepting_orders: bool | None
    enable_order_book: bool | None
    neg_risk: bool | None
    end_date: str | None
    fees_enabled: bool | None
    fee_schedule_source_json: str | None
    fee_schedule_source: str | None
    source_metadata_json: str

    @property
    def event(self) -> EventMetadata | None:
        """Return the unambiguous event, never guess among multiple relations."""
        return self.events[0] if len(self.events) == 1 else None

    @property
    def yes_token_id(self) -> str:
        return next(token.token_id for token in self.tokens if token.outcome == "YES")

    @property
    def no_token_id(self) -> str:
        return next(token.token_id for token in self.tokens if token.outcome == "NO")

    @property
    def is_binary(self) -> bool:
        return {token.outcome for token in self.tokens} == {"YES", "NO"}

    @property
    def is_tradeable(self) -> bool:
        return (
            self.is_binary
            and self.active is True
            and self.closed is False
            and self.archived is not True
            and self.accepting_orders is not False
            and self.enable_order_book is True
        )


@dataclass(frozen=True)
class MarketDiagnostic:
    market_id: str | None
    reason: str


def _canonical_json(value: Any, name: str) -> str:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise AdapterPayloadError(f"{name} is not JSON-compatible") from exc


def _normalize_events(raw: dict[str, Any]) -> tuple[EventMetadata, ...]:
    # Gamma has emitted both the current lower-case relation and a legacy
    # ORM-style capitalized alias. Ambiguous dual representations fail closed.
    keys = [key for key in ("events", "Events") if key in raw]
    if len(keys) > 1:
        raise AdapterInvariantError("events and Events aliases conflict")
    if not keys:
        return ()
    value = raw[keys[0]]
    if not isinstance(value, list):
        raise AdapterPayloadError(f"{keys[0]} must be an array")
    result: list[EventMetadata] = []
    for item in value:
        if not isinstance(item, dict):
            raise AdapterPayloadError("nested events must be objects")
        result.append(
            EventMetadata(
                event_id=_required_string(item, "id"),
                slug=_optional_string(item, "slug"),
                title=_optional_string(item, "title"),
                source_metadata_json=_canonical_json(item, "nested event"),
            )
        )
    ids = [event.event_id for event in result]
    if len(ids) != len(set(ids)):
        raise AdapterInvariantError("nested event IDs must be unique")
    return tuple(result)


def _normalize_fee_schedule(raw: dict[str, Any]) -> tuple[str | None, str | None]:
    # feeSchedule is current Gamma spelling. fee_schedule remains a strict
    # compatibility alias for previously captured payloads.
    keys = [key for key in ("feeSchedule", "fee_schedule") if key in raw]
    if len(keys) > 1:
        raise AdapterInvariantError("feeSchedule aliases conflict")
    if not keys:
        return None, None
    key = keys[0]
    value = raw[key]
    if value is None:
        return key, "null"
    if not isinstance(value, dict):
        raise AdapterPayloadError(f"{key} must be an object")
    return key, _canonical_json(value, key)


@dataclass(frozen=True)
class GammaDiscovery(Sequence[MarketMetadata]):
    markets: tuple[MarketMetadata, ...]
    diagnostics: tuple[MarketDiagnostic, ...]
    complete: bool = True
    next_cursor: str | None = None
    termination: str = "pagination_exhausted"

    def __getitem__(self, index):
        return self.markets[index]

    def __len__(self) -> int:
        return len(self.markets)

    def __iter__(self) -> Iterator[MarketMetadata]:
        return iter(self.markets)


def _normalize_market(raw: Any) -> MarketMetadata:
    if not isinstance(raw, dict):
        raise AdapterPayloadError("market must be an object")
    outcomes = _string_array(raw, "outcomes")
    token_ids = _string_array(raw, "clobTokenIds")
    if len(outcomes) != len(token_ids):
        raise AdapterInvariantError("outcome and token counts differ")
    normalized = tuple(outcome.strip().upper() for outcome in outcomes)
    if len(normalized) != 2 or set(normalized) != {"YES", "NO"}:
        raise AdapterInvariantError("market is not binary YES/NO")
    if len(set(normalized)) != 2 or len(set(token_ids)) != len(token_ids):
        raise AdapterInvariantError("outcomes and token IDs must be unique")
    fee_source, fee_source_json = _normalize_fee_schedule(raw)
    return MarketMetadata(
        market_id=_required_string(raw, "id"),
        condition_id=_required_string(raw, "conditionId"),
        question=_required_string(raw, "question"),
        slug=_optional_string(raw, "slug"),
        events=_normalize_events(raw),
        tokens=tuple(TokenMetadata(token, outcome) for token, outcome in zip(token_ids, normalized)),  # type: ignore[arg-type]
        active=_optional_bool(raw, "active"),
        closed=_optional_bool(raw, "closed"),
        archived=_optional_bool(raw, "archived"),
        accepting_orders=_optional_bool(raw, "acceptingOrders"),
        enable_order_book=_optional_bool(raw, "enableOrderBook"),
        neg_risk=_optional_bool(raw, "negRisk"),
        end_date=_optional_string(raw, "endDate"),
        fees_enabled=_optional_bool(raw, "feesEnabled"),
        fee_schedule_source_json=fee_source_json,
        fee_schedule_source=fee_source,
        source_metadata_json=_canonical_json(raw, "market"),
    )


class GammaClient:
    """Strict adapter for the public Gamma market keyset endpoint."""

    def __init__(
        self,
        http: httpx.AsyncClient | None = None,
        base_url: str = GAMMA_PUBLIC_ORIGIN,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
        max_response_bytes: int = 8_000_000,
    ) -> None:
        self.base_url = _validate_base_url(base_url, GAMMA_PUBLIC_ORIGIN)
        if isinstance(max_response_bytes, bool) or not isinstance(max_response_bytes, int):
            raise TypeError("max_response_bytes must be an integer")
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")
        if http is not None and transport is not None:
            raise ValueError("transport may only be supplied for an owned HTTP client")
        self._owned = http is None
        self.http = http or httpx.AsyncClient(
            transport=transport, timeout=timeout, trust_env=False
        )
        _reject_credential_headers(self.http)
        self.max_response_bytes = max_response_bytes
        self._closed = False

    async def __aenter__(self) -> GammaClient:
        if self._closed:
            raise RuntimeError("client is closed")
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owned and not self._closed:
            await self.http.aclose()
        self._closed = True

    async def _get_page(self, *, limit: int, cursor: str | None) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("client is closed")
        _reject_credential_headers(self.http)
        params: dict[str, str | int] = {"limit": limit, "closed": "false"}
        if cursor is not None:
            params["after_cursor"] = cursor
        try:
            async with self.http.stream(
                "GET",
                f"{self.base_url}/markets/keyset",
                params=params,
                follow_redirects=False,
            ) as response:
                _clear_response_cookies(self.http, response)
                await response.aread()
        except httpx.HTTPError as exc:
            raise AdapterTransportError("Gamma request failed") from exc
        if not 200 <= response.status_code < 300:
            raise AdapterHTTPError(f"Gamma returned HTTP {response.status_code}")
        if len(response.content) > self.max_response_bytes:
            raise AdapterPayloadError("Gamma response exceeds configured size limit")
        try:
            payload = response.json()
        except (ValueError, UnicodeError) as exc:
            raise AdapterPayloadError("Gamma returned invalid JSON") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("markets"), list):
            raise AdapterPayloadError("Gamma wrapper must contain a markets array")
        next_cursor = payload.get("next_cursor", "")
        if not isinstance(next_cursor, str):
            raise AdapterPayloadError("next_cursor must be a string")
        payload["next_cursor"] = next_cursor
        return payload

    async def active_markets(
        self,
        *,
        limit: int = 100,
        max_pages: int = 100,
        max_markets: int = 10_000,
        allow_partial: bool = False,
    ) -> GammaDiscovery:
        for name, value, upper in (
            ("limit", limit, 100),
            ("max_pages", max_pages, None),
            ("max_markets", max_markets, None),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value < 1 or (upper is not None and value > upper):
                raise ValueError(f"{name} is outside its allowed range")
        if type(allow_partial) is not bool:
            raise TypeError("allow_partial must be bool")

        cursor: str | None = None
        seen: set[str] = set()
        markets: list[MarketMetadata] = []
        diagnostics: list[MarketDiagnostic] = []
        seen_market_ids: set[str] = set()
        seen_condition_ids: set[str] = set()
        seen_token_ids: set[str] = set()
        for _page_number in range(max_pages):
            payload = await self._get_page(limit=limit, cursor=cursor)
            page_markets = payload["markets"]
            room = max_markets - len(markets)
            page_truncated = len(page_markets) > room
            if page_truncated and not allow_partial:
                raise AdapterInvariantError("max_markets bound exceeded")
            page_markets = page_markets[:room]
            for raw in page_markets:
                market_id = raw.get("id") if isinstance(raw, dict) and isinstance(raw.get("id"), str) else None
                try:
                    market = _normalize_market(raw)
                except (AdapterPayloadError, AdapterInvariantError) as exc:
                    diagnostics.append(MarketDiagnostic(market_id, str(exc)))
                    continue
                token_ids = {token.token_id for token in market.tokens}
                if market.market_id in seen_market_ids:
                    raise AdapterInvariantError(
                        f"duplicate market ID {market.market_id}"
                    )
                if market.condition_id in seen_condition_ids:
                    raise AdapterInvariantError(
                        f"duplicate condition ID {market.condition_id}"
                    )
                duplicate_tokens = token_ids & seen_token_ids
                if duplicate_tokens:
                    raise AdapterInvariantError(
                        f"duplicate token ID {sorted(duplicate_tokens)[0]}"
                    )
                seen_market_ids.add(market.market_id)
                seen_condition_ids.add(market.condition_id)
                seen_token_ids.update(token_ids)
                if not market.is_tradeable:
                    diagnostics.append(
                        MarketDiagnostic(market.market_id, "market is not tradeable")
                    )
                else:
                    markets.append(market)
            next_cursor = payload["next_cursor"]
            if not next_cursor:
                return GammaDiscovery(tuple(markets), tuple(diagnostics))
            if page_truncated or len(markets) >= max_markets:
                diagnostics.append(
                    MarketDiagnostic(None, "catalog truncated by max_markets")
                )
                return GammaDiscovery(
                    tuple(markets), tuple(diagnostics), False,
                    next_cursor, "max_markets",
                )
            if next_cursor == cursor or next_cursor in seen:
                raise AdapterInvariantError("keyset cursor repeated without progress")
            seen.add(next_cursor)
            cursor = next_cursor
        if allow_partial:
            diagnostics.append(
                MarketDiagnostic(None, "catalog truncated by max_pages")
            )
            return GammaDiscovery(
                tuple(markets), tuple(diagnostics), False,
                cursor, "max_pages",
            )
        raise AdapterInvariantError("max_pages bound reached before pagination completed")


__all__ = [
    "EventMetadata",
    "GammaClient",
    "GammaDiscovery",
    "MarketDiagnostic",
    "MarketMetadata",
    "TokenMetadata",
]
