"""The sole boundary for Polymarket SDK imports."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
import time
from typing import Any

from polymarket import AsyncPublicClient
from polymarket.streams import MarketSpec

from predmarket.domain.decimal import encode_decimal
from predmarket.domain.fees import FeeModel, FeeSchedule
from predmarket.domain.json import freeze_json_object
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel


MAPPING_VERSION = "polymarket-client-0.3.0b1:v1"


class GatewayMappingError(ValueError):
    """The SDK returned an entity that cannot satisfy the domain contract."""


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
        if type(self.subscription_generation) is not int or self.subscription_generation < 1:
            raise ValueError("subscription_generation must be at least one")
        _require_string(self.mapping_version, "mapping_version")
        object.__setattr__(
            self,
            "payload",
            freeze_json_object(self.payload, field_name="stream payload"),
        )


class MarketSubscription(AsyncIterator[MarketStreamEvent]):
    def __init__(
        self,
        handle: Any,
        *,
        mapper: Callable[[Any], MarketStreamEvent],
    ) -> None:
        self._handle = handle
        self._iterator = handle.__aiter__()
        self._mapper = mapper
        self._closed = False

    def __aiter__(self) -> "MarketSubscription":
        return self

    async def __anext__(self) -> MarketStreamEvent:
        if self._closed:
            raise StopAsyncIteration
        return self._mapper(await self._iterator.__anext__())

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            await self._handle.close()


class PolymarketGateway:
    def __init__(
        self,
        client: Any | None = None,
        *,
        clock_ms: Callable[[], int] | None = None,
        page_size: int = 100,
    ) -> None:
        if type(page_size) is not int or page_size < 1:
            raise ValueError("page_size must be a positive integer")
        self._client = client if client is not None else AsyncPublicClient()
        self._clock_ms = clock_ms or _system_clock_ms
        self._page_size = page_size
        self._sync_counter = 0
        self._sync_generation: str | None = None
        self._subscription_generation = 0
        self._market_id_by_condition_id: dict[str, str] = {}
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
                if snapshot.market.active:
                    self._remember_market(snapshot.market)
                    snapshots.append(snapshot)
        return tuple(snapshots)

    async def get_order_books(self, token_ids: Sequence[str]) -> tuple[OrderBook, ...]:
        requested = _token_ids(token_ids)
        received_at = self._now()
        sdk_books = await self._client.get_order_books(token_ids=requested)
        generation = max(1, self._subscription_generation)
        mapped: dict[str, OrderBook] = {}
        for sdk_book in sdk_books:
            token_id = _entity_identifier(sdk_book, "token_id", fallback="unknown")
            if token_id in mapped:
                raise GatewayMappingError(f"order books contain duplicate token {token_id}")
            book = self._map_order_book(
                sdk_book,
                received_at=received_at,
                subscription_generation=generation,
            )
            mapped[book.token_id] = book

        requested_set = set(requested)
        returned_set = set(mapped)
        unexpected = returned_set - requested_set
        if unexpected:
            joined = ", ".join(sorted(unexpected))
            raise GatewayMappingError(f"order books contain unexpected tokens: {joined}")
        missing = requested_set - returned_set
        if missing:
            joined = ", ".join(sorted(missing))
            raise GatewayMappingError(f"order books are missing requested tokens: {joined}")
        return tuple(mapped[token_id] for token_id in requested)

    async def subscribe_markets(self, token_ids: Sequence[str]) -> MarketSubscription:
        normalized = _token_ids(token_ids)
        self._subscription_generation += 1
        generation = self._subscription_generation
        handle = await self._client.subscribe(
            MarketSpec(
                token_ids=normalized,
                custom_feature_enabled=True,
            )
        )
        return MarketSubscription(
            handle,
            mapper=lambda event: self._map_stream_event(
                event,
                subscription_generation=generation,
            ),
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
                f"market refresh requested {market_id} but SDK returned {snapshot.market.id}"
            )
        self._remember_market(snapshot.market)
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

    def _remember_market(self, market: Market) -> None:
        existing = self._market_id_by_condition_id.get(market.condition_id)
        if existing is not None and existing != market.id:
            raise GatewayMappingError(
                f"condition {market.condition_id} maps to both {existing} and {market.id}"
            )
        self._market_id_by_condition_id[market.condition_id] = market.id

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
            try:
                market_id = self._market_id_by_condition_id[condition_id]
            except KeyError as error:
                raise GatewayMappingError(
                    f"condition {condition_id} has no mapped SDK market id"
                ) from error
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
    ) -> MarketStreamEvent:
        event_type = _entity_identifier(sdk_event, "type", fallback="unknown")
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
            status=_market_status(sdk_market),
            active=getattr(state, "active") is True,
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
    if getattr(state, "archived") is True:
        return MarketStatus.ARCHIVED
    if getattr(state, "closed") is True:
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
    if getattr(state, "active") is True:
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
