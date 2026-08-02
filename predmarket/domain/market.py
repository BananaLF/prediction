"""Immutable event, market, and token contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from predmarket.domain.fees import FeeSchedule
from predmarket.domain.json import freeze_json_object


class MarketStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    RESOLVED = "RESOLVED"
    ARCHIVED = "ARCHIVED"


@dataclass(frozen=True)
class Event:
    id: str
    title: str
    status: MarketStatus
    market_ids: tuple[str, ...]
    sync_generation: str
    sync_generation_complete: bool
    slug: str | None = None
    description: str | None = None
    neg_risk: bool = False
    neg_risk_id: str | None = None
    neg_risk_type: str | None = None
    neg_risk_complete: bool = False
    neg_risk_conversion_supported: bool = False
    neg_risk_metadata: Mapping[str, Any] | None = None
    neg_risk_synced_at: int | None = None
    start_at: int | None = None
    end_at: int | None = None
    resolved_at: int | None = None
    source_updated_at: int | None = None
    created_at: int = 0
    updated_at: int = 0

    def __post_init__(self) -> None:
        _identifier(self.id, "event id")
        _nonempty(self.title, "event title")
        _enum(self.status, MarketStatus, "event status")
        _identifier(self.sync_generation, "sync_generation")
        _boolean(self.sync_generation_complete, "sync_generation_complete")
        _boolean(self.neg_risk, "neg_risk")
        _boolean(self.neg_risk_complete, "neg_risk_complete")
        _boolean(self.neg_risk_conversion_supported, "neg_risk_conversion_supported")
        market_ids = _canonical_ids(self.market_ids, "market_ids")
        object.__setattr__(self, "market_ids", market_ids)
        if self.neg_risk_metadata is not None:
            if not isinstance(self.neg_risk_metadata, Mapping):
                raise ValueError("neg_risk_metadata must be a mapping")
            object.__setattr__(
                self,
                "neg_risk_metadata",
                freeze_json_object(
                    self.neg_risk_metadata,
                    field_name="neg_risk_metadata",
                ),
            )
        _timestamps(
            self.neg_risk_synced_at,
            self.start_at,
            self.end_at,
            self.resolved_at,
            self.source_updated_at,
            self.created_at,
            self.updated_at,
        )


@dataclass(frozen=True)
class Market:
    id: str
    event_id: str
    condition_id: str
    question: str
    status: MarketStatus
    active: bool
    accepting_orders: bool
    enable_orderbook: bool
    sync_generation: str
    sync_generation_complete: bool
    slug: str | None = None
    description: str | None = None
    neg_risk: bool = False
    neg_risk_outcome_position: int | None = None
    neg_risk_member_complete: bool = False
    tick_size: Decimal | None = None
    minimum_order_size: Decimal | None = None
    end_at: int | None = None
    resolved_at: int | None = None
    source_updated_at: int | None = None
    created_at: int = 0
    updated_at: int = 0

    def __post_init__(self) -> None:
        _identifier(self.id, "market id")
        _identifier(self.event_id, "event id")
        _identifier(self.condition_id, "condition id")
        _nonempty(self.question, "market question")
        _enum(self.status, MarketStatus, "market status")
        _boolean(self.active, "active")
        _boolean(self.accepting_orders, "accepting_orders")
        _boolean(self.enable_orderbook, "enable_orderbook")
        _boolean(self.neg_risk, "neg_risk")
        _boolean(self.neg_risk_member_complete, "neg_risk_member_complete")
        _identifier(self.sync_generation, "sync_generation")
        _boolean(self.sync_generation_complete, "sync_generation_complete")
        if self.neg_risk_outcome_position is not None and (
            type(self.neg_risk_outcome_position) is not int
            or self.neg_risk_outcome_position < 0
        ):
            raise ValueError("neg_risk_outcome_position must be non-negative")
        if self.tick_size is not None:
            _finite_decimal(self.tick_size, "tick_size")
            if not Decimal("0") < self.tick_size <= Decimal("1"):
                raise ValueError("tick_size must be in (0, 1]")
        if self.minimum_order_size is not None:
            _finite_decimal(self.minimum_order_size, "minimum_order_size")
            if self.minimum_order_size <= Decimal("0"):
                raise ValueError("minimum_order_size must be greater than zero")
        _timestamps(
            self.end_at,
            self.resolved_at,
            self.source_updated_at,
            self.created_at,
            self.updated_at,
        )


@dataclass(frozen=True)
class Token:
    id: str
    market_id: str
    outcome: str
    position: int
    sync_generation: str
    sync_generation_complete: bool
    fee_schedule: FeeSchedule | None = None
    fee_updated_at: int | None = None
    created_at: int = 0
    updated_at: int = 0

    def __post_init__(self) -> None:
        _identifier(self.id, "token id")
        _identifier(self.market_id, "market id")
        _nonempty(self.outcome, "token outcome")
        if type(self.position) is not int or self.position < 0:
            raise ValueError("token position must be non-negative")
        _identifier(self.sync_generation, "sync_generation")
        _boolean(self.sync_generation_complete, "sync_generation_complete")
        if self.fee_schedule is not None and not isinstance(self.fee_schedule, FeeSchedule):
            raise ValueError("fee_schedule must be a FeeSchedule")
        _timestamps(self.fee_updated_at, self.created_at, self.updated_at)


def _canonical_ids(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be an iterable of strings")
    try:
        normalized = tuple(values)
    except TypeError as error:
        raise ValueError(f"{field_name} must be an iterable of strings") from error
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    for value in normalized:
        _identifier(value, field_name)
    return tuple(sorted(set(normalized), key=lambda value: value.encode("utf-8")))


def _identifier(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")


def _nonempty(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _enum(value: object, enum_type: type[Enum], field_name: str) -> None:
    if not isinstance(value, enum_type):
        raise ValueError(f"{field_name} must be a {enum_type.__name__}")


def _boolean(value: object, field_name: str) -> None:
    if type(value) is not bool:
        raise ValueError(f"{field_name} must be a boolean")


def _finite_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")


def _timestamps(*values: int | None) -> None:
    if any(value is not None and (type(value) is not int or value < 0) for value in values):
        raise ValueError("timestamps must be non-negative integers")
