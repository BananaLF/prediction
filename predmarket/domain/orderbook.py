"""Immutable, canonical L2 order-book contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class OrderBookLevel:
    price: Decimal
    size: Decimal

    def __post_init__(self) -> None:
        _finite_decimal(self.price, "price")
        _finite_decimal(self.size, "size")
        if not Decimal("0") < self.price < Decimal("1"):
            raise ValueError("price must be in (0, 1)")
        if self.size <= Decimal("0"):
            raise ValueError("size must be greater than zero")


@dataclass(frozen=True)
class OrderBook:
    market_id: str
    token_id: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    subscription_generation: int
    book_hash: str
    exchange_timestamp: int
    received_timestamp: int
    tick_size: Decimal
    minimum_order_size: Decimal

    def __post_init__(self) -> None:
        _identifier(self.market_id, "market_id")
        _identifier(self.token_id, "token_id")
        _identifier(self.book_hash, "book_hash")
        if type(self.subscription_generation) is not int or self.subscription_generation < 1:
            raise ValueError("subscription_generation must be at least one")
        for name, value in (
            ("exchange_timestamp", self.exchange_timestamp),
            ("received_timestamp", self.received_timestamp),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        _finite_decimal(self.tick_size, "tick_size")
        _finite_decimal(self.minimum_order_size, "minimum_order_size")
        if not Decimal("0") < self.tick_size <= Decimal("1"):
            raise ValueError("tick_size must be in (0, 1]")
        if self.minimum_order_size <= Decimal("0"):
            raise ValueError("minimum_order_size must be greater than zero")

        bids = _levels(self.bids, "bids", reverse=True)
        asks = _levels(self.asks, "asks", reverse=False)
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)


def _levels(
    values: tuple[OrderBookLevel, ...], field_name: str, *, reverse: bool
) -> tuple[OrderBookLevel, ...]:
    try:
        levels = tuple(values)
    except TypeError as error:
        raise ValueError(f"{field_name} must be an iterable of levels") from error
    if any(not isinstance(level, OrderBookLevel) for level in levels):
        raise ValueError(f"{field_name} must contain OrderBookLevel values")
    prices = [level.price for level in levels]
    if len(prices) != len(set(prices)):
        raise ValueError(f"{field_name} must not contain duplicate prices")
    return tuple(sorted(levels, key=lambda level: level.price, reverse=reverse))


def _identifier(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")


def _finite_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")
