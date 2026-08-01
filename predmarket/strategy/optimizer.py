"""Exact Decimal L2 walking and breakpoint quantity optimization."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from predmarket.domain.orderbook import OrderBook, OrderBookLevel


Side = Literal["BUY", "SELL"]


class InsufficientDepth(ValueError):
    """Raised when a complete leg cannot be filled by visible L2 depth."""

    def __init__(self, *, requested: Decimal, available: Decimal) -> None:
        self.requested = requested
        self.available = available
        super().__init__(f"requested {requested} but only {available} is available")


@dataclass(frozen=True, slots=True)
class DepthFill:
    quantity: Decimal
    gross_amount: Decimal
    average_price: Decimal
    worst_price: Decimal


@dataclass(frozen=True, slots=True)
class DepthRequirement:
    book: OrderBook
    side: Side

    def __post_init__(self) -> None:
        if not isinstance(self.book, OrderBook):
            raise ValueError("book must be an OrderBook")
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")

    @property
    def levels(self) -> tuple[OrderBookLevel, ...]:
        return self.book.asks if self.side == "BUY" else self.book.bids


Evaluation = Callable[[Decimal], tuple[Decimal, Decimal]]


def walk_depth(
    levels: Sequence[OrderBookLevel],
    quantity: Decimal,
) -> DepthFill:
    """Fill exactly ``quantity`` in the supplied best-to-worst level order."""

    _positive_decimal(quantity, "quantity")
    remaining = quantity
    gross = Decimal("0")
    worst_price: Decimal | None = None
    available = Decimal("0")
    for level in levels:
        if not isinstance(level, OrderBookLevel):
            raise ValueError("levels must contain OrderBookLevel values")
        available += level.size
        if remaining <= 0:
            continue
        taken = min(remaining, level.size)
        gross += taken * level.price
        remaining -= taken
        if taken > 0:
            worst_price = level.price
    if remaining > 0:
        raise InsufficientDepth(requested=quantity, available=available)
    assert worst_price is not None
    return DepthFill(
        quantity=quantity,
        gross_amount=gross,
        average_price=gross / quantity,
        worst_price=worst_price,
    )


def breakpoint_quantities(
    requirements: Sequence[DepthRequirement],
    *,
    minimum_quantity: Decimal,
    default_quantity: Decimal = Decimal("1"),
) -> tuple[Decimal, ...]:
    """Return executable minimum/default/L2 boundary quantities."""

    materialized = _requirements(requirements)
    _positive_decimal(minimum_quantity, "minimum_quantity")
    _positive_decimal(default_quantity, "default_quantity")
    effective_minimum = max(
        minimum_quantity,
        *(requirement.book.minimum_order_size for requirement in materialized),
    )
    maximum_quantity = min(
        sum((level.size for level in requirement.levels), Decimal("0"))
        for requirement in materialized
    )
    if effective_minimum > maximum_quantity:
        return ()

    candidates = {effective_minimum, maximum_quantity}
    if effective_minimum <= default_quantity <= maximum_quantity:
        candidates.add(default_quantity)
    for requirement in materialized:
        cumulative = Decimal("0")
        for level in requirement.levels:
            cumulative += level.size
            if effective_minimum <= cumulative <= maximum_quantity:
                candidates.add(cumulative)
    return tuple(sorted(candidates))


def optimize_quantity(
    requirements: Sequence[DepthRequirement],
    *,
    minimum_quantity: Decimal,
    bankroll: Decimal,
    evaluate: Evaluation,
    default_quantity: Decimal = Decimal("1"),
) -> Decimal | None:
    """Choose the executable candidate with maximum expected profit.

    Besides L2 breakpoints, this inserts the exact quantity where a monotonic,
    piecewise-linear capital curve crosses the bankroll inside a depth level.
    """

    materialized = _requirements(requirements)
    _positive_decimal(bankroll, "bankroll")
    if not callable(evaluate):
        raise ValueError("evaluate must be callable")
    breakpoints = breakpoint_quantities(
        materialized,
        minimum_quantity=minimum_quantity,
        default_quantity=default_quantity,
    )
    evaluated: dict[Decimal, tuple[Decimal, Decimal]] = {}
    for quantity in breakpoints:
        evaluated[quantity] = _evaluation(evaluate(quantity))

    for lower, upper in zip(breakpoints, breakpoints[1:]):
        lower_capital = evaluated[lower][0]
        upper_capital = evaluated[upper][0]
        if lower_capital <= bankroll < upper_capital:
            if upper_capital <= lower_capital:
                continue
            boundary = lower + (
                (bankroll - lower_capital)
                * (upper - lower)
                / (upper_capital - lower_capital)
            )
            if lower < boundary < upper:
                evaluated[boundary] = _evaluation(evaluate(boundary))

    feasible = [
        (quantity, capital, profit)
        for quantity, (capital, profit) in evaluated.items()
        if capital <= bankroll
    ]
    if not feasible:
        return None
    # Lower capital then lower quantity wins an exact-profit tie.
    return max(feasible, key=lambda item: (item[2], -item[1], -item[0]))[0]


def _requirements(
    values: Sequence[DepthRequirement],
) -> tuple[DepthRequirement, ...]:
    materialized = tuple(values)
    if not materialized or any(
        not isinstance(value, DepthRequirement) for value in materialized
    ):
        raise ValueError("requirements must contain DepthRequirement values")
    identities = [(item.book.token_id, item.side) for item in materialized]
    if len(identities) != len(set(identities)):
        raise ValueError("depth requirements must be unique")
    return materialized


def _evaluation(value: object) -> tuple[Decimal, Decimal]:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError("evaluate must return (total_capital, expected_profit)")
    capital, profit = value
    _positive_decimal(capital, "total_capital")
    _finite_decimal(profit, "expected_profit")
    return capital, profit


def _positive_decimal(value: object, field_name: str) -> None:
    _finite_decimal(value, field_name)
    if value <= 0:  # type: ignore[operator]
        raise ValueError(f"{field_name} must be greater than zero")


def _finite_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")
