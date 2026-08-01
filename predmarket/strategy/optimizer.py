"""Exact Decimal L2 walking and breakpoint quantity optimization."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Generic, Literal, TypeVar

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.strategy.decimal_context import (
    MAX_LEVELS_PER_BOOK,
    MAX_OPTIMIZER_CANDIDATES,
    MAX_STRATEGY_LEGS,
    StrategyNumericLimitError,
    bounded_sequence,
    isolated_decimal_context,
)


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


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class QuantityCandidate(Generic[T]):
    """A complete, immutable strategy evaluation at one exact quantity."""

    evaluation: T
    quantity: Decimal
    total_capital: Decimal
    expected_profit: Decimal
    constraint_margins: tuple[tuple[str, Decimal], ...]
    feasible: bool

    def __post_init__(self) -> None:
        _positive_decimal(self.quantity, "quantity")
        _positive_decimal(self.total_capital, "total_capital")
        _finite_decimal(self.expected_profit, "expected_profit")
        if type(self.feasible) is not bool:
            raise ValueError("feasible must be a bool")
        margins = bounded_sequence(
            self.constraint_margins,
            field_name="constraint_margins",
            max_items=MAX_STRATEGY_LEGS * 2 + 3,
        )
        if not margins:
            raise ValueError("constraint_margins must not be empty")
        normalized: list[tuple[str, Decimal]] = []
        for item in margins:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
                or not item[0]
            ):
                raise ValueError("constraint margins must be name/Decimal pairs")
            _finite_decimal(item[1], "constraint margin")
            normalized.append(item)
        normalized.sort(key=lambda item: item[0].encode("utf-8"))
        names = [item[0] for item in normalized]
        if len(names) != len(set(names)):
            raise ValueError("constraint margin names must be unique")
        object.__setattr__(self, "constraint_margins", tuple(normalized))


@dataclass(frozen=True, slots=True)
class CandidateSelection(Generic[T]):
    candidate: T
    feasible: bool
    candidates: tuple[T, ...]


def constraint_root_quantities(
    candidates: Sequence[QuantityCandidate[object]],
) -> tuple[Decimal, ...]:
    """Derive hard-constraint roots from already-computed candidate data."""

    materialized = _quantity_candidates(candidates)
    return _constraint_root_quantities(materialized)


@isolated_decimal_context(operation_depth=12)
def _constraint_root_quantities(
    candidates: tuple[QuantityCandidate[object], ...],
) -> tuple[Decimal, ...]:
    ordered = tuple(sorted(candidates, key=lambda item: item.quantity))
    names = tuple(name for name, _ in ordered[0].constraint_margins)
    if any(tuple(name for name, _ in item.constraint_margins) != names for item in ordered):
        raise ValueError("constraint margin names must be stable")
    roots: set[Decimal] = set()
    for lower, upper in zip(ordered, ordered[1:]):
        lower_margins = dict(lower.constraint_margins)
        upper_margins = dict(upper.constraint_margins)
        for name in names:
            lower_margin = lower_margins[name]
            upper_margin = upper_margins[name]
            if lower_margin == upper_margin or lower_margin * upper_margin > 0:
                continue
            exact_fraction = Fraction(lower.quantity) + (
                -Fraction(lower_margin)
                * (Fraction(upper.quantity) - Fraction(lower.quantity))
                / (Fraction(upper_margin) - Fraction(lower_margin))
            )
            exact_root = _terminating_decimal(exact_fraction)
            root = exact_root or (
                lower.quantity
                + (-lower_margin)
                * (upper.quantity - lower.quantity)
                / (upper_margin - lower_margin)
            )
            if not lower.quantity < root < upper.quantity:
                continue
            roots.add(root)
            if exact_root is not None:
                _candidate_count(len(ordered) + len(roots))
                continue
            if lower_margin <= 0 < upper_margin:
                roots.add(root.next_minus())
            elif upper_margin <= 0 < lower_margin:
                roots.add(root.next_plus())
            _candidate_count(len(ordered) + len(roots))
    return tuple(sorted(roots))


def select_candidates(
    candidates: Sequence[QuantityCandidate[T]],
) -> CandidateSelection[T] | None:
    """Select maximum profit from complete candidate data without callbacks."""

    materialized = _quantity_candidates(candidates)
    return _select_candidates(materialized)


@isolated_decimal_context(operation_depth=4)
def _select_candidates(
    candidates: tuple[QuantityCandidate[T], ...],
) -> CandidateSelection[T] | None:
    if not candidates:
        return None
    quantities = [item.quantity for item in candidates]
    if len(quantities) != len(set(quantities)):
        raise ValueError("candidate quantities must be unique")
    feasible = tuple(item for item in candidates if item.feasible)
    pool = feasible or candidates
    selected = max(
        pool,
        key=lambda item: (
            item.expected_profit,
            -item.total_capital,
            -item.quantity,
        ),
    )
    ordered = tuple(sorted(candidates, key=lambda item: item.quantity))
    return CandidateSelection(
        selected.evaluation,
        bool(feasible),
        tuple(item.evaluation for item in ordered),
    )


def walk_depth(
    levels: Sequence[OrderBookLevel],
    quantity: Decimal,
) -> DepthFill:
    """Fill exactly ``quantity`` in the supplied best-to-worst level order."""

    materialized = bounded_sequence(
        levels,
        field_name="levels",
        max_items=MAX_LEVELS_PER_BOOK,
    )
    return _walk_depth(materialized, quantity)


@isolated_decimal_context(operation_depth=8)
def _walk_depth(
    levels: tuple[OrderBookLevel, ...],
    quantity: Decimal,
) -> DepthFill:

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
    return _breakpoint_quantities(
        materialized,
        minimum_quantity=minimum_quantity,
        default_quantity=default_quantity,
    )


def candidate_quantities(
    requirements: Sequence[DepthRequirement],
    *,
    minimum_quantity: Decimal,
    default_quantity: Decimal = Decimal("1"),
    extra_breakpoints: Sequence[Decimal] = (),
) -> tuple[Decimal, ...]:
    """Return bounded base quantities without evaluating strategy callbacks."""

    materialized = _requirements(requirements)
    extras = bounded_sequence(
        extra_breakpoints,
        field_name="extra_breakpoints",
        max_items=MAX_OPTIMIZER_CANDIDATES,
    )
    return _candidate_quantities(
        materialized,
        minimum_quantity=minimum_quantity,
        default_quantity=default_quantity,
        extra_breakpoints=extras,
    )


@isolated_decimal_context(operation_depth=8)
def _candidate_quantities(
    requirements: tuple[DepthRequirement, ...],
    *,
    minimum_quantity: Decimal,
    default_quantity: Decimal,
    extra_breakpoints: tuple[Decimal, ...],
) -> tuple[Decimal, ...]:
    base = set(
        _breakpoint_quantities(
            requirements,
            minimum_quantity=minimum_quantity,
            default_quantity=default_quantity,
        )
    )
    _candidate_count(len(base))
    if not base:
        return ()
    lower_bound = min(base)
    upper_bound = max(base)
    for value in extra_breakpoints:
        _positive_decimal(value, "extra breakpoint")
        if lower_bound <= value <= upper_bound:
            base.add(value)
            _candidate_count(len(base))
    return tuple(sorted(base))


@isolated_decimal_context(operation_depth=8)
def _breakpoint_quantities(
    requirements: tuple[DepthRequirement, ...],
    *,
    minimum_quantity: Decimal,
    default_quantity: Decimal,
) -> tuple[Decimal, ...]:
    materialized = requirements
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


def _requirements(
    values: Sequence[DepthRequirement],
) -> tuple[DepthRequirement, ...]:
    materialized = bounded_sequence(
        values,
        field_name="requirements",
        max_items=MAX_STRATEGY_LEGS,
    )
    if not materialized or any(
        not isinstance(value, DepthRequirement) for value in materialized
    ):
        raise ValueError("requirements must contain DepthRequirement values")
    identities = [(item.book.token_id, item.side) for item in materialized]
    if len(identities) != len(set(identities)):
        raise ValueError("depth requirements must be unique")
    return materialized


def _quantity_candidates(
    values: Sequence[QuantityCandidate[T]],
) -> tuple[QuantityCandidate[T], ...]:
    materialized = bounded_sequence(
        values,
        field_name="candidates",
        max_items=MAX_OPTIMIZER_CANDIDATES,
    )
    if any(not isinstance(value, QuantityCandidate) for value in materialized):
        raise ValueError("candidates must contain QuantityCandidate values")
    return materialized


def _candidate_count(count: int) -> None:
    if count > MAX_OPTIMIZER_CANDIDATES:
        raise StrategyNumericLimitError(
            "optimizer candidate count exceeds the strategy numeric limit"
        )


def _terminating_decimal(value: Fraction) -> Decimal | None:
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        return None
    scale = max(twos, fives)
    coefficient = abs(value.numerator)
    coefficient *= 2 ** (scale - twos)
    coefficient *= 5 ** (scale - fives)
    digits = tuple(int(character) for character in str(coefficient))
    return Decimal((1 if value.numerator < 0 else 0, digits, -scale))


def _positive_decimal(value: object, field_name: str) -> None:
    _finite_decimal(value, field_name)
    if value <= 0:  # type: ignore[operator]
        raise ValueError(f"{field_name} must be greater than zero")


def _finite_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")
