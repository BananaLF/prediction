"""Data-derived Decimal context isolation for public strategy calculations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextvars import ContextVar
from dataclasses import fields, is_dataclass
from decimal import (
    Context,
    Decimal,
    MAX_EMAX,
    MAX_PREC,
    MIN_EMIN,
    ROUND_HALF_EVEN,
    localcontext,
)
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar, cast

from predmarket.domain.orderbook import OrderBook


P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
_ACTIVE: ContextVar[bool] = ContextVar("strategy_decimal_context_active", default=False)


# Strategy arithmetic accepts values far beyond current exchange precision while
# retaining a finite, auditable CPU/memory envelope.
MAX_DECIMAL_COEFFICIENT_DIGITS = 128
MAX_DECIMAL_SCALE = 384
MIN_DECIMAL_ADJUSTED_EXPONENT = -384
MAX_DECIMAL_ADJUSTED_EXPONENT = 384
MAX_DECIMAL_INPUTS = 50_000
MAX_COLLECTION_ITEMS = 20_000
MAX_LEVELS_PER_BOOK = 2_000
MAX_TOTAL_BOOK_LEVELS = 10_000
MAX_STRATEGY_LEGS = 64
MAX_FAILURE_SCENARIOS = 64
MAX_OPTIMIZER_CANDIDATES = 20_000
MAX_CONTEXT_PRECISION = 16_384
MAX_CONTEXT_EXPONENT = 32_768


class StrategyNumericLimitError(ValueError):
    """Raised before arithmetic would exceed the strategy resource policy."""


def validate_strategy_decimal(
    value: object,
    *,
    field_name: str,
) -> Decimal:
    """Validate one Decimal against the shared strategy numeric policy."""

    if not isinstance(value, Decimal) or not value.is_finite():
        raise StrategyNumericLimitError(f"{field_name} must be a finite Decimal")
    digits = len(value.as_tuple().digits)
    exponent = value.as_tuple().exponent
    adjusted = value.adjusted()
    scale = max(0, -exponent)
    if digits > MAX_DECIMAL_COEFFICIENT_DIGITS:
        raise StrategyNumericLimitError(
            f"{field_name} coefficient exceeds the strategy numeric limit"
        )
    if scale > MAX_DECIMAL_SCALE:
        raise StrategyNumericLimitError(
            f"{field_name} scale exceeds the strategy numeric limit"
        )
    if not (
        MIN_DECIMAL_ADJUSTED_EXPONENT
        <= adjusted
        <= MAX_DECIMAL_ADJUSTED_EXPONENT
    ):
        raise StrategyNumericLimitError(
            f"{field_name} adjusted exponent exceeds the strategy numeric limit"
        )
    return value


def bounded_sequence(
    values: object,
    *,
    field_name: str,
    max_items: int,
) -> tuple[T, ...]:
    """Materialize a genuine finite Sequence without consuming iterators."""

    if (
        isinstance(values, (str, bytes, bytearray, memoryview))
        or not isinstance(values, Sequence)
    ):
        raise ValueError(f"{field_name} must be a bounded Sequence")
    try:
        width = len(values)
    except (OverflowError, TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be a bounded Sequence") from error
    if width > max_items:
        raise StrategyNumericLimitError(
            f"{field_name} exceeds the numeric limit of {max_items} items"
        )
    try:
        return tuple(cast(Sequence[T], values)[index] for index in range(width))
    except (IndexError, KeyError, TypeError) as error:
        raise ValueError(f"{field_name} must be a stable bounded Sequence") from error


class _Envelope:
    def __init__(self) -> None:
        self.decimal_count = 0
        self.max_digits = 1
        self.min_exponent = 0
        self.min_adjusted = 0
        self.max_adjusted = 0
        self.collection_width = 1
        self.book_count = 0
        self.total_book_levels = 0
        self._seen: set[int] = set()

    def add(self, value: object) -> None:
        if isinstance(value, Decimal):
            validate_strategy_decimal(value, field_name="strategy Decimal input")
            digits = len(value.as_tuple().digits)
            exponent = value.as_tuple().exponent
            adjusted = value.adjusted()
            self.decimal_count += 1
            if self.decimal_count > MAX_DECIMAL_INPUTS:
                raise StrategyNumericLimitError(
                    "Decimal input count exceeds the strategy numeric limit"
                )
            self.max_digits = max(self.max_digits, digits)
            self.min_exponent = min(self.min_exponent, exponent)
            self.min_adjusted = min(self.min_adjusted, adjusted)
            self.max_adjusted = max(self.max_adjusted, adjusted)
            return
        if value is None or isinstance(value, (str, bytes, int, float, bool, type)):
            return
        # Callable internals are deliberately opaque. Public optimizer APIs must
        # declare arithmetic dependencies explicitly instead of reflecting
        # globals, closures, bound objects, or arbitrary attributes.
        if callable(value):
            return
        identity = id(value)
        if identity in self._seen:
            return
        self._seen.add(identity)
        if isinstance(value, OrderBook):
            self.book_count += 1
            if self.book_count > MAX_STRATEGY_LEGS:
                raise StrategyNumericLimitError(
                    "strategy books exceed the numeric leg limit"
                )
            bid_count = len(value.bids)
            ask_count = len(value.asks)
            if bid_count > MAX_LEVELS_PER_BOOK or ask_count > MAX_LEVELS_PER_BOOK:
                raise StrategyNumericLimitError(
                    "order-book levels exceed the strategy numeric limit"
                )
            self.total_book_levels += bid_count + ask_count
            if self.total_book_levels > MAX_TOTAL_BOOK_LEVELS:
                raise StrategyNumericLimitError(
                    "total order-book levels exceed the strategy numeric limit"
                )
        if isinstance(value, Mapping):
            width = len(value)
            self._add_collection_width(width)
            for key, item in value.items():
                self.add(key)
                self.add(item)
            return
        if isinstance(value, (tuple, list, set, frozenset)):
            self._add_collection_width(len(value))
            for item in value:
                self.add(item)
            return
        if isinstance(value, Sequence):
            materialized = bounded_sequence(
                value,
                field_name="decimal context input",
                max_items=MAX_COLLECTION_ITEMS,
            )
            self._add_collection_width(len(materialized))
            for item in materialized:
                self.add(item)
            return
        if is_dataclass(value) and not isinstance(value, type):
            for field in fields(value):
                self.add(getattr(value, field.name))

    def _add_collection_width(self, width: int) -> None:
        if width > MAX_COLLECTION_ITEMS:
            raise StrategyNumericLimitError(
                "collection width exceeds the strategy numeric limit"
            )
        self.collection_width = max(self.collection_width, width)

    def context(self, operation_depth: int) -> Context:
        # Each public entry declares a conservative maximum multiplication/division
        # chain. Addition across an input collection needs only log10(width) carry
        # digits; exponent span preserves small terms when unlike magnitudes meet.
        carry_digits = len(str(max(1, self.collection_width)))
        exponent_span = self.max_adjusted - self.min_exponent + 1
        precision = (
            self.max_digits * operation_depth
            + exponent_span
            + carry_digits
            + operation_depth
            + 16
        )
        if precision > min(MAX_PREC, MAX_CONTEXT_PRECISION):
            raise StrategyNumericLimitError(
                "derived Decimal precision exceeds the strategy numeric limit"
            )
        magnitude = max(abs(self.min_adjusted), abs(self.max_adjusted), 1)
        exponent_guard = magnitude * operation_depth + precision
        minimum_exponent = self.min_exponent - exponent_guard
        maximum_exponent = self.max_adjusted + exponent_guard
        if (
            minimum_exponent < max(MIN_EMIN, -MAX_CONTEXT_EXPONENT)
            or maximum_exponent > min(MAX_EMAX, MAX_CONTEXT_EXPONENT)
        ):
            raise StrategyNumericLimitError(
                "derived Decimal exponent range exceeds the strategy numeric limit"
            )
        return Context(
            prec=precision,
            rounding=ROUND_HALF_EVEN,
            Emin=min(minimum_exponent, -1),
            Emax=max(maximum_exponent, 1),
            capitals=1,
            clamp=0,
        )


def isolated_decimal_context(
    *, operation_depth: int
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Run a public calculation in a deterministic input-derived Context."""

    if operation_depth <= 0:
        raise ValueError("operation_depth must be positive")

    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if _ACTIVE.get():
                return function(*args, **kwargs)
            envelope = _Envelope()
            envelope.add(args)
            envelope.add(kwargs)
            envelope.add(getattr(function, "__defaults__", None))
            envelope.add(getattr(function, "__kwdefaults__", None))
            token = _ACTIVE.set(True)
            try:
                with localcontext(envelope.context(operation_depth)):
                    return function(*args, **kwargs)
            finally:
                _ACTIVE.reset(token)

        return wrapped

    return decorate
