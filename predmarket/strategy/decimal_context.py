"""Data-derived Decimal context isolation for public strategy calculations."""

from __future__ import annotations

from collections.abc import Mapping
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
from math import ceil, log10
from typing import Any, Callable, ParamSpec, TypeVar


P = ParamSpec("P")
R = TypeVar("R")
_ACTIVE: ContextVar[bool] = ContextVar("strategy_decimal_context_active", default=False)


class _Envelope:
    def __init__(self) -> None:
        self.decimal_count = 0
        self.max_digits = 1
        self.min_exponent = 0
        self.min_adjusted = 0
        self.max_adjusted = 0
        self.collection_width = 1
        self._seen: set[int] = set()

    def add(self, value: object) -> None:
        if isinstance(value, Decimal):
            if value.is_finite():
                digits = len(value.as_tuple().digits)
                exponent = value.as_tuple().exponent
                adjusted = value.adjusted()
                self.decimal_count += 1
                self.max_digits = max(self.max_digits, digits)
                self.min_exponent = min(self.min_exponent, exponent)
                self.min_adjusted = min(self.min_adjusted, adjusted)
                self.max_adjusted = max(self.max_adjusted, adjusted)
            return
        if value is None or isinstance(value, (str, bytes, int, float, bool, type)):
            return
        identity = id(value)
        if identity in self._seen:
            return
        self._seen.add(identity)
        if isinstance(value, Mapping):
            self.collection_width = max(self.collection_width, len(value))
            for key, item in value.items():
                self.add(key)
                self.add(item)
            return
        if isinstance(value, (tuple, list, set, frozenset)):
            self.collection_width = max(self.collection_width, len(value))
            for item in value:
                self.add(item)
            return
        if is_dataclass(value) and not isinstance(value, type):
            for field in fields(value):
                self.add(getattr(value, field.name))
            return
        closure = getattr(value, "__closure__", None)
        if closure:
            for cell in closure:
                try:
                    self.add(cell.cell_contents)
                except ValueError:  # empty closure cell
                    continue
        bound_self = getattr(value, "__self__", None)
        if bound_self is not None and bound_self is not value:
            self.add(bound_self)

    def context(self, operation_depth: int) -> Context:
        # Each public entry declares a conservative maximum multiplication/division
        # chain. Addition across an input collection needs only log10(width) carry
        # digits; exponent span preserves small terms when unlike magnitudes meet.
        carry_digits = ceil(log10(self.collection_width + 1))
        exponent_span = self.max_adjusted - self.min_exponent + 1
        precision = min(
            MAX_PREC,
            self.max_digits * operation_depth
            + exponent_span
            + carry_digits
            + operation_depth
            + 16,
        )
        magnitude = max(abs(self.min_adjusted), abs(self.max_adjusted), 1)
        exponent_guard = magnitude * operation_depth + precision
        return Context(
            prec=precision,
            rounding=ROUND_HALF_EVEN,
            Emin=max(MIN_EMIN, min(self.min_exponent - exponent_guard, -1)),
            Emax=min(MAX_EMAX, max(self.max_adjusted + exponent_guard, 1)),
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
