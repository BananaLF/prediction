"""Canonical Decimal parsing and encoding."""

from __future__ import annotations

from decimal import Decimal
import re

_PLAIN_DECIMAL_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")


def parse_decimal(text: str) -> Decimal:
    """Parse a canonical, finite, plain-decimal string."""
    if not isinstance(text, str) or _PLAIN_DECIMAL_RE.fullmatch(text) is None:
        raise ValueError("decimal must be a canonical plain-decimal string")
    value = Decimal(text)
    if encode_decimal(value) != text:
        raise ValueError("decimal string is not canonical")
    return value


def encode_decimal(value: Decimal) -> str:
    """Encode a finite Decimal without exponent notation or redundant zeroes."""
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("value must be a finite Decimal")
    if value.is_zero():
        return "0"

    sign, digits, exponent = value.as_tuple()
    coefficient = "".join(str(digit) for digit in digits)
    if exponent >= 0:
        encoded = coefficient + ("0" * exponent)
    else:
        decimal_position = len(coefficient) + exponent
        if decimal_position <= 0:
            encoded = "0." + ("0" * -decimal_position) + coefficient
        else:
            encoded = coefficient[:decimal_position] + "." + coefficient[decimal_position:]
        encoded = encoded.rstrip("0").rstrip(".")

    return ("-" if sign else "") + encoded
