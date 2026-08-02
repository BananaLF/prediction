"""Canonical Decimal parsing and encoding."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
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


def decode_decimal(value: object) -> Decimal:
    """Decode a Decimal value, normalizing legacy database spellings.

    New persistence writes must use :func:`encode_decimal`. This boundary also
    accepts older strings that used exponent notation and SQLite scalar values
    encountered in pre-canonical databases, but always returns an exact
    ``Decimal`` value and rejects non-finite inputs.
    """

    if isinstance(value, bool):
        raise ValueError("boolean is not a Decimal value")
    if isinstance(value, Decimal):
        decoded = value
    elif isinstance(value, str):
        try:
            decoded = Decimal(value)
        except (InvalidOperation, ValueError) as error:
            raise ValueError("value must be a valid Decimal") from error
    elif isinstance(value, int):
        decoded = Decimal(value)
    elif isinstance(value, float):
        if not value.is_integer() and not value == value:
            # Keep the error path explicit for NaN while avoiding Decimal(float)
            # binary expansion for finite compatibility values.
            raise ValueError("value must be a finite Decimal")
        try:
            decoded = Decimal(str(value))
        except (InvalidOperation, ValueError) as error:
            raise ValueError("value must be a valid Decimal") from error
    else:
        raise ValueError("value must be a supported Decimal representation")

    if not decoded.is_finite():
        raise ValueError("value must be a finite Decimal")
    return decoded


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
