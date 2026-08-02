from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from predmarket.domain.decimal import encode_decimal, parse_decimal


@pytest.mark.parametrize(
    "text",
    [
        "0",
        "1",
        "-1",
        "0.25",
        "-0.25",
        "12345678901234567890.123456789",
    ],
)
def test_canonical_decimal_round_trips(text: str) -> None:
    assert encode_decimal(parse_decimal(text)) == text


@pytest.mark.parametrize(
    "value",
    [
        1.0,
        "",
        "NaN",
        "Infinity",
        "-Infinity",
        "1e2",
        "1E+2",
        "+1",
        "-0",
        "01",
        "00.1",
        "1.",
        ".1",
        "1.0",
        "1.2300",
    ],
)
def test_parse_decimal_rejects_noncanonical_input(value: object) -> None:
    with pytest.raises(ValueError):
        parse_decimal(value)  # type: ignore[arg-type]


def test_encode_decimal_removes_trailing_zeroes_without_rounding() -> None:
    assert encode_decimal(Decimal("1.2300")) == "1.23"


@pytest.mark.parametrize("value", [Decimal("0"), Decimal("0.0"), Decimal("-0.000")])
def test_encode_decimal_normalizes_all_zero_variants(value: Decimal) -> None:
    assert encode_decimal(value) == "0"


@pytest.mark.parametrize(
    "value",
    [
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
    ],
)
def test_encode_decimal_rejects_non_finite_values(value: Decimal) -> None:
    with pytest.raises(ValueError):
        encode_decimal(value)


@given(
    coefficient=st.integers(min_value=-(10**30), max_value=10**30),
    scale=st.integers(min_value=0, max_value=18),
)
def test_encode_decimal_is_plain_and_canonical(coefficient: int, scale: int) -> None:
    value = Decimal(coefficient).scaleb(-scale)

    encoded = encode_decimal(value)

    assert "e" not in encoded.lower()
    assert not encoded.startswith("+")
    assert encoded != "-0"
    if value.is_zero():
        assert encoded == "0"
    assert parse_decimal(encoded) == value
