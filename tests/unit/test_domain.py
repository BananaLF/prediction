from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from predmarket.domain import BookLevel, OpportunityStatus, PathKind, Side


def test_enum_values_are_stable() -> None:
    assert {member.name: member.value for member in Side} == {
        "BUY": "BUY",
        "SELL": "SELL",
    }
    assert {member.name: member.value for member in OpportunityStatus} == {
        "REJECTED": "REJECTED",
        "RESEARCH_CANDIDATE": "RESEARCH_CANDIDATE",
        "SNAPSHOT_EXECUTABLE": "SNAPSHOT_EXECUTABLE",
    }
    assert {member.name: member.value for member in PathKind} == {
        "IMMEDIATE_CONVERSION": "IMMEDIATE_CONVERSION",
        "HOLD_TO_RESOLUTION": "HOLD_TO_RESOLUTION",
    }


@pytest.mark.parametrize(
    ("price", "size"),
    [
        ("0.5", Decimal("1")),
        (Decimal("0.5"), "1"),
        (0.5, Decimal("1")),
        (Decimal("0.5"), 1),
    ],
)
def test_book_level_rejects_non_decimal_values(price: object, size: object) -> None:
    with pytest.raises(TypeError):
        BookLevel(price=price, size=size)  # type: ignore[arg-type]


@pytest.mark.parametrize("price", [Decimal("-0.1"), Decimal("0"), Decimal("1"), Decimal("1.1")])
def test_book_level_rejects_price_outside_open_unit_interval(price: Decimal) -> None:
    with pytest.raises(ValueError):
        BookLevel(price=price, size=Decimal("1"))


@pytest.mark.parametrize("size", [Decimal("-1"), Decimal("0")])
def test_book_level_rejects_nonpositive_size(size: Decimal) -> None:
    with pytest.raises(ValueError):
        BookLevel(price=Decimal("0.5"), size=size)


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
@pytest.mark.parametrize("field", ["price", "size"])
def test_book_level_rejects_nonfinite_decimals(field: str, value: Decimal) -> None:
    values = {"price": Decimal("0.5"), "size": Decimal("1")}
    values[field] = value

    with pytest.raises(ValueError):
        BookLevel(**values)


def test_book_level_is_immutable() -> None:
    level = BookLevel(price=Decimal("0.5"), size=Decimal("2"))

    with pytest.raises(FrozenInstanceError):
        level.size = Decimal("3")
