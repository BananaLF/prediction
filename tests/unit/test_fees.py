from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from predmarket.fees import FeeSchedule


D = Decimal


def test_taker_fee_uses_exact_decimal_arithmetic() -> None:
    schedule = FeeSchedule(D(".05"), 1, True, 123)

    assert schedule.taker_fee(D("10"), D(".5")) == D(".1250")


@pytest.mark.parametrize(
    "values, error",
    [
        (("0.05", 1, True, 0), TypeError),
        ((D("NaN"), 1, True, 0), ValueError),
        ((D("Infinity"), 1, True, 0), ValueError),
        ((D("-.01"), 1, True, 0), ValueError),
        ((D(".05"), 0, True, 0), ValueError),
        ((D(".05"), -1, True, 0), ValueError),
        ((D(".05"), True, True, 0), TypeError),
        ((D(".05"), 1.0, True, 0), TypeError),
        ((D(".05"), 1, 1, 0), TypeError),
        ((D(".05"), 1, True, -1), ValueError),
        ((D(".05"), 1, True, True), TypeError),
        ((D(".05"), 1, True, 1.0), TypeError),
    ],
)
def test_fee_schedule_rejects_invalid_parameters(
    values: tuple[object, object, object, object], error: type[Exception]
) -> None:
    with pytest.raises(error):
        FeeSchedule(*values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("shares", "price", "error"),
    [
        ("10", D(".5"), TypeError),
        (D("10"), ".5", TypeError),
        (D("NaN"), D(".5"), ValueError),
        (D("Infinity"), D(".5"), ValueError),
        (D("10"), D("NaN"), ValueError),
        (D("10"), D("Infinity"), ValueError),
        (D("0"), D(".5"), ValueError),
        (D("-1"), D(".5"), ValueError),
        (D("10"), D("0"), ValueError),
        (D("10"), D("1"), ValueError),
        (D("10"), D("1.1"), ValueError),
    ],
)
def test_taker_fee_rejects_invalid_arguments(
    shares: object, price: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        FeeSchedule(D(".05"), 1, True, 0).taker_fee(shares, price)  # type: ignore[arg-type]


def test_fee_schedule_is_immutable() -> None:
    schedule = FeeSchedule(D(".05"), 1, True, 0)

    with pytest.raises(FrozenInstanceError):
        schedule.rate = D(".06")
