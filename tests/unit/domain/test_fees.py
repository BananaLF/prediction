from __future__ import annotations

from decimal import Decimal

import pytest

from predmarket.domain.fees import FeeCalculator, FeeModel, FeeSchedule


def test_zero_fee_schedule_returns_exact_zero() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "ZERO",
            "enabled": True,
            "source": "clob",
            "parameters": {},
            "updated_at": 100,
        }
    )

    assert schedule.model is FeeModel.ZERO
    assert FeeCalculator.calculate(schedule, Decimal("0.42"), Decimal("12.5")) == Decimal("0")


def test_flat_fee_schedule_charges_rate_on_notional() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "FLAT",
            "enabled": True,
            "source": "clob",
            "parameters": {"rate": "0.02"},
            "updated_at": 100,
        }
    )

    assert FeeCalculator.calculate(schedule, Decimal("0.4"), Decimal("10")) == Decimal("0.08")


def test_disabled_known_schedule_returns_zero() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "FLAT",
            "enabled": False,
            "source": "clob",
            "parameters": {"rate": "0.02"},
            "updated_at": 100,
        }
    )

    assert FeeCalculator.calculate(schedule, Decimal("0.4"), Decimal("10")) == Decimal("0")


def test_unknown_fee_model_is_rejected_instead_of_assumed_zero() -> None:
    with pytest.raises(ValueError, match="unknown fee model"):
        FeeSchedule.from_json(
            {
                "model": "CURVE",
                "enabled": True,
                "source": "clob",
                "parameters": {},
                "updated_at": 100,
            }
        )


def test_flat_fee_schedule_requires_canonical_decimal_rate() -> None:
    for parameters in ({}, {"rate": 0.02}, {"rate": "0.020"}):
        with pytest.raises(ValueError):
            FeeSchedule.from_json(
                {
                    "model": "FLAT",
                    "enabled": True,
                    "source": "clob",
                    "parameters": parameters,
                    "updated_at": 100,
                }
            )


def test_fee_schedule_freshness_is_evaluated_without_wall_clock_access() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "ZERO",
            "enabled": True,
            "source": "clob",
            "parameters": {},
            "updated_at": 100_000,
        }
    )

    assert schedule.is_stale(evaluated_at=400_000, max_age_seconds=300) is False
    assert schedule.is_stale(evaluated_at=400_001, max_age_seconds=300) is True


@pytest.mark.parametrize(
    ("price", "quantity"),
    [
        (Decimal("-0.01"), Decimal("1")),
        (Decimal("1.01"), Decimal("1")),
        (Decimal("0.5"), Decimal("0")),
        (Decimal("0.5"), Decimal("-1")),
    ],
)
def test_fee_calculation_rejects_invalid_price_or_quantity(
    price: Decimal, quantity: Decimal
) -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "ZERO",
            "enabled": True,
            "source": "clob",
            "parameters": {},
            "updated_at": 100,
        }
    )

    with pytest.raises(ValueError):
        FeeCalculator.calculate(schedule, price, quantity)
