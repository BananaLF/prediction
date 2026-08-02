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
    assert FeeCalculator.calculate(
        schedule,
        Decimal("0.42"),
        Decimal("12.5"),
        evaluated_at_ms=100,
        max_age_seconds=300,
    ) == Decimal("0")


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

    assert FeeCalculator.calculate(
        schedule,
        Decimal("0.4"),
        Decimal("10"),
        evaluated_at_ms=100,
        max_age_seconds=300,
    ) == Decimal("0.08")


def test_curve_fee_schedule_calculates_polymarket_taker_fee() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "CURVE",
            "enabled": True,
            "source": "clob",
            "parameters": {
                "rate": "0.04",
                "exponent": "1",
                "rebate_rate": "0.25",
            },
            "taker_only": True,
            "updated_at": 100,
        }
    )

    assert schedule.model is FeeModel.CURVE
    assert schedule.taker_only is True
    assert schedule.parameters["exponent"] == Decimal("1")
    assert schedule.parameters["rebate_rate"] == Decimal("0.25")
    assert FeeCalculator.calculate(
        schedule,
        Decimal("0.4"),
        Decimal("10"),
        evaluated_at_ms=100,
        max_age_seconds=300,
    ) == Decimal("0.096")


def test_curve_taker_only_schedule_does_not_charge_maker_path() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "CURVE",
            "enabled": True,
            "source": "clob",
            "parameters": {
                "rate": "0.04",
                "exponent": "1",
                "rebate_rate": "0.25",
            },
            "taker_only": True,
            "updated_at": 100,
        }
    )

    assert FeeCalculator.calculate(
        schedule,
        Decimal("0.4"),
        Decimal("10"),
        evaluated_at_ms=100,
        max_age_seconds=300,
        is_taker=False,
    ) == Decimal("0")


def test_curve_fee_has_protocol_minimum_after_rounding() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "CURVE",
            "enabled": True,
            "source": "clob",
            "parameters": {
                "rate": "0.000001",
                "exponent": "1",
                "rebate_rate": "0",
            },
            "updated_at": 100,
        }
    )

    assert FeeCalculator.calculate(
        schedule,
        Decimal("0.5"),
        Decimal("1"),
        evaluated_at_ms=100,
        max_age_seconds=300,
    ) == Decimal("0.00001")


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

    assert FeeCalculator.calculate(
        schedule,
        Decimal("0.4"),
        Decimal("10"),
        evaluated_at_ms=100,
        max_age_seconds=300,
    ) == Decimal("0")


def test_unknown_fee_model_is_rejected_instead_of_assumed_zero() -> None:
    with pytest.raises(ValueError, match="unknown fee model"):
        FeeSchedule.from_json(
            {
                "model": "UNKNOWN",
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


def test_fee_calculation_rejects_schedule_without_update_timestamp() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "ZERO",
            "enabled": True,
            "source": "clob",
            "parameters": {},
        }
    )

    with pytest.raises(ValueError, match="stale"):
        FeeCalculator.calculate(
            schedule,
            Decimal("0.5"),
            Decimal("1"),
            evaluated_at_ms=100_000,
            max_age_seconds=300,
        )


def test_fee_calculation_rejects_schedule_older_than_maximum_age() -> None:
    schedule = FeeSchedule.from_json(
        {
            "model": "FLAT",
            "enabled": True,
            "source": "clob",
            "parameters": {"rate": "0.02"},
            "updated_at": 100_000,
        }
    )

    with pytest.raises(ValueError, match="stale"):
        FeeCalculator.calculate(
            schedule,
            Decimal("0.5"),
            Decimal("1"),
            evaluated_at_ms=400_001,
            max_age_seconds=300,
        )


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
        FeeCalculator.calculate(
            schedule,
            price,
            quantity,
            evaluated_at_ms=100,
            max_age_seconds=300,
        )
