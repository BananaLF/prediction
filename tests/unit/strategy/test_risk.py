from decimal import Decimal

from predmarket.domain.fees import FeeModel, FeeSchedule
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.strategy.risk import (
    FailureScenario,
    OpenExposure,
    assess_failure_scenarios,
    immediate_close_value,
)


def _zero_fee() -> FeeSchedule:
    return FeeSchedule(
        model=FeeModel.ZERO,
        enabled=False,
        source="sdk",
        parameters={},
        updated_at=1_000,
    )


def _book(
    token_id: str,
    *,
    bids: tuple[tuple[str, str], ...],
) -> OrderBook:
    return OrderBook(
        market_id=f"market-{token_id}",
        token_id=token_id,
        bids=tuple(OrderBookLevel(Decimal(price), Decimal(size)) for price, size in bids),
        asks=(OrderBookLevel(Decimal("0.80"), Decimal("20")),),
        subscription_generation=1,
        book_hash=f"hash-{token_id}",
        exchange_timestamp=1_000,
        received_timestamp=1_000,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def test_immediate_close_walks_bid_depth_and_values_uncloseable_remainder_at_zero() -> None:
    # Catches best-bid multiplication and invented recovery beyond visible depth.
    result = immediate_close_value(
        OpenExposure(
            token_id="a",
            quantity=Decimal("5"),
            entry_notional=Decimal("3"),
            book=_book("a", bids=(("0.50", "2"), ("0.40", "1"))),
            fee_schedule=_zero_fee(),
        ),
        evaluated_at_ms=1_000,
        fee_max_age_seconds=1,
    )

    assert result.recovery_value == Decimal("1.40")
    assert result.closed_quantity == Decimal("3")
    assert result.uncloseable_quantity == Decimal("2")


def test_failure_risk_enumerates_partial_legs_and_conversion_failure() -> None:
    # Catches checking only the successful all-legs path.
    book_a = _book("a", bids=(("0.40", "5"),))
    book_b = _book("b", bids=(("0.20", "2"),))
    schedule = _zero_fee()
    first = OpenExposure("a", Decimal("5"), Decimal("2.50"), book_a, schedule)
    second = OpenExposure("b", Decimal("5"), Decimal("2.00"), book_b, schedule)

    risk = assess_failure_scenarios(
        (
            FailureScenario("FIRST_LEG_ONLY", Decimal("2.50"), (first,)),
            FailureScenario("PARTIAL_LEGS", Decimal("4.50"), (first, second)),
            FailureScenario("CONVERSION_FAILURE", Decimal("5.00"), (first, second)),
        ),
        evaluated_at_ms=1_000,
        fee_max_age_seconds=1,
    )

    assert tuple((item.name, item.loss) for item in risk.scenarios) == (
        ("CONVERSION_FAILURE", Decimal("2.60")),
        ("FIRST_LEG_ONLY", Decimal("0.50")),
        ("PARTIAL_LEGS", Decimal("2.10")),
    )
    assert risk.worst_case_loss == Decimal("2.60")
    assert risk.unhedged_notional == Decimal("4.50")
    assert risk.risk_flags == (
        "CONVERSION_FAILURE",
        "FIRST_LEG_ONLY",
        "PARTIAL_LEGS",
        "UNCLOSEABLE_EXPOSURE",
    )


def test_failure_risk_uses_zero_recovery_when_no_immediate_close_depth_exists() -> None:
    # Catches treating an uncloseable position as fully recoverable.
    exposure = OpenExposure(
        "a",
        Decimal("4"),
        Decimal("1.60"),
        _book("a", bids=()),
        _zero_fee(),
    )

    risk = assess_failure_scenarios(
        (FailureScenario("FIRST_LEG_ONLY", Decimal("1.60"), (exposure,)),),
        evaluated_at_ms=1_000,
        fee_max_age_seconds=1,
    )

    assert risk.worst_case_loss == Decimal("1.60")
    assert risk.unhedged_notional == Decimal("1.60")
    assert "UNCLOSEABLE_EXPOSURE" in risk.risk_flags


def test_risk_loss_is_clamped_at_zero_when_recovery_exceeds_capital() -> None:
    # Catches recording negative loss and therefore a negative risk rate.
    exposure = OpenExposure(
        "a",
        Decimal("2"),
        Decimal("0.50"),
        _book("a", bids=(("0.40", "2"),)),
        _zero_fee(),
    )

    risk = assess_failure_scenarios(
        (FailureScenario("FIRST_LEG_ONLY", Decimal("0.50"), (exposure,)),),
        evaluated_at_ms=1_000,
        fee_max_age_seconds=1,
    )

    assert risk.worst_case_loss == Decimal("0")
