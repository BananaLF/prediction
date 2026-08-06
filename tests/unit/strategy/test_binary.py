from dataclasses import replace
from decimal import Decimal

import pytest

from predmarket.domain.signal import (
    Action,
    DecisionReason,
    NotEvaluable,
    OpportunityAbsent,
    OpportunityPresent,
    StrategyType,
)
from predmarket.strategy.binary import evaluate_binary


def _binary_context(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    *,
    strategy_type=StrategyType.BINARY_UNDERPRICED,
    yes_bid="0.39",
    yes_ask="0.40",
    no_bid="0.39",
    no_ask="0.40",
    yes_time=1_000,
    no_time=1_000,
    fees=None,
    evaluated_at=1_000,
    fee_schedule_evaluated_at=1_000,
    configuration=None,
    size="10",
    minimum="1",
):
    market = market_factory("market-1", minimum=minimum)
    yes = token_factory("yes", market.id, "Yes", 0)
    no = token_factory("no", market.id, "No", 1)
    books = (
        book_factory(
            yes.id,
            market.id,
            bid=yes_bid,
            ask=yes_ask,
            exchange_timestamp=yes_time,
            received_timestamp=yes_time,
            size=size,
            minimum=minimum,
        ),
        book_factory(
            no.id,
            market.id,
            bid=no_bid,
            ask=no_ask,
            exchange_timestamp=no_time,
            received_timestamp=no_time,
            size=size,
            minimum=minimum,
        ),
    )
    return context_factory(
        strategy_type,
        markets=(market,),
        tokens=(yes, no),
        orderbooks=books,
        fees=fees,
        evaluated_at=evaluated_at,
        fee_schedule_evaluated_at=fee_schedule_evaluated_at,
        configuration=configuration,
    )


def test_binary_underpriced_uses_full_depth_and_exact_economics(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches wrong complete-set proceeds or omitting a required action leg.
    decision = evaluate_binary(
        _binary_context(context_factory, market_factory, token_factory, book_factory)
    )

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.quantity == Decimal("10")
    assert decision.calculation.total_capital == Decimal("8.00")
    assert decision.calculation.expected_profit == Decimal("2.00")
    assert decision.calculation.return_rate == Decimal("0.25")
    assert decision.calculation.worst_case_loss == Decimal("0.20")
    assert decision.calculation.risk_rate == Decimal("0.025")
    assert decision.calculation.unhedged_notional == Decimal("4.00")
    assert [leg.action for leg in decision.legs] == [Action.BUY, Action.BUY, Action.MERGE]
    assert [book.token_id for book in decision.evidence] == ["no", "yes"]


def test_binary_underpriced_uses_authoritative_positions_for_arbitrary_labels(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Binary complete sets are identified by SDK positions, not display labels.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
    )
    context = replace(
        context,
        tokens=tuple(
            replace(token, outcome=("Up" if token.position == 0 else "Down"))
            for token in context.tokens
        ),
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert [leg.token_id for leg in decision.legs[:2]] == ["yes", "no"]


def test_binary_evidence_details_encode_decimal_without_exponents(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches non-canonical Decimal evidence that persistence must reject.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        size="1E+1",
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.details["minimum_proceeds"] == "10"


def test_binary_overpriced_uses_split_and_net_l2_sales(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches applying the underpriced formula to the overpricing path.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        strategy_type=StrategyType.BINARY_OVERPRICED,
        yes_bid="0.60",
        no_bid="0.60",
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.quantity == Decimal("10")
    assert decision.calculation.total_capital == Decimal("10")
    assert decision.calculation.expected_profit == Decimal("2.00")
    assert [leg.action for leg in decision.legs] == [Action.SPLIT, Action.SELL, Action.SELL]


def test_binary_returns_absent_for_complete_but_unprofitable_books(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches turning a known negative result into NotEvaluable or an opportunity.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_ask="0.55",
        no_ask="0.55",
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityAbsent)
    assert decision.reason_code is DecisionReason.PROFIT_BELOW_THRESHOLD
    assert decision.calculation.quantity == Decimal("1")
    assert decision.calculation.expected_profit == Decimal("-0.10")


def test_binary_fees_can_eliminate_apparent_profit(
    context_factory, market_factory, token_factory, book_factory, fee_factory
) -> None:
    # Catches treating enabled authoritative fees as zero.
    fees = {
        "yes": fee_factory(rate="0.30"),
        "no": fee_factory(rate="0.30"),
    }
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_ask="0.45",
        no_ask="0.45",
        fees=fees,
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityAbsent)
    assert decision.calculation.quantity == Decimal("1")
    assert decision.calculation.expected_profit == Decimal("-0.1700")


@pytest.mark.parametrize(
    ("yes_time", "no_time", "evaluated_at", "reason"),
    [
        (1_000, 1_000, 2_001, DecisionReason.ORDERBOOK_STALE),
        (1_000, 1_251, 1_251, DecisionReason.LEG_SKEW_EXCEEDED),
    ],
)
def test_binary_fails_closed_for_stale_or_skewed_books(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    yes_time,
    no_time,
    evaluated_at,
    reason,
) -> None:
    # Catches evaluating incoherent or old evidence.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_time=yes_time,
        no_time=no_time,
        evaluated_at=evaluated_at,
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is reason


def test_binary_uses_market_time_for_unchanged_books(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # A resting book is stale when market time advances beyond its exchange timestamp.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_time=1_000,
        no_time=1_500,
        evaluated_at=10_000,
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.ORDERBOOK_STALE


def test_binary_uses_only_market_time_for_orderbook_validity(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
) -> None:
    base = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        evaluated_at=1_500,
    )
    old_exchange = replace(
        base,
        orderbooks=tuple(
            replace(book, exchange_timestamp=0, received_timestamp=1_000)
            for book in base.orderbooks
        ),
    )
    exchange_ahead_of_host = replace(
        base,
        orderbooks=tuple(
            replace(book, exchange_timestamp=1_400, received_timestamp=1_000)
            for book in base.orderbooks
        ),
    )
    host_receipt_ahead = replace(
        base,
        orderbooks=tuple(
            replace(book, exchange_timestamp=1_400, received_timestamp=10_000)
            for book in base.orderbooks
        ),
    )
    future_market_time = replace(
        base,
        orderbooks=tuple(
            replace(book, exchange_timestamp=1_501, received_timestamp=0)
            for book in base.orderbooks
        ),
    )

    old_decision = evaluate_binary(old_exchange)
    exchange_ahead_decision = evaluate_binary(exchange_ahead_of_host)
    host_receipt_ahead_decision = evaluate_binary(host_receipt_ahead)
    future_market_time_decision = evaluate_binary(future_market_time)

    assert isinstance(old_decision, NotEvaluable)
    assert old_decision.reason_code is DecisionReason.ORDERBOOK_STALE
    assert isinstance(exchange_ahead_decision, OpportunityPresent)
    assert isinstance(host_receipt_ahead_decision, OpportunityPresent)
    assert isinstance(future_market_time_decision, NotEvaluable)
    assert future_market_time_decision.reason_code is DecisionReason.ORDERBOOK_INVALID
    assert future_market_time_decision.context["detail"] == "orderbook_from_future"


def test_binary_fails_closed_for_unknown_or_stale_fee(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    fee_factory,
    strategy_config_factory,
) -> None:
    # Catches silently substituting zero for absent or expired fee proof.
    missing = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        fees={"yes": fee_factory()},
    )
    stale = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        fees={"yes": fee_factory(updated_at=0), "no": fee_factory(updated_at=0)},
        evaluated_at=11_001,
        fee_schedule_evaluated_at=11_001,
        configuration=strategy_config_factory(maximum_book_age_ms=20_000),
    )

    missing_decision = evaluate_binary(missing)
    stale_decision = evaluate_binary(stale)

    assert isinstance(missing_decision, NotEvaluable)
    assert missing_decision.reason_code is DecisionReason.FEE_SCHEDULE_UNKNOWN
    assert isinstance(stale_decision, NotEvaluable)
    assert stale_decision.reason_code is DecisionReason.FEE_SCHEDULE_STALE


def test_binary_fee_freshness_uses_fee_cache_time_not_market_time(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    fee_factory,
) -> None:
    fees = {
        "yes": fee_factory(updated_at=9_500),
        "no": fee_factory(updated_at=9_500),
    }
    fresh = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_time=2_000,
        no_time=2_000,
        fees=fees,
        evaluated_at=2_000,
        fee_schedule_evaluated_at=10_000,
    )
    stale = replace(fresh, fee_schedule_evaluated_at=19_501)

    fresh_decision = evaluate_binary(fresh)
    stale_decision = evaluate_binary(stale)

    assert isinstance(fresh_decision, OpportunityPresent)
    assert isinstance(stale_decision, NotEvaluable)
    assert stale_decision.reason_code is DecisionReason.FEE_SCHEDULE_STALE


def test_binary_applies_return_risk_and_unhedged_hard_gates(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    strategy_config_factory,
) -> None:
    # Catches opening a mathematically profitable opportunity above a risk limit.
    configuration = strategy_config_factory(maximum_risk_rate=Decimal("0.01"))
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        configuration=configuration,
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityAbsent)
    assert decision.reason_code is DecisionReason.RISK_ABOVE_THRESHOLD


def test_binary_optimizer_selects_profitable_safe_quantity_below_risky_maximum(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    strategy_config_factory,
) -> None:
    # Catches selecting the max-profit q before applying the unhedged hard limit.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        size="100",
        configuration=strategy_config_factory(
            maximum_unhedged_notional=Decimal("20"),
        ),
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.quantity == Decimal("50")
    assert decision.calculation.unhedged_notional == Decimal("20")


def test_binary_optimizer_keeps_nonterminating_unhedged_root_on_safe_side(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    strategy_config_factory,
) -> None:
    # Catches a rounded root landing one ulp outside and falling back to q=1.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_ask="0.124",
        no_ask="0.50",
        yes_bid="0.12",
        no_bid="0.49",
        size="100",
        configuration=strategy_config_factory(
            maximum_unhedged_notional=Decimal("7"),
        ),
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.quantity > Decimal("56")
    assert decision.calculation.unhedged_notional <= Decimal("7")
    assert (decision.calculation.quantity + Decimal("1E-25")) * Decimal("0.124") > Decimal("7")


def test_buy_strategy_values_empty_reverse_depth_as_zero_recovery(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches requiring bids for BUY execution instead of risk-valuing them at zero.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        yes_bid="",
        no_bid="",
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.worst_case_loss == Decimal("8.00")
    assert decision.calculation.risk_rate == Decimal("1")
    assert "UNCLOSEABLE_EXPOSURE" in decision.calculation.risk_flags


def test_sell_strategy_does_not_require_asks(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches rejecting SPLIT/SELL because an unused ask side is empty.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        strategy_type=StrategyType.BINARY_OVERPRICED,
        yes_bid="0.60",
        no_bid="0.60",
        yes_ask="",
        no_ask="",
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.expected_profit == Decimal("2.00")


def test_subminimum_visible_depth_returns_auditable_absent(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches mapping known quantity infeasibility to NotEvaluable.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        size="2",
        minimum="3",
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityAbsent)
    assert decision.reason_code is DecisionReason.QUANTITY_BELOW_MINIMUM
    assert decision.calculation.quantity == Decimal("2")
    assert all(leg.quantity == Decimal("2") for leg in decision.legs)
    assert decision.evidence


def test_positive_but_insufficient_bankroll_returns_auditable_absent(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    strategy_config_factory,
) -> None:
    # Catches turning a known bankroll shortfall into input invalidity.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        configuration=strategy_config_factory(bankroll=Decimal("0.5")),
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, OpportunityAbsent)
    assert decision.reason_code is DecisionReason.INSUFFICIENT_CAPITAL
    assert decision.calculation.quantity == Decimal("1")
    assert decision.calculation.total_capital == Decimal("0.80")
    assert decision.calculation.details["required_capital"] == "0.8"
    assert decision.calculation.details["available_bankroll"] == "0.5"


@pytest.mark.parametrize(
    "override",
    [
        {"bankroll": Decimal("0")},
        {"bankroll": Decimal("NaN")},
        {"conversion_cost": Decimal("-1")},
        {"maximum_book_age_ms": -1},
        {"exchange_clock_skew_warning_ms": -1},
        {"maximum_leg_skew_ms": -1},
        {"maximum_risk_rate": Decimal("-0.1")},
        {"safety_buffer_rate": Decimal("1.1")},
    ],
)
def test_invalid_strategy_configuration_returns_not_evaluable_without_throwing(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    strategy_config_factory,
    override,
) -> None:
    # Catches invalid but constructible configuration escaping into Watch as ValueError.
    context = _binary_context(
        context_factory,
        market_factory,
        token_factory,
        book_factory,
        configuration=strategy_config_factory(**override),
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING
