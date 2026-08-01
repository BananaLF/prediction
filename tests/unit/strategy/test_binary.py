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
    configuration=None,
    size="10",
):
    market = market_factory("market-1")
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
        ),
        book_factory(
            no.id,
            market.id,
            bid=no_bid,
            ask=no_ask,
            exchange_timestamp=no_time,
            received_timestamp=no_time,
            size=size,
        ),
    )
    return context_factory(
        strategy_type,
        markets=(market,),
        tokens=(yes, no),
        orderbooks=books,
        fees=fees,
        evaluated_at=evaluated_at,
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
    assert [leg.action for leg in decision.legs] == [Action.BUY, Action.BUY, Action.MERGE]
    assert [book.token_id for book in decision.evidence] == ["no", "yes"]


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


def test_binary_uses_exchange_time_for_freshness_and_enforces_causality(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches a freshly received but old snapshot, or exchange time after receipt.
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
    impossible_time = replace(
        base,
        orderbooks=tuple(
            replace(book, exchange_timestamp=1_100, received_timestamp=1_000)
            for book in base.orderbooks
        ),
    )

    old_decision = evaluate_binary(old_exchange)
    impossible_decision = evaluate_binary(impossible_time)

    assert isinstance(old_decision, NotEvaluable)
    assert old_decision.reason_code is DecisionReason.ORDERBOOK_STALE
    assert isinstance(impossible_decision, NotEvaluable)
    assert impossible_decision.reason_code is DecisionReason.ORDERBOOK_INVALID


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
        configuration=strategy_config_factory(maximum_book_age_ms=20_000),
    )

    missing_decision = evaluate_binary(missing)
    stale_decision = evaluate_binary(stale)

    assert isinstance(missing_decision, NotEvaluable)
    assert missing_decision.reason_code is DecisionReason.FEE_SCHEDULE_UNKNOWN
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
