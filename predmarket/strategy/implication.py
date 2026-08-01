"""Approved A-implies-B hold-to-resolution strategy."""

from __future__ import annotations

from decimal import Decimal

from predmarket.domain.decimal import encode_decimal
from predmarket.domain.relation import RelationStatus
from predmarket.domain.signal import Action, DecisionReason, StrategyContext, StrategyDecision, StrategyType
from predmarket.strategy.common import (
    calculation,
    classify,
    conversion_leg,
    feasibility_details,
    long_entry_risk,
    not_evaluable,
    optimize_trades,
    token_by_outcome,
    trade,
    validate_inputs,
)


def evaluate_implication(context: StrategyContext) -> StrategyDecision:
    if context.strategy_type is not StrategyType.LOGICAL_IMPLICATION:
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "implication_strategy_type_required"
        )
    relation = context.approved_implication_relation
    if relation is None or relation.status is not RelationStatus.APPROVED:
        return not_evaluable(
            context, DecisionReason.RELATION_NOT_APPROVED, "approved_relation_required"
        )
    markets = {market.id: market for market in context.markets}
    if set(markets) != {relation.market_a_id, relation.market_b_id}:
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "relation_market_binding_invalid"
        )
    market_a = markets[relation.market_a_id]
    market_b = markets[relation.market_b_id]
    no_a = token_by_outcome(
        (token for token in context.tokens if token.market_id == market_a.id), "no"
    )
    yes_b = token_by_outcome(
        (token for token in context.tokens if token.market_id == market_b.id), "yes"
    )
    if no_a is None or yes_b is None:
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "implication_token_mapping_incomplete"
        )
    required = (no_a, yes_b)
    invalid = validate_inputs(
        context,
        markets=(market_a, market_b),
        tokens=required,
        actions=(Action.BUY, Action.BUY),
    )
    if invalid is not None:
        return invalid
    specs = ((market_a, no_a, Action.BUY), (market_b, yes_b, Action.BUY))

    def economics(quantity, trades):
        gross = sum((item.fill.gross_amount for item in trades), Decimal("0"))
        fees = sum((item.fee for item in trades), Decimal("0"))
        safety = gross * context.configuration.safety_buffer_rate
        capital = gross + fees + context.configuration.conversion_cost + safety
        return capital, quantity - capital

    optimized = optimize_trades(
        context,
        trade_specs=specs,
        economics=economics,
        risk_evaluator=lambda trades, capital: long_entry_risk(
            context, trades, total_capital=capital
        ),
    )
    if optimized is None:
        return not_evaluable(
            context, DecisionReason.ORDERBOOK_INVALID, "no_executable_quantity"
        )
    candidate = optimized.candidate
    quantity = candidate.quantity
    trades = candidate.trades
    total_capital = candidate.total_capital
    expected_profit = candidate.expected_profit
    risk = candidate.risk
    calc = calculation(
        quantity=quantity,
        total_capital=total_capital,
        expected_profit=expected_profit,
        risk=risk,
        details={
            "execution_mode": "HOLD_TO_RESOLUTION",
            "payout_states": {
                "A_FALSE_B_FALSE": encode_decimal(quantity),
                "A_FALSE_B_TRUE": encode_decimal(quantity * Decimal("2")),
                "A_TRUE_B_TRUE": encode_decimal(quantity),
            },
            "relation_id": relation.id,
            "strategy_type": context.strategy_type.value,
            **feasibility_details(context, optimized),
        },
    )
    legs = (
        trades[0].leg(0),
        trades[1].leg(1),
        conversion_leg(2, market_b.id, Action.REDEEM, quantity),
    )
    return classify(
        context,
        calc,
        legs,
        tuple(item.book for item in trades),
        optimized.forced_absent_reason,
    )
