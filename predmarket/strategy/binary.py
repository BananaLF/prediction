"""Binary complete-set underpricing and overpricing strategies."""

from __future__ import annotations

from decimal import Decimal

from predmarket.domain.decimal import encode_decimal
from predmarket.domain.signal import (
    Action,
    DecisionReason,
    StrategyContext,
    StrategyDecision,
    StrategyType,
)
from predmarket.strategy.common import (
    calculation,
    classify,
    conversion_leg,
    long_entry_risk,
    not_evaluable,
    optimize_trades,
    split_inventory_risk,
    token_by_outcome,
    trade,
    validate_inputs,
)


def evaluate_binary(context: StrategyContext) -> StrategyDecision:
    if context.strategy_type not in {
        StrategyType.BINARY_UNDERPRICED,
        StrategyType.BINARY_OVERPRICED,
    }:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "binary_strategy_type_required",
        )
    if len(context.markets) != 1:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "binary_requires_one_market",
        )
    market = context.markets[0]
    market_tokens = tuple(token for token in context.tokens if token.market_id == market.id)
    yes = token_by_outcome(market_tokens, "yes")
    no = token_by_outcome(market_tokens, "no")
    if len(market_tokens) != 2 or yes is None or no is None:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "binary_yes_no_mapping_incomplete",
        )
    required = (yes, no)
    actions = (
        (Action.BUY, Action.BUY)
        if context.strategy_type is StrategyType.BINARY_UNDERPRICED
        else (Action.SELL, Action.SELL)
    )
    invalid = validate_inputs(
        context,
        markets=(market,),
        tokens=required,
        actions=actions,
    )
    if invalid is not None:
        return invalid
    if context.strategy_type is StrategyType.BINARY_UNDERPRICED:
        return _underpriced(context, market, yes, no)
    return _overpriced(context, market, yes, no)


def _underpriced(context, market, yes, no) -> StrategyDecision:
    specs = ((market, yes, Action.BUY), (market, no, Action.BUY))

    def economics(quantity, trades):
        trading_cost = sum(
            (item.fill.gross_amount + item.fee for item in trades), Decimal("0")
        )
        safety = (
            sum((item.fill.gross_amount for item in trades), Decimal("0"))
            * context.configuration.safety_buffer_rate
        )
        capital = trading_cost + context.configuration.conversion_cost + safety
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
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "no_executable_quantity",
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
            "execution_mode": "IMMEDIATE_CONVERSION",
            "minimum_proceeds": encode_decimal(quantity),
            "strategy_type": context.strategy_type.value,
        },
    )
    legs = (
        trades[0].leg(0),
        trades[1].leg(1),
        conversion_leg(2, market.id, Action.MERGE, quantity),
    )
    return classify(
        context,
        calc,
        legs,
        tuple(item.book for item in trades),
        optimized.forced_absent_reason,
    )


def _overpriced(context, market, yes, no) -> StrategyDecision:
    specs = ((market, yes, Action.SELL), (market, no, Action.SELL))

    def economics(quantity, trades):
        safety = (
            sum((item.fill.gross_amount for item in trades), Decimal("0"))
            * context.configuration.safety_buffer_rate
        )
        capital = quantity + context.configuration.conversion_cost + safety
        proceeds = sum((item.net_proceeds for item in trades), Decimal("0"))
        return capital, proceeds - capital

    optimized = optimize_trades(
        context,
        trade_specs=specs,
        economics=economics,
        risk_evaluator=lambda trades, capital: split_inventory_risk(
            context, trades, total_capital=capital
        ),
    )
    if optimized is None:
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "no_executable_quantity",
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
            "execution_mode": "IMMEDIATE_CONVERSION",
            "strategy_type": context.strategy_type.value,
        },
    )
    legs = (
        conversion_leg(0, market.id, Action.SPLIT, quantity),
        trades[0].leg(1),
        trades[1].leg(2),
    )
    return classify(
        context,
        calc,
        legs,
        tuple(item.book for item in trades),
        optimized.forced_absent_reason,
    )
