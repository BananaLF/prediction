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
    EvaluatedTrades,
    calculation,
    classify,
    conversion_leg,
    feasibility_details,
    long_entry_risk,
    not_evaluable,
    plan_trade_quantities,
    select_trade_optimization,
    split_inventory_risk,
    trade,
    trade_root_quantities,
    validate_inputs,
)
from predmarket.strategy.decimal_context import (
    StrategyNumericLimitError,
    isolated_decimal_context,
)


def evaluate_binary(context: StrategyContext) -> StrategyDecision:
    try:
        return _evaluate_binary(context)
    except StrategyNumericLimitError:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "strategy_numeric_limit",
        )


@isolated_decimal_context(operation_depth=48)
def _evaluate_binary(context: StrategyContext) -> StrategyDecision:
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
    tokens_by_position = {token.position: token for token in market_tokens}
    first = tokens_by_position.get(0)
    second = tokens_by_position.get(1)
    if (
        len(market_tokens) != 2
        or len(tokens_by_position) != 2
        or first is None
        or second is None
    ):
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "binary_position_mapping_incomplete",
        )
    required = (first, second)
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
        return _underpriced(context, market, first, second)
    return _overpriced(context, market, first, second)


def _underpriced(context, market, yes, no) -> StrategyDecision:
    specs = ((market, yes, Action.BUY), (market, no, Action.BUY))

    plan = plan_trade_quantities(context, trade_specs=specs)
    if plan is None:
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "no_executable_quantity",
        )

    def evaluate_candidate(quantity):
        trades = tuple(trade(context, *spec, quantity) for spec in specs)
        trading_cost = sum(
            (item.fill.gross_amount + item.fee for item in trades), Decimal("0")
        )
        safety = (
            sum((item.fill.gross_amount for item in trades), Decimal("0"))
            * context.configuration.safety_buffer_rate
        )
        capital = trading_cost + context.configuration.conversion_cost + safety
        return EvaluatedTrades(
            quantity,
            trades,
            capital,
            quantity - capital,
            long_entry_risk(context, trades, total_capital=capital),
        )

    candidates = tuple(evaluate_candidate(value) for value in plan.quantities)
    if plan.forced_absent_reason is None:
        known = {item.quantity for item in candidates}
        candidates += tuple(
            evaluate_candidate(value)
            for value in trade_root_quantities(context, candidates)
            if value not in known
        )
    optimized = select_trade_optimization(
        context,
        candidates,
        forced_absent_reason=plan.forced_absent_reason,
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
            **feasibility_details(context, optimized),
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

    plan = plan_trade_quantities(context, trade_specs=specs)
    if plan is None:
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "no_executable_quantity",
        )

    def evaluate_candidate(quantity):
        trades = tuple(trade(context, *spec, quantity) for spec in specs)
        safety = (
            sum((item.fill.gross_amount for item in trades), Decimal("0"))
            * context.configuration.safety_buffer_rate
        )
        capital = quantity + context.configuration.conversion_cost + safety
        proceeds = sum((item.net_proceeds for item in trades), Decimal("0"))
        return EvaluatedTrades(
            quantity,
            trades,
            capital,
            proceeds - capital,
            split_inventory_risk(context, trades, total_capital=capital),
        )

    candidates = tuple(evaluate_candidate(value) for value in plan.quantities)
    if plan.forced_absent_reason is None:
        known = {item.quantity for item in candidates}
        candidates += tuple(
            evaluate_candidate(value)
            for value in trade_root_quantities(context, candidates)
            if value not in known
        )
    optimized = select_trade_optimization(
        context,
        candidates,
        forced_absent_reason=plan.forced_absent_reason,
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
            **feasibility_details(context, optimized),
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
