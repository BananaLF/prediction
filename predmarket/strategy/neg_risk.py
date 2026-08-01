"""Authoritative SDK-metadata NegRisk complete-set strategy."""

from __future__ import annotations

from decimal import Decimal

from predmarket.domain.market import MarketStatus
from predmarket.domain.signal import Action, DecisionReason, StrategyContext, StrategyDecision, StrategyType
from predmarket.strategy.common import (
    calculation,
    classify,
    conversion_leg,
    long_entry_risk,
    not_evaluable,
    optimize_trades,
    token_by_outcome,
    trade,
    validate_inputs,
)


def evaluate_neg_risk(context: StrategyContext) -> StrategyDecision:
    if context.strategy_type is not StrategyType.NEG_RISK_COMPLETE_SET:
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_strategy_type_required"
        )
    if len(context.events) != 1:
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "authoritative_event_required"
        )
    event = context.events[0]
    if (
        event.status is not MarketStatus.ACTIVE
        or not event.neg_risk
        or event.neg_risk_id is None
        or event.neg_risk_type is None
        or event.neg_risk_type not in context.supported_neg_risk_types
        or not event.neg_risk_complete
        or not event.neg_risk_conversion_supported
        or not event.neg_risk_metadata
        or not isinstance(event.neg_risk_metadata.get("mapping_version"), str)
    ):
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_event_proof_incomplete"
        )
    markets = tuple(
        sorted(context.markets, key=lambda item: item.neg_risk_outcome_position or 0)
    )
    market_ids = {market.id for market in markets}
    if set(event.market_ids) != market_ids or any(
        market.event_id != event.id
        or not market.neg_risk
        or not market.neg_risk_member_complete
        or market.neg_risk_outcome_position is None
        for market in markets
    ):
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_member_proof_incomplete"
        )
    positions = [market.neg_risk_outcome_position for market in markets]
    if positions != list(range(len(markets))):
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_member_positions_invalid"
        )
    if len({market.condition_id for market in markets}) != len(markets):
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_condition_mapping_invalid"
        )
    if (
        not event.sync_generation_complete
        or any(not market.sync_generation_complete for market in markets)
        or {event.sync_generation, *(market.sync_generation for market in markets)}
        != {event.sync_generation}
    ):
        return not_evaluable(
            context, DecisionReason.SYNC_GENERATION_INCOMPLETE, "neg_risk_generation_incomplete"
        )

    required = []
    for market in markets:
        member_tokens = tuple(token for token in context.tokens if token.market_id == market.id)
        yes = token_by_outcome(member_tokens, "yes")
        no = token_by_outcome(member_tokens, "no")
        if len(member_tokens) != 2 or yes is None or no is None:
            return not_evaluable(
                context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_token_mapping_incomplete"
            )
        if {token.position for token in member_tokens} != {0, 1}:
            return not_evaluable(
                context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_token_positions_invalid"
            )
        if (
            any(not token.sync_generation_complete for token in member_tokens)
            or any(token.sync_generation != event.sync_generation for token in member_tokens)
        ):
            return not_evaluable(
                context, DecisionReason.SYNC_GENERATION_INCOMPLETE, "neg_risk_token_generation_incomplete"
            )
        required.append((market, yes))
    if context.changed_token_id not in {token.id for _, token in required}:
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "changed_token_not_affected"
        )
    required_tokens = tuple(token for _, token in required)
    invalid = validate_inputs(context, markets=markets, tokens=required_tokens)
    if invalid is not None:
        return invalid
    specs = tuple((market, token, Action.BUY) for market, token in required)

    def economics(quantity, trades):
        gross = sum((item.fill.gross_amount for item in trades), Decimal("0"))
        fees = sum((item.fee for item in trades), Decimal("0"))
        safety = gross * context.configuration.safety_buffer_rate
        capital = gross + fees + context.configuration.conversion_cost + safety
        return capital, quantity - capital

    quantity = optimize_trades(context, trade_specs=specs, economics=economics)
    if quantity is None:
        return not_evaluable(
            context, DecisionReason.ORDERBOOK_INVALID, "no_executable_quantity"
        )
    trades = tuple(trade(context, *spec, quantity) for spec in specs)
    total_capital, expected_profit = economics(quantity, trades)
    risk = long_entry_risk(context, trades, total_capital=total_capital)
    calc = calculation(
        quantity=quantity,
        total_capital=total_capital,
        expected_profit=expected_profit,
        risk=risk,
        details={
            "event_id": event.id,
            "execution_mode": "IMMEDIATE_CONVERSION",
            "neg_risk_id": event.neg_risk_id,
            "neg_risk_type": event.neg_risk_type,
            "strategy_type": context.strategy_type.value,
        },
    )
    legs = tuple(item.leg(index) for index, item in enumerate(trades)) + (
        conversion_leg(len(trades), markets[0].id, Action.NEG_RISK_CONVERT, quantity),
    )
    return classify(context, calc, legs, tuple(item.book for item in trades))
