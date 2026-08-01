"""Authoritative SDK-metadata NegRisk complete-set strategy."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from predmarket.domain.decimal import encode_decimal, parse_decimal
from predmarket.domain.market import MarketStatus
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
from predmarket.strategy.decimal_context import (
    StrategyNumericLimitError,
    isolated_decimal_context,
)


_MAPPING_VERSION = "polymarket-client-0.3.0b1:v1"


@dataclass(frozen=True, slots=True)
class _NegRiskSchema:
    action: Action
    enable_neg_risk: bool
    neg_risk_augmented: bool
    cumulative_markets: bool


_NEG_RISK_SCHEMAS = {
    "STANDARD": _NegRiskSchema(Action.NEG_RISK_CONVERT, True, False, False),
    "STANDARD_REDEEM": _NegRiskSchema(Action.REDEEM, False, False, False),
}


def evaluate_neg_risk(context: StrategyContext) -> StrategyDecision:
    try:
        return _evaluate_neg_risk(context)
    except StrategyNumericLimitError:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "strategy_numeric_limit",
        )


@isolated_decimal_context(operation_depth=48)
def _evaluate_neg_risk(context: StrategyContext) -> StrategyDecision:
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
    ):
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "neg_risk_event_proof_incomplete"
        )
    semantics = _conversion_semantics(event.neg_risk_type, event.neg_risk_metadata)
    if semantics is None:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "neg_risk_conversion_schema_invalid",
        )
    conversion_action, conversion_fee_rate = semantics
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
    invalid = validate_inputs(
        context,
        markets=markets,
        tokens=required_tokens,
        actions=tuple(Action.BUY for _ in required_tokens),
    )
    if invalid is not None:
        return invalid
    specs = tuple((market, token, Action.BUY) for market, token in required)

    def economics(quantity, trades):
        gross = sum((item.fill.gross_amount for item in trades), Decimal("0"))
        fees = sum((item.fee for item in trades), Decimal("0"))
        safety = gross * context.configuration.safety_buffer_rate
        conversion_fee = quantity * conversion_fee_rate
        capital = (
            gross
            + fees
            + conversion_fee
            + context.configuration.conversion_cost
            + safety
        )
        return capital, quantity - capital

    optimized = optimize_trades(
        context,
        trade_specs=specs,
        economics=economics,
        risk_evaluator=lambda trades, capital: long_entry_risk(
            context, trades, total_capital=capital
        ),
        decimal_inputs=(conversion_fee_rate,),
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
            "event_id": event.id,
            "execution_mode": "IMMEDIATE_CONVERSION",
            "neg_risk_id": event.neg_risk_id,
            "neg_risk_type": event.neg_risk_type,
            "conversion_action": conversion_action.value,
            "conversion_fee_rate": encode_decimal(conversion_fee_rate),
            "strategy_type": context.strategy_type.value,
            **feasibility_details(context, optimized),
        },
    )
    conversion_fee = quantity * conversion_fee_rate
    legs = tuple(item.leg(index) for index, item in enumerate(trades)) + (
        conversion_leg(
            len(trades),
            markets[0].id,
            conversion_action,
            quantity,
            conversion_fee,
        ),
    )
    return classify(
        context,
        calc,
        legs,
        tuple(item.book for item in trades),
        optimized.forced_absent_reason,
    )


def _conversion_semantics(
    neg_risk_type: str,
    metadata,
) -> tuple[Action, Decimal] | None:
    schema = _NEG_RISK_SCHEMAS.get(neg_risk_type)
    if schema is None or set(metadata) != {
        "mapping_version",
        "enable_neg_risk",
        "neg_risk_augmented",
        "cumulative_markets",
        "neg_risk_fee_bips",
    }:
        return None
    if (
        metadata.get("mapping_version") != _MAPPING_VERSION
        or type(metadata.get("enable_neg_risk")) is not bool
        or metadata.get("enable_neg_risk") is not schema.enable_neg_risk
        or type(metadata.get("neg_risk_augmented")) is not bool
        or metadata.get("neg_risk_augmented") is not schema.neg_risk_augmented
        or type(metadata.get("cumulative_markets")) is not bool
        or metadata.get("cumulative_markets") is not schema.cumulative_markets
    ):
        return None
    raw_bips = metadata.get("neg_risk_fee_bips")
    if not isinstance(raw_bips, str):
        return None
    try:
        bips = parse_decimal(raw_bips)
    except ValueError:
        return None
    if not Decimal("0") <= bips <= Decimal("10000"):
        return None
    if schema.action is Action.REDEEM and bips != 0:
        return None
    return schema.action, bips / Decimal("10000")
