"""Pure affected-token strategy dispatcher."""

from __future__ import annotations

from predmarket.domain.signal import DecisionReason, StrategyContext, StrategyDecision, StrategyType
from predmarket.strategy.binary import evaluate_binary
from predmarket.strategy.common import not_evaluable
from predmarket.strategy.decimal_context import isolated_decimal_context
from predmarket.strategy.implication import evaluate_implication
from predmarket.strategy.neg_risk import evaluate_neg_risk


class StrategyEngine:
    @isolated_decimal_context(operation_depth=56)
    def evaluate(self, context: StrategyContext) -> StrategyDecision:
        if not isinstance(context, StrategyContext):
            raise TypeError("context must be a StrategyContext")
        if context.changed_token_id not in {token.id for token in context.tokens}:
            return not_evaluable(
                context, DecisionReason.INPUT_METADATA_MISSING, "changed_token_not_affected"
            )
        if context.strategy_type in {
            StrategyType.BINARY_UNDERPRICED,
            StrategyType.BINARY_OVERPRICED,
        }:
            return evaluate_binary(context)
        if context.strategy_type is StrategyType.LOGICAL_IMPLICATION:
            return evaluate_implication(context)
        if context.strategy_type is StrategyType.NEG_RISK_COMPLETE_SET:
            return evaluate_neg_risk(context)
        return not_evaluable(
            context, DecisionReason.INPUT_METADATA_MISSING, "unsupported_strategy_type"
        )
