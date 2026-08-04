"""Shared pure mechanics for the four exact strategy evaluators."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from predmarket.domain.fees import FeeCalculator, FeeSchedule
from predmarket.domain.decimal import encode_decimal
from predmarket.domain.market import Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook
from predmarket.domain.signal import (
    Action,
    DecisionReason,
    NotEvaluable,
    OpportunityAbsent,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
    StrategyContext,
    StrategyDecision,
)
from predmarket.strategy.optimizer import (
    DepthFill,
    DepthRequirement,
    QuantityCandidate,
    candidate_quantities,
    constraint_root_quantities,
    select_candidates,
    walk_depth,
)
from predmarket.strategy.risk import (
    FailureScenario,
    OpenExposure,
    RiskResult,
    assess_failure_scenarios,
)


@dataclass(frozen=True, slots=True)
class Trade:
    market: Market
    token: Token
    book: OrderBook
    action: Action
    fill: DepthFill
    fee: Decimal
    fee_schedule: FeeSchedule

    @property
    def entry_cost(self) -> Decimal:
        if self.action is not Action.BUY:
            raise ValueError("entry_cost is defined only for BUY trades")
        return self.fill.gross_amount + self.fee

    @property
    def net_proceeds(self) -> Decimal:
        if self.action is not Action.SELL:
            raise ValueError("net_proceeds is defined only for SELL trades")
        return self.fill.gross_amount - self.fee

    def leg(self, position: int) -> SignalLeg:
        return SignalLeg(
            position=position,
            market_id=self.market.id,
            token_id=self.token.id,
            action=self.action,
            quantity=self.fill.quantity,
            average_price=self.fill.average_price,
            worst_price=self.fill.worst_price,
            gross_amount=self.fill.gross_amount,
            fee_amount=self.fee,
        )


@dataclass(frozen=True, slots=True)
class EvaluatedTrades:
    quantity: Decimal
    trades: tuple[Trade, ...]
    total_capital: Decimal
    expected_profit: Decimal
    risk: RiskResult


@dataclass(frozen=True, slots=True)
class TradeOptimization:
    candidate: EvaluatedTrades
    forced_absent_reason: DecisionReason | None = None


@dataclass(frozen=True, slots=True)
class TradeQuantityPlan:
    quantities: tuple[Decimal, ...]
    forced_absent_reason: DecisionReason | None = None


def not_evaluable(
    context: StrategyContext,
    reason: DecisionReason,
    detail: str,
) -> NotEvaluable:
    return NotEvaluable(
        reason_code=reason,
        context={
            "changed_token_id": context.changed_token_id,
            "detail": detail,
            "strategy_type": context.strategy_type.value,
        },
    )


def validate_inputs(
    context: StrategyContext,
    *,
    markets: tuple[Market, ...],
    tokens: tuple[Token, ...],
    actions: tuple[Action, ...],
) -> NotEvaluable | None:
    configuration_error = validate_configuration(context)
    if configuration_error is not None:
        return configuration_error
    if len(actions) != len(tokens) or any(
        action not in {Action.BUY, Action.SELL} for action in actions
    ):
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "trade_action_mapping_invalid",
        )
    if context.fee_schedule_max_age_seconds is None:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "fee_schedule_max_age_missing",
        )
    if context.changed_token_id not in {token.id for token in tokens}:
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "changed_token_not_affected",
        )
    if any(
        market.status is not MarketStatus.ACTIVE
        or not market.active
        or not market.accepting_orders
        or not market.enable_orderbook
        for market in markets
    ):
        return not_evaluable(context, DecisionReason.MARKET_CLOSED, "market_not_watchable")
    if any(
        not item.sync_generation_complete for item in (*markets, *tokens)
    ) or len({item.sync_generation for item in (*markets, *tokens)}) != 1:
        return not_evaluable(
            context,
            DecisionReason.SYNC_GENERATION_INCOMPLETE,
            "catalog_generation_incomplete",
        )
    market_ids = {market.id for market in markets}
    if any(token.market_id not in market_ids for token in tokens):
        return not_evaluable(
            context,
            DecisionReason.INPUT_METADATA_MISSING,
            "token_market_binding_invalid",
        )

    books_by_token = {book.token_id: book for book in context.orderbooks}
    required_ids = {token.id for token in tokens}
    if not required_ids.issubset(books_by_token):
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "required_orderbook_missing",
        )
    books = tuple(books_by_token[token.id] for token in tokens)
    if any(book.market_id != token.market_id for book, token in zip(books, tokens)):
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "orderbook_identity_invalid",
        )
    if len({book.subscription_generation for book in books}) != 1:
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "orderbook_generation_mismatch",
        )
    if any(book.received_timestamp > context.evaluated_at for book in books):
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "orderbook_from_future",
        )
    if any(book.exchange_timestamp > book.received_timestamp for book in books):
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "orderbook_timestamp_causality_invalid",
        )
    observed_at = context.orderbook_observed_at
    if observed_at is not None:
        if observed_at > context.evaluated_at:
            return not_evaluable(
                context,
                DecisionReason.ORDERBOOK_INVALID,
                "orderbook_observation_from_future",
            )
        if (
            context.evaluated_at - observed_at
            > context.configuration.maximum_book_age_ms
        ):
            return not_evaluable(
                context,
                DecisionReason.ORDERBOOK_STALE,
                "orderbook_subscription_stale",
            )
    else:
        if any(
            context.evaluated_at - book.exchange_timestamp
            > context.configuration.maximum_book_age_ms
            for book in books
        ):
            return not_evaluable(context, DecisionReason.ORDERBOOK_STALE, "orderbook_stale")
        exchange_times = [book.exchange_timestamp for book in books]
        if (
            max(exchange_times) - min(exchange_times)
            > context.configuration.maximum_leg_skew_ms
        ):
            return not_evaluable(
                context,
                DecisionReason.LEG_SKEW_EXCEEDED,
                "leg_exchange_timestamp_skew",
            )
    for book, action in zip(books, actions):
        execution_levels = book.asks if action is Action.BUY else book.bids
        if not execution_levels:
            return not_evaluable(
                context,
                DecisionReason.ORDERBOOK_INVALID,
                "execution_depth_missing",
            )

    for token in tokens:
        schedule = context.fee_schedules.get(token.id)
        if schedule is None:
            return not_evaluable(
                context,
                DecisionReason.FEE_SCHEDULE_UNKNOWN,
                "fee_schedule_missing",
            )
        try:
            stale = schedule.is_stale(
                evaluated_at=context.evaluated_at,
                max_age_seconds=context.fee_schedule_max_age_seconds,
            )
        except ValueError:
            stale = True
        if stale:
            return not_evaluable(
                context,
                DecisionReason.FEE_SCHEDULE_STALE,
                "fee_schedule_stale",
            )
    return None


def validate_configuration(context: StrategyContext) -> NotEvaluable | None:
    config = context.configuration
    decimal_fields = (
        ("bankroll", config.bankroll, False, None),
        ("minimum_return_rate", config.minimum_return_rate, True, None),
        ("maximum_risk_rate", config.maximum_risk_rate, True, Decimal("1")),
        ("maximum_unhedged_notional", config.maximum_unhedged_notional, True, None),
        ("safety_buffer_rate", config.safety_buffer_rate, True, Decimal("1")),
        ("conversion_cost", config.conversion_cost, True, None),
    )
    for name, value, allow_zero, maximum in decimal_fields:
        if (
            not isinstance(value, Decimal)
            or not value.is_finite()
            or value < 0
            or (not allow_zero and value == 0)
            or (maximum is not None and value > maximum)
        ):
            return not_evaluable(
                context,
                DecisionReason.INPUT_METADATA_MISSING,
                f"invalid_configuration_{name}",
            )
    for name, value in (
        ("maximum_book_age_ms", config.maximum_book_age_ms),
        ("maximum_leg_skew_ms", config.maximum_leg_skew_ms),
    ):
        if type(value) is not int or value < 0:
            return not_evaluable(
                context,
                DecisionReason.INPUT_METADATA_MISSING,
                f"invalid_configuration_{name}",
            )
    return None


def token_by_outcome(tokens: Iterable[Token], outcome: str) -> Token | None:
    matched = [token for token in tokens if token.outcome.casefold() == outcome.casefold()]
    return matched[0] if len(matched) == 1 else None


def trade(
    context: StrategyContext,
    market: Market,
    token: Token,
    action: Action,
    quantity: Decimal,
) -> Trade:
    book = next(book for book in context.orderbooks if book.token_id == token.id)
    levels = book.asks if action is Action.BUY else book.bids
    fill = walk_depth(levels, quantity)
    schedule = context.fee_schedules[token.id]
    fee = FeeCalculator.calculate(
        schedule,
        fill.average_price,
        quantity,
        evaluated_at_ms=context.evaluated_at,
        max_age_seconds=context.fee_schedule_max_age_seconds,  # type: ignore[arg-type]
    )
    return Trade(market, token, book, action, fill, fee, schedule)


def plan_trade_quantities(
    context: StrategyContext,
    *,
    trade_specs: tuple[tuple[Market, Token, Action], ...],
) -> TradeQuantityPlan | None:
    requirements = tuple(
        DepthRequirement(
            next(book for book in context.orderbooks if book.token_id == token.id),
            "BUY" if action is Action.BUY else "SELL",
        )
        for _, token, action in trade_specs
    )
    minimum = max(
        *(requirement.book.minimum_order_size for requirement in requirements),
        *(market.minimum_order_size or Decimal("0") for market, _, _ in trade_specs),
    )

    maximum_depth = min(
        sum((level.size for level in requirement.levels), Decimal("0"))
        for requirement in requirements
    )
    if maximum_depth < minimum:
        if maximum_depth <= 0:
            return None
        return TradeQuantityPlan(
            (maximum_depth,),
            DecisionReason.QUANTITY_BELOW_MINIMUM,
        )

    extra_breakpoints: set[Decimal] = set()
    for requirement in requirements:
        for levels in (requirement.book.bids, requirement.book.asks):
            cumulative = Decimal("0")
            for level in levels:
                cumulative += level.size
                if minimum <= cumulative <= maximum_depth:
                    extra_breakpoints.add(cumulative)

    quantities = candidate_quantities(
        requirements,
        minimum_quantity=minimum,
        default_quantity=max(Decimal("1"), minimum),
        extra_breakpoints=tuple(extra_breakpoints),
    )
    return TradeQuantityPlan(quantities)


def trade_root_quantities(
    context: StrategyContext,
    candidates: tuple[EvaluatedTrades, ...],
) -> tuple[Decimal, ...]:
    closed = tuple(_quantity_candidate(context, item) for item in candidates)
    return constraint_root_quantities(closed)


def select_trade_optimization(
    context: StrategyContext,
    candidates: tuple[EvaluatedTrades, ...],
    *,
    forced_absent_reason: DecisionReason | None = None,
) -> TradeOptimization | None:
    if not candidates:
        return None
    if forced_absent_reason is not None:
        return TradeOptimization(candidates[0], forced_absent_reason)
    selection = select_candidates(
        tuple(_quantity_candidate(context, item) for item in candidates)
    )
    if selection is None:
        return None
    if selection.feasible:
        return TradeOptimization(selection.candidate)
    bankroll_candidates = tuple(
        item
        for item in selection.candidates
        if item.total_capital <= context.configuration.bankroll
    )
    if bankroll_candidates:
        selected = max(
            bankroll_candidates,
            key=lambda item: (
                item.expected_profit,
                -item.total_capital,
                -item.quantity,
            ),
        )
        return TradeOptimization(selected)
    selected = min(
        selection.candidates,
        key=lambda item: (item.total_capital, item.quantity),
    )
    return TradeOptimization(selected, DecisionReason.INSUFFICIENT_CAPITAL)


def _quantity_candidate(
    context: StrategyContext,
    candidate: EvaluatedTrades,
) -> QuantityCandidate[EvaluatedTrades]:
    margins = {
        "bankroll": candidate.total_capital - context.configuration.bankroll,
        "profit": -candidate.expected_profit,
        "return": (
            context.configuration.minimum_return_rate * candidate.total_capital
            - candidate.expected_profit
        ),
    }
    for scenario in candidate.risk.scenarios:
        margins[f"risk:{scenario.name}"] = (
            scenario.loss
            - context.configuration.maximum_risk_rate * candidate.total_capital
        )
        margins[f"unhedged:{scenario.name}"] = (
            scenario.unhedged_notional
            - context.configuration.maximum_unhedged_notional
        )
    feasible = all(margin <= 0 for margin in margins.values())
    return QuantityCandidate(
        evaluation=candidate,
        quantity=candidate.quantity,
        total_capital=candidate.total_capital,
        expected_profit=candidate.expected_profit,
        constraint_margins=tuple(margins.items()),
        feasible=feasible,
    )


def feasibility_details(
    context: StrategyContext,
    optimized: TradeOptimization,
) -> dict[str, str]:
    if optimized.forced_absent_reason is DecisionReason.INSUFFICIENT_CAPITAL:
        return {
            "available_bankroll": encode_decimal(context.configuration.bankroll),
            "required_capital": encode_decimal(optimized.candidate.total_capital),
        }
    return {}


def long_entry_risk(
    context: StrategyContext,
    trades: tuple[Trade, ...],
    *,
    total_capital: Decimal,
) -> RiskResult:
    scenarios: list[FailureScenario] = []
    for index in range(1, len(trades)):
        prefix = trades[:index]
        scenarios.append(
            FailureScenario(
                "FIRST_LEG_ONLY" if index == 1 else f"PARTIAL_LEGS_{index}",
                sum((item.entry_cost for item in prefix), Decimal("0")),
                tuple(_exposure(item) for item in prefix),
                sum((item.entry_cost for item in prefix), Decimal("0")),
            )
        )
    scenarios.append(
        FailureScenario(
            "CONVERSION_FAILURE",
            total_capital,
            tuple(_exposure(item) for item in trades),
            Decimal("0"),
        )
    )
    return assess_failure_scenarios(
        tuple(scenarios),
        evaluated_at_ms=context.evaluated_at,
        fee_max_age_seconds=context.fee_schedule_max_age_seconds,  # type: ignore[arg-type]
    )


def split_inventory_risk(
    context: StrategyContext,
    trades: tuple[Trade, ...],
    *,
    total_capital: Decimal,
) -> RiskResult:
    scenarios: list[FailureScenario] = []
    for sold_count in range(0, len(trades)):
        sold = trades[:sold_count]
        unsold = trades[sold_count:]
        net_sold = sum((item.net_proceeds for item in sold), Decimal("0"))
        remaining_capital = max(Decimal("0"), total_capital - net_sold)
        allocation = remaining_capital / Decimal(len(unsold))
        exposures = tuple(
            OpenExposure(
                item.token.id,
                item.fill.quantity,
                allocation,
                item.book,
                item.fee_schedule,
            )
            for item in unsold
        )
        name = "SPLIT_ONLY" if sold_count == 0 else f"PARTIAL_SALES_{sold_count}"
        unhedged = Decimal("0") if sold_count == 0 else trades[0].fill.quantity
        scenarios.append(
            FailureScenario(name, remaining_capital, exposures, unhedged)
        )
    return assess_failure_scenarios(
        tuple(scenarios),
        evaluated_at_ms=context.evaluated_at,
        fee_max_age_seconds=context.fee_schedule_max_age_seconds,  # type: ignore[arg-type]
    )


def calculation(
    *,
    quantity: Decimal,
    total_capital: Decimal,
    expected_profit: Decimal,
    risk: RiskResult,
    details: dict[str, object],
) -> OpportunityCalculation:
    return OpportunityCalculation(
        quantity=quantity,
        total_capital=total_capital,
        expected_profit=expected_profit,
        return_rate=expected_profit / total_capital,
        worst_case_loss=risk.worst_case_loss,
        risk_rate=risk.worst_case_loss / total_capital,
        unhedged_notional=risk.unhedged_notional,
        risk_flags=risk.risk_flags,
        details=details,
    )


def classify(
    context: StrategyContext,
    calculation_value: OpportunityCalculation,
    legs: tuple[SignalLeg, ...],
    evidence: tuple[OrderBook, ...],
    forced_reason: DecisionReason | None = None,
) -> StrategyDecision:
    reason = forced_reason
    if reason is None and (
        calculation_value.expected_profit <= 0
        or calculation_value.return_rate < context.configuration.minimum_return_rate
    ):
        reason = DecisionReason.PROFIT_BELOW_THRESHOLD
    elif reason is None and (
        calculation_value.risk_rate > context.configuration.maximum_risk_rate
        or calculation_value.unhedged_notional
        > context.configuration.maximum_unhedged_notional
    ):
        reason = DecisionReason.RISK_ABOVE_THRESHOLD
    if reason is not None:
        return OpportunityAbsent(reason, calculation_value, legs, evidence)
    return OpportunityPresent(calculation_value, legs, evidence)


def conversion_leg(
    position: int,
    market_id: str,
    action: Action,
    quantity: Decimal,
    fee_amount: Decimal = Decimal("0"),
) -> SignalLeg:
    return SignalLeg(
        position=position,
        market_id=market_id,
        token_id=None,
        action=action,
        quantity=quantity,
        average_price=None,
        worst_price=None,
        gross_amount=quantity,
        fee_amount=fee_amount,
    )


def _exposure(item: Trade) -> OpenExposure:
    return OpenExposure(
        item.token.id,
        item.fill.quantity,
        item.entry_cost,
        item.book,
        item.fee_schedule,
    )
