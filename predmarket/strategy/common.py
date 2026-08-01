"""Shared pure mechanics for the four exact strategy evaluators."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from predmarket.domain.fees import FeeCalculator, FeeSchedule
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
    optimize_quantity,
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
) -> NotEvaluable | None:
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
    if any(
        context.evaluated_at - book.exchange_timestamp
        > context.configuration.maximum_book_age_ms
        for book in books
    ):
        return not_evaluable(context, DecisionReason.ORDERBOOK_STALE, "orderbook_stale")
    exchange_times = [book.exchange_timestamp for book in books]
    if max(exchange_times) - min(exchange_times) > context.configuration.maximum_leg_skew_ms:
        return not_evaluable(
            context,
            DecisionReason.LEG_SKEW_EXCEEDED,
            "leg_exchange_timestamp_skew",
        )
    if any(not book.asks or not book.bids for book in books):
        return not_evaluable(
            context,
            DecisionReason.ORDERBOOK_INVALID,
            "two_sided_depth_missing",
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


def optimize_trades(
    context: StrategyContext,
    *,
    trade_specs: tuple[tuple[Market, Token, Action], ...],
    economics,
) -> Decimal | None:
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

    def evaluate(quantity: Decimal) -> tuple[Decimal, Decimal]:
        trades = tuple(
            trade(context, market, token, action, quantity)
            for market, token, action in trade_specs
        )
        return economics(quantity, trades)

    return optimize_quantity(
        requirements,
        minimum_quantity=minimum,
        default_quantity=max(Decimal("1"), minimum),
        bankroll=context.configuration.bankroll,
        evaluate=evaluate,
    )


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
            )
        )
    scenarios.append(
        FailureScenario(
            "CONVERSION_FAILURE",
            total_capital,
            tuple(_exposure(item) for item in trades),
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
        scenarios.append(FailureScenario(name, remaining_capital, exposures))
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
) -> StrategyDecision:
    reason: DecisionReason | None = None
    if (
        calculation_value.expected_profit <= 0
        or calculation_value.return_rate < context.configuration.minimum_return_rate
    ):
        reason = DecisionReason.PROFIT_BELOW_THRESHOLD
    elif (
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
        fee_amount=Decimal("0"),
    )


def _exposure(item: Trade) -> OpenExposure:
    return OpenExposure(
        item.token.id,
        item.fill.quantity,
        item.entry_cost,
        item.book,
        item.fee_schedule,
    )
