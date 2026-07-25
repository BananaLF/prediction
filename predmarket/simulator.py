"""Exact cash-flow simulation for executable action paths.

``minimum_received`` is terminal recoverable cash measured from the point of
maximum capital commitment: ``maximum_capital_used + minimum_profit``.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from predmarket.actions import Action, ActionKind, ActionPath
from predmarket.domain import PathKind, Side
from predmarket.exact_math import decimal_ratio
from predmarket.fees import FeeSchedule
from predmarket.orderbook import InsufficientDepth, OrderBook
from predmarket.relations import Relation, require_audited_active_relation


ZERO = Decimal("0")
ONE = Decimal("1")


def minimum_relation_received(relation: Relation, quantity: Decimal) -> Decimal:
    require_audited_active_relation(relation)
    quantity = _decimal(quantity, "quantity", positive=True)
    minimum = relation.minimum_units_received()
    if minimum < 1:
        raise ValueError("relation must guarantee minimum coverage")
    return Decimal(minimum) * quantity


def _decimal(
    value: object, name: str, *, positive: bool = False, signed: bool = False
) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if (positive and value <= ZERO) or (not positive and not signed and value < ZERO):
        qualifier = "positive" if positive else "nonnegative"
        raise ValueError(f"{name} must be {qualifier}")
    return value


@dataclass(frozen=True)
class SimulationResult:
    actions: tuple[Action, ...]
    quantity: Decimal
    maximum_capital_used: Decimal
    minimum_received: Decimal
    minimum_profit: Decimal
    minimum_return: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.actions, tuple) or not all(
            isinstance(action, Action) for action in self.actions
        ):
            raise TypeError("actions must be a tuple of Action values")
        if not self.actions:
            raise ValueError("actions must be non-empty")
        _decimal(self.quantity, "quantity", positive=True)
        capital = _decimal(self.maximum_capital_used, "maximum_capital_used", positive=True)
        received = _decimal(self.minimum_received, "minimum_received")
        profit = _decimal(self.minimum_profit, "minimum_profit", signed=True)
        rate = _decimal(self.minimum_return, "minimum_return", signed=True)
        if received - capital != profit:
            raise ValueError("minimum_received - maximum_capital_used must equal profit")
        if decimal_ratio(profit, capital) != rate:
            raise ValueError("minimum_return must equal profit / maximum_capital_used")


def _requirements(path: ActionPath) -> set[str]:
    return {
        action.token_id
        for action in path.actions
        if action.kind in (ActionKind.BUY, ActionKind.SELL)
        and action.token_id is not None
    }


def _validate_supported_actions(path: ActionPath) -> None:
    supported = {
        ActionKind.BUY,
        ActionKind.SELL,
        ActionKind.SPLIT,
        ActionKind.MERGE,
    }
    unsupported = tuple(
        action.kind for action in path.actions if action.kind not in supported
    )
    if unsupported:
        names = ", ".join(kind.value for kind in unsupported)
        raise ValueError(f"unsupported simulation action(s): {names}")


def _validate_binary_conservation(path: ActionPath) -> None:
    """Restrict Task 4 to its two inventory-conserving binary path families."""
    if path.kind is not PathKind.IMMEDIATE_CONVERSION:
        raise ValueError("binary paths must have kind IMMEDIATE_CONVERSION")
    kinds = tuple(action.kind for action in path.actions)
    underpriced = (ActionKind.BUY, ActionKind.BUY, ActionKind.MERGE)
    overpriced = (ActionKind.SPLIT, ActionKind.SELL, ActionKind.SELL)
    if kinds == underpriced:
        trading_actions = path.actions[:2]
        conversion_action = path.actions[2]
    elif kinds == overpriced:
        conversion_action = path.actions[0]
        trading_actions = path.actions[1:]
    else:
        raise ValueError("path must be a conserving binary BUY/BUY/MERGE or SPLIT/SELL/SELL")

    token_ids = tuple(action.token_id for action in trading_actions)
    if len(set(token_ids)) != 2:
        raise ValueError("binary trading legs must use distinct tokens")
    units = tuple(action.units for action in trading_actions)
    if units[0] != units[1] or conversion_action.units != units[0]:
        raise ValueError("binary trading and conversion legs must use matching units")


def _validate_inputs(
    path: ActionPath,
    books: Mapping[str, OrderBook],
    fees: Mapping[str, FeeSchedule],
) -> None:
    if not isinstance(path, ActionPath):
        raise TypeError("path must be an ActionPath")
    _validate_supported_actions(path)
    _validate_binary_conservation(path)
    required = _requirements(path)
    if set(books) != required:
        raise ValueError("books must cover trading tokens exactly")
    if set(fees) != required:
        raise ValueError("fees must cover trading tokens exactly")
    if any(not isinstance(book, OrderBook) for book in books.values()):
        raise TypeError("books values must be OrderBook")
    if any(key != book.token_id for key, book in books.items()):
        raise ValueError("book keys must match token IDs")
    if any(not isinstance(schedule, FeeSchedule) for schedule in fees.values()):
        raise TypeError("fees values must be FeeSchedule")


def _trade_cash(
    book: OrderBook, schedule: FeeSchedule, side: Side, quantity: Decimal
) -> tuple[Decimal, Decimal]:
    # walk first preserves the OrderBook's validation and insufficient-depth behavior.
    fill = book.walk(side, quantity)
    levels = book.asks if side is Side.BUY else book.bids
    remaining = quantity
    total_fee = ZERO
    for level in levels:
        consumed = min(remaining, level.size)
        if consumed:
            total_fee += schedule.taker_fee(consumed, level.price)
            remaining -= consumed
        if remaining == ZERO:
            break
    return fill.gross, total_fee


def simulate_path(
    path: ActionPath,
    quantity: Decimal,
    books: Mapping[str, OrderBook],
    fees: Mapping[str, FeeSchedule],
    safety_buffer: Decimal = ZERO,
    conversion_cost: Decimal = ZERO,
) -> SimulationResult:
    quantity = _decimal(quantity, "quantity", positive=True)
    safety_buffer = _decimal(safety_buffer, "safety_buffer")
    conversion_cost = _decimal(conversion_cost, "conversion_cost")
    _validate_inputs(path, books, fees)

    cash = ZERO
    maximum_deficit = ZERO
    trading_notional = ZERO
    for action in path.actions:
        action_quantity = quantity * action.units
        if action.kind in (ActionKind.BUY, ActionKind.SELL):
            assert action.token_id is not None and action.side is not None
            gross, trade_fee = _trade_cash(
                books[action.token_id], fees[action.token_id], action.side, action_quantity
            )
            trading_notional += gross
            cash += -(gross + trade_fee) if action.kind is ActionKind.BUY else gross - trade_fee
        elif action.kind is ActionKind.SPLIT:
            cash -= action_quantity + conversion_cost
        elif action.kind is ActionKind.MERGE:
            cash += action_quantity - conversion_cost
        maximum_deficit = max(maximum_deficit, -cash)

    buffer_cost = trading_notional * safety_buffer
    cash -= buffer_cost
    maximum_capital = maximum_deficit + buffer_cost
    if maximum_capital <= ZERO:
        raise ValueError("path does not require positive capital")
    received = maximum_capital + cash
    return SimulationResult(
        path.actions,
        quantity,
        maximum_capital,
        received,
        cash,
        decimal_ratio(cash, maximum_capital),
    )


def optimize_quantities(
    path: ActionPath,
    books: Mapping[str, OrderBook],
    fees: Mapping[str, FeeSchedule],
    safety_buffer: Decimal,
    conversion_cost: Decimal,
    bankroll: Decimal,
) -> tuple[SimulationResult, ...]:
    bankroll = _decimal(bankroll, "bankroll", positive=True)
    _decimal(safety_buffer, "safety_buffer")
    _decimal(conversion_cost, "conversion_cost")
    _validate_inputs(path, books, fees)

    candidates: set[Decimal] = set()
    trading_actions = [
        action for action in path.actions
        if action.kind in (ActionKind.BUY, ActionKind.SELL)
    ]
    for action in trading_actions:
        assert action.token_id is not None and action.side is not None
        book = books[action.token_id]
        candidates.add(book.minimum_order_size / action.units)
        cumulative = ZERO
        levels = book.asks if action.side is Side.BUY else book.bids
        for level in levels:
            cumulative += level.size
            candidates.add(cumulative / action.units)

    results: list[SimulationResult] = []
    for candidate in sorted(candidates):
        if any(
            candidate * action.units < books[action.token_id].minimum_order_size
            for action in trading_actions
            if action.token_id is not None
        ):
            continue
        try:
            result = simulate_path(
                path, candidate, books, fees, safety_buffer, conversion_cost
            )
        except InsufficientDepth:
            continue
        if result.maximum_capital_used <= bankroll:
            results.append(result)
    return tuple(sorted(results, key=lambda result: (
        result.maximum_capital_used, result.minimum_profit
    )))
