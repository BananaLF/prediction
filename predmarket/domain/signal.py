"""Strategy input and decision contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, TypeAlias

from predmarket.config import StrategyConfig
from predmarket.domain.fees import FeeSchedule
from predmarket.domain.market import Market, Token
from predmarket.domain.orderbook import OrderBook
from predmarket.domain.relation import Relation, RelationStatus


class StrategyType(str, Enum):
    BINARY_UNDERPRICED = "BINARY_UNDERPRICED"
    BINARY_OVERPRICED = "BINARY_OVERPRICED"
    LOGICAL_IMPLICATION = "LOGICAL_IMPLICATION"
    NEG_RISK_COMPLETE_SET = "NEG_RISK_COMPLETE_SET"


class ExecutionMode(str, Enum):
    IMMEDIATE_CONVERSION = "IMMEDIATE_CONVERSION"
    HOLD_TO_RESOLUTION = "HOLD_TO_RESOLUTION"


class Action(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    MERGE = "MERGE"
    SPLIT = "SPLIT"
    REDEEM = "REDEEM"
    NEG_RISK_CONVERT = "NEG_RISK_CONVERT"


class DecisionReason(str, Enum):
    PROFIT_BELOW_THRESHOLD = "PROFIT_BELOW_THRESHOLD"
    RISK_ABOVE_THRESHOLD = "RISK_ABOVE_THRESHOLD"
    INSUFFICIENT_DEPTH = "INSUFFICIENT_DEPTH"
    QUANTITY_BELOW_MINIMUM = "QUANTITY_BELOW_MINIMUM"
    MARKET_CLOSED = "MARKET_CLOSED"
    EVENT_SETTLED = "EVENT_SETTLED"
    ORDERBOOK_INVALID = "ORDERBOOK_INVALID"
    ORDERBOOK_STALE = "ORDERBOOK_STALE"
    LEG_SKEW_EXCEEDED = "LEG_SKEW_EXCEEDED"
    SDK_DISCONNECTED = "SDK_DISCONNECTED"
    INPUT_METADATA_MISSING = "INPUT_METADATA_MISSING"
    FEE_SCHEDULE_UNKNOWN = "FEE_SCHEDULE_UNKNOWN"
    FEE_SCHEDULE_STALE = "FEE_SCHEDULE_STALE"
    SYNC_GENERATION_INCOMPLETE = "SYNC_GENERATION_INCOMPLETE"
    RELATION_NOT_APPROVED = "RELATION_NOT_APPROVED"


_ABSENT_REASONS = {
    DecisionReason.PROFIT_BELOW_THRESHOLD,
    DecisionReason.RISK_ABOVE_THRESHOLD,
    DecisionReason.INSUFFICIENT_DEPTH,
    DecisionReason.QUANTITY_BELOW_MINIMUM,
}
_NOT_EVALUABLE_REASONS = set(DecisionReason) - _ABSENT_REASONS
_TRADE_ACTIONS = {Action.BUY, Action.SELL}


@dataclass(frozen=True)
class OpportunityCalculation:
    quantity: Decimal
    total_capital: Decimal
    expected_profit: Decimal
    return_rate: Decimal
    worst_case_loss: Decimal
    risk_rate: Decimal
    unhedged_notional: Decimal
    risk_flags: tuple[str, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("quantity", self.quantity),
            ("total_capital", self.total_capital),
            ("expected_profit", self.expected_profit),
            ("return_rate", self.return_rate),
            ("worst_case_loss", self.worst_case_loss),
            ("risk_rate", self.risk_rate),
            ("unhedged_notional", self.unhedged_notional),
        ):
            _finite_decimal(value, name)
        if self.quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if self.total_capital <= 0:
            raise ValueError("total_capital must be greater than zero")
        if self.worst_case_loss < 0:
            raise ValueError("worst_case_loss must not be negative")
        if self.unhedged_notional < 0:
            raise ValueError("unhedged_notional must not be negative")
        if self.return_rate != self.expected_profit / self.total_capital:
            raise ValueError("return_rate must equal expected_profit / total_capital")
        if self.risk_rate != self.worst_case_loss / self.total_capital:
            raise ValueError("risk_rate must equal worst_case_loss / total_capital")
        risk_flags = tuple(self.risk_flags)
        if any(not isinstance(flag, str) or not flag for flag in risk_flags):
            raise ValueError("risk_flags must contain non-empty strings")
        object.__setattr__(self, "risk_flags", risk_flags)
        if not isinstance(self.details, Mapping):
            raise ValueError("details must be a mapping")
        object.__setattr__(self, "details", _freeze_mapping(self.details))


@dataclass(frozen=True)
class SignalLeg:
    position: int
    market_id: str
    token_id: str | None
    action: Action
    quantity: Decimal
    average_price: Decimal | None
    worst_price: Decimal | None
    gross_amount: Decimal
    fee_amount: Decimal

    def __post_init__(self) -> None:
        if type(self.position) is not int or self.position < 0:
            raise ValueError("position must be non-negative")
        _identifier(self.market_id, "market_id")
        if not isinstance(self.action, Action):
            raise ValueError("action must be an Action")
        for name, value in (
            ("quantity", self.quantity),
            ("gross_amount", self.gross_amount),
            ("fee_amount", self.fee_amount),
        ):
            _finite_decimal(value, name)
        if self.quantity <= 0:
            raise ValueError("quantity must be greater than zero")
        if self.gross_amount < 0 or self.fee_amount < 0:
            raise ValueError("amounts must not be negative")
        if self.action in _TRADE_ACTIONS:
            _identifier(self.token_id, "token_id")
            for name, value in (
                ("average_price", self.average_price),
                ("worst_price", self.worst_price),
            ):
                _finite_decimal(value, name)
                if not Decimal("0") < value < Decimal("1"):  # type: ignore[operator]
                    raise ValueError(f"{name} must be in (0, 1)")
        elif (
            self.token_id is not None
            or self.average_price is not None
            or self.worst_price is not None
        ):
            raise ValueError("conversion actions cannot contain token_id or prices")

    @property
    def side(self) -> str | None:
        return self.action.value if self.action in _TRADE_ACTIONS else None


@dataclass(frozen=True)
class OpportunityPresent:
    calculation: OpportunityCalculation
    legs: tuple[SignalLeg, ...]
    evidence: tuple[OrderBook, ...]

    def __post_init__(self) -> None:
        _complete_payload(self.calculation, self.legs, self.evidence)
        object.__setattr__(self, "legs", tuple(self.legs))
        object.__setattr__(self, "evidence", tuple(self.evidence))


@dataclass(frozen=True)
class OpportunityAbsent:
    reason_code: DecisionReason
    calculation: OpportunityCalculation
    legs: tuple[SignalLeg, ...]
    evidence: tuple[OrderBook, ...]

    def __post_init__(self) -> None:
        if self.reason_code not in _ABSENT_REASONS:
            raise ValueError("reason_code is not valid for OpportunityAbsent")
        _complete_payload(self.calculation, self.legs, self.evidence)
        object.__setattr__(self, "legs", tuple(self.legs))
        object.__setattr__(self, "evidence", tuple(self.evidence))


@dataclass(frozen=True)
class NotEvaluable:
    reason_code: DecisionReason
    context: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.reason_code not in _NOT_EVALUABLE_REASONS:
            raise ValueError("reason_code is not valid for NotEvaluable")
        if not isinstance(self.context, Mapping) or not self.context:
            raise ValueError("context must be a non-empty mapping")
        object.__setattr__(self, "context", _freeze_mapping(self.context))


StrategyDecision: TypeAlias = OpportunityPresent | OpportunityAbsent | NotEvaluable


@dataclass(frozen=True)
class StrategyContext:
    strategy_type: StrategyType
    changed_token_id: str
    markets: tuple[Market, ...]
    tokens: tuple[Token, ...]
    approved_implication_relation: Relation | None
    orderbooks: tuple[OrderBook, ...]
    fee_schedules: Mapping[str, FeeSchedule]
    evaluated_at: int
    configuration: StrategyConfig

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_type, StrategyType):
            raise ValueError("strategy_type must be a StrategyType")
        _identifier(self.changed_token_id, "changed_token_id")
        markets = _sorted_unique(self.markets, Market, "markets")
        tokens = _sorted_unique(self.tokens, Token, "tokens")
        orderbooks = _sorted_unique(self.orderbooks, OrderBook, "orderbooks", key="token_id")
        if self.approved_implication_relation is not None and (
            not isinstance(self.approved_implication_relation, Relation)
            or self.approved_implication_relation.status is not RelationStatus.APPROVED
        ):
            raise ValueError("approved_implication_relation must be APPROVED")
        if not isinstance(self.fee_schedules, Mapping):
            raise ValueError("fee_schedules must be a mapping")
        fee_schedules = dict(self.fee_schedules)
        for token_id, schedule in fee_schedules.items():
            _identifier(token_id, "fee schedule token id")
            if not isinstance(schedule, FeeSchedule):
                raise ValueError("fee schedule values must be FeeSchedule instances")
        if type(self.evaluated_at) is not int or self.evaluated_at < 0:
            raise ValueError("evaluated_at must be a non-negative integer")
        if not isinstance(self.configuration, StrategyConfig):
            raise ValueError("configuration must be a StrategyConfig")
        object.__setattr__(self, "markets", markets)
        object.__setattr__(self, "tokens", tokens)
        object.__setattr__(self, "orderbooks", orderbooks)
        object.__setattr__(
            self,
            "fee_schedules",
            MappingProxyType(dict(sorted(fee_schedules.items()))),
        )


def _complete_payload(
    calculation: OpportunityCalculation,
    legs: tuple[SignalLeg, ...],
    evidence: tuple[OrderBook, ...],
) -> None:
    if not isinstance(calculation, OpportunityCalculation):
        raise ValueError("calculation must be an OpportunityCalculation")
    if not legs or any(not isinstance(leg, SignalLeg) for leg in legs):
        raise ValueError("legs must contain at least one SignalLeg")
    if not evidence or any(not isinstance(book, OrderBook) for book in evidence):
        raise ValueError("evidence must contain at least one OrderBook")


def _sorted_unique(
    values: tuple[Any, ...],
    item_type: type[Any],
    field_name: str,
    *,
    key: str = "id",
) -> tuple[Any, ...]:
    try:
        items = tuple(values)
    except TypeError as error:
        raise ValueError(f"{field_name} must be an iterable") from error
    if any(not isinstance(item, item_type) for item in items):
        raise ValueError(f"{field_name} contains an invalid item")
    identities = [getattr(item, key) for item in items]
    if len(identities) != len(set(identities)):
        raise ValueError(f"{field_name} must not contain duplicate IDs")
    return tuple(sorted(items, key=lambda item: getattr(item, key).encode("utf-8")))


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if any(not isinstance(key, str) for key in value):
        raise ValueError("mapping keys must be strings")
    return MappingProxyType({key: _freeze_value(item) for key, item in value.items()})


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    return value


def _identifier(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")


def _finite_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")
