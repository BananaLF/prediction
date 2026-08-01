"""Conservative, auditable failure-scenario risk calculation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from predmarket.domain.fees import FeeCalculator, FeeSchedule
from predmarket.domain.orderbook import OrderBook
from predmarket.strategy.decimal_context import (
    MAX_FAILURE_SCENARIOS,
    MAX_STRATEGY_LEGS,
    bounded_sequence,
    isolated_decimal_context,
)


@dataclass(frozen=True, slots=True)
class OpenExposure:
    token_id: str
    quantity: Decimal
    entry_notional: Decimal
    book: OrderBook
    fee_schedule: FeeSchedule

    def __post_init__(self) -> None:
        if not isinstance(self.token_id, str) or not self.token_id:
            raise ValueError("token_id must be a non-empty string")
        _positive_decimal(self.quantity, "quantity")
        _nonnegative_decimal(self.entry_notional, "entry_notional")
        if not isinstance(self.book, OrderBook) or self.book.token_id != self.token_id:
            raise ValueError("book must match token_id")
        if not isinstance(self.fee_schedule, FeeSchedule):
            raise ValueError("fee_schedule must be a FeeSchedule")


@dataclass(frozen=True, slots=True)
class ImmediateClose:
    recovery_value: Decimal
    closed_quantity: Decimal
    uncloseable_quantity: Decimal


@dataclass(frozen=True, slots=True)
class FailureScenario:
    name: str
    capital_at_risk: Decimal
    exposures: tuple[OpenExposure, ...]
    unhedged_notional: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("name must be a non-empty string")
        _nonnegative_decimal(self.capital_at_risk, "capital_at_risk")
        _nonnegative_decimal(self.unhedged_notional, "unhedged_notional")
        materialized = bounded_sequence(
            self.exposures,
            field_name="exposures",
            max_items=MAX_STRATEGY_LEGS,
        )
        if not materialized or any(
            not isinstance(exposure, OpenExposure) for exposure in materialized
        ):
            raise ValueError("exposures must contain OpenExposure values")
        token_ids = [exposure.token_id for exposure in materialized]
        if len(token_ids) != len(set(token_ids)):
            raise ValueError("scenario exposures must have unique token IDs")
        object.__setattr__(
            self,
            "exposures",
            tuple(sorted(materialized, key=lambda item: item.token_id.encode("utf-8"))),
        )


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    name: str
    loss: Decimal
    recovery_value: Decimal
    unhedged_notional: Decimal
    uncloseable_quantity: Decimal


@dataclass(frozen=True, slots=True)
class RiskResult:
    worst_case_loss: Decimal
    unhedged_notional: Decimal
    risk_flags: tuple[str, ...]
    scenarios: tuple[ScenarioResult, ...]


@isolated_decimal_context(operation_depth=12)
def immediate_close_value(
    exposure: OpenExposure,
    *,
    evaluated_at_ms: int,
    fee_max_age_seconds: int,
) -> ImmediateClose:
    """Value a long token against visible bids; missing depth recovers zero."""

    if not isinstance(exposure, OpenExposure):
        raise ValueError("exposure must be an OpenExposure")
    remaining = exposure.quantity
    gross = Decimal("0")
    closed = Decimal("0")
    for level in exposure.book.bids:
        if remaining <= 0:
            break
        taken = min(remaining, level.size)
        gross += taken * level.price
        closed += taken
        remaining -= taken
    fee = Decimal("0")
    if closed > 0:
        fee = FeeCalculator.calculate(
            exposure.fee_schedule,
            gross / closed,
            closed,
            evaluated_at_ms=evaluated_at_ms,
            max_age_seconds=fee_max_age_seconds,
        )
    return ImmediateClose(
        recovery_value=gross - fee,
        closed_quantity=closed,
        uncloseable_quantity=remaining,
    )


def assess_failure_scenarios(
    scenarios: tuple[FailureScenario, ...],
    *,
    evaluated_at_ms: int,
    fee_max_age_seconds: int,
) -> RiskResult:
    """Aggregate exact scenario losses using conservative immediate recovery."""

    materialized = bounded_sequence(
        scenarios,
        field_name="scenarios",
        max_items=MAX_FAILURE_SCENARIOS,
    )
    return _assess_failure_scenarios(
        materialized,
        evaluated_at_ms=evaluated_at_ms,
        fee_max_age_seconds=fee_max_age_seconds,
    )


@isolated_decimal_context(operation_depth=20)
def _assess_failure_scenarios(
    scenarios: tuple[FailureScenario, ...],
    *,
    evaluated_at_ms: int,
    fee_max_age_seconds: int,
) -> RiskResult:
    materialized = scenarios
    if not materialized or any(
        not isinstance(scenario, FailureScenario) for scenario in materialized
    ):
        raise ValueError("scenarios must contain FailureScenario values")
    names = [scenario.name for scenario in materialized]
    if len(names) != len(set(names)):
        raise ValueError("scenario names must be unique")
    if type(evaluated_at_ms) is not int or evaluated_at_ms < 0:
        raise ValueError("evaluated_at_ms must be a non-negative integer")
    if type(fee_max_age_seconds) is not int or fee_max_age_seconds < 0:
        raise ValueError("fee_max_age_seconds must be a non-negative integer")

    results: list[ScenarioResult] = []
    flags: set[str] = set()
    for scenario in materialized:
        recovery = Decimal("0")
        uncloseable = Decimal("0")
        unhedged = scenario.unhedged_notional
        for exposure in scenario.exposures:
            close = immediate_close_value(
                exposure,
                evaluated_at_ms=evaluated_at_ms,
                fee_max_age_seconds=fee_max_age_seconds,
            )
            recovery += close.recovery_value
            uncloseable += close.uncloseable_quantity
        loss = max(Decimal("0"), scenario.capital_at_risk - recovery)
        if loss > 0:
            flags.add(scenario.name)
        if uncloseable > 0:
            flags.add("UNCLOSEABLE_EXPOSURE")
        results.append(
            ScenarioResult(
                name=scenario.name,
                loss=loss,
                recovery_value=recovery,
                unhedged_notional=unhedged,
                uncloseable_quantity=uncloseable,
            )
        )
    ordered = tuple(sorted(results, key=lambda item: item.name.encode("utf-8")))
    return RiskResult(
        worst_case_loss=max(item.loss for item in ordered),
        unhedged_notional=max(item.unhedged_notional for item in ordered),
        risk_flags=tuple(sorted(flags, key=lambda item: item.encode("utf-8"))),
        scenarios=ordered,
    )


def _positive_decimal(value: object, field_name: str) -> None:
    _nonnegative_decimal(value, field_name)
    if value <= 0:  # type: ignore[operator]
        raise ValueError(f"{field_name} must be greater than zero")


def _nonnegative_decimal(value: object, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")
