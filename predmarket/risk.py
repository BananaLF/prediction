from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from predmarket.domain import OpportunityStatus


def _validate_decimal(name: str, value: object, *, nonnegative: bool = False) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if nonnegative and value < Decimal("0"):
        raise ValueError(f"{name} must be nonnegative")


@dataclass(frozen=True)
class RiskInputs:
    mathematical_return: Decimal
    data_valid: bool
    worst_leg_failure_loss: Decimal
    max_unhedged_notional: Decimal
    immediate_unwind_known: bool
    unresolved_rule_risk: bool
    unresolved_conversion_risk: bool
    unresolved_settlement_risk: bool
    release_date_known: bool

    def __post_init__(self) -> None:
        _validate_decimal("mathematical_return", self.mathematical_return)
        _validate_decimal(
            "worst_leg_failure_loss",
            self.worst_leg_failure_loss,
            nonnegative=True,
        )
        _validate_decimal(
            "max_unhedged_notional",
            self.max_unhedged_notional,
            nonnegative=True,
        )
        for name in (
            "data_valid",
            "immediate_unwind_known",
            "unresolved_rule_risk",
            "unresolved_conversion_risk",
            "unresolved_settlement_risk",
            "release_date_known",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be bool")


@dataclass(frozen=True)
class RiskAssessment:
    status: OpportunityStatus
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.status, OpportunityStatus):
            raise TypeError("status must be OpportunityStatus")
        if type(self.reasons) is not tuple:
            raise TypeError("reasons must be tuple")
        if any(type(reason) is not str for reason in self.reasons):
            raise TypeError("reasons must contain only strings")
        if any(not reason for reason in self.reasons):
            raise ValueError("reasons must be nonempty")
        if len(set(self.reasons)) != len(self.reasons):
            raise ValueError("reasons must be unique")


@dataclass(frozen=True)
class PartialFillRisk:
    worst_leg_failure_loss: Decimal
    max_unhedged_notional: Decimal

    def __post_init__(self) -> None:
        _validate_decimal(
            "worst_leg_failure_loss",
            self.worst_leg_failure_loss,
            nonnegative=True,
        )
        _validate_decimal(
            "max_unhedged_notional",
            self.max_unhedged_notional,
            nonnegative=True,
        )


def assess_risk(
    inputs: RiskInputs,
    minimum_return: Decimal,
    max_loss: Decimal,
    max_unhedged: Decimal,
) -> RiskAssessment:
    if not isinstance(inputs, RiskInputs):
        raise TypeError("inputs must be RiskInputs")
    _validate_decimal("minimum_return", minimum_return)
    _validate_decimal("max_loss", max_loss, nonnegative=True)
    _validate_decimal("max_unhedged", max_unhedged, nonnegative=True)
    if minimum_return <= Decimal("0"):
        raise ValueError("minimum_return must be positive")

    hard_reasons: list[str] = []
    if inputs.mathematical_return < minimum_return:
        hard_reasons.append("return_below_minimum")
    if not inputs.data_valid:
        hard_reasons.append("data_invalid")
    if inputs.worst_leg_failure_loss > max_loss:
        hard_reasons.append("loss_exceeds_limit")
    if inputs.max_unhedged_notional > max_unhedged:
        hard_reasons.append("unhedged_notional_exceeds_limit")
    if inputs.unresolved_rule_risk:
        hard_reasons.append("unresolved_rule_risk")
    if inputs.unresolved_conversion_risk:
        hard_reasons.append("unresolved_conversion_risk")
    if inputs.unresolved_settlement_risk:
        hard_reasons.append("unresolved_settlement_risk")
    if hard_reasons:
        return RiskAssessment(OpportunityStatus.REJECTED, tuple(hard_reasons))

    research_reasons: list[str] = []
    if not inputs.immediate_unwind_known:
        research_reasons.append("immediate_unwind_unknown")
    if not inputs.release_date_known:
        research_reasons.append("release_date_unknown")
    if research_reasons:
        return RiskAssessment(
            OpportunityStatus.RESEARCH_CANDIDATE,
            tuple(research_reasons),
        )
    return RiskAssessment(OpportunityStatus.SNAPSHOT_EXECUTABLE, ())


def _copy_and_validate_legs(name: str, values: object) -> dict[str, Decimal]:
    if not isinstance(values, Mapping):
        raise TypeError(f"{name} must be a mapping")
    copied = dict(values)
    if not copied:
        raise ValueError(f"{name} must be nonempty")
    for leg, value in copied.items():
        if type(leg) is not str:
            raise TypeError(f"{name} leg keys must be strings")
        if not leg:
            raise ValueError(f"{name} leg keys must be nonempty")
        _validate_decimal(f"{name}[{leg!r}]", value, nonnegative=True)
    return copied


def worst_partial_fill(
    entry_costs: Mapping[str, Decimal],
    immediate_unwind_values: Mapping[str, Decimal],
) -> PartialFillRisk:
    entries = _copy_and_validate_legs("entry_costs", entry_costs)
    unwinds = _copy_and_validate_legs(
        "immediate_unwind_values",
        immediate_unwind_values,
    )
    if entries.keys() != unwinds.keys():
        raise ValueError("entry and unwind legs must match exactly")

    zero = Decimal("0")
    return PartialFillRisk(
        worst_leg_failure_loss=max(
            max(zero, entries[leg] - unwinds[leg]) for leg in entries
        ),
        max_unhedged_notional=max(entries.values()),
    )
