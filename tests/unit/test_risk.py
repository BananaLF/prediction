from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from predmarket.domain import OpportunityStatus
from predmarket.risk import (
    PartialFillRisk,
    RiskAssessment,
    RiskInputs,
    assess_risk,
    worst_partial_fill,
)


class DecimalSubclass(Decimal):
    pass


def executable_inputs(**overrides: object) -> RiskInputs:
    values: dict[str, object] = {
        "mathematical_return": Decimal("0.008"),
        "data_valid": True,
        "worst_leg_failure_loss": Decimal("5"),
        "max_unhedged_notional": Decimal("10"),
        "immediate_unwind_known": True,
        "unresolved_rule_risk": False,
        "unresolved_conversion_risk": False,
        "unresolved_settlement_risk": False,
        "release_date_known": True,
    }
    values.update(overrides)
    return RiskInputs(**values)  # type: ignore[arg-type]


def assess(inputs: RiskInputs) -> RiskAssessment:
    return assess_risk(
        inputs,
        minimum_return=Decimal("0.0075"),
        max_loss=Decimal("5"),
        max_unhedged=Decimal("10"),
    )


def test_base_case_is_snapshot_executable() -> None:
    assert assess(executable_inputs()) == RiskAssessment(
        status=OpportunityStatus.SNAPSHOT_EXECUTABLE,
        reasons=(),
    )


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"mathematical_return": Decimal("0.0074")}, "return_below_minimum"),
        ({"data_valid": False}, "data_invalid"),
        ({"worst_leg_failure_loss": Decimal("5.01")}, "loss_exceeds_limit"),
        ({"max_unhedged_notional": Decimal("10.01")}, "unhedged_notional_exceeds_limit"),
        ({"unresolved_rule_risk": True}, "unresolved_rule_risk"),
        ({"unresolved_conversion_risk": True}, "unresolved_conversion_risk"),
        ({"unresolved_settlement_risk": True}, "unresolved_settlement_risk"),
    ],
)
def test_each_hard_gate_rejects_independently(
    override: dict[str, object], reason: str
) -> None:
    assert assess(executable_inputs(**override)) == RiskAssessment(
        OpportunityStatus.REJECTED, (reason,)
    )


def test_multiple_rejection_reasons_have_deterministic_gate_order() -> None:
    result = assess(
        executable_inputs(
            mathematical_return=Decimal("0"),
            data_valid=False,
            worst_leg_failure_loss=Decimal("6"),
            max_unhedged_notional=Decimal("11"),
            unresolved_rule_risk=True,
            unresolved_conversion_risk=True,
            unresolved_settlement_risk=True,
        )
    )
    assert result == RiskAssessment(
        OpportunityStatus.REJECTED,
        (
            "return_below_minimum",
            "data_invalid",
            "loss_exceeds_limit",
            "unhedged_notional_exceeds_limit",
            "unresolved_rule_risk",
            "unresolved_conversion_risk",
            "unresolved_settlement_risk",
        ),
    )


def test_equal_threshold_boundaries_pass_and_decimal_threshold_is_exact() -> None:
    inputs = executable_inputs(
        mathematical_return=Decimal("0.0075"),
        worst_leg_failure_loss=Decimal("5"),
        max_unhedged_notional=Decimal("10"),
    )
    assert assess(inputs).status is OpportunityStatus.SNAPSHOT_EXECUTABLE
    with pytest.raises(TypeError):
        assess_risk(inputs, 0.0075, Decimal("5"), Decimal("10"))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("override", "reasons"),
    [
        ({"immediate_unwind_known": False}, ("immediate_unwind_unknown",)),
        ({"release_date_known": False}, ("release_date_unknown",)),
        (
            {"immediate_unwind_known": False, "release_date_known": False},
            ("immediate_unwind_unknown", "release_date_unknown"),
        ),
    ],
)
def test_research_only_states(
    override: dict[str, object], reasons: tuple[str, ...]
) -> None:
    assert assess(executable_inputs(**override)) == RiskAssessment(
        OpportunityStatus.RESEARCH_CANDIDATE, reasons
    )


def test_hard_rejections_take_precedence_over_research_reasons() -> None:
    assert assess(
        executable_inputs(
            data_valid=False,
            immediate_unwind_known=False,
            release_date_known=False,
        )
    ) == RiskAssessment(OpportunityStatus.REJECTED, ("data_invalid",))


@pytest.mark.parametrize(
    "field",
    [
        "data_valid",
        "immediate_unwind_known",
        "unresolved_rule_risk",
        "unresolved_conversion_risk",
        "unresolved_settlement_risk",
        "release_date_known",
    ],
)
@pytest.mark.parametrize("value", [0, 1, "true", None])
def test_risk_inputs_require_exact_booleans(field: str, value: object) -> None:
    with pytest.raises(TypeError):
        executable_inputs(**{field: value})


@pytest.mark.parametrize(
    "field",
    ["mathematical_return", "worst_leg_failure_loss", "max_unhedged_notional"],
)
@pytest.mark.parametrize("value", [1, 0.1, "1", None])
def test_risk_inputs_require_decimals(field: str, value: object) -> None:
    with pytest.raises(TypeError):
        executable_inputs(**{field: value})


def test_risk_inputs_reject_decimal_subclasses() -> None:
    with pytest.raises(TypeError):
        executable_inputs(mathematical_return=DecimalSubclass("0.008"))


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
@pytest.mark.parametrize(
    "field",
    ["mathematical_return", "worst_leg_failure_loss", "max_unhedged_notional"],
)
def test_risk_inputs_reject_nonfinite_decimals(field: str, value: Decimal) -> None:
    with pytest.raises(ValueError):
        executable_inputs(**{field: value})


@pytest.mark.parametrize("field", ["worst_leg_failure_loss", "max_unhedged_notional"])
def test_risk_inputs_reject_negative_exposures(field: str) -> None:
    with pytest.raises(ValueError):
        executable_inputs(**{field: Decimal("-0.01")})


@pytest.mark.parametrize(
    ("thresholds", "error"),
    [
        (("0.1", Decimal("1"), Decimal("1")), TypeError),
        ((Decimal("NaN"), Decimal("1"), Decimal("1")), ValueError),
        ((Decimal("0"), Decimal("1"), Decimal("1")), ValueError),
        ((Decimal("-0.1"), Decimal("1"), Decimal("1")), ValueError),
        ((Decimal("0.1"), Decimal("-1"), Decimal("1")), ValueError),
        ((Decimal("0.1"), Decimal("1"), Decimal("-1")), ValueError),
    ],
)
def test_assess_risk_validates_thresholds_strictly(
    thresholds: tuple[object, object, object], error: type[Exception]
) -> None:
    with pytest.raises(error):
        assess_risk(executable_inputs(), *thresholds)  # type: ignore[arg-type]


def test_assess_risk_rejects_decimal_subclass_thresholds() -> None:
    with pytest.raises(TypeError):
        assess_risk(
            executable_inputs(),
            DecimalSubclass("0.0075"),
            Decimal("5"),
            Decimal("10"),
        )


def test_risk_value_objects_are_strict_and_immutable() -> None:
    assessment = RiskAssessment(OpportunityStatus.REJECTED, ("a", "b"))
    partial = PartialFillRisk(Decimal("1"), Decimal("2"))
    with pytest.raises(FrozenInstanceError):
        assessment.reasons = ()  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        partial.worst_leg_failure_loss = Decimal("0")  # type: ignore[misc]
    with pytest.raises(TypeError):
        RiskAssessment("REJECTED", ())  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RiskAssessment(OpportunityStatus.REJECTED, ["a"])  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RiskAssessment(OpportunityStatus.REJECTED, ("a", "a"))
    with pytest.raises(TypeError):
        RiskAssessment(OpportunityStatus.REJECTED, ("a", 1))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        RiskAssessment(OpportunityStatus.REJECTED, ("",))


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("worst_leg_failure_loss", 1, TypeError),
        ("max_unhedged_notional", "2", TypeError),
        ("worst_leg_failure_loss", Decimal("NaN"), ValueError),
        ("max_unhedged_notional", Decimal("Infinity"), ValueError),
        ("worst_leg_failure_loss", Decimal("-1"), ValueError),
        ("max_unhedged_notional", Decimal("-1"), ValueError),
    ],
)
def test_partial_fill_risk_validates_values(
    field: str, value: object, error: type[Exception]
) -> None:
    values = {
        "worst_leg_failure_loss": Decimal("1"),
        "max_unhedged_notional": Decimal("2"),
    }
    values[field] = value
    with pytest.raises(error):
        PartialFillRisk(**values)  # type: ignore[arg-type]


def test_partial_fill_risk_rejects_decimal_subclasses() -> None:
    with pytest.raises(TypeError):
        PartialFillRisk(DecimalSubclass("1"), Decimal("2"))


def test_worst_partial_fill_exact_example_and_does_not_mutate_inputs() -> None:
    entries = {"a": Decimal("38"), "b": Decimal("57")}
    unwinds = {"a": Decimal("35"), "b": Decimal("55")}
    assert worst_partial_fill(entries, unwinds) == PartialFillRisk(
        worst_leg_failure_loss=Decimal("3"),
        max_unhedged_notional=Decimal("57"),
    )
    assert entries == {"a": Decimal("38"), "b": Decimal("57")}
    assert unwinds == {"a": Decimal("35"), "b": Decimal("55")}


def test_unwind_above_entry_has_zero_loss() -> None:
    result = worst_partial_fill({"a": Decimal("2")}, {"a": Decimal("3")})
    assert result.worst_leg_failure_loss == Decimal("0")


@pytest.mark.parametrize(
    ("entries", "unwinds", "error"),
    [
        ({}, {}, ValueError),
        ({"a": Decimal("1")}, {}, ValueError),
        ({"a": Decimal("1")}, {"a": Decimal("1"), "b": Decimal("1")}, ValueError),
        ({"": Decimal("1")}, {"": Decimal("1")}, ValueError),
        ({"a": Decimal("-1")}, {"a": Decimal("0")}, ValueError),
        ({"a": Decimal("1")}, {"a": Decimal("-1")}, ValueError),
        ({"a": Decimal("NaN")}, {"a": Decimal("0")}, ValueError),
        ({"a": 1}, {"a": Decimal("0")}, TypeError),
        (["a"], {"a": Decimal("0")}, TypeError),
    ],
)
def test_worst_partial_fill_rejects_bad_mappings(
    entries: object, unwinds: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        worst_partial_fill(entries, unwinds)  # type: ignore[arg-type]


def test_worst_partial_fill_rejects_decimal_subclass_mapping_values() -> None:
    with pytest.raises(TypeError):
        worst_partial_fill(
            {"a": DecimalSubclass("1")},
            {"a": Decimal("0")},
        )


money = st.decimals(
    min_value=Decimal("0"),
    max_value=Decimal("100000"),
    allow_nan=False,
    allow_infinity=False,
    places=4,
)


@given(entry=money, unwind=money, decrease=money)
def test_lowering_unwind_cannot_reduce_worst_loss(
    entry: Decimal, unwind: Decimal, decrease: Decimal
) -> None:
    lower = max(Decimal("0"), unwind - decrease)
    original = worst_partial_fill({"a": entry}, {"a": unwind})
    changed = worst_partial_fill({"a": entry}, {"a": lower})
    assert changed.worst_leg_failure_loss >= original.worst_leg_failure_loss


@given(entry=money, unwind=money, increase=money)
def test_increasing_entry_cannot_reduce_max_unhedged(
    entry: Decimal, unwind: Decimal, increase: Decimal
) -> None:
    original = worst_partial_fill({"a": entry}, {"a": unwind})
    changed = worst_partial_fill({"a": entry + increase}, {"a": unwind})
    assert changed.max_unhedged_notional >= original.max_unhedged_notional


@given(a_entry=money, a_unwind=money, b_entry=money, b_unwind=money)
def test_adding_a_leg_cannot_reduce_partial_fill_maxima(
    a_entry: Decimal,
    a_unwind: Decimal,
    b_entry: Decimal,
    b_unwind: Decimal,
) -> None:
    original = worst_partial_fill({"a": a_entry}, {"a": a_unwind})
    changed = worst_partial_fill(
        {"a": a_entry, "b": b_entry},
        {"a": a_unwind, "b": b_unwind},
    )
    assert changed.worst_leg_failure_loss >= original.worst_leg_failure_loss
    assert changed.max_unhedged_notional >= original.max_unhedged_notional
