from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
from pathlib import Path

import pytest

from predmarket.actions import (
    ActionKind,
    implication_path,
    neg_risk_complete_set_path,
)
from predmarket.domain import PathKind, Side
from predmarket.relations import RelationStatus, load_relation
from predmarket.simulator import (
    minimum_relation_received,
    optimize_quantities,
    simulate_path,
)


D = Decimal
EXAMPLE = Path(__file__).parents[2] / "rules" / "example-implication.yaml"


def test_implication_path_plans_declared_buys_then_redemption() -> None:
    relation = load_relation(EXAMPLE)

    path = implication_path(relation)

    assert path.path_id == "a-implies-b"
    assert path.kind is PathKind.HOLD_TO_RESOLUTION
    assert [action.kind for action in path.actions] == [
        ActionKind.BUY,
        ActionKind.BUY,
        ActionKind.REDEEM,
    ]
    assert [action.token_id for action in path.actions] == ["no_a", "yes_b", None]
    assert [action.side for action in path.actions] == [Side.BUY, Side.BUY, None]
    assert [action.units for action in path.actions] == [D("1"), D("1"), D("1")]


def test_minimum_relation_received_scales_exactly() -> None:
    assert minimum_relation_received(load_relation(EXAMPLE), D("100")) == D("100")


@pytest.mark.parametrize("planner", ["path", "minimum"])
def test_relation_planning_requires_active_relation(planner: str) -> None:
    relation = load_relation(EXAMPLE)
    candidate = replace(relation, status=RelationStatus.PENDING)
    with pytest.raises(ValueError, match="active"):
        if planner == "path":
            implication_path(candidate)
        else:
            minimum_relation_received(candidate, D("1"))


def test_implication_path_requires_semantic_review() -> None:
    relation = replace(load_relation(EXAMPLE), semantic_review=None)
    with pytest.raises(ValueError, match="review"):
        implication_path(relation)


@pytest.mark.parametrize("planner", ["path", "minimum"])
def test_relation_planning_rejects_zero_guaranteed_coverage(planner: str) -> None:
    relation = load_relation(EXAMPLE)
    zero_states = tuple(
        replace(state, proceeds={leg.token_id: 0 for leg in relation.legs})
        for state in relation.states
    )
    relation = replace(relation, states=zero_states)

    with pytest.raises(ValueError, match="coverage|minimum"):
        if planner == "path":
            implication_path(relation)
        else:
            minimum_relation_received(relation, D("1"))


@pytest.mark.parametrize(
    "quantity",
    [1, "1", D("NaN"), D("Infinity"), D("0"), D("-1")],
)
def test_minimum_relation_received_rejects_bad_quantity(quantity: object) -> None:
    with pytest.raises((TypeError, ValueError), match="quantity"):
        minimum_relation_received(load_relation(EXAMPLE), quantity)  # type: ignore[arg-type]


def test_neg_risk_complete_set_path_preserves_token_order() -> None:
    path = neg_risk_complete_set_path(("alpha", "beta", "gamma"), True)

    assert path.path_id == "neg-risk-complete-set"
    assert path.kind is PathKind.IMMEDIATE_CONVERSION
    assert [action.kind for action in path.actions] == [
        ActionKind.BUY,
        ActionKind.BUY,
        ActionKind.BUY,
        ActionKind.NEG_RISK_CONVERT,
    ]
    assert [action.token_id for action in path.actions] == [
        "alpha",
        "beta",
        "gamma",
        None,
    ]
    assert all(action.side is Side.BUY for action in path.actions[:-1])
    assert all(action.units == D("1") for action in path.actions)


@pytest.mark.parametrize("enabled", [False, 1, "true", None])
def test_neg_risk_requires_explicit_boolean_enablement(enabled: object) -> None:
    with pytest.raises((TypeError, ValueError), match="conversion_enabled"):
        neg_risk_complete_set_path(("alpha", "beta"), enabled)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "tokens",
    [(), ("alpha",), ("alpha", "alpha"), ("alpha", ""), ("alpha", 2)],
)
def test_neg_risk_rejects_invalid_complete_sets(tokens: tuple[object, ...]) -> None:
    with pytest.raises((TypeError, ValueError), match="token"):
        neg_risk_complete_set_path(tokens, True)  # type: ignore[arg-type]


def test_planned_paths_are_deeply_immutable() -> None:
    implication = implication_path(load_relation(EXAMPLE))
    neg_risk = neg_risk_complete_set_path(("alpha", "beta"), True)

    with pytest.raises(FrozenInstanceError):
        implication.actions[0].units = D("2")
    with pytest.raises(TypeError):
        neg_risk.actions[0] = neg_risk.actions[1]


@pytest.mark.parametrize("family", ["implication", "neg-risk"])
@pytest.mark.parametrize("entry_point", ["simulate", "optimize"])
def test_generic_simulator_rejects_new_path_families(
    family: str, entry_point: str
) -> None:
    path = (
        implication_path(load_relation(EXAMPLE))
        if family == "implication"
        else neg_risk_complete_set_path(("alpha", "beta"), True)
    )

    with pytest.raises(ValueError, match="unsupported|binary|IMMEDIATE"):
        if entry_point == "simulate":
            simulate_path(path, D("1"), {}, {})
        else:
            optimize_quantities(path, {}, {}, D("0"), D("0"), D("100"))
