from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import yaml


class RelationValidationError(ValueError):
    """A relation file does not satisfy the audited relation schema."""


class RelationStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"


@dataclass(frozen=True)
class RelationLeg:
    token_id: str
    weight: int


@dataclass(frozen=True)
class RelationState:
    name: str
    proceeds: Mapping[str, int]


@dataclass(frozen=True)
class SemanticReview:
    reviewer: str
    reviewed_at: str
    conclusion: str


@dataclass(frozen=True)
class Relation:
    relation_id: str
    version: int
    status: RelationStatus
    source_rules_hash: str
    legs: tuple[RelationLeg, ...]
    states: tuple[RelationState, ...]
    semantic_review: SemanticReview | None


def _mapping(value: object, field: str) -> dict[object, object]:
    if not isinstance(value, dict):
        raise RelationValidationError(f"{field} must be a mapping")
    return value


def _sequence(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise RelationValidationError(f"{field} must be a sequence")
    return value


def _required(mapping: dict[object, object], field: str) -> object:
    if field not in mapping:
        raise RelationValidationError(f"missing required field: {field}")
    return mapping[field]


def _nonempty_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RelationValidationError(f"{field} must be a non-empty string")
    return value


def _parse_review(value: object) -> SemanticReview:
    review = _mapping(value, "semantic_review")
    reviewer = _nonempty_text(_required(review, "reviewer"), "semantic_review.reviewer")
    reviewed_at_value = _required(review, "reviewed_at")
    if not isinstance(reviewed_at_value, (str, date, datetime)) or not str(
        reviewed_at_value
    ).strip():
        raise RelationValidationError(
            "semantic_review.reviewed_at must be a non-empty date or string"
        )
    conclusion = _nonempty_text(
        _required(review, "conclusion"), "semantic_review.conclusion"
    )
    return SemanticReview(
        reviewer=reviewer,
        reviewed_at=str(reviewed_at_value),
        conclusion=conclusion,
    )


def load_relation(path: Path) -> Relation:
    try:
        raw = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        raise RelationValidationError(f"could not load relation YAML: {exc}") from exc

    root = _mapping(raw, "YAML root")
    relation_id = _nonempty_text(_required(root, "relation_id"), "relation_id")

    version = _required(root, "version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise RelationValidationError("version must be a positive integer")

    status_value = _required(root, "status")
    try:
        status = RelationStatus(status_value)
    except (ValueError, TypeError) as exc:
        raise RelationValidationError(
            f"status must be one of: {', '.join(item.value for item in RelationStatus)}"
        ) from exc

    source_rules_hash = _nonempty_text(
        _required(root, "source_rules_hash"), "source_rules_hash"
    )

    leg_items = _sequence(_required(root, "legs"), "legs")
    if not leg_items:
        raise RelationValidationError("legs must contain at least one leg")
    legs: list[RelationLeg] = []
    token_ids: set[str] = set()
    for index, item in enumerate(leg_items):
        leg = _mapping(item, f"legs[{index}]")
        token_id = _nonempty_text(
            _required(leg, "token_id"), f"legs[{index}].token_id"
        )
        if token_id in token_ids:
            raise RelationValidationError(f"duplicate leg token_id: {token_id}")
        token_ids.add(token_id)
        weight = _required(leg, "weight")
        if isinstance(weight, bool) or not isinstance(weight, int) or weight != 1:
            raise RelationValidationError(
                f"legs[{index}].weight must be the integer 1"
            )
        legs.append(RelationLeg(token_id=token_id, weight=weight))

    state_items = _sequence(_required(root, "states"), "states")
    if not state_items:
        raise RelationValidationError("states must contain at least one state")
    states: list[RelationState] = []
    state_names: set[str] = set()
    for index, item in enumerate(state_items):
        state = _mapping(item, f"states[{index}]")
        name = _nonempty_text(_required(state, "name"), f"states[{index}].name")
        if name in state_names:
            raise RelationValidationError(f"duplicate state name: {name}")
        state_names.add(name)
        proceeds = _mapping(
            _required(state, "proceeds"), f"states[{index}].proceeds"
        )
        payoff_tokens = set(proceeds)
        missing = token_ids - payoff_tokens
        extra = payoff_tokens - token_ids
        if missing:
            raise RelationValidationError(
                f"states[{index}].proceeds has missing tokens: "
                f"{sorted(missing, key=repr)}"
            )
        if extra:
            raise RelationValidationError(
                f"states[{index}].proceeds has extra tokens: "
                f"{sorted(extra, key=repr)}"
            )
        validated_proceeds: dict[str, int] = {}
        for token_id, payoff in proceeds.items():
            if (
                not isinstance(token_id, str)
                or isinstance(payoff, bool)
                or not isinstance(payoff, int)
                or payoff not in (0, 1)
            ):
                raise RelationValidationError(
                    f"states[{index}].proceeds values must be integer 0 or 1"
                )
            validated_proceeds[token_id] = payoff
        states.append(
            RelationState(name=name, proceeds=MappingProxyType(validated_proceeds))
        )

    review_value = root.get("semantic_review")
    semantic_review = (
        None if review_value is None else _parse_review(review_value)
    )
    if status is RelationStatus.ACTIVE and semantic_review is None:
        raise RelationValidationError("active relations require semantic_review")

    return Relation(
        relation_id=relation_id,
        version=version,
        status=status,
        source_rules_hash=source_rules_hash,
        legs=tuple(legs),
        states=tuple(states),
        semantic_review=semantic_review,
    )


def minimum_units_received(relation: Relation) -> int:
    minimum = min(
        sum(state.proceeds[leg.token_id] * leg.weight for leg in relation.legs)
        for state in relation.states
    )
    if minimum < 0:
        raise RelationValidationError("minimum units received cannot be negative")
    return minimum
