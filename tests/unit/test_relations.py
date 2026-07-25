from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from predmarket.relations import (
    RelationStatus,
    RelationValidationError,
    load_relation,
    minimum_units_received,
)


EXAMPLE = Path(__file__).parents[2] / "rules" / "example-implication.yaml"


def write_relation(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "relation.yaml"
    path.write_text(text)
    return path


def valid_yaml(*, status: str = "active", review: str = """
semantic_review:
  reviewer: Alice Auditor
  reviewed_at: 2026-07-26
  conclusion: The enumerated states match the reviewed implication.
""") -> str:
    return f"""
relation_id: test-relation
version: 1
status: {status}
source_rules_hash: reviewed-hash
legs:
  - token_id: no_a
    weight: 1
  - token_id: yes_b
    weight: 1
states:
  - name: a_false_b_false
    proceeds: {{no_a: 1, yes_b: 0}}
  - name: a_false_b_true
    proceeds: {{no_a: 1, yes_b: 1}}
  - name: a_true_b_true
    proceeds: {{no_a: 0, yes_b: 1}}
{review}
"""


def test_loads_audited_example_and_computes_minimum() -> None:
    relation = load_relation(EXAMPLE)

    assert relation.relation_id == "a-implies-b"
    assert relation.status is RelationStatus.ACTIVE
    assert len(relation.states) == 3
    assert [leg.weight for leg in relation.legs] == [1, 1]
    assert minimum_units_received(relation) == 1
    assert relation.semantic_review is not None
    assert relation.semantic_review.reviewer


def test_active_requires_semantic_review(tmp_path: Path) -> None:
    path = write_relation(tmp_path, valid_yaml(review=""))
    with pytest.raises(RelationValidationError, match="semantic_review"):
        load_relation(path)


def test_pending_may_omit_semantic_review(tmp_path: Path) -> None:
    relation = load_relation(write_relation(tmp_path, valid_yaml(status="pending", review="")))
    assert relation.status is RelationStatus.PENDING
    assert relation.semantic_review is None


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("- not\n- a\n- mapping\n", "root"),
        ("relation_id: only-field\n", "version"),
        (valid_yaml().replace("status: active", "status: retired"), "status"),
        (valid_yaml().replace("version: 1", "version: 0"), "version"),
        (valid_yaml().replace("version: 1", "version: true"), "version"),
        (valid_yaml().replace("source_rules_hash: reviewed-hash", "source_rules_hash: ''"), "source_rules_hash"),
        (valid_yaml().replace("relation_id: test-relation", "relation_id: ''"), "relation_id"),
    ],
)
def test_rejects_malformed_top_level_fields(tmp_path: Path, text: str, message: str) -> None:
    with pytest.raises(RelationValidationError, match=message):
        load_relation(write_relation(tmp_path, text))


def test_rejects_duplicate_legs(tmp_path: Path) -> None:
    text = valid_yaml().replace("token_id: yes_b", "token_id: no_a")
    with pytest.raises(RelationValidationError, match="duplicate.*token"):
        load_relation(write_relation(tmp_path, text))


def test_rejects_duplicate_states(tmp_path: Path) -> None:
    text = valid_yaml().replace("name: a_false_b_true", "name: a_false_b_false")
    with pytest.raises(RelationValidationError, match="duplicate.*state"):
        load_relation(write_relation(tmp_path, text))


@pytest.mark.parametrize("weight", ["0", "2", "true", "'1'"])
def test_rejects_non_unit_integer_weights(tmp_path: Path, weight: str) -> None:
    text = valid_yaml().replace("weight: 1", f"weight: {weight}", 1)
    with pytest.raises(RelationValidationError, match="weight"):
        load_relation(write_relation(tmp_path, text))


@pytest.mark.parametrize(
    ("proceeds", "message"),
    [
        ("{no_a: 1}", "missing"),
        ("{no_a: 1, yes_b: 0, surprise: 1}", "extra"),
        ("{no_a: true, yes_b: 0}", "proceeds"),
        ("{no_a: 2, yes_b: 0}", "proceeds"),
    ],
)
def test_rejects_invalid_payoff_tokens_and_values(
    tmp_path: Path, proceeds: str, message: str
) -> None:
    text = valid_yaml().replace("{no_a: 1, yes_b: 0}", proceeds, 1)
    with pytest.raises(RelationValidationError, match=message):
        load_relation(write_relation(tmp_path, text))


def test_relation_and_nested_values_are_immutable() -> None:
    relation = load_relation(EXAMPLE)

    with pytest.raises(FrozenInstanceError):
        relation.version = 2
    with pytest.raises(TypeError):
        relation.states[0].proceeds["no_a"] = 0
    with pytest.raises(FrozenInstanceError):
        relation.legs[0].weight = 2


def test_unsafe_yaml_tags_are_not_executed(tmp_path: Path) -> None:
    path = write_relation(
        tmp_path,
        "!!python/object/apply:pathlib.Path [['unsafe-marker']]\n",
    )
    with pytest.raises(RelationValidationError):
        load_relation(path)
