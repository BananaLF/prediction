from __future__ import annotations

from dataclasses import fields
from decimal import Decimal

import pytest

from predmarket.catalog.relations import (
    DeterministicFakeAnalyzer,
    RelationAnalysis,
    RelationDetector,
    RelationWorkflow,
)
from predmarket.domain.market import Event, Market, MarketStatus
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus


def _event(event_id: str, market_ids: tuple[str, ...]) -> Event:
    return Event(
        id=event_id,
        title=f"Event {event_id}",
        status=MarketStatus.ACTIVE,
        market_ids=market_ids,
        sync_generation="sync-1",
        sync_generation_complete=True,
        created_at=1,
        updated_at=1,
    )


def _market(market_id: str, event_id: str) -> Market:
    return Market(
        id=market_id,
        event_id=event_id,
        condition_id=f"condition-{market_id}",
        question=f"Question {market_id}?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
        created_at=1,
        updated_at=1,
    )


def test_detector_emits_only_direction_selected_by_the_rule() -> None:
    events = (_event("event-1", ("market-a", "market-b")),)
    markets = (_market("market-b", "event-1"), _market("market-a", "event-1"))
    detector = RelationDetector(
        lambda _event_a, market_a, _event_b, market_b: (
            market_a.id,
            market_b.id,
        )
        == ("market-a", "market-b")
    )

    first = detector.detect(events, markets)
    second = detector.detect(tuple(reversed(events)), tuple(reversed(markets)))

    assert first == second
    assert [(candidate.market_a_id, candidate.market_b_id) for candidate in first] == [
        ("market-a", "market-b")
    ]
    relation = first[0].to_relation(discovered_at=10)
    assert relation.status is RelationStatus.NO_LLM_APPROVE
    assert relation.discovery_source is DiscoverySource.RULE
    assert relation.created_at == relation.updated_at == 10


def test_default_detector_does_not_guess_or_auto_approve_relations() -> None:
    events = (_event("event-1", ("market-a", "market-b")),)
    markets = (_market("market-a", "event-1"), _market("market-b", "event-1"))

    assert RelationDetector().detect(events, markets) == []
    assert {field.name for field in fields(Relation)} == {
        "id",
        "market_a_id",
        "market_b_id",
        "status",
        "discovery_source",
        "created_at",
        "updated_at",
        "llm_confidence",
        "llm_analysis",
    }


class _MemoryRelations:
    def __init__(self, relation: Relation) -> None:
        self.relation = relation
        self.analysis_writes = 0

    async def get(self, relation_id: str) -> Relation | None:
        return self.relation if relation_id == self.relation.id else None

    async def save_analysis(self, relation: Relation) -> None:
        self.analysis_writes += 1
        self.relation = relation


def _unreviewed_relation() -> Relation:
    return Relation(
        id="relation-1",
        market_a_id="market-a",
        market_b_id="market-b",
        status=RelationStatus.NO_LLM_APPROVE,
        discovery_source=DiscoverySource.RULE,
        created_at=10,
        updated_at=10,
    )


async def test_disabled_llm_leaves_relation_unreviewed_without_calling_analyzer() -> None:
    repository = _MemoryRelations(_unreviewed_relation())
    analyzer = DeterministicFakeAnalyzer(
        {
            "relation-1": RelationAnalysis(
                approved=True,
                confidence=Decimal("0.9"),
                reasoning="A implies B",
                warnings=(),
            )
        }
    )
    workflow = RelationWorkflow(repository, analyzer, llm_enabled=False)

    result = await workflow.analyze("relation-1", updated_at=11)

    assert result.status is RelationStatus.NO_LLM_APPROVE
    assert repository.analysis_writes == 0
    assert analyzer.calls == ()


async def test_only_positive_analyzer_result_advances_one_step() -> None:
    repository = _MemoryRelations(_unreviewed_relation())
    analysis = RelationAnalysis(
        approved=True,
        confidence=Decimal("0.875"),
        reasoning="The stricter outcome entails the broader outcome.",
        warnings=("Settlement wording must remain unchanged",),
    )
    analyzer = DeterministicFakeAnalyzer({"relation-1": analysis})
    workflow = RelationWorkflow(repository, analyzer, llm_enabled=True)

    result = await workflow.analyze("relation-1", updated_at=11)

    assert result.status is RelationStatus.LLM_APPROVE
    assert result.llm_confidence == Decimal("0.875")
    assert result.llm_analysis == {
        "approved": True,
        "reasoning": "The stricter outcome entails the broader outcome.",
        "warnings": ("Settlement wording must remain unchanged",),
    }
    assert analyzer.calls == ("relation-1",)
    assert repository.analysis_writes == 1


async def test_negative_analyzer_result_persists_analysis_without_approval() -> None:
    repository = _MemoryRelations(_unreviewed_relation())
    analyzer = DeterministicFakeAnalyzer(
        {
            "relation-1": RelationAnalysis(
                approved=False,
                confidence=Decimal("0.2"),
                reasoning="Resolution criteria differ.",
                warnings=(),
            )
        }
    )
    workflow = RelationWorkflow(repository, analyzer, llm_enabled=True)

    result = await workflow.analyze("relation-1", updated_at=11)

    assert result.status is RelationStatus.NO_LLM_APPROVE
    assert result.llm_analysis == {
        "approved": False,
        "reasoning": "Resolution criteria differ.",
        "warnings": (),
    }
    assert repository.analysis_writes == 1


async def test_analyzer_cannot_run_on_a_relation_past_the_llm_gate() -> None:
    relation = _unreviewed_relation().transition_to(
        RelationStatus.LLM_APPROVE,
        updated_at=11,
    )
    repository = _MemoryRelations(relation)
    analyzer = DeterministicFakeAnalyzer({})
    workflow = RelationWorkflow(repository, analyzer, llm_enabled=True)

    with pytest.raises(ValueError, match="NO_LLM_APPROVE"):
        await workflow.analyze("relation-1", updated_at=12)

    assert repository.analysis_writes == 0
    assert analyzer.calls == ()
