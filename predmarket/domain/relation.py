"""Immutable A-implies-B relation state machine."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


class RelationStatus(str, Enum):
    NO_LLM_APPROVE = "NO_LLM_APPROVE"
    LLM_APPROVE = "LLM_APPROVE"
    APPROVED = "APPROVED"


class DiscoverySource(str, Enum):
    RULE = "RULE"
    MANUAL = "MANUAL"


_NEXT_STATUS = {
    RelationStatus.NO_LLM_APPROVE: RelationStatus.LLM_APPROVE,
    RelationStatus.LLM_APPROVE: RelationStatus.APPROVED,
}


@dataclass(frozen=True)
class Relation:
    id: str
    market_a_id: str
    market_b_id: str
    status: RelationStatus
    discovery_source: DiscoverySource
    created_at: int
    updated_at: int
    llm_confidence: Decimal | None = None
    llm_analysis: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("relation id", self.id),
            ("market_a_id", self.market_a_id),
            ("market_b_id", self.market_b_id),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        if self.market_a_id == self.market_b_id:
            raise ValueError("relation markets must be different")
        if not isinstance(self.status, RelationStatus):
            raise ValueError("status must be a RelationStatus")
        if not isinstance(self.discovery_source, DiscoverySource):
            raise ValueError("discovery_source must be a DiscoverySource")
        if (
            type(self.created_at) is not int
            or self.created_at < 0
            or type(self.updated_at) is not int
            or self.updated_at < self.created_at
        ):
            raise ValueError("relation timestamps are invalid")
        if self.llm_confidence is not None:
            if (
                not isinstance(self.llm_confidence, Decimal)
                or not self.llm_confidence.is_finite()
                or not Decimal("0") <= self.llm_confidence <= Decimal("1")
            ):
                raise ValueError("llm_confidence must be a Decimal between zero and one")
        if self.llm_analysis is not None:
            if not isinstance(self.llm_analysis, Mapping):
                raise ValueError("llm_analysis must be a mapping")
            object.__setattr__(
                self, "llm_analysis", MappingProxyType(dict(self.llm_analysis))
            )

    def transition_to(self, status: RelationStatus, *, updated_at: int) -> "Relation":
        if _NEXT_STATUS.get(self.status) is not status:
            raise ValueError(f"invalid relation transition: {self.status.value} -> {status.value}")
        return replace(self, status=status, updated_at=updated_at)
