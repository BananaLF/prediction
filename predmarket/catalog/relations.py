"""Deterministic implication discovery, LLM gating, and activation monitoring."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
import hashlib
import hmac
import inspect
import json
import logging
import math
from pathlib import Path
import sqlite3
import time
from typing import Protocol

import aiosqlite

from predmarket.domain.market import Event, Market, MarketStatus
from predmarket.domain.decimal import decode_decimal, encode_decimal
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus


RelationRule = Callable[[Event, Market, Event, Market], bool]
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RelationCandidate:
    """One directed rule result; it never carries an approval state."""

    id: str
    market_a_id: str
    market_b_id: str

    def __post_init__(self) -> None:
        if not self.id or not self.market_a_id or not self.market_b_id:
            raise ValueError("relation candidate IDs must be non-empty")
        if self.market_a_id == self.market_b_id:
            raise ValueError("relation candidate markets must differ")

    def to_relation(self, *, discovered_at: int) -> Relation:
        if type(discovered_at) is not int or discovered_at < 0:
            raise ValueError("discovered_at must be a non-negative integer")
        return Relation(
            id=self.id,
            market_a_id=self.market_a_id,
            market_b_id=self.market_b_id,
            status=RelationStatus.NO_LLM_APPROVE,
            discovery_source=DiscoverySource.RULE,
            created_at=discovered_at,
            updated_at=discovered_at,
        )


class RelationDetector:
    """Apply an explicit deterministic predicate to ordered market pairs.

    The default predicate deliberately discovers nothing. Logical settlement
    semantics must be supplied explicitly rather than guessed from prose.
    """

    def __init__(self, rule: RelationRule | None = None) -> None:
        if rule is not None and not callable(rule):
            raise TypeError("rule must be callable")
        self._rule = rule

    def detect(
        self,
        events: Sequence[Event],
        markets: Sequence[Market],
    ) -> list[RelationCandidate]:
        normalized_events = _typed_by_id(events, Event, "events")
        normalized_markets = _typed_by_id(markets, Market, "markets")
        if self._rule is None:
            return []

        eligible: list[tuple[Event, Market]] = []
        for market in normalized_markets.values():
            event = normalized_events.get(market.event_id)
            if event is None:
                raise ValueError(f"market {market.id!r} references a missing event")
            if market.id not in event.market_ids:
                raise ValueError(
                    f"market {market.id!r} is absent from event {event.id!r}"
                )
            if (
                event.sync_generation_complete
                and market.sync_generation_complete
                and event.status is MarketStatus.ACTIVE
                and market.status is MarketStatus.ACTIVE
                and market.active
                and not event.neg_risk
                and not market.neg_risk
            ):
                eligible.append((event, market))
        eligible.sort(key=lambda item: item[1].id.encode("utf-8"))

        candidates: list[RelationCandidate] = []
        for event_a, market_a in eligible:
            for event_b, market_b in eligible:
                if market_a.id == market_b.id:
                    continue
                if self._rule(event_a, market_a, event_b, market_b):
                    candidates.append(
                        RelationCandidate(
                            id=_candidate_id(market_a.id, market_b.id),
                            market_a_id=market_a.id,
                            market_b_id=market_b.id,
                        )
                    )
        return candidates


@dataclass(frozen=True, slots=True)
class RelationAnalysis:
    approved: bool
    confidence: Decimal
    reasoning: str
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.approved) is not bool:
            raise ValueError("approved must be a boolean")
        if (
            not isinstance(self.confidence, Decimal)
            or not self.confidence.is_finite()
            or not Decimal("0") <= self.confidence <= Decimal("1")
        ):
            raise ValueError("confidence must be a Decimal between zero and one")
        if not isinstance(self.reasoning, str) or not self.reasoning.strip():
            raise ValueError("reasoning must be a non-empty string")
        if isinstance(self.warnings, (str, bytes)):
            raise ValueError("warnings must be an iterable of strings")
        try:
            warnings = tuple(self.warnings)
        except TypeError as error:
            raise ValueError("warnings must be an iterable of strings") from error
        if any(not isinstance(item, str) or not item.strip() for item in warnings):
            raise ValueError("warnings must contain non-empty strings")
        object.__setattr__(self, "warnings", warnings)


class RelationAnalyzer(Protocol):
    def analyze(
        self,
        relation: Relation,
    ) -> RelationAnalysis | Awaitable[RelationAnalysis]: ...


class RelationAnalysisRepository(Protocol):
    async def get(self, relation_id: str) -> Relation | None: ...

    async def get_for_analysis(self, relation_id: str) -> Relation | None: ...

    async def save_analysis(
        self,
        relation: Relation,
        *,
        expected_semantic_digest: str,
    ) -> Relation: ...


class DeterministicFakeAnalyzer:
    """Explicit, side-effect-free analyzer used by tests and local fixtures."""

    def __init__(self, decisions: Mapping[str, RelationAnalysis]) -> None:
        materialized = dict(decisions)
        if any(
            not isinstance(key, str)
            or not key
            or not isinstance(value, RelationAnalysis)
            for key, value in materialized.items()
        ):
            raise ValueError("decisions must map relation IDs to RelationAnalysis")
        self._decisions = materialized
        self._calls: list[str] = []

    @property
    def calls(self) -> tuple[str, ...]:
        return tuple(self._calls)

    def analyze(self, relation: Relation) -> RelationAnalysis:
        if not isinstance(relation, Relation):
            raise TypeError("relation must be a Relation")
        self._calls.append(relation.id)
        try:
            return self._decisions[relation.id]
        except KeyError as error:
            raise ValueError(f"no fake analysis configured for {relation.id!r}") from error


class RelationWorkflow:
    """The sole application path from unreviewed to LLM-recommended."""

    def __init__(
        self,
        repository: RelationAnalysisRepository,
        analyzer: RelationAnalyzer,
        *,
        llm_enabled: bool,
    ) -> None:
        if type(llm_enabled) is not bool:
            raise ValueError("llm_enabled must be a boolean")
        self._repository = repository
        self._analyzer = analyzer
        self._llm_enabled = llm_enabled

    async def analyze(self, relation_id: str, *, updated_at: int) -> Relation:
        if not isinstance(relation_id, str) or not relation_id:
            raise ValueError("relation_id must be a non-empty string")
        if type(updated_at) is not int or updated_at < 0:
            raise ValueError("updated_at must be a non-negative integer")
        relation = await self._repository.get(relation_id)
        if relation is None:
            raise ValueError(f"relation {relation_id!r} does not exist")
        if not self._llm_enabled:
            return relation
        relation = await self._repository.get_for_analysis(relation_id)
        if relation is None:
            raise ValueError(f"relation {relation_id!r} does not exist")
        if relation.status is not RelationStatus.NO_LLM_APPROVE:
            raise ValueError("analyzer requires relation status NO_LLM_APPROVE")
        if updated_at < relation.updated_at:
            raise ValueError("updated_at must not move backwards")

        expected_semantic_digest = semantic_evidence_digest(relation)
        result = self._analyzer.analyze(relation)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, RelationAnalysis):
            raise TypeError("analyzer must return RelationAnalysis")
        analyzed = replace(
            relation,
            status=(
                RelationStatus.LLM_APPROVE
                if result.approved
                else RelationStatus.NO_LLM_APPROVE
            ),
            llm_confidence=result.confidence,
            llm_analysis={
                "approved": result.approved,
                "reasoning": result.reasoning,
                "warnings": result.warnings,
                "semantic_evidence": relation.llm_analysis["semantic_evidence"],
            },
            updated_at=updated_at,
        )
        return await _await_owned_analysis_save(
            self._repository.save_analysis(
                analyzed,
                expected_semantic_digest=expected_semantic_digest,
            )
        )


async def _await_owned_analysis_save(
    operation: Awaitable[Relation],
) -> Relation:
    """Do not let caller cancellation orphan an admitted writer request."""

    task = asyncio.ensure_future(operation)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        current = asyncio.current_task()
        if current is None or current.cancelling() == 0:
            return task.result()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except BaseException as error:
            _LOGGER.error(
                "Relation analysis save failed while caller cancellation "
                "was pending: %s",
                error,
            )
        raise cancellation


@dataclass(frozen=True, slots=True)
class RelationActivation:
    relation: Relation
    system_event_id: int | None


ActivationCallback = Callable[
    [RelationActivation],
    None | Awaitable[None],
]


class RelationChangeMonitor:
    """Observe cross-process approvals through the persistent event log."""

    def __init__(
        self,
        path: Path,
        on_activation: ActivationCallback,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        if not callable(on_activation):
            raise TypeError("on_activation must be callable")
        if (
            isinstance(poll_interval_seconds, bool)
            or not isinstance(poll_interval_seconds, (int, float))
            or not math.isfinite(poll_interval_seconds)
            or poll_interval_seconds <= 0
        ):
            raise ValueError("poll_interval_seconds must be finite and positive")
        self._path = Path(path)
        self._on_activation = on_activation
        self._poll_interval_seconds = float(poll_interval_seconds)
        self.ready = asyncio.Event()
        self.changed = asyncio.Event()
        self._last_event_id = 0
        self._active_relation_ids: set[str] = set()

    @property
    def last_event_id(self) -> int:
        return self._last_event_id

    async def run(self) -> None:
        uri = f"file:{self._path}?mode=ro"
        async with aiosqlite.connect(
            uri,
            uri=True,
            isolation_level=None,
        ) as connection:
            connection.row_factory = aiosqlite.Row
            await connection.execute("PRAGMA query_only = ON")
            await connection.execute("BEGIN")
            try:
                approved_cursor = await connection.execute(
                    """
                    SELECT * FROM relations
                    WHERE status = 'APPROVED'
                    ORDER BY CAST(id AS BLOB)
                    """
                )
                approved_rows = await approved_cursor.fetchall()
                cursor = await connection.execute(
                    "SELECT COALESCE(MAX(id), 0) AS max_id FROM system_events"
                )
                row = await cursor.fetchone()
                assert row is not None
                self._last_event_id = int(row["max_id"])
            finally:
                await connection.execute("ROLLBACK")

            for approved_row in approved_rows:
                relation = _relation_from_row(approved_row)
                await self._deliver(
                    RelationActivation(relation=relation, system_event_id=None)
                )
                self._active_relation_ids.add(relation.id)
            self.ready.set()

            while True:
                await self._poll(connection)
                await asyncio.sleep(self._poll_interval_seconds)

    async def _poll(self, connection: aiosqlite.Connection) -> None:
        cursor = await connection.execute(
            """
            SELECT id, event_type, details_json
            FROM system_events
            WHERE id > ?
            ORDER BY id
            """,
            (self._last_event_id,),
        )
        rows = await cursor.fetchall()
        for row in rows:
            event_id = int(row["id"])
            if event_id <= self._last_event_id:
                raise RuntimeError("system event IDs must increase strictly")
            if row["event_type"] == "RELATION_ACTIVATED":
                relation_id = _activation_relation_id(row["details_json"])
                if relation_id in self._active_relation_ids:
                    self._last_event_id = event_id
                    continue
                relation_cursor = await connection.execute(
                    "SELECT * FROM relations WHERE id = ? AND status = 'APPROVED'",
                    (relation_id,),
                )
                relation_row = await relation_cursor.fetchone()
                if relation_row is None:
                    raise RuntimeError(
                        "RELATION_ACTIVATED references a missing or inactive relation"
                    )
                await self._deliver(
                    RelationActivation(
                        relation=_relation_from_row(relation_row),
                        system_event_id=event_id,
                    )
                )
                self._active_relation_ids.add(relation_id)
                self.changed.set()
            self._last_event_id = event_id

    async def _deliver(self, activation: RelationActivation) -> None:
        result = self._on_activation(activation)
        if inspect.isawaitable(result):
            await result


class RelationCliStore:
    """Short, independent connections for the administrative CLI only."""

    def __init__(
        self,
        path: Path,
        *,
        busy_timeout_ms: int,
        retry_attempts: int = 3,
        retry_delay_seconds: float = 0.05,
    ) -> None:
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a non-negative integer")
        if type(retry_attempts) is not int or retry_attempts < 1:
            raise ValueError("retry_attempts must be a positive integer")
        if (
            isinstance(retry_delay_seconds, bool)
            or not isinstance(retry_delay_seconds, (int, float))
            or not math.isfinite(retry_delay_seconds)
            or retry_delay_seconds < 0
        ):
            raise ValueError("retry_delay_seconds must be finite and non-negative")
        self._path = Path(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._retry_attempts = retry_attempts
        self._retry_delay_seconds = float(retry_delay_seconds)

    def list(self) -> tuple[Relation, ...]:
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM relations ORDER BY CAST(id AS BLOB)"
            ).fetchall()
        return tuple(_relation_from_row(row) for row in rows)

    async def get(self, relation_id: str) -> Relation | None:
        _relation_id(relation_id)
        return await asyncio.to_thread(self._get_sync, relation_id)

    async def get_for_analysis(self, relation_id: str) -> Relation | None:
        _relation_id(relation_id)
        return await asyncio.to_thread(self._get_for_analysis_sync, relation_id)

    async def save_analysis(
        self,
        relation: Relation,
        *,
        expected_semantic_digest: str,
    ) -> Relation:
        if not isinstance(relation, Relation):
            raise TypeError("relation must be a Relation")
        validate_semantic_digest(expected_semantic_digest)
        return await asyncio.to_thread(
            self._save_analysis_sync,
            relation,
            expected_semantic_digest,
        )

    def approve_manual(self, relation_id: str, *, occurred_at: int) -> Relation:
        _relation_id(relation_id)
        if type(occurred_at) is not int or occurred_at < 0:
            raise ValueError("occurred_at must be a non-negative integer")

        def transaction(connection: sqlite3.Connection) -> Relation:
            row = connection.execute(
                """
                SELECT r.*,
                       a.status AS market_a_status,
                       a.active AS market_a_active,
                       a.resolved_at AS market_a_resolved_at,
                       b.status AS market_b_status,
                       b.active AS market_b_active,
                       b.resolved_at AS market_b_resolved_at
                FROM relations AS r
                JOIN markets AS a ON a.id = r.market_a_id
                JOIN markets AS b ON b.id = r.market_b_id
                WHERE r.id = ?
                """,
                (relation_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"relation {relation_id!r} does not exist")
            if row["status"] != RelationStatus.LLM_APPROVE.value:
                raise ValueError("manual approval requires relation status LLM_APPROVE")
            if occurred_at < row["updated_at"]:
                raise ValueError("approval occurred_at must not move backwards")
            if row["market_a_id"] == row["market_b_id"]:
                raise ValueError("relation markets must be different")
            if any(
                (
                    row[f"market_{side}_status"] != MarketStatus.ACTIVE.value
                    or row[f"market_{side}_active"] != 1
                    or row[f"market_{side}_resolved_at"] is not None
                )
                for side in ("a", "b")
            ):
                raise ValueError(
                    "relation markets no longer have active implication semantics"
                )
            stored_semantics = _stored_semantic_snapshot(row["llm_analysis_json"])
            current_semantics = _capture_relation_semantics_sync(
                connection,
                row["market_a_id"],
                row["market_b_id"],
            )
            if not _same_semantics(stored_semantics, current_semantics):
                raise ValueError("relation semantics changed after analysis")
            updated = connection.execute(
                """
                UPDATE relations SET status = 'APPROVED', updated_at = ?
                WHERE id = ? AND status = 'LLM_APPROVE'
                """,
                (occurred_at, relation_id),
            )
            if updated.rowcount != 1:
                raise ValueError("relation changed concurrently during approval")
            connection.execute(
                """
                INSERT INTO system_events (
                    component, severity, event_type, message,
                    details_json, occurred_at
                ) VALUES ('STRATEGY', 'INFO', 'RELATION_ACTIVATED', ?, ?, ?)
                """,
                (
                    f"Relation {relation_id} activated",
                    json.dumps(
                        {"relation_id": relation_id},
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    occurred_at,
                ),
            )
            approved = connection.execute(
                "SELECT * FROM relations WHERE id = ?",
                (relation_id,),
            ).fetchone()
            assert approved is not None
            return _relation_from_row(approved)

        return self._write(transaction)

    def _get_sync(self, relation_id: str) -> Relation | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM relations WHERE id = ?",
                (relation_id,),
            ).fetchone()
        return None if row is None else _relation_from_row(row)

    def _get_for_analysis_sync(self, relation_id: str) -> Relation | None:
        with self._read_connection() as connection:
            connection.execute("BEGIN")
            try:
                row = connection.execute(
                    "SELECT * FROM relations WHERE id = ?",
                    (relation_id,),
                ).fetchone()
                if row is None:
                    return None
                relation = _relation_from_row(row)
                semantics = _capture_relation_semantics_sync(
                    connection,
                    relation.market_a_id,
                    relation.market_b_id,
                )
                return relation_with_semantic_context(relation, semantics)
            finally:
                connection.rollback()

    def _save_analysis_sync(
        self,
        relation: Relation,
        expected_semantic_digest: str,
    ) -> Relation:
        if relation.status not in {
            RelationStatus.NO_LLM_APPROVE,
            RelationStatus.LLM_APPROVE,
        }:
            raise ValueError("analysis cannot set relation status APPROVED")
        if relation.llm_confidence is None or relation.llm_analysis is None:
            raise ValueError("analysis result is required")
        approved = relation.llm_analysis.get("approved")
        expected_status = (
            RelationStatus.LLM_APPROVE
            if approved is True
            else RelationStatus.NO_LLM_APPROVE
        )
        if type(approved) is not bool or relation.status is not expected_status:
            raise ValueError("analysis decision does not match relation status")

        def transaction(connection: sqlite3.Connection) -> Relation:
            row = connection.execute(
                "SELECT * FROM relations WHERE id = ?",
                (relation.id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"relation {relation.id!r} does not exist")
            current = _relation_from_row(row)
            if current.status is not RelationStatus.NO_LLM_APPROVE:
                raise ValueError("analysis requires relation status NO_LLM_APPROVE")
            if (
                current.market_a_id != relation.market_a_id
                or current.market_b_id != relation.market_b_id
                or current.discovery_source is not relation.discovery_source
                or current.created_at != relation.created_at
            ):
                raise ValueError("analysis cannot change relation identity")
            if relation.updated_at < current.updated_at:
                raise ValueError("analysis updated_at must not move backwards")
            provided_digest = semantic_evidence_digest(relation)
            if not hmac.compare_digest(provided_digest, expected_semantic_digest):
                raise ValueError("analysis semantic evidence does not match expected digest")
            semantics = _capture_relation_semantics_sync(
                connection,
                relation.market_a_id,
                relation.market_b_id,
            )
            if not hmac.compare_digest(
                _semantic_digest(semantics),
                expected_semantic_digest,
            ):
                raise ValueError("relation semantics changed during analysis")
            analysis = _thaw_json(relation.llm_analysis)
            assert isinstance(analysis, dict)
            updated = connection.execute(
                """
                UPDATE relations
                SET status = ?, llm_confidence = ?, llm_analysis_json = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'NO_LLM_APPROVE'
                """,
                (
                    relation.status.value,
                    encode_decimal(relation.llm_confidence),
                    json.dumps(
                        analysis,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    relation.updated_at,
                    relation.id,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError("relation changed concurrently during analysis")
            return replace(relation, llm_analysis=analysis)

        stored = self._write(transaction)
        assert isinstance(stored, Relation)
        return stored

    def _read_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            f"file:{self._path}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        return connection

    def _write(self, command: Callable[[sqlite3.Connection], object]):
        for attempt in range(self._retry_attempts):
            connection = sqlite3.connect(
                self._path,
                isolation_level=None,
                timeout=self._busy_timeout_ms / 1_000,
            )
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute(
                    f"PRAGMA busy_timeout = {self._busy_timeout_ms}"
                )
                connection.execute("BEGIN IMMEDIATE")
                result = command(connection)
                connection.commit()
                return result
            except sqlite3.OperationalError as error:
                connection.rollback()
                locked = "locked" in str(error).lower() or "busy" in str(error).lower()
                if not locked or attempt + 1 >= self._retry_attempts:
                    raise
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()
            time.sleep(self._retry_delay_seconds)
        raise RuntimeError("unreachable relation CLI retry state")


def _typed_by_id(
    values: Sequence[Event] | Sequence[Market],
    item_type: type[Event] | type[Market],
    field_name: str,
) -> dict[str, Event] | dict[str, Market]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be a sequence")
    try:
        materialized = tuple(values)
    except TypeError as error:
        raise ValueError(f"{field_name} must be a sequence") from error
    if any(not isinstance(value, item_type) for value in materialized):
        raise ValueError(f"{field_name} contains an invalid value")
    result = {value.id: value for value in materialized}
    if len(result) != len(materialized):
        raise ValueError(f"{field_name} contains duplicate IDs")
    return result


def _candidate_id(market_a_id: str, market_b_id: str) -> str:
    payload = (
        len(market_a_id).to_bytes(8, "big")
        + market_a_id.encode("utf-8")
        + len(market_b_id).to_bytes(8, "big")
        + market_b_id.encode("utf-8")
    )
    return f"implication:{hashlib.sha256(payload).hexdigest()}"


def _activation_relation_id(encoded: str | None) -> str:
    if encoded is None:
        raise RuntimeError("RELATION_ACTIVATED is missing details")
    try:
        details = json.loads(encoded)
    except (TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("RELATION_ACTIVATED has invalid details") from error
    relation_id = details.get("relation_id") if isinstance(details, dict) else None
    if not isinstance(relation_id, str) or not relation_id:
        raise RuntimeError("RELATION_ACTIVATED is missing relation_id")
    return relation_id


def _relation_id(value: object) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError("relation_id must be a non-empty string")


_MARKET_SEMANTICS_SQL = """
SELECT m.id, m.event_id, m.condition_id, m.question, m.description, m.end_at,
       e.id, e.title, e.description, e.end_at
FROM markets AS m
JOIN events AS e ON e.id = m.event_id
WHERE m.id = ?
"""

_TOKEN_SEMANTICS_SQL = """
SELECT id, outcome, position
FROM tokens
WHERE market_id = ?
ORDER BY position, CAST(id AS BLOB)
"""


async def capture_relation_semantics(
    connection: aiosqlite.Connection,
    market_a_id: str,
    market_b_id: str,
) -> dict[str, object]:
    snapshots: dict[str, object] = {}
    for side, market_id in (("market_a", market_a_id), ("market_b", market_b_id)):
        market_cursor = await connection.execute(_MARKET_SEMANTICS_SQL, (market_id,))
        market_row = await market_cursor.fetchone()
        token_cursor = await connection.execute(_TOKEN_SEMANTICS_SQL, (market_id,))
        token_rows = await token_cursor.fetchall()
        snapshots[side] = _semantic_market_snapshot(market_row, token_rows)
    return snapshots


def analysis_with_semantic_evidence(
    analysis: Mapping[str, object],
    semantics: Mapping[str, object],
) -> dict[str, object]:
    return _analysis_with_semantic_evidence(analysis, semantics)


def relation_with_semantic_context(
    relation: Relation,
    semantics: Mapping[str, object],
) -> Relation:
    evidence = _analysis_with_semantic_evidence({}, semantics)["semantic_evidence"]
    return replace(relation, llm_analysis={"semantic_evidence": evidence})


def semantic_evidence_digest(relation: Relation) -> str:
    if relation.llm_analysis is None:
        raise ValueError("analysis semantic evidence is missing")
    evidence = relation.llm_analysis.get("semantic_evidence")
    snapshot = _snapshot_from_evidence(evidence)
    return _semantic_digest(snapshot)


def _capture_relation_semantics_sync(
    connection: sqlite3.Connection,
    market_a_id: str,
    market_b_id: str,
) -> dict[str, object]:
    snapshots: dict[str, object] = {}
    for side, market_id in (("market_a", market_a_id), ("market_b", market_b_id)):
        market_row = connection.execute(_MARKET_SEMANTICS_SQL, (market_id,)).fetchone()
        token_rows = connection.execute(_TOKEN_SEMANTICS_SQL, (market_id,)).fetchall()
        snapshots[side] = _semantic_market_snapshot(market_row, token_rows)
    return snapshots


def _semantic_market_snapshot(
    market_row: object,
    token_rows: Sequence[object],
) -> dict[str, object]:
    if market_row is None:
        raise ValueError("relation semantic market or event is missing")
    try:
        row = tuple(market_row)  # type: ignore[arg-type]
        tokens = [tuple(item) for item in token_rows]  # type: ignore[arg-type]
    except TypeError as error:
        raise ValueError("relation semantic rows are invalid") from error
    if len(row) != 10 or not tokens:
        raise ValueError("relation semantic market, event, or tokens are missing")
    required_strings = (row[0], row[1], row[2], row[3], row[6], row[7])
    if any(not isinstance(value, str) or not value.strip() for value in required_strings):
        raise ValueError("relation semantic identity or text is missing")
    if any(
        value is not None and not isinstance(value, str)
        for value in (row[4], row[8])
    ):
        raise ValueError("relation semantic description is invalid")
    if any(
        value is not None and (type(value) is not int or value < 0)
        for value in (row[5], row[9])
    ):
        raise ValueError("relation semantic end time is invalid")
    encoded_tokens: list[dict[str, object]] = []
    for token in tokens:
        if (
            len(token) != 3
            or not isinstance(token[0], str)
            or not token[0]
            or not isinstance(token[1], str)
            or not token[1]
            or type(token[2]) is not int
            or token[2] < 0
        ):
            raise ValueError("relation token semantics are invalid")
        encoded_tokens.append(
            {"id": token[0], "outcome": token[1], "position": token[2]}
        )
    return {
        "event": {
            "id": row[6],
            "title": row[7],
            "description": row[8],
            "end_at": row[9],
        },
        "market": {
            "id": row[0],
            "event_id": row[1],
            "condition_id": row[2],
            "question": row[3],
            "description": row[4],
            "end_at": row[5],
        },
        "tokens": encoded_tokens,
    }


def _analysis_with_semantic_evidence(
    analysis: Mapping[str, object],
    semantics: Mapping[str, object],
) -> dict[str, object]:
    payload = _thaw_json(analysis)
    if not isinstance(payload, dict):
        raise ValueError("relation analysis must be a JSON object")
    canonical_semantics = json.loads(_canonical_json(semantics))
    payload["semantic_evidence"] = {
        "version": 1,
        "market_a": canonical_semantics["market_a"],
        "market_b": canonical_semantics["market_b"],
        "sha256": _semantic_digest(canonical_semantics),
    }
    return payload


def _stored_semantic_snapshot(encoded_analysis: str | None) -> dict[str, object]:
    try:
        analysis = json.loads(encoded_analysis) if encoded_analysis is not None else None
        if not isinstance(analysis, dict):
            raise TypeError
        return _snapshot_from_evidence(analysis["semantic_evidence"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(
            "relation semantic evidence is missing or invalid; analyze again"
        ) from error


def _snapshot_from_evidence(evidence: object) -> dict[str, object]:
    if not isinstance(evidence, Mapping):
        raise ValueError("analysis semantic evidence is missing")
    if set(evidence) != {"version", "market_a", "market_b", "sha256"}:
        raise ValueError("analysis semantic evidence shape is invalid")
    if evidence["version"] != 1 or not isinstance(evidence["sha256"], str):
        raise ValueError("analysis semantic evidence version is invalid")
    snapshot = {
        "market_a": evidence["market_a"],
        "market_b": evidence["market_b"],
    }
    digest = _semantic_digest(snapshot)
    if not hmac.compare_digest(evidence["sha256"], digest):
        raise ValueError("analysis semantic evidence digest is invalid")
    return snapshot


def validate_semantic_digest(value: object) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("expected_semantic_digest must be a SHA-256 hex digest")


def _same_semantics(
    stored: Mapping[str, object],
    current: Mapping[str, object],
) -> bool:
    return hmac.compare_digest(_semantic_digest(stored), _semantic_digest(current))


def _semantic_digest(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(
        _thaw_json(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _relation_from_row(row: aiosqlite.Row) -> Relation:
    return Relation(
        id=row["id"],
        market_a_id=row["market_a_id"],
        market_b_id=row["market_b_id"],
        status=RelationStatus(row["status"]),
        discovery_source=DiscoverySource(row["discovery_source"]),
        llm_confidence=(
            None
            if row["llm_confidence"] is None
            else decode_decimal(row["llm_confidence"])
        ),
        llm_analysis=(
            None
            if row["llm_analysis_json"] is None
            else json.loads(row["llm_analysis_json"])
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
