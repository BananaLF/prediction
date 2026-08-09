"""Recoverable, WAL-bounded staging for catalog generations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass, replace
from decimal import Decimal
from enum import Enum, StrEnum
import hashlib
import inspect
import json
from pathlib import Path
import time
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import aiosqlite

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.decimal import decode_decimal, encode_decimal
from predmarket.domain.fees import FeeSchedule
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.wal import (
    WalAction,
    WalPolicy,
    decide_wal_action,
    update_wal_amplification,
)
from predmarket.persistence.writer import CheckpointMode, CheckpointResult

if TYPE_CHECKING:
    from predmarket.persistence.repositories import PendingCatalogReconciliation


MiB = 1024 * 1024
_Entity = Event | Market | Token
_T = TypeVar("_T")


class CatalogGenerationError(RuntimeError):
    """Base error for generation staging."""


class CatalogSyncAborted(CatalogGenerationError):
    """Raised when WAL safety requires abandoning a complete sync."""

    def __init__(
        self,
        *,
        generation: str,
        reason: str,
        wal_peak_bytes: int,
    ) -> None:
        self.generation = generation
        self.reason = reason
        self.wal_peak_bytes = wal_peak_bytes
        super().__init__(reason)


class CatalogConstraintError(ValueError):
    """Candidate snapshot violates a catalog invariant."""


class CatalogActivationConflict(RuntimeError):
    """Runtime revision changed after candidate validation."""


class CatalogFaultPoint(StrEnum):
    """Explicit test-only interruption boundaries for generation workflows."""

    AFTER_BATCH_COMMIT = "AFTER_BATCH_COMMIT"
    AFTER_VALIDATION = "AFTER_VALIDATION"
    AFTER_REBASE = "AFTER_REBASE"
    BEFORE_ACTIVATION_COMMIT = "BEFORE_ACTIVATION_COMMIT"
    AFTER_ACTIVATION_COMMIT = "AFTER_ACTIVATION_COMMIT"
    AFTER_CHECKPOINT = "AFTER_CHECKPOINT"
    AFTER_CLEANUP_BATCH = "AFTER_CLEANUP_BATCH"


class _GenerationWriter(Protocol):
    async def execute(
        self,
        command: Callable[[aiosqlite.Connection], _T | Awaitable[_T]],
    ) -> _T: ...

    async def checkpoint(
        self,
        mode: CheckpointMode = CheckpointMode.PASSIVE,
    ) -> CheckpointResult: ...


@dataclass(frozen=True, slots=True)
class CatalogBatchLimits:
    max_rows: int = 8_000
    max_payload_bytes: int = 16 * MiB

    def __post_init__(self) -> None:
        if type(self.max_rows) is not int or self.max_rows <= 0:
            raise ValueError("max_rows must be a positive integer")
        if type(self.max_payload_bytes) is not int or self.max_payload_bytes <= 0:
            raise ValueError("max_payload_bytes must be a positive integer")


@dataclass(frozen=True, slots=True)
class GenerationInput:
    sync_generation: str
    updated_at: int
    events: tuple[Event, ...]
    markets: tuple[Market, ...]
    tokens: tuple[Token, ...]
    input_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.sync_generation, str) or not self.sync_generation:
            raise ValueError("sync_generation must be a non-empty string")
        if type(self.updated_at) is not int or self.updated_at < 0:
            raise ValueError("updated_at must be a non-negative integer")
        if not isinstance(self.input_digest, str) or not self.input_digest:
            raise ValueError("input_digest must be a non-empty string")
        for field_name, values, item_type in (
            ("events", self.events, Event),
            ("markets", self.markets, Market),
            ("tokens", self.tokens, Token),
        ):
            if not isinstance(values, tuple) or any(
                not isinstance(value, item_type) for value in values
            ):
                raise ValueError(f"{field_name} must be a tuple of {item_type.__name__}")
            identifiers = tuple(value.id for value in values)
            if len(identifiers) != len(set(identifiers)):
                raise ValueError(f"{field_name} contains duplicate IDs")
            if any(
                value.sync_generation != self.sync_generation
                or not value.sync_generation_complete
                or value.updated_at != self.updated_at
                for value in values
            ):
                raise ValueError(
                    f"{field_name} must belong to the requested complete generation"
                )


@dataclass(frozen=True, slots=True)
class StagedGeneration:
    id: int
    sync_generation: str
    base_generation_id: int
    base_runtime_revision: int


@dataclass(frozen=True, slots=True)
class CandidateValidation:
    generation_id: int
    event_count: int
    market_count: int
    token_count: int
    snapshot_digest: str
    validated_runtime_revision: int


@dataclass(frozen=True, slots=True)
class RebaseResult:
    generation_id: int
    from_revision: int
    through_revision: int
    copied_events: int
    copied_markets: int
    copied_tokens: int


@dataclass(frozen=True, slots=True)
class CleanupResult:
    deleted_versions: int
    deleted_journal_rows: int
    remaining_versions: int
    remaining_journal_rows: int


@dataclass(frozen=True, slots=True)
class _EntityPlan:
    kind: str
    values: tuple[_Entity, ...]
    payloads: tuple[bytes, ...]
    planned_digest: str


class CatalogGenerationCoordinator:
    """Stage one invisible generation using bounded, resumable transactions."""

    def __init__(
        self,
        writer: _GenerationWriter,
        *,
        batch_limits: CatalogBatchLimits = CatalogBatchLimits(),
        wal_policy: WalPolicy = WalPolicy(),
        initial_wal_amplification: float = 1.0,
        max_pause_checkpoints: int = 3,
        max_activation_rebases: int = 3,
        clock: Callable[[], int] = lambda: int(time.time()),
        database_path: Path | None = None,
        fault_hook: Callable[[CatalogFaultPoint], object | Awaitable[object]]
        | None = None,
    ) -> None:
        if not isinstance(batch_limits, CatalogBatchLimits):
            raise TypeError("batch_limits must be CatalogBatchLimits")
        if not isinstance(wal_policy, WalPolicy):
            raise TypeError("wal_policy must be WalPolicy")
        if (
            not isinstance(initial_wal_amplification, (int, float))
            or initial_wal_amplification <= 0
        ):
            raise ValueError("initial_wal_amplification must be positive")
        if type(max_pause_checkpoints) is not int or max_pause_checkpoints <= 0:
            raise ValueError("max_pause_checkpoints must be a positive integer")
        if type(max_activation_rebases) is not int or max_activation_rebases <= 0:
            raise ValueError("max_activation_rebases must be a positive integer")
        if fault_hook is not None and not callable(fault_hook):
            raise TypeError("fault_hook must be callable or None")
        self._writer = writer
        self._batch_limits = batch_limits
        self._wal_policy = wal_policy
        self._wal_amplification = float(initial_wal_amplification)
        self._max_pause_checkpoints = max_pause_checkpoints
        self._max_activation_rebases = max_activation_rebases
        self._clock = clock
        self._fault_hook = fault_hook
        inferred_path = getattr(writer, "path", None)
        if database_path is None and inferred_path is None:
            inferred_path = getattr(writer, "_path", None)
        if database_path is None and inferred_path is None:
            raise ValueError("database_path is required for candidate validation")
        self._database_path = Path(
            database_path if database_path is not None else inferred_path
        )

    async def stage(self, value: GenerationInput) -> StagedGeneration:
        """Create or resume one bounded, invisible staging generation."""

        if not isinstance(value, GenerationInput):
            raise TypeError("value must be GenerationInput")
        plans = _build_plans(value)
        preflight = await self._writer.checkpoint(CheckpointMode.TRUNCATE)
        wal_peak_bytes = preflight.wal_bytes
        if preflight.wal_bytes >= self._wal_policy.preflight_bytes:
            raise CatalogSyncAborted(
                generation=value.sync_generation,
                reason=(
                    "catalog WAL preflight could not reclaim below the safety threshold"
                ),
                wal_peak_bytes=wal_peak_bytes,
            )
        generation, cursors = await self._create_or_resume_staging(value, plans)
        wal_bytes = preflight.wal_bytes
        adaptive_max_rows = self._batch_limits.max_rows
        for plan, cursor in zip(plans, cursors, strict=True):
            start = _resume_index(plan.values, cursor)
            while start < len(plan.values):
                batch_end = _batch_end(
                    plan.payloads,
                    start=start,
                    max_rows=adaptive_max_rows,
                    max_payload_bytes=self._batch_limits.max_payload_bytes,
                )
                payload_bytes = sum(len(payload) for payload in plan.payloads[start:batch_end])
                predicted_delta = max(1, int(payload_bytes * self._wal_amplification))
                action = decide_wal_action(
                    wal_bytes=wal_bytes,
                    predicted_delta=predicted_delta,
                    policy=self._wal_policy,
                )
                if action is WalAction.SHRINK_BATCH:
                    if batch_end - start == 1:
                        await self._abort_generation(
                            generation.id,
                            "single-row batch would exceed the WAL safety reserve",
                        )
                        raise CatalogSyncAborted(
                            generation=value.sync_generation,
                            reason=(
                                "catalog staging cannot safely write a single-row batch"
                            ),
                            wal_peak_bytes=wal_peak_bytes,
                        )
                    adaptive_max_rows = max(1, (batch_end - start) // 2)
                    continue
                if action is WalAction.ABORT:
                    await self._abort_generation(
                        generation.id,
                        "WAL reached the abort waterline",
                    )
                    raise CatalogSyncAborted(
                        generation=value.sync_generation,
                        reason="catalog staging reached the WAL abort waterline",
                        wal_peak_bytes=wal_peak_bytes,
                    )
                if action is WalAction.PAUSE_AND_CHECKPOINT:
                    wal_bytes, wal_peak_bytes = await self._pause_for_wal(
                        generation.id,
                        sync_generation=value.sync_generation,
                        wal_peak_bytes=wal_peak_bytes,
                    )
                    continue
                if action is WalAction.PASSIVE_CHECKPOINT:
                    checkpoint = await self._writer.checkpoint(CheckpointMode.PASSIVE)
                    wal_bytes = checkpoint.wal_bytes
                    wal_peak_bytes = max(wal_peak_bytes, wal_bytes)
                    await asyncio.sleep(0)
                    action = decide_wal_action(
                        wal_bytes=wal_bytes,
                        predicted_delta=predicted_delta,
                        policy=self._wal_policy,
                    )
                    if action in {
                        WalAction.SHRINK_BATCH,
                        WalAction.PAUSE_AND_CHECKPOINT,
                        WalAction.ABORT,
                    }:
                        continue

                await self._write_batch(
                    generation_id=generation.id,
                    plan=plan,
                    start=start,
                    end=batch_end,
                )
                await self._hit_fault(CatalogFaultPoint.AFTER_BATCH_COMMIT)
                checkpoint = await self._writer.checkpoint(CheckpointMode.PASSIVE)
                await self._hit_fault(CatalogFaultPoint.AFTER_CHECKPOINT)
                wal_delta = max(0, checkpoint.wal_bytes - wal_bytes)
                self._wal_amplification = update_wal_amplification(
                    current=self._wal_amplification,
                    payload_bytes=payload_bytes,
                    wal_delta_bytes=wal_delta,
                )
                wal_bytes = checkpoint.wal_bytes
                wal_peak_bytes = max(wal_peak_bytes, wal_bytes)
                post_batch_action = decide_wal_action(
                    wal_bytes=wal_bytes,
                    predicted_delta=0,
                    policy=self._wal_policy,
                )
                if post_batch_action is WalAction.ABORT:
                    await self._abort_generation(
                        generation.id,
                        "WAL reached the abort waterline after a payload batch",
                    )
                    raise CatalogSyncAborted(
                        generation=value.sync_generation,
                        reason="catalog staging reached the WAL abort waterline",
                        wal_peak_bytes=wal_peak_bytes,
                    )
                if post_batch_action is WalAction.PAUSE_AND_CHECKPOINT:
                    wal_bytes, wal_peak_bytes = await self._pause_for_wal(
                        generation.id,
                        sync_generation=value.sync_generation,
                        wal_peak_bytes=wal_peak_bytes,
                    )
                start = batch_end
                await asyncio.sleep(0)
        return generation

    async def validate_candidate(
        self,
        generation: StagedGeneration,
    ) -> CandidateValidation:
        """Validate and freeze one effective candidate outside activation."""

        if not isinstance(generation, StagedGeneration):
            raise TypeError("generation must be StagedGeneration")
        try:
            validation = await self._read_candidate_validation(generation)
            await self._freeze_candidate(generation, validation)
            await self._hit_fault(CatalogFaultPoint.AFTER_VALIDATION)
            return validation
        except CatalogConstraintError as error:
            await self._abort_generation(generation.id, str(error))
            raise

    async def activate(
        self,
        validation: CandidateValidation,
        reconciliation: PendingCatalogReconciliation | None,
    ) -> None:
        """CAS-activate a candidate, rebasing bounded runtime conflicts."""

        if not isinstance(validation, CandidateValidation):
            raise TypeError("validation must be CandidateValidation")
        current = validation
        for attempt in range(self._max_activation_rebases + 1):
            try:
                await self._activate_once(current, reconciliation)
                return
            except CatalogActivationConflict:
                if attempt >= self._max_activation_rebases:
                    raise
                current, _result = await self.rebase(current)

    async def cleanup(self, *, max_rows: int = 8_000) -> CleanupResult:
        """Delete one recoverable batch of dominated payload and journal rows."""

        if type(max_rows) is not int or not 0 < max_rows <= 8_000:
            raise ValueError("max_rows must be an integer between 1 and 8000")
        preflight = await self._writer.checkpoint(CheckpointMode.TRUNCATE)
        if preflight.wal_bytes >= self._wal_policy.preflight_bytes:
            raise CatalogGenerationError(
                "catalog cleanup WAL preflight could not reclaim below the safety "
                "threshold"
            )
        now = self._clock()

        async def command(connection: aiosqlite.Connection) -> CleanupResult:
            state_cursor = await connection.execute(
                """
                SELECT active_generation_id, runtime_revision,
                       cleanup_entity_type, cleanup_entity_id,
                       cleanup_generation_id
                FROM catalog_state WHERE id = 1
                """
            )
            state = await state_cursor.fetchone()
            if state is None:
                raise CatalogGenerationError("catalog state singleton is missing")
            active_generation_id = int(state[0])
            runtime_revision = int(state[1])
            cursor_kind = state[2]
            cursor_entity_id = state[3]
            cursor_generation_id = state[4]
            kinds = (
                ("EVENT", "event_versions"),
                ("MARKET", "market_versions"),
                ("TOKEN", "token_versions"),
            )
            start_index = next(
                (
                    index
                    for index, (kind, _table) in enumerate(kinds)
                    if kind == cursor_kind
                ),
                0,
            )
            remaining_budget = max_rows
            deleted_versions = 0
            last_cursor: tuple[str, str, int] | None = None
            completed_version_cycle = True

            for index in range(start_index, len(kinds)):
                kind, table = kinds[index]
                after_entity = cursor_entity_id if kind == cursor_kind else None
                after_generation = (
                    int(cursor_generation_id)
                    if kind == cursor_kind and cursor_generation_id is not None
                    else None
                )
                selected = await _select_cleanup_versions(
                    connection,
                    table=table,
                    active_generation_id=active_generation_id,
                    after_entity=after_entity,
                    after_generation=after_generation,
                    limit=remaining_budget,
                )
                if selected:
                    await connection.executemany(
                        f"DELETE FROM {table} "
                        "WHERE entity_id = ? AND generation_id = ?",
                        selected,
                    )
                    deleted_versions += len(selected)
                    remaining_budget -= len(selected)
                    entity_id, generation_id = selected[-1]
                    last_cursor = (kind, entity_id, generation_id)
                if remaining_budget == 0:
                    completed_version_cycle = False
                    break

            deleted_journal_rows = 0
            if completed_version_cycle and remaining_budget:
                boundary_cursor = await connection.execute(
                    """
                    SELECT COALESCE(
                        MIN(base_runtime_revision),
                        ?
                    )
                    FROM catalog_generations
                    WHERE status = 'STAGING'
                    """,
                    (runtime_revision,),
                )
                boundary_row = await boundary_cursor.fetchone()
                journal_boundary = int(boundary_row[0])
                journal_cursor = await connection.execute(
                    """
                    SELECT runtime_revision, entity_type, entity_id
                    FROM catalog_runtime_changes
                    WHERE runtime_revision <= ?
                    ORDER BY runtime_revision, entity_type,
                             CAST(entity_id AS BLOB)
                    LIMIT ?
                    """,
                    (journal_boundary, remaining_budget),
                )
                journal_rows = [tuple(row) for row in await journal_cursor.fetchall()]
                if journal_rows:
                    await connection.executemany(
                        """
                        DELETE FROM catalog_runtime_changes
                        WHERE runtime_revision = ? AND entity_type = ? AND entity_id = ?
                        """,
                        journal_rows,
                    )
                    deleted_journal_rows = len(journal_rows)

            if completed_version_cycle:
                await connection.execute(
                    """
                    UPDATE catalog_state
                    SET cleanup_entity_type = NULL,
                        cleanup_entity_id = NULL,
                        cleanup_generation_id = NULL,
                        last_checkpoint_at = ?
                    WHERE id = 1
                    """,
                    (now,),
                )
            elif last_cursor is not None:
                await connection.execute(
                    """
                    UPDATE catalog_state
                    SET cleanup_entity_type = ?, cleanup_entity_id = ?,
                        cleanup_generation_id = ?, last_checkpoint_at = ?
                    WHERE id = 1
                    """,
                    (*last_cursor, now),
                )

            remaining_versions = await _count_cleanup_versions(
                connection,
                active_generation_id=active_generation_id,
            )
            staging_cursor = await connection.execute(
                """
                SELECT COALESCE(MIN(base_runtime_revision), ?)
                FROM catalog_generations WHERE status = 'STAGING'
                """,
                (runtime_revision,),
            )
            staging_row = await staging_cursor.fetchone()
            remaining_journal_cursor = await connection.execute(
                """
                SELECT COUNT(*) FROM catalog_runtime_changes
                WHERE runtime_revision <= ?
                """,
                (int(staging_row[0]),),
            )
            remaining_journal_row = await remaining_journal_cursor.fetchone()
            return CleanupResult(
                deleted_versions=deleted_versions,
                deleted_journal_rows=deleted_journal_rows,
                remaining_versions=remaining_versions,
                remaining_journal_rows=int(remaining_journal_row[0]),
            )

        result = await self._writer.execute(command)
        await self._hit_fault(CatalogFaultPoint.AFTER_CLEANUP_BATCH)
        checkpoint = await self._writer.checkpoint(CheckpointMode.PASSIVE)
        action = decide_wal_action(
            wal_bytes=checkpoint.wal_bytes,
            predicted_delta=0,
            policy=self._wal_policy,
        )
        if action in {WalAction.PAUSE_AND_CHECKPOINT, WalAction.ABORT}:
            await self._reclaim_cleanup_wal(checkpoint.wal_bytes)
        return result

    async def _reclaim_cleanup_wal(self, wal_peak_bytes: int) -> None:
        for _ in range(self._max_pause_checkpoints):
            checkpoint = await self._writer.checkpoint(CheckpointMode.RESTART)
            wal_peak_bytes = max(wal_peak_bytes, checkpoint.wal_bytes)
            await asyncio.sleep(0)
            if checkpoint.wal_bytes < self._wal_policy.pause_bytes:
                return
            if checkpoint.wal_bytes >= self._wal_policy.abort_bytes:
                break
        raise CatalogGenerationError(
            "catalog cleanup WAL pause could not be cleared "
            f"(peak_bytes={wal_peak_bytes})"
        )

    async def _activate_once(
        self,
        validation: CandidateValidation,
        reconciliation: PendingCatalogReconciliation | None,
    ) -> None:
        prepared_reconciliation = _prepare_reconciliation(reconciliation)
        now = self._clock()

        async def command(connection: aiosqlite.Connection) -> None:
            cursor = await connection.execute(
                """
                SELECT sync_generation, base_generation_id,
                       candidate_event_count, candidate_market_count,
                       candidate_token_count, candidate_snapshot_digest,
                       validated_at
                FROM catalog_generations
                WHERE id = ? AND status = 'STAGING'
                """,
                (validation.generation_id,),
            )
            row = await cursor.fetchone()
            expected = (
                validation.event_count,
                validation.market_count,
                validation.token_count,
                validation.snapshot_digest,
            )
            if row is None or tuple(row[2:6]) != expected or row[6] is None:
                raise CatalogActivationConflict(
                    "candidate validation is absent, stale, or no longer staging"
                )
            sync_generation = str(row[0])
            base_generation_id = int(row[1])
            effective = await _load_effective_entities(
                connection,
                generation_id=validation.generation_id,
                base_generation_id=base_generation_id,
                sync_generation=sync_generation,
            )
            if (
                tuple(len(values) for values in effective) != expected[:3]
                or _snapshot_digest(*effective) != validation.snapshot_digest
            ):
                raise CatalogActivationConflict(
                    "candidate payload changed after validation"
                )
            staged = await _load_staged_entities(
                connection,
                generation_id=validation.generation_id,
                sync_generation=sync_generation,
            )
            for effective_values, staged_values in zip(
                effective,
                staged,
                strict=True,
            ):
                staged_by_id = {entity.id: entity for entity in staged_values}
                for entity in effective_values:
                    staged_entity = staged_by_id.get(entity.id)
                    if staged_entity is not None and (
                        _canonical_payload(entity)
                        != _canonical_payload(staged_entity)
                    ):
                        await _upsert_entity(
                            connection,
                            validation.generation_id,
                            entity,
                        )
            cursor = await connection.execute(
                """
                UPDATE catalog_generations
                SET status = 'COMMITTED', activated_at = ?
                WHERE id = ? AND status = 'STAGING'
                  AND candidate_snapshot_digest = ?
                """,
                (now, validation.generation_id, validation.snapshot_digest),
            )
            if cursor.rowcount != 1:
                raise CatalogActivationConflict("candidate status changed before activation")
            cursor = await connection.execute(
                """
                UPDATE catalog_state
                SET active_generation_id = ?
                WHERE id = 1 AND active_generation_id = ?
                  AND runtime_revision = ?
                """,
                (
                    validation.generation_id,
                    base_generation_id,
                    validation.validated_runtime_revision,
                ),
            )
            if cursor.rowcount != 1:
                raise CatalogActivationConflict(
                    "runtime revision or active generation changed after validation"
                )
            if prepared_reconciliation is not None:
                change, market_ids = prepared_reconciliation
                if change.change_id.rsplit(":", 2)[0] != sync_generation:
                    raise ValueError(
                        "reconciliation change must belong to the activated generation"
                    )
                details = _reconciliation_details(
                    sync_generation=sync_generation,
                    change=change,
                    market_ids=market_ids,
                )
                await connection.execute(
                    """
                    INSERT INTO system_events (
                        component, severity, event_type, message,
                        details_json, occurred_at
                    ) VALUES (
                        'SYNC', 'INFO', 'CATALOG_RECONCILIATION_READY', ?, ?, ?
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        f"Catalog reconciliation {change.change_id} ready",
                        details,
                        change.occurred_at,
                    ),
                )

            await self._hit_fault(CatalogFaultPoint.BEFORE_ACTIVATION_COMMIT)

        await self._writer.execute(command)
        await self._hit_fault(CatalogFaultPoint.AFTER_ACTIVATION_COMMIT)

    async def rebase(
        self,
        validation: CandidateValidation,
    ) -> tuple[CandidateValidation, RebaseResult]:
        """Replay winning runtime changes and revalidate the candidate."""

        if not isinstance(validation, CandidateValidation):
            raise TypeError("validation must be CandidateValidation")
        now = self._clock()

        async def command(
            connection: aiosqlite.Connection,
        ) -> tuple[CandidateValidation, RebaseResult]:
            cursor = await connection.execute(
                """
                SELECT generations.sync_generation,
                       generations.base_generation_id,
                       generations.rebased_runtime_revision,
                       generations.status,
                       state.active_generation_id,
                       state.runtime_revision
                FROM catalog_generations AS generations
                JOIN catalog_state AS state ON state.id = 1
                WHERE generations.id = ?
                """,
                (validation.generation_id,),
            )
            cursor.row_factory = aiosqlite.Row
            row = await cursor.fetchone()
            if row is None or row["status"] != "STAGING":
                raise CatalogActivationConflict(
                    "candidate generation is missing or no longer staging"
                )
            base_generation_id = int(row["base_generation_id"])
            if int(row["active_generation_id"]) != base_generation_id:
                raise CatalogActivationConflict(
                    "active generation changed before candidate rebase"
                )
            from_revision = validation.validated_runtime_revision
            persisted_revision = row["rebased_runtime_revision"]
            if persisted_revision is not None:
                from_revision = max(from_revision, int(persisted_revision))
            through_revision = int(row["runtime_revision"])
            if through_revision < from_revision:
                raise CatalogActivationConflict("runtime revision moved backwards")

            cursor = await connection.execute(
                """
                SELECT entity_type, entity_id, MAX(runtime_revision)
                FROM catalog_runtime_changes
                WHERE runtime_revision > ? AND runtime_revision <= ?
                GROUP BY entity_type, entity_id
                ORDER BY entity_type, CAST(entity_id AS BLOB)
                """,
                (from_revision, through_revision),
            )
            cursor.row_factory = aiosqlite.Row
            changes = list(await cursor.fetchall())
            if through_revision > from_revision and not changes:
                raise CatalogActivationConflict(
                    "runtime revision changed without catalog journal entries"
                )

            sync_generation = str(row["sync_generation"])
            active_sync_generation = await _generation_sync_name(
                connection,
                base_generation_id,
            )
            active = await _load_effective_entities(
                connection,
                generation_id=-1,
                base_generation_id=base_generation_id,
                sync_generation=active_sync_generation,
            )
            staged = await _load_staged_entities(
                connection,
                generation_id=validation.generation_id,
                sync_generation=sync_generation,
            )
            active_maps = tuple({entity.id: entity for entity in values} for values in active)
            staged_maps = tuple({entity.id: entity for entity in values} for values in staged)
            kind_index = {"EVENT": 0, "MARKET": 1, "TOKEN": 2}
            copied = [0, 0, 0]
            for change in changes:
                index = kind_index[str(change["entity_type"])]
                entity_id = str(change["entity_id"])
                active_entity = active_maps[index].get(entity_id)
                staged_entity = staged_maps[index].get(entity_id)
                if active_entity is None:
                    raise CatalogActivationConflict(
                        f"journal entity {entity_id!r} is absent from active catalog"
                    )
                if staged_entity is None or (
                    active_entity.updated_at > staged_entity.updated_at
                ):
                    await _upsert_entity(
                        connection,
                        validation.generation_id,
                        replace(
                            active_entity,
                            sync_generation=sync_generation,
                            sync_generation_complete=True,
                        ),
                    )
                    copied[index] += 1

            effective = await _load_effective_entities(
                connection,
                generation_id=validation.generation_id,
                base_generation_id=base_generation_id,
                sync_generation=sync_generation,
            )
            _validate_effective_constraints(*effective)
            rebased = CandidateValidation(
                generation_id=validation.generation_id,
                event_count=len(effective[0]),
                market_count=len(effective[1]),
                token_count=len(effective[2]),
                snapshot_digest=_snapshot_digest(*effective),
                validated_runtime_revision=through_revision,
            )
            cursor = await connection.execute(
                """
                UPDATE catalog_generations
                SET candidate_event_count = ?, candidate_market_count = ?,
                    candidate_token_count = ?, candidate_snapshot_digest = ?,
                    validated_at = ?, rebased_runtime_revision = ?
                WHERE id = ? AND status = 'STAGING'
                """,
                (
                    rebased.event_count,
                    rebased.market_count,
                    rebased.token_count,
                    rebased.snapshot_digest,
                    now,
                    through_revision,
                    validation.generation_id,
                ),
            )
            if cursor.rowcount != 1:
                raise CatalogActivationConflict(
                    "candidate generation changed during rebase"
                )
            result = RebaseResult(
                generation_id=validation.generation_id,
                from_revision=from_revision,
                through_revision=through_revision,
                copied_events=copied[0],
                copied_markets=copied[1],
                copied_tokens=copied[2],
            )
            return rebased, result

        try:
            result = await self._writer.execute(command)
            await self._hit_fault(CatalogFaultPoint.AFTER_REBASE)
            return result
        except CatalogConstraintError as error:
            await self._abort_generation(validation.generation_id, str(error))
            raise

    async def _hit_fault(self, point: CatalogFaultPoint) -> None:
        if self._fault_hook is None:
            return
        result = self._fault_hook(point)
        if inspect.isawaitable(result):
            await result

    async def _read_candidate_validation(
        self,
        generation: StagedGeneration,
    ) -> CandidateValidation:
        async with aiosqlite.connect(
            self._database_path,
            isolation_level=None,
        ) as connection:
            connection.row_factory = aiosqlite.Row
            await connection.execute("PRAGMA query_only = ON")
            await connection.execute("BEGIN")
            try:
                cursor = await connection.execute(
                    """
                    SELECT generations.*, state.active_generation_id,
                           state.runtime_revision
                    FROM catalog_generations AS generations
                    JOIN catalog_state AS state ON state.id = 1
                    WHERE generations.id = ?
                    """,
                    (generation.id,),
                )
                row = await cursor.fetchone()
                if row is None or row["status"] != "STAGING":
                    raise CatalogConstraintError(
                        "candidate generation is missing or no longer staging"
                    )
                if (
                    row["sync_generation"] != generation.sync_generation
                    or row["base_generation_id"] != generation.base_generation_id
                    or row["base_runtime_revision"]
                    != generation.base_runtime_revision
                ):
                    raise CatalogConstraintError(
                        "candidate generation identity does not match staged handle"
                    )
                if row["active_generation_id"] != generation.base_generation_id:
                    raise CatalogActivationConflict(
                        "active generation changed before candidate validation"
                    )
                staged = await _load_staged_entities(
                    connection,
                    generation_id=generation.id,
                    sync_generation=generation.sync_generation,
                )
                _validate_staged_progress(row, staged)
                effective = await _load_effective_entities(
                    connection,
                    generation_id=generation.id,
                    base_generation_id=generation.base_generation_id,
                    sync_generation=generation.sync_generation,
                )
                _validate_effective_constraints(*effective)
                validation = CandidateValidation(
                    generation_id=generation.id,
                    event_count=len(effective[0]),
                    market_count=len(effective[1]),
                    token_count=len(effective[2]),
                    snapshot_digest=_snapshot_digest(*effective),
                    validated_runtime_revision=int(row["runtime_revision"]),
                )
            finally:
                await connection.execute("ROLLBACK")
        return validation

    async def _freeze_candidate(
        self,
        generation: StagedGeneration,
        validation: CandidateValidation,
    ) -> None:
        now = self._clock()

        async def command(connection: aiosqlite.Connection) -> None:
            cursor = await connection.execute(
                """
                SELECT active_generation_id, runtime_revision
                FROM catalog_state WHERE id = 1
                """
            )
            state = await cursor.fetchone()
            if state is None or tuple(state) != (
                generation.base_generation_id,
                validation.validated_runtime_revision,
            ):
                raise CatalogActivationConflict(
                    "runtime revision or active generation changed during validation"
                )
            cursor = await connection.execute(
                """
                UPDATE catalog_generations
                SET candidate_event_count = ?, candidate_market_count = ?,
                    candidate_token_count = ?, candidate_snapshot_digest = ?,
                    validated_at = ?
                WHERE id = ? AND status = 'STAGING'
                """,
                (
                    validation.event_count,
                    validation.market_count,
                    validation.token_count,
                    validation.snapshot_digest,
                    now,
                    validation.generation_id,
                ),
            )
            if cursor.rowcount != 1:
                raise CatalogActivationConflict(
                    "candidate generation changed during validation"
                )

        await self._writer.execute(command)

    async def _create_or_resume_staging(
        self,
        value: GenerationInput,
        plans: tuple[_EntityPlan, ...],
    ) -> tuple[StagedGeneration, tuple[str | None, str | None, str | None]]:
        now = self._clock()

        async def command(connection: aiosqlite.Connection) -> StagedGeneration:
            cursor = await connection.execute(
                """
                SELECT id, sync_generation, input_digest, base_generation_id,
                       base_runtime_revision,
                       planned_event_count, planned_market_count, planned_token_count,
                       planned_event_digest, planned_market_digest, planned_token_digest,
                       event_cursor, market_cursor, token_cursor
                FROM catalog_generations WHERE status = 'STAGING'
                """
            )
            existing = await cursor.fetchone()
            counts = tuple(len(plan.values) for plan in plans)
            digests = tuple(plan.planned_digest for plan in plans)
            if existing is not None and existing[2] == value.input_digest:
                if tuple(existing[5:8]) != counts or tuple(existing[8:11]) != digests:
                    raise CatalogGenerationError(
                        "matching input digest has different normalized catalog payload"
                    )
                return (
                    StagedGeneration(
                        id=int(existing[0]),
                        sync_generation=str(existing[1]),
                        base_generation_id=int(existing[3]),
                        base_runtime_revision=int(existing[4]),
                    ),
                    tuple(existing[11:14]),
                )
            if existing is not None:
                await connection.execute(
                    """
                    UPDATE catalog_generations
                    SET status = 'ABORTED', aborted_at = ?,
                        failure_reason = 'superseded by different input digest'
                    WHERE id = ? AND status = 'STAGING'
                    """,
                    (now, existing[0]),
                )

            cursor = await connection.execute(
                """
                SELECT active_generation_id, runtime_revision
                FROM catalog_state WHERE id = 1
                """
            )
            state = await cursor.fetchone()
            if state is None:
                raise CatalogGenerationError("catalog_state singleton is missing")
            cursor = await connection.execute(
                """
                INSERT INTO catalog_generations (
                    sync_generation, input_digest, base_generation_id,
                    base_runtime_revision,
                    planned_event_count, planned_market_count, planned_token_count,
                    planned_event_digest, planned_market_digest, planned_token_digest,
                    written_event_digest, written_market_digest, written_token_digest,
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'STAGING', ?)
                """,
                (
                    value.sync_generation,
                    value.input_digest,
                    state[0],
                    state[1],
                    *counts,
                    *digests,
                    *(
                        plan.planned_digest if not plan.values else None
                        for plan in plans
                    ),
                    now,
                ),
            )
            if cursor.lastrowid is None:
                raise CatalogGenerationError("failed to create staging generation")
            return (
                StagedGeneration(
                    id=int(cursor.lastrowid),
                    sync_generation=value.sync_generation,
                    base_generation_id=int(state[0]),
                    base_runtime_revision=int(state[1]),
                ),
                (None, None, None),
            )

        return await self._writer.execute(command)

    async def _write_batch(
        self,
        *,
        generation_id: int,
        plan: _EntityPlan,
        start: int,
        end: int,
    ) -> None:
        count_column, digest_column, cursor_column = _kind_columns(plan.kind)
        batch = plan.values[start:end]
        written_digest = _digest_payloads(plan.payloads[:end])

        async def command(connection: aiosqlite.Connection) -> None:
            for entity in batch:
                await _upsert_entity(connection, generation_id, entity)
            cursor = await connection.execute(
                f"""
                UPDATE catalog_generations
                SET {count_column} = ?, {digest_column} = ?, {cursor_column} = ?
                WHERE id = ? AND status = 'STAGING'
                """,
                (end, written_digest, batch[-1].id, generation_id),
            )
            if cursor.rowcount != 1:
                raise CatalogGenerationError("staging generation is no longer writable")

        await self._writer.execute(command)

    async def _pause_for_wal(
        self,
        generation_id: int,
        *,
        sync_generation: str,
        wal_peak_bytes: int,
    ) -> tuple[int, int]:
        for _ in range(self._max_pause_checkpoints):
            checkpoint = await self._writer.checkpoint(CheckpointMode.RESTART)
            wal_peak_bytes = max(wal_peak_bytes, checkpoint.wal_bytes)
            await asyncio.sleep(0)
            if checkpoint.wal_bytes < self._wal_policy.pause_bytes:
                return checkpoint.wal_bytes, wal_peak_bytes
            if checkpoint.wal_bytes >= self._wal_policy.abort_bytes:
                break
        await self._abort_generation(
            generation_id,
            "WAL could not be reclaimed while catalog staging was paused",
        )
        raise CatalogSyncAborted(
            generation=sync_generation,
            reason="catalog staging WAL pause could not be cleared",
            wal_peak_bytes=wal_peak_bytes,
        )

    async def _abort_generation(self, generation_id: int, reason: str) -> None:
        now = self._clock()

        async def command(connection: aiosqlite.Connection) -> None:
            await connection.execute(
                """
                UPDATE catalog_generations
                SET status = 'ABORTED', aborted_at = ?, failure_reason = ?
                WHERE id = ? AND status = 'STAGING'
                """,
                (now, reason, generation_id),
            )

        await self._writer.execute(command)


async def write_runtime_catalog(
    connection: aiosqlite.Connection,
    *,
    events: Sequence[Event] = (),
    markets: Sequence[Market] = (),
    tokens: Sequence[Token] = (),
    changed_at: int,
) -> int | None:
    """Apply winning runtime versions and journal one atomic catalog revision."""

    if type(changed_at) is not int or changed_at < 0:
        raise ValueError("changed_at must be a non-negative integer")
    materialized: tuple[tuple[_Entity, ...], ...] = (
        tuple(events),
        tuple(markets),
        tuple(tokens),
    )
    for label, values, expected_type in zip(
        ("events", "markets", "tokens"),
        materialized,
        (Event, Market, Token),
        strict=True,
    ):
        if any(not isinstance(value, expected_type) for value in values):
            raise TypeError(f"{label} must contain only {expected_type.__name__}")
        identifiers = tuple(value.id for value in values)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError(f"{label} contains duplicate IDs")

    cursor = await connection.execute(
        """
        SELECT state.active_generation_id, state.runtime_revision,
               generations.sync_generation
        FROM catalog_state AS state
        JOIN catalog_generations AS generations
          ON generations.id = state.active_generation_id
        WHERE state.id = 1 AND generations.status = 'COMMITTED'
        """
    )
    cursor.row_factory = aiosqlite.Row
    state = await cursor.fetchone()
    if state is None:
        raise CatalogGenerationError("active catalog generation is missing")
    generation_id = int(state["active_generation_id"])
    runtime_revision = int(state["runtime_revision"])
    sync_generation = str(state["sync_generation"])
    current = await _load_effective_entities(
        connection,
        generation_id=-1,
        base_generation_id=generation_id,
        sync_generation=sync_generation,
    )
    current_maps = tuple({entity.id: entity for entity in values} for values in current)
    prospective_maps = tuple(dict(values) for values in current_maps)

    for index, values in enumerate(materialized):
        for entity in values:
            normalized = replace(
                entity,
                sync_generation=sync_generation,
                sync_generation_complete=True,
            )
            existing = prospective_maps[index].get(entity.id)
            if existing is None or normalized.updated_at >= existing.updated_at:
                prospective_maps[index][entity.id] = normalized

    market_ids_by_event: dict[str, list[str]] = {
        event_id: [] for event_id in prospective_maps[0]
    }
    for market in prospective_maps[1].values():
        if market.event_id in market_ids_by_event:
            market_ids_by_event[market.event_id].append(market.id)
    for event_id, event in tuple(prospective_maps[0].items()):
        prospective_maps[0][event_id] = replace(
            event,
            market_ids=tuple(
                sorted(
                    market_ids_by_event[event_id],
                    key=lambda value: value.encode("utf-8"),
                )
            ),
        )

    prospective = tuple(
        tuple(
            values[entity_id]
            for entity_id in sorted(values, key=lambda value: value.encode("utf-8"))
        )
        for values in prospective_maps
    )
    _validate_effective_constraints(*prospective)
    changed = tuple(
        tuple(
            entity
            for entity in values
            if (
                entity.id not in current_maps[index]
                or _canonical_payload(entity)
                != _canonical_payload(current_maps[index][entity.id])
            )
        )
        for index, values in enumerate(prospective)
    )
    if not any(changed):
        return None

    changed_events, changed_markets, changed_tokens = changed
    if changed_events:
        await connection.executemany(
            "INSERT INTO catalog_event_ids (id, created_at) VALUES (?, ?) "
            "ON CONFLICT(id) DO NOTHING",
            ((event.id, event.created_at) for event in changed_events),
        )
        await connection.executemany(
            _UPSERT_EVENT_VERSION,
            (_event_values(generation_id, event) for event in changed_events),
        )
    if changed_markets:
        await connection.executemany(
            """
            INSERT INTO catalog_market_ids (id, event_id, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET event_id = excluded.event_id
            """,
            (
                (market.id, market.event_id, market.created_at)
                for market in changed_markets
            ),
        )
        await connection.executemany(
            _UPSERT_MARKET_VERSION,
            (_market_values(generation_id, market) for market in changed_markets),
        )
    if changed_tokens:
        await connection.executemany(
            """
            INSERT INTO catalog_token_ids (id, market_id, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET market_id = excluded.market_id
            """,
            ((token.id, token.market_id, token.created_at) for token in changed_tokens),
        )
        await connection.executemany(
            _UPSERT_TOKEN_VERSION,
            (_token_values(generation_id, token) for token in changed_tokens),
        )

    next_revision = runtime_revision + 1
    cursor = await connection.execute(
        """
        UPDATE catalog_state SET runtime_revision = ?
        WHERE id = 1 AND active_generation_id = ? AND runtime_revision = ?
        """,
        (next_revision, generation_id, runtime_revision),
    )
    if cursor.rowcount != 1:
        raise CatalogActivationConflict(
            "active generation or runtime revision changed during runtime write"
        )
    for entity_type, values in zip(
        ("EVENT", "MARKET", "TOKEN"),
        changed,
        strict=True,
    ):
        if values:
            await connection.executemany(
                """
                INSERT INTO catalog_runtime_changes (
                    runtime_revision, entity_type, entity_id,
                    generation_id, updated_at, changed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        next_revision,
                        entity_type,
                        entity.id,
                        generation_id,
                        entity.updated_at,
                        changed_at,
                    )
                    for entity in values
                ),
            )
    return next_revision


def catalog_input_digest(
    *,
    sync_generation: str,
    updated_at: int,
    events: Sequence[Event],
    markets: Sequence[Market],
    tokens: Sequence[Token],
) -> str:
    """Return a canonical digest for one normalized complete-sync input."""

    plans = tuple(
        _make_plan(kind, tuple(values))
        for kind, values in (
            ("event", events),
            ("market", markets),
            ("token", tokens),
        )
    )
    return hashlib.sha256(
        json.dumps(
            {
                "sync_generation": sync_generation,
                "updated_at": updated_at,
                "event_digest": plans[0].planned_digest,
                "market_digest": plans[1].planned_digest,
                "token_digest": plans[2].planned_digest,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


async def _load_staged_entities(
    connection: aiosqlite.Connection,
    *,
    generation_id: int,
    sync_generation: str,
) -> tuple[tuple[Event, ...], tuple[Market, ...], tuple[Token, ...]]:
    return (
        tuple(
            _event_from_version_row(row, sync_generation)
            for row in await _fetch_version_rows(
                connection,
                kind="event",
                generation_id=generation_id,
            )
        ),
        tuple(
            _market_from_version_row(row, sync_generation)
            for row in await _fetch_version_rows(
                connection,
                kind="market",
                generation_id=generation_id,
            )
        ),
        tuple(
            _token_from_version_row(row, sync_generation)
            for row in await _fetch_version_rows(
                connection,
                kind="token",
                generation_id=generation_id,
            )
        ),
    )


async def _select_cleanup_versions(
    connection: aiosqlite.Connection,
    *,
    table: str,
    active_generation_id: int,
    after_entity: str | None,
    after_generation: int | None,
    limit: int,
) -> list[tuple[str, int]]:
    cursor_filter = ""
    parameters: list[object] = [active_generation_id, active_generation_id]
    if after_entity is not None and after_generation is not None:
        cursor_filter = """
          AND (
              CAST(versions.entity_id AS BLOB) > CAST(? AS BLOB)
              OR (
                  versions.entity_id = ?
                  AND versions.generation_id > ?
              )
          )
        """
        parameters.extend((after_entity, after_entity, after_generation))
    parameters.append(limit)
    cursor = await connection.execute(
        f"""
        SELECT versions.entity_id, versions.generation_id
        FROM {table} AS versions
        JOIN catalog_generations AS generation
          ON generation.id = versions.generation_id
        WHERE (
            generation.status = 'ABORTED'
            OR (
                generation.status = 'COMMITTED'
                AND versions.generation_id <= ?
                AND versions.generation_id < (
                    SELECT MAX(candidate.generation_id)
                    FROM {table} AS candidate
                    JOIN catalog_generations AS candidate_generation
                      ON candidate_generation.id = candidate.generation_id
                     AND candidate_generation.status = 'COMMITTED'
                    WHERE candidate.entity_id = versions.entity_id
                      AND candidate.generation_id <= ?
                )
            )
        )
        {cursor_filter}
        ORDER BY CAST(versions.entity_id AS BLOB), versions.generation_id
        LIMIT ?
        """,
        parameters,
    )
    return [(str(row[0]), int(row[1])) for row in await cursor.fetchall()]


async def _count_cleanup_versions(
    connection: aiosqlite.Connection,
    *,
    active_generation_id: int,
) -> int:
    total = 0
    for table in ("event_versions", "market_versions", "token_versions"):
        cursor = await connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {table} AS versions
            JOIN catalog_generations AS generation
              ON generation.id = versions.generation_id
            WHERE generation.status = 'ABORTED'
               OR (
                    generation.status = 'COMMITTED'
                    AND versions.generation_id <= ?
                    AND versions.generation_id < (
                        SELECT MAX(candidate.generation_id)
                        FROM {table} AS candidate
                        JOIN catalog_generations AS candidate_generation
                          ON candidate_generation.id = candidate.generation_id
                         AND candidate_generation.status = 'COMMITTED'
                        WHERE candidate.entity_id = versions.entity_id
                          AND candidate.generation_id <= ?
                    )
               )
            """,
            (active_generation_id, active_generation_id),
        )
        row = await cursor.fetchone()
        total += int(row[0])
    return total


async def _generation_sync_name(
    connection: aiosqlite.Connection,
    generation_id: int,
) -> str:
    cursor = await connection.execute(
        "SELECT sync_generation FROM catalog_generations WHERE id = ?",
        (generation_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        raise CatalogGenerationError(f"catalog generation {generation_id} is missing")
    return str(row[0])


async def _load_effective_entities(
    connection: aiosqlite.Connection,
    *,
    generation_id: int,
    base_generation_id: int,
    sync_generation: str,
) -> tuple[tuple[Event, ...], tuple[Market, ...], tuple[Token, ...]]:
    return (
        tuple(
            _event_from_version_row(row, sync_generation)
            for row in await _fetch_version_rows(
                connection,
                kind="event",
                generation_id=generation_id,
                base_generation_id=base_generation_id,
            )
        ),
        tuple(
            _market_from_version_row(row, sync_generation)
            for row in await _fetch_version_rows(
                connection,
                kind="market",
                generation_id=generation_id,
                base_generation_id=base_generation_id,
            )
        ),
        tuple(
            _token_from_version_row(row, sync_generation)
            for row in await _fetch_version_rows(
                connection,
                kind="token",
                generation_id=generation_id,
                base_generation_id=base_generation_id,
            )
        ),
    )


async def _fetch_version_rows(
    connection: aiosqlite.Connection,
    *,
    kind: str,
    generation_id: int,
    base_generation_id: int | None = None,
) -> list[aiosqlite.Row]:
    identity_table, version_table, columns = _VERSION_QUERIES[kind]
    if base_generation_id is None:
        where = "versions.generation_id = ?"
        parameters = (generation_id,)
    else:
        where = f"""
        versions.generation_id = (
            SELECT candidate.generation_id
            FROM {version_table} AS candidate
            JOIN catalog_generations AS candidate_generation
              ON candidate_generation.id = candidate.generation_id
            WHERE candidate.entity_id = identities.id
              AND (
                  (
                      candidate.generation_id = ?
                      AND candidate_generation.status = 'STAGING'
                  ) OR (
                      candidate.generation_id <= ?
                      AND candidate_generation.status = 'COMMITTED'
                  )
              )
            ORDER BY candidate.updated_at DESC,
                     CASE WHEN candidate.generation_id = ? THEN 1 ELSE 0 END DESC,
                     candidate.generation_id DESC
            LIMIT 1
        )
        """
        parameters = (generation_id, base_generation_id, generation_id)
    cursor = await connection.execute(
        f"""
        SELECT {columns}
        FROM {identity_table} AS identities
        JOIN {version_table} AS versions
          ON versions.entity_id = identities.id
        WHERE {where}
        ORDER BY CAST(identities.id AS BLOB)
        """,
        parameters,
    )
    cursor.row_factory = aiosqlite.Row
    return list(await cursor.fetchall())


_VERSION_QUERIES = {
    "event": (
        "catalog_event_ids",
        "event_versions",
        """
        identities.id AS id, versions.slug, versions.title,
        versions.description, versions.status, versions.neg_risk,
        versions.neg_risk_id, versions.neg_risk_type,
        versions.neg_risk_complete,
        versions.neg_risk_conversion_supported,
        versions.neg_risk_metadata_json, versions.neg_risk_synced_at,
        versions.market_ids_json, versions.start_at, versions.end_at,
        versions.resolved_at, versions.source_updated_at,
        versions.created_at, versions.updated_at
        """,
    ),
    "market": (
        "catalog_market_ids",
        "market_versions",
        """
        identities.id AS id, identities.event_id, versions.condition_id,
        versions.slug, versions.question, versions.description,
        versions.status, versions.active, versions.accepting_orders,
        versions.enable_orderbook, versions.neg_risk,
        versions.neg_risk_outcome_position,
        versions.neg_risk_member_complete, versions.tick_size,
        versions.minimum_order_size, versions.end_at, versions.resolved_at,
        versions.source_updated_at, versions.created_at, versions.updated_at
        """,
    ),
    "token": (
        "catalog_token_ids",
        "token_versions",
        """
        identities.id AS id, identities.market_id, versions.outcome,
        versions.position, versions.fee_schedule_json, versions.fee_updated_at,
        versions.created_at, versions.updated_at
        """,
    ),
}


def _event_from_version_row(row: aiosqlite.Row, sync_generation: str) -> Event:
    return Event(
        id=row["id"],
        slug=row["slug"],
        title=row["title"],
        description=row["description"],
        status=MarketStatus(row["status"]),
        neg_risk=bool(row["neg_risk"]),
        neg_risk_id=row["neg_risk_id"],
        neg_risk_type=row["neg_risk_type"],
        neg_risk_complete=bool(row["neg_risk_complete"]),
        neg_risk_conversion_supported=bool(
            row["neg_risk_conversion_supported"]
        ),
        neg_risk_metadata=(
            None
            if row["neg_risk_metadata_json"] is None
            else json.loads(row["neg_risk_metadata_json"])
        ),
        neg_risk_synced_at=row["neg_risk_synced_at"],
        market_ids=tuple(json.loads(row["market_ids_json"])),
        sync_generation=sync_generation,
        sync_generation_complete=True,
        start_at=row["start_at"],
        end_at=row["end_at"],
        resolved_at=row["resolved_at"],
        source_updated_at=row["source_updated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _market_from_version_row(row: aiosqlite.Row, sync_generation: str) -> Market:
    return Market(
        id=row["id"],
        event_id=row["event_id"],
        condition_id=row["condition_id"],
        slug=row["slug"],
        question=row["question"],
        description=row["description"],
        status=MarketStatus(row["status"]),
        active=bool(row["active"]),
        accepting_orders=bool(row["accepting_orders"]),
        enable_orderbook=bool(row["enable_orderbook"]),
        neg_risk=bool(row["neg_risk"]),
        neg_risk_outcome_position=row["neg_risk_outcome_position"],
        neg_risk_member_complete=bool(row["neg_risk_member_complete"]),
        sync_generation=sync_generation,
        sync_generation_complete=True,
        tick_size=(
            None if row["tick_size"] is None else decode_decimal(row["tick_size"])
        ),
        minimum_order_size=(
            None
            if row["minimum_order_size"] is None
            else decode_decimal(row["minimum_order_size"])
        ),
        end_at=row["end_at"],
        resolved_at=row["resolved_at"],
        source_updated_at=row["source_updated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _token_from_version_row(row: aiosqlite.Row, sync_generation: str) -> Token:
    return Token(
        id=row["id"],
        market_id=row["market_id"],
        outcome=row["outcome"],
        position=row["position"],
        fee_schedule=(
            None
            if row["fee_schedule_json"] is None
            else FeeSchedule.from_json(json.loads(row["fee_schedule_json"]))
        ),
        fee_updated_at=row["fee_updated_at"],
        sync_generation=sync_generation,
        sync_generation_complete=True,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _validate_staged_progress(
    row: aiosqlite.Row,
    staged: tuple[tuple[Event, ...], tuple[Market, ...], tuple[Token, ...]],
) -> None:
    for kind, values in zip(("event", "market", "token"), staged, strict=True):
        actual_digest = _digest_payloads(
            tuple(_canonical_payload(value) for value in values)
        )
        if (
            row[f"planned_{kind}_count"] != len(values)
            or row[f"written_{kind}_count"] != len(values)
            or row[f"planned_{kind}_digest"] != actual_digest
            or row[f"written_{kind}_digest"] != actual_digest
        ):
            raise CatalogConstraintError(
                f"staged {kind} count or digest does not match persisted payload"
            )


def _validate_effective_constraints(
    events: tuple[Event, ...],
    markets: tuple[Market, ...],
    tokens: tuple[Token, ...],
) -> None:
    _require_unique(
        ((event.slug, event.id) for event in events if event.slug is not None),
        "event slug",
    )
    _require_unique(
        ((market.slug, market.id) for market in markets if market.slug is not None),
        "market slug",
    )
    _require_unique(
        ((market.condition_id, market.id) for market in markets),
        "market condition ID",
    )
    event_ids = {event.id for event in events}
    for market in markets:
        if market.event_id is not None and market.event_id not in event_ids:
            raise CatalogConstraintError(
                f"market parent event is missing for {market.id!r}"
            )
    market_ids = {market.id for market in markets}
    for token in tokens:
        if token.market_id not in market_ids:
            raise CatalogConstraintError(
                f"token parent market is missing for {token.id!r}"
            )
    _require_unique(
        (((token.market_id, token.position), token.id) for token in tokens),
        "token position for market",
    )
    _require_unique(
        (((token.market_id, token.outcome), token.id) for token in tokens),
        "token outcome for market",
    )


def _require_unique(values: Any, label: str) -> None:
    seen: dict[Any, str] = {}
    for value, entity_id in values:
        prior = seen.setdefault(value, entity_id)
        if prior != entity_id:
            raise CatalogConstraintError(
                f"duplicate {label} {value!r} on {prior!r} and {entity_id!r}"
            )


def _snapshot_digest(
    events: tuple[Event, ...],
    markets: tuple[Market, ...],
    tokens: tuple[Token, ...],
) -> str:
    digest = hashlib.sha256()
    for kind, values in (
        ("event", events),
        ("market", markets),
        ("token", tokens),
    ):
        digest.update(kind.encode("ascii"))
        digest.update(len(values).to_bytes(8, "big"))
        for value in values:
            payload = _canonical_payload(value)
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


def _prepare_reconciliation(
    reconciliation: PendingCatalogReconciliation | None,
) -> tuple[MarketChange, tuple[str, ...]] | None:
    if reconciliation is None:
        return None
    change = getattr(reconciliation, "change", None)
    market_ids = getattr(reconciliation, "market_ids", None)
    if not isinstance(change, MarketChange):
        raise TypeError("reconciliation.change must be MarketChange")
    if change.change_type is not MarketChangeType.CATALOG_RECONCILED:
        raise ValueError("reconciliation change must be CATALOG_RECONCILED")
    if not isinstance(market_ids, tuple) or any(
        not isinstance(market_id, str) or not market_id for market_id in market_ids
    ):
        raise ValueError("reconciliation.market_ids must contain non-empty IDs")
    if len(market_ids) != len(set(market_ids)):
        raise ValueError("reconciliation.market_ids must not contain duplicates")
    return change, tuple(sorted(market_ids, key=lambda value: value.encode("utf-8")))


def _reconciliation_details(
    *,
    sync_generation: str,
    change: MarketChange,
    market_ids: tuple[str, ...],
) -> str:
    return json.dumps(
        {
            "change_id": change.change_id,
            "change_type": change.change_type.value,
            "critical": change.critical,
            "event_id": change.event_id,
            "market_id": change.market_id,
            "market_ids": market_ids,
            "sync_generation": sync_generation,
            "token_ids": change.token_ids,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _build_plans(value: GenerationInput) -> tuple[_EntityPlan, ...]:
    return tuple(
        _make_plan(kind, values)
        for kind, values in (
            ("event", value.events),
            ("market", value.markets),
            ("token", value.tokens),
        )
    )


def _make_plan(kind: str, values: tuple[_Entity, ...]) -> _EntityPlan:
    ordered = tuple(sorted(values, key=lambda entity: entity.id.encode("utf-8")))
    payloads = tuple(_canonical_payload(entity) for entity in ordered)
    return _EntityPlan(
        kind=kind,
        values=ordered,
        payloads=payloads,
        planned_digest=_digest_payloads(payloads),
    )


def _canonical_payload(entity: _Entity) -> bytes:
    return json.dumps(
        _json_value(entity),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return encode_decimal(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    return value


def _digest_payloads(payloads: tuple[bytes, ...]) -> str:
    digest = hashlib.sha256()
    for payload in payloads:
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _resume_index(values: tuple[_Entity, ...], cursor: str | None) -> int:
    if cursor is None:
        return 0
    for index, entity in enumerate(values):
        if entity.id == cursor:
            return index + 1
    raise CatalogGenerationError(f"persisted cursor {cursor!r} is absent from input")


def _batch_end(
    payloads: tuple[bytes, ...],
    *,
    start: int,
    max_rows: int,
    max_payload_bytes: int,
) -> int:
    end = start
    payload_bytes = 0
    while end < len(payloads) and end - start < max_rows:
        candidate_bytes = len(payloads[end])
        if end > start and payload_bytes + candidate_bytes > max_payload_bytes:
            break
        payload_bytes += candidate_bytes
        end += 1
        if payload_bytes >= max_payload_bytes:
            break
    return end


def _kind_columns(kind: str) -> tuple[str, str, str]:
    if kind not in {"event", "market", "token"}:
        raise ValueError(f"unknown catalog entity kind {kind!r}")
    return (
        f"written_{kind}_count",
        f"written_{kind}_digest",
        f"{kind}_cursor",
    )


async def _upsert_entity(
    connection: aiosqlite.Connection,
    generation_id: int,
    entity: _Entity,
) -> None:
    if isinstance(entity, Event):
        await connection.execute(
            "INSERT INTO catalog_event_ids (id, created_at) VALUES (?, ?) "
            "ON CONFLICT(id) DO NOTHING",
            (entity.id, entity.created_at),
        )
        await connection.execute(_UPSERT_EVENT_VERSION, _event_values(generation_id, entity))
        return
    if isinstance(entity, Market):
        await connection.execute(
            """
            INSERT INTO catalog_market_ids (id, event_id, created_at)
            VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING
            """,
            (entity.id, entity.event_id, entity.created_at),
        )
        await connection.execute(
            _UPSERT_MARKET_VERSION,
            _market_values(generation_id, entity),
        )
        return
    await connection.execute(
        """
        INSERT INTO catalog_token_ids (id, market_id, created_at)
        VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING
        """,
        (entity.id, entity.market_id, entity.created_at),
    )
    await connection.execute(_UPSERT_TOKEN_VERSION, _token_values(generation_id, entity))


_UPSERT_EVENT_VERSION = """
INSERT INTO event_versions (
    entity_id, generation_id, slug, title, description, status, neg_risk,
    neg_risk_id, neg_risk_type, neg_risk_complete,
    neg_risk_conversion_supported, neg_risk_metadata_json,
    neg_risk_synced_at, market_ids_json, start_at, end_at, resolved_at,
    source_updated_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(entity_id, generation_id) DO UPDATE SET
    slug=excluded.slug, title=excluded.title, description=excluded.description,
    status=excluded.status, neg_risk=excluded.neg_risk,
    neg_risk_id=excluded.neg_risk_id, neg_risk_type=excluded.neg_risk_type,
    neg_risk_complete=excluded.neg_risk_complete,
    neg_risk_conversion_supported=excluded.neg_risk_conversion_supported,
    neg_risk_metadata_json=excluded.neg_risk_metadata_json,
    neg_risk_synced_at=excluded.neg_risk_synced_at,
    market_ids_json=excluded.market_ids_json, start_at=excluded.start_at,
    end_at=excluded.end_at, resolved_at=excluded.resolved_at,
    source_updated_at=excluded.source_updated_at, created_at=excluded.created_at,
    updated_at=excluded.updated_at
"""

_UPSERT_MARKET_VERSION = """
INSERT INTO market_versions (
    entity_id, generation_id, condition_id, slug, question, description,
    status, active, accepting_orders, enable_orderbook, neg_risk,
    neg_risk_outcome_position, neg_risk_member_complete, tick_size,
    minimum_order_size, end_at, resolved_at, source_updated_at,
    created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(entity_id, generation_id) DO UPDATE SET
    condition_id=excluded.condition_id, slug=excluded.slug,
    question=excluded.question, description=excluded.description,
    status=excluded.status, active=excluded.active,
    accepting_orders=excluded.accepting_orders,
    enable_orderbook=excluded.enable_orderbook, neg_risk=excluded.neg_risk,
    neg_risk_outcome_position=excluded.neg_risk_outcome_position,
    neg_risk_member_complete=excluded.neg_risk_member_complete,
    tick_size=excluded.tick_size, minimum_order_size=excluded.minimum_order_size,
    end_at=excluded.end_at, resolved_at=excluded.resolved_at,
    source_updated_at=excluded.source_updated_at, created_at=excluded.created_at,
    updated_at=excluded.updated_at
"""

_UPSERT_TOKEN_VERSION = """
INSERT INTO token_versions (
    entity_id, generation_id, outcome, position, fee_schedule_json,
    fee_updated_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(entity_id, generation_id) DO UPDATE SET
    outcome=excluded.outcome, position=excluded.position,
    fee_schedule_json=excluded.fee_schedule_json,
    fee_updated_at=excluded.fee_updated_at, created_at=excluded.created_at,
    updated_at=excluded.updated_at
"""


def _event_values(generation_id: int, event: Event) -> tuple[Any, ...]:
    return (
        event.id,
        generation_id,
        event.slug,
        event.title,
        event.description,
        event.status.value,
        int(event.neg_risk),
        event.neg_risk_id,
        event.neg_risk_type,
        int(event.neg_risk_complete),
        int(event.neg_risk_conversion_supported),
        _json_or_none(event.neg_risk_metadata),
        event.neg_risk_synced_at,
        _json(event.market_ids),
        event.start_at,
        event.end_at,
        event.resolved_at,
        event.source_updated_at,
        event.created_at,
        event.updated_at,
    )


def _market_values(generation_id: int, market: Market) -> tuple[Any, ...]:
    return (
        market.id,
        generation_id,
        market.condition_id,
        market.slug,
        market.question,
        market.description,
        market.status.value,
        int(market.active),
        int(market.accepting_orders),
        int(market.enable_orderbook),
        int(market.neg_risk),
        market.neg_risk_outcome_position,
        int(market.neg_risk_member_complete),
        None if market.tick_size is None else encode_decimal(market.tick_size),
        (
            None
            if market.minimum_order_size is None
            else encode_decimal(market.minimum_order_size)
        ),
        market.end_at,
        market.resolved_at,
        market.source_updated_at,
        market.created_at,
        market.updated_at,
    )


def _token_values(generation_id: int, token: Token) -> tuple[Any, ...]:
    return (
        token.id,
        generation_id,
        token.outcome,
        token.position,
        _fee_json(token.fee_schedule),
        token.fee_updated_at,
        token.created_at,
        token.updated_at,
    )


def _json(value: Any) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _json_or_none(value: Any | None) -> str | None:
    return None if value is None else _json(value)


def _fee_json(value: FeeSchedule | None) -> str | None:
    return None if value is None else _json(value)
