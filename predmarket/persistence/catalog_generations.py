"""Recoverable, WAL-bounded staging for catalog generations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal
from enum import Enum
import hashlib
import json
import time
from typing import Any, Protocol, TypeVar

import aiosqlite

from predmarket.domain.decimal import encode_decimal
from predmarket.domain.fees import FeeSchedule
from predmarket.domain.market import Event, Market, Token
from predmarket.persistence.wal import (
    WalAction,
    WalPolicy,
    decide_wal_action,
    update_wal_amplification,
)
from predmarket.persistence.writer import CheckpointMode, CheckpointResult


MiB = 1024 * 1024
_Entity = Event | Market | Token
_T = TypeVar("_T")


class CatalogGenerationError(RuntimeError):
    """Base error for generation staging."""


class CatalogSyncAborted(CatalogGenerationError):
    """Raised when WAL safety requires abandoning a complete sync."""


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
        clock: Callable[[], int] = lambda: int(time.time()),
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
        self._writer = writer
        self._batch_limits = batch_limits
        self._wal_policy = wal_policy
        self._wal_amplification = float(initial_wal_amplification)
        self._max_pause_checkpoints = max_pause_checkpoints
        self._clock = clock

    async def stage(self, value: GenerationInput) -> StagedGeneration:
        """Create or resume one bounded, invisible staging generation."""

        if not isinstance(value, GenerationInput):
            raise TypeError("value must be GenerationInput")
        plans = _build_plans(value)
        preflight = await self._writer.checkpoint(CheckpointMode.TRUNCATE)
        if preflight.wal_bytes >= self._wal_policy.preflight_bytes:
            raise CatalogSyncAborted(
                "catalog WAL preflight could not reclaim below the safety threshold"
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
                            "catalog staging cannot safely write a single-row batch"
                        )
                    adaptive_max_rows = max(1, (batch_end - start) // 2)
                    continue
                if action is WalAction.ABORT:
                    await self._abort_generation(
                        generation.id,
                        "WAL reached the abort waterline",
                    )
                    raise CatalogSyncAborted("catalog staging reached the WAL abort waterline")
                if action is WalAction.PAUSE_AND_CHECKPOINT:
                    wal_bytes = await self._pause_for_wal(generation.id)
                    continue
                if action is WalAction.PASSIVE_CHECKPOINT:
                    checkpoint = await self._writer.checkpoint(CheckpointMode.PASSIVE)
                    wal_bytes = checkpoint.wal_bytes
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
                checkpoint = await self._writer.checkpoint(CheckpointMode.PASSIVE)
                wal_delta = max(0, checkpoint.wal_bytes - wal_bytes)
                self._wal_amplification = update_wal_amplification(
                    current=self._wal_amplification,
                    payload_bytes=payload_bytes,
                    wal_delta_bytes=wal_delta,
                )
                wal_bytes = checkpoint.wal_bytes
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
                        "catalog staging reached the WAL abort waterline"
                    )
                if post_batch_action is WalAction.PAUSE_AND_CHECKPOINT:
                    wal_bytes = await self._pause_for_wal(generation.id)
                start = batch_end
                await asyncio.sleep(0)
        return generation

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
                    status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'STAGING', ?)
                """,
                (
                    value.sync_generation,
                    value.input_digest,
                    state[0],
                    state[1],
                    *counts,
                    *digests,
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

    async def _pause_for_wal(self, generation_id: int) -> int:
        for _ in range(self._max_pause_checkpoints):
            checkpoint = await self._writer.checkpoint(CheckpointMode.RESTART)
            await asyncio.sleep(0)
            if checkpoint.wal_bytes < self._wal_policy.pause_bytes:
                return checkpoint.wal_bytes
            if checkpoint.wal_bytes >= self._wal_policy.abort_bytes:
                break
        await self._abort_generation(
            generation_id,
            "WAL could not be reclaimed while catalog staging was paused",
        )
        raise CatalogSyncAborted("catalog staging WAL pause could not be cleared")

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
