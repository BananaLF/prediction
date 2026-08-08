from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

import aiosqlite

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.catalog_generations import (
    CatalogGenerationCoordinator,
    GenerationInput,
)
from predmarket.persistence.repositories import CatalogRepository, SystemEventRepository
from predmarket.persistence.schema import create_v4_database
from predmarket.persistence.writer import CheckpointMode, CheckpointResult


class _V4Writer:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def execute(self, command: Any) -> Any:
        async with aiosqlite.connect(self.path, isolation_level=None) as connection:
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("BEGIN IMMEDIATE")
            try:
                result = await command(connection)
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise
            return result

    async def checkpoint(
        self,
        mode: CheckpointMode = CheckpointMode.PASSIVE,
    ) -> CheckpointResult:
        return CheckpointResult(
            mode=mode,
            busy=0,
            log_pages=0,
            checkpointed_pages=0,
            wal_bytes=0,
        )


def _input(generation: str, *, title: str) -> GenerationInput:
    event = Event(
        id="event-1",
        slug="event-slug",
        title=title,
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    market = Market(
        id="market-1",
        event_id=event.id,
        condition_id="condition-1",
        slug="market-slug",
        question=f"{title}?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation=generation,
        sync_generation_complete=True,
        updated_at=10,
    )
    return GenerationInput(
        sync_generation=generation,
        updated_at=10,
        events=(event,),
        markets=(market,),
        tokens=(token,),
        input_digest=f"digest-{generation}",
    )


async def test_reader_sees_only_complete_generation_across_activation(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_V4Writer(database_path))
    old = await coordinator.stage(_input("sync-old", title="Old"))
    await coordinator.activate(await coordinator.validate_candidate(old), None)
    new = await coordinator.stage(_input("sync-new", title="New"))
    validation = await coordinator.validate_candidate(new)

    async with aiosqlite.connect(database_path, isolation_level=None) as reader:
        await reader.execute("BEGIN")
        old_event_generation = (
            await (await reader.execute("SELECT sync_generation FROM events")).fetchone()
        )[0]
        await coordinator.activate(validation, None)
        old_market_generation = (
            await (await reader.execute("SELECT sync_generation FROM markets")).fetchone()
        )[0]
        old_token_generation = (
            await (await reader.execute("SELECT sync_generation FROM tokens")).fetchone()
        )[0]
        await reader.execute("ROLLBACK")

        await reader.execute("BEGIN")
        visible_generations: list[str] = []
        for table in ("events", "markets", "tokens"):
            cursor = await reader.execute(f"SELECT sync_generation FROM {table}")
            row = await cursor.fetchone()
            assert row is not None
            visible_generations.append(str(row[0]))
        new_generations = tuple(visible_generations)
        await reader.execute("ROLLBACK")

    assert (
        old_event_generation,
        old_market_generation,
        old_token_generation,
    ) == ("sync-old", "sync-old", "sync-old")
    assert new_generations == ("sync-new", "sync-new", "sync-new")


async def test_v4_repository_runs_stage_validate_activate_and_unique_outbox(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    writer = _V4Writer(database_path)
    catalog = CatalogRepository(database_path, writer)  # type: ignore[arg-type]
    system_events = SystemEventRepository(database_path, writer)  # type: ignore[arg-type]
    value = _input("sync-repository", title="Repository")
    change = MarketChange(
        change_id="sync-repository:CATALOG_RECONCILED:catalog",
        change_type=MarketChangeType.CATALOG_RECONCILED,
        event_id=None,
        market_id=None,
        token_ids=(),
        occurred_at=10,
        critical=True,
    )

    await catalog.save_complete_catalog(
        generation=value.sync_generation,
        updated_at=value.updated_at,
        events=value.events,
        markets=value.markets,
        tokens=value.tokens,
        reconciliation_change=change,
        reconciliation_market_ids=("market-1",),
    )
    await catalog.save_complete_catalog(
        generation=value.sync_generation,
        updated_at=value.updated_at,
        events=value.events,
        markets=value.markets,
        tokens=value.tokens,
        reconciliation_change=change,
        reconciliation_market_ids=("market-1",),
    )

    snapshot = await catalog.load_catalog()
    pending = await system_events.list_pending_catalog_reconciliations()
    assert snapshot.events == value.events
    assert snapshot.markets == value.markets
    assert snapshot.tokens == value.tokens
    assert len(pending) == 1
    assert pending[0].change == change
    assert pending[0].market_ids == ("market-1",)


async def test_effective_candidate_inherits_unchanged_committed_payloads(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    coordinator = CatalogGenerationCoordinator(_V4Writer(database_path))
    first_value = _input("sync-first", title="First")
    first = await coordinator.stage(first_value)
    await coordinator.activate(await coordinator.validate_candidate(first), None)
    changed_event = replace(
        first_value.events[0],
        title="Second",
        sync_generation="sync-second",
    )
    second_value = GenerationInput(
        sync_generation="sync-second",
        updated_at=10,
        events=(changed_event,),
        markets=(),
        tokens=(),
        input_digest="digest-sync-second",
    )

    second = await coordinator.stage(second_value)
    validation = await coordinator.validate_candidate(second)
    await coordinator.activate(validation, None)

    snapshot = await CatalogRepository(
        database_path,
        _V4Writer(database_path),  # type: ignore[arg-type]
    ).load_catalog()
    assert validation.event_count == 1
    assert validation.market_count == 1
    assert validation.token_count == 1
    assert snapshot.events[0].title == "Second"
    assert snapshot.markets[0].sync_generation == "sync-second"
    assert snapshot.tokens[0].sync_generation == "sync-second"
