from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sqlite3
import threading

import pytest

import predmarket.persistence.repositories as repositories_module
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.catalog_generations import CatalogConstraintError
from predmarket.persistence.repositories import CatalogRepository
from predmarket.persistence.writer import DatabaseWriter


async def test_catalog_snapshot_reads_one_typed_catalog_view(tmp_path: Path) -> None:
    database_path = tmp_path / "catalog.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="old",
        sync_generation_complete=True,
    )
    market = Market(
        id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        question="Question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="old",
        sync_generation_complete=True,
    )
    tokens = tuple(
        Token(
            id=f"token-{position}",
            market_id=market.id,
            outcome=outcome,
            position=position,
            sync_generation="old",
            sync_generation_complete=True,
        )
        for position, outcome in enumerate(("YES", "NO"))
    )
    try:
        await catalog.save_catalog(
            events=(event,),
            markets=(market,),
            tokens=tokens,
        )

        snapshot = await catalog.load_catalog()
    finally:
        await writer.close()

    assert tuple(item.id for item in snapshot.events) == ("event-1",)
    assert tuple(item.id for item in snapshot.markets) == ("market-1",)
    assert tuple(item.id for item in snapshot.tokens) == ("token-0", "token-1")


async def test_catalog_snapshot_materialization_runs_off_event_loop_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "catalog.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    event_loop_thread = threading.current_thread()
    materialization_threads: list[threading.Thread] = []
    original = repositories_module._materialize_catalog_snapshot

    def recording_materialization(*args: object) -> object:
        materialization_threads.append(threading.current_thread())
        return original(*args)

    monkeypatch.setattr(
        repositories_module,
        "_materialize_catalog_snapshot",
        recording_materialization,
    )
    try:
        snapshot = await catalog.load_catalog()
    finally:
        await writer.close()

    assert snapshot == repositories_module.CatalogSnapshot((), (), ())
    assert len(materialization_threads) == 1
    assert materialization_threads[0] is not event_loop_thread


async def test_catalog_repository_rejects_orphan_market_then_rebuilds_event_index(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalog.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("stale-upstream-id",),
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    orphan = Market(
        id="market-orphan",
        event_id=None,
        condition_id="condition-orphan",
        question="Orphan?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    token = Token(
        id="token-orphan",
        market_id=orphan.id,
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    try:
        with pytest.raises(CatalogConstraintError, match="parent"):
            await catalog.save_catalog(
                events=(event,),
                markets=(orphan,),
                tokens=(token,),
            )
        assert await catalog.load_catalog() == repositories_module.CatalogSnapshot(
            (), (), ()
        )

        linked = Market(
            id="market-linked",
            event_id=event.id,
            condition_id="condition-linked",
            question="Linked?",
            status=MarketStatus.ACTIVE,
            active=True,
            accepting_orders=True,
            enable_orderbook=True,
            sync_generation="sync-1",
            sync_generation_complete=True,
        )
        linked_token = Token(
            id="token-linked",
            market_id=linked.id,
            outcome="YES",
            position=0,
            sync_generation="sync-1",
            sync_generation_complete=True,
        )
        await catalog.save_catalog(
            events=(event,),
            markets=(linked,),
            tokens=(linked_token,),
        )
        stored_event = await catalog.get_event(event.id)
        assert stored_event is not None
        assert stored_event.market_ids == (linked.id,)
        assert await catalog.has_watchable_catalog()
    finally:
        await writer.close()


async def test_catalog_repository_journals_each_runtime_write_and_ignores_stale_input(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalog.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="watch",
        sync_generation_complete=True,
        updated_at=10,
    )
    market = Market(
        id="market-1",
        event_id=event.id,
        condition_id="condition-1",
        question="Question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="watch",
        sync_generation_complete=True,
        updated_at=10,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation="watch",
        sync_generation_complete=True,
        updated_at=10,
    )
    try:
        await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))
        await catalog.save_event(replace(event, title="Event 2", updated_at=20))
        await catalog.save_market(
            replace(market, question="Question 2?", updated_at=30)
        )
        await catalog.save_token(replace(token, outcome="NO", updated_at=40))
        await catalog.save_catalog(
            events=(replace(event, title="Event 3", updated_at=50),),
            markets=(replace(market, question="Question 3?", updated_at=50),),
            tokens=(replace(token, outcome="UP", updated_at=50),),
        )
        await catalog.save_event(replace(event, title="Stale", updated_at=19))
        await catalog.save_market(replace(market, question="Stale?", updated_at=29))
        await catalog.save_token(replace(token, outcome="STALE", updated_at=39))
    finally:
        await writer.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT runtime_revision FROM catalog_state WHERE id = 1"
        ).fetchone() == (5,)
        assert connection.execute(
            """
            SELECT runtime_revision, GROUP_CONCAT(entity_type, ',')
            FROM (
                SELECT runtime_revision, entity_type
                FROM catalog_runtime_changes
                ORDER BY runtime_revision, entity_type
            )
            GROUP BY runtime_revision
            ORDER BY runtime_revision
            """
        ).fetchall() == [
            (1, "EVENT,MARKET,TOKEN"),
            (2, "EVENT"),
            (3, "MARKET"),
            (4, "TOKEN"),
            (5, "EVENT,MARKET,TOKEN"),
        ]
        assert connection.execute(
            "SELECT title FROM events WHERE id = 'event-1'"
        ).fetchone() == ("Event 3",)
        assert connection.execute(
            "SELECT question FROM markets WHERE id = 'market-1'"
        ).fetchone() == ("Question 3?",)
        assert connection.execute(
            "SELECT outcome FROM tokens WHERE id = 'token-1'"
        ).fetchone() == ("UP",)


async def test_catalog_repository_rolls_back_invalid_runtime_uniqueness(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "catalog.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="watch",
        sync_generation_complete=True,
        updated_at=10,
    )
    market = Market(
        id="market-1",
        event_id=event.id,
        condition_id="condition-1",
        question="Question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="watch",
        sync_generation_complete=True,
        updated_at=10,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation="watch",
        sync_generation_complete=True,
        updated_at=10,
    )
    duplicate = replace(
        market,
        id="market-2",
        question="Duplicate condition",
        updated_at=20,
    )
    try:
        await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))
        with pytest.raises(CatalogConstraintError, match="condition"):
            await catalog.save_market(duplicate)
    finally:
        await writer.close()

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT runtime_revision FROM catalog_state WHERE id = 1"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_runtime_changes"
        ).fetchone() == (3,)
        assert connection.execute(
            "SELECT COUNT(*) FROM catalog_market_ids WHERE id = 'market-2'"
        ).fetchone() == (0,)
