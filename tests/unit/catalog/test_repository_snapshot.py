from __future__ import annotations

from pathlib import Path

from predmarket.domain.market import Event, Market, MarketStatus, Token
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


async def test_catalog_repository_persists_orphan_markets_and_rebuilds_event_index(
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
        await catalog.save_catalog(events=(event,), markets=(orphan,), tokens=(token,))

        stored_event = await catalog.get_event(event.id)
        assert stored_event is not None
        assert stored_event.market_ids == ()
        stored_orphan = await catalog.get_market(orphan.id)
        assert stored_orphan is not None
        assert stored_orphan.event_id is None
        assert await catalog.has_watchable_catalog()

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
        await catalog.save_market(linked)
        stored_event = await catalog.get_event(event.id)
        assert stored_event is not None
        assert stored_event.market_ids == (linked.id,)
    finally:
        await writer.close()
