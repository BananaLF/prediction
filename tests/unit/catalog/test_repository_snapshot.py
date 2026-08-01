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
