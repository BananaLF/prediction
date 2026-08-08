from __future__ import annotations

from pathlib import Path
import sqlite3

import aiosqlite
import pytest

from predmarket.domain.market import Event, MarketStatus
from predmarket.persistence.catalog_generations import (
    CatalogBatchLimits,
    CatalogGenerationCoordinator,
    CatalogSyncAborted,
    GenerationInput,
    catalog_input_digest,
)
from predmarket.persistence.repositories import CatalogRepository
from predmarket.persistence.schema import create_v4_database
from predmarket.persistence.wal import WalPolicy
from predmarket.persistence.writer import DatabaseWriter


MiB = 1024 * 1024


def _large_generation() -> GenerationInput:
    generation = "sync-large"
    description = "x" * MiB
    events = tuple(
        Event(
            id=f"event-{index:03d}",
            slug=f"event-{index:03d}",
            title=f"Large event {index}",
            description=description,
            status=MarketStatus.ACTIVE,
            market_ids=(),
            sync_generation=generation,
            sync_generation_complete=True,
            updated_at=10,
        )
        for index in range(72)
    )
    return GenerationInput(
        sync_generation=generation,
        updated_at=10,
        events=events,
        markets=(),
        tokens=(),
        input_digest=catalog_input_digest(
            sync_generation=generation,
            updated_at=10,
            events=events,
            markets=(),
            tokens=(),
        ),
    )


async def test_long_reader_aborts_full_sync_before_hard_wal_limit_and_watch_survives(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    create_v4_database(database_path)
    writer = DatabaseWriter(database_path)
    await writer.start()
    reader = await aiosqlite.connect(database_path, isolation_level=None)
    try:
        await reader.execute("PRAGMA query_only = ON")
        await reader.execute("BEGIN")
        assert await (await reader.execute(
            "SELECT active_generation_id FROM catalog_state WHERE id = 1"
        )).fetchone() == (1,)

        coordinator = CatalogGenerationCoordinator(
            writer,
            batch_limits=CatalogBatchLimits(
                max_rows=4,
                max_payload_bytes=4 * MiB,
            ),
            wal_policy=WalPolicy(),
        )
        with pytest.raises(CatalogSyncAborted) as raised:
            await coordinator.stage(_large_generation())

        wal_path = database_path.with_name(database_path.name + "-wal")
        wal_bytes = wal_path.stat().st_size
        assert raised.value.wal_peak_bytes >= 64 * MiB
        assert wal_bytes < 128 * MiB

        with sqlite3.connect(database_path) as connection:
            assert connection.execute(
                "SELECT active_generation_id FROM catalog_state WHERE id = 1"
            ).fetchone() == (1,)
            assert connection.execute(
                "SELECT status FROM catalog_generations "
                "WHERE sync_generation = 'sync-large'"
            ).fetchone() == ("ABORTED",)

        watch_event = Event(
            id="watch-event",
            title="Watch remains writable",
            status=MarketStatus.ACTIVE,
            market_ids=(),
            sync_generation="watch-runtime",
            sync_generation_complete=True,
            updated_at=20,
        )
        catalog = CatalogRepository(database_path, writer)
        await catalog.save_event(watch_event)
        persisted_watch_event = await catalog.get_event(watch_event.id)
        assert persisted_watch_event is not None
        assert persisted_watch_event.title == watch_event.title
        assert persisted_watch_event.updated_at == watch_event.updated_at
        assert persisted_watch_event.sync_generation == "bootstrap-v4"
        assert wal_path.stat().st_size < 128 * MiB
    finally:
        if reader.in_transaction:
            await reader.execute("ROLLBACK")
        await reader.close()
        await writer.close()
