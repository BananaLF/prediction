from __future__ import annotations

import asyncio
from decimal import Decimal
from pathlib import Path
import sqlite3

import pytest

from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.domain.signal import (
    Action,
    ExecutionMode,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
    StrategyType,
)
from predmarket.persistence.repositories import (
    CatalogRepository,
    RelationRepository,
    SignalRepository,
    SystemEventRepository,
)
from predmarket.persistence.schema import initialize_database
from predmarket.persistence.writer import (
    DatabaseQueueFullError,
    DatabaseWriter,
    DatabaseWriterClosedError,
)


async def test_writer_serializes_commands_and_returns_results(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path, queue_size=8)
    await writer.start()
    active = 0
    maximum_active = 0
    completion_order: list[int] = []

    async def command(connection: object, value: int) -> int:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.002)
        completion_order.append(value)
        active -= 1
        return value * 10

    try:
        results = await asyncio.gather(
            *(writer.execute(lambda connection, value=value: command(connection, value))
              for value in range(5))
        )
    finally:
        await writer.close()

    assert maximum_active == 1
    assert completion_order == [0, 1, 2, 3, 4]
    assert results == [0, 10, 20, 30, 40]


async def test_writer_rolls_back_failed_command_and_continues(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    writer = DatabaseWriter(database_path)
    await writer.start()

    async def failing(connection: object) -> None:
        await connection.execute(  # type: ignore[attr-defined]
            """
            INSERT INTO system_events (
                component, severity, event_type, message, occurred_at
            ) VALUES ('DATABASE', 'ERROR', 'FAILED', 'must rollback', 1)
            """
        )
        raise RuntimeError("boom")

    async def succeeding(connection: object) -> int:
        cursor = await connection.execute(  # type: ignore[attr-defined]
            """
            INSERT INTO system_events (
                component, severity, event_type, message, occurred_at
            ) VALUES ('DATABASE', 'INFO', 'RECOVERED', 'writer continued', 2)
            """
        )
        return cursor.lastrowid

    try:
        with pytest.raises(RuntimeError, match="boom"):
            await writer.execute(failing)
        inserted_id = await writer.execute(succeeding)
    finally:
        await writer.close()

    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "SELECT id, event_type FROM system_events ORDER BY id"
        ).fetchall()
    assert rows == [(inserted_id, "RECOVERED")]


async def test_writer_queue_is_bounded_and_close_drains_accepted_work(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path, queue_size=1)
    await writer.start()
    release = asyncio.Event()
    started = asyncio.Event()
    completed: list[str] = []

    async def blocking(connection: object) -> None:
        started.set()
        await release.wait()
        completed.append("blocking")

    async def queued(connection: object) -> None:
        completed.append("queued")

    first = asyncio.create_task(writer.execute(blocking))
    await started.wait()
    second = asyncio.create_task(writer.execute(queued))
    await asyncio.sleep(0)
    with pytest.raises(DatabaseQueueFullError):
        await writer.execute(queued)

    close_task = asyncio.create_task(writer.close())
    release.set()
    await close_task
    await first
    await second

    assert completed == ["blocking", "queued"]
    with pytest.raises(DatabaseWriterClosedError):
        await writer.execute(queued)


async def test_writer_concurrent_close_calls_share_one_shutdown(
    tmp_path: Path,
) -> None:
    writer = DatabaseWriter(tmp_path / "market.db")
    await writer.start()

    await asyncio.gather(writer.close(), writer.close())

    with pytest.raises(DatabaseWriterClosedError):
        await writer.execute(lambda connection: None)


async def test_repositories_round_trip_typed_catalog_and_relation_records(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    relations = RelationRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-2", "market-1"),
        sync_generation="sync-1",
        sync_generation_complete=True,
        neg_risk_metadata={"mapping_version": "v1"},
        created_at=1,
        updated_at=2,
    )
    market = Market(
        id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        question="Will it happen?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
        tick_size=Decimal("0.0100"),
        minimum_order_size=Decimal("1.00"),
        created_at=1,
        updated_at=2,
    )
    token = Token(
        id="token-1",
        market_id="market-1",
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
        created_at=1,
        updated_at=2,
    )
    relation = Relation(
        id="relation-1",
        market_a_id="market-1",
        market_b_id="market-2",
        status=RelationStatus.NO_LLM_APPROVE,
        discovery_source=DiscoverySource.RULE,
        created_at=2,
        updated_at=2,
    )
    second_market = Market(
        id="market-2",
        event_id="event-1",
        condition_id="condition-2",
        question="Will the other thing happen?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
        created_at=1,
        updated_at=2,
    )

    try:
        await catalog.save_catalog(
            events=(event,),
            markets=(market, second_market),
            tokens=(token,),
        )
        await relations.save(relation)
        stored_market = await catalog.get_market("market-1")
        stored_relation = await relations.get("relation-1")
    finally:
        await writer.close()

    assert stored_market == market
    assert stored_relation == relation
    with sqlite3.connect(database_path) as connection:
        encoded = connection.execute(
            "SELECT market_ids_json, neg_risk_metadata_json FROM events"
        ).fetchone()
        decimals = connection.execute(
            "SELECT tick_size, minimum_order_size FROM markets WHERE id = 'market-1'"
        ).fetchone()
    assert encoded == ('["market-1","market-2"]', '{"mapping_version":"v1"}')
    assert decimals == ("0.01", "1")


async def test_catalog_single_entity_write_cannot_break_event_market_consistency(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    first_market = Market(
        id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        question="First?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    extra_market = Market(
        id="market-2",
        event_id="event-1",
        condition_id="condition-2",
        question="Second?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    try:
        await catalog.save_catalog(
            events=(event,),
            markets=(first_market,),
            tokens=(),
        )
        with pytest.raises(ValueError, match="market_ids"):
            await catalog.save_market(extra_market)
    finally:
        await writer.close()

    assert await CatalogRepository(
        database_path,
        DatabaseWriter(database_path),
    ).get_market("market-2") is None


async def test_system_and_signal_repositories_use_writer_and_short_reads(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    system_events = SystemEventRepository(database_path, writer)
    signals = SignalRepository(database_path, writer)

    try:
        event_id = await system_events.append(
            component="DATABASE",
            severity="INFO",
            event_type="READY",
            message="Database ready",
            details={"schema": 1},
            occurred_at=10,
        )
        rows = await system_events.read_after(0)
        latest = await signals.get_latest_revision("missing")
    finally:
        await writer.close()

    assert event_id == 1
    assert rows == (
        {
            "id": 1,
            "component": "DATABASE",
            "severity": "INFO",
            "event_type": "READY",
            "message": "Database ready",
            "details": {"schema": 1},
            "occurred_at": 10,
        },
    )
    assert latest is None


async def test_signal_repository_atomically_opens_typed_signal_with_evidence(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    signals = SignalRepository(database_path, writer)
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="sync-1",
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
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    token = Token(
        id="token-1",
        market_id="market-1",
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    decision = OpportunityPresent(
        calculation=OpportunityCalculation(
            quantity=Decimal("2.00"),
            total_capital=Decimal("0.8"),
            expected_profit=Decimal("0.2"),
            return_rate=Decimal("0.25"),
            worst_case_loss=Decimal("0.4"),
            risk_rate=Decimal("0.5"),
            unhedged_notional=Decimal("0.4"),
            risk_flags=("PARTIAL_FILL",),
            details={"model": "binary"},
        ),
        legs=(
            SignalLeg(
                position=0,
                market_id="market-1",
                token_id="token-1",
                action=Action.BUY,
                quantity=Decimal("2"),
                average_price=Decimal("0.4"),
                worst_price=Decimal("0.4"),
                gross_amount=Decimal("0.8"),
                fee_amount=Decimal("0"),
            ),
        ),
        evidence=(
            OrderBook(
                market_id="market-1",
                token_id="token-1",
                bids=(OrderBookLevel(Decimal("0.3"), Decimal("2")),),
                asks=(OrderBookLevel(Decimal("0.4"), Decimal("2")),),
                subscription_generation=1,
                book_hash="hash",
                exchange_timestamp=10,
                received_timestamp=11,
                tick_size=Decimal("0.010"),
                minimum_order_size=Decimal("1.0"),
            ),
        ),
    )
    try:
        await catalog.save_catalog(
            events=(event,),
            markets=(market,),
            tokens=(token,),
        )
        await signals.open_signal(
            signal_id="signal-1",
            opportunity_key="opportunity-1",
            strategy_type=StrategyType.BINARY_UNDERPRICED,
            market_ids=("market-1",),
            relation_id=None,
            execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
            observed_at=12,
            decision=decision,
        )
    finally:
        await writer.close()

    with sqlite3.connect(database_path) as connection:
        header = connection.execute(
            "SELECT status, latest_revision FROM arbitrage_signals"
        ).fetchone()
        revision = connection.execute(
            "SELECT quantity, risk_rate FROM signal_revisions"
        ).fetchone()
        counts = tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("signal_legs", "orderbook_snapshots", "orderbook_levels")
        )
    assert header == ("OPEN", 1)
    assert revision == ("2", "0.5")
    assert counts == (1, 1, 2)
