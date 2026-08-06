from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
import sqlite3
from typing import Any

import aiosqlite
import pytest

import predmarket.persistence.repositories as repositories_module
import predmarket.persistence.writer as writer_module
from predmarket.catalog.relations import semantic_evidence_digest
from predmarket.domain.fees import FeeModel, FeeSchedule
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


class _CountingConnection:
    def __init__(self, delegate: aiosqlite.Connection) -> None:
        self._delegate = delegate
        self.round_trips = 0

    async def execute(
        self,
        sql: str,
        parameters: tuple[Any, ...] = (),
    ) -> aiosqlite.Cursor:
        self.round_trips += 1
        return await self._delegate.execute(sql, parameters)

    async def executemany(
        self,
        sql: str,
        parameters: Any,
    ) -> aiosqlite.Cursor:
        self.round_trips += 1
        return await self._delegate.executemany(sql, parameters)


class _CountingWriter:
    def __init__(self, path: Path) -> None:
        self._path = path
        self.round_trips = 0

    async def execute(self, command: Any) -> Any:
        async with aiosqlite.connect(self._path, isolation_level=None) as delegate:
            await delegate.execute("PRAGMA foreign_keys = ON")
            await delegate.execute("BEGIN IMMEDIATE")
            connection = _CountingConnection(delegate)
            try:
                result = await command(connection)
                await delegate.commit()
            except BaseException:
                await delegate.rollback()
                raise
        self.round_trips += connection.round_trips
        return result


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


async def test_writer_close_during_connection_creation_cleans_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    real_connect = writer_module.aiosqlite.connect

    class DelayedConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection

        def __await__(self) -> object:
            async def wait_for_release() -> object:
                entered.set()
                await release.wait()
                return await self._connection  # type: ignore[misc]

            return wait_for_release().__await__()

    def delayed_connect(*args: object, **kwargs: object) -> DelayedConnection:
        return DelayedConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(writer_module.aiosqlite, "connect", delayed_connect)
    writer = DatabaseWriter(tmp_path / "market.db")
    start_task = asyncio.create_task(writer.start())
    await entered.wait()
    close_task = asyncio.create_task(writer.close())
    await asyncio.sleep(0)
    release.set()

    try:
        with pytest.raises(DatabaseWriterClosedError):
            await start_task
        await close_task
        assert writer._connection is None
        assert writer._worker is None
        assert writer._closed is True
    finally:
        if writer._connection is not None or writer._worker is not None:
            writer._closed = False
            writer._closing = False
            writer._close_task = None
            await writer.close()


async def test_writer_concurrent_start_calls_create_one_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    real_connect = writer_module.aiosqlite.connect

    class DelayedConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection

        def __await__(self) -> object:
            async def wait_for_release() -> object:
                entered.set()
                await release.wait()
                return await self._connection  # type: ignore[misc]

            return wait_for_release().__await__()

    def delayed_connect(*args: object, **kwargs: object) -> DelayedConnection:
        nonlocal calls
        calls += 1
        return DelayedConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(writer_module.aiosqlite, "connect", delayed_connect)
    writer = DatabaseWriter(tmp_path / "market.db")
    starts = [asyncio.create_task(writer.start()) for _ in range(2)]
    await entered.wait()
    await asyncio.sleep(0)

    if calls != 1:
        for task in starts:
            task.cancel()
        await asyncio.gather(*starts, return_exceptions=True)
        assert calls == 1

    release.set()
    await asyncio.gather(*starts)
    try:
        assert calls == 1
    finally:
        await writer.close()


async def test_writer_close_during_connection_configuration_closes_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    real_connect = writer_module.aiosqlite.connect
    proxy: object | None = None

    class BlockingConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection
            self.closed = False
            self._blocked = False

        async def execute(self, sql: str) -> object:
            if not self._blocked:
                self._blocked = True
                entered.set()
                await release.wait()
            return await self._connection.execute(sql)  # type: ignore[attr-defined]

        async def close(self) -> None:
            self.closed = True
            await self._connection.close()  # type: ignore[attr-defined]

        def __getattr__(self, name: str) -> object:
            return getattr(self._connection, name)

    class ProxiedConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection

        def __await__(self) -> object:
            async def create_proxy() -> object:
                nonlocal proxy
                connection = await self._connection  # type: ignore[misc]
                proxy = BlockingConnection(connection)
                return proxy

            return create_proxy().__await__()

    def proxied_connect(*args: object, **kwargs: object) -> ProxiedConnection:
        return ProxiedConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(writer_module.aiosqlite, "connect", proxied_connect)
    writer = DatabaseWriter(tmp_path / "market.db")
    start_task = asyncio.create_task(writer.start())
    await entered.wait()
    close_task = asyncio.create_task(writer.close())
    await asyncio.sleep(0)
    release.set()

    try:
        with pytest.raises(DatabaseWriterClosedError):
            await start_task
        await close_task
        assert proxy is not None
        assert proxy.closed is True  # type: ignore[union-attr]
        assert writer._connection is None
        assert writer._worker is None
    finally:
        if writer._connection is not None or writer._worker is not None:
            writer._closed = False
            writer._closing = False
            writer._close_task = None
            await writer.close()


async def test_writer_cancelled_start_can_be_retried_without_leaked_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    real_connect = writer_module.aiosqlite.connect

    class DelayedConnection:
        def __init__(self, connection: object) -> None:
            self._connection = connection

        def __await__(self) -> object:
            async def wait_for_release() -> object:
                entered.set()
                await release.wait()
                return await self._connection  # type: ignore[misc]

            return wait_for_release().__await__()

    def delayed_connect(*args: object, **kwargs: object) -> DelayedConnection:
        return DelayedConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(writer_module.aiosqlite, "connect", delayed_connect)
    writer = DatabaseWriter(tmp_path / "market.db")
    start_task = asyncio.create_task(writer.start())
    await entered.wait()
    start_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start_task
    assert writer._connection is None
    assert writer._worker is None
    assert writer._started is False
    assert writer._closed is False

    release.set()
    await writer.start()
    await writer.close()


async def test_writer_cancelled_close_still_finishes_shared_shutdown(
    tmp_path: Path,
) -> None:
    writer = DatabaseWriter(tmp_path / "market.db", queue_size=1)
    await writer.start()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking(connection: object) -> None:
        entered.set()
        await release.wait()

    first = asyncio.create_task(writer.execute(blocking))
    await entered.wait()
    second = asyncio.create_task(writer.execute(lambda connection: None))
    await asyncio.sleep(0)
    close_task = asyncio.create_task(writer.close())
    await asyncio.sleep(0)
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    release.set()
    await first
    await second
    await writer.close()
    assert writer._connection is None
    assert writer._worker is None
    assert writer._closed is True


async def test_writer_start_is_rejected_after_close_begins(
    tmp_path: Path,
) -> None:
    writer = DatabaseWriter(tmp_path / "market.db")
    await writer.start()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking(connection: object) -> None:
        entered.set()
        await release.wait()

    command = asyncio.create_task(writer.execute(blocking))
    await entered.wait()
    close_task = asyncio.create_task(writer.close())
    await asyncio.sleep(0)
    try:
        with pytest.raises(DatabaseWriterClosedError):
            await writer.start()
    finally:
        release.set()
        await command
        await close_task


async def test_repositories_round_trip_typed_catalog_and_relation_records(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    relations = RelationRepository(database_path, writer)
    system_events = SystemEventRepository(database_path, writer)
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
        fee_schedule=FeeSchedule(
            model=FeeModel.CURVE,
            enabled=True,
            source="fixture",
            parameters={
                "rate": Decimal("0.04"),
                "exponent": Decimal("1"),
                "rebate_rate": Decimal("0.25"),
            },
            updated_at=3,
            taker_only=True,
        ),
        fee_updated_at=3,
        created_at=1,
        updated_at=2,
    )
    second_token = Token(
        id="token-2",
        market_id="market-2",
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
            tokens=(token, second_token),
        )
        await relations.save(relation)
        for invalid_initial_status in (
            RelationStatus.LLM_APPROVE,
            RelationStatus.APPROVED,
        ):
            with pytest.raises(ValueError, match="NO_LLM_APPROVE"):
                await relations.save(
                    Relation(
                        id=f"relation-bad-{invalid_initial_status.value}",
                        market_a_id="market-2",
                        market_b_id="market-1",
                        status=invalid_initial_status,
                        discovery_source=DiscoverySource.MANUAL,
                        created_at=2,
                        updated_at=2,
                    )
                )
        context = await relations.get_for_analysis("relation-1")
        assert context is not None
        llm_approved = context.transition_to(
            RelationStatus.LLM_APPROVE,
            updated_at=3,
        )
        llm_approved = replace(
            llm_approved,
            llm_confidence=Decimal("0.8"),
            llm_analysis={
                "approved": True,
                "reasoning": "fixture",
                "warnings": (),
                "semantic_evidence": context.llm_analysis["semantic_evidence"],
            },
        )
        approved = llm_approved.transition_to(
            RelationStatus.APPROVED,
            updated_at=4,
        )
        with pytest.raises(TypeError):
            await relations.save_analysis(llm_approved)  # type: ignore[call-arg]
        forged_analysis = dict(llm_approved.llm_analysis or {})
        forged_evidence = dict(forged_analysis["semantic_evidence"])
        forged_evidence["sha256"] = "0" * 64
        forged_analysis["semantic_evidence"] = forged_evidence
        with pytest.raises(ValueError, match="digest|evidence"):
            await relations.save_analysis(
                replace(llm_approved, llm_analysis=forged_analysis),
                expected_semantic_digest=semantic_evidence_digest(context),
            )
        await relations.save_analysis(
            llm_approved,
            expected_semantic_digest=semantic_evidence_digest(context),
        )
        await relations.save(relation)
        with pytest.raises(ValueError, match="NO_LLM_APPROVE"):
            await relations.save(approved)
        stored_market = await catalog.get_market("market-1")
        stored_token = await catalog.get_token("token-1")
        stored_relation = await relations.get("relation-1")
        activation_events = await system_events.read_after(0)
    finally:
        await writer.close()

    assert stored_market == market
    assert stored_token == token
    assert stored_relation is not None
    assert replace(stored_relation, llm_analysis=llm_approved.llm_analysis) == llm_approved
    assert stored_relation.llm_analysis is not None
    assert "semantic_evidence" in stored_relation.llm_analysis
    assert activation_events == ()
    with sqlite3.connect(database_path) as connection:
        encoded = connection.execute(
            "SELECT market_ids_json, neg_risk_metadata_json FROM events"
        ).fetchone()
        decimals = connection.execute(
            "SELECT tick_size, minimum_order_size FROM markets WHERE id = 'market-1'"
        ).fetchone()
    assert encoded == ('["market-1","market-2"]', '{"mapping_version":"v1"}')
    assert decimals == ("0.01", "1")

    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE markets SET tick_size = '1E-5' WHERE id = 'market-1'"
        )
        connection.execute(
            "UPDATE relations SET llm_confidence = '8.75E-1' "
            "WHERE id = 'relation-1'"
        )
        connection.commit()

    reader_writer = DatabaseWriter(database_path)
    await reader_writer.start()
    try:
        legacy_market = await CatalogRepository(
            database_path, reader_writer
        ).get_market("market-1")
        legacy_relation = await RelationRepository(
            database_path, reader_writer
        ).get("relation-1")
    finally:
        await reader_writer.close()

    assert legacy_market is not None
    assert legacy_market.tick_size == Decimal("0.00001")
    assert legacy_relation is not None
    assert legacy_relation.llm_confidence == Decimal("0.875")


@pytest.mark.parametrize(
    "invalid_digest",
    [
        "",
        "A" * 64,
        "g" * 64,
        "0" * 63,
        123,
        object(),
    ],
)
async def test_relation_analysis_rejects_noncanonical_digest_before_admission(
    tmp_path: Path,
    invalid_digest: object,
) -> None:
    class AdmissionTrap:
        calls = 0

        async def execute(self, command: object) -> None:
            self.calls += 1
            raise AssertionError("invalid digest reached DatabaseWriter")

    trap = AdmissionTrap()
    repository = RelationRepository(tmp_path / "unused.db", trap)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="SHA-256"):
        await repository.save_analysis(
            object(),  # type: ignore[arg-type]
            expected_semantic_digest=invalid_digest,  # type: ignore[arg-type]
        )

    assert trap.calls == 0


async def test_relation_analysis_rejects_string_subclass_digest_before_admission(
    tmp_path: Path,
) -> None:
    class Digest(str):
        pass

    class AdmissionTrap:
        calls = 0

        async def execute(self, command: object) -> None:
            self.calls += 1
            raise AssertionError("invalid digest reached DatabaseWriter")

    trap = AdmissionTrap()
    repository = RelationRepository(tmp_path / "unused.db", trap)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="SHA-256"):
        await repository.save_analysis(
            object(),  # type: ignore[arg-type]
            expected_semantic_digest=Digest("0" * 64),
        )
    assert trap.calls == 0


async def test_catalog_single_entity_write_rebuilds_event_market_consistency(
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
        await catalog.save_catalog(
            events=(),
            markets=(replace(first_market, question="Updated question?"),),
            tokens=(),
        )
        assert (await catalog.get_market("market-1")).question == "Updated question?"  # type: ignore[union-attr]
        await catalog.save_market(extra_market)
        await catalog.save_catalog(
            events=(),
            markets=(replace(extra_market, question="Updated second question?"),),
            tokens=(),
        )
        stored_event = await catalog.get_event(event.id)
        assert stored_event is not None
        assert stored_event.market_ids == ("market-1", "market-2")

        second_event = replace(
            event,
            id="event-2",
            title="Second event",
            market_ids=(),
        )
        await catalog.save_event(second_event)
        await catalog.save_catalog(
            events=(),
            markets=(replace(extra_market, event_id=second_event.id),),
            tokens=(),
        )
        stored_event = await catalog.get_event(event.id)
        stored_second_event = await catalog.get_event(second_event.id)
        assert stored_event is not None
        assert stored_event.market_ids == ("market-1",)
        assert stored_second_event is not None
        assert stored_second_event.market_ids == ("market-2",)
    finally:
        await writer.close()

    assert await CatalogRepository(
        database_path,
        DatabaseWriter(database_path),
    ).get_market("market-2") is not None


async def test_catalog_bulk_save_uses_bounded_database_round_trips(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)
    writer = _CountingWriter(database_path)
    catalog = CatalogRepository(database_path, writer)  # type: ignore[arg-type]
    event = Event(
        id="event-bulk",
        title="Bulk event",
        status=MarketStatus.ACTIVE,
        market_ids=tuple(f"market-{index:03d}" for index in range(32)),
        sync_generation="sync-bulk",
        sync_generation_complete=True,
    )
    markets = tuple(
        Market(
            id=f"market-{index:03d}",
            event_id=event.id,
            condition_id=f"condition-{index:03d}",
            question=f"Question {index}?",
            status=MarketStatus.ACTIVE,
            active=True,
            accepting_orders=True,
            enable_orderbook=True,
            sync_generation="sync-bulk",
            sync_generation_complete=True,
        )
        for index in range(32)
    )
    tokens = tuple(
        Token(
            id=f"token-{index:03d}-{position}",
            market_id=market.id,
            outcome=outcome,
            position=position,
            sync_generation="sync-bulk",
            sync_generation_complete=True,
        )
        for index, market in enumerate(markets)
        for position, outcome in enumerate(("YES", "NO"))
    )

    await catalog.save_catalog(events=(event,), markets=markets, tokens=tokens)

    assert writer.round_trips <= 20
    stored_event = await catalog.get_event(event.id)
    assert stored_event is not None
    assert stored_event.market_ids == event.market_ids


async def test_complete_catalog_save_advances_unchanged_rows_without_upsert(
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
        sync_generation="sync-1",
        sync_generation_complete=True,
        updated_at=10,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
        updated_at=10,
    )
    try:
        await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))

        await catalog.save_complete_catalog(
            generation="sync-2",
            updated_at=20,
            events=(),
            markets=(),
            tokens=(),
        )
        stored = await catalog.load_catalog()
    finally:
        await writer.close()

    assert stored.events == (
        replace(event, sync_generation="sync-2", updated_at=20),
    )
    assert stored.markets == (
        replace(market, sync_generation="sync-2", updated_at=20),
    )
    assert stored.tokens == (
        replace(token, sync_generation="sync-2", updated_at=20),
    )


async def test_complete_catalog_save_does_not_overwrite_newer_watch_refresh(
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
        updated_at=10,
    )
    market = Market(
        id="market-1",
        event_id=event.id,
        condition_id="condition-1",
        question="Initial question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
        tick_size=Decimal("0.01"),
        updated_at=10,
    )
    token = Token(
        id="token-1",
        market_id=market.id,
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
        updated_at=10,
    )
    stale_sync_market = replace(
        market,
        question="Stale sync question?",
        sync_generation="sync-2",
        tick_size=Decimal("0.005"),
        updated_at=20,
    )
    stale_sync_token = replace(
        token,
        sync_generation="sync-2",
        fee_schedule=FeeSchedule(
            model=FeeModel.FLAT,
            enabled=True,
            source="sync",
            parameters={"rate": Decimal("0.02")},
            updated_at=20,
        ),
        fee_updated_at=20,
        updated_at=20,
    )
    refreshed_market = replace(
        stale_sync_market,
        question="Fresh watch question?",
        sync_generation="sync-1",
        tick_size=Decimal("0.001"),
        updated_at=30,
    )
    refreshed_token = replace(
        stale_sync_token,
        sync_generation="sync-1",
        fee_schedule=FeeSchedule(
            model=FeeModel.FLAT,
            enabled=True,
            source="watch",
            parameters={"rate": Decimal("0.01")},
            updated_at=30,
        ),
        fee_updated_at=30,
        updated_at=30,
    )
    try:
        await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))
        await catalog.save_catalog(
            events=(),
            markets=(refreshed_market,),
            tokens=(refreshed_token,),
        )

        await catalog.save_complete_catalog(
            generation="sync-2",
            updated_at=20,
            events=(),
            markets=(stale_sync_market,),
            tokens=(stale_sync_token,),
        )
        stored = await catalog.load_catalog()
    finally:
        await writer.close()

    assert stored.events == (
        replace(event, sync_generation="sync-2", updated_at=20),
    )
    assert stored.markets == (
        replace(refreshed_market, sync_generation="sync-2"),
    )
    assert stored.tokens == (
        replace(refreshed_token, sync_generation="sync-2"),
    )


async def test_catalog_load_order_uses_existing_primary_key_indexes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    initialize_database(database_path)

    async with aiosqlite.connect(database_path) as connection:
        for statement in repositories_module._LOAD_CATALOG_STATEMENTS:
            cursor = await connection.execute(f"EXPLAIN QUERY PLAN {statement}")
            details = tuple(str(row[3]) for row in await cursor.fetchall())
            assert not any("USE TEMP B-TREE FOR ORDER BY" in item for item in details)


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
