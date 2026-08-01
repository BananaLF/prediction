from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from io import StringIO
import json
from pathlib import Path
import sqlite3

import pytest
import yaml

from predmarket.catalog.relations import (
    DeterministicFakeAnalyzer,
    RelationActivation,
    RelationAnalysis,
    RelationChangeMonitor,
)
from predmarket.cli import main
from predmarket.domain.market import Event, Market, MarketStatus
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.persistence.repositories import CatalogRepository, RelationRepository
from predmarket.persistence.writer import DatabaseWriter


def _event() -> Event:
    return Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-a", "market-b"),
        sync_generation="sync-1",
        sync_generation_complete=True,
        created_at=1,
        updated_at=1,
    )


def _market(market_id: str) -> Market:
    return Market(
        id=market_id,
        event_id="event-1",
        condition_id=f"condition-{market_id}",
        question=f"Question {market_id}?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
        created_at=1,
        updated_at=1,
    )


async def _seed_relation(
    database_path: Path,
    relation_id: str,
    *,
    analyzed: bool = True,
    reverse: bool = False,
) -> None:
    writer = DatabaseWriter(database_path)
    await writer.start()
    try:
        catalog = CatalogRepository(database_path, writer)
        relations = RelationRepository(database_path, writer)
        if await catalog.get_event("event-1") is None:
            await catalog.save_catalog(
                events=(_event(),),
                markets=(_market("market-a"), _market("market-b")),
                tokens=(),
            )
        relation = Relation(
            id=relation_id,
            market_a_id="market-b" if reverse else "market-a",
            market_b_id="market-a" if reverse else "market-b",
            status=RelationStatus.NO_LLM_APPROVE,
            discovery_source=DiscoverySource.RULE,
            created_at=10,
            updated_at=10,
        )
        await relations.save(relation)
        if analyzed:
            await relations.save_analysis(
                replace(
                    relation,
                    status=RelationStatus.LLM_APPROVE,
                    llm_confidence=Decimal("0.8"),
                    llm_analysis={
                        "approved": True,
                        "reasoning": "fixture",
                        "warnings": (),
                    },
                    updated_at=11,
                )
            )
    finally:
        await writer.close()


def _write_config(path: Path, database_path: Path) -> Path:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw["database"]["path"] = str(database_path)
    config_path = path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw))
    return config_path


async def test_relations_list_show_and_analyze_use_persistent_database(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    config_path = _write_config(tmp_path, database_path)
    await _seed_relation(database_path, "relation-1", analyzed=False)

    listed = StringIO()
    assert await asyncio.to_thread(
        main,
        ["--config", str(config_path), "relations", "list"],
        stdout=listed,
    ) == 0
    shown = StringIO()
    assert await asyncio.to_thread(
        main,
        ["--config", str(config_path), "relations", "show", "relation-1"],
        stdout=shown,
    ) == 0
    assert [item["id"] for item in json.loads(listed.getvalue())] == ["relation-1"]
    assert json.loads(shown.getvalue())["status"] == "NO_LLM_APPROVE"

    analyzer = DeterministicFakeAnalyzer(
        {
            "relation-1": RelationAnalysis(
                approved=True,
                confidence=Decimal("0.91"),
                reasoning="fixture analysis",
                warnings=(),
            )
        }
    )
    with pytest.raises(ValueError, match="disabled"):
        await asyncio.to_thread(
            main,
            ["--config", str(config_path), "relations", "analyze", "relation-1"],
            stdout=StringIO(),
            analyzer=analyzer,
            now_ms=lambda: 11,
        )

    raw = yaml.safe_load(config_path.read_text())
    raw["relations"]["llm_enabled"] = True
    config_path.write_text(yaml.safe_dump(raw))
    analyzed_output = StringIO()
    assert await asyncio.to_thread(
        main,
        ["--config", str(config_path), "relations", "analyze", "relation-1"],
        stdout=analyzed_output,
        analyzer=analyzer,
        now_ms=lambda: 11,
    ) == 0
    assert json.loads(analyzed_output.getvalue())["status"] == "LLM_APPROVE"
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT status, llm_confidence FROM relations WHERE id = 'relation-1'"
        ).fetchone()
        activation_count = connection.execute(
            "SELECT count(*) FROM system_events "
            "WHERE event_type = 'RELATION_ACTIVATED'"
        ).fetchone()
    assert row == ("LLM_APPROVE", "0.91")
    assert activation_count == (0,)


async def test_cli_cannot_approve_an_unanalyzed_relation(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    config_path = _write_config(tmp_path, database_path)
    await _seed_relation(database_path, "relation-1", analyzed=False)

    with pytest.raises(ValueError, match="LLM_APPROVE"):
        await asyncio.to_thread(
            main,
            ["--config", str(config_path), "relations", "approve", "relation-1"],
            stdout=StringIO(),
            now_ms=lambda: 11,
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM relations WHERE id = 'relation-1'"
        ).fetchone() == ("NO_LLM_APPROVE",)
        assert connection.execute(
            "SELECT count(*) FROM system_events "
            "WHERE event_type = 'RELATION_ACTIVATED'"
        ).fetchone() == (0,)


async def test_cli_approval_rejects_a_backwards_status_timestamp(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    config_path = _write_config(tmp_path, database_path)
    await _seed_relation(database_path, "relation-1")

    with pytest.raises(ValueError, match="backwards"):
        await asyncio.to_thread(
            main,
            ["--config", str(config_path), "relations", "approve", "relation-1"],
            stdout=StringIO(),
            now_ms=lambda: 10,
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT status, updated_at FROM relations WHERE id = 'relation-1'"
        ).fetchone() == ("LLM_APPROVE", 11)


async def test_cli_approval_is_atomic_and_seen_once_in_event_id_order(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    config_path = _write_config(tmp_path, database_path)
    await _seed_relation(database_path, "relation-1")
    activations: list[RelationActivation] = []
    monitor = RelationChangeMonitor(
        database_path,
        activations.append,
        poll_interval_seconds=0.01,
    )
    monitor_task = asyncio.create_task(monitor.run())
    await asyncio.wait_for(monitor.ready.wait(), timeout=1)

    try:
        output = StringIO()
        exit_code = await asyncio.to_thread(
            main,
            ["--config", str(config_path), "relations", "approve", "relation-1"],
            stdout=output,
            now_ms=lambda: 12,
        )
        await asyncio.wait_for(monitor.changed.wait(), timeout=1)
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                """
                INSERT INTO system_events (
                    component, severity, event_type, message,
                    details_json, occurred_at
                ) VALUES (
                    'STRATEGY', 'INFO', 'RELATION_ACTIVATED',
                    'duplicate fixture', '{"relation_id":"relation-1"}', 13
                )
                """
            )
        await asyncio.sleep(0.03)
    finally:
        monitor_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await monitor_task

    assert exit_code == 0
    assert json.loads(output.getvalue()) == {
        "id": "relation-1",
        "market_a_id": "market-a",
        "market_b_id": "market-b",
        "status": "APPROVED",
        "discovery_source": "RULE",
        "llm_confidence": "0.8",
        "llm_analysis": {"approved": True, "reasoning": "fixture", "warnings": []},
        "created_at": 10,
        "updated_at": 12,
    }
    assert len(activations) == 1
    assert activations[0].relation.id == "relation-1"
    assert activations[0].system_event_id is not None

    with sqlite3.connect(database_path) as connection:
        relation_row = connection.execute(
            "SELECT status FROM relations WHERE id = 'relation-1'"
        ).fetchone()
        event_rows = connection.execute(
            "SELECT id, details_json FROM system_events "
            "WHERE event_type = 'RELATION_ACTIVATED' ORDER BY id"
        ).fetchall()
    assert relation_row == ("APPROVED",)
    assert event_rows[0] == (
        activations[0].system_event_id,
        '{"relation_id":"relation-1"}',
    )
    assert len(event_rows) == 2


async def test_failed_activation_event_insert_rolls_back_relation_status(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    config_path = _write_config(tmp_path, database_path)
    await _seed_relation(database_path, "relation-1")
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_relation_activation
            BEFORE INSERT ON system_events
            WHEN NEW.event_type = 'RELATION_ACTIVATED'
            BEGIN
                SELECT RAISE(ABORT, 'forced activation failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced activation failure"):
        await asyncio.to_thread(
            main,
            ["--config", str(config_path), "relations", "approve", "relation-1"],
            stdout=StringIO(),
            now_ms=lambda: 12,
        )

    with sqlite3.connect(database_path) as connection:
        status = connection.execute(
            "SELECT status FROM relations WHERE id = 'relation-1'"
        ).fetchone()
        count = connection.execute(
            "SELECT count(*) FROM system_events "
            "WHERE event_type = 'RELATION_ACTIVATED'"
        ).fetchone()
    assert status == ("LLM_APPROVE",)
    assert count == (0,)


async def test_monitor_skips_unrelated_events_and_delivers_strictly_increasing_ids(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.db"
    config_path = _write_config(tmp_path, database_path)
    await _seed_relation(database_path, "relation-1")
    await _seed_relation(database_path, "relation-2", reverse=True)
    activations: list[RelationActivation] = []
    monitor = RelationChangeMonitor(
        database_path,
        activations.append,
        poll_interval_seconds=0.01,
    )
    task = asyncio.create_task(monitor.run())
    await asyncio.wait_for(monitor.ready.wait(), timeout=1)

    try:
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                """
                INSERT INTO system_events (
                    component, severity, event_type, message, occurred_at
                ) VALUES ('SYNC', 'INFO', 'UNRELATED', 'fixture', 11)
                """
            )
        for relation_id, timestamp in (("relation-1", 12), ("relation-2", 13)):
            assert await asyncio.to_thread(
                main,
                ["--config", str(config_path), "relations", "approve", relation_id],
                stdout=StringIO(),
                now_ms=lambda timestamp=timestamp: timestamp,
            ) == 0
        for _ in range(100):
            if len(activations) == 2:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.03)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert [item.relation.id for item in activations] == ["relation-1", "relation-2"]
    event_ids = [item.system_event_id for item in activations]
    assert all(event_id is not None for event_id in event_ids)
    assert event_ids == sorted(set(event_ids))
