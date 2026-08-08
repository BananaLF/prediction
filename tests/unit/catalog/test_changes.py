from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging

import pytest

import predmarket.catalog.changes as changes_module

from predmarket.catalog.changes import (
    MarketChange,
    MarketChangeOverflow,
    MarketChangeQueue,
    MarketChangeType,
)


def _change(
    change_id: str,
    change_type: MarketChangeType,
    *,
    event_id: str | None = "event-1",
    critical: bool = False,
) -> MarketChange:
    return MarketChange(
        change_id=change_id,
        change_type=change_type,
        event_id=event_id,
        market_id=None if change_type is MarketChangeType.EVENT_SETTLED else "market-1",
        token_ids=("token-1", "token-2"),
        occurred_at=123,
        critical=critical,
    )


def test_market_change_allows_orphan_market_without_event() -> None:
    change = _change(
        "orphan-added",
        MarketChangeType.MARKET_ADDED,
        event_id=None,
    )

    assert change.event_id is None


def test_event_settled_change_requires_event() -> None:
    with pytest.raises(ValueError, match="event_id"):
        _change(
            "settled-without-event",
            MarketChangeType.EVENT_SETTLED,
            event_id=None,
        )


def test_catalog_reconciled_is_empty_critical_control() -> None:
    change = MarketChange(
        change_id="sync-2:CATALOG_RECONCILED:catalog",
        change_type=MarketChangeType.CATALOG_RECONCILED,
        event_id=None,
        market_id=None,
        token_ids=(),
        occurred_at=456,
        critical=True,
    )

    assert change.droppable is False

    with pytest.raises(ValueError, match="critical"):
        MarketChange(
            change_id="sync-2:CATALOG_RECONCILED:noncritical",
            change_type=MarketChangeType.CATALOG_RECONCILED,
            event_id=None,
            market_id=None,
            token_ids=(),
            occurred_at=456,
        )


@dataclass
class _OverflowRecorder:
    system_events: list[MarketChangeOverflow] = field(default_factory=list)
    notifications: list[MarketChangeOverflow] = field(default_factory=list)

    async def record_system_event(self, overflow: MarketChangeOverflow) -> None:
        self.system_events.append(overflow)

    async def notify(self, overflow: MarketChangeOverflow) -> None:
        self.notifications.append(overflow)


@pytest.mark.asyncio
async def test_full_queue_drops_new_droppable_change_and_keeps_watch_item(
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder = _OverflowRecorder()
    queue = MarketChangeQueue(
        1,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
    )
    first = _change("first", MarketChangeType.MARKET_ADDED)
    dropped = _change("dropped", MarketChangeType.MARKET_UPDATED)

    assert await queue.put(first) is True
    with caplog.at_level(logging.ERROR):
        assert await queue.put(dropped) is False

    assert await queue.get() == first
    assert queue.degraded is True
    assert recorder.system_events[0].dropped == dropped
    assert recorder.notifications == recorder.system_events
    assert "market change queue is full" in caplog.text.lower()


@pytest.mark.asyncio
async def test_critical_change_evicts_oldest_droppable_item() -> None:
    recorder = _OverflowRecorder()
    queue = MarketChangeQueue(
        3,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
    )
    oldest_droppable = _change("added", MarketChangeType.MARKET_ADDED)
    critical_update = _change(
        "critical-update",
        MarketChangeType.MARKET_UPDATED,
        critical=True,
    )
    newer_droppable = _change("updated", MarketChangeType.MARKET_UPDATED)
    deactivated = _change("closed", MarketChangeType.MARKET_DEACTIVATED)
    for change in (oldest_droppable, critical_update, newer_droppable):
        assert await queue.put(change) is True

    assert await queue.put(deactivated) is True

    assert [await queue.get() for _ in range(3)] == [
        critical_update,
        newer_droppable,
        deactivated,
    ]
    assert recorder.system_events[0].evicted == oldest_droppable
    assert recorder.system_events[0].dropped is None


@pytest.mark.asyncio
async def test_all_critical_full_queue_backpressures_until_watch_consumes() -> None:
    recorder = _OverflowRecorder()
    queue = MarketChangeQueue(
        1,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
    )
    deactivated = _change("closed", MarketChangeType.MARKET_DEACTIVATED)
    settled = _change("settled", MarketChangeType.EVENT_SETTLED)
    await queue.put(deactivated)

    blocked_put = asyncio.create_task(queue.put(settled))
    await asyncio.sleep(0)

    assert blocked_put.done() is False
    assert await queue.get() == deactivated
    assert await asyncio.wait_for(blocked_put, timeout=1) is True
    assert await queue.get() == settled
    assert recorder.system_events[0].backpressured is True


@pytest.mark.asyncio
async def test_watch_consumer_and_join_continue_after_overflow() -> None:
    recorder = _OverflowRecorder()
    queue = MarketChangeQueue(
        1,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
    )
    consumed: list[str] = []

    async def watch() -> None:
        for _ in range(2):
            change = await queue.get()
            consumed.append(change.change_id)
            queue.task_done()

    first = _change("first", MarketChangeType.MARKET_ADDED)
    dropped = _change("dropped", MarketChangeType.MARKET_ADDED)
    critical = _change("critical", MarketChangeType.MARKET_DEACTIVATED)
    await queue.put(first)
    assert await queue.put(dropped) is False
    consumer = asyncio.create_task(watch())
    await asyncio.sleep(0)
    await queue.put(critical)

    await asyncio.wait_for(queue.join(), timeout=1)
    await consumer
    assert consumed == ["first", "critical"]


@pytest.mark.asyncio
async def test_overflow_reports_detection_time_and_cumulative_telemetry_once_per_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _OverflowRecorder()
    now = 900
    monotonic_time = 900
    monkeypatch.setattr(
        changes_module.time,
        "monotonic_ns",
        lambda: monotonic_time * 1_000_000,
    )
    queue = MarketChangeQueue(
        1,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
        clock_ms=lambda: now,
        report_interval_ms=100,
    )
    await queue.put(_change("first", MarketChangeType.MARKET_ADDED))

    assert await queue.put(_change("dropped-1", MarketChangeType.MARKET_UPDATED)) is False
    now = 950
    monotonic_time = 950
    assert await queue.put(_change("dropped-2", MarketChangeType.MARKET_UPDATED)) is False

    assert len(recorder.system_events) == 1
    first_report = recorder.system_events[0]
    assert first_report.detected_at == 900
    assert first_report.detected_at != first_report.incoming.occurred_at
    assert first_report.queue_size == 1
    assert first_report.capacity == 1
    assert first_report.high_water_mark == 1
    assert first_report.overflow_count == 1
    assert first_report.dropped_count == 1
    assert first_report.evicted_count == 0
    assert first_report.backpressured_count == 0

    now = 1_000
    monotonic_time = 1_000
    assert await queue.put(_change("dropped-3", MarketChangeType.MARKET_UPDATED)) is False

    assert recorder.notifications == recorder.system_events
    assert len(recorder.system_events) == 2
    second_report = recorder.system_events[1]
    assert second_report.detected_at == 1_000
    assert second_report.overflow_count == 3
    assert second_report.dropped_count == 3
    assert queue.overflow_count == 3
    assert queue.dropped_count == 3
    assert queue.high_water_mark == 1


@pytest.mark.asyncio
async def test_overflow_reporting_interval_ignores_wall_clock_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _OverflowRecorder()
    wall_time = 900
    monotonic_time = 1_000
    monkeypatch.setattr(
        changes_module.time,
        "monotonic_ns",
        lambda: monotonic_time * 1_000_000,
    )
    queue = MarketChangeQueue(
        1,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
        clock_ms=lambda: wall_time,
        report_interval_ms=100,
    )
    await queue.put(_change("first", MarketChangeType.MARKET_ADDED))
    assert await queue.put(_change("dropped-1", MarketChangeType.MARKET_UPDATED)) is False

    wall_time = 100
    monotonic_time = 1_100
    assert await queue.put(_change("dropped-2", MarketChangeType.MARKET_UPDATED)) is False

    assert [event.detected_at for event in recorder.system_events] == [900, 100]


@pytest.mark.asyncio
async def test_overflow_counters_cover_eviction_drop_and_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = _OverflowRecorder()
    now = 100
    monotonic_time = 100
    monkeypatch.setattr(
        changes_module.time,
        "monotonic_ns",
        lambda: monotonic_time * 1_000_000,
    )
    queue = MarketChangeQueue(
        1,
        record_system_event=recorder.record_system_event,
        notify=recorder.notify,
        clock_ms=lambda: now,
        report_interval_ms=10,
    )
    await queue.put(_change("droppable", MarketChangeType.MARKET_ADDED))

    now = 110
    monotonic_time = 110
    assert await queue.put(_change("critical", MarketChangeType.MARKET_DEACTIVATED)) is True
    now = 120
    monotonic_time = 120
    assert await queue.put(_change("dropped", MarketChangeType.MARKET_UPDATED)) is False
    now = 130
    monotonic_time = 130
    blocked_put = asyncio.create_task(
        queue.put(_change("settled", MarketChangeType.EVENT_SETTLED))
    )
    await asyncio.sleep(0)
    assert blocked_put.done() is False

    assert await queue.get() == _change("critical", MarketChangeType.MARKET_DEACTIVATED)
    assert await asyncio.wait_for(blocked_put, timeout=1) is True

    last_report = recorder.system_events[-1]
    assert last_report.overflow_count == 3
    assert last_report.evicted_count == 1
    assert last_report.dropped_count == 1
    assert last_report.backpressured_count == 1
    assert queue.evicted_count == 1
    assert queue.backpressured_count == 1
