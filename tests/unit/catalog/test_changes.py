from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging

import pytest

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
