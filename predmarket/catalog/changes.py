"""Bounded market-change delivery with fail-safe critical admission."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
import logging
import time


_LOGGER = logging.getLogger(__name__)


class MarketChangeType(str, Enum):
    MARKET_ADDED = "MARKET_ADDED"
    MARKET_UPDATED = "MARKET_UPDATED"
    MARKET_DEACTIVATED = "MARKET_DEACTIVATED"
    EVENT_SETTLED = "EVENT_SETTLED"
    CATALOG_RECONCILED = "CATALOG_RECONCILED"


@dataclass(frozen=True, slots=True)
class MarketChange:
    change_id: str
    change_type: MarketChangeType
    event_id: str | None
    market_id: str | None
    token_ids: tuple[str, ...]
    occurred_at: int
    critical: bool = False

    def __post_init__(self) -> None:
        _identifier(self.change_id, "change_id")
        if not isinstance(self.change_type, MarketChangeType):
            raise ValueError("change_type must be a MarketChangeType")
        if self.change_type is MarketChangeType.CATALOG_RECONCILED:
            if self.event_id is not None or self.market_id is not None:
                raise ValueError(
                    "event_id and market_id must be absent for CATALOG_RECONCILED"
                )
            if self.token_ids:
                raise ValueError("token_ids must be empty for CATALOG_RECONCILED")
            if self.critical is not True:
                raise ValueError("CATALOG_RECONCILED must be critical")
        elif self.change_type is MarketChangeType.EVENT_SETTLED:
            if self.event_id is None:
                raise ValueError("event_id is required for EVENT_SETTLED")
            if self.market_id is not None:
                _identifier(self.market_id, "market_id")
        else:
            if self.event_id is not None:
                _identifier(self.event_id, "event_id")
            _identifier(self.market_id, "market_id")
        if isinstance(self.token_ids, (str, bytes)):
            raise ValueError("token_ids must be an iterable of identifiers")
        token_ids = tuple(self.token_ids)
        if (
            not token_ids
            and self.change_type is not MarketChangeType.CATALOG_RECONCILED
        ):
            raise ValueError("token_ids must not be empty")
        for token_id in token_ids:
            _identifier(token_id, "token_id")
        if len(token_ids) != len(set(token_ids)):
            raise ValueError("token_ids must not contain duplicates")
        object.__setattr__(
            self,
            "token_ids",
            tuple(sorted(token_ids, key=lambda value: value.encode("utf-8"))),
        )
        if type(self.occurred_at) is not int or self.occurred_at < 0:
            raise ValueError("occurred_at must be a non-negative integer")
        if type(self.critical) is not bool:
            raise ValueError("critical must be a boolean")

    @property
    def droppable(self) -> bool:
        return self.change_type is MarketChangeType.MARKET_ADDED or (
            self.change_type is MarketChangeType.MARKET_UPDATED
            and not self.critical
        )


@dataclass(frozen=True, slots=True)
class MarketChangeOverflow:
    incoming: MarketChange
    evicted: MarketChange | None = None
    dropped: MarketChange | None = None
    backpressured: bool = False
    detected_at: int = 0
    queue_size: int = 0
    capacity: int = 1
    high_water_mark: int = 0
    overflow_count: int = 1
    dropped_count: int = 0
    evicted_count: int = 0
    backpressured_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.incoming, MarketChange):
            raise ValueError("incoming must be a MarketChange")
        if self.evicted is not None and not isinstance(self.evicted, MarketChange):
            raise ValueError("evicted must be a MarketChange")
        if self.dropped is not None and not isinstance(self.dropped, MarketChange):
            raise ValueError("dropped must be a MarketChange")
        if type(self.backpressured) is not bool:
            raise ValueError("backpressured must be a boolean")
        actions = sum(
            (
                self.evicted is not None,
                self.dropped is not None,
                self.backpressured,
            )
        )
        if actions != 1:
            raise ValueError("overflow must describe exactly one queue action")
        for name in (
            "detected_at",
            "queue_size",
            "capacity",
            "high_water_mark",
            "overflow_count",
            "dropped_count",
            "evicted_count",
            "backpressured_count",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if self.queue_size > self.capacity:
            raise ValueError("queue_size must not exceed capacity")
        if self.high_water_mark > self.capacity:
            raise ValueError("high_water_mark must not exceed capacity")
        if self.high_water_mark < self.queue_size:
            raise ValueError("high_water_mark must not be below queue_size")
        if self.overflow_count != (
            self.dropped_count
            + self.evicted_count
            + self.backpressured_count
        ):
            raise ValueError("overflow_count must equal cumulative action counts")


OverflowSink = Callable[[MarketChangeOverflow], Awaitable[None]]


class MarketChangeQueue:
    """Async queue that never drops a critical market control change."""

    def __init__(
        self,
        capacity: int,
        *,
        record_system_event: OverflowSink,
        notify: OverflowSink,
        clock_ms: Callable[[], int] | None = None,
        report_interval_ms: int = 10_000,
    ) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if not callable(record_system_event):
            raise TypeError("record_system_event must be callable")
        if not callable(notify):
            raise TypeError("notify must be callable")
        if clock_ms is not None and not callable(clock_ms):
            raise TypeError("clock_ms must be callable")
        if type(report_interval_ms) is not int or report_interval_ms < 1:
            raise ValueError("report_interval_ms must be a positive integer")
        self._capacity = capacity
        self._record_system_event = record_system_event
        self._notify = notify
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._report_interval_ms = report_interval_ms
        self._items: deque[MarketChange] = deque()
        self._condition = asyncio.Condition()
        self._unfinished_tasks = 0
        self._finished = asyncio.Event()
        self._finished.set()
        self._degraded = False
        self._high_water_mark = 0
        self._overflow_count = 0
        self._dropped_count = 0
        self._evicted_count = 0
        self._backpressured_count = 0
        self._last_reported_monotonic_ms: int | None = None

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def maxsize(self) -> int:
        return self._capacity

    @property
    def high_water_mark(self) -> int:
        return self._high_water_mark

    @property
    def overflow_count(self) -> int:
        return self._overflow_count

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    @property
    def evicted_count(self) -> int:
        return self._evicted_count

    @property
    def backpressured_count(self) -> int:
        return self._backpressured_count

    def qsize(self) -> int:
        return len(self._items)

    def empty(self) -> bool:
        return not self._items

    def full(self) -> bool:
        return len(self._items) >= self._capacity

    async def put(self, change: MarketChange) -> bool:
        if not isinstance(change, MarketChange):
            raise TypeError("change must be a MarketChange")

        overflow: MarketChangeOverflow | None = None
        should_report = False
        async with self._condition:
            if len(self._items) >= self._capacity:
                self._degraded = True
                detected_at = self._read_clock_ms()
                if change.droppable:
                    overflow, should_report = self._record_overflow(
                        incoming=change,
                        dropped=change,
                        detected_at=detected_at,
                    )
                else:
                    evicted_index = next(
                        (
                            index
                            for index, queued in enumerate(self._items)
                            if queued.droppable
                        ),
                        None,
                    )
                    if evicted_index is not None:
                        evicted = self._items[evicted_index]
                        del self._items[evicted_index]
                        self._task_finished_without_delivery()
                        self._admit(change)
                        overflow, should_report = self._record_overflow(
                            incoming=change,
                            evicted=evicted,
                            detected_at=detected_at,
                        )
                    else:
                        overflow, should_report = self._record_overflow(
                            incoming=change,
                            backpressured=True,
                            detected_at=detected_at,
                        )

            if overflow is None:
                self._admit(change)
                self._condition.notify_all()
                return True
            if overflow.dropped is not None or overflow.evicted is not None:
                self._condition.notify_all()

        if should_report:
            await self._emit_overflow(overflow)
        if overflow.dropped is not None:
            return False
        if overflow.evicted is not None:
            return True

        async with self._condition:
            await self._condition.wait_for(
                lambda: len(self._items) < self._capacity
            )
            self._admit(change)
            self._condition.notify_all()
        return True

    async def get(self) -> MarketChange:
        async with self._condition:
            await self._condition.wait_for(lambda: bool(self._items))
            change = self._items.popleft()
            self._condition.notify_all()
            return change

    def task_done(self) -> None:
        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._task_finished_without_delivery()

    async def join(self) -> None:
        if self._unfinished_tasks:
            await self._finished.wait()

    def _admit(self, change: MarketChange) -> None:
        self._items.append(change)
        self._high_water_mark = max(self._high_water_mark, len(self._items))
        self._unfinished_tasks += 1
        self._finished.clear()

    def _read_clock_ms(self) -> int:
        value = self._clock_ms()
        if type(value) is not int or value < 0:
            raise ValueError("clock_ms must return a non-negative integer")
        return value

    def _record_overflow(
        self,
        *,
        incoming: MarketChange,
        detected_at: int,
        evicted: MarketChange | None = None,
        dropped: MarketChange | None = None,
        backpressured: bool = False,
    ) -> tuple[MarketChangeOverflow, bool]:
        self._overflow_count += 1
        self._dropped_count += int(dropped is not None)
        self._evicted_count += int(evicted is not None)
        self._backpressured_count += int(backpressured)
        overflow = MarketChangeOverflow(
            incoming=incoming,
            evicted=evicted,
            dropped=dropped,
            backpressured=backpressured,
            detected_at=detected_at,
            queue_size=len(self._items),
            capacity=self._capacity,
            high_water_mark=self._high_water_mark,
            overflow_count=self._overflow_count,
            dropped_count=self._dropped_count,
            evicted_count=self._evicted_count,
            backpressured_count=self._backpressured_count,
        )
        monotonic_ms = time.monotonic_ns() // 1_000_000
        should_report = self._last_reported_monotonic_ms is None or (
            monotonic_ms - self._last_reported_monotonic_ms
            >= self._report_interval_ms
        )
        if should_report:
            self._last_reported_monotonic_ms = monotonic_ms
        return overflow, should_report

    def _task_finished_without_delivery(self) -> None:
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    async def _emit_overflow(self, overflow: MarketChangeOverflow) -> None:
        # Queue correctness must not depend on either reporting channel.
        _LOGGER.error(
            "Market change queue is full; incoming=%s action=%s "
            "queue_size=%d capacity=%d high_water_mark=%d overflow_count=%d",
            overflow.incoming.change_id,
            (
                "drop"
                if overflow.dropped is not None
                else "evict"
                if overflow.evicted is not None
                else "backpressure"
            ),
            overflow.queue_size,
            overflow.capacity,
            overflow.high_water_mark,
            overflow.overflow_count,
        )
        await asyncio.gather(
            self._record_system_event(overflow),
            self._notify(overflow),
            return_exceptions=True,
        )


def _identifier(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
