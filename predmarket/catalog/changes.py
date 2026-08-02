"""Bounded market-change delivery with fail-safe critical admission."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
import logging


_LOGGER = logging.getLogger(__name__)


class MarketChangeType(str, Enum):
    MARKET_ADDED = "MARKET_ADDED"
    MARKET_UPDATED = "MARKET_UPDATED"
    MARKET_DEACTIVATED = "MARKET_DEACTIVATED"
    EVENT_SETTLED = "EVENT_SETTLED"


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
        if self.change_type is MarketChangeType.EVENT_SETTLED:
            _identifier(self.event_id, "event_id")
            if self.market_id is not None:
                _identifier(self.market_id, "market_id")
        elif self.event_id is not None:
            _identifier(self.event_id, "event_id")
        if self.change_type is not MarketChangeType.EVENT_SETTLED:
            _identifier(self.market_id, "market_id")
        if isinstance(self.token_ids, (str, bytes)):
            raise ValueError("token_ids must be an iterable of identifiers")
        token_ids = tuple(self.token_ids)
        if not token_ids:
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


OverflowSink = Callable[[MarketChangeOverflow], Awaitable[None]]


class MarketChangeQueue:
    """Async queue that never drops a critical market control change."""

    def __init__(
        self,
        capacity: int,
        *,
        record_system_event: OverflowSink,
        notify: OverflowSink,
    ) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if not callable(record_system_event):
            raise TypeError("record_system_event must be callable")
        if not callable(notify):
            raise TypeError("notify must be callable")
        self._capacity = capacity
        self._record_system_event = record_system_event
        self._notify = notify
        self._items: deque[MarketChange] = deque()
        self._condition = asyncio.Condition()
        self._unfinished_tasks = 0
        self._finished = asyncio.Event()
        self._finished.set()
        self._degraded = False

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def maxsize(self) -> int:
        return self._capacity

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
        async with self._condition:
            if len(self._items) >= self._capacity:
                self._degraded = True
                if change.droppable:
                    overflow = MarketChangeOverflow(
                        incoming=change,
                        dropped=change,
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
                        overflow = MarketChangeOverflow(
                            incoming=change,
                            evicted=evicted,
                        )
                    else:
                        overflow = MarketChangeOverflow(
                            incoming=change,
                            backpressured=True,
                        )

            if overflow is None:
                self._admit(change)
                self._condition.notify_all()
                return True
            if overflow.dropped is not None or overflow.evicted is not None:
                self._condition.notify_all()

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
        self._unfinished_tasks += 1
        self._finished.clear()

    def _task_finished_without_delivery(self) -> None:
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    async def _emit_overflow(self, overflow: MarketChangeOverflow) -> None:
        # Queue correctness must not depend on either reporting channel.
        _LOGGER.error(
            "Market change queue is full; incoming=%s action=%s",
            overflow.incoming.change_id,
            (
                "drop"
                if overflow.dropped is not None
                else "evict"
                if overflow.evicted is not None
                else "backpressure"
            ),
        )
        await asyncio.gather(
            self._record_system_event(overflow),
            self._notify(overflow),
            return_exceptions=True,
        )


def _identifier(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
