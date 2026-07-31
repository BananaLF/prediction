"""A bounded, single-connection actor for all in-process database writes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import inspect
from pathlib import Path
from typing import Any, Generic, TypeVar

import aiosqlite

from predmarket.persistence.schema import initialize_database


T = TypeVar("T")
DatabaseCommand = Callable[[aiosqlite.Connection], T | Awaitable[T]]


class DatabaseWriterError(RuntimeError):
    """Base class for writer lifecycle and admission errors."""


class DatabaseQueueFullError(DatabaseWriterError):
    """Raised when a command cannot be admitted to the bounded queue."""


class DatabaseWriterClosedError(DatabaseWriterError):
    """Raised when the writer is unavailable or has begun closing."""


@dataclass(frozen=True)
class _Request(Generic[T]):
    command: DatabaseCommand[T]
    result: asyncio.Future[T]


_STOP = object()


class DatabaseWriter:
    """Serialize short write transactions through one writer-owned connection."""

    def __init__(
        self,
        path: Path,
        *,
        queue_size: int = 128,
        busy_timeout_ms: int = 5_000,
    ) -> None:
        if type(queue_size) is not int or queue_size < 1:
            raise ValueError("queue_size must be a positive integer")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a non-negative integer")
        self._path = Path(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._queue: asyncio.Queue[_Request[Any] | object] = asyncio.Queue(
            maxsize=queue_size
        )
        self._connection: aiosqlite.Connection | None = None
        self._worker: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._started = False
        self._closing = False
        self._closed = False

    async def start(self) -> None:
        if self._started:
            return
        if self._closed:
            raise DatabaseWriterClosedError("database writer cannot be restarted")
        initialize_database(self._path)
        connection = await aiosqlite.connect(self._path, isolation_level=None)
        try:
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute(
                f"PRAGMA busy_timeout = {self._busy_timeout_ms}"
            )
        except BaseException:
            await connection.close()
            raise
        self._connection = connection
        self._started = True
        self._worker = asyncio.create_task(
            self._run(),
            name=f"database-writer:{self._path.name}",
        )

    async def execute(self, command: DatabaseCommand[T]) -> T:
        if not callable(command):
            raise TypeError("command must be callable")
        if not self._started or self._closing or self._closed:
            raise DatabaseWriterClosedError("database writer is not accepting commands")
        result: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        request = _Request(command=command, result=result)
        try:
            self._queue.put_nowait(request)
        except asyncio.QueueFull as error:
            raise DatabaseQueueFullError("database writer queue is full") from error
        return await result

    async def close(self) -> None:
        if self._closed:
            return
        if not self._started:
            self._closed = True
            self._closing = True
            return
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(
                self._finish_close(),
                name=f"database-writer-close:{self._path.name}",
            )
        await asyncio.shield(self._close_task)

    async def _finish_close(self) -> None:
        worker = self._worker
        connection = self._connection
        assert worker is not None
        assert connection is not None
        await self._queue.put(_STOP)
        try:
            await worker
        finally:
            await connection.close()
            self._connection = None
            self._worker = None
            self._closed = True

    async def _run(self) -> None:
        connection = self._connection
        assert connection is not None
        while True:
            item = await self._queue.get()
            try:
                if item is _STOP:
                    return
                request = item
                assert isinstance(request, _Request)
                try:
                    await connection.execute("BEGIN IMMEDIATE")
                    value = request.command(connection)
                    if inspect.isawaitable(value):
                        value = await value
                    await connection.commit()
                except BaseException as error:
                    try:
                        await connection.rollback()
                    except BaseException as rollback_error:
                        error.add_note(f"rollback also failed: {rollback_error!r}")
                    if not request.result.done():
                        request.result.set_exception(error)
                else:
                    if not request.result.done():
                        request.result.set_result(value)
            finally:
                self._queue.task_done()
