"""A bounded, single-connection actor for all in-process database writes."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
import inspect
from pathlib import Path
import sqlite3
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


class CheckpointMode(StrEnum):
    PASSIVE = "PASSIVE"
    RESTART = "RESTART"
    TRUNCATE = "TRUNCATE"


@dataclass(frozen=True, slots=True)
class CheckpointResult:
    mode: CheckpointMode
    busy: int
    log_pages: int
    checkpointed_pages: int
    wal_bytes: int


@dataclass(frozen=True)
class _Request(Generic[T]):
    command: DatabaseCommand[T]
    result: asyncio.Future[T]
    transactional: bool


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
        self._lifecycle_lock = asyncio.Lock()
        self._started = False
        self._closing = False
        self._closed = False

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._closing or self._closed:
                raise DatabaseWriterClosedError(
                    "database writer cannot be restarted"
                )
            if self._started:
                return
            initialize_database(self._path)
            connection: aiosqlite.Connection | None = None
            try:
                connection = await aiosqlite.connect(
                    self._path,
                    isolation_level=None,
                )
                self._raise_if_closing()
                await connection.execute("PRAGMA foreign_keys = ON")
                self._raise_if_closing()
                await connection.execute(
                    f"PRAGMA busy_timeout = {self._busy_timeout_ms}"
                )
                self._raise_if_closing()
            except BaseException:
                if connection is not None:
                    await asyncio.shield(connection.close())
                raise
            self._connection = connection
            self._started = True
            self._worker = asyncio.create_task(
                self._run(),
                name=f"database-writer:{self._path.name}",
            )

    async def execute(self, command: DatabaseCommand[T]) -> T:
        return await self._submit(command, transactional=True)

    async def execute_non_transactional(self, command: DatabaseCommand[T]) -> T:
        """Serialize a command that must run outside a transaction."""

        return await self._submit(command, transactional=False)

    async def checkpoint(
        self,
        mode: CheckpointMode = CheckpointMode.PASSIVE,
    ) -> CheckpointResult:
        """Run one validated checkpoint request through the writer actor."""

        if not isinstance(mode, CheckpointMode):
            raise TypeError("mode must be a CheckpointMode")

        async def command(connection: aiosqlite.Connection) -> CheckpointResult:
            cursor = await connection.execute(f"PRAGMA wal_checkpoint({mode.value})")
            try:
                row = await cursor.fetchone()
            finally:
                await cursor.close()
            if row is None or len(row) != 3:
                raise sqlite3.DatabaseError("unexpected wal_checkpoint result")
            wal_path = self._path.with_name(self._path.name + "-wal")
            try:
                wal_bytes = wal_path.stat().st_size
            except FileNotFoundError:
                wal_bytes = 0
            return CheckpointResult(
                mode=mode,
                busy=int(row[0]),
                log_pages=int(row[1]),
                checkpointed_pages=int(row[2]),
                wal_bytes=wal_bytes,
            )

        return await self.execute_non_transactional(command)

    async def _submit(
        self,
        command: DatabaseCommand[T],
        *,
        transactional: bool,
    ) -> T:
        if not callable(command):
            raise TypeError("command must be callable")
        if not self._started or self._closing or self._closed:
            raise DatabaseWriterClosedError("database writer is not accepting commands")
        result: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        request = _Request(
            command=command,
            result=result,
            transactional=transactional,
        )
        try:
            self._queue.put_nowait(request)
        except asyncio.QueueFull as error:
            raise DatabaseQueueFullError("database writer queue is full") from error
        return await result

    async def close(self) -> None:
        if self._closed:
            return
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(
                self._finish_close(),
                name=f"database-writer-close:{self._path.name}",
            )
        await asyncio.shield(self._close_task)

    async def _finish_close(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                return
            if not self._started:
                self._closed = True
                return
            worker = self._worker
            connection = self._connection
            assert worker is not None
            assert connection is not None
            await self._queue.put(_STOP)
            try:
                await worker
            finally:
                try:
                    await connection.close()
                finally:
                    self._connection = None
                    self._worker = None
                    self._started = False
                    self._closed = True

    def _raise_if_closing(self) -> None:
        if self._closing or self._closed:
            raise DatabaseWriterClosedError(
                "database writer closed during startup"
            )

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
                    if request.transactional:
                        await connection.execute("BEGIN IMMEDIATE")
                    else:
                        assert connection.in_transaction is False
                    value = request.command(connection)
                    if inspect.isawaitable(value):
                        value = await value
                    if request.transactional:
                        await connection.commit()
                    else:
                        assert connection.in_transaction is False
                except BaseException as error:
                    try:
                        if connection.in_transaction:
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
