from __future__ import annotations

import asyncio
from dataclasses import replace
from io import StringIO
from pathlib import Path

import pytest

from predmarket.app import Supervisor
from predmarket.config import AppConfig, DatabaseConfig
from predmarket.notification.notifier import Notifier


class _Events:
    def __init__(self) -> None:
        self.entries: list[dict[str, object]] = []

    async def append(self, **entry: object) -> int:
        self.entries.append(entry)
        return len(self.entries)


class _Sync:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def run_once(self):
        self.calls.append("sync")
        return type("Result", (), {"complete": True})()


class _Watch:
    def __init__(self, calls: list[str], *, crash: bool) -> None:
        self.calls = calls
        self.crash = crash

    async def start(self) -> None:
        self.calls.append("watch-start")

    async def run(self) -> None:
        self.calls.append("watch-run")
        if self.crash:
            raise RuntimeError("watch crashed")
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.calls.append("watch-close")


async def _wait_for_cancellation(_: float) -> None:
    await asyncio.Event().wait()


def _config(tmp_path: Path) -> AppConfig:
    base = AppConfig.load(Path("config/default.yaml"))
    return replace(
        base,
        database=DatabaseConfig(
            path=tmp_path / "signals.sqlite3",
            busy_timeout_ms=base.database.busy_timeout_ms,
            writer_queue_capacity=base.database.writer_queue_capacity,
        ),
    )


@pytest.mark.asyncio
async def test_supervisor_syncs_before_watch_and_terminates_after_watch_crash(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    events = _Events()
    output = StringIO()
    notifier = Notifier(terminal=output, system_events=events, clock_ms=lambda: 1)
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        notifier=notifier,
        sync_task_factory=lambda **_: _Sync(calls),
        watch_task_factory=lambda **_: _Watch(calls, crash=True),
        sleep=_wait_for_cancellation,
    )

    assert await supervisor.run() == 1

    assert calls == ["sync", "watch-start", "watch-run", "watch-close"]
    assert "RUNTIME_TASK_EXITED" in output.getvalue()
    assert events.entries == []
