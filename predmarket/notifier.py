"""Small, failure-isolated notification adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import asyncio
from typing import Protocol

from predmarket.domain import OpportunityStatus
from predmarket.engine import EngineResult


class DesktopNotificationError(RuntimeError):
    """A desktop notification was unsupported or failed."""


class NotificationSink(Protocol):
    async def notify(self, result: EngineResult) -> None: ...


def _text(value: object, limit: int = 180) -> str:
    rendered = str(value).replace("\r", " ").replace("\n", " ")
    return "".join(ch for ch in rendered if ch.isprintable())[:limit]


class TerminalNotifier:
    def __init__(
        self,
        *,
        condition_resolver: Callable[[EngineResult], str] | None = None,
        book_hash_resolver: Callable[[EngineResult], tuple[str, ...]] | None = None,
    ) -> None:
        self._condition = condition_resolver or (lambda result: result.condition_id)
        self._hashes = book_hash_resolver or (lambda result: result.book_hashes)

    async def notify(self, result: EngineResult) -> None:
        if not isinstance(result, EngineResult):
            raise TypeError("result must be EngineResult")
        values = {
            "status": result.status.value,
            "opportunity": result.opportunity_id,
            "condition": _text(self._condition(result)),
            "quantity": "" if result.quantity is None else format(result.quantity, "f"),
            "net_return": (
                "" if result.minimum_return is None
                else format(result.minimum_return, "f")
            ),
            "book_hashes": ",".join(_text(value) for value in self._hashes(result)),
            "risk": ",".join(_text(value) for value in result.risk_reasons),
        }
        print(" ".join(f"{key}={value}" for key, value in values.items()), flush=True)


async def _default_runner(*argv: str) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


class MacOSNotifier:
    _SCRIPT = (
        "on run argv\n"
        "display notification (item 2 of argv) with title (item 1 of argv)\n"
        "end run"
    )

    def __init__(
        self,
        *,
        runner: Callable[..., Awaitable[tuple[int, str, str]]] = _default_runner,
        platform: str,
    ) -> None:
        self._runner = runner
        self._platform = platform

    async def notify(self, result: EngineResult) -> None:
        if self._platform != "darwin":
            raise DesktopNotificationError("desktop notifications are unsupported")
        title = _text(f"Prediction scan: {result.status.value}", 80)
        body = _text(
            f"{result.opportunity_id} return="
            f"{'' if result.minimum_return is None else format(result.minimum_return, 'f')}",
            180,
        )
        code, _stdout, stderr = await self._runner(
            "/usr/bin/osascript", "-e", self._SCRIPT, "--", title, body
        )
        if code != 0:
            raise DesktopNotificationError(
                f"osascript failed: {_text(stderr, 120)}"
            )


@dataclass(frozen=True)
class NotificationRouter:
    terminal: NotificationSink
    desktop: NotificationSink | None = None

    async def notify(self, result: EngineResult) -> None:
        await self.terminal.notify(result)
        if result.status is not OpportunityStatus.SNAPSHOT_EXECUTABLE:
            return
        if self.desktop is not None:
            await self.desktop.notify(result)
