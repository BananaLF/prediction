"""Best-effort desktop notifications with an always-on terminal channel."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
import inspect
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, TextIO

from predmarket.signals.manager import SignalNotification


class Notifier:
    """Deliver operational and signal notifications without blocking the service.

    Terminal output is intentionally emitted before the optional desktop channel.
    A desktop failure is audited through the application's existing system-event
    repository and must never fail signal persistence or supervision.
    """

    def __init__(
        self,
        *,
        terminal: TextIO | None = None,
        desktop: Callable[[str, str], Any] | None = None,
        system_events: Any | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if terminal is not None and not hasattr(terminal, "write"):
            raise TypeError("terminal must be a text output stream or None")
        if desktop is not None and not callable(desktop):
            raise TypeError("desktop must be callable or None")
        self._terminal = sys.stdout if terminal is None else terminal
        self._desktop = desktop
        self._system_events = system_events
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)

    async def notify(
        self,
        notification: SignalNotification | None = None,
        *,
        event_type: str | None = None,
        message: str | None = None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        """Print a notification, then attempt desktop delivery if configured."""
        if notification is not None:
            if not isinstance(notification, SignalNotification):
                raise TypeError("notification must be a SignalNotification")
            event_type = f"SIGNAL_{notification.event_type}"
            message = f"{notification.signal_id} ({notification.opportunity_key})"
            details = {
                "signal_id": notification.signal_id,
                "opportunity_key": notification.opportunity_key,
                "revision": notification.revision,
            }
        if not isinstance(event_type, str) or not event_type:
            raise ValueError("event_type must be a non-empty string")
        if not isinstance(message, str) or not message:
            raise ValueError("message must be a non-empty string")
        if details is not None and not isinstance(details, Mapping):
            raise TypeError("details must be a mapping or None")

        print(f"{event_type}: {message}", file=self._terminal, flush=True)
        if self._desktop is None:
            return
        try:
            result = self._desktop(event_type, message)
            if inspect.isawaitable(result):
                await result
        except Exception as error:
            await self._record_desktop_failure(event_type, error)

    async def _record_desktop_failure(
        self,
        notification_event_type: str,
        error: Exception,
    ) -> None:
        if self._system_events is None:
            return
        try:
            occurred_at = self._clock_ms()
            if type(occurred_at) is not int or occurred_at < 0:
                raise ValueError("clock_ms must return a non-negative integer")
            result = self._system_events.append(
                component="NOTIFIER",
                severity="ERROR",
                event_type="DESKTOP_NOTIFICATION_FAILED",
                message="Desktop notification failed",
                occurred_at=occurred_at,
                details={
                    "error": str(error),
                    "notification_event_type": notification_event_type,
                },
            )
            if inspect.isawaitable(result):
                await result
        except Exception:
            # Notification reporting must not compromise the primary terminal
            # channel or a previously committed signal lifecycle transition.
            return


def macos_desktop_notification(title: str, message: str) -> None:
    """Best-effort macOS adapter, intentionally isolated from core runtime."""
    if not isinstance(title, str) or not isinstance(message, str):
        raise TypeError("desktop notification title and message must be strings")
    script = (
        f"display notification {_applescript_string(message)} "
        f"with title {_applescript_string(title)}"
    )
    subprocess.run(
        ["osascript", "-e", script],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _applescript_string(value: str) -> str:
    """Serialize text as an AppleScript double-quoted string literal."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'
