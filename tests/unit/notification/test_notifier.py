from __future__ import annotations

from io import StringIO

import pytest

from predmarket.notification.notifier import Notifier


class _SystemEvents:
    def __init__(self) -> None:
        self.entries: list[dict[str, object]] = []

    async def append(self, **entry: object) -> int:
        self.entries.append(entry)
        return len(self.entries)


@pytest.mark.asyncio
async def test_terminal_notification_survives_desktop_failure() -> None:
    output = StringIO()
    events = _SystemEvents()

    def failing_desktop(_: str, __: str) -> None:
        raise RuntimeError("desktop unavailable")

    notifier = Notifier(
        terminal=output,
        desktop=failing_desktop,
        system_events=events,
        clock_ms=lambda: 42,
    )

    await notifier.notify(
        event_type="SIGNAL_OPENED",
        message="signal-1 opened",
        details={"signal_id": "signal-1"},
    )

    assert "SIGNAL_OPENED: signal-1 opened" in output.getvalue()
    assert events.entries == [
        {
            "component": "NOTIFIER",
            "severity": "ERROR",
            "event_type": "DESKTOP_NOTIFICATION_FAILED",
            "message": "Desktop notification failed",
            "occurred_at": 42,
            "details": {
                "error": "desktop unavailable",
                "notification_event_type": "SIGNAL_OPENED",
            },
        }
    ]
