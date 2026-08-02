from __future__ import annotations

from io import StringIO
import logging

import pytest

from predmarket.notification import notifier as notifier_module
from predmarket.notification.notifier import Notifier, macos_desktop_notification


class _SystemEvents:
    def __init__(self) -> None:
        self.entries: list[dict[str, object]] = []

    async def append(self, **entry: object) -> int:
        self.entries.append(entry)
        return len(self.entries)


@pytest.mark.asyncio
async def test_notification_logs_desktop_failure_without_terminal_output(
    caplog: pytest.LogCaptureFixture,
) -> None:
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

    with caplog.at_level(logging.ERROR, logger="predmarket.notification.notifier"):
        await notifier.notify(
            event_type="SIGNAL_OPENED",
            message="signal-1 opened",
            details={"signal_id": "signal-1"},
        )

    assert output.getvalue() == ""
    assert any(
        "desktop_notification_failed" in record.getMessage()
        and "SIGNAL_OPENED" in record.getMessage()
        and "desktop unavailable" in record.getMessage()
        for record in caplog.records
    )
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


@pytest.mark.asyncio
async def test_operational_error_notification_does_not_print_details() -> None:
    output = StringIO()
    notifier = Notifier(terminal=output)

    await notifier.notify(
        event_type="SYNC_GENERATION_INCOMPLETE",
        message="Market sync generation was incomplete",
        details={"error": 'market request failed; api_response={"id":"200"}'},
    )

    assert output.getvalue() == ""


def test_macos_desktop_notification_escapes_double_quoted_applescript_literals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    def capture_run(*args: object, **kwargs: object) -> None:
        calls.append((*args, kwargs))

    monkeypatch.setattr(notifier_module.subprocess, "run", capture_run)

    macos_desktop_notification('Signal "alpha" \\ beta', 'body "quoted" \\ path')

    assert calls == [
        (
            [
                "osascript",
                "-e",
                'display notification "body \\"quoted\\" \\\\ path" '
                'with title "Signal \\"alpha\\" \\\\ beta"',
            ],
            {
                "check": True,
                "stdout": notifier_module.subprocess.DEVNULL,
                "stderr": notifier_module.subprocess.DEVNULL,
            },
        )
    ]


def test_macos_desktop_notification_serializes_crlf_and_linefeed_literals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    def capture_run(*args: object, **kwargs: object) -> None:
        calls.append((*args, kwargs))

    monkeypatch.setattr(notifier_module.subprocess, "run", capture_run)

    macos_desktop_notification("Signal\r\nnext", "body\nnext")

    assert calls[0][0] == [
        "osascript",
        "-e",
        'display notification "body" & linefeed & "next" '
        'with title "Signal" & return & linefeed & "next"',
    ]
