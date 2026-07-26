from decimal import Decimal
import io
import sys

import pytest

from predmarket.domain import OpportunityStatus
from predmarket.engine import EngineResult
from predmarket.notifier import (
    DesktopNotificationError,
    MacOSNotifier,
    NotificationDeliveryError,
    NotificationRouter,
    TerminalNotifier,
)


def result(status=OpportunityStatus.SNAPSHOT_EXECUTABLE):
    return EngineResult(
        "opp", "evidence", status, "all_gates_passed", "risk",
        False, False, False, False, False, True, "nf:abc",
        Decimal("10"), Decimal("9.8"), Decimal("10"),
        Decimal("0.2"), Decimal("0.020408163265306122"),
        ("safe",), "condition", ("hash-a", "hash-b"),
    )


@pytest.mark.asyncio
async def test_terminal_notifier_emits_one_exact_structured_line(capsys):
    await TerminalNotifier().notify(result())
    output = capsys.readouterr().out
    assert output.count("\n") == 1
    assert "SNAPSHOT_EXECUTABLE" in output
    assert "net_return=0.020408163265306122" in output
    assert "condition=condition" in output
    assert "book_hashes=hash-a,hash-b" in output
    assert "guaranteed" not in output.lower()


@pytest.mark.asyncio
async def test_macos_notifier_uses_safe_argv_and_reports_failure():
    calls = []

    async def runner(*argv):
        calls.append(argv)
        return 1, "", "failed"

    notifier = MacOSNotifier(runner=runner, platform="darwin")
    with pytest.raises(DesktopNotificationError):
        await notifier.notify(result())
    assert calls[0][0] == "/usr/bin/osascript"
    assert calls[0][1] == "-e"
    assert len(calls[0]) == 6
    assert "opp" not in calls[0][2]
    assert calls[0][3] == "--"
    assert "opp" in calls[0][5]


@pytest.mark.asyncio
async def test_router_always_audits_but_refuses_desktop_for_non_executable(capsys):
    desktop_calls = []

    class Desktop:
        async def notify(self, value):
            desktop_calls.append(value)

    router = NotificationRouter(TerminalNotifier(), Desktop())
    await router.notify(result(OpportunityStatus.REJECTED))
    assert desktop_calls == []
    assert "REJECTED" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_router_wraps_desktop_failure_after_terminal_audit(capsys):
    class Desktop:
        async def notify(self, value):
            raise RuntimeError("secret subprocess detail")

    with pytest.raises(NotificationDeliveryError) as error:
        await NotificationRouter(TerminalNotifier(), Desktop()).notify(result())
    assert "secret subprocess detail" not in str(error.value)
    assert "SNAPSHOT_EXECUTABLE" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_terminal_can_route_audit_away_from_json_stdout():
    stream = io.StringIO()
    await TerminalNotifier(stream=stream).notify(result())
    assert stream.getvalue().startswith("status=SNAPSHOT_EXECUTABLE")
