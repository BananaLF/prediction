from __future__ import annotations

import pytest

from predmarket.persistence.wal import (
    WalAction,
    WalPolicy,
    decide_wal_action,
    update_wal_amplification,
)


MIB = 1024 * 1024


@pytest.mark.parametrize(
    ("wal_mib", "expected"),
    [
        (0, WalAction.CONTINUE),
        (16, WalAction.CONTINUE),
        (48, WalAction.PASSIVE_CHECKPOINT),
        (64, WalAction.PAUSE_AND_CHECKPOINT),
        (96, WalAction.ABORT),
        (128, WalAction.ABORT),
    ],
)
def test_decide_wal_action_at_policy_boundaries(
    wal_mib: int,
    expected: WalAction,
) -> None:
    assert decide_wal_action(
        wal_bytes=wal_mib * MIB,
        predicted_delta=0,
        policy=WalPolicy(),
    ) is expected


def test_decide_wal_action_shrinks_before_using_abort_or_hard_limit_reserve(
) -> None:
    policy = WalPolicy()

    assert decide_wal_action(
        wal_bytes=63 * MIB,
        predicted_delta=33 * MIB,
        policy=policy,
    ) is WalAction.SHRINK_BATCH
    assert decide_wal_action(
        wal_bytes=95 * MIB,
        predicted_delta=1 * MIB,
        policy=policy,
    ) is WalAction.PAUSE_AND_CHECKPOINT


@pytest.mark.parametrize(
    "overrides",
    [
        {"preflight_bytes": 48 * MIB},
        {"passive_bytes": 64 * MIB},
        {"pause_bytes": 96 * MIB},
        {"abort_bytes": 128 * MIB},
        {"reserve_bytes": 33 * MIB},
    ],
)
def test_wal_policy_rejects_invalid_thresholds(overrides: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        WalPolicy(**overrides)


def test_update_wal_amplification_is_conservative_and_monotonic() -> None:
    assert update_wal_amplification(
        current=1.5,
        payload_bytes=10,
        wal_delta_bytes=20,
    ) == 2.0
    assert update_wal_amplification(
        current=2.0,
        payload_bytes=10,
        wal_delta_bytes=15,
    ) == 2.0


@pytest.mark.parametrize(
    ("current", "payload_bytes", "wal_delta_bytes"),
    [
        (0.0, 1, 1),
        (1.0, 0, 1),
        (1.0, 1, -1),
    ],
)
def test_update_wal_amplification_rejects_invalid_measurements(
    current: float,
    payload_bytes: int,
    wal_delta_bytes: int,
) -> None:
    with pytest.raises(ValueError):
        update_wal_amplification(
            current=current,
            payload_bytes=payload_bytes,
            wal_delta_bytes=wal_delta_bytes,
        )
