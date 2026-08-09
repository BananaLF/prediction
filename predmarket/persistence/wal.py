"""Pure WAL waterline and amplification decisions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import math


@dataclass(frozen=True, slots=True)
class WalPolicy:
    preflight_bytes: int = 16 * 1024 * 1024
    passive_bytes: int = 48 * 1024 * 1024
    pause_bytes: int = 64 * 1024 * 1024
    abort_bytes: int = 96 * 1024 * 1024
    reserve_bytes: int = 32 * 1024 * 1024
    hard_limit_bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        thresholds = (
            self.preflight_bytes,
            self.passive_bytes,
            self.pause_bytes,
            self.abort_bytes,
            self.hard_limit_bytes,
        )
        if any(type(value) is not int or value < 0 for value in thresholds):
            raise ValueError("WAL waterlines must be non-negative integers")
        if not all(left < right for left, right in zip(thresholds, thresholds[1:])):
            raise ValueError("WAL waterlines must be strictly increasing")
        if type(self.reserve_bytes) is not int or self.reserve_bytes <= 0:
            raise ValueError("WAL reserve must be a positive integer")
        if self.abort_bytes + self.reserve_bytes > self.hard_limit_bytes:
            raise ValueError("WAL abort waterline must preserve the hard-limit reserve")


class WalAction(StrEnum):
    CONTINUE = "CONTINUE"
    PASSIVE_CHECKPOINT = "PASSIVE_CHECKPOINT"
    PAUSE_AND_CHECKPOINT = "PAUSE_AND_CHECKPOINT"
    SHRINK_BATCH = "SHRINK_BATCH"
    ABORT = "ABORT"


def decide_wal_action(
    *,
    wal_bytes: int,
    predicted_delta: int,
    policy: WalPolicy,
) -> WalAction:
    """Return the next action without mutating writer or filesystem state."""

    if type(wal_bytes) is not int or wal_bytes < 0:
        raise ValueError("wal_bytes must be a non-negative integer")
    if type(predicted_delta) is not int or predicted_delta < 0:
        raise ValueError("predicted_delta must be a non-negative integer")
    if not isinstance(policy, WalPolicy):
        raise TypeError("policy must be a WalPolicy")

    if wal_bytes >= policy.abort_bytes:
        return WalAction.ABORT
    if wal_bytes >= policy.pause_bytes:
        return WalAction.PAUSE_AND_CHECKPOINT

    projected_bytes = wal_bytes + predicted_delta
    if (
        projected_bytes >= policy.abort_bytes
        or projected_bytes + policy.reserve_bytes > policy.hard_limit_bytes
    ):
        return WalAction.SHRINK_BATCH
    if wal_bytes >= policy.passive_bytes:
        return WalAction.PASSIVE_CHECKPOINT
    return WalAction.CONTINUE


def update_wal_amplification(
    *,
    current: float,
    payload_bytes: int,
    wal_delta_bytes: int,
) -> float:
    """Keep the worst observed WAL-to-payload ratio as a conservative estimate."""

    if not isinstance(current, (int, float)) or not math.isfinite(current) or current <= 0:
        raise ValueError("current amplification must be finite and positive")
    if type(payload_bytes) is not int or payload_bytes <= 0:
        raise ValueError("payload_bytes must be a positive integer")
    if type(wal_delta_bytes) is not int or wal_delta_bytes < 0:
        raise ValueError("wal_delta_bytes must be a non-negative integer")
    return max(float(current), wal_delta_bytes / payload_bytes)
