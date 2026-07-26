from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
import math


def _nonnegative_exact_int(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


def _positive_exact_int(value: object, name: str) -> None:
    _nonnegative_exact_int(value, name)
    if value == 0:
        raise ValueError(f"{name} must be positive")


def _nonnegative_number(value: object, name: str) -> None:
    if isinstance(value, bool) or type(value) not in (int, float):
        raise TypeError(f"{name} must be an int or float")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


@dataclass(frozen=True)
class Timing:
    exchange_ts_ms: int
    received_ts_ms: int
    received_monotonic: float
    evaluated_monotonic: float

    def __post_init__(self) -> None:
        _nonnegative_exact_int(self.exchange_ts_ms, "exchange_ts_ms")
        _nonnegative_exact_int(self.received_ts_ms, "received_ts_ms")
        _nonnegative_number(self.received_monotonic, "received_monotonic")
        _nonnegative_number(self.evaluated_monotonic, "evaluated_monotonic")
        if self.evaluated_monotonic < self.received_monotonic:
            raise ValueError("evaluated_monotonic must not precede received_monotonic")

    @property
    def apparent_network_latency_ms(self) -> int:
        return self.received_ts_ms - self.exchange_ts_ms

    @property
    def processing_latency_ms(self) -> Decimal:
        return (
            Decimal(str(self.evaluated_monotonic))
            - Decimal(str(self.received_monotonic))
        ) * 1000


@dataclass(frozen=True)
class TimingAssessment:
    valid: bool
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.valid) is not bool:
            raise TypeError("valid must be a bool")
        if not isinstance(self.reasons, tuple):
            raise TypeError("reasons must be a tuple")
        if any(not isinstance(reason, str) for reason in self.reasons):
            raise TypeError("reasons must contain strings")
        if any(not reason for reason in self.reasons):
            raise ValueError("reasons must be non-empty")
        if len(set(self.reasons)) != len(self.reasons):
            raise ValueError("reasons must be unique")
        if self.valid == bool(self.reasons):
            raise ValueError("valid must be true exactly when reasons is empty")


def validate_timings(
    items: Sequence[Timing],
    *,
    now_ms: int,
    max_age_ms: int,
    max_skew_ms: int,
    max_processing_ms: int,
) -> TimingAssessment:
    if isinstance(items, (str, bytes)) or not isinstance(items, Sequence):
        raise TypeError("items must be a sequence")
    snapshot = tuple(items)
    if any(not isinstance(item, Timing) for item in snapshot):
        raise TypeError("items must contain only Timing values")
    _nonnegative_exact_int(now_ms, "now_ms")
    _positive_exact_int(max_age_ms, "max_age_ms")
    _positive_exact_int(max_skew_ms, "max_skew_ms")
    _positive_exact_int(max_processing_ms, "max_processing_ms")

    if not snapshot:
        return TimingAssessment(False, ("missing_timing",))

    exchange_times = tuple(item.exchange_ts_ms for item in snapshot)
    reasons: list[str] = []
    if any(now_ms - exchange_ts > max_age_ms for exchange_ts in exchange_times):
        reasons.append("stale")
    if any(exchange_ts > now_ms for exchange_ts in exchange_times):
        reasons.append("future_exchange_ts")
    if any(
        item.exchange_ts_ms > item.received_ts_ms for item in snapshot
    ):
        reasons.append("exchange_after_receive")
    if max(exchange_times) - min(exchange_times) > max_skew_ms:
        reasons.append("leg_skew")
    if any(
        item.processing_latency_ms > max_processing_ms for item in snapshot
    ):
        reasons.append("processing_latency")
    return TimingAssessment(not reasons, tuple(reasons))
