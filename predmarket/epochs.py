from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum


class EpochState(str, Enum):
    WARMING = "WARMING"
    LIVE = "LIVE"
    STALE = "STALE"
    RESYNC = "RESYNC"


def _nonempty_string(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value:
        raise ValueError(f"{name} must be non-empty")


def _timestamp(value: object, name: str = "exchange_ts") -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


def _decimal_string(value: object, name: str, *, nonnegative: bool = False) -> Decimal:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{name} must be a decimal string") from error
    if not parsed.is_finite():
        raise ValueError(f"{name} must be finite")
    if nonnegative and parsed < 0:
        raise ValueError(f"{name} must be nonnegative")
    return parsed


@dataclass
class EpochBook:
    token_id: str
    state: EpochState = EpochState.WARMING
    snapshot_hash: str | None = None
    exchange_ts: int | None = None
    invalid_reason: str | None = None

    def __post_init__(self) -> None:
        _nonempty_string(self.token_id, "token_id")
        if not isinstance(self.state, EpochState):
            raise TypeError("state must be an EpochState")
        if self.snapshot_hash is not None:
            _nonempty_string(self.snapshot_hash, "snapshot_hash")
        if self.exchange_ts is not None:
            _timestamp(self.exchange_ts)
        if self.invalid_reason is not None:
            _nonempty_string(self.invalid_reason, "invalid_reason")

    def invalidate(self, reason: str) -> None:
        _nonempty_string(reason, "reason")
        self.state = EpochState.RESYNC
        self.invalid_reason = reason

    def mark_stale(self, reason: str) -> None:
        _nonempty_string(reason, "reason")
        self.state = EpochState.STALE
        self.invalid_reason = reason

    def apply_delta(
        self, price: str, size: str, side: str, exchange_ts: int
    ) -> bool:
        if self.state is not EpochState.LIVE:
            return False

        _decimal_string(price, "price")
        _decimal_string(size, "size", nonnegative=True)
        if not isinstance(side, str):
            raise TypeError("side must be a string")
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        _timestamp(exchange_ts)

        if self.exchange_ts is not None and exchange_ts < self.exchange_ts:
            self.invalidate("timestamp_regression")
            return False
        self.exchange_ts = exchange_ts
        return True

    def replace_snapshot(self, snapshot_hash: str, exchange_ts: int) -> None:
        _nonempty_string(snapshot_hash, "snapshot_hash")
        _timestamp(exchange_ts)
        self.snapshot_hash = snapshot_hash
        self.exchange_ts = exchange_ts
        self.invalid_reason = None
        self.state = EpochState.LIVE
