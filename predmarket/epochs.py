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


@dataclass(init=False)
class EpochBook:
    token_id: str
    _state: EpochState
    _snapshot_hash: str | None
    _exchange_ts_ms: int | None
    _invalid_reason: str | None

    def __init__(
        self,
        token_id: str,
        *,
        snapshot_hash: str | None = None,
        exchange_ts_ms: int | None = None,
        invalid_reason: str | None = None,
    ) -> None:
        _nonempty_string(token_id, "token_id")
        if snapshot_hash is not None:
            _nonempty_string(snapshot_hash, "snapshot_hash")
        if exchange_ts_ms is not None:
            _timestamp(exchange_ts_ms, "exchange_ts_ms")
        if invalid_reason is not None:
            _nonempty_string(invalid_reason, "invalid_reason")

        self.token_id = token_id
        self._state = EpochState.WARMING
        self._snapshot_hash = snapshot_hash
        self._exchange_ts_ms = exchange_ts_ms
        self._invalid_reason = invalid_reason

    @property
    def state(self) -> EpochState:
        return self._state

    @property
    def snapshot_hash(self) -> str | None:
        return self._snapshot_hash

    @property
    def exchange_ts_ms(self) -> int | None:
        return self._exchange_ts_ms

    @property
    def invalid_reason(self) -> str | None:
        return self._invalid_reason

    def invalidate(self, reason: str) -> None:
        _nonempty_string(reason, "reason")
        self._state = EpochState.RESYNC
        self._invalid_reason = reason

    def mark_stale(self, reason: str) -> None:
        _nonempty_string(reason, "reason")
        self._state = EpochState.STALE
        self._invalid_reason = reason

    def apply_delta(
        self, price: str, size: str, side: str, exchange_ts: int
    ) -> bool:
        if self._state is not EpochState.LIVE:
            return False

        _decimal_string(price, "price")
        _decimal_string(size, "size", nonnegative=True)
        if not isinstance(side, str):
            raise TypeError("side must be a string")
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        _timestamp(exchange_ts)

        if self._exchange_ts_ms is not None and exchange_ts < self._exchange_ts_ms:
            self.invalidate("timestamp_regression")
            return False
        self._exchange_ts_ms = exchange_ts
        return True

    def replace_snapshot(self, snapshot_hash: str, exchange_ts_ms: int) -> bool:
        _nonempty_string(snapshot_hash, "snapshot_hash")
        _timestamp(exchange_ts_ms, "exchange_ts_ms")
        if (
            self._exchange_ts_ms is not None
            and exchange_ts_ms < self._exchange_ts_ms
        ):
            self.invalidate("snapshot_timestamp_regression")
            return False

        self._snapshot_hash = snapshot_hash
        self._exchange_ts_ms = exchange_ts_ms
        self._invalid_reason = None
        self._state = EpochState.LIVE
        return True
