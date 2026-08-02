"""Generation-scoped in-memory order books behind a recovery barrier."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum

from predmarket.domain.decimal import parse_decimal
from predmarket.domain.orderbook import OrderBook, OrderBookLevel


class CacheState(str, Enum):
    INVALID = "INVALID"
    RESYNCING = "RESYNCING"
    VALID = "VALID"


class CacheInvalidatedError(RuntimeError):
    """A candidate update failed closed and invalidated its generation."""


@dataclass(frozen=True, slots=True)
class OrderBookDelta:
    """One canonical SDK price change with its opaque post-book hash."""

    token_id: str
    side: str
    price: str
    size: str
    book_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.token_id, str) or not self.token_id:
            raise ValueError("token_id must be a non-empty string")
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        if not isinstance(self.book_hash, str) or not self.book_hash:
            raise ValueError("book_hash must be a non-empty string")


HashVerifier = Callable[[OrderBook, OrderBook, str], bool]


class OrderBookCache:
    """Apply a generation atomically and expose immutable sorted books.

    The pinned SDK does not publish a sequence or a documented book-hash
    algorithm. ``sequence`` is therefore the WatchTask's local delivery order.
    The default opaque-hash policy can prove only that a token batch agrees on
    one non-empty post-book hash. A deterministic verifier may be injected when
    the upstream hash algorithm is known by the caller.
    """

    def __init__(self, *, hash_verifier: HashVerifier | None = None) -> None:
        if hash_verifier is not None and not callable(hash_verifier):
            raise TypeError("hash_verifier must be callable")
        self._hash_verifier = hash_verifier
        self._state = CacheState.INVALID
        self._generation = 0
        self._last_sequence = 0
        self._expected_token_ids: tuple[str, ...] = ()
        self._books: dict[str, OrderBook] = {}
        self._invalid_reason: str | None = "not_initialized"

    @property
    def state(self) -> CacheState:
        return self._state

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def last_sequence(self) -> int:
        return self._last_sequence

    @property
    def invalid_reason(self) -> str | None:
        return self._invalid_reason

    def begin_resync(self, *, generation: int, token_ids: Sequence[str]) -> None:
        if self._state is not CacheState.INVALID:
            raise RuntimeError("resync may begin only from INVALID")
        if type(generation) is not int or generation <= self._generation:
            raise ValueError("resync generation must be newer")
        normalized = _token_ids(token_ids)
        self._generation = generation
        self._last_sequence = 0
        self._expected_token_ids = normalized
        self._books.clear()
        self._state = CacheState.RESYNCING
        self._invalid_reason = None

    def apply_snapshot(self, books: Sequence[OrderBook]) -> tuple[OrderBook, ...]:
        if self._state is not CacheState.RESYNCING:
            raise RuntimeError("snapshot may be applied only while RESYNCING")
        try:
            materialized = tuple(books)
            if any(not isinstance(book, OrderBook) for book in materialized):
                raise ValueError("snapshot must contain OrderBook values")
            by_token = {book.token_id: book for book in materialized}
            if len(by_token) != len(materialized):
                raise ValueError("snapshot token IDs must be unique")
            if tuple(sorted(by_token, key=_utf8)) != self._expected_token_ids:
                raise ValueError("snapshot token set is incomplete or unexpected")
            if any(
                book.subscription_generation != self._generation
                for book in materialized
            ):
                raise ValueError("snapshot generation does not match resync")
            market_by_token = {
                token_id: book.market_id for token_id, book in by_token.items()
            }
            if any(not market_id for market_id in market_by_token.values()):
                raise ValueError("snapshot market identity is missing")
        except (TypeError, ValueError) as error:
            self._fail_closed(str(error))

        self._books = by_token
        self._state = CacheState.VALID
        self._invalid_reason = None
        return self.view()

    def apply_delta(
        self,
        deltas: Sequence[OrderBookDelta],
        *,
        generation: int,
        sequence: int,
        exchange_timestamp: int,
        received_timestamp: int,
    ) -> bool:
        if type(generation) is not int or generation < 1:
            raise ValueError("generation must be a positive integer")
        if generation < self._generation:
            return False
        if generation > self._generation:
            self._fail_closed("unexpected future generation")
        if self._state is not CacheState.VALID:
            return False
        if type(sequence) is not int or sequence != self._last_sequence + 1:
            self._fail_closed("sequence gap")
        for name, timestamp in (
            ("exchange_timestamp", exchange_timestamp),
            ("received_timestamp", received_timestamp),
        ):
            if type(timestamp) is not int or timestamp < 0:
                self._fail_closed(f"{name} must be a non-negative integer")

        try:
            materialized = tuple(deltas)
        except TypeError:
            self._fail_closed("deltas must be an iterable")
        if not materialized or any(
            not isinstance(delta, OrderBookDelta) for delta in materialized
        ):
            self._fail_closed("deltas must contain OrderBookDelta values")

        grouped: dict[str, list[OrderBookDelta]] = {}
        for delta in materialized:
            if delta.token_id not in self._books:
                self._fail_closed("delta token is outside the active snapshot")
            grouped.setdefault(delta.token_id, []).append(delta)

        candidates = dict(self._books)
        for token_id, token_deltas in grouped.items():
            hashes = {delta.book_hash for delta in token_deltas}
            if len(hashes) != 1:
                self._fail_closed("conflicting opaque hashes in one token batch")
            claimed_hash = hashes.pop()
            before = self._books[token_id]
            try:
                bids = {level.price: level.size for level in before.bids}
                asks = {level.price: level.size for level in before.asks}
                for delta in token_deltas:
                    price = parse_decimal(delta.price)
                    size = parse_decimal(delta.size)
                    if not Decimal("0") < price < Decimal("1"):
                        raise ValueError("delta price must be in (0, 1)")
                    if size < Decimal("0"):
                        raise ValueError("delta size must not be negative")
                    levels = bids if delta.side == "BUY" else asks
                    if size == 0:
                        levels.pop(price, None)
                    else:
                        levels[price] = size
                candidate = replace(
                    before,
                    bids=tuple(
                        OrderBookLevel(price=price, size=size)
                        for price, size in bids.items()
                    ),
                    asks=tuple(
                        OrderBookLevel(price=price, size=size)
                        for price, size in asks.items()
                    ),
                    book_hash=claimed_hash,
                    exchange_timestamp=exchange_timestamp,
                    received_timestamp=received_timestamp,
                )
            except (TypeError, ValueError) as error:
                message = str(error)
                if "decimal" in message:
                    message = f"canonical delta required: {message}"
                self._fail_closed(message)
            if self._hash_verifier is not None:
                try:
                    verified = self._hash_verifier(before, candidate, claimed_hash)
                except Exception as error:
                    self._fail_closed(f"book hash verifier failed: {error}")
                if type(verified) is not bool or not verified:
                    self._fail_closed("book hash mismatch")
            candidates[token_id] = candidate

        self._books = candidates
        self._last_sequence = sequence
        return True

    def invalidate(self, *, generation: int, reason: str) -> bool:
        if type(generation) is not int or generation < 0:
            raise ValueError("generation must be a non-negative integer")
        if not isinstance(reason, str) or not reason:
            raise ValueError("reason must be a non-empty string")
        if generation != self._generation or self._state is not CacheState.VALID:
            return False
        self._state = CacheState.INVALID
        self._invalid_reason = reason
        self._books.clear()
        self._expected_token_ids = ()
        return True

    def view(self) -> tuple[OrderBook, ...]:
        if self._state is not CacheState.VALID:
            return ()
        return tuple(self._books[token_id] for token_id in sorted(self._books, key=_utf8))

    def get(self, token_id: str) -> OrderBook | None:
        if self._state is not CacheState.VALID:
            return None
        return self._books.get(token_id)

    def _fail_closed(self, reason: str) -> None:
        self._state = CacheState.INVALID
        self._invalid_reason = reason
        self._books.clear()
        self._expected_token_ids = ()
        raise CacheInvalidatedError(reason)


def _token_ids(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError("token_ids must be an iterable of identifiers")
    try:
        materialized = tuple(values)
    except TypeError as error:
        raise ValueError("token_ids must be an iterable of identifiers") from error
    if not materialized:
        raise ValueError("token_ids must not be empty")
    if any(not isinstance(value, str) or not value for value in materialized):
        raise ValueError("token_ids must contain non-empty strings")
    if len(materialized) != len(set(materialized)):
        raise ValueError("token_ids must not contain duplicates")
    return tuple(sorted(materialized, key=_utf8))


def _utf8(value: str) -> bytes:
    return value.encode("utf-8")
