from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.watch.cache import (
    CacheInvalidatedError,
    CacheState,
    OrderBookCache,
    OrderBookDelta,
)


def _book(
    token_id: str,
    *,
    generation: int = 1,
    book_hash: str | None = None,
) -> OrderBook:
    return OrderBook(
        market_id="market-1",
        token_id=token_id,
        bids=(
            OrderBookLevel(price=Decimal("0.40"), size=Decimal("2")),
            OrderBookLevel(price=Decimal("0.42"), size=Decimal("3")),
        ),
        asks=(
            OrderBookLevel(price=Decimal("0.48"), size=Decimal("4")),
            OrderBookLevel(price=Decimal("0.46"), size=Decimal("5")),
        ),
        subscription_generation=generation,
        book_hash=book_hash or f"hash-{token_id}-{generation}",
        exchange_timestamp=100,
        received_timestamp=101,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def _valid_cache(*, verifier=None) -> OrderBookCache:
    cache = OrderBookCache(hash_verifier=verifier)
    cache.begin_resync(generation=1, token_ids=("token-b", "token-a"))
    cache.apply_snapshot((_book("token-b"), _book("token-a")))
    return cache


def test_complete_snapshot_becomes_valid_sorted_immutable_view() -> None:
    # Catches partial/mutable REST baselines escaping the recovery barrier.
    cache = OrderBookCache()

    cache.begin_resync(generation=1, token_ids=("token-b", "token-a"))
    view = cache.apply_snapshot((_book("token-b"), _book("token-a")))

    assert cache.state is CacheState.VALID
    assert tuple(book.token_id for book in view) == ("token-a", "token-b")
    assert tuple(level.price for level in view[0].bids) == (
        Decimal("0.42"),
        Decimal("0.40"),
    )
    assert tuple(level.price for level in view[0].asks) == (
        Decimal("0.46"),
        Decimal("0.48"),
    )
    with pytest.raises(FrozenInstanceError):
        view[0].book_hash = "mutated"  # type: ignore[misc]


def test_snapshot_requires_exact_resync_token_set() -> None:
    # Catches strategy resuming with one requested book missing.
    cache = OrderBookCache()
    cache.begin_resync(generation=1, token_ids=("token-a", "token-b"))

    with pytest.raises(CacheInvalidatedError, match="snapshot token set"):
        cache.apply_snapshot((_book("token-a"),))

    assert cache.state is CacheState.INVALID
    assert cache.view() == ()


def test_delta_uses_canonical_decimal_strings_and_updates_sorted_levels() -> None:
    # Catches binary floats or unsorted/zero levels entering strategy evidence.
    cache = _valid_cache()

    applied = cache.apply_delta(
        (
            OrderBookDelta(
                token_id="token-a",
                side="BUY",
                price="0.41",
                size="7.5",
                book_hash="post-a",
            ),
            OrderBookDelta(
                token_id="token-a",
                side="SELL",
                price="0.46",
                size="0",
                book_hash="post-a",
            ),
        ),
        generation=1,
        sequence=1,
        exchange_timestamp=102,
        received_timestamp=103,
    )

    assert applied is True
    book = cache.get("token-a")
    assert book is not None
    assert tuple((level.price, level.size) for level in book.bids) == (
        (Decimal("0.42"), Decimal("3")),
        (Decimal("0.41"), Decimal("7.5")),
        (Decimal("0.40"), Decimal("2")),
    )
    assert tuple(level.price for level in book.asks) == (Decimal("0.48"),)
    assert book.book_hash == "post-a"
    assert book.exchange_timestamp == 102
    assert book.received_timestamp == 103


@pytest.mark.parametrize(
    ("price", "size"),
    [("0.410", "1"), ("0.41", "1.0"), (0.41, "1")],
)
def test_noncanonical_or_non_string_delta_invalidates(price: object, size: object) -> None:
    # Catches Decimal/float coercion weakening the canonical evidence boundary.
    cache = _valid_cache()

    with pytest.raises(CacheInvalidatedError, match="canonical"):
        cache.apply_delta(
            (
                OrderBookDelta(
                    token_id="token-a",
                    side="BUY",
                    price=price,  # type: ignore[arg-type]
                    size=size,  # type: ignore[arg-type]
                    book_hash="post-a",
                ),
            ),
            generation=1,
            sequence=1,
            exchange_timestamp=102,
            received_timestamp=103,
        )

    assert cache.state is CacheState.INVALID


def test_old_generation_and_late_messages_are_rejected_without_mutation() -> None:
    # Catches a delayed old subscription overwriting the current generation.
    cache = _valid_cache()
    cache.invalidate(generation=1, reason="rotate")
    cache.begin_resync(generation=2, token_ids=("token-a", "token-b"))
    cache.apply_snapshot(
        (_book("token-a", generation=2), _book("token-b", generation=2))
    )
    before = cache.view()

    assert cache.apply_delta(
        (
            OrderBookDelta(
                token_id="token-a",
                side="BUY",
                price="0.41",
                size="9",
                book_hash="stale",
            ),
        ),
        generation=1,
        sequence=1,
        exchange_timestamp=102,
        received_timestamp=103,
    ) is False
    assert cache.view() == before
    assert cache.state is CacheState.VALID


def test_sequence_gap_invalidates_and_clears_generation() -> None:
    # Catches an undetected missing local delivery after a dropped SDK event.
    cache = _valid_cache()

    with pytest.raises(CacheInvalidatedError, match="sequence gap"):
        cache.apply_delta(
            (
                OrderBookDelta(
                    token_id="token-a",
                    side="BUY",
                    price="0.41",
                    size="9",
                    book_hash="post-a",
                ),
            ),
            generation=1,
            sequence=2,
            exchange_timestamp=102,
            received_timestamp=103,
        )

    assert cache.state is CacheState.INVALID
    assert cache.view() == ()


def test_unexpected_future_generation_invalidates_current_cache() -> None:
    # Catches a subscription ownership bug being mistaken for a harmless late event.
    cache = _valid_cache()

    with pytest.raises(CacheInvalidatedError, match="future generation"):
        cache.apply_delta(
            (OrderBookDelta("token-a", "BUY", "0.41", "9", "future"),),
            generation=2,
            sequence=1,
            exchange_timestamp=102,
            received_timestamp=103,
        )

    assert cache.state is CacheState.INVALID


def test_conflicting_hashes_in_one_token_batch_invalidate() -> None:
    # Catches one atomic SDK token update being split across conflicting post hashes.
    cache = _valid_cache()

    with pytest.raises(CacheInvalidatedError, match="conflicting opaque hashes"):
        cache.apply_delta(
            (
                OrderBookDelta("token-a", "BUY", "0.41", "2", "hash-1"),
                OrderBookDelta("token-a", "SELL", "0.47", "2", "hash-2"),
            ),
            generation=1,
            sequence=1,
            exchange_timestamp=102,
            received_timestamp=103,
        )

    assert cache.state is CacheState.INVALID


def test_injected_hash_verifier_mismatch_invalidates() -> None:
    # Catches a calculable post-book hash mismatch being accepted as evidence.
    cache = _valid_cache(verifier=lambda _before, _after, claimed: claimed == "good")

    with pytest.raises(CacheInvalidatedError, match="book hash mismatch"):
        cache.apply_delta(
            (OrderBookDelta("token-a", "BUY", "0.41", "2", "bad"),),
            generation=1,
            sequence=1,
            exchange_timestamp=102,
            received_timestamp=103,
        )

    assert cache.state is CacheState.INVALID


def test_recovery_state_machine_is_strict_and_generation_monotonic() -> None:
    # Catches resync bypasses and generation reuse after invalidation.
    cache = _valid_cache()

    assert cache.invalidate(generation=0, reason="late") is False
    assert cache.invalidate(generation=1, reason="disconnect") is True
    assert cache.state is CacheState.INVALID
    cache.begin_resync(generation=2, token_ids=("token-a", "token-b"))
    assert cache.state is CacheState.RESYNCING
    with pytest.raises(RuntimeError, match="only from INVALID"):
        cache.begin_resync(generation=1, token_ids=("token-a", "token-b"))
    view = cache.apply_snapshot(
        (_book("token-a", generation=2), _book("token-b", generation=2))
    )

    assert cache.state is CacheState.VALID
    assert {book.subscription_generation for book in view} == {2}
