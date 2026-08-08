from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
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
    exchange_timestamp: int = 100,
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
        exchange_timestamp=exchange_timestamp,
        received_timestamp=101,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def _valid_cache(*, verifier=None) -> OrderBookCache:
    cache = OrderBookCache(hash_verifier=verifier)
    cache.begin_resync(generation=1, token_ids=("token-b", "token-a"))
    cache.apply_snapshot((_book("token-b"), _book("token-a")))
    return cache


def test_revision_starts_at_zero_and_snapshot_increments_once() -> None:
    cache = OrderBookCache()

    assert cache.revision == 0
    cache.begin_resync(generation=1, token_ids=("token-a", "token-b"))
    assert cache.revision == 0

    cache.apply_snapshot((_book("token-a"), _book("token-b")))

    assert cache.revision == 1


def test_revision_increments_only_for_accepted_full_book_mutation() -> None:
    cache = _valid_cache()
    baseline_revision = cache.revision
    current = cache.get("token-a")
    assert current is not None

    assert cache.apply_book(current) is False
    assert cache.apply_book(
        _book("token-a", book_hash="stale", exchange_timestamp=99)
    ) is False
    assert cache.revision == baseline_revision

    assert cache.apply_book(
        _book("token-a", book_hash="accepted", exchange_timestamp=101)
    ) is True
    assert cache.revision == baseline_revision + 1


def test_revision_increments_once_for_multi_token_delta() -> None:
    cache = _valid_cache()
    baseline_revision = cache.revision

    assert cache.apply_delta(
        (
            OrderBookDelta("token-a", "BUY", "0.41", "7", "post-a"),
            OrderBookDelta("token-b", "BUY", "0.41", "8", "post-b"),
        ),
        generation=1,
        sequence=1,
        exchange_timestamp=102,
        received_timestamp=103,
    ) is True

    assert cache.revision == baseline_revision + 1


def test_revision_does_not_change_for_rejected_or_resync_state_changes() -> None:
    cache = _valid_cache()
    baseline_revision = cache.revision

    assert cache.apply_delta(
        (OrderBookDelta("token-a", "BUY", "0.41", "9", "stale"),),
        generation=1,
        sequence=1,
        exchange_timestamp=99,
        received_timestamp=103,
    ) is False
    assert cache.revision == baseline_revision

    assert cache.invalidate(generation=1, reason="disconnect") is True
    assert cache.apply_book(_book("token-a", exchange_timestamp=200)) is False
    cache.begin_resync(generation=2, token_ids=("token-a", "token-b"))

    assert cache.revision == baseline_revision


def test_token_revision_snapshot_tracks_only_changed_full_book() -> None:
    cache = _valid_cache()
    snapshot = cache.snapshot_token_revisions()

    assert snapshot.generation == 1
    assert snapshot.revisions == (("token-a", 1), ("token-b", 1))
    with pytest.raises(FrozenInstanceError):
        snapshot.generation = 2  # type: ignore[misc]

    current = cache.get("token-a")
    assert current is not None
    assert cache.apply_book(
        replace(
            current,
            book_hash="token-a-new",
            exchange_timestamp=101,
        )
    ) is True

    assert cache.token_revisions_match(snapshot, ("token-b",)) is True
    assert cache.token_revisions_match(snapshot, ("token-a",)) is False


def test_multi_token_delta_advances_each_changed_token_revision() -> None:
    cache = _valid_cache()
    snapshot = cache.snapshot_token_revisions()

    assert cache.apply_delta(
        (
            OrderBookDelta("token-a", "BUY", "0.41", "7", "post-a"),
            OrderBookDelta("token-b", "BUY", "0.41", "8", "post-b"),
        ),
        generation=1,
        sequence=1,
        exchange_timestamp=102,
        received_timestamp=103,
    ) is True

    assert cache.token_revisions_match(snapshot, ("token-a",)) is False
    assert cache.token_revisions_match(snapshot, ("token-b",)) is False


def test_token_revision_snapshot_fails_closed_after_invalidation_or_generation_change() -> None:
    cache = _valid_cache()
    snapshot = cache.snapshot_token_revisions()

    assert cache.invalidate(generation=1, reason="rotate") is True
    assert cache.token_revisions_match(snapshot, ("token-a",)) is False
    cache.begin_resync(generation=2, token_ids=("token-a", "token-b"))
    cache.apply_snapshot(
        (_book("token-a", generation=2), _book("token-b", generation=2))
    )

    assert cache.token_revisions_match(snapshot, ("token-a",)) is False


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


def test_crossed_snapshot_invalidates_cache() -> None:
    cache = OrderBookCache()
    cache.begin_resync(generation=1, token_ids=("token-a", "token-b"))
    crossed = replace(
        _book("token-a"),
        bids=(OrderBookLevel(Decimal("0.49"), Decimal("1")),),
    )

    with pytest.raises(CacheInvalidatedError, match="best bid must be below best ask"):
        cache.apply_snapshot((crossed, _book("token-b")))

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


def test_delta_that_crosses_top_of_book_invalidates_cache() -> None:
    cache = _valid_cache()

    with pytest.raises(CacheInvalidatedError, match="best bid must be below best ask"):
        cache.apply_delta(
            (OrderBookDelta("token-a", "BUY", "0.49", "1", "crossed"),),
            generation=1,
            sequence=1,
            exchange_timestamp=102,
            received_timestamp=103,
        )

    assert cache.state is CacheState.INVALID
    assert cache.invalid_reason == "best bid must be below best ask"
    assert cache.view() == ()


def test_delta_reconciles_stale_opposite_top_from_authoritative_server_top() -> None:
    cache = _valid_cache()
    baseline = replace(
        _book("token-a"),
        bids=(OrderBookLevel(Decimal("0.69"), Decimal("2")),),
        asks=(
            OrderBookLevel(Decimal("0.70"), Decimal("4")),
            OrderBookLevel(Decimal("0.71"), Decimal("5")),
        ),
    )
    cache.apply_book(baseline)

    applied = cache.apply_delta(
        (
            OrderBookDelta(
                "token-a",
                "BUY",
                "0.7",
                "19",
                "post-a",
                best_bid="0.7",
                best_ask="0.71",
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
    assert tuple(level.price for level in book.bids) == (Decimal("0.70"), Decimal("0.69"))
    assert tuple(level.price for level in book.asks) == (Decimal("0.71"),)
    assert cache.state is CacheState.VALID


def test_full_stream_book_replaces_rest_baseline_when_not_older() -> None:
    # Catches a normal initial WebSocket snapshot being treated as corruption.
    cache = _valid_cache()
    stream_book = _book(
        "token-a",
        book_hash="stream-hash",
        exchange_timestamp=110,
    )

    applied = cache.apply_book(stream_book)

    assert applied is True
    assert cache.get("token-a") == stream_book
    assert cache.state is CacheState.VALID


def test_crossed_full_stream_book_invalidates_cache() -> None:
    cache = _valid_cache()
    crossed = replace(
        _book("token-a", exchange_timestamp=110),
        bids=(OrderBookLevel(Decimal("0.49"), Decimal("1")),),
    )

    with pytest.raises(CacheInvalidatedError, match="best bid must be below best ask"):
        cache.apply_book(crossed)

    assert cache.state is CacheState.INVALID
    assert cache.view() == ()


def test_stale_full_book_and_delta_do_not_overwrite_rest_baseline() -> None:
    # Catches events buffered during REST recovery rolling the baseline backward.
    cache = _valid_cache()
    before = cache.get("token-a")

    assert cache.apply_book(
        _book("token-a", book_hash="stale-book", exchange_timestamp=99)
    ) is False
    assert cache.apply_delta(
        (OrderBookDelta("token-a", "BUY", "0.41", "9", "stale-delta"),),
        generation=1,
        sequence=1,
        exchange_timestamp=99,
        received_timestamp=110,
    ) is False

    assert cache.get("token-a") == before
    assert cache.state is CacheState.VALID


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
