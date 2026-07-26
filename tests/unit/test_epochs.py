from decimal import Decimal

import pytest

from predmarket.epochs import EpochBook, EpochState


def test_epoch_enum_values_are_stable() -> None:
    assert [(item.name, item.value) for item in EpochState] == [
        ("WARMING", "WARMING"),
        ("LIVE", "LIVE"),
        ("STALE", "STALE"),
        ("RESYNC", "RESYNC"),
    ]


def test_lifecycle_warming_live_resync_live() -> None:
    book = EpochBook("token")
    assert book.state is EpochState.WARMING
    assert book.apply_delta("0.4", "2", "BUY", 1) is False

    book.replace_snapshot("hash-1", 10)
    assert (book.state, book.snapshot_hash, book.exchange_ts_ms, book.invalid_reason) == (
        EpochState.LIVE,
        "hash-1",
        10,
        None,
    )
    assert book.apply_delta("0.5", "0", "SELL", 11) is True
    assert book.exchange_ts_ms == 11

    book.invalidate("gap")
    assert book.state is EpochState.RESYNC
    assert book.invalid_reason == "gap"
    assert book.apply_delta("0.5", "1", "BUY", 12) is False

    book.replace_snapshot("hash-2", 20)
    assert (book.state, book.snapshot_hash, book.exchange_ts_ms, book.invalid_reason) == (
        EpochState.LIVE,
        "hash-2",
        20,
        None,
    )


def test_timestamp_regression_invalidates_live_epoch() -> None:
    book = EpochBook("token")
    book.replace_snapshot("hash", 10)
    assert book.apply_delta("0.5", "1", "BUY", 9) is False
    assert (book.state, book.invalid_reason, book.exchange_ts_ms) == (
        EpochState.RESYNC,
        "timestamp_regression",
        10,
    )


def test_mark_stale_records_reason_and_ignores_deltas() -> None:
    book = EpochBook("token")
    book.replace_snapshot("hash", 10)
    book.mark_stale("heartbeat_timeout")
    assert (book.state, book.invalid_reason) == (EpochState.STALE, "heartbeat_timeout")
    assert book.apply_delta("0.5", "1", "BUY", 11) is False
    assert book.exchange_ts_ms == 10


@pytest.mark.parametrize("method", ["invalidate", "mark_stale"])
@pytest.mark.parametrize("reason", ["", 1, None])
def test_state_changes_require_nonempty_string_reason(method: str, reason: object) -> None:
    book = EpochBook("token")
    before = vars(book).copy()
    with pytest.raises((TypeError, ValueError)):
        getattr(book, method)(reason)
    assert vars(book) == before


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(token_id=""),
        dict(token_id=1),
        dict(snapshot_hash=""),
        dict(snapshot_hash=1),
        dict(exchange_ts_ms=-1),
        dict(exchange_ts_ms=True),
        dict(invalid_reason=""),
        dict(state="LIVE"),
    ],
)
def test_constructor_validation(kwargs: dict[str, object]) -> None:
    arguments: dict[str, object] = {"token_id": "token"}
    arguments.update(kwargs)
    with pytest.raises((TypeError, ValueError)):
        EpochBook(**arguments)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "arguments",
    [
        ("", 20),
        (1, 20),
        ("new", -1),
        ("new", True),
    ],
)
def test_invalid_snapshot_replacement_is_atomic(arguments: tuple[object, object]) -> None:
    book = EpochBook("token")
    book.replace_snapshot("old", 10)
    before = vars(book).copy()
    with pytest.raises((TypeError, ValueError)):
        book.replace_snapshot(*arguments)  # type: ignore[arg-type]
    assert vars(book) == before


@pytest.mark.parametrize(
    "arguments",
    [
        ("nan", "1", "BUY", 11),
        ("Infinity", "1", "BUY", 11),
        ("0.5", "-1", "BUY", 11),
        ("0.5", "nan", "BUY", 11),
        ("0.5", "1", "HOLD", 11),
        (Decimal("0.5"), "1", "BUY", 11),
        ("0.5", Decimal("1"), "BUY", 11),
        ("0.5", "1", "BUY", True),
    ],
)
def test_invalid_live_delta_is_atomic(arguments: tuple[object, ...]) -> None:
    book = EpochBook("token")
    book.replace_snapshot("hash", 10)
    before = vars(book).copy()
    with pytest.raises((TypeError, ValueError)):
        book.apply_delta(*arguments)  # type: ignore[arg-type]
    assert vars(book) == before


def test_ignored_delta_does_not_need_validation_or_mutate() -> None:
    book = EpochBook("token")
    before = vars(book).copy()
    assert book.apply_delta(object(), object(), object(), object()) is False
    assert vars(book) == before


def test_regressing_replacement_snapshot_fails_closed_without_replacing_book() -> None:
    book = EpochBook("token")
    book.replace_snapshot("current", 100)
    book.invalidate("sequence_gap")

    assert book.replace_snapshot("older", 90) is False
    assert (
        book.state,
        book.snapshot_hash,
        book.exchange_ts_ms,
        book.invalid_reason,
    ) == (
        EpochState.RESYNC,
        "current",
        100,
        "snapshot_timestamp_regression",
    )


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("state", EpochState.LIVE),
        ("exchange_ts_ms", -1),
        ("invalid_reason", None),
        ("snapshot_hash", "forged"),
    ],
)
def test_transition_controlled_fields_are_externally_read_only(
    attribute: str, value: object
) -> None:
    book = EpochBook("token")
    before = (
        book.state,
        book.snapshot_hash,
        book.exchange_ts_ms,
        book.invalid_reason,
    )
    with pytest.raises(AttributeError):
        setattr(book, attribute, value)
    assert (
        book.state,
        book.snapshot_hash,
        book.exchange_ts_ms,
        book.invalid_reason,
    ) == before
