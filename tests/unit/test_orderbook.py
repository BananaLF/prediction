from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from predmarket.domain import BookLevel, Side
from predmarket.orderbook import Fill, InsufficientDepth, OrderBook


D = Decimal


def level(price: str, size: str) -> BookLevel:
    return BookLevel(D(price), D(size))


def book(
    *,
    bids: tuple[BookLevel, ...] = (BookLevel(D(".40"), D("10")),),
    asks: tuple[BookLevel, ...] = (BookLevel(D(".50"), D("10")),),
    minimum: Decimal = D("1"),
) -> OrderBook:
    return OrderBook("token", bids, asks, D(".01"), minimum, 1, "hash")


def test_buy_walks_asks_across_levels_exactly() -> None:
    order_book = book(asks=(level(".50", "2"), level(".55", "4")))

    assert order_book.walk(Side.BUY, D("5")) == Fill(D("5"), D("2.65"), D(".55"))


def test_sell_walks_bids_across_levels_exactly() -> None:
    order_book = book(bids=(level(".45", "2"), level(".40", "4")))

    assert order_book.walk(Side.SELL, D("5")) == Fill(D("5"), D("2.10"), D(".40"))


def test_walk_raises_instead_of_returning_partial_fill() -> None:
    with pytest.raises(InsufficientDepth):
        book(asks=(level(".50", "2"),)).walk(Side.BUY, D("3"))


def test_walk_rejects_below_minimum_order_size() -> None:
    with pytest.raises(ValueError, match="minimum"):
        book(minimum=D("2")).walk(Side.BUY, D("1.99"))


@pytest.mark.parametrize("side", ["BUY", 1, None])
def test_walk_rejects_invalid_side(side: object) -> None:
    with pytest.raises(TypeError):
        book().walk(side, D("1"))  # type: ignore[arg-type]


@pytest.mark.parametrize("quantity", ["1", 1, 1.0])
def test_walk_rejects_non_decimal_quantity(quantity: object) -> None:
    with pytest.raises(TypeError):
        book().walk(Side.BUY, quantity)  # type: ignore[arg-type]


@pytest.mark.parametrize("quantity", [D("NaN"), D("Infinity"), D("-Infinity")])
def test_walk_rejects_nonfinite_quantity(quantity: Decimal) -> None:
    with pytest.raises(ValueError, match="finite"):
        book().walk(Side.BUY, quantity)


def test_order_book_and_fill_are_immutable_and_walk_does_not_mutate() -> None:
    order_book = book(asks=(level(".50", "2"), level(".55", "4")))
    original_asks = order_book.asks
    fill = order_book.walk(Side.BUY, D("3"))

    assert order_book.asks == original_asks
    with pytest.raises(FrozenInstanceError):
        order_book.book_hash = "new"
    with pytest.raises(FrozenInstanceError):
        fill.gross = D("0")


@pytest.mark.parametrize(
    "values, error",
    [
        (("1", D("1"), D(".5")), TypeError),
        ((D("1"), 1, D(".5")), TypeError),
        ((D("1"), D("1"), ".5"), TypeError),
        ((D("NaN"), D("1"), D(".5")), ValueError),
        ((D("1"), D("Infinity"), D(".5")), ValueError),
        ((D("1"), D("1"), D("NaN")), ValueError),
    ],
)
def test_fill_rejects_non_decimal_and_nonfinite_values(
    values: tuple[object, object, object], error: type[Exception]
) -> None:
    with pytest.raises(error):
        Fill(*values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "values",
    [
        (D("0"), D("1"), D(".5")),
        (D("-1"), D("1"), D(".5")),
        (D("1"), D("0"), D(".5")),
        (D("1"), D("-1"), D(".5")),
        (D("1"), D("1"), D("0")),
        (D("1"), D("1"), D("1")),
        (D("1"), D("1"), D("-.1")),
        (D("1"), D("1"), D("1.1")),
    ],
)
def test_fill_rejects_semantically_impossible_values(
    values: tuple[Decimal, Decimal, Decimal],
) -> None:
    with pytest.raises(ValueError):
        Fill(*values)


@pytest.mark.parametrize(
    "changes, error",
    [
        ({"token_id": ""}, ValueError),
        ({"book_hash": ""}, ValueError),
        ({"token_id": 1}, TypeError),
        ({"book_hash": 1}, TypeError),
        ({"tick_size": ".01"}, TypeError),
        ({"minimum_order_size": 1}, TypeError),
        ({"tick_size": D("0")}, ValueError),
        ({"tick_size": D("1")}, ValueError),
        ({"minimum_order_size": D("-1")}, ValueError),
        ({"tick_size": D("NaN")}, ValueError),
        ({"minimum_order_size": D("Infinity")}, ValueError),
        ({"exchange_ts_ms": -1}, ValueError),
        ({"exchange_ts_ms": True}, TypeError),
        ({"bids": [level(".4", "1")]}, TypeError),
        ({"asks": (object(),)}, TypeError),
    ],
)
def test_constructor_rejects_invalid_metadata(
    changes: dict[str, object], error: type[Exception]
) -> None:
    values: dict[str, object] = {
        "token_id": "token",
        "bids": (level(".40", "10"),),
        "asks": (level(".50", "10"),),
        "tick_size": D(".01"),
        "minimum_order_size": D("1"),
        "exchange_ts_ms": 1,
        "book_hash": "hash",
    }
    values.update(changes)
    with pytest.raises(error):
        OrderBook(**values)  # type: ignore[arg-type]


def test_constructor_rejects_prices_not_aligned_to_tick_size() -> None:
    with pytest.raises(ValueError, match="tick_size"):
        book(asks=(level(".505", "1"),))


@pytest.mark.parametrize(
    ("bids", "asks"),
    [
        ((level(".40", "1"), level(".45", "1")), (level(".50", "1"),)),
        ((level(".40", "1"),), (level(".55", "1"), level(".50", "1"))),
        ((level(".40", "1"), level(".40", "2")), (level(".50", "1"),)),
        ((level(".40", "1"),), (level(".50", "1"), level(".50", "2"))),
        ((level(".50", "1"),), (level(".50", "1"),)),
        ((level(".55", "1"),), (level(".50", "1"),)),
    ],
)
def test_constructor_rejects_malformed_level_order_duplicates_and_crosses(
    bids: tuple[BookLevel, ...], asks: tuple[BookLevel, ...]
) -> None:
    with pytest.raises(ValueError):
        book(bids=bids, asks=asks)


@given(
    first_size=st.decimals(min_value="1", max_value="100", places=2),
    second_size=st.decimals(min_value="1", max_value="100", places=2),
    quantity=st.decimals(min_value="1", max_value="100", places=2),
    increase=st.decimals(min_value=".01", max_value=".10", places=2),
)
def test_raising_every_ask_cannot_reduce_buy_cost(
    first_size: Decimal, second_size: Decimal, quantity: Decimal, increase: Decimal
) -> None:
    total = first_size + second_size
    quantity = min(quantity, total)
    original = book(
        bids=(), asks=(BookLevel(D(".30"), first_size), BookLevel(D(".60"), second_size))
    )
    raised = book(
        bids=(),
        asks=(
            BookLevel(D(".30") + increase, first_size),
            BookLevel(D(".60") + increase, second_size),
        ),
    )
    assert raised.walk(Side.BUY, quantity).gross >= original.walk(Side.BUY, quantity).gross


@given(
    first_size=st.decimals(min_value="1", max_value="100", places=2),
    second_size=st.decimals(min_value="1", max_value="100", places=2),
    quantity=st.decimals(min_value="1", max_value="100", places=2),
    decrease=st.decimals(min_value=".01", max_value=".10", places=2),
)
def test_lowering_every_bid_cannot_increase_sell_proceeds(
    first_size: Decimal, second_size: Decimal, quantity: Decimal, decrease: Decimal
) -> None:
    total = first_size + second_size
    quantity = min(quantity, total)
    original = book(
        bids=(BookLevel(D(".70"), first_size), BookLevel(D(".40"), second_size)), asks=()
    )
    lowered = book(
        bids=(
            BookLevel(D(".70") - decrease, first_size),
            BookLevel(D(".40") - decrease, second_size),
        ),
        asks=(),
    )
    assert lowered.walk(Side.SELL, quantity).gross <= original.walk(Side.SELL, quantity).gross


@given(
    first_size=st.decimals(min_value="1", max_value="100", places=2),
    second_size=st.decimals(min_value="1", max_value="100", places=2),
)
def test_reducing_depth_cannot_create_a_fill(first_size: Decimal, second_size: Decimal) -> None:
    full = book(asks=(BookLevel(D(".50"), first_size), BookLevel(D(".60"), second_size)))
    reduced = book(asks=(BookLevel(D(".50"), first_size),))
    quantity = first_size + second_size

    assert full.walk(Side.BUY, quantity).quantity == quantity
    with pytest.raises(InsufficientDepth):
        reduced.walk(Side.BUY, quantity)
