from decimal import Decimal
from dataclasses import dataclass

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.strategy.optimizer import (
    DepthRequirement,
    InsufficientDepth,
    breakpoint_quantities,
    optimize_quantity,
    optimize_candidates,
    walk_depth,
)


def _book(
    token_id: str,
    *,
    bids: tuple[tuple[str, str], ...] = (("0.40", "10"),),
    asks: tuple[tuple[str, str], ...] = (("0.50", "10"),),
    minimum: str = "1",
) -> OrderBook:
    return OrderBook(
        market_id=f"market-{token_id}",
        token_id=token_id,
        bids=tuple(OrderBookLevel(Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple(OrderBookLevel(Decimal(price), Decimal(size)) for price, size in asks),
        subscription_generation=1,
        book_hash=f"hash-{token_id}",
        exchange_timestamp=1_000,
        received_timestamp=1_010,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal(minimum),
    )


def test_walk_depth_uses_every_level_and_reports_exact_prices() -> None:
    # Catches replacing L2 walking with best-price multiplication.
    fill = walk_depth(
        (
            OrderBookLevel(Decimal("0.40"), Decimal("2")),
            OrderBookLevel(Decimal("0.45"), Decimal("3")),
        ),
        Decimal("4"),
    )

    assert fill.quantity == Decimal("4")
    assert fill.gross_amount == Decimal("1.70")
    assert fill.average_price == Decimal("0.425")
    assert fill.worst_price == Decimal("0.45")


def test_walk_depth_rejects_quantity_beyond_complete_depth() -> None:
    # Catches silently returning a partial leg as executable.
    levels = (OrderBookLevel(Decimal("0.40"), Decimal("2")),)

    try:
        walk_depth(levels, Decimal("3"))
    except InsufficientDepth as error:
        assert error.available == Decimal("2")
        assert error.requested == Decimal("3")
    else:  # pragma: no cover - explicit assertion message
        raise AssertionError("depth exhaustion must fail closed")


def test_breakpoints_include_minimum_default_and_every_relevant_l2_boundary() -> None:
    # Catches optimizing only at one fixed quantity or one book's boundaries.
    buy = _book("buy", asks=(("0.50", "2"), ("0.55", "3")), minimum="3")
    sell = _book("sell", bids=(("0.45", "1"), ("0.40", "4")), minimum="2")

    candidates = breakpoint_quantities(
        (
            DepthRequirement(buy, "BUY"),
            DepthRequirement(sell, "SELL"),
        ),
        minimum_quantity=Decimal("3"),
        default_quantity=Decimal("4"),
    )

    assert candidates == (Decimal("3"), Decimal("4"), Decimal("5"))


def test_optimizer_adds_exact_bankroll_boundary_inside_a_depth_interval() -> None:
    # Catches dropping the largest executable quantity when bankroll cuts a level.
    book = _book("buy", asks=(("0.50", "2"), ("0.60", "8")))

    quantity = optimize_quantity(
        (DepthRequirement(book, "BUY"),),
        minimum_quantity=Decimal("1"),
        default_quantity=Decimal("1"),
        bankroll=Decimal("2"),
        evaluate=lambda q: (q * Decimal("0.5"), q * Decimal("0.1")),
    )

    assert quantity == Decimal("4")


def test_optimizer_selects_maximum_profit_not_largest_quantity() -> None:
    # Catches using maximum depth rather than the specified profit objective.
    book = _book("buy", asks=(("0.40", "2"), ("0.90", "3")))

    def evaluate(quantity: Decimal) -> tuple[Decimal, Decimal]:
        profit = Decimal("1") if quantity == Decimal("2") else Decimal("0.5")
        return quantity * Decimal("0.4"), profit

    quantity = optimize_quantity(
        (DepthRequirement(book, "BUY"),),
        minimum_quantity=Decimal("1"),
        default_quantity=Decimal("1"),
        bankroll=Decimal("10"),
        evaluate=evaluate,
    )

    assert quantity == Decimal("2")


def test_optimizer_returns_none_for_minimum_size_or_depth_failure() -> None:
    # Catches manufacturing an undersized or partially filled opportunity.
    book = _book("buy", asks=(("0.50", "2"),), minimum="3")

    quantity = optimize_quantity(
        (DepthRequirement(book, "BUY"),),
        minimum_quantity=Decimal("3"),
        default_quantity=Decimal("1"),
        bankroll=Decimal("10"),
        evaluate=lambda q: (q, q),
    )

    assert quantity is None


def test_candidate_optimizer_inserts_constraint_boundary_before_selecting_profit() -> None:
    # Catches choosing the max-profit candidate before applying a linear risk limit.
    book = _book("buy", asks=(("0.40", "100"),))

    @dataclass(frozen=True)
    class Candidate:
        quantity: Decimal
        total_capital: Decimal
        expected_profit: Decimal
        unhedged: Decimal

    def evaluate(quantity: Decimal) -> Candidate:
        return Candidate(
            quantity,
            quantity * Decimal("0.8"),
            quantity * Decimal("0.2"),
            quantity * Decimal("0.4"),
        )

    selection = optimize_candidates(
        (DepthRequirement(book, "BUY"),),
        minimum_quantity=Decimal("1"),
        default_quantity=Decimal("1"),
        evaluate=evaluate,
        constraint_margins=lambda candidate: {
            "bankroll": candidate.total_capital - Decimal("1000"),
            "unhedged": candidate.unhedged - Decimal("20"),
        },
        is_feasible=lambda candidate: candidate.unhedged <= Decimal("20"),
        total_capital=lambda candidate: candidate.total_capital,
        expected_profit=lambda candidate: candidate.expected_profit,
        quantity=lambda candidate: candidate.quantity,
    )

    assert selection.feasible is True
    assert selection.candidate.quantity == Decimal("50")
