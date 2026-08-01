from decimal import Decimal
from dataclasses import dataclass

import pytest

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.strategy.optimizer import (
    DepthRequirement,
    InsufficientDepth,
    QuantityCandidate,
    breakpoint_quantities,
    candidate_quantities,
    constraint_root_quantities,
    select_candidates,
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


def test_candidate_quantity_generation_returns_empty_for_minimum_depth_failure() -> None:
    # Catches manufacturing an undersized or partially filled opportunity.
    book = _book("buy", asks=(("0.50", "2"),), minimum="3")

    quantities = candidate_quantities(
        (DepthRequirement(book, "BUY"),),
        minimum_quantity=Decimal("3"),
        default_quantity=Decimal("1"),
    )

    assert quantities == ()


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

    def closed(candidate: Candidate) -> QuantityCandidate[Candidate]:
        return QuantityCandidate(
            evaluation=candidate,
            quantity=candidate.quantity,
            total_capital=candidate.total_capital,
            expected_profit=candidate.expected_profit,
            constraint_margins=(
                ("bankroll", candidate.total_capital - Decimal("1000")),
                ("unhedged", candidate.unhedged - Decimal("20")),
            ),
            feasible=candidate.unhedged <= Decimal("20"),
        )

    base = tuple(closed(evaluate(value)) for value in (Decimal("1"), Decimal("100")))
    roots = constraint_root_quantities(base)
    selection = select_candidates(base + tuple(closed(evaluate(value)) for value in roots))

    assert selection.feasible is True
    assert selection.candidate.quantity == Decimal("50")


@pytest.mark.parametrize(
    ("constraint_name", "safe_below"),
    [("bankroll", True), ("risk_rate", True), ("return", False)],
)
def test_candidate_optimizer_evaluates_both_adjacent_decimals_at_rounded_roots(
    constraint_name: str,
    safe_below: bool,
) -> None:
    # Catches retaining only the rounded root when the safe side is one ulp away.
    book = _book("buy", asks=(("0.40", "100"),))

    @dataclass(frozen=True)
    class Candidate:
        quantity: Decimal
        total_capital: Decimal
        expected_profit: Decimal

    def evaluate(quantity: Decimal) -> Candidate:
        objective = quantity if safe_below else -quantity
        return Candidate(quantity, quantity, objective)

    def margin(candidate: Candidate) -> Decimal:
        raw = candidate.quantity * Decimal("0.124") - Decimal("7")
        return raw if safe_below else -raw

    def closed(candidate: Candidate) -> QuantityCandidate[Candidate]:
        return QuantityCandidate(
            evaluation=candidate,
            quantity=candidate.quantity,
            total_capital=candidate.total_capital,
            expected_profit=candidate.expected_profit,
            constraint_margins=((constraint_name, margin(candidate)),),
            feasible=margin(candidate) <= 0,
        )

    base = tuple(closed(evaluate(value)) for value in (Decimal("1"), Decimal("100")))
    roots = constraint_root_quantities(base)
    selection = select_candidates(base + tuple(closed(evaluate(value)) for value in roots))

    assert selection is not None
    assert selection.candidate.quantity > Decimal("56")
    assert selection.candidate.quantity < Decimal("57")
    assert margin(selection.candidate) <= 0
    assert any(margin(candidate) > 0 for candidate in selection.candidates)
