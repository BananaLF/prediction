from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from predmarket.actions import (
    Action,
    ActionKind,
    ActionPath,
    binary_overpriced_path,
    binary_underpriced_path,
)
from predmarket.domain import BookLevel, PathKind, Side
from predmarket.fees import FeeSchedule
from predmarket.orderbook import InsufficientDepth, OrderBook
from predmarket.simulator import SimulationResult, optimize_quantities, simulate_path


D = Decimal


def book(token: str, bids: tuple[tuple[str, str], ...], asks: tuple[tuple[str, str], ...],
         minimum: str = "1") -> OrderBook:
    return OrderBook(
        token,
        tuple(BookLevel(D(p), D(q)) for p, q in bids),
        tuple(BookLevel(D(p), D(q)) for p, q in asks),
        D(".01"),
        D(minimum),
        1,
        token + "-hash",
    )


def fee(rate: str = "0") -> FeeSchedule:
    return FeeSchedule(D(rate), 1, True, 1)


@pytest.fixture
def books() -> dict[str, OrderBook]:
    return {
        "yes": book("yes", ((".44", "50"),), ((".45", "10"), (".46", "40"))),
        "no": book("no", ((".47", "50"),), ((".48", "10"), (".49", "40"))),
    }


def test_underpriced_path_has_exact_profitable_cash_flow(books: dict[str, OrderBook]) -> None:
    path = binary_underpriced_path("yes", "no")
    result = simulate_path(path, D("10"), books, {"yes": fee(), "no": fee()})

    assert tuple(a.kind for a in path.actions) == (
        ActionKind.BUY, ActionKind.BUY, ActionKind.MERGE
    )
    assert result.maximum_capital_used == D("9.30")
    assert result.minimum_received == D("10")
    assert result.minimum_profit == D(".70")
    assert result.minimum_return == D(".70") / D("9.30")


def test_overpriced_path_has_exact_profitable_cash_flow() -> None:
    books = {
        "yes": book("yes", ((".54", "10"),), ((".55", "10"),)),
        "no": book("no", ((".53", "10"),), ((".54", "10"),)),
    }
    path = binary_overpriced_path("yes", "no")
    result = simulate_path(path, D("10"), books, {"yes": fee(), "no": fee()})

    assert tuple(a.kind for a in path.actions) == (
        ActionKind.SPLIT, ActionKind.SELL, ActionKind.SELL
    )
    assert result.maximum_capital_used == D("10")
    assert result.minimum_received == D("10.70")
    assert result.minimum_profit == D(".70")
    assert result.minimum_return == D(".07")


def test_fees_buffer_and_conversion_cost_reduce_exact_profit(
    books: dict[str, OrderBook],
) -> None:
    path = binary_underpriced_path("yes", "no")
    plain = simulate_path(path, D("10"), books, {"yes": fee(), "no": fee()})
    costly = simulate_path(
        path, D("10"), books, {"yes": fee(".10"), "no": fee(".10")},
        safety_buffer=D(".01"), conversion_cost=D(".02")
    )
    # Fees: 10*.1*(.45*.55 + .48*.52) = .4971; buffer: .093; conversion: .02.
    assert costly.minimum_profit == D(".70") - D(".49710") - D(".0930") - D(".02")
    assert costly.minimum_profit < plain.minimum_profit
    assert costly.minimum_received - costly.maximum_capital_used == costly.minimum_profit


def test_insufficient_depth_and_bad_book_or_fee_coverage(books: dict[str, OrderBook]) -> None:
    path = binary_underpriced_path("yes", "no")
    with pytest.raises(InsufficientDepth):
        simulate_path(path, D("100"), books, {"yes": fee(), "no": fee()})
    with pytest.raises(ValueError, match="books"):
        simulate_path(path, D("1"), {"yes": books["yes"]}, {"yes": fee(), "no": fee()})
    with pytest.raises(ValueError, match="fees"):
        simulate_path(path, D("1"), books, {"yes": fee()})
    with pytest.raises(ValueError, match="exactly"):
        simulate_path(path, D("1"), books, {"yes": fee(), "no": fee(), "x": fee()})


@pytest.mark.parametrize("value", [D("0"), D("-1"), D("NaN"), D("Infinity")])
def test_simulation_rejects_invalid_quantity_or_rates(
    value: Decimal, books: dict[str, OrderBook]
) -> None:
    with pytest.raises(ValueError):
        simulate_path(
            binary_underpriced_path("yes", "no"), value, books,
            {"yes": fee(), "no": fee()}
        )
    if not value.is_finite() or value < 0:
        with pytest.raises(ValueError):
            simulate_path(
                binary_underpriced_path("yes", "no"), D("1"), books,
                {"yes": fee(), "no": fee()}, safety_buffer=value
            )


def test_action_path_and_result_validation_and_immutability() -> None:
    with pytest.raises(TypeError):
        Action(ActionKind.BUY, "yes", Side.BUY, 1)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Action(ActionKind.BUY, None, Side.BUY)
    with pytest.raises(ValueError):
        Action(ActionKind.BUY, "yes", Side.SELL)
    with pytest.raises(ValueError):
        Action(ActionKind.MERGE, "yes")
    with pytest.raises(ValueError):
        ActionPath("", PathKind.IMMEDIATE_CONVERSION, (Action(ActionKind.MERGE),))
    with pytest.raises(TypeError):
        ActionPath("x", PathKind.IMMEDIATE_CONVERSION, [Action(ActionKind.MERGE)])  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        SimulationResult((), D("1"), D("1"), D("2"), D("2"), D("2"))
    with pytest.raises(ValueError, match="minimum_received"):
        SimulationResult(
            (Action(ActionKind.MERGE),), D("1"), D("1"), D("-1"), D("-2"), D("-2")
        )
    with pytest.raises(ValueError, match="quantity"):
        SimulationResult(
            (Action(ActionKind.MERGE),), D("0"), D("1"), D("0"), D("-1"), D("-1")
        )
    with pytest.raises(TypeError, match="quantity"):
        SimulationResult(
            (Action(ActionKind.MERGE),), 1, D("1"), D("0"), D("-1"), D("-1")
        )

    zero_received = SimulationResult(
        (Action(ActionKind.MERGE),), D("1"), D("1"), D("0"), D("-1"), D("-1")
    )
    assert zero_received.minimum_received == D("0")
    with pytest.raises(FrozenInstanceError):
        zero_received.minimum_profit = D("0")

    action = Action(ActionKind.MERGE)
    path = ActionPath("x", PathKind.IMMEDIATE_CONVERSION, (action,))
    with pytest.raises(FrozenInstanceError):
        action.units = D("2")
    with pytest.raises(FrozenInstanceError):
        path.actions = ()


def test_binary_helpers_reject_empty_or_equal_tokens() -> None:
    for helper in (binary_underpriced_path, binary_overpriced_path):
        with pytest.raises(ValueError):
            helper("", "no")
        with pytest.raises(ValueError):
            helper("same", "same")


@pytest.mark.parametrize(
    "actions",
    [
        (Action(ActionKind.BUY, "yes", Side.BUY), Action(ActionKind.MERGE)),
        (
            Action(ActionKind.BUY, "yes", Side.BUY, D("1")),
            Action(ActionKind.BUY, "no", Side.BUY, D("2")),
            Action(ActionKind.MERGE),
        ),
        (
            Action(ActionKind.MERGE),
            Action(ActionKind.BUY, "yes", Side.BUY),
            Action(ActionKind.BUY, "no", Side.BUY),
        ),
        (
            Action(ActionKind.BUY, "yes", Side.BUY),
            Action(ActionKind.BUY, "yes", Side.BUY),
            Action(ActionKind.MERGE),
        ),
        (
            Action(ActionKind.BUY, "yes", Side.BUY),
            Action(ActionKind.SELL, "no", Side.SELL),
            Action(ActionKind.MERGE),
        ),
        (
            Action(ActionKind.BUY, "yes", Side.BUY),
            Action(ActionKind.BUY, "no", Side.BUY),
            Action(ActionKind.MERGE),
            Action(ActionKind.MERGE),
        ),
        (
            Action(ActionKind.SELL, "yes", Side.SELL),
            Action(ActionKind.SELL, "no", Side.SELL),
            Action(ActionKind.SPLIT),
        ),
    ],
)
def test_simulator_rejects_nonconserving_binary_paths(
    actions: tuple[Action, ...], books: dict[str, OrderBook]
) -> None:
    path = ActionPath("malformed", PathKind.IMMEDIATE_CONVERSION, actions)
    token_ids = {action.token_id for action in actions if action.token_id is not None}
    selected_books = {token: books[token] for token in token_ids}
    selected_fees = {token: fee() for token in token_ids}

    with pytest.raises(ValueError, match="binary"):
        simulate_path(path, D("10"), selected_books, selected_fees)
    with pytest.raises(ValueError, match="binary"):
        optimize_quantities(
            path, selected_books, selected_fees, D("0"), D("0"), D("1000")
        )


def test_optimizer_uses_relevant_depth_breakpoints_and_bankroll(
    books: dict[str, OrderBook],
) -> None:
    path = binary_underpriced_path("yes", "no")
    results = optimize_quantities(
        path, books, {"yes": fee(), "no": fee()}, D("0"), D("0"), D("1000")
    )
    reduced = dict(books)
    reduced["yes"] = book("yes", ((".44", "5000"),), ((".45", "5"),), minimum="1")
    reduced_results = optimize_quantities(
        path, reduced, {"yes": fee(), "no": fee()}, D("0"), D("0"), D("1000")
    )

    assert results
    assert all(r.maximum_capital_used <= D("1000") for r in results)
    assert [r.maximum_capital_used for r in results] == sorted(
        r.maximum_capital_used for r in results
    )
    assert max(r.maximum_capital_used for r in reduced_results) <= max(
        r.maximum_capital_used for r in results
    )
    assert max(r.quantity for r in reduced_results) <= max(r.quantity for r in results)


def test_optimizer_reports_exact_scaled_relevant_side_breakpoints() -> None:
    scaled_books = {
        "yes": book(
            "yes", ((".44", "1000"),), ((".45", "4"), (".46", "6")), minimum="2"
        ),
        "no": book(
            "no", ((".47", "1000"),), ((".48", "6"), (".49", "4")), minimum="2"
        ),
    }
    path = ActionPath(
        "scaled-underpriced",
        PathKind.IMMEDIATE_CONVERSION,
        (
            Action(ActionKind.BUY, "yes", Side.BUY, D("2")),
            Action(ActionKind.BUY, "no", Side.BUY, D("2")),
            Action(ActionKind.MERGE, units=D("2")),
        ),
    )

    results = optimize_quantities(
        path, scaled_books, {"yes": fee(), "no": fee()},
        D("0"), D("0"), D("1000")
    )

    assert tuple(result.quantity for result in results) == (
        D("1"), D("2"), D("3"), D("5")
    )


def test_optimizer_propagates_unsupported_action_errors(
    books: dict[str, OrderBook],
) -> None:
    path = ActionPath(
        "unsupported",
        PathKind.IMMEDIATE_CONVERSION,
        (
            Action(ActionKind.BUY, "yes", Side.BUY),
            Action(ActionKind.REDEEM),
        ),
    )
    with pytest.raises(ValueError, match="unsupported"):
        optimize_quantities(
            path, {"yes": books["yes"]}, {"yes": fee()},
            D("0"), D("0"), D("1000")
        )


@pytest.mark.parametrize(
    "kind", [ActionKind.REDEEM, ActionKind.NEG_RISK_CONVERT]
)
def test_optimizer_rejects_action_only_unsupported_paths(kind: ActionKind) -> None:
    path = ActionPath(
        f"unsupported-{kind.value}",
        PathKind.IMMEDIATE_CONVERSION,
        (Action(kind),),
    )
    with pytest.raises(ValueError, match="unsupported"):
        optimize_quantities(path, {}, {}, D("0"), D("0"), D("1000"))


@pytest.mark.parametrize(
    "kind", [ActionKind.REDEEM, ActionKind.NEG_RISK_CONVERT]
)
def test_simulator_rejects_action_only_unsupported_paths(kind: ActionKind) -> None:
    path = ActionPath(
        f"unsupported-{kind.value}",
        PathKind.IMMEDIATE_CONVERSION,
        (Action(kind),),
    )
    with pytest.raises(ValueError, match="unsupported"):
        simulate_path(path, D("1"), {}, {})


@pytest.mark.parametrize("bankroll", [D("0"), D("-1"), D("NaN"), D("Infinity")])
def test_optimizer_rejects_invalid_bankroll(
    bankroll: Decimal, books: dict[str, OrderBook]
) -> None:
    with pytest.raises(ValueError):
        optimize_quantities(
            binary_underpriced_path("yes", "no"), books,
            {"yes": fee(), "no": fee()}, D("0"), D("0"), bankroll
        )
