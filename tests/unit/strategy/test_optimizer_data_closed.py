from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Context, Decimal, ROUND_DOWN, localcontext

import pytest

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.strategy.decimal_context import StrategyNumericLimitError
from predmarket.strategy.optimizer import (
    DepthRequirement,
    QuantityCandidate,
    constraint_root_quantities,
    select_candidates,
)


@dataclass
class _Evaluation:
    name: str
    quantity: Decimal
    total_capital: Decimal
    expected_profit: Decimal


_GLOBAL_DELTA = Decimal("1E-300")


def _candidate(
    name: str,
    *,
    quantity: str,
    capital: str,
    profit: str,
    margin: str,
    feasible: bool,
) -> QuantityCandidate[_Evaluation]:
    evaluation = _Evaluation(
        name,
        Decimal(quantity),
        Decimal(capital),
        Decimal(profit),
    )
    return QuantityCandidate(
        evaluation=evaluation,
        quantity=evaluation.quantity,
        total_capital=evaluation.total_capital,
        expected_profit=evaluation.expected_profit,
        constraint_margins=(("hard", Decimal(margin)),),
        feasible=feasible,
    )


@pytest.mark.parametrize(
    ("margin", "feasible"),
    (("1", True), ("-1", False)),
)
def test_quantity_candidate_rejects_forged_feasibility(
    margin: str,
    feasible: bool,
) -> None:
    # Catches trusting a caller-provided feasibility flag over hard margins.
    evaluation = _Evaluation(
        "forged",
        Decimal("1"),
        Decimal("1"),
        Decimal("1"),
    )

    with pytest.raises(ValueError, match="feasible"):
        QuantityCandidate(
            evaluation=evaluation,
            quantity=evaluation.quantity,
            total_capital=evaluation.total_capital,
            expected_profit=evaluation.expected_profit,
            constraint_margins=(("hard", Decimal(margin)),),
            feasible=feasible,
        )


@pytest.mark.parametrize(
    ("field_name", "forged_value"),
    (("total_capital", Decimal("2")), ("expected_profit", Decimal("2"))),
)
def test_quantity_candidate_rejects_economics_that_disagree_with_evaluation(
    field_name: str,
    forged_value: Decimal,
) -> None:
    # Catches selecting by wrapper economics while returning a different payload.
    evaluation = _Evaluation(
        "canonical",
        Decimal("1"),
        Decimal("1"),
        Decimal("1"),
    )
    values = {
        "quantity": evaluation.quantity,
        "total_capital": evaluation.total_capital,
        "expected_profit": evaluation.expected_profit,
    }
    values[field_name] = forged_value

    with pytest.raises(ValueError, match=field_name):
        QuantityCandidate(
            evaluation=evaluation,
            constraint_margins=(("hard", Decimal("-1")),),
            feasible=True,
            **values,
        )


def test_selector_revalidates_evaluation_economics_before_selection() -> None:
    # Catches a mutable evaluation diverging after candidate construction.
    candidate = _candidate(
        "changed",
        quantity="1",
        capital="1",
        profit="1",
        margin="-1",
        feasible=True,
    )
    candidate.evaluation.expected_profit = Decimal("2")

    with pytest.raises(ValueError, match="expected_profit"):
        select_candidates((candidate,))


def test_selector_uses_only_closed_candidate_data_under_any_ambient_context() -> None:
    # Catches reintroducing a callback whose undeclared global Decimal is rounded away.
    candidates = (
        _candidate("small", quantity="1", capital="1", profit="1", margin="-1", feasible=True),
        _candidate(
            "large",
            quantity="100",
            capital="100",
            profit="1",
            margin="-1",
            feasible=True,
        ),
    )

    expected = select_candidates(candidates)
    with localcontext(Context(prec=1, rounding=ROUND_DOWN, Emin=-9, Emax=9)):
        actual = select_candidates(candidates)

    assert _GLOBAL_DELTA > 0
    assert expected == actual
    assert actual is not None
    assert actual.candidate.name == "small"


def test_root_generation_is_data_only_and_preserves_safe_adjacent_quantity() -> None:
    # Catches dropping the safe-side adjacent Decimal at a non-terminating hard root.
    candidates = (
        _candidate("lower", quantity="1", capital="1", profit="1", margin="-6.876", feasible=True),
        _candidate("upper", quantity="100", capital="100", profit="100", margin="5.4", feasible=False),
    )

    roots = constraint_root_quantities(candidates)

    assert len(roots) == 2
    assert all(Decimal("56") < quantity < Decimal("57") for quantity in roots)
    assert roots[0] < roots[1]


@pytest.mark.parametrize(
    ("power", "coefficient_digits", "limit_name"),
    ((300, 301, "coefficient"), (425, 426, "scale")),
)
def test_terminating_root_preflights_decimal_policy_before_materialization(
    power: int,
    coefficient_digits: int,
    limit_name: str,
) -> None:
    # The exact root is 1 + 1 / 2**power and every input remains policy-valid.
    upper_margin = 2**power - 1
    exact_coefficient = (2**power + 1) * 5**power
    assert len(str(upper_margin)) <= 128
    assert len(str(exact_coefficient)) == coefficient_digits
    candidates = (
        _candidate(
            "lower",
            quantity="1",
            capital="1",
            profit="1",
            margin="-1",
            feasible=True,
        ),
        _candidate(
            "upper",
            quantity="2",
            capital="2",
            profit="2",
            margin=str(upper_margin),
            feasible=False,
        ),
    )

    with localcontext(Context(prec=1, rounding=ROUND_DOWN, Emin=-9, Emax=9)) as ambient:
        before = (
            ambient.prec,
            ambient.rounding,
            ambient.Emin,
            ambient.Emax,
            ambient.flags.copy(),
            ambient.traps.copy(),
        )
        with pytest.raises(
            StrategyNumericLimitError,
            match=f"root {limit_name}",
        ):
            constraint_root_quantities(candidates)
        after = (
            ambient.prec,
            ambient.rounding,
            ambient.Emin,
            ambient.Emax,
            ambient.flags.copy(),
            ambient.traps.copy(),
        )

    assert after == before


def _oversized_book() -> OrderBook:
    levels = tuple(
        OrderBookLevel(Decimal(index) / Decimal("10000"), Decimal("1"))
        for index in range(1, 2_002)
    )
    return OrderBook(
        market_id="market-large",
        token_id="large",
        bids=(),
        asks=levels,
        subscription_generation=1,
        book_hash="hash",
        exchange_timestamp=1,
        received_timestamp=1,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def test_data_optimizer_preflights_nested_oversized_books_before_selection() -> None:
    # Catches selector/context scanning a payload's 2001 levels before the shared book limit.
    @dataclass(frozen=True)
    class Payload:
        book: OrderBook
        quantity: Decimal
        total_capital: Decimal
        expected_profit: Decimal

    candidate = QuantityCandidate(
        evaluation=Payload(
            _oversized_book(),
            Decimal("1"),
            Decimal("1"),
            Decimal("1"),
        ),
        quantity=Decimal("1"),
        total_capital=Decimal("1"),
        expected_profit=Decimal("1"),
        constraint_margins=(("hard", Decimal("-1")),),
        feasible=True,
    )

    with pytest.raises(StrategyNumericLimitError):
        select_candidates((candidate,))


class _CountingCandidates(Sequence[QuantityCandidate[_Evaluation]]):
    def __init__(self, width: int, item: QuantityCandidate[_Evaluation]) -> None:
        self.width = width
        self.item = item
        self.reads = 0

    def __len__(self) -> int:
        return self.width

    def __getitem__(self, index: int) -> QuantityCandidate[_Evaluation]:
        self.reads += 1
        if index >= self.width:
            raise IndexError
        return self.item


def test_candidate_materialization_never_consumes_past_the_numeric_limit() -> None:
    candidate = _candidate("one", quantity="1", capital="1", profit="1", margin="-1", feasible=True)
    values = _CountingCandidates(20_001, candidate)

    with pytest.raises(StrategyNumericLimitError):
        select_candidates(values)

    assert values.reads <= 20_001
