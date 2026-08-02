from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from decimal import Context, Decimal, ROUND_DOWN, ROUND_FLOOR, ROUND_UP, getcontext, localcontext

import pytest

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.domain.signal import DecisionReason, NotEvaluable, StrategyType
from predmarket.strategy.binary import evaluate_binary
from predmarket.strategy.engine import StrategyEngine
from predmarket.strategy.implication import evaluate_implication
from predmarket.strategy.neg_risk import evaluate_neg_risk
from predmarket.strategy.optimizer import (
    DepthRequirement,
    QuantityCandidate,
    breakpoint_quantities,
    candidate_quantities,
    constraint_root_quantities,
    select_candidates,
    walk_depth,
)
from predmarket.strategy.decimal_context import (
    StrategyNumericLimitError,
    isolated_decimal_context,
)
from predmarket.strategy.risk import (
    FailureScenario,
    OpenExposure,
    assess_failure_scenarios,
    immediate_close_value,
)


_AMBIENT_CONTEXTS = (
    Context(prec=1, rounding=ROUND_DOWN, Emin=-9, Emax=9),
    Context(prec=2, rounding=ROUND_UP, Emin=-9, Emax=9),
    Context(prec=28, rounding=ROUND_FLOOR, Emin=-9, Emax=9),
)


def _context_signature(context: Context) -> tuple[object, ...]:
    return (
        context.prec,
        context.rounding,
        context.Emin,
        context.Emax,
        context.capitals,
        context.clamp,
        tuple(sorted((signal.__name__, value) for signal, value in context.traps.items())),
        tuple(sorted((signal.__name__, value) for signal, value in context.flags.items())),
    )


def _assert_ambient_invariant(call):
    before = _context_signature(getcontext())
    expected = call()
    assert _context_signature(getcontext()) == before
    actual = []
    for ambient in _AMBIENT_CONTEXTS:
        with localcontext(ambient):
            actual.append(call())
    assert actual == [expected] * len(_AMBIENT_CONTEXTS)
    assert _context_signature(getcontext()) == before
    return expected


def test_strategy_entry_points_are_isolated_from_the_ambient_decimal_context(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    event_factory,
) -> None:
    market = market_factory("binary")
    tokens = (
        token_factory("binary-yes", market.id, "Yes", 0),
        token_factory("binary-no", market.id, "No", 1),
    )
    binary = context_factory(
        StrategyType.BINARY_UNDERPRICED,
        markets=(market,),
        tokens=tokens,
        orderbooks=(
            book_factory(tokens[0].id, market.id, ask="0.123456789", size="11.111111111"),
            book_factory(tokens[1].id, market.id, ask="0.234567891", size="11.111111111"),
        ),
    )
    _assert_ambient_invariant(lambda: evaluate_binary(binary))
    _assert_ambient_invariant(lambda: StrategyEngine().evaluate(binary))

    markets = (
        market_factory("market-a", event_id="event-a"),
        market_factory("market-b", event_id="event-b"),
    )
    relation = Relation(
        id="relation",
        market_a_id=markets[0].id,
        market_b_id=markets[1].id,
        status=RelationStatus.APPROVED,
        discovery_source=DiscoverySource.MANUAL,
        created_at=1,
        updated_at=1,
    )
    implication_tokens = (
        token_factory("yes-a", markets[0].id, "Yes", 0),
        token_factory("no-a", markets[0].id, "No", 1),
        token_factory("yes-b", markets[1].id, "Yes", 0),
        token_factory("no-b", markets[1].id, "No", 1),
    )
    implication = context_factory(
        StrategyType.LOGICAL_IMPLICATION,
        markets=markets,
        tokens=implication_tokens,
        orderbooks=(
            book_factory("no-a", "market-a", ask="0.123456789", size="11.111111111"),
            book_factory("yes-b", "market-b", ask="0.234567891", size="11.111111111"),
        ),
        relation=relation,
        changed_token_id="no-a",
    )
    _assert_ambient_invariant(lambda: evaluate_implication(implication))

    neg_markets = (
        market_factory(
            "neg-a", neg_risk=True, neg_risk_position=0, neg_risk_complete=True
        ),
        market_factory(
            "neg-b", neg_risk=True, neg_risk_position=1, neg_risk_complete=True
        ),
    )
    neg_tokens = tuple(
        token_factory(f"{outcome}-{market.id}", market.id, outcome, position)
        for market in neg_markets
        for outcome, position in (("Yes", 0), ("No", 1))
    )
    neg_risk = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=neg_markets,
        tokens=neg_tokens,
        orderbooks=tuple(
            book_factory(
                f"Yes-{market.id}", market.id, ask=ask, size="11.111111111"
            )
            for market, ask in zip(neg_markets, ("0.123456789", "0.234567891"))
        ),
        events=(event_factory(tuple(market.id for market in neg_markets)),),
        changed_token_id="Yes-neg-a",
    )
    _assert_ambient_invariant(lambda: evaluate_neg_risk(neg_risk))


def _large_book() -> OrderBook:
    return OrderBook(
        market_id="market-large",
        token_id="large",
        bids=(OrderBookLevel(Decimal("1E-40"), Decimal("2E+40")),),
        asks=(OrderBookLevel(Decimal("2E-40"), Decimal("2E+40")),),
        subscription_generation=1,
        book_hash="large-hash",
        exchange_timestamp=1,
        received_timestamp=1,
        tick_size=Decimal("1E-40"),
        minimum_order_size=Decimal("1E+39"),
    )


@dataclass(frozen=True)
class _Candidate:
    quantity: Decimal
    total_capital: Decimal
    expected_profit: Decimal


def test_optimizer_public_apis_derive_precision_and_exponent_limits_from_inputs() -> None:
    book = _large_book()
    requirement = DepthRequirement(book, "BUY")

    @isolated_decimal_context(operation_depth=24)
    def exercise():
        fill = walk_depth(book.asks, Decimal("1E+40"))
        points = breakpoint_quantities(
            (requirement,), minimum_quantity=Decimal("1E+39")
        )
        quantities = candidate_quantities(
            (requirement,),
            minimum_quantity=Decimal("1E+39"),
        )
        def closed(quantity: Decimal) -> QuantityCandidate[_Candidate]:
            item = _Candidate(
                quantity, quantity * Decimal("2E-40"), quantity * Decimal("1E-40")
            )
            margin = item.total_capital - Decimal("3")
            return QuantityCandidate(
                item,
                item.quantity,
                item.total_capital,
                item.expected_profit,
                (("capital", margin),),
                margin <= 0,
            )

        base = tuple(closed(value) for value in quantities)
        roots = constraint_root_quantities(base)
        selected = select_candidates(base + tuple(closed(value) for value in roots))
        return fill, points, quantities, roots, selected

    _assert_ambient_invariant(exercise)


def test_risk_public_apis_are_isolated_from_ambient_decimal_context() -> None:
    book = _large_book()
    from predmarket.domain.fees import FeeModel, FeeSchedule

    schedule = FeeSchedule(FeeModel.ZERO, False, "sdk", {}, updated_at=1)
    exposure = OpenExposure(
        "large", Decimal("1E+40"), Decimal("2"), book, schedule
    )
    scenario = FailureScenario("LARGE", Decimal("2"), (exposure,), Decimal("2"))

    def exercise():
        return (
            immediate_close_value(exposure, evaluated_at_ms=1, fee_max_age_seconds=1),
            assess_failure_scenarios(
                (scenario,), evaluated_at_ms=1, fee_max_age_seconds=1
            ),
        )

    _assert_ambient_invariant(exercise)


class _CustomLevelSequence(Sequence[OrderBookLevel]):
    def __init__(self, values: tuple[OrderBookLevel, ...]) -> None:
        self._values = values

    def __getitem__(self, index):
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)


class _InfiniteLevelIterator(Iterator[OrderBookLevel]):
    def __iter__(self) -> "_InfiniteLevelIterator":
        return self

    def __next__(self) -> OrderBookLevel:
        raise AssertionError("an iterator must be rejected before it is consumed")


def test_walk_depth_materializes_custom_sequence_before_deriving_precision() -> None:
    price = Decimal("0.12345678901234567890123456789012345678901234567890")
    levels = (OrderBookLevel(price, Decimal("1")),)

    expected = walk_depth(levels, Decimal("1"))
    actual = walk_depth(_CustomLevelSequence(levels), Decimal("1"))

    assert actual == expected
    assert actual.gross_amount == price


@pytest.mark.parametrize("invalid", ["levels", b"levels", _InfiniteLevelIterator()])
def test_walk_depth_rejects_non_sequence_or_text_inputs_without_consuming_them(
    invalid: object,
) -> None:
    with pytest.raises(ValueError, match="levels must be a bounded Sequence"):
        walk_depth(invalid, Decimal("1"))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "book_overrides",
    ({"ask": "1E-1000000"}, {"size": "1E+1000000"}),
)
def test_strategy_entry_fails_closed_on_extreme_decimal_exponents(
    book_overrides,
    context_factory,
    market_factory,
    token_factory,
    book_factory,
) -> None:
    market = market_factory("bounded")
    tokens = (
        token_factory("bounded-yes", market.id, "Yes", 0),
        token_factory("bounded-no", market.id, "No", 1),
    )
    context = context_factory(
        StrategyType.BINARY_UNDERPRICED,
        markets=(market,),
        tokens=tokens,
        orderbooks=(
            book_factory(tokens[0].id, market.id, **book_overrides),
            book_factory(tokens[1].id, market.id),
        ),
    )

    decision = evaluate_binary(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING
    assert decision.context["detail"] == "strategy_numeric_limit"


def test_low_level_decimal_policy_rejects_huge_coefficients_and_level_counts() -> None:
    huge_coefficient = Decimal("0." + "1" * 129)
    with pytest.raises(StrategyNumericLimitError):
        walk_depth(
            (OrderBookLevel(huge_coefficient, Decimal("1")),),
            Decimal("1"),
        )

    level = OrderBookLevel(Decimal("0.5"), Decimal("1"))
    with pytest.raises(StrategyNumericLimitError):
        walk_depth((level,) * 2_001, Decimal("1"))
