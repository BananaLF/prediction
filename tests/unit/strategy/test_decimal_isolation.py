from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, ROUND_DOWN, ROUND_FLOOR, ROUND_UP, getcontext, localcontext

from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.domain.signal import StrategyType
from predmarket.strategy.binary import evaluate_binary
from predmarket.strategy.engine import StrategyEngine
from predmarket.strategy.implication import evaluate_implication
from predmarket.strategy.neg_risk import evaluate_neg_risk
from predmarket.strategy.optimizer import (
    DepthRequirement,
    breakpoint_quantities,
    optimize_candidates,
    optimize_quantity,
    walk_depth,
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

    def exercise():
        fill = walk_depth(book.asks, Decimal("1E+40"))
        points = breakpoint_quantities(
            (requirement,), minimum_quantity=Decimal("1E+39")
        )
        optimized = optimize_quantity(
            (requirement,),
            minimum_quantity=Decimal("1E+39"),
            bankroll=Decimal("3"),
            evaluate=lambda quantity: (quantity * Decimal("2E-40"), quantity * Decimal("1E-40")),
        )
        selected = optimize_candidates(
            (requirement,),
            minimum_quantity=Decimal("1E+39"),
            evaluate=lambda quantity: _Candidate(
                quantity, quantity * Decimal("2E-40"), quantity * Decimal("1E-40")
            ),
            constraint_margins=lambda candidate: {
                "capital": candidate.total_capital - Decimal("3")
            },
            is_feasible=lambda candidate: candidate.total_capital <= Decimal("3"),
            total_capital=lambda candidate: candidate.total_capital,
            expected_profit=lambda candidate: candidate.expected_profit,
            quantity=lambda candidate: candidate.quantity,
        )
        return fill, points, optimized, selected

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
