from decimal import Decimal

from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.domain.signal import (
    Action,
    DecisionReason,
    NotEvaluable,
    OpportunityPresent,
    StrategyType,
)
from predmarket.strategy.implication import evaluate_implication


def _relation(status=RelationStatus.APPROVED):
    return Relation(
        id="relation-1",
        market_a_id="market-a",
        market_b_id="market-b",
        status=status,
        discovery_source=DiscoverySource.MANUAL,
        created_at=1,
        updated_at=2,
    )


def _context(
    context_factory,
    market_factory,
    token_factory,
    book_factory,
    *,
    relation=None,
):
    market_a = market_factory("market-a", event_id="event-a")
    market_b = market_factory("market-b", event_id="event-b")
    yes_a = token_factory("yes-a", market_a.id, "Yes", 0)
    no_a = token_factory("no-a", market_a.id, "No", 1)
    yes_b = token_factory("yes-b", market_b.id, "Yes", 0)
    no_b = token_factory("no-b", market_b.id, "No", 1)
    books = (
        book_factory(no_a.id, market_a.id, ask="0.30", bid="0.29"),
        book_factory(yes_b.id, market_b.id, ask="0.40", bid="0.39"),
    )
    return context_factory(
        StrategyType.LOGICAL_IMPLICATION,
        markets=(market_a, market_b),
        tokens=(yes_a, no_a, yes_b, no_b),
        orderbooks=books,
        relation=relation,
        changed_token_id="no-a",
    )


def test_implication_uses_only_allowed_a_implies_b_states(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches using an invalid A=true/B=false state or the wrong payout floor.
    decision = evaluate_implication(
        _context(
            context_factory,
            market_factory,
            token_factory,
            book_factory,
            relation=_relation(),
        )
    )

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.quantity == Decimal("10")
    assert decision.calculation.expected_profit == Decimal("3.00")
    assert decision.calculation.details["execution_mode"] == "HOLD_TO_RESOLUTION"
    assert decision.calculation.details["payout_states"] == {
        "A_FALSE_B_FALSE": "10",
        "A_FALSE_B_TRUE": "20",
        "A_TRUE_B_TRUE": "10",
    }
    assert [leg.action for leg in decision.legs] == [Action.BUY, Action.BUY, Action.REDEEM]


def test_implication_requires_an_approved_relation(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches running logical arbitrage without the manual approval gate.
    decision = evaluate_implication(
        _context(
            context_factory,
            market_factory,
            token_factory,
            book_factory,
            relation=None,
        )
    )

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.RELATION_NOT_APPROVED


def test_implication_rejects_relation_market_binding_mismatch(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches applying an approved proof to different markets.
    wrong = Relation(
        id="wrong",
        market_a_id="market-a",
        market_b_id="other-market",
        status=RelationStatus.APPROVED,
        discovery_source=DiscoverySource.MANUAL,
        created_at=1,
        updated_at=2,
    )
    decision = evaluate_implication(
        _context(
            context_factory,
            market_factory,
            token_factory,
            book_factory,
            relation=wrong,
        )
    )

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING
