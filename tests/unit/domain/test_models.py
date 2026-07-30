from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal
from types import MappingProxyType

import pytest

from predmarket.config import StrategyConfig
from predmarket.domain.fees import FeeSchedule
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.domain.signal import (
    Action,
    DecisionReason,
    ExecutionMode,
    NotEvaluable,
    OpportunityAbsent,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
    StrategyContext,
    StrategyType,
)


def _market(*, market_id: str = "market-1") -> Market:
    return Market(
        id=market_id,
        event_id="event-1",
        condition_id=f"condition-{market_id}",
        question="Will it happen?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def _token(*, token_id: str = "token-1", market_id: str = "market-1") -> Token:
    return Token(
        id=token_id,
        market_id=market_id,
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )


def _book(*, token_id: str = "token-1", market_id: str = "market-1") -> OrderBook:
    return OrderBook(
        market_id=market_id,
        token_id=token_id,
        bids=(
            OrderBookLevel(price=Decimal("0.4"), size=Decimal("3")),
            OrderBookLevel(price=Decimal("0.6"), size=Decimal("1")),
        ),
        asks=(
            OrderBookLevel(price=Decimal("0.7"), size=Decimal("4")),
            OrderBookLevel(price=Decimal("0.65"), size=Decimal("2")),
        ),
        subscription_generation=1,
        book_hash="book-hash",
        exchange_timestamp=1_000,
        received_timestamp=1_001,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


def _calculation() -> OpportunityCalculation:
    return OpportunityCalculation(
        quantity=Decimal("2"),
        total_capital=Decimal("1.6"),
        expected_profit=Decimal("0.4"),
        return_rate=Decimal("0.25"),
        worst_case_loss=Decimal("0.8"),
        risk_rate=Decimal("0.5"),
        unhedged_notional=Decimal("0.6"),
        risk_flags=("PARTIAL_FILL",),
        details={"minimum_proceeds": "2"},
    )


def _leg() -> SignalLeg:
    return SignalLeg(
        position=0,
        market_id="market-1",
        token_id="token-1",
        action=Action.BUY,
        quantity=Decimal("2"),
        average_price=Decimal("0.4"),
        worst_price=Decimal("0.4"),
        gross_amount=Decimal("0.8"),
        fee_amount=Decimal("0"),
    )


def _strategy_config() -> StrategyConfig:
    return StrategyConfig(
        bankroll=Decimal("1000"),
        minimum_return_rate=Decimal("0.01"),
        maximum_risk_rate=Decimal("0.5"),
        maximum_unhedged_notional=Decimal("10"),
        safety_buffer_rate=Decimal("0.0025"),
        conversion_cost=Decimal("0"),
        maximum_book_age_ms=2_000,
        maximum_leg_skew_ms=500,
    )


def test_event_canonicalizes_market_ids_by_utf8_bytes() -> None:
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("β", "10", "2", "10"),
        sync_generation="sync-1",
        sync_generation_complete=True,
    )

    assert event.market_ids == ("10", "2", "β")
    with pytest.raises(FrozenInstanceError):
        event.title = "changed"  # type: ignore[misc]


def test_event_requires_at_least_one_market_id() -> None:
    with pytest.raises(ValueError, match="market_ids"):
        Event(
            id="event-1",
            title="Event",
            status=MarketStatus.ACTIVE,
            market_ids=(),
            sync_generation="sync-1",
            sync_generation_complete=True,
        )


def test_event_rejects_string_instead_of_market_id_collection() -> None:
    with pytest.raises(ValueError, match="market_ids"):
        Event(
            id="event-1",
            title="Event",
            status=MarketStatus.ACTIVE,
            market_ids="market-1",  # type: ignore[arg-type]
            sync_generation="sync-1",
            sync_generation_complete=True,
        )


@pytest.mark.parametrize(
    ("price", "size"),
    [
        (Decimal("0"), Decimal("1")),
        (Decimal("1"), Decimal("1")),
        (Decimal("-0.1"), Decimal("1")),
        (Decimal("1.1"), Decimal("1")),
        (Decimal("0.5"), Decimal("0")),
        (Decimal("0.5"), Decimal("-1")),
    ],
)
def test_orderbook_level_rejects_out_of_range_price_and_nonpositive_size(
    price: Decimal, size: Decimal
) -> None:
    with pytest.raises(ValueError):
        OrderBookLevel(price=price, size=size)


def test_orderbook_sorts_and_freezes_bids_and_asks() -> None:
    book = _book()

    assert [level.price for level in book.bids] == [Decimal("0.6"), Decimal("0.4")]
    assert [level.price for level in book.asks] == [Decimal("0.65"), Decimal("0.7")]
    assert isinstance(book.bids, tuple)
    with pytest.raises(FrozenInstanceError):
        book.book_hash = "changed"  # type: ignore[misc]


def test_relation_only_allows_forward_state_transitions() -> None:
    relation = Relation(
        id="relation-1",
        market_a_id="market-a",
        market_b_id="market-b",
        status=RelationStatus.NO_LLM_APPROVE,
        discovery_source=DiscoverySource.RULE,
        created_at=1,
        updated_at=1,
    )

    analyzed = relation.transition_to(RelationStatus.LLM_APPROVE, updated_at=2)
    approved = analyzed.transition_to(RelationStatus.APPROVED, updated_at=3)

    assert relation.status is RelationStatus.NO_LLM_APPROVE
    assert analyzed.status is RelationStatus.LLM_APPROVE
    assert approved.status is RelationStatus.APPROVED
    with pytest.raises(ValueError):
        relation.transition_to(RelationStatus.APPROVED, updated_at=2)
    with pytest.raises(ValueError):
        approved.transition_to(RelationStatus.LLM_APPROVE, updated_at=4)


def test_relation_rejects_self_implication() -> None:
    with pytest.raises(ValueError, match="different"):
        Relation(
            id="relation-1",
            market_a_id="same",
            market_b_id="same",
            status=RelationStatus.NO_LLM_APPROVE,
            discovery_source=DiscoverySource.MANUAL,
            created_at=1,
            updated_at=1,
        )


def test_present_and_absent_decisions_require_current_calculation_and_evidence() -> None:
    calculation = _calculation()
    leg = _leg()
    book = _book()

    present = OpportunityPresent(calculation=calculation, legs=(leg,), evidence=(book,))
    absent = OpportunityAbsent(
        reason_code=DecisionReason.PROFIT_BELOW_THRESHOLD,
        calculation=calculation,
        legs=(leg,),
        evidence=(book,),
    )

    assert present.calculation is calculation
    assert absent.evidence == (book,)
    with pytest.raises(ValueError, match="legs"):
        OpportunityPresent(calculation=calculation, legs=(), evidence=(book,))
    with pytest.raises(ValueError, match="evidence"):
        OpportunityAbsent(
            reason_code=DecisionReason.INSUFFICIENT_DEPTH,
            calculation=calculation,
            legs=(leg,),
            evidence=(),
        )


def test_not_evaluable_requires_stable_reason_and_context_without_economics() -> None:
    decision = NotEvaluable(
        reason_code=DecisionReason.ORDERBOOK_INVALID,
        context={"token_id": "token-1", "subscription_generation": 1},
    )

    assert decision.reason_code is DecisionReason.ORDERBOOK_INVALID
    assert isinstance(decision.context, MappingProxyType)
    with pytest.raises(ValueError, match="context"):
        NotEvaluable(reason_code=DecisionReason.FEE_SCHEDULE_UNKNOWN, context={})
    with pytest.raises(ValueError, match="not valid"):
        NotEvaluable(reason_code=DecisionReason.PROFIT_BELOW_THRESHOLD, context={"x": 1})


def test_strategy_context_is_deeply_immutable_and_canonical() -> None:
    market_a = _market(market_id="market-a")
    market_b = _market(market_id="market-b")
    token_a = _token(token_id="token-a", market_id="market-a")
    token_b = _token(token_id="token-b", market_id="market-b")
    book_a = _book(token_id="token-a", market_id="market-a")
    schedule = FeeSchedule.from_json(
        {
            "model": "ZERO",
            "enabled": True,
            "source": "clob",
            "parameters": {},
            "updated_at": 100,
        }
    )

    context = StrategyContext(
        strategy_type=StrategyType.BINARY_UNDERPRICED,
        changed_token_id="token-a",
        markets=(market_b, market_a),
        tokens=(token_b, token_a),
        approved_implication_relation=None,
        orderbooks=(book_a,),
        fee_schedules={"token-a": schedule},
        evaluated_at=200,
        configuration=_strategy_config(),
    )

    assert [market.id for market in context.markets] == ["market-a", "market-b"]
    assert [token.id for token in context.tokens] == ["token-a", "token-b"]
    assert isinstance(context.fee_schedules, MappingProxyType)
    with pytest.raises(TypeError):
        context.fee_schedules["token-b"] = schedule  # type: ignore[index]


def test_signal_leg_enforces_trade_and_conversion_payloads() -> None:
    with pytest.raises(ValueError, match="token_id"):
        SignalLeg(
            position=0,
            market_id="market-1",
            token_id=None,
            action=Action.BUY,
            quantity=Decimal("1"),
            average_price=Decimal("0.4"),
            worst_price=Decimal("0.4"),
            gross_amount=Decimal("0.4"),
            fee_amount=Decimal("0"),
        )

    conversion = SignalLeg(
        position=1,
        market_id="market-1",
        token_id=None,
        action=Action.MERGE,
        quantity=Decimal("1"),
        average_price=None,
        worst_price=None,
        gross_amount=Decimal("1"),
        fee_amount=Decimal("0"),
    )
    assert conversion.action is Action.MERGE


def test_calculation_enforces_exact_return_and_risk_formulas() -> None:
    with pytest.raises(ValueError, match="return_rate"):
        OpportunityCalculation(
            quantity=Decimal("2"),
            total_capital=Decimal("1.6"),
            expected_profit=Decimal("0.4"),
            return_rate=Decimal("0.24"),
            worst_case_loss=Decimal("0.8"),
            risk_rate=Decimal("0.5"),
            unhedged_notional=Decimal("0.6"),
        )

    with pytest.raises(ValueError, match="risk_rate"):
        OpportunityCalculation(
            quantity=Decimal("2"),
            total_capital=Decimal("1.6"),
            expected_profit=Decimal("0.4"),
            return_rate=Decimal("0.25"),
            worst_case_loss=Decimal("0.8"),
            risk_rate=Decimal("0.4"),
            unhedged_notional=Decimal("0.6"),
        )


def test_execution_mode_enum_contains_only_approved_modes() -> None:
    assert {mode.value for mode in ExecutionMode} == {
        "IMMEDIATE_CONVERSION",
        "HOLD_TO_RESOLUTION",
    }
