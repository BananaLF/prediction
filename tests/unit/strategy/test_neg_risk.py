from dataclasses import replace
from decimal import Decimal

import pytest

from predmarket.domain.signal import (
    Action,
    DecisionReason,
    NotEvaluable,
    OpportunityPresent,
    OpportunityAbsent,
    StrategyType,
)
from predmarket.strategy.neg_risk import evaluate_neg_risk


def _neg_risk_parts(market_factory, token_factory, book_factory):
    markets = (
        market_factory(
            "market-a",
            neg_risk=True,
            neg_risk_position=0,
            neg_risk_complete=True,
        ),
        market_factory(
            "market-b",
            neg_risk=True,
            neg_risk_position=1,
            neg_risk_complete=True,
        ),
    )
    tokens = (
        token_factory("yes-a", "market-a", "Yes", 0),
        token_factory("no-a", "market-a", "No", 1),
        token_factory("yes-b", "market-b", "Yes", 0),
        token_factory("no-b", "market-b", "No", 1),
    )
    books = (
        book_factory("yes-a", "market-a", ask="0.30", bid="0.29"),
        book_factory("yes-b", "market-b", ask="0.40", bid="0.39"),
    )
    return markets, tokens, books


def _context(context_factory, event_factory, market_factory, token_factory, book_factory, **overrides):
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    event = event_factory(("market-a", "market-b"))
    values = {
        "markets": markets,
        "tokens": tokens,
        "orderbooks": books,
        "events": (event,),
        "changed_token_id": "yes-a",
    }
    values.update(overrides)
    return context_factory(StrategyType.NEG_RISK_COMPLETE_SET, **values)


def test_neg_risk_complete_set_uses_authoritative_yes_members(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches guessing member semantics or reading logical relations.
    decision = evaluate_neg_risk(
        _context(context_factory, event_factory, market_factory, token_factory, book_factory)
    )

    assert isinstance(decision, OpportunityPresent)
    assert decision.calculation.total_capital == Decimal("7.025")
    assert decision.calculation.expected_profit == Decimal("2.975")
    assert [leg.action for leg in decision.legs] == [
        Action.BUY,
        Action.BUY,
        Action.NEG_RISK_CONVERT,
    ]
    assert [leg.token_id for leg in decision.legs[:2]] == ["yes-a", "yes-b"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mapping_version", "wrong-version"),
        ("enable_neg_risk", "true"),
        ("neg_risk_augmented", None),
        ("cumulative_markets", 0),
        ("neg_risk_fee_bips", None),
        ("neg_risk_fee_bips", "2.50"),
    ],
)
def test_neg_risk_requires_exact_authoritative_metadata_schema(
    context_factory,
    event_factory,
    market_factory,
    token_factory,
    book_factory,
    field,
    value,
) -> None:
    # Catches admitting a type without its fixed SDK mapping schema and fee proof.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    metadata = {
        "mapping_version": "polymarket-client-0.3.0b1:v1",
        "enable_neg_risk": True,
        "neg_risk_augmented": False,
        "cumulative_markets": False,
        "neg_risk_fee_bips": "25",
    }
    metadata[field] = value
    event = event_factory(("market-a", "market-b"), metadata=metadata)
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=markets,
        tokens=tokens,
        orderbooks=books,
        events=(event,),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING


def test_neg_risk_conversion_fee_can_eliminate_profit(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches ignoring authoritative neg_risk_fee_bips in economics.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    metadata = {
        "mapping_version": "polymarket-client-0.3.0b1:v1",
        "enable_neg_risk": True,
        "neg_risk_augmented": False,
        "cumulative_markets": False,
        "neg_risk_fee_bips": "4000",
    }
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=markets,
        tokens=tokens,
        orderbooks=books,
        events=(event_factory(("market-a", "market-b"), metadata=metadata),),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, OpportunityAbsent)
    assert decision.reason_code is DecisionReason.PROFIT_BELOW_THRESHOLD
    assert decision.calculation.expected_profit < 0


def test_neg_risk_supported_redeem_type_uses_redeem_action(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches hard-coding NEG_RISK_CONVERT instead of the type-bound action.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    metadata = {
        "mapping_version": "polymarket-client-0.3.0b1:v1",
        "enable_neg_risk": False,
        "neg_risk_augmented": False,
        "cumulative_markets": False,
        "neg_risk_fee_bips": "0",
    }
    event = event_factory(
        ("market-a", "market-b"),
        neg_risk_type="STANDARD_REDEEM",
        metadata=metadata,
    )
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=markets,
        tokens=tokens,
        orderbooks=books,
        events=(event,),
        supported_neg_risk_types=("STANDARD_REDEEM",),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, OpportunityPresent)
    assert decision.legs[-1].action is Action.REDEEM


@pytest.mark.parametrize(
    "event_change",
    [
        {"neg_risk": False},
        {"neg_risk_id": None},
        {"neg_risk_complete": False},
        {"neg_risk_conversion_supported": False},
        {"neg_risk_type": "UNSUPPORTED"},
    ],
)
def test_neg_risk_requires_every_event_eligibility_predicate(
    context_factory,
    event_factory,
    market_factory,
    token_factory,
    book_factory,
    event_change,
) -> None:
    # Catches weakening one authoritative event proof into a heuristic.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    event = replace(event_factory(("market-a", "market-b")), **event_change)
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=markets,
        tokens=tokens,
        orderbooks=books,
        events=(event,),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING


def test_neg_risk_requires_exact_members_positions_and_complete_mapping(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches accepting a missing, duplicated, or incomplete member.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    broken_markets = (
        markets[0],
        replace(markets[1], neg_risk_outcome_position=2, neg_risk_member_complete=False),
    )
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=broken_markets,
        tokens=tokens,
        orderbooks=books,
        events=(event_factory(("market-a", "market-b")),),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING


def test_neg_risk_requires_unique_condition_and_token_position_mappings(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches calling duplicated conditions or outcome positions a complete set.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    duplicate_condition = (
        markets[0],
        replace(markets[1], condition_id=markets[0].condition_id),
    )
    duplicate_token_position = (
        tokens[0],
        replace(tokens[1], position=0),
        *tokens[2:],
    )
    event = event_factory(("market-a", "market-b"))

    condition_decision = evaluate_neg_risk(
        context_factory(
            StrategyType.NEG_RISK_COMPLETE_SET,
            markets=duplicate_condition,
            tokens=tokens,
            orderbooks=books,
            events=(event,),
        )
    )
    position_decision = evaluate_neg_risk(
        context_factory(
            StrategyType.NEG_RISK_COMPLETE_SET,
            markets=markets,
            tokens=duplicate_token_position,
            orderbooks=books,
            events=(event,),
        )
    )

    assert isinstance(condition_decision, NotEvaluable)
    assert condition_decision.reason_code is DecisionReason.INPUT_METADATA_MISSING
    assert isinstance(position_decision, NotEvaluable)
    assert position_decision.reason_code is DecisionReason.INPUT_METADATA_MISSING


def test_neg_risk_requires_exact_event_member_set(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches evaluating a subset of the authoritative complete set.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=markets,
        tokens=tokens,
        orderbooks=books,
        events=(event_factory(("market-a", "market-b", "market-c")),),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.INPUT_METADATA_MISSING


def test_neg_risk_requires_one_complete_sync_generation(
    context_factory, event_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches combining authoritative fields from different catalog epochs.
    markets, tokens, books = _neg_risk_parts(market_factory, token_factory, book_factory)
    stale_tokens = (replace(tokens[0], sync_generation="generation-old"), *tokens[1:])
    context = context_factory(
        StrategyType.NEG_RISK_COMPLETE_SET,
        markets=markets,
        tokens=stale_tokens,
        orderbooks=books,
        events=(event_factory(("market-a", "market-b")),),
    )

    decision = evaluate_neg_risk(context)

    assert isinstance(decision, NotEvaluable)
    assert decision.reason_code is DecisionReason.SYNC_GENERATION_INCOMPLETE
