from dataclasses import replace

from predmarket.domain.signal import DecisionReason, NotEvaluable, OpportunityPresent, StrategyType
from predmarket.strategy.engine import StrategyEngine


def test_engine_routes_each_context_only_for_an_affected_token(
    context_factory, market_factory, token_factory, book_factory
) -> None:
    # Catches evaluating unrelated token changes and emitting duplicate signals.
    market = market_factory("market-1")
    tokens = (
        token_factory("yes", market.id, "Yes", 0),
        token_factory("no", market.id, "No", 1),
    )
    books = (
        book_factory("yes", market.id),
        book_factory("no", market.id),
    )
    context = context_factory(
        StrategyType.BINARY_UNDERPRICED,
        markets=(market,),
        tokens=tokens,
        orderbooks=books,
        changed_token_id="yes",
    )
    engine = StrategyEngine()

    affected = engine.evaluate(context)
    unrelated = engine.evaluate(replace(context, changed_token_id="unrelated"))

    assert isinstance(affected, OpportunityPresent)
    assert isinstance(unrelated, NotEvaluable)
    assert unrelated.reason_code is DecisionReason.INPUT_METADATA_MISSING
