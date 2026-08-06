from decimal import Decimal

import pytest

from predmarket.config import StrategyConfig
from predmarket.domain.fees import FeeModel, FeeSchedule
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import StrategyContext, StrategyType


@pytest.fixture
def strategy_config_factory():
    def build(**overrides: object) -> StrategyConfig:
        values = {
            "bankroll": Decimal("1000"),
            "minimum_return_rate": Decimal("0"),
            "maximum_risk_rate": Decimal("1"),
            "maximum_unhedged_notional": Decimal("1000"),
            "safety_buffer_rate": Decimal("0"),
            "conversion_cost": Decimal("0"),
            "maximum_book_age_ms": 1000,
            "exchange_clock_skew_warning_ms": 100,
            "maximum_leg_skew_ms": 250,
        }
        values.update(overrides)
        return StrategyConfig(**values)  # type: ignore[arg-type]

    return build

@pytest.fixture
def market_factory():
    def build(
        market_id: str,
        *,
        event_id: str = "event-1",
        generation: str = "generation-1",
        minimum: str = "1",
        neg_risk: bool = False,
        neg_risk_position: int | None = None,
        neg_risk_complete: bool = False,
        status: MarketStatus = MarketStatus.ACTIVE,
    ) -> Market:
        return Market(
            id=market_id,
            event_id=event_id,
            condition_id=f"condition-{market_id}",
            question=f"Question {market_id}?",
            status=status,
            active=status is MarketStatus.ACTIVE,
            accepting_orders=status is MarketStatus.ACTIVE,
            enable_orderbook=True,
            sync_generation=generation,
            sync_generation_complete=True,
            neg_risk=neg_risk,
            neg_risk_outcome_position=neg_risk_position,
            neg_risk_member_complete=neg_risk_complete,
            tick_size=Decimal("0.01"),
            minimum_order_size=Decimal(minimum),
        )

    return build


@pytest.fixture
def token_factory():
    def build(
        token_id: str,
        market_id: str,
        outcome: str,
        position: int,
        *,
        generation: str = "generation-1",
        complete: bool = True,
    ) -> Token:
        return Token(
            id=token_id,
            market_id=market_id,
            outcome=outcome,
            position=position,
            sync_generation=generation,
            sync_generation_complete=complete,
        )

    return build


@pytest.fixture
def book_factory():
    def build(
        token_id: str,
        market_id: str,
        *,
        bid: str = "0.39",
        ask: str = "0.40",
        size: str = "10",
        exchange_timestamp: int = 1_000,
        received_timestamp: int = 1_000,
        generation: int = 1,
        minimum: str = "1",
    ) -> OrderBook:
        bids = () if bid == "" else (OrderBookLevel(Decimal(bid), Decimal(size)),)
        asks = () if ask == "" else (OrderBookLevel(Decimal(ask), Decimal(size)),)
        return OrderBook(
            market_id=market_id,
            token_id=token_id,
            bids=bids,
            asks=asks,
            subscription_generation=generation,
            book_hash=f"hash-{token_id}",
            exchange_timestamp=exchange_timestamp,
            received_timestamp=received_timestamp,
            tick_size=Decimal("0.01"),
            minimum_order_size=Decimal(minimum),
        )

    return build


@pytest.fixture
def fee_factory():
    def build(
        *,
        rate: str | None = None,
        updated_at: int = 1_000,
    ) -> FeeSchedule:
        if rate is None:
            return FeeSchedule(
                FeeModel.ZERO,
                False,
                "sdk",
                {},
                updated_at=updated_at,
            )
        return FeeSchedule(
            FeeModel.FLAT,
            True,
            "sdk",
            {"rate": Decimal(rate)},
            updated_at=updated_at,
        )

    return build


@pytest.fixture
def event_factory():
    def build(
        market_ids: tuple[str, ...],
        *,
        event_id: str = "event-1",
        generation: str = "generation-1",
        neg_risk: bool = True,
        neg_risk_id: str | None = "neg-risk-1",
        neg_risk_type: str | None = "STANDARD",
        complete: bool = True,
        conversion_supported: bool = True,
        metadata=None,
    ) -> Event:
        return Event(
            id=event_id,
            title="NegRisk event",
            status=MarketStatus.ACTIVE,
            market_ids=market_ids,
            sync_generation=generation,
            sync_generation_complete=True,
            neg_risk=neg_risk,
            neg_risk_id=neg_risk_id,
            neg_risk_type=neg_risk_type,
            neg_risk_complete=complete,
            neg_risk_conversion_supported=conversion_supported,
            neg_risk_metadata=metadata
            or {
                "mapping_version": "polymarket-client-0.3.0b1:v1",
                "enable_neg_risk": True,
                "neg_risk_augmented": False,
                "cumulative_markets": False,
                "neg_risk_fee_bips": "25",
            },
            neg_risk_synced_at=1_000,
        )

    return build


@pytest.fixture
def context_factory(fee_factory, strategy_config_factory):
    def build(
        strategy_type: StrategyType,
        *,
        markets,
        tokens,
        orderbooks,
        relation=None,
        events=(),
        changed_token_id: str | None = None,
        fees=None,
        evaluated_at: int = 1_000,
        fee_schedule_evaluated_at: int = 1_000,
        configuration=None,
        fee_max_age_seconds: int | None = 10,
        supported_neg_risk_types: tuple[str, ...] = ("STANDARD",),
    ) -> StrategyContext:
        token_tuple = tuple(tokens)
        if fees is None:
            fees = {token.id: fee_factory(updated_at=1_000) for token in token_tuple}
        return StrategyContext(
            strategy_type=strategy_type,
            changed_token_id=changed_token_id or token_tuple[0].id,
            markets=tuple(markets),
            tokens=token_tuple,
            approved_implication_relation=relation,
            orderbooks=tuple(orderbooks),
            fee_schedules=fees,
            evaluated_at=evaluated_at,
            fee_schedule_evaluated_at=fee_schedule_evaluated_at,
            configuration=configuration or strategy_config_factory(),
            events=tuple(events),
            fee_schedule_max_age_seconds=fee_max_age_seconds,
            supported_neg_risk_types=supported_neg_risk_types,
        )

    return build
