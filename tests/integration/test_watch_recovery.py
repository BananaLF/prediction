from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import (
    Action,
    ExecutionMode,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
)
from predmarket.persistence.repositories import CatalogSnapshot
from predmarket.polymarket.gateway import MarketStreamInvalidated
from predmarket.watch.cache import CacheState
from predmarket.watch.task import EvaluationTarget, WatchTask


def _book(token_id: str, generation: int) -> OrderBook:
    return OrderBook(
        market_id="market-1",
        token_id=token_id,
        bids=(OrderBookLevel(Decimal("0.40"), Decimal("3")),),
        asks=(OrderBookLevel(Decimal("0.50"), Decimal("4")),),
        subscription_generation=generation,
        book_hash=f"hash-{generation}-{token_id}",
        exchange_timestamp=100 + generation,
        received_timestamp=101 + generation,
        tick_size=Decimal("0.01"),
        minimum_order_size=Decimal("1"),
    )


class _Subscription:
    def __init__(self, generation: int) -> None:
        self.subscription_generation = generation
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.Event().wait()
        raise StopAsyncIteration

    async def close(self) -> None:
        self.closed = True


class _Gateway:
    def __init__(self) -> None:
        self.generation = 0
        self.block_next: asyncio.Event | None = None
        self.subscriptions: list[_Subscription] = []

    async def recover_market_session(self, token_ids: tuple[str, ...]):
        self.generation += 1
        generation = self.generation
        subscription = _Subscription(generation)
        self.subscriptions.append(subscription)
        if self.block_next is not None:
            await self.block_next.wait()
        return SimpleNamespace(
            order_books=tuple(_book(token_id, generation) for token_id in token_ids),
            subscription=subscription,
            subscription_generation=generation,
        )


class _Catalog:
    async def load_catalog(self) -> CatalogSnapshot:
        event = Event(
            id="event-1",
            title="Event",
            status=MarketStatus.ACTIVE,
            market_ids=("market-1",),
            sync_generation="sync-1",
            sync_generation_complete=True,
        )
        market = Market(
            id="market-1",
            event_id="event-1",
            condition_id="condition-1",
            question="Question",
            status=MarketStatus.ACTIVE,
            active=True,
            accepting_orders=True,
            enable_orderbook=True,
            sync_generation="sync-1",
            sync_generation_complete=True,
            tick_size=Decimal("0.01"),
            minimum_order_size=Decimal("1"),
        )
        tokens = tuple(
            Token(
                id=f"token-{position + 1}",
                market_id="market-1",
                outcome=outcome,
                position=position,
                sync_generation="sync-1",
                sync_generation_complete=True,
            )
            for position, outcome in enumerate(("Yes", "No"))
        )
        return CatalogSnapshot(events=(event,), markets=(market,), tokens=tokens)


class _Changes:
    async def get(self):
        await asyncio.Event().wait()

    def task_done(self) -> None:
        raise AssertionError("no catalog change expected")


class _Contexts:
    def contexts_for(
        self,
        changed_token_id: str,
        orderbooks: tuple[OrderBook, ...],
    ) -> tuple[EvaluationTarget, ...]:
        if changed_token_id != "token-1":
            return ()
        return (
            EvaluationTarget(
                context=SimpleNamespace(
                    changed_token_id=changed_token_id,
                    orderbooks=orderbooks,
                ),  # type: ignore[arg-type]
                opportunity_key="same-opportunity",
                expected_revision=None,
            ),
        )


class _ProfitableStrategy:
    def evaluate(self, context: Any) -> OpportunityPresent:
        book = next(
            candidate for candidate in context.orderbooks if candidate.token_id == "token-1"
        )
        return OpportunityPresent(
            calculation=OpportunityCalculation(
                quantity=Decimal("1"),
                total_capital=Decimal("0.5"),
                expected_profit=Decimal("0.1"),
                return_rate=Decimal("0.2"),
                worst_case_loss=Decimal("0"),
                risk_rate=Decimal("0"),
                unhedged_notional=Decimal("0"),
            ),
            legs=(
                SignalLeg(
                    position=0,
                    market_id="market-1",
                    token_id="token-1",
                    action=Action.BUY,
                    quantity=Decimal("1"),
                    average_price=Decimal("0.5"),
                    worst_price=Decimal("0.5"),
                    gross_amount=Decimal("0.5"),
                    fee_amount=Decimal("0"),
                ),
            ),
            evidence=(book,),
        )


class _LifecycleSignals:
    def __init__(self) -> None:
        self.next_id = 1
        self.open_id: str | None = None
        self.opened_ids: list[str] = []
        self.closed_ids: list[str] = []

    async def apply(
        self,
        decision: Any,
        opportunity_key: str,
        expected_revision: int | None,
    ) -> str | None:
        assert opportunity_key == "same-opportunity"
        assert expected_revision is None
        if not isinstance(decision, OpportunityPresent):
            return None
        if self.open_id is None:
            self.open_id = f"signal-{self.next_id}"
            self.next_id += 1
            self.opened_ids.append(self.open_id)
        return self.open_id

    async def close_for_tokens(self, token_ids: tuple[str, ...], decision: Any) -> None:
        assert token_ids == ("token-1", "token-2")
        if self.open_id is not None:
            self.closed_ids.append(self.open_id)
            self.open_id = None


async def test_recovery_closes_old_signal_and_reopens_with_new_signal_id() -> None:
    # Catches a CLOSED signal ID being reused after a generation recovery barrier.
    gateway = _Gateway()
    signals = _LifecycleSignals()
    watch = WatchTask(
        gateway=gateway,
        catalog=_Catalog(),
        changes=_Changes(),
        strategy_engine=_ProfitableStrategy(),
        signal_manager=signals,
        context_source=_Contexts(),
    )
    await watch.start()
    assert signals.opened_ids == ["signal-1"]

    gateway.block_next = asyncio.Event()
    recovering = asyncio.create_task(
        watch.handle_stream_message(
            MarketStreamInvalidated(
                reason="connection_lost",
                token_ids=("token-1", "token-2"),
                received_timestamp=120,
                subscription_generation=1,
                mapping_version="mapping-v1",
            )
        )
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if signals.closed_ids:
            break

    assert signals.closed_ids == ["signal-1"]
    assert signals.open_id is None
    assert watch.cache.state is CacheState.INVALID
    assert signals.opened_ids == ["signal-1"]

    gateway.block_next.set()
    await recovering

    assert watch.cache.state is CacheState.VALID
    assert signals.opened_ids == ["signal-1", "signal-2"]
    assert signals.open_id == "signal-2"
    await watch.close()
