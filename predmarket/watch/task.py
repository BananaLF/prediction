"""Dynamic, generation-aware public market watcher."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import inspect
from typing import Any, Protocol

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.market import MarketStatus
from predmarket.domain.orderbook import OrderBook
from predmarket.domain.signal import (
    DecisionReason,
    NotEvaluable,
    StrategyContext,
    StrategyDecision,
)
from predmarket.persistence.repositories import CatalogSnapshot
from predmarket.polymarket.gateway import (
    MarketRecoverySession,
    MarketStreamEvent,
    MarketStreamInvalidated,
)
from predmarket.watch.cache import (
    CacheInvalidatedError,
    CacheState,
    OrderBookCache,
    OrderBookDelta,
)


class _Gateway(Protocol):
    async def recover_market_session(
        self,
        token_ids: Sequence[str],
    ) -> MarketRecoverySession: ...


class _Catalog(Protocol):
    async def load_catalog(self) -> CatalogSnapshot: ...


class _Changes(Protocol):
    async def get(self) -> MarketChange: ...

    def task_done(self) -> None: ...


class StrategyEngine(Protocol):
    def evaluate(
        self,
        context: StrategyContext,
    ) -> StrategyDecision | Awaitable[StrategyDecision]: ...


class SignalManager(Protocol):
    async def apply(
        self,
        decision: StrategyDecision,
        opportunity_key: str,
        expected_revision: int | None,
    ) -> Any: ...

    async def close_for_tokens(
        self,
        token_ids: tuple[str, ...],
        decision: NotEvaluable,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class EvaluationTarget:
    """A pure strategy context plus SignalManager concurrency identity."""

    context: StrategyContext
    opportunity_key: str
    expected_revision: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.opportunity_key, str) or not self.opportunity_key:
            raise ValueError("opportunity_key must be a non-empty string")
        if self.expected_revision is not None and (
            type(self.expected_revision) is not int or self.expected_revision < 1
        ):
            raise ValueError("expected_revision must be a positive integer or None")


class ContextSource(Protocol):
    def contexts_for(
        self,
        changed_token_id: str,
        orderbooks: tuple[OrderBook, ...],
    ) -> Sequence[EvaluationTarget] | Awaitable[Sequence[EvaluationTarget]]: ...


class WatchTask:
    """Own one subscription generation and its complete in-memory baseline."""

    def __init__(
        self,
        *,
        gateway: _Gateway,
        catalog: _Catalog,
        changes: _Changes,
        strategy_engine: StrategyEngine,
        signal_manager: SignalManager,
        context_source: ContextSource,
        cache: OrderBookCache | None = None,
    ) -> None:
        for value, name in (
            (gateway, "gateway"),
            (catalog, "catalog"),
            (changes, "changes"),
            (strategy_engine, "strategy_engine"),
            (signal_manager, "signal_manager"),
            (context_source, "context_source"),
        ):
            if value is None:
                raise TypeError(f"{name} is required")
        self._gateway = gateway
        self._catalog = catalog
        self._changes = changes
        self._strategy_engine = strategy_engine
        self._signal_manager = signal_manager
        self._context_source = context_source
        self._cache = cache or OrderBookCache()
        self._subscription: Any | None = None
        self._active_token_ids: tuple[str, ...] = ()
        self._started = False
        self._closed = False
        self._operation_lock = asyncio.Lock()

    @property
    def cache(self) -> OrderBookCache:
        return self._cache

    @property
    def active_token_ids(self) -> tuple[str, ...]:
        return self._active_token_ids

    async def start(self) -> None:
        async with self._operation_lock:
            if self._closed:
                raise RuntimeError("watch is closed")
            if self._started:
                return
            snapshot = await self._catalog.load_catalog()
            token_ids = _watchable_token_ids(snapshot)
            self._active_token_ids = token_ids
            if token_ids:
                await self._recover(token_ids)
            self._started = True

    async def run(self) -> None:
        await self.start()
        try:
            while not self._closed:
                change_task = asyncio.create_task(self._changes.get())
                stream_task: asyncio.Task[Any] | None = None
                if self._subscription is not None:
                    stream_task = asyncio.create_task(anext(self._subscription))
                tasks = (change_task,) if stream_task is None else (change_task, stream_task)
                try:
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except BaseException:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

                # A stream invalidation closes evidence before a simultaneous
                # catalog update can attempt to reuse it.
                if stream_task is not None and stream_task in done:
                    try:
                        message = stream_task.result()
                    except StopAsyncIteration:
                        message = MarketStreamInvalidated(
                            reason="sdk_handle_ended",
                            token_ids=self._active_token_ids,
                            received_timestamp=0,
                            subscription_generation=self._cache.generation,
                            mapping_version="watch-synthetic-v1",
                        )
                    await self.handle_stream_message(message)
                if change_task in done:
                    change = change_task.result()
                    try:
                        await self.handle_market_change(change)
                    finally:
                        self._changes.task_done()
        finally:
            await self.close()

    async def handle_market_change(self, change: MarketChange) -> None:
        if not isinstance(change, MarketChange):
            raise TypeError("change must be a MarketChange")
        async with self._operation_lock:
            if self._closed:
                return
            snapshot = await self._catalog.load_catalog()
            new_token_ids = _watchable_token_ids(snapshot)
            if (
                new_token_ids == self._active_token_ids
                and change.change_type is MarketChangeType.MARKET_UPDATED
            ):
                return
            removed = tuple(
                token_id
                for token_id in self._active_token_ids
                if token_id not in frozenset(new_token_ids)
            )
            reason = (
                DecisionReason.EVENT_SETTLED
                if change.change_type is MarketChangeType.EVENT_SETTLED
                else DecisionReason.MARKET_CLOSED
            )
            await self._rotate_to(
                new_token_ids,
                explicitly_closed=removed,
                close_reason=reason,
            )

    async def handle_stream_message(
        self,
        message: MarketStreamEvent | MarketStreamInvalidated,
    ) -> None:
        if not isinstance(message, (MarketStreamEvent, MarketStreamInvalidated)):
            raise TypeError("message must be a mapped gateway stream message")
        async with self._operation_lock:
            if self._closed or message.subscription_generation != self._cache.generation:
                return
            if isinstance(message, MarketStreamInvalidated):
                await self._invalidate_close_recover(
                    DecisionReason.SDK_DISCONNECTED,
                    detail=message.reason,
                )
                return
            if self._cache.state is not CacheState.VALID:
                return
            if message.event_type == "price_change":
                await self._apply_price_change(message)
                return
            if message.event_type == "market_resolved":
                token_ids = _payload_token_ids(message.payload)
                retained = tuple(
                    token_id
                    for token_id in self._active_token_ids
                    if token_id not in frozenset(token_ids)
                )
                await self._rotate_to(
                    retained,
                    explicitly_closed=token_ids,
                    close_reason=DecisionReason.EVENT_SETTLED,
                )
                return
            if message.event_type == "book":
                token_id = _required_string(message.payload.get("token_id"), "token_id")
                current = self._cache.get(token_id)
                opaque_hash = _required_string(message.payload.get("hash"), "book hash")
                if current is None or current.book_hash != opaque_hash:
                    await self._invalidate_close_recover(
                        DecisionReason.ORDERBOOK_INVALID,
                        detail="stream_book_differs_from_rest_baseline",
                    )
                return
            if message.event_type == "tick_size_change":
                await self._invalidate_close_recover(
                    DecisionReason.ORDERBOOK_INVALID,
                    detail="tick_size_changed",
                )
                return
            if message.event_type in {"last_trade_price", "best_bid_ask"}:
                return
            await self._invalidate_close_recover(
                DecisionReason.ORDERBOOK_INVALID,
                detail="unsupported_stream_event",
            )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        subscription, self._subscription = self._subscription, None
        if subscription is not None:
            await _close_owned(subscription)

    async def _apply_price_change(self, message: MarketStreamEvent) -> None:
        raw_changes = message.payload.get("price_changes")
        if isinstance(raw_changes, (str, bytes)) or not isinstance(raw_changes, Sequence):
            await self._invalidate_close_recover(
                DecisionReason.ORDERBOOK_INVALID,
                detail="price_changes_invalid",
            )
            return
        try:
            deltas = tuple(
                OrderBookDelta(
                    token_id=_required_string(change.get("token_id"), "token_id"),
                    side=_required_string(change.get("side"), "side"),
                    price=_required_string(change.get("price"), "price"),
                    size=_required_string(change.get("size"), "size"),
                    book_hash=_required_string(change.get("hash"), "hash"),
                )
                for change in raw_changes
                if isinstance(change, Mapping)
            )
            if len(deltas) != len(raw_changes):
                raise ValueError("price change entries must be mappings")
            exchange_timestamp = _timestamp_ms(
                message.payload.get("timestamp"),
                fallback=message.received_timestamp,
            )
            self._cache.apply_delta(
                deltas,
                generation=message.subscription_generation,
                sequence=self._cache.last_sequence + 1,
                exchange_timestamp=exchange_timestamp,
                received_timestamp=message.received_timestamp,
            )
        except (CacheInvalidatedError, TypeError, ValueError) as error:
            await self._invalidate_close_recover(
                DecisionReason.ORDERBOOK_INVALID,
                detail=f"price_change_invalid:{error}",
            )
            return
        changed = tuple(sorted({delta.token_id for delta in deltas}, key=_utf8))
        await self._evaluate_tokens(changed)

    async def _rotate_to(
        self,
        new_token_ids: tuple[str, ...],
        *,
        explicitly_closed: tuple[str, ...],
        close_reason: DecisionReason,
    ) -> None:
        old_token_ids = self._active_token_ids
        await self._close_current_subscription()
        if self._cache.state is CacheState.VALID:
            self._cache.invalidate(
                generation=self._cache.generation,
                reason="subscription_rotated",
            )
        explicit_set = frozenset(explicitly_closed)
        if explicitly_closed:
            await self._close_signals(
                explicitly_closed,
                close_reason,
                detail="market_control",
            )
        invalidated = tuple(
            token_id for token_id in old_token_ids if token_id not in explicit_set
        )
        if invalidated:
            await self._close_signals(
                invalidated,
                DecisionReason.ORDERBOOK_INVALID,
                detail="subscription_rotated",
            )
        self._active_token_ids = new_token_ids
        if new_token_ids:
            await self._recover(new_token_ids)

    async def _invalidate_close_recover(
        self,
        reason: DecisionReason,
        *,
        detail: str,
    ) -> None:
        token_ids = self._active_token_ids
        if self._cache.state is CacheState.VALID:
            self._cache.invalidate(
                generation=self._cache.generation,
                reason=detail,
            )
        await self._close_current_subscription()
        if token_ids:
            await self._close_signals(token_ids, reason, detail=detail)
            await self._recover(token_ids)

    async def _recover(self, token_ids: tuple[str, ...]) -> None:
        session: Any | None = None
        try:
            session = await self._gateway.recover_market_session(token_ids)
            generation = session.subscription_generation
            if type(generation) is not int or generation <= self._cache.generation:
                raise RuntimeError("gateway recovery generation must increase")
            books = tuple(session.order_books)
            self._cache.begin_resync(generation=generation, token_ids=token_ids)
            self._cache.apply_snapshot(books)
            self._subscription = session.subscription
        except BaseException:
            if session is not None:
                await _close_owned(session.subscription)
            raise
        await self._evaluate_tokens(token_ids)

    async def _close_current_subscription(self) -> None:
        subscription, self._subscription = self._subscription, None
        if subscription is not None:
            await _close_owned(subscription)

    async def _close_signals(
        self,
        token_ids: tuple[str, ...],
        reason: DecisionReason,
        *,
        detail: str,
    ) -> None:
        normalized = tuple(sorted(set(token_ids), key=_utf8))
        if not normalized:
            return
        decision = NotEvaluable(
            reason_code=reason,
            context={
                "token_ids": normalized,
                "subscription_generation": self._cache.generation,
                "detail": detail,
            },
        )
        await self._signal_manager.close_for_tokens(normalized, decision)

    async def _evaluate_tokens(self, token_ids: Sequence[str]) -> None:
        if self._cache.state is not CacheState.VALID:
            return
        books = self._cache.view()
        for token_id in tuple(sorted(set(token_ids), key=_utf8)):
            if self._cache.state is not CacheState.VALID:
                return
            targets = self._context_source.contexts_for(token_id, books)
            if inspect.isawaitable(targets):
                targets = await targets
            materialized = tuple(targets)
            if any(not isinstance(target, EvaluationTarget) for target in materialized):
                raise TypeError("context source must return EvaluationTarget values")
            for target in materialized:
                if self._cache.state is not CacheState.VALID:
                    return
                decision = self._strategy_engine.evaluate(target.context)
                if inspect.isawaitable(decision):
                    decision = await decision
                if not isinstance(decision, StrategyDecision.__args__):
                    raise TypeError("strategy engine returned an invalid decision")
                await self._signal_manager.apply(
                    decision,
                    target.opportunity_key,
                    target.expected_revision,
                )


def _watchable_token_ids(snapshot: CatalogSnapshot) -> tuple[str, ...]:
    if not isinstance(snapshot, CatalogSnapshot):
        raise TypeError("catalog must return CatalogSnapshot")
    watchable_market_ids = {
        market.id
        for market in snapshot.markets
        if (
            market.status is MarketStatus.ACTIVE
            and market.active
            and market.accepting_orders
            and market.enable_orderbook
            and market.resolved_at is None
        )
    }
    return tuple(
        sorted(
            (token.id for token in snapshot.tokens if token.market_id in watchable_market_ids),
            key=_utf8,
        )
    )


def _payload_token_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    raw = payload.get("token_ids")
    if isinstance(raw, (str, bytes)) or not isinstance(raw, Sequence):
        raise ValueError("market_resolved token_ids must be an iterable")
    values = tuple(_required_string(value, "token_id") for value in raw)
    if not values or len(values) != len(set(values)):
        raise ValueError("market_resolved token_ids must be non-empty and unique")
    return tuple(sorted(values, key=_utf8))


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _timestamp_ms(value: object, *, fallback: int) -> int:
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str):
        if value.isdigit():
            return int(value)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
        else:
            if parsed.tzinfo is not None:
                return int(parsed.timestamp() * 1000)
    return fallback


async def _close_owned(subscription: Any) -> None:
    """Do not let caller cancellation orphan an SDK subscription handle."""

    task = asyncio.create_task(subscription.close())
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        current = asyncio.current_task()
        if current is None or current.cancelling() == 0:
            return task.result()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        raise cancellation


def _utf8(value: str) -> bytes:
    return value.encode("utf-8")
