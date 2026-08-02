"""Application supervision and wiring for the read-only signal service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
import inspect
import logging
import time
from typing import Any, TextIO

from predmarket.catalog.changes import MarketChangeOverflow, MarketChangeQueue
from predmarket.config import AppConfig
from predmarket.domain.market import Event, Market, Token
from predmarket.domain.relation import Relation
from predmarket.domain.signal import (
    ExecutionMode,
    NotEvaluable,
    StrategyContext,
    StrategyType,
)
from predmarket.notification.notifier import Notifier, macos_desktop_notification
from predmarket.persistence.integrity import check_database_startup
from predmarket.persistence.repositories import (
    CatalogRepository,
    RelationRepository,
    SignalRepository,
    SystemEventRepository,
)
from predmarket.persistence.schema import initialize_database
from predmarket.persistence.writer import DatabaseWriter
from predmarket.signals.manager import SignalManager
from predmarket.strategy.engine import StrategyEngine
from predmarket.watch.cache import CacheState, OrderBookCache


Clock = Callable[[], int]
Factory = Callable[..., Any]
_LOGGER = logging.getLogger(__name__)


class Supervisor:
    """Own application lifecycle and fail closed when a runtime task exits."""

    def __init__(
        self,
        config: AppConfig,
        *,
        gateway: Any | None = None,
        notifier: Notifier | None = None,
        terminal: TextIO | None = None,
        sync_task_factory: Factory | None = None,
        watch_task_factory: Factory | None = None,
        strategy_engine: StrategyEngine | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock_ms: Clock | None = None,
    ) -> None:
        if not isinstance(config, AppConfig):
            raise TypeError("config must be an AppConfig")
        self._config = config
        self._provided_gateway = gateway
        self._provided_notifier = notifier
        self._terminal = terminal
        self._sync_task_factory = sync_task_factory
        self._watch_task_factory = watch_task_factory
        self._strategy_engine = strategy_engine
        self._sleep = sleep
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        # Database initialization can fail before SystemEventRepository exists.
        # Keep this terminal-only channel available for that failure boundary.
        self._runtime_notifier = notifier or Notifier(
            terminal=terminal,
            clock_ms=self._clock_ms,
        )

    async def run(self) -> int:
        """Start the pipeline and return non-zero on an unexpected task exit."""
        _LOGGER.info("runtime_starting")
        writer: DatabaseWriter | None = None
        gateway: Any | None = None
        watch: Any | None = None
        tasks: tuple[asyncio.Task[Any], ...] = ()
        try:
            writer, gateway, notifier, catalog, sync, watch = await self._build_runtime()
            initial = await sync.run_once()
            await _notify_skipped_markets(notifier, initial)
            while not initial.complete:
                await notifier.notify(
                    event_type="SYNC_GENERATION_INCOMPLETE",
                    message="Initial market sync was incomplete",
                    details={"error": getattr(initial, "error", None)},
                )
                if await catalog.has_watchable_catalog():
                    break
                await self._sleep(self._config.polymarket.sync_interval_seconds)
                initial = await sync.run_once()
                await _notify_skipped_markets(notifier, initial)

            await watch.start()
            _LOGGER.info("runtime_started")
            sync_task = asyncio.create_task(
                self._sync_forever(sync, notifier), name="SyncMarketTask"
            )
            watch_task = asyncio.create_task(watch.run(), name="WatchTask")
            tasks = (sync_task, watch_task)
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.cancelled():
                    continue
                error = task.exception()
                task_name = task.get_name()
                detail = "returned" if error is None else f"failed: {error}"
                if error is None:
                    _LOGGER.error(
                        "runtime_task_exited task=%s reason=returned", task_name
                    )
                else:
                    _LOGGER.error(
                        "runtime_task_exited task=%s error=%s",
                        task_name,
                        error,
                        exc_info=(type(error), error, error.__traceback__),
                    )
                await notifier.notify(
                    event_type="RUNTIME_TASK_EXITED",
                    message=f"{task_name} exited unexpectedly ({detail})",
                    details={"task": task_name, "error": None if error is None else str(error)},
                )
            return 1
        except asyncio.CancelledError:
            # asyncio.run() cancels the main task on the first Ctrl+C. Treat
            # that cancellation as an intentional shutdown so the runner can
            # return normally after this method's cleanup completes.
            _LOGGER.info("runtime_stopping reason=cancelled")
            return 0
        except Exception as error:
            _LOGGER.exception("runtime_startup_failed error=%s", error)
            notifier = self._runtime_notifier
            if notifier is not None:
                try:
                    await notifier.notify(
                        event_type="RUNTIME_STARTUP_FAILED",
                        message=f"Signal service startup failed: {error}",
                        details={"error": str(error)},
                    )
                except Exception:
                    pass
            return 1
        finally:
            await _cancel_and_drain(tasks)
            if watch is not None:
                await _maybe_await(getattr(watch, "close", None))
            if gateway is not None:
                await _maybe_await(getattr(gateway, "close", None))
            if writer is not None:
                await writer.close()
            _LOGGER.info("runtime_stopped")

    async def _build_runtime(
        self,
    ) -> tuple[DatabaseWriter, Any, Notifier, CatalogRepository, Any, Any]:
        # Initialize before the integrity read and before constructing the SDK
        # boundary, so the v2 ten-table schema is an invariant of every run.
        initialize_database(self._config.database.path)
        check_database_startup(self._config.database.path)
        _LOGGER.info("component_initialized component=database")
        writer = DatabaseWriter(
            self._config.database.path,
            queue_size=self._config.database.writer_queue_capacity,
            busy_timeout_ms=self._config.database.busy_timeout_ms,
        )
        await writer.start()
        _LOGGER.info("component_initialized component=database_writer")
        catalog = CatalogRepository(self._config.database.path, writer)
        relations = RelationRepository(self._config.database.path, writer)
        signals = SignalRepository(self._config.database.path, writer)
        system_events = SystemEventRepository(self._config.database.path, writer)
        _LOGGER.info("component_initialized component=repositories")
        notifier = self._provided_notifier or Notifier(
            terminal=self._terminal,
            desktop=(
                macos_desktop_notification
                if self._config.notification.desktop_enabled
                else None
            ),
            system_events=system_events,
            clock_ms=self._clock_ms,
        )
        self._runtime_notifier = notifier
        _LOGGER.info("component_initialized component=notifier")
        changes = MarketChangeQueue(
            self._config.runtime.market_change_queue_capacity,
            record_system_event=lambda overflow: self._record_overflow(
                system_events, overflow
            ),
            notify=lambda overflow: self._notify_overflow(notifier, overflow),
        )
        _LOGGER.info("component_initialized component=market_change_queue")
        gateway = self._provided_gateway
        if gateway is None:
            # This is the one and only Polymarket integration boundary.
            from predmarket.polymarket.gateway import PolymarketGateway

            gateway = PolymarketGateway()
        _LOGGER.info("component_initialized component=gateway")
        if self._sync_task_factory is None:
            from predmarket.catalog.sync import SyncMarketTask

            sync = SyncMarketTask(
                gateway=gateway,
                catalog=catalog,
                changes=changes,
                system_events=system_events,
                clock_ms=self._clock_ms,
            )
        else:
            sync = self._sync_task_factory(
                gateway=gateway,
                catalog=catalog,
                changes=changes,
                system_events=system_events,
                clock_ms=self._clock_ms,
            )
        _LOGGER.info("component_initialized component=sync_task")
        subscription_generation = _SubscriptionGenerationSource()
        router = _SignalManagerRouter(
            signals, notifier, self._clock_ms, subscription_generation
        )
        if self._watch_task_factory is None:
            from predmarket.watch.task import WatchTask

            watch = WatchTask(
                gateway=gateway,
                catalog=catalog,
                changes=changes,
                strategy_engine=self._strategy_engine or StrategyEngine(),
                signal_manager=router,
                context_source=_ApplicationContextSource(
                    config=self._config,
                    catalog=catalog,
                    relations=relations,
                    signals=signals,
                    clock_ms=self._clock_ms,
                ),
            )
        else:
            watch = self._watch_task_factory(
                gateway=gateway,
                catalog=catalog,
                changes=changes,
                strategy_engine=self._strategy_engine or StrategyEngine(),
                signal_manager=router,
                context_source=_ApplicationContextSource(
                    config=self._config,
                    catalog=catalog,
                    relations=relations,
                    signals=signals,
                    clock_ms=self._clock_ms,
                ),
            )
        _LOGGER.info("component_initialized component=watch_task")
        cache = getattr(watch, "cache", None)
        if isinstance(cache, OrderBookCache):
            subscription_generation.bind(cache)
        return writer, gateway, notifier, catalog, sync, watch

    async def _sync_forever(self, sync: Any, notifier: Notifier) -> None:
        while True:
            await self._sleep(self._config.polymarket.sync_interval_seconds)
            result = await sync.run_once()
            await _notify_skipped_markets(notifier, result)
            if getattr(result, "complete", True) is False:
                await notifier.notify(
                    event_type="SYNC_GENERATION_INCOMPLETE",
                    message="Market sync generation was incomplete",
                    details={
                        "error": getattr(result, "error", None),
                        "sync_generation": getattr(result, "sync_generation", None),
                    },
                )

    async def _record_overflow(
        self, system_events: SystemEventRepository, overflow: MarketChangeOverflow
    ) -> None:
        await system_events.append(
            component="SUPERVISOR",
            severity="ERROR",
            event_type="MARKET_CHANGE_QUEUE_OVERFLOW",
            message="Market-change queue entered degraded mode",
            occurred_at=overflow.incoming.occurred_at,
            details={
                "incoming_change_id": overflow.incoming.change_id,
                "evicted_change_id": None if overflow.evicted is None else overflow.evicted.change_id,
                "dropped_change_id": None if overflow.dropped is None else overflow.dropped.change_id,
                "backpressured": overflow.backpressured,
            },
        )

    async def _notify_overflow(
        self, notifier: Notifier, overflow: MarketChangeOverflow
    ) -> None:
        await notifier.notify(
            event_type="MARKET_CHANGE_QUEUE_OVERFLOW",
            message="Market-change queue entered degraded mode",
            details={"incoming_change_id": overflow.incoming.change_id},
        )


async def _notify_skipped_markets(notifier: Notifier, result: Any) -> None:
    market_ids = tuple(getattr(result, "skipped_market_ids", ()))
    if not market_ids:
        return
    warnings = tuple(getattr(result, "warnings", ()))
    await notifier.notify(
        event_type="SYNC_MARKET_SKIPPED",
        message="Malformed markets were skipped from the sync catalog",
        details={
            "sync_generation": getattr(result, "sync_generation", None),
            "markets": [
                {
                    "market_id": market_id,
                    "error": warnings[index] if index < len(warnings) else None,
                }
                for index, market_id in enumerate(market_ids)
            ],
        },
    )


class _ApplicationContextSource:
    """Create stable strategy contexts from the committed catalog only."""

    def __init__(
        self,
        *,
        config: AppConfig,
        catalog: CatalogRepository,
        relations: RelationRepository,
        signals: SignalRepository,
        clock_ms: Clock,
    ) -> None:
        self._config = config
        self._catalog = catalog
        self._relations = relations
        self._signals = signals
        self._clock_ms = clock_ms

    async def contexts_for(
        self, changed_token_id: str, orderbooks: tuple[Any, ...]
    ) -> Sequence[Any]:
        from predmarket.watch.task import EvaluationTarget

        snapshot = await self._catalog.load_catalog()
        token_by_id = {token.id: token for token in snapshot.tokens}
        changed = token_by_id.get(changed_token_id)
        if changed is None:
            return ()
        markets = {market.id: market for market in snapshot.markets}
        market = markets.get(changed.market_id)
        if market is None:
            return ()
        event_by_id = {event.id: event for event in snapshot.events}
        books = {book.token_id: book for book in orderbooks}
        tokens_by_market: dict[str, list[Token]] = {}
        for token in snapshot.tokens:
            tokens_by_market.setdefault(token.market_id, []).append(token)
        targets: list[tuple[StrategyContext, str]] = []
        local_tokens = tuple(tokens_by_market.get(market.id, ()))
        local_books = tuple(book for token in local_tokens if (book := books.get(token.id)) is not None)
        for strategy_type in (
            StrategyType.BINARY_UNDERPRICED,
            StrategyType.BINARY_OVERPRICED,
        ):
            targets.append(
                (
                    self._context(
                        strategy_type, changed_token_id, (market,), local_tokens,
                        local_books, event_by_id, None,
                    ),
                    f"{strategy_type.value}:{market.id}",
                )
            )
        event = event_by_id.get(market.event_id)
        if event is not None and event.neg_risk:
            event_markets = tuple(
                markets[market_id] for market_id in event.market_ids if market_id in markets
            )
            event_tokens = tuple(
                token for item in event_markets for token in tokens_by_market.get(item.id, ())
            )
            event_books = tuple(book for token in event_tokens if (book := books.get(token.id)) is not None)
            targets.append(
                (
                    self._context(
                        StrategyType.NEG_RISK_COMPLETE_SET, changed_token_id,
                        event_markets, event_tokens, event_books, event_by_id, None,
                        events=(event,),
                    ),
                    f"{StrategyType.NEG_RISK_COMPLETE_SET.value}:{event.id}",
                )
            )
        for relation in await self._relations.list_approved():
            if market.id not in {relation.market_a_id, relation.market_b_id}:
                continue
            relation_markets = tuple(
                markets[market_id]
                for market_id in (relation.market_a_id, relation.market_b_id)
                if market_id in markets
            )
            if len(relation_markets) != 2:
                continue
            relation_tokens = tuple(
                token for item in relation_markets for token in tokens_by_market.get(item.id, ())
            )
            relation_books = tuple(
                book for token in relation_tokens if (book := books.get(token.id)) is not None
            )
            targets.append(
                (
                    self._context(
                        StrategyType.LOGICAL_IMPLICATION, changed_token_id,
                        relation_markets, relation_tokens, relation_books,
                        event_by_id, relation,
                    ),
                    f"{StrategyType.LOGICAL_IMPLICATION.value}:{relation.id}",
                )
            )
        return tuple(
            await self._target(context, opportunity_key)
            for context, opportunity_key in targets
        )

    def _context(
        self,
        strategy_type: StrategyType,
        changed_token_id: str,
        markets: tuple[Market, ...],
        tokens: tuple[Token, ...],
        orderbooks: tuple[Any, ...],
        event_by_id: dict[str, Event],
        relation: Relation | None,
        *,
        events: tuple[Event, ...] = (),
    ) -> StrategyContext:
        return StrategyContext(
            strategy_type=strategy_type,
            changed_token_id=changed_token_id,
            markets=markets,
            tokens=tokens,
            approved_implication_relation=relation,
            orderbooks=orderbooks,
            fee_schedules={
                token.id: token.fee_schedule for token in tokens if token.fee_schedule is not None
            },
            evaluated_at=self._clock_ms(),
            configuration=self._config.strategy,
            events=events,
            fee_schedule_max_age_seconds=self._config.polymarket.fee_schedule_max_age_seconds,
            supported_neg_risk_types=("STANDARD", "STANDARD_REDEEM"),
        )

    async def _target(self, context: StrategyContext, opportunity_key: str) -> Any:
        from predmarket.watch.task import EvaluationTarget

        signal_id = await self._signals.find_open_signal_id(opportunity_key)
        expected_revision = (
            None if signal_id is None else await self._signals.get_latest_revision(signal_id)
        )
        return EvaluationTarget(context, opportunity_key, expected_revision)


class _SubscriptionGenerationSource:
    """Read the current, complete WatchTask generation at transaction time."""

    def __init__(self) -> None:
        self._cache: OrderBookCache | None = None

    def bind(self, cache: OrderBookCache) -> None:
        self._cache = cache

    def __call__(self, token_id: str) -> int | None:
        cache = self._cache
        if cache is None or cache.state is not CacheState.VALID:
            return None
        book = cache.get(token_id)
        if book is None or book.subscription_generation != cache.generation:
            return None
        return cache.generation


class _SignalManagerRouter:
    """Route each context to the manager with its immutable strategy identity."""

    def __init__(
        self,
        repository: SignalRepository,
        notifier: Notifier,
        clock_ms: Clock,
        subscription_generation: Callable[[str], int | None] | None = None,
    ) -> None:
        self._repository = repository
        self._notifier = notifier
        self._clock_ms = clock_ms
        self._subscription_generation = subscription_generation
        self._managers: dict[tuple[StrategyType, str | None], SignalManager] = {}
        self._closure_manager = SignalManager(
            repository,
            strategy_type=StrategyType.BINARY_UNDERPRICED,
            execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
            notifier=notifier,
            clock=clock_ms,
        )

    async def apply(self, decision: Any, opportunity_key: str, expected_revision: int | None) -> Any:
        return await self._manager(opportunity_key).apply(
            decision, opportunity_key, expected_revision
        )

    async def close_for_tokens(self, token_ids: tuple[str, ...], decision: NotEvaluable) -> None:
        await self._closure_manager.close_for_tokens(token_ids, decision)

    async def close_unwatchable_for_active_tokens(
        self, active_token_ids: tuple[str, ...]
    ) -> None:
        await self._closure_manager.close_unwatchable_for_active_tokens(active_token_ids)

    def _manager(self, opportunity_key: str) -> SignalManager:
        parts = opportunity_key.split(":", 2)
        try:
            strategy_type = StrategyType(parts[0])
        except ValueError as error:
            raise ValueError(f"unknown strategy opportunity key: {opportunity_key!r}") from error
        relation_id = parts[1] if strategy_type is StrategyType.LOGICAL_IMPLICATION and len(parts) > 1 else None
        key = (strategy_type, relation_id)
        if key not in self._managers:
            self._managers[key] = SignalManager(
                self._repository,
                strategy_type=strategy_type,
                execution_mode=(
                    ExecutionMode.HOLD_TO_RESOLUTION
                    if relation_id is not None
                    else ExecutionMode.IMMEDIATE_CONVERSION
                ),
                relation_id=relation_id,
                subscription_generation=self._subscription_generation,
                notifier=self._notifier,
                clock=self._clock_ms,
            )
        return self._managers[key]


async def _maybe_await(callback: Any) -> None:
    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _cancel_and_drain(tasks: Sequence[asyncio.Task[Any]]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
