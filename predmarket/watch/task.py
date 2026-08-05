"""Dynamic, generation-aware public market watcher."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
import inspect
import logging
import time
from typing import Any, Protocol

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.domain.decimal import parse_decimal
from predmarket.domain.market import MarketStatus
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import (
    DecisionReason,
    NotEvaluable,
    OpportunityAbsent,
    OpportunityPresent,
    StrategyContext,
    StrategyDecision,
)
from predmarket.persistence.repositories import CatalogSnapshot
from predmarket.polymarket.gateway import (
    MarketRecoveryInvalidatedError,
    MarketRecoverySession,
    MarketRecoveryTransientError,
    MarketSnapshot,
    MarketStreamEvent,
    MarketStreamInvalidated,
)
from predmarket.signals.manager import SubscriptionGenerationChanged
from predmarket.watch.cache import (
    CacheInvalidatedError,
    CacheState,
    OrderBookCache,
    OrderBookDelta,
)


_NO_CHANGE = object()
_LOGGER = logging.getLogger(__name__)
_EVALUATION_SUMMARY_INTERVAL_SECONDS = 10.0
_PRICE_CHANGE_PROGRESS_INTERVAL_SECONDS = 10.0
_SLOW_EVALUATION_SECONDS = 1.0
_RECOVERY_RETRY_INITIAL_SECONDS = 1.0
_RECOVERY_RETRY_MAX_SECONDS = 60.0


class WatchCleanupError(RuntimeError):
    """An owned watcher resource could not reach a confirmed closed state."""


@dataclass(frozen=True, slots=True)
class _RecoveryCleanupResult:
    cancelled: bool = False
    error: Exception | None = None


@dataclass(frozen=True, slots=True)
class _SubscriptionCloseResult:
    error: Exception | None = None


@dataclass(frozen=True, slots=True)
class _RecoveryScopePruned:
    effective_token_ids: tuple[str, ...]
    effective_market_ids: tuple[str, ...]
    removed_token_ids: tuple[str, ...]


@dataclass(slots=True)
class _OwnedRecovery:
    target: asyncio.Task[Any]
    cleanup_task: asyncio.Task[_RecoveryCleanupResult] | None = None
    pending_subscription: Any | None = None


@dataclass(slots=True)
class _OwnedSubscriptionClose:
    subscription: Any
    task: asyncio.Task[_SubscriptionCloseResult]
    retryable: bool = False


class _Gateway(Protocol):
    def hydrate_market_identities(
        self,
        markets: Sequence[Any],
        tokens: Sequence[Any],
        market_ids: Sequence[str],
    ) -> None: ...

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
        *,
        observed_at: int,
    ) -> Any: ...

    async def close_for_tokens(
        self,
        token_ids: tuple[str, ...],
        decision: NotEvaluable,
        *,
        observed_at: int,
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
        clock_ms: Callable[[], int] | None = None,
        market_limit: int = 100,
        minimum_end_horizon_seconds: int = 1_800,
        market_metadata_refresh_interval_seconds: int = 150,
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
        if type(market_limit) is not int or market_limit < 1:
            raise ValueError("market_limit must be a positive integer")
        if (
            type(minimum_end_horizon_seconds) is not int
            or minimum_end_horizon_seconds < 0
        ):
            raise ValueError(
                "minimum_end_horizon_seconds must be a non-negative integer"
            )
        if (
            type(market_metadata_refresh_interval_seconds) is not int
            or market_metadata_refresh_interval_seconds < 1
        ):
            raise ValueError(
                "market_metadata_refresh_interval_seconds must be a positive integer"
            )
        self._gateway = gateway
        self._catalog = catalog
        self._changes = changes
        self._strategy_engine = strategy_engine
        self._signal_manager = signal_manager
        self._context_source = context_source
        self._cache = cache or OrderBookCache()
        self._clock_ms = clock_ms or _system_clock_ms
        self._market_limit = market_limit
        self._minimum_end_horizon_ms = minimum_end_horizon_seconds * 1_000
        self._market_metadata_refresh_interval_seconds = (
            market_metadata_refresh_interval_seconds
        )
        self._subscription: Any | None = None
        self._active_token_ids: tuple[str, ...] = ()
        self._active_market_ids: tuple[str, ...] = ()
        self._started = False
        self._closed = False
        self._operation_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._close_task: asyncio.Task[None] | None = None
        self._recovery_owner: _OwnedRecovery | None = None
        self._subscription_close_owner: _OwnedSubscriptionClose | None = None
        self._stream_message_count = 0
        self._last_stream_progress_at = 0.0
        self._stream_handler_message_count = 0
        self._stream_handler_counts: dict[str, int] = {}
        self._stream_handler_seconds: dict[str, float] = {}
        self._stream_handler_progress_last_message_count = 0
        self._stream_handler_progress_last_counts: dict[str, int] = {}
        self._stream_handler_progress_last_seconds: dict[str, float] = {}
        self._stream_handler_progress_last_at = time.monotonic()
        self._price_change_message_count = 0
        self._price_change_entry_count = 0
        self._price_change_book_level_count = 0
        self._price_change_parse_seconds = 0.0
        self._price_change_cache_seconds = 0.0
        self._price_change_queue_seconds = 0.0
        self._price_change_progress_last_message_count = 0
        self._price_change_progress_last_entry_count = 0
        self._price_change_progress_last_book_level_count = 0
        self._price_change_progress_last_parse_seconds = 0.0
        self._price_change_progress_last_cache_seconds = 0.0
        self._price_change_progress_last_queue_seconds = 0.0
        self._price_change_progress_last_at = time.monotonic()
        self._last_orderbook_observed_at_ms: int | None = None
        self._pending_evaluation_token_ids: set[str] = set()
        self._evaluation_requested = asyncio.Event()
        self._evaluation_task: asyncio.Task[None] | None = None
        self._metadata_refresh_task: asyncio.Task[None] | None = None
        self._evaluation_request_count = 0
        self._evaluation_batch_count = 0
        self._evaluation_coalesced_count = 0
        self._catalog_control_without_rotation_count = 0
        self._last_catalog_change_generation: str | None = None
        self._catalog_change_generation_coalesced_count = 0
        self._catalog_change_generation_coalesced_total = 0
        self._catalog_snapshot: CatalogSnapshot | None = None
        self._catalog_snapshot_revision = 0
        self._catalog_context_lock = asyncio.Lock()
        self._last_evaluation_summary_at = float("-inf")
        self._recovery_retry_initial_seconds = _RECOVERY_RETRY_INITIAL_SECONDS
        self._recovery_retry_max_seconds = _RECOVERY_RETRY_MAX_SECONDS

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
            catalog_started_at = time.monotonic()
            _LOGGER.info("watch_catalog_load_started")
            snapshot = await self._catalog.load_catalog()
            _LOGGER.info(
                "watch_catalog_loaded events=%d markets=%d tokens=%d elapsed_ms=%d",
                len(snapshot.events),
                len(snapshot.markets),
                len(snapshot.tokens),
                int((time.monotonic() - catalog_started_at) * 1_000),
            )
            snapshot = await self._refresh_startup_markets(snapshot)
            await self._prepare_context_catalog(snapshot)
            subscription_started_at = time.monotonic()
            token_ids, market_ids = _watchable_subscription(
                snapshot,
                now_ms=self._now(),
                market_limit=self._market_limit,
                minimum_end_horizon_ms=self._minimum_end_horizon_ms,
            )
            _LOGGER.info(
                "watch_subscription_prepared markets=%d tokens=%d market_limit=%d "
                "token_id_bytes=%d elapsed_ms=%d",
                len(market_ids),
                len(token_ids),
                self._market_limit,
                sum(len(token_id.encode("utf-8")) for token_id in token_ids),
                int((time.monotonic() - subscription_started_at) * 1_000),
            )
            self._active_token_ids = token_ids
            self._active_market_ids = market_ids
            self._hydrate_gateway(snapshot, market_ids)
            recover_open_signals = getattr(
                self._signal_manager, "close_unwatchable_for_active_tokens", None
            )
            if recover_open_signals is not None:
                result = recover_open_signals(token_ids, observed_at=self._now())
                if inspect.isawaitable(result):
                    await result
            if token_ids:
                await self._recover(token_ids)
            else:
                self._log_subscription_success()
            self._started = True

    async def run(self) -> None:
        change_task: asyncio.Task[Any] | None = None
        stop_task: asyncio.Task[Any] | None = None
        stream_task: asyncio.Task[Any] | None = None
        try:
            await self.start()
            if self._evaluation_task is None:
                self._evaluation_task = asyncio.create_task(
                    self._run_deferred_evaluations(),
                    name="watch:strategy-evaluator",
                )
            if self._metadata_refresh_task is None:
                self._metadata_refresh_task = asyncio.create_task(
                    self._run_periodic_market_metadata_refreshes(),
                    name="watch:market-metadata-refresh",
                )
            change_task = asyncio.create_task(self._changes.get())
            stop_task = asyncio.create_task(self._stop_event.wait())
            while not self._closed:
                if stream_task is None and self._subscription is not None:
                    stream_task = asyncio.create_task(
                        self._consume_stream(),
                        name="watch:market-stream-reader",
                    )
                assert change_task is not None
                assert stop_task is not None
                transient_tasks = (
                    (change_task, stop_task)
                    if stream_task is None
                    else (change_task, stop_task, stream_task)
                )
                evaluation_task = self._evaluation_task
                assert evaluation_task is not None
                metadata_refresh_task = self._metadata_refresh_task
                assert metadata_refresh_task is not None
                tasks = (*transient_tasks, evaluation_task, metadata_refresh_task)
                try:
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except BaseException:
                    try:
                        await _cancel_and_drain(transient_tasks)
                    finally:
                        claimed = _claim_completed_change(change_task)
                        if claimed is not _NO_CHANGE:
                            self._changes.task_done()
                            change_task = None
                    raise

                change: object = _claim_completed_change(change_task)
                try:
                    readers_to_drain: tuple[asyncio.Task[Any], ...] = ()
                    terminal = (
                        stop_task in done
                        or self._closed
                        or evaluation_task in done
                        or metadata_refresh_task in done
                    )
                    if terminal:
                        readers_to_drain = tuple(
                            task for task in pending if task in transient_tasks
                        )
                    try:
                        await _cancel_and_drain(readers_to_drain)
                    finally:
                        if change is _NO_CHANGE:
                            change = _claim_completed_change(change_task)
                    if stream_task in readers_to_drain:
                        stream_task = None
                    if stop_task in done or self._closed:
                        continue
                    if evaluation_task in done:
                        await evaluation_task
                        raise RuntimeError("watch strategy evaluator exited unexpectedly")
                    if metadata_refresh_task in done:
                        await metadata_refresh_task
                        raise RuntimeError(
                            "watch market metadata refresher exited unexpectedly"
                        )

                    if stream_task is not None and stream_task in done:
                        await stream_task
                        stream_task = None
                    if change is not _NO_CHANGE:
                        change_task = None
                        await self.handle_market_change(change)
                finally:
                    if change is not _NO_CHANGE:
                        self._changes.task_done()
                        change_task = None
                if change_task is not None and change_task in done:
                    await change_task
                if change_task is None:
                    change_task = asyncio.create_task(self._changes.get())
        finally:
            cleanup_tasks = tuple(
                task
                for task in (change_task, stop_task, stream_task)
                if task is not None and not task.done()
            )
            try:
                await _cancel_and_drain(cleanup_tasks)
            finally:
                if change_task is not None:
                    claimed = _claim_completed_change(change_task)
                    if claimed is not _NO_CHANGE:
                        self._changes.task_done()
            await self.close()

    async def _consume_stream(self) -> None:
        """Consume one or more subscription generations in one outer task."""
        while not self._closed:
            subscription = self._subscription
            if subscription is None:
                return
            try:
                message = await anext(subscription)
            except StopAsyncIteration:
                if subscription is not self._subscription:
                    continue
                message = MarketStreamInvalidated(
                    reason="sdk_handle_ended",
                    token_ids=self._active_token_ids,
                    received_timestamp=0,
                    subscription_generation=subscription.subscription_generation,
                    mapping_version="watch-synthetic-v1",
                )
            handler_started_at = time.perf_counter()
            try:
                await self._handle_stream_message(
                    message,
                    defer_evaluation=True,
                )
            finally:
                event_type = (
                    message.event_type
                    if isinstance(message, MarketStreamEvent)
                    else "invalidated"
                )
                self._record_stream_handler_progress(
                    event_type=event_type,
                    elapsed_seconds=time.perf_counter() - handler_started_at,
                )

    def _record_stream_handler_progress(
        self,
        *,
        event_type: str,
        elapsed_seconds: float,
    ) -> None:
        self._stream_handler_message_count += 1
        self._stream_handler_counts[event_type] = (
            self._stream_handler_counts.get(event_type, 0) + 1
        )
        self._stream_handler_seconds[event_type] = (
            self._stream_handler_seconds.get(event_type, 0.0) + elapsed_seconds
        )
        now = time.monotonic()
        elapsed = now - self._stream_handler_progress_last_at
        if (
            self._stream_handler_message_count != 1
            and elapsed < _PRICE_CHANGE_PROGRESS_INTERVAL_SECONDS
        ):
            return
        message_delta = (
            self._stream_handler_message_count
            - self._stream_handler_progress_last_message_count
        )
        event_types = tuple(sorted(self._stream_handler_counts, key=_utf8))
        count_parts = []
        timing_parts = []
        for name in event_types:
            count_delta = self._stream_handler_counts[name] - (
                self._stream_handler_progress_last_counts.get(name, 0)
            )
            if count_delta == 0:
                continue
            seconds_delta = self._stream_handler_seconds[name] - (
                self._stream_handler_progress_last_seconds.get(name, 0.0)
            )
            count_parts.append(f"{name}:{count_delta}")
            timing_parts.append(f"{name}:{seconds_delta * 1_000 / count_delta:.3f}")
        rate = message_delta / elapsed if elapsed > 0 else 0.0
        _LOGGER.info(
            "watch_stream_handler_progress messages=%d messages_delta=%d "
            "rate_per_second=%.1f event_counts=%s event_ms_per_message=%s",
            self._stream_handler_message_count,
            message_delta,
            rate,
            ",".join(count_parts),
            ",".join(timing_parts),
        )
        self._stream_handler_progress_last_message_count = (
            self._stream_handler_message_count
        )
        self._stream_handler_progress_last_counts = dict(self._stream_handler_counts)
        self._stream_handler_progress_last_seconds = dict(
            self._stream_handler_seconds
        )
        self._stream_handler_progress_last_at = now

    async def handle_market_change(self, change: MarketChange) -> None:
        if not isinstance(change, MarketChange):
            raise TypeError("change must be a MarketChange")
        async with self._operation_lock:
            if self._closed:
                return
            self._prepare_current_subscription_close_retry()
            generation = _catalog_change_generation(change)
            control_change = change.change_type in {
                MarketChangeType.MARKET_DEACTIVATED,
                MarketChangeType.EVENT_SETTLED,
            }
            reason = (
                DecisionReason.EVENT_SETTLED
                if change.change_type is MarketChangeType.EVENT_SETTLED
                else DecisionReason.MARKET_CLOSED
            )
            if (
                generation is not None
                and generation == self._last_catalog_change_generation
            ):
                active_token_id_set = frozenset(self._active_token_ids)
                confirmed_closed = tuple(
                    token_id
                    for token_id in change.token_ids
                    if token_id not in active_token_id_set
                )
                if control_change:
                    await self._close_signals(
                        confirmed_closed,
                        reason,
                        detail="market_control",
                    )
                self._catalog_change_generation_coalesced_count += 1
                self._catalog_change_generation_coalesced_total += 1
                count = self._catalog_change_generation_coalesced_count
                if count == 1 or count % 1_000 == 0:
                    _LOGGER.info(
                        "watch_catalog_change_generation_coalesced "
                        "generation=%s generation_changes=%d total=%d "
                        "change_type=%s control_applied=%s",
                        _bounded_log_value(generation),
                        count,
                        self._catalog_change_generation_coalesced_total,
                        change.change_type.value,
                        control_change and bool(confirmed_closed),
                    )
                return
            started_at = time.monotonic()
            if generation is not None:
                _LOGGER.info(
                    "watch_catalog_change_generation_started generation=%s "
                    "change_type=%s",
                    _bounded_log_value(generation),
                    change.change_type.value,
                )
            snapshot = await self._catalog.load_catalog()
            new_token_ids, new_market_ids = _watchable_subscription(
                snapshot,
                now_ms=self._now(),
                market_limit=self._market_limit,
                minimum_end_horizon_ms=self._minimum_end_horizon_ms,
            )
            active_market_ids = frozenset(self._active_market_ids)
            added_market_ids = tuple(
                market_id
                for market_id in new_market_ids
                if market_id not in active_market_ids
            )
            if added_market_ids:
                _LOGGER.info(
                    "watch_subscription_market_refresh_requested change_type=%s "
                    "added_markets=%d candidate_markets=%d",
                    change.change_type.value,
                    len(added_market_ids),
                    len(new_market_ids),
                )
                snapshot = await self._refresh_markets(
                    snapshot,
                    added_market_ids,
                    trigger="subscription_change",
                )
                new_token_ids, new_market_ids = _watchable_subscription(
                    snapshot,
                    now_ms=self._now(),
                    market_limit=self._market_limit,
                    minimum_end_horizon_ms=self._minimum_end_horizon_ms,
                )
            await self._prepare_context_catalog(snapshot)
            if (
                new_token_ids == self._active_token_ids
                and change.change_type is MarketChangeType.MARKET_UPDATED
            ):
                self._complete_catalog_change_generation(
                    generation,
                    change=change,
                    started_at=started_at,
                )
                return
            new_token_id_set = frozenset(new_token_ids)
            removed = tuple(
                token_id
                for token_id in self._active_token_ids
                if token_id not in new_token_id_set
            )
            explicitly_closed = removed
            if control_change:
                explicitly_closed = tuple(
                    sorted(
                        set(removed).union(
                            token_id
                            for token_id in change.token_ids
                            if token_id not in new_token_id_set
                        ),
                        key=_utf8,
                    )
                )
            if control_change and new_token_ids == self._active_token_ids:
                if explicitly_closed:
                    await self._close_signals(
                        explicitly_closed,
                        reason,
                        detail="market_control",
                    )
                    self._catalog_control_without_rotation_count += 1
                    count = self._catalog_control_without_rotation_count
                    if count == 1 or count % 100 == 0:
                        _LOGGER.info(
                            "watch_catalog_control_applied_without_rotation "
                            "count=%d change_type=%s tokens=%d active_tokens=%d",
                            count,
                            change.change_type.value,
                            len(explicitly_closed),
                            len(self._active_token_ids),
                        )
                else:
                    _LOGGER.info(
                        "watch_catalog_control_ignored_stale change_type=%s "
                        "tokens=%d active_tokens=%d",
                        change.change_type.value,
                        len(change.token_ids),
                        len(self._active_token_ids),
                    )
                self._complete_catalog_change_generation(
                    generation,
                    change=change,
                    started_at=started_at,
                )
                return
            self._hydrate_gateway(snapshot, new_market_ids)
            await self._rotate_to(
                new_token_ids,
                new_market_ids=new_market_ids,
                explicitly_closed=explicitly_closed,
                close_reason=reason,
            )
            self._complete_catalog_change_generation(
                generation,
                change=change,
                started_at=started_at,
            )

    def _complete_catalog_change_generation(
        self,
        generation: str | None,
        *,
        change: MarketChange,
        started_at: float,
    ) -> None:
        if generation is None:
            return
        self._last_catalog_change_generation = generation
        self._catalog_change_generation_coalesced_count = 0
        _LOGGER.info(
            "watch_catalog_change_generation_completed generation=%s "
            "change_type=%s active_markets=%d active_tokens=%d elapsed_ms=%d",
            _bounded_log_value(generation),
            change.change_type.value,
            len(self._active_market_ids),
            len(self._active_token_ids),
            int((time.monotonic() - started_at) * 1_000),
        )

    async def _prepare_context_catalog(
        self,
        snapshot: CatalogSnapshot,
        *,
        expected_revision: int | None = None,
    ) -> bool:
        async with self._catalog_context_lock:
            if (
                expected_revision is not None
                and expected_revision != self._catalog_snapshot_revision
            ):
                _LOGGER.info(
                    "watch_catalog_context_prepare_skipped reason=snapshot_advanced "
                    "expected_revision=%d actual_revision=%d",
                    expected_revision,
                    self._catalog_snapshot_revision,
                )
                return False
            prepare = getattr(self._context_source, "use_catalog_snapshot", None)
            if prepare is not None:
                result = prepare(snapshot)
                if inspect.isawaitable(result):
                    await result
            self._catalog_snapshot = snapshot
            self._catalog_snapshot_revision += 1
            return True

    async def _refresh_startup_markets(
        self,
        snapshot: CatalogSnapshot,
    ) -> CatalogSnapshot:
        _, market_ids = _watchable_subscription(
            snapshot,
            now_ms=self._now(),
            market_limit=self._market_limit,
            minimum_end_horizon_ms=self._minimum_end_horizon_ms,
        )
        return await self._refresh_markets(
            snapshot,
            market_ids,
            trigger="startup",
        )

    async def _refresh_markets(
        self,
        snapshot: CatalogSnapshot,
        market_ids: tuple[str, ...],
        *,
        trigger: str,
    ) -> CatalogSnapshot:
        refresh_market = getattr(self._gateway, "refresh_market", None)
        save_catalog = getattr(self._catalog, "save_catalog", None)
        if not callable(refresh_market) or not callable(save_catalog):
            _LOGGER.info(
                "watch_catalog_refresh_skipped reason=unsupported gateway=%s "
                "catalog=%s trigger=%s",
                callable(refresh_market),
                callable(save_catalog),
                trigger,
            )
            return snapshot
        if not market_ids:
            _LOGGER.info(
                "watch_catalog_refresh_skipped reason=no_watchable_markets trigger=%s",
                trigger,
            )
            return snapshot

        concurrency = min(10, len(market_ids))
        semaphore = asyncio.Semaphore(concurrency)
        started_at = time.monotonic()
        _LOGGER.info(
            "watch_catalog_refresh_started markets=%d concurrency=%d trigger=%s",
            len(market_ids),
            concurrency,
            trigger,
        )

        async def refresh_one(
            market_id: str,
        ) -> tuple[str, MarketSnapshot | None, Exception | None]:
            try:
                async with semaphore:
                    result = await refresh_market(market_id)
                if not isinstance(result, MarketSnapshot):
                    raise TypeError("refresh_market must return MarketSnapshot")
                if result.market.id != market_id:
                    raise ValueError(
                        "refresh_market returned a different market identity"
                    )
                return market_id, result, None
            except Exception as error:
                return market_id, None, error

        results = await asyncio.gather(
            *(refresh_one(market_id) for market_id in market_ids)
        )
        successes = tuple(
            result
            for _, result, error in results
            if result is not None and error is None
        )
        failures = tuple(
            (market_id, error)
            for market_id, result, error in results
            if result is None and error is not None
        )
        if successes:
            await save_catalog(
                events=(),
                markets=tuple(
                    sorted(
                        (result.market for result in successes),
                        key=lambda market: _utf8(market.id),
                    )
                ),
                tokens=tuple(
                    sorted(
                        (
                            token
                            for result in successes
                            for token in result.tokens
                        ),
                        key=lambda token: _utf8(token.id),
                    )
                ),
            )
            snapshot = _merge_refreshed_catalog(snapshot, successes)
        if failures:
            samples = ";".join(
                f"{market_id}:{type(error).__name__}:"
                f"{_bounded_log_value(error)}"
                for market_id, error in failures[:3]
            )
            _LOGGER.warning(
                "watch_catalog_refresh_partial_failure failed=%d samples=%s "
                "trigger=%s",
                len(failures),
                samples,
                trigger,
            )
        _LOGGER.info(
            "watch_catalog_refresh_completed requested=%d succeeded=%d failed=%d "
            "elapsed_ms=%d trigger=%s",
            len(market_ids),
            len(successes),
            len(failures),
            int((time.monotonic() - started_at) * 1_000),
            trigger,
        )
        return snapshot

    def _hydrate_gateway(
        self,
        snapshot: CatalogSnapshot,
        market_ids: tuple[str, ...],
    ) -> None:
        if not market_ids:
            _LOGGER.info("watch_gateway_identities_hydration_skipped markets=0")
            return
        started_at = time.monotonic()
        self._gateway.hydrate_market_identities(
            snapshot.markets,
            snapshot.tokens,
            market_ids,
        )
        _LOGGER.info(
            "watch_gateway_identities_hydrated markets=%d elapsed_ms=%d",
            len(market_ids),
            int((time.monotonic() - started_at) * 1_000),
        )

    def _now(self) -> int:
        value = self._clock_ms()
        if type(value) is not int or value < 0:
            raise ValueError("clock_ms must return a non-negative integer")
        return value

    async def handle_stream_message(
        self,
        message: MarketStreamEvent | MarketStreamInvalidated,
    ) -> None:
        await self._handle_stream_message(message, defer_evaluation=False)

    async def _handle_stream_message(
        self,
        message: MarketStreamEvent | MarketStreamInvalidated,
        *,
        defer_evaluation: bool,
    ) -> None:
        if not isinstance(message, (MarketStreamEvent, MarketStreamInvalidated)):
            raise TypeError("message must be a mapped gateway stream message")
        async with self._operation_lock:
            if self._closed or message.subscription_generation < self._cache.generation:
                return
            self._prepare_current_subscription_close_retry()
            if message.subscription_generation > self._cache.generation:
                await self._invalidate_close_recover(
                    DecisionReason.ORDERBOOK_INVALID,
                    detail="unexpected_future_generation",
                )
                return
            if isinstance(message, MarketStreamInvalidated):
                await self._invalidate_close_recover(
                    DecisionReason.SDK_DISCONNECTED,
                    detail=message.reason,
                )
                return
            self._stream_message_count += 1
            self._last_orderbook_observed_at_ms = self._now()
            progress_at = time.monotonic()
            if (
                self._stream_message_count == 1
                or self._stream_message_count % 1_000 == 0
                or progress_at - self._last_stream_progress_at >= 30.0
            ):
                _LOGGER.info(
                    "watch_stream_progress messages=%d event_type=%s "
                    "generation=%d cache_state=%s",
                    self._stream_message_count,
                    message.event_type,
                    message.subscription_generation,
                    self._cache.state.value,
                )
                self._last_stream_progress_at = progress_at
            if self._cache.state is not CacheState.VALID:
                return
            if message.event_type == "price_change":
                await self._apply_price_change(
                    message,
                    defer_evaluation=defer_evaluation,
                )
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
                    new_market_ids=tuple(
                        market_id
                        for market_id in self._active_market_ids
                        if market_id != message.market_id
                    ),
                    explicitly_closed=token_ids,
                    close_reason=DecisionReason.EVENT_SETTLED,
                )
                return
            if message.event_type == "book":
                try:
                    token_id = _required_string(
                        message.payload.get("token_id"),
                        "token_id",
                    )
                    current = self._cache.get(token_id)
                    if current is None:
                        raise ValueError(
                            "book token is outside the active snapshot"
                        )
                    book = _stream_order_book(message, baseline=current)
                    applied = self._cache.apply_book(book)
                except (CacheInvalidatedError, TypeError, ValueError) as error:
                    await self._invalidate_close_recover(
                        DecisionReason.ORDERBOOK_INVALID,
                        detail=f"stream_book_invalid:{error}",
                    )
                    return
                if applied:
                    if defer_evaluation:
                        self._queue_evaluation((book.token_id,))
                    else:
                        await self._evaluate_tokens((book.token_id,))
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
        self._closed = True
        self._stop_event.set()
        if self._close_task is None:
            self._prepare_current_subscription_close_retry()
            owner = self._recovery_owner
            if owner is not None:
                _prepare_recovery_cleanup_retry(owner)
            self._close_task = asyncio.create_task(self._finish_close())
        close_task = self._close_task
        try:
            await _await_owned_task(close_task)
        except Exception:
            if self._close_task is close_task:
                self._close_task = None
            raise

    async def _finish_close(self) -> None:
        cleanup_error: Exception | None = None
        evaluation_task = self._evaluation_task
        if evaluation_task is not None and evaluation_task is not asyncio.current_task():
            if not evaluation_task.done():
                evaluation_task.cancel()
            await asyncio.gather(evaluation_task, return_exceptions=True)
            if self._evaluation_task is evaluation_task:
                self._evaluation_task = None
        metadata_refresh_task = self._metadata_refresh_task
        if (
            metadata_refresh_task is not None
            and metadata_refresh_task is not asyncio.current_task()
        ):
            if not metadata_refresh_task.done():
                metadata_refresh_task.cancel()
            await asyncio.gather(metadata_refresh_task, return_exceptions=True)
            if self._metadata_refresh_task is metadata_refresh_task:
                self._metadata_refresh_task = None
        self._pending_evaluation_token_ids.clear()
        self._evaluation_requested.clear()
        owner = self._recovery_owner
        if owner is not None:
            terminal = await _wait_recovery_cleanup(_ensure_recovery_cleanup(owner))
            cleanup_error = terminal.error
        async with self._operation_lock:
            # _recover() cannot register after the monotonic stop gate. Holding
            # operation ownership here proves no handler/session is still in
            # the install window.
            owner = self._recovery_owner
            if owner is not None:
                terminal = await _wait_recovery_cleanup(
                    _ensure_recovery_cleanup(owner)
                )
                if cleanup_error is None:
                    cleanup_error = terminal.error
                if terminal.error is None and owner.pending_subscription is None:
                    if self._recovery_owner is owner:
                        self._recovery_owner = None
            try:
                await self._close_current_subscription()
            except Exception as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None:
            raise cleanup_error

    async def _apply_price_change(
        self,
        message: MarketStreamEvent,
        *,
        defer_evaluation: bool = False,
    ) -> None:
        raw_changes = message.payload.get("price_changes")
        if isinstance(raw_changes, (str, bytes)) or not isinstance(raw_changes, Sequence):
            await self._invalidate_close_recover(
                DecisionReason.ORDERBOOK_INVALID,
                detail="price_changes_invalid",
            )
            return
        diagnostic_context = _price_change_diagnostic_context(
            raw_changes,
            cache=self._cache,
        )
        exchange_timestamp: int | None = None
        parse_started_at = time.perf_counter()
        try:
            deltas = tuple(
                OrderBookDelta(
                    token_id=_required_string(change.get("token_id"), "token_id"),
                    side=_required_string(change.get("side"), "side"),
                    price=_required_string(change.get("price"), "price"),
                    size=_required_string(change.get("size"), "size"),
                    book_hash=_required_string(change.get("hash"), "hash"),
                    best_bid=_optional_string(change.get("best_bid"), "best_bid"),
                    best_ask=_optional_string(change.get("best_ask"), "best_ask"),
                )
                for change in raw_changes
                if isinstance(change, Mapping)
            )
            if len(deltas) != len(raw_changes):
                raise ValueError("price change entries must be mappings")
            exchange_timestamp = _timestamp_ms(
                message.payload.get("timestamp"),
            )
            parse_seconds = time.perf_counter() - parse_started_at
            cache_started_at = time.perf_counter()
            applied = self._cache.apply_delta(
                deltas,
                generation=message.subscription_generation,
                sequence=self._cache.last_sequence + 1,
                exchange_timestamp=exchange_timestamp,
                received_timestamp=message.received_timestamp,
            )
            cache_seconds = time.perf_counter() - cache_started_at
        except (CacheInvalidatedError, TypeError, ValueError) as error:
            _LOGGER.warning(
                "watch_price_change_invalid market_id=%s generation=%d "
                "exchange_timestamp=%s received_timestamp=%d error=%s %s",
                message.market_id,
                message.subscription_generation,
                exchange_timestamp,
                message.received_timestamp,
                error,
                diagnostic_context,
            )
            await self._invalidate_close_recover(
                DecisionReason.ORDERBOOK_INVALID,
                detail=f"price_change_invalid:{error}",
            )
            return
        if not applied:
            return
        changed = tuple(sorted({delta.token_id for delta in deltas}, key=_utf8))
        book_levels = 0
        for token_id in changed:
            book = self._cache.get(token_id)
            if book is not None:
                book_levels += len(book.bids) + len(book.asks)
        if defer_evaluation:
            queue_started_at = time.perf_counter()
            self._queue_evaluation(changed)
            queue_seconds = time.perf_counter() - queue_started_at
        else:
            queue_seconds = 0.0
        self._record_price_change_progress(
            entries=len(deltas),
            book_levels=book_levels,
            parse_seconds=parse_seconds,
            cache_seconds=cache_seconds,
            queue_seconds=queue_seconds,
        )
        if not defer_evaluation:
            await self._evaluate_tokens(changed)

    def _record_price_change_progress(
        self,
        *,
        entries: int,
        book_levels: int,
        parse_seconds: float,
        cache_seconds: float,
        queue_seconds: float,
    ) -> None:
        self._price_change_message_count += 1
        self._price_change_entry_count += entries
        self._price_change_book_level_count += book_levels
        self._price_change_parse_seconds += parse_seconds
        self._price_change_cache_seconds += cache_seconds
        self._price_change_queue_seconds += queue_seconds
        now = time.monotonic()
        elapsed = now - self._price_change_progress_last_at
        if (
            self._price_change_message_count != 1
            and elapsed < _PRICE_CHANGE_PROGRESS_INTERVAL_SECONDS
        ):
            return
        messages_delta = (
            self._price_change_message_count
            - self._price_change_progress_last_message_count
        )
        entries_delta = (
            self._price_change_entry_count
            - self._price_change_progress_last_entry_count
        )
        book_levels_delta = (
            self._price_change_book_level_count
            - self._price_change_progress_last_book_level_count
        )
        parse_seconds_delta = (
            self._price_change_parse_seconds
            - self._price_change_progress_last_parse_seconds
        )
        cache_seconds_delta = (
            self._price_change_cache_seconds
            - self._price_change_progress_last_cache_seconds
        )
        queue_seconds_delta = (
            self._price_change_queue_seconds
            - self._price_change_progress_last_queue_seconds
        )
        _LOGGER.info(
            "watch_price_change_progress messages=%d messages_delta=%d "
            "entries_per_message=%.2f book_levels_per_message=%.1f "
            "parse_ms_per_message=%.3f cache_ms_per_message=%.3f "
            "queue_ms_per_message=%.3f",
            self._price_change_message_count,
            messages_delta,
            entries_delta / messages_delta,
            book_levels_delta / messages_delta,
            parse_seconds_delta * 1_000 / messages_delta,
            cache_seconds_delta * 1_000 / messages_delta,
            queue_seconds_delta * 1_000 / messages_delta,
        )
        self._price_change_progress_last_message_count = self._price_change_message_count
        self._price_change_progress_last_entry_count = self._price_change_entry_count
        self._price_change_progress_last_book_level_count = (
            self._price_change_book_level_count
        )
        self._price_change_progress_last_parse_seconds = self._price_change_parse_seconds
        self._price_change_progress_last_cache_seconds = self._price_change_cache_seconds
        self._price_change_progress_last_queue_seconds = self._price_change_queue_seconds
        self._price_change_progress_last_at = now

    def _queue_evaluation(self, token_ids: Sequence[str]) -> None:
        if self._closed or self._cache.state is not CacheState.VALID:
            return
        requested = frozenset(token_ids)
        before = len(self._pending_evaluation_token_ids)
        self._pending_evaluation_token_ids.update(requested)
        added = len(self._pending_evaluation_token_ids) - before
        self._evaluation_request_count += 1
        self._evaluation_coalesced_count += len(requested) - added
        self._evaluation_requested.set()
        if self._evaluation_request_count == 1 or self._evaluation_request_count % 1_000 == 0:
            _LOGGER.info(
                "watch_evaluation_queue_progress requests=%d pending_tokens=%d "
                "coalesced_tokens=%d generation=%d",
                self._evaluation_request_count,
                len(self._pending_evaluation_token_ids),
                self._evaluation_coalesced_count,
                self._cache.generation,
            )

    def _discard_pending_evaluations(self, *, reason: str) -> None:
        discarded = len(self._pending_evaluation_token_ids)
        self._pending_evaluation_token_ids.clear()
        self._evaluation_requested.clear()
        if discarded:
            _LOGGER.info(
                "watch_evaluation_queue_discarded tokens=%d generation=%d reason=%s",
                discarded,
                self._cache.generation,
                reason,
            )

    async def _run_deferred_evaluations(self) -> None:
        while not self._closed:
            await self._evaluation_requested.wait()
            self._evaluation_requested.clear()
            token_ids = tuple(sorted(self._pending_evaluation_token_ids, key=_utf8))
            self._pending_evaluation_token_ids.clear()
            if not token_ids or self._cache.state is not CacheState.VALID:
                continue
            self._evaluation_batch_count += 1
            started_at = time.monotonic()
            await self._evaluate_tokens(token_ids)
            elapsed = time.monotonic() - started_at
            if (
                self._evaluation_batch_count == 1
                or self._evaluation_batch_count % 1_000 == 0
                or elapsed >= _SLOW_EVALUATION_SECONDS
            ):
                _LOGGER.info(
                    "watch_evaluation_batch_completed batch=%d tokens=%d "
                    "pending_tokens=%d generation=%d elapsed_ms=%d",
                    self._evaluation_batch_count,
                    len(token_ids),
                    len(self._pending_evaluation_token_ids),
                    self._cache.generation,
                    int(elapsed * 1_000),
                )

    async def _run_periodic_market_metadata_refreshes(self) -> None:
        interval = self._market_metadata_refresh_interval_seconds
        _LOGGER.info(
            "watch_market_metadata_refresh_scheduled interval_seconds=%d",
            interval,
        )
        while not self._closed:
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
            except TimeoutError:
                pass
            if self._closed or self._stop_event.is_set():
                return
            started_at = time.monotonic()
            market_ids = self._active_market_ids
            snapshot = self._catalog_snapshot
            snapshot_source = "memory"
            if snapshot is None:
                snapshot_source = "database"
                snapshot = await self._catalog.load_catalog()
            snapshot_revision = self._catalog_snapshot_revision
            snapshot = await self._refresh_markets(
                snapshot,
                market_ids,
                trigger="periodic",
            )
            context_prepared = await self._prepare_context_catalog(
                snapshot,
                expected_revision=snapshot_revision,
            )
            _LOGGER.info(
                "watch_market_metadata_refresh_cycle_completed markets=%d "
                "snapshot_source=%s context_prepared=%s elapsed_ms=%d",
                len(market_ids),
                snapshot_source,
                context_prepared,
                int((time.monotonic() - started_at) * 1_000),
            )

    async def _rotate_to(
        self,
        new_token_ids: tuple[str, ...],
        *,
        new_market_ids: tuple[str, ...],
        explicitly_closed: tuple[str, ...],
        close_reason: DecisionReason,
    ) -> None:
        old_token_ids = self._active_token_ids
        self._discard_pending_evaluations(reason="subscription_rotated")
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
        self._active_market_ids = new_market_ids
        if new_token_ids:
            await self._recover(new_token_ids)
        else:
            self._log_subscription_success()

    async def _invalidate_close_recover(
        self,
        reason: DecisionReason,
        *,
        detail: str,
    ) -> None:
        token_ids = self._active_token_ids
        _LOGGER.warning(
            "watch_subscription_invalidated reason_code=%s detail=%s "
            "generation=%d tokens=%d",
            reason.value,
            detail,
            self._cache.generation,
            len(token_ids),
        )
        self._discard_pending_evaluations(reason=detail)
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
        attempt = 1
        retry_delay = self._recovery_retry_initial_seconds
        requested_token_ids = token_ids
        excluded_market_ids: set[str] = set()
        while not self._closed and not self._stop_event.is_set():
            try:
                pruned = await self._recover_once(requested_token_ids)
                if pruned is None:
                    return
                requested_token_ids = await self._prepare_recovery_refill(
                    requested_token_ids,
                    pruned,
                    excluded_market_ids=excluded_market_ids,
                )
                if not requested_token_ids:
                    self._active_token_ids = ()
                    self._active_market_ids = ()
                    _LOGGER.warning(
                        "watch_recovery_stopped reason=no_watchable_markets "
                        "excluded_markets=%d",
                        len(excluded_market_ids),
                    )
                    return
            except (
                MarketRecoveryInvalidatedError,
                MarketRecoveryTransientError,
                CacheInvalidatedError,
            ) as error:
                if (
                    isinstance(error, MarketRecoveryInvalidatedError)
                    and not error.retryable
                ):
                    raise
                if self._closed or self._stop_event.is_set():
                    return
                reason = (
                    error.reason
                    if isinstance(
                        error,
                        MarketRecoveryInvalidatedError | MarketRecoveryTransientError,
                    )
                    else f"cache_snapshot_invalid:{error}"
                )
                status = (
                    error.status
                    if isinstance(error, MarketRecoveryTransientError)
                    else None
                )
                retry_after = (
                    error.retry_after
                    if isinstance(error, MarketRecoveryTransientError)
                    else None
                )
                effective_retry_delay = min(
                    max(retry_delay, retry_after or 0.0),
                    self._recovery_retry_max_seconds,
                )
                _LOGGER.warning(
                    "watch_recovery_retry_scheduled attempt=%d tokens=%d "
                    "reason=%s status=%s retry_in_seconds=%.3f",
                    attempt,
                    len(requested_token_ids),
                    reason,
                    status if status is not None else "none",
                    effective_retry_delay,
                )
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=effective_retry_delay,
                    )
                except TimeoutError:
                    pass
                else:
                    return
                attempt += 1
                retry_delay = min(
                    retry_delay * 2,
                    self._recovery_retry_max_seconds,
                )

    async def _prepare_recovery_refill(
        self,
        requested_token_ids: tuple[str, ...],
        pruned: _RecoveryScopePruned,
        *,
        excluded_market_ids: set[str],
    ) -> tuple[str, ...]:
        snapshot_attempt = 1
        while True:
            token_ids = await self._prepare_recovery_refill_attempt(
                requested_token_ids,
                pruned,
                excluded_market_ids=excluded_market_ids,
                snapshot_attempt=snapshot_attempt,
            )
            if token_ids is not None:
                return token_ids
            snapshot_attempt += 1
            _LOGGER.info(
                "watch_recovery_refill_retry reason=snapshot_advanced attempt=%d",
                snapshot_attempt,
            )

    async def _prepare_recovery_refill_attempt(
        self,
        requested_token_ids: tuple[str, ...],
        pruned: _RecoveryScopePruned,
        *,
        excluded_market_ids: set[str],
        snapshot_attempt: int,
    ) -> tuple[str, ...] | None:
        started_at = time.monotonic()
        snapshot = self._catalog_snapshot
        snapshot_revision = self._catalog_snapshot_revision
        snapshot_source = "memory"
        if snapshot is None:
            snapshot_source = "database"
            _LOGGER.info("watch_recovery_refill_catalog_load_started")
            snapshot = await self._catalog.load_catalog()
        _LOGGER.info(
            "watch_recovery_refill_catalog_selected snapshot_source=%s "
            "events=%d markets=%d tokens=%d elapsed_ms=%d",
            snapshot_source,
            len(snapshot.events),
            len(snapshot.markets),
            len(snapshot.tokens),
            int((time.monotonic() - started_at) * 1_000),
        )
        market_id_by_token_id = {
            token.id: token.market_id for token in snapshot.tokens
        }
        requested_market_ids = {
            market_id_by_token_id[token_id]
            for token_id in requested_token_ids
            if token_id in market_id_by_token_id
        }
        removed_market_ids = {
            market_id_by_token_id[token_id]
            for token_id in pruned.removed_token_ids
            if token_id in market_id_by_token_id
        }
        removed_market_ids.update(
            requested_market_ids.difference(pruned.effective_market_ids)
        )
        excluded_market_ids.update(removed_market_ids)
        _LOGGER.info(
            "watch_recovery_refill_started target_markets=%d active_markets=%d "
            "removed_markets=%d excluded_markets=%d",
            self._market_limit,
            len(pruned.effective_market_ids),
            len(removed_market_ids),
            len(excluded_market_ids),
        )
        token_ids, market_ids = _watchable_subscription(
            snapshot,
            now_ms=self._now(),
            market_limit=self._market_limit,
            minimum_end_horizon_ms=self._minimum_end_horizon_ms,
            excluded_market_ids=frozenset(excluded_market_ids),
        )
        effective_market_ids = frozenset(pruned.effective_market_ids)
        added_market_ids = tuple(
            market_id
            for market_id in market_ids
            if market_id not in effective_market_ids
        )
        if added_market_ids:
            snapshot = await self._refresh_markets(
                snapshot,
                added_market_ids,
                trigger="recovery_refill",
            )
            token_ids, market_ids = _watchable_subscription(
                snapshot,
                now_ms=self._now(),
                market_limit=self._market_limit,
                minimum_end_horizon_ms=self._minimum_end_horizon_ms,
                excluded_market_ids=frozenset(excluded_market_ids),
            )
            added_market_ids = tuple(
                market_id
                for market_id in market_ids
                if market_id not in effective_market_ids
            )
        if not token_ids or token_ids == requested_token_ids:
            token_ids = pruned.effective_token_ids
            market_ids = pruned.effective_market_ids
            added_market_ids = ()
            _LOGGER.warning(
                "watch_recovery_refill_unavailable target_markets=%d "
                "available_markets=%d excluded_markets=%d",
                self._market_limit,
                len(market_ids),
                len(excluded_market_ids),
            )
        context_prepared = await self._prepare_context_catalog(
            snapshot,
            expected_revision=snapshot_revision,
        )
        if not context_prepared:
            return None
        self._hydrate_gateway(snapshot, market_ids)
        _LOGGER.info(
            "watch_recovery_refill_prepared target_markets=%d selected_markets=%d "
            "selected_tokens=%d added_markets=%d excluded_markets=%d "
            "snapshot_source=%s snapshot_attempt=%d elapsed_ms=%d",
            self._market_limit,
            len(market_ids),
            len(token_ids),
            len(added_market_ids),
            len(excluded_market_ids),
            snapshot_source,
            snapshot_attempt,
            int((time.monotonic() - started_at) * 1_000),
        )
        return token_ids

    async def _recover_once(
        self,
        token_ids: tuple[str, ...],
    ) -> _RecoveryScopePruned | None:
        if self._closed or self._stop_event.is_set():
            return
        recovery_started_at = time.monotonic()
        _LOGGER.info("watch_recovery_started tokens=%d", len(token_ids))
        session: Any | None = None
        owner = _OwnedRecovery(
            target=asyncio.create_task(
                self._gateway.recover_market_session(token_ids),
                name="watch:recover-market-session",
            )
        )
        self._recovery_owner = owner
        try:
            try:
                session = await asyncio.shield(owner.target)
            except asyncio.CancelledError as cancellation:
                current = asyncio.current_task()
                caller_cancelled = current is not None and current.cancelling() > 0
                terminal = await _wait_recovery_cleanup(
                    _ensure_recovery_cleanup(owner)
                )
                if caller_cancelled:
                    raise cancellation
                if terminal.error is not None:
                    raise terminal.error
                if self._closed or self._stop_event.is_set():
                    return
                raise
            if owner.cleanup_task is not None:
                terminal = await _wait_recovery_cleanup(owner.cleanup_task)
                session = None
                if terminal.error is not None:
                    raise terminal.error
                if self._closed or self._stop_event.is_set():
                    return
                raise asyncio.CancelledError
            if self._closed or self._stop_event.is_set():
                await _close_owned(session.subscription)
                session = None
                return
            generation = session.subscription_generation
            if type(generation) is not int or generation <= self._cache.generation:
                raise RuntimeError("gateway recovery generation must increase")
            books = tuple(session.order_books)
            effective_token_ids = tuple(
                sorted(
                    getattr(session, "token_ids", token_ids),
                    key=_utf8,
                )
            )
            if effective_token_ids != token_ids:
                effective_set = frozenset(effective_token_ids)
                removed_token_ids = tuple(
                    token_id for token_id in token_ids if token_id not in effective_set
                )
                effective_market_ids = tuple(
                    sorted({book.market_id for book in books}, key=_utf8)
                )
                _LOGGER.warning(
                    "watch_recovery_scope_pruned requested_tokens=%d "
                    "active_tokens=%d removed_tokens=%d active_markets=%d "
                    "generation=%d",
                    len(token_ids),
                    len(effective_token_ids),
                    len(removed_token_ids),
                    len(effective_market_ids),
                    generation,
                )
                await self._close_signals(
                    removed_token_ids,
                    DecisionReason.ORDERBOOK_INVALID,
                    detail="recovery_missing_order_books",
                )
                await _close_owned(session.subscription)
                session = None
                return _RecoveryScopePruned(
                    effective_token_ids=effective_token_ids,
                    effective_market_ids=effective_market_ids,
                    removed_token_ids=removed_token_ids,
                )
            _LOGGER.info(
                "watch_recovery_baseline_received tokens=%d books=%d "
                "generation=%d elapsed_ms=%d",
                len(token_ids),
                len(books),
                generation,
                int((time.monotonic() - recovery_started_at) * 1_000),
            )
            self._cache.begin_resync(generation=generation, token_ids=token_ids)
            self._cache.apply_snapshot(books)
            self._active_token_ids = effective_token_ids
            self._active_market_ids = tuple(
                sorted({book.market_id for book in books}, key=_utf8)
            )
            self._last_orderbook_observed_at_ms = self._now()
            self._subscription = session.subscription
        except BaseException as error:
            if isinstance(error, (MarketRecoveryInvalidatedError, CacheInvalidatedError)):
                reason = (
                    error.reason
                    if isinstance(error, MarketRecoveryInvalidatedError)
                    else f"cache_snapshot_invalid:{error}"
                )
                _LOGGER.warning(
                    "watch_recovery_attempt_invalidated tokens=%d elapsed_ms=%d "
                    "reason=%s",
                    len(token_ids),
                    int((time.monotonic() - recovery_started_at) * 1_000),
                    reason,
                )
            elif isinstance(error, Exception):
                _LOGGER.error(
                    "watch_recovery_failed tokens=%d elapsed_ms=%d error=%s",
                    len(token_ids),
                    int((time.monotonic() - recovery_started_at) * 1_000),
                    error,
                )
            if session is not None:
                await _close_owned(session.subscription)
            raise
        finally:
            if (
                self._recovery_owner is owner
                and owner.pending_subscription is None
            ):
                self._recovery_owner = None
        if self._closed or self._stop_event.is_set():
            await self._close_current_subscription()
            return
        evaluation_started_at = time.monotonic()
        _LOGGER.info("watch_evaluation_started tokens=%d", len(token_ids))
        await self._evaluate_tokens(token_ids, force_log=True)
        _LOGGER.info(
            "watch_evaluation_completed tokens=%d elapsed_ms=%d",
            len(token_ids),
            int((time.monotonic() - evaluation_started_at) * 1_000),
        )
        self._log_subscription_success()

    def _log_subscription_success(self) -> None:
        _LOGGER.info(
            "watch_subscribed markets=%d tokens=%d generation=%d",
            len(self._active_market_ids),
            len(self._active_token_ids),
            self._cache.generation,
        )

    async def _close_current_subscription(self) -> None:
        subscription = self._subscription
        if subscription is None:
            return
        owner = self._subscription_close_owner
        if owner is None or owner.subscription is not subscription:
            owner = _OwnedSubscriptionClose(
                subscription=subscription,
                task=asyncio.create_task(
                    _finish_subscription_close(subscription),
                    name="watch:close-subscription",
                ),
            )
            self._subscription_close_owner = owner
        terminal, cancellation = await _wait_subscription_close(owner.task)
        if terminal.error is not None:
            owner.retryable = True
            raise terminal.error
        if self._subscription is subscription:
            self._subscription = None
        if self._subscription_close_owner is owner:
            self._subscription_close_owner = None
        if cancellation is not None:
            raise cancellation

    def _prepare_current_subscription_close_retry(self) -> None:
        owner = self._subscription_close_owner
        if (
            owner is not None
            and owner.subscription is self._subscription
            and owner.retryable
        ):
            self._subscription_close_owner = None

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
        await self._signal_manager.close_for_tokens(
            normalized,
            decision,
            observed_at=self._now(),
        )

    async def _evaluate_tokens(
        self,
        token_ids: Sequence[str],
        *,
        force_log: bool = False,
    ) -> None:
        if self._closed or self._cache.state is not CacheState.VALID:
            return
        started_at = time.monotonic()
        generation = self._cache.generation
        normalized_token_ids = tuple(sorted(set(token_ids), key=_utf8))
        generated_target_count = 0
        target_count = 0
        deduplicated_target_count = 0
        evaluated_opportunity_keys: set[str] = set()
        persisted_signal_count = 0
        stale_after_evaluation_count = 0
        decision_counts: dict[str, int] = {}
        context_elapsed = 0.0
        strategy_elapsed = 0.0
        signal_apply_elapsed = 0.0
        best_return_rate: Decimal | None = None
        best_required_return_rate: Decimal | None = None
        best_opportunity_key = "none"
        best_market_ids = "none"
        maximum_observed_exchange_clock_skew_ms: int | None = None
        exchange_clock_skew_limit_ms: int | None = None
        exchange_clock_skew_token_id = "none"
        exchange_clock_skew_exchange_timestamp: int | None = None
        exchange_clock_skew_received_timestamp: int | None = None
        books = self._cache.view()
        observed_at = self._last_orderbook_observed_at_ms
        batched_targets: Mapping[str, Sequence[EvaluationTarget]] | None = None
        contexts_for_batch = getattr(self._context_source, "contexts_for_batch", None)
        if callable(contexts_for_batch):
            if not self._evaluation_is_current(generation):
                self._log_evaluation_aborted(generation, "before_batch_context")
                return
            context_started_at = time.monotonic()
            batched_targets = contexts_for_batch(normalized_token_ids, books)
            if inspect.isawaitable(batched_targets):
                batched_targets = await batched_targets
            context_elapsed += time.monotonic() - context_started_at
            if not isinstance(batched_targets, Mapping):
                raise TypeError("batch context source must return a mapping")
            if not self._evaluation_is_current(generation):
                self._log_evaluation_aborted(generation, "after_batch_context")
                return
        for token_id in normalized_token_ids:
            if not self._evaluation_is_current(generation):
                self._log_evaluation_aborted(generation, "before_context")
                return
            if batched_targets is None:
                context_started_at = time.monotonic()
                targets = self._context_source.contexts_for(token_id, books)
                if inspect.isawaitable(targets):
                    targets = await targets
                context_elapsed += time.monotonic() - context_started_at
            else:
                targets = batched_targets.get(token_id, ())
            if not self._evaluation_is_current(generation):
                self._log_evaluation_aborted(generation, "after_context")
                return
            materialized = tuple(targets)
            if any(not isinstance(target, EvaluationTarget) for target in materialized):
                raise TypeError("context source must return EvaluationTarget values")
            generated_target_count += len(materialized)
            for target in materialized:
                if target.opportunity_key in evaluated_opportunity_keys:
                    deduplicated_target_count += 1
                    continue
                evaluated_opportunity_keys.add(target.opportunity_key)
                target_count += 1
                if not self._evaluation_is_current(generation):
                    self._log_evaluation_aborted(generation, "before_strategy")
                    return
                context = target.context
                if isinstance(context, StrategyContext):
                    context = replace(
                        context,
                        evaluated_at=self._now(),
                        orderbook_observed_at=observed_at,
                    )
                strategy_started_at = time.monotonic()
                evaluate = self._strategy_engine.evaluate
                if inspect.iscoroutinefunction(evaluate):
                    decision = await evaluate(context)
                else:
                    decision = await asyncio.to_thread(evaluate, context)
                    if inspect.isawaitable(decision):
                        decision = await decision
                strategy_elapsed += time.monotonic() - strategy_started_at
                if not self._evaluation_is_current(generation):
                    self._log_evaluation_aborted(generation, "after_strategy")
                    return
                if not isinstance(decision, StrategyDecision.__args__):
                    raise TypeError("strategy engine returned an invalid decision")
                if isinstance(context, StrategyContext) and observed_at is not None:
                    completed_at_ms = self._now()
                    if (
                        completed_at_ms >= observed_at
                        and completed_at_ms - observed_at
                        > context.configuration.maximum_book_age_ms
                    ):
                        stale_after_evaluation_count += 1
                        decision = NotEvaluable(
                            DecisionReason.ORDERBOOK_STALE,
                            {
                                "changed_token_id": context.changed_token_id,
                                "detail": "orderbook_stale_after_evaluation",
                                "strategy_type": context.strategy_type.value,
                            },
                        )
                decision_key = _decision_log_key(decision)
                decision_counts[decision_key] = decision_counts.get(decision_key, 0) + 1
                if isinstance(decision, NotEvaluable) and (
                    decision.context.get("detail")
                    == "orderbook_timestamp_causality_invalid"
                ):
                    skew_ms = decision.context.get("exchange_clock_skew_ms")
                    if type(skew_ms) is int and (
                        maximum_observed_exchange_clock_skew_ms is None
                        or skew_ms > maximum_observed_exchange_clock_skew_ms
                    ):
                        maximum_observed_exchange_clock_skew_ms = skew_ms
                        limit_ms = decision.context.get(
                            "maximum_exchange_clock_skew_ms"
                        )
                        token_id = decision.context.get("token_id")
                        exchange_timestamp = decision.context.get(
                            "exchange_timestamp"
                        )
                        received_timestamp = decision.context.get(
                            "received_timestamp"
                        )
                        exchange_clock_skew_limit_ms = (
                            limit_ms if type(limit_ms) is int else None
                        )
                        exchange_clock_skew_token_id = (
                            _bounded_log_value(token_id)
                            if isinstance(token_id, str)
                            else "unknown"
                        )
                        exchange_clock_skew_exchange_timestamp = (
                            exchange_timestamp
                            if type(exchange_timestamp) is int
                            else None
                        )
                        exchange_clock_skew_received_timestamp = (
                            received_timestamp
                            if type(received_timestamp) is int
                            else None
                        )
                if isinstance(decision, (OpportunityPresent, OpportunityAbsent)) and (
                    best_return_rate is None
                    or decision.calculation.return_rate > best_return_rate
                ):
                    best_return_rate = decision.calculation.return_rate
                    best_required_return_rate = (
                        context.configuration.minimum_return_rate
                        if isinstance(context, StrategyContext)
                        else None
                    )
                    best_opportunity_key = target.opportunity_key
                    best_market_ids = ",".join(
                        sorted({leg.market_id for leg in decision.legs}, key=_utf8)
                    )
                try:
                    signal_apply_started_at = time.monotonic()
                    signal_id = await self._signal_manager.apply(
                        decision,
                        target.opportunity_key,
                        target.expected_revision,
                        observed_at=self._now(),
                    )
                    signal_apply_elapsed += time.monotonic() - signal_apply_started_at
                except SubscriptionGenerationChanged as error:
                    self._log_evaluation_aborted(
                        generation,
                        "signal_apply_generation_changed",
                        detail=str(error),
                    )
                    return
                if signal_id is not None:
                    persisted_signal_count += 1
        decisions = ",".join(
            f"{key}:{count}" for key, count in sorted(decision_counts.items())
        ) or "none"
        completed_at = time.monotonic()
        elapsed = completed_at - started_at
        if (
            force_log
            or persisted_signal_count > 0
            or elapsed >= _SLOW_EVALUATION_SECONDS
            or completed_at - self._last_evaluation_summary_at
            >= _EVALUATION_SUMMARY_INTERVAL_SECONDS
        ):
            self._last_evaluation_summary_at = completed_at
            _LOGGER.info(
                "watch_evaluation_summary generation=%d tokens=%d generated_targets=%d "
                "targets=%d deduplicated_targets=%d persisted_signals=%d "
                "stale_after_evaluation=%d observation_age_ms=%s decisions=%s "
                "exchange_clock_skew_ms=%s exchange_clock_skew_limit_ms=%s "
                "exchange_clock_skew_token_id=%s exchange_timestamp=%s "
                "received_timestamp=%s "
                "best_return_rate=%s required_return_rate=%s best_opportunity_key=%s "
                "best_market_ids=%s context_ms=%d strategy_ms=%d signal_apply_ms=%d "
                "elapsed_ms=%d",
                generation,
                len(normalized_token_ids),
                generated_target_count,
                target_count,
                deduplicated_target_count,
                persisted_signal_count,
                stale_after_evaluation_count,
                (
                    "unknown"
                    if observed_at is None
                    else str(max(0, self._now() - observed_at))
                ),
                decisions,
                (
                    "none"
                    if maximum_observed_exchange_clock_skew_ms is None
                    else str(maximum_observed_exchange_clock_skew_ms)
                ),
                (
                    "none"
                    if exchange_clock_skew_limit_ms is None
                    else str(exchange_clock_skew_limit_ms)
                ),
                exchange_clock_skew_token_id,
                (
                    "none"
                    if exchange_clock_skew_exchange_timestamp is None
                    else str(exchange_clock_skew_exchange_timestamp)
                ),
                (
                    "none"
                    if exchange_clock_skew_received_timestamp is None
                    else str(exchange_clock_skew_received_timestamp)
                ),
                _format_decimal_for_log(best_return_rate),
                _format_decimal_for_log(best_required_return_rate),
                best_opportunity_key,
                best_market_ids,
                int(context_elapsed * 1_000),
                int(strategy_elapsed * 1_000),
                int(signal_apply_elapsed * 1_000),
                int(elapsed * 1_000),
            )

    def _evaluation_is_current(self, generation: int) -> bool:
        return (
            not self._closed
            and self._cache.state is CacheState.VALID
            and self._cache.generation == generation
        )

    def _log_evaluation_aborted(
        self,
        generation: int,
        stage: str,
        *,
        detail: str | None = None,
    ) -> None:
        message = (
            "watch_evaluation_aborted expected_generation=%d actual_generation=%d "
            "cache_state=%s stage=%s"
        )
        arguments: tuple[Any, ...] = (
            generation,
            self._cache.generation,
            self._cache.state.value,
            stage,
        )
        if detail is not None:
            message += " detail=%s"
            arguments += (detail,)
        _LOGGER.info(message, *arguments)


def _watchable_subscription(
    snapshot: CatalogSnapshot,
    *,
    now_ms: int | None = None,
    market_limit: int = 100,
    minimum_end_horizon_ms: int = 0,
    excluded_market_ids: frozenset[str] = frozenset(),
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if not isinstance(snapshot, CatalogSnapshot):
        raise TypeError("catalog must return CatalogSnapshot")
    if now_ms is None:
        now_ms = _system_clock_ms()
    if type(now_ms) is not int or now_ms < 0:
        raise ValueError("now_ms must be a non-negative integer")
    if type(market_limit) is not int or market_limit < 1:
        raise ValueError("market_limit must be a positive integer")
    if type(minimum_end_horizon_ms) is not int or minimum_end_horizon_ms < 0:
        raise ValueError("minimum_end_horizon_ms must be a non-negative integer")
    if not isinstance(excluded_market_ids, frozenset) or any(
        not isinstance(market_id, str) for market_id in excluded_market_ids
    ):
        raise TypeError("excluded_market_ids must be a frozenset of strings")
    tokens_by_market: dict[str, list[Any]] = {}
    for token in snapshot.tokens:
        tokens_by_market.setdefault(token.market_id, []).append(token)
    candidates = []
    for market in snapshot.markets:
        market_tokens = tokens_by_market.get(market.id, ())
        if not (
            market.status is MarketStatus.ACTIVE
            and market.id not in excluded_market_ids
            and market.active
            and market.accepting_orders
            and market.enable_orderbook
            and market.resolved_at is None
            and market.sync_generation_complete
            and (
                market.end_at is None
                or market.end_at > now_ms + minimum_end_horizon_ms
            )
            and market_tokens
            and all(
                token.sync_generation_complete
                and token.sync_generation == market.sync_generation
                for token in market_tokens
            )
        ):
            continue
        candidates.append(market)
    candidates.sort(
        key=lambda market: (
            market.end_at is None,
            market.end_at if market.end_at is not None else 0,
            _utf8(market.id),
        )
    )
    watchable_market_ids = {
        market.id for market in candidates[:market_limit]
    }
    token_ids = tuple(
        sorted(
            (token.id for token in snapshot.tokens if token.market_id in watchable_market_ids),
            key=_utf8,
        )
    )
    market_ids = tuple(sorted(watchable_market_ids, key=_utf8))
    return token_ids, market_ids


def _catalog_change_generation(change: MarketChange) -> str | None:
    delimiter = f":{change.change_type.value}:"
    generation, separator, _ = change.change_id.partition(delimiter)
    return generation if separator and generation else None


def _watchable_token_ids(snapshot: CatalogSnapshot) -> tuple[str, ...]:
    """Return watchable token ids for compatibility with existing callers."""

    return _watchable_subscription(snapshot)[0]


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _decision_log_key(decision: StrategyDecision) -> str:
    if isinstance(decision, OpportunityPresent):
        return "PRESENT"
    category = "ABSENT" if isinstance(decision, OpportunityAbsent) else "NOT_EVALUABLE"
    reason = decision.reason_code.value
    detail = decision.context.get("detail") if isinstance(decision, NotEvaluable) else None
    if isinstance(detail, str) and detail:
        return f"{category}.{reason}.{detail}"
    return f"{category}.{reason}"


def _payload_token_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    raw = payload.get("token_ids")
    if isinstance(raw, (str, bytes)) or not isinstance(raw, Sequence):
        raise ValueError("market_resolved token_ids must be an iterable")
    values = tuple(_required_string(value, "token_id") for value in raw)
    if not values or len(values) != len(set(values)):
        raise ValueError("market_resolved token_ids must be non-empty and unique")
    return tuple(sorted(values, key=_utf8))


def _stream_order_book(
    message: MarketStreamEvent,
    *,
    baseline: OrderBook,
) -> OrderBook:
    return OrderBook(
        market_id=message.market_id,
        token_id=_required_string(message.payload.get("token_id"), "token_id"),
        bids=_stream_levels(message.payload.get("bids"), "bids"),
        asks=_stream_levels(message.payload.get("asks"), "asks"),
        subscription_generation=message.subscription_generation,
        book_hash=_required_string(message.payload.get("hash"), "book hash"),
        exchange_timestamp=_timestamp_ms(message.payload.get("timestamp")),
        received_timestamp=message.received_timestamp,
        tick_size=_stream_decimal_or_baseline(
            message.payload.get("tick_size"),
            baseline.tick_size,
            "tick_size",
        ),
        minimum_order_size=_stream_decimal_or_baseline(
            message.payload.get("min_order_size"),
            baseline.minimum_order_size,
            "min_order_size",
        ),
    )


def _stream_decimal_or_baseline(
    value: object,
    baseline: Decimal,
    name: str,
) -> Decimal:
    if value is None:
        return baseline
    return parse_decimal(_required_string(value, name))


def _stream_levels(value: object, name: str) -> tuple[OrderBookLevel, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be an iterable")
    levels = []
    for raw_level in value:
        if not isinstance(raw_level, Mapping):
            raise ValueError(f"{name} entries must be mappings")
        levels.append(
            OrderBookLevel(
                price=parse_decimal(
                    _required_string(raw_level.get("price"), f"{name} price")
                ),
                size=parse_decimal(
                    _required_string(raw_level.get("size"), f"{name} size")
                ),
            )
        )
    return tuple(levels)


def _required_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _optional_string(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_string(value, name)


def _timestamp_ms(value: object) -> int:
    maximum = 253_402_300_799_999
    if type(value) is int:
        if 0 <= value <= maximum:
            return value
        raise ValueError("exchange timestamp is out of range")
    if isinstance(value, str):
        if value.isdigit():
            parsed_epoch = int(value)
            if parsed_epoch <= maximum:
                return parsed_epoch
            raise ValueError("exchange timestamp is out of range")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, OverflowError, OSError):
            raise ValueError("exchange timestamp is malformed") from None
        try:
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError(
                    "exchange timestamp must contain an explicit offset"
                )
            if parsed.microsecond % 1000:
                raise ValueError(
                    "exchange timestamp must have millisecond precision"
                )
            epoch = datetime(1970, 1, 1, tzinfo=UTC)
            delta = parsed.astimezone(UTC) - epoch
            milliseconds = (
                delta.days * 86_400_000
                + delta.seconds * 1000
                + delta.microseconds // 1000
            )
        except (OverflowError, OSError):
            raise ValueError("exchange timestamp is out of range") from None
        if 0 <= milliseconds <= maximum:
            return milliseconds
        raise ValueError("exchange timestamp is out of range")
    raise ValueError("exchange timestamp is missing or has an invalid type")


def _claim_completed_change(task: asyncio.Task[Any]) -> object:
    """Claim a successfully returned queue item without an intervening await."""

    if not task.done() or task.cancelled():
        return _NO_CHANGE
    if task.exception() is not None:
        return _NO_CHANGE
    return task.result()


def _ensure_recovery_cleanup(
    owner: _OwnedRecovery,
) -> asyncio.Task[_RecoveryCleanupResult]:
    """Create the sole cancellation owner for one gateway recovery target."""

    if owner.cleanup_task is None:
        owner.cleanup_task = asyncio.create_task(
            _finish_recovery_cleanup(owner),
            name="watch:cleanup-recovery",
        )
    return owner.cleanup_task


def _prepare_recovery_cleanup_retry(owner: _OwnedRecovery) -> None:
    """Allow a new close call to retry one retained, unclosed SDK handle."""

    cleanup = owner.cleanup_task
    if cleanup is None or not cleanup.done() or cleanup.cancelled():
        return
    terminal = cleanup.result()
    if terminal.error is not None and owner.pending_subscription is not None:
        owner.cleanup_task = None


async def _finish_recovery_cleanup(
    owner: _OwnedRecovery,
) -> _RecoveryCleanupResult:
    target = owner.target
    if owner.pending_subscription is None:
        if not target.done():
            target.cancel()
        while not target.done():
            try:
                await asyncio.shield(target)
            except asyncio.CancelledError:
                continue
        if target.cancelled():
            return _RecoveryCleanupResult(cancelled=True)
        exception = target.exception()
        if exception is not None:
            if isinstance(exception, Exception):
                return _RecoveryCleanupResult(error=exception)
            return _RecoveryCleanupResult(
                error=WatchCleanupError(
                    f"gateway recovery cleanup failed: {type(exception).__name__}"
                )
            )
        session = target.result()
        owner.pending_subscription = session.subscription
    subscription = owner.pending_subscription
    try:
        await _close_owned(subscription)
    except Exception as error:
        return _RecoveryCleanupResult(error=error)
    owner.pending_subscription = None
    return _RecoveryCleanupResult()


async def _wait_recovery_cleanup(
    cleanup: asyncio.Task[_RecoveryCleanupResult],
) -> _RecoveryCleanupResult:
    """All waiters shield the same terminal cleanup from repeated cancellation."""

    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            continue
    return cleanup.result()


async def _finish_subscription_close(subscription: Any) -> _SubscriptionCloseResult:
    """Own exactly one SDK close attempt and normalize its terminal state."""

    try:
        await _close_owned(subscription)
    except asyncio.CancelledError:
        return _SubscriptionCloseResult(
            error=WatchCleanupError(
                "watch subscription close owner cancelled internally"
            )
        )
    except Exception as error:
        return _SubscriptionCloseResult(error=error)
    return _SubscriptionCloseResult()


async def _wait_subscription_close(
    task: asyncio.Task[_SubscriptionCloseResult],
) -> tuple[_SubscriptionCloseResult, asyncio.CancelledError | None]:
    """Shield one close attempt while preserving cancellation of each waiter."""

    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
            continue
    return task.result(), cancellation


async def _close_owned(subscription: Any) -> None:
    """Do not let caller cancellation orphan an SDK subscription handle."""

    task = asyncio.create_task(subscription.close())
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        current = asyncio.current_task()
        if current is None or current.cancelling() == 0:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            raise WatchCleanupError(
                "SDK subscription cleanup cancelled internally"
            ) from None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        try:
            task.result()
        except BaseException:
            pass
        raise cancellation


async def _await_owned_task(task: asyncio.Task[None]) -> None:
    """Wait for lifecycle cleanup even when its public waiter is cancelled."""

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


async def _cancel_and_drain(tasks: Sequence[asyncio.Task[Any]]) -> None:
    """Cancel loop-owned readers and never orphan cancellation-delayed tasks."""

    materialized = tuple(tasks)
    if not materialized:
        return
    cleanup = asyncio.create_task(
        _finish_task_drain(materialized),
        name="watch:cleanup-loop-readers",
    )
    cancellation: asyncio.CancelledError | None = None
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
            continue
    cleanup.result()
    if cancellation is not None:
        raise cancellation


async def _finish_task_drain(tasks: tuple[asyncio.Task[Any], ...]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _price_change_diagnostic_context(
    raw_changes: Sequence[Any],
    *,
    cache: OrderBookCache,
) -> str:
    samples: list[str] = []
    for raw_change in raw_changes[:8]:
        if not isinstance(raw_change, Mapping):
            samples.append("entry_type=" + type(raw_change).__name__)
            continue
        token_value = raw_change.get("token_id")
        token_id = token_value if isinstance(token_value, str) else None
        current = cache.get(token_id) if token_id is not None else None
        cache_best_bid = (
            max((level.price for level in current.bids), default=None)
            if current is not None
            else None
        )
        cache_best_ask = (
            min((level.price for level in current.asks), default=None)
            if current is not None
            else None
        )
        samples.append(
            " ".join(
                (
                    f"token_id={_bounded_log_value(token_value)}",
                    f"side={_bounded_log_value(raw_change.get('side'))}",
                    f"price={_bounded_log_value(raw_change.get('price'))}",
                    f"size={_bounded_log_value(raw_change.get('size'))}",
                    "server_best_bid="
                    f"{_bounded_log_value(raw_change.get('best_bid'))}",
                    "server_best_ask="
                    f"{_bounded_log_value(raw_change.get('best_ask'))}",
                    f"cache_best_bid={cache_best_bid}",
                    f"cache_best_ask={cache_best_ask}",
                    "cache_exchange_timestamp="
                    f"{current.exchange_timestamp if current is not None else 'none'}",
                )
            )
        )
    return (
        f"change_count={len(raw_changes)} "
        f"sample_count={len(samples)} samples={' | '.join(samples)}"
    )


def _merge_refreshed_catalog(
    snapshot: CatalogSnapshot,
    refreshes: Sequence[MarketSnapshot],
) -> CatalogSnapshot:
    market_updates = {refresh.market.id: refresh.market for refresh in refreshes}
    token_updates = {
        token.id: token for refresh in refreshes for token in refresh.tokens
    }
    markets = tuple(
        market_updates.pop(market.id, market) for market in snapshot.markets
    )
    tokens = tuple(token_updates.pop(token.id, token) for token in snapshot.tokens)
    if market_updates:
        markets += tuple(sorted(market_updates.values(), key=lambda item: _utf8(item.id)))
    if token_updates:
        tokens += tuple(sorted(token_updates.values(), key=lambda item: _utf8(item.id)))
    return CatalogSnapshot(events=snapshot.events, markets=markets, tokens=tokens)


def _bounded_log_value(value: Any) -> str:
    compact = " ".join(str(value).split())
    return compact[:128] if compact else "none"


def _utf8(value: str) -> bytes:
    return value.encode("utf-8")


def _format_decimal_for_log(value: Decimal | None) -> str:
    if value is None:
        return "none"
    return format(value, ".8f")
