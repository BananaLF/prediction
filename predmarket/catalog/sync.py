"""Complete-generation catalog synchronization."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Coroutine, Iterable, Sequence
from dataclasses import dataclass, replace
import logging
import math
from typing import Any, Protocol, TypeVar
from uuid import uuid4

from predmarket.catalog.changes import (
    MarketChange,
    MarketChangeQueue,
    MarketChangeType,
)
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.persistence.repositories import (
    CatalogRepository,
    CatalogSnapshot,
    SystemEventRepository,
)
from predmarket.polymarket.gateway import MarketMappingWarning, MarketSnapshot


T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


async def _await_auxiliary_write(
    operation: Coroutine[Any, Any, T],
    *,
    name: str,
) -> T:
    """Own an admitted auxiliary write until it reaches a terminal state."""

    task = asyncio.create_task(operation, name=f"catalog-auxiliary:{name}")
    try:
        return await asyncio.shield(task)
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
        except BaseException as error:
            _LOGGER.error(
                "Catalog auxiliary %s failed while caller cancellation "
                "was pending: %s",
                name,
                error,
            )
        raise cancellation


class _Gateway(Protocol):
    async def list_active_events(self) -> tuple[Event, ...]: ...

    async def list_active_markets(self) -> tuple[MarketSnapshot, ...]: ...

    async def refresh_market(self, market_id: str) -> MarketSnapshot: ...


class _ChangeSink(Protocol):
    async def put(self, change: MarketChange) -> bool: ...


class _CatalogStore(Protocol):
    async def load_catalog(self) -> CatalogSnapshot: ...

    async def save_catalog(
        self,
        *,
        events: Sequence[Event],
        markets: Sequence[Market],
        tokens: Sequence[Token],
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class SyncResult:
    sync_generation: str
    complete: bool
    events_seen: int
    markets_seen: int
    markets_persisted: int
    tokens_seen: int
    changes_published: int
    changes_dropped: int
    error: str | None = None
    degraded: bool = False
    publication_marker_failures: int = 0
    cursor_persistence_failed: bool = False
    skipped_market_ids: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


class SyncMarketTask:
    def __init__(
        self,
        *,
        gateway: _Gateway,
        catalog: CatalogRepository | _CatalogStore,
        changes: MarketChangeQueue | _ChangeSink,
        system_events: SystemEventRepository,
        clock_ms: Callable[[], int],
        generation_factory: Callable[[], str] | None = None,
        settlement_refresh_budget: int = 100,
        settlement_refresh_timeout_seconds: float = 5.0,
    ) -> None:
        if (
            type(settlement_refresh_budget) is not int
            or settlement_refresh_budget < 1
        ):
            raise ValueError("settlement_refresh_budget must be a positive integer")
        if (
            isinstance(settlement_refresh_timeout_seconds, bool)
            or not isinstance(settlement_refresh_timeout_seconds, (int, float))
            or not math.isfinite(settlement_refresh_timeout_seconds)
            or settlement_refresh_timeout_seconds <= 0
        ):
            raise ValueError(
                "settlement_refresh_timeout_seconds must be finite positive"
            )
        self._gateway = gateway
        self._catalog = catalog
        self._changes = changes
        self._system_events = system_events
        self._clock_ms = clock_ms
        self._generation_factory = generation_factory or (
            lambda: f"sync-{uuid4().hex}"
        )
        self._settlement_refresh_budget = settlement_refresh_budget
        self._settlement_refresh_timeout_seconds = float(
            settlement_refresh_timeout_seconds
        )
        self._degraded = False
        self._refresh_cursor_pending = False
        self._refresh_cursor_fallback: str | None = None

    @property
    def degraded(self) -> bool:
        return self._degraded

    async def run_once(self) -> SyncResult:
        _LOGGER.info("正在开始执行第一次扫描")
        occurred_at = self._now()
        generation = self._new_generation()
        errors: list[str] = []
        events: list[Event] = []
        snapshots: list[MarketSnapshot] = []
        market_warnings: tuple[MarketMappingWarning, ...] = ()

        try:
            raw_events = await self._gateway.list_active_events()
        except Exception as error:
            errors.append(_error_text("event request failed", error))
        else:
            events, validation_error = _validated_events(raw_events)
            if validation_error is not None:
                errors.append(validation_error)

        try:
            raw_snapshots = await self._gateway.list_active_markets()
            market_warnings = _gateway_market_mapping_warnings(self._gateway)
        except Exception as error:
            errors.append(_error_text("market request failed", error))
        else:
            snapshots, validation_error = _validated_snapshots(raw_snapshots)
            if validation_error is not None:
                errors.append(validation_error)

        previous = await self._catalog.load_catalog()
        published_market_ids = await self._system_events.list_published_market_ids()
        persisted_refresh_cursor = (
            await self._system_events.get_settlement_refresh_cursor()
        )
        refresh_cursor = (
            self._refresh_cursor_fallback
            if self._refresh_cursor_pending
            else persisted_refresh_cursor
        )
        next_refresh_cursor: str | None = None
        if not errors:
            (
                resolved_snapshots,
                refresh_error,
                next_refresh_cursor,
            ) = await _refresh_missing_markets(
                gateway=self._gateway,
                previous=previous,
                active_snapshots=snapshots,
                cursor=refresh_cursor,
                budget=self._settlement_refresh_budget,
                timeout_seconds=self._settlement_refresh_timeout_seconds,
            )
            snapshots.extend(resolved_snapshots)
            if refresh_error is not None:
                errors.append(refresh_error)
        if not errors:
            validation_error = _validate_complete_source(
                events=events,
                snapshots=snapshots,
                previous=previous,
                skipped_market_ids={warning.market_id for warning in market_warnings},
            )
            if validation_error is not None:
                errors.append(validation_error)

        if errors:
            partial = _prepare_incomplete(
                events=events,
                snapshots=snapshots,
                previous=previous,
                generation=generation,
                occurred_at=occurred_at,
            )
            markets_persisted = len(partial.markets)
            if partial.events or partial.markets or partial.tokens:
                await self._catalog.save_catalog(
                    events=partial.events,
                    markets=partial.markets,
                    tokens=partial.tokens,
                )
            error_message = "; ".join(errors)
            await self._system_events.append(
                component="SYNC",
                severity="ERROR",
                event_type="SYNC_GENERATION_INCOMPLETE",
                message="Catalog sync generation did not complete",
                occurred_at=occurred_at,
                details={
                    "sync_generation": generation,
                    "error": error_message,
                    "events_seen": len(events),
                    "markets_seen": len(snapshots),
                    "tokens_seen": sum(len(item.tokens) for item in snapshots),
                },
            )
            cursor_error = await self._advance_refresh_cursor(
                generation=generation,
                cursor=next_refresh_cursor,
                occurred_at=occurred_at,
            )
            if cursor_error is not None:
                error_message = f"{error_message}; {cursor_error}"
                await self._report_degraded(
                    occurred_at=occurred_at,
                    errors=(cursor_error,),
                    failed_change_ids=(),
                    cursor_persistence_failed=True,
                )
            _LOGGER.error(
                "sync_incomplete sync_generation=%s markets_seen=%d "
                "markets_persisted=%d tokens_seen=%d error=%s",
                generation,
                len(snapshots),
                markets_persisted,
                sum(len(item.tokens) for item in snapshots),
                error_message,
            )
            return SyncResult(
                sync_generation=generation,
                complete=False,
                events_seen=len(events),
                markets_seen=len(snapshots),
                markets_persisted=markets_persisted,
                tokens_seen=sum(len(item.tokens) for item in snapshots),
                changes_published=0,
                changes_dropped=0,
                error=error_message,
                degraded=self._degraded,
                cursor_persistence_failed=cursor_error is not None,
                skipped_market_ids=tuple(
                    warning.market_id for warning in market_warnings
                ),
                warnings=tuple(warning.error for warning in market_warnings),
            )

        prepared = _prepare_complete(
            events=events,
            snapshots=snapshots,
            previous=previous,
            generation=generation,
            occurred_at=occurred_at,
            published_market_ids=published_market_ids,
        )
        # One writer transaction completes before any Watch-visible change.
        await self._catalog.save_catalog(
            events=prepared.events,
            markets=prepared.markets,
            tokens=prepared.tokens,
        )
        markets_persisted = len(prepared.markets)
        published = 0
        dropped = 0
        admitted: list[tuple[MarketChange, tuple[str, ...]]] = []
        for change in prepared.changes:
            if await self._changes.put(change):
                published += 1
                if change.market_id is not None:
                    affected_market_ids = (change.market_id,)
                else:
                    affected_market_ids = tuple(
                        market.id
                        for market in prepared.markets
                        if market.event_id == change.event_id
                    )
                admitted.append((change, affected_market_ids))
            else:
                dropped += 1

        marker_errors: list[str] = []
        failed_change_ids: list[str] = []
        for change, affected_market_ids in admitted:
            marker_error = await self._record_publication_marker(
                change=change,
                market_ids=affected_market_ids,
            )
            if marker_error is not None:
                marker_errors.append(marker_error)
                failed_change_ids.append(change.change_id)

        cursor_error = await self._advance_refresh_cursor(
            generation=generation,
            cursor=next_refresh_cursor,
            occurred_at=occurred_at,
        )
        degradation_errors = tuple(
            marker_errors
            + ([] if cursor_error is None else [cursor_error])
        )
        if degradation_errors:
            await self._report_degraded(
                occurred_at=occurred_at,
                errors=degradation_errors,
                failed_change_ids=tuple(failed_change_ids),
                cursor_persistence_failed=cursor_error is not None,
            )
        if market_warnings:
            await self._system_events.append(
                component="SYNC",
                severity="WARNING",
                event_type="SYNC_MARKET_SKIPPED",
                message="Malformed markets were skipped from the sync catalog",
                occurred_at=occurred_at,
                details={
                    "sync_generation": generation,
                    "markets": [
                        {
                            "market_id": warning.market_id,
                            "error": warning.error,
                        }
                        for warning in market_warnings
                    ],
                },
            )
        _LOGGER.info(
            "sync_completed sync_generation=%s markets_seen=%d "
            "markets_persisted=%d tokens_seen=%d changes_published=%d "
            "changes_dropped=%d",
            generation,
            len(snapshots),
            markets_persisted,
            sum(len(item.tokens) for item in snapshots),
            published,
            dropped,
        )
        return SyncResult(
            sync_generation=generation,
            complete=True,
            events_seen=len(events),
            markets_seen=len(snapshots),
            markets_persisted=markets_persisted,
            tokens_seen=sum(len(item.tokens) for item in snapshots),
            changes_published=published,
            changes_dropped=dropped,
            degraded=self._degraded,
            publication_marker_failures=len(marker_errors),
            cursor_persistence_failed=cursor_error is not None,
            skipped_market_ids=tuple(
                warning.market_id for warning in market_warnings
            ),
            warnings=tuple(warning.error for warning in market_warnings),
        )

    async def _record_publication_marker(
        self,
        *,
        change: MarketChange,
        market_ids: tuple[str, ...],
    ) -> str | None:
        try:
            await _await_auxiliary_write(
                self._system_events.record_market_change_published(
                    change,
                    market_ids=market_ids,
                ),
                name=f"publication-marker:{change.change_id}",
            )
        except Exception as error:
            self._degraded = True
            message = _error_text("publication marker write failed", error)
            _LOGGER.error(
                "Publication marker failed for %s: %s",
                change.change_id,
                error,
            )
            return message
        return None

    async def _advance_refresh_cursor(
        self,
        *,
        generation: str,
        cursor: str | None,
        occurred_at: int,
    ) -> str | None:
        if cursor is None:
            return None
        try:
            await _await_auxiliary_write(
                self._system_events.record_settlement_refresh_cursor(
                    sync_generation=generation,
                    cursor=cursor,
                    occurred_at=occurred_at,
                ),
                name=f"settlement-refresh-cursor:{generation}",
            )
        except Exception as error:
            self._degraded = True
            self._refresh_cursor_pending = True
            self._refresh_cursor_fallback = cursor
            _LOGGER.error("Settlement refresh cursor write failed: %s", error)
            return _error_text(
                "settlement refresh cursor persistence failed",
                error,
            )
        self._refresh_cursor_pending = False
        self._refresh_cursor_fallback = None
        return None

    async def _report_degraded(
        self,
        *,
        occurred_at: int,
        errors: tuple[str, ...],
        failed_change_ids: tuple[str, ...],
        cursor_persistence_failed: bool,
    ) -> None:
        details: dict[str, Any] = {
            "errors": errors,
            "failed_change_ids": failed_change_ids,
            "cursor_persistence_failed": cursor_persistence_failed,
        }
        if failed_change_ids:
            details["failed_change_id"] = failed_change_ids[0]
        try:
            await _await_auxiliary_write(
                self._system_events.append(
                    component="SYNC",
                    severity="ERROR",
                    event_type="SYSTEM_DEGRADED",
                    message="Catalog sync auxiliary persistence degraded",
                    occurred_at=occurred_at,
                    details=details,
                ),
                name="degradation-report",
            )
        except Exception as error:
            _LOGGER.error("Catalog sync degradation report failed: %s", error)

    def _now(self) -> int:
        value = self._clock_ms()
        if type(value) is not int or value < 0:
            raise ValueError("clock_ms must return a non-negative integer")
        return value

    def _new_generation(self) -> str:
        value = self._generation_factory()
        if not isinstance(value, str) or not value:
            raise ValueError("generation_factory must return a non-empty string")
        return value


@dataclass(frozen=True, slots=True)
class _PreparedCatalog:
    events: tuple[Event, ...]
    markets: tuple[Market, ...]
    tokens: tuple[Token, ...]
    changes: tuple[MarketChange, ...] = ()


def _validated_events(values: object) -> tuple[list[Event], str | None]:
    if isinstance(values, (str, bytes)):
        return [], "required event collection is invalid"
    try:
        materialized = tuple(values)  # type: ignore[arg-type]
    except TypeError:
        return [], "required event collection is invalid"
    events: list[Event] = []
    seen: set[str] = set()
    for value in materialized:
        if not isinstance(value, Event):
            return events, "required event entity failed parsing"
        if value.id in seen:
            return events, f"required event ID is duplicated: {value.id}"
        seen.add(value.id)
        events.append(value)
    return events, None


def _validated_snapshots(
    values: object,
) -> tuple[list[MarketSnapshot], str | None]:
    if isinstance(values, (str, bytes)):
        return [], "required market snapshot collection is invalid"
    try:
        materialized = tuple(values)  # type: ignore[arg-type]
    except TypeError:
        return [], "required market snapshot collection is invalid"
    snapshots: list[MarketSnapshot] = []
    market_ids: set[str] = set()
    token_ids: set[str] = set()
    for value in materialized:
        if not isinstance(value, MarketSnapshot):
            return snapshots, "required market snapshot entity failed parsing"
        if value.market.id in market_ids:
            return snapshots, (
                f"required market ID is duplicated: {value.market.id}"
            )
        duplicate_tokens = token_ids.intersection(
            token.id for token in value.tokens
        )
        if duplicate_tokens:
            return snapshots, (
                "required token ID is duplicated: "
                f"{sorted(duplicate_tokens)[0]}"
            )
        market_ids.add(value.market.id)
        token_ids.update(token.id for token in value.tokens)
        snapshots.append(value)
    return snapshots, None


def _gateway_market_mapping_warnings(
    gateway: object,
) -> tuple[MarketMappingWarning, ...]:
    values = getattr(gateway, "market_mapping_warnings", ())
    if values is None or isinstance(values, (str, bytes)):
        raise ValueError("market mapping warnings collection is invalid")
    try:
        materialized = tuple(values)
    except TypeError as error:
        raise ValueError("market mapping warnings collection is invalid") from error
    if any(not isinstance(value, MarketMappingWarning) for value in materialized):
        raise ValueError("market mapping warning entity failed validation")
    return materialized


async def _refresh_missing_markets(
    *,
    gateway: _Gateway,
    previous: CatalogSnapshot,
    active_snapshots: Sequence[MarketSnapshot],
    cursor: str | None,
    budget: int,
    timeout_seconds: float,
) -> tuple[list[MarketSnapshot], str | None, str | None]:
    active_ids = {snapshot.market.id for snapshot in active_snapshots}
    candidates = [
        market
        for market in sorted(previous.markets, key=lambda item: _utf8(item.id))
        if market.id not in active_ids
        and market.resolved_at is None
        and market.status in {MarketStatus.ACTIVE, MarketStatus.CLOSED}
    ]
    if not candidates:
        return [], None, None
    start = next(
        (
            index
            for index, market in enumerate(candidates)
            if cursor is None or _utf8(market.id) > _utf8(cursor)
        ),
        0,
    )
    selected = [
        candidates[(start + offset) % len(candidates)]
        for offset in range(min(budget, len(candidates)))
    ]
    resolved: list[MarketSnapshot] = []
    for old_market in selected:
        try:
            refreshed = await asyncio.wait_for(
                gateway.refresh_market(old_market.id),
                timeout=timeout_seconds,
            )
        except Exception:
            # Missing from the complete active listing is enough to deactivate,
            # but a failed enrichment request is never settlement proof.
            continue
        if not isinstance(refreshed, MarketSnapshot):
            continue
        market = refreshed.market
        if market.id != old_market.id:
            continue
        if market.status is MarketStatus.ACTIVE and market.active:
            return [], (
                f"market {market.id} remained active during missing-market "
                "refresh"
            ), selected[-1].id
        if market.status is MarketStatus.RESOLVED and market.resolved_at is not None:
            resolved.append(refreshed)
    return resolved, None, selected[-1].id


def _validate_complete_source(
    *,
    events: Sequence[Event],
    snapshots: Sequence[MarketSnapshot],
    previous: CatalogSnapshot,
    skipped_market_ids: Iterable[str] = (),
) -> str | None:
    event_ids = {event.id for event in events}
    old_event_ids = {event.id for event in previous.events}
    for snapshot in snapshots:
        market = snapshot.market
        authoritative_resolved = (
            market.status is MarketStatus.RESOLVED
            and market.resolved_at is not None
            and market.event_id in old_event_ids
        )
        if (
            market.event_id is not None
            and market.event_id not in event_ids
            and not authoritative_resolved
        ):
            return (
                f"market {market.id} references event {market.event_id} "
                "missing from the complete generation"
            )
    old_markets = {market.id: market for market in previous.markets}
    old_tokens_by_market: dict[str, set[str]] = defaultdict(set)
    for token in previous.tokens:
        old_tokens_by_market[token.market_id].add(token.id)
    for snapshot in snapshots:
        old = old_markets.get(snapshot.market.id)
        if old is not None and (
            old.condition_id != snapshot.market.condition_id
        ):
            return f"market {old.id} changed condition identity"
        old_token_ids = old_tokens_by_market.get(snapshot.market.id, set())
        new_token_ids = {token.id for token in snapshot.tokens}
        if old_token_ids and old_token_ids != new_token_ids:
            return f"market {snapshot.market.id} changed token identity"
    return None


def _prepare_complete(
    *,
    events: Sequence[Event],
    snapshots: Sequence[MarketSnapshot],
    previous: CatalogSnapshot,
    generation: str,
    occurred_at: int,
    published_market_ids: frozenset[str],
) -> _PreparedCatalog:
    old_events = {event.id: event for event in previous.events}
    old_markets = {market.id: market for market in previous.markets}
    old_tokens = {token.id: token for token in previous.tokens}
    incoming_events = {event.id: event for event in events}
    incoming_snapshots = {item.market.id: item for item in snapshots}

    final_markets: dict[str, Market] = {}
    for market_id in set(old_markets) | set(incoming_snapshots):
        old = old_markets.get(market_id)
        item = incoming_snapshots.get(market_id)
        if item is None:
            assert old is not None
            final_markets[market_id] = replace(
                old,
                status=(
                    MarketStatus.RESOLVED
                    if old.resolved_at is not None
                    else MarketStatus.CLOSED
                ),
                active=False,
                accepting_orders=False,
                enable_orderbook=False,
                sync_generation=generation,
                sync_generation_complete=True,
                updated_at=occurred_at,
            )
            continue
        event = None
        if item.market.event_id is not None:
            event = incoming_events.get(item.market.event_id) or old_events[
                item.market.event_id
            ]
        market = item.market
        authoritative_market_resolution = (
            market.status is MarketStatus.RESOLVED
            and market.resolved_at is not None
        )
        if event is not None and not authoritative_market_resolution and (
            event.status is not MarketStatus.ACTIVE
            or event.resolved_at is not None
        ):
            market = replace(
                market,
                status=(
                    MarketStatus.RESOLVED
                    if event.status is MarketStatus.RESOLVED
                    or event.resolved_at is not None
                    else MarketStatus.CLOSED
                ),
                active=False,
                accepting_orders=False,
                enable_orderbook=False,
                resolved_at=event.resolved_at or market.resolved_at,
            )
        final_markets[market_id] = replace(
            market,
            sync_generation=generation,
            sync_generation_complete=True,
            created_at=old.created_at if old is not None else occurred_at,
            updated_at=occurred_at,
        )

    market_ids_by_event: dict[str, list[str]] = defaultdict(list)
    for market in final_markets.values():
        if market.event_id is not None:
            market_ids_by_event[market.event_id].append(market.id)

    final_events: dict[str, Event] = {}
    for event_id in set(old_events) | set(incoming_events):
        old = old_events.get(event_id)
        incoming = incoming_events.get(event_id)
        if incoming is None:
            assert old is not None
            related_markets = tuple(
                market
                for market in final_markets.values()
                if market.event_id == event_id
            )
            fully_resolved = bool(related_markets) and all(
                market.status is MarketStatus.RESOLVED
                and market.resolved_at is not None
                for market in related_markets
            )
            event = replace(
                old,
                status=(
                    MarketStatus.RESOLVED
                    if fully_resolved
                    else MarketStatus.CLOSED
                ),
                resolved_at=(
                    max(
                        market.resolved_at
                        for market in related_markets
                        if market.resolved_at is not None
                    )
                    if fully_resolved
                    else old.resolved_at
                ),
                neg_risk_complete=False,
            )
        else:
            event = incoming
        market_ids = tuple(market_ids_by_event[event_id])
        final_events[event_id] = replace(
            event,
            market_ids=market_ids,
            sync_generation=generation,
            sync_generation_complete=True,
            created_at=old.created_at if old is not None else occurred_at,
            updated_at=occurred_at,
        )

    final_tokens: dict[str, Token] = {}
    for item in snapshots:
        for token in item.tokens:
            old = old_tokens.get(token.id)
            final_tokens[token.id] = replace(
                token,
                sync_generation=generation,
                sync_generation_complete=True,
                created_at=old.created_at if old is not None else occurred_at,
                updated_at=occurred_at,
            )
    for token_id, old in old_tokens.items():
        if token_id not in final_tokens:
            final_tokens[token_id] = replace(
                old,
                sync_generation=generation,
                sync_generation_complete=True,
                updated_at=occurred_at,
            )

    changes = _catalog_changes(
        previous=previous,
        events=final_events,
        markets=final_markets,
        tokens=final_tokens,
        generation=generation,
        occurred_at=occurred_at,
        published_market_ids=published_market_ids,
    )
    return _PreparedCatalog(
        events=_ordered(final_events.values()),
        markets=_ordered(final_markets.values()),
        tokens=_ordered(final_tokens.values()),
        changes=changes,
    )


def _prepare_incomplete(
    *,
    events: Sequence[Event],
    snapshots: Sequence[MarketSnapshot],
    previous: CatalogSnapshot,
    generation: str,
    occurred_at: int,
) -> _PreparedCatalog:
    old_events = {event.id: event for event in previous.events}
    old_markets = {market.id: market for market in previous.markets}
    old_tokens = {token.id: token for token in previous.tokens}
    old_token_ids_by_market: dict[str, set[str]] = defaultdict(set)
    for token in previous.tokens:
        old_token_ids_by_market[token.market_id].add(token.id)
    incoming_events = {event.id: event for event in events}

    safe_snapshots: list[MarketSnapshot] = []
    for item in snapshots:
        old_market = old_markets.get(item.market.id)
        parent_exists = (
            item.market.event_id is None
            or item.market.event_id in incoming_events
            or item.market.event_id in old_events
        )
        identity_is_stable = old_market is None or (
            old_market.condition_id == item.market.condition_id
        )
        token_identity_is_stable = all(
            token.id not in old_tokens
            or old_tokens[token.id].market_id == item.market.id
            for token in item.tokens
        )
        if old_market is not None and old_token_ids_by_market[item.market.id]:
            token_identity_is_stable = token_identity_is_stable and (
                old_token_ids_by_market[item.market.id]
                == {token.id for token in item.tokens}
            )
        if parent_exists and identity_is_stable and token_identity_is_stable:
            safe_snapshots.append(item)

    market_upserts: dict[str, Market] = {}
    token_upserts: dict[str, Token] = {}
    for item in safe_snapshots:
        old = old_markets.get(item.market.id)
        market = item.market
        if old is not None:
            market = replace(
                market,
                status=old.status,
                active=old.active,
                accepting_orders=old.accepting_orders,
                enable_orderbook=old.enable_orderbook,
                neg_risk_member_complete=old.neg_risk_member_complete,
                resolved_at=old.resolved_at,
            )
        else:
            market = replace(
                market,
                neg_risk_member_complete=False,
            )
        market_upserts[market.id] = replace(
            market,
            sync_generation=generation,
            sync_generation_complete=False,
            created_at=old.created_at if old is not None else occurred_at,
            updated_at=occurred_at,
        )
        for token in item.tokens:
            old_token = old_tokens.get(token.id)
            token_upserts[token.id] = replace(
                token,
                sync_generation=generation,
                sync_generation_complete=False,
                created_at=(
                    old_token.created_at
                    if old_token is not None
                    else occurred_at
                ),
                updated_at=occurred_at,
            )

    all_market_ids_by_event: dict[str, set[str]] = defaultdict(set)
    for old in previous.markets:
        if old.event_id is not None:
            all_market_ids_by_event[old.event_id].add(old.id)
    for market in market_upserts.values():
        if market.event_id is not None:
            all_market_ids_by_event[market.event_id].add(market.id)

    event_upserts: dict[str, Event] = {}
    affected_event_ids = set(incoming_events)
    affected_event_ids.update(
        market.event_id
        for market in market_upserts.values()
        if market.event_id is not None
    )
    for event_id in affected_event_ids:
        old = old_events.get(event_id)
        incoming = incoming_events.get(event_id)
        market_ids = all_market_ids_by_event[event_id]
        if incoming is None:
            if old is None:
                continue
            event = old
        elif old is None:
            event = replace(
                incoming,
                neg_risk_complete=False,
                neg_risk_conversion_supported=False,
                neg_risk_synced_at=None,
            )
        else:
            event = replace(
                incoming,
                status=old.status,
                neg_risk=old.neg_risk,
                neg_risk_id=old.neg_risk_id,
                neg_risk_type=old.neg_risk_type,
                neg_risk_complete=old.neg_risk_complete,
                neg_risk_conversion_supported=old.neg_risk_conversion_supported,
                neg_risk_metadata=old.neg_risk_metadata,
                neg_risk_synced_at=old.neg_risk_synced_at,
                resolved_at=old.resolved_at,
            )
        event_upserts[event_id] = replace(
            event,
            market_ids=tuple(market_ids),
            sync_generation=generation,
            sync_generation_complete=False,
            created_at=old.created_at if old is not None else occurred_at,
            updated_at=occurred_at,
        )

    # A linked new market is safe only when its parent event can be stored in
    # the same transaction. Existing parents are included above to preserve
    # dual-write; orphan markets have no parent prerequisite.
    permitted_event_ids = set(event_upserts)
    market_upserts = {
        market_id: market
        for market_id, market in market_upserts.items()
        if market.event_id is None or market.event_id in permitted_event_ids
    }
    permitted_market_ids = set(market_upserts)
    token_upserts = {
        token_id: token
        for token_id, token in token_upserts.items()
        if token.market_id in permitted_market_ids
    }
    return _PreparedCatalog(
        events=_ordered(event_upserts.values()),
        markets=_ordered(market_upserts.values()),
        tokens=_ordered(token_upserts.values()),
    )


def _catalog_changes(
    *,
    previous: CatalogSnapshot,
    events: dict[str, Event],
    markets: dict[str, Market],
    tokens: dict[str, Token],
    generation: str,
    occurred_at: int,
    published_market_ids: frozenset[str],
) -> tuple[MarketChange, ...]:
    old_events = {event.id: event for event in previous.events}
    old_markets = {market.id: market for market in previous.markets}
    old_tokens_by_market = _tokens_by_market(previous.tokens)
    tokens_by_market = _tokens_by_market(tokens.values())
    settled_event_ids = {
        event_id
        for event_id, event in events.items()
        if _event_settled(event)
        and (
            event_id not in old_events
            or not _event_settled(old_events[event_id])
        )
    }
    changes: list[MarketChange] = []
    for event_id in sorted(settled_event_ids, key=_utf8):
        event_token_ids = tuple(
            token.id
            for market in markets.values()
            if market.event_id == event_id
            for token in tokens_by_market[market.id]
        )
        if event_token_ids:
            changes.append(
                MarketChange(
                    change_id=(
                        f"{generation}:{MarketChangeType.EVENT_SETTLED.value}:"
                        f"{event_id}"
                    ),
                    change_type=MarketChangeType.EVENT_SETTLED,
                    event_id=event_id,
                    market_id=None,
                    token_ids=event_token_ids,
                    occurred_at=occurred_at,
                    critical=True,
                )
            )

    for market_id in sorted(markets, key=_utf8):
        market = markets[market_id]
        market_tokens = tokens_by_market[market_id]
        token_ids = tuple(token.id for token in market_tokens)
        if not token_ids or market.event_id in settled_event_ids:
            continue
        old = old_markets.get(market_id)
        old_watchable = old is not None and _watchable(old)
        watchable = _watchable(market)
        old_event = (
            None
            if old is None or old.event_id is None
            else old_events.get(old.event_id)
        )
        prior_generation_incomplete = old is not None and (
            not old.sync_generation_complete
            or (
                old_event is not None
                and not old_event.sync_generation_complete
            )
            or any(
                not token.sync_generation_complete
                for token in old_tokens_by_market[market_id]
            )
        )
        change_type: MarketChangeType | None = None
        critical = False
        if watchable and prior_generation_incomplete:
            if old_watchable and market_id in published_market_ids:
                change_type = MarketChangeType.MARKET_UPDATED
                critical = True
            else:
                change_type = MarketChangeType.MARKET_ADDED
        elif old is None and watchable:
            change_type = MarketChangeType.MARKET_ADDED
        elif old_watchable and not watchable:
            change_type = MarketChangeType.MARKET_DEACTIVATED
            critical = True
        elif not old_watchable and watchable:
            change_type = MarketChangeType.MARKET_ADDED
        elif watchable and old is not None:
            event = (
                None
                if market.event_id is None
                else events.get(market.event_id)
            )
            market_changed = _market_signature(old) != _market_signature(market)
            event_changed = (
                (old_event is None) != (event is None)
                or (
                    old_event is not None
                    and event is not None
                    and _event_signature(old_event) != _event_signature(event)
                )
            )
            token_changed = _token_signatures(
                old_tokens_by_market[market_id]
            ) != _token_signatures(market_tokens)
            if market_changed or event_changed or token_changed:
                change_type = MarketChangeType.MARKET_UPDATED
                event_critical_changed = (
                    (old_event is None) != (event is None)
                    or (
                        old_event is not None
                        and event is not None
                        and _event_critical_signature(old_event)
                        != _event_critical_signature(event)
                    )
                )
                critical = (
                    _market_critical_signature(old)
                    != _market_critical_signature(market)
                    or event_critical_changed
                    or token_changed
                )
        if change_type is not None:
            changes.append(
                MarketChange(
                    change_id=f"{generation}:{change_type.value}:{market_id}",
                    change_type=change_type,
                    event_id=market.event_id,
                    market_id=market_id,
                    token_ids=token_ids,
                    occurred_at=occurred_at,
                    critical=critical,
                )
            )
    return tuple(changes)


def _tokens_by_market(tokens: Iterable[Token]) -> dict[str, tuple[Token, ...]]:
    grouped: dict[str, list[Token]] = defaultdict(list)
    for token in tokens:
        grouped[token.market_id].append(token)
    return {
        market_id: tuple(sorted(values, key=lambda item: item.position))
        for market_id, values in grouped.items()
    }


def _token_signatures(tokens: Sequence[Token]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            token.id,
            token.market_id,
            token.outcome,
            token.position,
            token.fee_schedule,
            token.fee_updated_at,
        )
        for token in tokens
    )


def _market_signature(market: Market) -> tuple[Any, ...]:
    return (
        market.event_id,
        market.condition_id,
        market.slug,
        market.question,
        market.description,
        market.status,
        market.active,
        market.accepting_orders,
        market.enable_orderbook,
        market.neg_risk,
        market.neg_risk_outcome_position,
        market.neg_risk_member_complete,
        market.tick_size,
        market.minimum_order_size,
        market.end_at,
        market.resolved_at,
        market.source_updated_at,
    )


def _market_critical_signature(market: Market) -> tuple[Any, ...]:
    return (
        market.event_id,
        market.condition_id,
        market.status,
        market.active,
        market.accepting_orders,
        market.enable_orderbook,
        market.neg_risk,
        market.neg_risk_outcome_position,
        market.neg_risk_member_complete,
        market.tick_size,
        market.minimum_order_size,
        market.end_at,
        market.resolved_at,
    )


def _event_signature(event: Event) -> tuple[Any, ...]:
    return (
        event.slug,
        event.title,
        event.description,
        event.status,
        event.neg_risk,
        event.neg_risk_id,
        event.neg_risk_type,
        event.neg_risk_complete,
        event.neg_risk_conversion_supported,
        event.neg_risk_metadata,
        event.neg_risk_synced_at,
        event.start_at,
        event.end_at,
        event.resolved_at,
        event.source_updated_at,
    )


def _event_critical_signature(event: Event) -> tuple[Any, ...]:
    return (
        event.status,
        event.neg_risk,
        event.neg_risk_id,
        event.neg_risk_type,
        event.neg_risk_complete,
        event.neg_risk_conversion_supported,
        event.neg_risk_metadata,
        event.neg_risk_synced_at,
        event.end_at,
        event.resolved_at,
    )


def _watchable(market: Market) -> bool:
    return (
        market.status is MarketStatus.ACTIVE
        and market.active
        and market.accepting_orders
        and market.enable_orderbook
        and market.resolved_at is None
    )


def _event_settled(event: Event) -> bool:
    return event.status is MarketStatus.RESOLVED or event.resolved_at is not None


def _ordered(values: Iterable[T]) -> tuple[T, ...]:
    return tuple(sorted(values, key=lambda value: _utf8(value.id)))


def _utf8(value: str) -> bytes:
    return value.encode("utf-8")


def _error_text(prefix: str, error: Exception) -> str:
    detail = str(error).strip() or type(error).__name__
    return f"{prefix}: {detail}"
