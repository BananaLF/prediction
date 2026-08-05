"""Auditable signal lifecycle management."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
import inspect
import json
import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiosqlite

from predmarket.domain.decimal import encode_decimal
from predmarket.domain.market import Market, MarketStatus
from predmarket.domain.relation import Relation, RelationStatus
from predmarket.domain.signal import (
    DecisionReason,
    ExecutionMode,
    NotEvaluable,
    OpportunityAbsent,
    OpportunityPresent,
    SignalLeg,
    StrategyDecision,
    StrategyType,
)
from predmarket.persistence.repositories import SignalRepository


StateSource = Mapping[str, Any] | Callable[[str], Any] | None
_LOGGER = logging.getLogger(__name__)


class SignalRevisionConflict(RuntimeError):
    """The caller's revision was superseded before its transaction committed."""


class SubscriptionGenerationChanged(ValueError):
    """A strategy decision references subscription evidence that is no longer current."""


@dataclass(frozen=True, slots=True)
class SignalNotification:
    signal_id: str
    opportunity_key: str
    event_type: str
    revision: int
    decision: StrategyDecision


class SignalManager:
    """Apply strategy decisions to immutable, auditable signal lifecycles.

    The manager is intentionally configured with the strategy identity because
    the watch interface carries only a decision and an opportunity key.
    """

    def __init__(
        self,
        repository: SignalRepository,
        *,
        strategy_type: StrategyType,
        execution_mode: ExecutionMode,
        relation_id: str | None = None,
        market_state: StateSource = None,
        relation_state: StateSource = None,
        subscription_generation: Mapping[str, int] | Callable[[str], int | None] | None = None,
        notifier: Any = None,
        max_retries: int = 3,
    ) -> None:
        if not isinstance(repository, SignalRepository):
            raise TypeError("repository must be a SignalRepository")
        if not isinstance(strategy_type, StrategyType):
            raise TypeError("strategy_type must be a StrategyType")
        if not isinstance(execution_mode, ExecutionMode):
            raise TypeError("execution_mode must be an ExecutionMode")
        if strategy_type is StrategyType.LOGICAL_IMPLICATION:
            if relation_id is None or execution_mode is not ExecutionMode.HOLD_TO_RESOLUTION:
                raise ValueError("logical implication signals require an approved relation and hold mode")
        elif relation_id is not None or execution_mode is not ExecutionMode.IMMEDIATE_CONVERSION:
            raise ValueError("non-implication signals require immediate conversion and no relation")
        if type(max_retries) is not int or max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")
        self._repository = repository
        self._strategy_type = strategy_type
        self._execution_mode = execution_mode
        self._relation_id = relation_id
        self._market_state = market_state
        self._relation_state = relation_state
        self._subscription_generation = subscription_generation
        self._notifier = notifier
        self._max_retries = max_retries
        self._apply_lock = asyncio.Lock()

    async def apply(
        self,
        decision: StrategyDecision,
        opportunity_key: str,
        expected_revision: int | None,
        *,
        observed_at: int,
    ) -> str | None:
        """Persist one decision and return its signal ID, if one exists."""

        if not isinstance(decision, (OpportunityPresent, OpportunityAbsent, NotEvaluable)):
            raise TypeError("decision must be a StrategyDecision")
        if not isinstance(opportunity_key, str) or not opportunity_key:
            raise ValueError("opportunity_key must be a non-empty string")
        if expected_revision is not None and (
            type(expected_revision) is not int or expected_revision < 0
        ):
            raise ValueError("expected_revision must be a non-negative integer or None")
        if type(observed_at) is not int or observed_at < 0:
            raise ValueError("observed_at must be a non-negative integer")

        async with self._apply_lock:
            self._validate_external_state(decision)
            if not isinstance(decision, OpportunityPresent) and expected_revision is None:
                return None
            try:
                result = await self._repository._writer.execute(  # noqa: SLF001
                    lambda connection: self._apply_transaction(
                        connection,
                        decision=decision,
                        opportunity_key=opportunity_key,
                        expected_revision=expected_revision,
                        observed_at=observed_at,
                    )
                )
            except SignalRevisionConflict:
                # A decision was evaluated against an older snapshot.  It must
                # be re-evaluated by the watcher; retrying its payload could
                # overwrite newer evidence or resurrect a closed signal.
                return None
            if result is not None and result.event_type is not None:
                await self._notify_after_commit(result)
            return None if result is None else result.signal_id

    async def close_for_tokens(
        self,
        token_ids: Sequence[str],
        decision: StrategyDecision,
        *,
        observed_at: int,
    ) -> tuple[str, ...]:
        """Close open signals whose persisted trade legs reference any token."""

        if not isinstance(decision, (OpportunityAbsent, NotEvaluable)):
            raise TypeError("close_for_tokens requires a closing decision")
        if type(observed_at) is not int or observed_at < 0:
            raise ValueError("observed_at must be a non-negative integer")
        wanted = tuple(token_ids)
        if not wanted or any(not isinstance(token_id, str) or not token_id for token_id in wanted):
            raise ValueError("token_ids must contain non-empty strings")
        rows = await _read_all(
            self._repository._path,  # noqa: SLF001
            """
            SELECT DISTINCT s.opportunity_key, s.latest_revision
            FROM arbitrage_signals AS s
            JOIN signal_legs AS l ON l.signal_id = s.id AND l.revision = s.latest_revision
            WHERE s.status = 'OPEN' AND l.token_id IN ({})
            """.format(",".join("?" for _ in wanted)),
            wanted,
        )
        closed: list[str] = []
        for row in rows:
            signal_id = await self.apply(
                decision,
                str(row[0]),
                int(row[1]),
                observed_at=observed_at,
            )
            if signal_id is not None:
                closed.append(signal_id)
        return tuple(closed)

    async def close_unwatchable_for_active_tokens(
        self,
        active_token_ids: Sequence[str],
        *,
        observed_at: int,
    ) -> tuple[str, ...]:
        """Close persisted OPEN signals whose legs are absent from the catalog."""

        active = frozenset(active_token_ids)
        if type(observed_at) is not int or observed_at < 0:
            raise ValueError("observed_at must be a non-negative integer")
        rows = await _read_all(
            self._repository._path,  # noqa: SLF001
            """
            SELECT DISTINCT l.token_id
            FROM arbitrage_signals AS s
            JOIN signal_legs AS l ON l.signal_id = s.id AND l.revision = s.latest_revision
            WHERE s.status = 'OPEN'
            """,
            (),
        )
        unavailable = tuple(
            sorted(
                {str(row[0]) for row in rows if str(row[0]) not in active},
                key=lambda value: value.encode("utf-8"),
            )
        )
        if not unavailable:
            return ()
        return await self.close_for_tokens(
            unavailable,
            NotEvaluable(
                reason_code=DecisionReason.MARKET_CLOSED,
                context={
                    "detail": "startup catalog recovery found an unwatchable signal leg",
                    "token_ids": unavailable,
                },
            ),
            observed_at=observed_at,
        )

    async def _current_expected_revision(self, opportunity_key: str) -> int | None:
        signal_id = await self._repository.find_open_signal_id(opportunity_key)
        if signal_id is None:
            return None
        return await self._repository.get_latest_revision(signal_id)

    async def _apply_transaction(
        self,
        connection: aiosqlite.Connection,
        *,
        decision: StrategyDecision,
        opportunity_key: str,
        expected_revision: int | None,
        observed_at: int,
    ) -> SignalNotification | None:
        self._validate_external_state(decision)
        cursor = await connection.execute(
            """
            SELECT id, strategy_type, market_ids_json, relation_id, execution_mode,
                   latest_revision, status
            FROM arbitrage_signals
            WHERE opportunity_key = ? AND status = 'OPEN'
            ORDER BY opened_at, id
            LIMIT 1
            """,
            (opportunity_key,),
        )
        current = await cursor.fetchone()
        if current is not None:
            signal_id = str(current[0])
            latest_revision = int(current[5])
            if expected_revision is None or expected_revision != latest_revision:
                raise SignalRevisionConflict(
                    f"signal {signal_id} expected revision {expected_revision}, current {latest_revision}"
                )
            if isinstance(decision, OpportunityPresent):
                await self._validate_database_state(connection, decision)
                if await self._matches_latest(connection, signal_id, latest_revision, decision):
                    return SignalNotification(
                        signal_id, opportunity_key, "NOOP", latest_revision, decision
                    )
                revision = latest_revision + 1
                await self._cas_update(
                    connection,
                    signal_id,
                    latest_revision,
                    revision,
                    observed_at,
                    market_ids_json=_canonical_market_ids_json(decision),
                )
                await self._insert_revision_payload(
                    connection, signal_id, revision, "UPDATED", observed_at, decision
                )
                return SignalNotification(signal_id, opportunity_key, "UPDATED", revision, decision)

            revision = latest_revision + 1
            if isinstance(decision, OpportunityAbsent):
                await self._cas_update(
                    connection,
                    signal_id,
                    latest_revision,
                    revision,
                    observed_at,
                    close_reason=decision.reason_code,
                )
                await self._insert_revision_payload(
                    connection, signal_id, revision, "CLOSED", observed_at, decision
                )
            else:
                closure_context = self._closure_context(decision, latest_revision, observed_at)
                await self._cas_update(
                    connection,
                    signal_id,
                    latest_revision,
                    revision,
                    observed_at,
                    close_reason=decision.reason_code,
                )
                await self._insert_revision_payload(
                    connection,
                    signal_id,
                    revision,
                    "CLOSED",
                    observed_at,
                    decision,
                    closure_context=closure_context,
                )
            return SignalNotification(signal_id, opportunity_key, "CLOSED", revision, decision)

        if not isinstance(decision, OpportunityPresent):
            return None
        if expected_revision is not None:
            cursor = await connection.execute(
                """
                SELECT latest_revision
                FROM arbitrage_signals
                WHERE opportunity_key = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (opportunity_key,),
            )
            closed = await cursor.fetchone()
            if closed is not None and int(closed[0]) != expected_revision:
                # The OPEN row disappeared after this decision was evaluated.
                # Do not turn an old present decision into a new lifecycle.
                return None
        await self._validate_database_state(connection, decision)
        signal_id = uuid4().hex
        await connection.execute(
            """
            INSERT INTO arbitrage_signals (
                id, opportunity_key, strategy_type, market_ids_json, relation_id,
                execution_mode, status, opened_at, updated_at, latest_revision
            ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, 1)
            """,
            (
                signal_id,
                opportunity_key,
                self._strategy_type.value,
                _canonical_market_ids_json(decision),
                self._relation_id,
                self._execution_mode.value,
                observed_at,
                observed_at,
            ),
        )
        await self._insert_revision_payload(
            connection, signal_id, 1, "OPENED", observed_at, decision
        )
        return SignalNotification(signal_id, opportunity_key, "OPENED", 1, decision)

    def _validate_external_state(self, decision: StrategyDecision) -> None:
        if not isinstance(decision, (OpportunityPresent, OpportunityAbsent)):
            return
        for market_id in {leg.market_id for leg in decision.legs} | {
            book.market_id for book in decision.evidence
        }:
            state = _state_value(self._market_state, market_id)
            if state is not None and not _market_watchable(state):
                raise ValueError(f"market {market_id!r} is not watchable")
        for book in decision.evidence:
            generation = _generation_value(self._subscription_generation, book.token_id)
            if self._subscription_generation is not None and generation is None:
                raise SubscriptionGenerationChanged(
                    f"subscription generation is unavailable for {book.token_id!r}"
                )
            if generation is not None and generation != book.subscription_generation:
                raise SubscriptionGenerationChanged(
                    f"stale subscription generation for {book.token_id!r}"
                )
        if self._relation_id is not None:
            state = _state_value(self._relation_state, self._relation_id)
            if state is not None and not _relation_approved(state):
                raise ValueError("relation is not approved")

    async def _validate_database_state(
        self, connection: aiosqlite.Connection, decision: OpportunityPresent
    ) -> None:
        # Called inside the writer transaction; the database is the authoritative
        # commit-time state even when an external cache was supplied.
        market_ids = _canonical_ids(
            [leg.market_id for leg in decision.legs]
            + [book.market_id for book in decision.evidence]
        )
        cursor = await connection.execute(
            "SELECT id, status, active, accepting_orders, enable_orderbook, sync_generation_complete "
            "FROM markets WHERE id IN ({})".format(",".join("?" for _ in market_ids)),
            market_ids,
        )
        rows = await cursor.fetchall()
        if len(rows) != len(market_ids) or any(
            row[1] != MarketStatus.ACTIVE.value
            or not bool(row[2])
            or not bool(row[3])
            or not bool(row[4])
            or not bool(row[5])
            for row in rows
        ):
            raise ValueError("signal market is not watchable")
        if self._relation_id is not None:
            cursor = await connection.execute(
                "SELECT status FROM relations WHERE id = ?", (self._relation_id,)
            )
            row = await cursor.fetchone()
            if row is None or row[0] != RelationStatus.APPROVED.value:
                raise ValueError("relation is not approved")

    async def _cas_update(
        self,
        connection: aiosqlite.Connection,
        signal_id: str,
        expected_revision: int,
        revision: int,
        observed_at: int,
        *,
        close_reason: DecisionReason | None = None,
        market_ids_json: str | None = None,
    ) -> None:
        if close_reason is None:
            cursor = await connection.execute(
                """
                UPDATE arbitrage_signals
                SET updated_at = ?, latest_revision = ?, market_ids_json = ?
                WHERE id = ? AND status = 'OPEN' AND latest_revision = ?
                """,
                (
                    observed_at,
                    revision,
                    market_ids_json,
                    signal_id,
                    expected_revision,
                ),
            )
        else:
            cursor = await connection.execute(
                """
                UPDATE arbitrage_signals
                SET status = 'CLOSED', updated_at = ?, closed_at = ?,
                    close_reason = ?, latest_revision = ?
                WHERE id = ? AND status = 'OPEN' AND latest_revision = ?
                """,
                (
                    observed_at,
                    observed_at,
                    close_reason.value,
                    revision,
                    signal_id,
                    expected_revision,
                ),
            )
        if cursor.rowcount != 1:
            raise SignalRevisionConflict("signal CAS failed")

    async def _matches_latest(
        self,
        connection: aiosqlite.Connection,
        signal_id: str,
        revision: int,
        decision: OpportunityPresent,
    ) -> bool:
        cursor = await connection.execute(
            "SELECT quantity, total_capital, expected_profit, return_rate, worst_case_loss, "
            "risk_rate, unhedged_notional, risk_flags_json, calculation_json "
            "FROM signal_revisions WHERE signal_id = ? AND revision = ?",
            (signal_id, revision),
        )
        row = await cursor.fetchone()
        if row is None:
            return False
        calculation = decision.calculation
        expected = (
            encode_decimal(calculation.quantity),
            encode_decimal(calculation.total_capital),
            encode_decimal(calculation.expected_profit),
            encode_decimal(calculation.return_rate),
            encode_decimal(calculation.worst_case_loss),
            encode_decimal(calculation.risk_rate),
            encode_decimal(calculation.unhedged_notional),
            json.dumps(list(calculation.risk_flags), separators=(",", ":")),
            _json_object(calculation.details),
        )
        if tuple(row) != expected:
            return False
        return await self._matches_legs_and_books(connection, signal_id, revision, decision)

    async def _matches_legs_and_books(
        self,
        connection: aiosqlite.Connection,
        signal_id: str,
        revision: int,
        decision: OpportunityPresent,
    ) -> bool:
        cursor = await connection.execute(
            "SELECT position, market_id, token_id, action, side, quantity, average_price, "
            "worst_price, gross_amount, fee_amount FROM signal_legs "
            "WHERE signal_id = ? AND revision = ? ORDER BY position",
            (signal_id, revision),
        )
        expected_legs = tuple(
            (
                leg.position,
                leg.market_id,
                leg.token_id,
                leg.action.value,
                leg.side,
                encode_decimal(leg.quantity),
                _decimal_or_none(leg.average_price),
                _decimal_or_none(leg.worst_price),
                encode_decimal(leg.gross_amount),
                encode_decimal(leg.fee_amount),
            )
            for leg in decision.legs
        )
        if tuple(await cursor.fetchall()) != expected_legs:
            return False
        cursor = await connection.execute(
            "SELECT market_id, token_id, subscription_generation, book_hash, exchange_timestamp, "
            "received_timestamp, tick_size, minimum_order_size, id FROM orderbook_snapshots "
            "WHERE signal_id = ? AND revision = ? ORDER BY token_id",
            (signal_id, revision),
        )
        rows = await cursor.fetchall()
        if len(rows) != len(decision.evidence):
            return False
        for row, book in zip(rows, sorted(decision.evidence, key=lambda value: value.token_id)):
            if tuple(row[:-1]) != (
                book.market_id,
                book.token_id,
                book.subscription_generation,
                book.book_hash,
                book.exchange_timestamp,
                book.received_timestamp,
                encode_decimal(book.tick_size),
                encode_decimal(book.minimum_order_size),
            ):
                return False
        return True

    async def _insert_revision_payload(
        self,
        connection: aiosqlite.Connection,
        signal_id: str,
        revision: int,
        event_type: str,
        observed_at: int,
        decision: StrategyDecision,
        *,
        closure_context: Mapping[str, Any] | None = None,
    ) -> None:
        if isinstance(decision, NotEvaluable):
            values = (None,) * 7
            risk_flags = "[]"
            calculation_json = None
            closure_json = _json_object(closure_context or {})
        else:
            calculation = decision.calculation
            values = (
                encode_decimal(calculation.quantity),
                encode_decimal(calculation.total_capital),
                encode_decimal(calculation.expected_profit),
                encode_decimal(calculation.return_rate),
                encode_decimal(calculation.worst_case_loss),
                encode_decimal(calculation.risk_rate),
                encode_decimal(calculation.unhedged_notional),
            )
            risk_flags = json.dumps(list(calculation.risk_flags), separators=(",", ":"))
            calculation_json = _json_object(calculation.details)
            closure_json = None
        await connection.execute(
            """
            INSERT INTO signal_revisions (
                signal_id, revision, event_type, observed_at, quantity,
                total_capital, expected_profit, return_rate, worst_case_loss,
                risk_rate, unhedged_notional, risk_flags_json, calculation_json,
                closure_context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (signal_id, revision, event_type, observed_at, *values, risk_flags, calculation_json, closure_json),
        )
        if isinstance(decision, NotEvaluable):
            return
        for leg in decision.legs:
            await connection.execute(
                """
                INSERT INTO signal_legs (
                    signal_id, revision, position, market_id, token_id, action,
                    side, quantity, average_price, worst_price, gross_amount, fee_amount
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    revision,
                    leg.position,
                    leg.market_id,
                    leg.token_id,
                    leg.action.value,
                    leg.side,
                    encode_decimal(leg.quantity),
                    _decimal_or_none(leg.average_price),
                    _decimal_or_none(leg.worst_price),
                    encode_decimal(leg.gross_amount),
                    encode_decimal(leg.fee_amount),
                ),
            )
        for book in decision.evidence:
            snapshot_id = f"{len(signal_id)}:{signal_id}:{revision}:{book.token_id}"
            await connection.execute(
                """
                INSERT INTO orderbook_snapshots (
                    id, signal_id, revision, market_id, token_id,
                    subscription_generation, book_hash, exchange_timestamp,
                    received_timestamp, tick_size, minimum_order_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    signal_id,
                    revision,
                    book.market_id,
                    book.token_id,
                    book.subscription_generation,
                    book.book_hash,
                    book.exchange_timestamp,
                    book.received_timestamp,
                    encode_decimal(book.tick_size),
                    encode_decimal(book.minimum_order_size),
                ),
            )
            for side, levels in (("BID", book.bids), ("ASK", book.asks)):
                for position, level in enumerate(levels):
                    await connection.execute(
                        "INSERT INTO orderbook_levels (snapshot_id, side, position, price, size) VALUES (?, ?, ?, ?, ?)",
                        (snapshot_id, side, position, encode_decimal(level.price), encode_decimal(level.size)),
                    )

    @staticmethod
    def _closure_context(
        decision: NotEvaluable, last_valid_revision: int, observed_at: int
    ) -> dict[str, Any]:
        context = dict(decision.context)
        context.update(
            {
                "reason_code": decision.reason_code.value,
                "last_valid_revision": last_valid_revision,
                "invalidation_observed_at": observed_at,
            }
        )
        return context

    async def _notify_after_commit(self, notification: SignalNotification) -> None:
        if notification.event_type == "NOOP":
            return
        _LOGGER.info(
            "signal_transition signal_id=%s opportunity_key=%s event_type=%s "
            "revision=%d strategy_type=%s",
            notification.signal_id,
            notification.opportunity_key,
            notification.event_type,
            notification.revision,
            self._strategy_type.value,
        )
        if self._notifier is None:
            return
        callback = getattr(self._notifier, "notify", self._notifier)
        try:
            result = _invoke_notifier(callback, notification)
            if inspect.isawaitable(result):
                await result
        except Exception:
            # Notification is deliberately outside the database transaction.
            _LOGGER.exception(
                "signal_notification_failed signal_id=%s event_type=%s",
                notification.signal_id,
                notification.event_type,
            )
            return


def _canonical_ids(values: Sequence[str]) -> tuple[str, ...]:
    if not values or any(not isinstance(value, str) or not value for value in values):
        raise ValueError("market_ids must contain non-empty strings")
    return tuple(sorted(set(values), key=lambda value: value.encode("utf-8")))


def _canonical_market_ids_json(decision: OpportunityPresent) -> str:
    market_ids = _canonical_ids(
        [leg.market_id for leg in decision.legs]
        + [book.market_id for book in decision.evidence]
    )
    return json.dumps(market_ids, ensure_ascii=False, separators=(",", ":"))


def _json_object(value: Mapping[str, Any]) -> str:
    return json.dumps(
        _thaw_json(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else encode_decimal(value)


def _state_value(source: StateSource, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(key)
    return source(key)


def _generation_value(source: Mapping[str, int] | Callable[[str], int | None] | None, key: str) -> int | None:
    if source is None:
        return None
    return source.get(key) if isinstance(source, Mapping) else source(key)


def _market_watchable(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, Market):
        return (
            value.status is MarketStatus.ACTIVE
            and value.active
            and value.accepting_orders
            and value.enable_orderbook
            and value.sync_generation_complete
        )
    return False


def _relation_approved(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, Relation):
        return value.status is RelationStatus.APPROVED
    return False


async def _read_all(path: Path, query: str, parameters: Sequence[Any]) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(path) as connection:
        cursor = await connection.execute(query, tuple(parameters))
        return await cursor.fetchall()


def _invoke_notifier(callback: Callable[..., Any], notification: SignalNotification) -> Any:
    try:
        parameters = inspect.signature(callback).parameters
    except (TypeError, ValueError):
        return callback(notification)
    positional = [
        parameter
        for parameter in parameters.values()
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) <= 1:
        return callback(notification)
    if len(positional) == 2:
        return callback(notification.signal_id, notification.decision)
    return callback(
        notification.signal_id,
        notification.opportunity_key,
        notification.decision,
    )
