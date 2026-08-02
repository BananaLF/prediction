"""Typed SQL boundaries backed by the single runtime writer."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

import aiosqlite

from predmarket.catalog.changes import MarketChange, MarketChangeType
from predmarket.catalog.relations import (
    capture_relation_semantics,
    relation_with_semantic_context,
    semantic_evidence_digest,
    validate_semantic_digest,
)
from predmarket.domain.decimal import encode_decimal
from predmarket.domain.fees import FeeSchedule
from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.relation import DiscoverySource, Relation, RelationStatus
from predmarket.domain.signal import (
    ExecutionMode,
    OpportunityPresent,
    StrategyType,
)
from predmarket.persistence.writer import DatabaseWriter


@dataclass(frozen=True, slots=True)
class CatalogSnapshot:
    events: tuple[Event, ...]
    markets: tuple[Market, ...]
    tokens: tuple[Token, ...]


class CatalogRepository:
    def __init__(self, path: Path, writer: DatabaseWriter) -> None:
        self._path = Path(path)
        self._writer = writer

    async def save_catalog(
        self,
        *,
        events: Sequence[Event],
        markets: Sequence[Market],
        tokens: Sequence[Token],
    ) -> None:
        materialized_events = _typed_tuple(events, Event, "events")
        materialized_markets = _typed_tuple(markets, Market, "markets")
        materialized_tokens = _typed_tuple(tokens, Token, "tokens")

        async def command(connection: aiosqlite.Connection) -> None:
            affected_event_ids = {event.id for event in materialized_events}
            for market in materialized_markets:
                affected_event_ids.add(market.event_id)
                cursor = await connection.execute(
                    "SELECT event_id FROM markets WHERE id = ?",
                    (market.id,),
                )
                row = await cursor.fetchone()
                if row is not None:
                    affected_event_ids.add(row[0])
            for event in materialized_events:
                await connection.execute(_UPSERT_EVENT, _event_values(event))
            for market in materialized_markets:
                await connection.execute(_UPSERT_MARKET, _market_values(market))
            for token in materialized_tokens:
                await connection.execute(_UPSERT_TOKEN, _token_values(token))
            for event_id in sorted(
                affected_event_ids,
                key=lambda value: value.encode("utf-8"),
            ):
                await _validate_stored_event_markets(connection, event_id)

        await self._writer.execute(command)

    async def save_event(self, event: Event) -> None:
        _require_type(event, Event, "event")

        async def command(connection: aiosqlite.Connection) -> None:
            await connection.execute(_UPSERT_EVENT, _event_values(event))
            await _validate_event_markets(connection, event.id, event.market_ids)

        await self._writer.execute(command)

    async def save_market(self, market: Market) -> None:
        _require_type(market, Market, "market")

        async def command(connection: aiosqlite.Connection) -> None:
            await connection.execute(_UPSERT_MARKET, _market_values(market))
            cursor = await connection.execute(
                "SELECT market_ids_json FROM events WHERE id = ?",
                (market.event_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError(f"event {market.event_id!r} does not exist")
            expected = tuple(json.loads(row[0]))
            await _validate_event_markets(connection, market.event_id, expected)

        await self._writer.execute(command)

    async def save_token(self, token: Token) -> None:
        _require_type(token, Token, "token")
        await self._writer.execute(
            lambda connection: connection.execute(_UPSERT_TOKEN, _token_values(token))
        )

    async def get_event(self, event_id: str) -> Event | None:
        row = await _fetch_one(self._path, "SELECT * FROM events WHERE id = ?", (event_id,))
        return None if row is None else _event_from_row(row)

    async def get_market(self, market_id: str) -> Market | None:
        row = await _fetch_one(
            self._path,
            "SELECT * FROM markets WHERE id = ?",
            (market_id,),
        )
        return None if row is None else _market_from_row(row)

    async def get_token(self, token_id: str) -> Token | None:
        row = await _fetch_one(
            self._path,
            "SELECT * FROM tokens WHERE id = ?",
            (token_id,),
        )
        return None if row is None else _token_from_row(row)

    async def load_catalog(self) -> CatalogSnapshot:
        """Read a consistent catalog view for one sync diff."""

        async with aiosqlite.connect(
            self._path,
            isolation_level=None,
        ) as connection:
            connection.row_factory = aiosqlite.Row
            await connection.execute("PRAGMA query_only = ON")
            await connection.execute("BEGIN")
            try:
                event_cursor = await connection.execute(
                    "SELECT * FROM events ORDER BY CAST(id AS BLOB)"
                )
                market_cursor = await connection.execute(
                    "SELECT * FROM markets ORDER BY CAST(id AS BLOB)"
                )
                token_cursor = await connection.execute(
                    "SELECT * FROM tokens ORDER BY CAST(id AS BLOB)"
                )
                event_rows = await event_cursor.fetchall()
                market_rows = await market_cursor.fetchall()
                token_rows = await token_cursor.fetchall()
            finally:
                await connection.execute("ROLLBACK")
        return CatalogSnapshot(
            events=tuple(_event_from_row(row) for row in event_rows),
            markets=tuple(_market_from_row(row) for row in market_rows),
            tokens=tuple(_token_from_row(row) for row in token_rows),
        )


class RelationRepository:
    def __init__(self, path: Path, writer: DatabaseWriter) -> None:
        self._path = Path(path)
        self._writer = writer

    async def save(self, relation: Relation) -> None:
        """Persist a newly discovered, unreviewed relation.

        Repeated discovery is idempotent and can never regress an analyzed or
        manually approved row. LLM analysis has a separate write boundary;
        manual approval belongs to the independent relations CLI.
        """

        _require_type(relation, Relation, "relation")
        if relation.status is not RelationStatus.NO_LLM_APPROVE:
            raise ValueError("discovered relation status must be NO_LLM_APPROVE")
        if relation.llm_confidence is not None or relation.llm_analysis is not None:
            raise ValueError("discovered relation must not contain LLM analysis")

        async def command(connection: aiosqlite.Connection) -> None:
            await connection.execute(
                """
                INSERT INTO relations (
                    id, market_a_id, market_b_id, status, discovery_source,
                    llm_confidence, llm_analysis_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_a_id, market_b_id) DO NOTHING
                """,
                _relation_values(relation),
            )
            cursor = await connection.execute(
                """
                SELECT market_a_id, market_b_id
                FROM relations WHERE id = ?
                """,
                (relation.id,),
            )
            row = await cursor.fetchone()
            if row is None or (row[0], row[1]) != (
                relation.market_a_id,
                relation.market_b_id,
            ):
                raise ValueError("relation ID conflicts with another implication")

        await self._writer.execute(command)

    async def save_analysis(
        self,
        relation: Relation,
        *,
        expected_semantic_digest: str,
    ) -> Relation:
        """Persist one analyzer result without crossing the manual gate."""

        validate_semantic_digest(expected_semantic_digest)
        _require_type(relation, Relation, "relation")
        if relation.status not in {
            RelationStatus.NO_LLM_APPROVE,
            RelationStatus.LLM_APPROVE,
        }:
            raise ValueError("analysis cannot set relation status APPROVED")
        if relation.llm_confidence is None or relation.llm_analysis is None:
            raise ValueError("analysis result is required")
        approved = relation.llm_analysis.get("approved")
        if type(approved) is not bool:
            raise ValueError("analysis approved decision must be a boolean")
        expected_status = (
            RelationStatus.LLM_APPROVE
            if approved
            else RelationStatus.NO_LLM_APPROVE
        )
        if relation.status is not expected_status:
            raise ValueError("analysis decision does not match relation status")

        async def command(connection: aiosqlite.Connection) -> Relation:
            cursor = await connection.execute(
                """
                SELECT market_a_id, market_b_id, status, discovery_source,
                       created_at, updated_at
                FROM relations WHERE id = ?
                """,
                (relation.id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError(f"relation {relation.id!r} does not exist")
            if RelationStatus(row[2]) is not RelationStatus.NO_LLM_APPROVE:
                raise ValueError("analysis requires relation status NO_LLM_APPROVE")
            if (
                row[0] != relation.market_a_id
                or row[1] != relation.market_b_id
                or DiscoverySource(row[3]) is not relation.discovery_source
                or row[4] != relation.created_at
            ):
                raise ValueError("analysis cannot change relation identity")
            if relation.updated_at < row[5]:
                raise ValueError("analysis updated_at must not move backwards")
            provided_digest = semantic_evidence_digest(relation)
            if provided_digest != expected_semantic_digest:
                raise ValueError("analysis semantic evidence does not match expected digest")
            semantics = await capture_relation_semantics(
                connection,
                relation.market_a_id,
                relation.market_b_id,
            )
            current_digest = semantic_evidence_digest(
                relation_with_semantic_context(relation, semantics)
            )
            if current_digest != expected_semantic_digest:
                raise ValueError("relation semantics changed during analysis")
            analysis = relation.llm_analysis
            updated = await connection.execute(
                """
                UPDATE relations
                SET status = ?, llm_confidence = ?, llm_analysis_json = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'NO_LLM_APPROVE'
                """,
                (
                    relation.status.value,
                    encode_decimal(relation.llm_confidence),
                    _encode_json_object(analysis),
                    relation.updated_at,
                    relation.id,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError("relation changed concurrently during analysis")
            return replace(relation, llm_analysis=analysis)

        return await self._writer.execute(command)

    async def get(self, relation_id: str) -> Relation | None:
        row = await _fetch_one(
            self._path,
            "SELECT * FROM relations WHERE id = ?",
            (relation_id,),
        )
        return None if row is None else _relation_from_row(row)

    async def get_for_analysis(self, relation_id: str) -> Relation | None:
        async with aiosqlite.connect(self._path, isolation_level=None) as connection:
            connection.row_factory = aiosqlite.Row
            await connection.execute("PRAGMA query_only = ON")
            await connection.execute("BEGIN")
            try:
                cursor = await connection.execute(
                    "SELECT * FROM relations WHERE id = ?",
                    (relation_id,),
                )
                row = await cursor.fetchone()
                if row is None:
                    return None
                relation = _relation_from_row(row)
                semantics = await capture_relation_semantics(
                    connection,
                    relation.market_a_id,
                    relation.market_b_id,
                )
                return relation_with_semantic_context(relation, semantics)
            finally:
                await connection.execute("ROLLBACK")

    async def list_approved(self) -> tuple[Relation, ...]:
        rows = await _fetch_all(
            self._path,
            """
            SELECT * FROM relations
            WHERE status = 'APPROVED'
            ORDER BY CAST(id AS BLOB)
            """,
        )
        return tuple(_relation_from_row(row) for row in rows)


class SignalRepository:
    """Atomic signal persistence plus short independent read queries."""

    def __init__(self, path: Path, writer: DatabaseWriter) -> None:
        self._path = Path(path)
        self._writer = writer

    async def open_signal(
        self,
        *,
        signal_id: str,
        opportunity_key: str,
        strategy_type: StrategyType,
        market_ids: Sequence[str],
        relation_id: str | None,
        execution_mode: ExecutionMode,
        observed_at: int,
        decision: OpportunityPresent,
    ) -> None:
        if not isinstance(signal_id, str) or not signal_id:
            raise ValueError("signal_id must be a non-empty string")
        if not isinstance(opportunity_key, str) or not opportunity_key:
            raise ValueError("opportunity_key must be a non-empty string")
        _require_type(strategy_type, StrategyType, "strategy_type")
        _require_type(execution_mode, ExecutionMode, "execution_mode")
        _require_type(decision, OpportunityPresent, "decision")
        if type(observed_at) is not int or observed_at < 0:
            raise ValueError("observed_at must be a non-negative integer")
        encoded_market_ids = _encode_ids(market_ids)
        canonical_market_ids = tuple(json.loads(encoded_market_ids))
        referenced_markets = {
            leg.market_id for leg in decision.legs
        } | {
            book.market_id for book in decision.evidence
        }
        if not referenced_markets.issubset(set(canonical_market_ids)):
            raise ValueError("signal evidence references a market outside market_ids")

        async def command(connection: aiosqlite.Connection) -> None:
            placeholders = ",".join("?" for _ in canonical_market_ids)
            cursor = await connection.execute(
                f"SELECT id FROM markets WHERE id IN ({placeholders})",
                canonical_market_ids,
            )
            existing = {row[0] for row in await cursor.fetchall()}
            if existing != set(canonical_market_ids):
                raise ValueError("signal market_ids reference a missing market")

            await connection.execute(
                """
                INSERT INTO arbitrage_signals (
                    id, opportunity_key, strategy_type, market_ids_json,
                    relation_id, execution_mode, status, opened_at, updated_at,
                    latest_revision
                ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, 1)
                """,
                (
                    signal_id,
                    opportunity_key,
                    strategy_type.value,
                    encoded_market_ids,
                    relation_id,
                    execution_mode.value,
                    observed_at,
                    observed_at,
                ),
            )
            calculation = decision.calculation
            await connection.execute(
                """
                INSERT INTO signal_revisions (
                    signal_id, revision, event_type, observed_at, quantity,
                    total_capital, expected_profit, return_rate,
                    worst_case_loss, risk_rate, unhedged_notional,
                    risk_flags_json, calculation_json, closure_context_json
                ) VALUES (?, 1, 'OPENED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    signal_id,
                    observed_at,
                    encode_decimal(calculation.quantity),
                    encode_decimal(calculation.total_capital),
                    encode_decimal(calculation.expected_profit),
                    encode_decimal(calculation.return_rate),
                    encode_decimal(calculation.worst_case_loss),
                    encode_decimal(calculation.risk_rate),
                    encode_decimal(calculation.unhedged_notional),
                    json.dumps(
                        list(calculation.risk_flags),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    _encode_json_object(calculation.details),
                ),
            )
            for leg in decision.legs:
                await connection.execute(
                    """
                    INSERT INTO signal_legs (
                        signal_id, revision, position, market_id, token_id,
                        action, side, quantity, average_price, worst_price,
                        gross_amount, fee_amount
                    ) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal_id,
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
                snapshot_id = (
                    f"{len(signal_id)}:{signal_id}:1:{book.token_id}"
                )
                await connection.execute(
                    """
                    INSERT INTO orderbook_snapshots (
                        id, signal_id, revision, market_id, token_id,
                        subscription_generation, book_hash,
                        exchange_timestamp, received_timestamp, tick_size,
                        minimum_order_size
                    ) VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        signal_id,
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
                            """
                            INSERT INTO orderbook_levels (
                                snapshot_id, side, position, price, size
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                snapshot_id,
                                side,
                                position,
                                encode_decimal(level.price),
                                encode_decimal(level.size),
                            ),
                        )

        await self._writer.execute(command)

    async def get_latest_revision(self, signal_id: str) -> int | None:
        row = await _fetch_one(
            self._path,
            "SELECT latest_revision FROM arbitrage_signals WHERE id = ?",
            (signal_id,),
        )
        return None if row is None else int(row["latest_revision"])

    async def find_open_signal_id(self, opportunity_key: str) -> str | None:
        row = await _fetch_one(
            self._path,
            """
            SELECT id FROM arbitrage_signals
            WHERE opportunity_key = ? AND status = 'OPEN'
            """,
            (opportunity_key,),
        )
        return None if row is None else str(row["id"])


class SystemEventRepository:
    def __init__(self, path: Path, writer: DatabaseWriter) -> None:
        self._path = Path(path)
        self._writer = writer

    async def append(
        self,
        *,
        component: str,
        severity: str,
        event_type: str,
        message: str,
        occurred_at: int,
        details: Mapping[str, Any] | None = None,
    ) -> int:
        encoded_details = (
            None if details is None else _encode_json_object(details)
        )

        async def command(connection: aiosqlite.Connection) -> int:
            cursor = await connection.execute(
                """
                INSERT INTO system_events (
                    component, severity, event_type, message,
                    details_json, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    component,
                    severity,
                    event_type,
                    message,
                    encoded_details,
                    occurred_at,
                ),
            )
            assert cursor.lastrowid is not None
            return int(cursor.lastrowid)

        return await self._writer.execute(command)

    async def record_market_change_published(
        self,
        change: MarketChange,
        *,
        market_ids: Sequence[str] | None = None,
    ) -> int:
        """Idempotently record an admitted Watch publication baseline."""

        if not isinstance(change, MarketChange):
            raise TypeError("change must be a MarketChange")
        active = change.change_type in {
            MarketChangeType.MARKET_ADDED,
            MarketChangeType.MARKET_UPDATED,
        }
        if change.market_id is not None:
            affected_market_ids = (change.market_id,)
        else:
            if market_ids is None:
                raise ValueError("event-wide publication requires market_ids")
            affected_market_ids = tuple(market_ids)
        encoded_market_ids = json.loads(_encode_ids(affected_market_ids))
        details = _encode_json_object(
            {
                "active": active,
                "change_id": change.change_id,
                "change_type": change.change_type.value,
                "event_id": change.event_id,
                "market_id": change.market_id,
                "market_ids": encoded_market_ids,
                "sync_generation": change.change_id.rsplit(":", 2)[0],
                "token_ids": change.token_ids,
            }
        )

        async def command(connection: aiosqlite.Connection) -> int:
            cursor = await connection.execute(
                """
                SELECT id FROM system_events
                WHERE event_type = 'MARKET_CHANGE_PUBLISHED'
                  AND json_extract(details_json, '$.change_id') = ?
                ORDER BY id
                LIMIT 1
                """,
                (change.change_id,),
            )
            row = await cursor.fetchone()
            if row is not None:
                return int(row[0])
            cursor = await connection.execute(
                """
                INSERT INTO system_events (
                    component, severity, event_type, message,
                    details_json, occurred_at
                ) VALUES ('SYNC', 'INFO', 'MARKET_CHANGE_PUBLISHED', ?, ?, ?)
                """,
                (
                    f"Market change {change.change_id} admitted to Watch queue",
                    details,
                    change.occurred_at,
                ),
            )
            assert cursor.lastrowid is not None
            return int(cursor.lastrowid)

        return await self._writer.execute(command)

    async def list_published_market_ids(self) -> frozenset[str]:
        rows = await _fetch_all(
            self._path,
            """
            SELECT details_json
            FROM system_events
            WHERE event_type = 'MARKET_CHANGE_PUBLISHED'
            ORDER BY id
            """,
        )
        active_market_ids: set[str] = set()
        for row in rows:
            details = json.loads(row["details_json"])
            affected = details.get("market_ids")
            if affected is None and details.get("market_id") is not None:
                affected = [details["market_id"]]
            if not isinstance(affected, list):
                continue
            is_active = details.get("active", True)
            for market_id in affected:
                if not isinstance(market_id, str) or not market_id:
                    continue
                if is_active is True:
                    active_market_ids.add(market_id)
                else:
                    active_market_ids.discard(market_id)
        return frozenset(active_market_ids)

    async def get_settlement_refresh_cursor(self) -> str | None:
        row = await _fetch_one(
            self._path,
            """
            SELECT json_extract(details_json, '$.cursor') AS cursor
            FROM system_events
            WHERE event_type = 'SETTLEMENT_REFRESH_CURSOR'
            ORDER BY id DESC
            LIMIT 1
            """,
            (),
        )
        return None if row is None else str(row["cursor"])

    async def record_settlement_refresh_cursor(
        self,
        *,
        sync_generation: str,
        cursor: str,
        occurred_at: int,
    ) -> int:
        details = _encode_json_object(
            {"cursor": cursor, "sync_generation": sync_generation}
        )

        async def command(connection: aiosqlite.Connection) -> int:
            existing = await connection.execute(
                """
                SELECT id FROM system_events
                WHERE event_type = 'SETTLEMENT_REFRESH_CURSOR'
                  AND json_extract(details_json, '$.sync_generation') = ?
                LIMIT 1
                """,
                (sync_generation,),
            )
            row = await existing.fetchone()
            if row is not None:
                return int(row[0])
            inserted = await connection.execute(
                """
                INSERT INTO system_events (
                    component, severity, event_type, message,
                    details_json, occurred_at
                ) VALUES ('SYNC', 'INFO', 'SETTLEMENT_REFRESH_CURSOR', ?, ?, ?)
                """,
                (
                    f"Settlement refresh advanced through {cursor}",
                    details,
                    occurred_at,
                ),
            )
            assert inserted.lastrowid is not None
            return int(inserted.lastrowid)

        return await self._writer.execute(command)

    async def read_after(
        self,
        event_id: int,
        *,
        limit: int = 1_000,
    ) -> tuple[dict[str, Any], ...]:
        if type(event_id) is not int or event_id < 0:
            raise ValueError("event_id must be a non-negative integer")
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be a positive integer")
        rows = await _fetch_all(
            self._path,
            """
            SELECT id, component, severity, event_type, message,
                   details_json, occurred_at
            FROM system_events
            WHERE id > ?
            ORDER BY id
            LIMIT ?
            """,
            (event_id, limit),
        )
        return tuple(
            {
                "id": int(row["id"]),
                "component": str(row["component"]),
                "severity": str(row["severity"]),
                "event_type": str(row["event_type"]),
                "message": str(row["message"]),
                "details": (
                    None
                    if row["details_json"] is None
                    else json.loads(row["details_json"])
                ),
                "occurred_at": int(row["occurred_at"]),
            }
            for row in rows
        )


_UPSERT_EVENT = """
INSERT INTO events (
    id, slug, title, description, status, neg_risk, neg_risk_id,
    neg_risk_type, neg_risk_complete, neg_risk_conversion_supported,
    neg_risk_metadata_json, neg_risk_synced_at, market_ids_json,
    sync_generation, sync_generation_complete, start_at, end_at,
    resolved_at, source_updated_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    slug = excluded.slug,
    title = excluded.title,
    description = excluded.description,
    status = excluded.status,
    neg_risk = excluded.neg_risk,
    neg_risk_id = excluded.neg_risk_id,
    neg_risk_type = excluded.neg_risk_type,
    neg_risk_complete = excluded.neg_risk_complete,
    neg_risk_conversion_supported = excluded.neg_risk_conversion_supported,
    neg_risk_metadata_json = excluded.neg_risk_metadata_json,
    neg_risk_synced_at = excluded.neg_risk_synced_at,
    market_ids_json = excluded.market_ids_json,
    sync_generation = excluded.sync_generation,
    sync_generation_complete = excluded.sync_generation_complete,
    start_at = excluded.start_at,
    end_at = excluded.end_at,
    resolved_at = excluded.resolved_at,
    source_updated_at = excluded.source_updated_at,
    updated_at = excluded.updated_at
"""

_UPSERT_MARKET = """
INSERT INTO markets (
    id, event_id, condition_id, slug, question, description, status,
    active, accepting_orders, enable_orderbook, neg_risk,
    neg_risk_outcome_position, neg_risk_member_complete, sync_generation,
    sync_generation_complete, tick_size, minimum_order_size, end_at,
    resolved_at, source_updated_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    event_id = excluded.event_id,
    condition_id = excluded.condition_id,
    slug = excluded.slug,
    question = excluded.question,
    description = excluded.description,
    status = excluded.status,
    active = excluded.active,
    accepting_orders = excluded.accepting_orders,
    enable_orderbook = excluded.enable_orderbook,
    neg_risk = excluded.neg_risk,
    neg_risk_outcome_position = excluded.neg_risk_outcome_position,
    neg_risk_member_complete = excluded.neg_risk_member_complete,
    sync_generation = excluded.sync_generation,
    sync_generation_complete = excluded.sync_generation_complete,
    tick_size = excluded.tick_size,
    minimum_order_size = excluded.minimum_order_size,
    end_at = excluded.end_at,
    resolved_at = excluded.resolved_at,
    source_updated_at = excluded.source_updated_at,
    updated_at = excluded.updated_at
"""

_UPSERT_TOKEN = """
INSERT INTO tokens (
    id, market_id, outcome, position, fee_schedule_json, fee_updated_at,
    sync_generation, sync_generation_complete, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    market_id = excluded.market_id,
    outcome = excluded.outcome,
    position = excluded.position,
    fee_schedule_json = excluded.fee_schedule_json,
    fee_updated_at = excluded.fee_updated_at,
    sync_generation = excluded.sync_generation,
    sync_generation_complete = excluded.sync_generation_complete,
    updated_at = excluded.updated_at
"""

def _event_values(event: Event) -> tuple[Any, ...]:
    return (
        event.id,
        event.slug,
        event.title,
        event.description,
        event.status.value,
        int(event.neg_risk),
        event.neg_risk_id,
        event.neg_risk_type,
        int(event.neg_risk_complete),
        int(event.neg_risk_conversion_supported),
        (
            None
            if event.neg_risk_metadata is None
            else _encode_json_object(event.neg_risk_metadata)
        ),
        event.neg_risk_synced_at,
        _encode_ids(event.market_ids),
        event.sync_generation,
        int(event.sync_generation_complete),
        event.start_at,
        event.end_at,
        event.resolved_at,
        event.source_updated_at,
        event.created_at,
        event.updated_at,
    )


def _market_values(market: Market) -> tuple[Any, ...]:
    return (
        market.id,
        market.event_id,
        market.condition_id,
        market.slug,
        market.question,
        market.description,
        market.status.value,
        int(market.active),
        int(market.accepting_orders),
        int(market.enable_orderbook),
        int(market.neg_risk),
        market.neg_risk_outcome_position,
        int(market.neg_risk_member_complete),
        market.sync_generation,
        int(market.sync_generation_complete),
        None if market.tick_size is None else encode_decimal(market.tick_size),
        (
            None
            if market.minimum_order_size is None
            else encode_decimal(market.minimum_order_size)
        ),
        market.end_at,
        market.resolved_at,
        market.source_updated_at,
        market.created_at,
        market.updated_at,
    )


def _token_values(token: Token) -> tuple[Any, ...]:
    return (
        token.id,
        token.market_id,
        token.outcome,
        token.position,
        (
            None
            if token.fee_schedule is None
            else _encode_fee_schedule(token.fee_schedule)
        ),
        token.fee_updated_at,
        token.sync_generation,
        int(token.sync_generation_complete),
        token.created_at,
        token.updated_at,
    )


def _relation_values(relation: Relation) -> tuple[Any, ...]:
    return (
        relation.id,
        relation.market_a_id,
        relation.market_b_id,
        relation.status.value,
        relation.discovery_source.value,
        (
            None
            if relation.llm_confidence is None
            else encode_decimal(relation.llm_confidence)
        ),
        (
            None
            if relation.llm_analysis is None
            else _encode_json_object(relation.llm_analysis)
        ),
        relation.created_at,
        relation.updated_at,
    )


def _event_from_row(row: aiosqlite.Row) -> Event:
    metadata = (
        None
        if row["neg_risk_metadata_json"] is None
        else json.loads(row["neg_risk_metadata_json"])
    )
    return Event(
        id=row["id"],
        slug=row["slug"],
        title=row["title"],
        description=row["description"],
        status=MarketStatus(row["status"]),
        neg_risk=bool(row["neg_risk"]),
        neg_risk_id=row["neg_risk_id"],
        neg_risk_type=row["neg_risk_type"],
        neg_risk_complete=bool(row["neg_risk_complete"]),
        neg_risk_conversion_supported=bool(row["neg_risk_conversion_supported"]),
        neg_risk_metadata=metadata,
        neg_risk_synced_at=row["neg_risk_synced_at"],
        market_ids=tuple(json.loads(row["market_ids_json"])),
        sync_generation=row["sync_generation"],
        sync_generation_complete=bool(row["sync_generation_complete"]),
        start_at=row["start_at"],
        end_at=row["end_at"],
        resolved_at=row["resolved_at"],
        source_updated_at=row["source_updated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _market_from_row(row: aiosqlite.Row) -> Market:
    return Market(
        id=row["id"],
        event_id=row["event_id"],
        condition_id=row["condition_id"],
        slug=row["slug"],
        question=row["question"],
        description=row["description"],
        status=MarketStatus(row["status"]),
        active=bool(row["active"]),
        accepting_orders=bool(row["accepting_orders"]),
        enable_orderbook=bool(row["enable_orderbook"]),
        neg_risk=bool(row["neg_risk"]),
        neg_risk_outcome_position=row["neg_risk_outcome_position"],
        neg_risk_member_complete=bool(row["neg_risk_member_complete"]),
        sync_generation=row["sync_generation"],
        sync_generation_complete=bool(row["sync_generation_complete"]),
        tick_size=(
            None if row["tick_size"] is None else Decimal(row["tick_size"])
        ),
        minimum_order_size=(
            None
            if row["minimum_order_size"] is None
            else Decimal(row["minimum_order_size"])
        ),
        end_at=row["end_at"],
        resolved_at=row["resolved_at"],
        source_updated_at=row["source_updated_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _token_from_row(row: aiosqlite.Row) -> Token:
    schedule = (
        None
        if row["fee_schedule_json"] is None
        else FeeSchedule.from_json(json.loads(row["fee_schedule_json"]))
    )
    return Token(
        id=row["id"],
        market_id=row["market_id"],
        outcome=row["outcome"],
        position=row["position"],
        fee_schedule=schedule,
        fee_updated_at=row["fee_updated_at"],
        sync_generation=row["sync_generation"],
        sync_generation_complete=bool(row["sync_generation_complete"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _relation_from_row(row: aiosqlite.Row) -> Relation:
    return Relation(
        id=row["id"],
        market_a_id=row["market_a_id"],
        market_b_id=row["market_b_id"],
        status=RelationStatus(row["status"]),
        discovery_source=DiscoverySource(row["discovery_source"]),
        llm_confidence=(
            None
            if row["llm_confidence"] is None
            else Decimal(row["llm_confidence"])
        ),
        llm_analysis=(
            None
            if row["llm_analysis_json"] is None
            else json.loads(row["llm_analysis_json"])
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _encode_ids(values: Sequence[str]) -> str:
    materialized = tuple(values)
    if not materialized or any(
        not isinstance(value, str) or not value for value in materialized
    ):
        raise ValueError("ID arrays must contain non-empty strings")
    if len(materialized) != len(set(materialized)):
        raise ValueError("ID arrays must not contain duplicates")
    canonical = sorted(materialized, key=lambda value: value.encode("utf-8"))
    return json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))


def _encode_json_object(value: Mapping[str, Any]) -> str:
    if not isinstance(value, Mapping):
        raise ValueError("JSON payload must be a mapping")
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


def _encode_fee_schedule(schedule: FeeSchedule) -> str:
    payload: dict[str, Any] = {
        "enabled": schedule.enabled,
        "model": schedule.model.value,
        "parameters": {
            key: encode_decimal(value)
            for key, value in sorted(schedule.parameters.items())
        },
        "source": schedule.source,
        "taker_only": schedule.taker_only,
    }
    if schedule.updated_at is not None:
        payload["updated_at"] = schedule.updated_at
    return _encode_json_object(payload)


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else encode_decimal(value)


def _typed_tuple(
    values: Sequence[Any],
    item_type: type[Any],
    field_name: str,
) -> tuple[Any, ...]:
    try:
        materialized = tuple(values)
    except TypeError as error:
        raise ValueError(f"{field_name} must be a sequence") from error
    if any(not isinstance(value, item_type) for value in materialized):
        raise ValueError(f"{field_name} contains an invalid value")
    identifiers = [value.id for value in materialized]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"{field_name} contains duplicate IDs")
    return materialized


def _require_type(value: Any, item_type: type[Any], field_name: str) -> None:
    if not isinstance(value, item_type):
        raise ValueError(f"{field_name} must be a {item_type.__name__}")


async def _validate_event_markets(
    connection: aiosqlite.Connection,
    event_id: str,
    expected: tuple[str, ...],
) -> None:
    cursor = await connection.execute(
        """
        SELECT id FROM markets
        WHERE event_id = ?
        ORDER BY CAST(id AS BLOB)
        """,
        (event_id,),
    )
    actual = tuple(row[0] for row in await cursor.fetchall())
    if actual != expected:
        raise ValueError(f"event {event_id!r} market_ids do not match its markets")


async def _validate_stored_event_markets(
    connection: aiosqlite.Connection,
    event_id: str,
) -> None:
    cursor = await connection.execute(
        "SELECT market_ids_json FROM events WHERE id = ?",
        (event_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        raise ValueError(f"event {event_id!r} does not exist")
    await _validate_event_markets(
        connection,
        event_id,
        tuple(json.loads(row[0])),
    )


async def _fetch_one(
    path: Path,
    sql: str,
    parameters: tuple[Any, ...],
) -> aiosqlite.Row | None:
    async with aiosqlite.connect(path) as connection:
        connection.row_factory = aiosqlite.Row
        await connection.execute("PRAGMA query_only = ON")
        cursor = await connection.execute(sql, parameters)
        return await cursor.fetchone()


async def _fetch_all(
    path: Path,
    sql: str,
    parameters: tuple[Any, ...] = (),
) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(path) as connection:
        connection.row_factory = aiosqlite.Row
        await connection.execute("PRAGMA query_only = ON")
        cursor = await connection.execute(sql, parameters)
        return await cursor.fetchall()
