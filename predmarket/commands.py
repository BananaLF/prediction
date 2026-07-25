"""Dependency-injectable command operations for the read-only CLI."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from contextlib import AsyncExitStack
import hashlib
import os
from pathlib import Path
import re
import sys
import time
from typing import Any
import uuid

from predmarket.engine import (
    BinaryMarket,
    EngineDependencies,
    FeeConfirmation,
    StructuralArbitrageEngine,
)
from predmarket.notifier import MacOSNotifier, NotificationRouter, TerminalNotifier
from predmarket.polymarket.clob import ClobRestClient
from predmarket.polymarket.gamma import GammaClient, GammaDiscovery, MarketMetadata
from predmarket.relations import (
    Relation,
    RelationLeg,
    RelationState,
    RelationStatus,
    RelationValidationError,
    SemanticReview,
    load_relation,
)
from types import MappingProxyType


def relation_payload(path: str | Path) -> dict[str, object]:
    relation = load_relation(Path(path))
    return {
        "relation_id": relation.relation_id,
        "version": relation.version,
        "status": relation.status.value,
        "audited": relation.semantic_review is not None,
        "minimum_units_received": relation.minimum_units_received(),
        "source_rules_hash": relation.source_rules_hash,
    }


class RelationRegistry:
    def __init__(self, rules_dir: str | Path) -> None:
        path = Path(rules_dir)
        if ".." in path.parts:
            raise ValueError("rules directory must not contain traversal")
        self.path = path

    def import_file(self, source: str | Path) -> Path:
        source_path = Path(source)
        relation = load_relation(source_path)
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", relation.relation_id) is None:
            raise RelationValidationError("relation_id is not safe for a registry path")
        raw = source_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        destination = self.path / f"{relation.relation_id}.v{relation.version}.yaml"
        self.path.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
                raise RelationValidationError(
                    "relation ID/version conflict; existing rule was not overwritten"
                )
            return destination
        try:
            with destination.open("xb") as output:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
        except FileExistsError:
            if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
                raise RelationValidationError(
                    "relation ID/version conflict; existing rule was not overwritten"
                )
        return destination

    def list(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        return [relation_payload(path) for path in sorted(self.path.glob("*.yaml"))]


def binary_market_from_metadata(market: MarketMetadata) -> BinaryMarket:
    if not isinstance(market, MarketMetadata):
        raise TypeError("market must be MarketMetadata")
    if not market.is_binary or market.event is None:
        raise ValueError("market must have exact binary tokens and one event")
    yes, no = market.yes_token_id, market.no_token_id
    relation = Relation(
        relation_id=f"binary:{market.condition_id}",
        version=1,
        status=RelationStatus.ACTIVE,
        source_rules_hash=f"gamma:{market.condition_id}",
        legs=(RelationLeg(yes, 1), RelationLeg(no, 1)),
        states=(
            RelationState("YES", MappingProxyType({yes: 1, no: 0})),
            RelationState("NO", MappingProxyType({yes: 0, no: 1})),
        ),
        semantic_review=SemanticReview(
            reviewer="system.binary_schema",
            reviewed_at="built-in-v1",
            conclusion="Exact YES/NO complete-set payoff; identifiers bound at runtime",
        ),
    )
    return BinaryMarket(
        event_id=market.event.event_id,
        market_id=market.market_id,
        condition_id=market.condition_id,
        yes_token_id=yes,
        no_token_id=no,
        active=market.active is True,
        tradeable=market.is_tradeable,
        relation=relation,
        immediate_conversion_evidenced=market.neg_risk is False,
        settlement_evidenced=True,
        release_date_known=market.end_date is not None,
    )


async def scan_catalog(
    discovery: GammaDiscovery,
    *,
    engine_factory,
) -> dict[str, object]:
    if not isinstance(discovery, GammaDiscovery):
        raise TypeError("discovery must be GammaDiscovery")
    results: list[object] = []
    skipped = failed = 0
    for metadata in discovery.markets:
        if not metadata.is_tradeable or metadata.event is None:
            skipped += 1
            continue
        try:
            market = binary_market_from_metadata(metadata)
            results.append(await engine_factory(market).scan_binary(market))
        except asyncio.CancelledError:
            raise
        except Exception:
            failed += 1
    return {
        "evaluated": len(results),
        "skipped": skipped,
        "failed": failed,
        "results": results,
        "diagnostics": [asdict(item) for item in discovery.diagnostics],
    }


class AuthoritativeFeeProvider:
    def __init__(self, clob: ClobRestClient) -> None:
        self._clob = clob

    async def confirm(
        self, condition_id: str, token_ids: tuple[str, ...]
    ) -> FeeConfirmation:
        info = await self._clob.market_info(
            condition_id, expected_token_ids=token_ids
        )
        return FeeConfirmation(
            condition_id=condition_id,
            token_ids=token_ids,
            schedules=dict(info.bound_fee_schedules()),
            authoritative=True,
            source="GET /clob-markets/{condition_id}:fd",
        )


def targeted_binary_market(
    condition_id: str, yes_token_id: str, no_token_id: str
) -> BinaryMarket:
    # Targeted input is explicit and not Gamma metadata; build the same complete
    # binary semantics while keeping settlement/release evidence conservative.
    yes, no = yes_token_id, no_token_id
    relation = Relation(
        f"binary:{condition_id}", 1, RelationStatus.ACTIVE,
        f"explicit:{condition_id}",
        (RelationLeg(yes, 1), RelationLeg(no, 1)),
        (
            RelationState("YES", MappingProxyType({yes: 1, no: 0})),
            RelationState("NO", MappingProxyType({yes: 0, no: 1})),
        ),
        SemanticReview(
            "operator.explicit_pair", "runtime",
            "Operator supplied exact YES/NO token pair",
        ),
    )
    return BinaryMarket(
        f"target:{condition_id}", f"target:{condition_id}", condition_id,
        yes, no, True, True, relation, True, False, False,
    )


def _result_payload(result: object) -> object:
    if hasattr(result, "__dataclass_fields__"):
        return asdict(result)
    return result


async def _scan_runtime(args: Any, settings: Any) -> dict[str, object]:
    from predmarket.storage import OpportunityStore

    async with AsyncExitStack() as stack:
        gamma = await stack.enter_async_context(GammaClient())
        discovery_clob = await stack.enter_async_context(ClobRestClient())
        confirmation_clob = await stack.enter_async_context(ClobRestClient())
        fee_clob = await stack.enter_async_context(ClobRestClient())
        store = await stack.enter_async_context(OpportunityStore(settings.database_path))
        terminal = TerminalNotifier()
        desktop = MacOSNotifier(platform=sys.platform)
        notifier = NotificationRouter(terminal, desktop)

        def engine_for(_market: BinaryMarket) -> StructuralArbitrageEngine:
            return StructuralArbitrageEngine(
                EngineDependencies(
                    discovery_clob,
                    confirmation_clob,
                    AuthoritativeFeeProvider(fee_clob),
                    store,
                    notifier,
                    settings,
                    lambda: time.time_ns() // 1_000_000,
                    time.monotonic,
                    lambda market: f"opp:{market.condition_id}",
                    lambda: uuid.uuid4().hex,
                    "predmarket-0.2.0",
                )
            )

        explicit = (args.condition, args.yes_token, args.no_token)
        if any(explicit):
            if not all(explicit):
                raise ValueError(
                    "--condition, --yes-token, and --no-token must be supplied together"
                )
            market = targeted_binary_market(*explicit)
            result = await engine_for(market).scan_binary(market)
            return {"evaluated": 1, "skipped": 0, "failed": 0,
                    "results": [_result_payload(result)], "diagnostics": []}
        discovery = await gamma.active_markets(
            limit=min(args.limit, 100),
            max_pages=max(1, (args.limit + 99) // 100),
            max_markets=args.limit,
        )
        summary = await scan_catalog(discovery, engine_factory=engine_for)
        summary["results"] = [_result_payload(item) for item in summary["results"]]
        return summary


async def _watch_runtime(args: Any, settings: Any) -> dict[str, object]:
    """Bounded public-WS discovery whose callback always reconfirms by REST."""
    import websockets
    from predmarket.polymarket.ws import MARKET_CHANNEL_URL, MarketWebSocket
    from predmarket.storage import OpportunityStore

    async with AsyncExitStack() as stack:
        gamma = await stack.enter_async_context(GammaClient())
        discovery_clob = await stack.enter_async_context(ClobRestClient())
        confirmation_clob = await stack.enter_async_context(ClobRestClient())
        fee_clob = await stack.enter_async_context(ClobRestClient())
        store = await stack.enter_async_context(OpportunityStore(settings.database_path))
        catalog = await gamma.active_markets(
            limit=100, max_pages=5, max_markets=500
        )
        markets = {
            item.condition_id: binary_market_from_metadata(item)
            for item in catalog.markets
            if item.is_tradeable and item.event is not None
        }
        if not markets:
            return {"evaluated": 0, "markets": 0, "ws_metrics": None}
        token_conditions = {
            token: condition
            for condition, market in markets.items()
            for token in market.token_ids
        }
        results: list[object] = []
        terminal = TerminalNotifier()
        desktop = MacOSNotifier(platform=sys.platform)
        notifier = NotificationRouter(terminal, desktop)

        def engine_for() -> StructuralArbitrageEngine:
            return StructuralArbitrageEngine(
                EngineDependencies(
                    discovery_clob, confirmation_clob,
                    AuthoritativeFeeProvider(fee_clob), store, notifier, settings,
                    lambda: time.time_ns() // 1_000_000, time.monotonic,
                    lambda market: f"opp:{market.condition_id}",
                    lambda: uuid.uuid4().hex, "predmarket-0.2.0",
                )
            )

        async def candidate(_tokens: tuple[str, ...], condition: str) -> None:
            # WS values are only a hint. Formal status comes from two new REST
            # snapshots and authoritative fees inside the engine.
            result = await engine_for().scan_binary(markets[condition])
            results.append(_result_payload(result))

        watcher = MarketWebSocket(
            token_conditions,
            queue_capacity=settings.queue_capacity,
            wall_clock_ms=lambda: time.time_ns() // 1_000_000,
            monotonic=time.monotonic,
            candidate_callback=candidate,
        )
        remaining = args.max_events
        for attempt in range(args.max_connections):
            connection = await websockets.connect(
                MARKET_CHANNEL_URL, open_timeout=10
            )
            per_connection = remaining
            await watcher.serve_connection(
                connection, max_messages=per_connection
            )
            if remaining is not None:
                # A bounded connection consumes its requested receive allowance.
                remaining = 0
                break
            if attempt + 1 < args.max_connections:
                await asyncio.sleep(min(30, 2**attempt))
        return {
            "evaluated": len(results),
            "markets": len(markets),
            "results": results,
            "ws_metrics": asdict(watcher.metrics()),
        }


async def dispatch(args: Any) -> object:
    """Default runtime dispatcher.

    Network/database operations are deliberately imported lazily; unit tests can
    inject a dispatcher into ``cli.main`` without ambient services.
    """
    from predmarket.config import Settings
    from predmarket.storage import OpportunityStore
    settings = Settings.load(args.config)
    if args.command == "relations":
        registry = RelationRegistry(args.rules_dir)
        if args.relation_command == "list":
            return {"relations": registry.list()}
        if args.relation_command == "validate":
            return relation_payload(args.path)
        return {"imported": str(registry.import_file(args.path))}
    if args.command == "sync-markets":
        async with GammaClient() as gamma:
            discovery = await gamma.active_markets(
                limit=args.limit, max_pages=args.max_pages,
                max_markets=args.max_markets,
            )
        return {
            "markets": len(discovery.markets),
            "tradeable": sum(market.is_tradeable for market in discovery.markets),
            "diagnostics": [asdict(item) for item in discovery.diagnostics],
        }
    if args.command == "scan-once":
        return await _scan_runtime(args, settings)
    if args.command == "watch":
        return await _watch_runtime(args, settings)
    if args.command in {"replay", "report"}:
        async with OpportunityStore(settings.database_path) as store:
            if args.command == "replay":
                audit = await store.replay_opportunity(args.opportunity_id)
                return {
                    "core_evidence": audit.evidence.data,
                    "notification_audit": {
                        "claims": [asdict(item) for item in audit.claims],
                        "attempts": audit.attempts,
                        "events": audit.events,
                    },
                }
            return await store.report(limit=args.limit)
    raise RuntimeError(
        f"{args.command} requires injected runtime services; no live action was taken"
    )
