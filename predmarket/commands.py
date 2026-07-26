"""Dependency-injectable command operations for the read-only CLI."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
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
from predmarket.runtime import Runtime
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

    def relations(self) -> tuple[Relation, ...]:
        if not self.path.exists():
            return ()
        return tuple(load_relation(path) for path in sorted(self.path.glob("*.yaml")))

    def by_id(self, relation_id: str) -> Relation | None:
        matches = [
            relation for relation in self.relations()
            if relation.relation_id == relation_id
        ]
        if len(matches) > 1:
            raise RelationValidationError(
                "multiple versions share relation_id; registry is ambiguous"
            )
        return matches[0] if matches else None

    def match(
        self, token_ids: tuple[str, ...], *, relation_id: str | None = None
    ) -> Relation | None:
        if type(token_ids) is not tuple or not token_ids:
            raise TypeError("token_ids must be a nonempty tuple")
        matches: list[Relation] = []
        if self.path.exists():
            for path in sorted(self.path.glob("*.yaml")):
                relation = load_relation(path)
                if relation_id is not None and relation.relation_id != relation_id:
                    continue
                if {leg.token_id for leg in relation.legs} == set(token_ids):
                    matches.append(relation)
        if len(matches) > 1:
            raise RelationValidationError(
                "multiple audited relation versions match; select --relation-id"
            )
        return matches[0] if matches else None


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
        source_rules_hash="builtin:binary-complete:v1",
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


def catalog_snapshot(discovery: GammaDiscovery, fetched_at_ms: int) -> dict[str, object]:
    markets = []
    relation_candidates = []
    for item in discovery.markets:
        event_ids = [event.event_id for event in item.events]
        markets.append({
            "id": item.market_id,
            "condition_id": item.condition_id,
            "event_ids": event_ids,
            "question": item.question,
            "tokens": [
                {"id": token.token_id, "outcome": token.outcome}
                for token in item.tokens
            ],
            "active": item.active,
            "closed": item.closed,
            "archived": item.archived,
            "tradeable": item.is_tradeable,
            "neg_risk": item.neg_risk,
            "fee_provenance": {
                "field": item.fee_schedule_source,
                "raw_json": item.fee_schedule_source_json,
            },
            "raw_json": item.source_metadata_json,
        })
        relation_candidates.append({
            "id": f"candidate:binary:{item.condition_id}",
            "kind": "BINARY_COMPLETE_SET",
            "status": "RESEARCH_UNAUDITED",
            "condition_ids": [item.condition_id],
            "event_ids": event_ids,
        })
        if item.neg_risk:
            relation_candidates.append({
                "id": f"candidate:neg-risk:{item.condition_id}",
                "kind": "NEG_RISK",
                "status": "RESEARCH_UNAUDITED",
                "condition_ids": [item.condition_id],
                "event_ids": event_ids,
            })
    by_event: dict[str, list[str]] = {}
    for item in discovery.markets:
        for event in item.events:
            by_event.setdefault(event.event_id, []).append(item.condition_id)
    for event_id, conditions in sorted(by_event.items()):
        if len(conditions) > 1:
            relation_candidates.append({
                "id": f"candidate:same-event:{event_id}",
                "kind": "SAME_EVENT_LOGICAL",
                "status": "RESEARCH_UNAUDITED",
                "condition_ids": sorted(conditions),
                "event_ids": [event_id],
            })
    return {
        "fetched_at_ms": fetched_at_ms,
        "complete": True,
        "provenance": "gamma_keyset_pagination_exhausted",
        "markets": markets,
        "diagnostics": [asdict(item) for item in discovery.diagnostics],
        "relation_candidates": relation_candidates,
    }
async def scan_catalog(
    discovery: GammaDiscovery,
    *,
    engine_factory,
    relation_resolver=None,
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
            if relation_resolver is not None:
                market = relation_resolver(market)
                if market is None:
                    skipped += 1
                    continue
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
        yes, no, True, True, relation, True, True, False,
    )


def _result_payload(result: object) -> object:
    if hasattr(result, "__dataclass_fields__"):
        return asdict(result)
    return result


async def _scan_runtime(
    args: Any, settings: Any, *, runtime_factory=Runtime,
    wall_clock_ms=lambda: time.time_ns() // 1_000_000,
    monotonic=time.monotonic,
) -> dict[str, object]:
    from predmarket.storage import OpportunityStore

    async with AsyncExitStack() as stack:
        runtime = await stack.enter_async_context(runtime_factory())
        gamma = runtime.gamma
        discovery_clob = runtime.discovery_clob
        confirmation_clob = runtime.confirmation_clob
        fee_clob = runtime.fee_clob
        store = await stack.enter_async_context(OpportunityStore(settings.database_path))
        terminal = TerminalNotifier(
            stream=sys.stderr if args.json_output else sys.stdout
        )
        desktop = MacOSNotifier(platform=sys.platform)
        notifier = NotificationRouter(terminal, desktop)
        registry = RelationRegistry(args.rules_dir)
        selected_relation = (
            registry.by_id(args.relation_id) if args.relation_id else None
        )
        if args.relation_id and selected_relation is None:
            raise RelationValidationError("selected relation_id is unknown")
        selected_matches = 0

        def resolve_relation(market: BinaryMarket) -> BinaryMarket | None:
            nonlocal selected_matches
            if selected_relation is not None:
                if (
                    {leg.token_id for leg in selected_relation.legs}
                    == set(market.token_ids)
                    and len(selected_relation.states) == 2
                ):
                    selected_matches += 1
                    return replace(market, relation=selected_relation)
                return None
            matched = selected_relation or registry.match(market.token_ids)
            if matched is None:
                return market
            # Generic logical/NegRisk execution is intentionally unsupported.
            # Only a strict two-state binary complete-set relation may replace
            # the audited built-in binary definition.
            if len(matched.states) != 2:
                return market
            return replace(market, relation=matched)

        def engine_for(_market: BinaryMarket) -> StructuralArbitrageEngine:
            return StructuralArbitrageEngine(
                EngineDependencies(
                    discovery_clob,
                    confirmation_clob,
                    AuthoritativeFeeProvider(fee_clob),
                    store,
                    notifier,
                    settings,
                    wall_clock_ms,
                    monotonic,
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
            market = resolve_relation(market)
            if market is None:
                raise RelationValidationError(
                    "selected relation does not match targeted market tokens"
                )
            result = await engine_for(market).scan_binary(market)
            return {"evaluated": 1, "skipped": 0, "failed": 0,
                    "results": [_result_payload(result)], "diagnostics": []}
        discovery = await gamma.active_markets(
            limit=min(args.limit, 100),
            max_pages=max(1, (args.limit + 99) // 100),
            max_markets=args.limit,
        )
        catalog_tokens = {
            token.token_id
            for metadata in discovery.markets for token in metadata.tokens
        }
        conditions_by_token = {
            token.token_id: metadata.condition_id
            for metadata in discovery.markets for token in metadata.tokens
        }
        research_outputs = []
        for relation in registry.relations():
            relation_tokens = {leg.token_id for leg in relation.legs}
            if relation_tokens.issubset(catalog_tokens) and len(relation.states) != 2:
                observation = {
                    "relation_id": relation.relation_id,
                    "status": "RESEARCH_CANDIDATE",
                    "reason": "generic_logical_execution_unsupported",
                    "observed_at_ms": wall_clock_ms(),
                    "condition_ids": sorted({
                        conditions_by_token[token] for token in relation_tokens
                    }),
                    "token_ids": sorted(relation_tokens),
                    "notified": False,
                }
                observation["id"] = await store.save_research_observation(
                    observation
                )
                research_outputs.append(observation)
        summary = await scan_catalog(
            discovery, engine_factory=engine_for,
            relation_resolver=resolve_relation,
        )
        summary["research_relations"] = [
            item for item in registry.list()
            if item["relation_id"] == args.relation_id
            or args.relation_id is None
        ]
        summary["research_candidates"] = research_outputs
        if args.relation_id and not research_outputs and selected_matches == 0:
            raise RelationValidationError(
                "selected relation matched no catalog market or research path"
            )
        summary["results"] = [_result_payload(item) for item in summary["results"]]
        return summary


async def _watch_runtime(
    args: Any, settings: Any, *, runtime_factory=Runtime,
    websocket_connector=None,
    sleeper=asyncio.sleep,
    wall_clock_ms=lambda: time.time_ns() // 1_000_000,
    monotonic=time.monotonic,
) -> dict[str, object]:
    """Bounded public-WS discovery whose callback always reconfirms by REST."""
    import websockets
    from predmarket.polymarket.ws import (
        MARKET_CHANNEL_URL, BookMetadata, MarketWebSocket,
    )
    from predmarket.storage import OpportunityStore

    async with AsyncExitStack() as stack:
        runtime = await stack.enter_async_context(runtime_factory())
        gamma = runtime.gamma
        discovery_clob = runtime.discovery_clob
        confirmation_clob = runtime.confirmation_clob
        fee_clob = runtime.fee_clob
        store = await stack.enter_async_context(OpportunityStore(settings.database_path))
        catalog = await gamma.active_markets(
            limit=100, max_pages=5, max_markets=500
        )
        registry = RelationRegistry(args.rules_dir)
        selected_relation = (
            registry.by_id(args.relation_id) if args.relation_id else None
        )
        if args.relation_id and selected_relation is None:
            raise RelationValidationError("selected relation_id is unknown")
        catalog_tokens = {
            token.token_id
            for item in catalog.markets for token in item.tokens
        }
        conditions_by_token = {
            token.token_id: item.condition_id
            for item in catalog.markets for token in item.tokens
        }
        research_outputs = []
        for relation in registry.relations():
            if selected_relation is not None and relation != selected_relation:
                continue
            tokens = {leg.token_id for leg in relation.legs}
            if tokens.issubset(catalog_tokens) and len(relation.states) != 2:
                observation = {
                    "relation_id": relation.relation_id,
                    "status": "RESEARCH_CANDIDATE",
                    "reason": "generic_logical_execution_unsupported",
                    "observed_at_ms": wall_clock_ms(),
                    "condition_ids": sorted({
                        conditions_by_token[token] for token in tokens
                    }),
                    "token_ids": sorted(tokens),
                    "notified": False,
                }
                observation["id"] = await store.save_research_observation(
                    observation
                )
                research_outputs.append(observation)
        markets = {
            item.condition_id: binary_market_from_metadata(item)
            for item in catalog.markets
            if item.is_tradeable and item.event is not None
        }
        selected_binary_matches = 0
        for condition, market in tuple(markets.items()):
            if selected_relation is not None:
                if (
                    {leg.token_id for leg in selected_relation.legs}
                    != set(market.token_ids)
                    or len(selected_relation.states) != 2
                ):
                    markets.pop(condition)
                    continue
                selected_binary_matches += 1
            matched = selected_relation or registry.match(market.token_ids)
            if matched is not None and len(matched.states) == 2:
                markets[condition] = replace(market, relation=matched)
        if not markets:
            if args.relation_id and not research_outputs:
                raise RelationValidationError(
                    "selected relation matched no watch market or research path"
                )
            return {
                "evaluated": 0, "markets": 0, "ws_metrics": None,
                "research_candidates": research_outputs,
            }
        book_metadata: dict[str, BookMetadata] = {}
        invalid_conditions: set[str] = set()
        for condition, market in markets.items():
            try:
                info = await fee_clob.market_info(
                    condition, expected_token_ids=market.token_ids
                )
                if info.tick_size is None or info.minimum_order_size is None:
                    raise ValueError("market info lacks WS book metadata")
                for token in market.token_ids:
                    book_metadata[token] = BookMetadata(
                        condition, info.tick_size, info.minimum_order_size
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                invalid_conditions.add(condition)
        for condition in invalid_conditions:
            markets.pop(condition)
        if not markets:
            return {
                "evaluated": 0, "markets": 0, "ws_metrics": None,
                "diagnostics": ["no markets with authoritative book metadata"],
            }
        token_conditions = {
            token: condition
            for condition, market in markets.items()
            for token in market.token_ids
        }
        book_metadata = {
            token: metadata for token, metadata in book_metadata.items()
            if token in token_conditions
        }
        results: list[object] = []
        terminal = TerminalNotifier(
            stream=sys.stderr if args.json_output else sys.stdout
        )
        desktop = MacOSNotifier(platform=sys.platform)
        notifier = NotificationRouter(terminal, desktop)

        def engine_for() -> StructuralArbitrageEngine:
            return StructuralArbitrageEngine(
                EngineDependencies(
                    discovery_clob, confirmation_clob,
                    AuthoritativeFeeProvider(fee_clob), store, notifier, settings,
                    wall_clock_ms, monotonic,
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
            wall_clock_ms=wall_clock_ms,
            monotonic=monotonic,
            candidate_callback=candidate,
            book_metadata=book_metadata,
        )
        watch_run_id = f"watch:{uuid.uuid4().hex}"
        started_at_ms = wall_clock_ms()
        connector = websocket_connector or (
            lambda url: websockets.connect(url, open_timeout=10)
        )
        try:
            await watcher.run(
                connector,
                max_attempts=args.max_connections,
                sleeper=sleeper,
                base_backoff=1,
                max_backoff=30,
                max_messages=args.max_events,
            )
        finally:
            metrics = asdict(watcher.metrics())
            metrics["processing_latencies_ms"] = [
                str(value) for value in metrics["processing_latencies_ms"]
            ]
            await store.save_watch_metrics(
                watch_run_id, started_at_ms,
                {
                    **metrics,
                    "epoch_states": {
                        token: epoch.state.value
                        for token, epoch in watcher.epochs.items()
                    },
                },
            )
        return {
            "evaluated": len(results),
            "markets": len(markets),
            "results": results,
            "ws_metrics": metrics,
            "research_candidates": research_outputs,
        }


async def dispatch(
    args: Any, *, runtime_factory=Runtime, websocket_connector=None,
    sleeper=asyncio.sleep,
    wall_clock_ms=lambda: time.time_ns() // 1_000_000,
    monotonic=time.monotonic,
) -> object:
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
        async with runtime_factory() as runtime:
            discovery = await runtime.gamma.active_markets(
                limit=args.limit, max_pages=args.max_pages,
                max_markets=args.max_markets,
            )
        snapshot = catalog_snapshot(
            discovery, wall_clock_ms()
        )
        snapshot["audited_relation_registry"] = [
            {**item, "execution_support": "RESEARCH_ONLY"}
            for item in RelationRegistry(args.rules_dir).list()
        ]
        async with OpportunityStore(settings.database_path) as store:
            snapshot_id = await store.save_catalog_snapshot(snapshot)
        return {
            "snapshot_id": snapshot_id,
            "markets": len(discovery.markets),
            "tradeable": sum(market.is_tradeable for market in discovery.markets),
            "diagnostics": [asdict(item) for item in discovery.diagnostics],
        }
    if args.command == "scan-once":
        return await _scan_runtime(
            args, settings, runtime_factory=runtime_factory,
            wall_clock_ms=wall_clock_ms, monotonic=monotonic,
        )
    if args.command == "watch":
        return await _watch_runtime(
            args, settings, runtime_factory=runtime_factory,
            websocket_connector=websocket_connector, sleeper=sleeper,
            wall_clock_ms=wall_clock_ms, monotonic=monotonic,
        )
    if args.command in {"replay", "report"}:
        async with OpportunityStore(settings.database_path) as store:
            if args.command == "replay":
                if bool(args.opportunity_id) == bool(args.bundle_id):
                    raise ValueError(
                        "provide exactly one opportunity ID or --bundle-id"
                    )
                audit = (
                    await store.replay_with_notification_audit(args.bundle_id)
                    if args.bundle_id
                    else await store.replay_opportunity(args.opportunity_id)
                )
                return {
                    "core_evidence": audit.evidence.data,
                    "notification_audit": {
                        "claims": [
                            asdict(item) for item in audit.current_claims
                        ],
                        "attempts": audit.attempts,
                        "events": audit.events,
                    },
                }
            return await store.report(limit=args.limit)
    raise RuntimeError(
        f"{args.command} requires injected runtime services; no live action was taken"
    )
