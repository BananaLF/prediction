from pathlib import Path
from decimal import Decimal

import pytest

from predmarket.cli import build_parser, main
from predmarket.commands import (
    RelationRegistry,
    binary_market_from_metadata,
    dispatch,
    relation_payload,
    scan_catalog,
)
from predmarket.polymarket.gamma import (
    EventMetadata,
    GammaDiscovery,
    MarketMetadata,
    TokenMetadata,
)
from predmarket.relations import RelationValidationError
from predmarket.storage import OpportunityStore
from predmarket.runtime import Runtime
import httpx
import json
import sys
from types import SimpleNamespace


def test_help_states_read_only_and_return_semantics(capsys):
    assert main(["--help"]) == 0
    output = capsys.readouterr().out
    assert "read-only" in output
    assert "no orders" in output
    assert "0.75% = 0.0075" in output


def test_parser_exposes_expected_commands_and_rejects_credentials():
    parser = build_parser()
    assert parser.parse_args(["sync-markets"]).command == "sync-markets"
    assert parser.parse_args(["scan-once"]).command == "scan-once"
    assert parser.parse_args(["watch", "--max-events", "1"]).command == "watch"
    assert parser.parse_args(["relations", "list"]).relation_command == "list"
    with pytest.raises(SystemExit):
        parser.parse_args(["--api-key", "secret", "scan-once"])
    with pytest.raises(SystemExit):
        parser.parse_args(["watch", "--max-events", "0"])


def test_main_has_stable_input_and_operational_exit_codes(tmp_path, capsys):
    assert main(["--config", str(tmp_path / "missing.yaml"), "report"]) == 2
    assert "error:" in capsys.readouterr().err


def _relation(path: Path, *, source_hash: str = "sha256:one") -> None:
    path.write_text(
        f"""
relation_id: implication-a-b
version: 1
status: active
source_rules_hash: {source_hash}
legs:
  - token_id: NO_A
    weight: 1
  - token_id: YES_B
    weight: 1
states:
  - name: a_true_b_true
    proceeds: {{NO_A: 0, YES_B: 1}}
  - name: a_false_b_true
    proceeds: {{NO_A: 1, YES_B: 1}}
  - name: a_false_b_false
    proceeds: {{NO_A: 1, YES_B: 0}}
semantic_review:
  reviewer: human
  reviewed_at: 2026-07-26
  conclusion: A implies B
""".strip(),
        encoding="utf-8",
    )


def test_relation_registry_is_idempotent_and_rejects_conflict_and_traversal(tmp_path):
    source = tmp_path / "source.yaml"
    _relation(source)
    registry = RelationRegistry(tmp_path / "rules")
    first = registry.import_file(source)
    assert registry.import_file(source) == first
    _relation(source, source_hash="sha256:different")
    with pytest.raises(RelationValidationError, match="conflict"):
        registry.import_file(source)
    with pytest.raises(ValueError):
        RelationRegistry(tmp_path / ".." / "escape")
    unsafe = tmp_path / "unsafe.yaml"
    _relation(unsafe)
    unsafe.write_text(
        unsafe.read_text(encoding="utf-8").replace(
            "relation_id: implication-a-b", "relation_id: ../escape"
        ),
        encoding="utf-8",
    )
    with pytest.raises(RelationValidationError, match="safe"):
        registry.import_file(unsafe)


def test_relation_payload_retains_audited_identity(tmp_path):
    source = tmp_path / "source.yaml"
    _relation(source)
    payload = relation_payload(source)
    assert payload["relation_id"] == "implication-a-b"
    assert payload["version"] == 1
    assert payload["audited"] is True


def test_registry_matching_relation_is_loaded_by_exact_tokens(tmp_path):
    source = tmp_path / "source.yaml"
    _relation(source)
    registry = RelationRegistry(tmp_path / "rules")
    registry.import_file(source)
    matched = registry.match(("NO_A", "YES_B"), relation_id="implication-a-b")
    assert matched is not None
    assert matched.relation_id == "implication-a-b"
    assert registry.match(("wrong", "tokens")) is None


def market(*, tradeable=True):
    return MarketMetadata(
        market_id="market", condition_id="condition", question="Question?",
        slug=None,
        events=(EventMetadata("event", None, None, "{}"),),
        tokens=(TokenMetadata("yes", "YES"), TokenMetadata("no", "NO")),
        active=True if tradeable else False, closed=False, archived=False,
        accepting_orders=True, enable_order_book=True, neg_risk=False,
        end_date="2027-01-01", fees_enabled=True,
        fee_schedule_source_json=None, fee_schedule_source=None,
        source_metadata_json="{}",
    )


def test_binary_market_factory_uses_exact_pairing_and_audited_payoffs():
    value = binary_market_from_metadata(market())
    assert value.token_ids == ("yes", "no")
    assert value.relation.minimum_units_received() == 1
    assert value.relation.semantic_review is not None


@pytest.mark.asyncio
async def test_catalog_scan_skips_bad_markets_and_continues_operational_errors():
    calls = []

    class Engine:
        async def scan_binary(self, value):
            calls.append(value.condition_id)
            raise RuntimeError("market failed")

    discovery = GammaDiscovery(
        (market(), market(tradeable=False)), ()
    )
    summary = await scan_catalog(discovery, engine_factory=lambda _market: Engine())
    assert calls == ["condition"]
    assert summary["failed"] == 1
    assert summary["skipped"] == 1


@pytest.mark.asyncio
async def test_empty_report_has_defined_quantiles_and_bounds(tmp_path):
    async with OpportunityStore(tmp_path / "evidence.sqlite3") as store:
        report = await store.report(limit=10)
    assert report["total"] == 0
    assert report["latency_ms"] == {"p50": None, "p95": None, "p99": None}
    with pytest.raises(ValueError):
        async with OpportunityStore(tmp_path / "evidence.sqlite3") as store:
            await store.report(limit=0)


@pytest.mark.asyncio
async def test_catalog_snapshot_is_idempotent_and_survives_reopen(tmp_path):
    path = tmp_path / "catalog.sqlite3"
    snapshot = {
        "fetched_at_ms": 10,
        "markets": [{"id": "m", "condition_id": "c", "event_ids": ["e"],
                     "tokens": [{"id": "yes", "outcome": "YES"},
                                {"id": "no", "outcome": "NO"}],
                     "active": True, "tradeable": True, "neg_risk": False,
                     "fee_provenance": {"source": "gamma"}, "raw_json": "{}"}],
        "diagnostics": [],
    }
    async with OpportunityStore(path) as store:
        first = await store.save_catalog_snapshot(snapshot)
        assert await store.save_catalog_snapshot(snapshot) == first
        later = {**snapshot, "fetched_at_ms": 99}
        assert await store.save_catalog_snapshot(later) == first
    async with OpportunityStore(path) as store:
        records = await store.list_catalog_snapshots(limit=10)
    assert records[0]["id"] == first
    assert records[0]["markets"][0]["condition_id"] == "c"


@pytest.mark.asyncio
async def test_catalog_current_lifecycle_marks_missing_inactive(tmp_path):
    path = tmp_path / "lifecycle.sqlite3"
    first = {
        "fetched_at_ms": 10,
        "markets": [{"id": "m", "condition_id": "c", "event_ids": ["e"],
                     "tokens": [{"id": "yes", "outcome": "YES"},
                                {"id": "no", "outcome": "NO"}],
                     "active": True, "closed": False, "tradeable": True,
                     "neg_risk": False, "fee_provenance": {}, "raw_json": "{}"}],
        "diagnostics": [],
    }
    async with OpportunityStore(path) as store:
        await store.save_catalog_snapshot(first)
        await store.save_catalog_snapshot(
            {"fetched_at_ms": 20, "markets": [], "diagnostics": []}
        )
    async with OpportunityStore(path) as store:
        current = await store.list_current_catalog_markets(limit=10)
    assert current == [{
        "market_id": "m", "condition_id": "c",
        "last_seen_snapshot": current[0]["last_seen_snapshot"],
        "fetched_at_ms": 10, "presence": "MISSING", "active": False,
        "closed": False, "tradeable": False,
    }]


@pytest.mark.asyncio
async def test_runtime_shares_one_public_http_client_and_closes_once(tmp_path):
    transport = httpx.MockTransport(lambda request: httpx.Response(500))
    runtime = Runtime(http_transport=transport)
    async with runtime:
        assert runtime.gamma.http is runtime.discovery_clob.http
        assert runtime.discovery_clob.http is runtime.confirmation_clob.http
        assert "authorization" not in runtime.http.headers
    assert runtime.http.is_closed


@pytest.mark.asyncio
async def test_store_query_limits_reject_bool_and_cap(tmp_path):
    async with OpportunityStore(tmp_path / "bounded.sqlite3") as store:
        assert await store.list_opportunities(limit=1) == []
        assert await store.list_runs(limit=1) == []
        for value in (True, 0, 10_001):
            with pytest.raises(ValueError):
                await store.list_opportunities(limit=value)


@pytest.mark.asyncio
async def test_sync_command_offline_persists_normalized_catalog(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        Path("config/default.yaml").read_text(encoding="utf-8").replace(
            "data/predmarket.sqlite3", str(tmp_path / "catalog.sqlite3")
        ),
        encoding="utf-8",
    )

    class Gamma:
        async def active_markets(self, **bounds):
            assert bounds == {"limit": 10, "max_pages": 1, "max_markets": 10}
            return GammaDiscovery((market(),), ())

    class FakeRuntime:
        gamma = Gamma()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    args = build_parser().parse_args([
        "--config", str(config), "--json", "sync-markets",
        "--limit", "10", "--max-pages", "1", "--max-markets", "10",
        "--rules-dir", str(tmp_path / "rules"),
    ])
    output = await dispatch(
        args, runtime_factory=FakeRuntime, wall_clock_ms=lambda: 123
    )
    assert output["markets"] == 1
    async with OpportunityStore(tmp_path / "catalog.sqlite3") as store:
        current = await store.list_current_catalog_markets(limit=10)
    assert current[0]["presence"] == "SEEN"
    assert current[0]["tradeable"] is True


def test_json_mode_keeps_terminal_audit_off_stdout(capsys):
    async def fake_dispatcher(args):
        print("status=SNAPSHOT_EXECUTABLE", file=sys.stderr)
        return {"收益": Decimal("0.0075")}

    assert main(["--json", "scan-once"], dispatcher=fake_dispatcher) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"收益": "0.0075"}
    assert captured.out.count("\n") == 1
    assert "SNAPSHOT_EXECUTABLE" in captured.err


@pytest.mark.asyncio
async def test_watch_command_retries_initial_connect_and_persists_metrics(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text(
        Path("config/default.yaml").read_text(encoding="utf-8").replace(
            "data/predmarket.sqlite3", str(tmp_path / "watch.sqlite3")
        ),
        encoding="utf-8",
    )

    class Gamma:
        async def active_markets(self, **_bounds):
            return GammaDiscovery((market(),), ())

    class FeeClob:
        async def market_info(self, condition, *, expected_token_ids):
            assert condition == "condition"
            assert expected_token_ids == ("yes", "no")
            return SimpleNamespace(
                tick_size=Decimal("0.01"),
                minimum_order_size=Decimal("1"),
            )

    class FakeRuntime:
        gamma = Gamma()
        discovery_clob = object()
        confirmation_clob = object()
        fee_clob = FeeClob()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    attempts = []
    async def connector(url):
        attempts.append(url)
        raise ConnectionError("offline")

    async def no_sleep(_delay):
        pass

    args = build_parser().parse_args([
        "--config", str(config), "watch", "--max-connections", "2",
        "--max-events", "1", "--rules-dir", str(tmp_path / "rules"),
    ])
    output = await dispatch(
        args, runtime_factory=FakeRuntime, websocket_connector=connector,
        sleeper=no_sleep, wall_clock_ms=lambda: 1000, monotonic=lambda: 1.0,
    )
    assert len(attempts) == 2
    assert output["ws_metrics"]["reconnects"] == 1
    async with OpportunityStore(tmp_path / "watch.sqlite3") as store:
        metrics = await store.list_watch_metrics(limit=1)
    assert metrics[0]["disconnects"] == 2
    assert metrics[0]["epoch_states"] == {"no": "RESYNC", "yes": "RESYNC"}
