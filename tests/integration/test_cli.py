from pathlib import Path

import pytest

from predmarket.cli import build_parser, main
from predmarket.commands import (
    RelationRegistry,
    binary_market_from_metadata,
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
