from decimal import Decimal
import json
import sqlite3

import aiosqlite
import pytest

from predmarket.storage import EvidenceBundle, EvidenceConflictError, OpportunityStore
import predmarket.storage as storage_module
from predmarket.actions import Action, ActionKind
from predmarket.exact_math import decimal_ratio
from predmarket.simulator import SimulationResult


def bundle(bundle_id: str = "bundle-1", opportunity_id: str = "opp-1") -> dict:
    exact_return = Decimal("0.25")
    return {
        "version": 2,
        "id": bundle_id,
        "producer": {
            "engine": "predmarket",
            "version": "0.2.0",
            "metadata": {"strategy": "structural"},
        },
        "evaluation": {
            "evaluated_at_ms": 1000,
            "evaluated_monotonic": Decimal("1.5"),
            "maximum_book_age_ms": 1000,
            "maximum_leg_skew_ms": 250,
            "maximum_processing_latency_ms": 100,
            "minimum_return": Decimal("0.0075"),
        },
        "run": {"id": "run-1", "status": "COMPLETED", "started_at_ms": 1000},
        "opportunity": {
            "id": opportunity_id,
            "status": "SNAPSHOT_EXECUTABLE",
            "relation_id": "rel-1",
            "quantity": Decimal("10.000"),
            "total_investment": Decimal("8.00"),
            "minimum_proceeds": Decimal("10"),
            "net_profit": Decimal("2.00"),
            "net_return": exact_return,
        },
        "economics": {
            "status": "EVALUATED",
            "gross_investment": Decimal("7.99"),
            "gross_proceeds": Decimal("10"),
            "fees": Decimal("0.01"),
            "total_costs": Decimal("8.00"),
            "net_profit": Decimal("2.00"),
            "net_return": exact_return,
            "costs": [
                {
                    "id": "cost-1",
                    "kind": "TRADING_FEE",
                    "leg_id": "leg-1",
                    "amount": Decimal("0.01"),
                }
            ],
        },
        "events": [{"id": "event-1", "metadata": {"title": "天气 ☀"}}],
        "markets": [
            {
                "id": "market-1",
                "event_id": "event-1",
                "metadata": {
                    "active": True,
                    "immediate_conversion_evidenced": True,
                    "settlement_evidenced": True,
                    "release_date_known": True,
                },
            }
        ],
        "tokens": [
            {
                "id": "yes-1",
                "market_id": "market-1",
                "outcome": "YES",
                "metadata": {},
            },
            {
                "id": "no-1",
                "market_id": "market-1",
                "outcome": "NO",
                "metadata": {},
            },
        ],
        "fee_schedules": [
            {
                "id": "fee-yes",
                "token_id": "yes-1",
                "rate": Decimal("0.0010"),
                "exponent": Decimal("2"),
                "direction": "BUY",
                "retrieved_at_ms": 995,
                "source": "CLOB",
            },
            {
                "id": "fee-no",
                "token_id": "no-1",
                "rate": Decimal("0"),
                "exponent": Decimal("2"),
                "direction": "BOTH",
                "retrieved_at_ms": 995,
                "source": "CLOB",
            },
        ],
        "relation": {
            "set": {
                "id": "set-1",
                "version": 3,
                "status": "active",
                "metadata": {"auditor": "alice", "audited": True},
                "provenance": {
                    "source": "rules/example-implication.yaml",
                    "content_hash": "sha256:abc123",
                },
            },
            "relations": [{"id": "rel-1", "kind": "BINARY_COMPLETE"}],
            "states": [{"id": "yes", "label": "YES"}, {"id": "no", "label": "NO"}],
            "payoffs": [
                {"state_id": "yes", "token_id": "yes-1", "amount": Decimal("1")},
                {"state_id": "yes", "token_id": "no-1", "amount": Decimal("0")},
                {"state_id": "no", "token_id": "yes-1", "amount": Decimal("0")},
                {"state_id": "no", "token_id": "no-1", "amount": Decimal("1")},
            ],
        },
        "discovery_books": [],
        "books": [
            {
                "epoch": {
                    "id": "epoch-yes",
                    "token_id": "yes-1",
                    "state": "LIVE",
                    "started_at_ms": 900,
                },
                "snapshot": {
                    "id": "snap-yes",
                    "exchange_ts_ms": 990,
                    "received_ts_ms": 992,
                    "received_monotonic": Decimal("1.25"),
                    "tick_size": Decimal("0.01"),
                    "book_hash": "hash-yes",
                },
                "levels": [
                    {
                        "side": "SELL",
                        "price": Decimal("0.4500"),
                        "size": Decimal("20.00"),
                        "position": 0,
                    }
                ],
            },
            {
                "epoch": {
                    "id": "epoch-no",
                    "token_id": "no-1",
                    "state": "LIVE",
                    "started_at_ms": 900,
                },
                "snapshot": {
                    "id": "snap-no",
                    "exchange_ts_ms": 990,
                    "received_ts_ms": 992,
                    "received_monotonic": Decimal("1.25"),
                    "tick_size": Decimal("0.01"),
                    "book_hash": "hash-no",
                },
                "levels": [
                    {
                        "side": "SELL",
                        "price": Decimal("0.349"),
                        "size": Decimal("20"),
                        "position": 0,
                    }
                ],
            },
        ],
        "legs": [
            {
                "id": "leg-1",
                "token_id": "yes-1",
                "side": "BUY",
                "quantity": Decimal("10"),
                "notional": Decimal("4.5"),
            },
            {
                "id": "leg-2",
                "token_id": "no-1",
                "side": "BUY",
                "quantity": Decimal("10"),
                "notional": Decimal("3.49"),
            },
        ],
        "actions": [
            {
                "id": "action-1",
                "kind": "BUY",
                "sequence": 0,
                "token_id": "yes-1",
                "quantity": Decimal("10"),
                "amount": Decimal("4.5"),
                "asset_in": "pUSD",
                "asset_out": "yes-1",
                "cash_flow": "OUTFLOW",
            },
            {
                "id": "action-2",
                "kind": "BUY",
                "sequence": 1,
                "token_id": "no-1",
                "quantity": Decimal("10"),
                "amount": Decimal("3.49"),
                "asset_in": "pUSD",
                "asset_out": "no-1",
                "cash_flow": "OUTFLOW",
            },
            {
                "id": "action-3",
                "kind": "MERGE",
                "sequence": 2,
                "quantity": Decimal("10"),
                "amount": Decimal("10"),
                "asset_in": "YES+NO",
                "asset_out": "pUSD",
                "cash_flow": "INFLOW",
            },
        ],
        "risk": {
            "status": "SNAPSHOT_EXECUTABLE",
            "reasons": [],
            "assessment_reasons": [],
            "timing_reasons": [],
            "worst_leg_failure_loss": Decimal("1.25"),
            "max_unhedged_notional": Decimal("4.5"),
            "entry_costs": {
                "yes-1": Decimal("4.5"),
                "no-1": Decimal("3.5"),
            },
            "immediate_unwind_values": {
                "yes-1": Decimal("3.25"),
                "no-1": Decimal("3.0"),
            },
            "thresholds": {
                "minimum_return": Decimal("0.0075"),
                "max_leg_failure_loss": Decimal("5"),
                "max_unhedged_notional": Decimal("20"),
            },
            "inputs": {
                "mathematical_return": exact_return,
                "data_valid": True,
                "immediate_unwind_known": True,
                "unresolved_rule_risk": False,
                "unresolved_conversion_risk": False,
                "unresolved_settlement_risk": False,
                "release_date_known": True,
            },
        },
        "latency_metrics": [
            {
                "id": "latency-1",
                "exchange_ts_ms": 990,
                "received_ts_ms": 992,
                "processing_latency_ms": 3,
            }
        ],
        "notifications": [
            {"id": "notice-1", "channel": "desktop", "status": "PENDING", "sent_at_ms": None}
        ],
    }


def test_bundle_canonicalizes_exact_decimals_and_unicode():
    evidence = EvidenceBundle.from_mapping(bundle())
    decoded = json.loads(evidence.canonical_json)
    assert decoded["opportunity"]["quantity"] == "10"
    assert decoded["opportunity"]["total_investment"] == "8"
    assert decoded["fee_schedules"][0]["rate"] == "0.001"
    assert "天气 ☀" in evidence.canonical_json
    assert ": " not in evidence.canonical_json


@pytest.mark.asyncio
async def test_schema_wal_foreign_keys_and_all_required_tables(tmp_path):
    path = tmp_path / "evidence.sqlite3"
    async with OpportunityStore(path) as store:
        journal = await store._connection.execute_fetchall("PRAGMA journal_mode")
        foreign_keys = await store._connection.execute_fetchall("PRAGMA foreign_keys")
        tables = {
            row[0]
            for row in await store._connection.execute_fetchall(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert journal[0][0].lower() == "wal"
    assert foreign_keys == [(1,)]
    assert {
        "events", "markets", "tokens", "fee_schedules", "relation_sets",
        "relations", "relation_states", "relation_payoffs", "book_epochs",
        "snapshots", "levels", "opportunities", "legs", "actions",
        "risk_assessments", "runs", "latency_metrics", "notifications",
    } <= tables


@pytest.mark.asyncio
async def test_schema_v1_is_rejected_without_mutation(tmp_path):
    path = tmp_path / "legacy-v1.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE legacy_payload(value TEXT)")
    connection.execute("INSERT INTO legacy_payload VALUES ('untouched')")
    connection.execute("PRAGMA user_version = 1")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="schema 1.*no supported migration"):
        await OpportunityStore(path).open()

    check = sqlite3.connect(path)
    assert check.execute("PRAGMA user_version").fetchone() == (1,)
    assert check.execute("SELECT value FROM legacy_payload").fetchall() == [
        ("untouched",)
    ]
    assert check.execute(
        "SELECT name FROM sqlite_master WHERE name = 'evidence_bundles'"
    ).fetchall() == []
    check.close()


@pytest.mark.asyncio
async def test_round_trip_reopen_is_byte_identical(tmp_path):
    path = tmp_path / "evidence.sqlite3"
    expected = EvidenceBundle.from_mapping(bundle()).canonical_json
    async with OpportunityStore(path) as store:
        assert await store.save(EvidenceBundle.from_mapping(bundle())) is True
    async with OpportunityStore(path) as store:
        replayed = await store.replay("bundle-1")
        assert replayed.canonical_json == expected
        assert await store.list_opportunities() == [
            ("opp-1", "SNAPSHOT_EXECUTABLE", "bundle-1")
        ]
        assert await store.list_runs() == [("run-1", "COMPLETED")]


@pytest.mark.asyncio
async def test_exact_duplicate_is_idempotent_but_conflict_fails():
    async with OpportunityStore(":memory:") as store:
        evidence = EvidenceBundle.from_mapping(bundle())
        assert await store.save(evidence) is True
        assert await store.save(evidence) is False
        changed = bundle()
        changed["producer"]["metadata"]["build"] = "different"
        with pytest.raises(EvidenceConflictError):
            await store.save(EvidenceBundle.from_mapping(changed))


@pytest.mark.asyncio
async def test_explicit_lifecycle_is_idempotent_and_closed_operations_fail():
    store = OpportunityStore(":memory:")
    with pytest.raises(RuntimeError, match="not open"):
        await store.list_runs()
    assert await store.open() is store
    assert await store.initialize() is store
    await store.close()
    await store.close()
    with pytest.raises(RuntimeError, match="not open"):
        await store.replay("bundle-1")


@pytest.mark.asyncio
async def test_watch_run_and_event_records_are_idempotent(tmp_path):
    path = tmp_path / "watch.sqlite3"
    async with OpportunityStore(path) as store:
        run_id = await store.save_watch_run({
            "run_id": "watch:run-1",
            "started_at_ms": 1000,
            "finished_at_ms": 1100,
            "status": "SUCCEEDED",
            "exit_reason": None,
            "params_json": {"max_connections": 3},
        })
        assert run_id == "watch:run-1"
        event_id = await store.save_watch_event({
            "run_id": run_id,
            "sequence": 1,
            "event_type": "book",
            "token_id": "yes",
            "condition_id": "condition-1",
            "canonical_json": "{\"event_type\":\"book\"}",
            "raw_json": "{\"event_type\":\"book\"}",
            "received_wall_ms": 1001,
            "received_monotonic": 2.0,
            "exchange_ts_ms": 1000,
            "persisted_at_ms": 1002,
        })
        assert await store.save_watch_event({
            "run_id": run_id,
            "sequence": 1,
            "event_type": "book",
            "token_id": "yes",
            "condition_id": "condition-1",
            "canonical_json": "{\"event_type\":\"book\"}",
            "raw_json": "{\"event_type\":\"book\"}",
            "received_wall_ms": 1001,
            "received_monotonic": 2.0,
            "exchange_ts_ms": 1000,
            "persisted_at_ms": 1002,
        }) == event_id
        runs = await store.list_watch_runs(limit=10)
        events = await store.list_watch_events(run_id)
        assert runs[0]["id"] == run_id
        assert runs[0]["status"] == "SUCCEEDED"
        assert events == [{
            "id": event_id,
            "run_id": run_id,
            "sequence": 1,
            "event_type": "book",
            "token_id": "yes",
            "condition_id": "condition-1",
            "canonical_json": "{\"event_type\":\"book\"}",
            "raw_json": "{\"event_type\":\"book\"}",
            "received_wall_ms": 1001,
            "received_monotonic": 2.0,
            "exchange_ts_ms": 1000,
            "persisted_at_ms": 1002,
        }]


@pytest.mark.asyncio
async def test_watch_metrics_are_persisted_and_listed(tmp_path):
    async with OpportunityStore(tmp_path / "watch-metrics.sqlite3") as store:
        await store.save_watch_run({
            "run_id": "watch:run-1",
            "started_at_ms": 1000,
            "finished_at_ms": 1100,
            "status": "SUCCEEDED",
            "exit_reason": None,
            "params_json": {"max_connections": 3},
        })
        await store.save_watch_metrics("watch:run-1", {
            "received": 3,
            "dropped": 1,
            "malformed": 0,
            "queue_high_water": 2,
        })
        runs = await store.list_watch_runs(limit=10)
        metrics = await store.list_watch_metrics(limit=10)
    assert runs[0]["id"] == "watch:run-1"
    assert runs[0]["status"] == "SUCCEEDED"
    assert metrics[0]["id"] == "watch:run-1"
    assert metrics[0]["received"] == 3
    assert metrics[0]["dropped"] == 1


@pytest.mark.asyncio
async def test_failed_initialization_leaves_store_closed_and_retryable(monkeypatch):
    store = OpportunityStore(":memory:")
    original = storage_module._SCHEMA
    monkeypatch.setattr(storage_module, "_SCHEMA", "THIS IS NOT SQL")
    with pytest.raises(aiosqlite.OperationalError):
        await store.open()
    assert store._connection is None
    with pytest.raises(RuntimeError, match="not open"):
        await store.list_runs()
    monkeypatch.setattr(storage_module, "_SCHEMA", original)
    assert await store.open() is store
    assert await store.list_runs() == []
    await store.close()


@pytest.mark.asyncio
async def test_invalid_child_rolls_back_every_row():
    async with OpportunityStore(":memory:") as store:
        evidence = EvidenceBundle.from_mapping(bundle())
        await store._connection.execute(
            """CREATE TRIGGER reject_action BEFORE INSERT ON actions
               BEGIN SELECT RAISE(ABORT, 'bad action'); END"""
        )
        with pytest.raises(aiosqlite.IntegrityError):
            await store.save(evidence)
        assert await store.list_opportunities() == []
        assert await store.list_runs() == []
        assert await store._connection.execute_fetchall("SELECT * FROM events") == []


@pytest.mark.asyncio
async def test_serialized_concurrent_writes():
    async with OpportunityStore(":memory:") as store:
        import asyncio

        second = bundle("bundle-2", "opp-2")
        second["run"]["id"] = "run-2"
        results = await asyncio.gather(
            store.save(EvidenceBundle.from_mapping(bundle())),
            store.save(EvidenceBundle.from_mapping(second)),
        )
        assert results == [True, True]
        assert len(await store.list_opportunities()) == 2


@pytest.mark.asyncio
async def test_validate_opportunity_detects_missing_chain():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-validate", "opp_123")))
        await store._connection.execute(
            "DELETE FROM legs WHERE bundle_id = ?", ("bundle-validate",)
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_123")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "INCOMPLETE_CHAIN"


@pytest.mark.asyncio
async def test_validate_opportunity_detects_missing_notifications():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-notify", "opp_notify")))
        await store._connection.execute(
            "DELETE FROM notifications WHERE bundle_id = ?", ("bundle-notify",)
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_notify")

    assert result["status"] == "fail"
    assert "notifications" in result["checks"]["completeness"]["missing"]
    assert result["errors"][0]["code"] == "INCOMPLETE_CHAIN"


@pytest.mark.asyncio
async def test_validate_opportunity_detects_partial_notifications():
    value = bundle("bundle-notify-2", "opp_notify_2")
    value["notifications"].append(
        {"id": "notice-2", "channel": "desktop", "status": "PENDING", "sent_at_ms": None}
    )
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(value))
        await store._connection.execute(
            "DELETE FROM notifications WHERE id = ?", ("notice-2",)
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_notify_2")

    assert result["status"] == "fail"
    assert "notifications" in result["checks"]["completeness"]["missing"]
    assert result["errors"][0]["code"] == "INCOMPLETE_CHAIN"


@pytest.mark.asyncio
async def test_validate_opportunity_detects_replay_mismatch(tmp_path):
    path = tmp_path / "replay-mismatch.sqlite3"
    async with OpportunityStore(path) as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-mismatch", "opp_123")))
        await store._connection.execute(
            "UPDATE legs SET payload = ? WHERE bundle_id = ? AND id = ?",
            (
                storage_module._json({
                    "id": "leg-1",
                    "token_id": "yes-1",
                    "side": "BUY",
                    "quantity": Decimal("9"),
                    "notional": Decimal("4.5"),
                }),
                "bundle-mismatch",
                "leg-1",
            ),
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_123")

    assert result["checks"]["consistency"]["status"] == "fail"
    assert result["errors"][0]["code"] == "REPLAY_MISMATCH"


@pytest.mark.asyncio
async def test_validate_opportunity_rejects_ambiguous_opportunity_id():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-one", "opp_123")))
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-two", "opp_123")))

        result = await store.validate_opportunity("opp_123")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "AMBIGUOUS_OPPORTUNITY"


@pytest.mark.asyncio
async def test_validate_opportunity_detects_corrupted_canonical_json():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-canonical", "opp_canonical")))
        await store._connection.execute(
            "UPDATE evidence_bundles SET canonical_json = ? WHERE id = ?",
            (
                json.dumps(
                    json.loads((await store._connection.execute_fetchall(
                        "SELECT canonical_json FROM evidence_bundles WHERE id = ?",
                        ("bundle-canonical",),
                    ))[0][0]),
                    ensure_ascii=False,
                    sort_keys=False,
                    indent=2,
                ),
                "bundle-canonical",
            ),
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_canonical")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "CORRUPTED_CANONICAL_JSON"


@pytest.mark.asyncio
async def test_validate_opportunity_reports_corrupted_canonical_json():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-corrupt", "opp_123")))
        await store._connection.execute(
            "UPDATE evidence_bundles SET canonical_json = ? WHERE id = ?",
            ("{not-json", "bundle-corrupt"),
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_123")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "CORRUPTED_CANONICAL_JSON"


@pytest.mark.asyncio
async def test_validate_opportunity_reports_schema_corrupted_canonical_json():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-schema", "opp_schema")))
        await store._connection.execute(
            "UPDATE evidence_bundles SET canonical_json = ? WHERE id = ?",
            ("{}", "bundle-schema"),
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_schema")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "CORRUPTED_CANONICAL_JSON"


@pytest.mark.asyncio
async def test_validate_opportunity_reports_nested_schema_corrupted_canonical_json():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle("bundle-nested", "opp_nested")))
        await store._connection.execute(
            "UPDATE evidence_bundles SET canonical_json = ? WHERE id = ?",
            (
                json.dumps(
                    {
                        **json.loads(
                            (await store._connection.execute_fetchall(
                                "SELECT canonical_json FROM evidence_bundles WHERE id = ?",
                                ("bundle-nested",),
                            ))[0][0]
                        ),
                        "evaluation": [],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "bundle-nested",
            ),
        )
        await store._connection.commit()

        result = await store.validate_opportunity("opp_nested")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "CORRUPTED_CANONICAL_JSON"


@pytest.mark.asyncio
async def test_validate_opportunity_reports_invalid_input():
    async with OpportunityStore(":memory:") as store:
        result = await store.validate_opportunity("not a safe identifier")

    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_notification_fingerprint_claim_is_atomic_under_concurrency():
    import asyncio

    first = bundle("bundle-a", "opp-a")
    first["run"]["id"] = "run-a"
    second = bundle("bundle-b", "opp-b")
    second["run"]["id"] = "run-b"
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(first))
        await store.save(EvidenceBundle.from_mapping(second))
        results = await asyncio.gather(
            store.claim_notification("nf:atomic", "bundle-a", 1000, 2000),
            store.claim_notification("nf:atomic", "bundle-b", 1001, 2001),
        )
    assert sorted(results) == [False, True]


@pytest.mark.asyncio
async def test_stranded_notification_claim_reclaims_after_lease_and_replays_audit(
    tmp_path,
):
    path = tmp_path / "leased.sqlite3"
    first = bundle("bundle-lease-a", "opp-lease-a")
    first["run"]["id"] = "run-lease-a"
    second = bundle("bundle-lease-b", "opp-lease-b")
    second["run"]["id"] = "run-lease-b"
    async with OpportunityStore(path) as store:
        await store.save(EvidenceBundle.from_mapping(first))
        await store.save(EvidenceBundle.from_mapping(second))
        assert await store.claim_notification(
            "nf:leased", "bundle-lease-a", 1000, 1100
        )
        assert not await store.claim_notification(
            "nf:leased", "bundle-lease-b", 1099, 1199
        )
    async with OpportunityStore(path) as reopened:
        assert await reopened.claim_notification(
            "nf:leased", "bundle-lease-b", 1100, 1200
        )
        claimed = await reopened.replay_with_notification_audit("bundle-lease-b")
        claim = claimed.current_claims[0]
        assert claim.owner_bundle_id == "bundle-lease-b"
        assert (
            claim.state, claim.claimed_at_ms, claim.lease_expires_at_ms,
            claim.attempt_count,
        ) == ("CLAIMED", 1100, 1200, 2)
        assert [event[1] for event in claimed.events] == ["RECLAIMED"]
        original = await reopened.replay_with_notification_audit("bundle-lease-a")
        assert (
            original.current_claims[0].owner_bundle_id == "bundle-lease-b"
        )
        assert [event[1] for event in original.events] == ["CLAIMED"]
        await reopened.record_notification_attempt(
            "nf:leased", "bundle-lease-b", "SUCCEEDED", 1110, None
        )
    async with OpportunityStore(path) as final_store:
        replay = await final_store.replay_with_notification_audit("bundle-lease-b")
        assert replay.evidence.canonical_json == EvidenceBundle.from_mapping(
            second
        ).canonical_json
        assert replay.current_claims[0].state == "SUCCEEDED"
        assert [event[1] for event in replay.events] == [
            "RECLAIMED", "SUCCEEDED"
        ]
        assert replay.attempts[0][1] == "SUCCEEDED"


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda value: value.pop("risk"), (KeyError, ValueError)),
        (lambda value: value.__setitem__("version", True), TypeError),
        (lambda value: value["opportunity"].__setitem__("quantity", 1.0), TypeError),
        (lambda value: value["opportunity"].__setitem__("quantity", True), TypeError),
        (lambda value: value["events"].append(value["events"][0]), ValueError),
        (lambda value: value["risk"].__setitem__("reasons", {"bad"}), TypeError),
        (lambda value: value.__setitem__("id", "../bad"), ValueError),
    ],
)
def test_malformed_evidence_is_rejected(mutate, expected):
    value = bundle()
    mutate(value)
    with pytest.raises(expected):
        EvidenceBundle.from_mapping(value)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["legs"].clear(),
        lambda value: value["actions"].clear(),
        lambda value: value["books"].clear(),
        lambda value: value["fee_schedules"].clear(),
        lambda value: value["latency_metrics"].clear(),
        lambda value: value["books"][0]["levels"].clear(),
        lambda value: value["books"][0]["epoch"].__setitem__("token_id", "no-1"),
        lambda value: value["fee_schedules"][0].__setitem__("token_id", "no-1"),
    ],
)
def test_executable_evidence_requires_complete_per_leg_market_data(mutate):
    value = bundle()
    mutate(value)
    with pytest.raises(ValueError):
        EvidenceBundle.from_mapping(value)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["books"][0]["levels"][0].__setitem__(
            "price", Decimal("-0.1")
        ),
        lambda value: value["books"][0]["levels"][0].__setitem__(
            "size", Decimal("-1")
        ),
        lambda value: value["legs"][0].__setitem__("quantity", Decimal("-1")),
        lambda value: value["economics"].__setitem__(
            "gross_investment", Decimal("-1")
        ),
        lambda value: value["economics"]["costs"][0].__setitem__(
            "amount", Decimal("-1")
        ),
        lambda value: value["fee_schedules"][0].__setitem__("rate", Decimal("1.1")),
    ],
)
def test_invalid_numeric_domains_are_rejected(mutate):
    value = bundle()
    mutate(value)
    with pytest.raises(ValueError):
        EvidenceBundle.from_mapping(value)


def test_scalar_audit_metadata_is_rejected():
    value = bundle()
    value["relation"]["set"]["metadata"] = "audited"
    with pytest.raises(TypeError):
        EvidenceBundle.from_mapping(value)


def test_missing_audit_provenance_is_rejected():
    value = bundle()
    value["relation"]["set"].pop("provenance")
    with pytest.raises(KeyError):
        EvidenceBundle.from_mapping(value)


def test_opportunity_and_risk_status_must_match():
    value = bundle()
    value["risk"]["status"] = "REJECTED"
    with pytest.raises(ValueError, match="status"):
        EvidenceBundle.from_mapping(value)


def test_non_executable_status_must_not_contain_notifications():
    value = bundle()
    value["opportunity"]["status"] = "RESEARCH_CANDIDATE"
    value["risk"]["status"] = "RESEARCH_CANDIDATE"
    with pytest.raises(ValueError, match="notification"):
        EvidenceBundle.from_mapping(value)


def test_not_evaluated_rejection_has_no_invented_financial_fields():
    value = bundle()
    value["opportunity"] = {
        "id": "opp-1",
        "status": "REJECTED",
        "relation_id": "rel-1",
    }
    value["economics"] = {
        "status": "NOT_EVALUATED",
        "reason": "invalid_catalog",
    }
    value["risk"]["status"] = "REJECTED"
    value["risk"]["reasons"] = ["invalid_catalog"]
    value["risk"]["assessment_reasons"] = []
    value["risk"]["timing_reasons"] = []
    value["risk"]["inputs"] = None
    value["risk"]["entry_costs"] = {}
    value["risk"]["immediate_unwind_values"] = {}
    value["risk"]["worst_leg_failure_loss"] = Decimal("0")
    value["risk"]["max_unhedged_notional"] = Decimal("0")
    value["legs"] = []
    value["actions"] = []
    value["notifications"] = []
    evidence = EvidenceBundle.from_mapping(value)
    assert "total_investment" not in evidence.data["opportunity"]
    assert evidence.data["economics"] == {
        "status": "NOT_EVALUATED",
        "reason": "invalid_catalog",
    }


def test_not_evaluated_economics_prohibits_financial_placeholders():
    value = bundle()
    value["opportunity"]["status"] = "REJECTED"
    value["risk"]["status"] = "REJECTED"
    value["risk"]["reasons"] = ["invalid_catalog"]
    value["risk"]["assessment_reasons"] = []
    value["risk"]["timing_reasons"] = []
    value["risk"]["inputs"] = None
    value["risk"]["entry_costs"] = {}
    value["risk"]["immediate_unwind_values"] = {}
    value["notifications"] = []
    value["economics"]["status"] = "NOT_EVALUATED"
    value["economics"]["reason"] = "invalid_catalog"
    with pytest.raises(ValueError, match="must not contain financial"):
        EvidenceBundle.from_mapping(value)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["opportunity"].__setitem__(
            "net_return", Decimal("999")
        ),
        lambda value: value["economics"].__setitem__("net_return", Decimal("999")),
        lambda value: (
            value["opportunity"].__setitem__("net_profit", Decimal("-0.1")),
            value["economics"].__setitem__("net_profit", Decimal("-0.1")),
        ),
        lambda value: (
            value["opportunity"].__setitem__("total_investment", Decimal("0")),
            value["economics"].__setitem__("gross_investment", Decimal("0")),
            value["economics"].__setitem__("fees", Decimal("0")),
            value["economics"].__setitem__("total_costs", Decimal("0")),
            value["economics"].__setitem__("net_profit", Decimal("10")),
            value["opportunity"].__setitem__("net_profit", Decimal("10")),
        ),
        lambda value: value["evaluation"].__setitem__(
            "minimum_return", Decimal("0.3")
        ),
    ],
)
def test_economics_are_derived_and_threshold_consistent(mutate):
    value = bundle()
    mutate(value)
    with pytest.raises(ValueError):
        EvidenceBundle.from_mapping(value)


@pytest.mark.asyncio
async def test_metadata_financial_name_collisions_round_trip_unchanged(tmp_path):
    value = bundle()
    value["producer"]["metadata"] = {
        "price": "原样价格",
        "rate": "not-a-decimal",
        "nested": {"amount": "金额文本 ☀"},
    }
    value["relation"]["set"]["provenance"]["price"] = "source-field"
    expected = EvidenceBundle.from_mapping(value)
    path = tmp_path / "metadata.sqlite3"
    async with OpportunityStore(path) as store:
        await store.save(expected)
    async with OpportunityStore(path) as store:
        replayed = await store.replay("bundle-1")
    assert replayed.canonical_json == expected.canonical_json
    assert replayed.data["producer"]["metadata"]["price"] == "原样价格"
    assert replayed.data["producer"]["metadata"]["nested"]["amount"] == "金额文本 ☀"
    assert replayed.data["opportunity"]["net_return"] == Decimal("0.25")


@pytest.mark.parametrize("unsupported", [1.5, Decimal("1.5"), {"bad"}])
def test_opaque_metadata_rejects_unsupported_json_types(unsupported):
    value = bundle()
    value["producer"]["metadata"]["price"] = unsupported
    with pytest.raises(TypeError):
        EvidenceBundle.from_mapping(value)


def test_nonterminating_simulation_return_is_valid_storage_economics():
    result = SimulationResult(
        actions=(Action(ActionKind.MERGE),),
        quantity=Decimal("10"),
        maximum_capital_used=Decimal("9.3"),
        minimum_received=Decimal("10"),
        minimum_profit=Decimal("0.7"),
        minimum_return=decimal_ratio(Decimal("0.7"), Decimal("9.3")),
    )
    value = bundle()
    value["opportunity"].update(
        total_investment=result.maximum_capital_used,
        minimum_proceeds=result.minimum_received,
        net_profit=result.minimum_profit,
        net_return=result.minimum_return,
    )
    value["economics"].update(
        gross_investment=result.maximum_capital_used,
        gross_proceeds=result.minimum_received,
        fees=Decimal("0"),
        total_costs=result.maximum_capital_used,
        net_profit=result.minimum_profit,
        net_return=result.minimum_return,
        costs=[],
    )
    value["risk"]["inputs"]["mathematical_return"] = result.minimum_return
    value["actions"][1]["amount"] = Decimal("4.8")
    value["legs"][1]["notional"] = Decimal("4.8")
    evidence = EvidenceBundle.from_mapping(value)
    assert evidence.data["opportunity"]["net_return"] == result.minimum_return

    value["economics"]["net_return"] += Decimal("0.0000000000000000000000000001")
    value["opportunity"]["net_return"] = value["economics"]["net_return"]
    with pytest.raises(ValueError, match="net return"):
        EvidenceBundle.from_mapping(value)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["actions"].pop(),
        lambda value: value["actions"].append(
            dict(value["actions"][-1], id="extra", sequence=3)
        ),
        lambda value: value["actions"].reverse(),
        lambda value: value["actions"][0].__setitem__("token_id", "no-1"),
        lambda value: value["actions"][1].__setitem__(
            "quantity", Decimal("9")
        ),
        lambda value: value["actions"][2].__setitem__(
            "amount", Decimal("0.01")
        ),
        lambda value: value["actions"][2].__setitem__("asset_in", "pUSD"),
        lambda value: value["actions"][2].__setitem__("asset_out", "NO"),
        lambda value: value["actions"][2].__setitem__(
            "cash_flow", "OUTFLOW"
        ),
    ],
)
def test_executable_binary_action_sequence_is_exact(mutate):
    value = bundle()
    mutate(value)
    with pytest.raises(ValueError):
        EvidenceBundle.from_mapping(value)
