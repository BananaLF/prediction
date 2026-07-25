from decimal import Decimal
import json

import aiosqlite
import pytest

from predmarket.storage import EvidenceBundle, EvidenceConflictError, OpportunityStore


def bundle(bundle_id: str = "bundle-1", opportunity_id: str = "opp-1") -> dict:
    return {
        "version": 1,
        "id": bundle_id,
        "run": {"id": "run-1", "status": "COMPLETED", "started_at_ms": 1000},
        "opportunity": {
            "id": opportunity_id,
            "status": "SNAPSHOT_EXECUTABLE",
            "relation_id": "rel-1",
            "quantity": Decimal("10.000"),
            "total_investment": Decimal("9.10"),
            "minimum_proceeds": Decimal("10"),
            "net_profit": Decimal("0.900"),
            "net_return": Decimal("0.098901098901098901"),
        },
        "events": [{"id": "event-1", "metadata": {"title": "天气 ☀"}}],
        "markets": [
            {"id": "market-1", "event_id": "event-1", "metadata": {"active": True}}
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
            }
        ],
        "relation": {
            "set": {
                "id": "set-1",
                "version": 3,
                "status": "active",
                "metadata": {"auditor": "alice"},
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
                    "tick_size": Decimal("0.01"),
                },
                "levels": [
                    {
                        "side": "SELL",
                        "price": Decimal("0.4500"),
                        "size": Decimal("20.00"),
                        "position": 0,
                    }
                ],
            }
        ],
        "legs": [
            {
                "id": "leg-1",
                "token_id": "yes-1",
                "side": "BUY",
                "quantity": Decimal("10"),
                "notional": Decimal("4.5"),
            }
        ],
        "actions": [
            {
                "id": "action-1",
                "kind": "BUY",
                "sequence": 0,
                "token_id": "yes-1",
                "quantity": Decimal("10"),
                "amount": Decimal("4.5"),
            }
        ],
        "risk": {
            "status": "SNAPSHOT_EXECUTABLE",
            "reasons": [],
            "worst_leg_failure_loss": Decimal("1.25"),
            "max_unhedged_notional": Decimal("4.5"),
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
    assert decoded["opportunity"]["total_investment"] == "9.1"
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
        changed["opportunity"]["net_profit"] = Decimal("0.91")
        with pytest.raises(EvidenceConflictError):
            await store.save(EvidenceBundle.from_mapping(changed))


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
