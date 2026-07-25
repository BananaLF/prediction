from decimal import Decimal
from types import MappingProxyType

import pytest

from predmarket.config import Settings
from predmarket.domain import BookLevel, OpportunityStatus
from predmarket.engine import (
    BinaryMarket,
    EngineDependencies,
    EngineResult,
    FeeConfirmation,
    StructuralArbitrageEngine,
)
from predmarket.fees import FeeSchedule
from predmarket.orderbook import OrderBook
from predmarket.polymarket.clob import BookSnapshot
from predmarket.relations import (
    Relation,
    RelationLeg,
    RelationState,
    RelationStatus,
    SemanticReview,
)
from predmarket.storage import OpportunityStore


def book(token, ask, bid="0.20", *, now=10_000, received=10_001, mono=10.0, size="100"):
    value = OrderBook(
        token,
        (BookLevel(Decimal(bid), Decimal(size)),),
        (BookLevel(Decimal(ask), Decimal(size)),),
        Decimal("0.01"),
        Decimal("1"),
        now,
        f"hash-{token}-{ask}",
    )
    return BookSnapshot(value, "market-1", False, None, received, mono)


def copies(snapshots):
    return tuple(BookSnapshot(item.book, item.market_id, item.neg_risk,
                              item.last_trade_price, item.received_at_ms,
                              item.received_monotonic) for item in snapshots)


class Books:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = 0

    async def books(self, token_ids):
        result = self.responses[self.calls]
        self.calls += 1
        return result


class Fees:
    def __init__(self, value):
        self.value = value

    async def confirm(self, condition_id, token_ids):
        return self.value


class Store:
    def __init__(self, fail=False, order=None, save_result=True):
        self.items = []
        self.fail = fail
        self.order = order
        self.save_result = save_result

    async def save(self, bundle):
        if self.order is not None:
            self.order.append("save")
        if self.fail:
            raise RuntimeError("disk")
        self.items.append(bundle)
        return self.save_result


class Notice:
    def __init__(self, fail=False, order=None):
        self.calls = []
        self.fail = fail
        self.order = order

    async def notify(self, result):
        if self.order is not None:
            self.order.append("notify")
        self.calls.append(result)
        if self.fail:
            raise RuntimeError("desktop")


def settings(**changes):
    values = dict(
        bankroll=Decimal("1000"),
        minimum_return=Decimal("0.0075"),
        safety_buffer_rate=Decimal("0.0025"),
        max_leg_failure_loss=Decimal("30"),
        max_unhedged_notional=Decimal("100"),
        default_simulation_quantity=Decimal("10"),
        conversion_cost=Decimal("0"),
        maximum_book_age_ms=1000,
        maximum_leg_skew_ms=250,
        maximum_processing_latency_ms=100,
        reconcile_interval_seconds=30,
        queue_capacity=100,
        database_path="ignored.sqlite",
    )
    values.update(changes)
    return Settings(**values)


def market(**changes):
    relation = Relation(
        "binary-rel",
        1,
        RelationStatus.ACTIVE,
        "sha256:abc",
        (RelationLeg("yes-1", 1), RelationLeg("no-1", 1)),
        (
            RelationState("YES", MappingProxyType({"yes-1": 1, "no-1": 0})),
            RelationState("NO", MappingProxyType({"yes-1": 0, "no-1": 1})),
        ),
        SemanticReview("alice", "2026-07-26", "binary complete set"),
    )
    values = dict(
        event_id="event-1",
        market_id="market-1",
        condition_id="condition-1",
        yes_token_id="yes-1",
        no_token_id="no-1",
        active=True,
        tradeable=True,
        relation=relation,
        immediate_conversion_evidenced=True,
        settlement_evidenced=True,
        release_date_known=True,
    )
    values.update(changes)
    return BinaryMarket(**values)


def fee_confirmation(authoritative=True, token_ids=("yes-1", "no-1"), rate="0"):
    schedule = FeeSchedule(Decimal(rate), 2, True, 9_999)
    return FeeConfirmation(
        "condition-1",
        token_ids,
        {token: schedule for token in token_ids},
        authoritative,
        "CLOB market-info",
    )


def engine(discovery, confirmation, fees=None, store=None, notice=None, **setting_changes):
    store = store or Store()
    notice = notice or Notice()
    deps = EngineDependencies(
        discovery,
        confirmation,
        fees or Fees(fee_confirmation()),
        store,
        notice,
        settings(**setting_changes),
        lambda: 10_010,
        lambda: 10.05,
        lambda m: "opp-deterministic",
        lambda: "run-deterministic",
        "0.2.0",
    )
    return StructuralArbitrageEngine(deps), store, notice


def test_engine_result_rejects_non_boolean_lifecycle_markers():
    with pytest.raises(TypeError, match="notified"):
        EngineResult(
            "opp", OpportunityStatus.REJECTED, "reason", "stage",
            1, False, True, None, None, None, None, None, ("reason",),
        )


@pytest.mark.asyncio
async def test_no_initial_candidate_persists_research_without_confirmation_or_notice():
    discovery = Books((book("yes-1", "0.60"), book("no-1", "0.45")))
    confirmation = Books(())
    subject, store, notice = engine(discovery, confirmation)
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert result.reason == "no_candidate"
    assert result.total_investment is None
    assert store.items[0].data["economics"] == {
        "status": "NOT_EVALUATED",
        "reason": "no_candidate",
    }
    assert len(store.items[0].data["discovery_books"]) == 2
    assert confirmation.calls == 0
    assert len(store.items) == 1
    assert notice.calls == []


@pytest.mark.asyncio
async def test_candidate_disappears_during_independent_confirmation():
    discovery = Books((book("yes-1", "0.45"), book("no-1", "0.45")))
    confirmation = Books((book("yes-1", "0.60"), book("no-1", "0.45")))
    subject, store, notice = engine(discovery, confirmation)
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert result.reason == "expired_before_confirmation"
    assert store.items[0].data["producer"]["metadata"]["discovery"]["candidate"] is True
    assert len(store.items[0].data["discovery_books"]) == 2
    assert len(store.items[0].data["books"]) == 2
    assert notice.calls == []


@pytest.mark.asyncio
async def test_discovery_and_confirmation_stages_round_trip_replay(tmp_path):
    discovery = (book("yes-1", "0.45"), book("no-1", "0.45"))
    confirmation = (book("yes-1", "0.60"), book("no-1", "0.45"))
    async with OpportunityStore(tmp_path / "engine.sqlite3") as real_store:
        subject, _, _ = engine(
            Books(discovery), Books(confirmation), store=real_store
        )
        result = await subject.evaluate_binary(market())
        replayed = await real_store.replay(result.opportunity_id)
    assert [row["snapshot"]["book_hash"] for row in replayed.data["discovery_books"]]
    assert [row["snapshot"]["book_hash"] for row in replayed.data["books"]]
    assert replayed.data["producer"]["metadata"]["pipeline_reason"] == "expired_before_confirmation"


@pytest.mark.asyncio
async def test_full_depth_and_all_costs_can_reject_profitable_top_level():
    yes_d = book("yes-1", "0.40")
    no_d = book("no-1", "0.40")
    yes = BookSnapshot(
        OrderBook("yes-1", yes_d.book.bids,
                  (BookLevel(Decimal("0.40"), Decimal("1")), BookLevel(Decimal("0.70"), Decimal("99"))),
                  Decimal("0.01"), Decimal("1"), 10_000, "yes-depth"),
        "market-1", False, None, 10_001, 10.0)
    no = BookSnapshot(
        OrderBook("no-1", no_d.book.bids,
                  (BookLevel(Decimal("0.40"), Decimal("1")), BookLevel(Decimal("0.70"), Decimal("99"))),
                  Decimal("0.01"), Decimal("1"), 10_000, "no-depth"),
        "market-1", False, None, 10_001, 10.0)
    subject, store, _ = engine(Books((yes_d, no_d)), Books((yes, no)), conversion_cost=Decimal("0.30"))
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert result.reason == "return_below_minimum"
    assert store.items[0].data["economics"]["costs"]


@pytest.mark.asyncio
async def test_exact_profitable_path_persists_before_notify_and_replays():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    order = []
    subject, store, notice = engine(Books(snapshots), Books(tuple(book(x.token_id, str(x.book.asks[0].price)) for x in snapshots)),
                                    store=Store(order=order), notice=Notice(order=order))
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.SNAPSHOT_EXECUTABLE
    assert result.minimum_return >= Decimal("0.0075")
    assert order == ["save", "notify"]
    assert notice.calls
    evidence = store.items[0]
    assert {row["direction"] for row in evidence.data["fee_schedules"]} == {"BOTH"}
    assert evidence.data["economics"]["gross_investment"] + evidence.data["economics"]["fees"] == evidence.data["economics"]["total_costs"]
    assert evidence.canonical_json == type(evidence).from_mapping(evidence.data).canonical_json


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshots,changes,timing_reason",
    [
        ((book("yes-1", "0.45", now=8_000), book("no-1", "0.45", now=8_000)), {}, "stale"),
        ((book("yes-1", "0.45", now=11_000), book("no-1", "0.45", now=11_000)), {}, "future_exchange_ts"),
        ((book("yes-1", "0.45", now=10_000), book("no-1", "0.45", now=9_000)), {}, "leg_skew"),
        ((book("yes-1", "0.45", mono=9.0), book("no-1", "0.45", mono=9.0)), {}, "processing_latency"),
    ],
)
async def test_timing_failures_reject(snapshots, changes, timing_reason):
    subject, store, notice = engine(Books(snapshots), Books(copies(snapshots)), **changes)
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert "data_invalid" in result.risk_reasons
    assert timing_reason in result.risk_reasons
    evidence_reasons = store.items[0].data["risk"]["reasons"]
    assert "data_invalid" in evidence_reasons
    assert timing_reason in evidence_reasons
    assert notice.calls == []


@pytest.mark.asyncio
async def test_incomplete_fee_binding_and_token_identity_fail_closed():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    subject, store, _ = engine(Books(snapshots), Books(copies(snapshots)),
                               fees=Fees(fee_confirmation(False)))
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert result.reason == "invalid_fee_binding"
    assert len(store.items) == 1


@pytest.mark.asyncio
async def test_partial_fill_loss_gate_rejects_even_when_mathematics_passes():
    snapshots = (
        book("yes-1", "0.45", bid="0.01"),
        book("no-1", "0.45", bid="0.01"),
    )
    subject, store, notice = engine(
        Books(snapshots), Books(copies(snapshots)),
        max_leg_failure_loss=Decimal("1"),
    )
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert "loss_exceeds_limit" in result.risk_reasons
    assert store.items[0].data["risk"]["worst_leg_failure_loss"] > Decimal("1")
    assert notice.calls == []


@pytest.mark.asyncio
async def test_market_identity_mismatch_is_persisted_and_rejected():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    wrong = tuple(
        BookSnapshot(item.book, "other-market", item.neg_risk, None,
                     item.received_at_ms, item.received_monotonic)
        for item in snapshots
    )
    subject, store, _ = engine(Books(snapshots), Books(wrong))
    result = await subject.evaluate_binary(market())
    assert result.reason == "invalid_confirmation"
    assert len(store.items) == 1


@pytest.mark.asyncio
async def test_unaudited_catalog_relation_fails_closed_with_evidence():
    pending = Relation(
        "binary-rel", 1, RelationStatus.PENDING, "sha256:pending",
        (RelationLeg("yes-1", 1), RelationLeg("no-1", 1)),
        (
            RelationState("YES", MappingProxyType({"yes-1": 1, "no-1": 0})),
            RelationState("NO", MappingProxyType({"yes-1": 0, "no-1": 1})),
        ),
        None,
    )
    subject, store, notice = engine(Books(()), Books(()))
    result = await subject.evaluate_binary(market(relation=pending))
    assert result.reason == "invalid_relation"
    assert result.status is OpportunityStatus.REJECTED
    assert len(store.items) == 1
    assert store.items[0].data["relation"]["set"]["status"] == "pending"
    assert store.items[0].data["relation"]["set"]["metadata"]["audited"] is False
    assert notice.calls == []


@pytest.mark.asyncio
async def test_audited_but_non_binary_payoff_relation_is_not_rewritten():
    wrong = Relation(
        "wrong-rel", 4, RelationStatus.ACTIVE, "sha256:wrong",
        (RelationLeg("yes-1", 1), RelationLeg("no-1", 1)),
        (
            RelationState("both", MappingProxyType({"yes-1": 1, "no-1": 1})),
            RelationState("neither", MappingProxyType({"yes-1": 0, "no-1": 0})),
        ),
        SemanticReview("bob", "2026-07-26", "reviewed but wrong for binary merge"),
    )
    subject, store, _ = engine(Books(()), Books(()))
    result = await subject.evaluate_binary(market(relation=wrong))
    assert result.reason == "invalid_relation"
    data = store.items[0].data
    assert data["relation"]["relations"][0]["id"] == "wrong-rel"
    assert data["relation"]["relations"][0]["kind"] == "INVALID_BINARY"
    assert {
        (row["token_id"], row["amount"]) for row in data["relation"]["payoffs"]
    } == {
        ("yes-1", Decimal("1")), ("no-1", Decimal("1")),
        ("yes-1", Decimal("0")), ("no-1", Decimal("0")),
    }


@pytest.mark.asyncio
async def test_evidence_replays_complete_timing_and_risk_inputs():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    subject, store, _ = engine(Books(snapshots), Books(copies(snapshots)))
    result = await subject.evaluate_binary(market())
    data = store.items[0].data
    assert data["evaluation"]["maximum_processing_latency_ms"] == 100
    assert data["evaluation"]["evaluated_monotonic"] == Decimal("10.05")
    assert data["markets"][0]["metadata"]["immediate_conversion_evidenced"] is True
    assert data["markets"][0]["metadata"]["settlement_evidenced"] is True
    assert data["markets"][0]["metadata"]["release_date_known"] is True

    from predmarket.latency import Timing, validate_timings
    from predmarket.risk import RiskInputs, assess_risk, worst_partial_fill

    timings = tuple(
        Timing(
            row["snapshot"]["exchange_ts_ms"],
            row["snapshot"]["received_ts_ms"],
            float(row["snapshot"]["received_monotonic"]),
            float(data["evaluation"]["evaluated_monotonic"]),
        )
        for row in data["books"]
    )
    timing = validate_timings(
        timings,
        now_ms=data["evaluation"]["evaluated_at_ms"],
        max_age_ms=data["evaluation"]["maximum_book_age_ms"],
        max_skew_ms=data["evaluation"]["maximum_leg_skew_ms"],
        max_processing_ms=data["evaluation"]["maximum_processing_latency_ms"],
    )
    risk = data["risk"]
    partial = worst_partial_fill(risk["entry_costs"], risk["immediate_unwind_values"])
    assert partial.worst_leg_failure_loss == risk["worst_leg_failure_loss"]
    assert partial.max_unhedged_notional == risk["max_unhedged_notional"]
    inputs = RiskInputs(
        risk["inputs"]["mathematical_return"],
        timing.valid,
        partial.worst_leg_failure_loss,
        partial.max_unhedged_notional,
        risk["inputs"]["immediate_unwind_known"],
        risk["inputs"]["unresolved_rule_risk"],
        risk["inputs"]["unresolved_conversion_risk"],
        risk["inputs"]["unresolved_settlement_risk"],
        risk["inputs"]["release_date_known"],
    )
    replayed = assess_risk(
        inputs,
        risk["thresholds"]["minimum_return"],
        risk["thresholds"]["max_leg_failure_loss"],
        risk["thresholds"]["max_unhedged_notional"],
    )
    assert replayed.status.value == risk["status"]
    assert replayed.reasons == tuple(risk["assessment_reasons"])
    assert tuple(timing.reasons) == tuple(risk["timing_reasons"])


@pytest.mark.asyncio
async def test_minimum_return_equality_at_point_zero_zero_seven_five_passes():
    snapshots = (
        book("yes-1", "0.44", bid="0.43", size="100.75"),
        book("no-1", "0.46", bid="0.45", size="100.75"),
    )
    subject, _, notice = engine(
        Books(snapshots), Books(copies(snapshots)),
        safety_buffer_rate=Decimal("0"),
        conversion_cost=Decimal("9.325"),
        default_simulation_quantity=Decimal("100.75"),
        max_leg_failure_loss=Decimal("100"),
    )
    result = await subject.evaluate_binary(market())
    assert result.minimum_return == Decimal("0.0075")
    assert result.status is OpportunityStatus.SNAPSHOT_EXECUTABLE
    assert notice.calls


@pytest.mark.asyncio
async def test_store_failure_propagates_and_never_notifies():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    subject, _, notice = engine(Books(snapshots), Books(copies(snapshots)), store=Store(fail=True))
    with pytest.raises(RuntimeError, match="disk"):
        await subject.evaluate_binary(market())
    assert notice.calls == []


@pytest.mark.asyncio
async def test_idempotent_duplicate_save_does_not_notify_again():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    order = []
    subject, _, notice = engine(
        Books(snapshots), Books(copies(snapshots)),
        store=Store(order=order, save_result=False),
        notice=Notice(order=order),
    )
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.SNAPSHOT_EXECUTABLE
    assert result.notified is False
    assert result.notification_failed is False
    assert result.newly_persisted is False
    assert order == ["save"]
    assert notice.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_return", [None, 1])
async def test_non_boolean_store_return_fails_closed_without_notification(bad_return):
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    subject, _, notice = engine(
        Books(snapshots), Books(copies(snapshots)),
        store=Store(save_result=bad_return),
    )
    with pytest.raises(TypeError, match="bool"):
        await subject.evaluate_binary(market())
    assert notice.calls == []


@pytest.mark.asyncio
async def test_notifier_failure_retains_persisted_evidence():
    snapshots = (book("yes-1", "0.45"), book("no-1", "0.45"))
    subject, store, _ = engine(Books(snapshots), Books(copies(snapshots)), notice=Notice(fail=True))
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.SNAPSHOT_EXECUTABLE
    assert result.notified is False
    assert result.notification_failed is True
    assert len(store.items) == 1


@pytest.mark.asyncio
async def test_exact_nonterminating_return_and_deterministic_evidence():
    snapshots = (book("yes-1", "0.33"), book("no-1", "0.33"))
    first, store1, _ = engine(Books(snapshots), Books(copies(snapshots)))
    second, store2, _ = engine(Books(snapshots), Books(copies(snapshots)))
    a = await first.evaluate_binary(market())
    b = await second.evaluate_binary(market())
    assert a.minimum_return == b.minimum_return
    assert store1.items[0].canonical_json == store2.items[0].canonical_json
