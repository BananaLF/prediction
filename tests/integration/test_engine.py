from decimal import Decimal

import pytest

from predmarket.config import Settings
from predmarket.domain import BookLevel, OpportunityStatus
from predmarket.engine import (
    BinaryMarket,
    EngineDependencies,
    FeeConfirmation,
    StructuralArbitrageEngine,
)
from predmarket.fees import FeeSchedule
from predmarket.orderbook import OrderBook
from predmarket.polymarket.clob import BookSnapshot


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
    def __init__(self, fail=False, order=None):
        self.items = []
        self.fail = fail
        self.order = order

    async def save(self, bundle):
        if self.order is not None:
            self.order.append("save")
        if self.fail:
            raise RuntimeError("disk")
        self.items.append(bundle)
        return True


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
    values = dict(
        event_id="event-1",
        market_id="market-1",
        condition_id="condition-1",
        yes_token_id="yes-1",
        no_token_id="no-1",
        active=True,
        tradeable=True,
        relation_id="binary-rel",
        relation_set_id="binary-set",
        relation_version=1,
        relation_audited=True,
        auditor="alice",
        provenance_source="catalog",
        provenance_hash="sha256:abc",
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


@pytest.mark.asyncio
async def test_no_initial_candidate_persists_research_without_confirmation_or_notice():
    discovery = Books((book("yes-1", "0.60"), book("no-1", "0.45")))
    confirmation = Books(())
    subject, store, notice = engine(discovery, confirmation)
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.RESEARCH_CANDIDATE
    assert result.reason == "no_candidate"
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
    assert notice.calls == []


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
    assert evidence.data["economics"]["gross_investment"] + evidence.data["economics"]["fees"] == evidence.data["economics"]["total_costs"]
    assert evidence.canonical_json == type(evidence).from_mapping(evidence.data).canonical_json


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshots,changes,reason",
    [
        ((book("yes-1", "0.45", now=8_000), book("no-1", "0.45", now=8_000)), {}, "data_invalid"),
        ((book("yes-1", "0.45", now=11_000), book("no-1", "0.45", now=11_000)), {}, "data_invalid"),
        ((book("yes-1", "0.45", now=10_000), book("no-1", "0.45", now=9_000)), {}, "data_invalid"),
        ((book("yes-1", "0.45", mono=9.0), book("no-1", "0.45", mono=9.0)), {}, "data_invalid"),
    ],
)
async def test_timing_failures_reject(snapshots, changes, reason):
    subject, store, notice = engine(Books(snapshots), Books(copies(snapshots)), **changes)
    result = await subject.evaluate_binary(market())
    assert result.status is OpportunityStatus.REJECTED
    assert reason in result.risk_reasons
    assert store.items[0].data["risk"]["status"] == "REJECTED"
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
    subject, store, notice = engine(Books(()), Books(()))
    result = await subject.evaluate_binary(market(relation_audited=False))
    assert result.reason == "invalid_relation"
    assert result.status is OpportunityStatus.REJECTED
    assert len(store.items) == 1
    assert notice.calls == []


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
