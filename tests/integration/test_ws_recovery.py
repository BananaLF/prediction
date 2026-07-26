import asyncio
from decimal import Decimal
import json
from pathlib import Path
from types import MappingProxyType

import pytest

from predmarket.epochs import EpochState
from predmarket.domain import BookLevel
from predmarket.orderbook import OrderBook
from predmarket.polymarket.clob import BookSnapshot
from predmarket.polymarket.ws import (
    BookMetadata,
    MARKET_CHANNEL_URL,
    MarketWebSocket,
    WatchOperationalError,
    WsProtocolError,
)


FIXTURES = Path(__file__).parents[1] / "fixtures" / "polymarket"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


class Clock:
    def __init__(self) -> None:
        self.wall = 2000
        self.mono = 2.0

    def wall_ms(self) -> int:
        return self.wall

    def monotonic(self) -> float:
        return self.mono


def scanner(*, capacity: int = 4, callback=None, **kwargs) -> MarketWebSocket:
    clock = Clock()
    kwargs.setdefault(
        "book_metadata",
        {
            token: BookMetadata(
                condition_id="condition",
                tick_size=Decimal("0.01"),
                minimum_order_size=Decimal("1"),
            )
            for token in ("yes", "no")
        },
    )
    return MarketWebSocket(
        {"yes": "condition", "no": "condition"},
        queue_capacity=capacity,
        wall_clock_ms=clock.wall_ms,
        monotonic=clock.monotonic,
        candidate_callback=callback,
        **kwargs,
    )


def rest_snapshot(token: str, timestamp: int = 1002) -> BookSnapshot:
    return BookSnapshot(
        OrderBook(
            token,
            (BookLevel(Decimal("0.44"), Decimal("5")),),
            (BookLevel(Decimal("0.45"), Decimal("5")),),
            Decimal("0.01"), Decimal("1"), timestamp, f"rest-{token}-{timestamp}",
        ),
        "condition", False, None, 2000, 2.0,
    )


@pytest.mark.asyncio
async def test_rest_reconciliation_atomically_restores_corrupt_epochs() -> None:
    ws = scanner()
    ws.epochs["yes"].invalidate("corrupt")
    assert await ws.reconcile_rest(
        (rest_snapshot("yes"), rest_snapshot("no"))
    ) is True
    assert all(epoch.state is EpochState.LIVE for epoch in ws.epochs.values())
    assert ws.depth("yes").asks == (("0.45", "5"),)
    assert ws.metrics().reconciliation_attempts == 1
    assert ws.metrics().reconciliation_successes == 1


@pytest.mark.asyncio
async def test_rest_reconciliation_mismatch_invalidates_everything() -> None:
    ws = scanner()
    assert await ws.reconcile_rest((rest_snapshot("yes"),)) is False
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())
    assert ws.metrics().reconciliation_failures == 1


def test_public_subscription_is_exact_and_contains_no_credentials() -> None:
    ws = scanner()
    assert MARKET_CHANNEL_URL == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    assert ws.subscription_payload() == {
        "assets_ids": ["no", "yes"],
        "type": "market",
        "custom_feature_enabled": True,
    }
    assert not ({"auth", "credentials", "cookie", "user"} & set(ws.subscription_payload()))


@pytest.mark.asyncio
async def test_snapshot_then_delta_updates_exact_depth_and_deletes_zero_size() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_book_yes.json"))
    received = await ws.process_one()
    assert received.event_type == "book"
    assert received.raw == json.dumps(json.loads(fixture("ws_book_yes.json")), sort_keys=True, separators=(",", ":"))
    assert (received.received_wall_ms, received.received_monotonic) == (2000, 2.0)
    assert ws.epochs["yes"].state is EpochState.LIVE
    assert ws.depth("yes").asks == (("0.48", "5"),)

    await ws.ingest(fixture("ws_delta.json"))
    await ws.process_one()
    assert ws.depth("yes").asks == (("0.47", "4"),)
    assert ws.epochs["yes"].snapshot_hash == "yes-h2"


@pytest.mark.asyncio
async def test_delta_before_snapshot_is_ignored() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_delta.json"))
    await ws.process_one()
    assert ws.epochs["yes"].state is EpochState.WARMING
    assert ws.depth("yes").asks == ()


@pytest.mark.asyncio
async def test_malformed_known_token_invalidates_atomically_and_unknown_fails_closed() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_book_yes.json"))
    await ws.process_one()
    before = ws.depth("yes")
    malformed = json.loads(fixture("ws_delta.json"))
    malformed["price_changes"][1]["price"] = "NaN"
    await ws.ingest(json.dumps(malformed))
    with pytest.raises(WsProtocolError):
        await ws.process_one()
    assert ws.epochs["yes"].state is EpochState.RESYNC
    assert ws.depth("yes") == before

    unknown = json.loads(fixture("ws_book_yes.json"))
    unknown["asset_id"] = "stranger"
    await ws.ingest(json.dumps(unknown))
    await ws.process_one()
    assert "stranger" not in ws.epochs
    assert ws.metrics().unknown == 1
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())


@pytest.mark.asyncio
async def test_disconnect_and_control_events_require_new_full_snapshots() -> None:
    ws = scanner()
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    ws.on_disconnect("socket_closed")
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())
    await ws.ingest(fixture("ws_delta.json"))
    await ws.process_one()
    assert ws.depth("yes").asks == (("0.48", "5"),)

    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    tick = {"event_type":"tick_size_change","asset_id":"yes","market":"condition","timestamp":"1002","old_tick_size":"0.01","new_tick_size":"0.001"}
    await ws.ingest(json.dumps(tick))
    await ws.process_one()
    assert ws.epochs["yes"].state is EpochState.RESYNC


@pytest.mark.asyncio
async def test_queue_overflow_drains_and_invalidates_without_blocking() -> None:
    ws = scanner(capacity=1)
    await ws.ingest(fixture("ws_book_yes.json"))
    assert await ws.ingest(fixture("ws_book_no.json")) is False
    assert ws.queue_size == 0
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())
    assert (ws.metrics().dropped, ws.metrics().queue_high_water, ws.metrics().resyncs) == (2, 1, 1)
    assert ws.metrics().overflows == 1


@pytest.mark.asyncio
async def test_paired_live_books_trigger_only_external_callback_and_coalesce() -> None:
    calls = []

    async def callback(token_ids, condition_id):
        calls.append((token_ids, condition_id))

    ws = scanner(callback=callback)
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    assert calls == [(("no", "yes"), "condition")]
    await ws.ingest(fixture("ws_book_no.json"))
    await ws.process_one()
    assert len(calls) == 1
    assert not hasattr(ws, "evidence") and not hasattr(ws, "opportunities")


def side_only_book(token: str, *, bid: str | None, ask: str | None) -> str:
    return json.dumps({
        "event_type": "book",
        "asset_id": token,
        "market": "condition",
        "timestamp": "1000",
        "hash": f"{token}-{bid}-{ask}",
        "bids": [] if bid is None else [{"price": bid, "size": "5"}],
        "asks": [] if ask is None else [{"price": ask, "size": "5"}],
    })


async def wait_for_state(predicate, *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("timed out waiting for websocket test state")
        await asyncio.sleep(min(0.01, remaining))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bid", "ask"),
    [("0.60", None), (None, "0.45")],
)
async def test_one_sided_binary_candidate_triggers_once(bid, ask) -> None:
    calls = []
    ws = scanner(callback=lambda tokens, condition: calls.append((tokens, condition)))
    for token in ("yes", "no"):
        await ws.ingest(side_only_book(token, bid=bid, ask=ask))
        await ws.process_one()
    assert calls == [(("no", "yes"), "condition")]
    assert not hasattr(ws, "evidence") and not hasattr(ws, "opportunities")


@pytest.mark.asyncio
async def test_unpaired_or_unprofitable_sides_do_not_trigger() -> None:
    calls = []
    ws = scanner(callback=lambda *_: calls.append(1))
    await ws.ingest(side_only_book("yes", bid="0.60", ask=None))
    await ws.process_one()
    await ws.ingest(side_only_book("no", bid=None, ask="0.45"))
    await ws.process_one()
    assert calls == []


@pytest.mark.asyncio
async def test_rest_reconciled_bid_only_overpricing_invokes_external_hint() -> None:
    calls = []
    ws = scanner(callback=lambda tokens, condition: calls.append((tokens, condition)))
    snapshots = tuple(
        BookSnapshot(
            OrderBook(
                token, (BookLevel(Decimal("0.60"), Decimal("5")),), (),
                Decimal("0.01"), Decimal("1"), 1002, f"rest-{token}",
            ),
            "condition", False, None, 2000, 2.0,
        )
        for token in ("yes", "no")
    )
    assert await ws.reconcile_rest(snapshots)
    assert calls == [(("no", "yes"), "condition")]
    assert not hasattr(ws, "evidence") and not hasattr(ws, "opportunities")


@pytest.mark.asyncio
async def test_callback_failure_isolated_and_batch_messages_supported() -> None:
    async def broken(*_):
        raise RuntimeError("REST engine unavailable")

    ws = scanner(callback=broken)
    batch = f"[{fixture('ws_book_yes.json')},{fixture('ws_book_no.json')}]"
    assert await ws.ingest(batch) is True
    await ws.process_one()
    await ws.process_one()
    assert ws.metrics().callback_failures == 1
    assert all(epoch.state is EpochState.LIVE for epoch in ws.epochs.values())


@pytest.mark.asyncio
async def test_received_payload_is_recursively_immutable() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_book_yes.json"))
    received = await ws.process_one()
    assert isinstance(received.payload, MappingProxyType)
    with pytest.raises(TypeError):
        received.payload["bids"][0]["size"] = "999"


@pytest.mark.asyncio
async def test_timestamp_regression_and_resolution_fail_closed() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_book_yes.json"))
    await ws.process_one()
    old = json.loads(fixture("ws_delta.json"))
    old["timestamp"] = "999"
    await ws.ingest(json.dumps(old))
    await ws.process_one()
    assert ws.epochs["yes"].state is EpochState.RESYNC

    resolution = {"event_type":"market_resolved","market":"condition","timestamp":"1002"}
    await ws.ingest(json.dumps(resolution))
    await ws.process_one()
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())


class Connection:
    def __init__(self, messages=()) -> None:
        self.messages = list(messages)
        self.sent = []
        self.closed = False

    async def send(self, value):
        self.sent.append(value)

    async def recv(self):
        if not self.messages:
            raise EOFError("closed")
        return self.messages.pop(0)

    async def close(self):
        self.closed = True


class ControlledSleeper:
    def __init__(self) -> None:
        self.calls = asyncio.Queue()
        self.releases = asyncio.Queue()

    async def __call__(self, delay):
        await self.calls.put(delay)
        await self.releases.get()


@pytest.mark.asyncio
async def test_connection_sends_public_subscription_and_literal_heartbeat() -> None:
    ws = scanner()
    connection = Connection(["PONG", fixture("ws_book_yes.json")])
    await ws.serve_connection(connection, max_messages=2)
    assert json.loads(connection.sent[0]) == ws.subscription_payload()
    assert connection.sent[1:] == []
    assert connection.closed is True
    assert ws.metrics().heartbeats == 1
    assert ws.epochs["yes"].state is EpochState.RESYNC


@pytest.mark.asyncio
async def test_cancellation_closes_connection_and_propagates() -> None:
    class Blocking(Connection):
        async def recv(self):
            await asyncio.Future()

    ws = scanner()
    connection = Blocking()
    task = asyncio.create_task(ws.serve_connection(connection))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert connection.closed is True


@pytest.mark.asyncio
async def test_bounded_reconnect_backoff_is_deterministic() -> None:
    ws = scanner()
    connections = [Connection(), Connection()]
    delays = []

    async def connect(_url):
        return connections.pop(0)

    async def sleep(delay):
        delays.append(delay)

    with pytest.raises(WatchOperationalError):
        await ws.run(connect, max_attempts=2, sleeper=sleep, base_backoff=0.25, max_backoff=1)
    assert delays == [0.25]
    assert ws.metrics().reconnects == 1


@pytest.mark.asyncio
async def test_message_budget_is_cumulative_and_stops_without_extra_reconnect() -> None:
    ws = scanner()
    connections = [
        Connection([fixture("ws_book_yes.json")]),
        Connection([fixture("ws_book_no.json"), fixture("ws_delta.json")]),
        Connection([fixture("ws_book_yes.json")]),
    ]
    calls = []

    async def connect(_url):
        calls.append(1)
        return connections.pop(0)

    async def sleep(_delay):
        pass

    await ws.run(
        connect, max_attempts=3, sleeper=sleep, base_backoff=0,
        max_backoff=0, max_messages=3,
    )
    assert len(calls) == 2
    assert ws.metrics().received == 3
    assert ws.metrics().reconnects == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [0, True])
async def test_run_rejects_invalid_whole_command_message_budget(invalid) -> None:
    ws = scanner()
    with pytest.raises(ValueError):
        await ws.run(
            lambda _url: None, max_attempts=1, sleeper=asyncio.sleep,
            base_backoff=0, max_backoff=0, max_messages=invalid,
        )


@pytest.mark.asyncio
async def test_budget_counts_only_accepted_events_not_pong_or_malformed() -> None:
    ws = scanner()
    connection = Connection([
        "PONG", '{"not": valid json', fixture("ws_book_yes.json"),
    ])
    await ws.serve_connection(connection, max_messages=1)
    assert ws.metrics().received == 1
    assert ws.metrics().heartbeats == 1
    assert ws.metrics().malformed == 1


@pytest.mark.asyncio
async def test_batch_is_truncated_to_remaining_budget_and_remainder_dropped() -> None:
    ws = scanner()
    batch = json.dumps([
        json.loads(fixture("ws_book_yes.json")),
        json.loads(fixture("ws_book_no.json")),
    ])
    connection = Connection([batch])
    await ws.serve_connection(connection, max_messages=1)
    assert ws.metrics().received == 1
    assert ws.metrics().dropped == 1


@pytest.mark.asyncio
async def test_latency_history_is_bounded_with_streaming_totals() -> None:
    ws = scanner()
    for _ in range(1100):
        await ws.ingest(fixture("ws_book_yes.json"))
        await ws.process_one()
    metrics = ws.metrics()
    assert metrics.processing_latency_count == 1100
    assert len(metrics.processing_latencies_ms) == 1024
    assert metrics.processing_latency_sample_truncated is True
    assert metrics.processing_latency_min_ms == 0
    assert metrics.processing_latency_max_ms == 0


@pytest.mark.asyncio
async def test_idle_time_based_heartbeat_timeout_closes_and_invalidates() -> None:
    sleeper = ControlledSleeper()
    ws = scanner(
        sleeper=sleeper,
        heartbeat_interval_seconds=10,
        heartbeat_timeout_seconds=3,
    )
    connection = Connection()
    connection.recv = lambda: asyncio.Future()
    task = asyncio.create_task(ws.serve_connection(connection))
    assert await sleeper.calls.get() == 10
    await sleeper.releases.put(None)
    await asyncio.sleep(0)
    assert connection.sent[-1] == "PING"
    assert await sleeper.calls.get() == 3
    await sleeper.releases.put(None)
    await asyncio.wait_for(task, timeout=1)
    assert connection.closed is True
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())


@pytest.mark.asyncio
async def test_slow_callback_does_not_block_receiver_and_overflow_forces_resync() -> None:
    callback_started = asyncio.Event()
    callback_release = asyncio.Event()

    async def callback(*_):
        callback_started.set()
        await callback_release.wait()

    class QueuedConnection(Connection):
        def __init__(self):
            super().__init__()
            self.incoming = asyncio.Queue()

        async def recv(self):
            value = await self.incoming.get()
            if isinstance(value, Exception):
                raise value
            return value

    async def never_sleep(_):
        await asyncio.Future()

    ws = scanner(
        capacity=1,
        callback=callback,
        sleeper=never_sleep,
        heartbeat_interval_seconds=10,
        heartbeat_timeout_seconds=3,
    )
    connection = QueuedConnection()
    task = asyncio.create_task(ws.serve_connection(connection))
    await connection.incoming.put(fixture("ws_book_yes.json"))
    await wait_for_state(lambda: ws.epochs["yes"].state is EpochState.LIVE)
    await connection.incoming.put(fixture("ws_book_no.json"))
    await callback_started.wait()
    await connection.incoming.put(fixture("ws_delta.json"))
    await connection.incoming.put(fixture("ws_delta.json"))
    await wait_for_state(lambda: ws.metrics().dropped > 0)
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())
    callback_release.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert connection.closed is True


@pytest.mark.asyncio
async def test_unscoped_parse_corruption_invalidates_every_epoch() -> None:
    ws = scanner()
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    assert await ws.ingest('{"not": valid json') is False
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())
    assert ws.metrics().malformed == 1


@pytest.mark.asyncio
async def test_multi_change_event_is_atomic_for_condition_on_bad_second_change() -> None:
    ws = scanner()
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    before = {token: ws.depth(token) for token in ("yes", "no")}
    event = {
        "event_type": "price_change",
        "market": "condition",
        "timestamp": "1002",
        "hash": "event",
        "price_changes": [
            {"asset_id": "yes", "price": "0.47", "size": "2", "side": "SELL", "hash": "yh"},
            {"asset_id": "no", "price": "0.48", "size": "2", "side": "WRONG", "hash": "nh"},
        ],
    }
    await ws.ingest(json.dumps(event))
    with pytest.raises(WsProtocolError):
        await ws.process_one()
    assert {token: ws.depth(token) for token in ("yes", "no")} == before
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())


@pytest.mark.asyncio
async def test_multi_token_commit_triggers_once_and_never_exposes_mixed_version() -> None:
    observations = []
    ws = None

    async def callback(*_):
        observations.append((ws.epochs["yes"].snapshot_hash, ws.epochs["no"].snapshot_hash))

    ws = scanner(callback=callback)
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    observations.clear()
    event = {
        "event_type": "price_change",
        "market": "condition",
        "timestamp": "1002",
        "hash": "event",
        "price_changes": [
            {"asset_id": "yes", "price": "0.47", "size": "2", "side": "SELL", "hash": "pair-h2"},
            {"asset_id": "no", "price": "0.48", "size": "2", "side": "SELL", "hash": "pair-h2"},
        ],
    }
    await ws.ingest(json.dumps(event))
    await ws.process_one()
    assert observations == [(("pair-h2"), ("pair-h2"))]


@pytest.mark.asyncio
async def test_delta_rejects_off_tick_or_crossed_book_without_mutation() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_book_yes.json"))
    await ws.process_one()
    before = ws.depth("yes")
    event = json.loads(fixture("ws_delta.json"))
    event["price_changes"] = [
        {"asset_id": "yes", "price": "0.475", "size": "2", "side": "SELL", "hash": "h2"}
    ]
    await ws.ingest(json.dumps(event))
    with pytest.raises(WsProtocolError):
        await ws.process_one()
    assert ws.depth("yes") == before
    assert ws.epochs["yes"].state is EpochState.RESYNC


@pytest.mark.asyncio
async def test_unknown_event_type_invalidates_all_live_epochs() -> None:
    ws = scanner()
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    await ws.ingest(json.dumps({
        "event_type": "mystery", "market": "condition", "timestamp": "1001"
    }))
    await ws.process_one()
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())


@pytest.mark.parametrize(
    "mapping,capacity",
    [
        ({}, 1),
        ({"": "condition"}, 1),
        ({"yes": ""}, 1),
        ({"yes": "condition"}, 0),
        ({"yes": "condition"}, True),
    ],
)
def test_constructor_rejects_unsafe_subscriptions(mapping, capacity) -> None:
    clock = Clock()
    with pytest.raises((TypeError, ValueError, WsProtocolError)):
        MarketWebSocket(
            mapping,
            queue_capacity=capacity,
            wall_clock_ms=clock.wall_ms,
            monotonic=clock.monotonic,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, "stranger"])
async def test_malformed_hash_with_unknown_scope_invalidates_all(token) -> None:
    ws = scanner()
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    event = json.loads(fixture("ws_book_yes.json"))
    if token is None:
        event.pop("asset_id")
    else:
        event["asset_id"] = token
    event["hash"] = 123
    assert await ws.ingest(json.dumps(event)) is False
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())
    assert ws.metrics().malformed >= 1


@pytest.mark.asyncio
async def test_processor_failure_closes_connection_and_reconnects_without_dead_consumer() -> None:
    class ScriptedConnection(Connection):
        def __init__(self, messages):
            super().__init__(messages)
            self.block = asyncio.Event()

        async def recv(self):
            if self.messages:
                return self.messages.pop(0)
            await self.block.wait()
            raise EOFError

    malformed = json.loads(fixture("ws_delta.json"))
    malformed["price_changes"][0]["side"] = "WRONG"
    first = ScriptedConnection([
        fixture("ws_book_yes.json"),
        fixture("ws_book_no.json"),
        json.dumps(malformed),
    ])
    second = Connection()
    connections = [first, second]
    delays = []

    async def connector(_):
        return connections.pop(0)

    async def backoff(delay):
        delays.append(delay)

    ws = scanner()
    await asyncio.wait_for(
        ws.run(
            connector,
            max_attempts=2,
            sleeper=backoff,
            base_backoff=0.1,
            max_backoff=1,
        ),
        timeout=1,
    )
    assert first.closed and second.closed
    assert delays == [0.1]
    assert ws.metrics().reconnects == 1
    assert ws.queue_size == 0
    assert all(epoch.state is EpochState.RESYNC for epoch in ws.epochs.values())


@pytest.mark.asyncio
async def test_official_snapshot_uses_authoritative_metadata_when_fields_absent() -> None:
    ws = scanner()
    await ws.ingest(fixture("ws_book_official.json"))
    await ws.process_one()
    assert ws.epochs["yes"].state is EpochState.LIVE


@pytest.mark.asyncio
async def test_snapshot_fails_closed_when_metadata_missing_or_payload_mismatches() -> None:
    clock = Clock()
    missing = MarketWebSocket(
        {"yes": "condition"},
        queue_capacity=2,
        wall_clock_ms=clock.wall_ms,
        monotonic=clock.monotonic,
        book_metadata={},
    )
    await missing.ingest(fixture("ws_book_official.json"))
    with pytest.raises(WsProtocolError):
        await missing.process_one()
    assert missing.epochs["yes"].state is EpochState.RESYNC

    mismatch = scanner()
    payload = json.loads(fixture("ws_book_yes.json"))
    payload["tick_size"] = "0.001"
    await mismatch.ingest(json.dumps(payload))
    with pytest.raises(WsProtocolError):
        await mismatch.process_one()
    assert mismatch.epochs["yes"].state is EpochState.RESYNC


@pytest.mark.asyncio
async def test_equivalent_decimal_spellings_delete_same_level_without_ghost_liquidity() -> None:
    ws = scanner()
    snapshot = json.loads(fixture("ws_book_yes.json"))
    snapshot["asks"][0]["price"] = ".480"
    await ws.ingest(json.dumps(snapshot))
    await ws.process_one()
    event = json.loads(fixture("ws_delta.json"))
    event["price_changes"] = [
        {"asset_id": "yes", "price": "0.48", "size": "0", "side": "SELL", "hash": "h2"}
    ]
    await ws.ingest(json.dumps(event))
    await ws.process_one()
    assert ws.depth("yes").asks == ()


@pytest.mark.asyncio
async def test_snapshot_rejects_canonically_duplicate_price_levels() -> None:
    ws = scanner()
    snapshot = json.loads(fixture("ws_book_yes.json"))
    snapshot["asks"] = [
        {"price": ".48", "size": "1"},
        {"price": "0.480", "size": "2"},
    ]
    await ws.ingest(json.dumps(snapshot))
    with pytest.raises(WsProtocolError):
        await ws.process_one()
    assert ws.epochs["yes"].state is EpochState.RESYNC


@pytest.mark.asyncio
async def test_failed_candidate_callback_releases_key_for_unchanged_hash_retry() -> None:
    attempts = 0

    async def flaky(*_):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary")

    ws = scanner(callback=flaky)
    for name in ("ws_book_yes.json", "ws_book_no.json"):
        await ws.ingest(fixture(name))
        await ws.process_one()
    assert attempts == 1
    await ws.ingest(fixture("ws_book_no.json"))
    await ws.process_one()
    assert attempts == 2
    await ws.ingest(fixture("ws_book_no.json"))
    await ws.process_one()
    assert attempts == 2
