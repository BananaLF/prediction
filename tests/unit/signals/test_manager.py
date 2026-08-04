from __future__ import annotations

from decimal import Decimal
from dataclasses import replace
import logging
from pathlib import Path
import sqlite3

import pytest

from predmarket.domain.market import Event, Market, MarketStatus, Token
from predmarket.domain.orderbook import OrderBook, OrderBookLevel
from predmarket.domain.signal import (
    Action,
    DecisionReason,
    ExecutionMode,
    NotEvaluable,
    OpportunityAbsent,
    OpportunityCalculation,
    OpportunityPresent,
    SignalLeg,
    StrategyType,
)
from predmarket.persistence.repositories import CatalogRepository, SignalRepository
from predmarket.persistence.writer import DatabaseWriter
from predmarket.signals.manager import SignalManager


def _catalog() -> tuple[Event, Market, Token]:
    event = Event(
        id="event-1",
        title="Event",
        status=MarketStatus.ACTIVE,
        market_ids=("market-1",),
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    market = Market(
        id="market-1",
        event_id="event-1",
        condition_id="condition-1",
        question="Question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    token = Token(
        id="token-1",
        market_id="market-1",
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    return event, market, token


def _present(
    *,
    expected_profit: str = "0.20",
    market_id: str = "market-1",
    token_id: str = "token-1",
) -> OpportunityPresent:
    profit = Decimal(expected_profit)
    capital = Decimal("0.80")
    return OpportunityPresent(
        calculation=OpportunityCalculation(
            quantity=Decimal("2"),
            total_capital=capital,
            expected_profit=profit,
            return_rate=profit / capital,
            worst_case_loss=Decimal("0.40"),
            risk_rate=Decimal("0.5"),
            unhedged_notional=Decimal("0.40"),
            risk_flags=("PARTIAL_FILL",),
            details={"model": "binary"},
        ),
        legs=(
            SignalLeg(
                position=0,
                market_id=market_id,
                token_id=token_id,
                action=Action.BUY,
                quantity=Decimal("2"),
                average_price=Decimal("0.4"),
                worst_price=Decimal("0.4"),
                gross_amount=capital,
                fee_amount=Decimal("0"),
            ),
        ),
        evidence=(
            OrderBook(
                market_id=market_id,
                token_id=token_id,
                bids=(OrderBookLevel(Decimal("0.3"), Decimal("2")),),
                asks=(OrderBookLevel(Decimal("0.4"), Decimal("2")),),
                subscription_generation=1,
                book_hash=f"hash-{token_id}",
                exchange_timestamp=10,
                received_timestamp=11,
                tick_size=Decimal("0.01"),
                minimum_order_size=Decimal("1"),
            ),
        ),
    )


async def _open_manager(
    tmp_path: Path,
    *,
    notifier=None,
) -> tuple[DatabaseWriter, CatalogRepository, SignalRepository, SignalManager]:
    database_path = tmp_path / "signals.db"
    writer = DatabaseWriter(database_path)
    await writer.start()
    catalog = CatalogRepository(database_path, writer)
    signals = SignalRepository(database_path, writer)
    event, market, token = _catalog()
    await catalog.save_catalog(events=(event,), markets=(market,), tokens=(token,))
    manager = SignalManager(
        signals,
        strategy_type=StrategyType.BINARY_UNDERPRICED,
        execution_mode=ExecutionMode.IMMEDIATE_CONVERSION,
        clock=lambda: 100,
        notifier=notifier,
    )
    return writer, catalog, signals, manager


@pytest.mark.asyncio
async def test_signal_manager_persists_open_update_noop_and_close_lifecycle(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    writer, _catalog_repo, _signals, manager = await _open_manager(tmp_path)
    try:
        with caplog.at_level(logging.INFO, logger="predmarket.signals.manager"):
            signal_id = await manager.apply(_present(), "opportunity-1", None)
            assert signal_id is not None
            assert await manager.apply(_present(), "opportunity-1", 1) == signal_id
            assert await manager.apply(_present(expected_profit="0.24"), "opportunity-1", 1) == signal_id

            absent = OpportunityAbsent(
                reason_code=DecisionReason.PROFIT_BELOW_THRESHOLD,
                calculation=_present().calculation,
                legs=_present().legs,
                evidence=_present().evidence,
            )
            assert await manager.apply(absent, "opportunity-1", 2) == signal_id

            reopened = await manager.apply(_present(), "opportunity-1", 3)
            assert reopened is not None and reopened != signal_id
    finally:
        await writer.close()

    messages = [record.getMessage() for record in caplog.records]
    assert [message.split()[0] for message in messages] == [
        "signal_transition",
        "signal_transition",
        "signal_transition",
        "signal_transition",
    ]
    assert [
        next(part.split("=", 1)[1] for part in message.split() if part.startswith("event_type="))
        for message in messages
    ] == ["OPENED", "UPDATED", "CLOSED", "OPENED"]
    assert all("strategy_type=BINARY_UNDERPRICED" in message for message in messages)

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        rows = connection.execute(
            "SELECT id, status, latest_revision, close_reason FROM arbitrage_signals ORDER BY id"
        ).fetchall()
        events = connection.execute(
            "SELECT signal_id, event_type FROM signal_revisions ORDER BY signal_id, revision"
        ).fetchall()
    assert len(rows) == 2
    assert sum(row[1] == "OPEN" for row in rows) == 1
    closed_row = next(row for row in rows if row[1] == "CLOSED")
    assert closed_row[2] == 3
    assert closed_row[3] == DecisionReason.PROFIT_BELOW_THRESHOLD.value
    event_types_by_signal: dict[str, list[str]] = {}
    for event_signal_id, event_type in events:
        event_types_by_signal.setdefault(event_signal_id, []).append(event_type)
    assert sorted(event_types_by_signal.values()) == [
        ["OPENED"],
        ["OPENED", "UPDATED", "CLOSED"],
    ]


@pytest.mark.asyncio
async def test_significant_update_replaces_canonical_signal_market_ids(tmp_path: Path) -> None:
    writer, catalog, _signals, manager = await _open_manager(tmp_path)
    event, _market, _token = _catalog()
    second_market = Market(
        id="market-2",
        event_id=event.id,
        condition_id="condition-2",
        question="Second question?",
        status=MarketStatus.ACTIVE,
        active=True,
        accepting_orders=True,
        enable_orderbook=True,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    second_token = Token(
        id="token-2",
        market_id=second_market.id,
        outcome="YES",
        position=0,
        sync_generation="sync-1",
        sync_generation_complete=True,
    )
    await catalog.save_catalog(
        events=(replace(event, market_ids=("market-1", "market-2")),),
        markets=(second_market,),
        tokens=(second_token,),
    )
    try:
        signal_id = await manager.apply(_present(), "opportunity-1", None)
        assert signal_id is not None
        assert await manager.apply(
            _present(expected_profit="0.24", market_id="market-2", token_id="token-2"),
            "opportunity-1",
            1,
        ) == signal_id
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        (market_ids_json,) = connection.execute(
            "SELECT market_ids_json FROM arbitrage_signals WHERE id = ?", (signal_id,)
        ).fetchone()
    assert market_ids_json == '["market-2"]'


@pytest.mark.asyncio
async def test_not_evaluable_closes_without_economic_or_orderbook_evidence(tmp_path: Path) -> None:
    writer, _catalog_repo, _signals, manager = await _open_manager(tmp_path)
    try:
        signal_id = await manager.apply(_present(), "opportunity-1", None)
        decision = NotEvaluable(
            reason_code=DecisionReason.MARKET_CLOSED,
            context={"affected_market_id": "market-1", "detail": "deactivated"},
        )
        assert await manager.apply(decision, "opportunity-1", 1) == signal_id
        assert await manager.apply(decision, "never-opened", None) is None
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        revision = connection.execute(
            "SELECT event_type, quantity, calculation_json, closure_context_json "
            "FROM signal_revisions"
        ).fetchall()
        counts = connection.execute(
            "SELECT COUNT(*), (SELECT COUNT(*) FROM orderbook_snapshots) FROM signal_revisions"
        ).fetchone()
    assert revision[-1][0] == "CLOSED"
    assert revision[-1][1:3] == (None, None)
    assert '"reason_code":"MARKET_CLOSED"' in revision[-1][3]
    assert counts == (2, 1)


@pytest.mark.asyncio
async def test_closing_decisions_without_open_signal_skip_database_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer, _catalog_repo, _signals, manager = await _open_manager(tmp_path)
    execute_calls = 0
    original_execute = writer.execute

    async def counted_execute(command):
        nonlocal execute_calls
        execute_calls += 1
        return await original_execute(command)

    monkeypatch.setattr(writer, "execute", counted_execute)
    try:
        absent = OpportunityAbsent(
            reason_code=DecisionReason.PROFIT_BELOW_THRESHOLD,
            calculation=_present().calculation,
            legs=_present().legs,
            evidence=_present().evidence,
        )
        not_evaluable = NotEvaluable(
            reason_code=DecisionReason.ORDERBOOK_INVALID,
            context={"detail": "missing depth"},
        )

        assert await manager.apply(absent, "never-opened-absent", None) is None
        assert await manager.apply(not_evaluable, "never-opened-invalid", None) is None
        assert execute_calls == 0
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_signal_manager_rejects_stale_decision_without_duplicate_revision(tmp_path: Path) -> None:
    writer, _catalog_repo, signals, manager = await _open_manager(tmp_path)
    try:
        signal_id = await manager.apply(_present(), "opportunity-1", None)
        assert signal_id is not None
        results = await __import__("asyncio").gather(
            manager.apply(_present(expected_profit="0.24"), "opportunity-1", 1),
            manager.apply(_present(expected_profit="0.24"), "opportunity-1", 1),
        )
        assert results == [signal_id, None]
        assert await signals.get_latest_revision(signal_id) == 2
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_deactivated_market_fails_closed_before_open(tmp_path: Path) -> None:
    writer, catalog, _signals, manager = await _open_manager(tmp_path)
    try:
        _event, market, _token = _catalog()
        await catalog.save_market(
            replace(
                market,
                status=MarketStatus.CLOSED,
                active=False,
                accepting_orders=False,
            )
        )
        with pytest.raises(ValueError, match="watchable"):
            await manager.apply(_present(), "opportunity-1", None)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_same_payload_noop_revalidates_deactivated_market(tmp_path: Path) -> None:
    writer, catalog, signals, manager = await _open_manager(tmp_path)
    try:
        signal_id = await manager.apply(_present(), "opportunity-1", None)
        assert signal_id is not None
        _event, market, _token = _catalog()
        await catalog.save_market(
            replace(
                market,
                status=MarketStatus.CLOSED,
                active=False,
                accepting_orders=False,
            )
        )

        with pytest.raises(ValueError, match="watchable"):
            await manager.apply(_present(), "opportunity-1", 1)

        assert await signals.get_latest_revision(signal_id) == 1
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_absent_without_open_does_not_create_signal(tmp_path: Path) -> None:
    writer, _catalog_repo, _signals, manager = await _open_manager(tmp_path)
    try:
        present = _present()
        absent = OpportunityAbsent(
            reason_code=DecisionReason.PROFIT_BELOW_THRESHOLD,
            calculation=present.calculation,
            legs=present.legs,
            evidence=present.evidence,
        )
        assert await manager.apply(absent, "never-opened", None) is None
    finally:
        await writer.close()

    with sqlite3.connect(tmp_path / "signals.db") as connection:
        assert connection.execute("SELECT COUNT(*) FROM arbitrage_signals").fetchone() == (0,)


@pytest.mark.asyncio
async def test_notification_observes_only_committed_complete_evidence(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    observations: list[tuple[int, int, int, int, int]] = []

    def notify(notification) -> None:
        with sqlite3.connect(database_path) as connection:
            observations.append(
                connection.execute(
                    """
                    SELECT s.latest_revision,
                           (SELECT COUNT(*) FROM signal_revisions WHERE signal_id = s.id),
                           (SELECT COUNT(*) FROM signal_legs WHERE signal_id = s.id),
                           (SELECT COUNT(*) FROM orderbook_snapshots WHERE signal_id = s.id),
                           (SELECT COUNT(*) FROM orderbook_levels)
                    FROM arbitrage_signals AS s WHERE s.id = ?
                    """,
                    (notification.signal_id,),
                ).fetchone()
            )

    writer, _catalog_repo, _signals, manager = await _open_manager(tmp_path, notifier=notify)
    try:
        await manager.apply(_present(), "opportunity-1", None)
    finally:
        await writer.close()

    assert observations == [(1, 1, 1, 1, 2)]
