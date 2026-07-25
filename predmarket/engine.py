"""Fail-closed, read-only binary structural-arbitrage evaluation pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
import math
from typing import Callable, Protocol

from predmarket.actions import ActionKind, binary_underpriced_path
from predmarket.config import Settings
from predmarket.domain import OpportunityStatus, Side
from predmarket.exact_math import decimal_ratio
from predmarket.fees import FeeSchedule
from predmarket.latency import Timing, validate_timings
from predmarket.orderbook import InsufficientDepth, OrderBook
from predmarket.polymarket.clob import BookSnapshot
from predmarket.risk import RiskInputs, assess_risk, worst_partial_fill
from predmarket.simulator import optimize_quantities
from predmarket.storage import EvidenceBundle


ZERO = Decimal("0")
ONE = Decimal("1")


class BookProvider(Protocol):
    async def books(self, token_ids: tuple[str, ...]) -> tuple[BookSnapshot, ...]: ...


class FeeProvider(Protocol):
    async def confirm(
        self, condition_id: str, token_ids: tuple[str, ...]
    ) -> "FeeConfirmation": ...


class EvidenceSaver(Protocol):
    async def save(self, bundle: EvidenceBundle) -> bool: ...


class Notifier(Protocol):
    async def notify(self, result: "EngineResult") -> None: ...


def _identifier(name: str, value: object) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value or value.strip() != value:
        raise ValueError(f"{name} must be nonempty and trimmed")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-" for character in value):
        raise ValueError(f"{name} contains unsafe characters")
    return value


@dataclass(frozen=True)
class BinaryMarket:
    event_id: str
    market_id: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    active: bool
    tradeable: bool
    relation_id: str
    relation_set_id: str
    relation_version: int
    relation_audited: bool
    auditor: str
    provenance_source: str
    provenance_hash: str
    immediate_conversion_evidenced: bool
    settlement_evidenced: bool
    release_date_known: bool

    def __post_init__(self) -> None:
        for name in (
            "event_id", "market_id", "condition_id", "yes_token_id",
            "no_token_id", "relation_id", "relation_set_id",
        ):
            _identifier(name, getattr(self, name))
        if self.yes_token_id == self.no_token_id:
            raise ValueError("YES and NO token IDs must differ")
        for name in ("auditor", "provenance_source", "provenance_hash"):
            value = getattr(self, name)
            if type(value) is not str or not value:
                raise ValueError(f"{name} must be nonempty")
        if type(self.relation_version) is not int or self.relation_version <= 0:
            raise ValueError("relation_version must be a positive integer")
        for name in (
            "active", "tradeable", "relation_audited",
            "immediate_conversion_evidenced",
            "settlement_evidenced", "release_date_known",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be bool")

    @property
    def token_ids(self) -> tuple[str, str]:
        return self.yes_token_id, self.no_token_id


@dataclass(frozen=True)
class FeeConfirmation:
    condition_id: str
    token_ids: tuple[str, ...]
    schedules: Mapping[str, FeeSchedule]
    authoritative: bool
    source: str

    def __post_init__(self) -> None:
        _identifier("condition_id", self.condition_id)
        if type(self.token_ids) is not tuple or not self.token_ids:
            raise TypeError("token_ids must be a nonempty tuple")
        if len(set(self.token_ids)) != len(self.token_ids):
            raise ValueError("token_ids must be unique")
        if not isinstance(self.schedules, Mapping):
            raise TypeError("schedules must be a mapping")
        copied = dict(self.schedules)
        if set(copied) != set(self.token_ids):
            raise ValueError("schedules must cover token_ids exactly")
        if any(not isinstance(value, FeeSchedule) for value in copied.values()):
            raise TypeError("schedules must contain FeeSchedule values")
        object.__setattr__(self, "schedules", copied)
        if type(self.authoritative) is not bool:
            raise TypeError("authoritative must be bool")
        if type(self.source) is not str or not self.source:
            raise ValueError("source must be nonempty")


@dataclass(frozen=True)
class EngineDependencies:
    discovery: BookProvider
    confirmation: BookProvider
    fees: FeeProvider
    store: EvidenceSaver
    notifier: Notifier
    settings: Settings
    wall_clock_ms: Callable[[], int]
    monotonic: Callable[[], float]
    opportunity_id_factory: Callable[[BinaryMarket], str]
    run_id_factory: Callable[[], str]
    engine_version: str

    def __post_init__(self) -> None:
        for name, method in (
            ("discovery", "books"), ("confirmation", "books"),
            ("fees", "confirm"), ("store", "save"), ("notifier", "notify"),
        ):
            if not callable(getattr(getattr(self, name), method, None)):
                raise TypeError(f"{name} must provide {method}()")
        if self.discovery is self.confirmation:
            raise ValueError("discovery and confirmation providers must be independent")
        if not isinstance(self.settings, Settings):
            raise TypeError("settings must be Settings")
        for name in (
            "wall_clock_ms", "monotonic", "opportunity_id_factory", "run_id_factory",
        ):
            if not callable(getattr(self, name)):
                raise TypeError(f"{name} must be callable")
        if type(self.engine_version) is not str or not self.engine_version:
            raise ValueError("engine_version must be nonempty")


@dataclass(frozen=True)
class EngineResult:
    opportunity_id: str
    status: OpportunityStatus
    reason: str
    stage: str
    notified: bool
    notification_failed: bool
    quantity: Decimal
    total_investment: Decimal
    minimum_proceeds: Decimal
    minimum_profit: Decimal
    minimum_return: Decimal
    risk_reasons: tuple[str, ...]


@dataclass(frozen=True)
class _Economics:
    quantity: Decimal
    gross: Decimal
    proceeds: Decimal
    fees: Decimal
    total: Decimal
    profit: Decimal
    rate: Decimal
    gross_notionals: Mapping[str, Decimal]
    entry_costs: Mapping[str, Decimal]
    trade_fees: Mapping[str, Decimal]
    buffer: Decimal


def _clock(deps: EngineDependencies) -> tuple[int, float]:
    wall, mono = deps.wall_clock_ms(), deps.monotonic()
    if type(wall) is not int or wall < 0:
        raise ValueError("wall clock must return nonnegative integer milliseconds")
    if isinstance(mono, bool) or type(mono) not in (int, float) or not math.isfinite(mono) or mono < 0:
        raise ValueError("monotonic clock must return a finite nonnegative number")
    return wall, float(mono)


def _snapshots(
    values: object, market: BinaryMarket
) -> dict[str, BookSnapshot]:
    if type(values) is not tuple or any(not isinstance(value, BookSnapshot) for value in values):
        raise ValueError("book response must be a tuple of BookSnapshot values")
    by_token = {value.token_id: value for value in values}
    if len(by_token) != len(values) or set(by_token) != set(market.token_ids):
        raise ValueError("book response token coverage mismatch")
    if any(value.market_id != market.market_id for value in values):
        raise ValueError("book response market mismatch")
    return by_token


def _candidate(books: Mapping[str, BookSnapshot], market: BinaryMarket, settings: Settings) -> bool:
    try:
        asks = [books[token].book.asks[0].price for token in market.token_ids]
    except IndexError:
        return False
    gross = sum(asks, ZERO)
    conservative_conversion_per_share = (
        settings.conversion_cost / settings.default_simulation_quantity
    )
    return (
        gross
        + gross * settings.safety_buffer_rate
        + conservative_conversion_per_share
        < ONE
    )


def _trade(book: OrderBook, fee: FeeSchedule, side: Side, quantity: Decimal) -> tuple[Decimal, Decimal]:
    fill = book.walk(side, quantity)
    remaining, total_fee = quantity, ZERO
    levels = book.asks if side is Side.BUY else book.bids
    for level in levels:
        used = min(remaining, level.size)
        if used:
            total_fee += fee.taker_fee(used, level.price)
            remaining -= used
        if remaining == ZERO:
            break
    return fill.gross, total_fee


def _economics(quantity: Decimal, books: Mapping[str, OrderBook],
               fees: Mapping[str, FeeSchedule], settings: Settings) -> _Economics:
    gross_by, fee_by = {}, {}
    for token in books:
        gross_by[token], fee_by[token] = _trade(books[token], fees[token], Side.BUY, quantity)
    gross = sum(gross_by.values(), ZERO)
    trade_fees = sum(fee_by.values(), ZERO)
    buffer = gross * settings.safety_buffer_rate
    costs = trade_fees + buffer + settings.conversion_cost
    total = gross + costs
    proceeds = quantity
    profit = proceeds - total
    per_leg_conversion = settings.conversion_cost / Decimal(len(books))
    entry_costs = {
        token: gross_by[token]
        + fee_by[token]
        + gross_by[token] * settings.safety_buffer_rate
        + per_leg_conversion
        for token in books
    }
    return _Economics(quantity, gross, proceeds, costs, total, profit,
                      decimal_ratio(profit, total), gross_by, entry_costs,
                      fee_by, buffer)


class StructuralArbitrageEngine:
    def __init__(self, dependencies: EngineDependencies) -> None:
        if not isinstance(dependencies, EngineDependencies):
            raise TypeError("dependencies must be EngineDependencies")
        self._d = dependencies

    async def scan_binary(self, market: BinaryMarket) -> EngineResult:
        return await self.evaluate_binary(market)

    async def evaluate_binary(self, market: BinaryMarket) -> EngineResult:
        if not isinstance(market, BinaryMarket):
            raise TypeError("market must be BinaryMarket")
        now_ms, evaluated_mono = _clock(self._d)
        opportunity_id = _identifier("opportunity_id", self._d.opportunity_id_factory(market))
        run_id = _identifier("run_id", self._d.run_id_factory())
        base_reason = self._catalog_reason(market)
        discovery: dict[str, BookSnapshot] = {}
        confirmed: dict[str, BookSnapshot] = {}
        fee_confirmation: FeeConfirmation | None = None
        economics: _Economics | None = None
        timing_reasons: tuple[str, ...] = ()
        partial_loss = ZERO
        unhedged = ZERO

        if base_reason is None:
            try:
                discovery = _snapshots(await self._d.discovery.books(market.token_ids), market)
            except (TypeError, ValueError):
                base_reason = "invalid_discovery"
        candidate = base_reason is None and _candidate(discovery, market, self._d.settings)
        if base_reason is None and not candidate:
            status, reason, stage = OpportunityStatus.RESEARCH_CANDIDATE, "no_candidate", "discovery"
        elif base_reason is not None:
            status, reason, stage = OpportunityStatus.REJECTED, base_reason, "catalog"
        else:
            try:
                confirmed = _snapshots(
                    await self._d.confirmation.books(market.token_ids), market
                )
                if any(confirmed[token] is discovery[token] for token in market.token_ids):
                    raise ValueError("confirmation reused discovery objects")
            except (TypeError, ValueError):
                status, reason, stage = OpportunityStatus.REJECTED, "invalid_confirmation", "confirmation"
            else:
                if not _candidate(confirmed, market, self._d.settings):
                    status, reason, stage = OpportunityStatus.REJECTED, "expired_before_confirmation", "confirmation"
                else:
                    try:
                        fee_confirmation = await self._d.fees.confirm(
                            market.condition_id, market.token_ids
                        )
                    except Exception:
                        fee_confirmation = None
                    if not self._valid_fees(fee_confirmation, market):
                        status, reason, stage = OpportunityStatus.REJECTED, "invalid_fee_binding", "fees"
                    else:
                        assert fee_confirmation is not None
                        books = {token: confirmed[token].book for token in market.token_ids}
                        fees = dict(fee_confirmation.schedules)
                        timings = tuple(
                            Timing(item.book.exchange_ts_ms, item.received_at_ms,
                                   item.received_monotonic, evaluated_mono)
                            for item in confirmed.values()
                        )
                        timing = validate_timings(
                            timings, now_ms=now_ms,
                            max_age_ms=self._d.settings.maximum_book_age_ms,
                            max_skew_ms=self._d.settings.maximum_leg_skew_ms,
                            max_processing_ms=self._d.settings.maximum_processing_latency_ms,
                        )
                        timing_reasons = timing.reasons
                        path = binary_underpriced_path(*market.token_ids)
                        simulations = optimize_quantities(
                            path, books, fees,
                            self._d.settings.safety_buffer_rate,
                            self._d.settings.conversion_cost,
                            self._d.settings.bankroll,
                        )
                        exact = []
                        for item in simulations:
                            try:
                                value = _economics(item.quantity, books, fees, self._d.settings)
                            except (InsufficientDepth, ValueError):
                                continue
                            if value.total <= self._d.settings.bankroll:
                                exact.append(value)
                        economics = max(exact, key=lambda x: (x.profit, x.rate, -x.quantity)) if exact else None
                        if economics is None:
                            status, reason, stage = OpportunityStatus.REJECTED, "no_feasible_quantity", "simulation"
                        else:
                            unwind = {}
                            for token in market.token_ids:
                                try:
                                    proceeds, unwind_fee = _trade(
                                        books[token], fees[token], Side.SELL, economics.quantity
                                    )
                                    unwind[token] = max(ZERO, proceeds - unwind_fee)
                                except (InsufficientDepth, ValueError):
                                    unwind[token] = ZERO
                            partial = worst_partial_fill(economics.entry_costs, unwind)
                            partial_loss, unhedged = (
                                partial.worst_leg_failure_loss,
                                partial.max_unhedged_notional,
                            )
                            assessment = assess_risk(
                                RiskInputs(
                                    economics.rate, timing.valid,
                                    partial_loss, unhedged,
                                    market.immediate_conversion_evidenced,
                                    False,
                                    not market.immediate_conversion_evidenced,
                                    not market.settlement_evidenced,
                                    market.release_date_known,
                                ),
                                self._d.settings.minimum_return,
                                self._d.settings.max_leg_failure_loss,
                                self._d.settings.max_unhedged_notional,
                            )
                            status = assessment.status
                            risk_reasons = list(assessment.reasons)
                            if not timing.valid and "data_invalid" in risk_reasons:
                                risk_reasons.extend(reason for reason in timing.reasons if reason not in risk_reasons)
                            if status is OpportunityStatus.SNAPSHOT_EXECUTABLE:
                                reason = "all_gates_passed"
                            elif "return_below_minimum" in assessment.reasons:
                                reason = "return_below_minimum"
                            else:
                                reason = assessment.reasons[0]
                            timing_reasons = tuple(risk_reasons)
                            stage = "risk"

        risk_reasons = timing_reasons or (() if status is OpportunityStatus.SNAPSHOT_EXECUTABLE else (reason,))
        bundle = self._bundle(
            market, opportunity_id, run_id, now_ms, evaluated_mono,
            status, reason, candidate, discovery, confirmed, fee_confirmation,
            economics, risk_reasons, partial_loss, unhedged,
        )
        await self._d.store.save(bundle)
        result = EngineResult(
            opportunity_id, status, reason, stage, False, False,
            economics.quantity if economics else ZERO,
            economics.total if economics else Decimal("1"),
            economics.proceeds if economics else ZERO,
            economics.profit if economics else Decimal("-1"),
            economics.rate if economics else Decimal("-1"),
            tuple(risk_reasons),
        )
        if status is OpportunityStatus.SNAPSHOT_EXECUTABLE:
            try:
                await self._d.notifier.notify(result)
            except Exception:
                return EngineResult(**{**result.__dict__, "notification_failed": True})
            return EngineResult(**{**result.__dict__, "notified": True})
        return result

    @staticmethod
    def _catalog_reason(market: BinaryMarket) -> str | None:
        if not market.active or not market.tradeable:
            return "market_not_tradeable"
        if (
            not market.relation_audited
            or not market.auditor
            or not market.provenance_hash
        ):
            return "invalid_relation"
        return None

    @staticmethod
    def _valid_fees(value: object, market: BinaryMarket) -> bool:
        return (
            isinstance(value, FeeConfirmation)
            and value.authoritative
            and value.condition_id == market.condition_id
            and set(value.token_ids) == set(market.token_ids)
            and len(value.token_ids) == 2
            and set(value.schedules) == set(market.token_ids)
        )

    def _bundle(
        self, market: BinaryMarket, opportunity_id: str, run_id: str,
        now_ms: int, evaluated_mono: float, status: OpportunityStatus,
        reason: str, candidate: bool, discovery: Mapping[str, BookSnapshot],
        confirmed: Mapping[str, BookSnapshot],
        fees: FeeConfirmation | None, economics: _Economics | None,
        risk_reasons: tuple[str, ...], partial_loss: Decimal, unhedged: Decimal,
    ) -> EvidenceBundle:
        econ = economics
        gross, proceeds, costs = (econ.gross, econ.proceeds, econ.fees) if econ else (Decimal("1"), ZERO, ZERO)
        total = econ.total if econ else Decimal("1")
        profit, rate, quantity = (econ.profit, econ.rate, econ.quantity) if econ else (Decimal("-1"), Decimal("-1"), ZERO)
        cost_rows = []
        if econ:
            for token in market.token_ids:
                cost_rows.append({"id": f"cost-fee-{token}", "kind": "TRADING_FEE",
                                  "leg_id": f"leg-{token}", "amount": econ.trade_fees[token]})
            cost_rows.append({"id": "cost-buffer", "kind": "SAFETY_BUFFER",
                              "component": "safety_buffer", "amount": econ.buffer})
            cost_rows.append({"id": "cost-conversion", "kind": "CONVERSION",
                              "component": "merge", "amount": self._d.settings.conversion_cost})
        book_rows, latency = [], []
        for token in market.token_ids:
            if token not in confirmed:
                continue
            snapshot = confirmed[token]
            levels = []
            for side, values in (("BUY", snapshot.book.bids), ("SELL", snapshot.book.asks)):
                for level in values:
                    levels.append({"side": side, "price": level.price, "size": level.size,
                                   "position": len(levels)})
            book_rows.append({
                "epoch": {"id": f"epoch-{token}", "token_id": token, "state": "LIVE",
                          "started_at_ms": snapshot.received_at_ms},
                "snapshot": {"id": f"snapshot-{token}",
                             "exchange_ts_ms": snapshot.book.exchange_ts_ms,
                             "received_ts_ms": snapshot.received_at_ms,
                             "tick_size": snapshot.book.tick_size},
                "levels": levels,
            })
            processing = (
                (Decimal(str(evaluated_mono)) - Decimal(str(snapshot.received_monotonic))) * 1000
            ).to_integral_value(rounding=ROUND_CEILING)
            latency.append({"id": f"latency-{token}",
                            "exchange_ts_ms": snapshot.book.exchange_ts_ms,
                            "received_ts_ms": snapshot.received_at_ms,
                            "processing_latency_ms": max(0, int(processing))})
        fee_rows = [] if fees is None else [
            {"id": f"fee-{token}", "token_id": token, "rate": schedule.rate,
             "exponent": Decimal(schedule.exponent), "direction": "BUY",
             "retrieved_at_ms": schedule.captured_at_ms, "source": fees.source}
            for token, schedule in ((token, fees.schedules[token]) for token in market.token_ids)
        ]
        legs = [] if econ is None else [
            {"id": f"leg-{token}", "token_id": token, "side": "BUY",
             "quantity": econ.quantity, "notional": econ.gross_notionals[token]}
            for token in market.token_ids
        ]
        actions = [] if econ is None else [
            {"id": f"action-{index}", "kind": kind, "sequence": index,
             **({"token_id": token} if token else {}),
             "quantity": econ.quantity,
             "amount": econ.gross_notionals[token] if token else self._d.settings.conversion_cost}
            for index, (kind, token) in enumerate((
                (ActionKind.BUY.value, market.yes_token_id),
                (ActionKind.BUY.value, market.no_token_id),
                (ActionKind.MERGE.value, None),
            ))
        ]
        value = {
            "version": 1, "id": opportunity_id,
            "producer": {"engine": "predmarket", "version": self._d.engine_version,
                         "metadata": {"strategy": "binary_underpriced",
                                      "pipeline_reason": reason,
                                      "discovery": {"candidate": candidate,
                                                    "book_hashes": [discovery[x].book.book_hash for x in market.token_ids if x in discovery]},
                                      "confirmation": {"book_hashes": [confirmed[x].book.book_hash for x in market.token_ids if x in confirmed]}}},
            "evaluation": {"evaluated_at_ms": now_ms,
                           "maximum_book_age_ms": self._d.settings.maximum_book_age_ms,
                           "maximum_leg_skew_ms": self._d.settings.maximum_leg_skew_ms,
                           "minimum_return": self._d.settings.minimum_return},
            "run": {"id": run_id, "status": "COMPLETED", "started_at_ms": now_ms},
            "opportunity": {"id": opportunity_id, "status": status.value,
                            "relation_id": market.relation_id, "quantity": quantity,
                            "total_investment": total, "minimum_proceeds": proceeds,
                            "net_profit": profit, "net_return": rate},
            "economics": {"gross_investment": gross, "gross_proceeds": proceeds,
                          "fees": costs, "total_costs": total,
                          "net_profit": profit, "net_return": rate, "costs": cost_rows},
            "events": [{"id": market.event_id, "metadata": {}}],
            "markets": [{"id": market.market_id, "event_id": market.event_id,
                         "metadata": {"active": market.active, "tradeable": market.tradeable,
                                      "condition_id": market.condition_id}}],
            "tokens": [
                {"id": market.yes_token_id, "market_id": market.market_id,
                 "outcome": "YES", "metadata": {}},
                {"id": market.no_token_id, "market_id": market.market_id,
                 "outcome": "NO", "metadata": {}},
            ],
            "fee_schedules": fee_rows,
            "relation": {
                "set": {"id": market.relation_set_id, "version": market.relation_version,
                        "status": "active", "metadata": {"audited": True, "auditor": market.auditor},
                        "provenance": {"source": market.provenance_source,
                                       "content_hash": market.provenance_hash}},
                "relations": [{"id": market.relation_id, "kind": "BINARY_COMPLETE"}],
                "states": [{"id": "state-yes", "label": "YES"},
                           {"id": "state-no", "label": "NO"}],
                "payoffs": [
                    {"state_id": "state-yes", "token_id": market.yes_token_id, "amount": ONE},
                    {"state_id": "state-yes", "token_id": market.no_token_id, "amount": ZERO},
                    {"state_id": "state-no", "token_id": market.yes_token_id, "amount": ZERO},
                    {"state_id": "state-no", "token_id": market.no_token_id, "amount": ONE},
                ],
            },
            "books": book_rows, "legs": legs, "actions": actions,
            "risk": {"status": status.value, "reasons": list(risk_reasons),
                     "worst_leg_failure_loss": partial_loss,
                     "max_unhedged_notional": unhedged},
            "latency_metrics": latency,
            "notifications": ([{"id": f"notice-{opportunity_id}", "channel": "desktop",
                                "status": "PENDING", "sent_at_ms": None}]
                              if status is OpportunityStatus.SNAPSHOT_EXECUTABLE else []),
        }
        return EvidenceBundle.from_mapping(value)


__all__ = [
    "BinaryMarket", "EngineDependencies", "EngineResult", "FeeConfirmation",
    "StructuralArbitrageEngine",
]
