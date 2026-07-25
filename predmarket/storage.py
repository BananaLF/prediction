"""Transactional, exact, replayable opportunity evidence storage."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
from typing import Any

import aiosqlite

from predmarket.exact_math import decimal_ratio
from predmarket.risk import RiskInputs, assess_risk, worst_partial_fill


SCHEMA_VERSION = 2
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_OPPORTUNITY_STATUSES = {
    "REJECTED",
    "RESEARCH_CANDIDATE",
    "SNAPSHOT_EXECUTABLE",
}
class EvidenceConflictError(ValueError):
    """An immutable evidence identifier already exists with different content."""


def _identifier(name: str, value: object) -> str:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _ID.fullmatch(value):
        raise ValueError(f"{name} is not a safe identifier")
    return value


def _integer(name: str, value: object, *, nullable: bool = False) -> int | None:
    if nullable and value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")
    return value


def _decimal(name: str, value: object) -> Decimal:
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be an exact Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _nonnegative_decimal(name: str, value: object) -> Decimal:
    result = _decimal(name, value)
    if result < 0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("Decimal values must be finite")
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _canonicalize(value: object) -> Any:
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is Decimal:
        return _canonical_decimal(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            if type(key) is not str:
                raise TypeError("JSON object keys must be strings")
            result[key] = _canonicalize(child)
        return result
    if type(value) in (list, tuple):
        return [_canonicalize(child) for child in value]
    raise TypeError(f"unsupported evidence value: {type(value).__name__}")


def _restore_decimal(record: dict[str, Any], field: str) -> None:
    value = record.get(field)
    if type(value) is not str:
        raise ValueError(f"stored {field} is not a canonical decimal string")
    restored = Decimal(value)
    if _canonical_decimal(restored) != value:
        raise ValueError(f"stored {field} is not canonical")
    record[field] = restored


def _restore_schema_decimals(value: Any) -> dict[str, Any]:
    """Restore only schema-defined financial paths, never opaque metadata keys."""
    if not isinstance(value, dict):
        raise ValueError("stored evidence root must be an object")
    for field in ("minimum_return",):
        _restore_decimal(value["evaluation"], field)
    _restore_decimal(value["evaluation"], "evaluated_monotonic")
    if value["economics"]["status"] == "EVALUATED":
        for field in (
            "quantity", "total_investment", "minimum_proceeds", "net_profit",
            "net_return",
        ):
            _restore_decimal(value["opportunity"], field)
        for field in (
            "gross_investment", "gross_proceeds", "fees", "total_costs",
            "net_profit", "net_return",
        ):
            _restore_decimal(value["economics"], field)
        for cost in value["economics"]["costs"]:
            _restore_decimal(cost, "amount")
    for fee in value["fee_schedules"]:
        _restore_decimal(fee, "rate")
        _restore_decimal(fee, "exponent")
    for payoff in value["relation"]["payoffs"]:
        _restore_decimal(payoff, "amount")
    for book in value["discovery_books"] + value["books"]:
        _restore_decimal(book["snapshot"], "tick_size")
        _restore_decimal(book["snapshot"], "received_monotonic")
        for level in book["levels"]:
            _restore_decimal(level, "price")
            _restore_decimal(level, "size")
    for leg in value["legs"]:
        _restore_decimal(leg, "quantity")
        _restore_decimal(leg, "notional")
    for action in value["actions"]:
        _restore_decimal(action, "quantity")
        _restore_decimal(action, "amount")
    _restore_decimal(value["risk"], "worst_leg_failure_loss")
    _restore_decimal(value["risk"], "max_unhedged_notional")
    for field in value["risk"]["entry_costs"]:
        value["risk"]["entry_costs"][field] = Decimal(
            value["risk"]["entry_costs"][field]
        )
    for field in value["risk"]["immediate_unwind_values"]:
        value["risk"]["immediate_unwind_values"][field] = Decimal(
            value["risk"]["immediate_unwind_values"][field]
        )
    for field in (
        "minimum_return", "max_leg_failure_loss", "max_unhedged_notional",
    ):
        _restore_decimal(value["risk"]["thresholds"], field)
    if value["risk"]["inputs"] is not None:
        _restore_decimal(value["risk"]["inputs"], "mathematical_return")
    return value


def _required(mapping: Mapping[str, object], *names: str) -> None:
    missing = [name for name in names if name not in mapping]
    if missing:
        raise KeyError(f"missing required fields: {', '.join(missing)}")


def _mapping(name: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _validate_opaque_json(name: str, value: object) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if type(key) is not str:
                raise TypeError(f"{name} keys must be strings")
            _validate_opaque_json(f"{name}.{key}", child)
        return
    if type(value) in (list, tuple):
        for index, child in enumerate(value):
            _validate_opaque_json(f"{name}[{index}]", child)
        return
    raise TypeError(f"{name} contains unsupported JSON value")


def _sequence(name: str, value: object) -> list[object] | tuple[object, ...]:
    if type(value) not in (list, tuple):
        raise TypeError(f"{name} must be a list or tuple")
    return value


def _validate_unique_ids(name: str, records: object) -> None:
    seen: set[str] = set()
    for index, raw in enumerate(_sequence(name, records)):
        record = _mapping(f"{name}[{index}]", raw)
        _required(record, "id")
        identifier = _identifier(f"{name}[{index}].id", record["id"])
        if identifier in seen:
            raise ValueError(f"duplicate {name} identifier: {identifier}")
        seen.add(identifier)


def _validate_book_collection(
    name: str, records: object, token_ids: set[str]
) -> tuple[set[str], set[str]]:
    epoch_ids: set[str] = set()
    snapshot_ids: set[str] = set()
    for index, raw_book in enumerate(_sequence(name, records)):
        book = _mapping(f"{name}[{index}]", raw_book)
        _required(book, "epoch", "snapshot", "levels")
        epoch = _mapping("epoch", book["epoch"])
        snapshot = _mapping("snapshot", book["snapshot"])
        _required(epoch, "id", "token_id", "state", "started_at_ms")
        _required(
            snapshot, "id", "exchange_ts_ms", "received_ts_ms",
            "received_monotonic", "tick_size", "book_hash",
        )
        epoch_id = _identifier("epoch.id", epoch["id"])
        snapshot_id = _identifier("snapshot.id", snapshot["id"])
        if epoch_id in epoch_ids or snapshot_id in snapshot_ids:
            raise ValueError("duplicate epoch or snapshot identifier")
        epoch_ids.add(epoch_id)
        snapshot_ids.add(snapshot_id)
        if _identifier("epoch.token_id", epoch["token_id"]) not in token_ids:
            raise ValueError("book epoch references an unknown token")
        if epoch["state"] != "LIVE":
            raise ValueError("evidence books must be LIVE")
        _integer("epoch.started_at_ms", epoch["started_at_ms"])
        _integer("snapshot.exchange_ts_ms", snapshot["exchange_ts_ms"])
        _integer("snapshot.received_ts_ms", snapshot["received_ts_ms"])
        _nonnegative_decimal(
            "snapshot.received_monotonic", snapshot["received_monotonic"]
        )
        if type(snapshot["book_hash"]) is not str or not snapshot["book_hash"]:
            raise ValueError("snapshot book_hash must be nonempty")
        tick_size = _nonnegative_decimal("snapshot.tick_size", snapshot["tick_size"])
        if not Decimal("0") < tick_size <= Decimal("1"):
            raise ValueError("snapshot tick size must be in (0, 1]")
        for position, raw_level in enumerate(_sequence("levels", book["levels"])):
            level = _mapping("level", raw_level)
            _required(level, "side", "price", "size", "position")
            if level["side"] not in {"BUY", "SELL"}:
                raise ValueError("invalid level side")
            price = _decimal("level.price", level["price"])
            if not Decimal("0") < price < Decimal("1"):
                raise ValueError("level price must be in (0, 1)")
            if _nonnegative_decimal("level.size", level["size"]) == 0:
                raise ValueError("level size must be positive")
            if _integer("level.position", level["position"]) != position:
                raise ValueError("level positions must be contiguous and ordered")
    return epoch_ids, snapshot_ids


def _validate_bundle(raw: Mapping[str, object]) -> None:
    _required(
        raw,
        "version", "id", "producer", "evaluation", "run", "opportunity",
        "economics", "events", "markets",
        "tokens", "fee_schedules", "relation", "books", "legs", "actions",
        "discovery_books", "risk", "latency_metrics", "notifications",
    )
    if type(raw["version"]) is not int:
        raise TypeError("version must be an integer")
    if raw["version"] != SCHEMA_VERSION:
        raise ValueError(f"unsupported evidence version: {raw['version']!r}")
    _identifier("id", raw["id"])

    producer = _mapping("producer", raw["producer"])
    _required(producer, "engine", "version", "metadata")
    for name in ("engine", "version"):
        if type(producer[name]) is not str or not producer[name]:
            raise ValueError(f"producer.{name} must be a nonempty string")
    _mapping("producer.metadata", producer["metadata"])
    _validate_opaque_json("producer.metadata", producer["metadata"])

    evaluation = _mapping("evaluation", raw["evaluation"])
    _required(
        evaluation, "evaluated_at_ms", "maximum_book_age_ms",
        "maximum_leg_skew_ms", "maximum_processing_latency_ms",
        "evaluated_monotonic", "minimum_return",
    )
    for name in (
        "evaluated_at_ms", "maximum_book_age_ms", "maximum_leg_skew_ms",
        "maximum_processing_latency_ms",
    ):
        _integer(f"evaluation.{name}", evaluation[name])
    minimum_return = _nonnegative_decimal(
        "evaluation.minimum_return", evaluation["minimum_return"]
    )
    _nonnegative_decimal(
        "evaluation.evaluated_monotonic", evaluation["evaluated_monotonic"]
    )

    run = _mapping("run", raw["run"])
    _required(run, "id", "status", "started_at_ms")
    _identifier("run.id", run["id"])
    if run["status"] not in {"RUNNING", "COMPLETED", "FAILED"}:
        raise ValueError("invalid run status")
    _integer("run.started_at_ms", run["started_at_ms"])

    opportunity = _mapping("opportunity", raw["opportunity"])
    _required(opportunity, "id", "status", "relation_id")
    _identifier("opportunity.id", opportunity["id"])
    _identifier("opportunity.relation_id", opportunity["relation_id"])
    if opportunity["status"] not in _OPPORTUNITY_STATUSES:
        raise ValueError("invalid opportunity status")
    economics = _mapping("economics", raw["economics"])
    _required(economics, "status")
    if economics["status"] not in {"NOT_EVALUATED", "EVALUATED"}:
        raise ValueError("invalid economics status")
    if economics["status"] == "NOT_EVALUATED":
        if set(economics) != {"status", "reason"}:
            raise ValueError("not-evaluated economics must not contain financial fields")
        if type(economics["reason"]) is not str or not economics["reason"]:
            raise ValueError("not-evaluated economics requires a reason")
        forbidden = {
            "quantity", "total_investment", "minimum_proceeds",
            "net_profit", "net_return",
        }
        if forbidden & set(opportunity):
            raise ValueError("not-evaluated opportunity must not contain financial fields")
        if opportunity["status"] == "SNAPSHOT_EXECUTABLE":
            raise ValueError("executable opportunity economics must be evaluated")
        if opportunity["status"] != "REJECTED":
            raise ValueError("not-evaluated economics requires rejected status")
        costs: list[object] | tuple[object, ...] = []
    else:
        _required(
            opportunity, "quantity", "total_investment", "minimum_proceeds",
            "net_profit", "net_return",
        )
        for name in (
            "quantity", "total_investment", "minimum_proceeds", "net_profit",
            "net_return",
        ):
            _decimal(f"opportunity.{name}", opportunity[name])
        _nonnegative_decimal("opportunity.quantity", opportunity["quantity"])
        _nonnegative_decimal("opportunity.total_investment", opportunity["total_investment"])
        _nonnegative_decimal("opportunity.minimum_proceeds", opportunity["minimum_proceeds"])
        _required(
            economics, "gross_investment", "gross_proceeds", "fees", "total_costs",
            "net_profit", "net_return", "costs",
        )
        costs = _sequence("economics.costs", economics["costs"])
        _validate_unique_ids("economics.costs", costs)
    _validate_bundle_tail(raw, minimum_return, opportunity, economics, costs)


def _validate_bundle_tail(
    raw: Mapping[str, object],
    minimum_return: Decimal,
    opportunity: Mapping[str, object],
    economics: Mapping[str, object],
    costs: list[object] | tuple[object, ...],
) -> None:
    if economics["status"] == "EVALUATED":
        for name in ("gross_investment", "gross_proceeds", "fees", "total_costs"):
            _nonnegative_decimal(f"economics.{name}", economics[name])
        _decimal("economics.net_profit", economics["net_profit"])
        _decimal("economics.net_return", economics["net_return"])
        if (
            economics["total_costs"] != opportunity["total_investment"]
            or economics["gross_proceeds"] != opportunity["minimum_proceeds"]
            or economics["net_profit"] != opportunity["net_profit"]
            or economics["net_return"] != opportunity["net_return"]
        ):
            raise ValueError("economics and opportunity totals must agree")
        cost_total = Decimal("0")
        for item in costs:
            cost = _mapping("cost", item)
            _required(cost, "id", "kind", "amount")
            if type(cost["kind"]) is not str or not cost["kind"]:
                raise ValueError("cost kind must be a nonempty string")
            if ("leg_id" in cost) == ("component" in cost):
                raise ValueError("cost requires exactly one of leg_id or component")
            if "leg_id" in cost:
                _identifier("cost.leg_id", cost["leg_id"])
            elif type(cost["component"]) is not str or not cost["component"]:
                raise ValueError("cost component must be a nonempty string")
            cost_total += _nonnegative_decimal("cost.amount", cost["amount"])
        if cost_total != economics["fees"]:
            raise ValueError("cost breakdown must sum exactly to fees")
        if economics["gross_investment"] + economics["fees"] != economics["total_costs"]:
            raise ValueError("gross investment plus fees must equal total costs")
        if economics["gross_proceeds"] - economics["total_costs"] != economics["net_profit"]:
            raise ValueError("gross proceeds minus total costs must equal net profit")
        if economics["total_costs"] <= 0:
            raise ValueError("total costs must be positive")
        derived_return = decimal_ratio(economics["net_profit"], economics["total_costs"])
        if economics["net_return"] != derived_return:
            raise ValueError("net return must equal net profit divided by total costs")

    for name in (
        "events", "markets", "tokens", "fee_schedules", "legs", "actions",
        "latency_metrics", "notifications",
    ):
        _validate_unique_ids(name, raw[name])

    event_ids: set[str] = set()
    for item in _sequence("events", raw["events"]):
        event = _mapping("event", item)
        _required(event, "id", "metadata")
        event_ids.add(_identifier("event.id", event["id"]))
        _mapping("event.metadata", event["metadata"])
        _validate_opaque_json("event.metadata", event["metadata"])

    market_ids: set[str] = set()
    for item in _sequence("markets", raw["markets"]):
        market = _mapping("market", item)
        _required(market, "id", "event_id", "metadata")
        market_ids.add(_identifier("market.id", market["id"]))
        if _identifier("market.event_id", market["event_id"]) not in event_ids:
            raise ValueError("market references an unknown event")
        _mapping("market.metadata", market["metadata"])
        _validate_opaque_json("market.metadata", market["metadata"])
        for name in (
            "immediate_conversion_evidenced", "settlement_evidenced",
            "release_date_known",
        ):
            if type(market["metadata"].get(name)) is not bool:
                raise ValueError(f"market metadata {name} must be bool")

    token_ids: set[str] = set()
    for item in _sequence("tokens", raw["tokens"]):
        token = _mapping("token", item)
        _required(token, "id", "market_id", "outcome", "metadata")
        token_ids.add(_identifier("token.id", token["id"]))
        if _identifier("token.market_id", token["market_id"]) not in market_ids:
            raise ValueError("token references an unknown market")
        if type(token["outcome"]) is not str or not token["outcome"]:
            raise ValueError("token outcome must be a nonempty string")
        _mapping("token.metadata", token["metadata"])
        _validate_opaque_json("token.metadata", token["metadata"])

    for item in _sequence("fee_schedules", raw["fee_schedules"]):
        fee = _mapping("fee schedule", item)
        _required(
            fee, "id", "token_id", "rate", "exponent", "direction",
            "retrieved_at_ms", "source",
        )
        if _identifier("fee.token_id", fee["token_id"]) not in token_ids:
            raise ValueError("fee schedule references an unknown token")
        rate = _nonnegative_decimal("fee.rate", fee["rate"])
        if rate > Decimal("1"):
            raise ValueError("fee rate must not exceed one")
        exponent = _nonnegative_decimal("fee.exponent", fee["exponent"])
        if exponent != exponent.to_integral_value():
            raise ValueError("fee exponent must be integral")
        if fee["direction"] not in {"BUY", "SELL", "BOTH"}:
            raise ValueError("invalid fee direction")
        _integer("fee.retrieved_at_ms", fee["retrieved_at_ms"])
        if type(fee["source"]) is not str or not fee["source"]:
            raise ValueError("fee source must be a nonempty string")

    relation = _mapping("relation", raw["relation"])
    _required(relation, "set", "relations", "states", "payoffs")
    relation_set = _mapping("relation.set", relation["set"])
    _required(
        relation_set, "id", "version", "status", "metadata", "provenance",
    )
    _identifier("relation.set.id", relation_set["id"])
    if type(relation_set["version"]) is not int or relation_set["version"] <= 0:
        raise ValueError("relation.set.version must be a positive integer")
    if relation_set["status"] not in {"active", "pending"}:
        raise ValueError("invalid relation set status")
    audit_metadata = _mapping("relation.set.metadata", relation_set["metadata"])
    _validate_opaque_json("relation.set.metadata", audit_metadata)
    if type(audit_metadata.get("audited")) is not bool:
        raise ValueError("relation audit status must be explicit")
    if audit_metadata["audited"] and (
        type(audit_metadata.get("auditor")) is not str
        or not audit_metadata["auditor"]
    ):
        raise ValueError("audited relation set must identify its auditor")
    if opportunity["status"] == "SNAPSHOT_EXECUTABLE" and (
        relation_set["status"] != "active"
        or audit_metadata["audited"] is not True
    ):
        raise ValueError("executable evidence requires an active audited relation")
    provenance = _mapping("relation.set.provenance", relation_set["provenance"])
    _validate_opaque_json("relation.set.provenance", provenance)
    _required(provenance, "source", "content_hash")
    if any(
        type(provenance[name]) is not str or not provenance[name]
        for name in ("source", "content_hash")
    ):
        raise ValueError("relation provenance values must be nonempty strings")
    if not relation["relations"] or not relation["states"] or not relation["payoffs"]:
        raise ValueError("active relation must have relations, states, and payoffs")
    _validate_unique_ids("relation.relations", relation["relations"])
    _validate_unique_ids("relation.states", relation["states"])
    relation_ids = {
        _identifier("relation.id", _mapping("relation", item)["id"])
        for item in _sequence("relation.relations", relation["relations"])
    }
    if opportunity["relation_id"] not in relation_ids:
        raise ValueError("opportunity references an unknown relation")
    for item in relation["relations"]:
        relation_item = _mapping("relation", item)
        _required(relation_item, "id", "kind")
        if relation_item["kind"] not in {
            "BINARY_COMPLETE", "INVALID_BINARY", "UNKNOWN",
        }:
            raise ValueError("invalid relation kind")
    state_ids = {
        _identifier("state.id", _mapping("state", item)["id"])
        for item in _sequence("relation.states", relation["states"])
    }
    for item in relation["states"]:
        state = _mapping("state", item)
        _required(state, "id", "label")
        if type(state["label"]) is not str or not state["label"]:
            raise ValueError("state label must be a nonempty string")
    payoff_pairs: set[tuple[str, str]] = set()
    for item in _sequence("relation.payoffs", relation["payoffs"]):
        payoff = _mapping("payoff", item)
        _required(payoff, "state_id", "token_id", "amount")
        state_id = _identifier("payoff.state_id", payoff["state_id"])
        token_id = _identifier("payoff.token_id", payoff["token_id"])
        if state_id not in state_ids or token_id not in token_ids:
            raise ValueError("payoff references an unknown state or token")
        pair = (state_id, token_id)
        if pair in payoff_pairs:
            raise ValueError("duplicate payoff state/token pair")
        payoff_pairs.add(pair)
        amount = _nonnegative_decimal("payoff.amount", payoff["amount"])
        if amount > Decimal("1"):
            raise ValueError("payoff amount must not exceed one")
    expected_payoffs = {(state, token) for state in state_ids for token in token_ids}
    if payoff_pairs != expected_payoffs:
        raise ValueError("payoff matrix must cover every state/token pair exactly")
    if opportunity["status"] == "SNAPSHOT_EXECUTABLE":
        payoff_by_state = {
            state_id: tuple(
                payoff["amount"]
                for token_id in sorted(token_ids)
                for payoff in relation["payoffs"]
                if payoff["state_id"] == state_id and payoff["token_id"] == token_id
            )
            for state_id in state_ids
        }
        if (
            len(token_ids) != 2
            or len(state_ids) != 2
            or set(payoff_by_state.values()) != {
                (Decimal("1"), Decimal("0")),
                (Decimal("0"), Decimal("1")),
            }
            or any(item["kind"] != "BINARY_COMPLETE" for item in relation["relations"])
        ):
            raise ValueError("executable relation must have binary complete-set payoffs")

    discovery_epoch_ids, discovery_snapshot_ids = _validate_book_collection(
        "discovery_books", raw["discovery_books"], token_ids
    )
    book_ids, snapshot_ids = _validate_book_collection("books", raw["books"], token_ids)
    if discovery_epoch_ids & book_ids or discovery_snapshot_ids & snapshot_ids:
        raise ValueError("discovery and confirmation book identifiers must differ")
    pipeline_reason = _mapping(
        "producer.metadata",
        _mapping("producer", raw["producer"])["metadata"],
    ).get("pipeline_reason")
    discovery_tokens = {
        _mapping("book", item)["epoch"]["token_id"]
        for item in _sequence("discovery_books", raw["discovery_books"])
    }
    confirmation_tokens = {
        _mapping("book", item)["epoch"]["token_id"]
        for item in _sequence("books", raw["books"])
    }
    if pipeline_reason == "no_candidate" and (
        discovery_tokens != token_ids or confirmation_tokens
    ):
        raise ValueError("no-candidate evidence requires only complete discovery books")
    if pipeline_reason == "expired_before_confirmation" and (
        discovery_tokens != token_ids or confirmation_tokens != token_ids
    ):
        raise ValueError("expired evidence requires complete discovery and confirmation books")

    for item in _sequence("legs", raw["legs"]):
        leg = _mapping("leg", item)
        _required(leg, "id", "token_id", "side", "quantity", "notional")
        if _identifier("leg.token_id", leg["token_id"]) not in token_ids:
            raise ValueError("leg references an unknown token")
        if leg["side"] not in {"BUY", "SELL"}:
            raise ValueError("invalid leg side")
        if _nonnegative_decimal("leg.quantity", leg["quantity"]) == 0:
            raise ValueError("leg quantity must be positive")
        _nonnegative_decimal("leg.notional", leg["notional"])
    leg_ids = {
        _mapping("leg", item)["id"] for item in _sequence("legs", raw["legs"])
    }
    if any(
        cost.get("leg_id") not in leg_ids
        for cost in costs
        if "leg_id" in cost
    ):
        raise ValueError("cost references an unknown leg")

    for sequence, item in enumerate(_sequence("actions", raw["actions"])):
        action = _mapping("action", item)
        _required(action, "id", "kind", "sequence", "quantity", "amount")
        if action["kind"] not in {
            "BUY", "SELL", "SPLIT", "MERGE", "NEG_RISK_CONVERT", "REDEEM",
        }:
            raise ValueError("invalid action kind")
        if _integer("action.sequence", action["sequence"]) != sequence:
            raise ValueError("action sequences must be contiguous and ordered")
        if "token_id" in action and action["token_id"] is not None:
            if _identifier("action.token_id", action["token_id"]) not in token_ids:
                raise ValueError("action references an unknown token")
        _nonnegative_decimal("action.quantity", action["quantity"])
        _nonnegative_decimal("action.amount", action["amount"])

    risk = _mapping("risk", raw["risk"])
    _required(
        risk, "status", "reasons", "worst_leg_failure_loss",
        "max_unhedged_notional", "entry_costs", "immediate_unwind_values",
        "thresholds", "inputs", "assessment_reasons", "timing_reasons",
    )
    if risk["status"] not in _OPPORTUNITY_STATUSES:
        raise ValueError("invalid risk status")
    if risk["status"] != opportunity["status"]:
        raise ValueError("opportunity and risk status must match")
    if opportunity["status"] != "SNAPSHOT_EXECUTABLE" and raw["notifications"]:
        raise ValueError("notifications are forbidden for non-executable evidence")
    reasons = _sequence("risk.reasons", risk["reasons"])
    if any(type(reason) is not str or not reason for reason in reasons):
        raise ValueError("risk reasons must be nonempty strings")
    if len(set(reasons)) != len(reasons):
        raise ValueError("risk reasons must be unique")
    assessment_reasons = _sequence(
        "risk.assessment_reasons", risk["assessment_reasons"]
    )
    timing_reasons = _sequence("risk.timing_reasons", risk["timing_reasons"])
    for name, values in (
        ("assessment_reasons", assessment_reasons),
        ("timing_reasons", timing_reasons),
    ):
        if any(type(reason) is not str or not reason for reason in values):
            raise ValueError(f"risk {name} must contain nonempty strings")
        if len(set(values)) != len(values):
            raise ValueError(f"risk {name} must be unique")
    _nonnegative_decimal(
        "risk.worst_leg_failure_loss", risk["worst_leg_failure_loss"]
    )
    _nonnegative_decimal(
        "risk.max_unhedged_notional", risk["max_unhedged_notional"]
    )
    entry_costs = _mapping("risk.entry_costs", risk["entry_costs"])
    unwind_values = _mapping(
        "risk.immediate_unwind_values", risk["immediate_unwind_values"]
    )
    if set(entry_costs) != set(unwind_values):
        raise ValueError("risk entry and unwind token keys must match")
    risk_leg_tokens = {
        _mapping("leg", item)["token_id"]
        for item in _sequence("legs", raw["legs"])
    }
    if set(entry_costs) not in (set(), risk_leg_tokens):
        raise ValueError("risk entry and unwind keys must match opportunity legs")
    for name, values in (
        ("entry_costs", entry_costs),
        ("immediate_unwind_values", unwind_values),
    ):
        for token_id, amount in values.items():
            _identifier(f"risk.{name}.token_id", token_id)
            _nonnegative_decimal(f"risk.{name}[{token_id}]", amount)
    thresholds = _mapping("risk.thresholds", risk["thresholds"])
    _required(
        thresholds, "minimum_return", "max_leg_failure_loss",
        "max_unhedged_notional",
    )
    for name in (
        "minimum_return", "max_leg_failure_loss", "max_unhedged_notional",
    ):
        _nonnegative_decimal(f"risk.thresholds.{name}", thresholds[name])
    inputs = risk["inputs"]
    if economics["status"] == "EVALUATED":
        inputs = _mapping("risk.inputs", inputs)
        _required(
            inputs, "mathematical_return", "data_valid",
            "immediate_unwind_known", "unresolved_rule_risk",
            "unresolved_conversion_risk", "unresolved_settlement_risk",
            "release_date_known",
        )
        for name in (
            "data_valid", "immediate_unwind_known", "unresolved_rule_risk",
            "unresolved_conversion_risk", "unresolved_settlement_risk",
            "release_date_known",
        ):
            if type(inputs[name]) is not bool:
                raise TypeError(f"risk.inputs.{name} must be bool")
        mathematical_return = _decimal(
            "risk.inputs.mathematical_return", inputs["mathematical_return"]
        )
        if mathematical_return != economics["net_return"]:
            raise ValueError("risk mathematical return must match economics")
        partial = worst_partial_fill(entry_costs, unwind_values)
        if (
            partial.worst_leg_failure_loss != risk["worst_leg_failure_loss"]
            or partial.max_unhedged_notional != risk["max_unhedged_notional"]
        ):
            raise ValueError("partial-fill aggregates do not recompute")
        replayed = assess_risk(
            RiskInputs(
                mathematical_return, inputs["data_valid"],
                partial.worst_leg_failure_loss, partial.max_unhedged_notional,
                inputs["immediate_unwind_known"], inputs["unresolved_rule_risk"],
                inputs["unresolved_conversion_risk"],
                inputs["unresolved_settlement_risk"],
                inputs["release_date_known"],
            ),
            thresholds["minimum_return"],
            thresholds["max_leg_failure_loss"],
            thresholds["max_unhedged_notional"],
        )
        if replayed.status.value != risk["status"] or replayed.reasons != tuple(assessment_reasons):
            raise ValueError("risk assessment does not recompute")
        composed = tuple(dict.fromkeys((*assessment_reasons, *timing_reasons)))
        if tuple(reasons) != composed:
            raise ValueError("risk reasons must compose assessment and timing reasons")
    elif inputs is not None or entry_costs or unwind_values:
        raise ValueError("not-evaluated risk must not contain computed inputs")
    if risk["status"] != opportunity["status"]:
        raise ValueError("opportunity and risk status must match")

    for item in _sequence("latency_metrics", raw["latency_metrics"]):
        metric = _mapping("latency metric", item)
        _required(
            metric, "id", "exchange_ts_ms", "received_ts_ms",
            "processing_latency_ms",
        )
        _integer("latency.exchange_ts_ms", metric["exchange_ts_ms"])
        _integer("latency.received_ts_ms", metric["received_ts_ms"])
        _integer("latency.processing_latency_ms", metric["processing_latency_ms"])

    for item in _sequence("notifications", raw["notifications"]):
        notification = _mapping("notification", item)
        _required(notification, "id", "channel", "status", "sent_at_ms")
        if notification["channel"] != "desktop":
            raise ValueError("only desktop notification records are supported")
        if notification["status"] not in {"PENDING", "SENT", "FAILED"}:
            raise ValueError("invalid notification status")
        _integer(
            "notification.sent_at_ms",
            notification["sent_at_ms"],
            nullable=True,
        )

    if opportunity["status"] == "SNAPSHOT_EXECUTABLE":
        if (
            opportunity["quantity"] <= 0
            or opportunity["total_investment"] <= 0
            or opportunity["net_profit"] <= 0
            or opportunity["net_return"] <= 0
        ):
            raise ValueError(
                "executable quantity, investment, profit, and return must be positive"
            )
        if opportunity["net_return"] < minimum_return:
            raise ValueError("executable return is below the recorded minimum threshold")
        for section in (
            "legs", "actions", "books", "fee_schedules", "latency_metrics",
        ):
            if not raw[section]:
                raise ValueError(
                    f"SNAPSHOT_EXECUTABLE evidence requires nonempty {section}"
                )
        leg_tokens = [
            _mapping("leg", item)["token_id"]
            for item in _sequence("legs", raw["legs"])
        ]
        if len(leg_tokens) != len(set(leg_tokens)):
            raise ValueError("executable evidence permits one leg per token")
        book_tokens = [
            _mapping("book", item)["epoch"]["token_id"]
            for item in _sequence("books", raw["books"])
        ]
        if sorted(book_tokens) != sorted(leg_tokens):
            raise ValueError("each executable leg requires exactly one LIVE book")
        if any(
            not _mapping("book", item)["levels"]
            for item in _sequence("books", raw["books"])
        ):
            raise ValueError("each executable book requires full depth levels")
        for item in _sequence("legs", raw["legs"]):
            leg = _mapping("leg", item)
            applicable = [
                fee
                for fee in _sequence("fee_schedules", raw["fee_schedules"])
                if _mapping("fee", fee)["token_id"] == leg["token_id"]
                and _mapping("fee", fee)["direction"] in {leg["side"], "BOTH"}
            ]
            if len(applicable) != 1:
                raise ValueError(
                    "each executable leg requires exactly one applicable fee schedule"
                )
    elif raw["notifications"]:
        raise ValueError("notifications are forbidden for non-executable evidence")

    # Validate every remaining value and every exact-decimal field recursively.
    _canonicalize(raw)


@dataclass(frozen=True)
class EvidenceBundle:
    """Immutable evidence represented by its canonical, versioned JSON bytes."""

    canonical_json: str

    def __post_init__(self) -> None:
        if type(self.canonical_json) is not str:
            raise TypeError("canonical_json must be a string")
        try:
            decoded = json.loads(self.canonical_json)
            restored = _restore_schema_decimals(decoded)
            _validate_bundle(restored)
            rebuilt = json.dumps(
                _canonicalize(restored),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (json.JSONDecodeError, InvalidOperation) as exc:
            raise ValueError("invalid canonical evidence JSON") from exc
        if rebuilt != self.canonical_json:
            raise ValueError("evidence JSON is not canonical")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "EvidenceBundle":
        if not isinstance(value, Mapping):
            raise TypeError("evidence must be a mapping")
        copied = dict(value)
        _validate_bundle(copied)
        canonical = _canonicalize(copied)
        return cls(
            json.dumps(
                canonical,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )

    @property
    def data(self) -> dict[str, Any]:
        return _restore_schema_decimals(json.loads(self.canonical_json))

    @property
    def id(self) -> str:
        return self.data["id"]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS evidence_bundles (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    canonical_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    bundle_id TEXT NOT NULL REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    PRIMARY KEY (bundle_id, id)
);
CREATE TABLE IF NOT EXISTS events (
    bundle_id TEXT NOT NULL REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id)
);
CREATE TABLE IF NOT EXISTS markets (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, event_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, event_id) REFERENCES events(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS tokens (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, market_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, market_id) REFERENCES markets(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS fee_schedules (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, token_id TEXT NOT NULL,
    rate TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, token_id) REFERENCES tokens(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS relation_sets (
    bundle_id TEXT NOT NULL REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    id TEXT NOT NULL, version INTEGER NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY (bundle_id, id)
);
CREATE TABLE IF NOT EXISTS relations (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, relation_set_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, relation_set_id) REFERENCES relation_sets(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS relation_states (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, relation_set_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, relation_set_id) REFERENCES relation_sets(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS relation_payoffs (
    bundle_id TEXT NOT NULL, relation_set_id TEXT NOT NULL, position INTEGER NOT NULL,
    amount TEXT NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY (bundle_id, relation_set_id, position),
    FOREIGN KEY (bundle_id, relation_set_id) REFERENCES relation_sets(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS book_epochs (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, token_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, token_id) REFERENCES tokens(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS snapshots (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, epoch_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, epoch_id) REFERENCES book_epochs(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS levels (
    bundle_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, position INTEGER NOT NULL,
    side TEXT NOT NULL, price TEXT NOT NULL, size TEXT NOT NULL,
    PRIMARY KEY (bundle_id, snapshot_id, position),
    FOREIGN KEY (bundle_id, snapshot_id) REFERENCES snapshots(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS opportunities (
    bundle_id TEXT PRIMARY KEY REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    id TEXT NOT NULL, run_id TEXT NOT NULL, status TEXT NOT NULL,
    payload TEXT NOT NULL, UNIQUE (bundle_id, id),
    FOREIGN KEY (bundle_id, run_id) REFERENCES runs(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS legs (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, opportunity_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, opportunity_id) REFERENCES opportunities(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS actions (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, opportunity_id TEXT NOT NULL,
    sequence INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, opportunity_id) REFERENCES opportunities(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS risk_assessments (
    bundle_id TEXT PRIMARY KEY, opportunity_id TEXT NOT NULL, status TEXT NOT NULL,
    payload TEXT NOT NULL,
    FOREIGN KEY (bundle_id, opportunity_id) REFERENCES opportunities(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS latency_metrics (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, opportunity_id TEXT NOT NULL,
    payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, opportunity_id) REFERENCES opportunities(bundle_id, id)
);
CREATE TABLE IF NOT EXISTS notifications (
    bundle_id TEXT NOT NULL, id TEXT NOT NULL, opportunity_id TEXT NOT NULL,
    status TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (bundle_id, id),
    FOREIGN KEY (bundle_id, opportunity_id) REFERENCES opportunities(bundle_id, id)
);
"""


def _json(value: object) -> str:
    return json.dumps(
        _canonicalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


class OpportunityStore:
    def __init__(self, path: str | Path) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("database path must be str or Path")
        rendered = str(path)
        if not rendered or "\x00" in rendered:
            raise ValueError("database path is invalid")
        self._path = rendered
        self._connection: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def __aenter__(self) -> "OpportunityStore":
        return await self.open()

    async def open(self) -> "OpportunityStore":
        """Open and initialize the database; repeated calls are harmless."""
        if self._connection is not None:
            return self
        if self._path != ":memory:":
            path = Path(self._path).expanduser()
            if path.exists() and path.is_dir():
                raise ValueError("database path points to a directory")
            path.parent.mkdir(parents=True, exist_ok=True)
            self._path = str(path)
        connection = await aiosqlite.connect(self._path)
        try:
            await connection.execute("PRAGMA foreign_keys = ON")
            await connection.execute("PRAGMA journal_mode = WAL")
            version_rows = await connection.execute_fetchall("PRAGMA user_version")
            existing_version = int(version_rows[0][0])
            if existing_version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema {existing_version} is newer than supported "
                    f"schema {SCHEMA_VERSION}"
                )
            await connection.executescript(_SCHEMA)
            await connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
                (SCHEMA_VERSION,),
            )
            await connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            await connection.commit()
        except BaseException:
            await connection.close()
            raise
        self._connection = connection
        return self

    async def initialize(self) -> "OpportunityStore":
        """Public compatibility name for opening and migrating the store."""
        return await self.open()

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    def _require_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("opportunity store is not open")
        return self._connection

    async def save(self, bundle: EvidenceBundle) -> bool:
        if not isinstance(bundle, EvidenceBundle):
            raise TypeError("bundle must be EvidenceBundle")
        data = bundle.data
        bundle_id = data["id"]
        async with self._write_lock:
            connection = self._require_connection()
            await connection.execute("BEGIN IMMEDIATE")
            try:
                row = await connection.execute_fetchall(
                    "SELECT canonical_json FROM evidence_bundles WHERE id = ?",
                    (bundle_id,),
                )
                if row:
                    if row[0][0] != bundle.canonical_json:
                        raise EvidenceConflictError(
                            f"conflicting evidence bundle: {bundle_id}"
                        )
                    await connection.rollback()
                    return False
                await self._insert_bundle(data, bundle.canonical_json)
                await connection.commit()
                return True
            except BaseException:
                await connection.rollback()
                raise

    async def _insert_bundle(self, data: dict[str, Any], canonical: str) -> None:
        connection = self._require_connection()
        bundle_id = data["id"]
        await connection.execute(
            "INSERT INTO evidence_bundles VALUES (?, ?, ?)",
            (bundle_id, data["version"], canonical),
        )
        run = data["run"]
        await self._connection.execute(
            "INSERT INTO runs VALUES (?, ?, ?, ?)",
            (bundle_id, run["id"], run["status"], run["started_at_ms"]),
        )
        for event in data["events"]:
            await self._connection.execute(
                "INSERT INTO events VALUES (?, ?, ?)",
                (bundle_id, event["id"], _json(event)),
            )
        for market in data["markets"]:
            await self._connection.execute(
                "INSERT INTO markets VALUES (?, ?, ?, ?)",
                (bundle_id, market["id"], market["event_id"], _json(market)),
            )
        for token in data["tokens"]:
            await self._connection.execute(
                "INSERT INTO tokens VALUES (?, ?, ?, ?)",
                (bundle_id, token["id"], token["market_id"], _json(token)),
            )
        for fee in data["fee_schedules"]:
            await self._connection.execute(
                "INSERT INTO fee_schedules VALUES (?, ?, ?, ?, ?)",
                (
                    bundle_id, fee["id"], fee["token_id"],
                    _canonical_decimal(fee["rate"]), _json(fee),
                ),
            )
        relation = data["relation"]
        relation_set = relation["set"]
        set_id = relation_set["id"]
        await self._connection.execute(
            "INSERT INTO relation_sets VALUES (?, ?, ?, ?)",
            (bundle_id, set_id, relation_set["version"], _json(relation_set)),
        )
        for item in relation["relations"]:
            await self._connection.execute(
                "INSERT INTO relations VALUES (?, ?, ?, ?)",
                (bundle_id, item["id"], set_id, _json(item)),
            )
        for state in relation["states"]:
            await self._connection.execute(
                "INSERT INTO relation_states VALUES (?, ?, ?, ?)",
                (bundle_id, state["id"], set_id, _json(state)),
            )
        for position, payoff in enumerate(relation["payoffs"]):
            await self._connection.execute(
                "INSERT INTO relation_payoffs VALUES (?, ?, ?, ?, ?)",
                (
                    bundle_id, set_id, position,
                    _canonical_decimal(payoff["amount"]), _json(payoff),
                ),
            )
        for book in data["books"]:
            epoch, snapshot = book["epoch"], book["snapshot"]
            await self._connection.execute(
                "INSERT INTO book_epochs VALUES (?, ?, ?, ?)",
                (bundle_id, epoch["id"], epoch["token_id"], _json(epoch)),
            )
            await self._connection.execute(
                "INSERT INTO snapshots VALUES (?, ?, ?, ?)",
                (bundle_id, snapshot["id"], epoch["id"], _json(snapshot)),
            )
            for level in book["levels"]:
                await self._connection.execute(
                    "INSERT INTO levels VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        bundle_id, snapshot["id"], level["position"], level["side"],
                        _canonical_decimal(level["price"]),
                        _canonical_decimal(level["size"]),
                    ),
                )
        opportunity = data["opportunity"]
        await self._connection.execute(
            "INSERT INTO opportunities VALUES (?, ?, ?, ?, ?)",
            (
                bundle_id, opportunity["id"], run["id"],
                opportunity["status"], _json(opportunity),
            ),
        )
        for leg in data["legs"]:
            await self._connection.execute(
                "INSERT INTO legs VALUES (?, ?, ?, ?)",
                (bundle_id, leg["id"], opportunity["id"], _json(leg)),
            )
        for action in data["actions"]:
            await self._connection.execute(
                "INSERT INTO actions VALUES (?, ?, ?, ?, ?)",
                (
                    bundle_id, action["id"], opportunity["id"],
                    action["sequence"], _json(action),
                ),
            )
        risk = data["risk"]
        await self._connection.execute(
            "INSERT INTO risk_assessments VALUES (?, ?, ?, ?)",
            (bundle_id, opportunity["id"], risk["status"], _json(risk)),
        )
        for metric in data["latency_metrics"]:
            await self._connection.execute(
                "INSERT INTO latency_metrics VALUES (?, ?, ?, ?)",
                (bundle_id, metric["id"], opportunity["id"], _json(metric)),
            )
        for notification in data["notifications"]:
            await self._connection.execute(
                "INSERT INTO notifications VALUES (?, ?, ?, ?, ?)",
                (
                    bundle_id, notification["id"], opportunity["id"],
                    notification["status"], _json(notification),
                ),
            )

    async def replay(self, bundle_id: str) -> EvidenceBundle:
        _identifier("bundle_id", bundle_id)
        rows = await self._require_connection().execute_fetchall(
            "SELECT canonical_json FROM evidence_bundles WHERE id = ?",
            (bundle_id,),
        )
        if not rows:
            raise KeyError(bundle_id)
        # The canonical payload is immutable; validate again to detect corruption.
        decoded = json.loads(rows[0][0])
        replayed = EvidenceBundle.from_mapping(_restore_schema_decimals(decoded))
        if replayed.canonical_json != rows[0][0]:
            raise ValueError("stored evidence is not canonical")
        return replayed

    async def list_opportunities(self) -> list[tuple[str, str, str]]:
        rows = await self._require_connection().execute_fetchall(
            "SELECT id, status, bundle_id FROM opportunities ORDER BY bundle_id"
        )
        return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]

    async def list_runs(self) -> list[tuple[str, str]]:
        rows = await self._require_connection().execute_fetchall(
            "SELECT id, status FROM runs ORDER BY bundle_id"
        )
        return [(str(row[0]), str(row[1])) for row in rows]
