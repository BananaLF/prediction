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


SCHEMA_VERSION = 1
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_OPPORTUNITY_STATUSES = {
    "REJECTED",
    "RESEARCH_CANDIDATE",
    "SNAPSHOT_EXECUTABLE",
}
_DECIMAL_FIELDS = {
    "amount",
    "exponent",
    "max_unhedged_notional",
    "minimum_proceeds",
    "net_profit",
    "net_return",
    "notional",
    "price",
    "quantity",
    "rate",
    "size",
    "tick_size",
    "total_investment",
    "worst_leg_failure_loss",
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


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("Decimal values must be finite")
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _canonicalize(value: object, *, field: str | None = None) -> Any:
    if field in _DECIMAL_FIELDS:
        return _canonical_decimal(_decimal(field, value))
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is Decimal:
        return _canonical_decimal(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            if type(key) is not str:
                raise TypeError("JSON object keys must be strings")
            result[key] = _canonicalize(child, field=key)
        return result
    if type(value) in (list, tuple):
        return [_canonicalize(child) for child in value]
    raise TypeError(f"unsupported evidence value: {type(value).__name__}")


def _restore_decimals(value: Any, *, field: str | None = None) -> Any:
    if field in _DECIMAL_FIELDS:
        if type(value) is not str:
            raise ValueError(f"stored {field} is not a canonical decimal string")
        restored = Decimal(value)
        if _canonical_decimal(restored) != value:
            raise ValueError(f"stored {field} is not canonical")
        return restored
    if isinstance(value, dict):
        return {
            key: _restore_decimals(child, field=key)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_restore_decimals(child) for child in value]
    return value


def _required(mapping: Mapping[str, object], *names: str) -> None:
    missing = [name for name in names if name not in mapping]
    if missing:
        raise KeyError(f"missing required fields: {', '.join(missing)}")


def _mapping(name: str, value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


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


def _validate_bundle(raw: Mapping[str, object]) -> None:
    _required(
        raw,
        "version", "id", "run", "opportunity", "events", "markets",
        "tokens", "fee_schedules", "relation", "books", "legs", "actions",
        "risk", "latency_metrics", "notifications",
    )
    if type(raw["version"]) is not int:
        raise TypeError("version must be an integer")
    if raw["version"] != SCHEMA_VERSION:
        raise ValueError(f"unsupported evidence version: {raw['version']!r}")
    _identifier("id", raw["id"])

    run = _mapping("run", raw["run"])
    _required(run, "id", "status", "started_at_ms")
    _identifier("run.id", run["id"])
    if run["status"] not in {"RUNNING", "COMPLETED", "FAILED"}:
        raise ValueError("invalid run status")
    _integer("run.started_at_ms", run["started_at_ms"])

    opportunity = _mapping("opportunity", raw["opportunity"])
    _required(
        opportunity, "id", "status", "relation_id", "quantity",
        "total_investment", "minimum_proceeds", "net_profit", "net_return",
    )
    _identifier("opportunity.id", opportunity["id"])
    _identifier("opportunity.relation_id", opportunity["relation_id"])
    if opportunity["status"] not in _OPPORTUNITY_STATUSES:
        raise ValueError("invalid opportunity status")
    for name in (
        "quantity", "total_investment", "minimum_proceeds", "net_profit",
        "net_return",
    ):
        _decimal(f"opportunity.{name}", opportunity[name])

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

    market_ids: set[str] = set()
    for item in _sequence("markets", raw["markets"]):
        market = _mapping("market", item)
        _required(market, "id", "event_id", "metadata")
        market_ids.add(_identifier("market.id", market["id"]))
        if _identifier("market.event_id", market["event_id"]) not in event_ids:
            raise ValueError("market references an unknown event")
        _mapping("market.metadata", market["metadata"])

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

    for item in _sequence("fee_schedules", raw["fee_schedules"]):
        fee = _mapping("fee schedule", item)
        _required(
            fee, "id", "token_id", "rate", "exponent", "direction",
            "retrieved_at_ms", "source",
        )
        if _identifier("fee.token_id", fee["token_id"]) not in token_ids:
            raise ValueError("fee schedule references an unknown token")
        _decimal("fee.rate", fee["rate"])
        _decimal("fee.exponent", fee["exponent"])
        if fee["direction"] not in {"BUY", "SELL", "BOTH"}:
            raise ValueError("invalid fee direction")
        _integer("fee.retrieved_at_ms", fee["retrieved_at_ms"])
        if type(fee["source"]) is not str or not fee["source"]:
            raise ValueError("fee source must be a nonempty string")

    relation = _mapping("relation", raw["relation"])
    _required(relation, "set", "relations", "states", "payoffs")
    relation_set = _mapping("relation.set", relation["set"])
    _required(relation_set, "id", "version", "status", "metadata")
    _identifier("relation.set.id", relation_set["id"])
    if type(relation_set["version"]) is not int or relation_set["version"] <= 0:
        raise ValueError("relation.set.version must be a positive integer")
    if relation_set["status"] != "active":
        raise ValueError("relation set must be active")
    _validate_unique_ids("relation.relations", relation["relations"])
    _validate_unique_ids("relation.states", relation["states"])
    relation_ids = {
        _identifier("relation.id", _mapping("relation", item)["id"])
        for item in _sequence("relation.relations", relation["relations"])
    }
    if opportunity["relation_id"] not in relation_ids:
        raise ValueError("opportunity references an unknown relation")
    state_ids = {
        _identifier("state.id", _mapping("state", item)["id"])
        for item in _sequence("relation.states", relation["states"])
    }
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
        _decimal("payoff.amount", payoff["amount"])

    book_ids: set[str] = set()
    snapshot_ids: set[str] = set()
    for index, raw_book in enumerate(_sequence("books", raw["books"])):
        book = _mapping(f"books[{index}]", raw_book)
        _required(book, "epoch", "snapshot", "levels")
        epoch = _mapping("epoch", book["epoch"])
        snapshot = _mapping("snapshot", book["snapshot"])
        _required(epoch, "id", "token_id", "state", "started_at_ms")
        _required(
            snapshot, "id", "exchange_ts_ms", "received_ts_ms", "tick_size",
        )
        epoch_id = _identifier("epoch.id", epoch["id"])
        snapshot_id = _identifier("snapshot.id", snapshot["id"])
        if epoch_id in book_ids or snapshot_id in snapshot_ids:
            raise ValueError("duplicate epoch or snapshot identifier")
        book_ids.add(epoch_id)
        snapshot_ids.add(snapshot_id)
        if _identifier("epoch.token_id", epoch["token_id"]) not in token_ids:
            raise ValueError("book epoch references an unknown token")
        if epoch["state"] != "LIVE":
            raise ValueError("evidence books must be LIVE")
        _integer("epoch.started_at_ms", epoch["started_at_ms"])
        _integer("snapshot.exchange_ts_ms", snapshot["exchange_ts_ms"])
        _integer("snapshot.received_ts_ms", snapshot["received_ts_ms"])
        _decimal("snapshot.tick_size", snapshot["tick_size"])
        for position, raw_level in enumerate(_sequence("levels", book["levels"])):
            level = _mapping("level", raw_level)
            _required(level, "side", "price", "size", "position")
            if level["side"] not in {"BUY", "SELL"}:
                raise ValueError("invalid level side")
            _decimal("level.price", level["price"])
            _decimal("level.size", level["size"])
            if _integer("level.position", level["position"]) != position:
                raise ValueError("level positions must be contiguous and ordered")

    for item in _sequence("legs", raw["legs"]):
        leg = _mapping("leg", item)
        _required(leg, "id", "token_id", "side", "quantity", "notional")
        if _identifier("leg.token_id", leg["token_id"]) not in token_ids:
            raise ValueError("leg references an unknown token")
        if leg["side"] not in {"BUY", "SELL"}:
            raise ValueError("invalid leg side")
        _decimal("leg.quantity", leg["quantity"])
        _decimal("leg.notional", leg["notional"])

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
        _decimal("action.quantity", action["quantity"])
        _decimal("action.amount", action["amount"])

    risk = _mapping("risk", raw["risk"])
    _required(
        risk, "status", "reasons", "worst_leg_failure_loss",
        "max_unhedged_notional",
    )
    if risk["status"] not in _OPPORTUNITY_STATUSES:
        raise ValueError("invalid risk status")
    reasons = _sequence("risk.reasons", risk["reasons"])
    if any(type(reason) is not str or not reason for reason in reasons):
        raise ValueError("risk reasons must be nonempty strings")
    if len(set(reasons)) != len(reasons):
        raise ValueError("risk reasons must be unique")
    _decimal("risk.worst_leg_failure_loss", risk["worst_leg_failure_loss"])
    _decimal("risk.max_unhedged_notional", risk["max_unhedged_notional"])

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
        if type(notification["channel"]) is not str or not notification["channel"]:
            raise ValueError("notification channel must be a nonempty string")
        if notification["status"] not in {"PENDING", "SENT", "FAILED"}:
            raise ValueError("invalid notification status")
        _integer(
            "notification.sent_at_ms",
            notification["sent_at_ms"],
            nullable=True,
        )

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
            restored = _restore_decimals(decoded)
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
        return json.loads(self.canonical_json)

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
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class OpportunityStore:
    def __init__(self, path: str | Path) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("database path must be str or Path")
        rendered = str(path)
        if not rendered or "\x00" in rendered:
            raise ValueError("database path is invalid")
        self._path = rendered
        self._connection: aiosqlite.Connection
        self._write_lock = asyncio.Lock()

    async def __aenter__(self) -> "OpportunityStore":
        if self._path != ":memory:":
            path = Path(self._path).expanduser()
            if path.exists() and path.is_dir():
                raise ValueError("database path points to a directory")
            path.parent.mkdir(parents=True, exist_ok=True)
            self._path = str(path)
        self._connection = await aiosqlite.connect(self._path)
        await self._connection.execute("PRAGMA foreign_keys = ON")
        await self._connection.execute("PRAGMA journal_mode = WAL")
        version_rows = await self._connection.execute_fetchall("PRAGMA user_version")
        existing_version = int(version_rows[0][0])
        if existing_version > SCHEMA_VERSION:
            await self._connection.close()
            raise RuntimeError(
                f"database schema {existing_version} is newer than supported "
                f"schema {SCHEMA_VERSION}"
            )
        await self._connection.executescript(_SCHEMA)
        await self._connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?)",
            (SCHEMA_VERSION,),
        )
        await self._connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        await self._connection.commit()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._connection.close()

    async def save(self, bundle: EvidenceBundle) -> bool:
        if not isinstance(bundle, EvidenceBundle):
            raise TypeError("bundle must be EvidenceBundle")
        data = bundle.data
        bundle_id = data["id"]
        async with self._write_lock:
            await self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = await self._connection.execute_fetchall(
                    "SELECT canonical_json FROM evidence_bundles WHERE id = ?",
                    (bundle_id,),
                )
                if row:
                    if row[0][0] != bundle.canonical_json:
                        raise EvidenceConflictError(
                            f"conflicting evidence bundle: {bundle_id}"
                        )
                    await self._connection.rollback()
                    return False
                await self._insert_bundle(data, bundle.canonical_json)
                await self._connection.commit()
                return True
            except BaseException:
                await self._connection.rollback()
                raise

    async def _insert_bundle(self, data: dict[str, Any], canonical: str) -> None:
        bundle_id = data["id"]
        await self._connection.execute(
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
                (bundle_id, fee["id"], fee["token_id"], fee["rate"], _json(fee)),
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
                (bundle_id, set_id, position, payoff["amount"], _json(payoff)),
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
                        level["price"], level["size"],
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
        rows = await self._connection.execute_fetchall(
            "SELECT canonical_json FROM evidence_bundles WHERE id = ?",
            (bundle_id,),
        )
        if not rows:
            raise KeyError(bundle_id)
        # The canonical payload is immutable; validate again to detect corruption.
        decoded = json.loads(rows[0][0])
        replayed = EvidenceBundle.from_mapping(_restore_decimals(decoded))
        if replayed.canonical_json != rows[0][0]:
            raise ValueError("stored evidence is not canonical")
        return replayed

    async def list_opportunities(self) -> list[tuple[str, str, str]]:
        rows = await self._connection.execute_fetchall(
            "SELECT id, status, bundle_id FROM opportunities ORDER BY bundle_id"
        )
        return [(str(row[0]), str(row[1]), str(row[2])) for row in rows]

    async def list_runs(self) -> list[tuple[str, str]]:
        rows = await self._connection.execute_fetchall(
            "SELECT id, status FROM runs ORDER BY bundle_id"
        )
        return [(str(row[0]), str(row[1])) for row in rows]
