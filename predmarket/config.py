"""Strict configuration loading for the Greenfield application."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseConfig:
    path: Path
    busy_timeout_ms: int
    writer_queue_capacity: int


@dataclass(frozen=True)
class PolymarketConfig:
    sync_interval_seconds: int
    request_timeout_seconds: int
    reconnect_max_seconds: int
    fee_schedule_max_age_seconds: int


@dataclass(frozen=True)
class RuntimeConfig:
    market_change_queue_capacity: int
    watch_market_limit: int
    watch_minimum_end_horizon_seconds: int
    market_stream_queue_capacity: int


@dataclass(frozen=True)
class StrategyConfig:
    bankroll: Decimal
    minimum_return_rate: Decimal
    maximum_risk_rate: Decimal
    maximum_unhedged_notional: Decimal
    safety_buffer_rate: Decimal
    conversion_cost: Decimal
    maximum_book_age_ms: int
    exchange_clock_skew_warning_ms: int
    maximum_leg_skew_ms: int


@dataclass(frozen=True)
class SignalConfig:
    profit_change_rate: Decimal
    risk_rate_change: Decimal
    quantity_change_rate: Decimal
    minimum_update_interval_seconds: int


@dataclass(frozen=True)
class RelationsConfig:
    llm_enabled: bool


@dataclass(frozen=True)
class NotificationConfig:
    terminal_enabled: bool
    desktop_enabled: bool


@dataclass(frozen=True)
class AppConfig:
    database: DatabaseConfig
    polymarket: PolymarketConfig
    runtime: RuntimeConfig
    strategy: StrategyConfig
    signal: SignalConfig
    relations: RelationsConfig
    notification: NotificationConfig

    @classmethod
    def load(cls, path: Path) -> "AppConfig":
        raw = _load_mapping(path)
        _require_keys(
            raw,
            {
                "database",
                "polymarket",
                "runtime",
                "strategy",
                "signal",
                "relations",
                "notification",
            },
            "configuration",
        )
        return cls(
            database=_database_config(_section(raw, "database")),
            polymarket=_polymarket_config(_section(raw, "polymarket")),
            runtime=_runtime_config(_section(raw, "runtime")),
            strategy=_strategy_config(_section(raw, "strategy")),
            signal=_signal_config(_section(raw, "signal")),
            relations=_relations_config(_section(raw, "relations")),
            notification=_notification_config(_section(raw, "notification")),
        )


def _database_config(raw: dict[str, Any]) -> DatabaseConfig:
    _require_keys(raw, {"path", "busy_timeout_ms", "writer_queue_capacity"}, "database")
    return DatabaseConfig(
        path=Path(_string(raw, "path", "database")),
        busy_timeout_ms=_integer(raw, "busy_timeout_ms", "database"),
        writer_queue_capacity=_integer(raw, "writer_queue_capacity", "database"),
    )


def _polymarket_config(raw: dict[str, Any]) -> PolymarketConfig:
    _require_keys(
        raw,
        {
            "sync_interval_seconds",
            "request_timeout_seconds",
            "reconnect_max_seconds",
            "fee_schedule_max_age_seconds",
        },
        "polymarket",
    )
    return PolymarketConfig(
        sync_interval_seconds=_integer(raw, "sync_interval_seconds", "polymarket"),
        request_timeout_seconds=_integer(raw, "request_timeout_seconds", "polymarket"),
        reconnect_max_seconds=_integer(raw, "reconnect_max_seconds", "polymarket"),
        fee_schedule_max_age_seconds=_integer(raw, "fee_schedule_max_age_seconds", "polymarket"),
    )


def _runtime_config(raw: dict[str, Any]) -> RuntimeConfig:
    _require_keys(
        raw,
        {
            "market_change_queue_capacity",
            "watch_market_limit",
            "watch_minimum_end_horizon_seconds",
            "market_stream_queue_capacity",
        },
        "runtime",
    )
    return RuntimeConfig(
        market_change_queue_capacity=_integer(raw, "market_change_queue_capacity", "runtime"),
        watch_market_limit=_integer(raw, "watch_market_limit", "runtime"),
        watch_minimum_end_horizon_seconds=_integer(
            raw, "watch_minimum_end_horizon_seconds", "runtime"
        ),
        market_stream_queue_capacity=_integer(
            raw, "market_stream_queue_capacity", "runtime"
        ),
    )


def _strategy_config(raw: dict[str, Any]) -> StrategyConfig:
    warning_key = "exchange_clock_skew_warning_ms"
    legacy_key = "maximum_exchange_clock_skew_ms"
    if warning_key in raw and legacy_key in raw:
        raise ValueError(
            "strategy cannot contain both "
            f"{legacy_key} and {warning_key}"
        )
    if legacy_key in raw:
        raw = dict(raw)
        raw[warning_key] = raw.pop(legacy_key)
        _LOGGER.warning(
            "strategy.%s is deprecated; use strategy.%s; exchange clock skew "
            "is diagnostic only and no longer rejects market data",
            legacy_key,
            warning_key,
        )
    _require_keys(
        raw,
        {
            "bankroll",
            "minimum_return_rate",
            "maximum_risk_rate",
            "maximum_unhedged_notional",
            "safety_buffer_rate",
            "conversion_cost",
            "maximum_book_age_ms",
            "exchange_clock_skew_warning_ms",
            "maximum_leg_skew_ms",
        },
        "strategy",
    )
    return StrategyConfig(
        bankroll=_decimal(raw, "bankroll", "strategy"),
        minimum_return_rate=_decimal(raw, "minimum_return_rate", "strategy"),
        maximum_risk_rate=_decimal(raw, "maximum_risk_rate", "strategy"),
        maximum_unhedged_notional=_decimal(raw, "maximum_unhedged_notional", "strategy"),
        safety_buffer_rate=_decimal(raw, "safety_buffer_rate", "strategy"),
        conversion_cost=_decimal(raw, "conversion_cost", "strategy"),
        maximum_book_age_ms=_integer(raw, "maximum_book_age_ms", "strategy"),
        exchange_clock_skew_warning_ms=_integer(
            raw, "exchange_clock_skew_warning_ms", "strategy"
        ),
        maximum_leg_skew_ms=_integer(raw, "maximum_leg_skew_ms", "strategy"),
    )


def _signal_config(raw: dict[str, Any]) -> SignalConfig:
    _require_keys(
        raw,
        {
            "profit_change_rate",
            "risk_rate_change",
            "quantity_change_rate",
            "minimum_update_interval_seconds",
        },
        "signal",
    )
    return SignalConfig(
        profit_change_rate=_decimal(raw, "profit_change_rate", "signal"),
        risk_rate_change=_decimal(raw, "risk_rate_change", "signal"),
        quantity_change_rate=_decimal(raw, "quantity_change_rate", "signal"),
        minimum_update_interval_seconds=_integer(raw, "minimum_update_interval_seconds", "signal"),
    )


def _relations_config(raw: dict[str, Any]) -> RelationsConfig:
    _require_keys(raw, {"llm_enabled"}, "relations")
    return RelationsConfig(llm_enabled=_boolean(raw, "llm_enabled", "relations"))


def _notification_config(raw: dict[str, Any]) -> NotificationConfig:
    _require_keys(raw, {"terminal_enabled", "desktop_enabled"}, "notification")
    return NotificationConfig(
        terminal_enabled=_boolean(raw, "terminal_enabled", "notification"),
        desktop_enabled=_boolean(raw, "desktop_enabled", "notification"),
    )


def _load_mapping(path: Path) -> dict[str, Any]:
    try:
        loaded = yaml.safe_load(path.read_text())
    except OSError as error:
        raise ValueError(f"could not read configuration file: {path}") from error
    except yaml.YAMLError as error:
        raise ValueError(f"invalid YAML in configuration file: {path}") from error
    if not isinstance(loaded, dict):
        raise ValueError("configuration must be a mapping")
    return loaded


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw[name]
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a mapping")
    return value


def _require_keys(raw: dict[str, Any], expected: set[str], section: str) -> None:
    if any(not isinstance(key, str) for key in raw):
        raise ValueError(f"keys in {section} must be strings")
    actual = set(raw)
    unknown = actual - expected
    missing = expected - actual
    if unknown:
        raise ValueError(f"unknown keys in {section}: {', '.join(sorted(unknown))}")
    if missing:
        raise ValueError(f"missing keys in {section}: {', '.join(sorted(missing))}")


def _string(raw: dict[str, Any], key: str, section: str) -> str:
    value = raw[key]
    if not isinstance(value, str):
        raise ValueError(f"{section}.{key} must be a string")
    return value


def _integer(raw: dict[str, Any], key: str, section: str) -> int:
    value = raw[key]
    if type(value) is not int:
        raise ValueError(f"{section}.{key} must be an integer")
    return value


def _boolean(raw: dict[str, Any], key: str, section: str) -> bool:
    value = raw[key]
    if type(value) is not bool:
        raise ValueError(f"{section}.{key} must be a boolean")
    return value


def _decimal(raw: dict[str, Any], key: str, section: str) -> Decimal:
    value = raw[key]
    if type(value) is not str:
        raise ValueError(f"{section}.{key} must be a decimal string")
    try:
        decimal = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{section}.{key} must be a valid decimal string") from error
    if not decimal.is_finite():
        raise ValueError(f"{section}.{key} must be a finite decimal string")
    return decimal
