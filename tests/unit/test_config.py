from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from predmarket.config import AppConfig


def test_default_config_has_greenfield_limits(tmp_path: Path) -> None:
    config = AppConfig.load(Path("config/default.yaml"))

    assert config.database.busy_timeout_ms == 5000
    assert config.polymarket.sync_interval_seconds == 1800
    assert config.runtime.market_change_queue_capacity == 10_000
    assert config.runtime.watch_market_limit == 50
    assert config.runtime.watch_minimum_end_horizon_seconds == 1_800
    assert config.runtime.market_stream_queue_capacity == 65_536
    assert config.strategy.bankroll == Decimal("1000")
    assert config.strategy.exchange_clock_skew_warning_ms == 100
    assert config.relations.llm_enabled is False


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("strategy", "bankroll", 1000),
        ("signal", "profit_change_rate", 0.05),
    ],
)
def test_load_rejects_non_string_decimal_values(
    tmp_path: Path, section: str, key: str, value: object
) -> None:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw[section][key] = value
    path = tmp_path / "invalid-decimal.yaml"
    path.write_text(yaml.safe_dump(raw))

    with pytest.raises(ValueError):
        AppConfig.load(path)

def test_load_rejects_unknown_keys(tmp_path: Path) -> None:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw["runtime"]["unexpected"] = True
    path = tmp_path / "unknown-key.yaml"
    path.write_text(yaml.safe_dump(raw))

    with pytest.raises(ValueError):
        AppConfig.load(path)


def test_load_accepts_exchange_clock_skew_warning_key_without_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    path = tmp_path / "new-skew-key.yaml"
    path.write_text(yaml.safe_dump(raw))

    with caplog.at_level(logging.WARNING, logger="predmarket.config"):
        config = AppConfig.load(path)

    assert config.strategy.exchange_clock_skew_warning_ms == 100
    assert caplog.records == []


def test_load_accepts_legacy_exchange_clock_skew_key_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw["strategy"]["maximum_exchange_clock_skew_ms"] = raw["strategy"].pop(
        "exchange_clock_skew_warning_ms"
    )
    path = tmp_path / "legacy-skew-key.yaml"
    path.write_text(yaml.safe_dump(raw))

    with caplog.at_level(logging.WARNING, logger="predmarket.config"):
        config = AppConfig.load(path)

    assert config.strategy.exchange_clock_skew_warning_ms == 100
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "maximum_exchange_clock_skew_ms" in message
    assert "exchange_clock_skew_warning_ms" in message
    assert "no longer rejects market data" in message


def test_load_rejects_both_exchange_clock_skew_keys(tmp_path: Path) -> None:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw["strategy"]["maximum_exchange_clock_skew_ms"] = 100
    path = tmp_path / "both-skew-keys.yaml"
    path.write_text(yaml.safe_dump(raw))

    with pytest.raises(ValueError, match="maximum_exchange_clock_skew_ms"):
        AppConfig.load(path)


def test_load_rejects_missing_exchange_clock_skew_key(tmp_path: Path) -> None:
    raw = yaml.safe_load(Path("config/default.yaml").read_text())
    raw["strategy"].pop("exchange_clock_skew_warning_ms")
    path = tmp_path / "missing-skew-key.yaml"
    path.write_text(yaml.safe_dump(raw))

    with pytest.raises(ValueError, match="exchange_clock_skew_warning_ms"):
        AppConfig.load(path)
