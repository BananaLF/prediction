from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from predmarket.config import AppConfig


def test_default_config_has_greenfield_limits(tmp_path: Path) -> None:
    config = AppConfig.load(Path("config/default.yaml"))

    assert config.database.busy_timeout_ms == 5000
    assert config.runtime.market_change_queue_capacity == 10_000
    assert config.strategy.bankroll == Decimal("1000")
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
