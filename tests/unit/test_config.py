from decimal import Decimal
from pathlib import Path

import pytest

from predmarket.config import Settings


def test_load_default_settings() -> None:
    settings = Settings.load(Path("config/default.yaml"))

    assert settings.bankroll == Decimal("1000")
    assert settings.minimum_return == Decimal("0.0075")
    assert settings.safety_buffer_rate == Decimal("0.0025")
    assert settings.max_leg_failure_loss == Decimal("5")
    assert settings.max_unhedged_notional == Decimal("20")
    assert settings.default_simulation_quantity == Decimal("10")
    assert settings.conversion_cost == Decimal("0")
    assert settings.maximum_book_age_ms == 1000
    assert settings.maximum_leg_skew_ms == 250
    assert settings.maximum_processing_latency_ms == 100
    assert settings.reconcile_interval_seconds == 30
    assert settings.queue_capacity == 10000
    assert settings.database_path == Path("data/predmarket.sqlite3")


def test_load_parses_financial_values_from_yaml_strings(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        "\n".join(
            [
                'bankroll: "12.30"',
                'minimum_return: "0.01"',
                'safety_buffer_rate: "0.02"',
                'max_leg_failure_loss: "3"',
                'max_unhedged_notional: "4"',
                'default_simulation_quantity: "5"',
                'conversion_cost: "0.10"',
                "maximum_book_age_ms: 6",
                "maximum_leg_skew_ms: 7",
                "maximum_processing_latency_ms: 8",
                "reconcile_interval_seconds: 9",
                "queue_capacity: 10",
                "database_path: somewhere.sqlite3",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings.load(str(config_path))

    assert settings.bankroll == Decimal("12.30")
    assert settings.conversion_cost == Decimal("0.10")
    assert isinstance(settings.bankroll, Decimal)
    assert settings.maximum_book_age_ms == 6
    assert isinstance(settings.maximum_book_age_ms, int)
    assert settings.database_path == Path("somewhere.sqlite3")


def test_settings_are_immutable() -> None:
    settings = Settings.load("config/default.yaml")

    try:
        settings.bankroll = Decimal("2000")
    except (AttributeError, TypeError):
        pass
    else:
        raise AssertionError("Settings must be immutable")


def _default_config_text() -> str:
    return Path("config/default.yaml").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "replacement",
    ["bankroll: 1000", "bankroll: true"],
)
def test_load_rejects_non_string_financial_values(
    tmp_path: Path, replacement: str
) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        _default_config_text().replace('bankroll: "1000"', replacement),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="bankroll"):
        Settings.load(config_path)


@pytest.mark.parametrize(
    "field",
    [
        "maximum_book_age_ms",
        "maximum_leg_skew_ms",
        "maximum_processing_latency_ms",
        "reconcile_interval_seconds",
        "queue_capacity",
    ],
)
@pytest.mark.parametrize("invalid_value", ["true", "1.5", '"1"'])
def test_load_rejects_invalid_operational_integer_types(
    tmp_path: Path, field: str, invalid_value: str
) -> None:
    config_path = tmp_path / "settings.yaml"
    original_line = next(
        line for line in _default_config_text().splitlines() if line.startswith(f"{field}:")
    )
    config_path.write_text(
        _default_config_text().replace(original_line, f"{field}: {invalid_value}"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=field):
        Settings.load(config_path)


@pytest.mark.parametrize("content", ["", "[]", "- item"])
def test_load_rejects_missing_or_non_mapping_yaml(tmp_path: Path, content: str) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="mapping"):
        Settings.load(config_path)


def test_load_rejects_missing_keys(tmp_path: Path) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        _default_config_text().replace('bankroll: "1000"\n', ""),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="bankroll"):
        Settings.load(config_path)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_load_rejects_nonfinite_financial_values(tmp_path: Path, value: str) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        _default_config_text().replace('bankroll: "1000"', f'bankroll: "{value}"'),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="bankroll"):
        Settings.load(config_path)


@pytest.mark.parametrize(
    ("field", "original", "invalid"),
    [
        ("bankroll", "1000", "0"),
        ("minimum_return", "0.0075", "0"),
        ("safety_buffer_rate", "0.0025", "-0.1"),
        ("max_leg_failure_loss", "5", "-1"),
        ("max_unhedged_notional", "20", "-1"),
        ("default_simulation_quantity", "10", "0"),
        ("conversion_cost", "0", "-0.1"),
    ],
)
def test_load_rejects_invalid_financial_ranges(
    tmp_path: Path, field: str, original: str, invalid: str
) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        _default_config_text().replace(
            f'{field}: "{original}"', f'{field}: "{invalid}"'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=field):
        Settings.load(config_path)


@pytest.mark.parametrize(
    "field",
    [
        "maximum_book_age_ms",
        "maximum_leg_skew_ms",
        "maximum_processing_latency_ms",
        "reconcile_interval_seconds",
        "queue_capacity",
    ],
)
@pytest.mark.parametrize("invalid_value", ["0", "-1"])
def test_load_rejects_nonpositive_operational_limits(
    tmp_path: Path, field: str, invalid_value: str
) -> None:
    config_path = tmp_path / "settings.yaml"
    original_line = next(
        line for line in _default_config_text().splitlines() if line.startswith(f"{field}:")
    )
    config_path.write_text(
        _default_config_text().replace(original_line, f"{field}: {invalid_value}"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=field):
        Settings.load(config_path)


@pytest.mark.parametrize("value", ["", "   ", "123", "true"])
def test_load_rejects_invalid_database_path(tmp_path: Path, value: str) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        _default_config_text().replace(
            "database_path: data/predmarket.sqlite3", f"database_path: {value}"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="database_path"):
        Settings.load(config_path)
