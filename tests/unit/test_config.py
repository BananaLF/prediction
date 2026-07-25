from decimal import Decimal
from pathlib import Path

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
