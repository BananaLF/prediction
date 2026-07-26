from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, DecimalException
from pathlib import Path

import yaml


_FINANCIAL_FIELDS = (
    "bankroll",
    "minimum_return",
    "safety_buffer_rate",
    "max_leg_failure_loss",
    "max_unhedged_notional",
    "default_simulation_quantity",
    "conversion_cost",
)
_INTEGER_FIELDS = (
    "maximum_book_age_ms",
    "maximum_leg_skew_ms",
    "maximum_processing_latency_ms",
    "reconcile_interval_seconds",
    "queue_capacity",
)
_REQUIRED_FIELDS = (*_FINANCIAL_FIELDS, *_INTEGER_FIELDS, "database_path")


def _financial_value(values: Mapping[object, object], field: str) -> Decimal:
    raw_value = values[field]
    if not isinstance(raw_value, str):
        raise ValueError(f"{field} must be a string")
    try:
        value = Decimal(raw_value)
    except DecimalException as error:
        raise ValueError(f"{field} must be a valid decimal string") from error
    if not value.is_finite():
        raise ValueError(f"{field} must be finite")
    return value


def _positive_integer(values: Mapping[object, object], field: str) -> int:
    value = values[field]
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value


@dataclass(frozen=True)
class Settings:
    bankroll: Decimal
    minimum_return: Decimal
    safety_buffer_rate: Decimal
    max_leg_failure_loss: Decimal
    max_unhedged_notional: Decimal
    default_simulation_quantity: Decimal
    conversion_cost: Decimal
    maximum_book_age_ms: int
    maximum_leg_skew_ms: int
    maximum_processing_latency_ms: int
    reconcile_interval_seconds: int
    queue_capacity: int
    database_path: Path

    @classmethod
    def load(cls, path: str | Path) -> "Settings":
        with Path(path).open(encoding="utf-8") as config_file:
            values = yaml.safe_load(config_file)

        if not isinstance(values, Mapping):
            raise ValueError("configuration root must be a mapping")

        missing_fields = [field for field in _REQUIRED_FIELDS if field not in values]
        if missing_fields:
            raise ValueError(
                f"missing configuration keys: {', '.join(missing_fields)}"
            )

        financial_values = {
            field: _financial_value(values, field) for field in _FINANCIAL_FIELDS
        }
        positive_financial_fields = (
            "bankroll",
            "minimum_return",
            "default_simulation_quantity",
        )
        for field in positive_financial_fields:
            if financial_values[field] <= 0:
                raise ValueError(f"{field} must be positive")
        for field in set(_FINANCIAL_FIELDS) - set(positive_financial_fields):
            if financial_values[field] < 0:
                raise ValueError(f"{field} must be nonnegative")

        database_path = values["database_path"]
        if not isinstance(database_path, str) or not database_path.strip():
            raise ValueError("database_path must be a non-empty string")

        return cls(
            bankroll=financial_values["bankroll"],
            minimum_return=financial_values["minimum_return"],
            safety_buffer_rate=financial_values["safety_buffer_rate"],
            max_leg_failure_loss=financial_values["max_leg_failure_loss"],
            max_unhedged_notional=financial_values["max_unhedged_notional"],
            default_simulation_quantity=financial_values[
                "default_simulation_quantity"
            ],
            conversion_cost=financial_values["conversion_cost"],
            maximum_book_age_ms=_positive_integer(values, "maximum_book_age_ms"),
            maximum_leg_skew_ms=_positive_integer(values, "maximum_leg_skew_ms"),
            maximum_processing_latency_ms=_positive_integer(
                values, "maximum_processing_latency_ms"
            ),
            reconcile_interval_seconds=_positive_integer(
                values, "reconcile_interval_seconds"
            ),
            queue_capacity=_positive_integer(values, "queue_capacity"),
            database_path=Path(database_path),
        )
