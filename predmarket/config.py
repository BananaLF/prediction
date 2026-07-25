from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import yaml


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

        return cls(
            bankroll=Decimal(values["bankroll"]),
            minimum_return=Decimal(values["minimum_return"]),
            safety_buffer_rate=Decimal(values["safety_buffer_rate"]),
            max_leg_failure_loss=Decimal(values["max_leg_failure_loss"]),
            max_unhedged_notional=Decimal(values["max_unhedged_notional"]),
            default_simulation_quantity=Decimal(values["default_simulation_quantity"]),
            conversion_cost=Decimal(values["conversion_cost"]),
            maximum_book_age_ms=int(values["maximum_book_age_ms"]),
            maximum_leg_skew_ms=int(values["maximum_leg_skew_ms"]),
            maximum_processing_latency_ms=int(values["maximum_processing_latency_ms"]),
            reconcile_interval_seconds=int(values["reconcile_interval_seconds"]),
            queue_capacity=int(values["queue_capacity"]),
            database_path=Path(values["database_path"]),
        )
