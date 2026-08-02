"""Authoritative fee schedules and exact fee calculation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from predmarket.domain.decimal import parse_decimal


class FeeModel(str, Enum):
    ZERO = "ZERO"
    FLAT = "FLAT"
    CURVE = "CURVE"


@dataclass(frozen=True)
class FeeSchedule:
    model: FeeModel
    enabled: bool
    source: str
    parameters: Mapping[str, Decimal]
    updated_at: int | None = None
    taker_only: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.model, FeeModel):
            raise ValueError("model must be a FeeModel")
        if type(self.enabled) is not bool:
            raise ValueError("enabled must be a boolean")
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("source must be a non-empty string")
        if self.updated_at is not None and (
            type(self.updated_at) is not int or self.updated_at < 0
        ):
            raise ValueError("updated_at must be a non-negative integer")
        if type(self.taker_only) is not bool:
            raise ValueError("taker_only must be a boolean")

        parameters = dict(self.parameters)
        if any(not isinstance(key, str) for key in parameters):
            raise ValueError("fee parameter names must be strings")
        for value in parameters.values():
            _finite_decimal(value, "fee parameter")
        if self.model is FeeModel.ZERO and parameters:
            raise ValueError("ZERO fee model does not accept parameters")
        if self.model is FeeModel.FLAT:
            if set(parameters) != {"rate"}:
                raise ValueError("FLAT fee model requires exactly the rate parameter")
            if not Decimal("0") <= parameters["rate"] <= Decimal("1"):
                raise ValueError("fee rate must be between zero and one")
        if self.model is FeeModel.CURVE:
            if set(parameters) != {"rate", "exponent", "rebate_rate"}:
                raise ValueError(
                    "CURVE fee model requires rate, exponent, and rebate_rate"
                )
            if not Decimal("0") <= parameters["rate"] <= Decimal("1"):
                raise ValueError("fee rate must be between zero and one")
            if parameters["exponent"] < Decimal("0"):
                raise ValueError("fee exponent must be non-negative")
            if not Decimal("0") <= parameters["rebate_rate"] <= Decimal("1"):
                raise ValueError("fee rebate rate must be between zero and one")
        object.__setattr__(self, "parameters", MappingProxyType(parameters))

    @classmethod
    def from_json(cls, data: object) -> "FeeSchedule":
        if not isinstance(data, dict):
            raise ValueError("fee schedule must be a JSON object")
        required = {"model", "enabled", "source", "parameters"}
        allowed = required | {"updated_at", "taker_only"}
        if set(data) - allowed:
            raise ValueError("fee schedule contains unknown fields")
        if required - set(data):
            raise ValueError("fee schedule is missing required fields")

        raw_model = data["model"]
        if not isinstance(raw_model, str):
            raise ValueError("fee model must be a string")
        try:
            model = FeeModel(raw_model)
        except ValueError as error:
            raise ValueError(f"unknown fee model: {raw_model}") from error

        raw_parameters = data["parameters"]
        if not isinstance(raw_parameters, dict):
            raise ValueError("fee parameters must be a JSON object")
        parameters: dict[str, Decimal] = {}
        for key, value in raw_parameters.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("fee parameters must be canonical decimal strings")
            parameters[key] = parse_decimal(value)

        return cls(
            model=model,
            enabled=data["enabled"],  # type: ignore[arg-type]
            source=data["source"],  # type: ignore[arg-type]
            parameters=parameters,
            updated_at=data.get("updated_at"),  # type: ignore[arg-type]
            taker_only=data.get("taker_only", False),  # type: ignore[arg-type]
        )

    def is_stale(self, *, evaluated_at: int, max_age_seconds: int) -> bool:
        if type(evaluated_at) is not int or evaluated_at < 0:
            raise ValueError("evaluated_at must be a non-negative integer")
        if type(max_age_seconds) is not int or max_age_seconds < 0:
            raise ValueError("max_age_seconds must be a non-negative integer")
        if self.updated_at is None:
            return True
        if evaluated_at < self.updated_at:
            raise ValueError("evaluated_at cannot precede updated_at")
        return evaluated_at - self.updated_at > max_age_seconds * 1_000


class FeeCalculator:
    @staticmethod
    def calculate(
        schedule: FeeSchedule,
        price: Decimal,
        quantity: Decimal,
        *,
        evaluated_at_ms: int,
        max_age_seconds: int,
        is_taker: bool = True,
    ) -> Decimal:
        if not isinstance(schedule, FeeSchedule):
            raise ValueError("schedule must be a FeeSchedule")
        if type(is_taker) is not bool:
            raise ValueError("is_taker must be a boolean")
        if schedule.is_stale(
            evaluated_at=evaluated_at_ms,
            max_age_seconds=max_age_seconds,
        ):
            raise ValueError("fee schedule is stale")
        _finite_decimal(price, "price")
        _finite_decimal(quantity, "quantity")
        if not Decimal("0") <= price <= Decimal("1"):
            raise ValueError("price must be between zero and one")
        if quantity <= Decimal("0"):
            raise ValueError("quantity must be greater than zero")
        if not schedule.enabled or schedule.model is FeeModel.ZERO:
            return Decimal("0")
        if schedule.taker_only and not is_taker:
            return Decimal("0")
        if schedule.model is FeeModel.FLAT:
            return price * quantity * schedule.parameters["rate"]
        if schedule.model is FeeModel.CURVE:
            rate = schedule.parameters["rate"]
            exponent = schedule.parameters["exponent"]
            base = price * (Decimal("1") - price)
            amount = quantity * rate * (base**exponent)
            if amount <= Decimal("0"):
                return Decimal("0")
            rounded = amount.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
            return max(rounded, Decimal("0.00001"))
        raise ValueError(f"unknown fee model: {schedule.model}")


def _finite_decimal(value: Any, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{field_name} must be a finite Decimal")
