"""Generation-scoped prediction-market business clock."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass


@dataclass(slots=True)
class MarketClock:
    """Track the greatest accepted exchange timestamp for one generation."""

    _generation: int = 0
    _watermark_ms: int | None = None

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def watermark_ms(self) -> int | None:
        return self._watermark_ms

    def initialize(
        self,
        *,
        generation: int,
        exchange_timestamps: Collection[int],
    ) -> int:
        if type(generation) is not int or generation <= self._generation:
            raise ValueError("generation must be a newer positive integer")
        values = tuple(exchange_timestamps)
        if not values or any(type(value) is not int or value < 0 for value in values):
            raise ValueError(
                "exchange_timestamps must contain non-negative integers"
            )

        self._generation = generation
        self._watermark_ms = max(values)
        return self._watermark_ms

    def advance(self, *, generation: int, exchange_timestamp: int) -> int:
        if generation != self._generation or self._watermark_ms is None:
            raise ValueError("generation is not active")
        if type(exchange_timestamp) is not int or exchange_timestamp < 0:
            raise ValueError("exchange_timestamp must be a non-negative integer")

        self._watermark_ms = max(self._watermark_ms, exchange_timestamp)
        return self._watermark_ms

    def read(self, *, generation: int) -> int | None:
        if generation != self._generation:
            return None
        return self._watermark_ms
