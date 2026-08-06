from __future__ import annotations

import pytest

from predmarket.watch.clock import MarketClock


def test_recovery_initializes_market_time_from_latest_snapshot() -> None:
    clock = MarketClock()

    watermark = clock.initialize(
        generation=1,
        exchange_timestamps=(100, 120),
    )

    assert watermark == 120
    assert clock.generation == 1
    assert clock.watermark_ms == 120
    assert clock.read(generation=1) == 120


def test_accepted_older_event_does_not_move_market_time_backwards() -> None:
    clock = MarketClock()
    clock.initialize(generation=1, exchange_timestamps=(120,))

    assert clock.advance(generation=1, exchange_timestamp=110) == 120
    assert clock.read(generation=1) == 120


def test_new_generation_cannot_reuse_previous_watermark() -> None:
    clock = MarketClock()
    clock.initialize(generation=1, exchange_timestamps=(500,))

    assert clock.read(generation=2) is None
    assert clock.initialize(generation=2, exchange_timestamps=(200,)) == 200
    assert clock.read(generation=1) is None
    assert clock.read(generation=2) == 200


@pytest.mark.parametrize("generation", (0, -1, True))
def test_initialize_rejects_invalid_generation(generation: int) -> None:
    with pytest.raises(ValueError, match="generation"):
        MarketClock().initialize(
            generation=generation,
            exchange_timestamps=(100,),
        )


def test_initialize_requires_strictly_newer_generation() -> None:
    clock = MarketClock()
    clock.initialize(generation=2, exchange_timestamps=(100,))

    with pytest.raises(ValueError, match="newer"):
        clock.initialize(generation=2, exchange_timestamps=(200,))


@pytest.mark.parametrize("exchange_timestamps", ((), (-1,), (True,)))
def test_initialize_rejects_invalid_exchange_timestamps(
    exchange_timestamps: tuple[int, ...],
) -> None:
    with pytest.raises(ValueError, match="exchange_timestamps"):
        MarketClock().initialize(
            generation=1,
            exchange_timestamps=exchange_timestamps,
        )


def test_failed_initialize_does_not_mutate_active_generation() -> None:
    clock = MarketClock()
    clock.initialize(generation=1, exchange_timestamps=(100,))

    with pytest.raises(ValueError, match="exchange_timestamps"):
        clock.initialize(generation=2, exchange_timestamps=(-1,))

    assert clock.generation == 1
    assert clock.watermark_ms == 100


def test_advance_rejects_non_active_generation() -> None:
    clock = MarketClock()
    clock.initialize(generation=1, exchange_timestamps=(100,))

    with pytest.raises(ValueError, match="active"):
        clock.advance(generation=2, exchange_timestamp=200)


@pytest.mark.parametrize("exchange_timestamp", (-1, True))
def test_advance_rejects_invalid_exchange_timestamp(
    exchange_timestamp: int,
) -> None:
    clock = MarketClock()
    clock.initialize(generation=1, exchange_timestamps=(100,))

    with pytest.raises(ValueError, match="exchange_timestamp"):
        clock.advance(generation=1, exchange_timestamp=exchange_timestamp)

    assert clock.watermark_ms == 100
