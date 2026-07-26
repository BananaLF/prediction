from dataclasses import FrozenInstanceError

import pytest

from predmarket.latency import Timing, TimingAssessment, validate_timings


def timing(
    exchange: int = 900,
    received: int = 950,
    received_mono: float = 1.0,
    evaluated_mono: float = 1.01,
) -> Timing:
    return Timing(exchange, received, received_mono, evaluated_mono)


def assess(items=(timing(),), **overrides) -> TimingAssessment:
    arguments = dict(now_ms=1000, max_age_ms=100, max_skew_ms=50, max_processing_ms=10)
    arguments.update(overrides)
    return validate_timings(items, **arguments)


def test_valid_timings_and_equal_boundaries_pass() -> None:
    result = assess(
        (
            timing(exchange=900, evaluated_mono=1.01),
            timing(exchange=950, received_mono=2.0, evaluated_mono=2.01),
        )
    )
    assert result == TimingAssessment(True, ())


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ((timing(exchange=899),), ("stale",)),
        ((timing(exchange=1001, received=1001),), ("future_exchange_ts",)),
        (
            (timing(exchange=900), timing(exchange=951, received=951)),
            ("leg_skew",),
        ),
        ((timing(evaluated_mono=1.010001),), ("processing_latency",)),
    ],
)
def test_each_timing_failure_reason(items: tuple[Timing, ...], expected: tuple[str, ...]) -> None:
    assert assess(items).reasons == expected


def test_all_applicable_reasons_have_stable_order() -> None:
    items = (
        timing(exchange=800, evaluated_mono=1.02),
        timing(exchange=1001, received_mono=2.0, evaluated_mono=2.0),
    )
    assert assess(items).reasons == (
        "stale",
        "future_exchange_ts",
        "exchange_after_receive",
        "leg_skew",
        "processing_latency",
    )


def test_exchange_after_receive_fails_closed_even_when_exchange_is_not_future() -> None:
    assert assess((timing(exchange=999, received=998),)).reasons == (
        "exchange_after_receive",
    )


def test_exchange_equal_to_receive_is_valid() -> None:
    assert assess((timing(exchange=999, received=999),)).valid


def test_empty_timings_fail_closed() -> None:
    assert assess(()).reasons == ("missing_timing",)


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(exchange_ts_ms=True),
        dict(received_ts_ms=1.0),
        dict(exchange_ts_ms=-1),
        dict(received_ts_ms=-1),
        dict(received_monotonic=True),
        dict(received_monotonic="1"),
        dict(received_monotonic=float("nan")),
        dict(evaluated_monotonic=float("inf")),
        dict(received_monotonic=-1),
        dict(received_monotonic=2, evaluated_monotonic=1),
    ],
)
def test_timing_rejects_invalid_values(kwargs: dict[str, object]) -> None:
    values: dict[str, object] = dict(
        exchange_ts_ms=1,
        received_ts_ms=1,
        received_monotonic=1.0,
        evaluated_monotonic=1.0,
    )
    values.update(kwargs)
    with pytest.raises((TypeError, ValueError)):
        Timing(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("items", "kwargs"),
    [
        ([timing()], dict(now_ms=True)),
        ([timing()], dict(max_age_ms=0)),
        ([timing()], dict(max_skew_ms=1.0)),
        ([timing()], dict(max_processing_ms=-1)),
        ("bad", {}),
        ([object()], {}),
    ],
)
def test_validator_rejects_malformed_arguments(items: object, kwargs: dict[str, object]) -> None:
    arguments: dict[str, object] = dict(
        now_ms=1000, max_age_ms=100, max_skew_ms=50, max_processing_ms=10
    )
    arguments.update(kwargs)
    with pytest.raises((TypeError, ValueError)):
        validate_timings(items, **arguments)  # type: ignore[arg-type]


def test_value_objects_are_deeply_immutable_and_reasons_unique() -> None:
    value = timing()
    assessment = TimingAssessment(False, ("stale",))
    with pytest.raises(FrozenInstanceError):
        value.exchange_ts_ms = 2  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        assessment.valid = True  # type: ignore[misc]
    with pytest.raises(ValueError):
        TimingAssessment(False, ("stale", "stale"))
    with pytest.raises(TypeError):
        TimingAssessment(False, ["stale"])  # type: ignore[arg-type]


def test_validator_does_not_mutate_input() -> None:
    items = [timing(exchange=900), timing(exchange=950)]
    before = list(items)
    assess(items)
    assert items == before
