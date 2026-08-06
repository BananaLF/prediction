# Prediction Market Business Clock Design

## Goal

Use Polymarket event timestamps as the authoritative business-time domain for
order-book evaluation and signal persistence. A host clock that is ahead of or
behind Polymarket must not by itself invalidate otherwise valid market data.

Runtime durations remain in a separate time domain. Timeouts, reconnect delays,
stream-silence detection, performance measurements, and log timestamps continue
to use the host monotonic or wall clock as appropriate.

## Time domains

- `exchange_timestamp` is the authoritative timestamp of a market event.
- `market_time` is a non-decreasing watermark equal to the greatest accepted
  `exchange_timestamp` in the active subscription generation.
- `received_timestamp` records the host wall-clock receipt time for diagnostics
  and audit only. It does not decide business validity.
- Monotonic host time measures elapsed runtime durations and detects a silent or
  stalled stream.

## Data flow

1. Recovery order-book snapshots initialize the generation's `market_time`.
2. Each accepted WebSocket order-book event advances the watermark with
   `max(current_market_time, exchange_timestamp)`.
3. Strategy evaluation uses the watermark as `StrategyContext.evaluated_at`.
4. Signal creation, update, and closure persist the same evaluation market time
   as their `observed_at` value instead of reading the host wall clock again.
5. A new subscription generation initializes its watermark from its own recovery
   snapshots; a previous generation cannot advance the new generation's clock.

No strategy evaluation or signal mutation is allowed before the active
generation has a market-time watermark.

## Validation rules

- Do not reject an event merely because its exchange timestamp is ahead of the
  host receipt timestamp.
- Retain the exchange/host skew as structured diagnostic logging.
- Continue rejecting per-token exchange timestamp regression through the order
  book cache.
- Compare the books required by one strategy against the same `market_time` to
  detect books older than `maximum_book_age_ms`.
- Continue enforcing `maximum_leg_skew_ms` between the exchange timestamps of
  books used by one opportunity.
- Detect a stream that stops delivering data using host monotonic elapsed time;
  this check must not depend on `market_time`, which cannot advance while the
  stream is silent.
- Preserve generation and post-evaluation freshness checks so an evaluation
  cannot persist results after newer market data supersedes its input.

## Configuration and compatibility

`maximum_exchange_clock_skew_ms` no longer controls signal validity because it
compares different clock authorities. Replace it in the default configuration
with `exchange_clock_skew_warning_ms`, which controls diagnostics only. Accept
the old key as a deprecated compatibility alias when the new key is absent, and
emit a warning that its value no longer rejects market data. Reject a
configuration that supplies both keys so precedence is never ambiguous.

Persisted `exchange_timestamp` and `received_timestamp` fields retain their
existing meanings. Existing rows require no schema migration. New signal
revision `observed_at` values use market time; historical rows remain valid as
records produced under the previous clock policy.

## Failure handling

- Missing or malformed exchange timestamps remain fail-closed.
- A regression invalidates or recovers the affected cache according to the
  existing cache policy.
- A generation without a recovery watermark does not evaluate or persist.
- Disconnect-driven closures use the last accepted market watermark for that
  generation. If none exists, no timestamped signal mutation is performed.

## Logging

Evaluation summaries report `market_time`, the largest observed
`exchange_timestamp - received_timestamp` diagnostic, and stream-silence age.
Component startup, recovery, and shutdown logs continue to use normal host log
timestamps.

## Verification

- Unit tests prove a market timestamp ahead of the host receipt timestamp is
  accepted.
- Unit tests prove old books, timestamp regression, leg skew, and missing market
  time remain fail-closed.
- Signal manager tests prove persisted `observed_at` equals the evaluation's
  supplied market time for open, update, and close revisions.
- Watch tests prove generation changes reset the watermark and stale evaluations
  cannot persist after newer data arrives.
- Integration tests start from recovery snapshots, process WebSocket updates,
  and verify signal timestamps in SQLite use market time.
- Full regression tests and a live run verify startup, synchronization, stream
  monitoring, signal logging, and database persistence.
