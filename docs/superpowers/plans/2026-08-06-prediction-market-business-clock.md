# Prediction Market Business Clock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make accepted Polymarket timestamps the sole business-time authority for order-book evaluation and signal revision persistence, while keeping runtime duration checks in the host monotonic time domain.

**Architecture:** Add a generation-scoped `MarketClock` watermark owned by `WatchTask`. Recovery snapshots initialize it and accepted stream mutations advance it. `WatchTask` supplies that watermark to strategies and passes the identical value explicitly to signal persistence. Host receipt timestamps remain diagnostic data, and host monotonic time exclusively measures stream silence and elapsed work. Cache revisions and generation checks prevent an evaluation from persisting after its input has been superseded.

**Tech Stack:** Python 3.14, asyncio, dataclasses, SQLite, pytest, PyYAML, existing Polymarket gateway and watch pipeline.

**Global Constraints:** Preserve existing database schema and historical rows; do not change SDK event timestamp meanings; do not use host wall time for signal business timestamps; do not weaken timestamp-regression, book-age, leg-skew, generation, or post-evaluation freshness checks; preserve unrelated working-tree changes; use `apply_patch` for source edits.

---

## File Structure

### New file

- `predmarket/watch/clock.py` — generation-scoped, non-decreasing market-time watermark.
- `tests/unit/watch/test_clock.py` — focused watermark initialization, advancement, and generation-isolation tests.

### Modified production files

- `predmarket/watch/cache.py` — expose a mutation revision used by evaluation freshness checks.
- `predmarket/watch/task.py` — own the market clock, separate monotonic stream timing, evaluate at market time, and persist with explicit market timestamps.
- `predmarket/domain/signal.py` — separate strategy business time from host-domain fee-cache inspection time.
- `predmarket/strategy/common.py` — validate age and leg skew only inside the market-time domain; retain host skew as diagnostics.
- `predmarket/strategy/risk.py` — check fee freshness with the host-domain fee timestamp.
- `predmarket/signals/manager.py` — require explicit `observed_at` for every signal mutation.
- `predmarket/app.py` — build context with distinct time domains and forward explicit timestamps through the manager router.
- `predmarket/config.py` — replace the validity threshold with a diagnostic warning threshold and implement strict legacy-key compatibility.
- `config/default.yaml` — use `exchange_clock_skew_warning_ms`.
- `docs/runtime-investigation-2026-08-04.md` — record the verified root cause, time-domain policy, and runtime evidence.

### Modified tests

- `tests/unit/watch/test_cache.py` — revision behavior for snapshots, mutations, no-ops, and rejected regressions.
- `tests/unit/watch/test_task.py` — recovery watermark, stream advancement, monotonic silence, diagnostics, closure timestamp, and stale-evaluation fencing.
- `tests/unit/signals/test_manager.py` — explicit timestamp propagation for open, update, and close revisions.
- `tests/unit/strategy/test_binary.py` — ahead-of-host acceptance, market-time staleness, and leg-skew behavior.
- `tests/unit/strategy/test_risk.py` — fee freshness remains independent of market/host clock skew.
- `tests/unit/strategy/test_decimal_isolation.py` — construct contexts with the explicit fee-cache inspection time.
- `tests/unit/strategy/test_implication.py` — construct implication contexts in the separated time domains.
- `tests/unit/strategy/conftest.py` — construct contexts with both time domains and the renamed config field.
- `tests/unit/domain/test_models.py` — update `StrategyConfig` and `StrategyContext` construction.
- `tests/unit/test_config.py` — new key, legacy alias, warning, and ambiguous-key rejection.
- `tests/integration/test_watch_recovery.py` — generation watermark reset and explicit persistence time.
- `tests/integration/test_app_pipeline.py` — router timestamp forwarding and SQLite revision timestamps.
- `tests/integration/test_signal_concurrency.py` — pass deterministic explicit timestamps through concurrent mutations.
- `tests/integration/test_relation_activation.py` — construct relation-strategy contexts with the separated time domains.

---

## Task 1: Add the generation-scoped market clock

**Files:**

- Create: `predmarket/watch/clock.py`
- Create: `tests/unit/watch/test_clock.py`

- [ ] Write failing tests for initialization, advancement, and generation isolation.

  Cover these exact cases:

  ```python
  def test_recovery_initializes_market_time_from_latest_snapshot() -> None:
      clock = MarketClock()
      assert clock.initialize(generation=1, exchange_timestamps=(100, 120)) == 120
      assert clock.read(generation=1) == 120


  def test_accepted_older_event_does_not_move_market_time_backwards() -> None:
      clock = MarketClock()
      clock.initialize(generation=1, exchange_timestamps=(120,))
      assert clock.advance(generation=1, exchange_timestamp=110) == 120


  def test_new_generation_cannot_reuse_previous_watermark() -> None:
      clock = MarketClock()
      clock.initialize(generation=1, exchange_timestamps=(500,))
      assert clock.read(generation=2) is None
      assert clock.initialize(generation=2, exchange_timestamps=(200,)) == 200
  ```

  Also assert that generation zero, non-increasing initialization generations, an empty recovery timestamp collection, negative timestamps, and advancing a non-active generation raise `ValueError`.

- [ ] Run the focused tests and confirm the expected import failure.

  ```bash
  .venv/bin/pytest -q tests/unit/watch/test_clock.py
  ```

  Expected: fail because `predmarket.watch.clock.MarketClock` does not exist.

- [ ] Implement `MarketClock` with the following public contract.

  ```python
  @dataclass(slots=True)
  class MarketClock:
      _generation: int = 0
      _watermark_ms: int | None = None

      @property
      def generation(self) -> int:
          return self._generation

      @property
      def watermark_ms(self) -> int | None:
          return self._watermark_ms

      def initialize(
          self, *, generation: int, exchange_timestamps: Collection[int]
      ) -> int:
          values = tuple(exchange_timestamps)
          if type(generation) is not int or generation <= self._generation:
              raise ValueError("generation must be a newer positive integer")
          if not values or any(type(value) is not int or value < 0 for value in values):
              raise ValueError("exchange_timestamps must contain non-negative integers")
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
  ```

  `initialize()` validates all timestamps before mutating state, sets the watermark to `max(exchange_timestamps)`, and requires a strictly newer generation. `advance()` accepts only the active generation and assigns `max(current, exchange_timestamp)`. `read()` returns `None` for a different generation. `watermark_ms` exposes the active generation's last accepted time; lifecycle code captures it before initializing the next generation.

- [ ] Run the focused tests and confirm they pass.

  ```bash
  .venv/bin/pytest -q tests/unit/watch/test_clock.py
  ```

- [ ] Commit the isolated primitive.

  ```bash
  git add predmarket/watch/clock.py tests/unit/watch/test_clock.py
  git commit -m "feat: add generation scoped market clock"
  ```

---

## Task 2: Fence evaluations with cache revisions

**Files:**

- Modify: `predmarket/watch/cache.py`
- Modify: `tests/unit/watch/test_cache.py`

- [ ] Add failing cache tests for the exact revision semantics.

  Assert:

  - a new cache starts at revision `0`;
  - an accepted recovery snapshot increments the revision once;
  - an accepted `apply_book()` mutation increments it once;
  - an accepted `apply_delta()` mutation increments it once, regardless of how many token books it updates;
  - an equal/no-op book, stale generation, invalid-cache write, and timestamp regression do not increment it;
  - `begin_resync()` invalidates the cache but does not masquerade as accepted market data by incrementing the revision.

- [ ] Run the focused cache tests and confirm the new assertions fail.

  ```bash
  .venv/bin/pytest -q tests/unit/watch/test_cache.py
  ```

  Expected: fail because `OrderBookCache.revision` is absent.

- [ ] Add `_revision: int`, a read-only `revision` property, and increment only after accepted state mutations.

  Preserve the existing boolean return contracts of `apply_book()` and `apply_delta()`. Increment after all validation and only when the method returns `True`; increment once after `apply_snapshot()` installs a valid snapshot. Do not alter per-token timestamp-regression behavior.

- [ ] Run the cache suite and watch tests that directly depend on cache behavior.

  ```bash
  .venv/bin/pytest -q tests/unit/watch/test_cache.py tests/unit/watch/test_task.py
  ```

- [ ] Commit the freshness primitive.

  ```bash
  git add predmarket/watch/cache.py tests/unit/watch/test_cache.py
  git commit -m "feat: track order book cache revisions"
  ```

---

## Task 3: Require an explicit business timestamp for every signal mutation

**Files:**

- Modify: `predmarket/signals/manager.py`
- Modify: `predmarket/watch/task.py`
- Modify: `predmarket/app.py`
- Modify: `tests/unit/signals/test_manager.py`
- Modify: `tests/unit/watch/test_task.py`
- Modify: `tests/integration/test_watch_recovery.py`
- Modify: `tests/integration/test_app_pipeline.py`
- Modify: `tests/integration/test_signal_concurrency.py`

- [ ] Change signal-manager tests first so every mutation supplies `observed_at`.

  Add direct assertions that:

  ```python
  await manager.apply(
      decision,
      opportunity_key="binary:market-1",
      expected_revision=None,
      observed_at=1_700_000_000_123,
  )
  ```

  writes `opened_at`, `updated_at`, and revision `observed_at` as `1_700_000_000_123`; a later update uses its newly supplied value; `close_for_tokens()` and `close_unwatchable_for_active_tokens()` use their supplied closure value. Assert a negative or non-integer `observed_at` raises `ValueError` before database access.

- [ ] Run manager tests and confirm signature failures.

  ```bash
  .venv/bin/pytest -q tests/unit/signals/test_manager.py
  ```

- [ ] Change the concrete manager API and remove its internal wall-clock dependency.

  Add a required keyword-only `observed_at: int` after `expected_revision` in `apply()`, after `decision` in `close_for_tokens()`, and after `active_token_ids` in `close_unwatchable_for_active_tokens()`. Keep their existing return types: `str | None`, `tuple[str, ...]`, and `tuple[str, ...]`, respectively.

  Delete the constructor `clock` field after all call sites are converted. Validate `type(observed_at) is int and observed_at >= 0` once at each public entry point, and thread that exact value through every revision and signal-row update.

- [ ] Update protocols, fakes, and `_SignalManagerRouter` to forward `observed_at` without replacing it.

  During this intermediate task, `WatchTask` may pass its existing `_now()` value so the suite stays executable. Task 6 replaces that source with `MarketClock` atomically. Remove `_SignalManagerRouter`'s clock constructor argument and make closure routing preserve the caller-supplied timestamp.

- [ ] Run all signal-manager and adapted caller tests.

  ```bash
  .venv/bin/pytest -q \
    tests/unit/signals/test_manager.py \
    tests/unit/watch/test_task.py \
    tests/integration/test_watch_recovery.py \
    tests/integration/test_app_pipeline.py \
    tests/integration/test_signal_concurrency.py
  ```

- [ ] Commit the explicit-timestamp API migration.

  ```bash
  git add predmarket/signals/manager.py predmarket/watch/task.py predmarket/app.py \
    tests/unit/signals/test_manager.py tests/unit/watch/test_task.py \
    tests/integration/test_watch_recovery.py tests/integration/test_app_pipeline.py \
    tests/integration/test_signal_concurrency.py
  git commit -m "refactor: make signal mutation time explicit"
  ```

---

## Task 4: Separate business evaluation time from fee-cache runtime time

**Files:**

- Modify: `predmarket/domain/signal.py`
- Modify: `predmarket/strategy/common.py`
- Modify: `predmarket/strategy/risk.py`
- Modify: `predmarket/app.py`
- Modify: `tests/unit/strategy/conftest.py`
- Modify: `tests/unit/strategy/test_risk.py`
- Modify: `tests/unit/strategy/test_decimal_isolation.py`
- Modify: `tests/unit/strategy/test_binary.py`
- Modify: `tests/unit/strategy/test_implication.py`
- Modify: `tests/unit/domain/test_models.py`
- Modify: `tests/unit/watch/test_task.py`
- Modify: `tests/integration/test_relation_activation.py`

- [ ] Add failing tests proving fee freshness is invariant to market/host skew.

  Construct a `StrategyContext` where:

  - `evaluated_at=2_000` is market time;
  - `fee_schedule_evaluated_at=10_000` is host runtime time;
  - `FeeSchedule.updated_at=9_500` is host receipt time.

  Assert the fee is fresh according to the 500 ms host-domain age even though comparing market time to `updated_at` would move backwards. Add the converse case where `fee_schedule_evaluated_at` exceeds the configured fee age and the strategy fails closed.

- [ ] Run focused domain and strategy tests and confirm construction/signature failures.

  ```bash
  .venv/bin/pytest -q tests/unit/domain/test_models.py tests/unit/strategy/test_risk.py
  ```

- [ ] Add `fee_schedule_evaluated_at: int` to `StrategyContext` and validate it independently.

  Keep `evaluated_at` documented and validated as market business time. Remove `orderbook_observed_at`; that host receipt field must not be available to strategy age calculations. Update all context fixtures and constructors explicitly instead of adding a permissive default.

- [ ] Update fee freshness call sites to use `context.fee_schedule_evaluated_at`.

  In the existing `_ApplicationContextSource._context()` constructor call, set `evaluated_at=0` and `fee_schedule_evaluated_at=self._clock_ms()`, leaving its catalog, relation, fee, and configuration arguments unchanged. The zero is an unevaluable sentinel; `WatchTask` must replace `evaluated_at` with a valid market watermark before strategy execution. The host fee timestamp is not replaced by `WatchTask`.

- [ ] Run all affected strategy, domain, watch, and application tests.

  ```bash
  .venv/bin/pytest -q \
    tests/unit/domain/test_models.py \
    tests/unit/strategy \
    tests/unit/watch/test_task.py \
    tests/integration/test_app_pipeline.py \
    tests/integration/test_relation_activation.py
  ```

- [ ] Commit the time-domain separation.

  ```bash
  git add predmarket/domain/signal.py predmarket/strategy/common.py \
    predmarket/strategy/risk.py predmarket/app.py tests/unit/domain/test_models.py \
    tests/unit/strategy tests/unit/watch/test_task.py \
    tests/integration/test_app_pipeline.py tests/integration/test_relation_activation.py
  git commit -m "refactor: separate market and fee cache time domains"
  ```

---

## Task 5: Replace clock-skew validity configuration with diagnostics

**Files:**

- Modify: `predmarket/config.py`
- Modify: `config/default.yaml`
- Modify: `predmarket/strategy/common.py`
- Modify: `tests/unit/test_config.py`
- Modify: `tests/unit/strategy/conftest.py`
- Modify: `tests/unit/strategy/test_binary.py`
- Modify: `tests/unit/domain/test_models.py`
- Modify: `tests/unit/watch/test_task.py`

- [ ] Add failing configuration tests for the compatibility contract.

  Cover all four inputs:

  1. `exchange_clock_skew_warning_ms` loads into `StrategyConfig.exchange_clock_skew_warning_ms` without warning.
  2. Legacy `maximum_exchange_clock_skew_ms` loads the same field and emits one `WARNING` containing the old and new keys and stating that skew no longer rejects market data.
  3. Supplying both keys raises `ConfigurationError`.
  4. Supplying neither key raises `ConfigurationError` under strict key validation.

- [ ] Run configuration tests and confirm the new cases fail.

  ```bash
  .venv/bin/pytest -q tests/unit/test_config.py
  ```

- [ ] Implement strict alias parsing and rename the dataclass field.

  Resolve the mutually exclusive key before `_require_keys()` so either accepted key satisfies the strategy schema. Log compatibility through `logging.getLogger(__name__).warning(...)`. Update `config/default.yaml` to:

  ```yaml
  exchange_clock_skew_warning_ms: 100
  ```

  Update strategy fingerprints and test factories to use the new field. Do not retain a `maximum_exchange_clock_skew_ms` property on `StrategyConfig`, because that would keep validity semantics ambiguous.

- [ ] Add strategy tests for market-time validation.

  Replace the old “exchange ahead by more than 100 ms is rejected” assertion with:

  - `exchange_timestamp > received_timestamp` is accepted when the book is current relative to `context.evaluated_at`;
  - `context.evaluated_at - exchange_timestamp > maximum_book_age_ms` is rejected as stale;
  - `max(exchange_timestamp) - min(exchange_timestamp) > maximum_leg_skew_ms` remains rejected;
  - `book.exchange_timestamp > context.evaluated_at` fails closed as inconsistent market time;
  - `received_timestamp` never participates in those validity decisions.

- [ ] Rewrite `_validate_orderbooks()` to keep all validity comparisons in market time.

  For the required books, calculate:

  ```python
  book_age_ms = context.evaluated_at - book.exchange_timestamp
  exchange_clock_skew_ms = book.exchange_timestamp - book.received_timestamp
  leg_skew_ms = max(exchange_timestamps) - min(exchange_timestamps)
  ```

  Reject negative `book_age_ms`, excessive `book_age_ms`, and excessive `leg_skew_ms`. Do not reject on `exchange_clock_skew_ms`; expose it only as diagnostic context for `WatchTask` logging. Remove the host-observation branch entirely so leg skew is always enforced.

- [ ] Run configuration and strategy tests.

  ```bash
  .venv/bin/pytest -q \
    tests/unit/test_config.py \
    tests/unit/strategy/test_binary.py \
    tests/unit/strategy/test_implication.py \
    tests/unit/domain/test_models.py
  ```

- [ ] Commit the policy and configuration migration.

  ```bash
  git add predmarket/config.py config/default.yaml predmarket/strategy/common.py \
    tests/unit/test_config.py tests/unit/strategy/conftest.py \
    tests/unit/strategy/test_binary.py tests/unit/domain/test_models.py \
    tests/unit/watch/test_task.py
  git commit -m "fix: treat exchange host skew as diagnostics"
  ```

---

## Task 6: Drive watch evaluation and persistence from market time

**Files:**

- Modify: `predmarket/watch/task.py`
- Modify: `tests/unit/watch/test_task.py`

- [ ] Add deterministic WatchTask tests before changing implementation.

  Use separate fake wall and monotonic clocks and assert:

  - recovery books at exchange timestamps 1,000 and 1,050 initialize `market_time=1_050`;
  - the initial strategy context has `evaluated_at=1_050` even when host wall time is 900 or 3,000;
  - an accepted stream mutation at 1,100 advances subsequent evaluation and signal `observed_at` to 1,100;
  - an event older than the watermark cannot move it backwards;
  - a rejected timestamp regression does not advance the watermark;
  - no strategy call or signal-manager call occurs when the active generation has no watermark;
  - host wall movement alone neither makes a book stale nor changes persisted `observed_at`.

- [ ] Add monotonic stream-silence tests.

  Inject `monotonic_ms: Callable[[], int]` into `WatchTask`. Assert wall-clock jumps do not affect stream-silence age, while monotonic elapsed time does. Rename `_last_orderbook_observed_at_ms` to `_last_market_data_monotonic_ms` and set it only after a recovery snapshot or accepted stream cache mutation.

- [ ] Add stale-evaluation fencing tests using cache revision.

  Pause the strategy coroutine, mutate a book in the same generation, resume it, and assert no signal is persisted. Repeat with a generation rotation. Assert `_evaluation_is_current()` requires the captured generation, a valid cache, and an unchanged cache revision.

- [ ] Run the WatchTask tests and confirm failures against the host-time implementation.

  ```bash
  .venv/bin/pytest -q tests/unit/watch/test_task.py
  ```

- [ ] Integrate `MarketClock` into `WatchTask` recovery and stream processing.

  Exact flow:

  1. after `cache.apply_snapshot()` succeeds, initialize the clock from every active recovery book's `exchange_timestamp`;
  2. after `cache.apply_book()` or `cache.apply_delta()` returns `True`, advance the clock with accepted exchange timestamps represented by that mutation;
  3. capture `generation`, `cache.revision`, and `market_time` together before building context;
  4. skip evaluation with an INFO/WARNING reason when `market_time is None`;
  5. call `replace(context, evaluated_at=market_time)` without modifying `fee_schedule_evaluated_at`;
  6. before and after each awaited boundary, require `_evaluation_is_current(generation, revision)`;
  7. call `signal_manager.apply(decision, opportunity_key, expected_revision, observed_at=market_time)`.

  For multi-book deltas, advance using the greatest exchange timestamp among books actually accepted into the cache. Do not advance for generation mismatch, invalid cache, timestamp regression, or no-op data.

- [ ] Compute diagnostics directly from evaluated books.

  Every evaluation summary should include:

  ```text
  market_time=<watermark>
  stream_silence_age_ms=<monotonic elapsed>
  maximum_exchange_clock_skew_ms=<max(exchange-received)>
  exchange_clock_skew_warning_ms=<configured threshold>
  maximum_exchange_clock_skew_token_id=<token>
  ```

  Emit a warning when the absolute diagnostic exceeds the configured warning threshold, including token ID plus exchange and receipt timestamps. The warning must explicitly say that evaluation continues. Keep the summary available even when the strategy decision is valid; do not derive diagnostics only from a rejection context.

- [ ] Run focused watch, cache, strategy, and manager tests.

  ```bash
  .venv/bin/pytest -q \
    tests/unit/watch/test_clock.py \
    tests/unit/watch/test_cache.py \
    tests/unit/watch/test_task.py \
    tests/unit/strategy \
    tests/unit/signals/test_manager.py
  ```

- [ ] Commit the watch pipeline conversion.

  ```bash
  git add predmarket/watch/task.py tests/unit/watch/test_task.py
  git commit -m "feat: evaluate watch data on market time"
  ```

---

## Task 7: Use the last market watermark for lifecycle closures

**Files:**

- Modify: `predmarket/watch/task.py`
- Modify: `predmarket/app.py`
- Modify: `tests/unit/watch/test_task.py`
- Modify: `tests/integration/test_watch_recovery.py`
- Modify: `tests/integration/test_app_pipeline.py`

- [ ] Add failing lifecycle tests.

  Assert:

  - startup does not call `close_unwatchable_for_active_tokens()` before recovery establishes a watermark;
  - after successful recovery, unwatchable closure uses that generation's recovery watermark;
  - disconnect/invalidation closure uses the last watermark accepted by the invalidated generation;
  - if recovery fails before any watermark exists, no timestamped signal mutation occurs and the log explains the skip;
  - after generation 1 ends at 5,000 and generation 2 recovers at 2,000, generation-2 evaluations and closures use 2,000 or later, never 5,000.

- [ ] Run focused watch and recovery integration tests and confirm lifecycle failures.

  ```bash
  .venv/bin/pytest -q tests/unit/watch/test_task.py tests/integration/test_watch_recovery.py
  ```

- [ ] Move startup pruning behind successful recovery and pass explicit watermark values to every close path.

  Capture the old generation watermark before beginning a resync so disconnect-driven closures can use it. Once a new generation initializes, only its watermark is valid for its strategy work. If a closure has no valid watermark, skip it and emit a structured log containing generation, reason, and `market_time=None`.

- [ ] Update application router tests to prove explicit timestamps survive strategy routing and closure routing unchanged.

- [ ] Run lifecycle and application pipeline tests.

  ```bash
  .venv/bin/pytest -q \
    tests/unit/watch/test_task.py \
    tests/integration/test_watch_recovery.py \
    tests/integration/test_app_pipeline.py
  ```

- [ ] Commit lifecycle timestamp handling.

  ```bash
  git add predmarket/watch/task.py predmarket/app.py tests/unit/watch/test_task.py \
    tests/integration/test_watch_recovery.py tests/integration/test_app_pipeline.py
  git commit -m "fix: timestamp watch closures with market time"
  ```

---

## Task 8: Verify SQLite persistence across recovery and stream updates

**Files:**

- Modify: `tests/integration/test_app_pipeline.py`
- Modify: `tests/integration/test_watch_recovery.py`
- Modify: `tests/integration/test_signal_concurrency.py`

- [ ] Add an integration scenario with deliberately skewed host and market clocks.

  Use a temporary SQLite database and deterministic inputs:

  - host wall clock: `10_000`;
  - recovery exchange timestamps: `20_000` and `20_020`;
  - accepted stream update: `20_100`;
  - configured skew warning threshold: `100`.

  Assert startup and strategy evaluation continue despite market time being ahead of host time. Query `arbitrage_signals` and `signal_revisions` directly and assert the open/update/close rows use `20_020` or `20_100` according to the evaluation that produced them, not `10_000`.

- [ ] Add a recovery rotation integration assertion.

  Recover a second generation whose snapshots have a smaller timestamp than the previous generation. Assert the second generation starts from its own maximum snapshot timestamp and no event from the first generation can mutate its cache, watermark, or SQLite rows.

- [ ] Run the integration tests and confirm they fail before final caller adaptation, then pass after all APIs use explicit timestamps.

  ```bash
  .venv/bin/pytest -q \
    tests/integration/test_app_pipeline.py \
    tests/integration/test_watch_recovery.py \
    tests/integration/test_signal_concurrency.py
  ```

- [ ] Commit integration coverage.

  ```bash
  git add tests/integration/test_app_pipeline.py \
    tests/integration/test_watch_recovery.py \
    tests/integration/test_signal_concurrency.py
  git commit -m "test: verify market time signal persistence"
  ```

---

## Task 9: Run full verification and capture live evidence

**Files:**

- Modify: `docs/runtime-investigation-2026-08-04.md`

- [ ] Run formatting-independent source validation and the full test suite.

  ```bash
  PYTHONPYCACHEPREFIX=/tmp/predmarket_pycache .venv/bin/python -m compileall -q predmarket tests
  .venv/bin/pytest -q
  ```

  Expected: compile succeeds and the full suite passes with no new skips.

- [ ] Check the final diff for whitespace, accidental files, old validity references, and host-clock persistence.

  ```bash
  git diff --check
  rg -n "maximum_exchange_clock_skew_ms|orderbook_observed_at" predmarket config tests
  rg -n "self\._clock\(\)" predmarket/signals/manager.py
  git status --short
  ```

  Expected: the old config key appears only in compatibility parsing/tests/log wording; `orderbook_observed_at` and the signal manager's internal clock read have no production matches; the personal untracked plan `docs/superpowers/plans/2026-08-04-review-runtime-reliability.md` remains untouched.

- [ ] Start the real runtime and wait for recovery, component startup, stream evaluation, and at least one persisted signal.

  ```bash
  .venv/bin/python -m predmarket run 2>&1 | tee /tmp/predmarket-market-clock-live.log
  ```

  Observe for long enough to capture:

  - INFO logs for every successfully started component;
  - recovery generation and initialized `market_time`;
  - exchange/host skew warning that states evaluation continues;
  - evaluation summaries with `market_time` and monotonic stream-silence age;
  - `persisted_signals>0` or an explainable valid no-op decision unrelated to clock skew;
  - no `orderbook_timestamp_causality_invalid` rejection caused solely by market time being ahead of host time.

  Stop with one `Ctrl-C` after evidence is captured and verify clean shutdown logs.

- [ ] Query the configured SQLite database and correlate the latest revision with log market time.

  ```bash
  sqlite3 ./data/predmarket-v1.sqlite3 "
  SELECT a.id,
         a.status,
         a.updated_at,
         a.latest_revision,
         r.observed_at,
         r.quantity,
         r.expected_profit
  FROM arbitrage_signals AS a
  JOIN signal_revisions AS r
    ON r.signal_id = a.id
   AND r.revision = a.latest_revision
  ORDER BY r.observed_at DESC
  LIMIT 5;
  "
  ```

  Confirm `observed_at` equals the corresponding evaluation's logged `market_time`. Do not require it to be close to the host clock.

- [ ] Update `docs/runtime-investigation-2026-08-04.md` with the root cause, final time-domain rules, exact test count, representative sanitized logs, SQLite evidence, and any remaining external WebSocket instability.

- [ ] Re-run the full suite after documentation-only edits are staged and verify the staged diff.

  ```bash
  .venv/bin/pytest -q
  git diff --check
  git diff --cached --check
  ```

- [ ] Commit final evidence and documentation.

  ```bash
  git add docs/runtime-investigation-2026-08-04.md
  git commit -m "docs: record market clock runtime verification"
  ```

---

## Task 10: Update the existing pull request

- [ ] Confirm branch scope and commits before publishing.

  ```bash
  git status --short --branch
  git log --oneline origin/fix/runtime-sync-watch-reliability..HEAD
  git diff --stat origin/fix/runtime-sync-watch-reliability...HEAD
  ```

  Ensure the diff contains the confirmed market-clock work and its specification/plan, but does not contain `docs/superpowers/plans/2026-08-04-review-runtime-reliability.md` or unrelated user files.

- [ ] Push the current branch and inspect PR #16 checks.

  ```bash
  git push origin fix/runtime-sync-watch-reliability
  gh pr checks 16 --watch
  ```

- [ ] Update PR #16's description with the time-domain policy, compatibility behavior, tests, live runtime evidence, and SQLite verification. Do not merge the PR unless the user separately requests it.
