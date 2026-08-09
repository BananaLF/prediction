# Implementation Plan: Govern 'watch_evaluation_aborted' Diagnostics

> **For agentic workers:** REQUIRED SUB-SKILL: use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make WatchTask explain why an evaluation became stale and aggregate high-frequency aborts into bounded interval logs, while preserving the existing token-revision fence, fail-closed recovery, and signal-apply atomicity.

**Architecture:** Keep the first implementation inside predmarket/watch/task.py. A pure classifier compares the captured TokenRevisionSnapshot with the current cache state and returns a bounded diagnostic value. WatchTask records abort counts keyed by reason and stage, emits at most one watch_evaluation_abort_summary per 10-second window, and flushes an unfinished window during shutdown. Existing evaluation call sites pass the snapshot and dependency token IDs only where revision checks are performed. No cache, strategy, signal, or database semantics change.

**Tech Stack:** Python 3.14, asyncio, frozen dataclass, existing OrderBookCache revision APIs, pytest/pytest-asyncio, standard-library logging.

**Global Constraints:**

- Do not weaken or remove TokenRevisionSnapshot checks at before_strategy, after_strategy, before_signal_apply, lock acquisition, or after_signal_apply.
- Never persist a decision produced from a stale order-book snapshot.
- Preserve generation invalidation, fail-closed recovery, SubscriptionGenerationChanged handling, and signal-apply atomicity.
- Do not implement per-target stale skipping, debounce, or scheduling changes in this issue; those remain follow-up options only if the new evidence proves they are necessary.
- Do not add a database table, change strategy results, or change public APIs.
- Keep diagnostic token and revision samples bounded to four token IDs per representative event.
- Do not emit one INFO log per abort. The default INFO path must emit at most one abort summary per 10-second window; diagnostic detail may be emitted only at DEBUG when explicitly useful.
- Use the injected monotonic_ms clock for new interval state so unit tests do not depend on wall-clock timing.
- Preserve unrelated working-tree changes and make no changes outside the dedicated Issue 19 worktree.

---

## Current code map

- predmarket/watch/task.py
  - WatchTask.__init__ owns evaluation counters, pending token IDs, the injected monotonic clock, and summary timestamps.
  - _evaluate_tokens captures TokenRevisionSnapshot, evaluates contexts/strategies, and performs the revision fences.
  - _evaluation_generation_is_current handles generation-only checks.
  - _evaluation_is_current handles generation, cache-state, and dependency revision checks through OrderBookCache.token_revisions_match.
  - _log_evaluation_aborted currently emits an INFO record containing only expected/actual generation, cache state, and stage.
  - close/_finish_close cancel the evaluator and clear pending work.
- predmarket/watch/cache.py
  - TokenRevisionSnapshot is the immutable captured revision input.
  - snapshot_token_revisions and token_revisions_match are the read-only cache APIs used by the classifier.
- tests/unit/watch/test_task.py
  - Existing deterministic fixtures include BlockingStreamStrategy, FakeSignals, BlockingApplySignals, controllable clocks, and revision-fence tests.
  - New assertions should use logger predmarket.watch.task and caplog.
- scripts/build_env.sh
  - Runs the repository's complete test suite and CLI checks.
- README.md / docs/OPERATIONS.md
  - predmarket run --config config/default.yaml --log-level DEBUG is the real runtime observation command.

## Task 1: Add failing tests for abort reason classification

**Files:**

- Modify: tests/unit/watch/test_task.py
- Read-only reference: predmarket/watch/cache.py (TokenRevisionSnapshot construction and revision mutation)

**Step 1: Add a same-generation revision-mismatch assertion.**

Extend the existing test_same_generation_cache_revision_change_fences_stale_evaluation scenario so the strategy is released after token-1 advances in the same valid generation. Capture DEBUG logs from predmarket.watch.task and assert that the per-abort diagnostic record includes:

~~~
watch_evaluation_aborted
reason=dependency_revision_changed
stage=after_strategy
sample_token_ids=token-1
sample_expected_revisions=token-1:1
sample_actual_revisions=token-1:2
~~~

Keep the existing assertion that no stale signal was applied. Window-flush assertions belong to Task 3.

**Step 2: Add independent classification cases.**

Cover the following observable cases with deterministic cache/task state and assert the exact reason value in the summary:

- generation changes while the snapshot is being evaluated: generation_changed;
- cache becomes invalid or the task closes: cache_invalid_or_closed;
- a dependency token disappears from the current cache: dependency_missing.

For each case assert that the expected/actual sample is present only when a revision comparison is meaningful, and that a missing dependency never causes a stale decision to reach signal apply.

**Step 3: Run the new tests before implementation.**

Run:

~~~
env -u ALL_PROXY -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u http_proxy -u https_proxy \
  .venv/bin/python -m pytest -q tests/unit/watch/test_task.py -k 'evaluation_abort or same_generation_cache_revision_change_fences_stale_evaluation'
~~~

Expected result: the new reason/revision assertions fail because the current implementation has no classification or revision sample, while the pre-existing safety assertion remains green. This is the RED checkpoint. Window-summary assertions belong to Task 3 and are intentionally not part of this checkpoint.

## Task 2: Implement structured abort classification

**Files:**

- Modify: predmarket/watch/task.py
- Test: tests/unit/watch/test_task.py

**Step 1: Define the bounded diagnostic value.**

Add a private frozen, slotted dataclass next to the existing private task helpers:

~~~
@dataclass(frozen=True, slots=True)
class _EvaluationAbortDiagnostic:
    reason: str
    changed_token_ids: tuple[str, ...] = ()
    expected_revisions: tuple[tuple[str, int], ...] = ()
    actual_revisions: tuple[tuple[str, int], ...] = ()
~~~

Add _MAX_EVALUATION_ABORT_TOKEN_SAMPLES = 4 and use UTF-8-stable token ordering before truncation.

**Step 2: Add a read-only classifier.**

Implement a private helper with this interface:

~~~
def _classify_evaluation_abort(
    self,
    expected_generation: int,
    *,
    revision_snapshot: TokenRevisionSnapshot | None,
    dependency_token_ids: tuple[str, ...],
) -> _EvaluationAbortDiagnostic:
    ...
~~~

Classify in this order:

1. cache_invalid_or_closed when the task is closed or cache state is not VALID.
2. generation_changed when the current cache generation differs from expected_generation.
3. dependency_missing when a requested dependency is absent from the current valid cache.
4. dependency_revision_changed when the current generation is valid but one or more requested dependency revisions differ from the captured snapshot.

For revision comparison, read a current token revision snapshot without mutating cache state. Record at most four changed IDs and matching expected/actual (token_id, revision) pairs. The helper must not call apply_*, invalidate, or otherwise alter cache state.

**Step 3: Extend _log_evaluation_aborted without changing its call contract for generation-only checks.**

Add optional keyword arguments:

~~~
revision_snapshot: TokenRevisionSnapshot | None = None,
dependency_token_ids: tuple[str, ...] = (),
~~~

Generate the structured diagnostic from those arguments and retain expected_generation, actual_generation, cache_state, and stage in the eventual log payload. Revision-fence call sites pass the captured snapshot and target dependency IDs; generation-only call sites pass only the current arguments. At this step, keep the existing per-abort log temporarily so the classifier can be verified before aggregation is introduced.

**Step 4: Run the focused classification tests.**

Run the Task 1 command again. Expected result: reason classification and bounded revision fields are correct, stale signal assertions remain green, and no existing generation/revision fence test regresses. This is the first GREEN checkpoint.

## Task 3: Add failing tests for bounded window aggregation

**Files:**

- Modify: tests/unit/watch/test_task.py

**Step 1: Test one summary per window.**

Use the existing blocking strategy and controllable monotonic clock to trigger multiple deterministic stale evaluations without advancing the clock beyond 10 seconds. Assert:

- exactly one watch_evaluation_abort_summary INFO record is emitted for the window;
- no individual watch_evaluation_aborted INFO records are emitted;
- window_aborted equals the number of stale evaluations;
- the summary includes reason/stage distribution and the bounded expected/actual sample;
- request, batch, coalesced, and pending-token fields are present.

Advance the injected monotonic clock by more than 10 seconds, trigger one more stale evaluation, and assert a second summary is emitted.

**Step 2: Test a window with no completed evaluation summary.**

Trigger only stale evaluations and never complete a normal evaluation. Advance the injected clock to the flush point and assert that watch_evaluation_abort_summary is still emitted. This proves the diagnostic does not depend on a preceding watch_evaluation_summary.

**Step 3: Test shutdown flush.**

Create an unfinished abort window, call await watch.close(), and assert that the last watch_evaluation_abort_summary is emitted once before close completes. A second close() must not emit a duplicate flush for the same empty window.

**Step 4: Run the aggregation tests before implementation.**

Run:

~~~
env -u ALL_PROXY -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u http_proxy -u https_proxy \
  .venv/bin/python -m pytest -q tests/unit/watch/test_task.py -k 'evaluation_abort_summary or evaluation_abort_window or evaluation_abort_close'
~~~

Expected result: the new aggregation assertions fail against the current per-abort INFO behavior. This is the RED checkpoint for aggregation.

## Task 4: Implement interval aggregation and call-site wiring

**Files:**

- Modify: predmarket/watch/task.py
- Test: tests/unit/watch/test_task.py

**Step 1: Add bounded window state.**

Initialize private state in WatchTask.__init__:

~~~
self._evaluation_abort_counts: dict[tuple[str, str], int] = {}
self._evaluation_abort_total = 0
self._evaluation_abort_max_elapsed_ms = 0
self._last_evaluation_abort_summary_at_ms = 0
self._last_completed_evaluation_summary_at_ms: int | None = None
self._last_evaluation_abort_sample: tuple[str, str, _EvaluationAbortDiagnostic] | None = None
~~~

Use an explicit initialization flag or None sentinel so the first abort always creates a flushable window. Keep the counter state private to WatchTask.

**Step 2: Replace per-abort INFO with counter updates and a flush helper.**

Implement:

~~~
def _maybe_log_evaluation_abort_summary(
    self,
    *,
    force: bool = False,
) -> None:
    ...
~~~

On every abort, increment the total and (reason, stage) counter, update the bounded representative sample, and call the flush helper. Flush when force is true or the injected monotonic clock has crossed the 10-second interval. The emitted record must be named watch_evaluation_abort_summary and include:

~~~
window_aborted
reason_counts
stage_counts
evaluation_requests
evaluation_batches
evaluation_coalesced
pending_tokens
last_completed_summary_age_ms
maximum_evaluation_elapsed_ms
sample_reason
sample_stage
sample_token_ids
sample_expected_revisions
sample_actual_revisions
~~~

Use stable sorted reason_counts/stage_counts strings and bounded token samples. Reset only window counters and sample after a successful log call. Preserve cumulative request/batch/coalesced counters so the summary still describes evaluator pressure.

**Step 3: Track completed-summary timing and maximum evaluation elapsed time.**

When watch_evaluation_summary is emitted, save the injected monotonic timestamp in _last_completed_evaluation_summary_at_ms. At completion, update the current abort window's maximum elapsed time if the evaluation elapsed time is larger. Report last_completed_summary_age_ms=unknown when no completed summary exists; otherwise report the non-negative age at flush time.

**Step 4: Wire all revision-fence aborts.**

Pass revision_snapshot and dependency_token_ids to _log_evaluation_aborted at before_strategy, after_strategy, before_signal_apply, signal_apply_lock_acquired, after_signal_apply, and signal_apply_generation_changed. Keep before_batch_context, after_batch_context, before_context, and after_context as generation-only classifications because no target dependency set exists at those points. Preserve the existing return behavior for the whole batch.

**Step 5: Flush during close.**

After evaluator cancellation has completed and before pending evaluation state is cleared, call _maybe_log_evaluation_abort_summary(force=True). Make the helper idempotent for an empty window so repeated close() calls cannot duplicate the final summary.

**Step 6: Run the focused suite.**

Run:

~~~
env -u ALL_PROXY -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u http_proxy -u https_proxy \
  .venv/bin/python -m pytest -q tests/unit/watch/test_task.py -k 'evaluation_abort or revision_change or generation_during_signal_apply or signal_apply_is_atomic'
~~~

Expected result: all new classification/aggregation tests and the existing safety tests pass; INFO output contains bounded interval summaries rather than one record per abort.

## Task 5: Run the complete verification and runtime observation

**Files:**

- Modify: docs/runtime-investigation-2026-08-09-issue-19.md
- Read-only verification: scripts/build_env.sh, README.md, docs/OPERATIONS.md

**Step 1: Run repository verification.**

Run with proxy variables removed and a writable temporary uv cache:

~~~
env -u ALL_PROXY -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u http_proxy -u https_proxy \
  UV_CACHE_DIR=/private/tmp/predmarket-uv-cache-issue19 \
  ./scripts/build_env.sh
~~~

Expected result: environment setup succeeds, CLI help checks succeed, and the complete suite reports no failures. If dependency installation is already available, retain the same proxy/cache isolation so network configuration does not mask test results.

**Step 2: Observe one real high-rate window.**

Run the documented runtime command from the dedicated worktree:

~~~
PYTHONUNBUFFERED=1 .venv/bin/predmarket run --config config/default.yaml --log-level DEBUG
~~~

Keep the process running through at least one 10-second abort-summary window and one additional window when the stream is active, then stop it with the normal interrupt path. Capture the runtime output without changing production configuration or persistent data.

**Step 3: Write the evidence report.**

Create docs/runtime-investigation-2026-08-09-issue-19.md with only observed values: command/environment, observation interval, total aborts and rate, reason/stage counts, completed-summary count and interval, pending/request/batch/coalesced values, context/strategy/signal-apply timing, and INFO log rate. State whether the observations meet the design criteria:

- normal expiration when completed summaries continue at a stable interval;
- concern after three consecutive 10-second windows with pending work and no completed summary;
- severe candidate after a pending-work summary gap beyond 60 seconds;
- compute-waste evidence only when abort/batch and timing/coalescing trends persist.

Do not claim that per-target skipping or debounce is needed unless the captured evidence satisfies those criteria.

## Task 6: Final review and handoff

**Files:**

- Review: predmarket/watch/task.py
- Review: tests/unit/watch/test_task.py
- Review: docs/runtime-investigation-2026-08-09-issue-19.md

**Step 1: Check the diff and test hygiene.**

Run:

~~~
git diff --check
git diff --stat origin/main...HEAD
env -u ALL_PROXY -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u http_proxy -u https_proxy \
  .venv/bin/python -m pytest -q tests/unit/watch/test_task.py tests/unit/watch/test_cache.py
~~~

Confirm that no unrelated files changed, no unbounded token/revision data is logged, and every existing fence remains asserted.

**Step 2: Self-review against the approved design.**

Confirm explicitly that the implementation:

- distinguishes generation changes from same-generation dependency revision changes;
- retains fail-closed behavior and signal-apply atomicity;
- emits a bounded summary even without completed evaluations;
- flushes once on close;
- does not implement deferred options B/C behavior;
- records runtime evidence before recommending any second-stage performance change.

**Step 3: Commit only after all verification is green.**

Use one focused commit:

~~~
git add -- predmarket/watch/task.py tests/unit/watch/test_task.py docs/runtime-investigation-2026-08-09-issue-19.md
git commit -m 'fix: govern aborted watch evaluations'
~~~

The implementation stage then follows the employee-mode workflow for review and PR publication; this plan stage itself makes no business-code change.

## Plan self-review checklist

- Spec coverage: root-cause diagnosis, reason classification, revision evidence, bounded aggregation, shutdown flush, safety regressions, full verification, and real runtime evidence are all mapped to concrete tasks.
- File mapping: each implementation/test/report file is named before the task that edits it.
- Type consistency: TokenRevisionSnapshot, CacheState, WatchTask._monotonic_now, predmarket.watch.task logger, and existing fixture names match the current repository interfaces.
- Testability: interval behavior uses the injected monotonic clock; no test requires wall-clock sleep.
- Safety: the plan never relaxes a revision fence or persists stale signals.
- Placeholder scan: no TODO, TBD, or unbounded 'add tests' step remains.
