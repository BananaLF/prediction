# Runtime Python Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add configurable Python logging for runtime lifecycle, sync counts, unique watched-market counts, errors, and committed signal transitions while keeping CLI JSON output isolated on `stdout`.

**Architecture:** Component modules own module-level loggers; only the `run` CLI command configures an application logging handler on `stderr`. `Supervisor`, `SyncMarketTask`, `WatchTask`, `SignalManager`, and `Notifier` emit events at their existing lifecycle/error boundaries. `Notifier` stops printing to the terminal but continues desktop delivery and `system_events` auditing.

**Tech Stack:** Python standard-library `logging`, `argparse`, existing `pytest`/`pytest-asyncio` test suite, SQLite repositories, and the existing Polymarket runtime components.

## Global Constraints

- Python version floor remains `>=3.11`.
- Do not add third-party logging dependencies, log files, or log rotation.
- Runtime logs go to `stderr`; `status`, `signals`, and `relations` JSON remains on `stdout`.
- Default runtime level is `INFO`; `run --log-level LEVEL` accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`, case-insensitively.
- Remove `Notifier` terminal printing; retain desktop notifications and `system_events` persistence.
- `markets_seen` means validated/processed sync snapshots; `markets_persisted` means market records passed to `save_catalog`, including retained records upserted again.
- Watch market count is the deduplicated `market_id` count represented by the active token subscription.
- Log only committed `OPENED`, `UPDATED`, and `CLOSED` signal transitions; do not log `NOOP`.
- Preserve unrelated user changes and do not change trading, synchronization, Watch, or signal business semantics.

## File Map

- Modify `predmarket/cli.py`: parse `--log-level` and configure the `predmarket` logger for `run`.
- Modify `predmarket/notification/notifier.py`: remove terminal writes and log desktop-delivery failures.
- Modify `predmarket/catalog/sync.py`: add `SyncResult.markets_persisted` and sync summary logs.
- Modify `predmarket/watch/task.py`: track deduplicated active market IDs and log successful subscriptions.
- Modify `predmarket/signals/manager.py`: log committed signal transitions before optional notification delivery.
- Modify `predmarket/app.py`: log component initialization, runtime lifecycle, and supervisor errors/task exits.
- Modify `tests/integration/test_cli.py`: cover log-level parsing and CLI output separation.
- Modify `tests/unit/notification/test_notifier.py`: replace terminal-output assertions with no-terminal-output and logging/audit assertions.
- Modify `tests/unit/catalog/test_sync.py`: assert persisted-market counts and sync summary records.
- Modify `tests/unit/watch/test_task.py`: assert unique-market counting and subscription logs.
- Modify `tests/unit/signals/test_manager.py`: assert committed transition logs and no `NOOP` log.
- Modify `tests/integration/test_app_pipeline.py`: assert Supervisor lifecycle/error logs instead of removed terminal notifications.
- Modify `docs/OPERATIONS.md`: document `stderr` logging, `--log-level`, and the unchanged `system_events` channel.

---

### Task 1: Add CLI log-level configuration and stream separation

**Files:**
- Modify: `predmarket/cli.py`
- Test: `tests/integration/test_cli.py`

**Interfaces:**
- Add `_LOG_LEVEL_NAMES: tuple[str, ...]` containing `("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")`.
- Add `_configure_logging(level_name: str, *, terminal_enabled: bool) -> None` that resolves the level with `getattr(logging, level_name)`, installs the application formatter/`stderr` handler when terminal logging is enabled, and does not modify unrelated logger handlers.
- Add `--log-level` to the `run` parser with `type=str.upper`, choices `_LOG_LEVEL_NAMES`, and default `"INFO"`.
- Call `_configure_logging(arguments.log_level, terminal_enabled=config.notification.terminal_enabled)` only on the `run` path before `asyncio.run(...)`.

- [ ] **Step 1: Write parser and logging behavior tests**

Add tests to `tests/integration/test_cli.py` that verify:

```python
def test_run_parser_accepts_case_insensitive_log_level() -> None:
    arguments = _build_parser().parse_args(["run", "--log-level", "debug"])
    assert arguments.log_level == "DEBUG"


def test_run_parser_defaults_to_info() -> None:
    arguments = _build_parser().parse_args(["run"])
    assert arguments.log_level == "INFO"


def test_run_parser_rejects_unknown_log_level() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["run", "--log-level", "TRACE"])
```

Add a test around `_configure_logging` that captures the application logger output and asserts the rendered record contains the timestamp, level, logger name, and message while the stream is `sys.stderr`. Keep the existing JSON tests and assert their injected `stdout` remains valid JSON.

- [ ] **Step 2: Run the focused tests and confirm they fail for the missing flag/helper**

Run:

```bash
pytest tests/integration/test_cli.py -q
```

Expected: failure because `run` has no `log_level` attribute and `_configure_logging` does not yet exist.

- [ ] **Step 3: Implement the CLI configuration**

Import `logging`, define the level tuple and helper, add the `run` argument, and configure logging after `AppConfig.load(...)` succeeds but before constructing/running `Supervisor`. Use a `logging.Formatter` equivalent to:

```text
%(asctime)s %(levelname)s %(name)s - %(message)s
```

Do not configure logging for `status`, `signals`, or `relations` so their JSON stdout contract remains unchanged.

- [ ] **Step 4: Re-run the focused tests**

Run:

```bash
pytest tests/integration/test_cli.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the CLI slice**

```bash
git add predmarket/cli.py tests/integration/test_cli.py
git commit -m "feat: configure runtime log level from cli"
```

### Task 2: Remove terminal Notifier output and preserve non-terminal delivery

**Files:**
- Modify: `predmarket/notification/notifier.py`
- Test: `tests/unit/notification/test_notifier.py`

**Interfaces:**
- Keep `Notifier.notify(...)` and its accepted arguments unchanged so Supervisor and SignalManager callers do not change.
- Keep the optional `terminal` constructor argument accepted for compatibility, but never write to it.
- Add a module logger and emit `ERROR` for desktop delivery failures while continuing `_record_desktop_failure(...)`.

- [ ] **Step 1: Rewrite the Notifier tests for the new terminal contract**

Change the desktop-failure test to assert `output.getvalue() == ""`, keep its exact `system_events` assertion, and use `caplog` to assert one `desktop_notification_failed` record includes `SIGNAL_OPENED` and `desktop unavailable`.

Change the operational-error test to assert `output.getvalue() == ""`; it must still complete without raising when `details` contains nested JSON text.

- [ ] **Step 2: Run the focused tests and confirm the old print behavior fails**

Run:

```bash
pytest tests/unit/notification/test_notifier.py -q
```

Expected: failure because `Notifier.notify` still prints to the injected stream.

- [ ] **Step 3: Remove terminal writes and add the error log**

Remove the `print(...)` calls and the operational-detail rendering that only supported terminal output. Retain input validation, desktop invocation, and `system_events` persistence. In `_record_desktop_failure`, log the event before/alongside the existing best-effort audit write; exceptions from notification reporting remain swallowed at the existing boundary.

- [ ] **Step 4: Re-run the focused tests**

Run:

```bash
pytest tests/unit/notification/test_notifier.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the Notifier slice**

```bash
git add predmarket/notification/notifier.py tests/unit/notification/test_notifier.py
git commit -m "feat: route notifier operations through logging"
```

### Task 3: Add sync persistence counts and summary logs

**Files:**
- Modify: `predmarket/catalog/sync.py`
- Test: `tests/unit/catalog/test_sync.py`

**Interfaces:**
- Extend `SyncResult` with `markets_persisted: int` immediately after `markets_seen`.
- Every `SyncResult` returned by `SyncMarketTask.run_once()` must set the field explicitly.
- The complete path reports `len(prepared.markets)`; the incomplete path reports `len(partial.markets)` only when a partial catalog save occurs, otherwise `0`.

- [ ] **Step 1: Add count assertions to existing complete and incomplete sync tests**

In the complete-generation test, assert `result.markets_persisted == 2`. Add an incomplete-generation case that records the `save_catalog` arguments and asserts the result count equals the number of partial market records passed to it. Add a no-partial-data case and assert `markets_persisted == 0`.

Add `caplog` assertions for:

```text
sync_completed ... sync_generation=sync-1 markets_seen=2 markets_persisted=2
sync_incomplete ... markets_persisted=0
```

- [ ] **Step 2: Run the focused sync tests and confirm the new field/logs fail**

Run:

```bash
pytest tests/unit/catalog/test_sync.py -q
```

Expected: failure because `SyncResult` has no `markets_persisted` field and summary records are not emitted.

- [ ] **Step 3: Implement the result field and boundary logs**

Add the logger and set `markets_persisted` at the exact `save_catalog` boundaries. Emit `sync_incomplete` at `ERROR` with generation, seen count, persisted count, and the joined error text. Emit `sync_completed` at `INFO` after the complete catalog save, including generation, seen count, persisted count, token count, and published/dropped change counts.

- [ ] **Step 4: Re-run the focused sync tests**

Run:

```bash
pytest tests/unit/catalog/test_sync.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the sync slice**

```bash
git add predmarket/catalog/sync.py tests/unit/catalog/test_sync.py
git commit -m "feat: log market sync persistence counts"
```

### Task 4: Track and log unique Watch markets

**Files:**
- Modify: `predmarket/watch/task.py`
- Test: `tests/unit/watch/test_task.py`

**Interfaces:**
- Add an internal subscription helper that returns both `tuple[str, ...]` token IDs and the corresponding sorted, deduplicated `tuple[str, ...]` market IDs.
- Track the active market IDs in an internal `_active_market_ids: tuple[str, ...]` alongside `_active_token_ids`; do not add a new public property.
- Update `_rotate_to(...)` to receive the new market IDs along with the new token IDs.
- Emit `watch_subscribed` only after `_recover(...)` succeeds (or after a successful zero-token transition), with `markets=len(_active_market_ids)` and `generation=self._cache.generation`.

- [ ] **Step 1: Add unique-market Watch tests**

Use the existing `_catalog()` fixture, where `token-1` and `token-2` both belong to `market-1`, and assert after `await watch.start()` that the captured `watch_subscribed` record has `markets=1`. Add a `second_market=True` case and assert `markets=2`. Add a rotation assertion that a successful change emits the new count and a recovery failure emits no successful subscription record.

- [ ] **Step 2: Run the focused Watch tests and confirm the new log contract fails**

Run:

```bash
pytest tests/unit/watch/test_task.py -q
```

Expected: failure because Watch does not yet track/log market IDs.

- [ ] **Step 3: Implement the paired token/market subscription calculation**

Factor the existing active-market predicate into the paired helper without changing which markets/tokens are watchable. Use token IDs that are actually returned for the subscription when deriving the market set, so multiple tokens for one market count once. Update `start`, `handle_market_change`, and `_rotate_to` to maintain the paired state and log only after successful recovery.

- [ ] **Step 4: Re-run the focused Watch tests**

Run:

```bash
pytest tests/unit/watch/test_task.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the Watch slice**

```bash
git add predmarket/watch/task.py tests/unit/watch/test_task.py
git commit -m "feat: log unique watched market counts"
```

### Task 5: Log committed signal transitions

**Files:**
- Modify: `predmarket/signals/manager.py`
- Test: `tests/unit/signals/test_manager.py`

**Interfaces:**
- Add a module logger.
- Keep `_notify_after_commit(notification: SignalNotification)` as the post-transaction boundary; log before checking whether a notifier exists, but return without logging for `NOOP`.
- Emit `signal_transition` at `INFO` with `event_type`, `signal_id`, `opportunity_key`, `revision`, and the configured strategy type.

- [ ] **Step 1: Add post-commit logging assertions**

Extend the existing open/update/noop/close lifecycle test with `caplog`. Assert the transition sequence is `OPENED`, `UPDATED`, `UPDATED`, `CLOSED`, `OPENED` for the lifecycle currently exercised, and assert no record has `event_type=NOOP`. Add a rollback/failure test or use the existing transaction-failure fixture to assert no transition record is emitted when the database operation raises.

- [ ] **Step 2: Run the focused signal tests and confirm logging is absent**

Run:

```bash
pytest tests/unit/signals/test_manager.py -q
```

Expected: failure on the new `caplog` assertions because SignalManager currently only invokes the notifier.

- [ ] **Step 3: Log after the committed transaction and before notification delivery**

In `_notify_after_commit`, return immediately for `NOOP`; otherwise log the committed `SignalNotification` fields, then invoke the existing notifier callback. Preserve the current exception swallowing for notification delivery so desktop failures cannot affect signal persistence or its log.

- [ ] **Step 4: Re-run the focused signal tests**

Run:

```bash
pytest tests/unit/signals/test_manager.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the signal slice**

```bash
git add predmarket/signals/manager.py tests/unit/signals/test_manager.py
git commit -m "feat: log committed signal transitions"
```

### Task 6: Add Supervisor lifecycle and error logs

**Files:**
- Modify: `predmarket/app.py`
- Test: `tests/integration/test_app_pipeline.py`

**Interfaces:**
- Add a module logger.
- Log `component_initialized` after each runtime component is successfully created: database/schema boundary, writer, repositories, notifier, change queue, gateway, sync task, and Watch task.
- Log `runtime_started` after initial sync completes and Watch starts; log `runtime_stopped` from the final cleanup path.
- Log startup exceptions as `runtime_startup_failed` and unexpected completed task exits as `runtime_task_exited`, while retaining the existing Notifier desktop/system-event calls.

- [ ] **Step 1: Convert Supervisor integration tests from terminal assertions to `caplog`**

Update the existing tests that assert `RUNTIME_STARTUP_FAILED`, `RUNTIME_TASK_EXITED`, `SYNC_GENERATION_INCOMPLETE`, and `SYNC_MARKET_SKIPPED` terminal text. Assert the corresponding `predmarket.app` log records instead, and retain assertions for `system_events`/Notifier calls where those are part of the behavior under test.

Add an initialization assertion that a successful `_build_runtime()`/`run()` path contains `component_initialized` for the writer and Watch task. Keep tests using injected `Notifier(terminal=...)` to verify the compatibility argument no longer produces output.

- [ ] **Step 2: Run the focused Supervisor tests and confirm the old terminal assertions fail**

Run:

```bash
pytest tests/integration/test_app_pipeline.py -q
```

Expected: failures where tests still expect terminal `print` output and where new lifecycle records are absent.

- [ ] **Step 3: Add lifecycle/error logging at Supervisor boundaries**

Use `INFO` for successful component/lifecycle records and `ERROR` for startup and unexpected task failures. Include task names and exception text. Keep cancellation as a normal shutdown path, preserve cleanup ordering, and do not replace existing Notifier/system-event calls.

- [ ] **Step 4: Re-run the focused Supervisor tests**

Run:

```bash
pytest tests/integration/test_app_pipeline.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit the Supervisor slice**

```bash
git add predmarket/app.py tests/integration/test_app_pipeline.py
git commit -m "feat: log supervisor lifecycle and failures"
```

### Task 7: Update operational documentation

**Files:**
- Modify: `docs/OPERATIONS.md`

**Interfaces:**
- Document `predmarket ... run --log-level DEBUG` and the supported level names.
- State that runtime logs go to `stderr`, while read-only command JSON stays on `stdout`.
- State that desktop notifications and `system_events` remain separate delivery/audit channels.

- [ ] **Step 1: Update the operations section**

Replace the statement that terminal `Notifier` output is the primary channel with the Python logging behavior, keep the existing `system_events` inspection command, and add a short example showing shell redirection of JSON stdout separately from runtime stderr.

- [ ] **Step 2: Review the documentation against the CLI parser contract**

Verify every documented command uses the accepted option order and that the documented levels exactly match `_LOG_LEVEL_NAMES`.

- [ ] **Step 3: Commit the documentation slice**

```bash
git add docs/OPERATIONS.md
git commit -m "docs: describe runtime logging operations"
```

### Task 8: Run the full verification suite and inspect the final diff

**Files:**
- Test: all files under `tests/`
- Inspect: `predmarket/`, `docs/OPERATIONS.md`, and the plan/spec documents

- [ ] **Step 1: Run formatting/whitespace checks available in the repository**

Run:

```bash
git diff --check main...HEAD
```

Expected: no output and exit code `0`.

- [ ] **Step 2: Run the complete test suite under a supported Python environment**

Run:

```bash
pytest -q
```

Expected: all tests pass. The current worktree environment has Python 3.9.6 and no `pytest`; install/use a Python `>=3.11` environment with the project test extras before running this step.

- [ ] **Step 3: Verify the CLI stream contract manually**

Run:

```bash
predmarket --config config/default.yaml run --log-level DEBUG >runtime.stdout 2>runtime.stderr
```

Stop the long-running process with `Ctrl-C`, then verify runtime records such as `component_initialized`, sync market counts, `watch_subscribed markets=`, and `signal_transition` are in `runtime.stderr`, while `runtime.stdout` is empty. Remove only these two explicitly created files after inspection.

- [ ] **Step 4: Inspect the final diff and working tree**

Run:

```bash
git diff main...HEAD --stat
git status --short
```

Expected: only the planned production, test, and operations-documentation files are changed; no generated database, log, or cache files are tracked.

- [ ] **Step 5: Commit any final test-only corrections**

```bash
git add predmarket tests docs/OPERATIONS.md
git commit -m "test: verify runtime logging contract"
```
