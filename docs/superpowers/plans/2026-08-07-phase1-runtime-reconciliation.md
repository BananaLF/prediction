# Phase 1 Runtime Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-market catalog control storms with one durable, idempotent generation reconciliation; close stale signals in one catalog-aware pass with truthful reasons; and allow evaluations to survive unrelated order-book updates without permitting stale signal writes.

**Architecture:** A complete catalog transaction writes the catalog and a `CATALOG_RECONCILIATION_READY` outbox record together. Sync publishes one `CATALOG_RECONCILED` queue item per generation and records admission idempotently; a later run republishes a pending record. Watch reloads the committed catalog once, computes its subscription, reconciles all open signals in one read, and rotates recovery. `OrderBookCache` tracks per-token revisions so Watch fences each strategy target by the books it actually consumed while retaining the generation barrier and operation lock around the final check and signal write.

**Tech Stack:** Python 3.14, asyncio, dataclasses, SQLite/aiosqlite, pytest, existing catalog/watch/signal pipeline.

**Global Constraints:** Keep the program read-only with respect to Polymarket; preserve historical database rows and schema compatibility; preserve legacy individual market-change handling for already queued messages; make the minimum root-cause changes; do not add dependencies; do not commit, push, deploy, or modify external systems; edit with `apply_patch`; run each production change only after observing its focused test fail for the intended reason.

---

## Task 1: Add the generation reconciliation message and durable outbox

**Files:**

- Modify: `predmarket/catalog/changes.py`
- Modify: `predmarket/persistence/repositories.py`
- Modify: `tests/unit/catalog/test_changes.py`
- Modify: `tests/unit/persistence/test_writer.py`

- [x] Add failing tests proving `CATALOG_RECONCILED` is critical, permits empty token/event/market identity, and remains non-droppable.
- [x] Add a failing repository test proving `save_complete_catalog()` atomically stores exactly one `CATALOG_RECONCILIATION_READY` record for the generation and that pending reconciliation loading excludes an idempotently admitted record.
- [x] Run the focused tests and confirm failures are caused by the missing type/outbox behavior.

  ```bash
  /Users/lifei/workspace/earn_money_from_prediction/.venv/bin/pytest -q tests/unit/catalog/test_changes.py tests/unit/persistence/test_writer.py
  ```

- [x] Implement the new message validation plus transactional ready-record insert, pending-record reconstruction, and exact-current-watchable-market baseline semantics for aggregate publication markers.
- [x] Re-run the focused tests until green.

## Task 2: Publish O(1) catalog controls and recover pending publication

**Files:**

- Modify: `predmarket/catalog/sync.py`
- Modify: `tests/unit/catalog/test_sync.py`

- [x] Add failing sync tests proving a complete generation publishes exactly one aggregate control regardless of catalog delta count, records one admission marker, and republishes a pending ready record before the next remote fetch.
- [x] Add a cancellation/idempotency test proving retry does not create duplicate ready/admission records.
- [x] Run the focused sync tests and observe the intended failures.

  ```bash
  /Users/lifei/workspace/earn_money_from_prediction/.venv/bin/pytest -q tests/unit/catalog/test_sync.py
  ```

- [x] Build the aggregate marker before persistence, include it in the complete catalog transaction, publish only that marker, and shield its admission marker write. Retain semantic delta calculation only for diagnostics and compatibility.
- [x] Re-run catalog change, repository, and sync tests.

## Task 3: Reconcile open signals once with catalog-aware close reasons

**Files:**

- Modify: `predmarket/signals/manager.py`
- Modify: `predmarket/app.py`
- Modify: `predmarket/watch/task.py`
- Modify: `tests/unit/signals/test_manager.py`
- Modify: `tests/unit/watch/test_task.py`
- Modify: `tests/integration/test_watch_recovery.py`

- [x] Add failing manager tests with multiple open signals proving one catalog read classifies `EVENT_SETTLED`, `MARKET_CLOSED`, and active-but-unsubscribed `ORDERBOOK_INVALID` correctly and leaves currently watchable signals open.
- [x] Add failing Watch tests proving one `CATALOG_RECONCILED` message reloads the catalog once, invokes one bulk reconciliation, and does not issue per-market close calls.
- [x] Run the focused tests and observe the missing bulk API/handler failures.

  ```bash
  /Users/lifei/workspace/earn_money_from_prediction/.venv/bin/pytest -q tests/unit/signals/test_manager.py tests/unit/watch/test_task.py tests/integration/test_watch_recovery.py
  ```

- [x] Implement one grouped open-signal/catalog read, deterministic reason precedence, per-signal CAS closure, router forwarding, and aggregate Watch handling. Preserve legacy individual control handling.
- [x] Re-run the focused manager and Watch suites.

## Task 4: Fence evaluations by their token dependencies

**Files:**

- Modify: `predmarket/watch/cache.py`
- Modify: `predmarket/watch/task.py`
- Modify: `tests/unit/watch/test_cache.py`
- Modify: `tests/unit/watch/test_task.py`

- [x] Add failing cache tests proving snapshots initialize per-token revisions, single/multi-token mutations advance only changed tokens, and invalidation/generation changes fail closed.
- [x] Add failing Watch concurrency tests proving an unrelated token update does not abort a target, while a dependency update before or during strategy evaluation prevents persistence; retain the operation-lock final-check guarantee.
- [x] Run focused cache and Watch tests and observe failures against the global revision fence.

  ```bash
  /Users/lifei/workspace/earn_money_from_prediction/.venv/bin/pytest -q tests/unit/watch/test_cache.py tests/unit/watch/test_task.py
  ```

- [x] Add immutable token-revision snapshots and replace global-revision checks with generation plus target-dependency checks at context, strategy, lock-acquisition, and post-apply boundaries.
- [x] Re-run focused tests.

## Task 5: Add truthful bounded queue telemetry and verify the phase

**Files:**

- Modify: `predmarket/catalog/changes.py`
- Modify: `predmarket/app.py`
- Modify: `tests/unit/catalog/test_changes.py`
- Modify: `tests/integration/test_app_pipeline.py`
- Modify: `docs/runtime-investigation-2026-08-04.md`

- [x] Add failing tests for actual overflow detection time, high-water mark/cumulative action counters, and at-most-one report per aggregation interval.
- [x] Implement injected/default operational clock telemetry and bounded reporting without changing admission correctness.
- [x] Run all touched suites, then the full test suite.

  ```bash
  /Users/lifei/workspace/earn_money_from_prediction/.venv/bin/pytest -q tests/unit/catalog tests/unit/persistence tests/unit/signals tests/unit/watch tests/integration/test_app_pipeline.py tests/integration/test_watch_recovery.py
  /Users/lifei/workspace/earn_money_from_prediction/.venv/bin/pytest -q
  ```

- [x] Run the program for 30 minutes with the existing safe observer configuration, then compare against the 2026-08-07 baseline: queue overflows `7,316`, WAL peak about `716 MB`, 200 generated/100 targets/0 signals, best return `-3.943%` versus required `+0.75%`.
- [ ] Verify: one catalog control and complete ready/admission audit per generation; zero queue overflows in the comparison run; queue high-water below capacity; no stale signal writes; materially smaller WAL/system-event growth; no new doctor findings. **Partial:** all checks passed except WAL peak, which remained about 727 MiB; see the runtime investigation for evidence and next-phase boundary.
- [x] Update the runtime investigation with exact commands, measurements, and remaining risks. Inspect `git diff --check` and `git status`; do not commit.
