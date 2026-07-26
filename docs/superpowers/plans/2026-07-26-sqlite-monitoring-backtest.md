# SQLite 监控与回测采集 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 SQLite 成为线上监控的唯一事实来源，并先把 `watch` 的持续采集落地，再把 `scan-once` 的持续采集落地。

**Architecture:** 在现有 `predmarket/storage.py` 上扩展运行元数据、事件和候选的持久化接口；先把 `watch` 的运行事实持续写库，再把 `scan-once` 的结果写入同一套 SQLite 事实层。回测直读 SQLite 拆入后续独立计划，按天导出仅作为后续派生，不作为主路径。

**Tech Stack:** Python 3.10+, `aiosqlite`, `sqlite3`, `pytest`, `pytest-asyncio`, 现有 `predmarket` 模块。

## Global Constraints

- SQLite 是唯一事实来源。所有后续分析、回放、报表和回测都以数据库中的记录为准。
- 采集层只记录事实，不做二次推断。它可以附带标准化字段，但不替代分析层。
- 回测能力拆入后续独立计划，本次不实现。
- 线上采集与离线读取共享同一套字段定义、同一套时间字段、同一套机会标识。
- 采集结果必须可幂等写入，重复运行不会制造逻辑重复记录。
- 本阶段不引入独立按天数据文件作为主路径，不要求另建消息队列或单独采集服务。

---

### Task 1: Extend SQLite schema and storage primitives for watch records

**Files:**
- Modify: `predmarket/storage.py`
- Modify: `tests/unit/test_storage.py`

**Interfaces:**
- Consumes: existing `Store`, `_immediate_transaction`, schema migration helpers, `save_*` patterns
- Produces: watch run/event/metric persistence methods that later CLI tasks can call

- [ ] **Step 1: Write the failing storage tests**

Add tests that prove the store can persist and query watch facts without duplicating records:

```python
@pytest.mark.asyncio
async def test_watch_run_and_event_records_are_idempotent(tmp_path):
    store = await Store.open(tmp_path / "db.sqlite3")
    run_id = await store.save_watch_run({
        "run_id": "watch:run-1",
        "started_at_ms": 1000,
        "finished_at_ms": 1100,
        "status": "SUCCEEDED",
        "exit_reason": None,
        "params_json": {"max_connections": 3},
    })
    event_id = await store.save_watch_event({
        "run_id": run_id,
        "sequence": 1,
        "event_type": "book",
        "token_id": "yes",
        "condition_id": "condition-1",
        "canonical_json": "{\"event_type\":\"book\"}",
        "raw_json": "{\"event_type\":\"book\"}",
        "received_wall_ms": 1001,
        "received_monotonic": 2.0,
        "exchange_ts_ms": 1000,
        "persisted_at_ms": 1002,
    })
    assert await store.save_watch_event({...same payload...}) == event_id
    assert await store.list_watch_events(run_id) == [...]
```

Add a metrics snapshot test:

```python
@pytest.mark.asyncio
async def test_watch_metrics_are_persisted_and_listed(tmp_path):
    store = await Store.open(tmp_path / "db.sqlite3")
    await store.save_watch_run({...})
    await store.save_watch_metrics("watch:run-1", {...})
    rows = await store.list_watch_runs(limit=10)
    assert rows[0]["status"] == "SUCCEEDED"
```

- [ ] **Step 2: Run the targeted tests and confirm they fail**

Run:

```bash
PYTHONPATH=. .venv-test-ws/bin/python -m pytest -q tests/unit/test_storage.py -k 'watch_run or watch_event or watch_metrics'
```

Expected: fail because the new methods do not exist yet.

- [ ] **Step 3: Implement the minimal schema and methods**

Add migration/schema support for the watch fact tables and implement the smallest set of store methods needed by the tests:

```python
async def save_watch_run(self, record: Mapping[str, object]) -> str: ...
async def save_watch_event(self, record: Mapping[str, object]) -> str: ...
async def save_watch_metrics(self, run_id: str, metrics: Mapping[str, object]) -> None: ...
async def list_watch_runs(self, limit: int = 100) -> list[dict[str, object]]: ...
async def list_watch_events(self, run_id: str) -> list[dict[str, object]]: ...
```

Use stable identifiers or content hashes to make the writes idempotent. Keep the schema additive; do not break existing evidence tables.

- [ ] **Step 4: Run the targeted tests and confirm they pass**

Run the same `pytest -k` command and confirm the new storage tests pass.

- [ ] **Step 5: Commit**

```bash
git add predmarket/storage.py tests/unit/test_storage.py
git commit -m "feat: add sqlite watch capture primitives"
```

---

### Task 2: Persist `watch` runtime facts and metrics

**Files:**
- Modify: `predmarket/commands.py`
- Modify: `predmarket/storage.py`
- Modify: `tests/integration/test_ws_recovery.py`
- Modify: `tests/integration/test_engine.py` if the existing watch command tests live there

**Interfaces:**
- Consumes: `MarketWebSocket.metrics()`, `serve_connection()`, `_watch_runtime()`, new store methods from Task 1
- Produces: persisted `watch_runs`, `watch_events`, and `watch_metrics`

- [ ] **Step 1: Write the failing integration test**

Add a test that runs the watch runtime with a fake connector and then asserts the database contains a completed watch run and its metrics:

```python
@pytest.mark.asyncio
async def test_watch_runtime_persists_run_and_metrics(tmp_path):
    store = await Store.open(tmp_path / "db.sqlite3")
    connection = Connection([fixture("ws_book_yes.json"), fixture("ws_book_no.json")])
    async def connect(_url):
        return connection
    async def sleep(_delay):
        pass
    await _watch_runtime(
        args,
        runtime_factory=runtime_factory,
        websocket_connector=connect,
        sleeper=sleep,
        store=store,
    )
    runs = await store.list_watch_runs(limit=10)
    assert runs[0]["status"] == "SUCCEEDED"
```

Also assert that at least one event row was persisted and that the stored metrics reflect received/heartbeat/reconnect counts.

- [ ] **Step 2: Run the targeted test and confirm it fails**

Run:

```bash
PYTHONPATH=. .venv-test-ws/bin/python -m pytest -q tests/integration/test_ws_recovery.py -k 'watch_runtime_persists_run_and_metrics'
```

Expected: fail because `_watch_runtime` does not yet write the new run/event rows.

- [ ] **Step 3: Implement the minimal watch persistence path**

In `_watch_runtime`, create one run record at the start, persist the final metrics snapshot at the end, and write a concise event row for each accepted market-domain event or connection boundary that matters for later inspection.

Keep the hot path non-blocking as much as practical:

```python
run_id = await store.save_watch_run({...})
...
await store.save_watch_event({...})
...
await store.save_watch_metrics(run_id, metrics)
```

Do not change the scanner’s public behavior; only add durable recording.

- [ ] **Step 4: Run the targeted test and confirm it passes**

Run the same `pytest -k` command and confirm the new watch persistence assertions pass.

- [ ] **Step 5: Commit**

```bash
git add predmarket/commands.py predmarket/storage.py tests/integration/test_ws_recovery.py tests/integration/test_engine.py
git commit -m "feat: persist watch runs and metrics"
```

---

### Task 3: Persist `scan-once` runs and candidates

**Files:**
- Modify: `predmarket/commands.py`
- Modify: `predmarket/storage.py`
- Modify: `tests/integration/test_engine.py`

**Interfaces:**
- Consumes: existing scan-once dispatch path and `StructuralArbitrageEngine` outputs
- Produces: `scan_runs`, `scan_candidates`, and replay-query records that later reporting tasks can consume

- [ ] **Step 1: Write the failing scan persistence test**

Add a test that runs `scan-once` against a fake market source and asserts a scan run plus at least one candidate row is stored:

```python
@pytest.mark.asyncio
async def test_scan_once_persists_run_and_candidates(tmp_path):
    store = await Store.open(tmp_path / "db.sqlite3")
    result = await dispatch(args_for_scan_once, store=store, runtime_factory=runtime_factory)
    runs = await store.list_scan_runs(limit=10)
    candidates = await store.list_scan_candidates(limit=10)
    assert runs
    assert candidates
```

- [ ] **Step 2: Run the targeted test and confirm it fails**

Run:

```bash
PYTHONPATH=. .venv-test-ws/bin/python -m pytest -q tests/integration/test_engine.py -k 'scan_once_persists_run_and_candidates'
```

Expected: fail because the scan path does not yet write the new rows.

- [ ] **Step 3: Implement minimal scan persistence**

Record scan run metadata at command start and persist each qualifying candidate with its canonical evidence summary and risk status. Reuse the existing opportunity/evidence model rather than inventing a second schema for the same concept.

- [ ] **Step 4: Run the targeted test and confirm it passes**

Run the same `pytest -k` command and confirm persistence works.

- [ ] **Step 5: Commit**

```bash
git add predmarket/commands.py predmarket/storage.py tests/integration/test_engine.py
git commit -m "feat: persist scan runs and candidates"
```

---

### Task 4: Document and verify the new watch-first flow

**Files:**
- Modify: `README.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `docs/VERIFICATION.md`
- Modify: `docs/SOAK-TEST.md` if the operational guidance needs to mention SQLite-backed monitoring

**Interfaces:**
- Consumes: the new watch/scan persistence behavior from Tasks 1-3
- Produces: user-facing documentation and end-to-end verification evidence

- [ ] **Step 1: Update the user-facing docs**

Add short, concrete instructions showing:

```bash
.venv/bin/predmarket watch --max-connections 3 --max-events 500
.venv/bin/predmarket scan-once --limit 100
```

Explain that `watch` now writes runtime facts into SQLite and that `scan-once` follows after it. Keep backtest documentation out of this plan.

- [ ] **Step 2: Run the narrowest meaningful verification**

Run the relevant tests for the touched paths, then run the full suite if the change is still small enough to justify it:

```bash
PYTHONPATH=. .venv-test-ws/bin/python -m pytest -q tests/unit/test_storage.py tests/integration/test_ws_recovery.py tests/integration/test_engine.py
```

If the suite is still fast enough, run the full test suite once before completion.

- [ ] **Step 3: Inspect for residual risks**

Check that:

```text
- watch writes run/metric facts exactly once
- scan-once writes candidate facts exactly once
- no new code path depends on a daily export file
```

- [ ] **Step 4: Commit**

```bash
git add README.md docs/OPERATIONS.md docs/VERIFICATION.md docs/SOAK-TEST.md
git commit -m "docs: describe sqlite-backed monitoring flow"
```

---

## Self-Review

- Spec coverage: the plan covers SQLite schema/storage, watch persistence, scan persistence, and docs/verification.
- Placeholder scan: no `TODO`/`TBD` placeholders remain in the plan body.
- Type consistency: the plan uses `Store.save_watch_run`, `Store.save_watch_event`, `Store.save_watch_metrics`, `Store.list_watch_runs`, `Store.list_watch_events`, `Store.save_scan_run`, and `Store.save_scan_candidate` consistently across tasks.
- Scope check: backtest is explicitly excluded and will be handled in a later plan.

