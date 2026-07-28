# SQLite Schema 精简实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `predmarket` 的 SQLite schema 从 39 张项目表精简到 30 张，并保持新数据库上的扫描、回放、报告、通知审计和监控行为不变。

**Architecture:** `evidence_bundles.canonical_json` 继续保存不可变完整证据；把相同生命周期的运行、机会、风险、关系、盘口和 watch 汇总合并存储。Schema 版本提升到 7，不迁移旧数据，旧版本数据库在任何修改前明确拒绝。

**Tech Stack:** Python 3.10+、SQLite/WAL、aiosqlite、pytest、pytest-asyncio。

## 全局约束

- 目标 schema 版本必须是 7，并且空库必须恰好创建规格中列出的 30 张项目表。
- 不迁移 schema v6 数据，不保留旧库的 `report` 或 `replay` 兼容。
- 应用不得自动删除、清空、覆盖或修改旧版数据库。
- 不修改扫描策略、经济模型、风险阈值、公开网络端点、CLI 输出结构或通知投递语义。
- 每份 evidence bundle 继续在一个事务内保存；任何子表失败必须整体回滚。
- 保留当前脏工作树中的所有无关用户改动，尤其是 `predmarket/storage.py`、CLI、测试和文档中的既有修改。
- 未经用户明确授权，不执行 Git commit、push 或部署；因此本计划以差异复核代替每个任务后的 commit。
- 当前 `data/predmarket.sqlite3` 只能在全部实现和最新验证成功后删除。

---

### Task 0：准备可复现的 Python 3.10+ 测试环境

**Files:**
- Read: `pyproject.toml`
- Read: `uv.lock`
- Create locally if absent: `.venv/`

**Interfaces:**
- Consumes: `pyproject.toml` 的 `requires-python = ">=3.10"` 和 `test` extra
- Produces: `.venv/bin/python`，供后续所有验证命令使用

- [ ] **Step 1：确认 uv 和项目 Python 约束**

Run:

```console
uv --version
rg -n '^requires-python = \">=3\\.10\"$|^test = \\[' pyproject.toml
```

Expected: `uv` 可用，并且两项约束都能匹配。

- [ ] **Step 2：按锁文件创建或同步测试环境**

Run:

```console
UV_CACHE_DIR=/tmp/predmarket-uv-cache \
  uv sync --extra test --locked --no-install-project
```

Expected: 退出码 0，不改写 `uv.lock`，生成可用的 `.venv`。使用
`--no-install-project` 是因为当前 setuptools 自动发现会把 `rules`、`config`
和 `predmarket` 识别为多个顶层包；测试通过 `PYTHONPATH=.` 加载工作树源码。

- [ ] **Step 3：确认解释器与测试基线**

Run:

```console
.venv/bin/python --version
PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_storage.py -q
```

Expected: Python 版本至少为 3.10；修改前存储测试基线通过。若基线失败，先记录并区分用户工作树中的既有失败，不进入 schema 修改。

---

### Task 1：建立 schema v7 和旧库失败关闭

**Files:**
- Modify: `predmarket/storage.py:23-24`
- Modify: `predmarket/storage.py:1016-1438`
- Modify: `tests/unit/test_storage.py:284-335`

**Interfaces:**
- Consumes: `OpportunityStore.open() -> OpportunityStore`
- Produces: `SCHEMA_VERSION = 7`；空库 30 表；任何 `0 < user_version < 7` 的数据库在修改前抛出 `RuntimeError`

- [ ] **Step 1：写出精确的目标表清单测试**

在 `tests/unit/test_storage.py` 中把原来的子集断言改成精确断言：

```python
EXPECTED_V7_TABLES = {
    "schema_migrations", "evidence_bundles", "evaluations", "events",
    "markets", "tokens", "fee_schedules", "relation_evidence",
    "book_snapshots", "levels", "legs", "actions", "latency_metrics",
    "notification_claims", "notification_attempts", "notification_events",
    "catalog_snapshots", "catalog_sync_runs", "catalog_markets",
    "catalog_events", "catalog_tokens", "catalog_relation_candidates",
    "current_catalog_markets", "current_catalog_tokens",
    "current_catalog_events", "watch_runs", "watch_events", "scan_runs",
    "scan_candidates", "research_observations",
}

@pytest.mark.asyncio
async def test_schema_v7_has_exact_project_tables(tmp_path):
    path = tmp_path / "evidence.sqlite3"
    async with OpportunityStore(path) as store:
        tables = {
            row[0]
            for row in await store._connection.execute_fetchall(
                """SELECT name FROM sqlite_schema
                   WHERE type='table' AND name NOT LIKE 'sqlite_%'"""
            )
        }
        version = await store._connection.execute_fetchall("PRAGMA user_version")
    assert tables == EXPECTED_V7_TABLES
    assert version == [(7,)]
```

- [ ] **Step 2：写 schema v6 不变性失败测试**

```python
@pytest.mark.asyncio
async def test_schema_v6_is_rejected_without_mutation(tmp_path):
    path = tmp_path / "legacy-v6.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE legacy_payload(value TEXT)")
    connection.execute("INSERT INTO legacy_payload VALUES ('untouched')")
    connection.execute("PRAGMA user_version = 6")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="schema 6.*unsupported.*new database"):
        await OpportunityStore(path).open()

    check = sqlite3.connect(path)
    assert check.execute("PRAGMA user_version").fetchone() == (6,)
    assert check.execute("SELECT value FROM legacy_payload").fetchall() == [
        ("untouched",)
    ]
    assert check.execute(
        "SELECT name FROM sqlite_schema WHERE name='evidence_bundles'"
    ).fetchall() == []
    check.close()
```

- [ ] **Step 3：运行测试并确认因旧 schema 实现失败**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py::test_schema_v7_has_exact_project_tables \
  tests/unit/test_storage.py::test_schema_v6_is_rejected_without_mutation -v
```

Expected: v7 表清单断言失败；v6 仍被现有兼容逻辑修改或接受。

- [ ] **Step 4：替换 schema 定义**

在 `predmarket/storage.py` 设置：

```python
SCHEMA_VERSION = 7
```

把 `_SCHEMA` 中被合并的表替换为以下核心结构；其余保留表沿用原列定义：

```sql
CREATE TABLE IF NOT EXISTS evaluations (
    bundle_id TEXT PRIMARY KEY REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    opportunity_id TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    run_status TEXT NOT NULL,
    opportunity_status TEXT NOT NULL,
    pipeline_reason TEXT NOT NULL,
    run_json TEXT NOT NULL,
    opportunity_json TEXT NOT NULL,
    risk_json TEXT NOT NULL,
    notification_intent_json TEXT NOT NULL,
    UNIQUE(opportunity_id, bundle_id)
);
CREATE TABLE IF NOT EXISTS relation_evidence (
    bundle_id TEXT PRIMARY KEY REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    relation_set_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    canonical_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS book_snapshots (
    bundle_id TEXT NOT NULL REFERENCES evidence_bundles(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    epoch_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY(bundle_id, id),
    FOREIGN KEY(bundle_id, token_id) REFERENCES tokens(bundle_id, id)
);
```

同步修改外键：

```sql
FOREIGN KEY (bundle_id, snapshot_id)
    REFERENCES book_snapshots(bundle_id, id)
```

`legs`、`actions`、`latency_metrics` 的 `opportunity_id` 保留为查询列，但外键改为仅通过 `bundle_id REFERENCES evaluations(bundle_id) ON DELETE CASCADE` 绑定，因为 `evaluations` 的主键是 bundle ID。

从 `_SCHEMA` 删除：

```text
runs, opportunities, risk_assessments,
relation_sets, relations, relation_states, relation_payoffs,
book_epochs, snapshots, notifications,
catalog_diagnostics, watch_metrics
```

- [ ] **Step 5：把初始化改为只接受空库或 v7**

在任何 `_SCHEMA` DDL 之前执行：

```python
if existing_version not in {0, SCHEMA_VERSION}:
    raise RuntimeError(
        f"database schema {existing_version} is unsupported by schema "
        f"{SCHEMA_VERSION}; use a new database path"
    )
```

删除 v3、v4、v5、v6 的迁移分支。保留未来版本失败关闭，但统一走上述错误。

- [ ] **Step 6：运行 schema 测试**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py::test_schema_v7_has_exact_project_tables \
  tests/unit/test_storage.py::test_schema_v6_is_rejected_without_mutation \
  tests/unit/test_storage.py::test_schema_wal_foreign_keys_and_all_required_tables -v
```

Expected: PASS，且没有生成第 31 张项目表。

- [ ] **Step 7：复核本任务差异**

Run:

```console
git diff --check
git diff -- predmarket/storage.py tests/unit/test_storage.py
```

Expected: 无空白错误；差异只包含 schema v7 初始化和相应测试，不提交。

---

### Task 2：将证据写入合并表

**Files:**
- Modify: `predmarket/storage.py:1462-1800`
- Modify: `tests/unit/test_storage.py:335-520`

**Interfaces:**
- Consumes: `EvidenceBundle.data` 现有 v2 证据结构
- Produces: `OpportunityStore.save(bundle: EvidenceBundle) -> bool`，在一个事务内写入 schema v7

- [ ] **Step 1：增加合并表写入断言**

在现有 round-trip 测试后增加：

```python
@pytest.mark.asyncio
async def test_save_writes_merged_schema_rows():
    async with OpportunityStore(":memory:") as store:
        assert await store.save(EvidenceBundle.from_mapping(bundle())) is True
        evaluation = (
            await store._connection.execute_fetchall(
                """SELECT run_id, opportunity_id, run_status,
                          opportunity_status, run_json, opportunity_json,
                          risk_json, notification_intent_json
                   FROM evaluations"""
            )
        )[0]
        relation = (
            await store._connection.execute_fetchall(
                "SELECT relation_set_id, canonical_json FROM relation_evidence"
            )
        )[0]
        books = await store._connection.execute_fetchall(
            """SELECT id, epoch_id, token_id, epoch_json, snapshot_json
               FROM book_snapshots ORDER BY id"""
        )
    assert evaluation[:4] == (
        "run-1", "opp-1", "COMPLETED", "SNAPSHOT_EXECUTABLE"
    )
    assert json.loads(evaluation[4])["id"] == "run-1"
    assert json.loads(evaluation[5])["id"] == "opp-1"
    assert json.loads(evaluation[6])["status"] == "SNAPSHOT_EXECUTABLE"
    assert isinstance(json.loads(evaluation[7]), list)
    assert relation[0] == "set-1"
    assert len(books) == 2
```

- [ ] **Step 2：增加事务回滚测试**

```python
@pytest.mark.asyncio
async def test_merged_bundle_insert_rolls_back_on_child_failure():
    async with OpportunityStore(":memory:") as store:
        await store._connection.executescript(
            """CREATE TRIGGER force_level_failure
               BEFORE INSERT ON levels
               BEGIN
                 SELECT RAISE(ABORT, 'forced level failure');
               END;"""
        )
        with pytest.raises(sqlite3.IntegrityError, match="forced level failure"):
            await store.save(EvidenceBundle.from_mapping(bundle()))

        assert await store._connection.execute_fetchall(
            "SELECT id FROM evidence_bundles"
        ) == []
        assert await store._connection.execute_fetchall(
            "SELECT bundle_id FROM evaluations"
        ) == []
```

- [ ] **Step 3：运行新增测试并确认旧写入路径失败**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py::test_save_writes_merged_schema_rows \
  tests/unit/test_storage.py::test_merged_bundle_insert_rolls_back_on_child_failure -v
```

Expected: FAIL，旧 `_insert_bundle` 仍写已删除的表。

- [ ] **Step 4：重写 `_insert_bundle` 的合并写入**

使用现有 `_json()`，不要改变 evidence v2 的公开结构：

```python
run = data["run"]
opportunity = data["opportunity"]
risk = data["risk"]
await connection.execute(
    """INSERT INTO evaluations
       (bundle_id, run_id, opportunity_id, started_at_ms, run_status,
        opportunity_status, pipeline_reason, run_json, opportunity_json,
        risk_json, notification_intent_json)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
    (
        bundle_id,
        run["id"],
        opportunity["id"],
        run["started_at_ms"],
        run["status"],
        opportunity["status"],
        str(data["producer"]["metadata"].get("pipeline_reason", "unknown")),
        _json(run),
        _json(opportunity),
        _json(risk),
        _json(data["notifications"]),
    ),
)
```

关系整体写入：

```python
relation = data["relation"]
relation_set = relation["set"]
await connection.execute(
    """INSERT INTO relation_evidence
       (bundle_id, relation_set_id, version, canonical_json)
       VALUES (?, ?, ?, ?)""",
    (
        bundle_id,
        relation_set["id"],
        relation_set["version"],
        _json(relation),
    ),
)
```

盘口合并写入：

```python
for book in data["books"]:
    epoch, snapshot = book["epoch"], book["snapshot"]
    await connection.execute(
        """INSERT INTO book_snapshots
           (bundle_id, id, epoch_id, token_id, epoch_json, snapshot_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            bundle_id, snapshot["id"], epoch["id"], epoch["token_id"],
            _json(epoch), _json(snapshot),
        ),
    )
```

继续逐行写 `levels`、`legs`、`actions`、`latency_metrics`；删除对旧合并表和 `notifications` 的 INSERT。

- [ ] **Step 5：运行持久化测试**

Run:

```console
.venv/bin/python -m pytest tests/unit/test_storage.py -k \
  'save or round_trip or duplicate or rollback or notification' -v
```

Expected: PASS；重复 bundle 仍返回 `False`，冲突 bundle 仍抛出 `EvidenceConflictError`。

- [ ] **Step 6：复核本任务差异**

Run:

```console
git diff --check
git diff -- predmarket/storage.py tests/unit/test_storage.py
```

Expected: `_insert_bundle` 不再引用已删除表，不提交。

---

### Task 3：适配回放、完整性验证、列表和报告

**Files:**
- Modify: `predmarket/storage.py:1801-2103`
- Modify: `predmarket/storage.py:2797-2950`
- Modify: `tests/unit/test_storage.py:520-780`
- Modify: `tests/integration/test_cli.py:820-855`

**Interfaces:**
- Consumes: schema v7 `evaluations`、`relation_evidence`、`book_snapshots`
- Produces: 保持现有签名的 `replay()`、`replay_opportunity()`、`validate_opportunity()`、`list_opportunities()`、`list_runs()` 和 `report()`

- [ ] **Step 1：为机会选择和报告写失败测试**

```python
@pytest.mark.asyncio
async def test_v7_latest_opportunity_and_report_use_evaluations(tmp_path):
    path = tmp_path / "report.sqlite3"
    first = bundle("bundle-old", "opp-shared")
    first["run"]["id"] = "run-old"
    first["run"]["started_at_ms"] = 1000
    second = bundle("bundle-new", "opp-shared")
    second["run"]["id"] = "run-new"
    second["run"]["started_at_ms"] = 2000

    async with OpportunityStore(path) as store:
        await store.save(EvidenceBundle.from_mapping(first))
        await store.save(EvidenceBundle.from_mapping(second))
        replayed = await store.replay_opportunity("opp-shared")
        report = await store.report(limit=10)
        opportunities = await store.list_opportunities(limit=10)
        runs = await store.list_runs(limit=10)

    assert replayed.evidence.id == "bundle-new"
    assert report["total"] == 2
    assert opportunities[0][0] == "opp-shared"
    assert {row[0] for row in runs} == {"run-old", "run-new"}
```

- [ ] **Step 2：为规范化表损坏写失败测试**

```python
@pytest.mark.asyncio
async def test_validate_detects_corrupt_merged_evaluation():
    async with OpportunityStore(":memory:") as store:
        await store.save(EvidenceBundle.from_mapping(bundle()))
        await store._connection.execute(
            "UPDATE evaluations SET risk_json='{}' WHERE bundle_id='bundle-1'"
        )
        await store._connection.commit()
        result = await store.validate_opportunity("opp-1")
    assert result["status"] == "fail"
    assert any(
        error["code"] == "REPLAY_MISMATCH" for error in result["errors"]
    )
```

- [ ] **Step 3：运行测试并确认旧查询失败**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py::test_v7_latest_opportunity_and_report_use_evaluations \
  tests/unit/test_storage.py::test_validate_detects_corrupt_merged_evaluation -v
```

Expected: FAIL，查询仍引用 `opportunities`、`runs` 或 `risk_assessments`。

- [ ] **Step 4：把机会选择和列表改为 `evaluations`**

```python
rows = await self._require_connection().execute_fetchall(
    """SELECT bundle_id FROM evaluations
       WHERE opportunity_id = ?
       ORDER BY started_at_ms DESC, rowid DESC LIMIT 1""",
    (opportunity_id,),
)
```

列表保持公开返回类型：

```sql
SELECT opportunity_id, opportunity_status, bundle_id
FROM evaluations
WHERE (? IS NULL OR bundle_id > ?)
ORDER BY bundle_id LIMIT ?
```

```sql
SELECT run_id, run_status
FROM evaluations
ORDER BY bundle_id LIMIT ?
```

- [ ] **Step 5：重建规范化证据并与 canonical JSON 比较**

在 `validate_opportunity` 的内部 replay-data 查询中：

- 从 `evaluations.run_json`、`opportunity_json`、`risk_json` 和
  `notification_intent_json` 恢复对应字段；
- 从 `relation_evidence.canonical_json` 恢复完整 `relation`；
- 从 `book_snapshots.epoch_json`、`snapshot_json` 和关联 `levels` 恢复
  `books`；
- 保持 Decimal 恢复使用 `_restore_schema_decimals()`；
- 最终继续使用 `EvidenceBundle.from_mapping()` 和 canonical JSON 字节比较。

明确查询：

```python
evaluation_rows = await connection.execute_fetchall(
    """SELECT run_json, opportunity_json, risk_json, notification_intent_json
       FROM evaluations WHERE bundle_id = ?""",
    (bundle_id,),
)
relation_rows = await connection.execute_fetchall(
    "SELECT canonical_json FROM relation_evidence WHERE bundle_id = ?",
    (bundle_id,),
)
book_rows = await connection.execute_fetchall(
    """SELECT id, epoch_json, snapshot_json
       FROM book_snapshots WHERE bundle_id = ? ORDER BY id""",
    (bundle_id,),
)
```

- [ ] **Step 6：把 `report` 改为读取 `evaluations`**

主查询改为：

```sql
SELECT e.bundle_id, e.opportunity_status, e.opportunity_json,
       e.risk_json, b.canonical_json
FROM evaluations e
JOIN evidence_bundles b ON b.id = e.bundle_id
ORDER BY e.started_at_ms DESC, e.rowid DESC
LIMIT ?
```

保持 `total`、`truncated`、`by_status`、`by_reason`、
`by_pipeline_reason`、`by_path`、`executable_economics`、`latency_ms` 和通知
统计的公开 JSON 结构不变。

- [ ] **Step 7：运行存储与 CLI 报告测试**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py \
  tests/integration/test_cli.py -k 'report or replay or validate or opportunity' -v
```

Expected: PASS。

- [ ] **Step 8：复核本任务差异**

Run:

```console
rg -n '\\b(runs|opportunities|risk_assessments|relation_sets|relation_states|relation_payoffs|book_epochs|snapshots|notifications)\\b' predmarket/storage.py
git diff --check
```

Expected: 命中只允许出现在 evidence JSON 字段名、错误消息或兼容接口语义中，不允许出现在 SQL 表引用中。

---

### Task 4：把 watch 指标合入 `watch_runs`

**Files:**
- Modify: `predmarket/storage.py:2466-2697`
- Modify: `tests/unit/test_storage.py` 中 watch run/metrics 测试
- Modify: `tests/integration/test_cli.py` 中 watch/report 测试

**Interfaces:**
- Consumes: `save_watch_run(record)` 和 `save_watch_metrics(run_id, started_at_ms_or_metrics, metrics=None)`
- Produces: 两个接口都写 `watch_runs`；`list_watch_metrics()` 从 `watch_runs` 返回最新最终指标

- [ ] **Step 1：写单表合并测试**

```python
@pytest.mark.asyncio
async def test_watch_metrics_update_existing_watch_run():
    async with OpportunityStore(":memory:") as store:
        await store.save_watch_run({
            "run_id": "watch:1",
            "started_at_ms": 1000,
            "finished_at_ms": None,
            "status": "RUNNING",
            "exit_reason": None,
            "params_json": {"max_events": 3},
        })
        await store.save_watch_metrics("watch:1", 1000, {"received": 3})
        rows = await store._connection.execute_fetchall(
            "SELECT id, canonical_json FROM watch_runs"
        )
        metrics = await store.list_watch_metrics(limit=1)
    assert len(rows) == 1
    assert json.loads(rows[0][1])["metrics"] == {"received": 3}
    assert metrics[0]["received"] == 3
```

- [ ] **Step 2：运行测试并确认 `watch_metrics` 表依赖导致失败**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py::test_watch_metrics_update_existing_watch_run -v
```

Expected: FAIL，旧实现尝试写不存在的 `watch_metrics`。

- [ ] **Step 3：实现 watch run JSON 合并更新**

`save_watch_metrics()` 在写锁和事务内读取已有 `watch_runs.canonical_json`，
保留运行字段并加入 `metrics`：

```python
rows = await connection.execute_fetchall(
    "SELECT started_at_ms, canonical_json FROM watch_runs WHERE id = ?",
    (run_id,),
)
base = json.loads(rows[0][1]) if rows else {
    "run_id": run_id,
    "started_at_ms": started_at_ms,
    "finished_at_ms": None,
    "status": "UNKNOWN",
    "exit_reason": None,
    "params_json": {},
}
base["metrics"] = dict(metrics_map)
canonical = _json(base)
```

随后对 `watch_runs` 执行 upsert。`list_watch_metrics()` 查询
`watch_runs`，只返回含 `metrics` 的记录，并把 metrics 字段展开为现有返回结构：

```python
return [
    {"id": str(run_id), "started_at_ms": int(started), **data["metrics"]}
    for run_id, started, canonical in rows
    if "metrics" in (data := json.loads(canonical))
]
```

- [ ] **Step 4：运行 watch 相关测试**

Run:

```console
.venv/bin/python -m pytest \
  tests/unit/test_storage.py -k watch \
  tests/integration/test_cli.py -k watch -v
```

Expected: PASS；报告的 `ws_metrics` 结构不变。

- [ ] **Step 5：复核本任务差异**

Run:

```console
rg -n 'watch_metrics' predmarket tests
git diff --check
```

Expected: 仅允许兼容方法名 `save_watch_metrics`、`list_watch_metrics` 和测试名称存在，不得出现 `FROM watch_metrics` 或 `INTO watch_metrics`。

---

### Task 5：把目录诊断合入目录快照 JSON

**Files:**
- Modify: `predmarket/storage.py:2104-2465`
- Modify: `tests/integration/test_cli.py:176-580`

**Interfaces:**
- Consumes: `save_catalog_snapshot(snapshot: Mapping[str, object]) -> str`
- Produces: 诊断仍通过 `catalog_snapshots.canonical_json` 完整保存和返回，不再写 `catalog_diagnostics`

- [ ] **Step 1：写无诊断子表的回放测试**

```python
@pytest.mark.asyncio
async def test_catalog_diagnostics_round_trip_inside_snapshot_json(tmp_path):
    snapshot = {
        "fetched_at_ms": 10,
        "complete": True,
        "provenance": "exhausted",
        "markets": [],
        "diagnostics": [{"reason": "bad-market", "position": 0}],
        "relation_candidates": [],
    }
    async with OpportunityStore(tmp_path / "catalog.sqlite3") as store:
        snapshot_id = await store.save_catalog_snapshot(snapshot)
        replayed = await store.list_catalog_snapshots(limit=1)
        tables = await store._connection.execute_fetchall(
            """SELECT name FROM sqlite_schema
               WHERE type='table' AND name='catalog_diagnostics'"""
        )
    assert replayed[0]["id"] == snapshot_id
    assert replayed[0]["diagnostics"] == snapshot["diagnostics"]
    assert tables == []
```

- [ ] **Step 2：运行测试并确认旧 INSERT 失败**

Run:

```console
.venv/bin/python -m pytest \
  tests/integration/test_cli.py::test_catalog_diagnostics_round_trip_inside_snapshot_json -v
```

Expected: FAIL，旧保存路径仍写 `catalog_diagnostics`。

- [ ] **Step 3：删除诊断表写入和独立重建逻辑**

在 `save_catalog_snapshot()` 中保留输入 snapshot 的 `diagnostics` 于 canonical
JSON，并删除以下完整写入块：

```python
for position, diagnostic in enumerate(snapshot["diagnostics"]):
    await connection.execute(
        "INSERT INTO catalog_diagnostics VALUES (?, ?, ?)",
        (snapshot_id, position, _json(diagnostic)),
    )
```

`list_catalog_snapshots()` 直接从 `catalog_snapshots.canonical_json` 读取
`diagnostics`，不得再查询诊断子表。

- [ ] **Step 4：运行目录生命周期测试**

Run:

```console
.venv/bin/python -m pytest tests/integration/test_cli.py -k catalog -v
```

Expected: PASS，包括完整同步、部分同步、`MISSING` 状态和 v7 新库目录回放。

- [ ] **Step 5：复核本任务差异**

Run:

```console
rg -n 'catalog_diagnostics' predmarket tests
git diff --check
```

Expected: 不存在生产 SQL 表引用。

---

### Task 6：完成跨命令验证并更新文档

**Files:**
- Modify: `tests/integration/test_cli.py`
- Modify: `README.md`
- Modify: `docs/PROJECT-GUIDE.md`
- Modify: `docs/TUTORIAL.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `docs/VERIFICATION.md`

**Interfaces:**
- Consumes: schema v7 存储接口和现有 CLI
- Produces: 不变的 `scan-once`、`report`、`replay`、`watch` 公共输出；准确的 schema v7 操作文档

- [ ] **Step 1：增加新库端到端 CLI 测试**

在现有离线 fake runtime 基础上新增或扩展测试，完成：

```python
scan_output = await dispatch(scan_args, runtime_factory=FakeRuntime)
assert scan_output["evaluated"] >= 1
report_output = await dispatch(report_args)
assert report_output["total"] >= 1
opportunity_id = scan_output["results"][0]["opportunity_id"]
bundle_id = scan_output["results"][0]["evidence_id"]
latest = await dispatch(replay_opportunity_args)
exact = await dispatch(replay_bundle_args)
assert latest["core_evidence"]["id"] == bundle_id
assert exact["core_evidence"]["id"] == bundle_id
assert latest["core_evidence"]["opportunity"]["id"] == opportunity_id
```

测试最后执行：

```python
connection = sqlite3.connect(database_path)
assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
connection.close()
```

- [ ] **Step 2：运行端到端测试**

Run:

```console
.venv/bin/python -m pytest \
  tests/integration/test_cli.py::test_watch_command_offline_success_reconfirms_with_two_rest_books \
  tests/integration/test_cli.py -k 'scan_once and replay and report' -v
```

Expected: PASS。

- [ ] **Step 3：更新用户文档**

所有文档统一说明：

```text
当前 SQLite schema 版本为 7，共 30 张项目表。
Schema v6 及更旧数据库不迁移；程序会在修改前拒绝打开。
升级时必须停止进程，删除旧数据库及匹配的 WAL/SHM，或把
database_path 指向一个新的空文件。
```

在 `docs/PROJECT-GUIDE.md` 增加 30 表分类；在
`docs/OPERATIONS.md` 给出明确替换流程；在 `docs/TUTORIAL.md` 说明新用户无需
手工建表；在 `docs/VERIFICATION.md` 记录实际测试结果，不复制旧的通过数量。

- [ ] **Step 4：检查文档和 CLI 字段一致性**

Run:

```console
rg -n 'schema (版本为 )?[0-6]|schema version [0-6]|39 张' \
  README.md docs predmarket tests
git diff --check
```

Expected: 旧版本描述只允许出现在“v6 不兼容”上下文和历史设计文档中。

- [ ] **Step 5：运行只读边界与相关集成测试**

Run:

```console
.venv/bin/python -m pytest \
  tests/integration/test_read_only_surface.py \
  tests/integration/test_cli.py \
  tests/unit/test_storage.py -q
```

Expected: 全部通过，零失败。

---

### Task 7：全量验证并替换旧数据库

**Files:**
- Delete after verification: `data/predmarket.sqlite3`
- Delete after verification if present: `data/predmarket.sqlite3-wal`
- Delete after verification if present: `data/predmarket.sqlite3-shm`
- Create at runtime: `data/predmarket.sqlite3`（schema v7 空库）

**Interfaces:**
- Consumes: 完整 schema v7 实现和配置 `database_path: data/predmarket.sqlite3`
- Produces: 经过完整性验证的 schema v7 空项目数据库

- [ ] **Step 1：运行全量自动测试**

Run:

```console
.venv/bin/python -m pytest
.venv/bin/python -m compileall -q predmarket
.venv/bin/python -m pytest tests/integration/test_read_only_surface.py -q
git diff --check
```

Expected: 所有 pytest 测试通过；compileall 和 diff check 退出码为 0。

- [ ] **Step 2：确认没有进程占用目标数据库**

Run:

```console
lsof data/predmarket.sqlite3
```

Expected: 无输出且退出码为 1，表示没有进程打开该文件。若有输出，停止并确认对应项目进程后再继续，不删除文件。

- [ ] **Step 3：再次确认目标文件和配置**

Run:

```console
pwd
rg -n '^database_path: data/predmarket\\.sqlite3$' config/default.yaml
ls -lh data/predmarket.sqlite3 data/predmarket.sqlite3-wal data/predmarket.sqlite3-shm
```

Expected: 当前目录是仓库根目录，配置精确匹配目标路径。`ls` 可因不存在某个 sidecar 返回非零，但不得显示其他路径。

- [ ] **Step 4：删除已明确放弃的旧数据库文件**

仅在前三步全部成功后，使用三个精确路径，不使用变量、glob 或递归删除：

```console
rm data/predmarket.sqlite3
rm data/predmarket.sqlite3-wal
rm data/predmarket.sqlite3-shm
```

若某个 sidecar 不存在，只跳过该条命令；不要扩大删除范围。

- [ ] **Step 5：创建 schema v7 空库**

Run:

```console
.venv/bin/python -m predmarket \
  --config config/default.yaml --json report --limit 1
```

Expected: 退出码 0，输出一个 JSON 文档，`total` 为 0。

- [ ] **Step 6：验证新数据库完整性和精确表清单**

Run:

```console
sqlite3 data/predmarket.sqlite3 \
  "PRAGMA integrity_check; PRAGMA foreign_key_check; PRAGMA user_version;"
sqlite3 data/predmarket.sqlite3 \
  "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
sqlite3 data/predmarket.sqlite3 \
  "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';"
```

Expected:

```text
ok
7
```

`foreign_key_check` 不输出记录；表名与 Task 1 的
`EXPECTED_V7_TABLES` 完全一致；计数为 `30`。

- [ ] **Step 7：复查工作树，确认没有覆盖无关改动**

Run:

```console
git status --short
git diff --stat
git diff --check
```

Expected: 只新增本任务计划内的 schema、测试和文档差异；用户原有无关改动仍在。不要 commit、push 或部署。
