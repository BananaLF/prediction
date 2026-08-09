# Catalog v4 部署验收与持续观察 Implementation Plan

> **状态：** 实施完成，最终验证完成，等待 PR 合并。

> **执行记录：** 各任务已在本隔离分支中按最小变更实现；中间任务未拆分为独立提交，统一在最终验证通过后提交。

当前已完成：探针 CLI、v4 catalog/SQLite/WAL 检查、运行与信号证据关联、非重复采样窗口、稳定 JSON 报告、文档契约和 v4 运维文档。最终验证结果为：探针测试 `28 passed`；完整构建脚本 `774 passed, 2 skipped`；CLI help 校验通过；`git diff --check` 通过。测试仍有 Python 3.14/`pytest-asyncio` 弃用警告，但没有失败。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个不改变生产写路径的 SQLite 只读验证探针，输出 catalog v4 部署和 30 分钟运行窗口的可追溯 JSON 证据，并把仓库运维文档统一到 v4。

**Architecture:** 将探针实现为 `scripts/validate_catalog_v4.py` 中的标准库 Python CLI。它通过 SQLite `mode=ro` 连接目标数据库，使用纯读取 SQL 检查 schema、完整性、catalog generation、WAL 和运行证据，再按固定时间间隔采样并写出有界 JSON 报告；采样器不进入 `predmarket` 运行时，也不执行 checkpoint、删除或数据库写入。测试使用 `create_v4_database` 和临时 SQLite fixture，注入时钟、睡眠函数和 WAL 读取器来确定性验证窗口逻辑。

**Tech Stack:** Python 3.9+、标准库 `sqlite3`/`argparse`/`json`/`datetime`/`pathlib`、pytest、现有 `predmarket.persistence.schema` 和 v4 catalog 表结构。

## Global Constraints

- 验证目标固定为 Schema v4；`PRAGMA user_version` 必须等于 `4`。
- 探针必须以 SQLite `mode=ro` 连接，不能创建或修改数据库、`-wal`、`-shm`，也不能执行 checkpoint、删除或网络请求。
- WAL 硬上限固定为 `128 * 1024 * 1024` bytes；超限只报告 `WAL_LIMIT_EXCEEDED`，不停止服务、不改变 writer 行为。
- CLI 参数为 `--database`（必填）、`--duration-seconds`（默认 `1800`）、`--interval-seconds`（默认 `30`）和可选 `--output`；`duration=0` 表示单次采样，interval 必须为正整数。
- 健康报告退出 `0`，验收失败退出 `1`，数据库不可用、参数无效或输出路径不可用退出 `2`。
- 报告必须明确区分 `theoretical_estimate`、`orderbook_checked_estimate`、`simulated` 和 `realized=unsupported`，不得把 `expected_profit` 或信号数量当作真实收益。
- 本计划不实现真实交易、策略修复、收益结算、新策略研究、自动迁移、自动重置、全量同步或生产进程控制。
- 不新增第三方依赖；文档命令必须能在现有 `scripts/build_env.sh` 创建的环境中运行。

---

## File Map

- Create: `scripts/validate_catalog_v4.py` — CLI 参数解析、只读连接、单次采样、窗口聚合、稳定退出码和 JSON 报告。
- Create: `tests/unit/test_validate_catalog_v4.py` — 探针契约、fixture、只读和报告内容测试。
- Modify: `README.md` — 将启动/数据库/验证入口改为 v4，并链接运维 runbook。
- Modify: `docs/OPERATIONS.md` — 增加停机、迁移/初始化、doctor、30 分钟探针和失败留证流程。
- Modify: `docs/VERIFICATION.md` — 增加自动化测试、单次预检、30 分钟采样和报告字段验收矩阵。
- Modify: `docs/PROJECT-GUIDE.md` — 删除过期 v2/v3 操作说明，说明当前只读观察边界和 Issue #20 后续拆分。

### Test fixture support

在 `tests/unit/test_validate_catalog_v4.py` 顶部定义测试专用辅助对象，避免从生产脚本反向导入测试状态。当前实现使用生产 schema 初始化函数，再通过最小 SQL fixture 注入测试证据；不会调用生产同步流程，也不会把测试数据写入仓库数据库：

当前夹具约束如下：

- `REPO_ROOT` 指向仓库根目录，测试直接 `from scripts import validate_catalog_v4 as module`；
- `make_v4_database` 调用 `create_v4_database`，再把 bootstrap generation 的 `sync_generation` 设置为 `g1`；
- catalog、signal、revision、legs、orderbook 和 system event 夹具全部位于 `tmp_path`，时间使用 `2026-08-09T00:00:00+00:00` 对应的 epoch；
- `signal_legs` 没有独立 `leg_id`，报告使用 `signal_id:revision:position` 作为稳定关联 ID；system event 使用 schema 生成的整数 ID；
- `set_user_version`、`break_active_generation` 和 `leave_migration_marker_incomplete` 分别只改变一个临时 fixture；WAL 读取通过注入函数完成。

## Task 1: Lock the probe CLI and read-only contract with tests

**Files:**
- Create: `tests/unit/test_validate_catalog_v4.py`
- Create: `scripts/validate_catalog_v4.py`

**Interfaces:**
- `ProbeOptions(database: Path, duration_seconds: int, interval_seconds: int, output: Path | None)` — 已解析并校验的 CLI 选项。
- `parse_args(argv: Sequence[str]) -> ProbeOptions` — 将参数错误映射为退出码 `2` 的 CLI 错误。
- `open_read_only(database: Path) -> sqlite3.Connection` — 使用 `file:<absolute-path>?mode=ro` 和 `uri=True` 打开连接，并启用 `PRAGMA query_only=ON`；目标不存在时抛出可归因异常。
- `run_probe(options: ProbeOptions, *, clock, sleep, wal_reader) -> dict[str, Any]` — 运行单次或窗口采样并返回完整报告，不负责写输出文件。
- `main(argv: Sequence[str] | None = None) -> int` — 打印一行摘要、按约定返回 `0/1/2`，只在显式 `--output` 时写报告。

- [ ] **Step 1: Write the failing CLI and read-only tests**

```python
def test_parse_args_requires_database_and_rejects_non_positive_interval() -> None:
    with pytest.raises(ProbeUsageError):
        module.parse_args(["--interval-seconds", "0"])


def test_open_read_only_does_not_create_missing_database(tmp_path: Path) -> None:
    database = tmp_path / "missing.sqlite3"
    with pytest.raises(module.ProbeUnavailableError):
        module.open_read_only(database)
    assert not database.exists()


def test_duration_zero_produces_one_sample_without_output_side_effect(tmp_path: Path) -> None:
    database = tmp_path / "catalog.sqlite3"
    create_v4_database(database)
    report = module.run_probe(
        module.ProbeOptions(database, 0, 30, None),
        clock=FakeClock("2026-08-09T00:00:00+00:00"),
        sleep=lambda _: pytest.fail("single sample must not sleep"),
        wal_reader=lambda _: 0,
    )
    assert len(report["samples"]) == 1
    assert not list(tmp_path.glob("catalog.sqlite3-*"))
```

- [ ] **Step 2: Run the focused tests and verify they fail for the missing probe contract**

Run: `pytest tests/unit/test_validate_catalog_v4.py -q`

Expected: FAIL because `scripts/validate_catalog_v4.py` and its public exceptions/options do not yet exist.

- [ ] **Step 3: Implement the minimal CLI contract**

Implement `ProbeUsageError`, `ProbeUnavailableError`, the `ProbeOptions` dataclass, parser validation, absolute-path read-only URI construction, `query_only`, one-sample execution for `duration_seconds == 0`, and `main`. Do not add catalog-specific checks in this step; an empty `checks` list is acceptable until Task 2.

- [ ] **Step 4: Run the focused tests and verify the contract passes**

Run: `pytest tests/unit/test_validate_catalog_v4.py -q`

Expected: PASS for argument validation, missing database behavior, and one-sample/no-side-effect behavior.

- [ ] **Step 5: Commit the isolated contract**

```bash
git add scripts/validate_catalog_v4.py tests/unit/test_validate_catalog_v4.py
git commit -m "test: define catalog v4 probe contract"
```

## Task 2: Add schema, integrity, catalog-generation and WAL checks

**Files:**
- Modify: `scripts/validate_catalog_v4.py`
- Modify: `tests/unit/test_validate_catalog_v4.py`

**Interfaces:**
- `CheckResult(code: str, status: str, observed: Any, expected: Any, records: list[dict[str, Any]])` — 每个检查的稳定 JSON 形状；`status` 只能为 `pass`, `fail` 或 `not_observed`。
- `collect_catalog_sample(connection: sqlite3.Connection, database: Path, observed_at: datetime, wal_bytes: int) -> dict[str, Any]` — 返回一条完整采样，包含 `checks`, `database`, `catalog`, `runtime` 和 `signals` 字段。
- `CATALOG_REQUIRED_OBJECTS` — 明确列出 v4 表和 `events`/`markets`/`tokens` 视图，缺项产生 `SCHEMA_OBJECT_MISSING`。
- `WAL_LIMIT_BYTES = 128 * 1024 * 1024` — 所有采样和窗口聚合共享的硬上限。

- [ ] **Step 1: Write failing fixture tests for healthy and invalid databases**

```python
def test_healthy_v4_sample_reports_schema_integrity_generation_and_wal(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "healthy.sqlite3", generation="g1")
    sample = run_single(database, wal_bytes=4096)["samples"][0]
    assert codes(sample) >= {
        "SCHEMA_VERSION",
        "SQLITE_INTEGRITY_CHECK",
        "FOREIGN_KEY_CHECK",
        "CATALOG_ACTIVE_GENERATION",
        "WAL_SIZE",
    }
    assert all(check["status"] == "pass" for check in sample["checks"])


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (set_user_version(3), "SCHEMA_VERSION_MISMATCH"),
        (break_active_generation(), "CATALOG_ACTIVE_GENERATION_INVALID"),
        (leave_migration_marker_incomplete(), "MIGRATION_MARKER_INCOMPLETE"),
    ],
)
def test_invalid_v4_fixtures_have_stable_failure_codes(
    tmp_path: Path, mutate, expected_code: str
) -> None:
    database = make_v4_database(tmp_path / "invalid.sqlite3", generation="g1")
    mutate(database)
    report = run_single(database)
    assert expected_code in failure_codes(report)
    assert report["exit_code"] == 1


def test_wal_limit_is_reported_without_checkpoint_or_mutation(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "wal.sqlite3", generation="g1")
    before = database.read_bytes()
    report = run_single(database, wal_bytes=128 * 1024 * 1024 + 1)
    assert "WAL_LIMIT_EXCEEDED" in failure_codes(report)
    assert report["exit_code"] == 1
    assert database.read_bytes() == before
```

- [ ] **Step 2: Run the fixture tests and verify they fail before checks exist**

Run: `pytest tests/unit/test_validate_catalog_v4.py -k 'healthy or invalid or wal_limit' -q`

Expected: FAIL because the sample does not yet inspect v4 schema, integrity, catalog generation or WAL.

- [ ] **Step 3: Implement the read-only checks**

Use `PRAGMA user_version`, `PRAGMA integrity_check`, `PRAGMA foreign_key_check`, `sqlite_schema`, and the existing v4 catalog tables. Report separately: active generation state, generation consistency across `events`/`markets`/`tokens`, `STAGING` count, candidate validity, journal backlog, cleanup backlog, and incomplete migration markers. Read `-wal` size through the injected `wal_reader`; keep observed current size and per-window peak separate. A check must include `code`, `status`, `observed`, and `expected`; warnings from the existing doctor must not be converted into an unconditional pass.

- [ ] **Step 4: Run focused fixture tests and verify stable results**

Run: `pytest tests/unit/test_validate_catalog_v4.py -k 'healthy or invalid or wal_limit' -q`

Expected: PASS, with all invalid fixtures returning exit `1` and the healthy v4 fixture returning exit `0`.

- [ ] **Step 5: Commit the database checks**

```bash
git add scripts/validate_catalog_v4.py tests/unit/test_validate_catalog_v4.py
git commit -m "feat: validate catalog v4 database health"
```

## Task 3: Add runtime, signal, revision, leg and orderbook evidence

**Files:**
- Modify: `scripts/validate_catalog_v4.py`
- Modify: `tests/unit/test_validate_catalog_v4.py`

**Interfaces:**
- `collect_runtime_evidence(connection, window_start: datetime, observed_at: datetime) -> dict[str, Any]` — 返回 `system_events`, signal/revision/orderbook/runtime revision 窗口增量、按 component/severity/event_type 聚合和 WARNING/ERROR/FATAL 摘要。
- `collect_signal_evidence(connection, signal_ids: Sequence[str], window_start: datetime, observed_at: datetime) -> dict[str, Any]` — 以 `arbitrage_signals` 为入口关联 `signal_revisions`, `signal_legs`、相关 `orderbook_snapshots`/`orderbook_levels`，不复制完整 payload。
- `classify_return_evidence(signal: Mapping[str, Any], orderbook: Mapping[str, Any] | None) -> str` — 只返回 `theoretical_estimate`, `orderbook_checked_estimate`, `simulated` 或 `unsupported`。

- [ ] **Step 1: Write failing evidence-linkage tests**

```python
def test_signal_report_links_revision_legs_orderbook_and_system_event(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "evidence.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)
    report = run_single(database)
    signal = report["samples"][0]["signals"][0]
    assert signal["signal_id"] == "signal-1"
    assert signal["latest_revision"] == "revision-1"
    assert {leg["leg_id"] for leg in signal["legs"]} == {"leg-1", "leg-2"}
    assert signal["orderbook"]["snapshot_ids"] == ["snapshot-1"]
    assert signal["evidence_class"] == "orderbook_checked_estimate"
    assert report["samples"][0]["runtime"]["system_events"][0]["event_id"] == "event-1"


def test_missing_orderbook_metadata_is_not_reported_as_executable(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "missing-book.sqlite3", generation="g1")
    seed_signal_without_orderbook(database)
    report = run_single(database)
    signal = report["samples"][0]["signals"][0]
    assert signal["evidence_class"] == "theoretical_estimate"
    assert signal["orderbook"]["status"] == "not_observed"
    assert "ORDERBOOK_METADATA_UNAVAILABLE" in failure_codes(report)


def test_realized_is_always_unsupported_without_fill_source(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "realized.sqlite3", generation="g1")
    seed_signal_revision_legs_orderbook_and_event(database)
    report = run_single(database)
    assert report["aggregates"]["returns"]["realized"] == "unsupported"
```

- [ ] **Step 2: Run evidence tests and verify they fail before linkage exists**

Run: `pytest tests/unit/test_validate_catalog_v4.py -k 'evidence or_orderbook or_realized' -q`

Expected: FAIL because the report has no signal evidence or runtime linkage.

- [ ] **Step 3: Implement bounded evidence collection**

Use the existing schema columns exactly: signal strategy/execution/status/close/latest revision, revision profit/return/loss/risk/unhedged/risk flags/calculation/closure context, leg action/side/quantity/prices/amount/fees, snapshot exchange/received time/generation/tick/minimum order metadata, BID/ASK depth totals, and system event IDs/details summary. Select only signals created or updated in `[window_start, observed_at]`, retain at most the configured bounded summary per category, and emit `not_observed` plus `ORDERBOOK_METADATA_UNAVAILABLE` when required metadata is absent. Never infer a fill, simulation, or realized return from expected values.

- [ ] **Step 4: Run evidence tests and verify classification/linkage**

Run: `pytest tests/unit/test_validate_catalog_v4.py -k 'evidence or_orderbook or_realized' -q`

Expected: PASS; each signal can be traced by IDs to its latest revision, legs, related orderbook snapshot and relevant runtime events, while realized remains `unsupported`.

- [ ] **Step 5: Commit the evidence collector**

```bash
git add scripts/validate_catalog_v4.py tests/unit/test_validate_catalog_v4.py
git commit -m "feat: report catalog v4 runtime evidence"
```

## Task 4: Add deterministic sampling, aggregation and JSON output

**Files:**
- Modify: `scripts/validate_catalog_v4.py`
- Modify: `tests/unit/test_validate_catalog_v4.py`

**Interfaces:**
- `build_report(database: Path, started_at: datetime, ended_at: datetime, samples: Sequence[dict[str, Any]]) -> dict[str, Any]` — 只产生固定顶层键：`schema_version`, `tool_version`, `database`, `started_at`, `ended_at`, `duration_seconds`, `status`, `exit_code`, `checks`, `samples`, `aggregates`, `limitations`。
- `sample_window(options: ProbeOptions, *, clock, sleep, wal_reader) -> list[dict[str, Any]]` — `duration=0` 采样一次；窗口模式按 `interval_seconds` 调度，最后一次不得超过 `duration_seconds`，并计算最大 WAL。
- `write_report(path: Path, report: Mapping[str, Any]) -> None` — 只创建/覆盖用户显式指定的报告路径，父目录不存在时抛出退出码 `2` 对应异常。

- [ ] **Step 1: Write failing deterministic window and output tests**

```python
def test_fixed_clock_generates_deterministic_window_and_wal_peak(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "window.sqlite3", generation="g1")
    clock = FakeClock("2026-08-09T00:00:00+00:00")
    wal_values = iter([100, 300, 200])
    report = module.run_probe(
        module.ProbeOptions(database, 60, 30, None),
        clock=clock,
        sleep=clock.advance,
        wal_reader=lambda _: next(wal_values),
    )
    assert len(report["samples"]) == 3
    assert report["aggregates"]["wal"]["peak_bytes"] == 300
    assert report["duration_seconds"] == 60


def test_report_has_stable_top_level_keys_and_bounded_summaries(tmp_path: Path) -> None:
    database = make_v4_database(tmp_path / "report.sqlite3", generation="g1")
    report_path = tmp_path / "reports" / "catalog.json"
    report_path.parent.mkdir()
    assert module.main([
        "--database", str(database), "--duration-seconds", "0",
        "--output", str(report_path),
    ]) == 0
    report = json.loads(report_path.read_text())
    assert list(report) == [
        "schema_version", "tool_version", "database", "started_at", "ended_at",
        "duration_seconds", "status", "exit_code", "checks", "samples",
        "aggregates", "limitations",
    ]
    assert "realized" in report["aggregates"]["returns"]
```

- [ ] **Step 2: Run deterministic tests and verify they fail before the loop/output exists**

Run: `pytest tests/unit/test_validate_catalog_v4.py -k 'clock or report or bounded' -q`

Expected: FAIL because the current implementation does not provide the multi-sample loop, WAL peak, stable report shape or output handling.

- [ ] **Step 3: Implement the sampling loop and report writer**

Use UTC ISO-8601 timestamps, sample at `started_at`, then advance by the configured interval until the logical end. Aggregate check failures across samples, retain sample timestamps and WAL sizes, and cap event/signal/depth summaries to explicit constants documented in the script. Set report status to `healthy` only when no check has `fail`; set `exit_code` to `1` for any failure and `0` otherwise. Keep `limitations` explicit, including upstream availability, absence of fill/realized evidence, and the fact that WAL is measured rather than controlled.

- [ ] **Step 4: Run the deterministic tests and CLI smoke checks**

Run:

```bash
pytest tests/unit/test_validate_catalog_v4.py -k 'clock or report or bounded' -q
python scripts/validate_catalog_v4.py --help
```

Expected: focused tests PASS; help exits `0` and documents all four arguments and the three exit-code meanings.

- [ ] **Step 5: Commit the report loop**

```bash
git add scripts/validate_catalog_v4.py tests/unit/test_validate_catalog_v4.py
git commit -m "feat: add catalog v4 observation reports"
```

## Task 5: Update the v4 operations and verification documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `docs/VERIFICATION.md`
- Modify: `docs/PROJECT-GUIDE.md`

**Interfaces:**
- The documented runbook must use `predmarket migrate --to 4 --database PATH`, `predmarket doctor --database PATH`, and `python scripts/validate_catalog_v4.py --database PATH --duration-seconds 1800 --interval-seconds 30 --output REPORT`.
- Documentation must state that production stop/reset/migration/full-sync/startup is operator-authorized and is not triggered by the probe.
- Verification matrix must map each acceptance item to report fields or stable check codes, including the `128 MiB` WAL peak and `realized=unsupported` boundary.

- [ ] **Step 1: Write documentation contract checks**

Add assertions to `tests/unit/test_validate_catalog_v4.py` that read the four documentation files and require the exact v4 command forms, `128 * 1024 * 1024`/`128 MiB` limit, `realized` unsupported statement, and the no-automatic-production-action statement. Keep the assertions text-specific so stale v2/v3 instructions cannot silently remain.

```python
@pytest.mark.parametrize("path", ["README.md", "docs/OPERATIONS.md", "docs/VERIFICATION.md", "docs/PROJECT-GUIDE.md"])
def test_documentation_describes_v4_validation_contract(path: str) -> None:
    text = (REPO_ROOT / path).read_text()
    assert "migrate --to 4 --database" in text
    assert "validate_catalog_v4.py" in text
    assert "128 MiB" in text or "128 * 1024 * 1024" in text
    assert "realized" in text and "unsupported" in text
    assert "不自动" in text or "不触发" in text or "not trigger" in text
```

- [ ] **Step 2: Run the documentation checks and verify the stale text fails**

Run: `pytest tests/unit/test_validate_catalog_v4.py -k documentation -q`

Expected: FAIL until all four documents contain the v4 runbook and evidence boundary.

- [ ] **Step 3: Update the four documents**

Replace obsolete v2/v3 database references where they describe the current workflow. Add the ordered operator procedure: stop and back up, migrate or initialize v4, run doctor and a single preflight, start the read-only observer, run the 30-minute probe, retain JSON/database/log evidence, and stop at diagnosis when checks fail. Explain theoretical versus orderbook-checked estimates and explicitly state that the probe does not prove execution or realized profit. Link the accepted design and the follow-up issue split without claiming the current task fixes signal profitability.

- [ ] **Step 4: Run documentation checks and inspect rendered Markdown text**

Run:

```bash
pytest tests/unit/test_validate_catalog_v4.py -k documentation -q
rg -n "v2|v3|migrate --to 4|validate_catalog_v4|128 MiB|realized|生产" README.md docs/OPERATIONS.md docs/VERIFICATION.md docs/PROJECT-GUIDE.md
```

Expected: PASS; current operational commands point to v4 and no current-workflow section instructs operators to use v2/v3.

- [ ] **Step 5: Commit the documentation update**

```bash
git add README.md docs/OPERATIONS.md docs/VERIFICATION.md docs/PROJECT-GUIDE.md tests/unit/test_validate_catalog_v4.py
git commit -m "docs: document catalog v4 validation runbook"
```

## Task 6: Run the complete verification matrix and prepare delivery

**Files:**
- Modify: `docs/superpowers/specs/2026-08-09-catalog-v4-deployment-observation-validation-design.md` only if implementation status needs to be recorded after code is complete.

- [ ] **Step 1: Run focused probe tests**

Run: `pytest tests/unit/test_validate_catalog_v4.py -q`

Expected: all probe contract, database, evidence, window, no-write and documentation tests PASS.

- [ ] **Step 2: Run existing v4 regression tests**

Run:

```bash
pytest tests/integration/test_catalog_generation_v4.py tests/integration/test_catalog_migration_v4.py tests/integration/test_catalog_wal_v4.py tests/unit/persistence/test_catalog_generations.py tests/unit/persistence/test_schema.py -q
```

Expected: all selected v4 persistence tests PASS; no database fixture is modified outside its temporary directory.

- [ ] **Step 3: Run repository-level verification**

Run:

```bash
./scripts/build_env.sh
pytest -q
python -m compileall -q predmarket scripts/validate_catalog_v4.py
python scripts/validate_catalog_v4.py --help
git diff --check
```

Expected: environment setup succeeds, the full suite has no failures, compilation and CLI help exit `0`, and `git diff --check` prints no errors. Existing Python 3.14 deprecation warnings may remain but are not failures.

- [ ] **Step 4: Verify the probe leaves only the requested report file**

Run the probe against a temporary v4 database with `--duration-seconds 0 --output REPORT`, then compare directory entries and database bytes before and after. Expected: only `REPORT` is newly created or changed; the database, `-wal` and `-shm` contents remain unchanged.

- [ ] **Step 5: Review the diff against the approved design**

Check that every design requirement has either a test or a documented operator acceptance step: schema/integrity/FK, generation consistency and backlog, WAL peak, runtime event deltas, signal-to-revision/legs/orderbook IDs, return classification, stable report keys, bounded summaries, no-write behavior, v4 docs, and explicit non-goals.

- [ ] **Step 6: Commit the final verified implementation**

```bash
git add scripts/validate_catalog_v4.py tests/unit/test_validate_catalog_v4.py README.md docs/OPERATIONS.md docs/VERIFICATION.md docs/PROJECT-GUIDE.md
git commit -m "feat: add catalog v4 deployment validation"
git push -u origin issue-20-catalog-v4-validation
```

After this plan is approved for implementation, create the PR and request review; merge only after the user explicitly authorizes the merge.
