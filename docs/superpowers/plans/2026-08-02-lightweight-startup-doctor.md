# Lightweight Startup Doctor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将运行时启动检查收敛为快速结构检查，并增加只读、可脚本消费的 `predmarket doctor` 完整语义检查命令。

**Architecture:** 在现有 `predmarket.persistence.integrity` 中提取共享的只读结构检查，保留 `check_database_integrity()` 的完整检查兼容入口，新增 `check_database_startup()` 和 `run_database_doctor()`。doctor 使用内部 finding collector 复用五类现有语义检查，输出分类、稳定错误码和受影响记录；CLI 只负责配置加载、JSON 输出和退出码。

**Tech Stack:** Python 3.11+、SQLite readonly URI、`argparse`、`dataclasses`、pytest、现有 `AppConfig`/CLI/Schema v1。

## Global Constraints

- 只在 `/Users/lifei/workspace/earn_money_from_prediction/.worktrees/issue-8-lightweight-startup-doctor` 修改文件。
- `predmarket run` 启动检查只执行 schema/version、SQLite `integrity_check`、foreign-key check 和十张必需表检查。
- `predmarket doctor` 默认输出 JSON，只读数据库，不初始化、迁移或修复数据。
- doctor 退出码固定为 `0`（无发现）、`1`（有 errors/warnings）、`2`（参数非法或无法检查数据库）。
- 现有 `check_database_integrity()` 的稳定 `DatabaseIntegrityError.violations` 行为保持兼容。
- 不新增依赖，不提交、不推送、不修改父 worktree 或其他 issue worktree。
- 每个实现步骤必须先写测试并看到预期失败，再写最小生产代码使其通过。

---

### Task 1: 重构完整检查的共享收集器并新增 doctor 报告

**Files:**
- Modify: `predmarket/persistence/integrity.py`
- Test: `tests/unit/persistence/test_integrity.py`

**Interfaces:**
- Produces `check_database_startup(path: Path) -> None`。
- Produces `run_database_doctor(path: Path) -> DatabaseDoctorReport`。
- `DatabaseDoctorReport.to_payload() -> dict[str, object]` 返回稳定 JSON payload，包含 `database`、`status`、`summary`、`categories`、`findings`；无法检查时额外包含稳定的 `error` 对象。
- `DatabaseDoctorReport.exit_code` 为 `0`、`1` 或 `2`。
- 保留 `check_database_integrity(path: Path) -> None` 及其 `DatabaseIntegrityError.violations`。

- [ ] **Step 1: 写启动检查和 doctor 健康库的失败测试**

在 `tests/unit/persistence/test_integrity.py` 增加导入和测试：

```python
from predmarket.persistence import integrity
from predmarket.persistence.integrity import (
    check_database_startup,
    run_database_doctor,
)


def test_startup_check_skips_semantic_scans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)

    for name in (
        "_check_id_arrays",
        "_check_json_payloads",
        "_check_decimals",
        "_check_latest_revisions",
        "_check_revision_payloads",
    ):
        monkeypatch.setattr(
            integrity,
            name,
            lambda *_args, _name=name: pytest.fail(f"startup called {_name}"),
        )

    check_database_startup(database_path)


def test_doctor_reports_a_healthy_database_as_json_payload(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)

    report = run_database_doctor(database_path)

    assert report.exit_code == 0
    assert report.to_payload()["status"] == "ok"
    assert report.to_payload()["summary"] == {"errors": 0, "warnings": 0}
    assert report.to_payload()["findings"] == []
```

- [ ] **Step 2: 运行测试确认它们因新接口不存在而失败**

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/persistence/test_integrity.py -k 'startup_check or doctor_reports_a_healthy'
```

Expected: FAIL with import errors for `check_database_startup` or `run_database_doctor`.

- [ ] **Step 3: 写最小共享 collector 和结构检查实现**

在 `integrity.py` 中增加内部 finding/collector 与公开 report 类型，至少包含以下行为：

```python
def check_database_startup(path: Path) -> None:
    collector = _collect_database_findings(Path(path), include_semantic=False)
    if collector.violations:
        raise DatabaseIntegrityError(collector.violations)


def run_database_doctor(path: Path) -> DatabaseDoctorReport:
    try:
        collector = _collect_database_findings(Path(path), include_semantic=True)
    except (OSError, sqlite3.DatabaseError) as error:
        return DatabaseDoctorReport.unavailable(Path(path), error)
    return DatabaseDoctorReport.from_collector(Path(path), collector)
```

结构检查逻辑从现有 `check_database_integrity()` 提取为共享路径；schema/version 不匹配或必需表集合不完整时不继续语义扫描。collector 保持首次发现顺序，按 `category`、`code` 和记录键去重；当前所有既有语义发现的 severity 为 `error`。

- [ ] **Step 4: 运行测试确认新接口通过且旧完整检查不回归**

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/persistence/test_integrity.py
```

Expected: existing integrity tests and the two new tests pass。

- [ ] **Step 5: 写受影响记录和退出状态失败测试**

增加一个事件 ID 数组损坏测试和缺失数据库测试：

```python
def test_doctor_reports_affected_event_record(tmp_path: Path) -> None:
    database_path = tmp_path / "market.db"
    _seed_valid_database(database_path)
    _corrupt(
        database_path,
        "UPDATE events SET market_ids_json = '[\"market-1\"]' WHERE id = 'event-1'",
    )

    report = run_database_doctor(database_path)
    finding = next(
        item for item in report.to_payload()["findings"]
        if item["code"] == "EVENT_MARKETS_MISMATCH"
    )

    assert report.exit_code == 1
    assert finding["category"] == "id_arrays"
    assert finding["severity"] == "error"
    assert finding["records"] == [
        {"field": "market_ids_json", "id": "event-1", "table": "events"}
    ]


def test_doctor_returns_unavailable_for_missing_database(tmp_path: Path) -> None:
    report = run_database_doctor(tmp_path / "missing.db")

    assert report.exit_code == 2
    assert report.to_payload()["status"] == "unavailable"
```

- [ ] **Step 6: 运行测试确认记录/不可用场景按预期失败**

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/persistence/test_integrity.py -k 'affected_event or missing_database'
```

Expected: FAIL because findings do not yet carry affected records and the unavailable report is not yet complete。

- [ ] **Step 7: 为五类语义检查补充记录上下文并完成 doctor 报告**

将现有 `_add(violations, code)` 路径改为 collector；每类检查传入确定性的表名、主键和字段：事件/信号 ID 数组、JSON 字段、Decimal 字段、signal revision 复合键、foreign-key 返回的表和 rowid。报告固定输出五个类别 `schema`、`id_arrays`、`json_payloads`、`decimals`、`revisions`，结构问题无具体记录时使用空 `records`。

- [ ] **Step 8: 运行完整完整性单元测试确认 green**

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/persistence/test_integrity.py
```

Expected: all integrity tests pass，且原有 `DatabaseIntegrityError.violations` 断言保持通过。

### Task 2: 将 Supervisor 启动路径切换到轻量检查

**Files:**
- Modify: `predmarket/app.py:23,157-164`
- Modify: `tests/integration/test_app_pipeline.py:307-344`

**Interfaces:**
- `Supervisor._build_runtime()` 调用 `initialize_database()` 后调用 `check_database_startup()`。
- 启动失败日志仍包含 `database integrity failed` 的既有测试语义，但测试替身挂在 `check_database_startup`。

- [ ] **Step 1: 修改启动失败测试使其表达新入口并先运行**

将 parametrized test 中的 monkeypatch 目标从 `predmarket.app.check_database_integrity` 改为 `predmarket.app.check_database_startup`，先运行：

```bash
.venv/bin/python -m pytest -q tests/integration/test_app_pipeline.py -k 'pre_notifier_database_failures'
```

Expected: FAIL because `app.py` 仍调用旧完整检查入口。

- [ ] **Step 2: 切换 Supervisor import 和调用**

只将 `predmarket.app` 的 import/call 从 `check_database_integrity` 替换为 `check_database_startup`，不改初始化、writer 或其他启动顺序。

- [ ] **Step 3: 运行启动回归测试**

Run:

```bash
.venv/bin/python -m pytest -q tests/integration/test_app_pipeline.py -k 'pre_notifier_database_failures'
```

Expected: parametrized initialize/integrity/writer 三个场景通过。

- [ ] **Step 4: 运行 persistence 与 app 相关回归测试**

Run:

```bash
.venv/bin/python -m pytest -q tests/unit/persistence tests/integration/test_app_pipeline.py
```

Expected: all selected tests pass。

### Task 3: 接入 `predmarket doctor` CLI 与稳定退出码

**Files:**
- Modify: `predmarket/cli.py:31-105,144-188`
- Modify: `tests/integration/test_cli.py:54-61`
- Add: `tests/integration/test_doctor_cli.py`

**Interfaces:**
- Parser adds top-level `doctor` command and accepts existing global `--config` placement normalization。
- `main([... "doctor" ...], stdout=stream) -> int` writes one JSON document and returns doctor report exit code。

- [ ] **Step 1: 写 parser、健康库和三种退出码的失败测试**

在 CLI tests 中将 expected command set 增加 `doctor`，并新增专门测试：

```python
def test_doctor_returns_json_for_healthy_database(tmp_path: Path) -> None:
    database_path = tmp_path / "doctor.sqlite3"
    initialize_database(database_path)
    config_path = _config(tmp_path, database_path)
    output = StringIO()

    assert main(["--config", str(config_path), "doctor"], stdout=output) == 0
    payload = json.loads(output.getvalue())
    assert payload["status"] == "ok"
    assert payload["summary"] == {"errors": 0, "warnings": 0}


def test_doctor_returns_one_for_findings(tmp_path: Path) -> None:
    database_path = tmp_path / "doctor.sqlite3"
    initialize_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "INSERT INTO events (id, title, status, neg_risk, neg_risk_complete, "
            "neg_risk_conversion_supported, market_ids_json, sync_generation, "
            "sync_generation_complete, created_at, updated_at) "
            "VALUES ('event-1', 'Event', 'ACTIVE', 0, 0, 0, '[]', 'sync', 1, 1, 1)"
        )
    config_path = _config(tmp_path, database_path)
    output = StringIO()

    assert main(["doctor", "--config", str(config_path)], stdout=output) == 1
    assert json.loads(output.getvalue())["status"] == "issues"


def test_doctor_returns_two_when_database_cannot_be_opened(tmp_path: Path) -> None:
    config_path = _config(tmp_path, tmp_path / "missing.sqlite3")
    output = StringIO()

    assert main(["doctor", "--config", str(config_path)], stdout=output) == 2
    assert json.loads(output.getvalue())["status"] == "unavailable"
```

- [ ] **Step 2: 运行 CLI 测试确认 doctor 尚未注册**

Run:

```bash
.venv/bin/python -m pytest -q tests/integration/test_cli.py -k 'command_families or doctor'
```

Expected: FAIL because `doctor` is not a parser choice and no CLI branch exists。

- [ ] **Step 3: 注册 parser 命令并接入 JSON/exit code 分支**

在 `_build_parser()` 注册 `commands.add_parser("doctor", help="... read-only ...")`；在 `main()` 完成配置加载并建立 `output` 后，在 `run/status/signals/relations` 分支前调用 `run_database_doctor(config.database.path)`、`_write_json(output, report.to_payload())` 并返回 `report.exit_code`。

- [ ] **Step 4: 运行 CLI 测试确认三个退出码通过**

Run:

```bash
.venv/bin/python -m pytest -q tests/integration/test_cli.py -k 'command_families or doctor'
```

Expected: all selected CLI tests pass。

- [ ] **Step 5: 验证 doctor 的只读性和帮助输出**

增加/运行测试确认 doctor 前后数据库 bytes、mtime 和 `-wal`/`-shm` 状态不因命令改变，并运行：

```bash
.venv/bin/python -m predmarket --help
.venv/bin/python -m predmarket doctor --help
.venv/bin/python -m pytest -q tests/integration/test_cli.py tests/integration/test_documented_commands.py
```

Expected: help exit `0`，CLI 与文档命令测试通过。

### Task 4: 更新运维文档并完成验证

**Files:**
- Modify: `docs/OPERATIONS.md`
- Modify: `docs/VERIFICATION.md`
- Modify: `tests/integration/test_documented_commands.py:55-75`

**Interfaces:**
- 运维文档明确 `run` 的启动失败与 `doctor` 的数据发现不同。
- 文档示例中的 `doctor --config` 能被 parser-only 文档测试解析。

- [ ] **Step 1: 写文档命令断言**

在 `test_documented_local_commands_parse_without_runtime_side_effects` 的最小命令集合中加入：

```python
("doctor", "--config", "config/default.yaml"),
```

先运行该测试，确认文档尚未包含 doctor 命令时失败。

- [ ] **Step 2: 更新 OPERATIONS 和 VERIFICATION**

在 `docs/OPERATIONS.md` 增加 doctor 示例、JSON/退出码说明、只读和不自动修复保证，并说明：启动检查失败阻止服务启动；doctor findings 是运维诊断结果，不能被当作修复或交易许可。在 `docs/VERIFICATION.md` 增加 doctor parser/temporary-db 验证命令和 `0/1/2` 含义。

- [ ] **Step 3: 运行文档测试确认通过**

Run:

```bash
.venv/bin/python -m pytest -q tests/integration/test_documented_commands.py
```

Expected: all documented command parsing tests pass。

- [ ] **Step 4: 运行完整基线和静态检查**

Run:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q predmarket
git diff --check
git status --short
```

Expected: pytest 无失败、compileall exit `0`、diff check 无输出；最终只出现本 worktree 中与 Issue #8 相关的设计/计划、代码、测试和文档改动。

## Execution Handoff

本计划在当前会话 inline 执行，使用 `superpowers:executing-plans` 的任务检查点；由于用户明确要求不提交、不推送，不执行计划模板中的 commit 步骤。完成前使用 verification-before-completion 重新运行完整验证，并检查 worktree 状态。
