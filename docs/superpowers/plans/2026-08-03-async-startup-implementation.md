# Issue #14 Async Startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让运行时先启动监听组件，再异步执行首轮市场同步，并保证首轮与周期同步串行。

**Architecture:** 在 `Supervisor.run()` 中先调用 `WatchTask.start()` 恢复持久化 catalog，再按顺序创建 watch task 和唯一的 sync task。将首轮 `run_once()` 放入现有 `_sync_forever()`，由同一个协程立即执行首轮、发送现有通知、等待配置间隔并继续周期同步。

**Tech Stack:** Python 3.11+、`asyncio`、pytest 8、pytest-asyncio、现有 `WatchTask`、`MarketChangeQueue` 和 `Notifier`。

## Global Constraints

- 不修改数据库 schema。
- 不重写 `SyncMarketTask`、`WatchTask` 或 `MarketChangeQueue` 的核心机制。
- 不改变同步失败、任务退出、取消和通知的既有语义。
- 监听从已持久化 catalog 启动；空 catalog 必须允许监听任务正常运行。
- 首轮与周期同步严格串行，不能并发执行 `run_once()`。
- 不新增依赖，不触碰根 worktree 中已有的无关修改。
- 保持服务为 public read-only Polymarket signal service，不添加交易行为。

---

## 文件结构

- Modify: `tests/integration/test_app_pipeline.py`：增加首轮不阻塞、启动日志、同步立即执行/串行和同步任务异常的回归测试，并调整旧启动顺序测试。
- Modify: `tests/unit/watch/test_task.py`：验证真实 `WatchTask` 在空 `CatalogSnapshot` 下能启动并由 `close()` 唤醒退出。
- Modify: `predmarket/app.py:76-157,159-265,267-280`：调整 Supervisor 生命周期与任务创建顺序，以及统一同步循环的首轮时序。
- Create: 无。

## Task 1: 先写启动与同步时序的失败测试

**Files:**
- Modify: `tests/integration/test_app_pipeline.py:42-134,161-371`

**Interfaces:**
- Consumes: `Supervisor.run()`、`Supervisor._sync_forever()`、现有 `_Watch`/`Notifier` 测试替身。
- Produces: 对后续 `predmarket/app.py` 实现的可执行行为约束。

- [ ] **Step 1: 添加可控制首轮同步完成时机的测试替身**

在现有 `_IncompletePeriodicSync` 附近添加：

```python
class _BlockingInitialSync:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run_once(self):
        self.started.set()
        await self.release.wait()
        return type("Result", (), {"complete": True})()
```

- [ ] **Step 2: 添加“首轮同步未完成时监听已启动”的失败测试**

测试创建 `_BlockingInitialSync` 和不崩溃的 `_Watch`，启动 `Supervisor.run()` 后等待
`sync.started`。断言 `calls[:2] == ["watch-start", "watch-run"]`，并断言日志包含
`component_started component=watch_task`、`component_started component=sync_task` 和
`runtime_started`；最后取消 Supervisor 并断言返回 0、最后一次调用为 `watch-close`：

```python
@pytest.mark.asyncio
async def test_supervisor_starts_watch_before_initial_sync_completes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []
    sync = _BlockingInitialSync()
    supervisor = Supervisor(
        _config(tmp_path),
        gateway=object(),
        terminal=StringIO(),
        sync_task_factory=lambda **_: sync,
        watch_task_factory=lambda **_: _Watch(calls, crash=False),
        sleep=_wait_for_cancellation,
    )
    running = asyncio.create_task(supervisor.run())
    try:
        with caplog.at_level(logging.INFO, logger="predmarket.app"):
            await asyncio.wait_for(sync.started.wait(), timeout=1)
            assert calls[:2] == ["watch-start", "watch-run"]
            assert "component_started component=watch_task" in caplog.text
            assert "component_started component=sync_task" in caplog.text
            assert "runtime_started" in caplog.text
    finally:
        running.cancel()
        assert await running == 0
    assert calls[-1] == "watch-close"
```

运行：

```bash
.venv/bin/python -m pytest -q tests/integration/test_app_pipeline.py::test_supervisor_starts_watch_before_initial_sync_completes
```

预期：旧实现失败，因为首轮 `run_once()` 阻塞时不会执行 `watch.start()`，且没有新的
`component_started` 日志。

- [ ] **Step 3: 添加“首轮立即执行且调用串行”的失败测试**

添加一个记录活动调用数的 `_SerialSync` 和一个在每次间隔暂停的 `_GateSleep`：

```python
class _SerialSync:
    def __init__(self) -> None:
        self.calls = 0
        self.active = 0
        self.max_active = 0
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()
        self.second_started = asyncio.Event()

    async def run_once(self):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls += 1
        if self.calls == 1:
            self.first_started.set()
            await self.release_first.wait()
        else:
            self.second_started.set()
        self.active -= 1
        return type("Result", (), {"complete": True})()


class _GateSleep:
    def __init__(self) -> None:
        self.called = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, _: float) -> None:
        self.called.set()
        await self.release.wait()
```

启动 `_sync_forever` 后先等待 `first_started`，断言 `_GateSleep.called` 未设置；释放
`release_first`，再释放 `_GateSleep.release`，等待 `second_started`，断言 `max_active == 1`，
最后取消任务并断言 `CancelledError`。
运行：

```bash
.venv/bin/python -m pytest -q tests/integration/test_app_pipeline.py::test_sync_forever_runs_initially_and_serially
```

预期：旧实现首个动作是等待 `_sleep()`，因此在 `first_started` 处超时失败。

- [ ] **Step 4: 更新旧 Supervisor 测试断言**

将 `test_supervisor_syncs_before_watch_and_terminates_after_watch_crash` 重命名为
`test_supervisor_starts_watch_and_terminates_after_watch_crash`，改为断言监听先启动、最后
关闭，不再断言同步先执行；加入三个启动日志断言。

将不完整初始同步测试的 `sleep=lambda _: pytest.fail(...)` 改为
`sleep=_wait_for_cancellation`，因为后台循环会按间隔重试，不再使用
`has_watchable_catalog()` 作为启动门槛。

将 `_FailingSync` 测试重命名为 `test_supervisor_reports_sync_task_failure`，把失败日志断言
从 `runtime_startup_failed` 改为 `runtime_task_exited`，同时保留 `initial sync failed` 和
返回值为 1 的断言。

- [ ] **Step 5: 运行集成测试并确认实现前失败**

```bash
.venv/bin/python -m pytest -q tests/integration/test_app_pipeline.py
```

预期：新增时序测试失败，未涉及的既有测试保持通过或只出现上述明确的旧行为断言失败。

- [ ] **Step 6: 提交测试变更**

```bash
git add tests/integration/test_app_pipeline.py
git diff --cached --check
git commit -m "test: define async startup ordering for issue 14"
```

## Task 2: 覆盖空 catalog 的 WatchTask 行为

**Files:**
- Modify: `tests/unit/watch/test_task.py:66-76,450-464`

**Interfaces:**
- Consumes: `CatalogSnapshot`、`FakeCatalog`、`WatchTask.run()` 和 `WatchTask.close()`。
- Produces: 空 catalog 下监听生命周期的回归保护；不要求修改 `predmarket/watch/task.py`。

- [ ] **Step 1: 添加空 catalog 构造器**

在 `_catalog()` 附近添加：

```python
def _empty_catalog() -> CatalogSnapshot:
    return CatalogSnapshot(events=(), markets=(), tokens=())
```

- [ ] **Step 2: 添加空 catalog 生命周期测试**

创建 `FakeCatalog(_empty_catalog())`，启动 `watch.run()`，让事件循环运行到
`active_token_ids == ()`，调用 `watch.close()`，再用 `asyncio.wait_for(asyncio.shield(task),
timeout=0.1)` 确认任务结束，并断言没有 gateway recovery 请求：

```python
async def test_run_starts_and_closes_with_empty_catalog() -> None:
    gateway = FakeGateway()
    watch, _, _, _, _, _ = _watch(
        catalog=FakeCatalog(_empty_catalog()), gateway=gateway,
    )
    task = asyncio.create_task(watch.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if watch.active_token_ids == ():
            break

    await watch.close()
    await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
    assert task.done() is True
    assert gateway.requests == []
```

- [ ] **Step 3: 运行并提交 WatchTask 回归测试**

```bash
.venv/bin/python -m pytest -q tests/unit/watch/test_task.py::test_run_starts_and_closes_with_empty_catalog
git add tests/unit/watch/test_task.py
git diff --cached --check
git commit -m "test: cover empty catalog watch startup"
```

预期：测试通过，证明空 catalog 要求由现有 WatchTask 行为满足；若失败，只修复与空 catalog
启停直接相关的最小缺陷。

## Task 3: 实现 Supervisor 异步启动和统一同步循环

**Files:**
- Modify: `predmarket/app.py:76-157,159-265,267-280`

**Interfaces:**
- Consumes: Task 1 的 Supervisor 时序测试和 Task 2 的空 catalog WatchTask 契约。
- Produces: `Supervisor.run()` 先启动 watch、再创建 sync；`_sync_forever()` 立即首轮执行并
  在每次调用结束后等待间隔。

- [ ] **Step 1: 移除启动阶段的同步门槛**

删除 `run()` 中从 `initial = await sync.run_once()` 到 `while not initial.complete` 的
代码；将 `_build_runtime()` 的返回类型和调用解包从六个对象调整为五个：

```python
writer, gateway, notifier, sync, watch = await self._build_runtime()
```

`CatalogRepository` 仍在 `_build_runtime()` 内创建并传给 sync/watch，只是不再作为启动门槛
返回给 `run()`。

- [ ] **Step 2: 按顺序启动 watch 和 sync task**

在 build 后执行以下顺序，保留原有 `asyncio.wait`、任务退出通知、取消和 `finally` 清理：

```python
await watch.start()
watch_task = asyncio.create_task(watch.run(), name="WatchTask")
_LOGGER.info("component_started component=watch_task")
sync_task = asyncio.create_task(
    self._sync_forever(sync, notifier), name="SyncMarketTask",
)
_LOGGER.info("component_started component=sync_task")
_LOGGER.info("runtime_started")
tasks = (watch_task, sync_task)
done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
```

不要修改 `WatchTask.run()`；它已有幂等 `start()`。`runtime_started` 必须在两个 task 创建
后记录。

- [ ] **Step 3: 让 `_sync_forever()` 首次立即同步并保留通知语义**

将循环改为先 `run_once()`，再处理跳过市场/不完整通知，最后等待间隔。用 `is_initial` 保留
首轮不完整通知的现有消息：

```python
is_initial = True
while True:
    result = await sync.run_once()
    await _notify_skipped_markets(notifier, result)
    if getattr(result, "complete", True) is False:
        await notifier.notify(
            event_type="SYNC_GENERATION_INCOMPLETE",
            message=(
                "Initial market sync was incomplete"
                if is_initial
                else "Market sync generation was incomplete"
            ),
            details={
                "error": getattr(result, "error", None),
                "sync_generation": getattr(result, "sync_generation", None),
            },
        )
    is_initial = False
    await self._sleep(self._config.polymarket.sync_interval_seconds)
```

不要加入第二个同步 task 或 `asyncio.Lock`；单一循环就是串行边界。`run_once()` 异常继续
离开 sync task，由现有 `FIRST_COMPLETED` 分支记录 `runtime_task_exited`、发送
`RUNTIME_TASK_EXITED` 并返回 1；取消和 finally 清理保持不变。

- [ ] **Step 4: 运行相关测试**

```bash
.venv/bin/python -m pytest -q tests/integration/test_app_pipeline.py
.venv/bin/python -m pytest -q tests/unit/watch/test_task.py
```

预期：两组测试全部通过，包含首轮未完成时 watch 已运行、启动日志、首轮立即执行、同步串行、
跳过/不完整通知、sync task 失败和取消清理。

- [ ] **Step 5: 检查并提交实现**

```bash
git diff --check
git add predmarket/app.py
git diff --cached --check
git commit -m "feat: start market sync asynchronously"
```

## Task 4: 全量验证与交付前检查

**Files:**
- Modify: 无；仅验证 Task 1-3 的变更。

**Interfaces:**
- Consumes: 已提交的测试和 Supervisor 实现。
- Produces: 可供 PR review 的测试证据和干净 diff。

- [ ] **Step 1: 运行全量 pytest**

```bash
.venv/bin/python -m pytest -q
```

预期：退出码为 0。

- [ ] **Step 2: 运行编译检查和 CLI help**

```bash
.venv/bin/python -m compileall -q predmarket tests
.venv/bin/python -m predmarket --help
```

预期：两个命令均退出码为 0，不访问生产数据库或发起市场同步。

- [ ] **Step 3: 检查 diff 与 worktree**

```bash
git diff --check origin/main...HEAD
git status --short --branch
```

预期：无 whitespace 错误；本分支只包含 Issue #14 设计文档、实施计划、测试和
`predmarket/app.py`；根 worktree 中用户已有的 `predmarket/catalog/sync.py` 和未跟踪计划
文件不被纳入本分支。

- [ ] **Step 4: 汇总验收证据并准备 PR**

记录首轮同步不阻塞监听启动、空 catalog 可运行、启动日志出现、`run_once()` 无并发，以及
跳过/不完整/失败/取消通知与清理回归通过的结果；之后按 employee-mode 流程推送分支、创建
PR，并在 Issue #14 关联 PR。
