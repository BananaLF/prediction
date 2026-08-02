# predmarket 运行状态日志 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or **superpowers:executing-plans** to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `predmarket run` 补齐低噪声、可检索的运行状态日志，让用户能确认组件初始化、同步/入库数量、watch 范围和信号生命周期。

**Architecture:** CLI 仅在 `run` 命令配置 `predmarket` 命名空间的标准库 logger；同步、Supervisor、WatchTask、SignalManager 各自使用模块 logger 输出已经完成的业务动作。同步结果增加持久化计数，watch 数量从已提交 catalog 计算，信号日志只在数据库事务提交成功后产生。现有 Notifier、数据库结构和业务决策流程保持不变。

**Tech Stack:** Python 3、标准库 `logging`、pytest/pytest-asyncio、现有 SQLite/aiosqlite 持久化边界。

## Global Constraints

- 只修改运行可观测性相关代码和测试，不改变同步 fail-closed、watch 恢复、信号 CAS 或通知语义。
- 日志消息使用稳定英文事件名和 `key=value` 字段；不输出完整订单簿、原始大响应、密钥或其他敏感配置。
- `predmarket run` 的日志默认写入 CLI 的 `stdout`；不提升第三方 logger 的等级。
- 不提交用户已有的 `predmarket.egg-info/` 等无关未跟踪文件。
- 不在本计划中执行 commit、push 或创建 PR。

---

## Task 1: Add CLI-scoped runtime logging configuration

**Files:**
- Create `predmarket/runtime_logging.py`
- Modify `predmarket/cli.py`
- Modify `tests/integration/test_cli.py`

- [ ] 先在 CLI 测试中覆盖 `configure_runtime_logging(StringIO())`：`predmarket` logger 输出 `INFO`，格式包含级别、logger 名称和消息；第三方 logger 不被提升；重复配置时只保留一个本次运行的 handler。
- [ ] 在 `predmarket/runtime_logging.py` 实现 `configure_runtime_logging(output: TextIO) -> None`：创建 `StreamHandler(output)` 和 `%(levelname)s %(name)s %(message)s` formatter，设置 `predmarket` logger 为 `INFO`、关闭向 root 传播，并通过私有 handler 标记清理本函数之前安装的 handler，保留外部/测试 handler。
- [ ] 在 `predmarket/cli.py` 的 `run` 分支中，在构造/运行 `Supervisor` 前调用该配置函数，确保注入的 `stdout` 和实际进程 stdout 行为一致；`status`、`signals`、`relations` 不改变输出格式。
- [ ] 运行 `pytest -q tests/integration/test_cli.py`，确认 CLI 配置测试和既有只读命令测试通过。

## Task 2: Record sync persistence counts and emit sync summaries

**Files:**
- Modify `predmarket/catalog/sync.py`
- Modify `tests/unit/catalog/test_sync.py`

- [ ] 先增加测试：完整同步返回 `events_persisted`、`markets_persisted`、`tokens_persisted`，并在 `caplog` 中出现一条 `sync completed`，字段同时包含 seen/persisted/change/degraded 信息；不完整同步测试验证错误日志保留 `api_response` 摘要且不伪造成功汇总。
- [ ] 为 `SyncResult` 增加三个默认值为 `0` 的持久化数量字段，放在已有默认字段区域，避免破坏现有调用方的构造顺序。
- [ ] 完整同步使用实际传给 `save_catalog` 的 `prepared.events/markets/tokens` 长度填充持久化数量；不完整同步使用实际写入的 `partial` 数量，未写入时为零。
- [ ] 在成功写入 catalog、变更发布和辅助持久化完成后记录一条 `_LOGGER.info`：事件名 `sync completed`，包含 `generation`、`events_seen`、`markets_seen`、`tokens_seen`、`events_persisted`、`markets_persisted`、`tokens_persisted`、`changes_published`、`changes_dropped`，degraded 时追加 `degraded=true`。
- [ ] 在不完整路径记录一条 `_LOGGER.error`，包含 `sync incomplete`、generation、seen 数量和已有的 `error_message`；继续使用当前 `SYNC_GENERATION_INCOMPLETE` 系统事件/Notifier，不打印完整响应。
- [ ] 运行 `pytest -q tests/unit/catalog/test_sync.py`，确认所有既有同步契约仍通过。

## Task 3: Log Supervisor component initialization

**Files:**
- Modify `predmarket/app.py`
- Modify `tests/integration/test_app_pipeline.py`

- [ ] 先增加 Supervisor 测试，使用现有可注入 factory 和 `caplog` 验证 `_build_runtime()` 成功后只产生一条 `runtime initialized`，并包含 `database,writer,repositories,notifier,gateway,sync,strategy_engine,watch` 组件字段。
- [ ] 在 `app.py` 增加模块 logger，并在 `_build_runtime()` 完成数据库初始化/完整性检查、writer、repositories、notifier、gateway、sync、strategy engine 和 watch 构造及必要绑定后记录初始化汇总。
- [ ] 保持启动失败路径的 `RUNTIME_STARTUP_FAILED` 通知和返回码不变；组件初始化日志不得在构造失败时提前声明成功。
- [ ] 运行 `pytest -q tests/integration/test_app_pipeline.py`，确认启动顺序、失败通知和任务退出行为不变。

## Task 4: Log watch scope at startup and real subscription rotations

**Files:**
- Modify `predmarket/watch/task.py`
- Modify `tests/unit/watch/test_task.py`

- [ ] 先增加 watch 测试：启动成功后记录 `watch started events=... markets=... tokens=...`；市场变更导致订阅范围变化并完成恢复后记录 `watch updated ... reason=...`；普通 `price_change`、`book`、`last_trade_price`、`best_bid_ask` 不产生 watch 状态日志。
- [ ] 增加基于 `CatalogSnapshot` 和当前 token 集合的内部计数 helper：只统计当前 watchable token 所属的 market 和 event，输出去重后的事件/市场数量及 token 数量。
- [ ] 在 `WatchTask.start()` 完成恢复和 `_started=True` 后记录启动汇总；在 `handle_market_change()` 的 `_rotate_to()` 成功后记录范围更新及 `MarketChangeType.value` 原因。
- [ ] 对 `market_resolved` 这种 stream 触发的实际轮换，在轮换成功后按当前 catalog 与保留 token 集合记录一次更新；恢复/失效但 watch 范围未改变的普通过程不记录，避免刷屏。
- [ ] 保持日志发生在订阅恢复成功之后；恢复失败、watch 关闭或普通行情消息不产生“已 watch”成功假象。
- [ ] 运行 `pytest -q tests/unit/watch/test_task.py`，确认现有订阅、恢复、关闭和信号关闭语义不变。

## Task 5: Log committed signal lifecycle events

**Files:**
- Modify `predmarket/signals/manager.py`
- Modify `tests/unit/signals/test_manager.py`

- [ ] 先增加测试覆盖 `OPENED`、`UPDATED`、`CLOSED` 的 `caplog` 摘要，确认字段包含 signal id、opportunity key、strategy type 和 revision；覆盖 `NOOP`、事务失败和 CAS 冲突不输出生命周期成功日志。
- [ ] 在 `_notify_after_commit()` 中先过滤 `NOOP`，再使用 `_LOGGER.info` 记录 `signal <event_type>`；日志必须在 `_apply_transaction()` 已返回通知之后、Notifier 回调之前/之外产生，因此即使通知失败也不会丢失成功提交日志。
- [ ] 使用 SignalManager 已绑定的策略身份输出 `strategy`，使用 notification 的 `signal_id`、`opportunity_key`、`revision`；没有 notifier 时也应记录日志，Notifier 异常仍按现有语义吞掉。
- [ ] 运行 `pytest -q tests/unit/signals/test_manager.py`，确认数据库事务、通知隔离和并发冲突行为不变。

## Task 6: Integrated verification and cleanup review

**Files:**
- No new production files; review all files changed by Tasks 1–5.

- [ ] 运行窄范围回归：
  `pytest -q tests/integration/test_cli.py tests/integration/test_app_pipeline.py tests/unit/catalog/test_sync.py tests/unit/watch/test_task.py tests/unit/signals/test_manager.py`
- [ ] 运行完整测试：`pytest -q`，记录环境依赖导致的收集失败与代码失败的区别；若出现失败，按失败信息修正后重跑最窄相关测试。
- [ ] 运行 `git diff --check` 和 `git status --short`，确认没有误改无关文件、没有把 `predmarket.egg-info/` 纳入变更，且 spec/plan 和实现改动可清晰区分。
- [ ] 交付时仅汇报实际修改、验证结果和仍需人工确认的运行步骤；不在本任务中提交或推送。

