# Issue #14：异步执行首轮市场同步设计

## 背景

当前 `Supervisor.run()` 在启动 `WatchTask` 和其他运行时组件前同步等待
`SyncMarketTask.run_once()`。首轮市场同步的网络耗时会阻塞整个运行时启动；当 catalog
为空或同步不完整时，监听组件也无法及时启动。

## 目标

- 先启动监听组件，再在后台执行首轮市场同步。
- 首轮同步完成后继续按配置间隔执行周期同步。
- 首轮与周期同步严格串行，不能并发执行 `run_once()`。
- 监听从已持久化 catalog 启动；空 catalog 允许监听任务正常运行。
- 保留现有不完整同步通知、失败/取消处理和资源清理行为。
- 保留现有 `MarketChangeQueue` 驱动的订阅切换机制。

## 非目标

- 不修改数据库 schema。
- 不重写 `SyncMarketTask`、`WatchTask` 或 `MarketChangeQueue` 的核心机制。
- 不改变同步失败、任务退出、取消和通知的既有语义。

## 方案

采用单一后台同步循环。`Supervisor` 负责调整启动顺序，`_sync_forever()` 负责首轮和周期
同步；首轮执行与周期执行由同一个协程拥有，因此无需额外锁即可保证串行。

### 启动流程

1. `_build_runtime()` 初始化数据库、repository、notifier、market change queue、gateway、
   `SyncMarketTask` 和 `WatchTask`。
2. 调用 `await watch.start()`。它从持久化 catalog 恢复可监听市场；没有可监听市场时，
   以空订阅成功返回，不阻塞或失败。
3. 创建监听运行任务，并记录
   `component_started component=watch_task`。
4. 创建统一后台同步任务，并记录
   `component_started component=sync_task`。
5. 记录 `runtime_started`，然后沿用现有 `FIRST_COMPLETED` 监督和退出处理。

`WatchTask.run()` 仍可重复调用幂等的 `start()`，因此由 `Supervisor` 提前启动不会改变
其现有运行循环和关闭逻辑。

### 同步流程

后台同步循环的顺序为：

1. 立即调用 `sync.run_once()`，不在首轮前等待间隔。
2. 处理跳过市场通知。
3. 对 `complete=False` 发送现有不完整同步通知，并保留错误详情。
4. 等待配置的同步间隔。
5. 重复第 1 步。

由于所有 `run_once()` 调用都属于同一后台任务，下一次调用只能在上一次返回后发生。首轮
同步提交的 catalog 变更继续进入 `MarketChangeQueue`，由已运行的 `WatchTask` 处理并切换
订阅；因此空 catalog 启动后，新同步出的可监听市场仍会最终被订阅。

### 异常、取消与清理

- `run_once()` 抛出异常时，异常沿同步任务向上暴露；Supervisor 继续使用现有任务退出日志、
  `RUNTIME_TASK_EXITED` 通知和运行时失败返回值。
- `complete=False` 不视为任务失败；继续按现有策略发送通知并在下一间隔重试。
- Supervisor 被取消时，继续取消并 drain 子任务，关闭 `WatchTask`、gateway、writer，
  并记录现有停止日志。
- 启动阶段其他异常仍使用现有启动失败通知和清理路径。

## 代码范围

- `predmarket/app.py`：调整 `Supervisor.run()` 的启动编排；让 `_sync_forever()` 首次
  立即执行并随后按间隔循环。
- `tests/integration/test_app_pipeline.py`：更新启动顺序断言，覆盖首轮同步未完成时监听
  已启动、启动日志、周期同步串行和既有失败/取消语义。
- 监听空 catalog 的行为补充最窄的现有 WatchTask 或应用集成测试，优先复用现有测试替身。

## 验收标准映射

| Issue 要求 | 设计保证 | 验证 |
| --- | --- | --- |
| 启动不等待首轮同步 | 监听先启动，后台任务立即执行同步 | 应用集成测试 |
| 空 catalog 可启动 | `WatchTask` 允许空 token 集合并持续运行 | WatchTask/集成测试 |
| 新市场最终订阅 | 复用 `MarketChangeQueue` 和现有 market-change 处理 | 集成测试 |
| 启动期间可见日志 | 启动三个组件日志点 | 日志断言 |
| 首轮/周期不并发 | 单一 `_sync_forever()` 协程 | 带重叠检测的测试 |
| 失败、不完整、取消、清理保持 | 复用现有异常和 finally 路径 | 回归测试 |

## 风险与取舍

监听组件可能在首轮同步完成前暂时没有可监听市场，这是 Issue 明确允许的状态。依赖
`has_watchable_catalog()` 的首轮阻塞重试逻辑将不再承担启动门槛，但不完整同步通知和后台
重试仍由统一循环负责；这样可以缩小改动范围并避免两个独立同步任务之间的竞态。
