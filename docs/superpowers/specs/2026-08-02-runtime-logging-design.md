# 运行时 Python logging 设计

## 状态

已确认，待编写实施计划。

## 背景

当前运行时的重要状态主要通过 `Notifier` 的终端 `print` 输出，组件初始化、同步进度、Watch 订阅规模和信号状态变化缺少统一的日志记录。终端输出也与 CLI 的标准输出边界混在一起，不利于保留 `status`、`signals` 和 `relations` 命令的 JSON 输出。

本次改动使用 Python 标准库 `logging` 作为运行时观察渠道，同时保留桌面通知和 `system_events` 作为已有的通知与审计渠道。

## 目标

1. 通过统一的 Python logging 查看程序当前运行状态。
2. 输出组件初始化、启动、退出和错误信息。
3. 每轮同步输出成功解析的市场数和实际写入 catalog 的市场记录数。
4. 输出当前订阅的唯一市场数，而不是用 token 数代替市场数。
5. 每次已提交的 `OPENED`、`UPDATED`、`CLOSED` 信号输出一条日志。
6. 启动命令可以配置日志级别，默认级别为 `INFO`。
7. 保持非 `run` 命令的 JSON 标准输出协议不变。

## 非目标

- 不增加日志文件、日志轮转或第三方 logging 依赖。
- 不把 `NOOP` 信号当作信号事件输出。
- 不改变同步、Watch、信号事务和通知的业务语义。
- 不删除桌面通知或 `system_events` 持久化。
- 不统计数据库真正新增的行数；本次统计的是传给 `save_catalog` 的市场记录数。

## 已确认的方案

- 使用方案 1：在组件模块中使用 `logging.getLogger(__name__)`，由 CLI 统一配置日志。
- 日志默认写入 `stderr`，格式为带时间、级别、logger 名称和消息的文本格式。
- 运行时日志默认级别为 `INFO`，可通过 `run --log-level LEVEL` 覆盖。
- `LEVEL` 支持 `DEBUG`、`INFO`、`WARNING`、`ERROR`、`CRITICAL`，命令行输入大小写不敏感。
- 移除 `Notifier` 的终端 `print`；保留桌面通知和 `system_events`。
- `watch` 数量定义为当前实际订阅 token 对应的去重后 `market_id` 数量。
- 信号日志只记录事务提交成功后的 `OPENED`、`UPDATED`、`CLOSED`。

## 设计

### 1. CLI 日志初始化

`predmarket.cli` 只为 `run` 子命令配置运行时日志。命令示例：

```console
predmarket --config config/default.yaml run --log-level DEBUG
```

`--log-level` 只影响 Python logging 的等级过滤，不改变 `stdout` 的用途。日志 handler 写入 `sys.stderr`，因此以下命令仍只向 `stdout` 输出 JSON：

```console
predmarket --config config/default.yaml status
predmarket --config config/default.yaml signals list
```

配置使用标准库能力，不强制覆盖调用方已经安装的 logging handler。现有 `notification.terminal_enabled` 配置键保留用于兼容，并继续作为 CLI 终端日志是否启用的开关；日志等级由 `--log-level` 控制。

默认文本格式示例：

```text
2026-08-02 12:00:01 INFO predmarket.app - component_initialized component=database_writer
```

### 2. 日志事件契约

业务字段使用稳定的 `key=value` 文本字段，事件名称使用 `event=` 或消息前缀表达，避免依赖自定义 JSON formatter。

| 模块 | 级别 | 事件 | 必要字段 |
| --- | --- | --- | --- |
| `predmarket.app` | `INFO` | `component_initialized` | `component` |
| `predmarket.app` | `INFO` | `runtime_started` / `runtime_stopped` | 生命周期信息 |
| `predmarket.app` | `ERROR` | `runtime_startup_failed` / `runtime_task_exited` | `task`、`error`（可用时） |
| `predmarket.catalog.sync` | `INFO` | `sync_completed` | `sync_generation`、`markets_seen`、`markets_persisted` |
| `predmarket.catalog.sync` | `ERROR` | `sync_incomplete` | `sync_generation`、`markets_seen`、`markets_persisted`、`error` |
| `predmarket.watch.task` | `INFO` | `watch_subscribed` | `markets`，以及可诊断的订阅代数 |
| `predmarket.signals.manager` | `INFO` | `signal_transition` | `event`、`signal_id`、`opportunity_key`、`revision` |
| `predmarket.notification.notifier` | `ERROR` | `desktop_notification_failed` | 通知事件类型和错误 |

错误日志在捕获异常的边界记录。预期的上游同步失败使用 `ERROR` 携带错误文本；未预期的后台任务异常使用异常信息，必要时包含 traceback。日志失败本身不能影响已提交的信号或运行时清理。

### 3. 同步统计口径

`SyncResult` 增加 `markets_persisted` 字段：

- `markets_seen`：本轮经过验证并纳入同步处理的快照数，包含必要的补刷新市场。
- `markets_persisted`：本轮实际传给 `CatalogRepository.save_catalog` 的市场记录数。
- 完整同步统计 `prepared.markets` 的长度。
- 不完整同步只有在存在可保存的部分目录时才写库，否则为 `0`。
- 该值包含为保持目录完整而再次 upsert 的既有市场，不表示 SQLite 新增行数。

同步保存完成后记录成功或不完整事件，确保日志中的统计对应实际持久化边界，而不是仅对应上游响应数量。

### 4. Watch 订阅统计

`WatchTask` 在计算可订阅 token 时同时得到这些 token 对应的市场 ID，并对市场 ID 去重。初次恢复完成、以及市场变化触发订阅轮换完成后，记录 `watch_subscribed markets=<唯一市场数>`。

统计来源是实际订阅集合：没有可订阅 token 时记录 `markets=0`；同一个市场有多个 token 时只计数一次。订阅轮换失败时只记录错误，不记录成功的订阅事件。

### 5. 信号日志和通知顺序

`SignalManager` 在信号事务成功提交后构造并记录 `signal_transition`。随后继续调用 `Notifier`，因此：

1. 数据库回滚不会产生信号转换日志。
2. `NOOP` 不产生信号转换日志。
3. 桌面通知失败不会删除或修改已经记录的信号日志。
4. `Notifier` 不再向终端打印信号或运行时通知，只负责桌面通知和 `system_events`。

### 6. 测试与文档

实施时补充以下验证：

- CLI 测试验证 `--log-level` 的默认值、覆盖值、非法值，以及日志走 `stderr`、JSON 走 `stdout`。
- `Notifier` 测试验证终端不再打印，同时桌面通知和桌面失败审计仍生效。
- 同步测试验证完整、不完整和无部分数据时的 `markets_persisted` 口径。
- Watch 测试验证多个 token 属于同一市场时只记录一个市场。
- SignalManager 测试验证只有提交成功的 `OPENED`、`UPDATED`、`CLOSED` 被记录。
- Supervisor 测试验证组件生命周期和异常边界日志。
- 更新 `docs/OPERATIONS.md`，说明 `stderr` 日志、`--log-level` 用法及 `system_events` 的关系。

## 验收标准

使用 `predmarket ... run` 启动后，可以在终端看到组件初始化、同步市场统计、当前唯一订阅市场数和每条已提交信号的日志；使用 `--log-level DEBUG` 可以看到更详细的调试信息。运行 `status`、`signals` 或 `relations` 时，标准输出仍可被 JSON 解析，日志不会混入其中。

