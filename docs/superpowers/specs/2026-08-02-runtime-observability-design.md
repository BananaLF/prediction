# predmarket 运行状态日志设计

## 状态

已确认设计，等待 spec review。

## 背景

`predmarket run` 当前主要通过 `Notifier` 输出异常事件。正常启动、市场同步、watch 订阅和信号生命周期没有稳定的终端日志，用户无法判断程序是否初始化成功、同步了多少数据、正在 watch 哪些范围，以及是否产生了信号。

本次改动目标是在不改变数据库结构和业务决策逻辑的前提下，补齐运行可观测性。

## 目标

- `predmarket run` 启动后能明确看到关键组件初始化成功。
- 每次完整同步后能看到同步源数据数量、入库数量和变更发布数量。
- watch 启动和订阅轮换后能看到当前 watch 的事件、市场、token 数量。
- 每次实际产生或更新/关闭信号后能看到信号摘要。
- 日志默认输出到终端，使用标准库 `logging`，不引入运行时依赖。
- 保留现有 `Notifier` 的系统事件、桌面通知和错误输出语义。
- 日志字段稳定、内容简洁，避免逐行情/逐 orderbook 刷屏。

## 非目标

- 不新增数据库表或改变现有数据模型。
- 不记录完整订单簿、完整 API 响应或敏感配置。
- 不把每一条 market change、stream message 或策略评估过程都打印出来。
- 不改变同步 fail-closed、watch 恢复或信号 CAS 行为。

## 方案

采用标准库 `logging`，使用 `predmarket` 命名空间 logger，由 CLI 在 `run` 命令启动时配置为 `INFO` 输出到终端。业务组件通过模块 logger 记录状态，Supervisor 负责跨组件生命周期汇总。

### 日志事件

日志消息使用稳定的英文事件名，字段使用 `key=value`，便于人工阅读和后续采集：

```text
INFO predmarket.app runtime initialized components=database,writer,notifier,gateway,sync,watch,strategy_engine
INFO predmarket.catalog.sync sync completed generation=... events_seen=10 markets_seen=42 tokens_seen=84 events_persisted=10 markets_persisted=42 tokens_persisted=84 changes_published=3 changes_dropped=0
INFO predmarket.watch.task watch started events=8 markets=42 tokens=84
INFO predmarket.watch.task watch updated events=8 markets=43 tokens=86 reason=MARKET_ACTIVATED
INFO predmarket.signals.manager signal OPENED id=... opportunity=... strategy=BINARY_UNDERPRICED
```

### 启动初始化

`Supervisor._build_runtime()` 完成数据库初始化、完整性检查、writer、repository、notifier、gateway、sync、strategy engine、watch 构造后，输出一条 `runtime initialized`。初始化失败仍走现有 `RUNTIME_STARTUP_FAILED` 通知，并保留异常文本。

### 同步汇总

`SyncResult` 增加已持久化数量字段，完整同步成功后由同步任务输出一条 `sync completed`，包含：

- `sync_generation`
- `events_seen`、`markets_seen`、`tokens_seen`
- `events_persisted`、`markets_persisted`、`tokens_persisted`
- `changes_published`、`changes_dropped`
- 如处于 degraded 状态，追加 `degraded=true`

不完整同步仍输出错误日志和现有通知；错误日志包含已有的 `api_response` 摘要，不额外打印原始大响应。

### Watch 状态

`WatchTask` 在首次启动和订阅轮换完成后输出状态汇总。watch 数量从当前已提交 catalog 计算，至少包含事件、市场和 token 数量。仅当 watch 范围实际变化或订阅成功轮换时输出，避免普通 stream message 造成日志噪声。

### 信号生命周期

信号数据库事务提交成功后，在现有通知回调之外输出一条 `signal <event_type>` 日志，包含 signal id、opportunity key 和 strategy type。`NOOP` 不输出，数据库提交失败或 CAS 冲突不伪造“产生信号”日志。

## 日志配置

- `predmarket run` 默认设置 `predmarket` logger 为 `INFO`，输出到 CLI 的 `stdout` 参数；未注入时使用进程标准输出。
- 避免重复添加 handler；测试可注入/捕获 logger。
- 第三方库 logger 不随本次改动提升到 INFO，避免 SDK 和网络库刷屏。
- 现有错误通知输出继续保留，不依赖 logging 配置才能看到启动失败。

## 测试设计

- CLI 集成测试验证 `run` 的 logging 配置只影响 `predmarket` logger，并且不会重复添加 handler。
- Supervisor 测试验证初始化成功日志包含组件名。
- Sync 单元/集成测试验证成功结果包含持久化数量，并输出同步汇总；不完整同步验证错误路径仍保留。
- Watch 测试验证启动和实际订阅轮换输出事件/市场/token 数量，普通消息不重复输出。
- SignalManager 测试验证 OPENED、UPDATED、CLOSED 输出摘要，NOOP 和失败事务不输出。
- 保留并运行现有全量测试，确保日志只增加可观测性，不改变业务行为。

## 验收标准

1. 执行 `predmarket run` 后，终端能看到初始化组件列表。
2. 成功同步后，终端能看到事件、市场、token 的源数据数量和入库数量。
3. watch 启动或范围变化后，终端能看到正在 watch 的事件、市场、token 数量。
4. 信号成功 OPENED、UPDATED 或 CLOSED 后，终端能看到对应信号摘要。
5. 正常运行不会因为逐消息日志造成明显刷屏。
6. 现有异常通知和数据库持久化语义保持不变。
7. 相关测试及全量测试通过。
