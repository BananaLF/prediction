# 单市场结构错误容错同步设计

## 状态

已确认，直接实施。

## 背景

Polymarket 某些活跃市场响应可能无法满足内部领域模型约束。例如市场
`2278824` 返回 `events: []`，而当前映射层要求每个市场恰好包含一个 event
引用，因此 `GatewayMappingError` 会中断整个 `list_active_markets()` 调用。

同步任务把该单条错误当作整轮 generation 不完整，Supervisor 因此不会启动
watch，而是持续重试。单个上游坏数据不应阻断其它市场的同步和服务启动。

## 目标

1. 单个市场映射失败时继续处理同一批其它市场。
2. 将失败市场从当前可交易/信号目录排除。
3. 保留已存在市场的历史记录用于审计，但不以旧记录恢复到当前可用目录。
4. 新市场映射失败时不写入当前目录。
5. 记录包含市场 ID、映射原因和受限 API 响应摘要的 warning/system event。
6. 仅单市场 skipped warning 不影响 generation 完整性；批量请求、分页、事件关系
   完整性和数据库写入失败仍按现有 fail-closed 语义处理。

## 非目标

- 不为缺失的 event 引用臆造 `event_id`。
- 不把未知或非法市场字段静默转换成默认值。
- 不删除历史市场记录或修改既有数据库 schema。
- 不改变订单簿、交易或策略计算逻辑。

## 设计

### 1. Gateway 按市场隔离映射错误

`PolymarketGateway.list_active_markets()` 在分页和批量请求成功的前提下，对每个
SDK market 独立调用 `_map_market()`：

- 映射成功的 active market 继续进入返回快照。
- `GatewayMappingError` 被收集为本轮 market mapping warning，并继续处理后续项。
- 分页器、网络请求或其它非单市场映射错误继续抛出，交由同步任务判定本轮不完整。
- warning 保留现有受限错误文本，不扩大 API 响应日志上限。

同步任务通过 gateway 的本轮 warning 结果取得 skipped market ID，避免从异常文本
反向解析标识。

### 2. 当前目录与历史记录

带 skipped ID 的市场不进入本轮 `snapshots`。完整 generation 的既有缺失市场处理
会将已存在的该市场标记为非 active、不可接收订单且不可使用 order book；其历史
记录仍保留在 catalog/database 中。新市场因为没有快照，不会写入目录。

如果 active event 因 skipped market 暂时没有可解析市场，允许该 event generation
完成，但其当前 `market_ids` 只包含可用市场；关系和信号层只依据 active market
与 token 交集，因此不会为坏市场产生信号。

### 3. Warning 记录与运行行为

当本轮只有 market mapping warnings 时：

- `SyncResult.complete` 为 `True`，并携带 warnings 摘要。
- 同步写入 `SYNC_MARKET_SKIPPED` system event，severity 为 `WARNING`。
- 初始同步继续启动 watch；周期同步通过现有 notifier 输出 warning。

当同时存在请求、分页、源完整性或事务错误时，仍写入
`SYNC_GENERATION_INCOMPLETE`，不发布 generation 变更，并按现有重试流程运行。

## 验收标准

- fixture 中一个市场的 `events` 为空时，gateway 返回其它有效市场，并暴露该市场
  的 warning。
- 初始同步包含一个坏新市场时仍 `complete=True`，坏市场不在当前 catalog，且
  `SYNC_MARKET_SKIPPED` 详情包含市场 ID、错误和 API 响应摘要。
- 已存在市场再次映射失败时，历史记录仍存在，但保存后的市场为 inactive、不可
  接单且不可进入关系/信号目录。
- 请求/分页失败、重复 ID、事件引用完整性错误和数据库失败仍保持不完整语义。
- `Supervisor` 对 complete-with-warning 的初始和周期同步都能启动/输出告警。
- 相关单元和集成测试通过，且 `git diff --check` 通过。
