# Polymarket 动态费率与同步错误日志设计

## 状态

已确认，直接实施。

## 背景

当前市场同步边界只支持 `ZERO` 和 `FLAT` 两种费率模型，并要求 SDK 返回 `fee_type=flat`、`exponent=0`、`rebate_rate=0`。Polymarket 当前市场数据会返回动态费率结构，例如：

```json
{
  "exponent": 1,
  "rate": "0.04",
  "takerOnly": true,
  "rebateRate": "0.25"
}
```

因此市场映射在同步阶段失败，完整 generation 不会提交，数据库也不会产生市场和 token 数据。

Polymarket 当前 taker 费用公式为：

```text
fee = quantity × rate × (price × (1 - price)) ^ exponent
```

当前项目的策略通过可见卖单/买单完成评估，代表 taker 路径；`rebate_rate` 属于 maker rebate 信息，不从 taker 费用中扣除。

## 目标

1. 支持新的动态费率结构，同时保持已有 `ZERO`、`FLAT` 数据和测试兼容。
2. 使用与 Polymarket 一致的价格曲线费用计算，避免继续低估费用或将新费率误判为不可映射。
3. 市场同步遇到上游响应无法映射时，立即在终端输出带市场标识和接口响应摘要的错误，并继续持久化同步失败事件。
4. 初始同步和后续周期同步使用同一套错误输出格式。

## 非目标

- 不实现下单、撤单或真实交易。
- 不将 maker rebate 作为当前策略的收益抵扣项。
- 不为同步失败自动清空已有有效目录。
- 不把未知费率模型静默降级为零费用。

## 设计

### 1. 领域费率模型

新增 `FeeModel.CURVE`，保留现有模型：

| 模型 | 参数 | 计算 |
| --- | --- | --- |
| `ZERO` | 无 | `0` |
| `FLAT` | `rate` | `price × quantity × rate` |
| `CURVE` | `rate`、`exponent`、`rebate_rate`，以及顶层 `taker_only` | `quantity × rate × (price × (1 - price)) ^ exponent` |

`FeeSchedule` 新增 `taker_only: bool` 字段，默认值为 `False`，使已有 JSON 无需迁移。`CURVE` 要求：

- 参数集合必须严格为 `rate`、`exponent`、`rebate_rate`。
- `rate` 和 `rebate_rate` 在 `[0, 1]` 内。
- `exponent` 为有限且非负的 Decimal。
- `taker_only` 必须是布尔值。

新的 JSON 结构如下：

```json
{
  "model": "CURVE",
  "enabled": true,
  "source": "polymarket-client-0.3.0b1:v1:Market.trading",
  "parameters": {
    "rate": "0.04",
    "exponent": "1",
    "rebate_rate": "0.25"
  },
  "taker_only": true,
  "updated_at": 1785405970000
}
```

旧的 `ZERO`、`FLAT` JSON 缺少 `taker_only` 时按 `False` 读取。映射层仅在返回值完全符合旧的 flat 形状时继续生成 `FLAT`，其余包含新 fee schedule 的启用费率生成 `CURVE`。

### 2. 费用计算

`FeeCalculator.calculate` 增加可选的 `is_taker: bool = True` 参数：

- 现有调用不需要修改，默认按当前策略的 taker 路径计算。
- `taker_only=true` 且 `is_taker=false` 时返回零。
- `rebate_rate` 只保存在 schedule 中用于审计，不减少 taker 费用。
- 动态费用使用 `Decimal` 计算，结果按 `0.00001` 进行 `ROUND_HALF_UP`；正费用经过舍入后低于 `0.00001` 时使用协议最小费用 `0.00001`。
- 价格仍限制在 `[0, 1]`，数量必须大于零，费率必须新鲜；任何未知或非法 schedule 继续抛错，由策略转换为 `NotEvaluable`。

### 3. SDK 映射

`_map_fee_schedule` 读取 SDK 的 `rate`、`exponent`、`taker_only`、`rebate_rate`，并按以下规则转换：

- `fees_enabled=false` → `ZERO(enabled=false)`。
- `fees_enabled=null` → `None`。
- `fees_enabled=true` 且 `fee_type=flat`、`exponent=0`、`rebate_rate=0` → 兼容的 `FLAT`。
- 其他启用费率 → `CURVE`，完整保留新结构。
- 缺少 schedule 或任何字段类型非法 → 映射失败，不返回部分市场。

### 4. 同步错误日志

市场映射失败时，`GatewayMappingError` 的错误文本增加：

- `market_id`；
- 原始映射错误；
- `api_response`：优先使用 SDK `model_dump(mode="json")`，否则使用安全的对象属性摘要；
- 响应摘要最多 8 KiB，超过部分标记为截断，防止异常响应淹没日志。

`SyncMarketTask` 将该错误原样放入 `SyncResult.error` 和 `SYNC_GENERATION_INCOMPLETE.details.error`。`Notifier` 对同步/运行时错误事件额外输出 JSON details；Supervisor 的初始同步和周期同步都使用该通道，因此第一次失败后即可看到接口响应摘要。

持续同步仍保留已有有效目录；只有完整 generation 才发布变更和更新目录快照。

## 验收标准

- 现有 `ZERO`、`FLAT` 单元测试和旧 fixture 继续通过。
- 新的 `exponent=1、rate=0.04、takerOnly=true、rebateRate=0.25` fixture 能完成市场同步。
- `price=0.4、quantity=10` 的动态费用为 `0.096`；maker 路径在 `taker_only` 下为零。
- 动态 schedule 可经数据库 JSON 编码和读取后保持全部字段。
- 市场映射失败的错误包含具体市场 ID 和 `api_response`，且被写入 system event。
- 初始同步和后续周期同步的终端输出均包含错误 details。
- 全量测试通过；若外部服务不可用，至少完成相关单元/集成测试并明确记录剩余风险。

## 实施验证（2026-08-02）

- 已实现动态费率领域模型、Polymarket 新费率结构映射、完整费用参数持久化，以及同步错误上下文输出。
- `.venv/bin/pytest -q`：`457 passed, 1 skipped`。
- `.venv/bin/python -m compileall -q predmarket` 与 `git diff --check` 均通过。
