# 机会精确验证 CLI 设计

日期：2026-07-26
状态：待用户审查
阶段：SQLite 事实校验与回放一致性验证

## 1. 目标

新增一个只读 CLI 命令，用于对“某一个具体机会”做精确验证。

这个命令必须同时完成两件事：

1. 校验该机会在 SQLite 中的事实链是否完整；
2. 回放该机会对应的证据，并检查回放结果与数据库中的存储事实是否一致。

命令只输出机器可读 JSON，不输出交互式提示，不依赖网络，不触发任何写操作。

## 2. 核心原则

1. 单一职责：这个命令只做“精确验证”，不承担普通回放、报表汇总或数据导出。
2. 只读：验证过程不得修改 SQLite，不得调用交易接口，不得修改运行状态。
3. 可审计：每个失败都必须可机器解析，且能定位到失败环节。
4. 可复现：同一份数据库输入应得到稳定一致的验证结果。
5. 语义优先：比较的是关键业务语义是否一致，不要求对所有原始 JSON 做字节级相等。

## 3. 命令定义

建议新增命令：

```text
predmarket validate-opportunity <opportunity_id>
```

CLI 解析契约：该子命令的 `command` 为 `"validate-opportunity"`，并将唯一的位置参数原样保存为 `opportunity_id`。

### 3.1 参数

- `opportunity_id`
  - 必填
  - 要验证的机会 ID

### 3.2 可选参数

当前版本先不增加额外过滤参数，避免把“精确验证”做成通用查询器。

如果后续确实需要 bundle 级别定位，可以再补 `--bundle-id`，但本阶段不作为主入口。

### 3.3 输出

- 只输出 JSON；
- 退出码用于表示命令成功执行与否，不承担业务通过/失败语义；
- 业务是否通过由 JSON 中的 `status` 字段表达。

## 4. 验证范围

### 4.1 完整性检查

先确认该机会在 SQLite 中是否具备完整闭环。至少检查以下对象是否存在：

- `opportunities`
- 关联的 `runs`
- 关联的 `legs`
- 关联的 `actions`
- 关联的 `risk_assessments`
- 关联的 `latency_metrics`
- 关联的 `notifications`（如该机会生成过通知）
- 该机会对应的 evidence bundle

如果存在同名机会的多个候选记录，应按现有存储语义选择“最新、最相关”的那一条，并把选择依据写入 JSON。

### 4.2 回放一致性检查

再对机会所属 evidence bundle 做 replay，并将回放结果与存储结果比较。

比较对象应限定在业务关键字段，而不是完整原始 JSON。建议至少比较：

- 证据 bundle ID；
- 机会 ID；
- run ID；
- 机会状态；
- 相关 legs；
- 相关 actions；
- risk 结论；
- latency 指标；
- 通知审计摘要（如果有）。

对于时间戳、内部排序、派生字段、空值填充等非语义字段，可允许规范化后比较，不要求逐字节一致。

## 5. 建议的 JSON 输出结构

命令输出建议包含以下字段：

```json
{
  "opportunity_id": "op_123",
  "status": "pass",
  "checks": {
    "completeness": {
      "status": "pass",
      "bundle_id": "bundle_456",
      "run_id": "run_789",
      "missing": []
    },
    "consistency": {
      "status": "pass",
      "matched_fields": ["bundle_id", "opportunity_id", "run_id"],
      "mismatched_fields": [],
      "normalized_diffs": []
    }
  },
  "evidence": {
    "bundle_id": "bundle_456",
    "opportunity_id": "op_123",
    "run_id": "run_789"
  },
  "errors": [],
  "selection": {
    "strategy": "latest_run_for_opportunity",
    "reason": "unique opportunity row selected by newest run timestamp"
  }
}
```

### 5.1 顶层字段

- `opportunity_id`
- `status`
  - `pass`
  - `fail`
- `checks`
- `evidence`
- `errors`
- `selection`

### 5.2 `status` 约定

- `pass`：完整性和一致性都通过；
- `fail`：任一环节失败；
- `pass_with_warnings` 本阶段不建议引入，避免模糊语义。

## 6. 失败分类

建议使用稳定的机器可读错误码：

- `NOT_FOUND`
  - 找不到指定机会；
- `AMBIGUOUS_OPPORTUNITY`
  - 找到多条候选且无法按既定规则唯一选择；
- `INCOMPLETE_CHAIN`
  - 机会存在，但关联证据链缺失；
- `REPLAY_MISMATCH`
  - 回放结果与存储事实不一致；
- `CORRUPTED_CANONICAL_JSON`
  - 证据 bundle 的 canonical JSON 自检失败；
- `STORAGE_ERROR`
  - SQLite 读取失败或结构异常；
- `INVALID_INPUT`
  - 参数非法。

每个错误都应携带：

- `code`
- `message`
- `context`

## 7. 选择规则

当同一个 `opportunity_id` 对应多条历史记录时，选择规则必须稳定且可解释。

建议优先级如下：

1. 最新 `run.started_at_ms`；
2. 同时间下最新插入顺序；
3. 仍无法区分时返回 `AMBIGUOUS_OPPORTUNITY`。

选择结果必须写入输出的 `selection` 字段，避免“命令悄悄挑了一条”。

## 8. 与现有命令的边界

### 8.1 与 `replay`

`replay` 的职责是返回原始证据和审计材料。
`validate-opportunity` 的职责是判断“这条机会在库里是否完整且回放一致”。

两者不能互相替代：

- `replay` 偏取证；
- `validate-opportunity` 偏验证。

### 8.2 与 `report`

`report` 继续负责汇总统计，不承担单机会精确验证。

### 8.3 与后续回测

后续回测可以复用该命令内部的完整性与一致性校验逻辑，但不应把本命令退化成泛化回测框架。

## 9. 设计原理

这个设计的本质是把“事实存在”与“事实一致”拆成两层：

1. 完整性层确认这条机会的证据链是否闭环；
2. 一致性层确认回放时是否能稳定恢复出同一语义结果。

这样做的好处是：

- 能区分“数据缺失”与“重放不一致”；
- 能快速定位 SQLite 中到底是漏写、损坏还是回放逻辑偏差；
- 更适合后续自动化监控和回归测试。

## 10. 风险项

### 10.1 机会选择歧义

如果同一机会对应多次运行记录，而选择规则不明确，验证结果会不稳定。

### 10.2 语义比较过宽或过窄

比较字段太少会漏掉真实问题；比较字段太多会被无关差异干扰。

### 10.3 证据链缺口

若 SQLite 中只有机会主表，没有 legs/actions/risk/notifications 之一，验证结果应明确失败，而不是降级为“部分通过”。

### 10.4 退出码与业务状态混淆

退出码应只表达命令执行是否成功；业务通过失败必须由 JSON 表达，否则脚本调用会混乱。

## 11. 测试要求

必须补的测试包括：

- 正常机会可通过完整性检查；
- 正常机会可通过回放一致性检查；
- 缺失 legs/actions/risk 时返回 `INCOMPLETE_CHAIN`；
- canonical JSON 损坏时返回 `CORRUPTED_CANONICAL_JSON`；
- 回放结果与存储事实不一致时返回 `REPLAY_MISMATCH`；
- 同一机会多记录时选择规则稳定；
- CLI 只输出 JSON，不夹带额外文本。

## 12. 验收标准

以下条件满足时，可认为本设计完成：

- 新增独立 CLI 命令可验证指定机会；
- 命令输出稳定 JSON；
- 命令同时完成完整性检查与回放一致性检查；
- 失败分类可机器解析；
- 与现有 `replay`、`report` 命令边界清晰；
- 测试能覆盖正常路径、缺失链路、回放不一致和歧义选择。
