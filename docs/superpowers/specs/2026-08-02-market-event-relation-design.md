# Market/Event 关系与 Watch 启动语义设计

- 状态：待用户评审
- Issue：[#6 重新定义 market/event 关系](https://github.com/BananaLF/prediction/issues/6)
- 日期：2026-08-02

## 背景

当前 Schema v1 将 `markets.event_id` 定义为 `NOT NULL` 外键，gateway 也要求每个 market 恰好返回一个 event。这个假设不符合真实业务：上游可能返回没有 event 的合法 market。

更严重的问题是，`sync` 的完整性检查与 `watch` 的启动被绑定在一起。只要本轮同步无法建立 market/event 映射，进程就会持续等待完整同步，导致数据库中已经存在合法 market 和 token 时，`watch` 仍然无法启动。

本设计的核心目标是：

> market 与 event 的关联可以不存在或最多存在一个；watch 是否启动只取决于数据库中已经提交的可监控 market/token 数据，不取决于本轮 sync 是否完整成功。

## 目标

1. 明确定义 market/event 的关系基数和 orphan market 的行为。
2. 允许合法 orphan market 被同步、保存、展示和 watch 监控。
3. 让 event 关联成为 market 的可变关系属性，而不是 market 身份的一部分。
4. 保留真正影响数据安全的校验：market/token 身份、非空外键的引用完整性、歧义的多 event 映射。
5. 提供显式、可验证、可回滚的 Schema v1 到新版本迁移；应用启动不自动执行迁移或修复。
6. 保持 event 相关策略在缺少 event 时安全地跳过，不伪造 event 或错误地产生 event 级信号。

## 非目标

- 本 issue 不把关系扩展为 market 对多个 event 的多对多模型。
- 不在启动时自动修改现有数据库，不为 orphan market 创建虚拟 event。
- 不因本轮 sync 不完整而阻止已有合法 catalog 被 watch 使用。
- 不改变 orderbook、token 身份和策略风控规则的既有语义。

## 关系契约

### 关系基数

最终关系为 `0..1`：

- 一个 market 可以没有 event，也可以关联一个 event。
- 一个 event 可以关联零个、一个或多个 market。
- 一个 market 不能同时归属于多个 event。
- `market.id` 与 `condition_id` 仍是 market 的身份和稳定性依据；`event_id` 只是可变的归属关系。

数据库模型仍使用 `markets.event_id`，但改为可空外键；当前没有足够业务证据支持引入 M:N 关联表。

### 反向索引

`events.market_ids_json` 保留，但重新定义为本地数据库中的派生反向索引，而不是上游 API 的权威关系声明：

- 只包含当前数据库中 `markets.event_id = event.id` 的 market ID。
- 可以是空数组。
- orphan market 不出现在任何 event 的 `market_ids_json` 中。
- sync 不再要求它与上游 event 返回的 market ID 数组逐项一致。
- repository 在保存或变更 market 归属后，在同一事务内重算受影响 event 的索引。

因此，“不再要求两边数组严格一一对应”指不再把上游数组当作强制的一对一关系；本地派生索引仍必须与本地行一致，否则 doctor 应报告完整性错误。

### Gateway 映射规则

对单个 SDK market 的 `events` 字段采用以下规则：

| 上游返回 | 处理 | 原因 |
| --- | --- | --- |
| `events` 为空 | 映射为 `event_id=None`，继续处理 market | 合法 orphan market |
| 恰好一个有效 event | 映射为该 event ID | 明确的单一归属 |
| 多于一个 event | 跳过该 market，并生成可诊断 warning | 当前契约不支持任意选择一个归属 |
| event 引用无有效 ID | 跳过该 market，并生成 mapping warning | 身份数据不安全 |

多 event 返回不应再被当作“必须恰好一个”的笼统错误，而应明确为当前 `0..1` 契约无法安全解析。它不能使其他合法 market 丢失；sync 可以保存部分结果并标记本轮不完整。

如果 market 带有非空 event ID，但本轮 event 响应和已提交 catalog 中都没有对应 event 行，则不能把它降级成 orphan，也不能创建伪 event。该 market 的新关系应被视为源数据不完整，保留旧的已提交关系（如有），并让本轮 sync 进入不完整路径。

## 各流程行为

### Sync

- `events=[]` 的 market 只要自身字段、token 和身份校验通过，就应写入 catalog。
- orphan market 不会因为没有 event 而使完整 generation 失败。
- 有 event 的 market 仍要求 event 行存在；非空 `event_id` 不得悬空。
- active event 没有 market 可以是合法状态，`Event.market_ids` 可以为空；不再以“每个 event 必须至少有一个 market”作为完整性条件。
- market 从 orphan 变为关联 event，或从一个 event 变为另一个 event，是关系更新，不是身份变化；必须按 market 更新发布并重建相关反向索引。
- market 从关联 event 变为 orphan 同样允许，但只有在上游明确返回空 event 数组且其他字段合法时才执行。
- token ID、condition ID、market ID 的稳定性校验保留。新 market 若有非空 event 关系，则 parent event 必须在同一代或已提交 catalog 中存在；新 orphan market 不需要 parent event。

`_prepare_complete` 与 `_prepare_incomplete` 都必须能处理 `event_id=None`，不能通过字典索引或 event 状态传播隐式假设 event 一定存在。event 状态只对已关联的 market 生效。

### Catalog 与展示

market 的基础详情、状态、token 和价格信息不依赖 event 才能展示。展示层应把 event 标记为“未关联”或等价状态，而不是隐藏 orphan market。

event 的 `market_ids` 展示本地已关联 market；不应把 orphan market 归入任意 event。

### Watch 启动与监控

`watch` 启动资格与 sync generation 解耦：

1. 应用初始化数据库并完成必要的只读完整性检查。
2. 执行首次 sync。
3. 若首次 sync 完整成功，按现有流程启动 watch。
4. 若首次 sync 返回不完整，但数据库中已有至少一个合法、可订阅的 market/token，则立即从最近一次已提交 catalog 启动 watch，同时保留 degraded/incomplete 状态通知并继续周期性 sync。
5. 若数据库没有可监控 catalog，则继续现有的 sync 重试；不能在完全没有可用数据时启动一个看似正常但没有监控对象的服务。

可监控条件只检查现有 watch predicate 所需的数据：market 处于可监控状态、token 行存在且字段合法、orderbook 订阅所需标志有效。它不要求 `event_id` 非空，也不要求本轮 sync `complete=True`。

因此，已经持久化的 orphan market 可以正常建立 orderbook 订阅并接收监控数据。sync 失败、mapping warning 或 generation incomplete 只影响 catalog 更新，不应清空或阻止已有 watch 订阅。

以下错误仍然是启动级错误，不应被这个 fallback 吞掉：数据库打不开、Schema 版本不支持、数据库完整性检查失败、watch 所需的基础设施初始化失败。对这些错误继续 fail closed。

### Signal

- market 级的 binary 和 logical/relation 上下文只要自身 market/token 输入合法，就可以继续评估 orphan market。
- 依赖 event 元数据的 neg-risk 等策略，在 `event_id=None` 或 event 不完整时返回不可评估/跳过，不伪造 event 上下文。如果event的确是neg-risk，但event 不完整需要输出错误日志。
- event-settled 等 event 级 change 必须携带非空 `event_id`；market 级 change 的 `event_id` 改为可选，并始终携带 `market_id`。
- event 关联变化必须触发相应 market 更新，使 signal manager 能刷新上下文；不能因为 change 没有 event 而丢弃 market 更新。

## 领域模型与同步改造

### Domain

- `Market.event_id` 改为 `str | None`，保留非空字符串校验。
- `Event.market_ids` 允许为空 tuple，仍然在 domain 层做排序、去重和 ID 格式校验。
- `MarketChange.event_id` 改为 `str | None`；只有 `EVENT_SETTLED` 等明确的 event 级变更要求非空 event ID。
- market 的稳定身份比较改为 `market.id + condition_id`；event_id 变化不再触发“身份不稳定”错误。

### Gateway 与 Catalog Sync

- 移除 `_map_market` 对 `len(events) == 1` 的强制要求，按上表处理 0、1、多 event。
- 把 event 关联处理成可选字段，所有 `_prepare_*`、变更生成和 affected-event 计算都必须过滤 `None`。
- `_validate_complete_source` 不再强制 event 与 market 的一一对应；保留非空 event 引用存在性、market/token 身份和上游多 event 歧义校验。
- 完整保存时保留没有 market 的 event，并由最终 market 集合重建每个 event 的 `market_ids`。
- 不完整保存时保留已有安全数据；新 orphan market 可直接保存，新关联 market 仍必须有可验证的 event parent。
- publication marker 的受影响 market 计算不能只按 `change.event_id` 查找；对 orphan 或 event 关系变化必须使用 `market_id`。

## Persistence 与 Schema v2

### Schema

Schema v2 与 v1 的主要差异是：

```sql
markets.event_id TEXT REFERENCES events(id)
```

即允许 NULL，但非 NULL 值仍受外键约束。`events.market_ids_json` 保留为非空 JSON 数组字段，允许 `[]`。

除关系语义所需的约束变化外，不新增无关字段或表；不通过删除数据来“修复”历史关系。

### Repository

repository 的保存操作必须在同一事务内完成：

1. 读取受影响 market 的旧 event ID。
2. upsert event、market 和 token。
3. 对旧、新 event ID 中的非空 ID，依据 `markets` 表实际行重算 `events.market_ids_json`。
4. 提交前执行外键和行级数据校验。

`save_market` 对 `event_id=None` 不再要求 parent event；对非空 event ID 仍要求 event 行存在。`save_event` 不再把调用方传入的 `market_ids` 当作强制关联集合，而是在保存后写回本地派生值。

### Integrity 与 Doctor

`check_database_integrity` 在 v2 下应检查：

- schema version 为支持的 v2；
- 所有 market/token 的 ID 与字段满足现有约束；
- 非空 `markets.event_id` 都能解析到 `events.id`；NULL `event_id` 合法；
- 每个 `events.market_ids_json` 都是合法、去重、规范化的数组；
- 数组内容恰好对应本地 `markets.event_id` 反向索引，允许空数组；
- SQLite foreign key、唯一约束和基础表结构通过检查。

增加只读 `doctor` 检查/报告，至少输出：schema 版本、foreign key/integrity 状态、orphan market 数量、无 market event 数量、可监控 market/token 数量和最近 sync generation 状态。数据违反硬约束时返回非零结果；单纯存在 orphan market 不应被报告为错误。

## 显式迁移与回滚边界

### v1 到 v2

迁移必须由显式命令触发，例如：

```text
predmarket migrate --to 2 --database <db> --backup <backup>
```

迁移流程：

1. 要求服务停止并取得数据库独占操作窗口。
2. 只读预检查：确认 `user_version=1`、表结构为预期 v1、foreign key/integrity 检查通过；失败则不改库。
3. 使用 SQLite backup API 或等价的原子备份方式生成用户指定的 backup artifact；备份成功前不进入 schema 变更。
4. 在事务中重建 `markets` 表，把 `event_id` 复制为可空列，保留所有原始数据和索引，再更新 `user_version=2`。
5. 事务内执行 v2 完整性检查；全部通过后提交，否则回滚事务。

v1 数据本身没有 orphan，因此迁移不会补写 event，也不会修改现有关系。后续 sync 才能新增 orphan market。

应用启动遇到 v1 数据库时必须提示“需要显式迁移到 v2”，不能隐式迁移、自动设定 event_id、删除 orphan 或重写反向索引。

### 回滚

- 正向迁移失败由事务回滚，原数据库保持 v1。
- 已提交的 v2 迁移通过迁移时生成的 backup artifact 回滚；恢复操作必须是显式命令/运维动作，并采用临时文件加原子替换，不能由应用启动自动执行。
- 逻辑 v2 到 v1 仅在数据库中不存在 orphan market、所有非空 event_id 均有效且反向索引满足 v1 约束时允许；否则拒绝降级并要求从备份恢复。
- 如果 v2 已经产生 orphan market，不能为了回滚而擅自删除 market 或选择一个 event 绑定；备份恢复是唯一安全回滚路径。

## 测试与文档

### Domain / Gateway

- `events=[]` 映射为 `event_id=None`。
- 一个 event 正常映射。
- 多 event 和无效 event 引用产生明确 warning 并跳过该 market。
- `Event.market_ids=()` 合法。
- event_id 从 NULL 到有效 ID、从有效 ID 到 NULL、从一个有效 ID 到另一个有效 ID 都不被判定为身份变化。

### Repository / Schema / Integrity

- v2 可以保存 orphan market、linked market 和无 market event。
- 保存、移动或删除 market 后，受影响 event 的反向索引正确重算。
- 非空悬空 event_id 被拒绝，NULL event_id 被接受。
- integrity/doctor 正确区分合法 orphan 与真正的外键/索引损坏。
- v1 到 v2 迁移保留数据、可重复验证、失败可回滚；v1 启动不会自动迁移。
- 含 orphan 的 v2 数据拒绝逻辑降级到 v1。

### Sync / Watch / Signal

- orphan market 能完成 catalog 保存并出现在展示数据中。
- sync 因其他 market mapping 问题返回 incomplete 时，已有 orphan/linked market 仍可供 watch 使用。
- 初始 sync incomplete 但数据库有可监控 token 时，Supervisor 启动 watch 并继续周期性 sync。
- 没有可监控 catalog 时仍按现有策略等待 sync，不伪造正常运行状态。
- orphan market 可以建立 orderbook 监控；binary/relation signal 可评估，event-dependent signal 安全跳过。
- market 级 change 在 event_id 为 NULL 时仍能刷新对应 market；event-settled change 不接受 NULL event_id。

### 文档

同步更新 Schema、sync、watch、完整性诊断和运行手册，特别说明：

- orphan market 是合法数据状态；
- `events.market_ids_json` 是本地派生索引；
- 初始 sync 不完整时 watch 的降级启动条件；
- 迁移必须显式执行以及备份恢复方式。

## 验收标准

完成实现后必须满足：

1. 关系基数、orphan 行为和多 event 处理规则有 domain、gateway、sync、repository、watch、signal 测试覆盖。
2. v2 数据库允许 NULL event_id，同时阻止非空悬空 event_id。
3. 本地 event 反向索引与本地 market 关系保持一致，但不再强制镜像上游 event 数组。
4. 初始 sync incomplete 不会阻止已有合法可监控 catalog 启动 watch；没有可用 catalog 时仍 fail closed。
5. v1 到 v2 迁移有备份、事务验证和明确回滚边界，应用启动不执行隐式破坏性修复。
6. 相关运行文档、integrity/doctor 输出和测试同步更新。

## 实现顺序

1. 先更新 domain 类型和 gateway 映射测试。
2. 再实现 Schema v2、显式迁移命令及 repository/integrity 语义。
3. 修改 catalog sync 的可选 event 处理和变更发布。
4. 修改 Supervisor 的 watch 启动门槛，并补齐 signal 行为。
5. 更新 doctor、文档和完整测试矩阵，执行窄测试后再跑完整测试。
