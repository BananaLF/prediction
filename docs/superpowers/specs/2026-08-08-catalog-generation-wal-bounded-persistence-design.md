# Catalog 代次化与 WAL 有界持久化设计

**日期：** 2026-08-08

**状态：** 等待书面规格审阅

**目标 Schema 版本：** 4

## 1. 背景

第一阶段已经把完整 catalog 同步产生的逐市场控制消息收敛为每代一个
`CATALOG_RECONCILED`，并通过 `CATALOG_RECONCILIATION_READY` outbox 保证提交后发布。
真实全量同步不再出现事件风暴，但当前 `save_complete_catalog()` 仍在一个
`BEGIN IMMEDIATE` 事务中先更新全部 event、market、token 的 generation，再 upsert
语义变化实体。

2026-08-08 的生产数据副本观测为：

- 38,264 events；
- 223,244 markets；
- 446,488 tokens；
- 19,850 event upserts；
- 16,420 market upserts；
- 272,942 token upserts；
- WAL 峰值约 727 MiB（762,397,792 bytes）。

单个大事务在提交前无法 checkpoint，因此即使跳过未变化业务字段，完整目录的
generation 推进与大量 token 写入仍会把 WAL 推高。本阶段必须改变目录的物理发布
模型，而不是继续微调现有 UPSERT。

## 2. 目标与成功标准

采用 copy-on-write 版本表和单一活动代指针，把完整同步拆成可 checkpoint 的短事务，
同时保留 catalog 的原子可见性、崩溃恢复和 Watch 刷新新鲜度。

在生产数据副本执行真实完整同步时必须满足：

1. WAL 文件峰值不超过 128 MiB，相对当前约 727 MiB 至少下降约 82%。
2. 任意读取事务只看到完整旧代或完整新代，不看到混合代次。
3. 暂存、失败或废弃代次不可见。
4. 完整同步期间发生的较新 Watch 更新不会被较旧同步结果覆盖。
5. 进程在写批次、rebase、激活、outbox 或清理阶段崩溃后可安全恢复。
6. Schema v3 数据无损迁移到 v4，迁移前自动保留 v3 备份。
7. `load_catalog` 和关键查询相对 v3 基线的耗时退化不超过 20%。

WAL 上限适用于本应用管理的连接。检测到无法回收 WAL 的外部长读事务时，本轮完整
同步必须在安全阈值前终止，而不是继续消耗 WAL。

## 3. 非目标

- 不支持旧版程序直接打开 Schema v4。
- 不提供成功迁移后的自动降级或双写兼容。
- 不把历史 catalog 版本作为产品功能长期保留。
- 不改变上游 Polymarket 抓取、catalog 语义 diff 或第一阶段队列聚合语义。
- 不引入 PostgreSQL、外部分布式锁或消息中间件。
- 不通过定期大型 `VACUUM` 控制日常磁盘占用。

## 4. 已确认的核心决策

- Schema 升级到 v4，并保留已有数据。
- 使用 copy-on-write 版本表加 `active_generation_id`，不采用原表分批原地更新。
- 未变化实体不为每个 generation 重写 payload。
- 新代在全部校验完成前保持 `STAGING`，仅通过一个小事务切换活动指针。
- Watch 继续即时更新活动 catalog；激活前通过 revision journal、rebase 和 CAS 保证新鲜度。
- 迁移使用同文件系统的旁路 v4 数据库；原 v3 文件在切换时成为自动备份。
- 成功切换后不支持旧二进制回退；备份仅用于人工灾难恢复。
- 激活后分批清理被支配的旧版本，不永久保留多代历史。

## 5. 数据模型

### 5.1 Generation 元数据

新增 `catalog_generations`，概念字段包括：

- 单调递增的内部 `id`；
- 唯一的外部 `sync_generation`；
- `base_generation_id`；
- `base_runtime_revision` 与 `rebased_runtime_revision`；
- `STAGING`、`COMMITTED` 或 `ABORTED` 状态；
- 分实体的计划数、已写数、摘要和持久化游标；
- 创建、校验、激活、废弃时间；
- 失败原因与恢复信息。

新增单行 `catalog_state`：

- `active_generation_id`；
- 单调递增的 `runtime_revision`；
- 可选的清理游标和最后 checkpoint 状态。

`active_generation_id` 只能指向 `COMMITTED` generation。应用层和完整性检查共同保证
只有一个活动代。主进程同一时间最多允许一个 `STAGING` generation，避免两个候选代共享
不同的 runtime revision 基线和清理保留边界。

### 5.2 稳定身份与 payload 版本

将当前实体拆为稳定身份表和 payload 版本表：

- `catalog_event_ids` / `event_versions`；
- `catalog_market_ids` / `market_versions`；
- `catalog_token_ids` / `token_versions`。

稳定身份表保存实体 ID 以及保持引用完整性所需的不可变父子身份。relations、signal
legs 和其他下游外键引用稳定身份表，不引用版本行。

版本表以 `(entity_id, generation_id)` 唯一，保存该实体的完整业务 payload、来源
`updated_at` 和审计时间。新同步只写语义变化实体；未变化实体继续由较早的已提交版本
提供 payload。

版本表至少建立：

- `(entity_id, generation_id DESC)`；
- `(generation_id, entity_id)`；
- 当前业务查询所需的 slug、condition、market、outcome position 等索引。

### 5.3 有效读取视图

现有 `events`、`markets`、`tokens` 名称改为只读视图。对每个稳定身份，视图选择：

1. generation 状态为 `COMMITTED`；
2. generation ID 不大于活动代；
3. generation ID 最大的 payload 版本。

视图把逻辑 `sync_generation` 投影为当前活动代的外部 generation，并始终投影
`sync_generation_complete = 1`。因此未变化实体即使物理 payload 来自更早代次，策略和
完整性检查仍看到同一完整逻辑代。

新实体在存在可见版本前不会出现在视图中。`STAGING` 和 `ABORTED` 版本永远不可见。
视图不提供写触发器；所有 catalog 写入必须显式经过 repository，错误的直接写入应立即
失败。

### 5.4 约束边界

实体 ID、不可变父子关系和下游引用由数据库外键保证。跨版本无法直接表达的当前快照
约束，例如 slug、condition ID、market/outcome position 唯一性，在 generation 激活前
对有效候选快照统一验证。违反约束的 generation 标记为 `ABORTED`，不得切换指针。

## 6. 原子可见性

完整同步的 payload 写入全部发生在未来的 `STAGING` generation。活动读者继续通过
`catalog_state.active_generation_id` 读取旧代。

完成写入、摘要核对、当前快照约束检查和 Watch rebase 后，最终激活事务只执行：

1. CAS 检查 `runtime_revision`；
2. 把 generation 标记为 `COMMITTED`；
3. 更新 `active_generation_id`；
4. 写入该代唯一的 `CATALOG_RECONCILIATION_READY` outbox。

这些操作在同一个短事务中提交。SQLite 读取事务的快照语义保证读者只能观察到完整旧代
或完整新代。重型摘要和快照验证在激活事务前完成；最终事务只验证已冻结结果和 CAS，
避免重新形成大事务。

## 7. 分批持久化

### 7.1 写入协议

`save_complete_catalog()` 从单命令大事务改为 generation 协调器：

1. 创建或恢复匹配输入摘要的 `STAGING` generation。
2. 按 event、market、token 的依赖顺序写入稳定身份和变化版本。
3. 每批在独立短事务中提交，同时更新 generation 的计数、摘要和 keyset 游标。
4. 每批提交后采样 WAL 并调度 checkpoint。
5. 所有批次完成后冻结 generation，执行全量候选快照验证。
6. 完成 Watch rebase 和 CAS 激活。

单批初始上限为 8,000 行或估算 payload 16 MiB，任一先到即提交。协调器根据上一批实际
WAL 增量、事务耗时和 checkpoint 结果动态缩小或放大批次。固定行数不是安全边界，实际
WAL 字节才是背压依据。

`DatabaseWriter` 仍是主进程唯一写 actor，但需要支持：

- generation 批次命令；
- 事务外 checkpoint 命令；
- 完整同步暂停时继续公平处理 Watch、signal 和 outbox 写入；
- 明确禁止在普通命令的活动事务中执行 checkpoint。

### 7.2 WAL 水位与背压

初始控制水位如下，最终可通过真实数据测试向下收紧，但不能放宽 128 MiB 验收上限：

- 开始完整同步前：先把 WAL 回收到 16 MiB 以下；无法做到则不创建 `STAGING` generation；
- 约 48 MiB：提交后执行非阻塞 `PRAGMA wal_checkpoint(PASSIVE)`；
- 约 64 MiB：暂停新的同步批次，在写队列安全点尝试 `RESTART` 或 `TRUNCATE`；
- checkpoint busy：Watch 等实时命令继续运行，完整同步保持暂停并有界重试；
- 接近 96 MiB 仍无法回收：安全终止本轮完整同步并将 generation 标记为 `ABORTED`。

每批提交前用已观测的最坏 WAL 放大系数预测增量，必须为终止记录及实时写入保留至少
32 MiB 余量；预测可能越过 96 MiB 时先缩批或 checkpoint，不得试写后再判断。首次没有
历史放大系数时使用最保守的小批量，并在真实数据验收中校准。

终止同步不改变活动指针。checkpoint 调度必须快速返回，不能以阻塞方式长期占用 writer。
应用内所有 catalog 读取事务都应保持短生命周期，以免阻止 WAL 回收。

## 8. Watch 并发与新鲜度

当前 UPSERT 使用 `excluded.updated_at >= existing.updated_at` 防止较旧同步覆盖较新 Watch
刷新。v4 保留完全相同的胜出规则。

Watch 每次产生实际 catalog 写入时，在一个事务中：

1. 读取当前活动 generation，并验证本次修改不会破坏活动快照的唯一性和父子约束；
2. 写入或更新当前活动 generation 的实体版本；
3. 递增 `catalog_state.runtime_revision`；
4. 把实体类型、实体 ID、新 revision 和 `updated_at` 写入
   `catalog_runtime_changes`。

完整同步创建时记录 `base_runtime_revision`。激活前读取其后的 change journal，对每个受影响
实体比较活动版本和暂存候选：

- Watch 版本更新时，复制到暂存 generation；
- 同步候选更新或时间相等时，保留同步候选，匹配现有 `>=` 语义；
- market 与 token 组合更新保持 repository 现有事务边界。

每轮 rebase 都要重新校验受影响的唯一性、父子约束和候选摘要，然后才记录
`rebased_runtime_revision`。最终激活事务要求当前 revision 与之相等；不相等则回滚激活、
读取新增 journal、再次 rebase。由于写入由同一个 writer 串行化，最终 CAS 可以排在
rebase 后立即执行，持续 Watch 流量不会破坏一致性。

激活后排队的 Watch 命令读取新的活动指针并自然写入新代。Watch 的读取始终经过有效视图，
不会看到尚未提交的同步候选。

## 9. Schema v3 到 v4 迁移

### 9.1 前置条件

迁移只在业务写入停止且取得进程级独占迁移锁后运行。启动器先检查：

- 当前版本确为 v3；
- 数据库、WAL 和 SHM 已安全关闭或 checkpoint；
- 临时 v4 文件与正式库位于同一文件系统；
- 可用空间足以容纳原 v3、预计 v4 和安全余量；
- 不存在未解决的旧迁移 marker。

空间不足或无法获得独占锁时拒绝迁移，不触碰原数据库。

### 9.2 旁路构建

迁移器创建唯一临时 v4 数据库，原 v3 始终保持只读：

1. 创建 v4 schema，但暂不暴露最终有效视图。
2. 创建一个合成的基准 `COMMITTED` generation。
3. 用 keyset 分页分批复制全部 event、market、token 身份和 payload。
4. 把所有迁移实体的逻辑 generation 统一投影为基准 generation，保留原业务字段和
   `updated_at`。
5. 按外键顺序复制 relations、signals、system events、游标和其他业务表，并把外键改为
   稳定身份表。
6. 每批提交、记录进度并执行与运行期相同的 WAL 控制。
7. 创建最终视图、索引和触发器，设置 `PRAGMA user_version = 4`。

### 9.3 切换前验证

必须全部通过：

- 所有业务表行数核对；
- v3 表与 v4 有效视图的规范化全量摘要核对；
- `PRAGMA integrity_check`；
- `PRAGMA foreign_key_check`；
- 当前快照唯一性、父子关系和孤儿检查；
- `load_catalog`、信号查询和关键 JOIN 冒烟测试；
- v4 schema 版本和活动指针检查。

失败时删除临时结果并保留原 v3 活动库。

### 9.4 自动备份与文件切换

验证后关闭并同步全部 SQLite 文件，通过正式库旁边的持久化 sidecar marker 记录切换阶段。
marker 的每次阶段更新都以临时文件写入、原子重命名和父目录同步完成，然后才执行下一步：

1. 把原正式 v3 文件重命名为带时间戳的 `*.pre-v4.sqlite3`；该原始文件即自动备份。
2. 把已验证的临时 v4 文件重命名到正式路径。
3. 同步父目录元数据并将 marker 标记完成。

若在两次重命名之间崩溃，v4 启动器依据 marker、临时库校验结果和备份恢复唯一有效的正式
路径。成功启动 v4 后不允许自动降级，备份也不自动删除。人工灾难恢复必须停止服务、归档
当前 v4、显式恢复 v3 备份并接受迁移后数据丢失。

## 10. 崩溃恢复

- **批次写入中：** 活动指针不变；输入摘要匹配时按持久化游标续写，否则废弃旧暂存代。
- **候选验证失败：** 标记 `ABORTED`，保留诊断元数据，payload 后台清理。
- **rebase 中：** 根据 journal 和 revision 幂等重做。
- **激活前：** 旧代继续可见。
- **激活事务中：** generation 状态、活动指针和 outbox 全部提交或全部回滚。
- **激活后、队列发布前：** 第一阶段的 ready outbox 恢复逻辑重新发布。
- **旧版本清理中：** 活动代不受影响，下次按清理游标继续。
- **迁移切换中：** 启动器只在验证通过时推进临时 v4，否则恢复原 v3 正式路径。

启动 doctor 增加残留 `STAGING`、异常活动指针、迁移 marker、journal 积压和清理积压检查。

## 11. 旧版本与日志回收

本阶段不提供历史代查询。激活成功后：

- 每个实体只保留当前可见的最新 payload 版本；
- 被新版本支配的已提交版本分批删除；
- `ABORTED` generation 的 payload 分批删除，generation 诊断行可按短期策略保留；
- runtime change journal 保留到所有引用该 revision 范围的 `STAGING` generation 完成
  rebase，之后分批裁剪；
- 清理共享批次、WAL 水位、checkpoint 和恢复游标机制。

无暂存 generation 时，版本行数应接近稳定身份行数。SQLite 释放页交给后续写入重用；日常
流程不运行大型 `VACUUM`，但记录 page count、freelist count 和逻辑可回收版本数。

## 12. 查询兼容与性能

所有读取 `events`、`markets`、`tokens` 的 repository、strategy、signal、relation、doctor
和 CLI SQL 都必须纳入兼容审计。原则上保留现有表名视图以减少调用方变更，但不能假设视图
性能天然足够。

实施时对以下查询保存 v3 基线并执行 `EXPLAIN QUERY PLAN`：

- 完整 `load_catalog`；
- event-market-token 连接；
- Watch 可监控市场加载；
- SignalManager generation 和 eligibility 检查；
- relation/integrity/doctor 查询；
- 按 ID、slug、condition 和 token 定位的点查询。

不得出现对版本表的无索引相关全扫描。真实数据下关键查询耗时相对 v3 基线不得恶化超过
20%；若通用视图无法满足，应在 repository 使用等价的索引友好 CTE，但不能改变原子可见
语义。

## 13. 可观测性

新增结构化日志和指标：

- generation 状态、进度、输入摘要和各阶段停留时间；
- 每批实体数、估算 payload、事务耗时、WAL 增量；
- WAL 当前字节数和本轮峰值；
- checkpoint 模式、耗时、busy 次数和结果页数；
- 完整同步背压、暂停、恢复和安全终止原因；
- runtime revision、rebase 实体数和 CAS 重试次数；
- 激活事务耗时和 outbox change ID；
- 待清理版本、废弃 generation 和 journal 积压；
- 活动代的 event、market、token 数量和一致性检查结果。

日志不能为每个实体输出一行；沿用阶段、批次和 generation 级聚合。

## 14. 验证计划

### 14.1 单元测试

- 有效视图选择最新可见的已提交版本。
- 未变化实体跨代继承 payload，同时投影活动逻辑 generation。
- `STAGING`、`ABORTED` 和未来版本不可见。
- 当前快照唯一性验证拒绝冲突候选。
- batch keyset、摘要、游标和恢复幂等。
- WAL 水位状态机和 checkpoint busy 背压。
- Watch 的 `updated_at >=` 胜出规则与 runtime revision CAS。
- 旧版本和 journal 清理边界。

### 14.2 集成测试

- 读循环跨激活事务时只观察旧代或新代。
- 同步写批次与 Watch 刷新交错，激活后保留较新数据。
- CAS 失败后重新 rebase 并成功激活。
- 在批次提交前后、验证、rebase、激活、outbox、checkpoint 和清理位置注入崩溃。
- checkpoint 被长读事务阻塞时停止同步且不切换活动代。
- v3 数据库迁移前后行数、摘要、外键和关键查询一致。
- 迁移在每个 marker 阶段崩溃后恢复唯一正式数据库。
- 旧 v3 二进制面对 v4 时明确拒绝启动。

### 14.3 真实数据验收

在生产数据库副本执行至少一次完整同步，并并发运行 Watch 与持续读一致性探针：

- 采样 WAL 文件大小并证明峰值不超过 128 MiB；
- 比较同步前后的 semantic delta、活动实体数和 outbox 数量；
- 验证只有一个 `CATALOG_RECONCILIATION_READY` 和一个聚合队列消息；
- 验证不存在 mixed generation 或 stale Watch 回退；
- 比较 `load_catalog` 和关键查询的 v3/v4 延迟；
- 清理完成后确认不存在可淘汰 payload 版本。

最终运行全量现有测试。只有真实数据 WAL、原子可见性、并发新鲜度、崩溃注入、迁移一致性
和查询性能全部达标，Schema v4 才可合入和部署。

## 15. 实施边界

实施应按以下顺序分解，但具体任务由后续 implementation plan 定义：

1. v4 schema、有效读取和约束测试；
2. 旁路迁移器及迁移故障恢复；
3. generation 分批 writer 与 checkpoint 背压；
4. Watch journal、rebase 与激活 CAS；
5. repository、直接 SQL、doctor 和监控适配；
6. 回收器、故障注入、性能基线与真实数据验收。

在设计规格获得书面批准前不开始实现。
