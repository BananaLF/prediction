# SQLite Schema 精简设计

**日期：** 2026-07-26
**状态：** 对话中已确认，等待书面规格审阅
**目标 Schema 版本：** 7

## 目标

将 `predmarket` 的 SQLite schema 从 39 张项目表精简为 30 张。通过合并生命周期相同的记录，在减少表数量的同时，保留对市场历史、订单簿深度、通知审计、扫描明细和 WebSocket 事件的关系型查询能力。

本次变更明确不迁移 schema v6 数据。Schema v7 将从一个空数据库文件开始创建。

## 非目标

- 不修改扫描策略、经济模型、风险阈值、公开网络端点、CLI 命令语义或通知投递语义。
- 不把数据库改成纯 JSON 存储。
- 不自动删除、清空、覆盖或迁移旧数据库。
- 不保留通过 `report` 或 `replay` 读取 schema v6 数据的能力。
- 不重构与本目标无关的生产代码。

## 设计原则

1. `evidence_bundles.canonical_json` 继续作为不可变的规范证据。
2. 对于有界查询、一对多数据、当前状态或追加式审计明显受益于 SQL 的数据，继续保留关系表。
3. 生命周期完全一致且没有独立查询需求的记录合并存储。
4. 一份证据 bundle 必须在同一个事务内完成写入，不允许提交不完整证据。
5. 打开旧版 schema 时采用失败关闭策略，不修改旧文件。

## Schema 变更

### 1. 合并评估表

将以下表：

- `runs`
- `opportunities`
- `risk_assessments`

替换为：

- `evaluations`

`evaluations` 每个证据 bundle 保存一行，包含：

- `bundle_id`：主键，同时是指向 `evidence_bundles` 的外键；
- 运行 ID、机会 ID、开始时间、状态和流水线原因，使用可索引或可直接查询的列；
- 完整的运行、机会和风险 JSON；
- 通知意图 JSON，用来替代独立的 `notifications` 表。

当前不变量是一份 bundle 对应一次正式评估，因此这些记录拥有相同生命周期。`report` 将直接查询 `evaluations`，不再联结三张表。

### 2. 合并关系证据

将以下表：

- `relation_sets`
- `relations`
- `relation_states`
- `relation_payoffs`

替换为：

- `relation_evidence`

`relation_evidence` 每个 bundle 关系保存一行，以规范 JSON 保存完整关系集合、成员关系、状态和支付矩阵。ID、状态、腿和支付守恒仍在持久化前进行校验。关系证据通常作为一个完整单元回放，因此把它拆成四张独立关系表的收益不足。

### 3. 合并盘口 epoch 与快照

将以下表：

- `book_epochs`
- `snapshots`

替换为：

- `book_snapshots`

每条 `book_snapshots` 记录包含 token 引用、epoch 元数据、快照元数据和盘口 payload。`levels` 继续独立存在，并引用 `book_snapshots(bundle_id, id)`，因为每个快照拥有数量不定的价格档位，而且保留 SQL 深度分析能力仍有价值。

### 4. 将 watch 指标合入 watch 运行

删除 `watch_metrics`。`watch_runs` 成为唯一的运行汇总表，并在规范 JSON 中保存最终指标。现有的 `list_watch_metrics` 存储接口可以作为兼容方法保留，但改为读取最新的 `watch_runs` 记录。

`watch_events` 继续独立存在，因为它是有序且可能高频的事件流。

### 5. 将通知意图合入评估

删除 `notifications`。生成证据时的通知意图保存在 `evaluations` 行中。

继续保留：

- `notification_claims`：当前租约式 outbox 状态；
- `notification_attempts`：最终投递尝试；
- `notification_events`：追加式 claim 和状态转换历史。

这三张表具有不同生命周期，是崩溃恢复和投递不确定性审计所必需的。

### 6. 将目录诊断合入快照

删除 `catalog_diagnostics`。诊断信息继续保存在 `catalog_snapshots.canonical_json` 中；它本来就是不可变目录快照的一部分。

目录市场、事件、token、关系候选、同步运行和当前状态表继续保留关系结构，因为它们支持历史与当前状态查询。

## 目标表清单

Schema v7 恰好创建以下 30 张项目表：

1. `schema_migrations`
2. `evidence_bundles`
3. `evaluations`
4. `events`
5. `markets`
6. `tokens`
7. `fee_schedules`
8. `relation_evidence`
9. `book_snapshots`
10. `levels`
11. `legs`
12. `actions`
13. `latency_metrics`
14. `notification_claims`
15. `notification_attempts`
16. `notification_events`
17. `catalog_snapshots`
18. `catalog_sync_runs`
19. `catalog_markets`
20. `catalog_events`
21. `catalog_tokens`
22. `catalog_relation_candidates`
23. `current_catalog_markets`
24. `current_catalog_tokens`
25. `current_catalog_events`
26. `watch_runs`
27. `watch_events`
28. `scan_runs`
29. `scan_candidates`
30. `research_observations`

`sqlite_sequence` 等 SQLite 内部表不属于项目表，不计入以上数量。

## 数据流

### 新数据库初始化

1. 打开一个不存在或为空的 SQLite 文件。
2. 启用外键和 WAL。
3. 创建 schema v7 的所有表。
4. 写入 schema 迁移版本 7，并设置 `PRAGMA user_version = 7`。
5. 提交初始化事务。

### 旧数据库处理

如果 `PRAGMA user_version` 非零且小于 7，打开操作将明确报错，说明旧数据不受支持，操作者必须使用新的数据库路径或手动删除旧数据库。

在版本检查通过前，打开流程不得执行 DDL、更新 `user_version`、清空表或删除文件。高于版本 7 的数据库同样采用失败关闭策略。

### 证据保存

1. 校验完整证据映射并生成规范 JSON。
2. 开始一个 SQLite 事务。
3. 写入 `evidence_bundles`。
4. 写入合并后的 `evaluations` 和 `relation_evidence`。
5. 写入市场、token、费率、盘口快照、深度、腿、动作和延迟记录。
6. 只有所有写入和外键约束都成功后才提交事务。
7. 任一步骤出错都回滚整个事务。

### 证据回放

`replay` 从规范化表重建证据，包括合并后的 JSON payload，并与 `evidence_bundles.canonical_json` 比较。它继续把不可变核心证据与通知审计分开返回。

### 报告

`report` 从 `evaluations`、证据 JSON 和其余子表取得状态、流水线原因、经济数据、风险原因、路径和延迟。通知审计继续按所选 bundle ID 限定范围。最近的 WebSocket 指标从 `watch_runs` 读取。

## 错误处理

- 旧版 schema：修改前抛出清晰的 `RuntimeError`。
- 未来版本 schema：修改前抛出清晰的 `RuntimeError`。
- 无效证据：在开始写事务前拒绝。
- 重复的不可变证据：保持现有幂等行为。
- 相同 ID 对应冲突证据：拒绝且不修改现有 bundle。
- 子表写入或外键检查失败：回滚整个 bundle。
- 合并 JSON 损坏：`validate_opportunity` 和 `replay` 报告 schema 或一致性错误，不静默信任其中一种表示。

## 现有数据库替换

仓库当前 `data/predmarket.sqlite3` 中的数据不需要保留。替换操作必须在 schema v7 实现和测试通过后显式执行：

1. 确认没有 scanner 或 watcher 进程正在使用数据库。
2. 确认配置路径准确指向本项目数据库。
3. 删除旧主数据库以及匹配的 `-wal`、`-shm` 辅助文件。
4. 执行初始化命令或有界命令，创建全新的 schema v7 数据库。
5. 执行 `PRAGMA integrity_check`、`PRAGMA foreign_key_check`，并确认项目表数量为 30。

应用启动时不会自动执行删除。

## 测试策略

### Schema 测试

- 新建空数据库时恰好创建指定的 30 张项目表。
- `PRAGMA user_version` 为 7。
- WAL 和外键已启用。
- `PRAGMA foreign_key_check` 不返回任何违规记录。

### 兼容性测试

- 人工构造的 schema v6 数据库会被拒绝。
- 拒绝后，其 `user_version`、表和哨兵记录保持不变。
- 未来版本数据库同样被拒绝且不发生修改。

### 持久化与回放测试

- 保存并回放被拒绝、研究候选和快照可执行三类证据。
- 准确重建合并后的评估、关系、盘口和通知意图数据。
- 能发现规范 JSON 或合并 payload 损坏。
- 验证重复证据保持幂等，冲突证据写入失败。
- 强制制造子表写入失败，并证明数据库中没有残留部分 bundle。

### 命令集成测试

- `scan-once` 能在 schema v7 中保存证据。
- `report` 保持现有公开结果结构。
- `replay OPPORTUNITY_ID` 和 `replay --bundle-id` 返回一致证据。
- `watch` 把最终指标写入 `watch_runs`，报告返回最近一次运行。
- 通知 claim、attempt 和 event 行为保持不变。

### 最终验证

- 运行完整测试套件。
- 运行只读安全边界测试。
- 运行 `git diff --check`。
- 创建全新数据库并执行 `PRAGMA integrity_check`。
- 执行 `PRAGMA foreign_key_check`，要求不返回记录。
- 查询 `sqlite_schema`，要求恰好包含指定的 30 张项目表。

## 文档更新

更新：

- `README.md`
- `docs/PROJECT-GUIDE.md`
- `docs/TUTORIAL.md`
- `docs/OPERATIONS.md`
- `docs/VERIFICATION.md`

文档必须明确说明 schema 版本为 7、30 张表的组织方式、不支持 v6 迁移，以及显式替换空数据库的操作流程。

## 交付约束

- 保留脏工作树中所有与本任务无关的用户改动。
- 未经明确授权，不提交、不推送、不部署，也不修改外部系统。
- 在实现和最新验证成功前，不删除当前数据库。
