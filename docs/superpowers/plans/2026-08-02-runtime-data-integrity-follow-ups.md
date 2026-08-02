# 后续任务：catalog 关系、数值存储与启动检查

> 状态：待后续生成实施任务。本文件只记录方向，当前不修改运行逻辑。

## 背景与已确认问题

1. 业务上并不保证每个 `market` 都关联一个 `event`。
   当前 Schema v1 的 `markets.event_id TEXT NOT NULL REFERENCES events(id)`
   实际把它实现成了强制的一对多关系；`gateway` 的
   `events must contain exactly one event reference` 又额外强化了“恰好一个”
   的映射假设。两者都需要重新评估，不能继续当作业务不变量。
2. 当前启动路径会调用完整的 `check_database_integrity()`，包含
   `_check_decimals`、JSON、ID 数组、revision 和跨表一致性等应用层扫描。
   这些检查适合诊断，不适合成为每次启动的长耗时门槛。
3. SQLite 中多个 Decimal 字段目前以 `TEXT` 持久化。后续目标是统一为归一化的
   double 存储格式；这需要连同读写、迁移、精度和舍入策略一起设计，不能只替换
   SQLite 列类型。

## 待生成任务 A：重新定义 market/event 关系

- 明确允许的关系基数：market 可以无 event、关联一个 event，还是需要支持多个
  event；明确 orphan market 的同步、展示、watch 和 signal 行为。
- 放宽或重建 `markets.event_id` 的 `NOT NULL/FK` 约束，并处理现有
  `events.market_ids_json` 反向索引的语义；不能再要求两边数组严格一一对应。
- 移除或改造 gateway 中“必须恰好一个 event”的映射错误；保留真正影响身份、去重
  或数据安全的校验。
- 更新 domain、schema/repository、catalog sync、integrity/doctor 检查、文档和测试，
  覆盖无 event、已有 event、异常/多 event 返回等场景。
- 为 Schema v1 到新 schema 设计迁移与回滚边界；现有数据库不能靠启动时隐式破坏性
  修复。

## 待生成任务 B：统一 Decimal 的 SQLite 持久化格式

- 按目标将适用的 Decimal 字段归一化为 double 存储；先列出字段清单，并定义
  `NULL`、范围、精度、舍入、NaN/Infinity 和读取兼容规则。
- 评估 IEEE-754 double 对价格、数量、手续费和风控公式的误差影响；如果某些金额或
  公式仍要求精确十进制定点语义，应单独确定替代方案，不能未经确认直接丢失精度。
- 在 gateway/domain/repository 的入库边界统一归一化，保证所有新写入数据格式一致；
  更新读取、SQLite CHECK、索引、迁移脚本和历史数据转换策略。
- 用边界值和旧数据回读测试验证归一化结果、计算结果及向后兼容性，并同步修正文档
  中关于 Decimal 字符串存储的描述。

## 待生成任务 C：轻量启动检查与 `doctor` 命令

- 启动阶段只保留快速、运行必需的检查，例如 schema/version、SQLite 结构完整性、
  foreign key 基础检查和运行所需表是否存在；不再默认扫描全部应用数据。
- 将完整语义检查迁移到 `predmarket doctor`，至少覆盖当前
  `_check_id_arrays`、`_check_json_payloads`、`_check_decimals`、
  `_check_latest_revisions` 和 `_check_revision_payloads`，并随着新关系/数值格式更新。
- `doctor` 默认只读，输出按类别汇总的错误/警告、影响记录和退出码；默认不自动修复，
  修复能力另行设计并显式授权。
- 增加 CLI、集成测试和运维文档，明确启动失败与 doctor 发现问题的处理边界。
