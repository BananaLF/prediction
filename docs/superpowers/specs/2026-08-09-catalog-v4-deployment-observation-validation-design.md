# Catalog v4 部署验收与持续观察设计

**日期：** 2026-08-09

**状态：** 设计已批准，等待实施批准

**关联 Issue：** [#20](https://github.com/BananaLF/prediction/issues/20)

## 1. 目标与边界

本阶段只完成 catalog v4 上线验收和持续观察的基础能力，目标是让一次部署和
随后 30 分钟运行都能产出可复查、可归因的本地证据。生产库的停机、重置、迁移、
全量同步和启动由操作人员按 runbook 执行；本阶段不自动触碰生产数据库。

本阶段不实现：

- 真实交易、钱包、下单或收益结算；当前服务仍是 Polymarket 公共数据只读观察器；
- CONVERSION_FAILURE、FIRST_LEG_ONLY、PARTIAL_SALES、SPLIT_ONLY 等问题的策略修复；
- 新套利策略的研究、实现或外部资料验证；
- 通过运行时埋点、网络采集或新依赖扩大业务代码范围。

## 2. 当前事实

- `origin/main` 已包含 Schema v4、copy-on-write catalog generation、WAL 水位控制、
  v3→v4 迁移和只读数据库 doctor。
- PR #18（设计）和 PR #21（实现）已经合并；Issue 正文中关于“PR 未合并”的描述已过期。
- 当前 `README.md`、`docs/OPERATIONS.md`、`docs/VERIFICATION.md` 和
  `docs/PROJECT-GUIDE.md` 仍有 v2/v3 说明，需要统一到 v4。
- 现有 v4 测试覆盖代次原子可见性、迁移、WAL 上限和完整性，但没有面向操作人员的
  30 分钟报告入口。
- `arbitrage_signals`、`signal_revisions`、`signal_legs`、`orderbook_snapshots` 和
  `system_events` 已经保存信号计算、风险标记、腿、订单簿快照和运行事件所需的主要证据。

## 3. 方案比较

### 方案 A：只更新运维文档

把 v2/v3 文档改为 v4，并补充人工 SQL 检查清单。

优点是改动最小、风险最低；缺点是 30 分钟期间需要人工重复查询，WAL 峰值、代次漂移、
信号变化和异常事件难以形成统一证据，不满足 Issue 对“可复查记录”的要求。

### 方案 B：独立只读验证探针（推荐）

新增 `scripts/validate_catalog_v4.py`，通过 SQLite `mode=ro` 读取指定数据库，按固定
间隔采样数据库和 `-wal` 文件，输出机器可读 JSON 报告；同时更新 v4 运维和验证文档。

优点是无需改变运行时写路径，不引入依赖，既能在临时副本测试，也能在服务运行时采样；
失败项有稳定 code，便于后续 Issue #20 的诊断和优化阶段复用。缺点是它只能报告当前数据库
已有的证据，无法凭空证明真实成交或 realized return，因此必须明确区分估算值和实际收益。

### 方案 C：把观测能力内置到运行时

在 `predmarket run` 中增加周期性 metrics、报告文件和额外 system events。

它能提供更完整的进程内上下文，但会扩大 writer、运行时生命周期和 schema 的改动面，
使观测工具与被观测对象共享故障面；对本阶段“验收基础”的收益不足以抵消风险。

选择方案 B。

## 4. 推荐设计

### 4.1 验证探针接口

新增：

```console
python scripts/validate_catalog_v4.py \
  --database data/predmarket-v1.sqlite3 \
  --duration-seconds 1800 \
  --interval-seconds 30 \
  --output reports/catalog-v4-<timestamp>.json
```

接口约束：

- `--database` 必填；不读取配置并不修改数据库；
- 默认一次预检加 30 分钟采样，测试可传 `--duration-seconds 0` 只执行一次；
- `--interval-seconds` 必须为正整数；
- 连接使用 SQLite 只读 URI，探针不得创建、checkpoint、删除或写入 WAL/SHM；
- stdout 输出简短摘要，`--output` 写完整 JSON；父目录不存在或参数无效时退出码为 `2`；
- 发现验收失败时退出码为 `1`，健康报告退出码为 `0`，语义与 `doctor` 一致。

### 4.2 每次采样内容

每次采样记录时间戳、数据库路径、主库字节数和 WAL 字节数，并执行以下只读检查：

1. **Schema 与 SQLite：** `PRAGMA user_version = 4`、`PRAGMA integrity_check = ok`、
   `PRAGMA foreign_key_check` 无结果、v4 表和 `events/markets/tokens` 视图存在。
2. **Catalog 代次：** 活动代存在且为 `COMMITTED`；活动视图中的 event/market/token
   逻辑 generation 一致；`STAGING`、候选、journal backlog、cleanup backlog 和未完成迁移
   marker 均单独计数并报告；不把 doctor 的 warning 隐藏成成功。
3. **WAL：** 记录当前字节数、整个观测窗口的 `peak_bytes` 和采样点；`peak_bytes` 的硬
   验收值为 `128 * 1024 * 1024`，超限生成稳定失败 code，但探针不负责终止服务。
4. **运行状态：** 记录 `system_events` 的窗口增量，按 component、severity、event_type
   聚合，并保留 WARNING/ERROR/FATAL 的事件摘要和时间范围；记录信号、revision、订单簿
   快照和 runtime revision 的窗口增量。
5. **信号证据：** 对窗口内新建或更新的 signal 记录 strategy、execution mode、status、
   latest revision、`expected_profit`、`return_rate`、`worst_case_loss`、`risk_rate`、
   `unhedged_notional`、`risk_flags`、calculation/closure context 和 legs 摘要；按
   `close_reason`、risk flag 和 strategy 聚合，确保每条信号可回到数据库记录。
6. **订单簿与元数据：** 记录 snapshot 的交易所时间、接收时间、subscription generation、
   tick size、minimum order size，以及每个相关 snapshot 的 BID/ASK 深度汇总。缺失快照、
   费用或元数据时报告 `not_observed`/对应 failure code，不用默认值冒充可执行性。

### 4.3 收益分类

报告必须显式输出能力边界：

- `theoretical_estimate`：策略计算产生的 expected profit/return/risk；
- `orderbook_checked_estimate`：存在相关订单簿和费用/最小订单元数据，且通过本地可执行性
  检查的估算；这仍不是成交；
- `simulated`：本阶段不产生，除非已有记录明确标识模拟来源；
- `realized`：固定为 `unsupported`，因为服务没有钱包、订单和成交回报。

这样可以避免把 `expected_profit` 或信号数量误报成真实收益。

### 4.4 报告结构

JSON 顶层固定包含：`schema_version`、`tool_version`、`database`、`started_at`、
`ended_at`、`duration_seconds`、`status`、`exit_code`、`checks`、`samples`、`aggregates`
和 `limitations`。每个 check 包含 `code`、`status`、`observed`、`expected` 和必要的
`records`；报告只保留有界摘要，不把整个订单簿或完整 payload 复制到报告。

### 4.5 文档与操作流程

同步更新 README、`docs/OPERATIONS.md`、`docs/VERIFICATION.md` 和
`docs/PROJECT-GUIDE.md` 中的 v2/v3 描述，统一写明：

1. 停止所有 `predmarket` 进程并确认数据库备份/路径；
2. 按现有 `predmarket migrate --to 4 --database PATH` 流程完成迁移或初始化 v4；
3. 迁移后先运行一次 `doctor` 和验证探针预检；
4. 启动只读观察器，运行 30 分钟探针并保存 JSON 报告；
5. 以报告中的 schema、完整性、代次、WAL、事件和信号证据逐项判断验收；
6. 若失败，保留报告和数据库/日志证据，停止在“诊断/修复”阶段，不把失败信号当作收益。

实际生产迁移、重置、全量同步和启动不作为自动化测试步骤，也不由探针触发。

## 5. 验收标准

### 自动化验收

- 健康 v4 临时数据库的一次采样返回 `0`，包含 user_version、SQLite integrity、FK、活动代、
  WAL 和空窗口统计；
- 构造 v3、损坏、FK 失败、活动代无效、WAL 超限和未完成迁移 marker 的 fixture，分别返回
  稳定失败 code 和退出码 `1` 或 `2`；
- 固定时间源/采样器后，30 分钟逻辑窗口可确定性生成，`peak_bytes` 等于采样最大值；
- 报告能从 signal 到 revision、legs、orderbook snapshot 和 system event 建立 ID 关联；
- 现有完整测试和文档命令测试全部通过，`git diff --check` 通过；
- 探针测试证明不会创建或修改数据库、WAL、SHM 或目标报告之外的文件。

### 操作验收

在获得单独的生产操作授权后，使用数据库副本或目标环境执行一次完整流程，报告至少证明：

- `PRAGMA user_version = 4`、integrity/FK 正常；
- 活动 catalog 只有一个完整已提交代次，未留下不可解释的 staging/journal/cleanup backlog；
- 观测窗口 WAL `peak_bytes <= 128 MiB`；
- 每条窗口内信号都有计算、风险、腿和订单簿证据，无法观察的字段明确标为 unavailable；
- 所有异常事件均可从 `system_events` 或报告样本追溯；
- 报告明确声明没有 simulated/realized execution evidence。

## 6. 风险与后续拆分

- 只读探针不能证明上游 API 的完整可用性；网络连通性和数据源新鲜度仍需结合运行日志判断。
- 30 分钟没有信号不等于策略正确，报告应记录“无样本”而不是通过收益验收。
- WAL 峰值受外部长读事务和实际数据规模影响；探针只测量和报警，不改变 writer 背压策略。
- Issue #20 后续应分别建立“信号失败归因与优化”和“新策略研究”任务，使用本阶段报告作为
  输入，并在任何策略行为变更前重新进行设计审查和回归测试。
