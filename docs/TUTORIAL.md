# 从零开始教程

这份教程带你完成一次最小、可验证的使用流程：安装项目、检查配置、同步少量市场、执行一次扫描、查看报告并回放证据。所有联网操作都只访问公开市场数据，不需要也不应配置 Polymarket 凭据或钱包。

以下命令都在仓库根目录执行。

## 1. 准备环境

确认 Python 版本：

```console
python3 --version
```

项目要求 Python 3.10+。创建独立虚拟环境并安装项目与测试依赖：

```console
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -e '.[test]'
```

检查 CLI：

```console
./bin/predmarket --help
```

也可以使用安装生成的 `.venv/bin/predmarket`；本教程统一使用 `./bin/predmarket`，便于确认当前仓库里的启动器路径。

## 2. 先做离线自检

运行完整测试：

```console
.venv/bin/python -m pytest
```

单独检查只读安全边界：

```console
.venv/bin/python -m pytest tests/integration/test_read_only_surface.py -q
```

校验仓库自带的示例关系规则：

```console
./bin/predmarket \
  relations validate rules/example-implication.yaml
```

输出中的 `audited` 应为 `true`，`minimum_units_received` 应为 `"1"`。该文件只用于演示规则格式，其中的 `no_a` 和 `yes_b` 不是实际市场 token。

## 3. 创建本地配置

保留默认配置作为基线：

```console
cp config/default.yaml config/local.yaml
```

如果只想试用，默认值可以直接使用。至少确认：

```yaml
minimum_return: "0.0075"
database_path: data/predmarket.sqlite3
```

这里的 `0.0075` 是 0.75%，不是 75%。金额和比例必须保留为带引号的十进制字符串。

如果希望测试数据与日常数据隔离，可把数据库改为：

```yaml
database_path: data/tutorial.sqlite3
```

当前 SQLite schema 版本为 7，共 30 张项目表。新用户无需手工建表：首次运行使用不存在或为空的 `database_path` 时，程序会自动初始化 schema。Schema v6 及更旧数据库不迁移；程序会在修改前拒绝打开。升级时必须停止进程，删除旧数据库及匹配的 WAL/SHM，或把 `database_path` 指向一个新的空文件。

后续所有命令都应使用同一个 `--config config/local.yaml`，否则可能读写不同数据库。

## 4. 同步一个小型市场目录

先进行有界同步，减少首次试用的网络量：

```console
./bin/predmarket \
  --config config/local.yaml \
  sync-markets --limit 20 --max-pages 1 --max-markets 20
```

典型输出结构：

```json
{
  "diagnostics": [],
  "markets": 20,
  "snapshot_id": "...",
  "tradeable": 18
}
```

字段含义：

- `snapshot_id`：保存到 SQLite 的目录快照 ID。
- `markets`：本次取得的市场数。
- `tradeable`：其中通过目录级可交易判断的数量。
- `diagnostics`：被跳过或无法规范化的上游条目说明。

这里设置了页数和市场数上限，所以这通常只是一个确定性的目录前缀。只有完整翻页结束的同步才有资格把新快照中缺失的旧市场标记为 `MISSING`；有界或部分同步不会错误停用未看到的市场。

## 5. 执行一次扫描

扫描最多 20 个公开市场：

```console
./bin/predmarket \
  --config config/local.yaml \
  scan-once --limit 20
```

顶层字段：

- `evaluated`：实际进入引擎评估的市场数。
- `skipped`：不满足二元、活跃、可交易等目录条件的数量。
- `failed`：单个市场在读取或评估时发生异常的数量。
- `results`：每个已评估市场的结果。
- `diagnostics`：目录解析诊断。

一个结果中优先看这些字段：

```json
{
  "opportunity_id": "opp:...",
  "evidence_id": "...",
  "status": "REJECTED",
  "reason": "no_candidate",
  "stage": "discovery",
  "newly_persisted": true,
  "minimum_profit": null,
  "minimum_return": null,
  "risk_reasons": ["no_candidate"]
}
```

`REJECTED/no_candidate` 是正常结果，表示当时盘口没有形成模型候选。零个机会不表示程序失败。

### 使用 JSON 模式

自动化处理时，把 `--json` 放在子命令前：

```console
./bin/predmarket \
  --config config/local.yaml --json \
  scan-once --limit 20
```

JSON 模式保证 stdout 只有一个 JSON 文档；人类可读的通知审计写到 stderr。不要写成 `scan-once --json`，因为 `--json` 是全局参数。

### 精确扫描已知市场

如果已经从可信来源获得准确的 condition ID 和 YES/NO token ID：

```console
./bin/predmarket \
  --config config/local.yaml \
  scan-once \
  --condition CONDITION_ID \
  --yes-token YES_TOKEN_ID \
  --no-token NO_TOKEN_ID
```

三个参数必须一起提供。显式 token 顺序必须确实是 YES、NO；程序会验证 CLOB 映射，但不会替你完成人工语义审计。

## 6. 查看汇总

扫描后读取最近 100 条证据：

```console
./bin/predmarket \
  --config config/local.yaml \
  report --limit 100
```

重点字段：

| 字段 | 如何解读 |
|---|---|
| `total` / `truncated` | 本次汇总条数，以及是否还有更早数据未包含 |
| `by_status` | 三类状态的数量 |
| `by_pipeline_reason` | 引擎最终原因，例如 `no_candidate` |
| `by_reason` | 风险层原因计数 |
| `by_path` | `IMMEDIATE_CONVERSION` 或 `HOLD_TO_RESOLUTION` |
| `executable_economics` | 仅通过全部门槛的快照经济数据 |
| `latency_ms` | 最近证据的 p50/p95/p99 处理延迟 |
| `notification_claims` / `notification_attempts` | 通知租约和尝试状态 |
| `delivery_uncertain` | 仍处于不确定通知租约的数量 |
| `ws_metrics` | 最近一次 `watch` 指标；未运行过时为 `null` |

报告是有界窗口，不是全库统计。`limit` 的合法范围是 1 到 10000。

## 7. 回放一条证据

从 `scan-once` 结果复制 `opportunity_id`：

```console
./bin/predmarket \
  --config config/local.yaml \
  replay 'opp:CONDITION_ID'
```

同一个机会可能被多次扫描，这个命令返回最新一次评估。要固定到某次不可变证据，请复制结果中的 `evidence_id`，作为 bundle ID 使用：

```console
./bin/predmarket \
  --config config/local.yaml \
  replay --bundle-id BUNDLE_ID
```

输出分为：

- `core_evidence`：市场、盘口、费率、动作、成本、收益、风险、延迟和生产者版本。
- `notification_audit`：通知 claim、尝试和状态事件。

如果 `replay` 报错，先确认使用了产生该证据时相同的配置文件，因为 `database_path` 决定读取哪个 SQLite 文件。

## 8. 有界运行实时观察

首次运行不要直接做无限长观察，先给连接和事件设置小上限：

```console
./bin/predmarket \
  --config config/local.yaml \
  watch --max-connections 3 --max-events 500
```

- `--max-connections` 是整个命令最多进行的连接尝试数。
- `--max-events` 是跨所有重连累计接受的市场事件预算。
- PONG 和无效帧不消耗事件预算。
- 到达预算后程序会干净退出，不会再打开新连接。

输出中的 `ws_metrics` 可用于判断运行质量：

- `received` / `dropped`：接受和丢弃的市场事件；
- `queue_high_water` / `overflows`：队列压力和溢出；
- `reconnects` / `disconnects`：连接稳定性；
- `resyncs`：本地 epoch 被强制重建的次数；
- `reconciliation_successes` / `reconciliation_failures`：周期 REST 校准结果；
- `processing_latency_*`：累计处理延迟；
- `epoch_states`：各 token 的当前本地状态。

`watch` 只保留最近 100 条结果和最近 1024 个延迟样本，但累计计数仍然保留。出现 `results_truncated: true` 不表示数据库证据丢失。

## 9. 使用人工审计关系规则

先复制示例并填写真实、已人工审计的关系：

```console
cp rules/example-implication.yaml /tmp/reviewed-relation.yaml
```

规则至少需要稳定的 `relation_id`、版本、状态、来源哈希、人工审查、token 腿和完整状态支付表。校验：

```console
./bin/predmarket \
  relations validate /tmp/reviewed-relation.yaml
```

确认无误后导入：

```console
./bin/predmarket \
  relations import /tmp/reviewed-relation.yaml
./bin/predmarket relations list
```

导入会写入 `rules/`。相同 ID 和版本如果内容不同会拒绝覆盖。扫描时可通过：

```console
./bin/predmarket \
  --config config/local.yaml \
  scan-once --limit 100 --relation-id RELATION_ID
```

显式选择规则。导入规则并不会自动使跨市场逻辑或 NegRisk 获得可执行资格。

## 10. 常见问题

### `No such file or directory: .venv/bin/python`

虚拟环境还没创建，或当前不在仓库根目录。重新执行第 1 节。

### `error: configuration ...`

这类错误退出码是 2。检查配置字段是否齐全；金额和比例是否为字符串；整数是否为正数；全局参数是否位于子命令前。

### `operational error: ...`

运行期、网络或数据库错误的退出码是 1。先重试小范围命令，再检查网络、磁盘空间、数据库权限和上游响应。不要通过添加凭据或切换到私有端点解决。

### 扫描结果全是 `REJECTED`

这是合理且常见的。依次查看 `stage`、`reason`、`risk_reasons`，再通过 `replay` 检查盘口、费率和时间证据。不要为了“制造机会”随意放宽延迟、损失或收益门槛。

### 没有桌面通知

只有新保存的 `SNAPSHOT_EXECUTABLE` 才会尝试通知，且桌面通知不保证送达。以 SQLite、`report` 和 `replay` 为事实来源。非 macOS 环境主要依赖终端和数据库。

### 如何清空数据重来

不要在程序运行时直接删除数据库或 WAL。先停止所有使用该库的进程。更安全的做法是复制配置，把 `database_path` 改到一个新的明确空文件，然后用新配置开始；如果确认不再保留旧库，也必须把主数据库及匹配的 `-wal`、`-shm` 一并移出使用路径。Schema v6 及更旧数据库不能由当前程序读取。

## 11. 下一步

- 想理解公式和风险：阅读 [策略说明](../STRATEGY.md)。
- 想长期运行、备份或处理故障：阅读 [运维手册](OPERATIONS.md)。
- 想做 24 小时或 7 天验证：阅读 [稳定性验证协议](SOAK-TEST.md)。
- 想了解只读安全边界：阅读 [安全模型](../SECURITY.md)。
- 想理解模块和数据流：阅读 [项目说明](PROJECT-GUIDE.md)。
