# 运维手册

## 安装与配置

要求 Python 3.10+。建议在仓库根目录运行：

```console
python -m venv .venv
.venv/bin/pip install -e '.[test]'
./bin/help
```

默认配置位于 `config/default.yaml`。金额和比率必须写成十进制字符串；`minimum_return: "0.0075"` 表示 0.75%。数据库默认是 `data/predmarket.sqlite3`。生产运行前复制配置并使用 `--config PATH`，不要直接覆盖可审计的默认值。

## 常用命令

```console
# 有界同步公开市场目录
./bin/predmarket sync-markets --limit 100 --max-pages 2 --max-markets 1000

# 目录发现后用 REST 确认
./bin/scan-once --limit 100

# 已知 condition/token 时做确定性确认
./bin/scan-once \
  --condition CONDITION --yes-token YES_TOKEN --no-token NO_TOKEN

# 有界观察；max-events 是整个命令跨重连累计预算
./bin/watch --max-connections 3 --max-events 500

# 规则
./bin/predmarket relations validate rules/example-implication.yaml
./bin/predmarket relations import reviewed.yaml
./bin/predmarket relations list

# 证据和汇总
./bin/predmarket replay OPPORTUNITY_ID
./bin/predmarket replay --bundle-id BUNDLE_ID
./bin/predmarket validate-opportunity OPPORTUNITY_ID
./bin/predmarket --json report --limit 100
```

全局参数必须放在子命令之前，例如 `--json report`。JSON 模式的 stdout 恰好输出一个 JSON 文档；通知审计等人类提示写到 stderr。退出码：`0` 成功，`1` 网络、数据库或运行期故障，`2` 参数、配置、规则或输入错误。

## 数据库、备份和迁移

SQLite 开启 WAL。备份前优先停止写入进程，然后使用 SQLite 在线备份命令，避免只复制主文件而漏掉 `-wal`：

```console
sqlite3 data/predmarket.sqlite3 \
  ".backup '/absolute/backup/predmarket-$(date +%Y%m%d).sqlite3'"
sqlite3 /absolute/backup/predmarket-YYYYMMDD.sqlite3 "PRAGMA integrity_check;"
```

恢复时先保留当前数据库及同名 `-wal`、`-shm`，再把校验成功的备份放到新的显式路径，通过新配置验证 `report` 和 `replay`，最后再切换。不要对运行中的数据库做文件级覆盖。

当前代码的 schema 版本为 6。打开数据库时自动建表；v3 有受控迁移，v1 明确拒绝，未来版本数据库也会拒绝降级打开。升级前备份并记录当前提交、`PRAGMA user_version` 和文件哈希。迁移失败不要手工改 `user_version`；恢复备份，在副本上复现。

`replay` 返回不可变核心证据以及单独的通知审计。报告默认有界，避免把完整历史载入内存。

`validate-opportunity` 是一个单机会精确核验命令：它先检查 SQLite 中的证据链是否完整，再把存储事实与 replay 结果做语义一致性比较。该命令只输出 JSON，失败时通过 JSON `errors[].code` 区分 `NOT_FOUND`、`AMBIGUOUS_OPPORTUNITY`、`INCOMPLETE_CHAIN`、`REPLAY_MISMATCH`、`CORRUPTED_CANONICAL_JSON` 和 `INVALID_INPUT`。

`watch` 和 `scan-once` 现在都会把运行事实写入 SQLite：

- `watch` 记录运行元数据、每条接受的市场域事件，以及最终 WS 指标快照；
- `scan-once` 记录运行元数据以及每条扫描结果候选；
- 这两类记录是后续分析和复核的事实来源，不能只依赖 stdout/stderr。

## 规则审计与导入

规则文件必须包含稳定 ID、版本、状态、来源哈希、腿、完整状态支付表和人工审查信息。流程：

1. 两人核对问题文本、时区、截止时间、取消/无效规则、解析来源和 token ID。
2. 用 `relations validate` 检查结构和最低支付。
3. 保存原始来源或截图的外部审计位置，并更新 `source_rules_hash`。
4. 用 `relations import` 导入；同 ID/版本内容不同会拒绝覆盖。
5. `relations list` 复核。多版本产生歧义时用 `--relation-id` 明确选择。

逻辑关系和 NegRisk 当前仍是研究路径；导入审计规则不会自动赋予可执行资格。

## 公开网络边界

允许的目标和方法只有：

| 主机 | 方法/路径 | 用途 |
|---|---|---|
| `gamma-api.polymarket.com` | `GET /markets/keyset` | 市场目录 |
| `clob.polymarket.com` | `POST /books` | 批量公开盘口，只读 POST |
| `clob.polymarket.com` | `GET /fee-rate` | token 公开费率证据 |
| `clob.polymarket.com` | `GET /clob-markets/{condition_id}` | 市场映射和正式费用曲线 |
| `ws-subscriptions-clob.polymarket.com` | `WSS /ws/market` | 公开市场变化发现 |

客户端不信任代理环境，不允许 cookie、HTTP auth 或凭据请求头。禁止项详见 `SECURITY.md`。

## 通知语义

SQLite 通知 outbox 使用**持久单次尝试 + 租约式崩溃回收**，不保证成功送达：

- 新指纹先取得 `CLAIMED` 租约。
- 租约到期且未终结时，重启进程可 `RECLAIMED`。
- `SUCCEEDED` 和有意记录的 `FAILED` 为终态；失败不自动重试。
- 若发送结果不确定且进程崩溃，租约到期后可回收；发送耗时超过租约时可能重复，因此下游应按 fingerprint 去重。
- claim、尝试和事件均附着于证据 bundle；通知不是事实来源。

只对 `SNAPSHOT_EXECUTABLE` 请求桌面通知。桌面通知失败不会改变已保存的机会证据。运维人员必须轮询 SQLite `report`/`replay` 发现漏提醒，不能依赖桌面弹窗作为完整事件流。

## 指标解释

- `received`：入队接受的市场域事件；PONG 和无效帧不计。
- `dropped`：超过累计事件预算或队列容量而丢弃的事件。结合 `overflows` 判断是否由队列溢出导致。
- `queue_high_water`：队列实际达到的最高深度；接近 `queue_capacity` 说明处理能力不足。
- `reconnects`、`disconnects`：重连次数和断开次数。实现不输出 `connection_attempts`；需要尝试次数时由外部监管器独立记录，不能用不存在的键。
- `overflows`、`resyncs`：队列溢出次数和强制重新同步次数；任何 overflow 都必须导致相关 epoch 失效。
- `malformed`、`unknown`、`heartbeats`、`callback_failures`：畸形帧、未知事件、心跳和回调失败累计数。
- `reconciliation_attempts`、`reconciliation_successes`、`reconciliation_failures`：周期公开 REST 本地 epoch 校准；失败必须使 epoch 失效。`reconciliation_interval_seconds` 记录实际采用的配置。
- 延迟 `p50/p95/p99`：最近有界样本的 nearest-rank 分位数；空样本为 `null`。
- `status_counts` / `reason_counts`：分类结果与拒绝原因；大量 stale/leg_skew/processing_latency 指向数据链路问题。
- `notification_claims` / `notification_attempts`：积压、成功和失败；长期 CLAIMED 可能等待租约恢复。

`watch` 只保留最近 100 条结果摘要和 1024 个延迟样本，同时保留累计计数与流式 `processing_latency_count`、`processing_latency_sum_ms`、`processing_latency_min_ms`、`processing_latency_max_ms`；`processing_latency_sample_truncated` 或顶层 `results_truncated` 表示更早明细已丢弃，不表示累计计数丢失。

`report` 的 `ws_metrics` 最近一次记录键图如下；`id`、`started_at_ms` 和 `epoch_states` 是持久化包装字段：

```json
{
  "ws_metrics": {
    "id": "watch:...",
    "started_at_ms": 0,
    "received": 0,
    "dropped": 0,
    "malformed": 0,
    "unknown": 0,
    "heartbeats": 0,
    "disconnects": 0,
    "reconnects": 0,
    "resyncs": 0,
    "overflows": 0,
    "callback_failures": 0,
    "reconciliation_attempts": 0,
    "reconciliation_successes": 0,
    "reconciliation_failures": 0,
    "reconciliation_interval_seconds": 30,
    "queue_high_water": 0,
    "processing_latencies_ms": [],
    "processing_latency_count": 0,
    "processing_latency_sum_ms": "0.0",
    "processing_latency_min_ms": null,
    "processing_latency_max_ms": null,
    "processing_latency_sample_truncated": false,
    "epoch_states": {}
  }
}
```

## 故障处置

### WebSocket 断线

确认 `reconnects`、`disconnects`、REST reconciliation 成败和最后事件时间；让程序按上限重连。连接尝试次数由外部监管器记录。重连后必须先取得完整快照或周期 REST 校准，不能沿用旧 epoch；任何正式机会仍须重新取得两次独立 REST 证据。反复失败时停止依赖 WS，运行有界 `scan-once` 并保留指标。

### 队列溢出

看到 `dropped` 或 `overflows` 增长后，将相关 epoch 视为无效；确认 `resyncs` 同步增长并等待完整快照重建。降低订阅范围/事件预算，检查 CPU 和数据库写入延迟。不能通过扩大队列掩盖持续处理不足。

### 数据陈旧或延迟门失败

比较 exchange、received、evaluated 三类时间，检查系统时钟、网络延迟和腿间偏斜。不要临时放宽门槛来制造机会；先恢复时钟和链路。

### 提供商失败或响应变化

记录 HTTP 状态、端点、时间和适配器错误，不记录敏感环境。用冻结 fixture 运行适配器测试；若字段变化，先更新契约和测试，再更新解析器。禁止切换到认证端点。

### 数据库失败

立即停止写入实例，保留数据库、`-wal`、`-shm` 和磁盘信息；检查空间、权限和 `PRAGMA integrity_check`。使用最近校验备份在新路径恢复，随后用 `report`/`replay` 验证。不要删除 WAL 或手工修表。

### 通知失败或卡租约

先查 SQLite 通知审计，证据仍有效。终态 FAILED 不自动重试；人工处理必须避免重复提醒。CLAIMED 在租约到期后才可安全重新 claim。

## 日常检查

检查磁盘剩余空间、数据库/WAL 增长、最近运行退出码、事件丢弃、延迟分位数、长期 CLAIMED 通知和目录同步完整性。长期验证流程见 `docs/SOAK-TEST.md`。
