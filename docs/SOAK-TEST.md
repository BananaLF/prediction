# 长时间稳定性验证协议

**状态：NOT RUN（2026-07-26）**

本文只定义可复现的 24 小时 soak 和 7 天观察流程，没有声称已经运行或得到结果。期间出现零个机会是可接受结果；稳定性验收不能以“必须发现套利”为条件。

## 前置条件

- 使用专用只读主机/用户和独立数据库路径，确认无凭据环境、钱包或交易工具。
- 记录 UTC/本地时间、Git 提交、Python/依赖版本、配置文件哈希、磁盘容量和系统时钟同步状态。
- 先运行全量测试、只读边界测试、帮助和规则验证。
- 设置日志轮转与磁盘报警；stdout/stderr 分开，不把无限日志留在仓库。

示例准备：

```console
mkdir -p /absolute/soak/logs /absolute/soak/data
cp config/default.yaml /absolute/soak/soak.yaml
# 编辑 database_path 为 /absolute/soak/data/predmarket.sqlite3
git rev-parse HEAD
sha256sum /absolute/soak/soak.yaml
.venv/bin/python -m pytest
.venv/bin/python -m pytest tests/integration/test_read_only_surface.py -q
```

## 24 小时 soak

使用外部进程监管器启动，不在测试命令中用无限等待。示例以 `timeout` 表达严格上限；macOS 可用等效监管器：

```console
timeout --signal=INT --kill-after=60s 24h \
  .venv/bin/python -m predmarket \
  --config /absolute/soak/soak.yaml --json \
  watch --max-connections 1000000 --max-events 100000000 \
  > /absolute/soak/logs/watch.stdout.jsonl \
  2> /absolute/soak/logs/watch.stderr.log
echo $? > /absolute/soak/logs/watch.exit
```

每 5 分钟由外部采集器记录：

```console
date -u +%FT%TZ
ps -o pid,rss,vsz,%cpu,etime -p PROCESS_ID
df -k /absolute/soak
du -k /absolute/soak/data/predmarket.sqlite3*
.venv/bin/python -m predmarket \
  --config /absolute/soak/soak.yaml --json report --limit 100
```

收集 queue high-watermark、accepted/dropped、连接尝试/重连、stale/skew/processing-latency 原因、延迟 count/min/max/mean/p50/p95/p99、状态计数、通知 claims/attempts、目录同步完整/截断信息，以及结果/延迟 recent-window truncation 标志。

### 中断和重启检查

在第 6 小时左右用 SIGINT 正常中断，记录停止耗时和退出码；确认数据库完整后用相同配置重启。第 12 小时左右模拟进程意外终止（只终止测试进程，不删文件），再次启动并确认：

- 旧 WS epoch 不会在重连后直接接受增量；
- 完整快照/REST 重新确认后才产生正式结果；
- SQLite `integrity_check` 为 `ok`；
- 已成功通知的 fingerprint 不重复，过期 CLAIMED 可回收；
- report/replay 能读取中断前后的证据。

## 7 天观察

在 24 小时验收后才开始。每天固定时间执行：

```console
.venv/bin/python -m predmarket \
  --config /absolute/soak/soak.yaml \
  sync-markets --limit 100 --max-pages 100 --max-markets 10000
.venv/bin/python -m predmarket \
  --config /absolute/soak/soak.yaml --json report --limit 100 \
  > /absolute/soak/logs/report-DAY.json
sqlite3 /absolute/soak/data/predmarket.sqlite3 "PRAGMA integrity_check;"
sqlite3 /absolute/soak/data/predmarket.sqlite3 \
  ".backup '/absolute/soak/data/backup-DAY.sqlite3'"
```

每天保存提交、配置哈希、退出码、资源/磁盘曲线、数据库/WAL 大小、事件与通知指标、目录完整性、错误摘要和人工事件记录。任何代码或配置变化都结束当前观察窗口并开启新窗口，不能拼接成“连续 7 天”。

## 验收标准

24 小时和 7 天分别验收：

- 无未处理异常、数据库损坏、进程无界增长或磁盘耗尽。
- 有界缓存保持约束：最近结果 ≤100，延迟样本 ≤1024；截断标志准确。
- 队列丢弃一旦发生，相关 epoch 失效并经完整同步恢复；不存在从失效本地书直接升级为可执行。
- 只有正式 REST 结果可为 `SNAPSHOT_EXECUTABLE`；每个结果都有完整证据 bundle。
- 延迟、fee、tick、深度、部分成交和关系门均能在证据中解释分类。
- 通知 claim/attempt/event 可追溯；没有无法解释的重复或永久 CLAIMED。
- 重启后 report/replay 一致，迁移版本稳定，备份可打开且 integrity_check 为 `ok`。
- 资源基线无持续单调泄漏；数据库增长与事件量大致成比例并在容量预算内。
- 机会为零不构成失败；出现机会也不构成成功，必须审查证据完整性。

## 证据清单

最终归档：

- `git rev-parse HEAD`、Python 和依赖版本；
- 原始配置及 SHA-256；
- 分离的 stdout/stderr、退出码和监管器事件；
- 周期 report JSON、资源与磁盘序列；
- 数据库备份、大小、schema version、integrity check；
- 代表性的 REJECTED、RESEARCH_CANDIDATE、SNAPSHOT_EXECUTABLE（若存在）replay；
- 通知租约/尝试/事件审计；
- 所有事故单及处置时间线。

## 事故模板

```text
事故 ID：
发现时间（UTC/本地）：
提交/配置哈希：
影响窗口：
症状和触发指标：
连接/队列/epoch 状态：
最近 REST/WS 时间与错误：
数据库/WAL/磁盘状态：
通知 claim/attempt 状态：
是否产生错误的正式分类：
即时止损动作：
保留的证据路径和哈希：
根因：
修复与回归测试：
恢复验证：
负责人和复盘日期：
```
