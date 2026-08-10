# Catalog v4 production observation

本报告记录本地生产数据库实例 `data/predmarket-v1.sqlite3` 的完整同步和运行观察结果。原始采样数据见 [catalog-v4-production-observation-2026-08-10.json](catalog-v4-production-observation-2026-08-10.json)。

## 结果

- 观察窗口：2026-08-10 15:32:04–16:02:08 UTC，`1804` 秒，`58` 个采样。
- 观察结果：`healthy`，`exit_code=0`，所有采样失败计数为 0。
- 数据库 schema：v4；`PRAGMA integrity_check=ok`；`PRAGMA foreign_key_check` 无结果。
- 活跃 catalog generation：generation 2，状态 `COMMITTED`，事件/市场/token 数量为 `18,942 / 140,888 / 281,776`。
- cleanup：观察结束时无可回收 generation、staging candidate 或 journal backlog；journal 已处理至 revision 14。
- WAL：峰值 `53,592` bytes，低于 `128 MiB` 限制。
- 信号与收益：信号数为 0；没有订单、成交、钱包余额或 realized P&L，因此 realized return 不适用。

## 验证范围

观察期间运行了 catalog cleanup worker 和 watch runtime。cleanup worker 能在初始 backlog 清理后持续保持 journal 为 0；watch runtime 完成 50 个市场、100 个 token 的订阅，并在连接异常后执行恢复。

上游 REST/WebSocket 在观察期间出现超时、连接重试和交易所时钟偏移告警；这些情况被运行时记录并恢复，未导致 catalog 完整性、外键或 cleanup 检查失败。由于本次验证是只读观察，没有执行真实交易或收益验证。
