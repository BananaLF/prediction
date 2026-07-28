# 验证记录

## Schema v7 / Task 6 fresh 验证

**日期：2026-07-29（Asia/Hong_Kong）**

当前 SQLite schema 版本为 7，共 30 张项目表。Schema v6 及更旧数据库不迁移；程序会在修改前拒绝打开。升级时必须停止进程，删除旧数据库及匹配的 WAL/SHM，或把 `database_path` 指向一个新的空文件。

本节只记录 Task 6 在 schema v7 工作树上的 fresh 输出，不沿用下方历史验证的通过数量。

| 命令 | 本次实际结果 |
|---|---|
| `PYTHONPATH=. .venv/bin/python -m pytest tests/integration/test_cli.py::test_watch_command_offline_success_reconfirms_with_two_rest_books tests/integration/test_cli.py -k 'scan_once and replay and report' -v` | 最终树 fresh 输出：1 passed，30 deselected，0.18s；`-k` 会过滤显式列出的 watch 节点，本次实际选中 fresh DB 的 scan-once → replay(latest/exact) → report 链路 |
| `PYTHONPATH=. .venv/bin/python -m pytest tests/integration/test_cli.py::test_watch_command_offline_success_reconfirms_with_two_rest_books tests/integration/test_cli.py::test_scan_once_replay_and_report_flow_through_main_json_on_fresh_database -q` | 2 passed，0.21s |
| `PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_storage.py::test_schema_v7_has_exact_project_tables tests/unit/test_storage.py::test_schema_v6_is_rejected_without_mutation -v` | 2 passed，0.10s；fresh DB 精确为 30 张项目表，旧库在修改前拒绝 |
| `PYTHONPATH=. .venv/bin/python -m pytest tests/integration/test_read_only_surface.py -q` | 4 passed，0.22s |
| `PYTHONPATH=. .venv/bin/python -m pytest tests/integration/test_cli.py -k 'not retries_initial_connect' -q` | 29 passed，1 deselected，0.46s |
| `PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_storage.py -q` | 82 passed，0.46s |
| `PYTHONPATH=. .venv/bin/python -m pytest tests/integration/test_read_only_surface.py tests/integration/test_cli.py tests/unit/test_storage.py -k 'not retries_initial_connect' -q` | 最终 fresh 有界轮次：115 passed，1 deselected，0.84s |
| `rg -n 'schema (版本为 )?[0-6]\|schema version [0-6]\|39 张' README.md docs predmarket tests` | 命中仅位于 schema 精简历史设计/计划、下方 schema 6 历史验证记录，以及旧库拒绝测试 |
| `git diff --check` | exit 0，无输出 |

fresh DB 跨命令测试还直接断言了：

- `scan-once` 至少评估一个结果，`report.total >= 1`；
- opportunity ID 与最新 replay 一致，bundle ID 与 latest/exact replay 一致；
- `PRAGMA integrity_check` 返回 `ok`；
- `PRAGMA foreign_key_check` 返回空结果。

原计划的三文件合并命令未记录为通过：`test_watch_command_retries_initial_connect_and_persists_metrics` 给后台 reconciliation 注入了不会让出事件循环的 `no_sleep`，会形成无界紧循环。本轮按有界方式分别执行其余 CLI、只读边界和存储测试，没有伪造合并套件数量。

环境补充：worktree 初始 `.venv` 缺少 `predmarket` distribution metadata；本地 editable install 也会因 setuptools flat-layout 同时发现 `rules`、`config`、`predmarket` 而失败。本次只在已忽略的 `.venv` 内补齐与 `pyproject.toml` 一致的 `0.2.0` metadata 以执行只读边界测试，没有修改打包配置。

## 历史验证：schema v6 / Task 13

**日期：2026-07-26（Asia/Hong_Kong）**

## 被验证版本

- 工作树：`feature/structural-arbitrage-scanner`
- 验证开始时基线 HEAD：`aba36e6891f7d1729950d93e933f5e46c14e88f2`
- Task 13 最终提交：本文件所在提交；检出后用 `git rev-parse HEAD` 获取精确值
- 包/发行版版本：`0.2.0`
- Python：3.13.12

本记录中的测试和 smoke 针对基线加 Task 13 工作树变更。提交前会再次运行全量验证和差异检查；不要把基线哈希误认为最终提交哈希。

## 自动与离线验证

| 命令 | 观察结果 |
|---|---|
| `.venv/bin/python -m pytest` | Task 13 初始轮 629 passed in 1.17s；SQLite 监控采集补丁的局部轮次见下方 |
| `.venv/bin/python -m pytest tests/integration/test_read_only_surface.py -q` | 4 passed |
| `.venv/bin/python -m compileall -q predmarket` | exit 0 |
| `./bin/predmarket --help` | exit 0；显示 6 个只读子命令和 `0.75%=0.0075` |
| `./bin/predmarket relations validate rules/example-implication.yaml` | exit 0；audited=true，minimum_units_received=1 |
| 临时配置 + `--json report --limit 5` | exit 0；空库 total=0，p50/p95/p99=null |
| `sqlite3 ... "PRAGMA integrity_check; PRAGMA user_version;"` | `ok`，schema 6 |
| 旧文件存在性检查 | `core.py`、`api.py`、`ledger.py`、`tests/test_core.py` 均不存在 |

临时数据库是 `/tmp/predmarket-task13-smoke.sqlite3`，未保存进仓库。JSON stdout 保持单一文档。

## 公开只读联网 smoke

本次联网成功，严格限制为公开数据，没有认证、钱包、签名、WebSocket 长连接或订单调用。

1. Gamma：

   ```console
   ./bin/predmarket \
     --config /tmp/predmarket-task13-smoke.yaml --json \
     sync-markets --limit 5 --max-pages 1 --max-markets 5
   ```

   结果：exit 0；读取 5 个市场，5 个 tradeable；按 `max_markets` 明确标记截断，没有继续翻页。

2. 一个活跃二元市场的公开 CLOB：

   - condition：`0x1fad72fae204143ff1c3035e99e7c0f65ea8d5cd9bd1070987bd1a3316f772be`
   - `POST /books` 仅用于这一市场的 YES/NO 公开盘口。
   - 修复前 targeted `scan-once`：exit 0，evaluated=1，拒绝为 `invalid_discovery`。复审确认这是把 CLOB `market`（condition ID）错误地与 Gamma 数字 market ID 比较造成的标识符集成缺陷，不是“不盈利”结论。
   - `GET /clob-markets/{condition}`：exit 0，确认 2 个 token，正式 fee rate=`0.05`、tick size=`0.01`。
   - replay：exit 0，状态 `REJECTED`，无通知 claim/attempt/event。

修复后盘口快照以 `condition_id` 绑定，Gamma `market_id` 仅保留为目录/证据身份。旧 smoke 结果已撤销，不能用于判断盈利性；修复后的有界重跑结果记录在下方。

2026-07-26 修复后使用同一个 condition 和同一对 token 重新运行一次公开只读 targeted scan：exit 0、evaluated=1、failed=0，分类为 `REJECTED/no_candidate`，没有通知。它成功通过 CLOB condition/token 标识符接缝，随后才在发现阶段判断该快照同时不存在低估或高估候选；该单点结果不代表其他时刻或市场。

## 最终提交前验证

最终轮记录：

```text
full pytest: 629 passed in 1.17s
focused read-only: 4 passed
compileall: exit 0
CLI help: exit 0
relation validate: exit 0
offline report/replay/integrity: exit 0 / ok
git diff --check: exit 0
```

## 质量复审后追加验证

复审后把 Gamma/CLOB origin 从“任意无凭据 HTTP(S) origin”收紧为精确官方 HTTPS origin，静态测试改为 AST 枚举实际 HTTP 调用和端点构造器，并增加 `overflows` 实际输出指标。通知文档统一为“持久单次尝试 + 租约式崩溃回收”，不作送达保证。

```text
Gamma/CLOB/read-only/WS/CLI focused: 153 passed in 0.54s
full pytest: 639 passed in 1.19s
read-only AST surface: 4 passed
```

联网 smoke 未因本轮变更重新扩大范围；仍以本文件前述最多 5 个 Gamma 市场和一个 CLOB 二元市场结果为准。

## 最终集成修复追加验证

```text
full pytest: 650 passed in 1.47s
identifier seam (actual ClobRestClient + MockTransport): 3 passed
read-only AST surface: 4 passed
compileall / CLI help / git diff --check: exit 0
bounded public rerun: exit 0, evaluated=1, failed=0, REJECTED/no_candidate
```

新增覆盖包括：CLOB condition 与 Gamma market ID 分离、`exchange_after_receive` 因果时间门、二元高估 `SPLIT/SELL/SELL` 的完整深度/风险/回放、以及采用配置周期的 REST epoch reconciliation。没有执行订单、认证、钱包或交易端点。

## SQLite 监控采集补充验证

```text
storage watch facts: 2 passed
watch command persistence: 1 passed
scan-once command persistence: 1 passed
sqlite py_compile: exit 0
validate-opportunity JSON-only and error-classification tests: passed
```

新增覆盖包括：`watch_runs`/`watch_events`/`watch_metrics` 的持久化、`scan_runs`/`scan_candidates` 的持久化，以及 `watch` 与 `scan-once` 在 SQLite 中留下的运行事实。`backtest` 仍在后续计划中，不在本轮验证范围内。

`validate-opportunity` 追加覆盖了机会完整性、回放一致性、`NOT_FOUND` / `AMBIGUOUS_OPPORTUNITY` / `INCOMPLETE_CHAIN` / `REPLAY_MISMATCH` / `CORRUPTED_CANONICAL_JSON` / `INVALID_INPUT` 的 JSON 结构化返回，以及缺少 `opportunity_id` 时仍保持 JSON-only 输出。

## 限制

- **24 小时 soak：NOT RUN（2026-07-26）。**
- **7 天观察：NOT RUN（2026-07-26）。**
- 联网 smoke 只覆盖 5 个 Gamma 市场和其中一个二元市场，不能代表全目录或长期稳定性。
- 没有执行任何交易，不能验证同时成交、真实排队位置、部分成交退出或实际结算。
- `SNAPSHOT_EXECUTABLE` 仍只表示快照模型通过，不是利润或成交保证。
- 零机会是正常且可接受的观察结果。

长期验收必须独立执行 `docs/SOAK-TEST.md`，不得用本次短 smoke 替代。
