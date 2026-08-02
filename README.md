# Predmarket

## 项目背景

Polymarket 是公开的预测市场。市场价格、订单簿和相关市场之间的结构性定价关系，可以为研究、监控和运维提供可复核的观察材料。Predmarket 面向这些场景，读取 Polymarket 的公开市场数据，持续整理证据并记录可供人工复查的市场信号。

## 项目描述

Predmarket 是一个本地、对 Polymarket 只读、证据驱动的市场信号服务，不是交易机器人。它允许将市场快照、关系和信号证据写入本地 SQLite，帮助用户观察机会及其变化；信号不代表已经成交，也不承诺或计算实际收益。

## 核心能力

- 通过 `PolymarketGateway` 读取公开 REST/WebSocket 数据，并同步市场与订单簿信息。
- 识别并评估结构性市场关系，记录可审计的信号证据。
- 通过本地 CLI 查询服务状态、信号和关系。
- 以 SQLite 持久化数据，并由 `DatabaseWriter` 串行化数据库写入。
- 使用 `Notifier` 提供终端或桌面通知；通知同样不执行交易。

## 安全边界

运行时只通过 `PolymarketGateway` 访问 Polymarket 公开 REST/WebSocket 数据。不认证、不持有钱包、不签名、不下单、不撤单、不执行链上操作，也不提供交易、认证或钱包入口。`run` 会写入本地数据库；其余查询命令只读本地 SQLite。

关系审批是唯一需要特别注意的本地状态变更：`relations approve` 只接受仍满足语义校验、状态为 `LLM_APPROVE` 的关系，写入本地关系状态并记录激活事件；它不会修改 Polymarket。

## 架构概览

服务由 Gateway、Sync、Watch、Strategy、SignalManager、SQLite 和 Notifier 组成。Gateway 负责公开数据访问，Sync 和 Watch 负责数据同步与变化观察，Strategy 计算候选关系，SignalManager 管理信号生命周期，SQLite 保存证据，Notifier 报告变化。数据库写入统一经过 `DatabaseWriter` 串行化。

当前策略路径包括：

- `BINARY_UNDERPRICED`
- `BINARY_OVERPRICED`
- `LOGICAL_IMPLICATION`
- `NEG_RISK_COMPLETE_SET`

## 环境要求与安装

- Python `>=3.11`
- `polymarket-client==0.3.0b1`
- 建议从仓库根目录执行以下命令。

推荐使用 uv 安装测试依赖：

```console
uv sync --extra test
source .venv/bin/activate
```

不使用 uv 时，也可以用 pip 以可编辑模式安装：

```console
python -m pip install -e ".[test]"
```

## 快速开始

以下命令均从仓库根目录执行，首选使用已安装的 `predmarket` console entry：

```console
uv sync --extra test
source .venv/bin/activate
predmarket --help
predmarket run --config config/default.yaml
```

`predmarket run` 会读取默认配置、初始化数据库并启动长期运行的公开数据采集。停止服务后，可在另一个终端运行查询命令查看本地结果。

运行状态通过 Python `logging` 输出到 `stderr`，默认级别为 `INFO`。可在启动命令中使用 `--log-level DEBUG|INFO|WARNING|ERROR|CRITICAL` 调整级别，例如 `predmarket run --config config/default.yaml --log-level DEBUG`。

不使用 uv 的等价入口是：

```console
python -m pip install -e ".[test]"
predmarket run
```

## CLI 使用

下表命令都从仓库根目录执行。除 `run` 外，命令不会访问 Polymarket；`status`、`signals` 和 `relations list/show` 都是本地 SQLite 查询。`run` 是长期运行、会初始化并写入数据库的公开数据采集命令。

| 命令 | 用途 | 读写性质与前提 |
| --- | --- | --- |
| `predmarket --help` | 查看全局帮助和子命令 | 只读，不需要数据库或网络 |
| `predmarket run --config config/default.yaml` | 启动公开市场数据采集和信号服务 | 读取配置并写入/初始化本地 SQLite；需要公开数据网络可用 |
| `predmarket status --config config/default.yaml` | 查看本地信号和系统事件计数 | 只读本地 SQLite；数据库必须已初始化 |
| `predmarket signals list --config config/default.yaml` | 列出已记录信号 | 只读本地 SQLite；数据库必须已初始化 |
| `predmarket signals show SIGNAL_ID --config config/default.yaml` | 查看一个信号及其证据字段 | 只读本地 SQLite；数据库必须已初始化且 ID 存在 |
| `predmarket relations list --config config/default.yaml` | 列出本地关系 | 只读本地 SQLite；数据库必须已初始化 |
| `predmarket relations show RELATION_ID --config config/default.yaml` | 查看一个关系 | 只读本地 SQLite；数据库必须已初始化且 ID 存在 |
| `predmarket relations approve RELATION_ID --config config/default.yaml` | 审批一个关系 | 写入本地关系状态和激活事件；只接受仍通过语义校验的 `LLM_APPROVE` 关系 |

### 兼容入口

`python -m predmarket` 仍然兼容，但不是首选写法：

```console
python -m predmarket --help
python -m predmarket status --config config/default.yaml
```

## 配置说明

默认配置位于 [`config/default.yaml`](config/default.yaml)。策略阈值来自该文件，包括资金规模、最低收益率、最大风险率、未对冲名义金额、订单簿时效和腿部偏斜等参数。配置中的相对路径以当前工作目录解析，因此应从仓库根目录执行命令，或传入明确的配置路径。

`relations.llm_enabled` 默认是 `false`。打开这个开关本身不会自动配置 LLM，也不会让标准 console entry 获得 analyzer provider。

## 信号语义

信号是机会观察和证据记录，不是成交收益。它们用于描述本地观测到的条件及其变化，不表示订单已执行、资金已结算或用户已经获得收益。

- `OPEN`：首次记录一个机会。
- `UPDATED`：同一机会的证据或计算结果发生修订。
- `CLOSED`：机会消失，或已经无法继续验证。

这些状态都不代表成交或实际收益。缺失、过期、不一致或无法验证的数据不会被当作确定性结果。

## 数据库与 reset

默认数据库路径是 `data/predmarket-v1.sqlite3`。`status`、`signals` 和 `relations list/show` 要求数据库已经由 `run` 初始化；数据库不存在或尚未完成初始化时，先启动 `run` 并等待首轮数据处理。

reset 使用独立脚本，不提供 `predmarket reset` 子命令。执行前必须先停止运行中的服务，先检查 dry-run 输出的绝对路径，确认目标后再使用 `--execute`：

```console
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

脚本的安全说明见 [`docs/OPERATIONS.md`](docs/OPERATIONS.md) 和 [`SECURITY.md`](SECURITY.md)。

## 文档索引

- [`docs/PRD.md`](docs/PRD.md)
- [`docs/PROJECT-GUIDE.md`](docs/PROJECT-GUIDE.md)
- [`docs/TUTORIAL.md`](docs/TUTORIAL.md)
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- [`STRATEGY.md`](STRATEGY.md)
- [`SECURITY.md`](SECURITY.md)
- [`docs/VERIFICATION.md`](docs/VERIFICATION.md)

## 常见问题

### 数据库尚未初始化怎么办？

先从仓库根目录运行 `predmarket run --config config/default.yaml`，让服务初始化并写入本地 SQLite；之后再运行 `status`、`signals` 或 `relations` 查询。

### Polymarket 依赖或网络不可用怎么办？

确认已安装固定版本 `polymarket-client==0.3.0b1`，并检查公开 REST/WebSocket 网络连接。`run` 依赖公开数据采集；查询本地 SQLite 的命令不访问 Polymarket，但仍需要已有数据库。

### `status` 有什么前提？

`status` 只读取配置所指向的本地 SQLite，不会初始化数据库，也不会访问 Polymarket；因此必须先由 `run` 创建并初始化数据库。

### 为什么 `relations analyze` 不可用？

虽然 CLI 保留了 `relations analyze` 路由，但标准 console entry 没有 `RelationAnalyzer` provider。仅打开 `relations.llm_enabled` 不会自动配置 LLM，因此不能把它视为默认可用功能。

### 信号是否等于收益？

不是。信号只表示机会观察和证据记录；`OPEN`、`UPDATED`、`CLOSED` 都不表示成交或实际收益，服务也不会下单、撤单或执行链上操作。
