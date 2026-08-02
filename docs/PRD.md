# Predmarket 当前版本产品需求文档

## 文档信息

| 项目 | 内容 |
| --- | --- |
| 项目版本 | `0.2.0` |
| 状态 | 当前实现版本 |
| 适用代码基线 | `82d72e8` |
| Python | `>=3.11` |
| SDK | `polymarket-client==0.3.0b1` |
| 本地数据库 | SQLite Schema v3 |

## 产品背景与问题定义

Polymarket 的公开市场和订单簿会持续变化，研究和运维人员需要从中发现可验证的结构性定价关系，并能回溯判断计算当时使用的数据是否完整、及时且一致。

Predmarket 解决的问题是：从 Polymarket 公开市场和订单簿中发现可验证的结构性定价关系，保存计算所需证据，向研究/运维用户提供本地可审计的机会观察。它是一个对 Polymarket 只读、会将证据和信号写入本地 SQLite 的服务，不是交易机器人；它不认证、不持有钱包、不签名、不下单、不撤单、不执行链上操作；信号不表示订单已执行、资金已结算或实际收益。

## 产品定位与价值主张

产品以公开数据为输入，在本地 SQLite 证据库中保留市场、关系、信号修订和系统事件。用户可以通过 CLI 观察服务状态、信号和关系，并在受控条件下对已有关系进行人工审批。缺失、过期、不一致或无法验证的输入会抑制信号，而不是推测结果。

## 用户角色

### 研究/运维用户

使用默认配置启动服务，观察本地证据和信号，检查数据失效与恢复过程，并只对仍有效的关系执行人工审批。

### 开发维护者

维护同步、监控、策略、持久化与通知组件；在离线测试中验证 fail-closed 行为、Schema v3 一致性和 CLI 合同，不把未接入的 provider 或交易能力描述为现有功能。

## 用户场景

1. 安装项目后，用户执行 `predmarket --help`，再以 `predmarket run` 启动长期运行的公开数据采集和信号服务。
2. 服务运行并初始化数据库后，用户在另一终端以 `predmarket status`、`predmarket signals list`、`predmarket relations list` 或 `predmarket relations show RELATION_ID` 查看本地 SQLite 证据；这些查询不访问 Polymarket。
3. 用户先查看关系，对已有、语义证据仍有效且状态为 `LLM_APPROVE` 的关系执行 `predmarket relations approve RELATION_ID`。审批仅修改本地关系状态并记录激活事件，不会修改 Polymarket。
4. 用户通过 `system_events`、终端/桌面通知以及 WebSocket 断线后的 REST 恢复信息，排查订单簿或同步数据为何失效、何时恢复。
5. 用户停止 `predmarket` 后，先运行 `python scripts/reset_database.py --config config/default.yaml` 检查 dry-run 的绝对路径；确认无误才使用 `--execute` 清理临时本地证据库。

## 产品目标

- 本地运行：数据、证据和操作记录保存在本地 SQLite。
- 对 Polymarket 只读：仅访问公开市场与订单簿数据，不产生外部交易动作。
- 证据驱动：每个可观察机会都可回溯其计算、订单簿与生命周期修订。
- Fail-closed：输入不完整、过期或不一致时不产生可用信号。
- 可审计：保留系统事件、关系状态和信号 revision，供本地查询与排查。
- 可测试：核心行为可在离线环境中验证；外部 SDK 连通性另行处理。

## 非目标

当前版本不提供或不实现以下能力：

- 自动交易、下单、撤单或交易执行；
- 钱包、认证、签名或凭据管理；
- 对真实成交概率的预测；
- 链上操作；
- 新增 LLM provider、API key 或默认 LLM 分析服务；
- v2 之前的 SQLite Schema 兼容层；
- 全量 WebSocket 消息的永久保存。

## 功能需求

### 公开市场同步

- 系统通过唯一的 `PolymarketGateway` 访问 Polymarket 公开 REST/WebSocket 数据边界。
- 同步公开市场、事件、token、fee schedule 和 NegRisk 元数据，并以完整的同步 generation 标识数据批次。
- 只有完整 generation 中一致的市场、事件和 token 元数据可参与后续计算。

### 实时订单簿监控与恢复

- 系统订阅实时订单簿变化，并将变化转换为待评估的本地市场变更。
- WebSocket 中断、队列溢出或订单簿无效时，必须先使旧 subscription generation 失效；随后通过 REST 快照建立新 generation，才恢复基于订单簿的评估。
- 不永久保存全量 WebSocket 消息；仅持久化产生信号或审计所需的证据。

### 策略评估

- 支持二元低估 `BINARY_UNDERPRICED` 与二元高估 `BINARY_OVERPRICED` 评估。
- 支持仅针对已审批关系的逻辑蕴含 `LOGICAL_IMPLICATION` 评估。
- 支持使用 SDK 权威 NegRisk 元数据、完整成员集合和受支持转换信息的 `NEG_RISK_COMPLETE_SET` 评估。
- 使用 `Decimal` 计算收益、风险、手续费和深度；同时约束 bankroll、最小数量、订单簿年龄（book age）与腿部时间偏差（leg skew）。
- 任何元数据、费用、订单簿或关系条件不满足时，策略以不可评估或关闭机会处理，不补全或猜测缺失值。

### 信号与证据

- 信号以 `OPEN`、`UPDATED`、`CLOSED` 三类生命周期事件保存，并关联 revision 与计算/订单簿 evidence。数据库主信号记录的状态为 `OPEN` 或 `CLOSED`；`UPDATED` 是同一信号的 revision 事件，不表示新增订单状态。
- `OPEN` 表示首次记录机会，`UPDATED` 表示证据或计算修订，`CLOSED` 表示机会消失或无法继续验证。
- 信号是机会观察，不代表订单执行、成交、结算或实际收益。

### 关系发现与人工审批

- 系统支持逻辑关系发现和分析扩展点，关系状态依次为 `NO_LLM_APPROVE`、`LLM_APPROVE`、`APPROVED`。
- `relations approve` 仅接受仍有效的 `LLM_APPROVE` 关系，并写入本地审批状态和激活系统事件。
- CLI 保留 `relations analyze` 路由，但标准 `predmarket` console entry 尚未接入 analyzer provider；默认 `relations.llm_enabled: false`，仅打开开关不会自动配置 LLM。

### 本地查询、存储与通知

- Schema v3 SQLite 保存市场证据、关系、信号 revision 与 `system_events`，数据库写入由 `DatabaseWriter` 串行化。Decimal 字段以可保留任意小数位的规范化 `TEXT` 保存，由 Python `Decimal` 读写；v2 数据库在初始化时事务性迁移到 v3。
- `status`、`signals list/show` 与 `relations list/show` 通过只读 SQLite 连接查询本地数据；数据库须先由 `run` 初始化。
- 支持 terminal 与 desktop notifier 配置，并为启动、同步、监控与恢复中的重要故障提供通知。

### CLI 入口

- 以下命令均应从仓库根目录执行。
- 安装后以 `predmarket` 作为主入口，默认配置为 `config/default.yaml`，默认数据库文件名仍为 `data/predmarket-v1.sqlite3`（文件名为历史兼容名称，SQLite Schema 为 v3）；配置相对路径按当前工作目录解析。
- `python -m predmarket` 保留为兼容入口，功能与主入口一致。

## 数据生命周期

同步 generation、订单簿 subscription generation、关系状态和信号 revision 共同隔离证据时间边界，防止旧证据污染新计算：

1. 每轮目录同步建立新的同步 generation；未完成 generation 的市场、事件或 token 不得参与策略评估。
2. 每次订单簿订阅或恢复使用新的 subscription generation。断线后先失效旧 generation，再取 REST 快照；旧 generation 的订单簿不能与新 generation 混合计算。
3. 关系从 `NO_LLM_APPROVE` 到 `LLM_APPROVE`，只有当前证据仍有效时才能人工转为 `APPROVED`；逻辑蕴含策略只使用 `APPROVED` 关系。
4. 同一 opportunity 的新证据创建新的 signal revision。可验证机会首次生成 `OPEN`，计算改变生成 `UPDATED`，失效或消失生成 `CLOSED`；revision 将每次结论绑定到当时的 evidence。

## 非功能需求

- 只读外部边界：运行时不认证、不持有钱包、不签名、不下单、不撤单、不执行链上操作。
- 安全 reset：reset 为独立脚本，要求服务停止、dry-run 审核和显式 `--execute`；只处理配置的 SQLite 主文件及精确的 `-wal`、`-shm` 同级文件。
- 可审计与一致性：Schema v3 约束持久化结构；写入串行化，查询有专用只读边界。
- Fail-closed 与 Decimal 精度：金融数值使用 `Decimal`，不以二进制浮点代替；不可靠数据不输出可用信号。
- Bounded queue：市场变更队列容量受配置限制，溢出会记录事件并要求快照恢复。
- 恢复能力：WebSocket 中断后按 generation 失效与 REST 快照恢复，不继续使用旧订单簿。
- 离线可测试性：核心单元、集成与文档命令检查不依赖外网；可选的 live SDK smoke 单独依赖网络。

## 产品限制

- 产品不交易，也不预测真实成交概率。
- 产品不保存全量 WebSocket 消息，只保留本地审计所需的证据和事件。
- 标准 CLI 没有 analyzer provider，`relations analyze` 不能被视为默认可用的 LLM 分析功能。
- Schema v2 会在初始化时迁移到 v3；其他旧版本或未知版本拒绝启动。
- live SDK smoke 依赖外部网络和可用的 Polymarket 公开接口，不能作为离线验证的前提。
- `CLOSED` 不表示成交完成、结算或实现收益。

## 验收标准

当前版本至少以以下命令验证代码、入口和格式：

```console
pytest -q
python -m predmarket --help
predmarket --help
python -m compileall -q predmarket
git diff --check
```

同时应确认：文档只承诺当前代码能力；`predmarket` 为主入口且 `python -m predmarket` 兼容；默认配置和数据库路径准确；关系分析 provider 与交易能力均未被虚构为已实现。

## 当前版本之后的候选方向

本节均为未实现候选方向，不属于当前版本功能需求：

- 以明确配置和安全边界接入 analyzer provider。
- 在独立的认证、钱包、签名、风险控制和执行设计完成后，研究交易执行能力。
- 在明确兼容策略后，设计未来 SQLite Schema 演进。
