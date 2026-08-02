# 项目文档体系、中文 README、CLI 入口与 PRD 设计

**日期：** 2026-08-02
**状态：** 设计已确认，待用户审阅书面 spec
**适用基线：** `main` 分支，提交 `7db782d`

## 1. 背景

当前项目已经完成 Greenfield market signal system 的主要实现，代码基线包含：

- Python 3.11+ 模块化单体应用；
- 固定版本 `polymarket-client==0.3.0b1`；
- Schema v1 SQLite 本地证据库；
- 市场同步、实时订单簿监控、策略计算和信号生命周期；
- 二元完整集、逻辑蕴含和 SDK 权威 NegRisk 机会评估；
- 逻辑关系发现、分析和人工审批流程；
- fail-closed 数据校验、订单簿恢复和安全数据库 reset；
- 仅访问 Polymarket 公开数据，不认证、不持有钱包、不签名、不下单。

现有文档能够覆盖大部分架构和安全约束，但存在以下问题：

1. `README.md` 仍以英文为主，缺少面向使用者的项目背景、完整描述和快速上手流程。
2. 文档命令主要使用 `python -m predmarket`，没有把已声明的 console entry `predmarket` 作为主入口。
3. 文档之间有重复内容，用户无法快速判断某份文档的职责和适用场景。
4. 项目缺少一份面向产品和业务理解的 PRD。
5. 文档命令测试当前只解析 `python -m predmarket`，无法验证用户实际使用的 `predmarket` 命令。
6. `relations analyze` 在 CLI 中存在路由，但标准 console 入口没有配置实际 `RelationAnalyzer`，不能被描述成开箱即用的 LLM 分析命令。

本设计的目标是让文档准确反映当前代码，同时把安装后的日常使用收敛到简单、稳定的 `predmarket ...` 命令。

## 2. 目标

### 2.1 用户目标

用户完成项目安装后，可以直接执行：

```console
predmarket --help
predmarket run
predmarket status
predmarket signals list
predmarket relations list
```

无需记忆完整的 `python -m predmarket` 模块调用方式。

### 2.2 文档目标

- 用中文重写 `README.md`，作为项目主入口。
- 新增中文 `docs/PRD.md`，描述产品背景、目标、功能、限制和验收标准。
- 为每份现有文档定义单一职责，减少重复和相互矛盾。
- 所有面向用户的命令优先使用 `predmarket` console entry。
- 保留 `python -m predmarket` 作为兼容入口并在文档中说明。
- 使文档中的功能描述、策略术语、信号语义、数据库版本和安全边界与当前代码一致。
- 增强文档命令校验，防止以后文档和 CLI 漂移。

### 2.3 产品目标

PRD 应将当前代码描述为一个本地、只读、可审计的 Polymarket 市场信号研究工具，而不是交易机器人。PRD 需要明确：

- 当前产品解决的问题；
- 当前版本实际提供的能力；
- 用户可以执行的操作；
- 系统如何产生和关闭信号；
- 系统明确不做什么；
- 目前的实现限制和后续可能方向。

## 3. 非目标

本次不包括：

- 自动交易、下单、撤单、钱包或签名能力；
- LLM provider、API key 或自动分析服务的新增实现；
- 将数据库 reset 包装为普通的 `predmarket reset` 子命令；
- Schema 迁移或数据库兼容层；
- 修改历史 `docs/superpowers/specs/` 与 `docs/superpowers/plans/` 的设计记录；
- 与文档无关的业务逻辑重构、策略调整或依赖升级；
- 为未安装项目的环境额外维护 shell wrapper。

破坏性的 SQLite reset 继续作为独立脚本运行，并保留 dry-run、进程检查、目标校验和显式 `--execute` 边界。

## 4. 现状审查结论

### 4.1 当前 CLI 事实

`pyproject.toml` 已声明：

```toml
[project.scripts]
predmarket = "predmarket.cli:main"
```

因此，完成包安装后，`predmarket` 已是合法的命令入口。当前 CLI 包含：

| 命令 | 当前行为 | 是否适合作为普通用户命令 |
| --- | --- | --- |
| `predmarket run` | 启动长期运行的只读采集和监控服务 | 是 |
| `predmarket status` | 只读查看本地 SQLite 统计 | 是，要求数据库已初始化 |
| `predmarket signals list` | 只读列出信号 | 是 |
| `predmarket signals show SIGNAL_ID` | 只读查看单个信号 | 是 |
| `predmarket relations list` | 只读列出关系 | 是 |
| `predmarket relations show RELATION_ID` | 只读查看关系 | 是 |
| `predmarket relations approve RELATION_ID` | 在满足状态和语义校验时写入审批和激活事件 | 是，属于受控运维操作 |
| `predmarket relations analyze RELATION_ID` | 调用注入的 analyzer 并保存分析 | 当前标准入口不可直接使用 |

`relations analyze` 的限制必须在文档中明确：配置中的 `relations.llm_enabled` 只表示功能开关，当前标准 console entry 没有 analyzer provider 配置，也不会自动创建 LLM 客户端。只有程序化调用 `main(..., analyzer=...)` 或后续补充 analyzer wiring 后，才可执行该流程。

### 4.2 当前运行事实

- 默认配置为 `config/default.yaml`，相对路径以当前工作目录解析。
- 数据库默认路径为 `data/predmarket-v1.sqlite3`。
- `predmarket run` 会初始化数据库并执行完整性检查。
- `status`、`signals` 和 `relations` 的读取不会访问 Polymarket。
- 数据库写入由运行时 `DatabaseWriter` 串行化；关系 CLI 使用独立短事务。
- 信号是机会观察和证据记录，不代表成交、结算或实际收益。
- `CLOSED` 表示机会消失或无法继续验证，不表示交易完成。
- 全量订单簿消息不会永久保存，只保存产生信号时所需的证据。
- 默认配置关闭关系 LLM 分析：`relations.llm_enabled: false`。
- live SDK smoke test 依赖外部网络和可用的 Polymarket 公开接口，不作为离线全量测试的必要条件。

## 5. 选定方案

采用“文档体系重整 + console entry 产品化使用 + 文档命令合同测试”的方案。

该方案保留现有文件的工程历史和职责边界，不通过大规模迁移制造额外 churn；同时让用户入口、文档索引和 PRD 形成稳定的阅读路径。

## 6. 文档信息架构

### 6.1 文档职责和语言

| 文件 | 职责 | 语言要求 |
| --- | --- | --- |
| `README.md` | 项目主入口、安装、快速开始、命令索引和边界说明 | 中文 |
| `docs/PRD.md` | 产品需求、用户场景、功能需求、验收标准和版本限制 | 中文 |
| `docs/PROJECT-GUIDE.md` | 当前架构、组件边界、数据流、Schema v1 和一致性规则 | 保持工程文档风格，内容必须与代码一致 |
| `docs/TUTORIAL.md` | 从安装到运行、查看结果和关系操作的任务式教程 | 保持工程文档风格，内容必须可执行 |
| `docs/OPERATIONS.md` | 运行监控、恢复、队列溢出、信号关闭和数据库 reset | 保持工程文档风格，强调安全边界 |
| `STRATEGY.md` | 策略类型、收益/风险公式、数量优化和资格条件 | 保持工程文档风格，公式必须与实现一致 |
| `SECURITY.md` | 只读边界、reset 威胁模型和文件安全规则 | 保持工程文档风格，边界必须与实现一致 |
| `docs/VERIFICATION.md` | 测试、静态检查、文档命令校验和验证前提 | 保持工程文档风格，命令必须可执行 |
| `docs/superpowers/specs/*` | 设计历史和技术决策记录 | 保留历史，不在本任务中重写 |
| `docs/superpowers/plans/*` | 实施计划和执行记录 | 保留历史，不在本任务中重写 |

README 和 PRD 面向使用者和产品理解，使用中文；其余工程文档沿用当前项目语言风格，仅修正内容、命令和代码事实，不把全量翻译扩大成独立任务。

### 6.2 README 内容要求

README 至少包含以下章节：

1. 项目背景：Polymarket 公开市场数据、机会发现和研究场景。
2. 项目描述：本地、只读、证据驱动的市场信号服务。
3. 核心能力：市场同步、实时订单簿、策略评估、信号生命周期、关系审批和本地审计。
4. 安全边界：不认证、不持有钱包、不签名、不下单、不撤单、不执行链上操作。
5. 系统架构概览：Gateway、Sync、Watch、Strategy、SignalManager、SQLite 和 Notifier。
6. 环境要求：Python 3.11+、依赖安装和可选测试依赖。
7. 快速开始：使用默认配置执行 `predmarket run`。
8. CLI 命令表：命令用途、读写性质和必要前提。
9. 配置说明：配置文件位置、数据库路径、策略阈值、通知和关系分析开关。
10. 信号解释：`OPEN`、`UPDATED`、`CLOSED` 的语义，以及信号不是成交记录。
11. 数据库和 reset 说明：查看数据和链接到安全运维文档。
12. 文档索引：链接到 PRD、架构、教程、策略、运维、安全和验证文档。
13. 常见问题：数据库不存在、Polymarket 依赖、网络不可用、`relations analyze` 不可用等。

README 中的快速开始优先使用：

```console
uv sync --extra test
source .venv/bin/activate
predmarket --help
predmarket run
```

同时提供不使用 uv 的等价安装方式：

```console
python -m pip install -e ".[test]"
predmarket run
```

命令示例默认从仓库根目录执行。需要自定义配置时，使用：

```console
predmarket run --config path/to/config.yaml
```

### 6.3 PRD 内容要求

`docs/PRD.md` 需要描述当前版本，而不是虚构尚未实现的交易产品。结构如下：

1. 文档信息：版本、状态、适用代码基线。
2. 产品背景和问题定义。
3. 产品定位和价值主张。
4. 用户角色：研究/运维用户、开发维护者。
5. 用户场景：启动服务、观察本地证据、审核关系、排查数据失效。
6. 产品目标和非目标。
7. 功能需求：
   - 公开市场同步；
   - 实时订单簿监控和恢复；
   - 二元完整集、逻辑蕴含和 NegRisk 评估；
   - 收益、风险、深度和数量约束；
   - 信号的 OPEN/UPDATE/CLOSE 生命周期；
   - 关系发现、分析扩展点和人工审批；
   - SQLite 证据保存和查询；
   - terminal/desktop 通知；
   - CLI 操作。
8. 非功能需求：只读、安全、可审计、fail-closed、Decimal 精度、SQLite 一致性、恢复能力和可测试性。
9. 数据和生命周期说明：同步 generation、订单簿 generation、关系状态和信号 revision。
10. 产品限制：不交易、不预测真实成交概率、不保存全量 WebSocket 消息、标准 CLI 尚未接入 analyzer provider、旧 Schema 不迁移。
11. 验收标准：引用现有测试和文档验证命令。
12. 当前版本之后的候选方向：仅作为明确标注的后续规划，不写入当前已实现能力。

### 6.4 现有工程文档的整理要求

- `PROJECT-GUIDE.md` 作为架构和数据库事实的唯一主要来源，保留准确的十张业务表清单和 `user_version = 1` 说明。
- `TUTORIAL.md` 只保留用户能按顺序执行的流程，所有命令改为 `predmarket` 主入口，并说明 `status` 需要已初始化数据库。
- `OPERATIONS.md` 继续详细描述长时间运行、WebSocket 恢复、队列溢出、信号关闭和安全 reset；不把 reset 简化为普通删除命令。
- `STRATEGY.md` 使用当前实现中的策略名称、动作腿、收益率/风险率定义和 eligibility 条件；明确 `risk_rate` 不是亏损概率。
- `SECURITY.md` 反映 Gateway 唯一公开数据边界、无交易能力和 reset 的 POSIX/同 UID 威胁模型。
- `VERIFICATION.md` 同时验证代码、文档命令、Schema v1、完整性检查和可选 live smoke 的边界。
- 所有文档不应把历史架构决策中的未来计划写成已经存在的 CLI 或服务能力。

## 7. CLI 和脚本设计

### 7.1 主入口

安装项目后，以下命令作为文档主入口：

```console
predmarket --help
predmarket run
predmarket status
predmarket signals list
predmarket signals show SIGNAL_ID
predmarket relations list
predmarket relations show RELATION_ID
predmarket relations approve RELATION_ID
```

`--config` 默认值为 `config/default.yaml`，也支持放在子命令后：

```console
predmarket run --config config/default.yaml
```

`python -m predmarket ...` 继续保留为兼容和调试入口，但不再作为 README 的首选写法。

### 7.2 关系分析命令的准确表述

文档不能将以下命令描述成默认可执行的 LLM 功能：

```console
predmarket relations analyze RELATION_ID
```

应说明：

- 该命令的路由和工作流接口存在；
- `relations.llm_enabled` 默认关闭；
- 当前标准 console entry 没有 analyzer provider；
- 未注入 analyzer 时命令会拒绝执行；
- `approve` 只能处理已经处于 `LLM_APPROVE` 且当前语义证据仍有效的关系。

### 7.3 数据库 reset

reset 不纳入普通 CLI 子命令，继续使用独立脚本：

```console
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

文档必须要求先停止 `predmarket`，先检查 dry-run 输出的绝对路径，再决定是否使用 `--execute`。不得提供 wildcard、目录或 shell 删除替代方案。

### 7.4 是否新增代码

当前 `pyproject.toml` 已声明 console entry，因此原则上不新增包装脚本，也不改变业务 CLI 行为。实施时只在以下情况修改代码或配置：

- console entry 在实际安装验证中不可用；
- 文档命令合同测试需要支持 `predmarket` 形式；
- 为了保持兼容性必须补充最小的入口测试。

不为了统一命令而新增 `reset` 子命令或 LLM provider。

## 8. 文档命令合同测试

修改 `tests/integration/test_documented_commands.py`，使其能够识别两种合法形式：

```console
predmarket status --config config/default.yaml
python -m predmarket status --config config/default.yaml
```

测试要求：

1. 解析 README、核心 docs 和安全脚本文档中的命令。
2. 对 `predmarket` 命令去除命令名后交给 `_build_parser()` 校验。
3. 对 `python -m predmarket` 命令保持现有解析逻辑。
4. 保证 `--help` 不访问网络或数据库。
5. 保证默认文档至少覆盖 `run`、`status`、`signals list` 和 `relations list`。
6. 不执行会修改真实数据库的示例命令。
7. 对 `relations analyze` 的文档描述不要求无 analyzer 的标准入口成功。

必要时增加一个轻量安装/entry-point smoke test，验证包元数据中存在 `predmarket` console entry；测试不应依赖外部网络。

## 9. 验收标准

### 9.1 README 和 PRD

- `README.md` 为中文，并明确包含项目背景、项目描述和项目如何使用。
- README 可以指导新用户完成依赖安装、查看帮助和启动服务。
- README 首选展示 `predmarket ...` 命令。
- `docs/PRD.md` 存在，且只描述当前代码已实现或明确标记为后续规划的能力。
- README 和 PRD 明确说明只读边界、信号语义和非目标。

### 9.2 内容与代码一致

- 文档反映 Python 3.11+、`polymarket-client==0.3.0b1` 和 Schema v1。
- 文档准确描述四条策略路径：二元低估、二元高估、逻辑蕴含和 NegRisk 完整集；不把二元低估/高估重复误报成两个独立产品。
- 文档准确描述关系状态和 `relations analyze` 的当前限制。
- 文档准确描述信号不是订单或成交记录。
- 文档准确描述 reset 只删除配置主库及精确的 `-wal`、`-shm` 兄弟文件。
- 文档不再把不存在的同步子命令、旧 Schema 或交易能力写成当前功能。

### 9.3 验证

实施完成后至少运行：

```console
pytest -q
python -m predmarket --help
predmarket --help
python -m compileall -q predmarket
git diff --check
```

其中 `predmarket --help` 需要在项目已安装或 editable install 的环境中执行。若环境没有安装 console entry，应先按 README 的安装步骤安装，而不是将该环境问题误判为代码功能失败。

## 10. 风险和取舍

### 10.1 保留历史 spec/plan

历史 spec/plan 可能包含“计划实现”而不是最终代码事实。保留它们可以保留设计溯源，但 README、PRD 和工程指南不能直接把历史计划当作现状。当前事实应由代码、测试和维护中的工程文档共同确认。

### 10.2 不新增 `predmarket reset`

统一命令体验会更好，但 reset 是破坏性操作。继续使用独立脚本可让用户看到更强的操作意图，并保留现有安全校验。后续如果要加入 `predmarket reset`，应另立设计和安全审查任务。

### 10.3 不接入 analyzer provider

当前项目只有 analyzer 协议和工作流，缺少 provider 配置与凭证边界。把它包装成开箱即用命令会产生错误承诺，因此本 spec 只要求文档准确标记限制，不扩大到 LLM 集成。

## 11. 预期改动范围

预计实施阶段修改或新增：

```text
README.md
docs/PRD.md
docs/PROJECT-GUIDE.md
docs/TUTORIAL.md
docs/OPERATIONS.md
docs/VERIFICATION.md
STRATEGY.md
SECURITY.md
tests/integration/test_documented_commands.py
```

`pyproject.toml` 只有在 entry-point 安装验证发现缺口时才修改；业务源码、数据库 Schema、历史 spec/plan 和 reset 安全实现默认不修改。
