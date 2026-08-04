# Runtime investigation log — 2026-08-04

目标：模拟人工启动 `predmarket`，持续验证目录同步、市场监听和信号产出；每个运行问题均记录证据、根因、修复和验证结果。

## 基线

- 启动命令：`PYTHONUNBUFFERED=1 .venv/bin/predmarket run --config config/default.yaml --log-level DEBUG`
- 数据库：`data/predmarket-v1.sqlite3`（约 818 MB，`PRAGMA integrity_check` 返回 `ok`）
- 核心数据量：events 15,822；markets 120,111；tokens 240,222；watchable markets 120,054；watchable tokens 240,108。
- 信号基线：relations 0；arbitrage_signals 0；signal_revisions 0。

## ISSUE-001：SOCKS 代理缺少运行依赖

- 状态：已修复（已有提交 `d7818a2`）。
- 现象：WebSocket 启动时报 `ImportError: python-socks is required to use a SOCKS proxy`。
- 证据：macOS 系统代理将 HTTP、HTTPS、SOCKS 均配置为 `127.0.0.1:7890`；`websockets` 对 WSS 优先采用 SOCKS 代理。
- 修复：将 `python-socks` 加入项目依赖并同步锁文件。
- 验证：错误不再出现，程序已进入 WebSocket 恢复订阅阶段。

## ISSUE-002：SDK 断线日志丢失 close code/reason

- 状态：已在工作区修复，尚未提交。
- 现象：应用只记录 `recovery stream invalidated: connection_lost`，无法判断 WebSocket 如何关闭。
- 根因：应用层收到的是 SDK 归一化后的 `connection_lost` 事件，原始 close code/reason 未透传。
- 修复：在 SDK market stream manager 的 `_on_socket_connection_lost(code, reason)` 回调边界增加日志，再调用原回调。
- 验证：单元测试覆盖回调安装时机、日志字段和原回调调用；真实启动记录到 `close_code=1006 close_reason=''`。

## ISSUE-003：启动时恢复市场监听失败

- 状态：已在工作区修复，待真实运行验证。
- 现象：启动约 9 秒后在 `watch.start() -> _recover()` 中失败，应用随后记录 `runtime_startup_failed` 并退出。
- 已确认事实：断线日志早于 `app.py` 的失败日志；code 1006 且 reason 为空，表示连接异常丢失，没有收到包含服务端原因的 close frame。
- 规模证据：恢复订阅包含 240,108 个 token ID；ID 文本总长度约 18,497,765 字符，SDK 初始 JSON frame 预计约 19 MB。
- 对照实验：使用同一 SDK、同一 SOCKS 代理和数据库中的同一类 token，以 2 个 token 直接订阅时，0.564 秒完成连接并立即收到 `MarketBookEvent`。
- 当前结论：代理和 WebSocket 基础链路可用；故障与一次性恢复 240,108 个 token 的规模强相关。实测 100 至 50,000 个 token 可连接，100,000 个 token 以 code 1006 断开；REST `/books` 在 1,000 个 token 时拒绝请求并返回 `Payload exceeds the limit`。
- 修复：仅选择完整、未过期且可下单的市场，按结束时间确定性排序，并通过 `runtime.watch_market_limit` 将默认监听范围限制为 100 个市场。

## ISSUE-004：目录同步 generation 不完整

- 状态：已在工作区修复，待真实运行验证。
- 现象：`system_events` 存在重复的 `SYNC_GENERATION_INCOMPLETE`；最近 generation 已读取 events 15,843、markets 121,873、tokens 243,746，但因 market 引用的 event 不在完整 generation 中而未完成切换。
- 另一类历史证据：market `2278824` 的 `events=[]`，mapper 拒绝该数据（`events must contain exactly one event reference`）。
- 根因：上游仍返回 active market，但其 parent event 已不在本次 active event 集合中；完整性校验因此拒绝整个 generation。
- 修复：对这类孤立 active market 保留市场与 token，但将 `event_id` 脱离并记录 `sync_market_parent_missing` warning，使其不再阻止完整 generation 切换。

## ISSUE-005：当前没有可验证信号

- 状态：基线记录，待监听恢复后继续验证。
- 证据：`arbitrage_signals=0`、`signal_revisions=0`、`relations=0`。
- 备注：需要先恢复同步和监听，再从运行日志与数据库共同确认策略是否实际执行以及是否产生信号。

## ISSUE-006：目录仓储模块存在语法损坏

- 状态：已修复，尚未提交。
- 现象：`predmarket/persistence/repositories.py` 中的 `CatalogSnapshot` 声明被拆成 `class t` 和 `CatalogSnapshot:`，程序无法导入仓储模块。
- 证据：`.venv/bin/python -m py_compile predmarket/persistence/repositories.py` 返回 `SyntaxError: expected ':'`。
- 修复：恢复为 `class CatalogSnapshot:`，未修改类行为。
- 验证：模块 `py_compile` 通过；`tests/unit/catalog/test_repository_snapshot.py` 结果为 `2 passed`。

## ISSUE-007：异步启动后 Gateway 未恢复持久化目录映射

- 状态：已在工作区修复，待真实运行验证。
- 现象：将恢复订阅缩小为 2 个 token 后，WebSocket 和 `/books` 请求均成功，但恢复阶段报 `GatewayMappingError: order book <token> has no mapped token identity`。
- 根因：`PolymarketGateway` 的 token、market 和 condition identity 映射只在远端 `list_active_markets()` 时填充；当前启动顺序先执行 `watch.start()`，Watcher 从 SQLite 读取了目录，但新建 Gateway 没有用该快照恢复映射。
- 影响：即使解决超大订阅问题，异步启动的恢复流程仍必然在首批订单簿映射时失败。
- 修复：在监听恢复和目录变更切换订阅前，用 SQLite 的 `CatalogSnapshot` 显式 hydrate Gateway identity；保持目录同步异步启动，不重新引入阻塞式 `run_once()`。

## ISSUE-008：运行阶段日志不足，无法判断具体阻塞边界

- 状态：已修复，尚未提交。
- 现象：启动日志在数据库加载、WebSocket 恢复和目录同步期间存在长空窗，只能从最终异常反推阶段。
- 修复：为 runtime build、watch bootstrap、目录加载、订阅规模、Gateway subscribe、REST baseline、首次评估和 catalog sync 各阶段增加结构化 INFO 日志，包含数量与 `elapsed_ms`；监听消息在首条、每 1,000 条或间隔 30 秒时限频输出进度。
- 安全约束：不输出完整 token ID 列表；只记录 `token_id_bytes`，避免日志泄露和大对象序列化。
- 验证：`tests/integration/test_app_pipeline.py`、`tests/unit/watch/test_task.py`、`tests/unit/catalog/test_sync.py`、`tests/unit/polymarket/test_gateway.py` 共 154 个测试通过。

## ISSUE-009：旧目录把过期和不完整市场纳入监听

- 状态：已在工作区修复，待真实运行验证。
- 现象：旧选择器只检查状态和 orderbook 标志，数据库中 30,810 个仍标记 accepting orders 的市场实际上已经超过 `end_at`；不完整 generation 的对象也可能被恢复监听。
- 影响：启动订阅集合异常膨胀，且策略会因输入不完整而拒绝评估，无法产生有效信号。
- 修复：市场必须完整、未过期且其全部 token 与市场属于同一完整 generation 才可监听；默认最多选择结束时间最近的 100 个市场。

## ISSUE-010：持久化目录无法独立恢复 Gateway identity

- 状态：已在工作区修复，待真实运行验证。
- 现象：应用异步启动后 Watcher 直接读取 SQLite，而 Gateway 的内存 identity map 为空。
- 修复：新增 `hydrate_market_identities`，在 WebSocket 恢复前从持久化 market/token 快照恢复映射。
- 验证：配置、应用管线、同步、监听和 Gateway 相关测试共 162 个通过。

## ISSUE-011：零个可监听市场时仍执行 Gateway hydration

- 状态：已修复，真实启动待复验。
- 现象：旧目录经完整性与过期过滤后得到 0 个市场，但启动仍调用 Gateway hydration，触发 `ValueError: token_ids must not be empty`，后台同步没有机会启动。
- 根因：Watcher 已对空 token 集合跳过 recovery，却没有对空 market 集合跳过 hydration。
- 修复：空集合时跳过 identity hydration，输出 `watch_gateway_identities_hydration_skipped markets=0`，并按无订阅状态启动。
- 验证：回归测试 `test_start_and_close_allow_an_empty_catalog_without_recovery` 通过。

## ISSUE-012：缺失父事件 warning 造成日志洪泛

- 状态：修复中。
- 现象：完整同步发现 2,823 个缺失父事件的 active market，并逐条输出 warning，单次运行产生约 11 万 token 的日志，淹没后续恢复失败信息。
- 根因：`SyncMarketTask` 没有对同类批量告警采样或汇总。
- 修复方案：保留前 10 条样本并输出总数和省略数，完整 warning 仍保留在 `SyncResult.warnings`。

## ISSUE-013：完整同步后市场变更队列洪泛

- 状态：修复中。
- 现象：完整 generation 持久化 153,154 个市场后逐市场发布十万级 `MARKET_ADDED`；容量 10,000 的队列满后，每次丢弃都同步写 system event 和通知，导致同步长期停留在发布阶段并持续输出 ERROR。
- 根因：Watcher 收到任一非关键目录变化后都会重新读取原子快照，逐市场投递可丢弃变化没有额外信息价值；当前实现没有按完整快照语义合并批量通知。
- 修复方案：超过队列容量的批次保留全部关键控制变化，并将可丢弃变化合并为一条刷新触发；输出合并数量日志。

## ISSUE-014：恢复阶段 SDK 订阅事件队列溢出

- 状态：初始恢复已缓解；运行期仍会溢出，后续根因见 ISSUE-021。
- 现象：100 个市场（200 个 token）的 WebSocket 订阅在 524 ms 内成功，但 REST baseline 约 1.65 秒后报 `recovery stream invalidated: subscription_event_dropped`。
- 根因：SDK `AsyncSubscriptionHandle` 内部队列固定容量为 1,024；baseline 请求期间收到的初始 book/update 事件超过容量，SDK 按 drop-oldest 丢弃，Gateway 的生命周期探针正确拒绝不完整恢复。
- 初步修复：默认监听上限降为 50 个市场（通常约 100 个 token），使初始事件规模与 REST `/books` 的 100-book 实测边界一致；保留显式配置能力。真实运行证明这只能让 REST baseline 成功，不能解决运行期消费速度不足。

## ISSUE-015：恢复集成测试替身未实现新增 Gateway 协议

- 状态：已修复。
- 现象：相关测试集首次运行得到 174 passed、1 failed；`test_watch_recovery` 的 `_Gateway` 缺少 `hydrate_market_identities`。
- 根因：持久化 identity 恢复加入 Watcher 后，旧集成测试替身未同步协议。
- 修复：为测试替身补充无副作用的 hydration 方法，生产代码不做兼容性降级。

## ISSUE-016：单个结算竞态市场使整个恢复启动失败

- 状态：已修复并通过真实启动验证。
- 现象：限制为 50 个市场后，WebSocket 订阅成功，REST baseline 在 619 ms 内返回 98/100 个 order book；随后严格完整性校验报缺少 2 个 token，应用整体退出。
- 数据库证据：缺少的两个 token 同属市场 `3301076`；SQLite 仍记录其 `status=ACTIVE`、`active=1`、`accepting_orders=1`、`enable_orderbook=1`、`resolved_at=NULL`。
- 上游复核：SDK `get_market("3301076")` 同样返回 active 且 accepting orders，但 resolution 已是 `PROPOSED`，两个 token 单独请求均返回 `No orderbook exists for the requested token id`。
- 根因：市场进入结算时，上游市场元数据与 CLOB order book 的生命周期存在竞态；目录选择时尚可监听，但恢复 baseline 时 order book 已消失。当前严格恢复将一个市场的竞态扩大为所有市场启动失败。
- 修复方案：Gateway 仅在 REST 明确缺少 order book 时，按 identity 映射剔除缺失 token 所属的完整市场，关闭旧订阅并用剩余市场新 generation 重试；Watcher 采用恢复会话返回的实际 token 集合并关闭被剔除 token 的旧信号。连接与事件完整性错误继续 fail-closed。
- 真实验证：100 个 token 中 14 个 token（7 个完整市场）缺少 order book；第 1 代订阅被关闭，第 2 代以 86 个 token/43 个市场取得 86 本完整 baseline，Watcher 进入首次评估。

## ISSUE-017：首次策略上下文生成错误使用异步生成器

- 状态：已修复，待真实启动复验。
- 现象：恢复 baseline 成功后，首次评估在 `_ApplicationContextSource.contexts_for()` 报 `TypeError: 'async_generator' object is not an iterator`，应用退出。
- 根因：`tuple(await self._target(...) for ...)` 在当前 Python 中创建异步生成器，`tuple()` 只能消费同步迭代器。
- 修复方案：显式逐项 await 并物化 `EvaluationTarget`，补充使用真实 SQLite repositories 的上下文集成测试。
- 验证：新增集成测试先复现相同 `TypeError`，修复后通过。

## ISSUE-018：首次评估为每个 token 重复读取完整目录

- 状态：已修复并通过真实启动验证。
- 现象：首次评估 86 个 token；`contexts_for()` 每次都调用 `CatalogRepository.load_catalog()`，单次真实加载 21,624 events、153,154 markets、306,308 tokens 约需 5.6 秒，理论上首次评估会耗时数分钟。
- 根因：Watcher 已持有用于选择订阅的原子目录快照，但上下文源没有接收和复用它，导致每个 token 重复进行全表读取、领域对象反序列化和索引构建。
- 修复：Watcher 在启动和处理目录变更时，把同一原子快照交给支持缓存的上下文源；应用上下文源一次构建 token/market/event 索引，后续 token 评估复用，并记录快照准备耗时。目录更新即使不改变订阅范围，也会在提前返回前刷新上下文索引。
- 验证：新增启动快照交接与同范围目录更新刷新测试，连同真实 SQLite 上下文物化测试共 3 个通过。
- 真实验证：目录快照加载 5.649 秒后，策略索引一次构建耗时 279 ms；100 个 token 的首次评估耗时 332 ms，不再出现逐 token 重读全量目录的分钟级等待。

## ISSUE-019：监听进入持续全量恢复循环

- 状态：已在工作区修复，待真实运行验证。
- 现象：真实启动完成 generation 2 的 84-token baseline 和首次评估后，每约 1–2 秒关闭订阅、重新获取全部 REST baseline 并再次评估；约 18 秒内进入 generation 12。
- 已排除：SDK close-frame 回调没有输出 `market_stream_connection_lost`，因此当前证据不支持服务端断线；恢复本身每次都能取得 84/84 个 order book。
- 诊断增强：在所有 Watch 失效恢复入口输出 `reason_code`、`detail`、generation 和 token 数，直接确认触发分支。
- 直接证据：增强日志后，每一代首次流事件均输出 `reason_code=ORDERBOOK_INVALID detail=stream_book_differs_from_rest_baseline`，没有出现 `market_stream_connection_lost`；循环到 generation 32 后才因 `recovery_buffer_overflow` 退出。
- 根因：订阅建立后，SDK 会推送完整 WebSocket `book` 快照；Gateway 同时请求 REST baseline。两份快照采集时刻不同，盘口 hash 不同是正常市场变化，Watcher 却将任何 hash 差异视为损坏并重新订阅。每次新订阅又产生新 `book`，形成自激恢复循环，最后撑爆恢复缓冲区。
- 修复：把完整流 `book` 解析为领域 `OrderBook`，按 generation、market/token identity 和 exchange timestamp 与 REST baseline 对账；相同或更新的快照原子替换，较旧快照忽略，格式或身份异常继续 fail-closed。恢复期间缓存的旧 `price_change` 同样按 exchange timestamp 丢弃，且未应用时不触发策略评估。
- 自动验证：新增 4 个回归测试，先复现缺少快照应用接口和重复 recovery，再验证较新完整快照替换、较旧完整快照/增量忽略以及不关闭当前订阅。

## ISSUE-020：完整流盘口缺少最小订单量导致继续恢复

- 状态：已在工作区修复，待真实运行验证。
- 现象：ISSUE-019 首次修复后真实启动，仍逐代输出 `stream_book_invalid:min_order_size must be a non-empty string`，30 秒内进入 generation 15。
- 根因：SDK `MarketBookPayload.min_order_size` 和 `tick_size` 均为可选字段，真实首个 `book` 没有 `min_order_size`；测试 fixture 恰好总是提供该字段，掩盖了协议差异。
- 修复：流事件提供交易约束时严格解析；省略时复用同 token 已验证 REST baseline 的 `minimum_order_size`/`tick_size`。这些字段是市场交易约束，不随该次盘口层级变化；未知 token 没有 baseline 时仍 fail-closed。
- 验证：回归测试去掉 `min_order_size` 后先复现二次 recovery，修复后验证继承 REST 值且不关闭订阅；相关 16 个测试通过。

## ISSUE-021：流消息消费被策略评估和 SQLite I/O 阻塞

- 状态：异步评估已修复原阻塞；真实运行证明仍存在 SDK 固定小队列问题，见 ISSUE-023。
- 现象：50 个市场/100 个 token 已完成 REST baseline 和首次评估，但随后每 0.8–1.4 秒出现 `reason_code=SDK_DISCONNECTED detail=subscription_event_dropped`，不断重新恢复；全过程没有 WebSocket close-frame 日志。
- 直接证据：SDK `ClobMarketStreamManager.DEFAULT_QUEUE_SIZE=1024`，队列满时 `AsyncSubscriptionHandle` 丢弃最旧事件并累计 `dropped`；Watcher 原主循环每读取一条盘口消息，就同步等待上下文生成、策略评估和信号 SQLite 读写，期间不再读取下一条 SDK 消息。
- 根因：网络流摄取和串行策略/持久化处于同一背压链路。50 市场的实时事件速率高于逐事件 SQLite 评估吞吐，最终稳定撑满 SDK 队列；继续降低市场数只能推迟或降低复现概率，不能消除结构性问题。
- 修复：主循环只校验并应用盘口，然后把变更 token 合并进独立评估队列；单独的评估 worker 串行处理最新快照，同一 token 的重复请求自动合并。订阅切代时丢弃待处理旧请求，并在策略 await 前后校验 cache generation，禁止旧 generation 决策提交到新订阅。
- 诊断增强：新增流消费进度、评估排队/合并、批次耗时、generation 中止，以及按 `PRESENT`、`ABSENT.<reason>`、`NOT_EVALUABLE.<reason>.<detail>` 聚合的评估结果日志；记录实际持久化信号数。
- 自动验证：新增慢策略测试，证明第一条消息的评估被阻塞时第二条消息仍可更新 cache；新增 generation 切换测试，证明阻塞中的旧决策不会写入。Watch/cache 相关测试共 62 个通过。
- 回归中发现并修复：正常 `close()` 取消评估 worker 时，主循环曾误报 `watch strategy evaluator exited unexpectedly`；调整为优先识别 stop/closed，再传播非正常 worker 退出。
- 真实复验：约 2 分钟内成功读取约 19,000 条流事件，证明策略评估已与摄取解耦；但 subscription generation 从 2 增至 46，每一代仍在突发事件超过 SDK 句柄 1,024 容量后输出 `subscription_event_dropped`。因此异步评估是必要修复，但不足以吸收上游初始突发。

## ISSUE-022：二元策略错误依赖 Yes/No 展示文案

- 状态：已在工作区修复，待真实运行验证。
- 现象：完整 baseline 已取得 96 个 token，首次评估 192 个策略目标中有 172 个返回 `NOT_EVALUABLE.INPUT_METADATA_MISSING.binary_yes_no_mapping_incomplete`。
- 数据库证据：被监听的二元市场不仅使用 `Yes/No`，还使用 `Up/Down`、`Odd/Even` 和双方队名；全库 active token 中也存在大量 `Over/Under` 等标签。
- 根因：Gateway 已把 SDK outcome 顺序保存为权威 `Token.position=0/1`，完整集合的买入、卖出与 merge/split 公式对两侧对称；`evaluate_binary` 却再次按可变展示标签查找 `Yes` 和 `No`，错误拒绝绝大多数合法二元市场。
- 修复：按唯一的 `position=0/1` 选择两侧 token，展示标签仅保留为领域数据；缺失或重复 position 继续返回 `binary_position_mapping_incomplete`。
- 验证：新增任意 `Up/Down` 标签回归测试，修复前复现 `NotEvaluable`，修复后得到 `OpportunityPresent`；binary 策略 24 个测试通过。

## ISSUE-023：SDK 单订阅句柄 1,024 容量无法吸收真实流量突发

- 状态：已在工作区修复，待真实运行验证。
- 现象：ISSUE-021 后 Watcher 已持续读取流，但约每 1–3 秒仍出现 `subscription_event_dropped` 并全量恢复；每代通常已消费数百至两千条事件。
- 直接证据：固定版本 SDK 的 `ClobMarketStreamManager.DEFAULT_QUEUE_SIZE=1024`；创建订阅时把 manager `_queue_size` 复制给 `AsyncSubscriptionHandle`，队列满后采用 drop-oldest 并增加 `handle.dropped`。真实运行没有与每次丢弃对应的 close frame，主因是本地有界缓冲不足而非服务端主动断开。
- 修复：新增严格配置 `runtime.market_stream_queue_capacity`，默认 65,536；Gateway 在调用 SDK `subscribe()` 创建 handle 前，校验固定版本私有 `_queue_size` 形状并设置容量。输出 `market_stream_queue_configured capacity=... previous_capacity=...`，SDK 形状变化时继续 fail-closed。
- 验证：新增回归测试证明配置发生在 handle 创建之前，并验证容量日志；配置和聚焦 Gateway 测试通过。

## ISSUE-024：启动依赖全量市场扫描更新 fee，导致监听长期不可评估

- 状态：已在工作区修复，待真实运行验证。
- 现象：Watcher 已持续接收实时盘口，但策略汇总长期出现 `NOT_EVALUABLE.FEE_SCHEDULE_STALE.fee_schedule_stale`；SQLite 中被监听 token 的 fee 时间约落后当前时间 57 分钟。后台同步完成 events 后，markets 阶段数分钟没有完成。
- 直接证据：当前目录约 153,000 个市场；SDK/API 即使请求更大的 `page_size`，单页仍最多返回 100 条，因此完整 markets 扫描约需 1,530 页。完整 events 扫描实测 170 页、16,942 个 events、约 29.5 秒；events 内嵌市场只覆盖现有 active market 的约 98.4%，仍缺 2,030 个，不能作为完整市场目录的权威替代。并发刷新选中的 50 个市场实测约 0.98 秒且 50/50 成功。
- 根因：监听启动错误地等待后台完整目录扫描间接刷新少量被监听市场的动态 fee 元数据；扫描耗时接近或超过 fee freshness 窗口，使策略即使拿到新盘口也无法评估。
- 修复：Watcher 从持久化目录初选最多 50 个市场，以并发度 10 调用 `refresh_market`，把成功结果原子写回 SQLite，重新读取目录后再准备策略上下文、identity 和恢复订阅。单市场失败按样本汇总 warning，不阻塞其他成功市场；持久化失败仍直接失败，避免内存与 SQLite 不一致。
- 自动验证：新增启动顺序回归测试，确认刷新、持久化和重载发生在策略上下文与 recovery 之前；Gateway/Watch 聚焦测试共 116 个通过。

## ISSUE-025：完整目录分页阶段缺少持续进度日志

- 状态：已修复，待真实运行验证。
- 现象：events 或 markets 全量扫描期间只有阶段开始/结束日志，markets 超过 1,500 页时数分钟没有输出，外观上与程序卡死无法区分。
- 修复：Gateway 每完成 25 页输出 events/markets 当前页数、有效实体数、mapping warning 数及累计耗时，并在分页耗尽时输出最终汇总。
- 验证：新增 25 页 paginator 回归测试，确认周期进度和完成日志均输出；包含在上述 116 个通过测试中。

## ISSUE-026：`sdk_event_dropped` 缺少 SDK 解析失败现场

- 状态：诊断增强已完成；本轮长时间复验未复现事件丢弃。
- 现象：Gateway 只能观察到 SDK handle 的 `dropped` 增长并把订阅标记为无效，日志不能区分队列溢出与 SDK 在 `_on_message` 中因 Pydantic 校验失败而主动丢弃消息。
- 修复：在 SDK 收到 WebSocket frame 并执行 `_on_message` 的边界安装最小包装；正常消息不重复解析，仅当 `dropped` 在本次回调增长时重放校验并输出原始 event type、Pydantic 字段错误和限长 raw payload。保留 close code/reason 回调日志。
- 验证：回归测试先证明 malformed 消息只有 drop、没有原因日志，修复后可直接看到校验错误；真实运行约 2 分钟持续处理超过 76,000 条消息，未出现 malformed、drop、connection_lost 或 subscription invalidation。

## ISSUE-027：静态盘口被错误当作过期证据

- 状态：已修复，待真实信号验证。
- 现象：WebSocket 持续高速接收消息且 cache generation 保持 VALID，但评估几乎 100% 返回 `NOT_EVALUABLE.ORDERBOOK_STALE.orderbook_stale`；数据库始终没有信号。
- 根因：策略用每本盘口的 `exchange_timestamp` 同时判断 freshness 和多腿一致性。该字段表示交易所最后一次盘口变价时间；在无丢包且仍活跃的订阅中，静止盘口即使很久未变也仍是当前可成交状态，不能因最后变价较早而判过期。不同 token 最后变价时刻不同也不表示组合 cache 不一致。
- 修复：`StrategyContext` 新增当前 subscription generation 的本地 `orderbook_observed_at`。Watcher 在完整 REST baseline 安装以及每条已解析流事件到达时更新该水位，并在评估前注入上下文；生产 Watch 路径按水位判断连接证据新鲜度，同一有效 generation 不再用各腿最后变价时间判断 skew。交易所时间、接收时间和 generation 仍保留并继续做身份及因果校验。评估日志新增 `observation_age_ms`。
- 自动验证：新增静止且最后变价时间相差 500 ms、但订阅刚刚观测到的二元盘口用例；修复前构造参数不存在/旧逻辑会拒绝，修复后得到 `OpportunityPresent`。策略、Watcher 和领域模型相关 185 个测试通过。

## ISSUE-028：无信号时缺少收益差距诊断

- 状态：诊断增强已完成，待真实运行验证。
- 现象：修复订阅 freshness 后，Watcher 稳定处理超过 25,000 条行情且所有可计算结果均为 `PROFIT_BELOW_THRESHOLD`，但原汇总日志只有原因计数，无法判断结果仅略低于阈值，还是市场选择或计算明显异常。
- 修复：每个评估批次额外输出最高 `return_rate`、对应要求阈值、`opportunity_key` 和 market IDs；保持原聚合计数，且不输出盘口明细或完整 token 列表。
- 用途：用真实收益差距决定后续应继续等待、调整监听市场选择，还是排查策略计算；不会改变策略阈值或信号判定。

## ISSUE-029：同一批次按 token 重复评估相同机会

- 状态：已在工作区修复，待真实运行验证队列稳定性。
- 现象：二元市场的两个 token 都会生成同一组 `BINARY_UNDERPRICED:<market_id>` 和 `BINARY_OVERPRICED:<market_id>` 机会；恢复 100 个 token 时日志显示 200 个目标，但实际只有 100 个唯一机会。每个重复目标都会再次执行策略计算、查询 SQLite 当前信号并调用 signal manager。
- 根因：评估请求按变更 token 合并，但 `_evaluate_tokens()` 没有在批次内按稳定的 `opportunity_key` 去重。同一市场两侧同时变化时，CPU 和 SQLite 工作量近似翻倍，增加事件循环负载，是 65,536 容量 SDK handle 队列仍发生 `subscription_event_dropped` 的直接压力来源之一。
- 测试证据：新增两个 token 返回相同 `opportunity_key` 的回归测试；修复前策略和 signal manager 都被调用 2 次，断言预期 1 次失败。
- 修复：同一 cache generation、同一评估批次内只计算并提交每个 `opportunity_key` 一次；日志新增 `generated_targets`、实际 `targets` 和 `deduplicated_targets`，便于量化去重效果。
- 自动验证：新增回归测试转绿；Watcher 单元与恢复集成测试共 68 个通过。

## ISSUE-030：畸形全局 `new_market` 公告误使盘口 generation 失效

- 状态：已在工作区修复，待真实运行验证。
- 现象：真实运行约至 12:00:19 后，每条 `new_market` 都先输出 `game_start_time` 的 epoch-ms 校验失败，随后约 0.4 秒输出 `reason_code=SDK_DISCONNECTED detail=sdk_event_dropped`；generation 在约 12 秒内从 1 增至 9。
- 直接证据：固定 SDK 的 `manager.dropped_events` 只在 `parse_events()` 解析原始市场消息失败时增长；订阅句柄队列溢出由独立的 `handle.dropped` 统计。本次原始字段为 ISO 时间 `2026-08-08 14:00:00+00`，SDK 模型却只接受 epoch-ms，因此不是 65,536 句柄队列溢出。
- 根因：Gateway 把 SDK manager 的任何解析 drop 都视为盘口证据缺口。但 `custom_feature_enabled` 会广播全局 `new_market`，Gateway 本来就过滤这类不属于当前 token 的控制公告；其解析失败不会造成 `book`/`price_change` 连续性缺口，不应触发全量恢复。
- 修复：在 SDK `_on_message` 解析边界预校验 `new_market`；畸形公告输出带 `action=ignored_unscoped_control_event` 的 warning 并不交给 SDK drop 计数。其他事件解析失败、未知事件以及真实 handle 队列丢失仍保持 fail-closed。
- 自动验证：新增回归测试证明畸形 `new_market` 不增加 manager drop、生命周期仍有效；原有畸形 `price_change` 和 lifecycle drop 测试继续通过，共 7 个聚焦测试通过。

## ISSUE-031：订阅恢复期间旧评估的安全拒绝被误当成致命错误

- 状态：已修复并通过真实断线恢复验证。
- 现象：真实连接以 `close_code=1006` 异常断开后，Watcher 成功建立 generation 2 并取得 100/100 本 baseline；随后延迟评估 worker 抛出 `ValueError: subscription generation is unavailable`，导致整个 `WatchTask` 和 runtime 退出。
- 直接证据：异常来自 `SignalManager._apply_transaction()` 的提交前二次外部状态校验，而不是订阅恢复；这说明 generation 1 决策在等待串行数据库 writer 时遇到 generation 切换，已被 fail-closed 校验正确拒绝。
- 根因：Watcher 只在策略执行前后检查 generation，没有区分 `SignalManager` 提交前发现的正常换代竞态与真正的程序错误。拒绝旧证据本应只终止该评估批次，却作为普通 `ValueError` 传播并杀死长期 worker。
- 修复：为“订阅 generation 不可用或已过期”引入 `SubscriptionGenerationChanged`（保持 `ValueError` 兼容）；Watcher 仅捕获这一明确异常，输出 `watch_evaluation_aborted stage=signal_apply_generation_changed` 后丢弃旧批次。市场状态错误、关系状态错误及其他异常仍保持 fail-fast。
- 自动验证：新增提交阶段 generation 变化的回归测试；修复前测试模块因缺少异常类型失败，修复后与原有提交前二次校验测试共 3 个通过。
- 真实验证：generation 1、2、3 分别因 `close_code=1006` 失效后，旧评估按 `after_context` 等 generation 防线中止，Watcher 依次恢复至 generation 4 并继续消费超过 61,000 条行情；未再出现 `runtime_task_exited` 或 `runtime_stopped`。

## ISSUE-032：收益诊断日志输出无界 Decimal 精度

- 状态：已修复并通过真实运行验证。
- 现象：每个评估批次的 `best_return_rate` 直接输出 `Decimal.__str__()`，真实策略计算产生数百位小数；约 1 秒 2–3 条汇总日志即可制造大量无效文本并增加同步日志 I/O。
- 根因：ISSUE-028 新增诊断字段时没有限定仅供观测的展示精度。
- 修复：收益率和要求阈值统一格式化为固定 8 位小数；策略计算及信号判定继续使用原始 Decimal，不改变业务精度。
- 自动验证：新增超长 Decimal 格式化回归测试，修复前因缺少边界格式化失败，修复后与换代和去重测试共 3 个通过。
- 真实验证：长期运行中 `best_return_rate` 和 `required_return_rate` 持续以 8 位小数输出，未再出现数百位 Decimal 日志。

## ISSUE-033：REST 盘口接收时间记录在请求开始前

- 状态：已修复并通过真实重启验证。
- 现象：每次恢复取得完整 REST baseline 后，首次策略评估稳定出现大量 `NOT_EVALUABLE.ORDERBOOK_INVALID.orderbook_timestamp_causality_invalid`；例如 generation 2 的 100 个机会中有 28 个被拒绝。随着两侧盘口被 WebSocket 更新，错误逐渐消失。
- 根因：Gateway 在 `await client.get_order_books()` 之前记录 `received_timestamp`。请求在途期间交易所盘口时间继续前进，响应中的 `exchange_timestamp` 因此可能大于本地的请求开始时间，被策略正确地视为不满足时间因果关系。
- 修复：REST 请求成功返回后再读取本地时钟，并把该响应完成时间用于整批盘口。网络异常路径不产生盘口，因此无需记录开始时间。
- 自动验证：新增可在 REST await 内推进时钟的回归测试；修复前返回盘口仍使用旧时间并失败，修复后使用响应完成时间；Gateway 与恢复相关 6 个测试通过。
- 真实验证：新进程首批 100 个 token 的 baseline 评估为 `PROFIT_BELOW_THRESHOLD:84,ORDERBOOK_STALE:16`，`orderbook_timestamp_causality_invalid` 从旧进程的 28 个降为 0。

## ISSUE-034：启动刷新后的 fee 元数据会在长期监听中再次过期

- 状态：已修复并通过超过 freshness 窗口的真实运行验证。
- 现象：真实进程稳定消费约 146,000 条流消息后，评估结果从可计算的 `PROFIT_BELOW_THRESHOLD` 变成全部 `NOT_EVALUABLE.FEE_SCHEDULE_STALE.fee_schedule_stale`；进程和订阅仍存活，但此后无法产生任何真实信号。
- 根因：ISSUE-024 只在 Watch 启动时刷新一次被监听市场；监听期间没有按 freshness 窗口续期。后台全量目录同步需要扫描约 153,000 个市场，不能保证在 fee 到期前更新这 50 个市场。
- 修复：Watch 启动独立的周期刷新任务，仅刷新当前订阅的市场并持久化，然后原子替换策略上下文目录；刷新与 WebSocket 消费并行，不阻塞 SDK 事件读取。应用层从 `fee_schedule_max_age_seconds` 推导一半窗口作为刷新周期，当前配置为每 150 秒刷新一次。日志输出调度周期、每轮开始/完成、成功/失败数和耗时。
- 自动验证：新增 1 秒周期回归测试，证明 `run()` 在启动刷新后会再次刷新同一 active market、持久化并更新 context snapshot；Watch 与应用流水线相关 18 个测试通过。
- 真实验证：首次周期刷新在 12:24:34 开始，50/50 市场刷新成功，12:24:49 完成；期间流消息从 55,000 增长到 59,000。12:26:51 已超过启动元数据的 300 秒失效点，评估仍为 `PROFIT_BELOW_THRESHOLD`，未再出现 `FEE_SCHEDULE_STALE`。

## ISSUE-035：全量目录对象转换长时间占用事件循环

- 状态：已修复并通过聚焦测试，待真实重启验证。
- 现象：周期元数据刷新读取目录时，一个策略评估批次从约 300–400ms 突增到 4,998ms，流进度日志也出现空档。后台全量同步的 `catalog_load` 在 12:26:28.667 至 12:26:33.398 间造成约 4.7 秒无其他任务输出，恢复调度后立即处理到 WebSocket `close_code=1006`。
- 根因：`CatalogRepository.load_catalog()` 在 SQLite `fetchall()` 后，直接在 asyncio 事件循环中把 21,624 events、153,154 markets 和 306,308 tokens 转换为领域对象；这段 CPU 转换没有 await 边界。
- 修复：保持 SQLite 一致性读取和事务边界不变，把 rows 到 `CatalogSnapshot` 的纯 CPU 转换通过 `asyncio.to_thread()` 移到 worker thread，避免在事件循环内连续构造约 48 万个领域对象。
- 自动验证：新增线程边界回归测试，确认 snapshot materialization 不在事件循环线程执行；目录快照聚焦测试 3 个通过。真实运行需要重启进程才能加载该改动。

## ISSUE-036：目录同步后订阅新市场未立即刷新 fee 元数据

- 状态：已在工作区修复，待真实完整同步换代验证。
- 现象：12:31:04 全量同步完成并触发订阅换代后，generation 13 首批 100 个机会中有 30 个返回 `FEE_SCHEDULE_STALE`；此前旧订阅已由周期任务刷新且跨过 300 秒窗口仍正常。
- 根因：周期刷新只续期刷新时刻的 active market 集合；`handle_market_change()` 从新目录算出订阅后直接切代，新进入集合的市场没有在策略上下文和 recovery 前执行定向 fee 刷新。
- 修复：订阅候选集合出现新增 market 时，只并发刷新新增 market、持久化并重载目录，再重新计算最终订阅集合；刷新完成后才替换策略上下文和执行 recovery。新增 `watch_subscription_market_refresh_requested` 日志，并复用已有刷新阶段成功/失败日志。
- 自动验证：新增市场加入订阅的回归测试，修复前仅刷新启动市场、断言失败；修复后确认新增市场也被刷新和持久化，且 recovery 使用刷新后的四个 token。Watch 与 Catalog Sync 单元测试共 97 个通过。

## ISSUE-037：全量同步产生的市场变更超过队列容量

- 状态：已在工作区修复，待真实完整同步验证无背压。
- 现象：12:31:04 日志出现 `Market change queue is full ... action=backpressure`，同期订阅 generation 12 结束并恢复到 generation 13。
- 根因：上一次未完整发布的同步代会把已经发布过的 active market 重发为 critical `MARKET_UPDATED`。当数量超过 10,000 队列容量时，原 `_changes_for_delivery()` 因其 critical 属性逐条保留；而 Watch 每处理一条都会重新加载约 48 万对象的完整目录，形成巨大重复工作并让 producer 进入背压。
- 安全边界：完整目录已在任何通知入队前由单个事务提交；`MARKET_ADDED/MARKET_UPDATED` 对 Watch 的语义都是“目录已变化，请重载完整快照”，因此一个代表通知足以覆盖整批状态。`MARKET_DEACTIVATED/EVENT_SETTLED` 携带显式关闭 token 的控制语义，仍逐条保留且绝不合并。
- 修复：当批次超过队列容量时，不再依据 critical 属性保留所有目录唤醒通知，而是把所有 `MARKET_ADDED/MARKET_UPDATED` 合并为一条，逐条保留停用/结算控制事件。正常容量内的小批次完全不变。
- 自动验证：新增 12 条 critical `MARKET_UPDATED` 加 1 条 critical `MARKET_DEACTIVATED`、容量 10 的回归测试；修复前错误返回 13 条，修复后仅返回一条更新唤醒和停用控制事件。相关 97 个单元测试通过。

## ISSUE-038：生命周期轮询与 SDK close 回调竞争导致断线原因日志丢失

- 状态：已在工作区修复并通过聚焦测试，待真实重启验证。
- 现象：generation 1 在持续接收约 29,000 条行情后被判定为 `connection_lost`，但没有 `market_stream_connection_lost` 的 close code/reason 日志；紧接着 generation 2 的异常断线则正确记录 `close_code=1006 close_reason=''`。这证明不是所有断线都经过了当前日志包装。
- 根因：固定版本 SDK 在 reader finalizer 中先把 `_socket` 原子清空，再停止 heartbeat、关闭 socket，最后调用 `on_connection_lost(code, reason)`。但在 reader 进入 finalizer 前，WebSocket socket state 已可能不再是 OPEN。Gateway 生命周期轮询把 `manager.is_open == False` 立即视为断线并关闭订阅；SDK 的 `connection.close()` 若抢先取得仍存在的 socket，会把它归类为用户主动关闭并抑制 `on_connection_lost`，因此我们自己的 fail-closed 检测吞掉了服务端 close 信息。
- 修复：仍校验 `manager.is_open` 类型，但 socket 对象尚存在时不抢先关闭；只在 pinned SDK reader 已原子清空 `_socket` 后判定 `connection_lost`。这样 reader 保有清理权并能先在 SDK close 回调边界输出 code/reason，之后旧 generation 仍会立即 fail-closed。socket identity 更换、事件丢失和 handle 结束等其他判定不变。
- 自动验证：新增竞态回归测试，模拟 socket state 已关闭但 SDK reader 尚未清空 `_socket`。修复前 lifecycle 在 10ms 内抢先失效，断言失败；修复后会等待，随后模拟 SDK 清空 socket 并调用回调，可同时看到 `close_code=1001 close_reason='server shutdown'` 和 `connection_lost` invalidation。断线、连接替换、恢复期间失效等 14 个聚焦测试通过。

## ISSUE-039：无信号评估放大 SQLite 读写负载

- 状态：已在工作区修复，待真实运行验证耗时。
- 现象：真实监听能持续消费约 20 万条行情，但每个 16–40 token 的评估批次通常耗时 300–600ms，争用期间达到 4.7 秒；全量同步的 `catalog_load` 从无争用时约 5.6 秒增至 20.3 秒，变更计算另耗时约 65 秒。数据库始终为 0 个信号和 0 个修订。
- 根因证据：每个低于阈值或不可评估、且从未打开过的机会仍进入 `SignalManager.apply()` 的串行 writer，执行 `BEGIN IMMEDIATE`、查询 OPEN signal 后再无操作返回。真实行情绝大多数属于该路径，造成无业务效果的 SQLite 写事务，并与目录持久化和读取竞争。
- 修复目标：保持输入和外部状态校验；当关闭型决策的 `expected_revision is None` 时直接返回，不进入数据库 writer。已有 OPEN signal 的关闭路径仍携带 revision 并完整持久化。
- 测试证据：新增 writer 调用计数回归测试；修复前两个从未打开机会的关闭决策提交 2 次事务，修复后为 0 次。Signal Manager、Catalog Sync 与 Gateway 相关 123 个测试通过。

## ISSUE-040：完整目录变更准备饿死 WebSocket 心跳

- 状态：线程隔离不足，已由 ISSUE-043 继续修复。
- 现象：全量同步在 12:45:31 完成 `settlement_refresh`，直到 12:46:36 才开始 `catalog_persist`，中间约 65 秒没有任何协程日志；持久化开始后立即出现 `WebSocket heartbeat stale`，订阅被迫从 generation 8 恢复到 generation 10。
- 根因：这段无日志区间是同步调用 `_prepare_complete()`，它在 asyncio 事件循环中比较并复制约 15 万市场、30 万 token 并生成变更。长时间纯 CPU 工作阻止 SDK reader 与 heartbeat 获得调度。ISSUE-035 只把数据库 snapshot 的对象转换移出事件循环，没有覆盖同步后的变更准备阶段。
- 修复：把完整代 `_prepare_complete()` 放入 `asyncio.to_thread()`；增加 `catalog_prepare` 的 started/completed INFO 日志，输出 events、markets、tokens、changes 和耗时，从而能直接区分准备与持久化耗时。
- 自动验证：完整同步测试新增线程边界与阶段日志断言；修复前因缺少 `catalog_prepare` 阶段而失败，修复后通过。相关 123 个测试通过。

## ISSUE-041：策略评估汇总无法定位内部耗时阶段

- 状态：诊断增强已在工作区完成，待真实重启采样。
- 现象：新实例每批 16–38 个 token 通常耗时 0.4–1.1 秒，但原日志只有总耗时，不能判定瓶颈来自上下文/SQLite 读取、策略计算还是信号提交。
- 修复：`watch_evaluation_summary` 增加 `context_ms`、`strategy_ms` 和 `signal_apply_ms` 三个累计阶段耗时；保留原有总耗时、决策计数和收益诊断，不改变业务判断。
- 自动验证：新增日志字段回归测试，修复前缺少 `context_ms` 断言失败，修复后与去重、换代中止测试共 3 个通过。

## ISSUE-042：评估吞吐不足导致本地订阅事件丢弃

- 状态：已修复并通过真实重启验证批量上下文耗时。
- 现象：generation 2 在约一分钟内读取至 20,000 条流事件后，以 `detail=subscription_event_dropped` 失效并恢复；该次没有 close frame，因此不是服务端主动断开。
- 直接证据：失效前待评估 token 长期保持 28–38 个，每批耗时约 0.6–1.1 秒；请求数 18,000 时已合并 31,940 个重复 token。SDK handle 容量虽已提升至 65,536，但本地摄取与评估持续竞争事件循环，最终仍发生丢弃。
- 初步代码证据：生产 `_ApplicationContextSource.contexts_for()` 对每个 changed token 都查询一次全部 approved relations，并对每个目标分别查询 OPEN signal；数据库当前为 0 signals、0 revisions、0 snapshots，这些读取没有产生业务状态但会反复创建 SQLite 连接。
- 阶段采样：启用 ISSUE-041 日志后，31/59-token 批次的 `context_ms` 都约 1,600ms，而 `strategy_ms` 约 510–560ms、`signal_apply_ms=0`；后续 24–32-token 批次的 context 通常仍为 180–850ms，证实瓶颈是上下文中的重复 SQLite 读取，不是 writer。
- 安全边界：不调整 `minimum_return_rate=0.0075`，不把低于阈值结果伪装成信号；先用阶段耗时证明主耗时位置，再优化等价读取路径。
- 修复：生产上下文新增批量物化接口，每个评估批次只构建一次 orderbook 索引、读取一次 approved relations，并用单条 `IN (...)` 查询取得所有 OPEN opportunity 的 revision；单 token 接口委托给批量实现。Watcher 检测到批量接口时整批调用，其他测试替身和兼容实现继续逐 token 回退。
- 自动验证：新增 Watch 优先批量物化回归测试，并把应用流水线测试扩展为双 token 批量上下文；修复前分别因仍调用单 token 接口和缺少生产批量接口失败，修复后转绿。相关 Config、Gateway、Catalog Sync、Watch 和应用流水线共 192 个测试通过。
- 真实验证：13:13:39 重启后，首次 100-token baseline 的 `context_ms=3`，后续实时微批通常为 3–13ms；相比修复前 31/59-token 批次约 1,600ms，重复 SQLite 读取瓶颈已消除。进程在 45 秒内消费超过 6,000 条流消息并继续评估。

## ISSUE-043：catalog prepare 存在二次方扫描，线程仍抢占 GIL

- 状态：二次方退化已修复并通过真实全量同步验证；残余 GIL 延迟另见 ISSUE-049。
- 真实证据：12:58:58 开始 `catalog_prepare` 后，流进度约 45 秒几乎停滞；正在执行的评估批次耗时 53,640ms，随后 WebSocket 以 `close_code=1006` 断开。该阶段最终耗时 67,436ms，处理 22,083 events、154,661 markets 和 309,322 tokens。
- 根因：`asyncio.to_thread()` 与事件循环仍共享 GIL；更关键的是 `_prepare_complete()` 对每个未返回的旧 event 都重新扫描全部 `final_markets` 来判断是否完全结算，复杂度为 O(missing events × markets)。本轮有 2,743 个 orphan warning，真实目录规模下产生数亿次 Python 迭代，线程隔离只能移动代码位置，不能消除 CPU/GIL 饥饿。
- 修复：前置构建的 `market_ids_by_event` 已包含完整 market 归属；缺失 event 现在只遍历该 event 对应的 market IDs，不再全表扫描。业务判定、最终 event 状态和变更发布语义不变。
- 测试证据：新增 20,000 个旧 event、10,000 个旧 market 的线性复杂度回归测试；修复前 1.5 秒预算超时，证明存在二次方退化。
- 自动验证：修复后同一复杂度回归测试在 1.5 秒预算内完成并通过；目录同步聚焦测试通过。
- 真实验证：13:38:38 对 22,191 events、156,594 markets 和 313,188 tokens 执行 `catalog_prepare`，耗时 3,762ms；相比修复前同规模约 67,436ms，确认二次方扫描已消除。

## ISSUE-044：11 分钟全量同步仅空闲 30 秒就再次启动

- 状态：默认配置已修复，待真实运行验证调度日志。
- 现象：`sync-af84...` 从 12:50:56 运行到 13:02:05，共 669,511ms；13:02:36 又启动 `sync-c17...`，仅空闲约 30 秒。第二轮在 13:04:57 已扫描到 150 页，监听、定向 fee 刷新和全量扫描继续共享网络与运行资源。
- 根因：默认 `sync_interval_seconds=30` 是小目录阶段的静态值；当前目录已有 154,661 markets、309,322 tokens，单轮受上游每页 100 条限制需约 11 分钟。周期任务按“完成后休眠”执行，因此默认配置使全量扫描近乎连续运行。
- 修复：默认完整目录同步空闲窗口改为 1,800 秒。启动时监听仍立即从 SQLite 恢复，当前订阅的动态 fee 继续每 150 秒定向刷新；本改动不影响盘口监听和 fee freshness，仅降低新市场进入本地完整目录的频率。
- 自动验证：默认配置断言先以 `30 != 1800` 失败，修改后通过。

## ISSUE-045：畸形 `new_market` 广播造成日志洪泛

- 状态：已在工作区修复，待真实广播验证。
- 现象：13:04:03 的一个 SDK 回调逐条输出大型 `new_market` warning；停止进程时 PTY 一次积压约 89,868 tokens 日志。每条样本最多 8,192 字符，同一批广播产生数十至上百条，足以阻塞终端、日志 I/O 和 SDK 回调。
- 根因：ISSUE-030 为每个被安全忽略的畸形公告输出完整 warning，没有按 SDK callback 聚合，也没有按累计数量限频；通用 API 摘要上限对批量流日志过大。
- 修复：同一 callback 聚合为一条；首批和累计每跨 100 条输出一次，保留本批数量、累计数量、校验错误和一个样本；样本单独限制为 1,024 字符。公告仍不进入 SDK drop 计数，真实盘口畸形事件仍逐次告警并 fail-closed。
- 自动验证：新增 100 条、三批 callback 回归测试；修复前输出 100 条且单条过长，修复后仅输出 2 条、每条小于 1,500 字符。相关 Gateway 与默认配置 4 个测试通过。

## ISSUE-046：实时评估逐批 INFO 造成 stdout 背压

- 状态：已修复并通过真实重启验证。
- 现象：13:13:39 的真实实例在约 86 秒内执行 2,159 个评估微批；每批同时输出 `watch_evaluation_summary` 和 `watch_evaluation_batch_completed`，日志达到 4,417 行、约 2MiB。终端输出未被及时消费时，PTY 积压数万 token，程序时间推进明显依赖终端读取。
- 风险：同步日志 handler 在 asyncio 事件循环线程写 stdout；高频微批正常路径的两次写入会与 SDK reader、heartbeat 和策略计算争用事件循环，外部日志消费者变慢时还会形成 stdout 背压。generation 2 同期出现 `close_code=1006`；该 close code 不能单独证明日志是服务端断线根因，但当前日志频率会放大调度延迟和观测干扰。
- 修复：恢复 baseline 继续强制输出完整评估汇总；真实信号、超过 1 秒的慢评估仍即时输出；普通实时评估汇总最多每 10 秒一条。批次完成日志保留首批、每 1,000 批进度和超过 1 秒的慢批次。既保留阶段耗时与收益诊断，又移除每个微批的同步日志写入。
- 自动验证：新增普通实时汇总和批次完成限频测试；修复前分别记录 3 条汇总、2 条完成日志而失败，修复后均仅记录 1 条。恢复 baseline 的阶段耗时日志测试同时通过。
- 真实验证：13:19:09 至 13:22:00 的实例中，普通 `watch_evaluation_summary` 稳定约每 10 秒一条；批次完成仅输出 batch 1、1,000 和 2,000。同期实际完成超过 2,000 个评估批次，没有恢复到逐批双 INFO 的日志洪泛。

## ISSUE-047：恢复 baseline 期间再次断线导致整个 runtime 退出

- 状态：已在工作区修复并通过自动验证，待真实断线场景复验。
- 现象：13:22:07 generation 4 因订阅失效进入 generation 5 recovery；REST orderbook baseline 尚未完成时，13:22:09 WebSocket 以 `close_code=1006 close_reason=''` 异常断开。Gateway 正确拒绝安装失去流完整性屏障的 baseline，但异常一路传播，13:22:10 出现 `runtime_task_exited task=WatchTask error=recovery stream invalidated: connection_lost`，13:22:11 整个 runtime 停止。
- 根因：运行期订阅失效本来允许关闭旧 generation 并 recovery，但 recovery 自身遇到同类瞬时 `connection_lost` 时没有重试边界。Gateway 的 fail-closed 异常被 Watch 当成不可恢复启动错误，导致一个短暂代理/网络断线终止目录同步和后续监听。
- 安全边界：只重试 Gateway 明确标记的 recovery stream invalidation；`sdk_version_changed`、`sdk_lifecycle_shape_changed` 和 `sdk_lifecycle_state_unknown` 仍立即失败，普通 Gateway/数据异常也不吞掉。每次失败的 baseline 和订阅仍由原 owner 清理，不会安装到 cache。
- 修复：新增带 `reason`/`retryable` 的 `MarketRecoveryInvalidatedError`；Watch 对瞬时原因执行 stop-aware 指数退避（1、2、4 秒，最高 60 秒），输出 `watch_recovery_attempt_invalidated` 和 `watch_recovery_retry_scheduled`。关闭会立即唤醒退避等待。
- 自动验证：先新增 connection lost 回归测试，修复前因异常类型不存在而在收集期失败；修复后瞬时失效第二次 recovery 成功。另验证 SDK 版本变化立即抛出、60 秒退避可在 1 秒内被 close 中断、普通 `RuntimeError` 不被吞掉。Watch 与 Gateway 完整单元测试 133 个通过。

## ISSUE-048：订阅事件丢弃日志缺少 SDK 队列现场

- 状态：诊断日志已在工作区完成并通过聚焦测试，待真实重启采样。
- 现象：当前真实实例的 generation 2、5 和 9 均以 `subscription_event_dropped` 失效后成功恢复；该路径没有 close frame，原日志只有失效原因，无法看到发生丢弃时的队列积压和丢弃数量。
- SDK 代码证据：固定版本 SDK 的 `AsyncSubscriptionHandle._push()` 使用有界 `asyncio.Queue`；`put_nowait()` 抛出 `QueueFull` 时采用 drop-oldest 并增加 `_dropped`。生命周期在连接关闭前已看到该计数变化，因此这些实例属于本地 handle 队列曾瞬时填满，不是累计消费条数达到某个阈值。
- 诊断增强：首次检测到 handle drop 时新增 `market_stream_subscription_drop_detected` WARNING，记录当前/初始 `handle_dropped`、增量、`queue_size/maxsize` 以及 manager drop 当前/初始值；保持原有 fail-closed、关闭旧 generation 和 recovery 行为不变。
- 自动验证：新增真实生命周期探针回归测试，修复前能失效但没有诊断日志；修复后日志完整记录 `drop_delta` 和队列字段。该测试与全部生命周期变化参数用例共 6 个通过。

## ISSUE-049：线性 catalog prepare 仍产生秒级 GIL 延迟

- 状态：调查中。
- 现象：ISSUE-043 修复后 `catalog_prepare` 已从约 67 秒降到 3,762ms，但同一时段一个 Watch 上下文批次耗时 3,224ms（其中 `context_ms=3221`），结果因盘口过期变为 `ORDERBOOK_STALE`；prepare 结束前记录到一次 `close_code=1006` 并触发恢复。
- 当前判断：`asyncio.to_thread()` 避免直接在事件循环协程中执行，但准备 22,191 events、156,594 markets、313,188 tokens 和 128,991 changes 的 Python 对象比较/复制仍与主线程争用 GIL。close 1006 与该延迟紧邻，但仅凭时序不能断言客户端延迟就是服务端断线原因。
- 后续：先完成本轮目录持久化和订阅换代，确认整体状态；再用聚焦性能证据决定继续降低 prepare 分配量或采用进程隔离，避免只为一次网络断线扩大架构改动。

## ISSUE-050：完整目录持久化对约 49 万行执行无差别 UPSERT

- 状态：第二阶段优化已在工作区完成并通过自动验证，待真实全量同步复验。
- 真实现象：13:38:42 开始持久化 22,191 events、156,594 markets 和 313,188 tokens；截至 13:46:37 仍未完成，超过 7 分 55 秒。为加载修复而人工中止旧进程，日志中没有 `catalog_persist` 完成记录。
- 根因：`CatalogRepository.save_catalog()` 对每个 market 单独查询旧 event，再逐条 upsert event、market、token；随后对每个受影响 event 分别执行存在性查询、market 列表查询和索引更新。本轮约产生 15.7 万次旧归属查询、49.2 万次逐条 upsert 和最多 6.7 万组索引查询/更新，跨 `aiosqlite` 工作线程的 await 总数约 70 万以上。它不是 SQLite 批量写入本身慢，而是 Python 事件循环与 SQLite 线程之间的逐行往返被放大。
- 运行影响：长事务阻止定向 fee 刷新进入 writer，监听虽继续消费约 15.9 万条消息，但新订阅逐渐出现 `FEE_SCHEDULE_STALE`，目录也无法完成换代；期间偶发 `context_ms` 升至秒级。
- 修复：以两个 writer 连接级 TEMP 表批量取得旧 event 归属和受影响 event 集合；events、markets、tokens 统一改用 `executemany()`；一次读取所有受影响 event 的实际 market 归属，再一次 `executemany()` 更新索引。仍在同一 `BEGIN IMMEDIATE` 事务内，保留缺失 event 拒绝、market 跨 event 移动时同时重建新旧索引、UTF-8 ID 排序及原子提交语义。
- 测试证据：新增数据库往返计数回归测试。32 markets、64 tokens 的旧实现发生 132 次异步往返，断言失败；修复后固定不超过 20 次。另验证 market 跨 event 的批量更新会清理旧 event 并写入新 event。持久化与目录快照相关 25 个测试通过。
- 第一阶段真实复验：批量化后，22,234 events、156,886 markets、313,772 tokens 的完整持久化成功结束，但仍耗时 216,564ms；同期 WAL 增长约 525MiB。这证明逐行异步往返已消除，但把约 49 万个完整对象全部编码并执行冲突 UPSERT 仍是主要写放大。
- 第二阶段根因：完整代中的绝大多数实体只有 `sync_generation`、`sync_generation_complete` 和 `updated_at` 改变，原实现仍更新全部业务列；尤其 31 万余 token 的业务内容通常完全未变。
- 第二阶段修复：完整同步准备阶段额外计算 event/market/token 的语义变化子集；持久化事务先用每表一条集合 UPDATE 推进所有既存实体的完整代次，再只对新增或业务字段变化的实体执行 UPSERT。event 比较包含规范排序后的 `market_ids`，仍保证父子关系与完整代次原子提交。新增 INFO 日志分别记录代次推进、三类 UPSERT、命令执行和事务提交总耗时。
- 第二阶段测试：新增“无语义变化时只推进代次、不要求 UPSERT”及“只选择真实变化实体”的回归测试；Catalog、Persistence、Watch 相关 130 个测试通过。

## ISSUE-051：无关停用通知反复重建相同 WebSocket 订阅

- 状态：已在工作区修复并通过自动验证，待真实运行复验。
- 真实现象：完整同步发布一条目录唤醒及约 440 条 `MARKET_DEACTIVATED` 后，Watch 约每 10–12 秒关闭并重建同一组 100-token 订阅，generation 从 20 连续增长到 31；期间还出现一次 `close_code=1006`。日志中只有一次新增市场刷新请求，排除了每次都是订阅集合变化。
- 根因：每条停用/结算控制通知都会无条件调用 `_rotate_to()`。这些通知中的绝大多数 token 不属于当前 50-market/100-token 订阅，重新加载目录后计算出的 `new_token_ids` 与当前完全相同，但仍关闭连接、重新取 baseline 并切换 generation。
- 修复：控制通知仍先重载最新目录并对其 token 执行信号关闭；若通知 token 与当前订阅无交集且新旧订阅 token 完全相同，则记录限频 INFO 后返回，不再重建 WebSocket。任何影响当前 token 或实际改变订阅集合的通知仍按原流程切代。
- 自动验证：新增“订阅外市场停用不切代”回归测试，同时验证订阅内停用及市场更新仍维持原行为；3 个聚焦测试和上述 130 个相关测试通过。

## ISSUE-052：每条行情重复创建三层 asyncio task，消费速度落后于 SDK 入队

- 状态：两层任务复用已在工作区修复并通过自动验证；真实进程正在持续复验。
- 真实现象：旧实现单个 generation 最多持续约 98 秒，消费约 19,000 条行情后 SDK handle 的 65,536 队列填满，日志记录 `handle_dropped=3 queue_size=65536`，随后以 `subscription_event_dropped` fail-closed 并恢复。该次没有 close frame，不是服务端断线。
- 根因：`WatchTask.run()` 每收到一条行情就创建并销毁一个 `anext(subscription)` task；`MarketSubscription._next_live()` 内部又为每条消息创建 SDK event task 和 lifecycle polling task。真实流量下，大量 task 创建、`asyncio.wait()` 和取消清理把消费速度压到约 200 条/秒，而独立 SDK 消费采样可达到约 1,325 条/秒。
- 第一轮修复及结果：先让 Watch 的目录 change reader 和 stop reader 跨行情复用，单测通过；真实运行仍在 98 秒后填满队列，证明只消除目录 reader 抖动不够。
- 第二轮修复：Watch 改为一个持续 stream reader task，连续消费当前及恢复后的 subscription generation；仅目录变化、停止或异常退出时取消。Gateway 的 lifecycle polling task 也改为每个 subscription 只启动一次，关闭时统一取消清理。每条消息只保留必须与 lifecycle 竞速的 SDK event wait。
- 测试证据：新增 task identity 与 lifecycle 启动次数回归测试，旧实现分别观察到 2 个外层 reader task 和 2 次 lifecycle monitor 启动；修复后均为 1。显式关闭、读取取消、断线与并发异常聚焦测试通过，Watch/Gateway 完整单测共 138 个通过。
- 初步真实复验：系统代理路径已消费超过 6,000 条行情，期间没有 `market_stream_subscription_drop_detected`；但连接先后以无 close frame 的 `1006` 断开，generation 存活窗口不足以单独证明队列在长连接下不再溢出，仍需跨过旧实现约 98 秒的同一 generation 窗口。

## ISSUE-053：WebSocket 经系统代理频繁出现 1006，无服务端 close frame

- 状态：调查中；已证明代理不是唯一根因。
- 环境证据：macOS 系统代理同时配置 HTTP/HTTPS/SOCKS 到 `127.0.0.1:7890`；`websockets` 默认会读取系统代理。显式为 `ws-subscriptions-clob.polymarket.com` 设置 `NO_PROXY` 后可直接建立 TLS/WebSocket，说明直连可用。
- A/B 证据：系统代理路径多个 generation 约 9–59 秒后得到 `close_code=1006 close_reason=''`；直连曾维持约 98 秒，但也在约 50 秒后出现过同样的 1006。1006 表示连接在没有收到标准 close frame 的情况下丢失，因此 SDK close-frame 回调边界已经没有更多服务端 reason 可输出。
- 当前结论：代理路径看起来更不稳定，但直连也复现，不能把代理认定为唯一根因。Watcher 已能在 1006 后 fail-closed、取得新 baseline 并继续运行；下一步需要检查 SDK/WebSocket reader 的底层异常和心跳现场，而不是继续推测服务端主动关闭原因。

## ISSUE-054：恢复失效时取消共享 HTTP/2 请求会污染后续连接

- 状态：已修复并通过聚焦回归测试。
- 现象：WebSocket 在 REST baseline 请求期间失效后，后续恢复可能连续出现 HTTP/2 stream 错误；旧 generation 已不能安装 baseline，但直接取消 SDK 的在途 order-books 请求会影响共享 HTTP client/connection 的后续请求。
- 根因：`_guard_awaitable()` 在检测到流失效后进入通用清理路径，取消了仍在执行的 REST operation。该 operation 属于 SDK 共享 HTTP/2 连接；业务上只需要丢弃它的结果，不需要中断底层传输。
- 修复：流失效或 recovery buffer overflow 时把 operation 从当前 recovery 脱离，由 Gateway 持有并异步 drain 终态；日志记录 generation、失效原因、pending 数、最终 outcome 和耗时。调用方立即按原 fail-closed 语义重试，不会安装旧 baseline。显式任务取消仍执行原取消清理。
- 验证：新增在途 recovery 请求失效后继续运行并被 drain 的回归覆盖；相关恢复聚焦测试通过。

## ISSUE-055：缺少分层吞吐日志导致误判行情映射为瓶颈

- 状态：诊断增强完成，真实采样已定位耗时层级。
- 现象：SDK handle 队列持续增长，但旧日志无法区分 SDK 入队、Gateway 映射/交接、Watch handler 和策略评估各阶段，曾怀疑大型 `price_change` 映射或 orderbook cache 更新过慢。
- 增强：Gateway 每 10 秒输出 SDK 消费率、映射耗时、交接等待和交接队列；Watch 输出事件类型吞吐、`price_change` 的 entries/book levels 以及 parse/cache/queue 分段耗时；策略汇总保留 context/strategy/apply 分段。
- 真实证据：映射约 0.03–0.05ms/事件，Watch `price_change` handler 约 0.17–0.25ms/消息，均不足以解释数毫秒级交接等待；旧单槽交接下 `handoff_wait` 达 8–12ms，SDK 队列最终填满。由此排除 mapper/cache 主因，并把问题收敛到事件循环调度和逐事件交接。

## ISSUE-056：同步策略函数在 asyncio task 内直接执行，阻塞事件循环

- 状态：已修复并通过自动与真实验证。
- 根因：策略 `evaluate()` 是同步 CPU 函数；即使外层评估 worker 是 asyncio task，直接调用仍在事件循环线程执行。批量行情到来时，策略计算阻止 SDK reader、Gateway pump 和 Watch handler 获得调度。
- 修复：原生 async strategy 保持直接 await；同步 strategy 通过 `asyncio.to_thread()` 执行，并兼容同步函数返回 awaitable 的实现，不改变决策与阈值。
- 测试证据：新增阻塞型同步策略回归测试；修复前事件循环无法在策略返回前继续而失败，修复后与相关 Watch 测试共 5 个通过。
- 真实结果：消费能力由约 85–120 条/秒提升到约 189–294 条/秒，但 SDK 队列仍可增长到 85%，说明该修复必要但不足，剩余根因见 ISSUE-057。

## ISSUE-057：单槽 Gateway 交接队列强制每条行情进行任务握手

- 状态：已修复并通过真实持续运行验证。
- 根因：`MarketSubscription._live_items` 的 `maxsize=1` 使 event pump 每放入一条消息就等待 Watch 取走，形成逐事件 task switch/握手；真实流量约 1,000 条/秒时，调度成本远高于映射和 handler 本身，SDK handle 队列在上游持续堆积。
- 修复：交接队列改为有界预取，容量取 recovery buffer 与 4,096 的较小值；仍有明确上限且 lifecycle invalidation 保持优先。进度日志新增当前交接队列大小和容量。
- 测试证据：新增四条 ready event 的行为测试，要求消费第一条后 pump 已预取至少两条；旧单槽实现失败，修改后与既有 Gateway 流测试共 3 个通过。
- 真实验证：SDK 消费与 Watch 消费稳定约 694–1,325 条/秒，SDK handle 队列持续为 0%，`handoff_wait_ms_per_event≈0.001`，交接队列通常仅 1/4,096，且 `handle_dropped=0 manager_dropped=0`。期间仍发生 1006，证明异常断线与本地队列溢出是两个独立问题。

## ISSUE-058：SDK 吞掉 `ConnectionClosed`，1006 日志缺少底层 TCP/解析异常

- 状态：诊断修复已通过聚焦测试和真实 1006 验证。
- 代码证据：固定 SDK 的 `_read_loop()` 对 `ConnectionClosed` 直接 `pass`，因此该异常不会进入 `_on_socket_error`；最终 callback 只得到 code/reason。1006 本身仅表示未收到 close frame，不能区分 TCP reset、EOF、代理中断、解析器异常或心跳主动关闭。
- 修复：在 Gateway 安装 SDK connection 实例级 reader 包装，仅保存当前 reader socket 引用；仍在 SDK 原始 connection-lost callback 边界输出日志，并从该 socket 读取 `recv_exc`、protocol `parser_exc`、socket state、应用心跳距最后 PONG 的秒数、WebSocket latency 和 transport closing 状态。不修改 `.venv`，不改变 SDK 读写或重连行为。
- 测试证据：新增模拟 `ConnectionResetError`、parser EOF 和 10 秒心跳年龄的回归测试；修复前日志只有 1006 而失败，修复后包含全部诊断字段；与既有 close code/reason、回调竞态测试共 3 个通过。
- 真实证据：15:24:00、15:24:21、15:25:03 三次 1006 均记录 `reader_exception_type=none`、`parser_exception_type=EOFError`、`transport_closing=True`；parser 分别报告 `unexpected end of stream`、只收到 191/620 bytes、只收到 477/614 bytes。最后一次断开前 PONG 年龄约 20.3 秒、WebSocket latency 约 1.245 秒，不支持“应用先返回”或“长时间收不到 PONG 主动关闭”的判断，直接证据是底层字节流被截断。
- 网络路径：Python 从 macOS 系统代理发现 HTTP/HTTPS/SOCKS 均指向 `127.0.0.1:7890`。结合 ISSUE-053 的直连 A/B，代理可能放大不稳定性，但直连也复现过 1006，因此仍不能把它认定为唯一根因。当前 Watch 会 fail-closed 并自动恢复，真实三次断线后 runtime 均继续运行。

## ISSUE-059：交叉盘口触发两个短暂的伪套利信号

- 状态：已修复并通过缓存层、Watch 及全量回归。
- 真实现象：15:24:49 在没有调整 `required_return_rate=0.0075` 的情况下，市场 `3301319` 先后持久化 `BINARY_UNDERPRICED`（收益约 0.291392）和 `BINARY_OVERPRICED`（收益约 0.223638），约 1.2–1.4 秒后均以 `PROFIT_BELOW_THRESHOLD` 关闭。数据库共有 2 个 signal、4 个 revision、12 个 leg，证明行情监听、策略评估和持久化链路已经贯通。
- 数据证据：两个 OPENED revision 使用的是同一时段互相矛盾的盘口。token A 的 ask=0.34、bid=0.60，token B 的 ask=0.40、bid=0.66；两个 token 都满足 `best_bid > best_ask`。因此它们不是可执行套利机会，而是截断/乱序流状态形成的交叉盘口，不能作为真实收益信号。
- 根因：可信订单簿缓存只校验 generation、token 集合、时间戳、到达序列、档位格式和可选 hash，没有校验 top-of-book 不交叉；SDK 又没有可用于排序的上游 sequence，缓存只能按本地到达顺序应用同毫秒更新，交叉状态会直接进入策略。
- 修复：在策略可见的缓存入口建立 `best_bid < best_ask` 不变量；REST snapshot、完整 stream book 或 delta 形成 locked/crossed book 时均 fail-closed。Watch 现有的 `watch_subscription_invalidated detail=price_change_invalid:best bid must be below best ask` 日志会记录现场，随后关闭信号、丢弃当前 generation、重新取得 REST baseline，策略不会看到该盘口。校验没有放到通用 `OrderBook` 构造器，避免离线策略计算或 REST 映射在缓存恢复边界之外提前失败。
- 自动验证：先增加 locked/crossed book 测试并确认旧实现失败；最终分别覆盖 snapshot、完整 book 和 delta 三条缓存入口，均验证失效并清空 view。全量测试 583 个通过、1 个跳过。

## ISSUE-060：线程化策略使固定次数事件循环让步测试过早断言

- 状态：测试已修复。
- 现象：联合回归中启动恢复测试只执行 10 次 `await asyncio.sleep(0)` 就断言两个 token 均完成评估；同步策略移入 worker thread 后，第二个 token 尚未回到事件循环，测试稳定失败并在退出时遗留异步任务。
- 根因：测试依赖实现调度次数而非可观察完成条件，不是生产运行故障。
- 修复：改为在 1 秒边界内等待两个 token 的策略调用集合达到预期，并用 `finally` 确保 Watch task 一定取消清理。聚焦测试及全量测试均通过。

## ISSUE-061：连接诊断探针抢先绕过 lifecycle fail-closed 原因

- 状态：已修复并通过全量回归。
- 现象：全量测试中，模拟 SDK 私有 `_connection` 字段消失时，连接诊断安装逻辑直接抛出 `GatewayLifecycleError`；原设计应由 lifecycle monitor 产出 `sdk_lifecycle_shape_changed` invalidation 并关闭订阅。
- 根因：ISSUE-058 的观测增强被错误地当成订阅成功的强制前置条件，诊断能力缺失反而改变了原故障语义。
- 修复：诊断边界不可用时输出 `market_stream_connection_diagnostics_unavailable manager_type=...` WARNING 并停止安装包装；随后仍由既有 lifecycle 屏障 fail-closed，不吞错、不把 generation 标记健康。测试同时断言 warning 和结构化失效原因。
- 验证：相关 Gateway、策略边界及缓存聚焦测试 26 个通过；全量测试 583 个通过、1 个跳过。

## 本轮运行终态（15:25）

- 真实实例成功初始化全部组件、载入目录、订阅 50 个市场/100 个 token，并输出 `runtime_started`；持续行情的 SDK 队列保持 0%，没有本地事件丢弃。
- 三次 1006 均自动完成 fail-closed 恢复，runtime 没有退出；底层原因已收敛到 WebSocket 字节流 EOF/截断，不是程序回到 `app.py` 后才断线。
- 未降低收益阈值时，监听、评估和持久化链路确实产生 2 个 signal；进一步核验后确认二者来自交叉盘口，现已增加 fail-closed 防护，不能将其宣称为真实可交易机会。
- 达到“监听到信号并定位问题”的终止条件后于 15:25 人工停止实例；数据库保留完整 signal/revision/leg 证据。真实市场何时再次出现合法套利不可控，本轮未等待或制造新的合法信号。
