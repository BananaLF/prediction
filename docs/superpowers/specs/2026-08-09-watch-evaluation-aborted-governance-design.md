# Issue #19：Watch 评估大量 aborted 的根因与治理设计

- Issue：<https://github.com/BananaLF/prediction/issues/19>
- 阶段：DESIGN_REVIEW
- 日期：2026-08-09
- 范围：只定义根因诊断、可观测性和最小治理方案；本阶段不修改业务代码。

## 结论摘要

当前 `watch_evaluation_aborted` 的主要直接原因是：Watch 在一次评估开始时捕获了依赖 token 的 revision 快照，随后异步构建 context 或执行 strategy；在这段时间内，WebSocket 行情继续更新同一 generation 下的有效订单簿，相关 token revision 因而前进。评估到达 `before_strategy`、`after_strategy` 或 signal apply fence 时，旧快照已不再匹配，于是被 fail-closed 丢弃。

因此，日志中的：

```text
expected_generation=19 actual_generation=19 cache_state=VALID
```

并不矛盾。这里失效的维度是依赖 token revision，不是 subscription generation 或 cache validity。

当前实现还有两个放大因素：

1. 任一 target 过期后，`_evaluate_tokens` 直接 `return`，整个 batch 的后续 target 都被放弃；行情继续到来后又会触发下一批评估。
2. 每次中止都以 INFO 输出，但只输出 generation/cache state，不输出实际失效维度，造成高频 I/O 和错误诊断方向。

已有真实运行记录中，7,179 次 aborted 对应 172 条完整 summary，另一个 30 分钟窗口中 12,957 次 aborted 对应 176 条完整 summary；这说明目前有“正常过期重试”的证据，但不足以排除高峰期间的额外计算浪费，也没有证据表明 generation fence 本身失效。

推荐先实施“原因分类 + revision 诊断 + 窗口聚合日志”，保持 revision fence 和 signal apply 原子性不变。只有当新数据证明存在评估饥饿或明显的 batch 级浪费时，才继续引入过期 target 局部跳过或调度策略调整。

## 现有执行链路

1. stream reader 将 `apply_book`/`apply_delta` 应用到 cache，并把 changed token 放入 pending set。
2. deferred evaluator 从 pending set 取出 token，捕获 generation、全局 revision 和 token revision snapshot。
3. batch context source 可能执行关系查询、open revision 查询和 context 构建；strategy 随后可能异步或在线程中执行。
4. 行情流可在上述 await/线程执行期间继续更新 cache。相同 generation 下，变化 token 的 revision 会增加。
5. `_evaluation_is_current` 发现依赖 revision 不匹配，当前实现记录 `watch_evaluation_aborted` 并终止整个 batch。
6. pending set 中的新行情继续触发后续评估，因此大量 aborted 本身不等于没有后续评估。

应用层的 binary context 通常包含同一 market 的多个 orderbook；这使一个 token 的变化可能使同一 market 的完整策略输入过期，但 `_target_dependency_token_ids` 已按 context 中实际 orderbook 缩小依赖范围，不能简单通过放宽 fence 修复。

## 证据与已验证边界

### 已确认

- `cache.token_revisions_match` 同时检查 cache state、generation 和依赖 token revision。
- `apply_book`/`apply_delta` 在 generation 不变时也会推进相关 token revision。
- `before_strategy` 的 revision 失败会直接退出 `_evaluate_tokens`。
- 现有确定性测试覆盖了：同 generation revision 变化会丢弃陈旧评估、无关 token revision 不会误伤、signal apply 前后原子复核，以及 generation 变化时安全中止；本次 5 个目标测试全部通过。
- 基于 `origin/main` 的完整基线为 `743 passed, 2 skipped`。环境中存在代理变量时会使 httpx SOCKS 依赖导致无关失败；清除代理变量后完整验证通过。

### 尚未证明

- “整批 return”是否是实际吞吐下降的主因，还是行情 revision 更新频率远高于评估窗口导致的正常过期。
- context、strategy、signal apply 各阶段在真实高行情速率下的具体占比。
- aborted 是否在某些窗口造成 summary 间隔超过可接受阈值。

## 候选方案

### 方案 A：诊断字段、原因分类和窗口聚合（推荐的第一步）

在 revision fence 失败时区分以下原因：

- `generation_changed`：实际 generation 与快照不同；
- `cache_invalid_or_closed`：cache 不再有效或 task 已关闭；
- `dependency_revision_changed`：generation 仍相同，但一个或多个依赖 token revision 变化；
- `dependency_missing`：快照中的依赖在当前 cache 中不存在。

对于 `dependency_revision_changed`，在单次代表性日志中记录 bounded 的 changed token 样本，以及对应的 expected/actual revision；聚合计数记录完整数量。单条 aborted 不再逐次输出 INFO，而是在现有评估摘要窗口内或独立的固定窗口内最多输出一次 `watch_evaluation_abort_summary`，至少包含：

- window 内 aborted 总数、按 reason/stage 的分布；
- evaluation batch、request、pending/coalesced 计数；
- 最近一次代表性 expected/actual generation/revision；
- 最近一次 completed summary 的年龄、窗口内最大评估耗时；
- 必要时输出 bounded token 样本，避免 token 数量导致日志膨胀。

若持续 aborted 导致没有 completed summary，abort summary 仍必须能按窗口刷新，以免“没有 summary”时反而失去诊断能力。关闭 Watch 时可补刷最后一个未完成窗口。

优点：改动最小、先获得可验证证据、保留现有安全语义；缺点：不能直接减少 revision 竞争，只能减少日志噪声并定位是否需要第二阶段治理。

### 方案 B：过期 target 局部跳过，继续处理同批其他 target

将当前遇到 revision mismatch 的 target 标记为 stale，跳过该 target，继续检查后续 target；每个 target 仍须在 strategy 前、signal apply 前、signal apply 后执行相同的 revision fence。

优点：一个 target 过期时不会浪费同批中仍然有效的独立 target；缺点：需要证明 batch context 中 target 之间没有共享陈旧状态，且当依赖范围重叠时可能继续产生大量跳过；还会改变当前“整批快照一致”的批处理语义。应在方案 A 的数据证明存在明显 batch 级浪费后再采用，并补充同 market、多 target、重叠依赖和 signal apply 原子性的测试。

### 方案 C：调度/coalescing/debounce，优先评估最新快照

在行情突发期间合并 pending token，并在短暂静默或固定调度点重新捕获最新 cache 快照，减少“刚捕获就过期”的评估。

优点：可能降低 context/strategy 重复计算和 aborted 率；缺点：增加机会判断延迟，难以选择 debounce 窗口，在持续高流量下仍可能饥饿；若调度实现不严谨，可能遗漏最后一次必要重评估。只有在 metrics 证明 evaluator queue age 或 completed summary 间隔恶化时才考虑。

## 推荐设计

第一阶段采用方案 A，不改变以下语义：

- 不删除或弱化 token revision fence；
- 不使用过期 orderbook 持久化 signal；
- 保持 generation invalidation 和 fail-closed recovery；
- 保持 signal apply 前的锁内检查、异常处理和 apply 后最终复核；
- 不新增数据库表或改变 strategy 结果。

建议以 WatchTask 内部状态保存一个有限窗口的 abort counters，复用现有 summary 的 monotonic 时间基准。诊断逻辑应返回结构化的 abort classification，而不是由 `_log_evaluation_aborted` 猜测；这样 generation mismatch 和 revision mismatch 不会继续共用同一条模糊日志。revision 对比只读取当前 cache 快照，不改变 cache。

推荐的日志策略是：

- 每次 abort 只计数，不输出 INFO；必要的逐条细节降到 DEBUG；
- 每个固定窗口最多输出一次 INFO 聚合；
- 聚合日志中明确 `reason=dependency_revision_changed` 等实际失效维度；
- expected/actual revision 只记录有限样本，完整数量由 counters 表示；
- 窗口中没有 completed summary 时仍能独立输出 abort summary。

## 可观测性判据

初始诊断窗口建议使用 10 秒，与现有 `watch_evaluation_summary` 周期保持一致。评估健康度不能只看 aborted 数量，而应同时看：

- 正常过期：aborted 与 completed summary 并存，summary 间隔稳定，pending set 可持续被消费；
- 需要关注：pending/request 继续增长，且连续 3 个窗口（约 30 秒）没有 completed summary；
- 评估饥饿：在 pending 持续非空的情况下，summary 间隔超过 30 秒；超过 60 秒作为严重告警候选；
- 计算浪费：同一窗口 aborted/batch 比例、context/strategy 耗时和 coalesced 数持续升高，并伴随 completed summary 变慢。

这些阈值用于第一轮真实复验和告警，不代表业务机会本身的 SLA；若真实基线显示不适合，再依据数据调整。

## 验证计划

实现阶段必须先补测试再改代码：

1. 保留并运行现有 revision fence、无关 token 和 signal apply 原子性测试。
2. 增加同 generation、cache VALID、依赖 revision 变化的确定性测试，断言原因是 `dependency_revision_changed`，且包含 expected/actual revision。
3. 增加多次 abort 的聚合测试，证明窗口内不产生逐条 INFO 洪泛，并证明没有 completed summary 时仍能刷新 abort summary。
4. 增加 generation change、cache invalid/closed、missing dependency 的分类测试，避免把不同失效维度混在一起。
5. 运行 `./scripts/build_env.sh` 的完整测试与 CLI 检查。
6. 在真实高行情速率下复验至少一个完整观察窗口，记录 aborted 速率/原因/stage、summary 间隔、pending/coalesced、context/strategy/apply 耗时和日志速率；至少覆盖一次高峰，而不是只观察启动低流量阶段。

如果真实复验显示 summary 持续正常，仅保留方案 A；如果发现 batch 级浪费，再单独评审方案 B；如果发现 queue age 或 summary 间隔恶化，再单独评审方案 C。任何 B/C 变更都必须证明不会持久化陈旧盘口、不会遗漏必要重评估、不会破坏 signal apply 的原子性。

## 设计评审结论

本阶段建议批准方案 A 进入实施计划。方案 B、C 暂不作为默认实现，等待新增诊断数据证明其必要性。当前没有发现需要修改 cache revision fence 的 correctness bug；问题集中在过期评估的可观测性和可能的重复计算治理。
