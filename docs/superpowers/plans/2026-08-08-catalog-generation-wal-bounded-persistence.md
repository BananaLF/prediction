# Catalog Generation WAL-Bounded Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 SQLite schema 升级到 v4，以 copy-on-write catalog generation、短事务分批写入和 WAL 背压替代单个超大事务；在生产数据副本上把完整同步 WAL 峰值限制在 128 MiB 内，同时保持原子可见性和 Watch 更新新鲜度。

**Architecture:** `events`、`markets`、`tokens` 变为只读兼容视图；稳定身份表保存外键目标，版本表保存 payload，`catalog_state.active_generation_id` 是唯一发布指针。完整同步由 generation coordinator 分批暂存、校验、rebase 并通过短 CAS 事务激活；`DatabaseWriter` 串行处理事务命令和事务外 checkpoint，`WalController` 根据 WAL 字节水位暂停或终止同步。v3→v4 使用同文件系统旁路构建、全量校验、持久化 marker 和原子重命名，不支持自动降级。

**Tech Stack:** Python 3.11、SQLite WAL、`sqlite3`、`aiosqlite`、`asyncio`、pytest、Hypothesis。

## Global Constraints

- Schema 目标版本固定为 4；v4 不支持旧二进制打开，也不提供自动降级或双写。
- 普通读取始终只观察一个已提交活动代；`STAGING`、`ABORTED` 和未来版本不可见。
- 完整同步的初始批次为 8,000 行或估算 payload 16 MiB，任一先到即提交；按实际 WAL 增量自适应。
- WAL 水位固定为：预检 `<16 MiB`、PASSIVE `48 MiB`、暂停并 RESTART/TRUNCATE `64 MiB`、终止 `96 MiB`、至少保留 `32 MiB`；128 MiB 是不可放宽的验收上限。
- Watch 胜出规则保持 `excluded.updated_at >= existing.updated_at`；每次有效 Watch 写入同时递增 `runtime_revision` 并记 journal。
- 激活事务只做 revision CAS、generation COMMIT、活动指针切换和单条 `CATALOG_RECONCILIATION_READY` outbox。
- 日常清理使用短事务和可恢复游标，不运行大型 `VACUUM`。
- 每项任务只修改列出的文件；发现需要扩大范围时先更新本计划。

## File Structure

- Modify: `predmarket/persistence/schema.py` — 定义 v4 表、索引、兼容视图与专用建库函数；在 repository 完整接入前暂不切换运行时版本。
- Create: `predmarket/persistence/catalog_generations.py` — generation 生命周期、批次游标、候选校验、rebase、CAS 激活和回收协议。
- Create: `predmarket/persistence/wal.py` — WAL 采样、checkpoint 结果、批次预测和水位状态机；保持纯逻辑可单测。
- Modify: `predmarket/persistence/writer.py` — 在同一有界 actor 中支持事务命令与事务外 checkpoint。
- Modify: `predmarket/persistence/repositories.py` — catalog 读写切换到稳定身份/版本模型，保留公开 repository API。
- Create: `predmarket/persistence/catalog_migration.py` — v3→v4 旁路复制、校验、marker 和原子切换恢复。
- Modify: `predmarket/persistence/migration.py` — 按目标版本路由旧 v1→v2 与新 v3→v4 迁移。
- Modify: `predmarket/persistence/integrity.py` — v4 generation、视图、journal、清理和 marker 诊断。
- Modify: `predmarket/cli.py` — v4 迁移输出、备份路径和失败信息。
- Modify: `predmarket/app.py` — 启动前恢复迁移切换状态并拒绝不安全 schema。
- Modify: `predmarket/catalog/sync.py` — 接收分批同步结果与安全终止原因，不改变语义 diff 和单聚合消息语义。
- Create: `scripts/validate_catalog_v4.py` — 生产副本 WAL、原子性、新鲜度、查询延迟和清理验收探针。
- Modify: `docs/runtime-investigation-2026-08-04.md` — 记录真实数据 v4 验收结果及 v3 基线对比。
- Tests: `tests/unit/persistence/test_schema.py`, `test_writer.py`, `test_integrity.py`, `test_migration.py` — 对应底层协议。
- Create tests: `tests/unit/persistence/test_wal.py`, `test_catalog_generations.py`, `tests/integration/test_catalog_generation_v4.py`, `test_catalog_migration_v4.py`, `test_catalog_wal_v4.py` — 状态机、并发、崩溃、迁移和容量验收。
- Modify tests: `tests/unit/catalog/test_repository_snapshot.py`, `test_sync.py`, `tests/integration/test_app_pipeline.py`, `test_signal_concurrency.py`, `test_relation_activation.py`, `test_watch_recovery.py`, `test_cli.py` — 兼容现有调用与查询。

---

### Task 1: 定义 schema v4、稳定身份和原子读取视图

**Files:**
- Modify: `predmarket/persistence/schema.py:13-730`
- Modify: `tests/unit/persistence/test_schema.py:1-260`
- Create: `tests/unit/persistence/test_catalog_generations.py`

**Interfaces:**

```python
SCHEMA_VERSION = 3  # Task 4 完整接入时原子切换为 4
TARGET_SCHEMA_VERSION = 4
SCHEMA_V4: str

def create_v4_database(path: Path) -> None:
    """Create a new, empty schema-v4 database for tests and migration."""

class GenerationStatus(StrEnum):
    STAGING = "STAGING"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
```

- [ ] 写失败测试：初始化空库后断言 `catalog_generations`、`catalog_state`、三类 identity/version 表和 `catalog_runtime_changes` 存在，`events`、`markets`、`tokens` 的 `sqlite_schema.type == "view"`，`PRAGMA user_version == 4`。

```python
def test_create_v4_database_creates_generation_schema(tmp_path: Path) -> None:
    path = tmp_path / "market.db"
    create_v4_database(path)
    with sqlite3.connect(path) as connection:
        objects = dict(connection.execute(
            "SELECT name, type FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
        ))
        assert objects["events"] == "view"
        assert objects["catalog_event_ids"] == "table"
        assert objects["event_versions"] == "table"
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
```

- [ ] 写失败测试：插入 `COMMITTED` 基准代、未来 `STAGING` 代和 `ABORTED` 代的版本；断言视图只选择 `generation_id <= active_generation_id` 的最新 committed 版本，并投影活动代的 `sync_generation` 与 `sync_generation_complete=1`。
- [ ] 写失败测试：未变化实体在新活动代没有新版本时仍可见；新实体只有 STAGING 版本时不可见；直接 `INSERT INTO markets` 因视图只读失败。
- [ ] 运行 `pytest tests/unit/persistence/test_schema.py tests/unit/persistence/test_catalog_generations.py -q`，确认失败原因是 v4 schema 尚不存在。
- [ ] 在 `SCHEMA_V4` 中创建单行 `catalog_state(id=1, active_generation_id, runtime_revision, cleanup_generation_id, cleanup_entity_type, cleanup_entity_id, last_checkpoint_at)`，并用约束固定 `id=1`。
- [ ] 创建 `catalog_generations`，包含输入摘要、base/rebased revision、三实体计划/写入计数和摘要、keyset 游标、状态及时间字段；用 partial unique index 保证最多一个 `STAGING`。
- [ ] 创建 `catalog_event_ids`、`catalog_market_ids(event_id FK)`、`catalog_token_ids(market_id FK)`；将所有现有下游外键改为指向 identity 表。
- [ ] 创建三张 version 表，唯一键为 `(entity_id, generation_id)`；复制当前 payload 列，建立 `(entity_id, generation_id DESC)`、`(generation_id, entity_id)` 及 slug、condition、market/position 查询所需索引。
- [ ] 创建三个只读视图，使用 `catalog_state`、committed generations 和每实体 `MAX(generation_id)` 解析有效版本；不要创建 INSTEAD OF 写触发器。
- [ ] 新增 v4 table/view 清单和空库 bootstrap：创建合成 `bootstrap-v4` committed generation 并将活动指针指向它；本任务保持 `SCHEMA_VERSION=3` 和 `initialize_database()` 原行为，避免在 repository 尚未接入时破坏主线。
- [ ] 运行上述测试，预期全部通过；再运行 `pytest tests/unit/persistence/test_schema.py -q`，确认现有 v3 运行时测试未回归。
- [ ] Commit: `git add predmarket/persistence/schema.py tests/unit/persistence/test_schema.py tests/unit/persistence/test_catalog_generations.py && git commit -m "feat: add catalog generation schema v4"`

### Task 2: 为 writer 增加事务外 checkpoint 与 WAL 水位状态机

**Files:**
- Modify: `predmarket/persistence/writer.py:16-170`
- Create: `predmarket/persistence/wal.py`
- Modify: `tests/unit/persistence/test_writer.py:1-450`
- Create: `tests/unit/persistence/test_wal.py`

**Interfaces:**

```python
class CheckpointMode(StrEnum):
    PASSIVE = "PASSIVE"
    RESTART = "RESTART"
    TRUNCATE = "TRUNCATE"

@dataclass(frozen=True, slots=True)
class CheckpointResult:
    mode: CheckpointMode
    busy: int
    log_pages: int
    checkpointed_pages: int
    wal_bytes: int

async def DatabaseWriter.execute_non_transactional(
    self, command: DatabaseCommand[T]
) -> T:
    """Serialize a command that must run outside a transaction."""

async def DatabaseWriter.checkpoint(
    self, mode: CheckpointMode = CheckpointMode.PASSIVE
) -> CheckpointResult:
    """Run one validated checkpoint request through the writer actor."""
```

```python
@dataclass(frozen=True, slots=True)
class WalPolicy:
    preflight_bytes: int = 16 * 1024 * 1024
    passive_bytes: int = 48 * 1024 * 1024
    pause_bytes: int = 64 * 1024 * 1024
    abort_bytes: int = 96 * 1024 * 1024
    reserve_bytes: int = 32 * 1024 * 1024
    hard_limit_bytes: int = 128 * 1024 * 1024

class WalAction(StrEnum):
    CONTINUE = "CONTINUE"
    PASSIVE_CHECKPOINT = "PASSIVE_CHECKPOINT"
    PAUSE_AND_CHECKPOINT = "PAUSE_AND_CHECKPOINT"
    SHRINK_BATCH = "SHRINK_BATCH"
    ABORT = "ABORT"

def decide_wal_action(*, wal_bytes: int, predicted_delta: int, policy: WalPolicy) -> WalAction:
    """Return the next action without mutating writer or filesystem state."""
```

- [ ] 写失败测试：在 `execute_non_transactional` 的命令中断言 `connection.in_transaction is False`；普通 `execute` 内断言为 True；异常不会让后续队列请求失效。
- [ ] 写失败测试：持有长读事务后调用 PASSIVE checkpoint，断言 `busy/log_pages/checkpointed_pages` 被完整返回且调用没有嵌套 `BEGIN IMMEDIATE`。
- [ ] 用表驱动测试覆盖 16/48/64/96/128 MiB 边界、32 MiB reserve 和预测增量导致的提前缩批。
- [ ] 运行 `pytest tests/unit/persistence/test_writer.py tests/unit/persistence/test_wal.py -q`，确认新增用例失败。
- [ ] 给 `_Request` 增加 `transactional: bool`；actor 只对事务请求包裹 `BEGIN IMMEDIATE/commit/rollback`，非事务请求执行前后均断言无活动事务。
- [ ] 实现 `checkpoint()`，只允许 enum 模式，执行 `PRAGMA wal_checkpoint(<mode>)` 后读取 `<database>-wal` 文件大小；不允许调用方拼接任意 PRAGMA。
- [ ] 实现纯函数水位决策和保守放大系数更新；验证 `preflight < passive < pause < abort < hard_limit` 且 `abort + reserve <= hard_limit`。
- [ ] 运行新增测试，预期通过；再运行 `pytest tests/unit/persistence/test_writer.py -q`，确认 writer 生命周期、队列满和关闭语义未回归。
- [ ] Commit: `git add predmarket/persistence/writer.py predmarket/persistence/wal.py tests/unit/persistence/test_writer.py tests/unit/persistence/test_wal.py && git commit -m "feat: add WAL-aware writer checkpoints"`

### Task 3: 实现可恢复的 generation 分批暂存

**Files:**
- Create: `predmarket/persistence/catalog_generations.py`
- Modify: `tests/unit/persistence/test_catalog_generations.py`
- Modify: `tests/unit/persistence/test_writer.py:450-1100`

**Interfaces:**

```python
@dataclass(frozen=True, slots=True)
class CatalogBatchLimits:
    max_rows: int = 8_000
    max_payload_bytes: int = 16 * 1024 * 1024

@dataclass(frozen=True, slots=True)
class GenerationInput:
    sync_generation: str
    updated_at: int
    events: tuple[Event, ...]
    markets: tuple[Market, ...]
    tokens: tuple[Token, ...]
    input_digest: str

@dataclass(frozen=True, slots=True)
class StagedGeneration:
    id: int
    sync_generation: str
    base_generation_id: int
    base_runtime_revision: int

async def CatalogGenerationCoordinator.stage(
    self, value: GenerationInput
) -> StagedGeneration:
    """Create or resume one bounded, invisible staging generation."""
```

- [ ] 写失败测试：用 8,001 个小实体断言至少产生两个 writer 事务；每批写 identity、version、计数、滚动摘要和 keyset cursor 后一同提交。
- [ ] 写失败测试：在首批后抛出注入异常，再次传入相同 `input_digest` 时从持久化 cursor 续写且无重复版本；不同 digest 时旧代标为 ABORTED 并创建新代。
- [ ] 写失败测试：单个 payload 超过 16 MiB 时单行独立提交；预测增量触发 `SHRINK_BATCH` 时下一批缩小，触发 `ABORT` 时活动指针不变且 generation 进入 ABORTED。
- [ ] 运行 `pytest tests/unit/persistence/test_catalog_generations.py -q`，确认失败。
- [ ] 实现规范化 SHA-256：按 UTF-8 字节序排序实体 ID，序列化使用稳定 JSON separators，分别保存 planned/written count 和 digest。
- [ ] 实现 `create_or_resume_staging()`；在创建前把 WAL 回收到 `<16 MiB`，失败时不创建 generation。
- [ ] 按 event→market→token 依赖顺序形成批次；每批只通过一次 `writer.execute()`，使用 `ON CONFLICT(entity_id, generation_id)` 保证恢复幂等。
- [ ] 每批后通过 `writer.checkpoint()` 采样并应用 `WalAction`；暂停期间用 `await asyncio.sleep(0)` 让 Watch、signal 和 outbox 请求继续进入同一队列。
- [ ] 本任务只交付可独立测试的 coordinator staging API；公开 `save_complete_catalog()` 仍走现有原子路径，直到 Task 4 可一次性接入完整 stage→validate→activate 流程。
- [ ] 运行新增测试和 `pytest tests/unit/persistence/test_writer.py -q`，预期通过。
- [ ] Commit: `git add predmarket/persistence/catalog_generations.py tests/unit/persistence/test_catalog_generations.py tests/unit/persistence/test_writer.py && git commit -m "feat: stage catalog generations in bounded batches"`

### Task 4: 候选快照校验与短事务原子激活

**Files:**
- Modify: `predmarket/persistence/catalog_generations.py`
- Modify: `predmarket/persistence/repositories.py:173-333`
- Modify: `tests/unit/persistence/test_catalog_generations.py`
- Create: `tests/integration/test_catalog_generation_v4.py`

**Interfaces:**

```python
class CatalogConstraintError(ValueError):
    """Candidate snapshot violates a catalog invariant."""

class CatalogActivationConflict(RuntimeError):
    """Runtime revision changed after candidate validation."""

@dataclass(frozen=True, slots=True)
class CandidateValidation:
    generation_id: int
    event_count: int
    market_count: int
    token_count: int
    snapshot_digest: str
    validated_runtime_revision: int

async def CatalogGenerationCoordinator.validate_candidate(
    self, generation: StagedGeneration
) -> CandidateValidation:
    """Validate and freeze the effective candidate snapshot."""

async def CatalogGenerationCoordinator.activate(
    self,
    validation: CandidateValidation,
    reconciliation: PendingCatalogReconciliation | None,
) -> None:
    """CAS-activate a validated generation and its unique ready outbox."""
```

- [ ] 写失败测试：候选 snapshot 出现重复 slug、condition ID、同 market position/outcome、缺失 parent 或 count/digest 不符时抛 `CatalogConstraintError`，generation 置 ABORTED，活动指针不变。
- [ ] 写失败测试：用持续读取循环跨 activation，记录到的三表 generation tuple 只能是完整旧代或完整新代，不能出现 mixed generation。
- [ ] 写失败测试：在 activation 命令中注入 outbox INSERT 失败，断言 generation 状态、活动指针、outbox 全部回滚；重试后只产生一个 ready event。
- [ ] 运行 `pytest tests/unit/persistence/test_catalog_generations.py tests/integration/test_catalog_generation_v4.py -q`，确认失败。
- [ ] 用索引友好 SQL/临时候选集合验证当前候选；所有重型 count/digest/唯一性检查均在激活事务之前完成并把结果冻结到 generation 行。
- [ ] 实现 activation 单命令：条件更新 `catalog_generations.status`、CAS 更新 `catalog_state.active_generation_id`、插入唯一 `CATALOG_RECONCILIATION_READY`；受影响行数不为 1 时抛 conflict 并回滚。
- [ ] 为 ready outbox 建立幂等唯一键或等价唯一索引，避免依赖 `NOT EXISTS` 扫描。
- [ ] 完成 v4 `save_complete_catalog()` 的 stage→validate→activate 路径；保留第一阶段单聚合 reconciliation 语义。运行时版本在 Task 5 才切换，因此本任务的 v4 集成 fixture 显式调用 `create_v4_database()`。
- [ ] 运行新增测试、`pytest tests/unit/catalog/test_sync.py -q` 和 `pytest tests/integration/test_app_pipeline.py -q`，预期通过。
- [ ] Commit: `git add predmarket/persistence/catalog_generations.py predmarket/persistence/repositories.py tests/unit/persistence/test_catalog_generations.py tests/integration/test_catalog_generation_v4.py && git commit -m "feat: atomically activate catalog generations"`

### Task 5: Watch journal、updated_at rebase 与 revision CAS

**Files:**
- Modify: `predmarket/persistence/catalog_generations.py`
- Modify: `predmarket/persistence/repositories.py:334-385,1100-1190`
- Modify: `predmarket/persistence/schema.py:13-740`
- Modify: `tests/unit/catalog/test_repository_snapshot.py`
- Modify: `tests/unit/persistence/test_catalog_generations.py`
- Modify: `tests/unit/persistence/test_schema.py`
- Modify: `tests/integration/test_signal_concurrency.py`
- Modify: `tests/integration/test_watch_recovery.py`

**Interfaces:**

```python
@dataclass(frozen=True, slots=True)
class RebaseResult:
    generation_id: int
    from_revision: int
    through_revision: int
    copied_events: int
    copied_markets: int
    copied_tokens: int

async def CatalogGenerationCoordinator.rebase(
    self, validation: CandidateValidation
) -> tuple[CandidateValidation, RebaseResult]:
    """Replay winning runtime changes and revalidate the candidate."""
```

- [ ] 写失败测试：`save_event/save_market/save_token/save_catalog` 每次实际改变活动版本时，在同一事务递增 `runtime_revision` 并写一条 entity journal；较旧输入不写版本、不增 revision。
- [ ] 写失败测试：Watch `updated_at` 较新时复制到 STAGING；同步候选较新或相等时保留候选，覆盖现有 `>=` 语义。
- [ ] 写失败测试：rebase 与 activation 之间插入一次 Watch 写，首次 CAS 冲突，第二轮仅重放新增 revision 并成功；market/token 父子更新保持一个事务。
- [ ] 写失败测试：Watch 试图造成 slug/condition/position 冲突或缺失 parent 时，整个写事务失败且 revision/journal 不前进。
- [ ] 运行本任务四个测试文件，确认新增用例失败。
- [ ] 将现有 `_UPSERT_EVENT/_MARKET/_TOKEN` 改为 identity + 当前活动 generation version upsert；用 `RETURNING` 或受影响行数区分实际胜出的更新。
- [ ] 在同一 writer 命令中完成版本写、revision 增量和 journal insert；journal 唯一键为 `(runtime_revision, entity_type, entity_id)`。
- [ ] 实现 keyset revision rebase，复制 Watch 胜出版本到候选代，更新候选摘要并重跑受影响约束；保存 `rebased_runtime_revision`。
- [ ] 把 activation 包装为有界 CAS 重试循环；每次 conflict 重新读取新 journal、rebase 和 validate，不重写已完成同步批次。
- [ ] 在所有 catalog 写路径均已适配后，把 `SCHEMA_VERSION` 从 3 切为 4；`initialize_database()` 仅创建/接受 v4，现有 v1-v3 明确提示执行显式迁移，不再自动执行 v2→v3。
- [ ] 运行新增测试和 `pytest tests/unit/catalog/test_repository_snapshot.py tests/integration/test_watch_recovery.py tests/integration/test_signal_concurrency.py -q`，预期通过。
- [ ] Commit: `git add predmarket/persistence/catalog_generations.py predmarket/persistence/repositories.py predmarket/persistence/schema.py tests/unit/catalog/test_repository_snapshot.py tests/unit/persistence/test_catalog_generations.py tests/unit/persistence/test_schema.py tests/integration/test_signal_concurrency.py tests/integration/test_watch_recovery.py && git commit -m "feat: rebase runtime catalog changes before activation"`

### Task 6: 适配同步任务和所有 catalog 读取调用方

**Files:**
- Modify: `predmarket/catalog/sync.py:70-660`
- Modify: `predmarket/persistence/repositories.py`
- Modify: `predmarket/catalog/relations.py`
- Modify: `predmarket/signals/manager.py`
- Modify: `predmarket/watch/task.py`
- Modify: `predmarket/cli.py`
- Modify: `tests/unit/catalog/test_sync.py`
- Modify: `tests/integration/test_app_pipeline.py`
- Modify: `tests/integration/test_relation_activation.py`

**Interfaces:**

```python
class CatalogSyncAborted(RuntimeError):
    generation: str
    reason: str
    wal_peak_bytes: int
```

- [ ] 写失败测试：完整同步被 WAL 安全终止时，SyncMarketTask 记录 generation 级错误、保留旧 catalog、不发布 reconciliation，并允许下一轮同步重试。
- [ ] 写失败测试：现有 `load_catalog/get_event/get_market/get_token/has_watchable_catalog`、relation discovery、signal eligibility 和 CLI 只读查询在 v4 返回与 v3 fixture 相同的 domain 值。
- [ ] 用 `rg -n "\b(events|markets|tokens)\b" predmarket` 建立直接 SQL 审计清单；逐项确认只读视图可满足，写 SQL 必须改走 repository/version 表。
- [ ] 运行 `pytest tests/unit/catalog/test_sync.py tests/integration/test_app_pipeline.py tests/integration/test_relation_activation.py -q`，确认新增用例失败。
- [ ] 让 `CatalogRepository` 继续承担兼容边界；只在视图执行计划不合格时把调用方查询改为等价、索引友好的 active-generation CTE。
- [ ] SyncMarketTask 捕获 `CatalogSyncAborted`，按 generation 聚合记录日志和 system event；不得把安全终止当作已完成同步，也不得向 Watch 队列发送聚合 change。
- [ ] 检查并修正所有下游 FK 插入，让它们引用稳定 identity；保持公开 domain 类型和协议不变。
- [ ] 运行本任务测试及 `pytest tests/unit/catalog tests/unit/watch tests/unit/signals -q`，预期通过。
- [ ] Commit: `git add predmarket/catalog/sync.py predmarket/persistence/repositories.py predmarket/catalog/relations.py predmarket/signals/manager.py predmarket/watch/task.py predmarket/cli.py tests/unit/catalog/test_sync.py tests/integration/test_app_pipeline.py tests/integration/test_relation_activation.py && git commit -m "refactor: route catalog consumers through v4 views"`

### Task 7: 实现 v3→v4 旁路迁移、marker 恢复和禁止回退

**Files:**
- Create: `predmarket/persistence/catalog_migration.py`
- Modify: `predmarket/persistence/migration.py:1-210`
- Modify: `predmarket/persistence/schema.py:708-740`
- Modify: `predmarket/cli.py:38-62,190-205`
- Modify: `predmarket/app.py:145-180`
- Modify: `tests/unit/persistence/test_migration.py`
- Create: `tests/integration/test_catalog_migration_v4.py`
- Modify: `tests/integration/test_cli.py`

**Interfaces:**

```python
class MigrationStage(StrEnum):
    BUILDING = "BUILDING"
    VALIDATED = "VALIDATED"
    V3_BACKED_UP = "V3_BACKED_UP"
    V4_INSTALLED = "V4_INSTALLED"
    COMPLETE = "COMPLETE"

@dataclass(frozen=True, slots=True)
class MigrationResult:
    database: Path
    backup: Path
    source_version: int
    target_version: int

def migrate_v3_to_v4(database_path: Path) -> MigrationResult:
    """Build, validate, and atomically install a side-by-side v4 database."""

def recover_v4_switch(database_path: Path) -> None:
    """Resolve an interrupted marker-governed v4 file switch."""

def migrate_database(
    database_path: Path,
    backup_path: Path | None = None,
    *,
    target_version: int,
) -> MigrationResult | None:
    """Route legacy v1→v2 or side-by-side v3→v4 migration."""
```

- [ ] 写失败测试：从带完整下游数据的 v3 fixture 迁移，断言原 v3 文件变为唯一时间戳 `*.pre-v4.sqlite3`、正式路径为 v4、行数/规范化摘要/FK/关键查询一致。
- [ ] 写失败测试：空间预检失败、锁失败、非 v3 输入或遗留未解析 marker 时不修改原库。
- [ ] 参数化注入 `BUILDING/VALIDATED/V3_BACKED_UP/V4_INSTALLED` 各阶段崩溃；`recover_v4_switch()` 后正式路径唯一且只能是验证通过的 v4 或原 v3。
- [ ] 写失败测试：`initialize_database()` 遇到 v3 明确拒绝并提示执行 `predmarket migrate --to 4`；遇到 v4 正常启动；旧版本逻辑不能将 v4 视为可降级输入。
- [ ] 运行 `pytest tests/unit/persistence/test_migration.py tests/integration/test_catalog_migration_v4.py tests/integration/test_cli.py -q`，确认失败。
- [ ] 在同目录创建唯一临时 v4 文件；获得进程级独占锁，checkpoint 并验证 WAL/SHM 静止，检查可用空间覆盖 v3+预计 v4+安全余量。
- [ ] keyset 分页复制三类 identity/version 和全部业务表；创建合成 committed baseline generation；每批复用 `WalPolicy`，原 v3 连接始终只读。
- [ ] 切换前执行全表计数、规范化摘要、`integrity_check`、`foreign_key_check`、当前快照约束、`load_catalog` 与关键 JOIN 冒烟。
- [ ] marker 更新必须使用同目录临时 marker、`os.replace()`、文件 `fsync` 和父目录 `fsync`；按 stage 决定恢复动作，不依据文件名猜测。
- [ ] 原正式 v3 先原子重命名为备份，再安装已验证 v4；成功后 marker 标 COMPLETE，备份永不自动删除。
- [ ] 保留 `migrate_database(database, backup, target_version=2)` 行为且要求显式 backup；`target_version=4` 拒绝显式 backup、路由新迁移并让 CLI 输出实际自动备份路径。CLI 的 `--backup` 改为仅 v1→v2 使用的可选参数，分支校验缺失或多余参数。
- [ ] 运行新增测试和 `pytest tests/unit/persistence/test_schema.py tests/integration/test_cli.py -q`，预期通过。
- [ ] Commit: `git add predmarket/persistence/catalog_migration.py predmarket/persistence/migration.py predmarket/persistence/schema.py predmarket/cli.py predmarket/app.py tests/unit/persistence/test_migration.py tests/integration/test_catalog_migration_v4.py tests/integration/test_cli.py && git commit -m "feat: migrate schema v3 to v4 side by side"`

### Task 8: v4 doctor、清理器和可恢复游标

**Files:**
- Modify: `predmarket/persistence/integrity.py:1-390`
- Modify: `predmarket/persistence/catalog_generations.py`
- Modify: `tests/unit/persistence/test_integrity.py`
- Modify: `tests/unit/persistence/test_catalog_generations.py`

**Interfaces:**

```python
@dataclass(frozen=True, slots=True)
class CleanupResult:
    deleted_versions: int
    deleted_journal_rows: int
    remaining_versions: int
    remaining_journal_rows: int

async def CatalogGenerationCoordinator.cleanup(
    self, *, max_rows: int = 8_000
) -> CleanupResult:
    """Delete one recoverable batch of dominated versions and journal rows."""
```

- [ ] 写失败测试：doctor 报告无效 active pointer、多个 STAGING、候选计数/摘要异常、journal 超过最老 staging baseline、marker 未完成和 cleanup backlog；健康 v4 不误报。
- [ ] 写失败测试：清理只删除被当前有效版本支配的旧 committed 版本和 ABORTED payload；保留当前可见版本、任何 STAGING rebase 仍需的 journal 和 generation 诊断行。
- [ ] 写失败测试：清理中断后从 `catalog_state` cursor 恢复；每批不超过 8,000 行并遵守与同步相同的 WAL 暂停/终止逻辑。
- [ ] 运行 `pytest tests/unit/persistence/test_integrity.py tests/unit/persistence/test_catalog_generations.py -q`，确认失败。
- [ ] 更新 `_PROJECT_TABLES`/required views，并新增 generation/journal/marker finding codes；startup 只做快速结构检查，doctor 做完整语义检查。
- [ ] 实现按 entity type + entity ID 的 keyset 清理；无 staging 时 journal 可裁剪到当前 revision，有 staging 时保留最小 `base_runtime_revision` 之后记录。
- [ ] 记录 `page_count`、`freelist_count`、逻辑可淘汰版本数；不得执行 `VACUUM`。
- [ ] 运行新增测试，预期通过。
- [ ] Commit: `git add predmarket/persistence/integrity.py predmarket/persistence/catalog_generations.py tests/unit/persistence/test_integrity.py tests/unit/persistence/test_catalog_generations.py && git commit -m "feat: diagnose and clean catalog generations"`

### Task 9: 崩溃注入、长读阻塞和查询性能守门

**Files:**
- Modify: `tests/integration/test_catalog_generation_v4.py`
- Create: `tests/integration/test_catalog_wal_v4.py`
- Modify: `tests/integration/test_watch_recovery.py`
- Modify: `predmarket/persistence/catalog_generations.py`

**Interfaces:**

```python
class CatalogFaultPoint(StrEnum):
    AFTER_BATCH_COMMIT = "AFTER_BATCH_COMMIT"
    AFTER_VALIDATION = "AFTER_VALIDATION"
    AFTER_REBASE = "AFTER_REBASE"
    BEFORE_ACTIVATION_COMMIT = "BEFORE_ACTIVATION_COMMIT"
    AFTER_ACTIVATION_COMMIT = "AFTER_ACTIVATION_COMMIT"
    AFTER_CHECKPOINT = "AFTER_CHECKPOINT"
    AFTER_CLEANUP_BATCH = "AFTER_CLEANUP_BATCH"
```

- [ ] 参数化子进程/连接中断测试覆盖全部 fault point；重启后断言活动 catalog 完整、outbox 至多一次、STAGING 可恢复或可安全 ABORT、清理可续跑。
- [ ] 建立长读事务阻止 checkpoint，运行足以跨过 64 MiB 的生成数据；断言同步暂停，接近 96 MiB 前 ABORT，活动指针不变，Watch 小写入仍能执行。
- [ ] 持续读探针与 Watch 写并发完整同步；断言没有 mixed generation，最终值满足 updated_at 胜出规则。
- [ ] 保存 v3 fixture 的 `load_catalog`、watchable join、signal eligibility、relation/doctor、ID/slug/condition/token 点查基线和 `EXPLAIN QUERY PLAN`；v4 中不允许 version 表无索引相关全扫描。
- [ ] 运行 `pytest tests/integration/test_catalog_generation_v4.py tests/integration/test_catalog_wal_v4.py tests/integration/test_watch_recovery.py -q`，预期先失败。
- [ ] 仅增加测试所需的显式 fault hook，默认 `None` 且生产路径零副作用；不要通过环境变量静默启用。
- [ ] 对超过 v3 基线 20% 的查询补精确索引或 repository CTE，保持相同 active-generation 快照语义。
- [ ] 重跑本任务测试，预期全部通过且 WAL 始终 `<128 MiB`。
- [ ] Commit: `git add predmarket/persistence/catalog_generations.py tests/integration/test_catalog_generation_v4.py tests/integration/test_catalog_wal_v4.py tests/integration/test_watch_recovery.py && git commit -m "test: cover catalog generation crash and WAL bounds"`

### Task 10: 生产数据副本验收、文档和全量回归

**Files:**
- Create: `scripts/validate_catalog_v4.py`
- Modify: `docs/runtime-investigation-2026-08-04.md`
- Modify: `tests/integration/test_documented_commands.py`

**Command interface:**

```text
python scripts/validate_catalog_v4.py \
  --v3-database /absolute/path/catalog-v3.sqlite3 \
  --v4-database /absolute/path/catalog-v4.sqlite3 \
  --report /absolute/path/catalog-v4-report.json
```

- [ ] 先写 `test_documented_commands.py` 失败用例，断言脚本 `--help` 可执行、必须显式提供输入/输出路径且默认不修改 v3 输入。
- [ ] 实现只接受普通文件绝对路径的验收脚本；复制输入到工作 v4 路径，记录迁移、完整同步、并发 Watch/读探针、checkpoint、cleanup 和查询性能的 JSON 证据。
- [ ] 报告必须包含 v3/v4 行数与摘要、WAL `peak_bytes`、每个 checkpoint 结果、mixed generation 计数、stale Watch 计数、ready/outbound 聚合消息数、清理残留和每条关键查询 p50/p95 比值。
- [ ] 运行 `pytest tests/integration/test_documented_commands.py -q`，预期通过。
- [ ] 在生产数据库副本运行脚本；验收：`peak_bytes <= 134217728`、mixed/stale 为 0、ready 和聚合消息各 1、清理后可淘汰版本为 0、所有关键查询 p95 退化 `<=20%`。
- [ ] 将报告中的真实数值和命令写入 `docs/runtime-investigation-2026-08-04.md`；不得写“通过”而不附报告路径和数值。
- [ ] 运行最窄静态检查：`python -m compileall -q predmarket scripts/validate_catalog_v4.py`，预期退出码 0。
- [ ] 运行全量测试：`pytest -q`，预期全部通过且无 warning/error 新增。
- [ ] 运行 `git diff --check`，预期无输出；运行 `git status --short`，确认只含本计划所列文件。
- [ ] Commit: `git add scripts/validate_catalog_v4.py docs/runtime-investigation-2026-08-04.md tests/integration/test_documented_commands.py && git commit -m "docs: record catalog v4 production acceptance"`

## Final Review Gate

- [ ] 对照设计文档逐条确认：WAL 128 MiB、原子读取、Watch 新鲜度、崩溃恢复、v3 数据迁移、无自动回退、查询退化不超过 20% 全有自动化或真实数据证据。
- [ ] 搜索占位符：`rg -n "TODO|TBD|FIXME|pass$|NotImplemented" predmarket scripts tests docs/superpowers`，逐项解释已有内容或清除本次新增占位符。
- [ ] 搜索 catalog 直接写：`rg -n "(INSERT INTO|UPDATE|DELETE FROM) (events|markets|tokens)" predmarket`，预期生产代码无对兼容视图的写入。
- [ ] 搜索 schema 常量和迁移路由：`rg -n "SCHEMA_VERSION|target_version|user_version" predmarket tests`，确认 v4 是运行时唯一目标，v1→v2 仅保留显式历史迁移能力。
- [ ] 使用 `superpowers:requesting-code-review` 做合并前审阅；修复发现后再次运行受影响测试、`pytest -q` 和生产副本验收脚本。
