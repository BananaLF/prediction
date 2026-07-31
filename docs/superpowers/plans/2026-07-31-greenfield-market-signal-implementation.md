# Greenfield Market Signal System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy schema-v7 scanner with the read-only, SDK-backed Greenfield market sync, live watch, strategy, and auditable arbitrage-signal system defined in the approved architecture spec.

**Architecture:** Build a Python 3.11 modular monolith with one asyncio process, an official `polymarket-client==0.3.0b1` gateway, a bounded sync-to-watch queue, direct in-process strategy calls, and a single SQLite writer actor. Strategies remain pure; SignalManager owns lifecycle decisions and persists immutable revisions plus exact order-book evidence.

**Tech Stack:** Python 3.11, `polymarket-client==0.3.0b1`, asyncio, aiosqlite, PyYAML, pytest, pytest-asyncio, Hypothesis, SQLite WAL.

## Global Constraints

- Source of truth: `docs/superpowers/specs/2026-07-31-greenfield-market-signal-architecture-design.md`.
- Python must be `>=3.11`; pin `polymarket-client==0.3.0b1`.
- Runtime is read-only toward Polymarket: public SDK client only; no wallet, authentication, signing, orders, or on-chain mutation.
- Schema v1 contains exactly 10 project tables; do not migrate or read schema v7.
- All runtime SQLite writes go through one `DatabaseWriter`; independent relation CLI writes use short transactions, busy timeout, and bounded retries.
- Use TDD for every behavior: write a focused failing test, run it and capture the expected failure, implement minimally, then run focused and relevant broader tests.
- Monetary values, prices, ratios, and quantities use canonical Decimal strings; never use float for business calculations.
- `risk_rate = worst_case_loss / total_capital`.
- Strategy returns only `OpportunityPresent`, `OpportunityAbsent`, or `NotEvaluable`; SignalManager alone chooses OPENED, UPDATED, CLOSED, or no-op.
- A CLOSED revision caused by `OpportunityAbsent` stores current metrics and evidence; a CLOSED revision caused by `NotEvaluable` stores null economic fields and non-null closure context.
- Only `APPROVED` A⇒B relations participate in logical strategy. `APPROVED` is not revocable in this validation version.
- NegRisk is driven only by complete authoritative SDK metadata and fee schedules; missing proof returns `NotEvaluable`.
- Do not permanently store ordinary rejected evaluations or the full WebSocket stream.
- Preserve unrelated user files. Do not add, commit, or delete `docs/ARCHITECTURE-REVIEW.md` or `docs/reviews/`.

## Target File Map

```text
predmarket/
├── __init__.py                 package version
├── __main__.py                 module entry point
├── app.py                      supervisor and task wiring
├── cli.py                      read-only CLI and relation workflow
├── config.py                   strict YAML/environment configuration
├── domain/
│   ├── __init__.py
│   ├── decimal.py              canonical Decimal parsing/encoding
│   ├── fees.py                 normalized fee schedules/calculator
│   ├── market.py               event/market/token and sync models
│   ├── orderbook.py            immutable L2 books and generation state
│   ├── relation.py             A⇒B relation state machine
│   └── signal.py               decisions, signals, revisions, legs
├── polymarket/
│   ├── __init__.py
│   └── gateway.py              only SDK import boundary
├── persistence/
│   ├── __init__.py
│   ├── schema.py               schema v1 DDL
│   ├── writer.py               single writer actor
│   ├── repositories.py         typed reads and write commands
│   └── integrity.py            startup/acceptance integrity checks
├── catalog/
│   ├── __init__.py
│   ├── sync.py                 complete-generation synchronization
│   ├── changes.py              bounded MarketChangeQueue policy
│   └── relations.py            detection, analysis, approval/change log
├── watch/
│   ├── __init__.py
│   ├── cache.py                generation-aware order-book cache
│   └── task.py                 subscriptions, recovery barrier, dispatch
├── strategy/
│   ├── __init__.py
│   ├── engine.py               affected-strategy routing
│   ├── binary.py               underpriced and overpriced binary sets
│   ├── implication.py          approved A⇒B strategy
│   ├── neg_risk.py             complete-set NegRisk strategy
│   ├── optimizer.py            L2 breakpoint quantity optimization
│   └── risk.py                 failure scenarios and risk metrics
├── signals/
│   ├── __init__.py
│   └── manager.py              lifecycle, CAS, atomic evidence writes
└── notification/
    ├── __init__.py
    └── notifier.py             terminal/desktop notifications
```

Legacy `predmarket/*.py`, `predmarket/polymarket/{clob,gamma,ws}.py`, legacy tests, and obsolete `bin/*` entry points are removed rather than adapted.

---

### Task 1: Greenfield package, configuration, and safety boundary

**Files:**
- Delete: legacy files under `predmarket/`, `tests/`, and `bin/` not present in the Target File Map
- Create: package directories and `__init__.py` files from the Target File Map
- Create: `predmarket/config.py`
- Create: `tests/unit/test_config.py`
- Create: `tests/integration/test_read_only_surface.py`
- Modify: `pyproject.toml`
- Modify: `config/default.yaml`

**Interfaces:**
- Produces: `AppConfig.load(path: Path) -> AppConfig`
- Produces: `DatabaseConfig`, `PolymarketConfig`, `RuntimeConfig`, `StrategyConfig`, `SignalConfig`, `RelationsConfig`, `NotificationConfig`
- Produces: package dependency and import boundary enforced by tests

- [ ] **Step 1: Delete the explicit legacy implementation/test/entry-point files and create the empty target package structure**

Use `git rm` only for tracked files listed by `git ls-files predmarket tests bin`. Preserve documentation and unrelated untracked files.

- [ ] **Step 2: Write failing configuration and read-only-boundary tests**

```python
def test_default_config_has_greenfield_limits(tmp_path: Path) -> None:
    config = AppConfig.load(Path("config/default.yaml"))
    assert config.database.busy_timeout_ms == 5000
    assert config.runtime.market_change_queue_capacity == 10_000
    assert config.strategy.bankroll == Decimal("1000")
    assert config.relations.llm_enabled is False

def test_only_gateway_imports_polymarket_sdk() -> None:
    offenders = sdk_imports_outside(Path("predmarket/polymarket/gateway.py"))
    assert offenders == []
```

- [ ] **Step 3: Run tests and verify RED**

Run: `pytest tests/unit/test_config.py tests/integration/test_read_only_surface.py -q`

Expected: FAIL because `AppConfig` and the new package boundary do not exist.

- [ ] **Step 4: Implement strict dataclass configuration and dependency pins**

Require every configured decimal to be a string before parsing. Reject unknown keys. Set:

```toml
requires-python = ">=3.11"
dependencies = [
  "aiosqlite>=0.20,<1",
  "PyYAML>=6,<7",
  "polymarket-client==0.3.0b1",
]
```

Remove direct `httpx` and `websockets` dependencies from application code.

- [ ] **Step 5: Run focused tests and package import smoke test**

Run: `pytest tests/unit/test_config.py tests/integration/test_read_only_surface.py -q`

Run: `python -m predmarket --help`

Expected: tests PASS; module help exits 0 without network access.

- [ ] **Step 6: Commit**

```bash
git add -A predmarket tests bin pyproject.toml uv.lock config/default.yaml
git commit -m "refactor: establish greenfield project skeleton"
```

### Task 2: Domain models, canonical Decimal, fees, and decisions

**Files:**
- Create: `predmarket/domain/decimal.py`
- Create: `predmarket/domain/fees.py`
- Create: `predmarket/domain/market.py`
- Create: `predmarket/domain/orderbook.py`
- Create: `predmarket/domain/relation.py`
- Create: `predmarket/domain/signal.py`
- Create: `tests/unit/domain/test_decimal.py`
- Create: `tests/unit/domain/test_fees.py`
- Create: `tests/unit/domain/test_models.py`

**Interfaces:**
- Produces: `parse_decimal(text: str) -> Decimal`, `encode_decimal(value: Decimal) -> str`
- Produces: `FeeSchedule.from_json(data)`,
  `FeeCalculator.calculate(schedule, price, quantity, *, evaluated_at_ms, max_age_seconds) -> Decimal`
- Produces: immutable `Event`, `Market`, `Token`, `OrderBook`, `OrderBookLevel`, `Relation`
- Produces: `OpportunityPresent`, `OpportunityAbsent`, `NotEvaluable`, `StrategyContext`

- [ ] **Step 1: Write failing canonical Decimal property and example tests**

Cover rejection of float input, NaN, infinity, exponent form, `+1`, negative zero, leading zero, zero/negative quantity, and out-of-range price. Assert `Decimal("1.2300")` encodes as `"1.23"` and all zero variants encode as `"0"`.

- [ ] **Step 2: Run Decimal tests and verify RED**

Run: `pytest tests/unit/domain/test_decimal.py -q`

Expected: FAIL because canonical functions are missing.

- [ ] **Step 3: Implement canonical Decimal functions and rerun GREEN**

Use one compiled plain-decimal regex and `Decimal.as_tuple()`; never convert through float.

- [ ] **Step 4: Write failing fee and domain-invariant tests**

Test known zero/flat schedules, unknown model, missing parameter, stale schedule, event canonical market IDs, relation transitions, immutable order-book sorting, and exact decision payload requirements.

- [ ] **Step 5: Run fee/model tests and verify RED**

Run: `pytest tests/unit/domain/test_fees.py tests/unit/domain/test_models.py -q`

- [ ] **Step 6: Implement minimal immutable domain models and FeeCalculator**

Use frozen dataclasses and explicit enums. `NotEvaluable` requires a stable reason code; `OpportunityAbsent` requires current calculation and evidence.

- [ ] **Step 7: Run all Task 2 tests**

Run: `pytest tests/unit/domain -q`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add predmarket/domain tests/unit/domain
git commit -m "feat: define greenfield domain contracts"
```

### Task 3: SQLite schema v1, writer actor, repositories, and integrity checks

**Files:**
- Create: `predmarket/persistence/schema.py`
- Create: `predmarket/persistence/writer.py`
- Create: `predmarket/persistence/repositories.py`
- Create: `predmarket/persistence/integrity.py`
- Create: `tests/unit/persistence/test_schema.py`
- Create: `tests/unit/persistence/test_writer.py`
- Create: `tests/unit/persistence/test_integrity.py`

**Interfaces:**
- Produces: `initialize_database(path: Path) -> None`
- Produces: `DatabaseWriter.start()`, `execute(command)`, `close()`
- Produces: `CatalogRepository`, `RelationRepository`, `SignalRepository`, `SystemEventRepository`
- Consumes: canonical domain encoders from Task 2

- [ ] **Step 1: Write failing schema tests for exactly 10 tables and all documented constraints**

Assert `user_version=1`, WAL, foreign keys, table names, strategy/relation/execution checks, nullable CLOSED metrics rules, `(market_id, token_id)` composite FKs, and one-open-signal partial index.

- [ ] **Step 2: Run schema tests and verify RED**

Run: `pytest tests/unit/persistence/test_schema.py -q`

- [ ] **Step 3: Implement schema v1 DDL and rerun GREEN**

Keep DDL centralized in `SCHEMA_V1`; reject any existing non-empty database whose `user_version != 1`.

- [ ] **Step 4: Write failing writer serialization and rollback tests**

Launch concurrent commands, assert maximum one active write transaction, deterministic results, rollback on failure, bounded queue, and clean shutdown.

- [ ] **Step 5: Implement DatabaseWriter and typed repositories**

Commands are callables receiving the writer-owned connection. Reads use short independent read connections. No repository exposes raw connection ownership.

- [ ] **Step 6: Write failing integrity tests**

Insert fixtures for non-array JSON, empty/duplicate/non-string/unsorted IDs, dangling signal market IDs, event dual-write mismatch, noncanonical Decimal, bad risk formula, and stale `latest_revision`.

- [ ] **Step 7: Implement integrity queries and run Task 3 tests**

Run: `pytest tests/unit/persistence -q`

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add predmarket/persistence tests/unit/persistence
git commit -m "feat: add schema v1 persistence layer"
```

### Task 4: Official SDK gateway and fixture contract

**Files:**
- Create: `predmarket/polymarket/gateway.py`
- Create: `tests/fixtures/sdk/events.json`
- Create: `tests/fixtures/sdk/books.json`
- Create: `tests/unit/polymarket/test_gateway.py`
- Create: `tests/integration/test_sdk_live_readonly.py`

**Interfaces:**
- Produces: `PolymarketGateway.list_active_events()`
- Produces: `PolymarketGateway.list_active_markets()`
- Produces: `PolymarketGateway.get_order_books(token_ids)`
- Produces: `PolymarketGateway.subscribe_markets(token_ids)`
- Produces: `PolymarketGateway.refresh_market(market_id)`
- Produces: `PolymarketGateway.close()`
- Consumes: Task 2 domain types

- [ ] **Step 1: Inspect installed `polymarket-client==0.3.0b1` public models and record exact adapter fields in fixture tests**

Do not bypass the SDK with direct HTTP/WebSocket calls. If the pinned SDK lacks a required public read operation, stop with `BLOCKED` rather than inventing an API.

- [ ] **Step 2: Write failing gateway contract tests**

Use SDK-shaped fixtures to verify pagination termination, event/market/token mapping, authoritative NegRisk mapping, fee schedule mapping, complete L2 order books, and malformed-entity errors.

- [ ] **Step 3: Run gateway tests and verify RED**

Run: `pytest tests/unit/polymarket/test_gateway.py -q`

- [ ] **Step 4: Implement the AsyncPublicClient adapter**

Only this file may import `polymarket`. Convert SDK models immediately into immutable domain models and attach adapter mapping version.

- [ ] **Step 5: Run contract and optional live smoke tests**

Run: `pytest tests/unit/polymarket/test_gateway.py tests/integration/test_read_only_surface.py -q`

Run only when explicitly enabled: `POLYMARKET_LIVE_READONLY=1 pytest tests/integration/test_sdk_live_readonly.py -q`.

- [ ] **Step 6: Commit**

```bash
git add predmarket/polymarket tests/fixtures/sdk tests/unit/polymarket tests/integration
git commit -m "feat: adapt official Polymarket public SDK"
```

### Task 5: Complete-generation sync and market change queue

**Files:**
- Create: `predmarket/catalog/changes.py`
- Create: `predmarket/catalog/sync.py`
- Create: `tests/unit/catalog/test_changes.py`
- Create: `tests/unit/catalog/test_sync.py`

**Interfaces:**
- Produces: `MarketChange`, `MarketChangeType`, `MarketChangeQueue`
- Produces: `SyncMarketTask.run_once() -> SyncResult`
- Consumes: gateway from Task 4 and CatalogRepository/DatabaseWriter from Task 3

- [ ] **Step 1: Write failing queue policy tests**

Test that added/noncritical-updated events may be dropped, deactivated/settled events evict the oldest droppable item, all-critical full queues backpressure, overflow emits notification/system event, and existing Watch consumers continue.

- [ ] **Step 2: Implement queue policy and run GREEN**

Run: `pytest tests/unit/catalog/test_changes.py -q`

- [ ] **Step 3: Write failing complete/incomplete generation tests**

Cover full pagination, half pagination, request failure, required entity parse failure, idempotent upsert, commit-before-publish, no publish on rollback, and no missing/deactivation diff for incomplete generations.

- [ ] **Step 4: Implement SyncMarketTask**

Persist event/market/token generation identity and completeness. For incomplete generations, permit only non-deletion upserts and preserve previous active state.

- [ ] **Step 5: Run catalog tests**

Run: `pytest tests/unit/catalog -q`

- [ ] **Step 6: Commit**

```bash
git add predmarket/catalog tests/unit/catalog
git commit -m "feat: synchronize complete market generations"
```

### Task 6: Relation detection, LLM seam, approval CLI, and persistent activation log

**Files:**
- Create: `predmarket/catalog/relations.py`
- Create: `tests/unit/catalog/test_relations.py`
- Create: `tests/integration/test_relation_activation.py`
- Modify: `predmarket/cli.py`

**Interfaces:**
- Produces: `RelationDetector.detect(events, markets) -> list[RelationCandidate]`
- Produces: `RelationAnalyzer.analyze(relation) -> RelationAnalysis`
- Produces: `RelationChangeMonitor.run()`
- Produces CLI: `relations list/show/analyze/approve`

- [ ] **Step 1: Write failing relation lifecycle tests**

Assert only A⇒B, initial `NO_LLM_APPROVE`, analyzer-only transition to `LLM_APPROVE`, manual-only transition to `APPROVED`, no direct approval, no automatic approval, and no reviewer/time/version audit fields.

- [ ] **Step 2: Implement detector and analyzer interface**

Provide a deterministic fake analyzer for tests. Default `llm_enabled=false` leaves relations in `NO_LLM_APPROVE`.

- [ ] **Step 3: Write failing cross-process activation test**

Approve through a separate CLI database connection; assert relation status and `RELATION_ACTIVATED` are committed atomically and RelationChangeMonitor observes monotonically increasing system-event IDs.

- [ ] **Step 4: Implement approval CLI and monitor**

Never attempt to reach the process-local MarketChangeQueue from CLI.

- [ ] **Step 5: Run focused tests**

Run: `pytest tests/unit/catalog/test_relations.py tests/integration/test_relation_activation.py -q`

- [ ] **Step 6: Commit**

```bash
git add predmarket/catalog/relations.py predmarket/cli.py tests/unit/catalog/test_relations.py tests/integration/test_relation_activation.py
git commit -m "feat: add approved implication relation workflow"
```

### Task 7: Generation-aware WatchTask and WebSocket recovery barrier

**Files:**
- Create: `predmarket/watch/cache.py`
- Create: `predmarket/watch/task.py`
- Create: `tests/unit/watch/test_cache.py`
- Create: `tests/unit/watch/test_task.py`
- Create: `tests/integration/test_watch_recovery.py`

**Interfaces:**
- Produces: `OrderBookCache.apply_snapshot`, `apply_delta`, `invalidate`
- Produces: `WatchTask.run()`, `handle_market_change()`, `handle_stream_message()`
- Consumes: gateway, MarketChangeQueue, StrategyEngine protocol, SignalManager protocol

- [ ] **Step 1: Write failing cache generation tests**

Test valid sorted snapshot, delta application, stale/old generation rejection, sequence gap invalidation, hash mismatch invalidation, and immutable views.

- [ ] **Step 2: Implement OrderBookCache and run GREEN**

Run: `pytest tests/unit/watch/test_cache.py -q`

- [ ] **Step 3: Write failing recovery and subscription tests**

Assert initial active-token subscription, dynamic add/remove, disconnect closes related OPEN signals via `NotEvaluable`, REST full-book validation precedes resume, late old-generation messages are ignored, and recovered opportunity receives a new signal ID.

- [ ] **Step 4: Implement WatchTask recovery barrier**

State transition is exactly `VALID -> INVALID -> RESYNCING -> VALID`; no strategy evaluation occurs outside VALID.

- [ ] **Step 5: Run watch tests**

Run: `pytest tests/unit/watch tests/integration/test_watch_recovery.py -q`

- [ ] **Step 6: Commit**

```bash
git add predmarket/watch tests/unit/watch tests/integration/test_watch_recovery.py
git commit -m "feat: add resilient live order book watcher"
```

### Task 8: Pure strategy engine, optimization, and risk

**Files:**
- Create: `predmarket/strategy/engine.py`
- Create: `predmarket/strategy/binary.py`
- Create: `predmarket/strategy/implication.py`
- Create: `predmarket/strategy/neg_risk.py`
- Create: `predmarket/strategy/optimizer.py`
- Create: `predmarket/strategy/risk.py`
- Create: `tests/unit/strategy/test_binary.py`
- Create: `tests/unit/strategy/test_implication.py`
- Create: `tests/unit/strategy/test_neg_risk.py`
- Create: `tests/unit/strategy/test_optimizer.py`
- Create: `tests/unit/strategy/test_risk.py`

**Interfaces:**
- Produces: `StrategyEngine.evaluate(context) -> StrategyDecision`
- Produces four exact strategy types from the Spec
- Consumes: immutable domain models and FeeCalculator

- [ ] **Step 1: Write failing L2 optimizer and risk tests**

Cover breakpoint candidates, minimum size, bankroll cap, depth exhaustion, partial legs, failed conversion, immediate close depth, zero recovery for uncloseable exposure, and exact risk formula.

- [ ] **Step 2: Implement optimizer/risk and run GREEN**

Run: `pytest tests/unit/strategy/test_optimizer.py tests/unit/strategy/test_risk.py -q`

- [ ] **Step 3: Write failing tests for all four strategies**

Cover profitable and unprofitable paths, fees eliminating profit, stale books, leg skew, unknown fees, unapproved relation, A⇒B state payouts, NegRisk completeness/type/conversion/member/generation predicates, and affected-token routing.

- [ ] **Step 4: Implement minimal pure strategies**

No SDK, database, notification, current-signal, or wall-clock access. All time enters through `StrategyContext.evaluated_at`.

- [ ] **Step 5: Run strategy tests**

Run: `pytest tests/unit/strategy -q`

- [ ] **Step 6: Commit**

```bash
git add predmarket/strategy tests/unit/strategy
git commit -m "feat: evaluate four arbitrage strategies"
```

### Task 9: SignalManager lifecycle, CAS, and immutable evidence

**Files:**
- Create: `predmarket/signals/manager.py`
- Create: `tests/unit/signals/test_manager.py`
- Create: `tests/integration/test_signal_concurrency.py`

**Interfaces:**
- Produces: `SignalManager.apply(decision, opportunity_key, expected_revision)`
- Consumes: SignalRepository, DatabaseWriter, current market/relation state, subscription generation

- [ ] **Step 1: Write failing lifecycle tests**

Cover first OPENED, significant UPDATED, insignificant no-op, OpportunityAbsent CLOSED with current metrics/books/legs, NotEvaluable CLOSED with null economics and closure context, no signal for absent/not-evaluable without OPEN, and new signal after prior CLOSED.

- [ ] **Step 2: Run lifecycle tests and verify RED**

Run: `pytest tests/unit/signals/test_manager.py -q`

- [ ] **Step 3: Implement lifecycle and atomic persistence**

Persist revision, legs, snapshots, levels, main signal update, and canonical market IDs in one writer transaction; invoke Notifier only after the transaction commits.

- [ ] **Step 4: Write failing concurrency tests**

Race market deactivation against OPEN creation and two updates against the same expected revision. Assert precommit state validation, CAS rollback, retry/redecision, no duplicate revisions, and correct `latest_revision`.

- [ ] **Step 5: Implement CAS retry and run signal tests**

Run: `pytest tests/unit/signals tests/integration/test_signal_concurrency.py -q`

- [ ] **Step 6: Commit**

```bash
git add predmarket/signals tests/unit/signals tests/integration/test_signal_concurrency.py
git commit -m "feat: persist auditable signal lifecycles"
```

### Task 10: Supervisor, CLI, notifications, and end-to-end pipeline

**Files:**
- Create: `predmarket/app.py`
- Create: `predmarket/notification/notifier.py`
- Modify: `predmarket/cli.py`
- Modify: `predmarket/__main__.py`
- Create: `tests/unit/notification/test_notifier.py`
- Create: `tests/integration/test_app_pipeline.py`
- Create: `tests/integration/test_cli.py`

**Interfaces:**
- Produces CLI: `run`, `status`, `signals list/show`, relation commands
- Produces: `Supervisor.run()` with sync/watch crash termination semantics
- Consumes: all prior task interfaces

- [ ] **Step 1: Write failing notifier and CLI tests**

Assert terminal notification, notification failure system event, read-only command surface, signal market IDs, relation commands, no trading/auth commands, and exit codes.

- [ ] **Step 2: Implement notifier and CLI**

Desktop notification is optional and failure-isolated; terminal output always remains available.

- [ ] **Step 3: Write failing fake-gateway end-to-end tests**

Cover new market → sync commit → queue → subscription → book → strategy → OPEN evidence → notification; settlement → unsubscribe → CLOSED; queue overflow degradation; Watch crash notification plus process termination.

- [ ] **Step 4: Implement Supervisor and application wiring**

Start order is schema/integrity → gateway → first complete sync → watch initial REST books/subscriptions → continuous tasks. Unexpected WatchTask or SyncMarketTask exit terminates the process after notification.

- [ ] **Step 5: Run integration suite**

Run: `pytest tests/integration -q`

- [ ] **Step 6: Commit**

```bash
git add predmarket/app.py predmarket/cli.py predmarket/__main__.py predmarket/notification tests
git commit -m "feat: wire greenfield signal application"
```

### Task 11: Documentation, safe database reset, and final verification

**Files:**
- Modify: `README.md`
- Modify: `SECURITY.md`
- Modify: `STRATEGY.md`
- Modify: `docs/PROJECT-GUIDE.md`
- Modify: `docs/TUTORIAL.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `docs/VERIFICATION.md`
- Delete: `docs/QUICK-CHEAT-SHEET.md`
- Delete: `docs/SOAK-TEST.md`
- Create: `tests/integration/test_documented_commands.py`

**Interfaces:**
- Documents only implemented commands and schema v1 behavior
- Defines exact operator procedure for deleting the configured old SQLite main file and its explicit `-wal`/`-shm` siblings

- [ ] **Step 1: Write failing documented-command tests**

Extract local `predmarket` command examples from docs and assert help/argument parsing succeeds without network or database mutation.

- [ ] **Step 2: Rewrite project documentation**

Document architecture, 10 tables and fields, read-only safety, failure notifications, queue degradation, WebSocket recovery, relation approval, NegRisk eligibility, CLOSED semantics, and verification commands.

- [ ] **Step 3: Add and exercise safe database reset procedure**

The procedure must resolve and print the exact configured absolute path, refuse directories/symlinks/root/home/workspace roots, stop if a predmarket process is running, and target only the main file plus exact `-wal` and `-shm` siblings. Test it against a temporary directory; do not delete a real user database during tests.

- [ ] **Step 4: Run full verification**

Run:

```bash
pytest -q
python -m predmarket --help
python -m predmarket status --config config/default.yaml
```

Expected: all tests PASS; help and status exit successfully; no network mutation occurs.

- [ ] **Step 5: Verify repository and schema invariants**

Run:

```bash
git diff --check
python -m compileall -q predmarket
```

Create a temporary schema and verify exactly 10 project tables, `user_version=1`, `integrity_check=ok`, and empty `foreign_key_check`.

- [ ] **Step 6: Commit**

```bash
git add README.md SECURITY.md STRATEGY.md docs tests/integration/test_documented_commands.py
git commit -m "docs: document greenfield market signal system"
```

## Final Review Gate

- Generate a whole-branch review package from the implementation branch merge base through HEAD.
- Dispatch the most capable reviewer using `superpowers:requesting-code-review`.
- If findings exist, send one complete fix wave to one implementer, run focused and full tests, and perform one scoped re-review.
- Run `superpowers:verification-before-completion`.
- Finish with `superpowers:finishing-a-development-branch`; do not merge, push, or delete the original workspace without the user's explicit choice at that gate.
