# Polymarket Dynamic Fees and Sync Errors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持 Polymarket 动态费率并在市场同步映射失败时立即输出接口响应摘要。

**Architecture:** 在现有 `FeeSchedule`/`FeeCalculator` 中加入 `CURVE` 模型和 taker 语义；SDK 网关负责将 `feeSchedule` 完整映射到领域模型；同步任务返回并持久化包含限长 API 响应的错误，Supervisor/Notifier 负责初始及周期同步的终端输出。保留 `ZERO`、`FLAT` 和已有目录的 fail-closed 行为。

**Tech Stack:** Python 3.14、`Decimal`、pytest/pytest-asyncio、SQLite JSON、Polymarket pinned SDK。

## Global Constraints

- 不新增运行时依赖。
- `ZERO`、`FLAT` 的既有 JSON 和构造方式保持兼容。
- 动态费用公式为 `quantity × rate × (price × (1 - price)) ^ exponent`。
- `rebate_rate` 保存用于审计，不抵扣当前 taker 费用。
- 映射失败必须 fail closed，不能静默转换为零费用或提交不完整 generation。
- API 响应日志最多 8 KiB，并通过现有终端/system event 通道输出和持久化。

---

### Task 1: Add the CURVE fee domain model and calculator

**Files:**
- Modify: `predmarket/domain/fees.py`
- Test: `tests/unit/domain/test_fees.py`

**Interfaces:**
- Produces `FeeModel.CURVE`.
- Extends `FeeSchedule` with `taker_only: bool = False` and JSON read support.
- Extends `FeeCalculator.calculate(..., is_taker: bool = True)`.

- [x] **Step 1: Write failing tests for dynamic fee parsing and calculation**

Add tests that construct a JSON schedule with `model="CURVE"`, `rate="0.04"`, `exponent="1"`, `rebate_rate="0.25"`, and `taker_only=True`; assert the fields are preserved and `price=0.4`, `quantity=10` calculates `Decimal("0.096")`. Add a test that `is_taker=False` returns zero for a taker-only schedule, and a test that a positive fee below five decimal places returns `Decimal("0.00001")`.

- [x] **Step 2: Run the focused tests and verify they fail for the intended reason**

Run:

```bash
.venv/bin/pytest tests/unit/domain/test_fees.py -q
```

Expected: the new tests fail because `CURVE` is not an accepted `FeeModel` and `FeeSchedule` has no `taker_only` field.

- [x] **Step 3: Implement the minimal domain changes**

In `predmarket/domain/fees.py`:

```python
class FeeModel(str, Enum):
    ZERO = "ZERO"
    FLAT = "FLAT"
    CURVE = "CURVE"
```

Add `taker_only: bool = False` after `updated_at`, accept optional `taker_only` in `from_json`, validate `CURVE` parameters exactly, and calculate the curve with `Decimal`, `ROUND_HALF_UP`, `Decimal("0.00001")`, and `is_taker`.

- [x] **Step 4: Run the focused tests and verify they pass**

Run:

```bash
.venv/bin/pytest tests/unit/domain/test_fees.py -q
```

Expected: all fee-domain tests pass, including existing `ZERO`/`FLAT` behavior.

### Task 2: Map the SDK fee schedule and persist the new field

**Files:**
- Modify: `predmarket/polymarket/gateway.py`
- Modify: `predmarket/persistence/repositories.py`
- Modify: `tests/fixtures/sdk/events.json` (the fixture contains both events and markets)
- Modify: `tests/unit/polymarket/test_gateway.py`
- Test: `tests/unit/persistence/test_repositories.py` (if an existing fee round-trip fixture is present; otherwise add the smallest repository round-trip test beside the existing token tests)

**Interfaces:**
- Produces `FeeSchedule(model=FeeModel.CURVE, parameters={"rate", "exponent", "rebate_rate"}, taker_only=...)` for new SDK data.
- Keeps the existing flat fixture mapped to `FeeModel.FLAT`.
- Persists and reads `taker_only` in `fee_schedule_json`.

- [x] **Step 1: Update the SDK fixture and add failing mapping assertions**

Change the enabled market fixture to use `fee_type="politics_fees"`, `exponent=1`, `rate="0.04"`, `taker_only=true`, and `rebate_rate="0.25"`. Update the gateway test to assert `CURVE`, all three Decimal parameters, `taker_only is True`, and the existing disabled market remains `ZERO`.

- [x] **Step 2: Run the gateway test and verify the old mapper rejects the new shape**

Run:

```bash
.venv/bin/pytest tests/unit/polymarket/test_gateway.py::test_list_active_markets_maps_tokens_and_authoritative_fee_schedules -q
```

Expected: FAIL with `SDK fee schedule cannot be represented by the FLAT domain model`.

- [x] **Step 3: Implement mapping and JSON persistence**

Update `_map_fee_schedule` to retain the legacy `FLAT` branch only for the exact old shape; map all other enabled schedules to `CURVE`. Update `_encode_fee_schedule` to emit `taker_only` when true or whenever required by the schedule, and make `FeeSchedule.from_json` default missing legacy values to false.

- [x] **Step 4: Add a persistence round-trip assertion and run focused tests**

Persist a token with the new schedule, reload it through the repository, and assert `model`, all parameters, `taker_only`, and `updated_at` are unchanged. Run:

```bash
.venv/bin/pytest tests/unit/domain/test_fees.py tests/unit/polymarket/test_gateway.py tests/unit/persistence -q
```

Expected: all focused tests pass.

### Task 3: Include bounded upstream responses in mapping failures

**Files:**
- Modify: `predmarket/polymarket/gateway.py`
- Modify: `predmarket/catalog/sync.py`
- Test: `tests/unit/polymarket/test_gateway.py`
- Test: `tests/unit/catalog/test_sync.py`

**Interfaces:**
- `GatewayMappingError` text for market mapping failures contains `market <id>`, the original error, and `api_response=<JSON summary>`.
- `SyncResult.error` and `SYNC_GENERATION_INCOMPLETE.details.error` preserve that text.

- [x] **Step 1: Add failing tests for response logging**

Create a malformed SDK market whose fee schedule cannot be mapped. Assert that `PolymarketGateway.list_active_markets()` raises `GatewayMappingError` containing the market ID and `api_response`. Add a sync test with a gateway raising that error and assert the returned error and persisted system event details contain the same response marker.

- [x] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
.venv/bin/pytest tests/unit/polymarket/test_gateway.py tests/unit/catalog/test_sync.py -q
```

Expected: FAIL because current mapping errors do not include an upstream response summary.

- [x] **Step 3: Implement bounded response serialization**

Add a gateway-local helper that uses `model_dump(mode="json")` when available, falls back to a safe attribute representation, serializes with deterministic compact JSON, and truncates at 8192 characters. Wrap market mapping errors with the summary while preserving the original exception as `__cause__`.

- [x] **Step 4: Run focused tests and inspect the persisted event**

Run the commands from Step 2. Verify the response is present, capped, and the sync event still has `event_type="SYNC_GENERATION_INCOMPLETE"`.

### Task 4: Print sync error details for initial and recurring syncs

**Files:**
- Modify: `predmarket/notification/notifier.py`
- Modify: `predmarket/app.py`
- Modify: `tests/unit/notification/test_notifier.py`
- Modify: `tests/integration/test_app_pipeline.py`

**Interfaces:**
- Error notifications print a second JSON details line to the configured terminal.
- `_sync_forever(sync, notifier)` notifies on every incomplete periodic result.

- [x] **Step 1: Add failing notification tests**

Add a notifier test that sends `SYNC_GENERATION_INCOMPLETE` with `details={"error": "...api_response=..."}` and asserts the terminal contains both the event line and a JSON details line. Add an app test using a fake sync result with `complete=False`, run one periodic iteration, cancel it, and assert the notifier received the error details.

- [x] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
.venv/bin/pytest tests/unit/notification/test_notifier.py tests/integration/test_app_pipeline.py -q
```

Expected: the notifier test fails because details are currently not printed, and the periodic-sync test fails because `_sync_forever` ignores its result.

- [x] **Step 3: Implement terminal details and recurring notifications**

Serialize only operational error event details using compact JSON and `default=str`; keep signal notification output unchanged. Change `_sync_forever` to receive the notifier, inspect `SyncResult.complete`, and call `notifier.notify(event_type="SYNC_GENERATION_INCOMPLETE", message="Market sync generation was incomplete", details={"error": result.error, "sync_generation": result.sync_generation})` when incomplete.

- [x] **Step 4: Run focused integration tests**

Run the command from Step 2. Expected: all notification and app pipeline tests pass.

### Task 5: Run regression verification and update project documentation

**Files:**
- Modify: `README.md` only if the fee behavior or troubleshooting instructions are missing.
- Modify: `docs/superpowers/specs/2026-08-02-polymarket-dynamic-fees-sync-errors-design.md` only for verified implementation notes.

- [x] **Step 1: Run the narrow full regression suite**

Run:

```bash
.venv/bin/pytest tests/unit/domain/test_fees.py tests/unit/polymarket/test_gateway.py tests/unit/persistence tests/unit/catalog/test_sync.py tests/unit/notification/test_notifier.py tests/integration/test_app_pipeline.py -q
```

Expected: PASS, with no collection failures caused by the new code.

- [x] **Step 2: Run the full test suite**

Run:

```bash
.venv/bin/pytest -q
```

Expected: all collected tests pass. If an external dependency or network fixture prevents collection, record the exact failing module and error without claiming full success.

- [x] **Step 3: Run a static syntax/import check**

Run:

```bash
.venv/bin/python -m compileall -q predmarket
```

Expected: exit code 0.
