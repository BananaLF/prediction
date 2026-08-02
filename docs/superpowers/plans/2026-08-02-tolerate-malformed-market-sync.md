# Tolerate Malformed Market Sync Implementation Plan

> **For agentic workers:** Use `superpowers:executing-plans` to execute this plan with verification checkpoints.

**Goal:** A single malformed market returned by Polymarket does not abort an otherwise valid market sync. The malformed market is excluded from the current catalog; an existing local record is retained for audit but deactivated, while a new market is not inserted. Batch-level request failures and cross-entity consistency failures remain generation-blocking.

**Architecture:** Keep item-level mapping isolation at the Polymarket gateway boundary. The gateway returns valid snapshots plus typed mapping warnings. `SyncMarketTask` treats those warnings as a complete generation, permits source validation only for the explicitly skipped market IDs, persists the normal complete snapshot, and emits a `SYNC_MARKET_SKIPPED` warning event. Runtime supervision forwards the warning to the existing notifier without changing retry behavior for incomplete generations.

**Tech Stack:** Python, asyncio, dataclasses, SQLite catalog repositories, pytest.

## Global constraints

- Work only in the isolated worktree `fix/skip-malformed-market-sync`.
- Preserve unrelated user changes in the main worktree and do not merge the PR.
- Keep the public behavior narrowly scoped to malformed individual market records.
- Do not catch errors from pagination, HTTP/client operations, or cross-market identity validation as item warnings.
- Use red-green TDD for each behavior and run the narrowest relevant tests before broader verification.

## Task 1: Add a typed per-market mapping warning at the gateway boundary

**Files:**

- Modify `predmarket/polymarket/gateway.py`.
- Modify `tests/unit/polymarket/test_gateway.py`.

**Steps:**

1. Add a frozen `MarketMappingWarning` value object containing `market_id` and the bounded mapping error text.
2. Extend `GatewayMappingError` with an optional `market_id`, populated only by `_map_market` when wrapping a malformed SDK market.
3. Add a `market_mapping_warnings` result property on `PolymarketGateway`, reset at the beginning of each active-market listing.
4. In `list_active_markets`, catch `GatewayMappingError` only around `_map_market`. Record errors with a market ID and continue to the next SDK market. Leave `_remember_market` and paginator errors uncaught so cross-entity and batch failures still abort the request.
5. Add/update gateway tests proving:
   - an empty `events` array skips only that market and still returns valid markets;
   - warning data contains the market ID and bounded API response;
   - malformed fee/state fields follow the same item-level behavior;
   - cross-market identity conflicts remain `GatewayMappingError` failures.

**Verification:**

```bash
pytest -q tests/unit/polymarket/test_gateway.py
```

## Task 2: Make catalog sync complete with explicit skipped-market warnings

**Files:**

- Modify `predmarket/catalog/sync.py`.
- Modify `tests/unit/catalog/test_sync.py`.

**Steps:**

1. Add stable warning fields to `SyncResult`: skipped market IDs and warning messages, both defaulting to empty tuples for compatibility.
2. Read the gateway’s typed `market_mapping_warnings` after a successful market request. Do not infer skipped IDs by parsing exception strings.
3. Pass skipped IDs into complete-source validation. Allow a source event to have no parsed market, or to reference the skipped market, only when the missing market ID is explicitly present in this warning set. Preserve all existing validation failures for unexplained omissions and unknown IDs.
4. Continue through the normal complete-generation path. This naturally retains missing historical markets and marks them inactive/closed/non-tradable, while omitted new markets are not inserted.
5. Append one `SYNC_MARKET_SKIPPED` system event with warning severity, generation, each skipped market ID, and its bounded mapping error details. Return the same warning information in `SyncResult`.
6. Add tests proving:
   - a new malformed market does not enter the catalog and the generation is complete;
   - an existing malformed market remains in the catalog for audit but is deactivated and excluded from the active/signal view;
   - the warning event is persisted with the market ID and error details;
   - a whole-market-request `GatewayMappingError` still produces an incomplete generation.

**Verification:**

```bash
pytest -q tests/unit/catalog/test_sync.py
```

## Task 3: Surface skipped-market warnings during startup and periodic sync

**Files:**

- Modify the relevant runtime supervisor/notifier implementation under `predmarket/` (identified by the existing `SYNC_GENERATION_INCOMPLETE` notification flow).
- Modify `tests/integration/test_app_pipeline.py`.

**Steps:**

1. Add `SYNC_MARKET_SKIPPED` to the notifier’s operational event types so its details are visible in normal runtime output.
2. After both initial and periodic sync attempts, notify the warning only when `SyncResult` reports skipped markets. Keep the existing retry loop unchanged for `complete=False`.
3. Add integration tests proving a complete sync with a skipped market starts/continues runtime operation and emits the warning, while incomplete batch failures retain their current retry and notification behavior.

**Verification:**

```bash
pytest -q tests/integration/test_app_pipeline.py
```

## Task 4: Regression verification and handoff

**Files:** No new source files; review all changed files and the committed design/plan documents.

**Steps:**

1. Run formatting/lint or type checks already configured by the repository, if any.
2. Run the complete relevant unit/integration suite, then inspect `git diff --check` and the final diff for unrelated changes.
3. Commit the implementation on `fix/skip-malformed-market-sync`.
4. Push the branch and open one draft PR for review. Do not merge it; wait for the user’s review and explicit merge direction.

**Verification:**

```bash
pytest -q tests/unit/polymarket/test_gateway.py tests/unit/catalog/test_sync.py tests/integration/test_app_pipeline.py
git diff --check
git status --short
```
