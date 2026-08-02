# Market/Event Relation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Allow a legal market without an upstream event relation to be persisted and watched, while keeping event-linked markets safe, rebuilding the local event reverse index transactionally, and making startup independent from a successful initial sync when a usable catalog already exists.

**Architecture:** Treat `Market.event_id` as an optional relation. Keep `Event.market_ids` as a local, derived reverse index rather than an upstream completeness requirement. Permit orphan markets through Gateway, domain validation, sync preparation, and repository persistence. Version the SQLite schema to v2 with a nullable market event foreign key and provide an explicit backup-backed migration command. Let Supervisor start Watch from an existing watchable catalog after a degraded/incomplete sync, while retaining periodic sync retries and fatal handling for database/integrity failures.

**Tech Stack:** Python 3.14, asyncio, aiosqlite, SQLite, pytest, argparse.

## Global Constraints

- Work only in `/Users/lifei/workspace/earn_money_from_prediction/.worktrees/issue-6-market-event-relation`.
- Preserve unrelated user changes and do not commit, push, deploy, or edit the main worktree.
- Follow TDD for each behavior: add or update the narrowest test, run it to observe failure, implement the minimum change, then run it green.
- Do not infer event-level strategies for an orphan market; binary market watching remains valid, while event/neg-risk behavior requires a resolved event relation.
- Do not make startup perform implicit schema migration. Existing v1 databases must request the explicit migration command.

---

## Task 1: Make the domain, changes, and Gateway accept zero-or-one event relation

**Files:** `predmarket/domain/market.py`, `predmarket/catalog/changes.py`, `predmarket/polymarket/gateway.py`, `tests/unit/domain/test_models.py`, `tests/unit/catalog/test_changes.py`, `tests/unit/polymarket/test_gateway.py`.

- [ ] Add tests proving `Market(event_id=None)` is valid, `Event(market_ids=())` is valid, and `MarketChange` permits a null event for market-scoped changes but still requires an event for `EVENT_SETTLED`.
- [ ] Run the focused model/change tests and confirm they fail under the current non-null validation.
- [ ] Change `Market.event_id` and non-event `MarketChange.event_id` to `str | None`; retain strict non-empty validation when a value is present and preserve event-settlement invariants.
- [ ] Allow an empty `Event.market_ids` tuple while retaining canonical, unique, non-empty IDs when entries exist.
- [ ] Add Gateway mapping tests for no event, one event, multiple events, and an invalid event reference; preserve warning-and-skip behavior for malformed market records.
- [ ] Change `_map_market` to map zero references to `None`, exactly one valid reference to its ID, and reject more than one or malformed references with a mapping warning.
- [ ] Run `pytest -q tests/unit/domain/test_models.py tests/unit/catalog/test_changes.py tests/unit/polymarket/test_gateway.py`.

## Task 2: Introduce schema v2 and an explicit migration path

**Files:** `predmarket/persistence/schema.py`, new `predmarket/persistence/migration.py`, `predmarket/cli.py`, `tests/unit/persistence/test_schema.py`, new `tests/unit/persistence/test_migration.py`, `tests/integration/test_cli.py`.

- [ ] Add failing tests that a new database is v2 with nullable `markets.event_id`, that initialization refuses v1 with an explicit migration message, and that migration preserves rows, indexes, foreign keys, and creates the requested backup.
- [ ] Run the focused schema/migration tests to record the expected failures.
- [ ] Add `SCHEMA_V2`, set `SCHEMA_VERSION = 2`, keep v1 SQL available for migration fixtures, and make fresh initialization use v2.
- [ ] Implement an explicit transactional migration function that validates v1, creates the backup through SQLite backup/copy semantics, rebuilds only the `markets` table with a nullable foreign key, restores indexes, sets `PRAGMA user_version = 2`, and verifies integrity before success; failures must roll back and leave the source usable.
- [ ] Add `predmarket migrate --to 2 --database PATH --backup PATH` with clear success/error output and no startup side effect.
- [ ] Update schema-facing tests and CLI tests, then run the focused suite.

## Task 3: Rebuild the local Event reverse index from persisted markets

**Files:** `predmarket/persistence/repositories.py`, `predmarket/persistence/integrity.py`, `tests/unit/persistence/test_writer.py`, `tests/unit/persistence/test_integrity.py`, `tests/unit/catalog/test_repository_snapshot.py`.

- [ ] Add failing repository tests showing an orphan market can be saved in one catalog transaction, event `market_ids_json` is derived only from linked markets, and a linked market relation change updates both the market and affected event indexes atomically.
- [ ] Add an integrity/doctor-facing test for empty event market arrays and orphan counts/watchable counts.
- [ ] Change repository serialization/deserialization to support null `event_id` and replace upstream-provided reverse indexes with a deterministic transaction-local rebuild for every affected event, including events with zero linked markets.
- [ ] Keep `save_market` safe for linked and orphan markets; reject a non-null missing parent, but do not reject a null parent.
- [ ] Permit canonical empty ID arrays in integrity checks and retain mismatch detection against actual linked rows.
- [ ] Add a read-only catalog diagnostics function/command payload that reports schema status, orphan markets, empty events, and watchable market/token counts without mutating the database.
- [ ] Run persistence and integrity focused tests.

## Task 4: Remove sync’s false event-parent and upstream reverse-index requirements

**Files:** `predmarket/catalog/sync.py`, `tests/unit/catalog/test_sync.py`.

- [ ] Add failing sync tests for a complete generation containing an orphan market, an active event with zero parsed markets, a market changing only its event relation, and a market change whose publication affects only its `market_id`.
- [ ] Run the focused sync tests before implementation.
- [ ] Update complete-source validation so null event IDs are legal, missing non-null event parents remain an error, active events may have zero markets, and `Event.market_ids` is not treated as authoritative input completeness.
- [ ] Update complete and incomplete preparation to avoid indexing orphan markets under an event, to preserve zero-market events, to propagate event state only when a parent exists, and to compare market identity by stable `condition_id` while allowing `event_id` changes.
- [ ] Ensure incomplete preparation can retain/store safe orphan snapshots and never filters them solely because no event row exists.
- [ ] Make event-settled changes event-wide and market changes market-specific; avoid `events[None]` lookups and ensure marker affected IDs match the change scope.
- [ ] Run `pytest -q tests/unit/catalog/test_sync.py tests/unit/catalog/test_changes.py` and then the broader catalog suite.

## Task 5: Start Watch from an existing legal catalog during degraded sync

**Files:** `predmarket/app.py`, `predmarket/watch/task.py`, `predmarket/strategy/common.py`, `tests/integration/test_app_pipeline.py`, `tests/unit/watch/test_task.py`, relevant strategy tests.

- [ ] Add a Supervisor integration test where the first sync is incomplete, the database already contains a legal active market/token, and Watch starts immediately while sync retry remains scheduled; add a no-catalog test proving retries continue without Watch startup.
- [ ] Run the new integration tests to observe failure caused by the current unconditional retry loop.
- [ ] Add a catalog-level watchability check or equivalent snapshot predicate and change Supervisor startup to use it: incomplete sync plus usable committed catalog starts Watch; incomplete sync without one keeps retrying; database/schema/integrity failures remain fatal.
- [ ] Keep Watch’s existing subscription/recovery behavior and ensure `_watchable_token_ids` does not require an event relation. Keep `_ApplicationContextSource` event-only paths conditional so orphan markets still receive binary evaluation contexts.
- [ ] Ensure incomplete sync metadata does not unnecessarily invalidate a previously committed watchable catalog, or otherwise preserve the documented “last committed catalog” semantics for monitoring.
- [ ] Run the focused app/watch/strategy tests and the full integration pipeline suite.

## Task 6: Document operations and perform full verification

**Files:** `docs/OPERATIONS.md`, `docs/VERIFICATION.md`, `docs/TUTORIAL.md`, `tests/` as needed for regression coverage.

- [ ] Add documentation for schema v1→v2 migration, required backup path, rollback/failure handling, orphan-market semantics, the degraded startup behavior, and the diagnostics command.
- [ ] Run `pytest -q` from the issue worktree.
- [ ] Run `git diff --check` and inspect the final diff/status for unrelated changes, generated artifacts, and accidental main-worktree edits.
- [ ] Report changed files, focused/full verification results, migration usage, and any remaining manual risk without claiming external issue closure or deployment.
