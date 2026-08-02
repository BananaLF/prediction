# Decimal SQLite v3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the database contract to v3 so every top-level Decimal value is stored as canonical plain-decimal `TEXT`, with exact `Decimal` reads/writes, long-fraction support, and a transactional v2→v3 migration.

**Architecture:** Keep `Decimal` as the only business-layer numeric type. Centralize compatibility decoding in `predmarket.domain.decimal`, retain canonical encoding for every new write, and make the v3 SQLite schema enforce text plus the canonical decimal grammar alongside existing domain ranges. Migrate legacy v2 rows by rebuilding affected tables inside one transaction, normalizing old decimal spellings before inserting into v3, then restore the existing indexes and set `PRAGMA user_version = 3`.

**Tech Stack:** Python 3.11+, `decimal.Decimal`, SQLite/`sqlite3`, `aiosqlite`, pytest, pytest-asyncio.

## Global Constraints

- Database version for this task is `3`; the preceding database task owns version `2`.
- Decimal persistence is canonical plain decimal `TEXT`; scientific notation is never emitted by new writes.
- Decimal persistence has no fixed scale; Python `Decimal` must preserve long finite coefficients and fractional scales.
- Business arithmetic and risk validation remain exact `Decimal` operations; no float conversion is allowed in normal write/read paths.
- Valid legacy decimal spellings may be normalized during migration; non-finite, malformed, or out-of-range data must roll back the complete migration.
- Preserve unrelated user changes; do not commit, push, deploy, or add dependencies.

---

### Task 1: Define Decimal compatibility and long-fraction behavior

**Files:**
- Modify: `predmarket/domain/decimal.py`
- Test: `tests/unit/domain/test_decimal.py` (create if absent)

**Interfaces:**
- Produces `decode_decimal(value: object) -> Decimal`, which accepts canonical strings and valid legacy decimal representations, rejects non-finite values, and never returns a float.
- Keeps `encode_decimal(value: Decimal) -> str` as the only new-write formatter and guarantees no exponent notation, redundant fractional zeroes, leading zeroes, or negative zero.

- [x] **Step 1: Write failing tests** for a 500-place fractional value, canonical output, legacy exponent input such as `"1E-5"`, integer input, finite float compatibility input converted through `str`, and rejection of `NaN`, `Infinity`, booleans, malformed strings, and non-finite floats.

- [x] **Step 2: Run the focused tests** with `UV_CACHE_DIR=/tmp/predmarket_uv_cache .venv/bin/python -m pytest -q tests/unit/domain/test_decimal.py`; verify the new decoder tests fail before implementation.

- [x] **Step 3: Implement the decoder** by constructing a `Decimal` without arithmetic rounding, checking finiteness, and returning the normalized Decimal. Keep `encode_decimal` based on `Decimal.as_tuple()` so long coefficients are not routed through binary floating point.

- [x] **Step 4: Run the focused tests** again and verify they pass, then run the existing Decimal/domain tests to catch regressions.

### Task 2: Build the v3 schema and transactional v2→v3 migration

**Files:**
- Modify: `predmarket/persistence/schema.py`
- Test: `tests/unit/persistence/test_schema.py`

**Interfaces:**
- Sets `SCHEMA_VERSION = 3` and exposes the v3 schema used for new databases.
- `initialize_database(path)` creates v3 for an empty path, accepts v3 unchanged, migrates v2 transactionally, and rejects other non-empty versions without mutation.
- Migration normalizes all top-level Decimal columns using `decode_decimal` + `encode_decimal`, rebuilds the affected tables with v3 constraints, restores existing indexes, and only updates `user_version` after all data is copied successfully.

- [x] **Step 1: Add failing schema tests** for empty v3 creation, idempotent v3 initialization, v2 data containing exponent and redundant-zero spellings becoming canonical, long-fraction preservation, an invalid/non-finite v2 value rolling back with version 2 intact, and direct v3 inserts rejecting scientific notation/noncanonical text.

- [x] **Step 2: Run the focused schema tests** and verify the new v3/migration assertions fail against the current v1-only initializer.

- [x] **Step 3: Implement v3 schema checks** for all 19 top-level Decimal columns: require SQLite `TEXT`, allow NULL where currently allowed, enforce the canonical plain-decimal grammar, and retain each existing positive/nonnegative/range condition. Keep current non-Decimal indexes unchanged because Decimal TEXT has lexical rather than numeric ordering and there are no Decimal range queries.

- [x] **Step 4: Implement v2→v3 migration** with foreign keys disabled before `BEGIN IMMEDIATE`, transactional renaming/recreation/copying, per-column normalization, rollback on any conversion or constraint error, and restoration of the existing index definitions before setting `PRAGMA user_version = 3`.

- [x] **Step 5: Run the focused schema tests** and verify creation, migration, rollback, constraints, and version handling pass.

### Task 3: Route all persistence reads through Decimal decoding

**Files:**
- Modify: `predmarket/persistence/repositories.py`
- Modify: `predmarket/catalog/relations.py`
- Modify: `predmarket/persistence/integrity.py`
- Test: `tests/unit/persistence/test_writer.py`
- Test: `tests/unit/persistence/test_integrity.py`
- Test: existing catalog/relation persistence tests that assert Decimal reads

**Interfaces:**
- Catalog/relation row mappers use `decode_decimal` rather than direct `Decimal(row[column])` construction.
- Integrity checks use strict canonical parsing for v3 rows and retain exact Decimal range and risk-formula validation.
- Every write boundary continues to call `encode_decimal`; no Decimal field is converted to `float`.

- [x] **Step 1: Add failing read/write tests** for long fractional catalog values, legacy-compatible decoded values, canonical raw SQLite strings, and exact risk formula validation after round-trip.

- [x] **Step 2: Run the focused persistence tests** and verify the new expectations fail or expose the direct-constructor paths.

- [x] **Step 3: Replace direct Decimal construction** with the shared decoder in all repository and relation row mappers, while leaving fee JSON’s canonical string contract intact.

- [x] **Step 4: Update integrity handling** so a noncanonical v3 string reports `DECIMAL_INVALID`, while canonical long decimals and exact risk formulas remain valid.

- [x] **Step 5: Run the focused persistence and integrity tests** and verify all pass.

### Task 4: Update documentation and complete regression verification

**Files:**
- Modify: `docs/PROJECT-GUIDE.md`
- Modify: `docs/VERIFICATION.md`
- Test: `tests/unit/persistence/test_schema.py`
- Test: `tests/unit/persistence/test_integrity.py`
- Test: `tests/unit/persistence/test_writer.py`

**Interfaces:**
- Documentation states schema v3, v2→v3 migration behavior, canonical plain-decimal TEXT storage, long-fraction semantics, and the unchanged exact Decimal calculation contract.

- [x] **Step 1: Update documentation** to remove v1-only/no-migration claims and document why Decimal indexes are not added.

- [x] **Step 2: Run the narrow regression suite**:
  `UV_CACHE_DIR=/tmp/predmarket_uv_cache .venv/bin/python -m pytest -q tests/unit/domain/test_decimal.py tests/unit/persistence/test_schema.py tests/unit/persistence/test_integrity.py tests/unit/persistence/test_writer.py`

- [x] **Step 3: Run the full suite** with `UV_CACHE_DIR=/tmp/predmarket_uv_cache .venv/bin/python -m pytest -q` and record the result before handoff.

- [x] **Step 4: Review `git diff --check` and `git status`** in the dedicated worktree; do not commit or push.
