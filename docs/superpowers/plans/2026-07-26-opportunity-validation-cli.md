# Opportunity Validation CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new read-only CLI command that validates one opportunity in SQLite by checking evidence-chain completeness and replay consistency, and returns machine-readable JSON only.

**Architecture:** Extend the existing argparse CLI with a dedicated `validate-opportunity` command, implement the validation logic in storage/command layers by reusing current replay primitives, and cover the new behavior with focused unit and integration tests. The command must stay read-only and deterministic, with failure modes expressed as JSON error codes rather than interactive output.

**Tech Stack:** Python 3.10/3.11, argparse CLI, asyncio, SQLite, pytest.

## Global Constraints

- Read-only command only; no network calls, no writes, no trading actions.
- CLI output must be machine-readable JSON only.
- Failure semantics must be encoded in JSON, not hidden in exit code alone.
- Compare semantic business fields, not raw byte-for-byte JSON.
- Reuse existing `replay`, `replay_opportunity`, and `replay_with_notification_audit` behavior where possible.

---

### Task 1: Define the CLI surface and JSON contract

**Files:**
- Modify: `predmarket/cli.py`
- Modify: `docs/superpowers/specs/2026-07-26-opportunity-validation-cli-design.md`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: existing `build_parser()` patterns, `dispatch(args)` entrypoint, current `--json` handling.
- Produces: `validate-opportunity` parser entry, top-level result schema for validation output.

- [ ] **Step 1: Write the failing test**

```python
import argparse

from predmarket.cli import build_parser


def test_validate_opportunity_command_exists():
    parser = build_parser()
    args = parser.parse_args(["validate-opportunity", "opp_123"])
    assert args.command == "validate-opportunity"
    assert args.opportunity_id == "opp_123"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_cli.py::test_validate_opportunity_command_exists -v`
Expected: parser error because the command does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add a new subparser in `predmarket/cli.py`:

```python
validate_opportunity = commands.add_parser(
    "validate-opportunity", help="validate one opportunity in SQLite"
)
validate_opportunity.add_argument("opportunity_id")
```

Keep JSON output behavior unchanged: the command will later return a dict and be printed by the existing JSON serializer.

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/unit/test_cli.py::test_validate_opportunity_command_exists -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add predmarket/cli.py tests/unit/test_cli.py docs/superpowers/specs/2026-07-26-opportunity-validation-cli-design.md
git commit -m "feat: define opportunity validation cli"
```

---

### Task 2: Implement opportunity lookup and completeness checks in storage

**Files:**
- Modify: `predmarket/storage.py`
- Test: `tests/unit/test_storage.py`

**Interfaces:**
- Consumes: `OpportunityStore.replay_opportunity()`, `OpportunityStore.replay_with_notification_audit()`, existing SQLite schema accessors.
- Produces: a storage-level validation helper returning structured completeness results.

- [ ] **Step 1: Write the failing test**

```python
import pytest


@pytest.mark.asyncio
async def test_validate_opportunity_detects_missing_chain(tmp_path):
    # set up a store with an opportunity row but without the associated evidence chain
    ...
    result = await store.validate_opportunity("opp_123")
    assert result["status"] == "fail"
    assert result["errors"][0]["code"] == "INCOMPLETE_CHAIN"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_storage.py::test_validate_opportunity_detects_missing_chain -v`
Expected: `AttributeError` or `NameError` for missing `validate_opportunity`.

- [ ] **Step 3: Write minimal implementation**

Add a new async method on `OpportunityStore`:

```python
async def validate_opportunity(self, opportunity_id: str) -> dict[str, object]:
    ...
```

This method should:

1. resolve the opportunity row and associated bundle/run;
2. verify required chain members exist;
3. return a structured dict with `status`, `checks.completeness`, `evidence`, `errors`, and `selection`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/unit/test_storage.py::test_validate_opportunity_detects_missing_chain -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add predmarket/storage.py tests/unit/test_storage.py
git commit -m "feat: add opportunity completeness validation"
```

---

### Task 3: Add replay-consistency comparison on top of the completeness result

**Files:**
- Modify: `predmarket/storage.py`
- Test: `tests/unit/test_storage.py`

**Interfaces:**
- Consumes: `validate_opportunity()` completeness result, `replay_opportunity()`, `replay_with_notification_audit()`.
- Produces: `checks.consistency` fields, mismatch/error classification, normalized comparison behavior.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.asyncio
async def test_validate_opportunity_detects_replay_mismatch(tmp_path):
    # seed a bundle, then mutate a relevant stored field to force a mismatch
    result = await store.validate_opportunity("opp_123")
    assert result["checks"]["consistency"]["status"] == "fail"
    assert result["errors"][0]["code"] == "REPLAY_MISMATCH"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_storage.py::test_validate_opportunity_detects_replay_mismatch -v`
Expected: failure because consistency comparison is not implemented yet.

- [ ] **Step 3: Write minimal implementation**

Inside `validate_opportunity()`, compare normalized semantic fields between stored facts and replayed bundle:

```python
def _normalize_validation_view(value: object) -> object:
    ...
```

Use this normalization to compare:

- bundle ID
- opportunity ID
- run ID
- legs
- actions
- risk assessment
- latency metrics
- notification audit summary

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/unit/test_storage.py::test_validate_opportunity_detects_replay_mismatch -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add predmarket/storage.py tests/unit/test_storage.py
git commit -m "feat: compare opportunity replay consistency"
```

---

### Task 4: Wire the new CLI command to the storage validation result

**Files:**
- Modify: `predmarket/commands.py`
- Modify: `predmarket/cli.py`
- Test: `tests/integration/test_cli.py`

**Interfaces:**
- Consumes: `OpportunityStore.validate_opportunity()`, current `dispatch(args)` command routing, current JSON serialization path.
- Produces: end-to-end CLI JSON output for `validate-opportunity`.

- [ ] **Step 1: Write the failing test**

```python
import json


@pytest.mark.asyncio
async def test_validate_opportunity_cli_returns_json(tmp_path):
    result = await run_cli(["--json", "validate-opportunity", "opp_123"])
    payload = json.loads(result.stdout)
    assert payload["opportunity_id"] == "opp_123"
    assert "checks" in payload
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/integration/test_cli.py::test_validate_opportunity_cli_returns_json -v`
Expected: command not found / missing dispatch branch.

- [ ] **Step 3: Write minimal implementation**

Add a new `if args.command == "validate-opportunity"` branch in `predmarket/commands.py` that calls the new storage method and returns its dict unchanged.

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/integration/test_cli.py::test_validate_opportunity_cli_returns_json -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add predmarket/commands.py predmarket/cli.py tests/integration/test_cli.py
git commit -m "feat: expose opportunity validation cli"
```

---

### Task 5: Cover error codes, ambiguity handling, and JSON-only output

**Files:**
- Modify: `predmarket/storage.py`
- Modify: `predmarket/cli.py`
- Test: `tests/unit/test_storage.py`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: the JSON schema from Task 1 and the validation method from Tasks 2–4.
- Produces: stable `NOT_FOUND`, `AMBIGUOUS_OPPORTUNITY`, `INCOMPLETE_CHAIN`, `REPLAY_MISMATCH`, `CORRUPTED_CANONICAL_JSON`, and `INVALID_INPUT` behaviors.

- [ ] **Step 1: Write the failing tests**

```python
def test_validate_opportunity_unknown_id_is_json_error():
    ...

def test_validate_opportunity_emits_json_only(capsys):
    ...
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
pytest tests/unit/test_cli.py::test_validate_opportunity_unknown_id_is_json_error -v
pytest tests/unit/test_cli.py::test_validate_opportunity_emits_json_only -v
```

- [ ] **Step 3: Write minimal implementation**

Ensure:

```python
{"status": "fail", "errors": [{"code": "NOT_FOUND", ...}]}
```

and keep the command path free of extra print statements.

- [ ] **Step 4: Run the tests to verify they pass**

Run the same pytest commands again.

- [ ] **Step 5: Commit**

```bash
git add predmarket/storage.py predmarket/cli.py tests/unit/test_storage.py tests/unit/test_cli.py
git commit -m "feat: harden opportunity validation errors"
```

---

### Task 6: Update docs and verification notes

**Files:**
- Modify: `docs/VERIFICATION.md`
- Modify: `docs/OPERATIONS.md`
- Modify: `README.md` if the new command is user-facing there

**Interfaces:**
- Consumes: final CLI behavior and JSON schema.
- Produces: operator-facing usage notes and verification commands.

- [ ] **Step 1: Write the documentation update**

Document:

- the new command syntax;
- JSON-only output;
- meaning of `pass`/`fail`;
- the error codes;
- how it differs from `replay`.

- [ ] **Step 2: Validate the docs against the code**

Manually confirm the documented command name and fields match the implementation.

- [ ] **Step 3: Commit**

```bash
git add docs/VERIFICATION.md docs/OPERATIONS.md README.md
git commit -m "docs: add opportunity validation command"
```

---

## Self-Review Checklist

- The spec’s “new CLI command” requirement is covered by Tasks 1 and 4.
- The “completeness check” requirement is covered by Task 2.
- The “replay consistency” requirement is covered by Task 3.
- The “JSON only” requirement is covered by Tasks 1 and 5.
- Failure classification is explicitly represented in Task 5.
- Documentation is covered by Task 6.
- No task requires a broad refactor or unrelated feature work.

