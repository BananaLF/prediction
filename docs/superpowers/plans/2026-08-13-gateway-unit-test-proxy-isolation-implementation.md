# Gateway Unit Test Proxy Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make gateway unit tests deterministic in SOCKS proxy environments without changing production proxy behavior or adding dependencies.

**Architecture:** Add one module-local, automatically used pytest fixture to remove the six standard HTTP/HTTPS/all-proxy environment variable spellings while `tests/unit/polymarket/test_gateway.py` runs. Use a regression assertion plus the five real-SDK lifecycle tests under an explicitly injected SOCKS proxy as the red/green boundary; pytest restores the caller environment after each test.

**Tech Stack:** Python 3.11+, pytest 8.x, pytest `monkeypatch`, `polymarket-client==0.3.0b1`, Bash build verification.

## Global Constraints

- Modify only `tests/unit/polymarket/test_gateway.py` plus the approved design and implementation-plan documents.
- Do not modify `predmarket/polymarket/gateway.py`, `pyproject.toml`, or `uv.lock`.
- Do not add `socksio` or claim SOCKS production proxy support.
- Remove only `ALL_PROXY`, `all_proxy`, `HTTP_PROXY`, `http_proxy`, `HTTPS_PROXY`, and `https_proxy`; preserve `NO_PROXY` and `no_proxy`.
- Keep real `polymarket-client==0.3.0b1` lifecycle shape, registry, and close/cancellation coverage.

---

## File Structure

- Modify: `tests/unit/polymarket/test_gateway.py` — owns gateway unit-test fixtures, SDK lifecycle tests, and the new proxy-isolation regression assertion.
- Existing: `docs/superpowers/specs/2026-08-13-gateway-unit-test-proxy-isolation-design.md` — approved behavior and scope; no implementation edits expected.
- Create: `docs/superpowers/plans/2026-08-13-gateway-unit-test-proxy-isolation-implementation.md` — this executable plan.

### Task 1: Isolate Gateway Unit Tests from Host Proxy Variables

**Files:**
- Modify: `tests/unit/polymarket/test_gateway.py` near module constants and existing fixtures around lines 25–445
- Test: `tests/unit/polymarket/test_gateway.py`

**Interfaces:**
- Consumes: pytest built-in `monkeypatch: pytest.MonkeyPatch` fixture and `os.environ`
- Produces: `_isolate_host_proxy_environment(monkeypatch: pytest.MonkeyPatch) -> None`, an `autouse=True` module-local fixture; `test_gateway_tests_do_not_inherit_host_proxy_environment() -> None`

- [ ] **Step 1: Reproduce the existing failure under an explicit SOCKS proxy**

Run the five affected tests before editing. The environment assignment makes the failure independent of the developer shell:

```bash
ALL_PROXY=socks5://127.0.0.1:7890 \
all_proxy=socks5://127.0.0.1:7890 \
HTTP_PROXY=socks5://127.0.0.1:7890 \
http_proxy=socks5://127.0.0.1:7890 \
HTTPS_PROXY=socks5://127.0.0.1:7890 \
https_proxy=socks5://127.0.0.1:7890 \
/Users/lifei/workspace/earn_money_from_prediction/.venv/bin/python -m pytest -q \
  tests/unit/polymarket/test_gateway.py::test_pinned_sdk_private_lifecycle_shape_is_exactly_supported \
  tests/unit/polymarket/test_gateway.py::test_recovery_replay_rejects_unexpected_real_sdk_handle_end \
  tests/unit/polymarket/test_gateway.py::test_recovery_overflow_detaches_rest_before_blocked_sdk_close \
  tests/unit/polymarket/test_gateway.py::test_cancelled_invalidation_close_cleans_real_sdk_registry \
  tests/unit/polymarket/test_gateway.py::test_cancelled_owned_close_task_cleans_real_sdk_registry
```

Expected: FAIL in `httpx.AsyncHTTPTransport` with `ImportError: Using SOCKS proxy, but the 'socksio' package is not installed.`

- [ ] **Step 2: Add a focused regression assertion before the fixture**

Import `os`, define the exact proxy-name tuple, and add a test near the existing fixtures:

```python
import os


HOST_PROXY_ENVIRONMENT_VARIABLES = (
    "ALL_PROXY",
    "all_proxy",
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
)


def test_gateway_tests_do_not_inherit_host_proxy_environment() -> None:
    assert all(
        variable not in os.environ
        for variable in HOST_PROXY_ENVIRONMENT_VARIABLES
    )
```

- [ ] **Step 3: Run the regression assertion under an explicit proxy and verify red**

```bash
ALL_PROXY=socks5://127.0.0.1:7890 \
all_proxy=socks5://127.0.0.1:7890 \
HTTP_PROXY=socks5://127.0.0.1:7890 \
http_proxy=socks5://127.0.0.1:7890 \
HTTPS_PROXY=socks5://127.0.0.1:7890 \
https_proxy=socks5://127.0.0.1:7890 \
/Users/lifei/workspace/earn_money_from_prediction/.venv/bin/python -m pytest -q \
  tests/unit/polymarket/test_gateway.py::test_gateway_tests_do_not_inherit_host_proxy_environment
```

Expected: FAIL because one or more named variables remain in `os.environ`.

- [ ] **Step 4: Add the minimal module-local isolation fixture**

Place the fixture immediately after `HOST_PROXY_ENVIRONMENT_VARIABLES` so its scope and intent are visible before test helpers:

```python
@pytest.fixture(autouse=True)
def _isolate_host_proxy_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for variable in HOST_PROXY_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(variable, raising=False)
```

Do not move it to `tests/conftest.py`, alter `NO_PROXY`, catch SDK errors, or add a transport abstraction.

- [ ] **Step 5: Verify the regression assertion and five real-SDK tests are green**

Run the Step 1 command with the new regression test appended to its node list.

Expected: `6 passed`; no `socksio` ImportError. The lifecycle shape test must still assert SDK version `0.3.0b1` and the existing exact shape dictionary.

- [ ] **Step 6: Verify the complete gateway unit-test module under the proxy environment**

```bash
ALL_PROXY=socks5://127.0.0.1:7890 \
all_proxy=socks5://127.0.0.1:7890 \
HTTP_PROXY=socks5://127.0.0.1:7890 \
http_proxy=socks5://127.0.0.1:7890 \
HTTPS_PROXY=socks5://127.0.0.1:7890 \
https_proxy=socks5://127.0.0.1:7890 \
/Users/lifei/workspace/earn_money_from_prediction/.venv/bin/python -m pytest -q \
  tests/unit/polymarket/test_gateway.py
```

Expected: all tests in the module pass. Existing Python 3.14 / `pytest-asyncio` deprecation warnings are allowed.

- [ ] **Step 7: Commit the focused test change**

```bash
git add -- tests/unit/polymarket/test_gateway.py
git diff --cached --check
git commit -m "test: isolate gateway SDK tests from host proxies"
```

Expected: one commit containing only `tests/unit/polymarket/test_gateway.py`.

### Task 2: Run End-to-End Build Verification

**Files:**
- Verify: `scripts/build_env.sh`
- Verify: `tests/unit/polymarket/test_gateway.py`
- Verify: `pyproject.toml`
- Verify: `uv.lock`

**Interfaces:**
- Consumes: the Task 1 autouse fixture and existing `scripts/build_env.sh` contract
- Produces: reproducible full-suite and CLI verification evidence for Issue #35 and the PR

- [ ] **Step 1: Run the exact user-facing build command with SOCKS proxy variables present**

```bash
ALL_PROXY=socks5://127.0.0.1:7890 \
all_proxy=socks5://127.0.0.1:7890 \
HTTP_PROXY=socks5://127.0.0.1:7890 \
http_proxy=socks5://127.0.0.1:7890 \
HTTPS_PROXY=socks5://127.0.0.1:7890 \
https_proxy=socks5://127.0.0.1:7890 \
./scripts/build_env.sh
```

Expected: `uv sync --extra test` succeeds; the full pytest suite passes; `.venv/bin/python -m predmarket --help` and `.venv/bin/predmarket --help` both succeed; script exits 0 with `构建环境与完整验证已完成。`

- [ ] **Step 2: Verify dependency and production-code scope did not change**

```bash
git diff origin/main...HEAD -- predmarket pyproject.toml uv.lock
```

Expected: no output.

- [ ] **Step 3: Run final repository checks**

```bash
git diff --check origin/main...HEAD
git status --short --branch
```

Expected: no whitespace errors; only the approved branch commits are present; worktree is clean after commits.

- [ ] **Step 4: Publish implementation evidence**

Record in Issue #35 and the PR:

- gateway module test count and result under explicit SOCKS variables;
- full `build_env.sh` test count, skipped count, warnings, and exit status;
- confirmation that production code and dependency files have no diff;
- explicit statement that SOCKS production proxy support remains outside this Issue.

No additional code commit is required for this evidence; GitHub updates occur after verification and PR creation under employee-mode authorization.
