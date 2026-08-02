# 构建环境与完整验证脚本 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个可重复执行的 Bash 入口，创建/复用 Python `>=3.11` 的 `.venv`，安装 `predmarket[test]`，运行完整测试并验证两个 CLI 入口。

**Architecture:** `scripts/build_env.sh` 通过自身路径定位仓库根目录。优先调用 `uv python find '>=3.11'` 与 `uv sync --extra test --python <path>`；没有 `uv` 时选择本机 `python3.11` 及更高版本解释器，创建 venv 并用 venv 内 `pip` 做 editable install。安装后所有测试和 CLI 命令都显式使用 `.venv/bin`，只有在脚本被 `source` 时额外激活当前 shell。

**Tech Stack:** Bash、Python `venv`、`uv`、`pip`、pytest、现有 `predmarket` console entry。

## Global Constraints

- 项目 Python 版本要求保持为 `>=3.11`。
- 默认环境目录固定为仓库根目录 `.venv`。
- 安装必须包含项目 editable install 和测试依赖：`.[test]` 或 `uv sync --extra test`。
- 脚本不得删除已有 `.venv`、初始化/重置数据库或依赖系统 Python 执行测试。
- `source scripts/build_env.sh` 需要保留当前 shell 的 venv 激活状态；直接执行时使用 venv 内命令完成工作。
- 不新增第三方运行时依赖；测试仅使用标准库和现有 pytest 配置。

---

### Task 1: Add script behavior tests

**Files:**
- Create: `tests/integration/test_build_env_script.py`
- Test: `scripts/build_env.sh`

**Interfaces:**
- Consumes: `scripts/build_env.sh` as a subprocess from a temporary fixture repository.
- Produces: executable contract covering direct invocation, source activation, and test failure exit propagation.

- [x] **Step 1: Write the failing tests**

Add a Python integration test that copies the build script into a temporary fake repository and places a fake `uv` executable first on `PATH`. The fake `uv` must handle `python find` by printing a fake interpreter path, handle `sync` by creating `.venv/bin/python`, `.venv/bin/predmarket`, and `.venv/bin/activate`, and make the fake Python return success for `-m pytest` and `-m predmarket`. The test cases must be:

```python
def test_build_script_direct_invocation_uses_venv_and_cli(tmp_path):
    result = run_fake_build_script(tmp_path)
    assert result.returncode == 0
    assert ".venv/bin/python -m pytest -q" in result.stdout
    assert ".venv/bin/predmarket --help" in result.stdout


def test_build_script_source_invocation_activates_current_shell(tmp_path):
    result = run_fake_build_script_from_source(tmp_path)
    assert result.returncode == 0
    assert result.stdout.strip().endswith("VIRTUAL_ENV_ACTIVE")


def test_build_script_propagates_test_failure(tmp_path):
    result = run_fake_build_script(tmp_path, fake_test_exit=17)
    assert result.returncode == 17
```

The helpers must create only the fake repository and fake tools under `tmp_path`; they must not invoke the real package manager or mutate the checkout `.venv`.

- [x] **Step 2: Run tests to verify they fail**

Run: `pytest tests/integration/test_build_env_script.py -q`

Expected: FAIL because `scripts/build_env.sh` does not exist yet.

- [x] **Step 3: Commit**

Do not commit yet; repository working agreements require user authorization before commits. Keep the failing tests in the issue worktree for the next task.

### Task 2: Implement the build and verification script

**Files:**
- Create: `scripts/build_env.sh`

**Interfaces:**
- Consumes: optional `uv` on `PATH`, otherwise a local `python3.11` or newer executable.
- Produces: `.venv`, editable `predmarket[test]` installation, complete pytest run, and both CLI help checks.

- [x] **Step 1: Implement repository and invocation detection**

Use Bash strict mode and resolve the repository from the script location:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "${script_dir}/.." && pwd -P)"
venv_dir="${repo_root}/.venv"
venv_python="${venv_dir}/bin/python"
source_mode=0
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  source_mode=1
fi
```

Define `info()` for prefixed progress output and `fail()` for actionable errors. All commands must run from `repo_root` using a subshell or an explicit `cd` so callers can invoke the script from another directory.

- [x] **Step 2: Implement existing venv validation**

If `.venv` exists, require an executable `.venv/bin/python`. Query its version with:

```bash
"${venv_python}" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'
```

If that check fails, return a non-zero error explaining that the script will not delete or replace the existing environment and that the user must recreate it manually. If `.venv` is absent, continue to interpreter selection.

- [x] **Step 3: Implement the uv installation path**

When `command -v uv` succeeds and `.venv` is absent, select an interpreter with `uv python find '>=3.11'`, fail clearly if none is available, then run:

```bash
uv sync --extra test --python "${selected_python}"
```

When `.venv` already passed version validation, use the same `uv sync` command with `.venv/bin/python` as the selected interpreter so dependencies are synchronized into the existing environment. After sync, require both `.venv/bin/python` and `.venv/bin/predmarket` to exist.

- [x] **Step 4: Implement the pip fallback path**

When `uv` is unavailable and `.venv` is absent, inspect `python3`, `python3.11`, `python3.12`, `python3.13`, and `python3.14` in that order, retaining only executable candidates whose `sys.version_info >= (3, 11)`. Use the first valid candidate to run:

```bash
"${selected_python}" -m venv "${venv_dir}"
"${venv_python}" -m pip install -e "${repo_root}/.[test]"
```

If no candidate qualifies, fail with the required version and installation guidance. If `.venv` already exists, skip creation and install with its validated Python. Require `.venv/bin/predmarket` after installation.

- [x] **Step 5: Implement activation and verification**

If `source_mode=1`, source `.venv/bin/activate` after installation. Regardless of invocation mode, run these commands from `repo_root` with explicit venv paths:

```bash
"${venv_python}" -m pytest -q
"${venv_python}" -m predmarket --help >/dev/null
"${venv_dir}/bin/predmarket" --help >/dev/null
```

Print success only after all commands return zero. Do not use `exec` for the final command so sourced invocation can return to the caller’s shell.

- [x] **Step 6: Run the focused tests to verify they pass**

Run: `pytest tests/integration/test_build_env_script.py -q`

Expected: all three script contract tests pass.

### Task 3: Document the supported entry point

**Files:**
- Modify: `README.md` in “环境要求与安装” and “快速开始”
- Modify: `docs/VERIFICATION.md` at the setup command block

**Interfaces:**
- Consumes: `scripts/build_env.sh` direct and sourced invocation forms.
- Produces: copyable setup commands and explicit fallback/activation semantics.

- [x] **Step 1: Add the recommended setup command**

Document:

```console
./scripts/build_env.sh
```

Explain that direct execution prepares and verifies the environment but cannot activate the parent shell, while:

```console
source scripts/build_env.sh
```

also leaves `.venv` active. State that `uv` is preferred and pip fallback requires a local Python `>=3.11`.

- [x] **Step 2: Replace duplicated manual verification instructions**

Keep the individual commands as troubleshooting/reference checks, but make the script the primary path. State that the script runs `pytest -q`, `python -m predmarket --help`, and `predmarket --help` inside `.venv` and does not touch the default database.

- [x] **Step 3: Run documentation and script checks**

Run:

```console
bash -n scripts/build_env.sh
pytest tests/integration/test_build_env_script.py tests/integration/test_documented_commands.py -q
```

Expected: Bash syntax succeeds and both focused test groups pass.

### Task 4: Full verification

**Files:**
- Verify: all changed files in this plan

- [x] **Step 1: Run the complete test suite with the prepared environment**

Run: `.venv/bin/python -m pytest -q`

Expected: all existing tests pass with no new failures.

- [x] **Step 2: Verify both entry points and source behavior manually**

Run:

```console
./scripts/build_env.sh
bash -c 'source scripts/build_env.sh >/tmp/predmarket-build-env.log && test "$VIRTUAL_ENV" = "$PWD/.venv"'
```

Expected: both commands return zero; the second command confirms the caller shell receives the venv activation.

- [x] **Step 3: Run repository hygiene checks**

Run:

```console
python -m compileall -q predmarket
git diff --check
git status --short
```

Expected: compileall and diff check return zero; status lists only files belonging to issue #9.
