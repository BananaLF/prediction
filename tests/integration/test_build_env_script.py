from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).parents[2]
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_env.sh"


def _make_executable(path: Path) -> None:
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _write_fake_tools(tmp_path: Path, fake_test_exit: int = 0) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    assert BUILD_SCRIPT.exists(), "scripts/build_env.sh has not been implemented"
    script = repo / "scripts" / "build_env.sh"
    shutil.copy2(BUILD_SCRIPT, script)
    _make_executable(script)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = tmp_path / "fake-python"
    fake_python.write_text(
        """#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${1:-}" == "-m" && "${2:-}" == "pytest" ]]; then
  exit "${FAKE_TEST_EXIT:-0}"
fi
if [[ "${1:-}" == "-m" && "${2:-}" == "predmarket" ]]; then
  exit 0
fi
exit 0
""",
        encoding="utf-8",
    )
    _make_executable(fake_python)

    fake_predmarket = tmp_path / "fake-predmarket"
    fake_predmarket.write_text(
        """#!/usr/bin/env bash
set -Eeuo pipefail
exit 0
""",
        encoding="utf-8",
    )
    _make_executable(fake_predmarket)

    fake_activate = tmp_path / "fake-activate"
    fake_activate.write_text(
        f"""VIRTUAL_ENV={shlex.quote(str(repo / '.venv'))}
export VIRTUAL_ENV
PATH="${{VIRTUAL_ENV}}/bin:${{PATH}}"
export PATH
""",
        encoding="utf-8",
    )

    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        f"""#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${{1:-}}" == "python" && "${{2:-}}" == "find" ]]; then
  printf '%s\\n' {shlex.quote(str(fake_python))}
  exit 0
fi
if [[ "${{1:-}}" == "sync" ]]; then
  mkdir -p .venv/bin
  cp {shlex.quote(str(fake_python))} .venv/bin/python
  cp {shlex.quote(str(fake_predmarket))} .venv/bin/predmarket
  cp {shlex.quote(str(fake_activate))} .venv/bin/activate
  exit 0
fi
exit 99
""",
        encoding="utf-8",
    )
    _make_executable(fake_uv)
    return repo, fake_bin


def _run_fake_build_script(tmp_path: Path, fake_test_exit: int = 0) -> subprocess.CompletedProcess[str]:
    repo, fake_bin = _write_fake_tools(tmp_path, fake_test_exit)
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["FAKE_TEST_EXIT"] = str(fake_test_exit)
    return subprocess.run(
        [str(repo / "scripts" / "build_env.sh")],
        cwd=repo,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_fake_build_script_from_source(
    tmp_path: Path, fake_test_exit: int = 0
) -> subprocess.CompletedProcess[str]:
    repo, fake_bin = _write_fake_tools(tmp_path)
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["FAKE_TEST_EXIT"] = str(fake_test_exit)
    command = (
        f"source {shlex.quote(str(repo / 'scripts' / 'build_env.sh'))}"
        " && test \"$VIRTUAL_ENV\" = \"$PWD/.venv\""
        " && echo VIRTUAL_ENV_ACTIVE"
    )
    return subprocess.run(
        ["bash", "-c", command],
        cwd=repo,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_fake_build_script_from_outside_repo(
    tmp_path: Path, fake_test_exit: int = 0
) -> subprocess.CompletedProcess[str]:
    repo, fake_bin = _write_fake_tools(tmp_path, fake_test_exit)
    environment = os.environ.copy()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["FAKE_TEST_EXIT"] = str(fake_test_exit)
    command = (
        "set +e; "
        f"source {shlex.quote(str(repo / 'scripts' / 'build_env.sh'))}; "
        "source_rc=$?; "
        "printf 'CALLER_CONTINUED\\n'; "
        "printf 'CALLER_FLAGS=%s\\n' \"$-\"; "
        "printf 'CALLER_PWD=%s\\n' \"$PWD\"; "
        "printf 'CALLER_VIRTUAL_ENV=%s\\n' \"${VIRTUAL_ENV:-}\"; "
        "exit \"$source_rc\""
    )
    return subprocess.run(
        ["bash", "-c", command],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


def _write_pipless_venv(repo: Path) -> None:
    venv_bin = repo / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    fake_python = venv_bin / "python"
    fake_python.write_text(
        """#!/usr/bin/env bash
set -Eeuo pipefail
if [[ "${1:-}" == "-c" ]]; then
  exit 0
fi
if [[ "${1:-}" == "-m" && "${2:-}" == "pip" ]]; then
  if [[ "${3:-}" == "--version" ]]; then
    [[ -f .venv/pip-ready ]]
    exit
  fi
  if [[ "${3:-}" == "install" ]]; then
    [[ -f .venv/pip-ready ]]
    exit
  fi
fi
if [[ "${1:-}" == "-m" && "${2:-}" == "ensurepip" ]]; then
  touch .venv/pip-ready
  exit 0
fi
if [[ "${1:-}" == "-m" && "${2:-}" == "pytest" ]]; then
  exit 0
fi
if [[ "${1:-}" == "-m" && "${2:-}" == "predmarket" ]]; then
  exit 0
fi
exit 0
""",
        encoding="utf-8",
    )
    _make_executable(fake_python)
    fake_predmarket = venv_bin / "predmarket"
    fake_predmarket.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    _make_executable(fake_predmarket)


def test_build_script_direct_invocation_uses_venv_and_cli(tmp_path: Path) -> None:
    result = _run_fake_build_script(tmp_path)

    assert result.returncode == 0
    assert ".venv/bin/python -m pytest -q" in result.stdout
    assert ".venv/bin/predmarket --help" in result.stdout


def test_build_script_source_invocation_activates_current_shell(tmp_path: Path) -> None:
    result = _run_fake_build_script_from_source(tmp_path)

    assert result.returncode == 0
    assert result.stdout.strip().endswith("VIRTUAL_ENV_ACTIVE")


def test_build_script_source_preserves_caller_shell_and_directory(tmp_path: Path) -> None:
    result = _run_fake_build_script_from_outside_repo(tmp_path)

    assert result.returncode == 0
    assert "CALLER_CONTINUED" in result.stdout
    assert "CALLER_FLAGS=" in result.stdout
    caller_flags = next(
        line for line in result.stdout.splitlines() if line.startswith("CALLER_FLAGS=")
    ).split("=", 1)[1]
    assert "e" not in caller_flags
    assert f"CALLER_PWD={tmp_path}" in result.stdout
    assert f"CALLER_VIRTUAL_ENV={tmp_path / 'repo' / '.venv'}" in result.stdout


def test_build_script_source_failure_returns_to_caller(tmp_path: Path) -> None:
    result = _run_fake_build_script_from_outside_repo(tmp_path, fake_test_exit=17)

    assert result.returncode == 17
    assert "CALLER_CONTINUED" in result.stdout


def test_build_script_propagates_test_failure(tmp_path: Path) -> None:
    result = _run_fake_build_script(tmp_path, fake_test_exit=17)

    assert result.returncode == 17


def test_build_script_bootstraps_pip_for_existing_venv(tmp_path: Path) -> None:
    repo, _ = _write_fake_tools(tmp_path)
    _write_pipless_venv(repo)
    environment = os.environ.copy()
    environment["PATH"] = "/usr/bin:/bin"

    result = subprocess.run(
        [str(repo / "scripts" / "build_env.sh")],
        cwd=repo,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert (repo / ".venv" / "pip-ready").exists()
