#!/usr/bin/env bash

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "${script_dir}/.." && pwd -P)"
venv_dir="${repo_root}/.venv"
venv_python="${venv_dir}/bin/python"
source_mode=0
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  source_mode=1
fi

info() {
  printf '[build-env] %s\n' "$*"
}

fail() {
  printf '[build-env] ERROR: %s\n' "$*" >&2
  return 1
}

python_is_supported() {
  local python_executable="$1"
  "${python_executable}" -c \
    'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'
}

find_fallback_python() {
  local candidate candidate_path
  for candidate in python3 python3.11 python3.12 python3.13 python3.14; do
    if ! command -v "${candidate}" >/dev/null 2>&1; then
      continue
    fi
    candidate_path="$(command -v "${candidate}")"
    if python_is_supported "${candidate_path}"; then
      printf '%s\n' "${candidate_path}"
      return 0
    fi
  done
  return 1
}

validate_existing_venv() {
  if [[ ! -x "${venv_python}" ]]; then
    fail "已有 .venv 缺少可执行的 .venv/bin/python；脚本不会删除或替换它，请手动重建。"
    return 1
  fi
  if ! python_is_supported "${venv_python}"; then
    fail "已有 .venv 的 Python 版本低于 3.11 或不可用；脚本不会删除或替换它，请手动重建。"
    return 1
  fi
}

ensure_pip() {
  if "${venv_python}" -m pip --version >/dev/null 2>&1; then
    return 0
  fi
  info "已有 .venv 未提供 pip，使用 ensurepip 初始化。"
  "${venv_python}" -m ensurepip --upgrade
}

install_with_uv() {
  local selected_python
  if [[ -e "${venv_dir}" ]]; then
    validate_existing_venv
    selected_python="${venv_python}"
    info "复用已有 .venv，使用 uv 同步项目和测试依赖。"
  else
    if ! selected_python="$(uv python find '>=3.11')" || [[ -z "${selected_python}" ]]; then
      fail "uv 未找到 Python >=3.11；请安装 Python 3.11+，或使用 pip fallback。"
      return 1
    fi
    info "使用 uv 找到 Python: ${selected_python}"
  fi
  uv sync --extra test --python "${selected_python}"
}

install_with_pip() {
  local selected_python
  if [[ -e "${venv_dir}" ]]; then
    validate_existing_venv
  else
    if ! selected_python="$(find_fallback_python)"; then
      fail "未找到 Python >=3.11；请安装 Python 3.11+ 或安装 uv 后重试。"
      return 1
    fi
    info "使用 Python 创建 .venv: ${selected_python}"
    "${selected_python}" -m venv "${venv_dir}"
  fi
  info "使用 venv 内 pip 安装 predmarket[test]。"
  ensure_pip
  "${venv_python}" -m pip install -e ".[test]"
}

main() {
  cd -- "${repo_root}"

  if [[ -e "${venv_dir}" ]]; then
    info "检查已有 .venv 的 Python 版本。"
    validate_existing_venv
  fi

  if command -v uv >/dev/null 2>&1; then
    install_with_uv
  else
    info "未找到 uv，使用 pip fallback。"
    install_with_pip
  fi

  if [[ ! -x "${venv_python}" ]]; then
    fail "环境安装后未找到可执行的 .venv/bin/python。"
    return 1
  fi
  if [[ ! -x "${venv_dir}/bin/predmarket" ]]; then
    fail "环境安装后未找到 .venv/bin/predmarket。"
    return 1
  fi

  info "运行完整测试: .venv/bin/python -m pytest -q"
  "${venv_python}" -m pytest -q
  info "验证 Python CLI: .venv/bin/python -m predmarket --help"
  "${venv_python}" -m predmarket --help >/dev/null
  info "验证 console script: .venv/bin/predmarket --help"
  "${venv_dir}/bin/predmarket" --help >/dev/null
  info "构建环境与完整验证已完成。"
}

run() {
  if (( source_mode == 1 )); then
    local caller_errexit=0 main_status
    case "$-" in
      *e*)
        caller_errexit=1
        set +e
        ;;
    esac

    (
      set -Eeuo pipefail
      main "$@"
    )
    main_status=$?

    if (( caller_errexit == 1 )); then
      set -e
    fi
    if (( main_status != 0 )); then
      return "${main_status}"
    fi

    # shellcheck disable=SC1091
    source "${venv_dir}/bin/activate"
    local activation_status=$?
    if (( activation_status != 0 )); then
      return "${activation_status}"
    fi
    info "已激活当前 shell 的 .venv。"
    return 0
  fi

  set -Eeuo pipefail
  main "$@"
}

run "$@"
