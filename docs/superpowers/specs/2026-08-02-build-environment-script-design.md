# 构建环境与完整验证脚本设计

## 背景

项目要求 Python `>=3.11`，并且只有在可编辑安装项目及测试 extra 后，
`predmarket` console entry 和完整测试套件才可用。当前环境准备依赖 README 中的多条手工命令，
且系统 Python 可能低于项目要求。Issue #9 需要一个可重复执行的统一入口。

## 目标与边界

目标是新增一个从仓库根目录运行的 `scripts/build_env.sh`，完成以下动作：

1. 选择并验证 Python `>=3.11`。
2. 创建或复用项目根目录的 `.venv`。
3. 安装当前项目及测试依赖，确保 `predmarket` console entry 可用。
4. 使用 `.venv` 内的 Python 运行完整测试，并检查两个 CLI 入口。
5. 在 `source` 调用时保留当前 shell 的 venv 激活状态。

脚本不负责启动服务、初始化或重置数据库，也不删除已有 `.venv`。已有但版本不符合要求的
`.venv` 直接报错并给出人工处理提示。

## 方案

脚本使用 Bash，并通过脚本自身路径定位仓库根目录，因此不依赖调用者当前目录。所有命令都
使用明确的 `.venv/bin/python` 或 `.venv/bin/predmarket`，避免直接执行时子 shell 的 `source`
无法改变父 shell 环境。

当 `uv` 可用时，执行 `uv sync --extra test`，让现有 `uv.lock` 负责解析和安装锁定依赖及项目
editable install；`uv` 会选择满足项目约束的 Python。没有 `uv` 时，脚本在
`python3.11`、`python3.12` 等候选解释器中选择第一个满足 `>=3.11` 的命令，执行
`python -m venv .venv`，再执行 `.venv/bin/python -m pip install -e ".[test]"`。

两条路径随后执行相同的验证：

```console
.venv/bin/python -m pytest -q
.venv/bin/python -m predmarket --help
.venv/bin/predmarket --help
```

脚本启用 `set -Eeuo pipefail`，每个阶段输出带前缀的提示，并传播安装、测试和 CLI 验证的
非零退出码。`source scripts/build_env.sh` 时调用 `.venv/bin/activate`；直接执行时不承诺
改变父 shell，但仍使用 `.venv` 内命令完成全部工作。

## 文件与验证

- 新增 `scripts/build_env.sh`：环境选择、创建/复用、安装、激活和完整验证。
- 新增脚本级测试：至少验证 Bash 语法、脚本引用的关键命令/失败模式，以及当前仓库中的实际
  `source`/直接执行路径；不触碰用户数据库。
- 更新 `README.md` 与 `docs/VERIFICATION.md`：说明推荐入口、`source` 与直接执行的区别、
  Python 前置条件及 `uv` 缺失时的 pip fallback。

## 验收标准

- 干净工作区可创建 `.venv`，已有环境可重复执行。
- 安装完成后两个 `predmarket --help` 入口均成功。
- 完整测试通过，安装或测试失败能返回非零退出码。
- 系统默认 Python 为 3.9 时，`uv` 路径仍使用满足项目要求的 Python；无 `uv` 且无
  Python `>=3.11` 时给出明确错误。
- 直接执行和 `source` 执行都不依赖调用者事先激活 venv；`source` 执行结束后当前 shell 的
  `VIRTUAL_ENV` 和 `PATH` 指向该 `.venv`。
