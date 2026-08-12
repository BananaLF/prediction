# Gateway 单元测试代理环境隔离设计

## 背景与目标

Issue #35 记录了一个可重复的环境相关失败：宿主机设置 SOCKS5 代理、但未安装 `socksio` 时，`./scripts/build_env.sh` 在 5 个 gateway 单元测试中失败。测试直接构造固定版本 `polymarket-client==0.3.0b1` 的 `AsyncPublicClient`；其内部 `httpx.AsyncClient(trust_env=True)` 自动读取代理环境，并在测试进入生命周期断言前尝试初始化 SOCKS transport。

目标是让这些离线单元测试不依赖宿主机代理设置，同时继续使用真实 SDK 对象验证私有 lifecycle shape、registry 和 close/cancellation 行为。此次不改变应用运行时代理语义，也不扩展项目的 SOCKS 运行时支持契约。

## 已确认事实

- 当前失败：`5 failed, 790 passed, 2 skipped`，共同异常为缺少 `socksio`。
- 仅移除大小写两组 `ALL_PROXY`、`HTTP_PROXY`、`HTTPS_PROXY` 后，5 个测试通过。
- 最新 `origin/main` 在无代理对照环境下全量测试为 `795 passed, 2 skipped`。
- 项目依赖 `python-socks`，它不是 `httpx` SOCKS transport 所需的 `socksio`。
- 被测生命周期逻辑不发送网络请求；代理只在真实 SDK client 构造时被意外读取。

## 方案比较

### 方案 A：限定 gateway 测试模块的代理隔离 fixture（推荐）

在 `tests/unit/polymarket/test_gateway.py` 增加自动使用的 pytest fixture，仅在该模块每个测试执行期间删除大小写两组 `ALL_PROXY`、`HTTP_PROXY`、`HTTPS_PROXY`。pytest `monkeypatch` 在测试结束后恢复原环境。

优点：改动最小；所有当前及未来在该模块构造真实 SDK client 的测试都保持离线确定性；不修改生产代码、依赖或其他测试模块。代价是该模块不能顺带验证代理继承行为，但代理支持本来不属于 gateway 单元测试职责。

### 方案 B：只给当前 5 个测试显式注入 fixture

隔离范围最窄，但未来新增真实 SDK 构造测试时容易遗漏，再次产生相同环境故障；同时 5 个签名都需改变。可行但维护性弱于方案 A。

### 方案 C：安装 `httpx[socks]` / `socksio`

可以绕过当前 ImportError，但单元测试仍受宿主机代理影响，且无必要地扩大运行依赖。代理不可达或配置变化仍可能带来其他环境相关失败，因此不采用。

## 设计

### 测试隔离边界

新增模块级 `autouse=True` fixture，依赖 pytest 的 `monkeypatch`：

1. 在测试开始前删除 `ALL_PROXY`、`all_proxy`、`HTTP_PROXY`、`http_proxy`、`HTTPS_PROXY`、`https_proxy`，缺失变量不报错。
2. 不删除 `NO_PROXY` / `no_proxy`，因为它们本身不会选择代理 transport，且不需要扩大环境改写范围。
3. fixture 仅位于 gateway 单元测试模块，不放入全局 `tests/conftest.py`，避免改变集成测试或其他组件的环境契约。
4. 测试结束后由 `monkeypatch` 自动恢复调用者原有环境。

### 生产行为与依赖

- 不修改 `predmarket/polymarket/gateway.py`；应用构造真实 SDK 时仍按 SDK/httpx 默认行为读取 HTTP、HTTPS 或 SOCKS 代理。
- 不修改 `pyproject.toml` 和 `uv.lock`；本 Issue 不承诺 SOCKS 运行时支持。
- 若后续要求应用正式支持 SOCKS，应单独增加 `socksio` 运行依赖、代理连通性测试和运维文档。

## 错误处理

fixture 使用 `monkeypatch.delenv(name, raising=False)`，兼容变量不存在的环境。它不捕获 SDK 构造或 cleanup 异常，因此真实 lifecycle shape 漂移、registry 错误和关闭失败仍会使测试失败。

## 测试与验收

实施阶段按以下顺序验证：

1. 红灯复现：在显式设置 SOCKS5 代理且未安装 `socksio` 的环境中运行 5 个目标测试，确认现有代码报相同 ImportError。
2. 修改后在同一代理环境运行整个 `tests/unit/polymarket/test_gateway.py`，确认模块不再继承代理且 lifecycle 测试仍覆盖真实 SDK。
3. 在同一代理环境运行 `./scripts/build_env.sh`，确认环境同步、完整测试和两个 CLI 验证全部通过。
4. 检查 `git diff --check`，确认只改 gateway 测试与本设计/计划文档。

验收结果应明确记录测试数量、跳过数量和既有 warnings；不把 warnings 误报为本 Issue 回归。

## 非目标

- 不修改生产 gateway 或 SDK transport 创建方式。
- 不新增 SOCKS 依赖或宣称支持 SOCKS 生产代理。
- 不全局清理 pytest 进程的代理变量。
- 不处理 Python 3.14 / `pytest-asyncio` 的弃用警告。
