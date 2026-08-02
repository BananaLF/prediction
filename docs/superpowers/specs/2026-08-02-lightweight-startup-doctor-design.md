# Issue #8：轻量启动检查与 `predmarket doctor` 设计

状态：已确认

## 1. 目标与边界

Issue #8 要求将应用启动检查与完整的数据语义检查分离：启动阶段只执行快速、必需的数据库检查；完整检查通过只读的 `predmarket doctor` 主动运行。

本次范围包括：

- 启动时检查 schema/version、SQLite 结构完整性、foreign key 基础约束和必需表。
- 将现有五类语义检查接入 `doctor`：ID 数组、JSON payload、Decimal、latest revision、revision payload。
- `doctor` 输出分类结果、稳定错误码、受影响记录和稳定退出码。
- 增加 CLI、集成测试和运维文档。

本次不包括自动修复、迁移、数据删除、后台定时 doctor 或新增数据库约束。

## 2. 方案选择

采用“保留现有完整检查 API，新增轻量启动检查和 doctor 报告入口”的方案。

- 启动入口与完整检查入口职责清晰，避免慢扫描回到启动路径。
- 保留 `check_database_integrity()` 的现有行为，降低对已有调用方和测试的影响。
- doctor 复用现有检查逻辑，通过内部 finding collector 补充分类和受影响记录。

## 3. 组件设计

### 3.1 启动检查

在 `predmarket.persistence.integrity` 增加只读的轻量检查入口，例如 `check_database_startup(path)`，执行顺序为：

1. 读取并校验 `PRAGMA user_version`。
2. 执行 SQLite `PRAGMA integrity_check`。
3. 执行 `PRAGMA foreign_key_check`。
4. 校验项目必需表集合。

该入口不执行 `_check_id_arrays`、`_check_json_payloads`、`_check_decimals`、`_check_latest_revisions` 或 `_check_revision_payloads`。`Supervisor` 在初始化数据库后调用该入口。

数据库连接继续使用 SQLite 只读 URI；启动检查失败时沿用现有 fail-closed 行为。

### 3.2 Doctor 报告

增加 doctor 专用报告入口，使用同一个只读数据库连接执行完整语义检查。内部 finding 至少包含：

- `category`：`schema`、`id_arrays`、`json_payloads`、`decimals`、`revisions`。
- `code`：沿用现有稳定错误码，例如 `JSON_PAYLOAD_INVALID`。
- `severity`：当前现有检查全部为 `error`；保留 `warning` 字段以支持后续非阻断诊断。
- `records`：受影响的表、记录 ID，必要时包含字段名或 revision ID。

结构性问题没有具体记录时，`records` 为空或只包含受影响表名。相同 code 和相同记录的重复发现应合并，保证输出稳定且便于脚本消费。

### 3.3 CLI 契约

新增 `predmarket doctor` 命令，默认输出 JSON，不执行初始化或修复。建议稳定结构如下：

```json
{
  "database": "data/predmarket.db",
  "status": "ok",
  "summary": {"errors": 0, "warnings": 0},
  "categories": {
    "schema": {"errors": 0, "warnings": 0},
    "id_arrays": {"errors": 0, "warnings": 0},
    "json_payloads": {"errors": 0, "warnings": 0},
    "decimals": {"errors": 0, "warnings": 0},
    "revisions": {"errors": 0, "warnings": 0}
  },
  "findings": []
}
```

退出码固定为：

- `0`：数据库可检查且没有 errors/warnings。
- `1`：数据库可检查，但发现一个或多个 errors/warnings。
- `2`：参数非法，或数据库不存在、无法打开、无法完成检查。

无法检查数据库时仍输出 JSON 错误结果，便于运维脚本统一解析；argparse 的非法参数也返回 `2`。doctor 不调用 `initialize_database`，因此不会创建或修改数据库。

## 4. 数据流与错误处理

```text
predmarket run
  -> initialize_database
  -> check_database_startup
  -> 启动 Supervisor

predmarket doctor
  -> 只读打开数据库
  -> 结构检查 + 五类语义检查
  -> DoctorReport
  -> JSON stdout + exit code
```

启动检查继续抛出 `DatabaseIntegrityError`，由现有 Supervisor 启动失败路径处理。doctor 将数据库打开错误和检查异常转换为不可检查结果并返回 `2`；发现数据问题则返回报告和 `1`，不抛出未处理 traceback。

## 5. 测试计划

- 单元测试：轻量启动入口不会调用五类语义检查；结构性损坏仍能返回现有错误码。
- 单元测试：doctor 报告覆盖健康库、每类语义损坏、错误码、分类、受影响记录和 findings 合并。
- 集成测试：`predmarket doctor` 的 JSON 输出、三个退出码、缺失数据库和只读性。
- 回归测试：现有 `check_database_integrity()`、`run` 启动失败路径和其他 CLI 命令保持通过。
- 文档测试：命令帮助和运维文档明确区分启动失败与 doctor findings。

## 6. 验收标准映射

| Issue #8 要求 | 设计落点 |
| --- | --- |
| 启动检查轻量且 fail-closed | `check_database_startup()` 与 Supervisor 启动路径 |
| 五类检查迁移到 doctor | Doctor 报告入口复用五类现有检查 |
| 分类、错误码、受影响记录 | `category`、`code`、`severity`、`records` |
| 稳定退出码 | `0 / 1 / 2` |
| 默认只读、无自动修复 | SQLite readonly URI；不初始化、不迁移、不写库 |
| CLI、集成测试、运维文档 | 测试计划与文档更新 |

## 7. 待确认项

以上 JSON 字段、分类名称和退出码是本次实现拟遵循的稳定契约；确认后再进入实施计划和代码修改。
