# Verification

Run this command from the repository root to create/reuse a Python 3.11+ virtual
environment, install the editable project with test dependencies, and run the
complete offline verification:

```console
./scripts/build_env.sh
```

The script prefers `uv` and falls back to a local Python 3.11+ interpreter with
venv `pip`. To keep the environment activated in the current shell, use
`source scripts/build_env.sh`. Direct execution cannot activate its parent
shell. For troubleshooting, the equivalent checks inside an already prepared
environment are:

```console
.venv/bin/python -m pytest -q
.venv/bin/python -m predmarket --help
.venv/bin/predmarket --help
python -m compileall -q predmarket
git diff --check
```

The default configuration remains `config/default.yaml`, whose default SQLite
path is `data/predmarket-v1.sqlite3` (relative to the repository working
directory).  That file is not created by the configuration template.
The filename is retained for historical compatibility; an initialized database
uses SQLite Schema v4 (`PRAGMA user_version = 4`). Legacy databases are
migrated explicitly while the service is stopped.

The matrix lists both CLI help forms:

```console
python -m predmarket --help
predmarket --help
python -m predmarket status --config config/default.yaml
python -m predmarket run --config config/default.yaml
python -m predmarket doctor --config config/default.yaml
python -m predmarket signals list --config config/default.yaml
python -m predmarket relations list --config config/default.yaml
```

The four module-form service examples above are parser-only examples for the
documentation test; they are not runtime smoke commands.

For an initialized temporary database, `doctor` is the full read-only semantic
check. Its JSON output has stable categories and finding codes. Exit status `0`
means healthy, `1` means findings were reported, and `2` means the database
could not be checked. A normal `run` startup failure is handled separately: it
stops before the writer and runtime tasks start, and should be investigated as
a structural/schema problem. `doctor` does not initialize or repair a database.

The `predmarket` console entry point is available only after the editable
install.  In the current `tests/integration/test_documented_commands.py`,
`_documented_predmarket_commands()` collects documented commands in both
`python -m predmarket` module form and `predmarket` console form, and the
parser loop validates both forms without running them.  Its parameterized
subprocess check validates help for both `python -m predmarket --help` and
`predmarket --help`; if `shutil.which("predmarket")` cannot find the console
entry point, only the console-form help case is skipped.  Both help commands
are static checks and do not need a database or network.

`status` opens SQLite read-only and therefore needs a configuration whose
`database.path` names an initialized temporary Schema v4 database:

```console
predmarket status --config /path/to/initialized-temporary-config.yaml
```

The repository `config/default.yaml` is a template and does not ship its
default database, so it is not a successful root-directory status smoke test.
For an initialized temporary database, verify all of the following before
using it for a status check:

| Check | Expected result |
| --- | --- |
| `PRAGMA user_version` | Schema v4 (`4`) |
| Project objects | Expected Schema v4 tables plus `events`/`markets`/`tokens` views |
| `PRAGMA integrity_check` | `ok` |
| `PRAGMA foreign_key_check` | No rows |

Use a temporary path for database verification; do not reset or delete a user
database as part of these checks.  A live smoke test depends on external
network services and is optional: passing offline tests does not assert that a
live smoke will succeed, and a live smoke is not required for offline test
success.

The smallest repository-backed Schema v4 check uses pytest's temporary
database directory and does not touch `data/predmarket-v1.sqlite3`:

```console
pytest -q \
  tests/unit/persistence/test_schema.py::test_initialize_database_creates_exact_schema_v4_and_wal \
  tests/unit/persistence/test_integrity.py::test_integrity_accepts_a_valid_schema_v4_database
```

These tests initialize a temporary database through the production
`initialize_database()` path, then check the Schema v4 version, project-object
set, SQLite integrity, and read-only application integrity checks.  The
documentation-command test only parses documented local commands and does not
execute real database, network, or reset side effects.  Reset behavior is
exercised separately only against temporary files.

## Schema v4 发布后观测验证

升级目标数据库前先停止写入服务并确认备份，然后显式执行迁移、doctor 和只读观测：

```console
predmarket migrate --to 4 --database PATH
predmarket doctor --database PATH
python scripts/validate_catalog_v4.py --database PATH --duration-seconds 1800 --interval-seconds 30 --output reports/catalog-v4.json
```

观测脚本不自动停止服务、不迁移、不重置、不全量同步，也不触发交易。它记录 WAL 大小并以 128 MiB 作为硬阈值；报告中的估计不等于成交或实际收益，`realized` 明确为 `unsupported`。应保留报告、数据库和操作日志供复核。
