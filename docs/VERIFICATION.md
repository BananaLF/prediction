# Verification

Run these commands from the repository root with Python 3.11 or newer:

```console
python -m pip install -e ".[test]"
pytest -q
python -m predmarket --help
predmarket --help
python -m compileall -q predmarket
git diff --check
```

The default configuration remains `config/default.yaml`, whose default SQLite
path is `data/predmarket-v1.sqlite3` (relative to the repository working
directory).  That file is not created by the configuration template.

The matrix lists both CLI help forms:

```console
python -m predmarket --help
predmarket --help
python -m predmarket status --config config/default.yaml
python -m predmarket run --config config/default.yaml
python -m predmarket signals list --config config/default.yaml
python -m predmarket relations list --config config/default.yaml
```

The four module-form service examples above are parser-only examples for the
documentation test; they are not runtime smoke commands.

The `predmarket` console entry point is available only after the editable
install.  In the current `tests/integration/test_documented_commands.py`,
`_documented_predmarket_commands()` collects only documented lines beginning
with `python -m predmarket`, so the parser loop covers the module-form
examples, including `python -m predmarket --help`.  Its subprocess check also
executes only `python -m predmarket --help`; it does not parse or execute
`predmarket --help`.  The console-form help command is therefore a manual
check after the editable install.  Both help commands are static checks and do
not need a database or network.

`status` opens SQLite read-only and therefore needs a configuration whose
`database.path` names an initialized temporary Schema v1 database:

```console
predmarket status --config /path/to/initialized-temporary-config.yaml
```

The repository `config/default.yaml` is a template and does not ship its
default database, so it is not a successful root-directory status smoke test.
For an initialized temporary database, verify all of the following before
using it for a status check:

| Check | Expected result |
| --- | --- |
| `PRAGMA user_version` | Schema v1 (`1`) |
| Project tables | Expected table count for Schema v1 (ten project tables) |
| `PRAGMA integrity_check` | `ok` |
| `PRAGMA foreign_key_check` | No rows |

Use a temporary path for database verification; do not reset or delete a user
database as part of these checks.  A live smoke test depends on external
network services and is optional: passing offline tests does not assert that a
live smoke will succeed, and a live smoke is not required for offline test
success.

The smallest repository-backed Schema v1 check uses pytest's temporary
database directory and does not touch `data/predmarket-v1.sqlite3`:

```console
pytest -q \
  tests/unit/persistence/test_schema.py::test_initialize_database_creates_exact_schema_v1_and_wal \
  tests/unit/persistence/test_integrity.py::test_integrity_accepts_a_valid_schema_v1_database
```

These tests initialize a temporary database through the production
`initialize_database()` path, then check the Schema v1 version, project-table
set, SQLite integrity, and read-only application integrity checks.  The
documentation-command test only parses documented local commands and does not
execute real database, network, or reset side effects.  Reset behavior is
exercised separately only against temporary files.
