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

The `predmarket` console entry point is available only after the editable
install.  `python -m predmarket --help` verifies the module entry point; both
help commands are static CLI checks and do not need a database or network.

`status` opens SQLite read-only and therefore needs a configuration whose
`database.path` names an initialized temporary Schema v1 database:

```console
predmarket status --config /path/to/initialized-temporary-config.yaml
```

The repository `config/default.yaml` is a template and does not ship a
database, so it is not a successful root-directory status smoke test.  For an
initialized temporary database, verify all of the following before using it
for a status check:

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

The verification matrix has two CLI forms (`python -m predmarket` and
`predmarket`).  The documentation-command test reads the documented module-form
examples and parses their arguments with the CLI parser; it does not execute
real database, network, or reset side effects.  Reset behavior is exercised
separately only against temporary files.
