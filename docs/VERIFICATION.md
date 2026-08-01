# Verification

Run verification from the repository root with Python 3.11 or newer:

```console
pytest -q
python -m predmarket --help
git diff --check
python -m compileall -q predmarket
```

`status` opens SQLite in read-only mode.  It therefore requires a config whose
`database.path` already names an initialized temporary schema-v1 database, for
example:

```console
python -m predmarket status --config /path/to/initialized-temporary-config.yaml
```

The repository's `config/default.yaml` is only a template and does not ship a
database, so it is not a successful root-directory status smoke test.

To verify a newly initialized temporary database, check that SQLite reports
`user_version = 1`, exactly the ten project tables listed in
[PROJECT-GUIDE.md](PROJECT-GUIDE.md), `PRAGMA integrity_check = ok`, and
`PRAGMA foreign_key_check` returns no rows.  Use a temporary path for this
check; do not reset or delete a user database as part of verification.

The integration test `tests/integration/test_documented_commands.py` extracts
the `python -m predmarket` examples in the maintained documentation and ensures
they parse without network access or database mutation.  It also exercises the
reset guard only against temporary files.
