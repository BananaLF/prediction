# Verification

Run verification from the repository root with Python 3.11 or newer:

```console
pytest -q
python -m predmarket --help
python -m predmarket status --config config/default.yaml
git diff --check
python -m compileall -q predmarket
```

To verify a newly initialized temporary database, check that SQLite reports
`user_version = 1`, exactly the ten project tables listed in
[PROJECT-GUIDE.md](PROJECT-GUIDE.md), `PRAGMA integrity_check = ok`, and
`PRAGMA foreign_key_check` returns no rows.  Use a temporary path for this
check; do not reset or delete a user database as part of verification.

The integration test `tests/integration/test_documented_commands.py` extracts
the `python -m predmarket` examples in the maintained documentation and ensures
they parse without network access or database mutation.  It also exercises the
reset guard only against temporary files.
