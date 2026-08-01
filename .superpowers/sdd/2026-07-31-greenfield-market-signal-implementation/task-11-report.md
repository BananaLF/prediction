# Task 11 report: documentation and safe reset

## RED / GREEN

- **RED:** `tests/integration/test_documented_commands.py` was added before the
  documentation and CLI change.  It requires documented local commands to
  parse without contacting Polymarket or mutating SQLite, and requires reset
  tests to use only pytest temporary paths.
- **GREEN:** the documented-command test now passes (`6 passed`).  It accepts
  the documented `COMMAND --config PATH` order, verifies real `--help` exit
  behavior, and verifies the reset helper's exact-target, directory-refusal,
  and running-process refusal behavior.
- **RED (review remediation):** a temporary-directory test replaced the
  already-planned reset parent with a symlink to a neighbouring directory. The
  original path-based `Path.unlink()` followed that replacement and did not
  refuse; its neighbour targets were therefore deletable. A second test showed
  that `/venv/bin/predmarket run ...` was not detected as an active process.
- **GREEN (review remediation):** the parent-replacement test now refuses
  before deletion and preserves all neighbour targets. Command parsing detects
  both a `predmarket` console-script basename and `python -m predmarket`, while
  ignoring ordinary filenames and pytest invocations. The documented-command
  suite passes with the two new tests (`8 passed`).

## Changes

- Replaced the maintained user documentation with the current greenfield CLI,
  read-only boundary, fail-closed behavior, WebSocket recovery, queue
  degradation, relationship approval, NegRisk eligibility, CLOSED semantics,
  schema v1, and verification instructions.
- Documented every field of the ten schema-v1 project tables in
  `docs/PROJECT-GUIDE.md`; deleted the obsolete quick cheat sheet and soak-test
  documents.
- Added `scripts/reset_database.py` and `predmarket.operator_reset`.  The dry
  run prints the resolved absolute configured main SQLite path and exact main,
  `-wal`, and `-shm` targets.  Execution rejects active Predmarket processes,
  symlinks, directories, filesystem root, home, and workspace root, and deletes
  only validated exact targets.
- Hardened reset execution against parent-directory TOCTOU. The reset plan
  binds the verified parent directory device/inode; execution opens it with
  `O_DIRECTORY | O_NOFOLLOW`, compares the opened descriptor, verifies each
  exact basename through that descriptor without following symlinks, then uses
  `os.unlink(..., dir_fd=...)`. It never deletes through a re-resolved target
  path.
- Extended active-process detection to parse argv and recognize both the
  `predmarket` console script (including `/venv/bin/predmarket`) and
  `python -m predmarket`.
- Clarified `README.md`: `relations list` and `relations show` are local
  read-only operations; `relations analyze` and `relations approve` perform
  controlled local relationship/event writes. Polymarket access remains
  read-only.
- Added CLI normalization so the documented `subcommand --config PATH` form is
  accepted by the actual parser.

## Verification results

| Command | Result |
| --- | --- |
| `pytest -q tests/integration/test_documented_commands.py` | Passed after remediation: `8 passed, 1 warning in 0.15s`. |
| `pytest -q tests/integration/test_documented_commands.py -k reset` | Passed: `6 passed, 2 deselected, 1 warning in 0.08s`. |
| `pytest -q` | Collection failed with 5 errors in `0.25s` because the optional `polymarket` package is absent: `ModuleNotFoundError: No module named 'polymarket'` in SDK, watch, catalog-sync, and gateway tests. No dependency boundary was changed. |
| `python -m predmarket --help` | Passed (exit 0); showed `run`, `status`, `signals`, and `relations`. |
| `python -m predmarket status --config config/default.yaml` | Failed: `sqlite3.OperationalError: unable to open database file`; `config/default.yaml` points to the absent `data/predmarket-v1.sqlite3`. No default user database was created for verification. |
| `python scripts/reset_database.py --config config/default.yaml` | Passed dry run; printed `/Users/lifei/workspace/earn_money_from_prediction/data/predmarket-v1.sqlite3` and only its exact `-wal` and `-shm` siblings. No file was removed. |
| `git diff --check` | Passed. |
| `python -m compileall -q predmarket` | Passed. |
| temporary SQLite schema check | Passed: exactly 10 project tables, `user_version=1`, `integrity_check=ok`, and no `foreign_key_check` rows. |

## Environment concerns

- The active Python environment is Python 3.14 and does not install the
  optional `polymarket` package, so tests importing the gateway cannot collect.
- The configured default database has not been initialized.  `status` is
  deliberately read-only and therefore does not create it.
- The sandbox denies process enumeration with `ps`; the reset helper fails
  closed when process inspection is unavailable.  Its file-deletion behavior
  was exercised only against pytest temporary files.
