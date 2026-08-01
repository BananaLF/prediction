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
- **RED (review remediation round 2):** verifying a target entry with `stat`
  and then deleting its name with `unlinkat` remains a TOCTOU: the entry can be
  replaced by another regular file in the same verified directory between those
  operations. The previous argv scan also treated `pytest -m predmarket` and
  `tool --mode -m predmarket` as running collectors.
- **GREEN (review remediation round 2):** Python/POSIX exposes no atomic
  unlink that binds deletion to verified `st_dev`/`st_ino`, so reset execution
  now refuses before target-entry inspection or deletion. A real temporary-path
  regression replaces the planned SQLite target and confirms it survives.
  Process parsing now accepts only `predmarket` as argv[0], or a Python
  interpreter whose immediate argv is `-m predmarket`.
- **RED (review remediation round 3):** permanently refusing `--execute`
  contradicted the documented operator procedure.  The Python argv parser also
  missed `python -u -m predmarket` and `python3.14 -X dev -m predmarket`.
- **GREEN (review remediation round 3):** `--execute` now deletes validated
  temporary main/WAL/SHM files end-to-end.  It binds operations to an opened,
  identity-checked parent directory, opens and verifies regular targets with
  `O_NOFOLLOW`, and uses non-blocking advisory locks for cooperating reset
  operators.  A target replaced after planning is refused before deletion.
  Python option parsing recognizes the supported pre-`-m` options while still
  rejecting pytest, another Python module, and non-Python commands.

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
  symlinks, directories, filesystem root, home, and workspace root.
- Hardened reset execution against parent-directory TOCTOU. The reset plan
  binds the verified parent directory device/inode; execution opens it with
  `O_DIRECTORY | O_NOFOLLOW` and compares the opened descriptor.
- Made `--execute` usable again under an explicit Darwin/POSIX threat model.
  It requires an owner-only parent directory, records target identities during
  planning, reopens and locks each exact regular target before deletion, and
  unlinks exact names only through the verified parent descriptor.  The narrow
  hostile-same-UID replacement race remains a documented platform boundary.
- Extended active-process detection to parse argv and recognize both the
  `predmarket` console script (including `/venv/bin/predmarket`) and a Python
  interpreter with supported Python options before `-m predmarket`.
- Clarified `README.md`: `relations list` and `relations show` are local
  read-only operations; `relations analyze` updates only a relationship, while
  `relations approve` updates it and records `RELATION_ACTIVATED`. Polymarket
  access remains read-only.
- Added CLI normalization so the documented `subcommand --config PATH` form is
  accepted by the actual parser.

## Verification results

| Command | Result |
| --- | --- |
| `pytest -q tests/integration/test_documented_commands.py` | Passed after round-3 remediation: `10 passed, 1 warning in 0.69s`. |
| `pytest -q tests/integration/test_documented_commands.py -k reset` | Passed: `8 passed, 2 deselected, 1 warning in 0.60s`. |
| `pytest -q` | Collection failed with 5 errors in `0.26s` because the optional `polymarket` package is absent: `ModuleNotFoundError: No module named 'polymarket'` in SDK, watch, catalog-sync, and gateway tests. No dependency boundary was changed. |
| `python -m predmarket --help` | Passed (exit 0); showed `run`, `status`, `signals`, and `relations`. |
| `python -m predmarket status --config config/default.yaml` | Failed: `sqlite3.OperationalError: unable to open database file`; `config/default.yaml` points to the absent `data/predmarket-v1.sqlite3`. No default user database was created for verification. |
| `python scripts/reset_database.py --config config/default.yaml` | Passed dry run; printed `/Users/lifei/workspace/earn_money_from_prediction/data/predmarket-v1.sqlite3` and only its exact `-wal` and `-shm` siblings. No file was removed. |
| `python scripts/reset_database.py --help` | Passed (exit 0); documents the executable `--execute` action and exact SQLite sibling scope. |
| `git diff --check` | Passed. |
| `python -m compileall -q predmarket` | Passed. |
| temporary SQLite schema check | Passed: exactly 10 project tables, `user_version=1`, `integrity_check=ok`, and no `foreign_key_check` rows. |

## Environment concerns

- The active Python environment is Python 3.14 and does not install the
  optional `polymarket` package, so tests importing the gateway cannot collect.
- The configured default database has not been initialized.  `status` is
  deliberately read-only and therefore does not create it.
- The sandbox denies process enumeration with `ps`; the reset helper fails
  closed when process inspection is unavailable. The end-to-end reset test uses
  a temporary test-only `ps` executable that reports no running process.
- Python/POSIX lacks an inode-bound unlink. The reset procedure is safe against
  path drift, symlinks, parent replacement, and cooperative concurrency, but
  not a hostile same-UID process that races an exact basename after final
  verification while disregarding advisory locks.
