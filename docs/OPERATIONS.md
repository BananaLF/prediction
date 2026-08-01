# Operations

## Run and observe

```console
python -m predmarket run --config config/default.yaml
python -m predmarket status --config config/default.yaml
python -m predmarket signals list --config config/default.yaml
```

`run` is a read-only Polymarket client: it uses `PolymarketGateway` for public
data and never issues wallet, auth, signing, or order actions.  Its local SQLite
evidence writes are serialized by `DatabaseWriter`.

Watch terminal output and `system_events` for failures.  A full market-change
queue drops stale work, records an overflow event, and requires a fresh snapshot
before the market can produce another signal.  After a WebSocket interruption,
the service invalidates the old generation, refreshes through REST, and only
then resumes book-based evaluation.  This is deliberately fail closed.

## Controlled SQLite reset

Only use this procedure when the collector is stopped and the local evidence
database must be discarded.  It is not part of the runtime CLI.

```console
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

The dry run prints the resolved absolute main database path and its three exact
potential targets: the main file, `main-file-wal`, and `main-file-shm`.  Confirm
the printed path before executing.  The helper refuses if it finds a running
Predmarket process, or if the configured main path is a directory, symlink,
filesystem root, user home directory, repository root, or a parent directory
not owned by the invoking user or writable by group or world.  It deletes no
other file and cannot accept a wildcard or directory target.

`--execute` is executable.  It fixes and rechecks the parent directory by
device/inode and `O_DIRECTORY | O_NOFOLLOW`, takes non-blocking exclusive
advisory locks on that directory and every opened regular target, compares each
target's planned device/inode before it begins any deletion, then deletes only
verified exact basenames using the parent descriptor.  A target that changed
since planning, or is symlinked or non-regular, causes refusal before deletion.
The three sibling deletions are not a filesystem transaction: if an unlink
fails after an earlier deletion, the helper exits with `reset partially
completed`, listing the deleted paths and the failed path.

Darwin/POSIX does not expose an unlink primitive bound atomically to an opened
inode.  The locks serialize cooperating operators, and the owner-only directory
requirement excludes other users, but an adversarial same-UID process that
ignores advisory locks can still replace a basename after final verification.
Do not use this operator procedure in that threat model; see `SECURITY.md` for
the full boundary.  The helper also requires POSIX `O_DIRECTORY`, `O_NOFOLLOW`,
and `fcntl.flock`; platforms without them refuse before changing files.  Never
substitute shell deletion.

## Signal semantics

Signals are observations, not orders.  A `CLOSED` signal means that its
opportunity disappeared or could no longer be verified; it does not report a
fill, settlement, or realized profit.  An approved relationship and complete,
supported NegRisk metadata are both required before the relevant opportunity can
be evaluated.
