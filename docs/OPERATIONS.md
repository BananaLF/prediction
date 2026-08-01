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
filesystem root, user home directory, or repository root.  It deletes no other
file and cannot accept a wildcard or directory target.

The current Python/POSIX implementation has no supported atomic unlink operation
that binds deletion to a previously verified file identity.  Consequently,
`--execute` fails closed with a refusal and removes nothing; do not replace it
with shell deletion.  The dry run remains available to identify the configured
path safely.  A deletion-capable reset requires a future platform-specific
identity-checked unlink primitive and matching regression tests.

## Signal semantics

Signals are observations, not orders.  A `CLOSED` signal means that its
opportunity disappeared or could no longer be verified; it does not report a
fill, settlement, or realized profit.  An approved relationship and complete,
supported NegRisk metadata are both required before the relevant opportunity can
be evaluated.
