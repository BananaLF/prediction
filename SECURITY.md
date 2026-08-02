# Security model

Predmarket is read-only at the Polymarket public-interface boundary.  External
market data enters only through public REST and WebSocket access in
`PolymarketGateway`; it has no authentication, wallet, signing, order-placement,
cancellation, or on-chain write path.  It does write local SQLite: `DatabaseWriter`
serializes local snapshots/evidence, signals, relations, and operational state
such as `system_events`; readers consume stored snapshots and do not mutate
external systems.

Prices, amounts, and strategy calculations use `Decimal` (or canonical stored
decimal strings).  The system fails closed when required invariants, numeric
precision limits, catalog or subscription generation checks, fee freshness, or
order-book depth/identity/timestamp checks cannot be satisfied.  Stale,
malformed, unavailable, or inconsistent source data is not trusted for a
relationship or signal.

`system_events` records operational failures.  On queue overflow the service
may discard stale queued market changes, records that fact, and requires a
fresh verified snapshot before another evaluation.

## Local database reset

Reset is an operator-only helper, not a runtime service command:

```console
predmarket --help
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

With the repository `config/default.yaml`, the configured default database is
`data/predmarket-v1.sqlite3`, relative to the repository working directory.
Keep `config/default.yaml` as the configuration template; use a copied config
with a temporary database path for reset or verification examples.

The first reset command is a dry run: it prints the absolute configured SQLite
main path and the only files execution can remove.  After reviewing it, stop
Predmarket processes before using `--execute`.  Reset rejects a running
Predmarket process, directories, symlinks, filesystem root, the user home,
repository root, and a parent directory that is not owned by the invoking user
or is group/world writable.  It can delete only the configured main file and
its exact same-directory `-wal` and `-shm` basenames—never a directory or
wildcard.

The reset plan captures the parent device/inode and each target's device/inode.
On execution it reopens the parent with `O_DIRECTORY | O_NOFOLLOW`, obtains
non-blocking exclusive `flock` advisory locks on that directory and each opened
regular target, then performs a second inode/device comparison.  It unlinks
only the validated exact basenames through the verified parent descriptor.  A
failure after an earlier unlink is reported as `reset partially completed`,
including deleted paths and the failed path.

Darwin/POSIX provides no supported unlink-by-file-descriptor operation that
atomically binds the final unlink to the checked inode.  The advisory locks
coordinate cooperating processes only; they are not mandatory isolation from a
hostile process running under the same UID.  That process can ignore the locks
and replace a target in the interval after final validation and before unlink.
This residual threat is outside the reset guarantee: use an owner-only
directory, stop Predmarket, and do not run another local process that changes
these three names.  Platforms without POSIX `O_DIRECTORY`, `O_NOFOLLOW`, or
`fcntl.flock` refuse reset before deletion.
