# Security model

Predmarket is intentionally a read-only signal system.  It has no wallet or
authentication configuration and contains no order-placement, cancellation, or
signing path.  Public REST and WebSocket traffic is accessed only through
`PolymarketGateway`.

The service keeps evidence in a local SQLite database.  Normal writes are
serialized by `DatabaseWriter`; readers use the stored snapshot and do not
mutate external systems.  Prices, sizes, and business calculations use
`Decimal` or canonical strings.  If a source is stale, malformed, unavailable,
or mutually inconsistent, the affected relationship or signal is not trusted.

`system_events` preserves operational failures.  Terminal notifications report
important failures and recovery-related state, including queue overflow.  The
service degrades by dropping stale queued market changes, recording the event,
and requiring a fresh verified snapshot before it evaluates again.

## Local database reset

Reset is a deliberate operator action, not a runtime command:

```console
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

The first command is a dry run: it prints the absolute configured main SQLite
path and the only files an execution can remove.  Review that output, stop all
Predmarket processes, then use `--execute`.  The helper refuses a running
Predmarket process, directories, symlinks, filesystem root, the user home
directory, repository root, or a parent directory not owned by the invoking
user or writable by group or world.  It can remove only the configured main
file and its exact same-directory `-wal` and `-shm` siblings; it never accepts a
directory or wildcard target.

The reset plan records the parent directory device/inode and the initial
device/inode of each exact target.  Execution reopens the parent by descriptor
with `O_DIRECTORY | O_NOFOLLOW`, takes non-blocking exclusive advisory locks on
that directory and each opened regular target, rechecks the identities, and
unlinks only exact basenames through the verified parent descriptor.  This
prevents path drift, parent-directory replacement, symlinks, and changes made
before execution.

Darwin/POSIX has no supported unlink-by-file-descriptor operation that can
atomically bind the final unlink to a checked inode.  The procedure therefore
assumes no hostile same-UID process ignores the advisory locks and replaces an
exact target in the narrow interval between final verification and unlink.  It
is not safe for a shared or adversarial same-account directory; use an
owner-only database directory, stop Predmarket, and do not run another local
tool that mutates those three names during reset.
