# Operations

Run commands from the repository root. The daily interfaces are:

```console
predmarket run --config config/default.yaml
predmarket status --config config/default.yaml
predmarket signals list --config config/default.yaml
```

The default configuration is `config/default.yaml`; it stores local evidence in
`data/predmarket-v1.sqlite3`. `run` is the long-running observer: it reads only
public Polymarket data, writes local evidence, relations, signals, and
operational state, initializes Schema v1, watches order books, evaluates
strategies, and sends notifications. `status` and `signals list` are local
SQLite reads, so use them after `run` has initialized the database. They do not
make Polymarket requests.

Terminal output is the primary operational channel. With the default
configuration, terminal and macOS desktop notifications are enabled; important
conditions are also stored in `system_events`. There is no separate configured
application log file in the default configuration. Check a running process in
its terminal, and use `status` for the configured database, signal count, and
system-event count.

## Long-running recovery and shutdown

When a WebSocket connection or subscription becomes invalid, the watcher
invalidates its current generation, closes affected signal evidence as needed,
fetches a fresh REST snapshot, and starts a new generation before resuming
evaluation. Do not treat an old book as current while recovery is in progress.

The market-change queue has a configured bound (`runtime.market_change_queue_capacity`,
10,000 by default). On overflow, the service enters a degraded path: stale work
may be evicted or dropped, but critical control changes are preserved. It records
`MARKET_CHANGE_QUEUE_OVERFLOW` in `system_events` and emits a terminal
notification; wait for fresh evidence before relying on the affected market.

Inspect persisted operational events after stopping the service, or from another
local SQLite client:

```console
sqlite3 data/predmarket-v1.sqlite3 'SELECT occurred_at, severity, event_type, message FROM system_events ORDER BY id DESC LIMIT 20;'
```

For planned shutdown, send `Ctrl-C` to `predmarket run` and wait for it to exit.
The supervisor cancels and drains runtime tasks, closes the watcher and gateway,
and closes the database writer. Unexpected task exits, startup failures, stale
books, incomplete syncs, malformed metadata, and failed invariants are fail
closed: they suppress a usable signal rather than guess or trade.

## Controlled SQLite reset

Reset is an independent operator script, not `predmarket reset`. Stop every
running `predmarket` process first, then run a dry-run and inspect its absolute
path before an explicit deletion:

```console
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

The dry run prints the resolved absolute main database path and the only allowed
targets: that exact main file plus siblings with the same basename ending in
`-wal` and `-shm`. Confirm those paths match the intended database. Never use
`predmarket reset`, wildcards, recursive directory removal, or shell deletion.

The helper refuses unsafe paths and active Predmarket processes. During execute
it rechecks the validated parent and target identities and removes only verified
regular files with those exact names. If an error occurs after one target is
removed, it reports the partial completion; do not assume the three SQLite files
were deleted as one transaction.

## Service boundary

Polymarket access is read-only and confined to the public REST/WebSocket gateway.
The service has no wallet, credentials, signing, order submission, cancellation,
or execution capability. Signals and `CLOSED` records remain evidence about an
opportunity, not trades, fills, settlement, or realized returns.
