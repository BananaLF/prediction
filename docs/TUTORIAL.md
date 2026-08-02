# Tutorial

Run every command below from the repository root.

## 1. Install and inspect the CLI

Use Python `>=3.11`. The project pins `polymarket-client==0.3.0b1` for
read-only access to Polymarket's public data; `pip install -e .` installs that
dependency and the `predmarket` console entry from the current checkout. The
`python3` command below must resolve to Python 3.11 or newer:

```console
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
predmarket --help
```

## 2. Start the local observer

Start the long-running observer with the supplied configuration. It reads only
public Polymarket data and writes evidence, relations, signals, and operational
state to local SQLite:

```console
predmarket run --config config/default.yaml
```

`run` initializes the configured SQLite database before it starts public
Polymarket sync, order-book monitoring, strategy evaluation, and notifications.
It never authenticates, signs, or trades on Polymarket.

Leave this process running. Use `Ctrl-C` for an orderly stop; after it exits,
start the same command again to resume observing the existing local database.
The watcher invalidates old WebSocket generations and rebuilds them from REST
snapshots after disconnections, so it does not evaluate a mixture of old and new
books.

## 3. Query local evidence from another terminal

Open a second terminal in the repository root:

```console
predmarket status --config config/default.yaml
predmarket signals list --config config/default.yaml
predmarket relations list --config config/default.yaml
```

These commands query local SQLite only and do not access Polymarket. The database
must already have been initialized by `run`; otherwise the read-only query cannot
open its tables.

## 4. Inspect and approve a relation

Choose an actual ID from `relations list` and replace `RELATION_ID` below:

```console
predmarket relations show RELATION_ID --config config/default.yaml
predmarket relations approve RELATION_ID --config config/default.yaml
```

Approval is deliberately narrow. It accepts only a relation currently in
`LLM_APPROVE` whose semantic evidence is still valid; it writes the local
`APPROVED` state and a local activation event. It does not alter Polymarket or
turn incomplete NegRisk metadata into an evaluable relationship.

The CLI also exposes `relations analyze`, but do not treat it as a default LLM
feature. Standard `predmarket` has no analyzer provider. Setting `llm_enabled:
true` only enables the workflow when a caller programmatically injects a valid
analyzer; it neither creates nor configures one.

## 5. Recover, inspect events, and clean local evidence

For a WebSocket interruption or queue overflow, keep the service running and
watch its terminal notifications while it refreshes via REST. Operational events
are persisted locally; after stopping the service, inspect recent events with a
SQLite client, for example:

```console
sqlite3 data/predmarket-v1.sqlite3 'SELECT occurred_at, severity, event_type, message FROM system_events ORDER BY id DESC LIMIT 20;'
```

Use the database path from your configuration if it differs from the default.
For a clean restart that discards local evidence, first stop every `predmarket
run` process. Always preview the exact absolute targets before deleting anything:

```console
python scripts/reset_database.py --config config/default.yaml
python scripts/reset_database.py --config config/default.yaml --execute
```

The first command is a dry run. Read its resolved absolute path and exact main,
`-wal`, and `-shm` targets before using `--execute`.
