# Predmarket

Predmarket is a local, read-only market-signal service for Polymarket public
market data.  It discovers approved market relationships, maintains local
order-book evidence, and records auditable arbitrage signals.  It never holds
credentials, signs, places, or cancels orders.

## Commands

Run these commands from the repository root.  `--config` may appear after the
subcommand as shown.

```console
python -m predmarket --help
python -m predmarket run --config config/default.yaml
python -m predmarket status --config config/default.yaml
python -m predmarket signals list --config config/default.yaml
python -m predmarket relations list --config config/default.yaml
```

`run` is the long-running collector.  `status`, `signals`, and `relations list`
or `relations show` read the local SQLite database.  `relations analyze` and
`relations approve` are controlled local writes: they update relationship and
event records but never mutate Polymarket.  See [the project
guide](docs/PROJECT-GUIDE.md) for the schema and
[operations](docs/OPERATIONS.md) for safe lifecycle and reset procedures.

## Safety boundary

All Polymarket reads pass through `PolymarketGateway`; the runtime does not
make wallet, authentication, signing, trading, or direct HTTP/WebSocket calls.
Database writes are serialized through `DatabaseWriter`.  Business values are
stored and evaluated as `Decimal` values or strings, never binary floats.
Missing, expired, inconsistent, or unverifiable inputs suppress a signal rather
than guessing.
