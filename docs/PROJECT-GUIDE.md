# Project guide

## Architecture and boundaries

`predmarket` is a local signal service that reads only public Polymarket market
and order-book data. It writes evidence, relations, signals, and operational
state to local SQLite, then reports verifiable opportunities. It does not
authenticate, hold a wallet, sign, send, cancel, or execute orders.

The runtime is assembled in `predmarket/app.py`:

- `App` (`Supervisor`) initializes the database, starts `DatabaseWriter`, then
  runs catalog sync and order-book watch tasks under one lifecycle.
- `Catalog` (`predmarket/catalog/sync.py`) obtains events, markets, tokens, fees,
  and NegRisk metadata through the gateway and commits complete sync generations.
- `Sync` emits bounded market changes to `Catalog`'s `MarketChangeQueue`.
- `Watch` (`predmarket/watch/task.py`) owns subscription generations, gets REST
  snapshots and WebSocket updates through the gateway, and only evaluates valid
  current order books.
- `Strategy` (`predmarket/strategy/engine.py`) evaluates pure contexts; `Signal`
  (`predmarket/signals/manager.py`) persists their open, update, or close evidence.
- `Relation` (`predmarket/catalog/relations.py`) discovers, analyzes when an
  injected analyzer is supplied, and manually approves implication relations.
- `Persistence` (`predmarket/persistence/`) supplies the Schema v3 repositories
  and serializes writes through `DatabaseWriter`; `Notifier`
  (`predmarket/notification/notifier.py`) sends signal and operational events
  to the optional macOS desktop channel. Runtime status is emitted through
  Python `logging`.

`predmarket/polymarket/gateway.py` is the only external-access boundary: it owns
all public Polymarket REST and WebSocket access. The other components depend on
that boundary and on local repositories, rather than accessing Polymarket
directly. `DatabaseWriter` serializes service writes. Invalid, incomplete, stale,
or contradictory evidence makes calculation fail closed: no usable signal is
produced.

`predmarket run` initializes the database and starts sync, order-book monitoring,
strategy evaluation, and notification runtime. `predmarket status`, `signals
list/show`, and `relations list/show` read local SQLite only; they do not contact
Polymarket. `relations approve` validates and writes local relation state, then
records a local activation event. `relations analyze` is an extension point: it
requires a programmatically injected analyzer. The normal console entry has no
provider, and `llm_enabled` is only a configuration switch, not proof that an
analyzer exists.

## Recovery and failure behavior

WebSocket recovery invalidates the affected subscription generation, obtains a
fresh REST snapshot via the gateway, and only then resumes book-based evaluation.
The bounded market-change queue can evict or drop stale work while preserving
critical control changes; an overflow records a `system_events` entry and logs
the degraded condition. The affected evidence must be refreshed before it can be
used again. Startup, sync, metadata, order-book, strategy, and persistence
invariant failures are all handled fail closed.

## Schema v3

The local SQLite database is **Schema v3**. Its initializer creates the schema
and sets `PRAGMA user_version = 3`. An existing Schema v2 database is migrated
to v3 transactionally; other versions are rejected. Schema v3 retains the v2
contract that `markets.event_id` may be `NULL`, so an orphan market can remain
valid until a later catalog sync connects it to an event.

Apart from SQLite internal tables, Schema v3 has exactly these ten tables:

| Table | Responsibility |
| --- | --- |
| `events` | Public event identity, lifecycle, NegRisk metadata, and sync-generation completeness. |
| `markets` | Market metadata, eligibility fields, and its sync-generation completeness. |
| `tokens` | Outcome tokens, positions, and fee-schedule evidence for markets. |
| `relations` | Discovered implication pairs, analysis evidence, confidence, and approval status. |
| `arbitrage_signals` | Stable opportunity identity and its current `OPEN` or `CLOSED` lifecycle state. |
| `signal_revisions` | Timestamped `OPENED`, `UPDATED`, and `CLOSED` calculation and risk evidence. |
| `signal_legs` | Per-revision market/token actions, quantities, prices, and fees. |
| `orderbook_snapshots` | Per-signal evidence snapshots, including subscription generation and book identity. |
| `orderbook_levels` | Bid and ask levels belonging to a persisted snapshot. |
| `system_events` | Auditable startup, sync, recovery, queue, notification, and relation-activation events. |

`CLOSED` is an opportunity lifecycle result, not an order fill, settlement, or
realized profit. A later independently valid opportunity receives a distinct
signal record.

### Decimal storage contract

Decimal-valued SQLite columns use `TEXT`, not IEEE-754 `REAL`. New writes use a
canonical plain-decimal string: no exponent notation, plus sign, leading or
trailing redundant zeroes, or negative zero. There is no fixed scale, so prices,
quantities, fees, and risk values retain arbitrarily long fractional parts in
Python's `Decimal` type. The v2-to-v3 migration normalizes legacy spellings before
the rows enter v3; reads accept legacy Decimal spellings at the compatibility
boundary and return `Decimal`. Fee-schedule parameters use the same canonical
strings inside JSON. Decimal columns are not used as numeric SQLite indexes,
because their `TEXT` ordering is lexical rather than numeric.

`markets.event_id` is nullable because an upstream market may be valid even
when its event response is absent. `events.market_ids` is a derived reverse
index rebuilt from locally stored markets; an event may therefore have an
empty list. A market without an event is not a database integrity violation.
During startup, an incomplete sync does not block `Watch` when the committed
database already contains at least one active, orderbook-enabled market with a
token. Sync remains a degraded background task until a complete generation is
available.
