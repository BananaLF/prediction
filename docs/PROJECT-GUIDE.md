# Project guide

## Architecture and safety

`predmarket` is a local signal collector.  The CLI starts the application,
`PolymarketGateway` owns all public Polymarket REST/WebSocket access, and the
pipeline normalizes markets, relationships, order books, and signals.  SQLite
is local evidence storage: `DatabaseWriter` serializes service writes and read
commands inspect stored facts.  No component authenticates, holds a wallet,
signs, or sends orders.

WebSocket recovery invalidates the affected book generation, obtains a fresh
REST snapshot through the gateway, then resumes subscriptions.  Signals are
evaluated only after complete current evidence is available.  If the bounded
market-change queue overflows, stale queued changes are dropped, an operational
event and terminal notification are emitted, and fresh data is required before
the affected market is usable again.

Failures are fail closed: unavailable APIs, stale books, malformed metadata,
incomplete syncs, contradictory relationship membership, and unsupported
NegRisk conversion metadata suppress evaluation rather than produce a number.

## Schema v1

The SQLite `user_version` is `1`.  Apart from SQLite internal tables, the
project schema has exactly these ten tables:

| Table | Fields |
| --- | --- |
| `events` | `id`, `slug`, `title`, `description`, `status`, `neg_risk`, `neg_risk_id`, `neg_risk_type`, `neg_risk_complete`, `neg_risk_conversion_supported`, `neg_risk_metadata_json`, `neg_risk_synced_at`, `market_ids_json`, `sync_generation`, `sync_generation_complete`, `start_at`, `end_at`, `resolved_at`, `source_updated_at`, `created_at`, `updated_at` |
| `markets` | `id`, `event_id`, `condition_id`, `slug`, `question`, `description`, `status`, `active`, `accepting_orders`, `enable_orderbook`, `neg_risk`, `neg_risk_outcome_position`, `neg_risk_member_complete`, `sync_generation`, `sync_generation_complete`, `tick_size`, `minimum_order_size`, `end_at`, `resolved_at`, `source_updated_at`, `created_at`, `updated_at` |
| `tokens` | `id`, `market_id`, `outcome`, `position`, `fee_schedule_json`, `fee_updated_at`, `sync_generation`, `sync_generation_complete`, `created_at`, `updated_at` |
| `relations` | `id`, `market_a_id`, `market_b_id`, `status`, `discovery_source`, `llm_confidence`, `llm_analysis_json`, `created_at`, `updated_at` |
| `arbitrage_signals` | `id`, `opportunity_key`, `strategy_type`, `market_ids_json`, `relation_id`, `execution_mode`, `status`, `opened_at`, `updated_at`, `closed_at`, `close_reason`, `latest_revision` |
| `signal_revisions` | `signal_id`, `revision`, `event_type`, `observed_at`, `quantity`, `total_capital`, `expected_profit`, `return_rate`, `worst_case_loss`, `risk_rate`, `unhedged_notional`, `risk_flags_json`, `calculation_json`, `closure_context_json` |
| `signal_legs` | `signal_id`, `revision`, `position`, `market_id`, `token_id`, `action`, `side`, `quantity`, `average_price`, `worst_price`, `gross_amount`, `fee_amount` |
| `orderbook_snapshots` | `id`, `signal_id`, `revision`, `market_id`, `token_id`, `subscription_generation`, `book_hash`, `exchange_timestamp`, `received_timestamp`, `tick_size`, `minimum_order_size` |
| `orderbook_levels` | `snapshot_id`, `side`, `position`, `price`, `size` |
| `system_events` | `id`, `component`, `severity`, `event_type`, `message`, `details_json`, `occurred_at` |

`CLOSED` is an opportunity lifecycle state, not a trade or settlement record.
The close reason and evidence remain stored; a later validated opportunity
opens a distinct signal.
