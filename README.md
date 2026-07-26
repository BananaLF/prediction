# Prediction-market structural-arbitrage scanner

This is a read-only research scanner. It reads public Polymarket catalog,
order-book, fee, and WebSocket data; it cannot authenticate, sign, hold a
wallet, place an order, or execute a trade.

The default research budget is `1000`, and the minimum modeled return is
`"0.0075"`: **0.75% = 0.0075**, not 0.75. All financial inputs and evidence
remain exact decimal strings.

## Quick start

```console
python -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/predmarket --help
.venv/bin/predmarket sync-markets --limit 100 --max-pages 2
.venv/bin/predmarket scan-once --limit 100
.venv/bin/predmarket --json report --limit 100
```

`sync-markets` stores a versioned, content-hashed catalog snapshot in SQLite:
normalized markets/events/tokens, lifecycle flags, fee provenance, diagnostics,
and explicitly unaudited relation candidates. Repeated identical snapshots are
idempotent; changed lifecycle data appends a new snapshot. Transactional
current-state tables point to the last snapshot that saw each stable ID;
markets missing from a newer snapshot become `MISSING`, inactive, and
non-tradeable without altering historical snapshots. Missing-state transitions
are allowed only after a complete, pagination-exhausted sync; bounded or partial
observations refresh seen entries without deactivating unseen ones. Every sync
has its own observation/run even when its immutable content hash is unchanged.
Current rows track `last_seen_at_ms` separately from their state-update
timestamp/sequence, so a delayed older SEEN cannot revive a newer MISSING row
and a delayed older MISSING cannot hide a newer SEEN row.
When a caller intentionally reaches its page or market bound, the returned
catalog is explicitly marked incomplete with its continuation cursor and
truncation reason; scan/watch may process that deterministic prefix, but it can
never deactivate unseen current records.

A deterministic targeted confirmation can be requested when the exact
condition and token IDs are already known:

```console
.venv/bin/predmarket scan-once \
  --condition CONDITION_ID --yes-token YES_TOKEN_ID --no-token NO_TOKEN_ID
```

`watch` uses the public WebSocket only to discover a possible price movement.
Every callback then obtains two independent REST book snapshots and the
authoritative CLOB market fee schedule before assigning a formal status:

```console
.venv/bin/predmarket watch --max-connections 3 --max-events 500
```

`--max-events` is one cumulative budget of accepted market-domain events for
the whole command, including all reconnections. PONG and invalid frames do not
consume it. If a JSON batch exceeds the remaining budget, the prefix is
accepted and the remainder is counted as dropped. Once reached, the command
closes cleanly without opening another connection; `--max-connections` bounds
total connection attempts. Exhausting all attempts without one accepted event
is an operational failure (exit code 1), with metrics still persisted.
Long-running watch uses fixed memory: only the latest 100 result summaries and
the latest 1024 latency samples are retained, alongside cumulative
status/reason counts and streaming latency count/min/max/sum. Output marks when
the recent-result or latency sample was truncated.

Audited logical rules are managed without silently replacing an existing
ID/version:

```console
.venv/bin/predmarket relations validate rules/example-implication.yaml
.venv/bin/predmarket relations import my-reviewed-rule.yaml
.venv/bin/predmarket relations list
```

Imported rules are loaded by `scan-once` and `watch` using exact token IDs
(`--relation-id` resolves ambiguity). Only a strict audited binary complete-set
definition can enter the binary simulator. Logical, same-event, and NegRisk
rules are retained and reported as `RESEARCH_ONLY`; the program does not
silently reinterpret them as executable binary arbitrage.

## Architecture and evidence

The pipeline is:

1. Gamma discovers exact binary YES/NO markets.
2. One CLOB REST client finds a cheap complete set.
3. An independent CLOB REST client confirms full executable depth.
4. Market-info binds the authoritative fee curve to both legs.
5. Simulation walks depth using exact decimals; latency, partial-fill, rule,
   conversion, settlement, bankroll, and return gates then classify the result.
6. SQLite stores canonical evidence before a notification can be claimed.

`REJECTED` means a required gate failed. `RESEARCH_CANDIDATE` means the
mathematics may be interesting but unresolved risk prevents an executable
classification. `SNAPSHOT_EXECUTABLE` means all modeled gates passed for the
captured REST snapshots; it is not a promise that a real trade can be filled.

`replay OPPORTUNITY_ID` returns the latest opportunity by evaluation time and
insertion order; `replay --bundle-id BUNDLE_ID` selects exact evidence. Both
return immutable core evidence separately from the
notification audit. `report` produces bounded status/reason/path counts,
executable economics, nearest-rank p50/p95/p99 latency, and delivery outcomes.
For empty latency samples all quantiles are `null`; for one sample all three
equal that sample.

With `--json`, stdout contains exactly one JSON document. Human-readable
notification audit lines are sent to stderr so machine consumers are never
given a mixed stream. Command input/configuration errors return 2 and
operational failures return 1.

## Important limitations

- The scanner does not execute both legs, so it cannot provide simultaneous
  fills.
- Prices can move between observation and any manual action.
- Partial fills, shallow unwind liquidity, fee changes, data latency, local
  queue overflow, and connection loss can erase an apparent edge.
- Logical rules require human semantic review and can still be wrong or become
  stale.
- Conversion, settlement interpretation, release timing, and platform behavior
  remain risks even where currently evidenced.
- Desktop delivery can fail or be uncertain; SQLite evidence remains the source
  of truth.
- No 24-hour soak test or seven-day observation is claimed by this repository.
- One composition root owns a single credential-free HTTP client shared by all
  public adapters and closes it once; no credential environment is trusted.

The program is a measurement and evidence tool, not financial advice and not a
guaranteed-profit system.
