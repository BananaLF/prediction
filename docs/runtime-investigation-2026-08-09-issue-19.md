# Issue 19 Runtime Investigation — 2026-08-09

## Command

```sh
env -u ALL_PROXY -u HTTP_PROXY -u HTTPS_PROXY \
  -u all_proxy -u http_proxy -u https_proxy \
  PYTHONUNBUFFERED=1 \
  .venv/bin/predmarket run --config config/default.yaml --log-level DEBUG
```

The process was observed for approximately 27 seconds and then stopped with
`Ctrl-C` after the initial catalog scan was still in progress.

## Observations

- Runtime construction and startup completed successfully.
- The persisted catalog initially contained `0` events, `0` markets, and `0`
  tokens, so watch bootstrap subscribed to `0` markets and `0` tokens.
- The first sync reached at least `150` event pages and `13,247` active events
  while the observation was stopped.
- No `watch_evaluation_aborted` or `watch_evaluation_abort_summary` records
  appeared during this run because no watchable markets had been loaded and no
  evaluation batch started.
- Cancellation completed cleanly with `runtime_stopping reason=cancelled` and
  `runtime_stopped`.

## Interpretation

This run verifies process startup, catalog-sync entry, and graceful shutdown,
but it does not provide production abort-rate evidence. The new classification
and aggregation behavior is therefore covered by the deterministic watch unit
tests; a follow-up observation with a populated watchable catalog is still
needed to measure the live `watch_evaluation_aborted` rate.
