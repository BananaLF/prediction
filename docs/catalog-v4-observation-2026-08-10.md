# Catalog v4 runtime observation

## Result

- Status: `healthy`
- Observation window: `1804s` (`2026-08-10T03:37:16Z` – `2026-08-10T04:07:21Z`)
- Samples: `58`
- Validator exit code: `0`
- Database: `data/predmarket-v1.sqlite3`

All validator samples passed the schema, SQLite integrity, foreign-key,
catalog generation, staging, journal, cleanup, migration marker, and WAL
checks. The active catalog generation was `COMMITTED`; the catalog journal and
reclaimable cleanup backlog were both zero at the final sample.

## Runtime evidence

- The initial sync completed with `complete=true`.
- Parent-event reconciliation completed with `errors=0`.
- The cleanup worker ran during the observation and repeatedly reduced the
  journal backlog to `remaining_journal_rows=0`.
- The watch stream remained `cache_state=VALID` and processed more than
  `343000` messages before shutdown.
- After shutdown, `predmarket doctor` returned `status=ok` with zero errors and
  zero warnings in every category.

## Traceable runtime notices

- `dependency_revision_changed` caused evaluation windows to abort when the
  catalog changed concurrently; these were logged with generation, stage,
  expected revisions, and actual revisions, and the runtime continued.
- Exchange clock skew warnings were logged with token and timestamp details and
  `evaluation_continues=true`.
- No `catalog_cleanup_failed`, `runtime_task_exited`, or startup failure was
  observed.

## Evidence limits

No fill source was observed during this read-only run. Therefore realized
returns are `unsupported`; theoretical estimates are not treated as realized
returns. WAL was measured only and was not externally checkpointed by the
validator.

Artifacts:

- [Raw validator report](catalog-v4-observation-2026-08-10.json)
- [Runtime log](catalog-v4-runtime-2026-08-10.log)
