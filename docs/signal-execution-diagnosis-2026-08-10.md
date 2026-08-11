# Signal execution-failure and return-semantics diagnosis

**Issue:** [#28](https://github.com/BananaLF/prediction/issues/28)
**Evidence date:** 2026-08-10
**Repository baseline:** origin/main at 238e3b9

## Conclusion

The available evidence does not show an execution failure or a realized loss. It shows a read-only signal evaluator that calculated a profitable opportunity, then recalculated the same market after the books moved and closed the signal with PROFIT_BELOW_THRESHOLD. CONVERSION_FAILURE, FIRST_LEG_ONLY, PARTIAL_SALES_1, and SPLIT_ONLY are conservative modelled failure scenarios stored in risk_flags; they are not order, fill, cancellation, or settlement events.

The root cause of the reported return false positive is an observability and semantic boundary: a consumer can mistake a theoretical expected_profit or a scenario risk flag for a realized trading result. No strategy threshold is widened or changed by this diagnosis.

## Evidence boundary

The reproduction used `data/catalog-v4-acceptance.sqlite3`. It is a v3 acceptance database, not the production target. A deterministic, reviewable subset is committed as [signal-execution-diagnosis-evidence-2026-08-10.json](signal-execution-diagnosis-evidence-2026-08-10.json) (SHA-256 `9808f6bf1a3a7796dbae1cd215d0a582d4b27fc7037974340452e8bd341817e2`). The bundle records the source database SHA-256 (`1778c19b9ac2e36a459655182f0b8182a288600255d48232e002cadd1b8008d6`) and contains the relevant signals, revisions, legs, full order-book depth, fee schedules, and table-capability inventory. The source database itself remains uncommitted.

Reproduce the bundle when the source database is available:

```bash
python scripts/export_signal_diagnosis.py data/catalog-v4-acceptance.sqlite3 \
  --output docs/signal-execution-diagnosis-evidence-2026-08-10.json
```

Runtime and schema inspection confirm the boundary:

- predmarket/strategy/risk.py computes scenario results from supplied exposures and visible bid depth. A scenario name is added to risk_flags only when the modelled loss is positive.
- predmarket/strategy/common.py creates hypothetical prefixes for FIRST_LEG_ONLY and PARTIAL_LEGS_n, all-leg CONVERSION_FAILURE, and SPLIT_ONLY or PARTIAL_SALES_n inventory cases.
- predmarket/signals/manager.py persists an OpportunityAbsent decision as a closed signal revision. It does not submit or observe an order.
- predmarket/persistence/schema.py has signal, revision, leg, order-book, and event tables, but no order, fill, trade, or realized-return tables.

The existing strategy path does use visible L2 depth, fee schedules, minimum order sizes, book freshness, leg timestamp skew, and catalog-generation checks. The evidence therefore does not support blaming fees, stale books, or best-price-only arithmetic for this sample.

## Reproduced signal lifecycles

| Strategy | Revision | Capital | Expected profit | Return | Risk flags | Final state |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| BINARY_UNDERPRICED | 1 | 3.87179 | 1.12821 | 0.29139 | none | OPENED |
| BINARY_UNDERPRICED | 2 | 5.222895 | -0.222895 | -0.04267 | CONVERSION_FAILURE, FIRST_LEG_ONLY | CLOSED / PROFIT_BELOW_THRESHOLD |
| BINARY_OVERPRICED | 1 | 20.063 | 4.48684 | 0.22364 | none | OPENED |
| BINARY_OVERPRICED | 2 | 5.012375 | -0.222645 | -0.044419 | PARTIAL_SALES_1, SPLIT_ONLY | CLOSED / PROFIT_BELOW_THRESHOLD |

For the underpriced signal, the buy asks changed from approximately 0.34/0.40 to 0.65/0.36. For the overpriced signal, the sell bids changed from approximately 0.60/0.66 to 0.64/0.35. The second revision is therefore a valid negative revaluation of the opportunity, not proof that a first leg was actually submitted and left unconverted.

There are eight order-book snapshots, two per signal revision, with complete levels and matching subscription generation. No system event links either signal to a conversion, order, fill, cancellation, or execution failure. The observed system-event types are sync, queue, and settlement events; they do not constitute trading events.

The evidence chain is independently reviewable from the committed bundle:

| Signal | Revision evidence | Leg evidence | Order-book evidence | System-event evidence |
| --- | --- | --- | --- | --- |
| 1d1ff4e0cab8489e838fd6a9d53b7213 | revisions 1 OPENED and 2 CLOSED | BUY/BUY/MERGE on both revisions | 4 snapshots, generation 3, exchange timestamps 1785828288304 and 1785828289104 | no signal, order, fill, conversion, or cancellation event |
| cbec6442236146b5bc303e22665c2acd | revisions 1 OPENED and 2 CLOSED | SPLIT/SELL/SELL on both revisions | 4 snapshots, generation 3, exchange timestamps 1785828288304 and 1785828289104 | no signal, order, fill, conversion, or cancellation event |

The runtime has no order submission, cancellation, wallet, or settlement adapter to inspect after a hypothetical failure. Therefore the cancellation and exit question is answered as a capability boundary: there is no post-submit state to reconcile, and CLOSED only records the evaluator's decision.

## Root-cause classification

| Reported symptom | Classification | Finding | Impact |
| --- | --- | --- | --- |
| CONVERSION_FAILURE / FIRST_LEG_ONLY | Calculation semantics | Possible loss paths are evaluated from hypothetical open exposures. | A reader may call a worst-case scenario an observed failure. |
| PARTIAL_SALES_1 / SPLIT_ONLY | Calculation semantics | The split inventory model evaluates unsold inventory and immediate recovery. | A reader may call theoretical residual exposure a realized position. |
| Positive revision followed by negative revision | Market revaluation | Later L2 prices no longer satisfy the profitability gate. | Earlier theoretical opportunity must not be reported as realized P&L. |
| No execution record | Recording/capability boundary | This service is public-data read-only and has no fill source. | Simulated and realized results cannot be inferred from signal rows. |

No reproducible code defect was found in the failure-label calculation. The defect is in the meaning assigned downstream if these fields are presented as execution outcomes. The minimal safe policy is to label these values as theoretical_estimate or orderbook_checked_estimate, and to report simulated and realized as unavailable unless an explicit execution source exists.

## Deterministic replay

Run `python scripts/replay_signal_evidence.py docs/signal-execution-diagnosis-evidence-2026-08-10.json`. The replay independently walks the committed L2 levels, recalculates average/worst prices and current fee schedules, separates the inferred safety buffer, and checks all persisted expected-profit values exactly. The four results are `1.12821`, `-0.222895`, `4.48684`, and `-0.222645`; inferred buffers are `0.00925`, `0.012625`, `0.063`, and `0.012375`. This is an `orderbook_checked_estimate`, not fill or realized-P&L evidence.

## Regression contract

The risk tests reproduce positive-loss scenario labels using deterministic exposures and visible bid depth. They must continue to assert that:

1. a scenario name is emitted from the calculated loss of the supplied FailureScenario, not from an external order event;
2. missing visible recovery depth contributes UNCLOSEABLE_EXPOSURE and does not invent a fill or recovery value;
3. the strategy closes a later negative calculation with PROFIT_BELOW_THRESHOLD rather than labeling it as an execution failure;
4. persisted signal legs and order-book evidence remain traceable to the revision that produced the calculation.

The existing tests cover the conservative depth walk, partial-leg and conversion scenarios, split-only and partial-sales scenarios, no-depth exposure, zero-loss clamping, L2 fee and depth calculation, stale/future/skewed books, and signal closure. A future execution integration must add separate order, fill, and cancel identifiers before any field may be classified as simulated or realized.

## Follow-up acceptance

This diagnosis is complete for the current read-only implementation when a reviewer can reproduce the table above from the committed bundle and verify that no order or fill source exists. A source-database holder can additionally regenerate the bundle byte-for-byte using the recorded hash and command. A behavior change is deliberately not proposed until the product defines an execution model, event states, fill authority, and realized-P&L reconciliation.
