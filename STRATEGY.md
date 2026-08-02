# Signal strategy

Predmarket is a read-only signal system: it records bounded, evidence-backed
pricing relationships and does not place, cancel, sign, or settle trades.  A
strategy decision is made only from the affected token's current catalog,
order-book, fee, and relationship evidence.  Invalid or incomplete evidence is
not evaluable; a candidate that misses a feasibility threshold is recorded as
absent rather than actionable.

## Strategy paths

| Path | `StrategyType` | Necessary eligibility |
| --- | --- | --- |
| Binary underpriced | `BINARY_UNDERPRICED` | Exactly one complete Yes/No market; both tokens map uniquely and buying both legs can be immediately merged. |
| Binary overpriced | `BINARY_OVERPRICED` | Exactly one complete Yes/No market; both tokens map uniquely and both legs are sold after a split, with split/inventory risk assessed. |
| Logical implication | `LOGICAL_IMPLICATION` | The bound implication relationship is `APPROVED`; its two markets match the relation, and the combination is not-A plus Yes-B. |
| NegRisk complete set | `NEG_RISK_COMPLETE_SET` | One active NegRisk event with complete members in continuous outcome positions, complete matching sync generations for event/markets/tokens, and supported complete SDK conversion metadata. |

The dispatcher evaluates only these four enumerated paths.  Every required
market must be active, watchable, accepting orders, and have its order book
enabled.  Required tokens must belong to those markets and the changed token
must be one of them.  The catalog and required order books must share complete,
consistent generations.  Each required book must have the correct identity,
usable side-specific depth, no future or stale timestamp, and timestamps within
the configured leg-skew limit; each token also needs a current fee schedule.

Quantity planning respects market and book minimum quantities and available
depth.  Feasibility additionally gates on bankroll, positive expected profit,
the return threshold, risk threshold, and unhedged-notional threshold.  Thus
fees, depth, minimum quantity, book age, leg skew, active state, and all of
those thresholds are necessary checks, not estimates applied after a signal is
opened.

For each candidate quantity, the stored calculation uses `Decimal` values:

```python
return_rate = expected_profit / total_capital
risk_rate = worst_case_loss / total_capital
```

`risk_rate` is a capital-loss ratio from the evaluated failure scenarios, not
an event probability.  `unhedged_notional` is a separate inventory/unhedged
notional field.  The risk gate checks both `risk_rate` and
`unhedged_notional`, and stores `worst_case_loss` plus the scenario-derived
`risk_flags` with the calculation.

The underpriced binary path models two buys followed by an immediate merge;
the overpriced path models a split followed by two sells and evaluates the
remaining inventory if those sells do not complete.  The implication path is
hold-to-resolution and records the not-A + Yes-B payout states.  NegRisk uses
only its authoritative conversion schema and metadata; incomplete, unsupported,
mixed, or generation-inconsistent metadata has no inferred payoff.

## Relationship gate

Relationship discovery records candidates.  Analysis can produce
`LLM_APPROVE` only when it is configured and supplied; it is not a default
execution path.  The explicit human command below is the transition to
`APPROVED`; missing analysis, stale source data, changed members, or any
non-`APPROVED` relationship fail closed for logical implication.

```console
predmarket relations list --config config/default.yaml
predmarket relations show RELATION_ID --config config/default.yaml
predmarket relations approve RELATION_ID --config config/default.yaml
```

`CLOSED` means the recorded opportunity is no longer actionable (for example,
its evidence became invalid).  It does not claim that anything filled, settled,
or was profitable; a later valid observation is a new signal with its own
evidence and revisions.
