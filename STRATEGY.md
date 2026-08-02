# Signal strategy

Predmarket identifies bounded, evidence-backed pricing relationships; it does
not trade them.  A relationship must be approved before it can produce a
signal.  Relationship discovery records candidates, analysis supplies an
auditable confidence and rationale, and the `relations approve` command is the
explicit human transition to `APPROVED`.

```console
python -m predmarket relations list --config config/default.yaml
python -m predmarket relations show RELATION_ID --config config/default.yaml
python -m predmarket relations approve RELATION_ID --config config/default.yaml
```

Only an `LLM_APPROVE` candidate whose current evidence remains valid can be
approved.  Missing analysis, stale source data, changed members, or an
unapproved relationship fail closed.

NegRisk candidates are eligible only when their event and market metadata is
complete and supports the recorded conversion semantics.  Unsupported,
incomplete, or mixed NegRisk metadata never receives an inferred payoff.

The engine uses verified best bid/ask levels and `Decimal` arithmetic.  It opens
or revises a signal only when all legs, fees, and relationship invariants can be
verified.  `CLOSED` means the recorded opportunity is no longer actionable (for
example, the opportunity disappeared or its evidence became invalid); it is not
a claim that a trade settled, filled, or was profitable.  A later valid
opportunity is recorded as a new signal, preserving the earlier evidence and
revisions.
