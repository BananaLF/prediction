# Tutorial

## Start and inspect

From the repository root, inspect the available interface and run the local
collector with the sample configuration:

```console
python -m predmarket --help
python -m predmarket run --config config/default.yaml
```

In another terminal, inspect the local evidence:

```console
python -m predmarket status --config config/default.yaml
python -m predmarket signals list --config config/default.yaml
python -m predmarket relations list --config config/default.yaml
```

The collector only reads public market data.  It may write its local evidence
database through `DatabaseWriter`, but it cannot authenticate, sign, or trade.
When an input cannot be verified, the service records the operational condition
and withholds a signal.

## Review a relationship

Use an existing relationship identifier from `relations list`:

```console
python -m predmarket relations show RELATION_ID --config config/default.yaml
python -m predmarket relations approve RELATION_ID --config config/default.yaml
```

Approval is intentionally narrow: it accepts only a current, valid
`LLM_APPROVE` analysis.  It does not turn an incomplete NegRisk event into an
eligible relationship.  NegRisk needs complete membership and supported
conversion metadata before payoff logic can be evaluated.

If a relationship later loses valid evidence, its opportunity is closed rather
than treated as a completed trade.  WebSocket disconnects likewise require a
fresh REST book snapshot before evaluation resumes.
