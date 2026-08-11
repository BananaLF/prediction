# Polymarket arbitrage strategy research and evaluation

**Issue:** [#29](https://github.com/BananaLF/prediction/issues/29)
**Research date:** 2026-08-11
**Scope:** research, replay, and acceptance design only; no live orders or funds

## Executive recommendation

For the current read-only service, implement no new trading path. Keep the existing binary complete-set calculations as evidence-producing evaluators, and use a deterministic replay or simulation harness before designing any order submission system.

If a future execution project is approved, prioritize:

1. binary underpricing: buy Yes plus buy No, then merge, with all legs checked;
2. binary overpricing: split, then sell Yes and No, with explicit residual inventory handling;
3. NegRisk complete-set conversion only when the authoritative event member set and adapter metadata are complete;
4. logical implications as a hold-to-resolution research strategy, not an immediate conversion strategy;
5. cross-platform strategies last, because they lack a shared atomic execution and settlement boundary.

This ordering is a risk and observability recommendation, not a claim that any strategy is profitable after live costs.

## Official source register

Sources were checked on 2026-08-11. Polymarket documentation is time-sensitive; the fee rate and market capabilities must be fetched for each market at evaluation time rather than hard-coded.

| Topic | Official source and current fact used |
| --- | --- |
| Price and executable book | [Prices & Orderbook](https://docs.polymarket.com/concepts/prices-orderbook): buys consume asks, sells consume bids; displayed prices and midpoints are not a fill guarantee. |
| Fees | [Fees](https://docs.polymarket.com/trading/fees): current taker formula is `C × feeRate × p × (1-p)`; fees are rounded to five decimals and values below the precision round to zero. |
| Order states, precision, and partial fills | [Place Orders](https://docs.polymarket.com/trading/place-orders): each FOK order fills entirely or not at all; tick, size, amount precision, and `min_order_size` must be validated before submission. |
| Position-operation gas | [Wallets & Authentication](https://docs.polymarket.com/trading/wallets-auth#execute-gasless-transactions): account-wallet relayer/builder flows may sponsor position operations; direct EOA operations require POL gas. |
| Trading boundary | [Trading overview](https://docs.polymarket.com/trading/overview): matching is off-chain while settlement is on-chain; authenticated order APIs are distinct from public market data. |
| Binary positions | [Positions & Tokens](https://docs.polymarket.com/concepts/positions-tokens): splitting creates Yes and No tokens and merging consumes equal quantities. |
| Split/merge | [Merge tokens](https://docs.polymarket.com/trading/ctf/merge): merge is atomic and requires equal quantities; insufficient balances revert. |
| NegRisk conversion | [Negative Risk Markets](https://docs.polymarket.com/advanced/neg-risk): a No position can convert through the Neg Risk Adapter into Yes positions for other outcomes, subject to the event member set and conversion rules. |
| Settlement | [Redeem tokens](https://docs.polymarket.com/trading/ctf/redeem): after resolution, the winning token pays one unit and the losing token pays zero. |

The official docs describe platform mechanics. They do not establish a guaranteed arbitrage return, fill probability, or cross-platform atomicity. Public strategy claims are treated as hypotheses until their mechanics agree with these primary sources and pass the replay gates below; no unaudited profitability claim is used as evidence.

## Candidate strategies

Let `q` be the candidate quantity, `q_i` the quantity consumed at depth level `i`, `a_i`/`b_i` that level's ask/bid, `f_i` the fee charged for a fill, and `c` the conversion, gas, and safety cost. Every price and fee must come from the same valid catalog and order-book observation.

| Strategy | Gross calculation | Necessary execution conditions | Main failure or risk |
| --- | --- | --- | --- |
| Binary underpriced | P = q - sum(q_i * a_i + f_i) - c_merge - c_gas - buffer | Exactly two complementary tokens; enough ask depth on both; valid minimum order and fee data; both buys and merge checked as one plan. | One leg fills first, price moves, insufficient merge inventory, slippage, fee or rounding error, or uncloseable residual. |
| Binary overpriced | P = sum(q_i * b_i - f_i) - q - c_split - c_gas - buffer | Collateral available for split; enough bid depth on both sells; both sales and residual inventory modelled. | Split succeeds but one or both sells are partial; residual token exposure; bid depth disappears; fees exceed margin. |
| NegRisk complete set | Evaluate the adapter complete event conversion plus all required market legs and costs; do not infer missing members. | One authoritative event/member set, supported conversion metadata, consistent generation, and executable depth for every required leg. | Incomplete or changed member set, unsupported adapter type, stale metadata, multileg skew, or conversion mismatch. |
| Logical implication | For an approved A -> B relation, compare No A plus Yes B payoff at resolution against capital and costs; no immediate merge is assumed. | Human-approved relation, explicit outcome mapping, settlement rules, and accepted capital lockup. | Relation is wrong or changes; resolution correlation is not atomic; capital remains locked until settlement. |
| Time or settlement mismatch | Compare the same economic outcome across different close times or resolution rules after all lockup and settlement costs. | Explicit timestamps, resolution authority, payout mapping, and a hedge that remains valid through both settlement points. | Timing drift, unresolved capital, incompatible payout rules, or a leg resolving before the hedge. |
| Cross-platform | proceeds on platform B minus cost on platform A minus transfer, withdrawal, fee, and latency costs | Two platforms, compatible instruments, balances, transfer path, and independently verified settlement. | No shared atomic transaction; transfer latency, limits, outages, fees, and basis or price risk. Reject for current scope. |

For binary paths, sums mean depth-walked fills rather than top-of-book multiplication. The current walk_depth, fee, minimum-order, freshness, leg-skew, and generation checks are prerequisites for an orderbook_checked_estimate, not proof of a fill.

## Cost, capital, and atomicity assessment

- Net profit: `proceeds - leg notionals - all fill fees - conversion cost - conservative gas bound - safety buffer`. Any unknown term rejects the plan.
- Fees: for the current official schedule, `fee = shares × feeRate × price × (1-price)`, rounded half-up to five decimals; smaller values round to zero. Example: 10 taker shares at `p=0.40`, `feeRate=0.05` cost `0.12000`; at `p=0.50` they cost `0.12500`. Buying complementary legs at those prices costs `9.24500`, leaving `0.75500` before conversion, gas, and safety costs.
- Slippage: consume every visible level needed for q and report average and worst price per leg. Reject a plan when required depth is absent or the price changes beyond the evidence window.
- Precision and liquidity: quantize price to the market tick, round share quantity down to the documented size precision (currently two decimals), encode USD amount with the tick-dependent precision table, then recheck `min_order_size`. Reject if the exact SDK-encoded amount cannot be reproduced or any leg falls below its minimum.
- Capital: reserve collateral for split or merge and a safety buffer; include capital locked while waiting for resolution or transfer.
- Gas: use zero only after confirming an authenticated sponsored relayer route for that operation; direct EOA split/merge/conversion requires a configured conservative POL gas quote. Missing credentials, route confirmation, quote, or bound fails closed.
- Atomicity: CTF or NegRisk conversion may be atomic at the conversion boundary, but each FOK guarantee applies to one order only. Sequential or batched multi-order submission is not all-leg atomic, so every filled-leg prefix and delayed `pending` response must be modelled explicitly.
- Settlement: immediate conversion differs from hold-to-resolution. A settlement payoff is not a realized return until the position is actually settled and reconciled.

## Replay and simulation plan

The committed historical fixture is [signal-execution-diagnosis-evidence-2026-08-10.json](signal-execution-diagnosis-evidence-2026-08-10.json). Running `python scripts/replay_signal_evidence.py docs/signal-execution-diagnosis-evidence-2026-08-10.json` independently walks full depth and recalculates fees for two binary strategies across four revisions. It exactly reproduces profits `1.12821`, `-0.222895`, `4.48684`, and `-0.222645`, including the transition from profitable to below-threshold. It validates the binary candidate paths as `orderbook_checked_estimate`; NegRisk, implication, timing, and cross-platform candidates remain research-only until equivalent fixtures exist.

1. Freeze a catalog generation ID and complete event, market, and token metadata.
2. Capture immutable order-book snapshots for every required token, including exchange timestamps, subscription generation, full depth, minimum order size, tick size, and fee parameters.
3. Re-run each strategy at each snapshot with exact decimal arithmetic. Store theoretical_estimate separately from orderbook_checked_estimate.
4. Apply a deterministic fill model: each individual FOK order either fills completely or not at all; this does not make several FOK legs atomic. FAK consumes available depth and records the filled prefix; GTC or a delayed response remains pending until an explicit match or cancellation. The model must not convert a theoretical leg into a fill without evidence.
5. Replay conversion, cancellation, residual-exposure, and resolution paths. Record simulated cash, token inventory, fees, slippage, capital lock time, and terminal settlement separately.
6. Compare decisions and outcomes against the source snapshot and preserve a replay manifest containing source hashes, strategy version, fee version, and assumptions. Do not call the result realized.

Required metrics are fill and partial-fill ratio, leg-skew, slippage by leg, fee-to-margin ratio, conversion-failure scenario rate, residual exposure, time-to-close, capital utilization, theoretical-versus-simulated P&L, and eventual realized P&L only after a future authoritative fill and settlement source exists.

## Acceptance gates before implementation

### Research gate

- Every platform-mechanics claim links to an official source and has a checked date; assumptions are labeled separately.
- Each strategy has a formula, all-leg eligibility rules, cost model, liquidity and minimum-order constraints, atomicity boundary, and settlement risk.
- Cross-market completeness and NegRisk metadata are proven from one catalog generation; missing evidence fails closed.
- Any public or community strategy claim is recorded as a hypothesis and cross-checked against the official mechanics table before replay.

### Replay gate

- A fixed fixture produces deterministic strategy decisions and decimal calculations.
- The fixture includes profitable and unprofitable books, insufficient depth, fee changes, stale or skewed books, partial fills, conversion failure, and resolution outcomes.
- The report distinguishes theoretical, orderbook-checked, simulated, and realized values. With the current repository, realized must remain unsupported.

### Execution-design gate

Before any order API is added, approve a separate design for credentials, balances, idempotency, order state machine, fill authority, cancellation, reconciliation, risk limits, kill switch, and realized-P&L accounting. This research document does not authorize live trading.

## Decisions and open assumptions

| Item | Status |
| --- | --- |
| Current service can submit, cancel, or settle trades | Fact: no; it is public-data read-only. |
| expected_profit means realized profit | Rejected assumption: it is a theoretical calculation. |
| Current books guarantee executable fills | Rejected assumption: they support only an orderbook-checked estimate. |
| Fee schedule is constant across markets and time | Rejected assumption: fetch and snapshot current market parameters. |
| Cross-platform spread is atomic | Rejected assumption: there is no shared atomic settlement boundary. |
| Live strategy implementation is part of #29 | No: implementation requires a separately approved execution design. |
