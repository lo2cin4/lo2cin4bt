# Quant Interpretation Risks

Use this before accepting a backtest, Parameter Matrix, or WFA result as meaningful research evidence.

## Hard Boundaries

- Backtests are diagnostics, not financial advice.
- No local result authorizes live trading.
- Missing fields are not zero.
- Visualizations are not a second source of truth; payloads and artifacts are.

## Data And Provenance

Check before performance:

- Provider and symbol mapping.
- Frequency, calendar, timezone.
- Effective start date.
- Missing assets.
- Data health status.
- Historical Universe Constituents / date-aware membership source.
- Survivorship risk.
- Corporate actions or adjusted price assumptions where relevant.

If universe provenance is weak, lead with the risk before quoting performance.

## Benchmark Risk

- A benchmark is a configured comparison, not always "the market."
- `QQQ Buy & Hold` and `SPY Benchmark` can differ because they are different symbols.
- Crypto benchmark should stay provider-compatible when possible.
- Multi-asset portfolios should not silently invent a benchmark.

## Costs And Slippage

- Always identify configured transaction cost and slippage.
- Cost drag is measured only when generated.
- `gross_available=false` or slippage sensitivity unavailable means not generated, not zero.
- High turnover strategies require cost sensitivity before strong conclusions.

## Parameter Matrix Risk

- Parameter Matrix ranks candidates from available backtest evidence.
- It is not out-of-sample proof by itself.
- Robust score, cluster, plateau, and accepted candidate status are screening aids.
- A top row should lead to WFA/rolling validation or additional review.

## WFA Risk

- Official WFA evidence is selected optimum per IS/OOS window.
- Candidate diagnostics are inspectable but not formal pass/fail rows.
- Fixed strategies use rolling validation and should not be described as optimized in IS.
- Low candidate count, fallback constraints, or unstable selected parameters require caution.

## Portfolio Accounting Risk

- Holdings, rebalance audit, and rebalance trades answer different questions.
- Target weights are desired weights, not always final filled positions.
- Active rebalances differ from scheduled checkpoints.
- Asset contribution may include residual cash/cost effects.
- Short overlays require gross exposure and short permission checks.

## Calendar Event Risk

- Same calendar date rows can be separate exchange-time events.
- For open-to-close event overlays, inspect event time labels.
- Do not judge sequence from end-of-day holdings alone.

## Stale Artifact Policy

If an old run lacks fields required by current verification, delete it from the validation set and rerun the strategy with the current version. Do not mix old and new artifact contracts for final evidence.
