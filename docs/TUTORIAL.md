# lo2cin4bt Tutorial

This tutorial is the beginner path for a local GitHub checkout. It assumes no
live trading and no external deployment. Optional broker or exchange accounts
are for market-data access only, not live trading, order placement, fund
movement, position changes, or account setting changes.

## 1. Install

Windows:

```powershell
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
.\scripts\setup.ps1
uv run --locked --exact python main.py
```

macOS / Linux:

```bash
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
bash scripts/setup.sh
uv run --locked --exact python main.py
```

Open:

```text
http://127.0.0.1:2424/
```

If setup fails, run:

```bash
uv run --locked --exact python scripts/doctor.py
```

## 2. Understand The Workspace

Run Center reads local JSON files from:

- `workspace/runs/`
- `workspace/wfa/`

Public GitHub does not track local workspace config copies. That is intentional:
on first app launch, lo2cin4bt seeds included examples from
`backtester/contracts/strategy/examples/` into `workspace/runs/` and
`workspace/wfa/`. A WFA config should point to its strategy config with an
explicit path such as `workspace/runs/my-strategy.json`, not just the filename.
If the local QQQ SMA Cross beginner example was deleted, recreate it with:

Windows:

```powershell
New-Item -ItemType Directory -Force workspace\runs
Copy-Item backtester\contracts\strategy\examples\strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json workspace\runs\strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json
```

macOS / Linux:

```bash
mkdir -p workspace/runs
cp backtester/contracts/strategy/examples/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json
```

You can also ask Codex to create another supported `strategy_run` config
under `workspace/runs/`.

Generated results appear under:

- `outputs/app/run_snapshots/`
- `outputs/app/chart_payloads/`
- `outputs/app/ai_review/`

These are local runtime outputs and are ignored by Git.

## 3. Run A Backtest

1. Open Run Center.
2. Select one backtest config.
3. Start the backtest batch.
4. Wait for the batch to complete.
5. Open Metrics Overview.
6. Select the strategy row you want to inspect.

When reading the result, check in this order:

1. Strategy rule and selected parameters.
2. Date range and effective data start.
3. Benchmark label.
4. Total return, CAGR, Sharpe, Calmar, max drawdown.
5. Trade count, win rate, profit factor, and exposure.
6. Costs, slippage, turnover, and data warnings.

Do not treat a single high-return result as proof. It is only a candidate for
deeper validation.

## 4. Read The Backtests Page

Backtests is the drill-down page.

Look first at:

- equity curve;
- benchmark curve if enabled;
- drawdown;
- monthly/yearly rows;
- trade or reconstructed closed-leg outcomes;
- holdings and rebalance audit for portfolio strategies;
- asset contribution and turnover diagnostics.

For calendar event strategies, same-date rows can represent separate exchange
session events. Inspect the time label before judging sequence.

## 5. Use Parameter Matrix

Parameter Matrix helps answer:

```text
Which parameter region is worth validating next?
```

It does not answer:

```text
Is this strategy robust out of sample?
```

Use it to find stable regions, not just the best-looking row. Check:

- parameter axes;
- robust score;
- local plateau score;
- trade count;
- drawdown;
- shortlist / accepted candidate reasons.

The next step after a promising matrix result is WFA or rolling validation.

## 6. Use WFA / Rolling Validation

WFA means:

1. search in the in-sample window;
2. select one policy;
3. test only that selected policy in the paired out-of-sample window.

Fixed strategies without tunable parameters should use rolling validation
instead of WFA optimization.

Read WFA in this order:

1. window sizing;
2. selected optimum rows;
3. OOS return, Sharpe, Calmar, and drawdown;
4. OOS/IS ratio;
5. positive OOS window share;
6. parameter stability;
7. portfolio allocation/contribution by window if present.

Diagnostic candidate rows are useful for research, but they are not the official
selected OOS evidence.

## 7. Ask Codex To Review A Result

Use this prompt:

```text
Use the lo2cin4bt skill.
Open the latest outputs/app/ai_review/{run_id}/ai_review_pack.json.
Summarize the result, but separate evidence from inference.
Check benchmark, costs, data provenance, WFA status, and missing fields before
making any conclusion.
```

Codex should inspect:

- `source_payloads`;
- `artifact_table_profiles`;
- `metric_field_catalog`;
- `payload_index`;
- artifact manifests and run snapshots.

Missing fields mean not generated or unavailable, not zero.

## 8. Write A New Strategy

Start with a supported family:

- single-asset signal;
- calendar/session event;
- multi-asset portfolio;
- rotation or top-N selection;
- fixed strategy for rolling validation;
- parameterized strategy for Parameter Matrix / WFA.

Then define:

1. universe;
2. provider and typed execution/decision `BarSpec`;
3. indicators or signals;
4. allocation;
5. rebalance trigger;
6. execution timing, transaction cost, and slippage;
7. benchmark;
8. parameter domains if needed;
9. requested outputs.

For performance metrics, add:

```json
"metricstracker": {
  "enable_metrics_analysis": true,
  "time_unit": 252,
  "risk_free_rate": 0.04
}
```

`time_unit` is the periods-per-year assumption used by Sharpe, Sortino,
annualized volatility, CAGR, Calmar, and benchmark annualized metrics. Daily
traditional assets default to `252`; crypto defaults to `365`. `risk_free_rate`
is annual and accepts either `0.04` or `4` for 4%.

Keep strategy logic inside `strategy_run` and related contracts. Do not hide
rules in frontend code or ad hoc scripts.

Every committed strategy must also produce a complete strategy summary after
normalization. The shared summary projection must expose the tradable universe,
indicators, signals or selection rules, allocation, rebalance trigger, execution
model and ordered actions, costs, risk limits, and parameter domains when they
exist. Do not write a frontend-only summary for one strategy.

The contract test automatically scans `workspace/runs/`, strategy contract
examples, and every WFA strategy reference:

```powershell
uv run --locked --exact --group dev python -m pytest tests/test_metrics_contract_payload.py -q
```

Adding a strategy config that cannot produce the same summary contract must
fail this test before the strategy is accepted into the runnable workspace.

## 8A. Current Execution And Metrics Layer

Current user-facing configs should express fills with
`fill_model.timing = "timeline"` and explicit `actions[]`. In plain terms:

```text
signals / rebalance / calendar rules
-> timeline actions
-> unified accounting
-> Rust metrics export
-> frontend payloads
```

Compatibility shapes such as older bar-offset or calendar-event wording should
be understood as inputs that compile toward the common timeline/accounting
model when supported. Performance summary math is generated through the
metricstracker exporter and Rust metrics kernel using the metric assumptions in
the config.

## 9. Common Beginner Problems

Run Center has no configs:

- add a local config under `workspace/runs/` or `workspace/wfa/`;
- use `skills/lo2cin4bt/references/indicator-recipes.md` as the guide.

Metrics page is empty:

- confirm the run completed;
- inspect `outputs/app/artifact_manifests/{run_id}.json`;
- rerun if the artifact came from an older schema.

Benchmark looks different from another page:

- check whether the label says same-symbol buy-and-hold or another benchmark
  such as SPY.

Broker provider fails:

- broker packages and gateways are optional read-only market-data experiments;
- they are not required for the first run;
- this project does not place live orders, move funds, change positions, or
  change account settings.

## 10. Next Reading

- `skills/lo2cin4bt/SKILL.md`
- `skills/lo2cin4bt/references/frontend-pages.md`
- `skills/lo2cin4bt/references/metric-dictionary.md`
- `skills/lo2cin4bt/references/quant-interpretation-risks.md`
- `docs/CHANGELOG.md`
