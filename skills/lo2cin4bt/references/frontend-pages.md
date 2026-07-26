# Frontend Pages

Use this to teach users how to read the browser app.

## Command Center

Look first:

- Recent completed runs.
- Latest result links.
- Successful/failed module counts.
- Whether the user should open Run Center, Metrics, or WFA next.

Can mislead:

- A recent run being listed does not prove every artifact needed by every page exists.
- A failed or partial run may still have useful logs but should not be treated as final evidence.

Source:

- `GET /api/app/command-center`
- `plotter/web/src/pages/CommandCenterPage.tsx`

## Run Center

Look first:

- Config list for Backtests and WFA.
- Batch status.
- Stage/status messages.
- Buttons to open config or output folders.

Can mislead:

- Deleted workspace configs disappear after refresh, but completed output artifacts remain until outputs/registry are cleaned.
- Stale running jobs may need registry/log inspection.
- Backtests and WFA use different config folders and artifact directories.

Source:

- `GET /api/app/run-center/configs`
- `POST /api/app/batches`
- `plotter/web/src/pages/RunCenterPage.tsx`

## Metrics Overview

Look first:

- Strategy Table selected row.
- Total return, CAGR, Sharpe, Calmar, max drawdown.
- Benchmark label and whether benchmark is shown.
- Data health, effective start, loaded assets, warnings.

Can mislead:

- `QQQ Buy & Hold` and `SPY Benchmark` differ because the benchmark symbol differs, even if both are buy-and-hold.
- Parameter Matrix winners are not WFA proof.
- Portfolio rows may have rebalance counts rather than closed single-asset trade counts.

Source:

- `GET /api/app/metrics/{run_id}/overview`
- `outputs/app/chart_payloads/{run_id}/metrics_overview_payload.json`
- `plotter/web/src/pages/MetricsOverviewPage.tsx`

## Parameter Matrix

Look first:

- Parameter axes and candidate table.
- Objective/ranking basis.
- Robust shortlist and acceptance templates.
- Cluster/plateau evidence if available.

Can mislead:

- It is in-sample/grid evidence unless linked to WFA OOS.
- No-domain or single-axis table-only state is not automatically an error.
- Candidate review helps choose what to validate next; it is not a final production signal.

Source:

- `GET /api/app/metrics/{run_id}/parameter-matrix`
- `plotter/web/src/pages/ParameterMatrixPage.tsx`

## Backtests

Look first:

- Selected strategy summary.
- Equity and benchmark curves.
- Drawdown, monthly/yearly rows, risk metrics.
- Trades or reconstructed closed portfolio legs.
- Allocation changes, target holdings, asset contribution, turnover, costs, risk gates.

Can mislead:

- Missing gross/net or slippage sensitivity means not generated, not zero.
- Portfolio rebalance rows are not identical to single-asset closed trades.
- Same date rows may be open/close session events; inspect exchange time.

Source:

- `GET /api/app/backtests/{run_id}/{backtest_id}`
- `plotter/web/src/pages/BacktestsPage.tsx`

## WFA / Rolling Validation

Look first:

- Whether it is WFA optimization or rolling validation.
- IS/OOS window sizing.
- Selected optimum per window.
- Average OOS metric, OOS positive window share, OOS/IS ratio, parameter stability.
- Portfolio allocation/contribution by window when available.

Can mislead:

- Diagnostic candidate rows are not official selected optimum rows.
- Legacy grid artifacts can be inspectable but should not become final pass/fail evidence.
- Fixed strategies validate windows without IS re-optimization.

Source:

- `GET /api/app/wfa/{run_id}/dashboard`
- `plotter/web/src/pages/WFAPage.tsx`

## Optional Statistical Analysis Output

Look first:

- Whether an optional statanalyser diagnostic artifact exists for this run.
- Whether the user explicitly requested factor diagnostics in addition to the normal backtest.

Can mislead:

- Do not claim IC, Rank IC, Fama-MacBeth, VIF, or full risk model support unless the current artifact and tests prove it.

Source:

- `GET /api/app/statanalyser/{run_id}`
- `statanalyser/`

There is currently no dedicated React route or page for this output. The API is not part of the required `dataloader -> Rust backtester -> mandatory result validator -> Rust metrics/PlotBundle -> plotter/web` chain. Its absence must be reported as `not applicable` unless the optional diagnostic workflow was requested.
