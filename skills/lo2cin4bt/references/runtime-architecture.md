# Current Runtime Architecture

This file is the runtime truth that every lo2cin4bt agent, skill, and lecture must follow.

## One Public Flow

```text
strategy_run / wfa_run
-> Python control plane validates config and loads market data
-> NormalizedStrategyPlan + EngineRequest + MarketDataBundle
-> persistent Rust engine service
-> CanonicalResultBundle.v1
-> mandatory Rust result validator
-> Rust metricstracker + PlotBundle.v1
-> app-managed manifests and content-addressed chart payload indexes
-> FastAPI on port 2424
-> React plotter frontend
```

All strategies use this contract. Strategy profiles such as
`selection_timing_portfolio`, `allocation_portfolio`, `rotation_portfolio`,
`calendar_event_portfolio`, `pair_spread_portfolio`, and
`multi_leg_event_portfolio` describe authoring and validation shape; they are
not separate backtest engines or family-specific runtime paths. Single-asset
strategies are one-asset portfolios.

The current overall execution backend is `vector_hybrid`: Rust precomputes
indicators, signals, rankings, and target weights in vector form, then performs
fills, holdings, costs, risk events, and equity accounting sequentially. This is
one internal implementation plan, not a public strategy choice and not a second
backtest path.

## Language Boundaries

Rust owns result-changing calculations:

- indicators and computed fields supported by the op registry
- period, logarithmic, overnight, and intraday returns through
  `rust/lo2cin4bt_core/src/computed_fields/returns.rs`
- signals, calendar triggers, selection, ranking, and target weights
- sequential fills, holdings, cash, fees, slippage, turnover, and equity
- risk actions and risk events
- canonical result validation
- canonical performance metrics and PlotBundle projection

Python remains an active control plane, not a second backtester:

- public config validation and normalization
- provider/file market-data loading and alignment
- scheduler and persistent Rust service transport
- artifact, manifest, registry, API, and payload-index I/O
- WFA window orchestration and optional statistical-analysis orchestration

Production integration is a persistent process/JSON/parquet boundary through
`backtester/RustCoreBridge_backtester.py` and `engine_service_cli`. It is not a
PyO3 or maturin extension path. Do not teach one-shot per-strategy CLI programs
as public execution routes.

Return calculations have one implementation. Python must not recalculate return
columns, provide a no-op fallback, or add Numba/llvmlite as runtime dependencies.
Research and strategy extensions must call the shared Rust return functions
rather than maintain independent formulas.

## Workflow Boundaries

- `single_backtest` runs one fixed config.
- `parameter_matrix` expands declared `parameter_domains`; it is optional and
  is not validation.
- `walk_forward_analysis` performs in-sample parameter selection and paired
  out-of-sample testing through `validation_workflow/`.
- `rolling_validation` tests a fixed policy across rolling windows and does not
  imply parameter optimization.
- The mandatory result validator runs after every backtest candidate. WFA and
  rolling validation remain explicit workflows and do not run automatically for
  every backtest.
- `statanalyser/` is an optional diagnostic overlay. It is not between the
  backtester and metricstracker in the required pipeline.

## Artifact Boundary

The public output root is `outputs/app/`:

- `run_registry/` and `latest_runs.json`
- `stage_status/`
- `run_snapshots/<run_id>/managed_artifacts/`
- `artifact_manifests/<run_id>.json`
- `chart_payloads/<run_id>/`
- `ai_review/<run_id>/`
- `screenshots/<run_id>/<capture_id>/`

Rust producers return full canonical bundles. Repeated chart series are stored
once as SHA-256-addressed objects; small payload indexes reference those shared
series. API and AI consumers materialize and validate the full public contract.
Storage indexes must never create a strategy-specific compute path.

## Frontend Boundary

There is one supported frontend: the React app under `plotter/web`, built by
Vite and served by the Python app server at `http://127.0.0.1:2424/`. Vite is a
build tool, not a second product frontend or the production server port.

## Forbidden New Design

- no strategy-family or per-variant engine branches
- no new Python implementation of result-changing backtest or metric math
- no compatibility mapping for new configs
- no workflow name stored as strategy mode
- no direct consumer dependency on old module-specific output folders
- no silent fallback when canonical result, validator, metric, or PlotBundle
  contracts are missing
