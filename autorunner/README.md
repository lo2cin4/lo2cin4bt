# Autorunner

Autorunner is the orchestration boundary for a canonical `strategy_run`. It does
not define a second strategy language and it does not accept a separate runtime
configuration format.

## Public Contract

Every selectable backtest or standalone statistical-analysis config must use
`schema_version=strategy_run`, including a typed `data.bar_time` contract and
`data.stream_binding`. The complete runnable authoring example is
`autorunner/templates/config_template.json`; legacy `frequency`, `interval`,
`calendar`, and `timezone` fields are rejected rather than mapped or defaulted.

The authoritative schema is
`backtester/contracts/strategy/strategy-run.schema.json`. Working examples live
under `workspace/runs/`, and the authoring template lives at
`autorunner/templates/config_template.json`.

Any config without `schema_version=strategy_run` is rejected at both validation
and loading boundaries. Runtime sections produced by `ConfigLoader` are internal
compiled data and must not be authored directly.

## Responsibilities

- `ConfigSelector_autorunner.py` discovers canonical configs and reads their
  strategy, workflow, provider, universe, and parameter-domain metadata.
- `ConfigValidator_autorunner.py` validates the canonical strategy contract plus
  metric and statistical-analysis settings.
- `ConfigLoader_autorunner.py` normalizes the public config and compiles internal
  dataloader, backtester, metricstracker, and statanalyser runtime sections.
- `DataLoader_autorunner.py` coordinates market-data loading for the compiled run.
- `BacktestRunner_autorunner.py` invokes the unified backtest runner and expands
  canonical parameter domains when the workflow requests a matrix.
- `MetricsRunner_autorunner.py` and `StatAnalyserRunner_autorunner.py` consume the
  canonical run context and generated artifacts.

## Current Execution Boundary

```text
strategy_run
  -> ConfigValidator
  -> ConfigLoader / execution plan
  -> DataLoader
  -> UnifiedBacktestRunner
  -> metrics and optional statistical analysis
  -> managed artifacts
```

This is the current boundary, not the final Rust-upgrade claim. The upgrade plan
still requires the compiled execution request, simulation/accounting, mandatory
validation, metrics, and plot payload generation to converge on the new Rust
service interfaces.

## Statistical Analysis

Statistical analysis uses the same `strategy_run` contract. Set
`platform.workflow_id` to `statanalyser`, and include a top-level
`statanalyser` section with `enabled: true`, target columns, selected tests, and
report settings. It is not a separate legacy config family.

## Verification

```powershell
uv run --locked --exact python -m pytest tests/test_strategy_run_config.py tests/test_unified_config_cutover_audit.py tests/test_statanalyser_autorunner_stage.py -q
uv run --locked --exact python -m ruff check autorunner backtester/StrategyRunConfig_backtester.py
```

The cutover audit must stay green before adding or changing any public config
surface.
