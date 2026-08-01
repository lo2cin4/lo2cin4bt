# Contracts Index

Use this to find schemas and contract docs before creating or interpreting configs.

## Main Docs

- `skills/lo2cin4bt/references/runtime-architecture.md`: canonical runtime and language boundary.
- `README.md`: supported strategy types, runtime map, quick start.
- `docs/backtest-architecture.md`: high-level architecture.
- `docs/backtest-config-and-contracts.md`: config/input roots and contract notes.
- `docs/contracts/strategy-mode-and-workflow-contract.md`: strategy mode and workflow meaning.
- `docs/app-core-contracts.md`: app payload and truth-source rules.
- `docs/ai/AI_READABLE_OUTPUT_CONTRACT.md`: AI review pack boundary.
- `skills/lo2cin4bt/references/strategy-identity-and-summary.md`: canonical strategy naming, result-selector preview, and deterministic Strategy Logic projection contract.

## Strategy And Feature Schemas

- `backtester/contracts/strategy_authoring/strategy-authoring-layers-v1.json`
- `backtester/contracts/strategy_authoring/templates/`
- `backtester/contracts/strategy/strategy-run.schema.json`
- `backtester/contracts/strategy/factor-pipeline-v1.schema.json`: retired
  fail-closed tombstone; use Rust-backed `computed_fields[]`.
- `backtester/contracts/strategy/examples/`
- `backtester/contracts/ops/op-spec-v1.schema.json`
- `app/contracts/generated/op-registry-v1.json`
- `backtester/contracts/ops/`: reviewed Rust strategy building-block registry.
- `backtester/contracts/runtime/engine-request-v2.schema.json`
- `backtester/contracts/runtime/market-data-bundle-v2.schema.json`
- `backtester/contracts/runtime/examples/market-data-bundle-v2.example.json`
- `backtester/contracts/runtime/examples/engine-request-profile-fixtures-v2.json`
- `app/contracts/`

## Source Code Authorities

- `backtester/StrategyRunConfig_backtester.py`: config normalization and planning.
- `backtester/EngineRequest_backtester.py`: canonical request compilation,
  validation, hashing, and the Python control-plane adapter.
- `backtester/UnifiedBacktestRunner_backtester.py`: unified execution route.
- `rust/lo2cin4bt_core/src/engine_runtime.rs`: canonical portfolio execution.
- `rust/lo2cin4bt_core/src/daily_rank.rs`: feature, selection, and portfolio accounting kernel.
- `rust/lo2cin4bt_core/src/result_validator.rs`: mandatory canonical-result validation.
- `rust/lo2cin4bt_core/src/metrics.rs`: result metrics.
- `rust/lo2cin4bt_core/src/plot.rs`: canonical PlotBundle projection.
- `validation_workflow/UnifiedPortfolioWFARunner_validation_workflow.py`: internal validation-workflow runtime for portfolio WFA and rolling validation.
- `rust/lo2cin4bt_core/src/engine_request.rs`: Rust request and market-data
  types plus fixture validation.
- `app/api/payloads.py`: frontend/API payload construction.
- `plotter/web/src/pages/`: frontend page consumers.

## Tests To Consult

- `tests/test_op_registry_backtester.py`
- `tests/test_strategy_authoring_layers.py`
- `tests/test_strategy_run_config.py`
- `tests/test_engine_request_contract.py`
- `tests/test_strategy_compiler_autorunner.py`
- `tests/test_app_api_payloads.py`
- `tests/test_unified_portfolio_wfa_runner.py`
- `tests/test_rust_accounting_golden.py`
- `tests/test_calendar_event_strategy_backtester.py`
