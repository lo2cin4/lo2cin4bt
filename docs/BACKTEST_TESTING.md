# Backtest Testing Guide

This project already has a deterministic smoke path for checking whether the
backtest pipeline still works.

## Quick Health Check

Run the smoke tier only:

```bash
pytest -m smoke -q
```

This is the fastest way to verify:

- config validation still works
- autorunner config loading still works
- file-based data loading still works
- the backtest engine still produces trade results

## Full Phase 1 Regression

Run everything:

```bash
pytest -q
```

## Test Markers

- `smoke`: the shortest end-to-end checks
- `golden`: deterministic snapshot coverage
- `regression`: contract and edge-case coverage

## Coverage Map

For a full project check, use the smoke and regression commands above plus the
targeted contract tests referenced by the README and quality gates.

## Golden Test Coverage

Run only the immutable result baselines:

```bash
uv run --locked --exact --group dev python -m pytest -m golden -q
```

The suite contains 20 cases:

- two low-level Rust accounting fixtures
- one content-addressed `MarketDataBundle` fixture
- one complete-month Binance BTCUSDT 1m SMA(10,20) fixture
- one XNYS multilevel time-contract fixture
- all eight public built-in strategy examples, including the two bounded
  parameter-matrix workflows
- one Rust metrics fixture
- one intraday-to-session-close metrics fixture
- one Rust `PlotBundle.v1` fixture
- one WFA selected-optimum fixture
- two completed-period WFA warmup fixtures
- one end-to-end config-to-metrics-and-plot fixture

The canonical fixtures are:

- `tests/fixtures/backtester/rust_accounting_golden_v1.json`
- `tests/fixtures/golden/canonical_pipeline_golden_v1.json`
- `tests/fixtures/golden/binance_btcusdt_1m_sma_10_20_golden_v1.json`

The tests are:

- `tests/test_rust_accounting_golden.py`
- `tests/test_canonical_pipeline_golden.py`
- `tests/test_binance_1m_sma_golden.py`
- `tests/test_unified_portfolio_wfa_runner.py`

The baselines lock stable business fields such as content hashes, resolved
parameters, result hashes, equity, trades, turnover, costs, validation status,
IS/OOS metrics, Rust kernel labels, and chart payload structure. They
intentionally exclude temporary paths, machine-specific paths, request IDs, and
runtime-generated timestamps.

Every public strategy Golden case follows the same production route:

```text
strategy_run config
-> EngineRequest
-> MarketDataBundle
-> persistent Rust engine service
-> ResultValidationReport
-> Rust metrics
-> PlotBundle
```

Do not restore deleted Python exporters or maintain a second expected-result
implementation. When an intentional result-changing release updates a baseline,
review the business change first, then update the fixture and document the
reason in the changelog.

## What To Watch

If `smoke` fails, the backtest pipeline is no longer trustworthy enough for
normal use. Check the smoke test first, then inspect the config, loader, engine,
and exporter layers in that order.
