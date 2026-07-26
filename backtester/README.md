# Backtester Overview

This folder is the canonical home for lo2cin4bt backtest runtime and backtest-specific contracts.

## Runtime Components
- `rust/lo2cin4bt_core/src/engine_runtime.rs`: canonical EngineRequest execution router
- `rust/lo2cin4bt_core/src/daily_rank.rs`: shared feature, selection, allocation, accounting, cost, and risk kernel
- `rust/lo2cin4bt_core/src/result_validator.rs`: mandatory canonical result validation
- `rust/lo2cin4bt_core/src/risk.rs`: shared pre-trade and drawdown risk controls
- `rust/lo2cin4bt_core/src/daily_rank.rs`: canonical indicator and condition kernel

## Contract Canonical Root
- `backtester/contracts/strategy/`
- `backtester/contracts/feature/`
- `backtester/contracts/ops/`

`docs/contracts` is index-only and no longer source-of-truth.

## User Extension Entry
- New strategy building blocks must be implemented in Rust and registered through
  the reviewed op registry with tests and temporal-safety metadata.

## Runtime Contract
- Public runs use `strategy_run` / `wfa_run` configs and the unified timeline,
  event-driven Rust, or vectorized portfolio execution paths.
- Legacy semantic `strategy_contract_path` / NodeIR execution has been retired
  from the public runtime. Migrate timing logic to `fill_model.timing =
  "timeline"` with explicit `actions[]`.

## Notes
- Portfolio invariant checks are opt-in and independent from engine behavior. Existing result formats can be checked directly in tests; multi-asset results are currently converted in weight space because share-level cash ledger fields are not yet part of the result contract.
- Portfolio risk controls are opt-in fields directly under `risk`, such as `risk.max_positions`, `risk.max_order_size`, and `risk.max_drawdown`. Public configs do not accept a nested `risk.gates` compatibility shape. Triggered controls are exported as `risk_gate_events` plus `risk_gate_summary`.
