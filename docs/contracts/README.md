# Contracts Migration Index

`docs/contracts` is now an index-only folder.

Backtest contract canonical root has moved to:
- `backtester/contracts/`

## New Canonical Structure
- `backtester/contracts/strategy/`
  - `strategy-contract-v2.schema.json`
  - `examples/strategy-vix-regime-ma-cross.json`
- `backtester/contracts/feature/`
  - `feature-contract-v1.schema.json`
  - `examples/feature-contract-vix-price-v1.json`
- `backtester/contracts/ops/`
  - reviewed Rust strategy building-block contracts

## Migration Mapping
- `docs/contracts/strategy-contract-v2.schema.json` -> `backtester/contracts/strategy/strategy-contract-v2.schema.json`
- `docs/contracts/feature-contract-v1.schema.json` -> `backtester/contracts/feature/feature-contract-v1.schema.json`
- `docs/contracts/examples/*` -> corresponding `backtester/contracts/*/examples/*`

## Legacy Policy
- Existing runtime now resolves contracts from `backtester/contracts` first.
- Legacy `docs/contracts` lookup is no longer a runtime fallback. Keep this
  folder as a migration index only.
- Draft archive notes are not part of the public 2.0.0 source tree.
