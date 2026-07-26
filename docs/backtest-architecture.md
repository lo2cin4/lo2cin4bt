# Backtest Architecture

## Goal
Build an extensible backtest app around one unified strategy/config/execution contract. Single-asset and multi-asset strategies both compile into target weights and then use the same portfolio accounting boundary.

The runtime model is vector-hybrid:
- vector precompute for features, signals, calendar masks, eligibility, ranking, and target weights
- sequential accounting for execution state, holdings, cash, costs, turnover, contribution, and rebalance events

## Core Design
- `plotter/web` is the default React frontend served by `app.api`.
- `app.runtime` contains Python app runtime services used by the API.
- `plotter` is now the visualization namespace; legacy Python Dash/Plotly
  plotter modules were removed in favor of the React frontend under
  `plotter/web`.
- `Strategy Run Config` is the normalized backtester entrypoint
- WFA references a strategy config and stores only windowing, optimizer, objective, and acceptance settings
- Engine routing is explicit through unified timeline/event-driven Rust and vectorized portfolio runtime plans.
- All strategies preserve the same downstream result contract: metrics overview, equity curve, trade or rebalance events, holdings, asset contribution, data quality, and run metadata

## Accounting Boundary
- Stateful exit timing is expressed as timeline `actions[]` with
  `offset_bars`; re-entry timer behavior is controlled by
  `fill_model.position_policy.on_entry_signal_while_holding`.
- `offset_bars: 0` is reserved for actions that are fully known before the
  session, including calendar events and scheduled static rebalances. Targets
  derived from the current bar must use next-bar execution through
  `offset_bars: 1` or `signal_close_for_next_bar`; the support checker rejects
  same-bar data-dependent execution before the Rust engine starts.
- Portfolio accounting is always sequential, even when feature and target-weight construction is vectorized
- Single asset is treated internally as a one-asset portfolio, while the UI may still render single-asset trade charts

## Semantic Strategy Direction
- Strategy authoring uses explicit `strategy_run` sections: `signals`,
  `selection`, `allocation`, `rebalance`, `fill_model`, and `risk`.
- Legacy `condition_pairs` is migration-only compatibility path
- Parameter exploration is handled by `parameter_domains`
- Feature inputs are field-based, not predictor-slot-based
- Multiple semantic fields can already be combined in one strategy (`A AND B AND C`)
- `selected_predictor` remains only as a downstream compatibility label and should not be treated as the semantic design center

## Contract Placement Policy
- Backtest contracts live under `backtester/contracts` as single source of truth
- `docs/contracts` keeps migration index only
- Unified run schema: `backtester/contracts/strategy/strategy-run.schema.json`
- Unified WFA schema: `backtester/contracts/strategy/wfa-run.schema.json`
- Normalizer and planner bridge: `backtester/StrategyRunConfig_backtester.py`

## Feature Input Direction
- Feature contract v1 already supports multiple declared fields and multiple source URIs
- This is the base for future multi-factor / multi-predictor / cross-dataset strategy logic
- Future contract evolution should extend feature binding clarity without reintroducing preset strategy composition
