# Strategy Mode And Workflow Contract

Strategy Rules displays two separate ideas:

- `platform.strategy_mode_id`: what kind of trading structure the strategy is.
- `platform.workflow_id`: how the current artifact was produced.

Workflow names such as Parameter Matrix and Walk-Forward Analysis must not be stored as strategy modes.

Engine family is a third idea and should stay small:

- `multi_asset_portfolio_engine`: one shared execution engine for checkpoints, target weights, fills, holdings, and risk events.

Execution backend is separate from both mode and workflow:

- `vector`
- `non_vector` / sequential event simulation
- `vector_hybrid`: vector precompute for features/signals/selection/target weights, then sequential accounting for cash, costs, turnover, holdings, and rebalance state

`strategy_run.factor_pipeline` is retired and fails closed because its former
implementation performed result-changing factor and return calculations in
Python. A value, momentum, quality, growth, or volatility strategy remains
`multi_asset_portfolio`, but supported calculations must be expressed through
`computed_fields[]` and executed by the shared Rust engine. There is no automatic
mapping or fallback; an unsupported operation needs a reviewed Rust building
block before the config becomes runnable.

## Strategy Mode

The canonical list lives in:

`backtester/contracts/strategy/mode-registry-v1.json`

The normalized unified run schema lives in:

`backtester/contracts/strategy/strategy-run.schema.json`

The compatibility bridge and runtime planner live in:

`backtester/StrategyRunConfig_backtester.py`

Current primary mode:

- `multi_asset_portfolio`: the shared execution mode for one-asset and many-asset strategies after they compile into target weights or timeline actions.

Pattern win-rate scanning is an `analysis_overlay` only when it is pure diagnostics and does not simulate trades, holdings, capital, or portfolio equity. Once it trades triggered assets or enforces max holdings, it belongs to `selection_timing_portfolio` on the shared portfolio engine.

Multi-factor investing is not a third engine family. It is a vector data and scoring layer that feeds the shared portfolio engine:

- Selection/timing structures use `selection_timing_portfolio`.
- Cross-sectional rotation structures use `rotation_portfolio`, including long-only top-N and disjoint strongest-long / weakest-short portfolios.
- Rule-driven allocation uses `allocation_portfolio`.

`allocation_portfolio` also covers baseline-plus-event allocation. Current
public configs should express event overlays as timeline actions:

- baseline holdings are ordinary target weights.
- event entry uses `fill_model.actions[]` with `price = open`.
- event exit / restore uses later `flatten` or `set_target_weights` actions.
- the event trigger can still be a calendar signal such as
  `calendar.nth_weekday_of_month`.

This layer can model baseline-plus-event strategies, but the current public example uses a simpler event-driven shape: BTCUSDT enters at the open on the configured monthly Nth weekday event and exits at the same session close. Daily crypto candles approximate the open-to-close session under exchange UTC candle conventions and are not live fill guarantees.

Calendar/event rules are a shared trigger layer, not a separate engine family. The same `utils.calendar_events` resolver can produce:

- signal masks for selection-timing logic
- trigger sessions and event audit rows for `multi_asset_portfolio_engine` rebalance or selection policies

For example, monthly rotation can use `indicator.calendar_return` over completed month ends, `calendar.month_end` as the decision trigger, and next-session open execution. The portfolio engine handles deterministic ranking, disjoint long/short tails, signed target weights, transaction costs, borrow costs, exposure checks, and simulated fills through the same shared route.

The canonical long/short rotation contract is:

- `selection.long_top_n` for the strongest names and `selection.short_bottom_n` for the weakest names.
- `allocation.method = equal_weight_long_short` with explicit long and short gross exposure.
- `risk.allow_short = true` with compatible gross and net limits.
- `fill_model.timing = signal_close_for_next_bar` and `price = next_open` for a score known only after the month-end close.
- `fill_model.cost.short_borrow_rate_annual` and `borrow_day_count` for short carry.

This contract adds reusable operations to the existing schema and Rust engine. It does not create another strategy family or Python execution path.

Portfolio rebalance triggers currently include:

- `calendar.every_session`
- `calendar.month_start`
- `calendar.month_end`
- `calendar.quarter_start`
- `calendar.quarter_end`
- `calendar.nth_weekday_of_month`
- `calendar.last_weekday_of_month`
- `calendar.event_date`

The active multi-asset runtime lives in `rust/lo2cin4bt_core/src/engine_runtime.rs`
and the shared rank/feature/accounting kernel lives in
`rust/lo2cin4bt_core/src/daily_rank.rs`.

The current full example config lives at:

`backtester/contracts/strategy/examples/strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json`

## Workflow

Current workflow ids:

- `single_backtest`
- `parameter_matrix`
- `walk_forward_analysis`
- `rolling_validation`
- `statanalyser`

The same shared execution mode can appear in multiple workflows. For example, the QQQ MA strategy remains `multi_asset_portfolio` with the `single_asset_signal` authoring preset in both Metrics and WFA.

`walk_forward_analysis` means rolling IS optimization followed by paired OOS testing of the selected policy. It requires tunable `parameter_domains`. A fixed strategy with no tunable parameters should use `rolling_validation`; it may share the same window runner, but it must not be described as WFA optimization.

## Strategy Rules Sources

The app API builds `strategy_summary` from:

- normalized `strategy_run` when available
- original run or WFA config via `resolved_configs.run_config.config_path`
- `platform.strategy_mode_id` and `platform.workflow_id`
- resolved dataloader config with typed execution/decision `BarSpec`
- backtester trading params for fill timing and cost assumptions
- strategy contract for entry, exit, and parameter domains
