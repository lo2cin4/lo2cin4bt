# Strategy Config Fields

Preferred user-editable backtest configs use `strategy_run`. Validation workflow configs use `wfa_run` and reference a reusable strategy run instead of duplicating it. For seeded or user-facing workspace runs, put the executable strategy config in `workspace/runs/` and set `strategy_run_path` to an explicit repo-relative path such as `workspace/runs/my-strategy.json`; do not use a bare filename.

## Required Thinking Before Writing Config

Collect:

- Asset/universe.
- Data provider.
- Frequency.
- Calendar and timezone.
- Shared execution mode.
- Strategy profile or authoring preset when relevant.
- Workflow.
- Entry/exit, selection, or allocation rules.
- Fill timing.
- Costs and slippage.
- Benchmark.
- Risk gates.
- Parameter domains, if any.

If any item is ambiguous, ask before writing. If the repo has no implementation evidence for the requested behavior, mark it unsupported.

For naming and frontend summary behavior, also apply
`strategy-identity-and-summary.md`. `platform.display_label` is the concise
human identity; the Strategy Logic panel is generated from normalized
executable fields and never from the label or `metadata.notes`.

## Main Sections

| Section | Purpose | Common mistake |
| --- | --- | --- |
| `schema_version` | declares config contract | omitting version on new configs |
| `platform` | `strategy_mode_id`, `workflow_id`, specific `display_label` | adding `Backtest`, date, run id, workflow, or a generic family-only strategy name |
| `data` | provider, frequency, calendar, timezone, benchmark, external features | comparing across incompatible providers |
| `universe` | tradable symbols | using current-only constituents without provenance |
| `computed_fields` | named values computed from market data, such as SMA, EMA, momentum, volatility, ATR, RSI, MACD, z-score, percentile, or Bollinger values | using a computed field before it exists |
| `signals` | entry/exit rules for signal strategies | writing natural language instead of structured fields |
| `selection` | ranking/eligibility for portfolio strategies | ranking on future or unavailable fields |
| `allocation` | weights, top-N, position limits | forgetting short permission or cash behavior |
| `rebalance` | schedule, event, or signal trigger | assuming daily bars can show intraday events without session logic |
| `fill_model` | timeline fill actions, costs, slippage, accounting assumptions | omitting cost model or leaving timing ambiguous |
| `risk` | gates, short permission, exposure limits | treating disabled gates as active |
| `parameter_domains` | matrix/WFA tunable values | using WFA when no parameter exists |
| `metricstracker` | annualization and risk-free-rate assumptions for performance metrics | comparing Sharpe/CAGR from different assumptions as if they were the same |
| `outputs` | requested artifacts | expecting pages without required artifacts |

## Strategy Building Block Kinds

Do not treat every Strategy Building Block as an indicator. The authoring layer separates the blocks by `block_kind`:

| `block_kind` | DSL use | Examples |
| --- | --- | --- |
| `indicator` | computed values in `computed_fields` | SMA, EMA, momentum, volatility, ATR, RSI, MACD line/signal/histogram, z-score, percentile, Bollinger |
| `condition_logic` | combine boolean rules in `signals` or `selection` | all, any, not |
| `condition_comparator` | compare fields, constants, or computed values | greater than, less than, equal |
| `cross_condition` | detect crossing between two operands | cross up, cross down |
| `calendar` | create date/session masks or rebalance/event triggers | nth weekday, month start, year end |
| `execution` | mark fill timing or accounting semantics | timeline actions, same-session close |
| `strategy_template` | expand a common strategy shape into config | MA cross, fixed allocation rebalance, momentum rotation |

The Strategy Config DSL is for human/AI authoring. Machine IR is for validators and runtimes. A DSL entry such as `op: indicator.rsi` should compile to the registry canonical id `indicator.rsi` before validation.

## Shared Execution Mode, Profiles, And Presets

Public `strategy_run` configs now use one shared execution mode:

- `multi_asset_portfolio`: the shared runtime for one-asset and many-asset strategies after they compile into target weights or timeline actions.

Common public strategy profiles:

- `selection_timing_portfolio`: selection/timing logic, including one-asset MA cross strategies after compilation.
- `allocation_portfolio`: fixed weights or baseline-plus-event allocation rules.
- `rotation_portfolio`: cross-sectional ranking for long-only top-N rotation or disjoint strongest-long / weakest-short rotation.
- `calendar_event_portfolio`: event-driven entry/exit rules expressed through calendar triggers plus timeline actions.

Common public authoring presets:

- `single_asset_signal`: a beginner-friendly authoring preset for one-asset signal strategies that compile into `selection_timing_portfolio`.

Treat old names such as `dynamic_allocation_rules`, `multi_asset_trigger_selection`, or `calendar_event_session` as retired taxonomy rather than the current public contract.

## Semantic Indicator Support

For `strategy_run` configs routed through the unified portfolio engine, top-level `computed_fields[]` can define `indicator.sma`, `indicator.ema`, `indicator.momentum`, `indicator.calendar_return`, `indicator.volatility`, `indicator.atr`, `indicator.rsi`, `indicator.macd`, `indicator.zscore`, `indicator.percentile`, and `indicator.bollinger`. After the computed field is named, reuse that name in `selection.rank_by`, `selection.eligible`, or `signals.entry` / `signals.exit`. Use only these canonical `indicator.*` op names; aliases such as `sma`, `ta.sma`, `atr`, or `average_true_range` are intentionally rejected so AI-authored configs stay consistent. MACD uses `indicator.macd` only; set `output` to `line`, `signal`, or `histogram`.

`indicator.atr` reads `high`, `low`, and `close` by default. If the data uses other column names, set `high_source`, `low_source`, and `close_source`. Public indicators are current-bar-completed fields and cannot be used for same-session open entries; use them for next-bar decisions or non-entry analysis unless a reviewed implementation proves the signal was known before the fill.

Inline condition feature nodes are no longer part of the public strategy config surface. Define every calculation in `computed_fields[]` first, then reference it by field name.

### Monthly cross-sectional long/short rotation

Use shared blocks rather than a strategy-specific producer:

- `indicator.calendar_return` calculates return between completed monthly observations. A 12-1 momentum score uses `start_lag: 12`, `end_lag: 1`, and `sampling: "month_end"`.
- `selection.long_top_n` selects the strongest names and `selection.short_bottom_n` selects a disjoint weakest tail. Equal scores use the engine's deterministic symbol tie-breaker.
- `allocation.method: "equal_weight_long_short"` combines the two tails. Set `long_gross_exposure` and `short_gross_exposure` explicitly; for two names on each side with 100% gross and zero net, use `0.5` and `0.5`.
- Set `risk.allow_short: true`, `risk.long_short: "long_short"`, and exposure limits that agree with the allocation.
- A month-end close score must use `fill_model.timing: "signal_close_for_next_bar"` with `price: "next_open"`; the runtime must receive an open-price table and must not replace it with close prices.
- Put annual short borrow under `fill_model.cost.short_borrow_rate_annual` and its accrual denominator under `fill_model.cost.borrow_day_count`. Borrow is charged against held short notional once per session.

These are optional blocks in the same `strategy_run` schema. Adding another strategy should compose them with existing blocks rather than add a new config family or runtime path.

External research data belongs in `data.external_features[]`. The config should
say what local file and column are needed; dataloader handles date alignment,
market-level broadcasting, and symbol-level pivoting. Do not ask users to create
separate intermediate files for `open`, `high`, `low`, `close`, a feature, or a
constant threshold. Put threshold constants directly in the signal rule.

For a local market-breadth source, use the actual source columns:

```json
"external_features": [
  {
    "name": "market_breadth",
    "path": "workspace/datasets/MARKET_BREADTH_1D.csv",
    "time_column": "time",
    "value_column": "close",
    "scope": "market"
  }
]
```

Do not generate intermediate field CSV files for new configs.

## Fill Timing And Timeline Actions

New user-facing examples should prefer:

```json
"fill_model": {
  "timing": "timeline",
  "actions": [
    {"signal": "entry", "offset_bars": 1, "price": "open", "action": "enter"},
    {"signal": "exit", "offset_bars": 1, "price": "open", "action": "exit"}
  ],
  "cost": {"transaction_cost": 0.001, "slippage": 0.0005}
}
```

Default runnable strategies must set `transaction_cost` to `0.001` (0.1%).
`slippage` is a separate assumption and may explicitly be `0.0`. If an authored
config omits the cost block, normalization applies `transaction_cost=0.001`
and `slippage=0.0`; the Strategy Builder should still write both values so the
assumption is visible before execution.

Plain meaning:

- `signal`: which trigger the action listens to, such as `entry`, `exit`, or `rebalance`.
- `offset_bars`: how many bars after the trigger date the action executes. Use `0` only when the action is fully known before the session, such as a calendar event or scheduled static rebalance. A signal or target derived from the current bar must use `1` with next-bar execution, or use `fill_model.timing=signal_close_for_next_bar` with `price=close_to_close`.
- `price`: the fill checkpoint, currently `open` or `close`.
- `action`: what the portfolio should do, such as `enter`, `exit`, `flatten`, or `set_target_weights`.

Common patterns:

- Signal known after today's close, trade next open:
  `entry +1 open enter` and `exit +1 open exit`.
- Baseline position on the first tradable row:
  set `rebalance.trigger.op` to `calendar.first_session` and add a
  `rebalance +0 open set_target_weights` action.
- Signal switches to another asset for N bars, then restores baseline:
  use `entry +1 open set_target_weights` for the switch and
  `entry +N close set_target_weights` for the restore.
- If a fresh entry signal should extend an active holding period, use
  `fill_model.position_policy.on_entry_signal_while_holding = "reset_timer"`.
- Close-to-close diagnostic parity:
  `entry +0 close enter` and `exit +0 close exit`.
- Same-session event:
  `entry +0 open enter` and `entry +0 close exit`.
- Fixed allocation or rotation rebalance known before the fill:
  `rebalance +0 close set_target_weights`.
- Rotation score known only after the current close:
  use `signal_close_for_next_bar` and `next_open`; explicit open prices are mandatory.

Open fills require explicit open market data. Do not silently substitute close prices for open prices.

## Risk Gates

Place every risk control directly under `risk` for every strategy profile:

```json
"risk": {
  "max_positions": 1,
  "max_gross_exposure": 1,
  "long_short": "long_only",
  "allow_short": false,
  "max_drawdown": 0.1,
  "gate_action": "shadow_until_recovery"
}
```

Plain meaning:

- `max_drawdown: 0.1` means stop when live-trading equity falls more than 10% from its current live equity peak.
- `gate_action: "shadow_until_recovery"` means flatten and stop live trading, keep a virtual portfolio running, and resume live trading only after the virtual portfolio recovers the halted peak and the next rebalance/action arrives.

Supported `gate_action` values currently include:

- `flatten`
- `permanent_stop`
- `shadow_until_recovery`
- `block_new_orders`
- `reduce_exposure`

Important behavior note:

- For `shadow_until_recovery`, each time live trading resumes, the live drawdown peak resets to the resumed live equity level. This is why repeated 10% stop cycles can stair-step lower over time without using one single historical peak forever.

Applicability:

- This gate layer is shared by every profile because all formal runs route through the same Rust risk state machine.

## Metric Assumptions

Add `metricstracker` when writing user-facing configs:

```json
"metricstracker": {
  "enable_metrics_analysis": true,
  "time_unit": 252,
  "risk_free_rate": 0.04
}
```

Plain meaning:

- `time_unit`: periods per year used by Sharpe, Sortino, annualized volatility, CAGR, Calmar, and benchmark annualized metrics. Traditional daily assets default to `252`; crypto defaults to `365`.
- `risk_free_rate`: annual risk-free rate. Use `0.04` for 4%; `4` is accepted and normalized to `0.04`.

Do not compare Sharpe, CAGR, Calmar, or annualized volatility across runs unless these assumptions match or the difference is explicitly explained.

## Workflows

- `single_backtest`: one policy.
- `parameter_matrix`: expand `parameter_domains`.
- `walk_forward_analysis`: IS search then OOS selected optimum.
- `rolling_validation`: fixed/no-domain policy across OOS windows.
- `statanalyser`: factor/stat artifact analysis only when generated.

## Provider Notes

- `yfinance`: public ETF/equity data; best beginner route.
- `binance`: crypto symbols such as `BTCUSDT`; keep benchmark provider compatible.
- `coinbase`: product notation such as `BTC-USD` when configured.
- `file`: local CSV/parquet OHLCV.
- `futu` and `ibkr`: require local gateway apps, packages, permissions, and market-data entitlements.

## Validation Before Full Run

- Validate schema.
- Run `python scripts/doctor.py`.
- Start with one small config.
- Confirm Metrics Overview and Backtests payloads exist.
- Only then run a large Parameter Matrix or WFA.
