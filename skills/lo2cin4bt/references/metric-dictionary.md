# Metric And Field Dictionary

All fields are local research diagnostics. They are not investment advice.

Annualized metrics use the run config's `metricstracker.time_unit` and
`metricstracker.risk_free_rate`. If the config omits them, runtime defaults are
`252` periods/year for traditional daily assets, `365` for crypto, and `0.04`
for the annual risk-free rate. When comparing Sharpe, Sortino, CAGR, Calmar, or
annualized volatility across runs, first confirm these assumptions match.

## Core Performance Metrics

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `total_return` | ratio | Ending equity return over the tested period. | Not annualized. |
| `cagr` | ratio/year | Annualized compound growth rate. | Can look high on short histories. |
| `sharpe` | ratio | Return per unit total volatility. | Sensitive to frequency and low-variance artifacts. |
| `sortino` | ratio | Return per unit downside volatility. | Not comparable if downside samples are scarce. |
| `calmar` | ratio | CAGR divided by absolute max drawdown. | Can be unstable when drawdown is tiny. |
| `max_drawdown` | ratio | Worst peak-to-trough equity loss. | Negative values indicate loss depth. |
| `average_drawdown` | ratio | Average drawdown while below peak. | Does not show duration by itself. |
| `recovery_factor` | ratio | Total return divided by absolute max drawdown. | Inflated if drawdown is understated. |
| `std` | ratio | Return volatility for the source period. | Check sampling frequency. |
| `annualized_std` | ratio/year | Annualized volatility. | Depends on annualization assumption. |
| `downside_risk` | ratio | Downside deviation. | Only downside samples count. |
| `annualized_downside_risk` | ratio/year | Annualized downside deviation. | Depends on sample size. |
| `information_ratio` | ratio | Excess return per unit tracking error. | Requires compatible benchmark. |
| `alpha` | ratio | Regression-style excess component versus benchmark. | Not meaningful with wrong benchmark. |
| `beta` | ratio | Sensitivity versus benchmark. | Not meaningful with wrong benchmark. |
| `excess_return` | ratio | Strategy total return minus benchmark total return. | Only valid when benchmark is comparable. |
| `final_equity` | currency/index | Ending equity value. | Starting equity is usually normalized around 100. |

## Trade And Exposure Metrics

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `trade_count` | count | Count of trade events or closed trades depending on run type. | Portfolio strategies may count rebalances/legs differently from single-asset trades. |
| `win_rate` | ratio | Share of closed trade/leg outcomes with positive return. | Requires reconstructed or recorded closed outcomes. |
| `profit_factor` | ratio | Gross profit divided by absolute gross loss. | Undefined when no losses exist. |
| `avg_trade_return` | ratio | Average closed trade/leg return. | Not a portfolio CAGR substitute. |
| `max_consecutive_losses` | count | Longest losing streak in closed outcomes. | Requires trade outcomes. |
| `exposure_time` | ratio | Share of period with market exposure. | 1.0 does not mean 100% long-only; check gross/short exposure. |
| `max_holding_period_ratio` | ratio | Longest holding period divided by total period. | High values can mean buy-and-hold-like behavior. |

## Benchmark Metrics

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `bah_total_return` | ratio | Benchmark/buy-and-hold total return. | Benchmark symbol/provider must match intent. |
| `bah_cagr` | ratio/year | Benchmark annualized return. | Not the same as strategy CAGR. |
| `bah_std` | ratio | Benchmark return volatility for the source period. | Check benchmark sampling frequency. |
| `bah_annualized_std` | ratio/year | Annualized benchmark volatility. | Depends on the same annualization assumption as the strategy. |
| `bah_downside_risk` | ratio | Benchmark downside deviation. | Only downside benchmark samples count. |
| `bah_annualized_downside_risk` | ratio/year | Annualized benchmark downside deviation. | Depends on frequency and downside sample size. |
| `bah_sharpe` | ratio | Benchmark Sharpe. | Same frequency caveats apply. |
| `bah_sortino` | ratio | Benchmark downside-risk-adjusted return. | Unstable when downside samples are scarce. |
| `bah_calmar` | ratio | Benchmark Calmar. | Compare with same date range. |
| `bah_max_drawdown` | ratio | Benchmark max drawdown. | Different symbols can have very different drawdowns. |
| `bah_average_drawdown` | ratio | Average benchmark drawdown while below peak. | Does not show drawdown duration. |
| `bah_recovery_factor` | ratio | Benchmark total return divided by absolute max drawdown. | Inflated if benchmark drawdown is understated. |
| `benchmark_correlation` | ratio | Correlation between strategy and benchmark returns. | Needs overlapping dates. |
| `benchmark_label` | text | Human label for benchmark. | `QQQ Buy & Hold` and `SPY Benchmark` are different baselines. |

## Portfolio And Accounting Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `rebalance_count` | count | Rebalance events recorded in portfolio summary. | Scheduled and active counts may differ. |
| `active_rebalance_count` | count | Rebalances that actually changed holdings. | Zero can be valid for static policies. |
| `scheduled_rebalances` | count | Scheduled checkpoints. | Not every checkpoint changes positions. |
| `avg_holdings` | count | Average number of held assets. | Does not show weights. |
| `avg_gross_exposure` | ratio | Average absolute exposure. | Short exposure can raise gross exposure. |
| `avg_cash_weight` | ratio | Average uninvested cash weight. | Cash may appear around event transitions. |
| `avg_turnover` | ratio | Average turnover on active rebalance events. | High turnover implies more cost sensitivity. |
| `turnover` | ratio | Portfolio weight changed at a row/event. | Not a return metric. |
| `cost_drag` | ratio | Equity drag from configured costs. | Missing means not generated, not zero. |
| `trade_cost_drag` | ratio | Trade-level cost impact where available. | Requires cost artifacts. |
| `Target_weight` | ratio | Desired asset weight after rebalance. | Target is not always filled position after constraints. |
| `Before_weight` | ratio | Weight before rebalance/trade. | Helps explain turnover. |
| `Trade_delta` | ratio | Change in target/position weight. | Positive/negative depends on action. |
| `Selected` | boolean | Asset passed selection for that checkpoint. | Selected is not always bought if allocation/risk blocks it. |
| `Eligible` | boolean | Asset passed eligibility filter. | Ineligible assets can still appear for audit. |
| `Rank` | ordinal | Cross-asset rank for selection. | Lower rank may mean better depending on config; inspect selection rule. |
| `return_contribution` | ratio | Asset contribution to portfolio return. | Contributions may not sum exactly because of cash/cost/residual. |
| `risk_gate_event_count` | count | Number of risk gate events. | Disabled gates should show zero/none, not hidden risk. |

## Parameter Matrix Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `semantic_combo` | object | Human-meaningful parameter set. | Use this instead of opaque ids. |
| `robust_score` | score | Ranking score from configured robustness logic. | Formula depends on ranking config. |
| `local_plateau_score` | score | Whether neighbors have similar performance. | Not a standalone profit metric. |
| `cluster_id` | id | Cluster of similar candidates. | Clusters help stability, not OOS proof. |
| `cluster_size` | count | Number of candidates in cluster. | Large cluster can still be poor. |
| `accepted_candidate_count` | count | Candidates passing pre-review gates. | Not a live-trading approval. |
| `parameter_importance` | score | Search/Optuna-style relative importance. | Only valid for that search/run. |

## WFA Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `window_id` | id | Rolling IS/OOS window number. | Window IDs are not calendar months. |
| `train_start_date`, `train_end_date` | date | IS training/search period. | Must not include OOS evidence. |
| `test_start_date`, `test_end_date` | date | OOS validation period. | OOS is the key robustness evidence. |
| `is_sharpe`, `is_calmar` | ratio | In-sample ranking metrics. | Good IS can still fail OOS. |
| `oos_sharpe`, `oos_calmar`, `oos_total_return` | ratio | Out-of-sample selected policy metrics. | Use selected optimum rows only for verdicts. |
| `oos_profit_factor`, `oos_win_rate`, `oos_max_drawdown` | ratio | OOS trade/risk diagnostics. | May be unavailable for some portfolio artifacts. |
| `oos_is_ratio` | ratio | OOS performance relative to IS. | Very high/low can indicate instability or regime shift. |
| `candidate_count` | count | Candidates evaluated or available for a window. | Diagnostics are not official selected rows. |
| `selected_window_count` | count | Windows using a candidate/family. | Stability signal, not return. |
| `candidate_budget` | count | Candidate limit used for search. | Budget constraints affect coverage. |
| `linked_backtest_id` | id | Full backtest row linked to WFA window. | Link is evidence pointer, not extra validation by itself. |

## Frontend Payload Field Coverage

These fields are extracted from the frontend page contracts. Keep them documented when adding new visible data to Backtests, Parameter Matrix, or WFA.

### Backtest Detail Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `mdd` | ratio | Alias for maximum drawdown. | Treat the sign convention consistently with `max_drawdown`. |
| `profitable_period_ratio` | ratio | Share of periods with positive return. | Period win rate is not closed-trade win rate. |
| `percent_profitable_trades` | ratio | Share of closed trades or reconstructed legs with positive outcome. | Unavailable trade outcomes mean not generated, not zero. |
| `average_win` | ratio | Average positive closed outcome. | Does not include losing trades. |
| `average_loss` | ratio | Average negative closed outcome. | Usually negative; compare absolute loss carefully. |
| `average_win_loss_ratio` | ratio | Average win divided by absolute average loss. | Unstable when losses are scarce. |
| `gain_loss_ratio` | ratio | Gain-to-loss ratio from closed outcomes. | Check whether portfolio legs were reconstructed. |
| `gross_profit` | ratio | Sum of positive closed outcomes. | Gross profit ignores losing outcomes. |
| `gross_loss` | ratio | Sum of losing closed outcomes. | Gross loss should not be read as net return. |
| `max_consecutive_wins` | count | Longest winning streak in closed outcomes. | Requires trade outcome data. |
| `max_drawdown_duration_days` | days | Longest drawdown duration measured in calendar/trading days. | Duration can matter even when drawdown depth is moderate. |
| `max_drawdown_duration_periods` | count | Longest drawdown duration measured in rows/periods. | Compare only with same frequency. |
| `skewness` | ratio | Return distribution asymmetry. | Tail metrics need enough observations. |
| `kurtosis` | ratio | Return distribution tail/heaviness. | High values can be one-event driven. |
| `var_95` | ratio | 95% value-at-risk estimate. | VaR is threshold loss, not worst loss. |
| `cvar_95` | ratio | Expected loss beyond 95% VaR. | More tail-sensitive than VaR. |
| `var_99` | ratio | 99% value-at-risk estimate. | Needs a long sample to be stable. |
| `cvar_99` | ratio | Expected loss beyond 99% VaR. | Very sample-sensitive. |
| `best_month` | ratio | Best monthly return. | Month aggregation depends on available dates. |
| `worst_month` | ratio | Worst monthly return. | One month can dominate the impression. |
| `positive_month_ratio` | ratio | Share of positive months. | Not equivalent to daily or trade win rate. |
| `trade_events` | count | Count of recorded trade events. | One event is not always one closed round-trip. |
| `entry_time` | exchange time | Entry timestamp or session label. | For event strategies, date alone is not enough. |
| `exit_time` | exchange time | Exit timestamp or session label. | Same-date open/close rows need time labels. |
| `equity_value` | currency/index | Equity value at a trade/event row. | May be normalized rather than account currency. |
| `price_pnl` | currency/index | Price PnL per unit or row-level price contribution. | Not the same as portfolio return. |
| `holding_period` | count/time | Holding duration for a position or leg. | Frequency determines the unit. |

### Portfolio And WFA Detail Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `asset` | symbol | Asset represented by a row. | Symbol/provider mapping still matters. |
| `asset_count` | count | Number of assets in a portfolio snapshot. | Does not show concentration. |
| `asset_summary` | object/list | Per-asset WFA summary rows. | Sort by contribution before reading winners. |
| `allocation` | list | Snapshot allocation rows. | Allocation is exposure state, not performance by itself. |
| `allocation_by_window` | list | WFA allocation summary grouped by OOS window. | Compare only within the same WFA pack. |
| `contribution` | list | Asset contribution rows for a snapshot. | Residual cash/cost can prevent exact summing. |
| `contribution_by_window` | list | WFA contribution rows grouped by OOS window. | Contribution can be negative even with positive weight. |
| `contributions` | list | Nested contribution rows. | Read with the parent window/date. |
| `weights` | list | Nested asset weight rows. | Weight does not equal return. |
| `avg_weight` | ratio | Average asset weight. | A high average can hide timing losses. |
| `last_weight` | ratio | Last observed asset weight. | Last state may not represent the whole window. |
| `active_days` | count | Days/rows where the asset had active exposure. | Zero can be valid for never-selected assets. |
| `active_windows` | count | WFA windows where the asset appeared. | Stability signal, not return proof. |
| `avg_exposure` | ratio | Average exposure in a WFA or portfolio slice. | Gross and net exposure can differ. |
| `checkpoint_count` | count | Rebalance/evaluation checkpoints. | Checkpoints do not all change holdings. |
| `total_turnover` | ratio | Total weight changed over a slice. | High turnover needs cost sensitivity. |
| `is_portfolio_wfa` | boolean | Whether WFA payload includes portfolio allocation/contribution panels. | False means single-asset or no portfolio artifact. |
| `is_risk_gate_event_count` | count | Risk gate events during IS/training slice. | IS risk gates do not prove OOS safety. |
| `risk_gate_summary` | object | Risk-gate diagnostic breakdown. | Missing means not generated, not no risk. |
| `linked_backtest` | object | Run/backtest link for a selected WFA row. | Link is a drill-down pointer, not extra validation. |
| `run_id` | id | App run identifier. | Different runs can use different code/data versions. |
| `backtest_id` | id | Backtest row/artifact identifier. | A row id is not a strategy name. |
| `oos_portfolio` | object | Portfolio snapshot for the OOS selected policy. | Use only with its window boundaries. |
| `oos_rebalance_count` | count | OOS rebalance count. | Count alone does not show turnover size. |
| `oos_avg_exposure` | ratio | Average OOS exposure. | Exposure can include short/gross effects. |
| `oos_avg_holdings` | count | Average OOS holdings. | Holdings count ignores weights. |
| `oos_total_turnover` | ratio | OOS total turnover. | Turnover is a cost-risk driver. |
| `oos_cost_drag` | ratio | OOS cost drag. | Missing is not zero. |
| `oos_risk_gate_event_count` | count | OOS risk gate events. | Events require inspection, not automatic rejection. |
| `oos_risk_gate_summary` | object | OOS risk-gate diagnostic breakdown. | Compare against configured gate policy. |
| `mean_avg_weight` | ratio | Mean of average weights across WFA windows. | Can hide unstable window-by-window allocation. |
| `mean_last_weight` | ratio | Mean of final weights across WFA windows. | Last weights are endpoint states. |
| `mean_is_sharpe` | ratio | Mean IS Sharpe across windows/groups. | IS evidence is not OOS proof. |
| `mean_is_calmar` | ratio | Mean IS Calmar across windows/groups. | Can overstate optimized stability. |
| `mean_oos_sharpe` | ratio | Mean OOS Sharpe across selected windows/groups. | Average hides weak individual windows. |
| `mean_oos_calmar` | ratio | Mean OOS Calmar across selected windows/groups. | Sensitive to drawdown estimation. |
| `selection_evidence` | text | Why a WFA row/candidate was selected or included. | Diagnostics may not be official selected rows. |
| `selection_source` | text | Source of selection decision. | Source explains provenance, not quality. |
| `selection_rank` | ordinal | Rank used by the selection process. | Rank depends on configured objective. |
| `selection_metric` | text/number | Metric used for selection. | Must match the WFA configuration. |
| `wfa_row_type` | text | Selected or diagnostic row category. | Only selected rows should drive WFA verdicts. |
| `wfa_pack_inclusion_reason` | text | Why a combo appears in the WFA pack. | Inclusion is not acceptance. |

### Parameter Matrix And Search Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `acceptance` | text | Acceptance/rejection status from pre-review gates. | Pre-review is screening, not final validation. |
| `accepted` | boolean | Whether a candidate/group passed acceptance filters. | Acceptance depends on configured thresholds. |
| `acceptance_reasons` | list | Reasons supporting or rejecting acceptance. | Read reasons before trusting the badge. |
| `accepted_candidate_count` | count | Count of candidates passing acceptance gates. | Already covered candidates are not new OOS proof. |
| `best_robust_score` | score | Best robust score in a study summary. | Score formula depends on ranking config. |
| `candidate_key` | id | Stable candidate identifier. | Key identity is not human strategy logic. |
| `combo_key` | id | Stable key for a parameter combination. | Use the human combo/rules for interpretation. |
| `cluster_count` | count | Number of parameter clusters. | More clusters can mean less stability. |
| `cluster_method` | text | Method used to cluster candidates. | Method choice changes grouping. |
| `cluster_summary` | list | Cluster-level candidate summaries. | Cluster summaries are screening aids. |
| `completed_trials` | count | Completed optimizer/search trials. | Pruned/failed trials are excluded. |
| `importance` | score | Parameter importance score. | Importance is local to this search. |
| `parameter` | text | Parameter name. | Parameter meaning comes from strategy config. |
| `unique_values` | count | Distinct values tested for a parameter. | A narrow grid limits conclusions. |
| `max_drawdown_floor` | ratio | Acceptance floor for maximum drawdown. | Sign convention matters. |
| `min_oos_is_ratio` | ratio | Minimum OOS/IS ratio threshold. | Too strict can reject good but conservative runs. |
| `min_profit_factor` | ratio | Minimum profit factor threshold. | Undefined losses need special handling. |
| `min_trade_count` | count | Minimum trade count threshold. | More trades do not guarantee quality. |
| `min_win_rate` | ratio | Minimum win-rate threshold. | Win rate without payoff ratio can mislead. |
| `multivariate` | boolean | Whether optimizer sampled parameters jointly. | Joint sampling affects importance interpretation. |
| `n_trials` | count | Requested optimizer/search trials. | Requested count may differ from completed count. |
| `n_startup_trials` | count | Random/startup trials before model-based search. | Startup settings affect optimizer behavior. |
| `objective` | text | Main search objective. | Objective must match the research question. |
| `pruned_trials` | count | Trials stopped early. | Pruning can bias what completed results show. |
| `sampler` | text | Optimizer sampler. | Different samplers explore differently. |
| `select_default` | boolean | Candidate selected by default in UI. | Default selection is not final approval. |
| `suggested_for_wfa` | boolean | Candidate suggested for WFA validation. | Suggestion means validate next. |
| `rank` | ordinal | Candidate rank. | Rank is objective-dependent. |
| `size` | count | Cluster or group size. | Size does not equal quality. |
| `representative_type` | text | How a representative candidate was chosen. | Representative may not be the single best row. |
| `representative_params` | object | Parameters for a representative candidate. | Read alongside selection source. |
| `representative_combo_label` | text | Human label for representative parameters. | Label is display text, not full config. |
| `stability_score` | score | Stability diagnostic score. | A stable bad strategy is still bad. |
| `stability_std` | ratio | Standard deviation used for stability. | Lower is not always better without return context. |
| `sort_priority` | list | Ranking sort priority. | Changing priority changes winners. |
| `top_n_candidates` | count | Candidate count selected for robust review. | Top-N cutoff can hide nearby alternatives. |
| `timeout_seconds` | seconds | Search timeout. | Timeouts can make search incomplete. |
| `profile` | text | Ranking/acceptance profile name. | Profile is a policy label. |
| `pick` | text | Robust selection pick rule. | Pick rule controls which candidate becomes representative. |
| `ranking` | object | Ranking config block. | Explain the active weights/priority. |
| `ranking_config` | object | Effective ranking config in payload. | It may differ from defaults. |
| `robust_selection` | object | Robust candidate selection config. | This is screening logic, not OOS validation. |
| `pre_review_acceptance_config` | object | Acceptance gates used before WFA. | Gates are configurable and should be reported. |
| `future_live_search_config` | object | Saved search config for future/live-style searches. | It is a config artifact, not a live-trading command. |
| `aggregation_modes` | list | Available heatmap aggregation modes. | Aggregation changes cell values. |
| `reduction_modes` | list | Available reduction modes. | Reduction can hide distribution shape. |
| `axis_values` | object | Available parameter values per axis. | Missing values mean not searched. |
| `param_axes` | list | Parameter axes available for the heatmap/table. | Single-axis payloads use table view. |
| `objectives` | list | Metrics/objectives available in the matrix. | Objective choice changes ranking. |
| `default_x_axis` | text | Default X-axis parameter. | Axis choice is display, not analysis. |
| `default_y_axis` | text | Default Y-axis parameter. | Single-axis runs may not have a Y axis. |
| `search_source_options` | list | Available result sources for matrix review. | Source selection changes visible candidates. |
| `default_search_source` | text | Default candidate source. | Do not assume all sources are loaded. |
| `selected_representative_mode` | text | Active representative selection mode. | Mode changes shortlist composition. |
| `ml_search_status` | text | Optimizer/search availability status. | Disabled/unavailable is not necessarily failure. |
| `plateau_summary` | object | Local plateau diagnostics. | Plateau evidence is stability context. |
| `top_cells` | list | Highest-scoring heatmap cells in plateau diagnostics. | Top cells are still in-sample unless WFA-linked. |
| `shortlist_rows` | list | Robust shortlist rows. | Shortlist is a next-validation queue. |
| `study_summary` | object | Search/study summary. | Check trial counts and warnings. |

### Generic Payload And UI Contract Fields

| Field | Unit | Meaning | Misread warning |
| --- | --- | --- | --- |
| `id` | id | Generic nested row or option identifier. | IDs are technical handles. |
| `label` | text | Display label. | Labels can abbreviate full strategy logic. |
| `name` | text | Template or object name. | Name is not proof of current config. |
| `params` | object | Parameter values. | Always inspect all parameters, not only label text. |
| `rows` | list | Main row collection in a payload. | Row type depends on the page. |
| `source` | text | Source label or artifact source. | Source is provenance, not quality. |
| `source_filename` | path/name | Filename used as a config/source hint. | Filename may be copied or renamed. |
| `source_row_count` | count | Number of source rows used. | Low counts limit confidence. |
| `config_path` | path | Config file path. | User workspace paths are local and ignored by Git unless tracked. |
| `schema_version` | version | Payload/schema version. | Old schema versions may need rerun. |
| `artifact_type` | text | Runtime artifact type. | Different artifacts answer different questions. |
| `result_type` | text | Result category. | Category controls what charts/tables apply. |
| `availability` | text | Availability state for a panel/payload. | Unavailable means not generated or unsupported. |
| `reason` | text | Human-readable reason for state/acceptance. | Always read before assuming error. |
| `warnings` | list | Warning messages from payload/study. | Warnings should lead the interpretation. |
| `templates` | list | Available review templates. | Template choice changes gates. |
| `default_template_name` | text | Default review template. | Default is policy, not universal truth. |
| `is_default` | boolean | Whether an item is the default. | Default does not mean recommended for every strategy. |
| `enabled` | boolean | Whether a feature/config is enabled. | Disabled features should not be inferred from results. |
| `mode` | text | Generic mode flag. | Meaning depends on the parent object. |
| `note` | text | Optional explanatory note. | Notes can be stale if copied manually. |
| `updated_at` | timestamp | Last update timestamp. | Timezone/provenance may matter. |
| `date_range_start` | date | Start date for a result row. | Compare with effective data start. |
| `date_range_end` | date | End date for a result row. | Different end dates make metrics incomparable. |
| `strategy_id` | id | Strategy identifier. | Identifier is not full strategy logic. |
| `strategy_display_label` | text | Human-readable strategy label. | Use rules/config for exact behavior. |

## AI-Readable Pack Fields

| Field | Meaning | Misread warning |
| --- | --- | --- |
| `source_payloads` | Embedded app payload files. | Payloads are generated views, not new calculations. |
| `artifact_table_profiles` | Column/type/sample profiles for manifest artifacts. | Samples are for inspection, not full data replacement. |
| `metric_field_catalog` | Auto-discovered numeric fields. | Future metrics appear here before docs catch up. |
| `payload_index` | Map of included JSON payloads. | Missing payload means not generated or not ready. |
