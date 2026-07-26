use crate::accounting::{
    apply_risk_gates, AccountingConfig, AccountingError, AccountingRiskGateEvent,
};
use crate::artifact_tables::write_result_rows_parquet;
use crate::computed_fields::{compute_fields, ComputedFieldError, ComputedFieldSpec};
use crate::result_validator::{
    validate_result_tables, ResultTableView, ResultValidationError, ResultValidationReport,
};
use crate::risk::{
    RiskControlError, RiskControlState, PERMANENT_STOP_ACTION, SHADOW_ACTION,
    SHADOW_RECOVERY_ARMED_ACTION, SHADOW_RECOVERY_RESUMED_ACTION,
};
use crate::selection::{run_rank_selection, RankSelectionInput, RankSelectionSummary};
use crate::simulation::{
    execute_target_weight_orders, maintenance_margin_breached, SettlementEvent,
    SettlementInstruction, SettlementLedger, SimulatedOrderEvent, SimulationError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct DailyRankAccountingInput {
    pub config: AccountingConfig,
    pub dates: Vec<String>,
    pub symbols: Vec<String>,
    pub close: Vec<f64>,
    #[serde(default)]
    pub open: Vec<f64>,
    #[serde(default)]
    pub execute_next_open: bool,
    #[serde(default)]
    pub market_fields: BTreeMap<String, Vec<f64>>,
    #[serde(default)]
    pub rebalance: Vec<bool>,
    #[serde(default)]
    pub eligible: Vec<bool>,
    #[serde(default)]
    pub score: Vec<f64>,
    #[serde(default)]
    pub ascending: bool,
    pub top_n: usize,
    #[serde(default)]
    pub short_bottom_n: usize,
    #[serde(default = "default_long_gross_exposure")]
    pub long_gross_exposure: f64,
    #[serde(default)]
    pub short_gross_exposure: f64,
    #[serde(default = "default_position_limit")]
    pub position_limit: f64,
    #[serde(default)]
    pub feature_specs: Vec<DailyRankFeatureSpec>,
    #[serde(default)]
    pub eligible_rule: Option<DailyRankConditionInput>,
    #[serde(default)]
    pub rank_by: Option<String>,
    #[serde(default)]
    pub target_change: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DailyRankBatchCandidateInput {
    pub candidate_id: String,
    #[serde(default)]
    pub resolved_params: BTreeMap<String, String>,
    #[serde(default)]
    pub eligible: Vec<bool>,
    #[serde(default)]
    pub score: Vec<f64>,
    #[serde(default)]
    pub rebalance: Vec<bool>,
    #[serde(default)]
    pub ascending: bool,
    pub top_n: usize,
    #[serde(default)]
    pub short_bottom_n: usize,
    #[serde(default = "default_long_gross_exposure")]
    pub long_gross_exposure: f64,
    #[serde(default)]
    pub short_gross_exposure: f64,
    #[serde(default = "default_position_limit")]
    pub position_limit: f64,
    #[serde(default)]
    pub feature_specs: Vec<DailyRankFeatureSpec>,
    #[serde(default)]
    pub eligible_rule: Option<DailyRankConditionInput>,
    #[serde(default)]
    pub rank_by: Option<String>,
    #[serde(default)]
    pub target_change: bool,
    #[serde(default)]
    pub execute_next_open: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DailyRankBatchInput {
    pub config: AccountingConfig,
    pub dates: Vec<String>,
    pub symbols: Vec<String>,
    pub close: Vec<f64>,
    #[serde(default)]
    pub open: Vec<f64>,
    #[serde(default)]
    pub market_fields: BTreeMap<String, Vec<f64>>,
    #[serde(default)]
    pub include_full_results: bool,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
    pub candidates: Vec<DailyRankBatchCandidateInput>,
}

pub type DailyRankFeatureSpec = ComputedFieldSpec;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DailyRankConditionInput {
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub left: Option<String>,
    #[serde(default)]
    pub op: Option<String>,
    #[serde(default)]
    pub right_field: Option<String>,
    #[serde(default)]
    pub right: Option<f64>,
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub all: Vec<DailyRankConditionInput>,
    #[serde(default)]
    pub any: Vec<DailyRankConditionInput>,
    #[serde(default)]
    pub not: Option<Box<DailyRankConditionInput>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DailyRankAccountingEvent {
    pub date: String,
    pub rebalance: bool,
    pub equity_after_trade: f64,
    pub portfolio_return: f64,
    pub turnover: f64,
    pub trade_cost: f64,
    pub borrow_cost: f64,
    pub cash_weight: f64,
    pub gross_exposure: f64,
    pub active_positions: usize,
    pub target_weights: Vec<f64>,
    pub executed_weights: Vec<f64>,
    pub before_weights: Vec<f64>,
    pub contribution: Vec<f64>,
    pub selected_indices: Vec<usize>,
    pub ranked_indices: Vec<usize>,
    pub decision_row: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub orders: Vec<SimulatedOrderEvent>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub settlements: Vec<SettlementInstruction>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DailyRankAccountingSummary {
    pub start_equity: f64,
    pub final_equity: f64,
    pub total_return: f64,
    pub days: usize,
    pub active_rebalances: usize,
    pub average_turnover: f64,
    pub average_gross_exposure: f64,
    pub risk_gate_events: Vec<AccountingRiskGateEvent>,
    pub settlement_events: Vec<SettlementEvent>,
    pub selection: RankSelectionSummary,
    pub events: Vec<DailyRankAccountingEvent>,
    pub result_tables: DailyRankResultTables,
    pub result_validation: ResultValidationReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct DailyRankCompactResult {
    pub candidate_id: String,
    pub resolved_params: BTreeMap<String, String>,
    pub final_equity: f64,
    pub total_return: f64,
    pub days: usize,
    pub active_rebalances: usize,
    pub average_turnover: f64,
    pub average_gross_exposure: f64,
    pub result_validation: ResultValidationReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<DailyRankAccountingSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DailyRankBatchSummary {
    pub candidate_count: usize,
    pub results: Vec<DailyRankCompactResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_bundle: Option<DailyRankRustArtifactBundle>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DailyRankRustArtifactBundle {
    pub schema_version: String,
    pub artifact_type: String,
    pub run_id: String,
    pub candidate_count: usize,
    pub bundle_paths: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DailyRankResultTables {
    pub schema_version: String,
    pub equity_curve: Vec<BTreeMap<String, Value>>,
    pub holdings: Vec<BTreeMap<String, Value>>,
    pub rebalance_audit: Vec<BTreeMap<String, Value>>,
    pub rebalance_trades: Vec<BTreeMap<String, Value>>,
    pub risk_gate_events: Vec<BTreeMap<String, Value>>,
    pub settlements: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Error)]
pub enum DailyRankAccountingError {
    #[error("daily rank input requires non-empty dates and symbols")]
    InvalidShape,
    #[error("close/eligible/score arrays must match dates * symbols")]
    InvalidArrayLength,
    #[error("non-finite close price at row {row}, col {col}")]
    NonFiniteClose { row: usize, col: usize },
    #[error("market price must be positive at row {row}, col {col}")]
    NonPositivePrice { row: usize, col: usize },
    #[error("non-finite derived return at row {row}, col {col}")]
    NonFiniteDerivedReturn { row: usize, col: usize },
    #[error("non-finite accounting state at row {row}, col {col}")]
    NonFiniteAccountingState { row: usize, col: usize },
    #[error("unsupported daily rank feature: {0}")]
    UnsupportedFeature(String),
    #[error("unknown daily rank field: {0}")]
    UnknownField(String),
    #[error("invalid daily rank feature period for {0}")]
    InvalidFeaturePeriod(String),
    #[error(transparent)]
    ComputedField(#[from] ComputedFieldError),
    #[error("unsupported daily rank comparator: {0}")]
    UnsupportedComparator(String),
    #[error("invalid daily rank condition: {0}")]
    InvalidCondition(String),
    #[error(transparent)]
    Accounting(#[from] AccountingError),
    #[error(transparent)]
    RiskControl(#[from] RiskControlError),
    #[error(transparent)]
    Selection(#[from] crate::selection::RankSelectionError),
    #[error(transparent)]
    Simulation(#[from] SimulationError),
    #[error(transparent)]
    ResultValidation(#[from] ResultValidationError),
    #[error("artifact export failed: {0}")]
    ArtifactExport(String),
}

fn map_computed_field_error(error: ComputedFieldError) -> DailyRankAccountingError {
    match error {
        ComputedFieldError::UnsupportedOperation(operation) => {
            DailyRankAccountingError::UnsupportedFeature(operation)
        }
        ComputedFieldError::UnknownField(field) => DailyRankAccountingError::UnknownField(field),
        invalid @ ComputedFieldError::InvalidParameter(_) => {
            DailyRankAccountingError::ComputedField(invalid)
        }
    }
}

fn default_position_limit() -> f64 {
    1.0
}

fn default_long_gross_exposure() -> f64 {
    1.0
}

pub fn run_daily_rank_accounting(
    mut input: DailyRankAccountingInput,
) -> Result<DailyRankAccountingSummary, DailyRankAccountingError> {
    materialize_rust_producer_fields(&mut input)?;
    validate_input(&input)?;
    validate_accounting_config(&input.config)?;
    let rows = input.dates.len();
    let cols = input.symbols.len();
    let selection = run_rank_selection(RankSelectionInput {
        rows,
        cols,
        eligible: input.eligible.clone(),
        score: input.score.clone(),
        ascending: input.ascending,
        top_n: input.top_n,
        short_bottom_n: input.short_bottom_n,
        long_gross_exposure: input.long_gross_exposure,
        short_gross_exposure: input.short_gross_exposure,
        position_limit: input.position_limit,
    })?;

    let mut pre_trade_returns = vec![0.0; rows * cols];
    let mut post_trade_returns = vec![0.0; rows * cols];
    for row in 1..rows {
        for col in 0..cols {
            let previous_close = input.close[(row - 1) * cols + col];
            let current_open = if input.execute_next_open {
                input.open[row * cols + col]
            } else {
                input.close[row * cols + col]
            };
            let pre_trade_return = current_open / previous_close - 1.0;
            if !pre_trade_return.is_finite() {
                return Err(DailyRankAccountingError::NonFiniteDerivedReturn { row, col });
            }
            pre_trade_returns[row * cols + col] = pre_trade_return;
            if input.execute_next_open {
                let post_trade_return = input.close[row * cols + col] / current_open - 1.0;
                if !post_trade_return.is_finite() {
                    return Err(DailyRankAccountingError::NonFiniteDerivedReturn { row, col });
                }
                post_trade_returns[row * cols + col] = post_trade_return;
            }
        }
    }

    let start_equity = input.config.starting_equity;
    let mut equity = start_equity;
    let mut previous_weights = vec![0.0; cols];
    let mut events = Vec::with_capacity(rows);
    let mut turnover_sum = 0.0;
    let mut gross_sum = 0.0;
    let mut active_rebalances = 0usize;
    let mut equity_peak = equity;
    let mut risk_gate_events = Vec::new();
    let symbol_set = input.symbols.iter().cloned().collect::<BTreeSet<_>>();
    let mut risk_control = RiskControlState::default();
    let mut shadow_equity = 0.0;
    let mut shadow_weights = vec![0.0; cols];
    let mut settlement_ledger = SettlementLedger::default();

    for row in 0..rows {
        if row > 0 {
            settlement_ledger.advance_session();
        }
        let row_start = row * cols;
        let returns_row = &pre_trade_returns[row_start..row_start + cols];
        let post_returns_row = &post_trade_returns[row_start..row_start + cols];
        let pre_return_weights = previous_weights.clone();
        let mut contribution = vec![0.0; cols];
        let mut daily_return = 0.0;
        for col in 0..cols {
            contribution[col] = pre_return_weights[col] * returns_row[col];
            daily_return += contribution[col];
        }
        if row > 0 {
            equity *= 1.0 + daily_return;
            let denominator = 1.0 + daily_return;
            if denominator > 0.0 {
                for col in 0..cols {
                    let drifted = pre_return_weights[col] * (1.0 + returns_row[col]) / denominator;
                    if !drifted.is_finite() {
                        return Err(DailyRankAccountingError::NonFiniteAccountingState {
                            row,
                            col,
                        });
                    }
                    previous_weights[col] = drifted;
                }
            }
        }
        if equity > equity_peak {
            equity_peak = equity;
        }

        let before_weights = previous_weights.clone();
        let decision_row = if input.execute_next_open {
            row.saturating_sub(1)
        } else {
            row
        };
        let has_executable_decision = !input.execute_next_open || row > 0;
        let decision_start = decision_row * cols;
        let is_rebalance = has_executable_decision && input.rebalance[decision_row];
        let mut selected_target_weights = if is_rebalance {
            selection.target_weights[decision_start..decision_start + cols].to_vec()
        } else {
            before_weights.clone()
        };
        let before_map = vector_weights_to_map(&input.symbols, &before_weights);
        let maintenance_liquidation =
            maintenance_margin_breached(&before_map, &input.config.simulated_account);
        let execute_rebalance = is_rebalance || maintenance_liquidation;
        if maintenance_liquidation {
            selected_target_weights.fill(0.0);
            risk_gate_events.push(AccountingRiskGateEvent {
                time: input.dates[row].clone(),
                gate: "maintenance_margin".to_string(),
                threshold: 1.0,
                observed: before_weights.iter().map(|value| value.abs()).sum::<f64>()
                    * input.config.simulated_account.maintenance_margin_ratio,
                action: "margin_liquidation".to_string(),
                affected_assets: input.symbols.clone(),
                resulting_target_weights: BTreeMap::new(),
            });
        }
        if risk_control.is_shadow() {
            let shadow_return = shadow_weights
                .iter()
                .zip(returns_row.iter())
                .map(|(weight, asset_return)| weight * asset_return)
                .sum::<f64>();
            if row > 0 {
                shadow_equity *= 1.0 + shadow_return;
                let denominator = 1.0 + shadow_return;
                if denominator > 0.0 {
                    for col in 0..cols {
                        shadow_weights[col] =
                            shadow_weights[col] * (1.0 + returns_row[col]) / denominator;
                    }
                }
            }
            if is_rebalance {
                let shadow_turnover = selected_target_weights
                    .iter()
                    .zip(shadow_weights.iter())
                    .map(|(target, before)| (target - before).abs())
                    .sum::<f64>();
                shadow_equity *= (1.0 - shadow_turnover * input.config.cost_rate).max(0.0);
                shadow_weights = selected_target_weights.clone();
            }
            if input.execute_next_open && row > 0 {
                let shadow_post_return = shadow_weights
                    .iter()
                    .zip(post_returns_row.iter())
                    .map(|(weight, asset_return)| weight * asset_return)
                    .sum::<f64>();
                shadow_equity *= 1.0 + shadow_post_return;
                let denominator = 1.0 + shadow_post_return;
                if denominator > 0.0 {
                    for col in 0..cols {
                        shadow_weights[col] =
                            shadow_weights[col] * (1.0 + post_returns_row[col]) / denominator;
                    }
                }
                let shadow_short_gross = shadow_weights
                    .iter()
                    .filter(|weight| **weight < 0.0)
                    .map(|weight| weight.abs())
                    .sum::<f64>();
                shadow_equity *= (1.0
                    - shadow_short_gross * input.config.short_borrow_rate_annual
                        / input.config.borrow_day_count as f64)
                    .max(0.0);
            }
            if risk_control.observe_shadow_equity(shadow_equity) {
                risk_gate_events.push(AccountingRiskGateEvent {
                    time: input.dates[row].clone(),
                    gate: "max_drawdown".to_string(),
                    threshold: risk_control.recovery_target(),
                    observed: shadow_equity,
                    action: SHADOW_RECOVERY_ARMED_ACTION.to_string(),
                    affected_assets: input.symbols.clone(),
                    resulting_target_weights: vector_weights_to_map(
                        &input.symbols,
                        &shadow_weights,
                    ),
                });
            }
        }
        let target_map = vector_weights_to_map(&input.symbols, &selected_target_weights);
        let (adjusted_map, mut row_risk_events) =
            if risk_control.is_shadow() && risk_control.recovery_armed() && is_rebalance {
                let recovery_target = risk_control.recovery_target();
                risk_control.resume_on_next_action();
                equity_peak = equity;
                risk_gate_events.push(AccountingRiskGateEvent {
                    time: input.dates[row].clone(),
                    gate: "max_drawdown".to_string(),
                    threshold: recovery_target,
                    observed: equity,
                    action: SHADOW_RECOVERY_RESUMED_ACTION.to_string(),
                    affected_assets: input.symbols.clone(),
                    resulting_target_weights: target_map.clone(),
                });
                apply_risk_gates(
                    &input.config.risk_gates,
                    &symbol_set,
                    &before_map,
                    &target_map,
                    equity,
                    equity_peak,
                    daily_return,
                    &input.dates[row],
                )
            } else if risk_control.live_orders_allowed() {
                apply_risk_gates(
                    &input.config.risk_gates,
                    &symbol_set,
                    &before_map,
                    &target_map,
                    equity,
                    equity_peak,
                    daily_return,
                    &input.dates[row],
                )
            } else {
                (BTreeMap::new(), Vec::new())
            };
        if let Some(event) = row_risk_events
            .iter()
            .find(|event| event.action == SHADOW_ACTION || event.action == PERMANENT_STOP_ACTION)
        {
            if event.action == SHADOW_ACTION {
                risk_control.activate(SHADOW_ACTION, equity_peak)?;
                let shadow_turnover = selected_target_weights
                    .iter()
                    .zip(before_weights.iter())
                    .map(|(target, before)| (target - before).abs())
                    .sum::<f64>();
                shadow_equity = equity * (1.0 - shadow_turnover * input.config.cost_rate).max(0.0);
                shadow_weights = selected_target_weights.clone();
                risk_control.observe_shadow_equity(shadow_equity);
            } else {
                risk_control.activate(PERMANENT_STOP_ACTION, equity_peak)?;
            }
        }
        risk_gate_events.append(&mut row_risk_events);
        let (target_weights, turnover, orders, settlements) = if execute_rebalance {
            let execution = execute_target_weight_orders(
                &format!("{}:rank", input.dates[row]),
                &before_map,
                &adjusted_map,
                &input.config.simulated_venue,
                &input.config.simulated_account,
            )?;
            for settlement in &execution.settlements {
                settlement_ledger.submit(settlement.clone());
            }
            (
                map_weights_to_vector(&input.symbols, &execution.resulting_weights),
                execution.turnover,
                execution.orders,
                execution.settlements,
            )
        } else {
            (before_weights.clone(), 0.0, Vec::new(), Vec::new())
        };
        let trade_cost = if turnover > 0.0 && input.config.cost_rate > 0.0 {
            let cost = equity * turnover * input.config.cost_rate;
            equity *= (1.0 - turnover * input.config.cost_rate).max(0.0);
            cost
        } else {
            0.0
        };
        let executed_weights = target_weights.clone();
        let mut final_weights = executed_weights.clone();
        let post_return = if input.execute_next_open && row > 0 {
            let value = executed_weights
                .iter()
                .zip(post_returns_row.iter())
                .map(|(weight, asset_return)| weight * asset_return)
                .sum::<f64>();
            equity *= 1.0 + value;
            let denominator = 1.0 + value;
            if denominator > 0.0 {
                for col in 0..cols {
                    contribution[col] += executed_weights[col] * post_returns_row[col];
                    let drifted =
                        executed_weights[col] * (1.0 + post_returns_row[col]) / denominator;
                    if !drifted.is_finite() {
                        return Err(DailyRankAccountingError::NonFiniteAccountingState {
                            row,
                            col,
                        });
                    }
                    final_weights[col] = drifted;
                }
            }
            value
        } else {
            0.0
        };
        daily_return = (1.0 + daily_return) * (1.0 + post_return) - 1.0;
        let borrow_basis = if input.execute_next_open {
            &executed_weights
        } else {
            &pre_return_weights
        };
        let short_gross = borrow_basis
            .iter()
            .filter(|weight| **weight < 0.0)
            .map(|weight| weight.abs())
            .sum::<f64>();
        let borrow_cost = if short_gross > 0.0 && input.config.short_borrow_rate_annual > 0.0 {
            let cost = equity * short_gross * input.config.short_borrow_rate_annual
                / input.config.borrow_day_count as f64;
            equity = (equity - cost).max(0.0);
            cost
        } else {
            0.0
        };
        previous_weights = final_weights.clone();
        equity_peak = equity_peak.max(equity);
        let gross_exposure = previous_weights
            .iter()
            .map(|value| value.abs())
            .sum::<f64>();
        let cash_weight = (1.0 - previous_weights.iter().sum::<f64>()).max(0.0);
        let active_positions = previous_weights
            .iter()
            .filter(|value| value.abs() > 1e-12)
            .count();
        if turnover > 1e-12 {
            active_rebalances += 1;
        }
        turnover_sum += turnover;
        gross_sum += gross_exposure;
        let selected_indices = executed_weights
            .iter()
            .enumerate()
            .filter_map(
                |(idx, value)| {
                    if value.abs() > 1e-12 {
                        Some(idx)
                    } else {
                        None
                    }
                },
            )
            .collect();
        events.push(DailyRankAccountingEvent {
            date: input.dates[row].clone(),
            rebalance: execute_rebalance,
            equity_after_trade: equity,
            portfolio_return: daily_return,
            turnover,
            trade_cost,
            borrow_cost,
            cash_weight,
            gross_exposure,
            active_positions,
            target_weights: final_weights,
            executed_weights,
            before_weights,
            contribution,
            selected_indices,
            ranked_indices: if is_rebalance {
                selection.ranked_indices[decision_row].clone()
            } else {
                Vec::new()
            },
            decision_row,
            orders,
            settlements,
        });
    }

    let settlement_events = settlement_ledger.events().to_vec();
    let result_tables = build_result_tables(&input, &events, &risk_gate_events, &settlement_events);
    let result_validation = validate_result_tables(ResultTableView {
        result_schema_version: &result_tables.schema_version,
        equity_curve: &result_tables.equity_curve,
        holdings: &result_tables.holdings,
        rebalance_audit: &result_tables.rebalance_audit,
        rebalance_trades: &result_tables.rebalance_trades,
        risk_gate_events: &result_tables.risk_gate_events,
        settlements: &result_tables.settlements,
    })?;
    Ok(DailyRankAccountingSummary {
        start_equity,
        final_equity: equity,
        total_return: equity / start_equity - 1.0,
        days: rows,
        active_rebalances,
        average_turnover: turnover_sum / rows as f64,
        average_gross_exposure: gross_sum / rows as f64,
        risk_gate_events,
        settlement_events,
        selection,
        events,
        result_tables,
        result_validation,
    })
}

pub fn run_daily_rank_accounting_batch(
    input: DailyRankBatchInput,
) -> Result<DailyRankBatchSummary, DailyRankAccountingError> {
    let export_artifacts = input
        .artifact_output_dir
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let mut results = Vec::with_capacity(input.candidates.len());
    let mut full_summaries: Vec<(String, DailyRankAccountingSummary)> = Vec::new();
    let mut seen_ids: BTreeMap<String, usize> = BTreeMap::new();

    for (idx, candidate) in input.candidates.into_iter().enumerate() {
        let candidate_id = if candidate.candidate_id.trim().is_empty() {
            format!("candidate_{idx}")
        } else {
            candidate.candidate_id
        };
        let suffix = seen_ids.entry(candidate_id.clone()).or_insert(0);
        let unique_id = if *suffix == 0 {
            candidate_id
        } else {
            format!("{candidate_id}_{}", *suffix)
        };
        *suffix += 1;
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: input.config.clone(),
            dates: input.dates.clone(),
            symbols: input.symbols.clone(),
            close: input.close.clone(),
            open: input.open.clone(),
            execute_next_open: candidate.execute_next_open,
            market_fields: input.market_fields.clone(),
            rebalance: candidate.rebalance,
            eligible: candidate.eligible,
            score: candidate.score,
            ascending: candidate.ascending,
            top_n: candidate.top_n,
            short_bottom_n: candidate.short_bottom_n,
            long_gross_exposure: candidate.long_gross_exposure,
            short_gross_exposure: candidate.short_gross_exposure,
            position_limit: candidate.position_limit,
            feature_specs: candidate.feature_specs,
            eligible_rule: candidate.eligible_rule,
            rank_by: candidate.rank_by,
            target_change: candidate.target_change,
        })?;
        if export_artifacts {
            full_summaries.push((unique_id.clone(), summary.clone()));
        }
        results.push(DailyRankCompactResult {
            candidate_id: unique_id,
            resolved_params: candidate.resolved_params,
            final_equity: summary.final_equity,
            total_return: summary.total_return,
            days: summary.days,
            active_rebalances: summary.active_rebalances,
            average_turnover: summary.average_turnover,
            average_gross_exposure: summary.average_gross_exposure,
            result_validation: summary.result_validation.clone(),
            summary: if input.include_full_results && !export_artifacts {
                Some(summary)
            } else {
                None
            },
        });
    }
    let artifact_bundle = if export_artifacts {
        Some(export_daily_rank_bundle(
            input.artifact_output_dir.as_deref().unwrap_or_default(),
            input
                .artifact_run_id
                .as_deref()
                .unwrap_or("daily_rank_matrix"),
            &full_summaries,
        )?)
    } else {
        None
    };
    Ok(DailyRankBatchSummary {
        candidate_count: results.len(),
        results,
        artifact_bundle,
    })
}

fn export_daily_rank_bundle(
    output_dir: &str,
    run_id: &str,
    summaries: &[(String, DailyRankAccountingSummary)],
) -> Result<DailyRankRustArtifactBundle, DailyRankAccountingError> {
    let output_path = PathBuf::from(output_dir);
    fs::create_dir_all(&output_path)
        .map_err(|exc| DailyRankAccountingError::ArtifactExport(exc.to_string()))?;
    let safe_run_id = slugify(run_id);
    let mut bundle_paths = BTreeMap::new();
    let table_specs = [
        ("equity_curve", "equity_curve"),
        ("holdings", "holdings"),
        ("rebalance_audit", "rebalance_audit"),
        ("rebalance_trades", "rebalance_trades"),
        ("risk_gate_events", "risk_gate_events"),
        ("settlements", "settlements"),
    ];
    for (table_key, file_key) in table_specs {
        let rows = combined_daily_rank_rows(summaries, table_key);
        let path = output_path.join(format!("{safe_run_id}_{file_key}.parquet"));
        write_result_rows_parquet(&path, &rows, table_key)
            .map_err(DailyRankAccountingError::ArtifactExport)?;
        bundle_paths.insert(file_key.to_string(), path.to_string_lossy().to_string());
    }
    Ok(DailyRankRustArtifactBundle {
        schema_version: "rust_portfolio_result_bundle.v1".to_string(),
        artifact_type: "rust_daily_rank_matrix_bundle".to_string(),
        run_id: safe_run_id,
        candidate_count: summaries.len(),
        bundle_paths,
    })
}

fn combined_daily_rank_rows(
    summaries: &[(String, DailyRankAccountingSummary)],
    table_key: &str,
) -> Vec<BTreeMap<String, Value>> {
    let mut out = Vec::new();
    for (candidate_id, summary) in summaries {
        let rows = match table_key {
            "equity_curve" => &summary.result_tables.equity_curve,
            "holdings" => &summary.result_tables.holdings,
            "rebalance_audit" => &summary.result_tables.rebalance_audit,
            "rebalance_trades" => &summary.result_tables.rebalance_trades,
            "risk_gate_events" => &summary.result_tables.risk_gate_events,
            "settlements" => &summary.result_tables.settlements,
            _ => continue,
        };
        for row in rows {
            let mut enriched = row.clone();
            enriched.insert(
                "Backtest_id".to_string(),
                Value::String(candidate_id.clone()),
            );
            out.push(enriched);
        }
    }
    out
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut previous_underscore = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            previous_underscore = false;
        } else if !previous_underscore {
            out.push('_');
            previous_underscore = true;
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "daily_rank_matrix".to_string()
    } else {
        trimmed
    }
}

fn materialize_rust_producer_fields(
    input: &mut DailyRankAccountingInput,
) -> Result<(), DailyRankAccountingError> {
    let rows = input.dates.len();
    let cols = input.symbols.len();
    if rows == 0 || cols == 0 {
        return Ok(());
    }
    let expected_len = rows * cols;
    if input.rebalance.is_empty() && !input.target_change {
        input.rebalance = vec![true; rows];
    }
    if input.close.len() != expected_len {
        return Ok(());
    }
    if input.eligible.len() == expected_len && input.score.len() == expected_len {
        return Ok(());
    }

    let fields = compute_feature_fields_with_dates_and_market_fields(
        &input.close,
        &input.market_fields,
        &input.dates,
        rows,
        cols,
        &input.feature_specs,
    )?;

    if input.eligible.len() != expected_len {
        input.eligible = if let Some(rule) = &input.eligible_rule {
            evaluate_condition(rule, &fields, rows, cols)?
        } else {
            vec![true; expected_len]
        };
    }
    if input.score.len() != expected_len {
        let rank_by = input
            .rank_by
            .as_deref()
            .ok_or_else(|| {
                DailyRankAccountingError::InvalidCondition(
                    "rank_by is required when score is not supplied".to_string(),
                )
            })?
            .trim()
            .to_lowercase();
        input.score = fields
            .get(&rank_by)
            .ok_or_else(|| DailyRankAccountingError::UnknownField(rank_by.clone()))?
            .clone();
    }
    if input.target_change {
        input.rebalance = target_change_flags(RankSelectionInput {
            rows,
            cols,
            eligible: input.eligible.clone(),
            score: input.score.clone(),
            ascending: input.ascending,
            top_n: input.top_n,
            short_bottom_n: input.short_bottom_n,
            long_gross_exposure: input.long_gross_exposure,
            short_gross_exposure: input.short_gross_exposure,
            position_limit: input.position_limit,
        })?;
    }
    Ok(())
}

fn target_change_flags(
    selection_input: RankSelectionInput,
) -> Result<Vec<bool>, DailyRankAccountingError> {
    let rows = selection_input.rows;
    let cols = selection_input.cols;
    let mut flags = vec![false; rows];
    let selection = run_rank_selection(selection_input)?;
    let mut previous = vec![0.0_f64; cols];
    for (row, flag) in flags.iter_mut().enumerate() {
        let offset = row * cols;
        let target = &selection.target_weights[offset..offset + cols];
        *flag = row == 0
            || target
                .iter()
                .zip(&previous)
                .any(|(left, right)| (left - right).abs() > 1e-12);
        previous.copy_from_slice(target);
    }
    Ok(flags)
}

#[cfg(test)]
pub(crate) fn compute_feature_fields(
    close: &[f64],
    rows: usize,
    cols: usize,
    feature_specs: &[DailyRankFeatureSpec],
) -> Result<BTreeMap<String, Vec<f64>>, DailyRankAccountingError> {
    compute_feature_fields_with_market_fields(close, &BTreeMap::new(), rows, cols, feature_specs)
}

pub(crate) fn compute_feature_fields_with_market_fields(
    close: &[f64],
    market_fields: &BTreeMap<String, Vec<f64>>,
    rows: usize,
    cols: usize,
    feature_specs: &[DailyRankFeatureSpec],
) -> Result<BTreeMap<String, Vec<f64>>, DailyRankAccountingError> {
    compute_feature_fields_with_dates_and_market_fields(
        close,
        market_fields,
        &[],
        rows,
        cols,
        feature_specs,
    )
}

pub(crate) fn compute_feature_fields_with_dates_and_market_fields(
    close: &[f64],
    market_fields: &BTreeMap<String, Vec<f64>>,
    dates: &[String],
    rows: usize,
    cols: usize,
    feature_specs: &[DailyRankFeatureSpec],
) -> Result<BTreeMap<String, Vec<f64>>, DailyRankAccountingError> {
    compute_fields(close, market_fields, dates, rows, cols, feature_specs)
        .map_err(map_computed_field_error)
}

pub(crate) fn evaluate_condition(
    rule: &DailyRankConditionInput,
    fields: &BTreeMap<String, Vec<f64>>,
    rows: usize,
    cols: usize,
) -> Result<Vec<bool>, DailyRankAccountingError> {
    if !rule.all.is_empty() {
        let mut output = vec![true; rows * cols];
        for child in &rule.all {
            let mask = evaluate_condition(child, fields, rows, cols)?;
            for (value, child_value) in output.iter_mut().zip(mask) {
                *value = *value && child_value;
            }
        }
        return Ok(output);
    }
    if !rule.any.is_empty() {
        let mut output = vec![false; rows * cols];
        for child in &rule.any {
            let mask = evaluate_condition(child, fields, rows, cols)?;
            for (value, child_value) in output.iter_mut().zip(mask) {
                *value = *value || child_value;
            }
        }
        return Ok(output);
    }
    if let Some(child) = &rule.not {
        return Ok(evaluate_condition(child, fields, rows, cols)?
            .into_iter()
            .map(|value| !value)
            .collect());
    }
    let left_name = rule
        .left
        .as_deref()
        .or(rule.field.as_deref())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            DailyRankAccountingError::InvalidCondition(
                "leaf condition requires left or field".to_string(),
            )
        })?
        .trim()
        .to_lowercase();
    let left = fields
        .get(&left_name)
        .ok_or_else(|| DailyRankAccountingError::UnknownField(left_name.clone()))?;
    let right_field = rule
        .right_field
        .as_deref()
        .map(|value| value.trim().to_lowercase());
    let right_values = if let Some(name) = right_field {
        Some(
            fields
                .get(&name)
                .ok_or_else(|| DailyRankAccountingError::UnknownField(name.clone()))?,
        )
    } else {
        None
    };
    let scalar = if right_values.is_none() {
        rule.right.or(rule.value).ok_or_else(|| {
            DailyRankAccountingError::InvalidCondition(
                "leaf condition requires right_field, right, or value".to_string(),
            )
        })?
    } else {
        0.0
    };
    let op = rule
        .op
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            DailyRankAccountingError::InvalidCondition("leaf condition requires op".to_string())
        })?
        .trim()
        .to_lowercase();
    let mut output = vec![false; rows * cols];
    for idx in 0..rows * cols {
        let left_value = left[idx];
        let right_value = right_values.map_or(scalar, |values| values[idx]);
        output[idx] = if !left_value.is_finite() || !right_value.is_finite() {
            false
        } else {
            match op.as_str() {
                "gt" | ">" => left_value > right_value,
                "ge" | ">=" => left_value >= right_value,
                "lt" | "<" => left_value < right_value,
                "le" | "<=" => left_value <= right_value,
                "eq" | "==" => (left_value - right_value).abs() <= f64::EPSILON,
                "ne" | "!=" => (left_value - right_value).abs() > f64::EPSILON,
                "crosses_above" | "cross_up" => {
                    let row = idx / cols;
                    if row == 0 {
                        false
                    } else {
                        let previous = idx - cols;
                        let previous_left = left[previous];
                        let previous_right = right_values.map_or(scalar, |values| values[previous]);
                        previous_left.is_finite()
                            && previous_right.is_finite()
                            && previous_left <= previous_right
                            && left_value > right_value
                    }
                }
                "crosses_below" | "cross_down" => {
                    let row = idx / cols;
                    if row == 0 {
                        false
                    } else {
                        let previous = idx - cols;
                        let previous_left = left[previous];
                        let previous_right = right_values.map_or(scalar, |values| values[previous]);
                        previous_left.is_finite()
                            && previous_right.is_finite()
                            && previous_left >= previous_right
                            && left_value < right_value
                    }
                }
                _ => return Err(DailyRankAccountingError::UnsupportedComparator(op)),
            }
        }
    }
    Ok(output)
}

fn build_result_tables(
    input: &DailyRankAccountingInput,
    events: &[DailyRankAccountingEvent],
    risk_gate_events: &[AccountingRiskGateEvent],
    settlement_events: &[SettlementEvent],
) -> DailyRankResultTables {
    DailyRankResultTables {
        schema_version: "rust_daily_rank_result_tables.v1".to_string(),
        equity_curve: build_equity_rows(input, events),
        holdings: build_holding_rows(input, events),
        rebalance_audit: build_rebalance_rows(input, events),
        rebalance_trades: build_trade_rows(input, events),
        risk_gate_events: build_risk_gate_rows(risk_gate_events),
        settlements: build_settlement_rows(settlement_events),
    }
}

fn build_settlement_rows(events: &[SettlementEvent]) -> Vec<BTreeMap<String, Value>> {
    events
        .iter()
        .map(|event| {
            BTreeMap::from([
                ("Order_id".to_string(), json!(event.order_id)),
                ("Asset".to_string(), json!(event.asset)),
                (
                    "Remaining_sessions".to_string(),
                    json!(event.remaining_sessions),
                ),
                ("Cash_delta".to_string(), json!(event.cash_delta)),
                (
                    "Status".to_string(),
                    json!(match event.status {
                        crate::simulation::SettlementStatus::Pending => "pending",
                        crate::simulation::SettlementStatus::Settled => "settled",
                    }),
                ),
            ])
        })
        .collect()
}

fn build_equity_rows(
    input: &DailyRankAccountingInput,
    events: &[DailyRankAccountingEvent],
) -> Vec<BTreeMap<String, Value>> {
    events
        .iter()
        .map(|event| {
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert(
                "Equity_value".to_string(),
                json_f64(event.equity_after_trade),
            );
            row.insert(
                "Portfolio_return".to_string(),
                json_f64(event.portfolio_return),
            );
            row.insert("Turnover".to_string(), json_f64(event.turnover));
            row.insert("Trade_cost".to_string(), json_f64(event.trade_cost));
            row.insert("Borrow_cost".to_string(), json_f64(event.borrow_cost));
            row.insert(
                "Cost_drag".to_string(),
                json_f64(event.trade_cost + event.borrow_cost),
            );
            row.insert("Selected_count".to_string(), json!(event.active_positions));
            row.insert("Gross_exposure".to_string(), json_f64(event.gross_exposure));
            row.insert(
                "Cash_weight".to_string(),
                json_f64(event.cash_weight.max(0.0)),
            );
            for (idx, symbol) in input.symbols.iter().enumerate() {
                row.insert(
                    format!("Weight_{symbol}"),
                    json_f64(*event.target_weights.get(idx).unwrap_or(&0.0)),
                );
                row.insert(
                    format!("Contribution_{symbol}"),
                    json_f64(*event.contribution.get(idx).unwrap_or(&0.0)),
                );
            }
            row
        })
        .collect()
}

fn build_holding_rows(
    input: &DailyRankAccountingInput,
    events: &[DailyRankAccountingEvent],
) -> Vec<BTreeMap<String, Value>> {
    let cols = input.symbols.len();
    let mut rows = Vec::new();
    for event in events {
        for (rank, asset_idx) in event.ranked_indices.iter().enumerate() {
            if *asset_idx >= cols {
                continue;
            }
            let symbol = &input.symbols[*asset_idx];
            let flat_idx = event.decision_row * cols + *asset_idx;
            let selected = event.selected_indices.contains(asset_idx);
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert("Asset".to_string(), json!(symbol));
            row.insert("Rank".to_string(), json!(rank + 1));
            row.insert("Selected".to_string(), json!(selected));
            row.insert(
                "Eligible".to_string(),
                json!(*input.eligible.get(flat_idx).unwrap_or(&false)),
            );
            row.insert(
                "Score".to_string(),
                json_f64(*input.score.get(flat_idx).unwrap_or(&f64::NAN)),
            );
            row.insert(
                "Target_weight".to_string(),
                json_f64(*event.target_weights.get(*asset_idx).unwrap_or(&0.0)),
            );
            rows.push(row);
        }
    }
    rows
}

fn build_rebalance_rows(
    input: &DailyRankAccountingInput,
    events: &[DailyRankAccountingEvent],
) -> Vec<BTreeMap<String, Value>> {
    events
        .iter()
        .filter(|event| event.rebalance)
        .map(|event| {
            let selected_assets = event
                .selected_indices
                .iter()
                .filter_map(|idx| input.symbols.get(*idx).cloned())
                .collect::<Vec<_>>();
            let ranked_assets = event
                .ranked_indices
                .iter()
                .filter_map(|idx| input.symbols.get(*idx).cloned())
                .collect::<Vec<_>>();
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert("Rebalance".to_string(), json!(true));
            row.insert("Selected_assets".to_string(), json!(selected_assets));
            row.insert(
                "Selected_count".to_string(),
                json!(event.selected_indices.len()),
            );
            row.insert("Ranked_candidates".to_string(), json!(ranked_assets));
            row.insert("Turnover".to_string(), json_f64(event.turnover));
            row.insert("Cost_rate".to_string(), json_f64(input.config.cost_rate));
            row.insert("Trade_cost".to_string(), json_f64(event.trade_cost));
            row.insert("Borrow_cost".to_string(), json_f64(event.borrow_cost));
            row.insert(
                "Equity_value".to_string(),
                json_f64(event.equity_after_trade),
            );
            row
        })
        .collect()
}

fn build_trade_rows(
    input: &DailyRankAccountingInput,
    events: &[DailyRankAccountingEvent],
) -> Vec<BTreeMap<String, Value>> {
    let cols = input.symbols.len();
    let mut rows = Vec::new();
    for event in events {
        let ranked_lookup = event
            .ranked_indices
            .iter()
            .enumerate()
            .map(|(rank, idx)| (*idx, rank + 1))
            .collect::<BTreeMap<_, _>>();
        for asset_idx in 0..cols {
            let before = *event.before_weights.get(asset_idx).unwrap_or(&0.0);
            let target = *event.executed_weights.get(asset_idx).unwrap_or(&0.0);
            let delta = target - before;
            let abs_delta = delta.abs();
            if abs_delta <= 1e-12 && target <= 1e-12 && before <= 1e-12 {
                continue;
            }
            let action = if target < -1e-12 && before >= -1e-12 {
                "new_short"
            } else if before < -1e-12 && target >= -1e-12 {
                "close_short"
            } else if delta > 1e-12 {
                "buy"
            } else if delta < -1e-12 && target <= 1e-12 {
                "exit"
            } else if delta < -1e-12 {
                "sell"
            } else {
                "hold"
            };
            let allocated_cost = if event.turnover > 0.0 {
                event.trade_cost * abs_delta / event.turnover
            } else {
                0.0
            };
            let flat_idx = event.decision_row * cols + asset_idx;
            let rank = ranked_lookup.get(&asset_idx).copied();
            let eligible = *input.eligible.get(flat_idx).unwrap_or(&false);
            let selected = event.selected_indices.contains(&asset_idx);
            let mut reason_parts = Vec::new();
            if let Some(rank) = rank {
                reason_parts.push(format!("rank {rank}"));
            } else if before > 0.0 && target <= 0.0 {
                reason_parts.push("not selected at this rebalance".to_string());
            }
            if eligible {
                reason_parts.push("eligible".to_string());
            } else if action != "hold" {
                reason_parts.push("not eligible".to_string());
            }
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert("Asset".to_string(), json!(input.symbols[asset_idx]));
            row.insert("Before_weight".to_string(), json_f64(before));
            row.insert("Target_weight".to_string(), json_f64(target));
            row.insert("Trade_delta".to_string(), json_f64(delta));
            row.insert("Action".to_string(), json!(action));
            row.insert("Trade_turnover".to_string(), json_f64(abs_delta));
            row.insert("Allocated_cost".to_string(), json_f64(allocated_cost));
            row.insert("Selected".to_string(), json!(selected));
            row.insert("Eligible".to_string(), json!(eligible));
            row.insert(
                "Rank".to_string(),
                rank.map_or(Value::Null, |value| json!(value)),
            );
            row.insert(
                "Score".to_string(),
                json_f64(*input.score.get(flat_idx).unwrap_or(&f64::NAN)),
            );
            row.insert(
                "Reason".to_string(),
                json!(if reason_parts.is_empty() {
                    "target unchanged".to_string()
                } else {
                    reason_parts.join("; ")
                }),
            );
            rows.push(row);
        }
    }
    rows
}

fn build_risk_gate_rows(
    risk_gate_events: &[AccountingRiskGateEvent],
) -> Vec<BTreeMap<String, Value>> {
    risk_gate_events
        .iter()
        .map(|event| {
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.time));
            row.insert("Gate".to_string(), json!(event.gate));
            row.insert("Threshold".to_string(), json_f64(event.threshold));
            row.insert("Observed".to_string(), json_f64(event.observed));
            row.insert("Action".to_string(), json!(event.action));
            row.insert("Affected_assets".to_string(), json!(event.affected_assets));
            row.insert(
                "Resulting_target_weights".to_string(),
                json!(event.resulting_target_weights),
            );
            row
        })
        .collect()
}

fn json_f64(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else {
        Value::Null
    }
}

fn vector_weights_to_map(symbols: &[String], weights: &[f64]) -> BTreeMap<String, f64> {
    symbols
        .iter()
        .zip(weights.iter())
        .filter_map(|(asset, value)| {
            if value.abs() > 1e-12 {
                Some((asset.clone(), *value))
            } else {
                None
            }
        })
        .collect()
}

fn map_weights_to_vector(symbols: &[String], weights: &BTreeMap<String, f64>) -> Vec<f64> {
    symbols
        .iter()
        .map(|asset| *weights.get(asset).unwrap_or(&0.0))
        .collect()
}

fn validate_input(input: &DailyRankAccountingInput) -> Result<(), DailyRankAccountingError> {
    let rows = input.dates.len();
    let cols = input.symbols.len();
    if rows == 0 || cols == 0 {
        return Err(DailyRankAccountingError::InvalidShape);
    }
    let expected_len = rows * cols;
    if input.close.len() != expected_len
        || input.eligible.len() != expected_len
        || input.score.len() != expected_len
        || input.rebalance.len() != rows
    {
        return Err(DailyRankAccountingError::InvalidArrayLength);
    }
    if input.execute_next_open && input.open.len() != expected_len {
        return Err(DailyRankAccountingError::InvalidArrayLength);
    }
    for row in 0..rows {
        for col in 0..cols {
            if !input.close[row * cols + col].is_finite() {
                return Err(DailyRankAccountingError::NonFiniteClose { row, col });
            }
            if input.close[row * cols + col] <= 0.0 {
                return Err(DailyRankAccountingError::NonPositivePrice { row, col });
            }
            if input.execute_next_open && !input.open[row * cols + col].is_finite() {
                return Err(DailyRankAccountingError::NonFiniteClose { row, col });
            }
            if input.execute_next_open && input.open[row * cols + col] <= 0.0 {
                return Err(DailyRankAccountingError::NonPositivePrice { row, col });
            }
        }
    }
    Ok(())
}

fn validate_accounting_config(config: &AccountingConfig) -> Result<(), AccountingError> {
    if config.starting_equity <= 0.0 {
        return Err(AccountingError::InvalidStartingEquity);
    }
    if config.cost_rate < 0.0 {
        return Err(AccountingError::InvalidCostRate);
    }
    if config.short_borrow_rate_annual < 0.0 {
        return Err(AccountingError::InvalidShortBorrowRate);
    }
    if config.borrow_day_count == 0 {
        return Err(AccountingError::InvalidShortBorrowDayCount);
    }
    if config.max_gross_exposure <= 0.0 {
        return Err(AccountingError::InvalidMaxGrossExposure);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_positive_market_price_fails_closed() {
        let error = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig::default(),
            dates: vec!["2024-01-01".to_string()],
            symbols: vec!["AAA".to_string()],
            close: vec![0.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: Vec::new(),
            eligible: vec![true],
            score: vec![1.0],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect_err("zero prices must fail");

        assert!(matches!(
            error,
            DailyRankAccountingError::NonPositivePrice { row: 0, col: 0 }
        ));
    }

    #[test]
    fn non_finite_derived_return_fails_closed() {
        let error = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig::default(),
            dates: vec!["2024-01-01".to_string(), "2024-01-02".to_string()],
            symbols: vec!["AAA".to_string()],
            close: vec![f64::MIN_POSITIVE, f64::MAX],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: vec![true, true],
            eligible: vec![true, true],
            score: vec![1.0, 1.0],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect_err("overflowing derived returns must fail");

        assert!(matches!(
            error,
            DailyRankAccountingError::NonFiniteDerivedReturn { row: 1, col: 0 }
        ));
    }

    #[test]
    fn daily_rank_accounting_keeps_cash_from_position_limit() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig::default(),
            dates: vec!["2024-01-01".to_string(), "2024-01-02".to_string()],
            symbols: vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()],
            close: vec![100.0, 90.0, 80.0, 110.0, 95.0, 70.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: Vec::new(),
            eligible: vec![true, true, true, true, true, true],
            score: vec![100.0, 90.0, 80.0, 110.0, 95.0, 70.0],
            ascending: false,
            top_n: 2,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 0.4,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect("daily rank should run");

        assert_eq!(summary.events[0].selected_indices, vec![0, 1]);
        assert_eq!(summary.events[0].target_weights, vec![0.4, 0.4, 0.0]);
        assert!((summary.events[0].cash_weight - 0.2).abs() < 1e-12);
        assert!(summary.final_equity > 100.0);
        assert_eq!(
            summary.result_tables.schema_version,
            "rust_daily_rank_result_tables.v1"
        );
        assert_eq!(summary.result_tables.equity_curve.len(), 2);
        assert_eq!(summary.result_tables.rebalance_audit.len(), 2);
        assert!(!summary.result_tables.holdings.is_empty());
        assert!(!summary.result_tables.rebalance_trades.is_empty());
    }

    #[test]
    fn daily_rank_keeps_positions_between_scheduled_rebalances() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig::default(),
            dates: vec![
                "2024-01-30".to_string(),
                "2024-01-31".to_string(),
                "2024-02-01".to_string(),
                "2024-02-02".to_string(),
            ],
            symbols: vec!["AAA".to_string()],
            close: vec![100.0, 101.0, 102.0, 103.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: vec![true, false, true, false],
            eligible: vec![true; 4],
            score: vec![1.0; 4],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect("scheduled daily rank should run");

        assert_eq!(
            summary
                .events
                .iter()
                .map(|event| event.rebalance)
                .collect::<Vec<_>>(),
            vec![true, false, true, false]
        );
        assert_eq!(summary.result_tables.rebalance_audit.len(), 2);
        assert!(summary.events[1].turnover.abs() < 1e-12);
        assert!(summary.events[3].turnover.abs() < 1e-12);
    }

    #[test]
    fn daily_rank_accounting_applies_max_positions_risk_gate() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig {
                risk_gates: crate::accounting::AccountingRiskGateConfig {
                    max_positions: Some(1),
                    ..crate::accounting::AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            dates: vec!["2024-01-01".to_string()],
            symbols: vec!["AAA".to_string(), "BBB".to_string(), "CCC".to_string()],
            close: vec![100.0, 90.0, 80.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: Vec::new(),
            eligible: vec![true, true, true],
            score: vec![100.0, 90.0, 80.0],
            ascending: false,
            top_n: 3,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 0.4,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect("daily rank should run");

        assert_eq!(summary.risk_gate_events[0].gate, "max_positions");
        assert_eq!(summary.events[0].selected_indices, vec![0]);
        assert_eq!(summary.events[0].active_positions, 1);
    }

    #[test]
    fn daily_rank_permanent_stop_blocks_later_ranked_targets() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig {
                risk_gates: crate::accounting::AccountingRiskGateConfig {
                    max_drawdown: Some(0.10),
                    gate_action: Some("permanent_stop".to_string()),
                    ..crate::accounting::AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            dates: (1..=4).map(|day| format!("2024-01-0{day}")).collect(),
            symbols: vec!["AAA".to_string()],
            close: vec![100.0, 120.0, 90.0, 110.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: vec![true; 4],
            eligible: vec![true; 4],
            score: vec![1.0; 4],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect("permanent stop daily rank should run");

        assert_eq!(summary.risk_gate_events[0].action, "permanent_stop");
        assert!(summary.events[2]
            .target_weights
            .iter()
            .all(|weight| weight.abs() < 1e-12));
        assert!(summary.events[3]
            .target_weights
            .iter()
            .all(|weight| weight.abs() < 1e-12));
    }

    #[test]
    fn daily_rank_shadow_recovers_before_live_rank_resumes() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig {
                risk_gates: crate::accounting::AccountingRiskGateConfig {
                    max_drawdown: Some(0.10),
                    gate_action: Some("shadow_until_recovery".to_string()),
                    ..crate::accounting::AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            dates: (1..=4).map(|day| format!("2024-01-0{day}")).collect(),
            symbols: vec!["AAA".to_string()],
            close: vec![100.0, 120.0, 90.0, 120.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: vec![true; 4],
            eligible: vec![true; 4],
            score: vec![1.0; 4],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
        })
        .expect("shadow daily rank should run");

        assert_eq!(summary.risk_gate_events[0].action, "shadow_until_recovery");
        assert!(summary.events[2]
            .target_weights
            .iter()
            .all(|weight| weight.abs() < 1e-12));
        assert!((summary.events[3].target_weights[0] - 1.0).abs() < 1e-12);
        assert!(summary
            .risk_gate_events
            .iter()
            .any(|event| event.action == "shadow_recovery_resumed"));
    }

    #[test]
    fn daily_rank_producer_materializes_momentum_sma_eligibility() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig::default(),
            dates: vec![
                "2024-01-01".to_string(),
                "2024-01-02".to_string(),
                "2024-01-03".to_string(),
                "2024-01-04".to_string(),
            ],
            symbols: vec!["AAA".to_string(), "BBB".to_string()],
            close: vec![10.0, 10.0, 11.0, 9.0, 12.0, 8.0, 13.0, 7.0],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            target_change: false,
            rebalance: Vec::new(),
            eligible: Vec::new(),
            score: Vec::new(),
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: vec![
                DailyRankFeatureSpec {
                    name: "return_momentum".to_string(),
                    op: "indicator.momentum".to_string(),
                    source: Some("close".to_string()),
                    period: Some(1),
                    ..DailyRankFeatureSpec::default()
                },
                DailyRankFeatureSpec {
                    name: "sma_filter".to_string(),
                    op: "indicator.sma".to_string(),
                    source: Some("close".to_string()),
                    period: Some(2),
                    ..DailyRankFeatureSpec::default()
                },
            ],
            eligible_rule: Some(DailyRankConditionInput {
                field: Some("close".to_string()),
                op: Some("gt".to_string()),
                right_field: Some("sma_filter".to_string()),
                ..DailyRankConditionInput::default()
            }),
            rank_by: Some("return_momentum".to_string()),
        })
        .expect("daily rank producer should run");

        assert_eq!(summary.events[1].selected_indices, vec![0]);
        assert_eq!(summary.events[2].selected_indices, vec![0]);
        assert_eq!(summary.events[3].selected_indices, vec![0]);
        assert_eq!(summary.result_tables.equity_curve.len(), 4);
    }

    #[test]
    fn feature_kernel_covers_public_close_based_indicators() {
        let close = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let specs = vec![
            DailyRankFeatureSpec {
                name: "ema".to_string(),
                op: "indicator.ema".to_string(),
                source: Some("close".to_string()),
                period: Some(3),
                ..Default::default()
            },
            DailyRankFeatureSpec {
                name: "zscore".to_string(),
                op: "indicator.zscore".to_string(),
                source: Some("close".to_string()),
                period: Some(3),
                ..Default::default()
            },
            DailyRankFeatureSpec {
                name: "percentile".to_string(),
                op: "indicator.percentile".to_string(),
                source: Some("close".to_string()),
                period: Some(3),
                percentile: Some(50.0),
                ..Default::default()
            },
            DailyRankFeatureSpec {
                name: "bollinger".to_string(),
                op: "indicator.bollinger".to_string(),
                source: Some("close".to_string()),
                period: Some(3),
                stddev: Some(2.0),
                band: Some("upper".to_string()),
                ..Default::default()
            },
            DailyRankFeatureSpec {
                name: "rsi".to_string(),
                op: "indicator.rsi".to_string(),
                source: Some("close".to_string()),
                period: Some(3),
                ..Default::default()
            },
            DailyRankFeatureSpec {
                name: "volatility".to_string(),
                op: "indicator.volatility".to_string(),
                source: Some("close".to_string()),
                period: Some(3),
                annualize: Some(true),
                ..Default::default()
            },
            DailyRankFeatureSpec {
                name: "macd".to_string(),
                op: "indicator.macd".to_string(),
                source: Some("close".to_string()),
                fastperiod: Some(2),
                slowperiod: Some(3),
                signalperiod: Some(2),
                output: Some("histogram".to_string()),
                ..Default::default()
            },
        ];

        let fields = compute_feature_fields(&close, 6, 1, &specs).unwrap();

        assert!((fields["ema"][5] - 5.03125).abs() < 1e-12);
        assert!((fields["zscore"][5] - 1.0).abs() < 1e-12);
        assert!((fields["percentile"][5] - 5.0).abs() < 1e-12);
        assert!((fields["bollinger"][5] - 7.0).abs() < 1e-12);
        assert_eq!(fields["rsi"][5], 100.0);
        let returns = [1.0 / 3.0, 0.25, 0.2];
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let expected_volatility = (returns
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (returns.len() - 1) as f64)
            .sqrt()
            * 252.0_f64.sqrt();
        assert!((fields["volatility"][5] - expected_volatility).abs() < 1e-12);
        assert!(fields["macd"][5].is_finite());
    }

    #[test]
    fn atr_uses_ohlc_true_range_and_wilder_average() {
        let close = vec![10.0, 12.0, 13.0, 17.0];
        let market_fields = BTreeMap::from([
            ("high".to_string(), vec![11.0, 13.0, 14.0, 18.0]),
            ("low".to_string(), vec![9.0, 11.0, 12.0, 16.0]),
        ]);
        let spec = DailyRankFeatureSpec {
            name: "atr".to_string(),
            op: "indicator.atr".to_string(),
            source: Some("close".to_string()),
            high_source: Some("high".to_string()),
            low_source: Some("low".to_string()),
            close_source: Some("close".to_string()),
            period: Some(3),
            method: Some("wilder".to_string()),
            ..Default::default()
        };

        let fields = compute_feature_fields_with_market_fields(
            &close,
            &market_fields,
            close.len(),
            1,
            &[spec],
        )
        .unwrap();

        assert!(fields["atr"][0].is_nan());
        assert!(fields["atr"][1].is_nan());
        assert!((fields["atr"][2] - 7.0 / 3.0).abs() < 1e-12);
        assert!((fields["atr"][3] - 29.0 / 9.0).abs() < 1e-12);
    }

    #[test]
    fn atr_fails_closed_when_ohlc_market_fields_are_missing() {
        let error = compute_feature_fields(
            &[10.0, 11.0, 12.0],
            3,
            1,
            &[DailyRankFeatureSpec {
                name: "atr".to_string(),
                op: "indicator.atr".to_string(),
                source: Some("close".to_string()),
                high_source: Some("high".to_string()),
                low_source: Some("low".to_string()),
                close_source: Some("close".to_string()),
                period: Some(2),
                method: Some("wilder".to_string()),
                ..Default::default()
            }],
        )
        .unwrap_err();

        assert!(matches!(error, DailyRankAccountingError::UnknownField(field) if field == "high"));
    }

    #[test]
    fn condition_kernel_composes_all_any_and_not_without_strategy_paths() {
        let fields = BTreeMap::from([
            ("close".to_string(), vec![1.0, 2.0, 3.0]),
            ("floor".to_string(), vec![0.0, 2.5, 2.5]),
        ]);
        let above_floor = DailyRankConditionInput {
            left: Some("close".to_string()),
            op: Some("gt".to_string()),
            right_field: Some("floor".to_string()),
            ..Default::default()
        };
        let below_two = DailyRankConditionInput {
            left: Some("close".to_string()),
            op: Some("lt".to_string()),
            right: Some(2.0),
            ..Default::default()
        };
        let rule = DailyRankConditionInput {
            any: vec![
                below_two.clone(),
                DailyRankConditionInput {
                    all: vec![
                        above_floor,
                        DailyRankConditionInput {
                            not: Some(Box::new(below_two)),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            evaluate_condition(&rule, &fields, 3, 1).unwrap(),
            vec![true, false, true]
        );
    }

    #[test]
    fn condition_kernel_rejects_missing_leaf_fields_instead_of_defaulting() {
        let fields = BTreeMap::from([("close".to_string(), vec![1.0])]);

        let missing_left =
            evaluate_condition(&DailyRankConditionInput::default(), &fields, 1, 1).unwrap_err();
        assert!(matches!(
            missing_left,
            DailyRankAccountingError::InvalidCondition(message)
                if message.contains("left or field")
        ));

        let missing_right = evaluate_condition(
            &DailyRankConditionInput {
                left: Some("close".to_string()),
                op: Some("gt".to_string()),
                ..Default::default()
            },
            &fields,
            1,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            missing_right,
            DailyRankAccountingError::InvalidCondition(message)
                if message.contains("right_field")
        ));

        let missing_op = evaluate_condition(
            &DailyRankConditionInput {
                left: Some("close".to_string()),
                right: Some(0.0),
                ..Default::default()
            },
            &fields,
            1,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            missing_op,
            DailyRankAccountingError::InvalidCondition(message)
                if message.contains("requires op")
        ));
    }

    #[test]
    fn target_change_rebalances_only_when_desired_weights_change() {
        let flags = target_change_flags(RankSelectionInput {
            rows: 4,
            cols: 2,
            eligible: vec![true; 8],
            score: vec![4.0, 1.0, 5.0, 2.0, 1.0, 6.0, 0.0, 7.0],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
        })
        .unwrap();

        assert_eq!(flags, vec![true, false, true, false]);
    }

    #[test]
    fn calendar_return_uses_completed_month_ends_and_12_1_lags() {
        let dates = vec![
            "2024-01-30",
            "2024-01-31",
            "2024-02-28",
            "2024-02-29",
            "2024-03-28",
            "2024-03-29",
            "2024-04-29",
            "2024-04-30",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let close = vec![10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 18.0];
        let fields = compute_feature_fields_with_dates_and_market_fields(
            &close,
            &BTreeMap::new(),
            &dates,
            dates.len(),
            1,
            &[DailyRankFeatureSpec {
                name: "calendar_momentum".to_string(),
                op: "indicator.calendar_return".to_string(),
                source: Some("close".to_string()),
                sampling: Some("month_end".to_string()),
                start_lag: Some(3),
                end_lag: Some(1),
                ..Default::default()
            }],
        )
        .expect("calendar return should run");

        let values = &fields["calendar_momentum"];
        assert!(values[5].is_nan());
        assert!((values[7] - (15.0 / 11.0 - 1.0)).abs() < 1e-12);
    }

    #[test]
    fn next_open_execution_does_not_capture_signal_day_return() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig::default(),
            dates: vec![
                "2024-01-31".to_string(),
                "2024-02-01".to_string(),
                "2024-02-02".to_string(),
            ],
            symbols: vec!["AAA".to_string(), "BBB".to_string()],
            close: vec![100.0, 100.0, 200.0, 100.0, 200.0, 100.0],
            open: vec![100.0, 100.0, 100.0, 100.0, 200.0, 100.0],
            execute_next_open: true,
            market_fields: BTreeMap::new(),
            rebalance: vec![true, false, false],
            eligible: vec![true; 6],
            score: vec![2.0, 1.0, 2.0, 1.0, 2.0, 1.0],
            ascending: false,
            top_n: 1,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
            target_change: false,
        })
        .expect("next-open daily rank should run");

        assert!(!summary.events[0].rebalance);
        assert!(summary.events[0]
            .target_weights
            .iter()
            .all(|weight| weight.abs() < 1e-12));
        assert!(summary.events[1].rebalance);
        assert_eq!(summary.events[1].decision_row, 0);
        assert!((summary.events[1].equity_after_trade - 200.0).abs() < 1e-12);
        assert!((summary.final_equity - 200.0).abs() < 1e-12);
    }

    #[test]
    fn short_borrow_cost_is_charged_once_per_held_session() {
        let summary = run_daily_rank_accounting(DailyRankAccountingInput {
            config: AccountingConfig {
                allow_short: true,
                short_borrow_rate_annual: 0.252,
                borrow_day_count: 252,
                simulated_account: crate::simulation::SimulatedAccountConfig {
                    account_type: crate::simulation::SimulatedAccountType::Margin,
                    leverage_limit: 1.0,
                    initial_margin_ratio: 1.0,
                    maintenance_margin_ratio: 0.25,
                    allow_short_borrow: true,
                    settlement_days: 0,
                },
                ..AccountingConfig::default()
            },
            dates: vec!["2024-01-02".to_string(), "2024-01-03".to_string()],
            symbols: vec!["AAA".to_string(), "BBB".to_string()],
            close: vec![100.0; 4],
            open: Vec::new(),
            execute_next_open: false,
            market_fields: BTreeMap::new(),
            rebalance: vec![true, false],
            eligible: vec![true; 4],
            score: vec![2.0, 1.0, 2.0, 1.0],
            ascending: false,
            top_n: 1,
            short_bottom_n: 1,
            long_gross_exposure: 0.5,
            short_gross_exposure: 0.5,
            position_limit: 0.5,
            feature_specs: Vec::new(),
            eligible_rule: None,
            rank_by: None,
            target_change: false,
        })
        .expect("long-short daily rank should run");

        assert!(summary.events[0].borrow_cost.abs() < 1e-12);
        assert!((summary.events[1].borrow_cost - 0.05).abs() < 1e-12);
        assert!((summary.final_equity - 99.95).abs() < 1e-12);
    }
}
