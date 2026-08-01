use crate::artifact_tables::write_result_rows_parquet;
use crate::computed_fields::returns::simple_return;
use crate::result_validator::{
    validate_result_tables, ResultTableView, ResultValidationError, ResultValidationReport,
};
use crate::risk::{
    RiskControlError, RiskControlState, PERMANENT_STOP_ACTION, SHADOW_ACTION,
    SHADOW_RECOVERY_ARMED_ACTION, SHADOW_RECOVERY_RESUMED_ACTION,
};
use crate::session_progress::SessionProgress;
use crate::simulation::{
    execute_target_weight_orders, maintenance_margin_breached, SettlementEvent,
    SettlementInstruction, SettlementLedger, SimulatedAccountConfig, SimulatedOrderEvent,
    SimulatedVenueConfig, SimulationError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum AccountingError {
    #[error("starting equity must be positive")]
    InvalidStartingEquity,
    #[error("cost rate cannot be negative")]
    InvalidCostRate,
    #[error("short borrow rate cannot be negative")]
    InvalidShortBorrowRate,
    #[error("short borrow day count must be positive")]
    InvalidShortBorrowDayCount,
    #[error("max gross exposure must be positive")]
    InvalidMaxGrossExposure,
    #[error("non-finite value in {field} for {asset}")]
    NonFiniteValue { field: &'static str, asset: String },
    #[error("missing return for held asset {0}")]
    MissingHeldAssetReturn(String),
    #[error("negative target weight for {0} is not allowed in long-only accounting")]
    NegativeWeightLongOnly(String),
    #[error("target gross exposure {actual:.6} exceeds configured max {limit:.6}")]
    GrossExposureExceeded { actual: f64, limit: f64 },
    #[error("accounting input requires at least one checkpoint")]
    EmptyCheckpoints,
    #[error("artifact export failed: {0}")]
    ArtifactExport(String),
    #[error("unsupported risk gate action: {0}")]
    InvalidRiskAction(String),
    #[error("risk gate action is required when a loss or drawdown gate is configured")]
    MissingRiskGateAction,
    #[error("reduce_exposure requires reduce_exposure_factor in (0, 1]")]
    InvalidReduceExposureFactor,
    #[error("invalid session progression: {0}")]
    InvalidSessionProgress(String),
    #[error(transparent)]
    RiskControl(#[from] RiskControlError),
    #[error(transparent)]
    Simulation(#[from] SimulationError),
    #[error(transparent)]
    ResultValidation(#[from] ResultValidationError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingConfig {
    pub starting_equity: f64,
    pub cost_rate: f64,
    pub max_gross_exposure: f64,
    pub allow_short: bool,
    #[serde(default)]
    pub short_borrow_rate_annual: f64,
    #[serde(default = "default_borrow_day_count")]
    pub borrow_day_count: u32,
    #[serde(default)]
    pub session_label_by_event_time: BTreeMap<String, String>,
    #[serde(default)]
    pub risk_gates: AccountingRiskGateConfig,
    #[serde(default)]
    pub simulated_venue: SimulatedVenueConfig,
    #[serde(default)]
    pub simulated_account: SimulatedAccountConfig,
}

impl Default for AccountingConfig {
    fn default() -> Self {
        Self {
            starting_equity: 100.0,
            cost_rate: 0.0,
            max_gross_exposure: 1.0,
            allow_short: false,
            short_borrow_rate_annual: 0.0,
            borrow_day_count: default_borrow_day_count(),
            session_label_by_event_time: BTreeMap::new(),
            risk_gates: AccountingRiskGateConfig::default(),
            simulated_venue: SimulatedVenueConfig::default(),
            simulated_account: SimulatedAccountConfig::default(),
        }
    }
}

fn default_borrow_day_count() -> u32 {
    252
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountingRiskGateConfig {
    pub max_positions: Option<usize>,
    pub max_daily_loss: Option<f64>,
    pub max_order_size: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub gate_action: Option<String>,
    pub reduce_exposure_factor: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointInput {
    pub time: String,
    #[serde(default = "default_rebalance")]
    pub rebalance: bool,
    pub returns: BTreeMap<String, f64>,
    pub target_weights: BTreeMap<String, f64>,
    #[serde(default)]
    pub selected_assets: Vec<String>,
    #[serde(default)]
    pub ranked_assets: Vec<String>,
    #[serde(default)]
    pub score: BTreeMap<String, f64>,
    #[serde(default)]
    pub eligible: BTreeMap<String, bool>,
    #[serde(default)]
    pub rank_by: Option<String>,
}

fn default_rebalance() -> bool {
    true
}

impl Default for CheckpointInput {
    fn default() -> Self {
        Self {
            time: String::new(),
            rebalance: default_rebalance(),
            returns: BTreeMap::new(),
            target_weights: BTreeMap::new(),
            selected_assets: Vec::new(),
            ranked_assets: Vec::new(),
            score: BTreeMap::new(),
            eligible: BTreeMap::new(),
            rank_by: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingInput {
    pub config: AccountingConfig,
    pub checkpoints: Vec<CheckpointInput>,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingEvent {
    pub time: String,
    pub session_label: String,
    pub equity_before_trade: f64,
    pub equity_after_trade: f64,
    pub portfolio_return: f64,
    pub turnover: f64,
    pub cost_drag: f64,
    pub borrow_cost: f64,
    pub cash_weight: f64,
    pub gross_exposure: f64,
    pub active_positions: usize,
    pub target_weights: BTreeMap<String, f64>,
    pub drift_weights: BTreeMap<String, f64>,
    pub contribution: BTreeMap<String, f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orders: Vec<SimulatedOrderEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settlements: Vec<SettlementInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingRiskGateEvent {
    pub time: String,
    pub gate: String,
    pub threshold: f64,
    pub observed: f64,
    pub action: String,
    pub affected_assets: Vec<String>,
    pub resulting_target_weights: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingSummary {
    pub start_equity: f64,
    pub final_equity: f64,
    pub total_return: f64,
    pub checkpoints: usize,
    pub active_rebalances: usize,
    pub average_turnover: f64,
    pub average_gross_exposure: f64,
    pub risk_gate_events: Vec<AccountingRiskGateEvent>,
    pub settlement_events: Vec<SettlementEvent>,
    pub events: Vec<AccountingEvent>,
    pub result_tables: AccountingResultTables,
    pub result_validation: ResultValidationReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_bundle: Option<AccountingRustArtifactBundle>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountingResultTables {
    pub schema_version: String,
    pub equity_curve: Vec<BTreeMap<String, Value>>,
    pub holdings: Vec<BTreeMap<String, Value>>,
    pub rebalance_audit: Vec<BTreeMap<String, Value>>,
    pub rebalance_trades: Vec<BTreeMap<String, Value>>,
    pub risk_gate_events: Vec<BTreeMap<String, Value>>,
    pub settlements: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountingRustArtifactBundle {
    pub schema_version: String,
    pub artifact_type: String,
    pub run_id: String,
    pub candidate_count: usize,
    pub bundle_paths: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
struct DrawdownRecoveryState {
    control: RiskControlState,
    shadow_equity: f64,
    shadow_weights: BTreeMap<String, f64>,
    shadow_cash_weight: f64,
}

pub fn run_accounting(input: AccountingInput) -> Result<AccountingSummary, AccountingError> {
    validate_config(&input.config)?;
    if input.checkpoints.is_empty() {
        return Err(AccountingError::EmptyCheckpoints);
    }
    let artifact_output_dir = input.artifact_output_dir.clone();
    let artifact_run_id = input.artifact_run_id.clone();

    let mut equity = input.config.starting_equity;
    let start_equity = equity;
    let mut previous_weights: BTreeMap<String, f64> = BTreeMap::new();
    let mut previous_cash_weight = 1.0;
    let mut events = Vec::with_capacity(input.checkpoints.len());
    let mut active_rebalances = 0usize;
    let mut turnover_sum = 0.0;
    let mut gross_sum = 0.0;
    let mut equity_peak = equity;
    let mut risk_gate_events = Vec::new();
    let mut drawdown_recovery = DrawdownRecoveryState::default();
    let mut settlement_ledger = SettlementLedger::default();
    let mut session_progress = SessionProgress::default();
    let result_contexts = input
        .checkpoints
        .iter()
        .map(AccountingResultContext::from_checkpoint)
        .collect::<Vec<_>>();

    for checkpoint in input.checkpoints {
        let session = session_progress
            .observe(&checkpoint.time, &input.config.session_label_by_event_time)
            .map_err(AccountingError::InvalidSessionProgress)?;
        if session.advanced {
            settlement_ledger.advance_session();
        }
        validate_checkpoint(&checkpoint, &input.config)?;
        validate_held_asset_returns(&checkpoint.returns, &previous_weights)?;
        let assets = asset_union(
            &previous_weights,
            &checkpoint.returns,
            &checkpoint.target_weights,
        );

        let equity_before_return = equity;
        let mut asset_values: BTreeMap<String, f64> = BTreeMap::new();
        let mut contribution = BTreeMap::new();
        let mut pre_trade_equity = equity_before_return * previous_cash_weight;
        for asset in &assets {
            let previous_weight = *previous_weights.get(asset).unwrap_or(&0.0);
            let asset_return = *checkpoint.returns.get(asset).unwrap_or(&0.0);
            let value_before = equity_before_return * previous_weight;
            let value_after = value_before * (1.0 + asset_return);
            asset_values.insert(asset.clone(), value_after);
            contribution.insert(asset.clone(), previous_weight * asset_return);
            pre_trade_equity += value_after;
        }

        let portfolio_return = if equity_before_return > 0.0 {
            simple_return(pre_trade_equity, equity_before_return)
        } else {
            0.0
        };
        let short_gross = previous_weights
            .values()
            .filter(|weight| **weight < 0.0)
            .map(|weight| weight.abs())
            .sum::<f64>();
        let borrow_cost =
            if session.advanced && short_gross > 0.0 && input.config.short_borrow_rate_annual > 0.0
            {
                pre_trade_equity * short_gross * input.config.short_borrow_rate_annual
                    / input.config.borrow_day_count as f64
            } else {
                0.0
            };
        pre_trade_equity = (pre_trade_equity - borrow_cost).max(0.0);
        if pre_trade_equity > equity_peak {
            equity_peak = pre_trade_equity;
        }

        let drift_weights = if pre_trade_equity > 0.0 {
            asset_values
                .iter()
                .map(|(asset, value)| (asset.clone(), value / pre_trade_equity))
                .collect::<BTreeMap<_, _>>()
        } else {
            BTreeMap::new()
        };

        if drawdown_recovery.control.is_shadow() {
            let newly_armed =
                update_shadow_recovery_state(&mut drawdown_recovery, &checkpoint, &input.config)?;
            if newly_armed {
                risk_gate_events.push(risk_gate_event(
                    &checkpoint.time,
                    "max_drawdown",
                    drawdown_recovery.control.recovery_target(),
                    drawdown_recovery.shadow_equity,
                    SHADOW_RECOVERY_ARMED_ACTION.to_string(),
                    drawdown_recovery.shadow_weights.keys().cloned().collect(),
                    &drawdown_recovery.shadow_weights,
                ));
            }
        }

        let maintenance_liquidation =
            maintenance_margin_breached(&drift_weights, &input.config.simulated_account);
        if maintenance_liquidation {
            risk_gate_events.push(risk_gate_event(
                &checkpoint.time,
                "maintenance_margin",
                1.0,
                drift_weights.values().map(|value| value.abs()).sum::<f64>()
                    * input.config.simulated_account.maintenance_margin_ratio,
                "margin_liquidation".to_string(),
                drift_weights.keys().cloned().collect(),
                &BTreeMap::new(),
            ));
        }
        let execute_rebalance = checkpoint.rebalance || maintenance_liquidation;
        let requested_target_weights = if maintenance_liquidation {
            BTreeMap::new()
        } else if checkpoint.rebalance {
            normalized_target_weights(&checkpoint.target_weights, &input.config)?
        } else {
            drift_weights
                .iter()
                .filter_map(|(asset, value)| {
                    if value.abs() > 1e-12 {
                        Some((asset.clone(), *value))
                    } else {
                        None
                    }
                })
                .collect::<BTreeMap<_, _>>()
        };
        let mut target_weights = requested_target_weights.clone();
        if execute_rebalance {
            if !drawdown_recovery.control.live_orders_allowed() {
                if drawdown_recovery.control.is_shadow()
                    && drawdown_recovery.control.recovery_armed()
                {
                    risk_gate_events.push(risk_gate_event(
                        &checkpoint.time,
                        "max_drawdown",
                        drawdown_recovery.control.recovery_target(),
                        pre_trade_equity,
                        SHADOW_RECOVERY_RESUMED_ACTION.to_string(),
                        target_weights.keys().cloned().collect(),
                        &target_weights,
                    ));
                    drawdown_recovery.control.resume_on_next_action();
                    drawdown_recovery.shadow_equity = 0.0;
                    drawdown_recovery.shadow_weights.clear();
                    drawdown_recovery.shadow_cash_weight = 0.0;
                    equity_peak = pre_trade_equity;
                } else {
                    target_weights = BTreeMap::new();
                }
            }
            if drawdown_recovery.control.live_orders_allowed() {
                let (adjusted, mut events) = apply_risk_gates(
                    &input.config.risk_gates,
                    &assets,
                    &drift_weights,
                    &target_weights,
                    pre_trade_equity,
                    equity_peak,
                    portfolio_return,
                    &checkpoint.time,
                );
                target_weights = normalized_target_weights(&adjusted, &input.config)?;
                if let Some(event) = events.iter().find(|event| {
                    event.action == SHADOW_ACTION || event.action == PERMANENT_STOP_ACTION
                }) {
                    if event.action == SHADOW_ACTION {
                        drawdown_recovery = initialize_shadow_recovery_state(
                            pre_trade_equity,
                            equity_peak,
                            &drift_weights,
                            &requested_target_weights,
                            &input.config,
                        )?;
                    } else {
                        drawdown_recovery
                            .control
                            .activate(PERMANENT_STOP_ACTION, equity_peak)?;
                    }
                }
                risk_gate_events.append(&mut events);
            }
        }
        let (target_weights, turnover, orders, settlements) = if execute_rebalance {
            let execution = execute_target_weight_orders(
                &format!("{}:rebalance", checkpoint.time),
                &drift_weights,
                &target_weights,
                &input.config.simulated_venue,
                &input.config.simulated_account,
            )?;
            for settlement in &execution.settlements {
                settlement_ledger.submit(settlement.clone());
            }
            (
                execution.resulting_weights,
                execution.turnover,
                execution.orders,
                execution.settlements,
            )
        } else {
            (target_weights, 0.0, Vec::new(), Vec::new())
        };
        let trade_cost_drag = turnover * input.config.cost_rate;
        let borrow_cost_drag = if equity_before_return > 0.0 {
            borrow_cost / equity_before_return
        } else {
            0.0
        };
        let cost_drag = trade_cost_drag + borrow_cost_drag;
        let equity_after_trade = pre_trade_equity * (1.0 - trade_cost_drag);
        let gross_exposure = target_weights
            .values()
            .map(|value| value.abs())
            .sum::<f64>();
        let cash_weight = 1.0 - target_weights.values().sum::<f64>();
        let active_positions = target_weights
            .values()
            .filter(|value| value.abs() > 1e-12)
            .count();

        if turnover > 1e-12 {
            active_rebalances += 1;
        }
        turnover_sum += turnover;
        gross_sum += gross_exposure;

        events.push(AccountingEvent {
            time: checkpoint.time,
            session_label: session.label,
            equity_before_trade: pre_trade_equity,
            equity_after_trade,
            portfolio_return,
            turnover,
            cost_drag,
            borrow_cost,
            cash_weight,
            gross_exposure,
            active_positions,
            target_weights: target_weights.clone(),
            drift_weights,
            contribution,
            orders,
            settlements,
        });

        equity = equity_after_trade;
        previous_cash_weight = cash_weight;
        previous_weights = target_weights;
    }

    let checkpoints = events.len();
    let settlement_events = settlement_ledger.events().to_vec();
    let result_tables = build_result_tables(
        &events,
        &result_contexts,
        &risk_gate_events,
        &settlement_events,
        input.config.cost_rate,
    );
    let result_validation = validate_result_tables(ResultTableView {
        result_schema_version: &result_tables.schema_version,
        equity_curve: &result_tables.equity_curve,
        execution_equity_curve: &[],
        holdings: &result_tables.holdings,
        rebalance_audit: &result_tables.rebalance_audit,
        rebalance_trades: &result_tables.rebalance_trades,
        risk_gate_events: &result_tables.risk_gate_events,
        settlements: &result_tables.settlements,
    })?;
    let artifact_bundle = if let Some(output_dir) = artifact_output_dir.as_deref() {
        if output_dir.trim().is_empty() {
            None
        } else {
            Some(export_accounting_bundle(
                output_dir,
                artifact_run_id.as_deref().unwrap_or("accounting"),
                &result_tables,
            )?)
        }
    } else {
        None
    };
    Ok(AccountingSummary {
        start_equity,
        final_equity: equity,
        total_return: simple_return(equity, start_equity),
        checkpoints,
        active_rebalances,
        average_turnover: turnover_sum / checkpoints as f64,
        average_gross_exposure: gross_sum / checkpoints as f64,
        risk_gate_events,
        settlement_events,
        events,
        result_tables,
        result_validation,
        artifact_bundle,
    })
}

#[derive(Debug, Clone)]
struct AccountingResultContext {
    selected_assets: Vec<String>,
    ranked_assets: Vec<String>,
    score: BTreeMap<String, f64>,
    eligible: BTreeMap<String, bool>,
    rank_by: String,
    rebalance: bool,
}

impl AccountingResultContext {
    fn from_checkpoint(checkpoint: &CheckpointInput) -> Self {
        Self {
            selected_assets: checkpoint.selected_assets.clone(),
            ranked_assets: checkpoint.ranked_assets.clone(),
            score: checkpoint.score.clone(),
            eligible: checkpoint.eligible.clone(),
            rank_by: checkpoint.rank_by.clone().unwrap_or_default(),
            rebalance: checkpoint.rebalance,
        }
    }
}

fn build_result_tables(
    events: &[AccountingEvent],
    contexts: &[AccountingResultContext],
    risk_gate_events: &[AccountingRiskGateEvent],
    settlement_events: &[SettlementEvent],
    cost_rate: f64,
) -> AccountingResultTables {
    AccountingResultTables {
        schema_version: "rust_accounting_result_tables.v1".to_string(),
        equity_curve: build_equity_rows(events, cost_rate),
        holdings: build_holding_rows(events, contexts),
        rebalance_audit: build_rebalance_rows(events, contexts, cost_rate),
        rebalance_trades: build_trade_rows(events, contexts, cost_rate),
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

fn build_equity_rows(events: &[AccountingEvent], cost_rate: f64) -> Vec<BTreeMap<String, Value>> {
    let assets = events
        .iter()
        .flat_map(|event| {
            event
                .target_weights
                .keys()
                .chain(event.contribution.keys())
                .cloned()
        })
        .collect::<BTreeSet<_>>();
    events
        .iter()
        .map(|event| {
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.time));
            row.insert("Session_label".to_string(), json!(event.session_label));
            row.insert(
                "Equity_value".to_string(),
                json_f64(event.equity_after_trade),
            );
            row.insert(
                "Portfolio_return".to_string(),
                json_f64(event.portfolio_return),
            );
            row.insert("Turnover".to_string(), json_f64(event.turnover));
            row.insert(
                "Trade_cost".to_string(),
                json_f64(event.equity_before_trade * event.turnover * cost_rate),
            );
            row.insert("Borrow_cost".to_string(), json_f64(event.borrow_cost));
            row.insert("Cost_drag".to_string(), json_f64(event.cost_drag));
            row.insert("Selected_count".to_string(), json!(event.active_positions));
            row.insert("Gross_exposure".to_string(), json_f64(event.gross_exposure));
            row.insert("Cash_weight".to_string(), json_f64(event.cash_weight));
            for asset in &assets {
                row.insert(
                    format!("Weight_{asset}"),
                    json_f64(*event.target_weights.get(asset).unwrap_or(&0.0)),
                );
                row.insert(
                    format!("Contribution_{asset}"),
                    json_f64(*event.contribution.get(asset).unwrap_or(&0.0)),
                );
            }
            row
        })
        .collect()
}

fn build_holding_rows(
    events: &[AccountingEvent],
    contexts: &[AccountingResultContext],
) -> Vec<BTreeMap<String, Value>> {
    let mut rows = Vec::new();
    for (idx, event) in events.iter().enumerate() {
        let context = contexts.get(idx);
        if !context.map(|item| item.rebalance).unwrap_or(false) {
            continue;
        }
        let selected_assets = selected_assets_for(event, context);
        let ranked_assets = ranked_assets_for(event, context, &selected_assets);
        for (rank, asset) in ranked_assets.iter().enumerate() {
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.time));
            row.insert("Asset".to_string(), json!(asset));
            row.insert("Rank".to_string(), json!(rank + 1));
            row.insert(
                "Selected".to_string(),
                json!(selected_assets.contains(asset)),
            );
            row.insert(
                "Eligible".to_string(),
                json!(context
                    .and_then(|item| item.eligible.get(asset).copied())
                    .unwrap_or(true)),
            );
            row.insert(
                "Score".to_string(),
                json_f64(
                    context
                        .and_then(|item| item.score.get(asset).copied())
                        .unwrap_or(f64::NAN),
                ),
            );
            row.insert(
                "Target_weight".to_string(),
                json_f64(*event.target_weights.get(asset).unwrap_or(&0.0)),
            );
            rows.push(row);
        }
    }
    rows
}

fn build_rebalance_rows(
    events: &[AccountingEvent],
    contexts: &[AccountingResultContext],
    cost_rate: f64,
) -> Vec<BTreeMap<String, Value>> {
    let mut rows = Vec::new();
    for (idx, event) in events.iter().enumerate() {
        let context = contexts.get(idx);
        if !context.map(|item| item.rebalance).unwrap_or(false) {
            continue;
        }
        let selected_assets = selected_assets_for(event, context);
        let ranked_assets = ranked_assets_for(event, context, &selected_assets);
        let mut row = BTreeMap::new();
        row.insert("Time".to_string(), json!(event.time));
        row.insert("Rebalance".to_string(), json!(true));
        row.insert("Selected_assets".to_string(), json!(selected_assets));
        row.insert("Selected_count".to_string(), json!(event.active_positions));
        row.insert("Ranked_candidates".to_string(), json!(ranked_assets));
        row.insert("Turnover".to_string(), json_f64(event.turnover));
        row.insert("Cost_rate".to_string(), json_f64(cost_rate));
        row.insert(
            "Trade_cost".to_string(),
            json_f64(event.equity_before_trade * event.turnover * cost_rate),
        );
        row.insert("Borrow_cost".to_string(), json_f64(event.borrow_cost));
        row.insert(
            "Equity_value".to_string(),
            json_f64(event.equity_after_trade),
        );
        rows.push(row);
    }
    rows
}

fn build_trade_rows(
    events: &[AccountingEvent],
    contexts: &[AccountingResultContext],
    cost_rate: f64,
) -> Vec<BTreeMap<String, Value>> {
    let mut rows = Vec::new();
    for (idx, event) in events.iter().enumerate() {
        let context = contexts.get(idx);
        if !context.map(|item| item.rebalance).unwrap_or(false) {
            continue;
        }
        let selected_assets = selected_assets_for(event, context);
        let ranked_assets = ranked_assets_for(event, context, &selected_assets);
        let ranked_lookup = ranked_assets
            .iter()
            .enumerate()
            .map(|(rank, asset)| (asset.clone(), rank + 1))
            .collect::<BTreeMap<_, _>>();
        let assets =
            asset_union_for_result(&event.drift_weights, &event.target_weights, &ranked_assets);
        let trade_cost = event.equity_before_trade * event.turnover * cost_rate;
        for asset in assets {
            let before = *event.drift_weights.get(&asset).unwrap_or(&0.0);
            let target = *event.target_weights.get(&asset).unwrap_or(&0.0);
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
            let rank = ranked_lookup.get(&asset).copied();
            let eligible = context
                .and_then(|item| item.eligible.get(&asset).copied())
                .unwrap_or(true);
            let score = context
                .and_then(|item| item.score.get(&asset).copied())
                .unwrap_or(f64::NAN);
            let mut reason_parts = Vec::new();
            if let Some(rank) = rank {
                let rank_by = context.map(|item| item.rank_by.as_str()).unwrap_or("");
                if rank_by.is_empty() {
                    reason_parts.push(format!("rank {rank}"));
                } else {
                    reason_parts.push(format!("rank {rank} by {rank_by}"));
                }
            } else if before > 0.0 && target <= 0.0 {
                reason_parts.push("not selected at this rebalance".to_string());
            }
            if eligible {
                reason_parts.push("eligible".to_string());
            } else if action != "hold" {
                reason_parts.push("not eligible".to_string());
            }
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.time));
            row.insert("Asset".to_string(), json!(asset));
            row.insert("Before_weight".to_string(), json_f64(before));
            row.insert("Target_weight".to_string(), json_f64(target));
            row.insert("Trade_delta".to_string(), json_f64(delta));
            row.insert("Action".to_string(), json!(action));
            row.insert("Trade_turnover".to_string(), json_f64(abs_delta));
            row.insert(
                "Allocated_cost".to_string(),
                json_f64(if event.turnover > 0.0 {
                    trade_cost * abs_delta / event.turnover
                } else {
                    0.0
                }),
            );
            row.insert(
                "Selected".to_string(),
                json!(selected_assets.contains(&asset)),
            );
            row.insert("Eligible".to_string(), json!(eligible));
            row.insert(
                "Rank".to_string(),
                rank.map_or(Value::Null, |value| json!(value)),
            );
            row.insert("Score".to_string(), json_f64(score));
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

fn export_accounting_bundle(
    output_dir: &str,
    run_id: &str,
    result_tables: &AccountingResultTables,
) -> Result<AccountingRustArtifactBundle, AccountingError> {
    let output_path = PathBuf::from(output_dir);
    fs::create_dir_all(&output_path)
        .map_err(|exc| AccountingError::ArtifactExport(exc.to_string()))?;
    let safe_run_id = slugify(run_id);
    let mut bundle_paths = BTreeMap::new();
    let table_specs = [
        ("equity_curve", "equity_curve"),
        ("execution_equity_curve", "execution_equity_curve"),
        ("holdings", "holdings"),
        ("rebalance_audit", "rebalance_audit"),
        ("rebalance_trades", "rebalance_trades"),
        ("risk_gate_events", "risk_gate_events"),
        ("settlements", "settlements"),
    ];
    for (table_key, file_key) in table_specs {
        let rows = accounting_table_rows(result_tables, table_key, run_id);
        let path = output_path.join(format!("{safe_run_id}_{file_key}.parquet"));
        write_result_rows_parquet(&path, &rows, table_key)
            .map_err(AccountingError::ArtifactExport)?;
        bundle_paths.insert(file_key.to_string(), path.to_string_lossy().to_string());
    }
    Ok(AccountingRustArtifactBundle {
        schema_version: "rust_portfolio_result_bundle.v1".to_string(),
        artifact_type: "rust_accounting_bundle".to_string(),
        run_id: safe_run_id,
        candidate_count: 1,
        bundle_paths,
    })
}

fn accounting_table_rows(
    result_tables: &AccountingResultTables,
    table_key: &str,
    candidate_id: &str,
) -> Vec<BTreeMap<String, Value>> {
    let rows = match table_key {
        "equity_curve" | "execution_equity_curve" => &result_tables.equity_curve,
        "holdings" => &result_tables.holdings,
        "rebalance_audit" => &result_tables.rebalance_audit,
        "rebalance_trades" => &result_tables.rebalance_trades,
        "risk_gate_events" => &result_tables.risk_gate_events,
        "settlements" => &result_tables.settlements,
        _ => return Vec::new(),
    };
    rows.iter()
        .map(|row| {
            let mut enriched = row.clone();
            enriched.insert(
                "Backtest_id".to_string(),
                Value::String(candidate_id.to_string()),
            );
            enriched
        })
        .collect()
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut previous_underscore = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            previous_underscore = false;
        } else if !previous_underscore {
            out.push('_');
            previous_underscore = true;
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "run".to_string()
    } else {
        trimmed
    }
}

fn selected_assets_for(
    event: &AccountingEvent,
    context: Option<&AccountingResultContext>,
) -> Vec<String> {
    context
        .map(|item| item.selected_assets.clone())
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| {
            event
                .target_weights
                .iter()
                .filter_map(|(asset, value)| {
                    if value.abs() > 1e-12 {
                        Some(asset.clone())
                    } else {
                        None
                    }
                })
                .collect()
        })
}

fn ranked_assets_for(
    event: &AccountingEvent,
    context: Option<&AccountingResultContext>,
    selected_assets: &[String],
) -> Vec<String> {
    context
        .map(|item| item.ranked_assets.clone())
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| {
            if !selected_assets.is_empty() {
                selected_assets.to_vec()
            } else {
                asset_union_for_result(&event.drift_weights, &event.target_weights, &[])
            }
        })
}

fn asset_union_for_result(
    before: &BTreeMap<String, f64>,
    target: &BTreeMap<String, f64>,
    ranked_assets: &[String],
) -> Vec<String> {
    let mut assets = before
        .keys()
        .chain(target.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    assets.extend(ranked_assets.iter().cloned());
    assets.into_iter().collect()
}

fn json_f64(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else {
        Value::Null
    }
}

fn validate_config(config: &AccountingConfig) -> Result<(), AccountingError> {
    if !config.starting_equity.is_finite() || config.starting_equity <= 0.0 {
        return Err(AccountingError::InvalidStartingEquity);
    }
    if !config.cost_rate.is_finite() || config.cost_rate < 0.0 {
        return Err(AccountingError::InvalidCostRate);
    }
    if !config.short_borrow_rate_annual.is_finite() || config.short_borrow_rate_annual < 0.0 {
        return Err(AccountingError::InvalidShortBorrowRate);
    }
    if config.borrow_day_count == 0 {
        return Err(AccountingError::InvalidShortBorrowDayCount);
    }
    if !config.max_gross_exposure.is_finite() || config.max_gross_exposure <= 0.0 {
        return Err(AccountingError::InvalidMaxGrossExposure);
    }
    validate_risk_gates(&config.risk_gates)?;
    Ok(())
}

fn validate_risk_gates(gates: &AccountingRiskGateConfig) -> Result<(), AccountingError> {
    for value in [
        gates.max_daily_loss,
        gates.max_order_size,
        gates.max_drawdown,
    ]
    .into_iter()
    .flatten()
    {
        if !value.is_finite() || value < 0.0 {
            return Err(AccountingError::InvalidCostRate);
        }
    }
    validate_risk_action(gates.gate_action.as_deref())?;
    let action = gates.gate_action.as_deref().unwrap_or("").trim();
    if (gates.max_daily_loss.is_some() || gates.max_drawdown.is_some()) && action.is_empty() {
        return Err(AccountingError::MissingRiskGateAction);
    }
    if action == "reduce_exposure"
        && !matches!(
            gates.reduce_exposure_factor,
            Some(value) if value.is_finite() && value > 0.0 && value <= 1.0
        )
    {
        return Err(AccountingError::InvalidReduceExposureFactor);
    }
    Ok(())
}

fn validate_risk_action(action: Option<&str>) -> Result<(), AccountingError> {
    let action = action.unwrap_or("").trim();
    if action.is_empty()
        || matches!(
            action,
            "flatten"
                | "permanent_stop"
                | "shadow_until_recovery"
                | "block_new_orders"
                | "reduce_exposure"
        )
    {
        Ok(())
    } else {
        Err(AccountingError::InvalidRiskAction(action.to_string()))
    }
}

fn validate_checkpoint(
    checkpoint: &CheckpointInput,
    config: &AccountingConfig,
) -> Result<(), AccountingError> {
    for (asset, value) in &checkpoint.returns {
        if !value.is_finite() {
            return Err(AccountingError::NonFiniteValue {
                field: "returns",
                asset: asset.clone(),
            });
        }
    }
    for (asset, value) in &checkpoint.target_weights {
        if !value.is_finite() {
            return Err(AccountingError::NonFiniteValue {
                field: "target_weights",
                asset: asset.clone(),
            });
        }
        if !config.allow_short && *value < -1e-12 {
            return Err(AccountingError::NegativeWeightLongOnly(asset.clone()));
        }
    }
    Ok(())
}

fn validate_held_asset_returns(
    returns: &BTreeMap<String, f64>,
    held_weights: &BTreeMap<String, f64>,
) -> Result<(), AccountingError> {
    for (asset, weight) in held_weights {
        if weight.abs() > 1e-12 && !returns.contains_key(asset) {
            return Err(AccountingError::MissingHeldAssetReturn(asset.clone()));
        }
    }
    Ok(())
}

fn normalized_target_weights(
    target_weights: &BTreeMap<String, f64>,
    config: &AccountingConfig,
) -> Result<BTreeMap<String, f64>, AccountingError> {
    let mut out = BTreeMap::new();
    for (asset, value) in target_weights {
        if value.abs() > 1e-12 {
            out.insert(asset.clone(), *value);
        }
    }
    let gross = out.values().map(|value| value.abs()).sum::<f64>();
    if gross > config.max_gross_exposure + 1e-10 {
        return Err(AccountingError::GrossExposureExceeded {
            actual: gross,
            limit: config.max_gross_exposure,
        });
    }
    Ok(out)
}

fn asset_union(
    previous_weights: &BTreeMap<String, f64>,
    returns: &BTreeMap<String, f64>,
    target_weights: &BTreeMap<String, f64>,
) -> BTreeSet<String> {
    previous_weights
        .keys()
        .chain(returns.keys())
        .chain(target_weights.keys())
        .cloned()
        .collect()
}

fn turnover_between(
    drift_weights: &BTreeMap<String, f64>,
    target_weights: &BTreeMap<String, f64>,
    assets: &BTreeSet<String>,
) -> f64 {
    assets
        .iter()
        .map(|asset| {
            let drift = *drift_weights.get(asset).unwrap_or(&0.0);
            let target = *target_weights.get(asset).unwrap_or(&0.0);
            (target - drift).abs()
        })
        .sum()
}

fn risk_gates_enabled(gates: &AccountingRiskGateConfig) -> bool {
    gates.max_positions.is_some()
        || gates.max_daily_loss.is_some()
        || gates.max_order_size.is_some()
        || gates.max_drawdown.is_some()
}

fn initialize_shadow_recovery_state(
    pre_trade_equity: f64,
    equity_peak: f64,
    drift_weights: &BTreeMap<String, f64>,
    requested_target_weights: &BTreeMap<String, f64>,
    config: &AccountingConfig,
) -> Result<DrawdownRecoveryState, AccountingError> {
    let assets = asset_union_for_weights(drift_weights, requested_target_weights);
    let turnover = turnover_between(drift_weights, requested_target_weights, &assets);
    let cost_drag = turnover * config.cost_rate;
    let shadow_equity = pre_trade_equity * (1.0 - cost_drag);
    let mut control = RiskControlState::default();
    control.activate(SHADOW_ACTION, equity_peak)?;
    control.observe_shadow_equity(shadow_equity);
    Ok(DrawdownRecoveryState {
        control,
        shadow_equity,
        shadow_weights: trim_weights(requested_target_weights),
        shadow_cash_weight: 1.0 - requested_target_weights.values().sum::<f64>(),
    })
}

fn update_shadow_recovery_state(
    state: &mut DrawdownRecoveryState,
    checkpoint: &CheckpointInput,
    config: &AccountingConfig,
) -> Result<bool, AccountingError> {
    if !state.control.is_shadow() {
        return Ok(false);
    }
    validate_held_asset_returns(&checkpoint.returns, &state.shadow_weights)?;
    let was_armed = state.control.recovery_armed();
    let assets = asset_union(
        &state.shadow_weights,
        &checkpoint.returns,
        &checkpoint.target_weights,
    );
    let equity_before_return = state.shadow_equity;
    let mut asset_values: BTreeMap<String, f64> = BTreeMap::new();
    let mut pre_trade_equity = equity_before_return * state.shadow_cash_weight;
    for asset in &assets {
        let previous_weight = *state.shadow_weights.get(asset).unwrap_or(&0.0);
        let asset_return = *checkpoint.returns.get(asset).unwrap_or(&0.0);
        let value_before = equity_before_return * previous_weight;
        let value_after = value_before * (1.0 + asset_return);
        asset_values.insert(asset.clone(), value_after);
        pre_trade_equity += value_after;
    }
    let drift_weights = if pre_trade_equity > 0.0 {
        asset_values
            .iter()
            .filter_map(|(asset, value)| {
                if value.abs() > 1e-12 {
                    Some((asset.clone(), value / pre_trade_equity))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };
    let target_weights = if checkpoint.rebalance {
        normalized_target_weights(&checkpoint.target_weights, config)?
    } else {
        drift_weights.clone()
    };
    let assets = asset_union_for_weights(&drift_weights, &target_weights);
    let turnover = turnover_between(&drift_weights, &target_weights, &assets);
    let cost_drag = turnover * config.cost_rate;
    state.shadow_equity = pre_trade_equity * (1.0 - cost_drag);
    state.shadow_weights = trim_weights(&target_weights);
    state.shadow_cash_weight = 1.0 - target_weights.values().sum::<f64>();
    state.control.observe_shadow_equity(state.shadow_equity);
    Ok(!was_armed && state.control.recovery_armed())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_risk_gates(
    gates: &AccountingRiskGateConfig,
    symbols: &BTreeSet<String>,
    before_weights: &BTreeMap<String, f64>,
    target_weights: &BTreeMap<String, f64>,
    equity: f64,
    equity_peak: f64,
    daily_return: f64,
    time: &str,
) -> (BTreeMap<String, f64>, Vec<AccountingRiskGateEvent>) {
    let mut adjusted = target_weights.clone();
    let mut events = Vec::new();
    if !risk_gates_enabled(gates) {
        return (adjusted, events);
    }

    if let Some(limit) = gates.max_daily_loss {
        if daily_return.is_finite() && daily_return <= -limit.abs() {
            adjusted = apply_gate_action(gates, &adjusted, before_weights, symbols);
            events.push(risk_gate_event(
                time,
                "max_daily_loss",
                -limit.abs(),
                daily_return,
                effective_gate_action(gates),
                symbols.iter().cloned().collect(),
                &adjusted,
            ));
        }
    }

    if let Some(limit) = gates.max_drawdown {
        if equity.is_finite() && equity_peak.is_finite() && equity_peak > 0.0 {
            let drawdown = simple_return(equity, equity_peak);
            if drawdown <= -limit.abs() {
                adjusted = apply_gate_action(gates, &adjusted, before_weights, symbols);
                events.push(risk_gate_event(
                    time,
                    "max_drawdown",
                    -limit.abs(),
                    drawdown,
                    effective_gate_action(gates),
                    symbols.iter().cloned().collect(),
                    &adjusted,
                ));
            }
        }
    }

    if let Some(max_positions) = gates.max_positions {
        let active_assets = symbols
            .iter()
            .filter(|asset| adjusted.get(*asset).unwrap_or(&0.0).abs() > 1e-12)
            .cloned()
            .collect::<Vec<_>>();
        if active_assets.len() > max_positions {
            let mut ranked = active_assets.clone();
            ranked.sort_by(|lhs, rhs| {
                let lhs_abs = adjusted.get(lhs).unwrap_or(&0.0).abs();
                let rhs_abs = adjusted.get(rhs).unwrap_or(&0.0).abs();
                rhs_abs
                    .partial_cmp(&lhs_abs)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| lhs.cmp(rhs))
            });
            let keep = ranked
                .iter()
                .take(max_positions)
                .cloned()
                .collect::<BTreeSet<_>>();
            let removed = active_assets
                .into_iter()
                .filter(|asset| !keep.contains(asset))
                .collect::<Vec<_>>();
            for asset in &removed {
                adjusted.remove(asset);
            }
            events.push(risk_gate_event(
                time,
                "max_positions",
                max_positions as f64,
                ranked.len() as f64,
                "reduce_selected_positions".to_string(),
                removed,
                &adjusted,
            ));
        }
    }

    if let Some(max_order_size) = gates.max_order_size {
        let limit = max_order_size.abs();
        let mut affected_assets = Vec::new();
        for asset in symbols {
            let before = *before_weights.get(asset).unwrap_or(&0.0);
            let target = *adjusted.get(asset).unwrap_or(&0.0);
            let delta = target - before;
            if delta.abs() > limit + 1e-12 {
                adjusted.insert(asset.clone(), before + limit.copysign(delta));
                affected_assets.push(asset.clone());
            }
        }
        if !affected_assets.is_empty() {
            let observed = affected_assets
                .iter()
                .map(|asset| {
                    (*target_weights.get(asset).unwrap_or(&0.0)
                        - *before_weights.get(asset).unwrap_or(&0.0))
                    .abs()
                })
                .fold(0.0, f64::max);
            events.push(risk_gate_event(
                time,
                "max_order_size",
                limit,
                observed,
                "clamp_order_delta".to_string(),
                affected_assets,
                &adjusted,
            ));
        }
    }

    (trim_weights(&adjusted), events)
}

fn effective_gate_action(gates: &AccountingRiskGateConfig) -> String {
    let value = gates
        .gate_action
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if value.is_empty() || value == "none" {
        "flatten".to_string()
    } else {
        value
    }
}

fn apply_gate_action(
    gates: &AccountingRiskGateConfig,
    target: &BTreeMap<String, f64>,
    before: &BTreeMap<String, f64>,
    symbols: &BTreeSet<String>,
) -> BTreeMap<String, f64> {
    match effective_gate_action(gates).as_str() {
        "flatten" | "permanent_stop" | "shadow_until_recovery" => BTreeMap::new(),
        "reduce_exposure" => {
            let factor = gates
                .reduce_exposure_factor
                .expect("validated reduce_exposure_factor");
            target
                .iter()
                .filter_map(|(asset, value)| {
                    let scaled = *value * factor;
                    if scaled.abs() > 1e-12 {
                        Some((asset.clone(), scaled))
                    } else {
                        None
                    }
                })
                .collect()
        }
        "block_new_orders" => {
            let mut adjusted = target.clone();
            for asset in symbols {
                let target_value = *target.get(asset).unwrap_or(&0.0);
                let before_value = *before.get(asset).unwrap_or(&0.0);
                let same_direction =
                    before_value == 0.0 || target_value.signum() == before_value.signum();
                if target_value.abs() > before_value.abs() && same_direction {
                    if before_value.abs() > 1e-12 {
                        adjusted.insert(asset.clone(), before_value);
                    } else {
                        adjusted.remove(asset);
                    }
                }
            }
            adjusted
        }
        _ => target.clone(),
    }
}

fn trim_weights(weights: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    weights
        .iter()
        .filter_map(|(asset, value)| {
            if value.abs() > 1e-12 {
                Some((asset.clone(), *value))
            } else {
                None
            }
        })
        .collect()
}

fn asset_union_for_weights(
    before: &BTreeMap<String, f64>,
    target: &BTreeMap<String, f64>,
) -> BTreeSet<String> {
    before.keys().chain(target.keys()).cloned().collect()
}

fn risk_gate_event(
    time: &str,
    gate: &str,
    threshold: f64,
    observed: f64,
    action: String,
    affected_assets: Vec<String>,
    resulting_target_weights: &BTreeMap<String, f64>,
) -> AccountingRiskGateEvent {
    AccountingRiskGateEvent {
        time: time.to_string(),
        gate: gate.to_string(),
        threshold,
        observed,
        action,
        affected_assets,
        resulting_target_weights: trim_weights(resulting_target_weights),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    fn weights(rows: &[(&str, f64)]) -> BTreeMap<String, f64> {
        rows.iter()
            .map(|(key, value)| ((*key).to_string(), *value))
            .collect()
    }

    #[test]
    fn missing_held_asset_return_fails_closed() {
        let error = run_accounting(AccountingInput {
            config: AccountingConfig::default(),
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    returns: BTreeMap::new(),
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-03".to_string(),
                    returns: BTreeMap::new(),
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .expect_err("missing held-asset returns must fail");

        assert_eq!(
            error,
            AccountingError::MissingHeldAssetReturn("AAA".to_string())
        );
    }

    #[test]
    fn fixed_weight_rebalance_computes_drift_turnover() {
        let input = AccountingInput {
            config: AccountingConfig {
                starting_equity: 100.0,
                cost_rate: 0.0,
                max_gross_exposure: 1.0,
                allow_short: false,
                short_borrow_rate_annual: 0.0,
                borrow_day_count: 252,
                session_label_by_event_time: BTreeMap::new(),
                risk_gates: AccountingRiskGateConfig::default(),
                simulated_venue: SimulatedVenueConfig::default(),
                simulated_account: SimulatedAccountConfig::default(),
            },
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    rebalance: true,
                    returns: BTreeMap::new(),
                    target_weights: weights(&[("AAA", 0.6), ("BBB", 0.4)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2025-01-02".to_string(),
                    rebalance: true,
                    returns: weights(&[("AAA", 0.20), ("BBB", 0.0)]),
                    target_weights: weights(&[("AAA", 0.6), ("BBB", 0.4)]),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();

        assert_abs_diff_eq!(summary.events[0].turnover, 1.0, epsilon = 1e-12);
        assert!(summary.events[1].turnover > 0.08);
        assert!(summary.final_equity > 100.0);
        assert_eq!(summary.active_rebalances, 2);
    }

    #[test]
    fn rotation_from_one_full_position_to_another_has_two_way_turnover() {
        let input = AccountingInput {
            config: AccountingConfig::default(),
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    rebalance: true,
                    returns: BTreeMap::new(),
                    target_weights: weights(&[("VOO", 1.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-02-01".to_string(),
                    rebalance: true,
                    returns: weights(&[("VOO", 0.10), ("GLD", 0.0)]),
                    target_weights: weights(&[("GLD", 1.0)]),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();

        assert_abs_diff_eq!(summary.events[0].turnover, 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(summary.events[1].turnover, 2.0, epsilon = 1e-12);
        assert_abs_diff_eq!(summary.events[1].cash_weight, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn long_only_accounting_rejects_short_weight() {
        let input = AccountingInput {
            config: AccountingConfig::default(),
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                rebalance: true,
                returns: BTreeMap::new(),
                target_weights: weights(&[("QQQ", -1.0)]),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        assert!(matches!(
            run_accounting(input),
            Err(AccountingError::NegativeWeightLongOnly(asset)) if asset == "QQQ"
        ));
    }

    #[test]
    fn gross_exposure_is_fail_fast() {
        let input = AccountingInput {
            config: AccountingConfig::default(),
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                rebalance: true,
                returns: BTreeMap::new(),
                target_weights: weights(&[("AAA", 0.7), ("BBB", 0.7)]),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        assert!(matches!(
            run_accounting(input),
            Err(AccountingError::GrossExposureExceeded { .. })
        ));
    }

    #[test]
    fn non_rebalance_checkpoint_drifts_without_turnover() {
        let input = AccountingInput {
            config: AccountingConfig::default(),
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    rebalance: true,
                    returns: BTreeMap::new(),
                    target_weights: weights(&[("AAA", 0.6), ("BBB", 0.4)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-03".to_string(),
                    rebalance: false,
                    returns: weights(&[("AAA", 0.20), ("BBB", 0.0)]),
                    target_weights: BTreeMap::new(),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();

        assert_abs_diff_eq!(summary.events[1].turnover, 0.0, epsilon = 1e-12);
        assert!(summary.events[1].target_weights["AAA"] > 0.6);
        assert_abs_diff_eq!(summary.events[1].cash_weight, 0.0, epsilon = 1e-12);
        assert_eq!(summary.active_rebalances, 1);
    }

    #[test]
    fn max_positions_gate_keeps_largest_target_weights() {
        let input = AccountingInput {
            config: AccountingConfig {
                risk_gates: AccountingRiskGateConfig {
                    max_positions: Some(1),
                    ..AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                rebalance: true,
                returns: BTreeMap::new(),
                target_weights: weights(&[("AAA", 0.7), ("BBB", 0.3)]),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();

        assert_eq!(summary.risk_gate_events[0].gate, "max_positions");
        assert!(summary.events[0].target_weights.contains_key("AAA"));
        assert!(
            !summary.events[0].target_weights.contains_key("BBB"),
            "unexpected weights: {:?}",
            summary.events[0].target_weights
        );
    }

    #[test]
    fn max_order_size_gate_clamps_target_delta() {
        let input = AccountingInput {
            config: AccountingConfig {
                risk_gates: AccountingRiskGateConfig {
                    max_order_size: Some(0.25),
                    ..AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                rebalance: true,
                returns: BTreeMap::new(),
                target_weights: weights(&[("AAA", 1.0)]),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();

        assert_eq!(summary.risk_gate_events[0].gate, "max_order_size");
        assert_abs_diff_eq!(
            summary.events[0].target_weights["AAA"],
            0.25,
            epsilon = 1e-12
        );
        assert_abs_diff_eq!(summary.events[0].turnover, 0.25, epsilon = 1e-12);
    }

    #[test]
    fn max_drawdown_shadow_recovers_and_resumes_on_next_rebalance() {
        let input = AccountingInput {
            config: AccountingConfig {
                risk_gates: AccountingRiskGateConfig {
                    max_drawdown: Some(0.10),
                    gate_action: Some("shadow_until_recovery".to_string()),
                    ..AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    rebalance: true,
                    returns: BTreeMap::new(),
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-03".to_string(),
                    rebalance: true,
                    returns: weights(&[("AAA", -0.20)]),
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-04".to_string(),
                    rebalance: false,
                    returns: weights(&[("AAA", 0.25)]),
                    target_weights: BTreeMap::new(),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-05".to_string(),
                    rebalance: true,
                    returns: weights(&[("AAA", 0.0)]),
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();

        assert_eq!(summary.risk_gate_events[0].gate, "max_drawdown");
        assert_eq!(summary.risk_gate_events[0].action, "shadow_until_recovery");
        assert!(summary.events[1].target_weights.is_empty());
        assert!(
            summary.events[2].target_weights.is_empty(),
            "unexpected weights: {:?}",
            summary.events[2].target_weights
        );
        assert_abs_diff_eq!(
            summary.events[3].target_weights["AAA"],
            1.0,
            epsilon = 1e-12
        );
        assert_abs_diff_eq!(summary.events[3].turnover, 1.0, epsilon = 1e-12);
    }

    #[test]
    fn retired_pause_action_is_rejected() {
        let input = AccountingInput {
            config: AccountingConfig {
                risk_gates: AccountingRiskGateConfig {
                    max_drawdown: Some(0.10),
                    gate_action: Some("pause_trading".to_string()),
                    ..AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                rebalance: true,
                target_weights: weights(&[("AAA", 1.0)]),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        assert_eq!(
            run_accounting(input).unwrap_err(),
            AccountingError::InvalidRiskAction("pause_trading".to_string())
        );
    }

    #[test]
    fn loss_gate_requires_explicit_action_and_reduce_factor() {
        let mut config = AccountingConfig::default();
        config.risk_gates.max_drawdown = Some(0.1);
        let missing_action = run_accounting(AccountingInput {
            config: config.clone(),
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .unwrap_err();
        assert_eq!(missing_action, AccountingError::MissingRiskGateAction);

        config.risk_gates.gate_action = Some("reduce_exposure".to_string());
        let missing_factor = run_accounting(AccountingInput {
            config,
            checkpoints: vec![CheckpointInput {
                time: "2024-01-02".to_string(),
                ..CheckpointInput::default()
            }],
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .unwrap_err();
        assert_eq!(missing_factor, AccountingError::InvalidReduceExposureFactor);
    }

    #[test]
    fn max_drawdown_permanent_stop_blocks_later_rebalances() {
        let input = AccountingInput {
            config: AccountingConfig {
                risk_gates: AccountingRiskGateConfig {
                    max_drawdown: Some(0.10),
                    gate_action: Some("permanent_stop".to_string()),
                    ..AccountingRiskGateConfig::default()
                },
                ..AccountingConfig::default()
            },
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    rebalance: true,
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-03".to_string(),
                    rebalance: true,
                    returns: weights(&[("AAA", -0.20)]),
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-04".to_string(),
                    rebalance: true,
                    target_weights: weights(&[("AAA", 1.0)]),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        };

        let summary = run_accounting(input).unwrap();
        assert_eq!(summary.risk_gate_events[0].action, "permanent_stop");
        assert!(summary.events[1].target_weights.is_empty());
        assert!(summary.events[2].target_weights.is_empty());
        assert_abs_diff_eq!(summary.events[2].turnover, 0.0, epsilon = 1e-12);
    }

    #[test]
    fn maintenance_margin_liquidates_fixed_allocation_between_rebalances() {
        let summary = run_accounting(AccountingInput {
            config: AccountingConfig {
                max_gross_exposure: 2.0,
                simulated_account: SimulatedAccountConfig {
                    account_type: crate::simulation::SimulatedAccountType::Margin,
                    leverage_limit: 2.0,
                    initial_margin_ratio: 0.5,
                    maintenance_margin_ratio: 0.6,
                    allow_short_borrow: false,
                    settlement_days: 0,
                },
                ..AccountingConfig::default()
            },
            checkpoints: vec![
                CheckpointInput {
                    time: "2024-01-02".to_string(),
                    rebalance: true,
                    target_weights: weights(&[("AAA", 2.0)]),
                    ..CheckpointInput::default()
                },
                CheckpointInput {
                    time: "2024-01-03".to_string(),
                    rebalance: false,
                    returns: weights(&[("AAA", 0.0)]),
                    ..CheckpointInput::default()
                },
            ],
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .unwrap();

        assert!(summary.events[1].target_weights.is_empty());
        assert_eq!(
            summary.events[1].orders[0].status,
            crate::simulation::OrderStatus::Filled
        );
        assert_eq!(summary.risk_gate_events[0].gate, "maintenance_margin");
        assert_eq!(summary.risk_gate_events[0].action, "margin_liquidation");
    }
}
