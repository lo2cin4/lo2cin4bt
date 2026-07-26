use crate::result_validator::{
    validate_result_tables, ResultTableView, ResultValidationError, ResultValidationReport,
};
use crate::risk::{
    RiskControlError, RiskControlState, RiskRunMode, PERMANENT_STOP_ACTION, SHADOW_ACTION,
    SHADOW_RECOVERY_ARMED_ACTION, SHADOW_RECOVERY_RESUMED_ACTION,
};
use crate::simulation::{
    execute_target_weight_orders, maintenance_margin_breached, SettlementEvent,
    SettlementInstruction, SettlementLedger, SimulatedAccountConfig, SimulatedOrderEvent,
    SimulatedVenueConfig, SimulationError,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum TimelineAccountingError {
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
    #[error("timeline input requires at least one checkpoint")]
    EmptyCheckpoints,
    #[error("checkpoint phase must be open or close: {0}")]
    InvalidPhase(String),
    #[error("timeline action must be enter, exit, flatten, or set_target_weights: {0}")]
    InvalidAction(String),
    #[error("position policy must be ignore_new_signal, add_position, or reset_timer: {0}")]
    InvalidPositionPolicy(String),
    #[error("non-finite value in {field} for {asset}")]
    NonFiniteValue { field: &'static str, asset: String },
    #[error("missing return for held asset {0}")]
    MissingHeldAssetReturn(String),
    #[error("negative target weight for {0} is not allowed in long-only timeline accounting")]
    NegativeWeightLongOnly(String),
    #[error("target gross exposure {actual:.6} exceeds configured max {limit:.6}")]
    GrossExposureExceeded { actual: f64, limit: f64 },
    #[error("unsupported risk gate action: {0}")]
    InvalidRiskAction(String),
    #[error("risk gate action is required when a loss or drawdown gate is configured")]
    MissingRiskGateAction,
    #[error("reduce_exposure requires reduce_exposure_factor in (0, 1]")]
    InvalidReduceExposureFactor,
    #[error(transparent)]
    Simulation(#[from] SimulationError),
    #[error(transparent)]
    RiskControl(#[from] RiskControlError),
    #[error(transparent)]
    ResultValidation(#[from] ResultValidationError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineAccountingConfig {
    pub starting_equity: f64,
    pub cost_rate: f64,
    pub max_gross_exposure: f64,
    pub allow_short: bool,
    #[serde(default)]
    pub short_borrow_rate_annual: f64,
    #[serde(default = "default_borrow_day_count")]
    pub borrow_day_count: u32,
    #[serde(default)]
    pub position_policy: TimelinePositionPolicy,
    #[serde(default)]
    pub risk_gates: TimelineRiskGateConfig,
    #[serde(default)]
    pub simulated_venue: SimulatedVenueConfig,
    #[serde(default)]
    pub simulated_account: SimulatedAccountConfig,
}

impl Default for TimelineAccountingConfig {
    fn default() -> Self {
        Self {
            starting_equity: 100.0,
            cost_rate: 0.0,
            max_gross_exposure: 1.0,
            allow_short: false,
            short_borrow_rate_annual: 0.0,
            borrow_day_count: default_borrow_day_count(),
            position_policy: TimelinePositionPolicy::default(),
            risk_gates: TimelineRiskGateConfig::default(),
            simulated_venue: SimulatedVenueConfig::default(),
            simulated_account: SimulatedAccountConfig::default(),
        }
    }
}

fn default_borrow_day_count() -> u32 {
    252
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelinePositionPolicy {
    #[serde(default = "default_overlap_policy")]
    pub on_entry_signal_while_holding: String,
}

impl Default for TimelinePositionPolicy {
    fn default() -> Self {
        Self {
            on_entry_signal_while_holding: default_overlap_policy(),
        }
    }
}

fn default_overlap_policy() -> String {
    "ignore_new_signal".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimelineRiskGateConfig {
    #[serde(default)]
    pub max_positions: Option<usize>,
    #[serde(default)]
    pub max_daily_loss: Option<f64>,
    #[serde(default)]
    pub max_order_size: Option<f64>,
    #[serde(default)]
    pub max_drawdown: Option<f64>,
    #[serde(default)]
    pub gate_action: Option<String>,
    #[serde(default)]
    pub reduce_exposure_factor: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineActionInput {
    pub action: String,
    #[serde(default)]
    pub target_weights: BTreeMap<String, f64>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineCheckpointInput {
    pub date: String,
    pub phase: String,
    #[serde(default)]
    pub returns: BTreeMap<String, f64>,
    #[serde(default)]
    pub actions: Vec<TimelineActionInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineAccountingInput {
    pub config: TimelineAccountingConfig,
    pub checkpoints: Vec<TimelineCheckpointInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineActionEvent {
    pub action: String,
    pub reason: Option<String>,
    pub skipped: bool,
    pub skip_reason: Option<String>,
    pub turnover: f64,
    pub trade_cost: f64,
    pub before_weights: BTreeMap<String, f64>,
    pub target_weights: BTreeMap<String, f64>,
    pub orders: Vec<SimulatedOrderEvent>,
    pub settlements: Vec<SettlementInstruction>,
    pub risk_gate_events: Vec<TimelineRiskGateEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineRiskGateEvent {
    pub date: String,
    pub phase: String,
    pub gate: String,
    pub threshold: f64,
    pub observed: f64,
    pub action: String,
    pub affected_assets: Vec<String>,
    pub resulting_target_weights: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineCheckpointEvent {
    pub date: String,
    pub phase: String,
    pub equity_before_return: f64,
    pub equity_before_trade: f64,
    pub equity_after_trade: f64,
    pub portfolio_return: f64,
    pub turnover: f64,
    pub trade_cost: f64,
    pub borrow_cost: f64,
    pub cost_drag: f64,
    pub cash_weight: f64,
    pub gross_exposure: f64,
    pub active_positions: usize,
    pub target_weights: BTreeMap<String, f64>,
    pub drift_weights: BTreeMap<String, f64>,
    pub contribution: BTreeMap<String, f64>,
    pub actions: Vec<TimelineActionEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineDailyEvent {
    pub date: String,
    pub equity_after_trade: f64,
    pub portfolio_return: f64,
    pub turnover: f64,
    pub trade_cost: f64,
    pub borrow_cost: f64,
    pub cost_drag: f64,
    pub cash_weight: f64,
    pub gross_exposure: f64,
    pub active_positions: usize,
    pub target_weights: BTreeMap<String, f64>,
    pub contribution: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineAccountingSummary {
    pub start_equity: f64,
    pub final_equity: f64,
    pub total_return: f64,
    pub checkpoints: usize,
    pub days: usize,
    pub active_rebalances: usize,
    pub average_turnover: f64,
    pub average_gross_exposure: f64,
    pub events: Vec<TimelineCheckpointEvent>,
    pub daily_events: Vec<TimelineDailyEvent>,
    pub risk_gate_events: Vec<TimelineRiskGateEvent>,
    pub settlement_events: Vec<SettlementEvent>,
    pub result_tables: TimelineResultTables,
    pub result_validation: ResultValidationReport,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimelineResultTables {
    pub schema_version: String,
    pub equity_curve: Vec<BTreeMap<String, Value>>,
    pub holdings: Vec<BTreeMap<String, Value>>,
    pub rebalance_audit: Vec<BTreeMap<String, Value>>,
    pub rebalance_trades: Vec<BTreeMap<String, Value>>,
    pub risk_gate_events: Vec<BTreeMap<String, Value>>,
    pub settlements: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, Default)]
struct DrawdownRecoveryState {
    control: RiskControlState,
    shadow_equity: f64,
    shadow_weights: BTreeMap<String, f64>,
    shadow_cash_weight: f64,
}

pub fn run_timeline_accounting(
    input: TimelineAccountingInput,
) -> Result<TimelineAccountingSummary, TimelineAccountingError> {
    validate_config(&input.config)?;
    if input.checkpoints.is_empty() {
        return Err(TimelineAccountingError::EmptyCheckpoints);
    }

    let mut checkpoints = input.checkpoints;
    for checkpoint in &checkpoints {
        validate_checkpoint(checkpoint, &input.config)?;
    }
    checkpoints.sort_by_key(|checkpoint| {
        (
            checkpoint.date.clone(),
            phase_rank(&checkpoint.phase).unwrap_or(99),
        )
    });

    let mut equity = input.config.starting_equity;
    let start_equity = equity;
    let mut previous_weights: BTreeMap<String, f64> = BTreeMap::new();
    let mut previous_cash_weight = 1.0;
    let mut events = Vec::with_capacity(checkpoints.len());
    let mut daily_events = Vec::new();
    let mut current_daily: Option<DailyAccumulator> = None;
    let mut active_rebalances = 0usize;
    let mut turnover_sum = 0.0;
    let mut gross_sum = 0.0;
    let mut equity_peak = equity;
    let mut risk_gate_events: Vec<TimelineRiskGateEvent> = Vec::new();
    let mut drawdown_recovery = DrawdownRecoveryState::default();
    let mut settlement_ledger = SettlementLedger::default();

    for mut checkpoint in checkpoints {
        let is_new_session = current_daily
            .as_ref()
            .map(|daily| daily.date != checkpoint.date)
            .unwrap_or(false);
        if is_new_session {
            settlement_ledger.advance_session();
            if let Some(daily) = current_daily.take() {
                daily_events.push(daily.finish(&previous_weights, previous_cash_weight, equity));
            }
        }
        let charge_borrow = current_daily.is_none();
        if current_daily.is_none() {
            current_daily = Some(DailyAccumulator::new(checkpoint.date.clone()));
        }
        validate_held_asset_returns(&checkpoint.returns, &previous_weights)?;

        let equity_before_borrow = equity;
        let short_gross = previous_weights
            .values()
            .filter(|weight| **weight < 0.0)
            .map(|weight| weight.abs())
            .sum::<f64>();
        let borrow_cost =
            if charge_borrow && short_gross > 0.0 && input.config.short_borrow_rate_annual > 0.0 {
                equity * short_gross * input.config.short_borrow_rate_annual
                    / input.config.borrow_day_count as f64
            } else {
                0.0
            };
        equity = (equity - borrow_cost).max(0.0);
        let equity_before_return = equity;
        let assets = asset_union(&previous_weights, &checkpoint.returns, &checkpoint.actions);
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
            pre_trade_equity / equity_before_return - 1.0
        } else {
            0.0
        };
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

        let mut current_weights = drift_weights.clone();
        let mut checkpoint_turnover = 0.0;
        let mut checkpoint_trade_cost = 0.0;
        let mut action_events = Vec::with_capacity(checkpoint.actions.len());
        equity = pre_trade_equity;
        equity_peak = equity_peak.max(equity);
        if maintenance_margin_breached(&current_weights, &input.config.simulated_account) {
            let observed = gross_exposure(&current_weights)
                * input.config.simulated_account.maintenance_margin_ratio;
            risk_gate_events.push(risk_gate_event(
                &checkpoint.date,
                &checkpoint.phase,
                "maintenance_margin",
                1.0,
                observed,
                "margin_liquidation".to_string(),
                current_weights.keys().cloned().collect(),
                &BTreeMap::new(),
            ));
            checkpoint.actions.insert(
                0,
                TimelineActionInput {
                    action: "flatten".to_string(),
                    target_weights: BTreeMap::new(),
                    reason: Some("maintenance_margin_liquidation".to_string()),
                },
            );
        }

        if drawdown_recovery.control.is_shadow() {
            let newly_armed = update_timeline_shadow_recovery_state(
                &mut drawdown_recovery,
                &checkpoint,
                &input.config,
            )?;
            if newly_armed {
                risk_gate_events.push(risk_gate_event(
                    &checkpoint.date,
                    &checkpoint.phase,
                    "max_drawdown",
                    drawdown_recovery.control.recovery_target(),
                    drawdown_recovery.shadow_equity,
                    SHADOW_RECOVERY_ARMED_ACTION.to_string(),
                    drawdown_recovery.shadow_weights.keys().cloned().collect(),
                    &drawdown_recovery.shadow_weights,
                ));
            }
        }

        for (action_index, action) in checkpoint.actions.into_iter().enumerate() {
            let action_name = normalize_action(&action.action)?;
            let before_weights = current_weights.clone();
            let (policy_target_weights, skipped, skip_reason) = apply_action_policy(
                &input.config,
                &action_name,
                &before_weights,
                &action.target_weights,
            )?;
            if !drawdown_recovery.control.live_orders_allowed()
                && !drawdown_recovery.control.recovery_armed()
            {
                let target_weights = BTreeMap::new();
                current_weights = target_weights.clone();
                action_events.push(TimelineActionEvent {
                    action: action_name,
                    reason: action.reason,
                    skipped: true,
                    skip_reason: Some(match drawdown_recovery.control.mode() {
                        RiskRunMode::PermanentStopped => "permanent_stop_active".to_string(),
                        RiskRunMode::Shadow => "shadow_recovery_active".to_string(),
                        RiskRunMode::Live => unreachable!(),
                    }),
                    turnover: 0.0,
                    trade_cost: 0.0,
                    before_weights: trim_weights(&before_weights),
                    target_weights,
                    orders: Vec::new(),
                    settlements: Vec::new(),
                    risk_gate_events: Vec::new(),
                });
                continue;
            }
            if drawdown_recovery.control.is_shadow() && drawdown_recovery.control.recovery_armed() {
                risk_gate_events.push(risk_gate_event(
                    &checkpoint.date,
                    &checkpoint.phase,
                    "max_drawdown",
                    drawdown_recovery.control.recovery_target(),
                    equity,
                    SHADOW_RECOVERY_RESUMED_ACTION.to_string(),
                    policy_target_weights.keys().cloned().collect(),
                    &policy_target_weights,
                ));
                drawdown_recovery.control.resume_on_next_action();
                drawdown_recovery.shadow_equity = 0.0;
                drawdown_recovery.shadow_weights.clear();
                drawdown_recovery.shadow_cash_weight = 0.0;
                equity_peak = equity;
            }
            let daily_return_so_far = current_daily
                .as_ref()
                .map(|daily| daily.portfolio_return)
                .unwrap_or(0.0)
                + portfolio_return;
            let (target_weights, action_risk_events) = apply_risk_gates(
                &input.config.risk_gates,
                &assets,
                &before_weights,
                &policy_target_weights,
                equity,
                equity_peak,
                daily_return_so_far,
                &checkpoint.date,
                &checkpoint.phase,
            );
            if let Some(event) = action_risk_events.iter().find(|event| {
                event.action == SHADOW_ACTION || event.action == PERMANENT_STOP_ACTION
            }) {
                if event.action == SHADOW_ACTION {
                    drawdown_recovery = initialize_timeline_shadow_recovery_state(
                        equity,
                        equity_peak,
                        &before_weights,
                        &policy_target_weights,
                        &input.config,
                    )?;
                } else {
                    drawdown_recovery
                        .control
                        .activate(PERMANENT_STOP_ACTION, equity_peak)?;
                }
            }
            let execution = execute_target_weight_orders(
                &format!("{}:{}:{action_index}", checkpoint.date, checkpoint.phase),
                &before_weights,
                &target_weights,
                &input.config.simulated_venue,
                &input.config.simulated_account,
            )?;
            for settlement in &execution.settlements {
                settlement_ledger.submit(settlement.clone());
            }
            let turnover = execution.turnover;
            let trade_cost = if turnover > 0.0 && input.config.cost_rate > 0.0 {
                let cost = equity * turnover * input.config.cost_rate;
                equity *= (1.0 - turnover * input.config.cost_rate).max(0.0);
                cost
            } else {
                0.0
            };
            if turnover > 1e-12 {
                active_rebalances += 1;
            }
            checkpoint_turnover += turnover;
            checkpoint_trade_cost += trade_cost;
            current_weights = execution.resulting_weights.clone();
            action_events.push(TimelineActionEvent {
                action: action_name,
                reason: action.reason,
                skipped,
                skip_reason,
                turnover,
                trade_cost,
                before_weights: trim_weights(&before_weights),
                target_weights: trim_weights(&execution.resulting_weights),
                orders: execution.orders,
                settlements: execution.settlements,
                risk_gate_events: action_risk_events.clone(),
            });
            risk_gate_events.extend(action_risk_events);
        }

        previous_weights = trim_weights(&current_weights);
        previous_cash_weight = cash_weight_for(&previous_weights);
        let gross_exposure = gross_exposure(&previous_weights);
        let cost_drag = if equity_before_borrow > 0.0 {
            (checkpoint_trade_cost + borrow_cost) / equity_before_borrow
        } else {
            0.0
        };
        turnover_sum += checkpoint_turnover;
        gross_sum += gross_exposure;
        if let Some(daily) = current_daily.as_mut() {
            daily.portfolio_return += portfolio_return;
            daily.turnover += checkpoint_turnover;
            daily.trade_cost += checkpoint_trade_cost;
            daily.borrow_cost += borrow_cost;
            daily.cost_drag += cost_drag;
            add_contribution(&mut daily.contribution, &contribution);
        }

        events.push(TimelineCheckpointEvent {
            date: checkpoint.date,
            phase: normalize_phase(&checkpoint.phase)?,
            equity_before_return,
            equity_before_trade: pre_trade_equity,
            equity_after_trade: equity,
            portfolio_return,
            turnover: checkpoint_turnover,
            trade_cost: checkpoint_trade_cost,
            borrow_cost,
            cost_drag,
            cash_weight: previous_cash_weight,
            gross_exposure,
            active_positions: active_positions(&previous_weights),
            target_weights: previous_weights.clone(),
            drift_weights: trim_weights(&drift_weights),
            contribution,
            actions: action_events,
        });
    }

    if let Some(daily) = current_daily.take() {
        daily_events.push(daily.finish(&previous_weights, previous_cash_weight, equity));
    }

    let checkpoints = events.len();
    let settlement_events = settlement_ledger.events().to_vec();
    let result_tables = build_result_tables(
        &events,
        &daily_events,
        &risk_gate_events,
        &settlement_events,
    );
    let result_validation = validate_result_tables(ResultTableView {
        result_schema_version: &result_tables.schema_version,
        equity_curve: &result_tables.equity_curve,
        holdings: &result_tables.holdings,
        rebalance_audit: &result_tables.rebalance_audit,
        rebalance_trades: &result_tables.rebalance_trades,
        risk_gate_events: &result_tables.risk_gate_events,
        settlements: &result_tables.settlements,
    })?;
    Ok(TimelineAccountingSummary {
        start_equity,
        final_equity: equity,
        total_return: equity / start_equity - 1.0,
        checkpoints,
        days: daily_events.len(),
        active_rebalances,
        average_turnover: turnover_sum / checkpoints as f64,
        average_gross_exposure: gross_sum / checkpoints as f64,
        events,
        daily_events,
        risk_gate_events,
        settlement_events,
        result_tables,
        result_validation,
    })
}

fn build_result_tables(
    events: &[TimelineCheckpointEvent],
    daily_events: &[TimelineDailyEvent],
    risk_gate_events: &[TimelineRiskGateEvent],
    settlement_events: &[SettlementEvent],
) -> TimelineResultTables {
    TimelineResultTables {
        schema_version: "rust_timeline_result_tables.v1".to_string(),
        equity_curve: build_equity_rows(daily_events),
        holdings: build_holding_rows(events),
        rebalance_audit: build_rebalance_rows(events),
        rebalance_trades: build_trade_rows(events),
        risk_gate_events: build_risk_gate_rows(risk_gate_events),
        settlements: build_settlement_rows(settlement_events),
    }
}

fn build_settlement_rows(events: &[SettlementEvent]) -> Vec<BTreeMap<String, Value>> {
    events
        .iter()
        .map(|event| {
            let mut row = BTreeMap::new();
            row.insert("Order_id".to_string(), json!(event.order_id));
            row.insert("Asset".to_string(), json!(event.asset));
            row.insert(
                "Remaining_sessions".to_string(),
                json!(event.remaining_sessions),
            );
            row.insert("Cash_delta".to_string(), json!(event.cash_delta));
            row.insert("Status".to_string(), json!(event.status));
            row
        })
        .collect()
}

fn build_equity_rows(daily_events: &[TimelineDailyEvent]) -> Vec<BTreeMap<String, Value>> {
    let assets = daily_events
        .iter()
        .flat_map(|event| {
            event
                .target_weights
                .keys()
                .chain(event.contribution.keys())
                .cloned()
        })
        .collect::<BTreeSet<_>>();
    daily_events
        .iter()
        .map(|event| {
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert("Equity_value".to_string(), json!(event.equity_after_trade));
            row.insert(
                "Portfolio_return".to_string(),
                json!(event.portfolio_return),
            );
            row.insert("Turnover".to_string(), json!(event.turnover));
            row.insert("Trade_cost".to_string(), json!(event.trade_cost));
            row.insert("Borrow_cost".to_string(), json!(event.borrow_cost));
            row.insert("Cost_drag".to_string(), json!(event.cost_drag));
            row.insert("Selected_count".to_string(), json!(event.active_positions));
            row.insert("Gross_exposure".to_string(), json!(event.gross_exposure));
            row.insert("Cash_weight".to_string(), json!(event.cash_weight));
            for asset in &assets {
                row.insert(
                    format!("Weight_{asset}"),
                    json!(*event.target_weights.get(asset).unwrap_or(&0.0)),
                );
                row.insert(
                    format!("Contribution_{asset}"),
                    json!(*event.contribution.get(asset).unwrap_or(&0.0)),
                );
            }
            row
        })
        .collect()
}

fn build_holding_rows(events: &[TimelineCheckpointEvent]) -> Vec<BTreeMap<String, Value>> {
    let mut rows = Vec::new();
    for event in events {
        for action in active_action_events(event) {
            let ranked_assets = ranked_assets_for_action(action);
            let selected_assets = selected_assets_for_action(action);
            for (rank, asset) in ranked_assets.iter().enumerate() {
                let target = *action.target_weights.get(asset).unwrap_or(&0.0);
                let mut row = BTreeMap::new();
                row.insert("Time".to_string(), json!(event.date));
                row.insert("Asset".to_string(), json!(asset));
                row.insert("Rank".to_string(), json!(rank + 1));
                row.insert(
                    "Selected".to_string(),
                    json!(selected_assets.contains(asset)),
                );
                row.insert("Eligible".to_string(), json!(true));
                row.insert("Score".to_string(), Value::Null);
                row.insert("Target_weight".to_string(), json!(target));
                rows.push(row);
            }
        }
    }
    rows
}

fn build_rebalance_rows(events: &[TimelineCheckpointEvent]) -> Vec<BTreeMap<String, Value>> {
    let mut rows = Vec::new();
    for event in events {
        for action in active_action_events(event) {
            let selected_assets = selected_assets_for_action(action);
            let ranked_assets = ranked_assets_for_action(action);
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert("Rebalance".to_string(), json!(true));
            row.insert("Selected_assets".to_string(), json!(selected_assets));
            row.insert("Selected_count".to_string(), json!(selected_assets.len()));
            row.insert("Ranked_candidates".to_string(), json!(ranked_assets));
            row.insert("Turnover".to_string(), json!(action.turnover));
            row.insert("Cost_rate".to_string(), Value::Null);
            row.insert("Trade_cost".to_string(), json!(action.trade_cost));
            row.insert("Equity_value".to_string(), json!(event.equity_after_trade));
            rows.push(row);
        }
    }
    rows
}

fn build_trade_rows(events: &[TimelineCheckpointEvent]) -> Vec<BTreeMap<String, Value>> {
    let mut rows = Vec::new();
    for event in events {
        for action in active_action_events(event) {
            for asset in asset_union_for_weights(&action.before_weights, &action.target_weights) {
                let before = *action.before_weights.get(&asset).unwrap_or(&0.0);
                let target = *action.target_weights.get(&asset).unwrap_or(&0.0);
                let delta = target - before;
                if delta.abs() <= 1e-12 {
                    continue;
                }
                let trade_action = if delta > 1e-12 {
                    "buy"
                } else if target <= 1e-12 {
                    "exit"
                } else {
                    "sell"
                };
                let selected = target.abs() > 1e-12;
                let mut row = BTreeMap::new();
                row.insert("Time".to_string(), json!(event.date));
                row.insert("Asset".to_string(), json!(asset));
                row.insert("Before_weight".to_string(), json!(before));
                row.insert("Target_weight".to_string(), json!(target));
                row.insert("Trade_delta".to_string(), json!(delta));
                row.insert("Action".to_string(), json!(trade_action));
                row.insert("Trade_turnover".to_string(), json!(delta.abs()));
                row.insert("Allocated_cost".to_string(), json!(action.trade_cost));
                row.insert("Selected".to_string(), json!(selected));
                row.insert("Eligible".to_string(), json!(true));
                row.insert("Rank".to_string(), json!(1));
                row.insert("Score".to_string(), Value::Null);
                row.insert(
                    "Reason".to_string(),
                    json!(action
                        .reason
                        .clone()
                        .unwrap_or_else(|| "rust timeline result table".to_string())),
                );
                rows.push(row);
            }
        }
    }
    rows
}

fn build_risk_gate_rows(
    risk_gate_events: &[TimelineRiskGateEvent],
) -> Vec<BTreeMap<String, Value>> {
    risk_gate_events
        .iter()
        .map(|event| {
            let mut row = BTreeMap::new();
            row.insert("Time".to_string(), json!(event.date));
            row.insert("Gate".to_string(), json!(event.gate));
            row.insert("Threshold".to_string(), json!(event.threshold));
            row.insert("Observed".to_string(), json!(event.observed));
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

fn active_action_events(
    event: &TimelineCheckpointEvent,
) -> impl Iterator<Item = &TimelineActionEvent> {
    event
        .actions
        .iter()
        .filter(|action| action.turnover > 1e-12)
}

fn selected_assets_for_action(action: &TimelineActionEvent) -> Vec<String> {
    action
        .target_weights
        .iter()
        .filter_map(|(asset, weight)| {
            if weight.abs() > 1e-12 {
                Some(asset.clone())
            } else {
                None
            }
        })
        .collect()
}

fn ranked_assets_for_action(action: &TimelineActionEvent) -> Vec<String> {
    asset_union_for_weights(&action.before_weights, &action.target_weights)
        .into_iter()
        .filter(|asset| {
            action
                .before_weights
                .get(asset)
                .copied()
                .unwrap_or(0.0)
                .abs()
                > 1e-12
                || action
                    .target_weights
                    .get(asset)
                    .copied()
                    .unwrap_or(0.0)
                    .abs()
                    > 1e-12
        })
        .collect()
}

#[derive(Debug)]
struct DailyAccumulator {
    date: String,
    portfolio_return: f64,
    turnover: f64,
    trade_cost: f64,
    borrow_cost: f64,
    cost_drag: f64,
    contribution: BTreeMap<String, f64>,
}

impl DailyAccumulator {
    fn new(date: String) -> Self {
        Self {
            date,
            portfolio_return: 0.0,
            turnover: 0.0,
            trade_cost: 0.0,
            borrow_cost: 0.0,
            cost_drag: 0.0,
            contribution: BTreeMap::new(),
        }
    }

    fn finish(
        self,
        weights: &BTreeMap<String, f64>,
        cash_weight: f64,
        equity: f64,
    ) -> TimelineDailyEvent {
        TimelineDailyEvent {
            date: self.date,
            equity_after_trade: equity,
            portfolio_return: self.portfolio_return,
            turnover: self.turnover,
            trade_cost: self.trade_cost,
            borrow_cost: self.borrow_cost,
            cost_drag: self.cost_drag,
            cash_weight,
            gross_exposure: gross_exposure(weights),
            active_positions: active_positions(weights),
            target_weights: weights.clone(),
            contribution: self.contribution,
        }
    }
}

fn validate_config(config: &TimelineAccountingConfig) -> Result<(), TimelineAccountingError> {
    if !config.starting_equity.is_finite() || config.starting_equity <= 0.0 {
        return Err(TimelineAccountingError::InvalidStartingEquity);
    }
    if !config.cost_rate.is_finite() || config.cost_rate < 0.0 {
        return Err(TimelineAccountingError::InvalidCostRate);
    }
    if !config.short_borrow_rate_annual.is_finite() || config.short_borrow_rate_annual < 0.0 {
        return Err(TimelineAccountingError::InvalidShortBorrowRate);
    }
    if config.borrow_day_count == 0 {
        return Err(TimelineAccountingError::InvalidShortBorrowDayCount);
    }
    if !config.max_gross_exposure.is_finite() || config.max_gross_exposure <= 0.0 {
        return Err(TimelineAccountingError::InvalidMaxGrossExposure);
    }
    validate_policy(&config.position_policy.on_entry_signal_while_holding)?;
    validate_risk_gates(&config.risk_gates)?;
    Ok(())
}

fn validate_risk_gates(gates: &TimelineRiskGateConfig) -> Result<(), TimelineAccountingError> {
    for (field, value) in [
        ("risk_gates.max_daily_loss", gates.max_daily_loss),
        ("risk_gates.max_order_size", gates.max_order_size),
        ("risk_gates.max_drawdown", gates.max_drawdown),
        (
            "risk_gates.reduce_exposure_factor",
            gates.reduce_exposure_factor,
        ),
    ] {
        if let Some(value) = value {
            if !value.is_finite() {
                return Err(TimelineAccountingError::NonFiniteValue {
                    field,
                    asset: "portfolio".to_string(),
                });
            }
        }
    }
    let action = gates.gate_action.as_deref().unwrap_or("").trim();
    if (gates.max_daily_loss.is_some() || gates.max_drawdown.is_some()) && action.is_empty() {
        return Err(TimelineAccountingError::MissingRiskGateAction);
    }
    if !action.is_empty()
        && !matches!(
            action,
            "flatten"
                | "permanent_stop"
                | "shadow_until_recovery"
                | "block_new_orders"
                | "reduce_exposure"
        )
    {
        return Err(TimelineAccountingError::InvalidRiskAction(
            action.to_string(),
        ));
    }
    if action == "reduce_exposure"
        && !matches!(
            gates.reduce_exposure_factor,
            Some(value) if value.is_finite() && value > 0.0 && value <= 1.0
        )
    {
        return Err(TimelineAccountingError::InvalidReduceExposureFactor);
    }
    Ok(())
}

fn initialize_timeline_shadow_recovery_state(
    equity: f64,
    equity_peak: f64,
    before_weights: &BTreeMap<String, f64>,
    requested_target_weights: &BTreeMap<String, f64>,
    config: &TimelineAccountingConfig,
) -> Result<DrawdownRecoveryState, TimelineAccountingError> {
    let assets = asset_union_for_weights(before_weights, requested_target_weights);
    let turnover = turnover_between(before_weights, requested_target_weights, &assets);
    let trade_cost_factor = (1.0 - turnover * config.cost_rate).max(0.0);
    let shadow_equity = equity * trade_cost_factor;
    let mut control = RiskControlState::default();
    control.activate(SHADOW_ACTION, equity_peak)?;
    control.observe_shadow_equity(shadow_equity);
    Ok(DrawdownRecoveryState {
        control,
        shadow_equity,
        shadow_weights: trim_weights(requested_target_weights),
        shadow_cash_weight: cash_weight_for(requested_target_weights),
    })
}

fn update_timeline_shadow_recovery_state(
    state: &mut DrawdownRecoveryState,
    checkpoint: &TimelineCheckpointInput,
    config: &TimelineAccountingConfig,
) -> Result<bool, TimelineAccountingError> {
    if !state.control.is_shadow() {
        return Ok(false);
    }
    validate_held_asset_returns(&checkpoint.returns, &state.shadow_weights)?;
    let was_armed = state.control.recovery_armed();
    let assets = asset_union(
        &state.shadow_weights,
        &checkpoint.returns,
        &checkpoint.actions,
    );
    let equity_before_return = state.shadow_equity;
    let mut asset_values: BTreeMap<String, f64> = BTreeMap::new();
    let mut equity = equity_before_return * state.shadow_cash_weight;
    for asset in &assets {
        let previous_weight = *state.shadow_weights.get(asset).unwrap_or(&0.0);
        let asset_return = *checkpoint.returns.get(asset).unwrap_or(&0.0);
        let value_before = equity_before_return * previous_weight;
        let value_after = value_before * (1.0 + asset_return);
        asset_values.insert(asset.clone(), value_after);
        equity += value_after;
    }
    let drift_weights = if equity > 0.0 {
        asset_values
            .iter()
            .filter_map(|(asset, value)| {
                if value.abs() > 1e-12 {
                    Some((asset.clone(), value / equity))
                } else {
                    None
                }
            })
            .collect::<BTreeMap<_, _>>()
    } else {
        BTreeMap::new()
    };
    let mut current_weights = drift_weights;
    for action in &checkpoint.actions {
        let action_name = normalize_action(&action.action)?;
        let (target_weights, _, _) = apply_action_policy(
            config,
            &action_name,
            &current_weights,
            &action.target_weights,
        )?;
        let assets = asset_union_for_weights(&current_weights, &target_weights);
        let turnover = turnover_between(&current_weights, &target_weights, &assets);
        if turnover > 0.0 && config.cost_rate > 0.0 {
            equity *= (1.0 - turnover * config.cost_rate).max(0.0);
        }
        current_weights = trim_weights(&target_weights);
    }
    state.shadow_equity = equity;
    state.shadow_weights = current_weights;
    state.shadow_cash_weight = cash_weight_for(&state.shadow_weights);
    state.control.observe_shadow_equity(state.shadow_equity);
    Ok(!was_armed && state.control.recovery_armed())
}

fn validate_checkpoint(
    checkpoint: &TimelineCheckpointInput,
    config: &TimelineAccountingConfig,
) -> Result<(), TimelineAccountingError> {
    normalize_phase(&checkpoint.phase)?;
    for (asset, value) in &checkpoint.returns {
        if !value.is_finite() {
            return Err(TimelineAccountingError::NonFiniteValue {
                field: "returns",
                asset: asset.clone(),
            });
        }
    }
    for action in &checkpoint.actions {
        normalize_action(&action.action)?;
        for (asset, value) in &action.target_weights {
            if !value.is_finite() {
                return Err(TimelineAccountingError::NonFiniteValue {
                    field: "target_weights",
                    asset: asset.clone(),
                });
            }
            if !config.allow_short && *value < -1e-12 {
                return Err(TimelineAccountingError::NegativeWeightLongOnly(
                    asset.clone(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_held_asset_returns(
    returns: &BTreeMap<String, f64>,
    held_weights: &BTreeMap<String, f64>,
) -> Result<(), TimelineAccountingError> {
    for (asset, weight) in held_weights {
        if weight.abs() > 1e-12 && !returns.contains_key(asset) {
            return Err(TimelineAccountingError::MissingHeldAssetReturn(
                asset.clone(),
            ));
        }
    }
    Ok(())
}

fn validate_policy(policy: &str) -> Result<String, TimelineAccountingError> {
    let normalized = policy.trim().to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "ignore_new_signal" | "add_position" | "reset_timer"
    ) {
        Ok(normalized)
    } else {
        Err(TimelineAccountingError::InvalidPositionPolicy(
            policy.to_string(),
        ))
    }
}

fn normalize_phase(phase: &str) -> Result<String, TimelineAccountingError> {
    let normalized = phase.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "open" | "close") {
        Ok(normalized)
    } else {
        Err(TimelineAccountingError::InvalidPhase(phase.to_string()))
    }
}

fn phase_rank(phase: &str) -> Result<u8, TimelineAccountingError> {
    match normalize_phase(phase)?.as_str() {
        "open" => Ok(0),
        "close" => Ok(1),
        _ => unreachable!(),
    }
}

fn normalize_action(action: &str) -> Result<String, TimelineAccountingError> {
    let normalized = action.trim().to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "enter" | "exit" | "flatten" | "set_target_weights"
    ) {
        Ok(normalized)
    } else {
        Err(TimelineAccountingError::InvalidAction(action.to_string()))
    }
}

type ActionPolicyOutcome = (BTreeMap<String, f64>, bool, Option<String>);

fn apply_action_policy(
    config: &TimelineAccountingConfig,
    action: &str,
    current_weights: &BTreeMap<String, f64>,
    requested_weights: &BTreeMap<String, f64>,
) -> Result<ActionPolicyOutcome, TimelineAccountingError> {
    if matches!(action, "exit" | "flatten") {
        return Ok((BTreeMap::new(), false, None));
    }

    let requested = normalized_target_weights(requested_weights, config)?;
    if action == "enter" {
        let policy = validate_policy(&config.position_policy.on_entry_signal_while_holding)?;
        if policy == "ignore_new_signal" && overlaps(current_weights, &requested) {
            return Ok((
                current_weights.clone(),
                true,
                Some("entry_signal_while_holding_ignored".to_string()),
            ));
        }
        if policy == "add_position" {
            let combined = add_weights(current_weights, &requested);
            let gross = gross_exposure(&combined);
            if gross > config.max_gross_exposure + 1e-10 {
                return Ok((
                    current_weights.clone(),
                    true,
                    Some("insufficient_gross_exposure_capacity".to_string()),
                ));
            }
            return Ok((trim_weights(&combined), false, None));
        }
    }

    Ok((requested, false, None))
}

fn normalized_target_weights(
    target_weights: &BTreeMap<String, f64>,
    config: &TimelineAccountingConfig,
) -> Result<BTreeMap<String, f64>, TimelineAccountingError> {
    let out = trim_weights(target_weights);
    let gross = gross_exposure(&out);
    if gross > config.max_gross_exposure + 1e-10 {
        return Err(TimelineAccountingError::GrossExposureExceeded {
            actual: gross,
            limit: config.max_gross_exposure,
        });
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn apply_risk_gates(
    gates: &TimelineRiskGateConfig,
    symbols: &BTreeSet<String>,
    before_weights: &BTreeMap<String, f64>,
    target_weights: &BTreeMap<String, f64>,
    equity: f64,
    equity_peak: f64,
    daily_return: f64,
    date: &str,
    phase: &str,
) -> (BTreeMap<String, f64>, Vec<TimelineRiskGateEvent>) {
    let mut adjusted = target_weights.clone();
    let mut events = Vec::new();
    if !risk_gates_enabled(gates) {
        return (adjusted, events);
    }

    if let Some(limit) = gates.max_daily_loss {
        if daily_return.is_finite() && daily_return <= -limit.abs() {
            adjusted = apply_gate_action(gates, &adjusted, before_weights, symbols);
            events.push(risk_gate_event(
                date,
                phase,
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
            let drawdown = equity / equity_peak - 1.0;
            if drawdown <= -limit.abs() {
                adjusted = apply_gate_action(gates, &adjusted, before_weights, symbols);
                events.push(risk_gate_event(
                    date,
                    phase,
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
                date,
                phase,
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
                date,
                phase,
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

fn risk_gates_enabled(gates: &TimelineRiskGateConfig) -> bool {
    gates.max_positions.is_some()
        || gates.max_daily_loss.is_some()
        || gates.max_order_size.is_some()
        || gates.max_drawdown.is_some()
}

fn effective_gate_action(gates: &TimelineRiskGateConfig) -> String {
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
    gates: &TimelineRiskGateConfig,
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
                    let adjusted = *value * factor;
                    if adjusted.abs() > 1e-12 {
                        Some((asset.clone(), adjusted))
                    } else {
                        None
                    }
                })
                .collect()
        }
        "block_new_orders" => symbols
            .iter()
            .filter_map(|asset| {
                let target_value = *target.get(asset).unwrap_or(&0.0);
                let before_value = *before.get(asset).unwrap_or(&0.0);
                let same_direction = target_value.signum()
                    == if before_value != 0.0 {
                        before_value.signum()
                    } else {
                        target_value.signum()
                    };
                let adjusted = if target_value.abs() > before_value.abs() && same_direction {
                    before_value
                } else {
                    target_value
                };
                if adjusted.abs() > 1e-12 {
                    Some((asset.clone(), adjusted))
                } else {
                    None
                }
            })
            .collect(),
        _ => target.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn risk_gate_event(
    date: &str,
    phase: &str,
    gate: &str,
    threshold: f64,
    observed: f64,
    action: String,
    affected_assets: Vec<String>,
    resulting_target_weights: &BTreeMap<String, f64>,
) -> TimelineRiskGateEvent {
    TimelineRiskGateEvent {
        date: date.to_string(),
        phase: phase.to_string(),
        gate: gate.to_string(),
        threshold,
        observed,
        action,
        affected_assets,
        resulting_target_weights: trim_weights(resulting_target_weights),
    }
}

fn asset_union(
    previous_weights: &BTreeMap<String, f64>,
    returns: &BTreeMap<String, f64>,
    actions: &[TimelineActionInput],
) -> BTreeSet<String> {
    let mut assets: BTreeSet<String> = previous_weights.keys().cloned().collect();
    assets.extend(returns.keys().cloned());
    for action in actions {
        assets.extend(action.target_weights.keys().cloned());
    }
    assets
}

fn asset_union_for_weights(
    before_weights: &BTreeMap<String, f64>,
    target_weights: &BTreeMap<String, f64>,
) -> BTreeSet<String> {
    before_weights
        .keys()
        .chain(target_weights.keys())
        .cloned()
        .collect()
}

fn add_weights(lhs: &BTreeMap<String, f64>, rhs: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    let assets = asset_union_for_weights(lhs, rhs);
    assets
        .into_iter()
        .filter_map(|asset| {
            let value = lhs.get(&asset).unwrap_or(&0.0) + rhs.get(&asset).unwrap_or(&0.0);
            if value.abs() > 1e-12 {
                Some((asset, value))
            } else {
                None
            }
        })
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

fn overlaps(
    current_weights: &BTreeMap<String, f64>,
    target_weights: &BTreeMap<String, f64>,
) -> bool {
    target_weights.iter().any(|(asset, weight)| {
        weight.abs() > 1e-12 && current_weights.get(asset).unwrap_or(&0.0).abs() > 1e-12
    })
}

fn cash_weight_for(weights: &BTreeMap<String, f64>) -> f64 {
    1.0 - weights.values().sum::<f64>()
}

fn gross_exposure(weights: &BTreeMap<String, f64>) -> f64 {
    weights.values().map(|value| value.abs()).sum()
}

fn active_positions(weights: &BTreeMap<String, f64>) -> usize {
    weights.values().filter(|value| value.abs() > 1e-12).count()
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

fn add_contribution(lhs: &mut BTreeMap<String, f64>, rhs: &BTreeMap<String, f64>) {
    for (asset, value) in rhs {
        *lhs.entry(asset.clone()).or_insert(0.0) += value;
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

    fn action(action: &str, rows: &[(&str, f64)]) -> TimelineActionInput {
        TimelineActionInput {
            action: action.to_string(),
            target_weights: weights(rows),
            reason: None,
        }
    }

    #[test]
    fn missing_held_asset_return_fails_closed() {
        let error = run_timeline_accounting(TimelineAccountingInput {
            config: TimelineAccountingConfig::default(),
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "close".to_string(),
                    returns: BTreeMap::new(),
                    actions: Vec::new(),
                },
            ],
        })
        .expect_err("missing held-asset returns must fail");

        assert_eq!(
            error,
            TimelineAccountingError::MissingHeldAssetReturn("AAA".to_string())
        );
    }

    #[test]
    fn same_day_open_to_close_timeline_accounts_two_phases() {
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config: TimelineAccountingConfig::default(),
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "close".to_string(),
                    returns: weights(&[("AAA", 0.10)]),
                    actions: vec![action("exit", &[])],
                },
            ],
        })
        .unwrap();

        assert_abs_diff_eq!(summary.final_equity, 110.0, epsilon = 1e-12);
        assert_eq!(summary.days, 1);
        assert_abs_diff_eq!(
            summary.daily_events[0].portfolio_return,
            0.10,
            epsilon = 1e-12
        );
        assert_abs_diff_eq!(summary.daily_events[0].turnover, 2.0, epsilon = 1e-12);
        assert_abs_diff_eq!(summary.daily_events[0].cash_weight, 1.0, epsilon = 1e-12);
        assert_eq!(
            summary.result_tables.schema_version,
            "rust_timeline_result_tables.v1"
        );
        assert_eq!(summary.result_tables.equity_curve.len(), 1);
        assert_eq!(summary.result_tables.rebalance_audit.len(), 2);
        assert_eq!(summary.result_tables.rebalance_trades.len(), 2);
        assert_eq!(summary.result_tables.holdings.len(), 2);
        assert_abs_diff_eq!(
            summary.result_tables.equity_curve[0]
                .get("Equity_value")
                .and_then(Value::as_f64)
                .unwrap(),
            110.0,
            epsilon = 1e-12
        );
    }

    #[test]
    fn next_open_entry_later_close_exit_sorts_open_before_close() {
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config: TimelineAccountingConfig::default(),
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-04".to_string(),
                    phase: "close".to_string(),
                    returns: weights(&[("AAA", 0.05)]),
                    actions: vec![action("exit", &[])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![action("enter", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "close".to_string(),
                    returns: weights(&[("AAA", 0.02)]),
                    actions: vec![],
                },
                TimelineCheckpointInput {
                    date: "2024-01-04".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.01)]),
                    actions: vec![],
                },
            ],
        })
        .unwrap();

        assert_abs_diff_eq!(
            summary.final_equity,
            100.0 * 1.02 * 1.01 * 1.05,
            epsilon = 1e-12
        );
        assert_eq!(summary.events[0].phase, "open");
        assert_eq!(summary.events[1].phase, "close");
        assert_eq!(summary.events[2].phase, "open");
        assert_eq!(summary.events[3].phase, "close");
    }

    #[test]
    fn ignore_new_signal_while_holding_skips_overlapping_enter() {
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config: TimelineAccountingConfig::default(),
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 0.5)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![action("enter", &[("AAA", 0.5)])],
                },
            ],
        })
        .unwrap();

        assert!(summary.events[1].actions[0].skipped);
        assert_abs_diff_eq!(summary.events[1].turnover, 0.0, epsilon = 1e-12);
        assert_abs_diff_eq!(summary.final_equity, 100.0, epsilon = 1e-12);
    }

    #[test]
    fn add_position_combines_weight_when_capacity_allows() {
        let mut config = TimelineAccountingConfig::default();
        config.position_policy.on_entry_signal_while_holding = "add_position".to_string();
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 0.25)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![action("enter", &[("AAA", 0.25)])],
                },
            ],
        })
        .unwrap();

        assert_abs_diff_eq!(
            summary.events[1].target_weights["AAA"],
            0.5,
            epsilon = 1e-12
        );
        assert_abs_diff_eq!(summary.events[1].turnover, 0.25, epsilon = 1e-12);
    }

    #[test]
    fn add_position_skips_when_capacity_is_insufficient() {
        let mut config = TimelineAccountingConfig::default();
        config.position_policy.on_entry_signal_while_holding = "add_position".to_string();
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 0.75)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![action("enter", &[("AAA", 0.5)])],
                },
            ],
        })
        .unwrap();

        assert!(summary.events[1].actions[0].skipped);
        assert_eq!(
            summary.events[1].actions[0].skip_reason.as_deref(),
            Some("insufficient_gross_exposure_capacity")
        );
        assert_abs_diff_eq!(
            summary.events[1].target_weights["AAA"],
            0.75,
            epsilon = 1e-12
        );
    }

    #[test]
    fn risk_gate_max_order_size_clamps_timeline_action_delta() {
        let mut config = TimelineAccountingConfig::default();
        config.risk_gates.max_order_size = Some(0.25);
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![TimelineCheckpointInput {
                date: "2024-01-02".to_string(),
                phase: "open".to_string(),
                returns: BTreeMap::new(),
                actions: vec![action("enter", &[("AAA", 1.0)])],
            }],
        })
        .unwrap();

        assert_abs_diff_eq!(
            summary.events[0].target_weights["AAA"],
            0.25,
            epsilon = 1e-12
        );
        assert_abs_diff_eq!(summary.events[0].turnover, 0.25, epsilon = 1e-12);
        assert_eq!(summary.risk_gate_events[0].gate, "max_order_size");
        assert_eq!(
            summary.events[0].actions[0].risk_gate_events[0].affected_assets,
            vec!["AAA".to_string()]
        );
    }

    #[test]
    fn risk_gate_max_positions_reduces_selected_assets() {
        let mut config = TimelineAccountingConfig::default();
        config.risk_gates.max_positions = Some(1);
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![TimelineCheckpointInput {
                date: "2024-01-02".to_string(),
                phase: "open".to_string(),
                returns: BTreeMap::new(),
                actions: vec![action("set_target_weights", &[("AAA", 0.7), ("BBB", 0.3)])],
            }],
        })
        .unwrap();

        assert_abs_diff_eq!(
            summary.events[0].target_weights["AAA"],
            0.7,
            epsilon = 1e-12
        );
        assert!(!summary.events[0].target_weights.contains_key("BBB"));
        assert_abs_diff_eq!(summary.events[0].turnover, 0.7, epsilon = 1e-12);
        assert_eq!(summary.risk_gate_events[0].gate, "max_positions");
        assert_eq!(
            summary.risk_gate_events[0].affected_assets,
            vec!["BBB".to_string()]
        );
    }

    #[test]
    fn max_drawdown_shadow_recovers_and_resumes_on_next_action() {
        let mut config = TimelineAccountingConfig::default();
        config.risk_gates.max_drawdown = Some(0.10);
        config.risk_gates.gate_action = Some("shadow_until_recovery".to_string());
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", -0.20)]),
                    actions: vec![action("set_target_weights", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-04".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.25)]),
                    actions: vec![],
                },
                TimelineCheckpointInput {
                    date: "2024-01-05".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![action("set_target_weights", &[("AAA", 1.0)])],
                },
            ],
        })
        .unwrap();

        assert_eq!(summary.risk_gate_events[0].gate, "max_drawdown");
        assert_eq!(summary.risk_gate_events[0].action, "shadow_until_recovery");
        assert!(summary.events[1].target_weights.is_empty());
        assert!(summary.events[3].target_weights.contains_key("AAA"));
        assert_abs_diff_eq!(
            summary.events[3].target_weights["AAA"],
            1.0,
            epsilon = 1e-12
        );
    }

    #[test]
    fn retired_pause_action_is_rejected() {
        let mut config = TimelineAccountingConfig::default();
        config.risk_gates.max_drawdown = Some(0.10);
        config.risk_gates.gate_action = Some("pause_trading".to_string());
        let error = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![TimelineCheckpointInput {
                date: "2024-01-02".to_string(),
                phase: "open".to_string(),
                returns: BTreeMap::new(),
                actions: vec![action("enter", &[("AAA", 1.0)])],
            }],
        })
        .unwrap_err();

        assert_eq!(
            error,
            TimelineAccountingError::InvalidRiskAction("pause_trading".to_string())
        );
    }

    #[test]
    fn loss_gate_requires_explicit_action_and_reduce_factor() {
        let mut config = TimelineAccountingConfig::default();
        config.risk_gates.max_drawdown = Some(0.1);
        let missing_action = run_timeline_accounting(TimelineAccountingInput {
            config: config.clone(),
            checkpoints: vec![TimelineCheckpointInput {
                date: "2024-01-02".to_string(),
                phase: "open".to_string(),
                returns: BTreeMap::new(),
                actions: vec![],
            }],
        })
        .unwrap_err();
        assert_eq!(
            missing_action,
            TimelineAccountingError::MissingRiskGateAction
        );

        config.risk_gates.gate_action = Some("reduce_exposure".to_string());
        let missing_factor = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![TimelineCheckpointInput {
                date: "2024-01-02".to_string(),
                phase: "open".to_string(),
                returns: BTreeMap::new(),
                actions: vec![],
            }],
        })
        .unwrap_err();
        assert_eq!(
            missing_factor,
            TimelineAccountingError::InvalidReduceExposureFactor
        );
    }

    #[test]
    fn max_drawdown_permanent_stop_blocks_every_later_action() {
        let mut config = TimelineAccountingConfig::default();
        config.risk_gates.max_drawdown = Some(0.10);
        config.risk_gates.gate_action = Some("permanent_stop".to_string());
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", -0.20)]),
                    actions: vec![action("set_target_weights", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-04".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("set_target_weights", &[("AAA", 1.0)])],
                },
            ],
        })
        .unwrap();

        assert_eq!(summary.risk_gate_events[0].action, "permanent_stop");
        assert!(summary.events[1].target_weights.is_empty());
        assert!(summary.events[2].target_weights.is_empty());
        assert_eq!(
            summary.events[2].actions[0].skip_reason.as_deref(),
            Some("permanent_stop_active")
        );
    }

    #[test]
    fn maintenance_margin_breach_inserts_liquidation_and_flattens_account() {
        let config = TimelineAccountingConfig {
            allow_short: true,
            max_gross_exposure: 2.0,
            simulated_account: SimulatedAccountConfig {
                account_type: crate::simulation::SimulatedAccountType::Margin,
                leverage_limit: 2.0,
                initial_margin_ratio: 0.5,
                maintenance_margin_ratio: 0.6,
                allow_short_borrow: true,
                settlement_days: 0,
            },
            ..TimelineAccountingConfig::default()
        };
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("set_target_weights", &[("AAA", 2.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![],
                },
            ],
        })
        .unwrap();

        assert_eq!(summary.events[1].actions[0].action, "flatten");
        assert_eq!(
            summary.events[1].actions[0].reason.as_deref(),
            Some("maintenance_margin_liquidation")
        );
        assert!(summary.events[1].target_weights.is_empty());
        assert_eq!(summary.risk_gate_events[0].gate, "maintenance_margin");
        assert_eq!(summary.risk_gate_events[0].action, "margin_liquidation");
    }

    #[test]
    fn timeline_charges_short_borrow_once_per_held_session() {
        let config = TimelineAccountingConfig {
            allow_short: true,
            short_borrow_rate_annual: 0.252,
            borrow_day_count: 252,
            simulated_account: SimulatedAccountConfig {
                account_type: crate::simulation::SimulatedAccountType::Margin,
                allow_short_borrow: true,
                ..SimulatedAccountConfig::default()
            },
            ..TimelineAccountingConfig::default()
        };
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action(
                        "set_target_weights",
                        &[("LONG", 0.5), ("SHORT", -0.5)],
                    )],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("LONG", 0.0), ("SHORT", 0.0)]),
                    actions: vec![],
                },
            ],
        })
        .unwrap();

        assert_abs_diff_eq!(summary.events[0].borrow_cost, 0.0, epsilon = 1e-12);
        assert_abs_diff_eq!(summary.events[1].borrow_cost, 0.05, epsilon = 1e-12);
        assert_abs_diff_eq!(summary.final_equity, 99.95, epsilon = 1e-12);
    }

    #[test]
    fn timeline_advances_t_plus_two_settlements_across_sessions() {
        let mut config = TimelineAccountingConfig::default();
        config.simulated_account.settlement_days = 2;
        let summary = run_timeline_accounting(TimelineAccountingInput {
            config,
            checkpoints: vec![
                TimelineCheckpointInput {
                    date: "2024-01-02".to_string(),
                    phase: "open".to_string(),
                    returns: BTreeMap::new(),
                    actions: vec![action("enter", &[("AAA", 1.0)])],
                },
                TimelineCheckpointInput {
                    date: "2024-01-03".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![],
                },
                TimelineCheckpointInput {
                    date: "2024-01-04".to_string(),
                    phase: "open".to_string(),
                    returns: weights(&[("AAA", 0.0)]),
                    actions: vec![],
                },
            ],
        })
        .unwrap();

        assert_eq!(summary.settlement_events.len(), 2);
        assert_eq!(
            summary.settlement_events[0].status,
            crate::simulation::SettlementStatus::Pending
        );
        assert_eq!(
            summary.settlement_events[1].status,
            crate::simulation::SettlementStatus::Settled
        );
        assert_eq!(summary.result_tables.settlements.len(), 2);
        assert_eq!(
            summary.result_tables.settlements[1]
                .get("Status")
                .and_then(Value::as_str),
            Some("settled")
        );
    }
}
