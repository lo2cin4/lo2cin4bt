use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    Rejected,
    Canceled,
    Expired,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SimulatedAccountType {
    Cash,
    Margin,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    #[default]
    Gtc,
    Ioc,
    Fok,
    Day,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedAccountConfig {
    pub account_type: SimulatedAccountType,
    pub leverage_limit: f64,
    pub initial_margin_ratio: f64,
    pub maintenance_margin_ratio: f64,
    pub allow_short_borrow: bool,
    pub settlement_days: u32,
}

impl Default for SimulatedAccountConfig {
    fn default() -> Self {
        Self {
            account_type: SimulatedAccountType::Cash,
            leverage_limit: 1.0,
            initial_margin_ratio: 1.0,
            maintenance_margin_ratio: 1.0,
            allow_short_borrow: false,
            settlement_days: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedVenueConfig {
    #[serde(default = "default_max_fill_fraction")]
    pub max_fill_fraction: f64,
    #[serde(default = "default_min_order_delta")]
    pub min_order_delta: f64,
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub atomic_batch: bool,
}

impl Default for SimulatedVenueConfig {
    fn default() -> Self {
        Self {
            max_fill_fraction: default_max_fill_fraction(),
            min_order_delta: default_min_order_delta(),
            time_in_force: TimeInForce::default(),
            atomic_batch: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedOrderEvent {
    pub order_id: String,
    pub asset: String,
    pub requested_delta: f64,
    pub filled_delta: f64,
    pub remaining_delta: f64,
    pub status: OrderStatus,
    pub transitions: Vec<OrderStatus>,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementInstruction {
    pub order_id: String,
    pub asset: String,
    pub due_after_sessions: u32,
    pub cash_delta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SettlementStatus {
    Pending,
    Settled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementEvent {
    pub order_id: String,
    pub asset: String,
    pub remaining_sessions: u32,
    pub cash_delta: f64,
    pub status: SettlementStatus,
}

#[derive(Debug, Clone, Default)]
pub struct SettlementLedger {
    pending: Vec<SettlementInstruction>,
    events: Vec<SettlementEvent>,
}

impl SettlementLedger {
    pub fn submit(&mut self, instruction: SettlementInstruction) {
        if instruction.due_after_sessions == 0 {
            self.events.push(SettlementEvent {
                order_id: instruction.order_id,
                asset: instruction.asset,
                remaining_sessions: 0,
                cash_delta: instruction.cash_delta,
                status: SettlementStatus::Settled,
            });
        } else {
            self.events.push(SettlementEvent {
                order_id: instruction.order_id.clone(),
                asset: instruction.asset.clone(),
                remaining_sessions: instruction.due_after_sessions,
                cash_delta: instruction.cash_delta,
                status: SettlementStatus::Pending,
            });
            self.pending.push(instruction);
        }
    }

    pub fn advance_session(&mut self) {
        for instruction in &mut self.pending {
            instruction.due_after_sessions = instruction.due_after_sessions.saturating_sub(1);
        }
        let mut still_pending = Vec::new();
        for instruction in self.pending.drain(..) {
            if instruction.due_after_sessions == 0 {
                self.events.push(SettlementEvent {
                    order_id: instruction.order_id,
                    asset: instruction.asset,
                    remaining_sessions: 0,
                    cash_delta: instruction.cash_delta,
                    status: SettlementStatus::Settled,
                });
            } else {
                still_pending.push(instruction);
            }
        }
        self.pending = still_pending;
    }

    pub fn events(&self) -> &[SettlementEvent] {
        &self.events
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[derive(Debug, Clone)]
pub struct SimulatedExecutionResult {
    pub resulting_weights: BTreeMap<String, f64>,
    pub turnover: f64,
    pub orders: Vec<SimulatedOrderEvent>,
    pub settlements: Vec<SettlementInstruction>,
}

#[derive(Debug, Error, PartialEq)]
pub enum SimulationError {
    #[error("max_fill_fraction must be in (0, 1]")]
    InvalidFillFraction,
    #[error("min_order_delta cannot be negative")]
    InvalidMinimumOrder,
    #[error("non-finite target weight for {0}")]
    NonFiniteWeight(String),
    #[error("invalid simulated account configuration: {0}")]
    InvalidAccount(String),
}

pub fn execute_target_weight_orders(
    order_namespace: &str,
    before_weights: &BTreeMap<String, f64>,
    requested_weights: &BTreeMap<String, f64>,
    config: &SimulatedVenueConfig,
    account: &SimulatedAccountConfig,
) -> Result<SimulatedExecutionResult, SimulationError> {
    validate_config(config)?;
    validate_account(account)?;
    let assets = before_weights
        .keys()
        .chain(requested_weights.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut resulting_weights = before_weights
        .iter()
        .filter(|(_, weight)| weight.abs() > 1e-12)
        .map(|(asset, weight)| (asset.clone(), *weight))
        .collect::<BTreeMap<_, _>>();
    let mut turnover = 0.0;
    let mut orders = Vec::new();
    let mut settlements = Vec::new();

    if let Some(reason) = preflight_rejection(requested_weights, account) {
        for (index, asset) in assets.into_iter().enumerate() {
            let before = finite_weight(before_weights, &asset)?;
            let requested = finite_weight(requested_weights, &asset)?;
            let requested_delta = requested - before;
            if requested_delta.abs() <= config.min_order_delta {
                continue;
            }
            orders.push(rejected_order(
                format!("{order_namespace}:{index}"),
                asset,
                requested_delta,
                reason.clone(),
            ));
        }
        return Ok(SimulatedExecutionResult {
            resulting_weights,
            turnover,
            orders,
            settlements,
        });
    }

    for (index, asset) in assets.into_iter().enumerate() {
        let before = finite_weight(before_weights, &asset)?;
        let requested = finite_weight(requested_weights, &asset)?;
        let requested_delta = requested - before;
        if requested_delta.abs() <= config.min_order_delta {
            continue;
        }
        if config.time_in_force == TimeInForce::Fok && config.max_fill_fraction < 1.0 {
            orders.push(rejected_order(
                format!("{order_namespace}:{index}"),
                asset,
                requested_delta,
                "fill_or_kill_not_fully_fillable".to_string(),
            ));
            continue;
        }
        let filled_delta = requested_delta * config.max_fill_fraction;
        let remaining_delta = requested_delta - filled_delta;
        let resulting = before + filled_delta;
        if resulting.abs() <= 1e-12 {
            resulting_weights.remove(&asset);
        } else {
            resulting_weights.insert(asset.clone(), resulting);
        }
        turnover += filled_delta.abs();
        let order_id = format!("{order_namespace}:{index}");
        let fill_status = if remaining_delta.abs() <= 1e-12 {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };
        let final_status = match (fill_status.clone(), config.time_in_force) {
            (OrderStatus::PartiallyFilled, TimeInForce::Ioc) => OrderStatus::Canceled,
            (OrderStatus::PartiallyFilled, TimeInForce::Day) => OrderStatus::Expired,
            (status, _) => status,
        };
        let mut transitions = vec![OrderStatus::Submitted, OrderStatus::Accepted, fill_status];
        if transitions.last() != Some(&final_status) {
            transitions.push(final_status.clone());
        }
        settlements.push(SettlementInstruction {
            order_id: order_id.clone(),
            asset: asset.clone(),
            due_after_sessions: account.settlement_days,
            cash_delta: -filled_delta,
        });
        orders.push(SimulatedOrderEvent {
            order_id,
            asset,
            requested_delta,
            filled_delta,
            remaining_delta,
            status: final_status,
            transitions,
            rejection_reason: None,
        });
    }

    Ok(SimulatedExecutionResult {
        resulting_weights,
        turnover,
        orders,
        settlements,
    })
}

pub fn maintenance_margin_breached(
    weights: &BTreeMap<String, f64>,
    account: &SimulatedAccountConfig,
) -> bool {
    if account.account_type != SimulatedAccountType::Margin {
        return false;
    }
    let gross = weights.values().map(|value| value.abs()).sum::<f64>();
    gross * account.maintenance_margin_ratio > 1.0 + 1e-12
}

fn preflight_rejection(
    requested_weights: &BTreeMap<String, f64>,
    account: &SimulatedAccountConfig,
) -> Option<String> {
    if !account.allow_short_borrow && requested_weights.values().any(|value| *value < -1e-12) {
        return Some("short_borrow_not_available".to_string());
    }
    let gross = requested_weights
        .values()
        .map(|value| value.abs())
        .sum::<f64>();
    if gross > account.leverage_limit + 1e-12 {
        return Some("leverage_limit_exceeded".to_string());
    }
    if gross * account.initial_margin_ratio > 1.0 + 1e-12 {
        return Some("initial_margin_insufficient".to_string());
    }
    None
}

fn rejected_order(
    order_id: String,
    asset: String,
    requested_delta: f64,
    reason: String,
) -> SimulatedOrderEvent {
    SimulatedOrderEvent {
        order_id,
        asset,
        requested_delta,
        filled_delta: 0.0,
        remaining_delta: requested_delta,
        status: OrderStatus::Rejected,
        transitions: vec![OrderStatus::Submitted, OrderStatus::Rejected],
        rejection_reason: Some(reason),
    }
}

fn finite_weight(weights: &BTreeMap<String, f64>, asset: &str) -> Result<f64, SimulationError> {
    let value = *weights.get(asset).unwrap_or(&0.0);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(SimulationError::NonFiniteWeight(asset.to_string()))
    }
}

fn validate_config(config: &SimulatedVenueConfig) -> Result<(), SimulationError> {
    if !config.max_fill_fraction.is_finite()
        || config.max_fill_fraction <= 0.0
        || config.max_fill_fraction > 1.0
    {
        return Err(SimulationError::InvalidFillFraction);
    }
    if !config.min_order_delta.is_finite() || config.min_order_delta < 0.0 {
        return Err(SimulationError::InvalidMinimumOrder);
    }
    Ok(())
}

fn validate_account(account: &SimulatedAccountConfig) -> Result<(), SimulationError> {
    if !account.leverage_limit.is_finite() || account.leverage_limit < 1.0 {
        return Err(SimulationError::InvalidAccount(
            "leverage_limit must be at least 1".to_string(),
        ));
    }
    for (name, value) in [
        ("initial_margin_ratio", account.initial_margin_ratio),
        ("maintenance_margin_ratio", account.maintenance_margin_ratio),
    ] {
        if !value.is_finite() || value <= 0.0 || value > 1.0 {
            return Err(SimulationError::InvalidAccount(format!(
                "{name} must be in (0, 1]"
            )));
        }
    }
    if account.account_type == SimulatedAccountType::Cash && account.leverage_limit != 1.0 {
        return Err(SimulationError::InvalidAccount(
            "cash account leverage_limit must equal 1".to_string(),
        ));
    }
    Ok(())
}

fn default_max_fill_fraction() -> f64 {
    1.0
}

fn default_min_order_delta() -> f64 {
    1e-12
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_venue_fills_target_delta_completely() {
        let result = execute_target_weight_orders(
            "checkpoint",
            &BTreeMap::from([("AAA".to_string(), 0.2)]),
            &BTreeMap::from([("AAA".to_string(), 0.8)]),
            &SimulatedVenueConfig::default(),
            &SimulatedAccountConfig::default(),
        )
        .expect("full fill should execute");

        assert!((result.resulting_weights["AAA"] - 0.8).abs() < 1e-12);
        assert!((result.turnover - 0.6).abs() < 1e-12);
        assert_eq!(result.orders[0].status, OrderStatus::Filled);
    }

    #[test]
    fn venue_partial_fill_leaves_remainder_open() {
        let result = execute_target_weight_orders(
            "checkpoint",
            &BTreeMap::new(),
            &BTreeMap::from([("AAA".to_string(), 1.0)]),
            &SimulatedVenueConfig {
                max_fill_fraction: 0.25,
                ..SimulatedVenueConfig::default()
            },
            &SimulatedAccountConfig::default(),
        )
        .expect("partial fill should execute");

        assert!((result.resulting_weights["AAA"] - 0.25).abs() < 1e-12);
        assert!((result.orders[0].remaining_delta - 0.75).abs() < 1e-12);
        assert_eq!(result.orders[0].status, OrderStatus::PartiallyFilled);
    }

    #[test]
    fn cash_account_rejects_leveraged_or_unborrowed_short_batch_atomically() {
        let result = execute_target_weight_orders(
            "pair",
            &BTreeMap::new(),
            &BTreeMap::from([("AAA".to_string(), 1.0), ("BBB".to_string(), -1.0)]),
            &SimulatedVenueConfig {
                atomic_batch: true,
                ..SimulatedVenueConfig::default()
            },
            &SimulatedAccountConfig::default(),
        )
        .expect("invalid account order should be represented as rejection");

        assert!(result.resulting_weights.is_empty());
        assert_eq!(result.orders.len(), 2);
        assert!(result
            .orders
            .iter()
            .all(|order| order.status == OrderStatus::Rejected));
        assert!(result
            .orders
            .iter()
            .all(|order| order.rejection_reason.as_deref() == Some("short_borrow_not_available")));
    }

    #[test]
    fn margin_pair_emits_settlement_ledger_for_both_legs() {
        let result = execute_target_weight_orders(
            "pair",
            &BTreeMap::new(),
            &BTreeMap::from([("AAA".to_string(), 1.0), ("BBB".to_string(), -1.0)]),
            &SimulatedVenueConfig {
                atomic_batch: true,
                ..SimulatedVenueConfig::default()
            },
            &SimulatedAccountConfig {
                account_type: SimulatedAccountType::Margin,
                leverage_limit: 2.0,
                initial_margin_ratio: 0.5,
                maintenance_margin_ratio: 0.25,
                allow_short_borrow: true,
                settlement_days: 2,
            },
        )
        .expect("margin pair should fill");

        assert_eq!(result.orders.len(), 2);
        assert!(result
            .orders
            .iter()
            .all(|order| order.status == OrderStatus::Filled));
        assert_eq!(result.settlements.len(), 2);
        assert!(result
            .settlements
            .iter()
            .all(|settlement| settlement.due_after_sessions == 2));
    }

    #[test]
    fn ioc_partial_fill_cancels_remainder() {
        let result = execute_target_weight_orders(
            "ioc",
            &BTreeMap::new(),
            &BTreeMap::from([("AAA".to_string(), 1.0)]),
            &SimulatedVenueConfig {
                max_fill_fraction: 0.25,
                time_in_force: TimeInForce::Ioc,
                ..SimulatedVenueConfig::default()
            },
            &SimulatedAccountConfig::default(),
        )
        .expect("ioc should partially fill then cancel");

        assert_eq!(result.orders[0].status, OrderStatus::Canceled);
        assert_eq!(
            result.orders[0].transitions,
            vec![
                OrderStatus::Submitted,
                OrderStatus::Accepted,
                OrderStatus::PartiallyFilled,
                OrderStatus::Canceled,
            ]
        );
        assert!((result.resulting_weights["AAA"] - 0.25).abs() < 1e-12);
    }

    #[test]
    fn maintenance_margin_detects_liquidation_boundary() {
        let account = SimulatedAccountConfig {
            account_type: SimulatedAccountType::Margin,
            leverage_limit: 4.0,
            initial_margin_ratio: 0.25,
            maintenance_margin_ratio: 0.30,
            allow_short_borrow: true,
            settlement_days: 0,
        };
        assert!(maintenance_margin_breached(
            &BTreeMap::from([("AAA".to_string(), 2.0), ("BBB".to_string(), -2.0)]),
            &account,
        ));
        assert!(!maintenance_margin_breached(
            &BTreeMap::from([("AAA".to_string(), 1.0), ("BBB".to_string(), -1.0)]),
            &account,
        ));
    }

    #[test]
    fn settlement_ledger_releases_instruction_after_configured_sessions() {
        let mut ledger = SettlementLedger::default();
        ledger.submit(SettlementInstruction {
            order_id: "order-1".to_string(),
            asset: "AAA".to_string(),
            due_after_sessions: 2,
            cash_delta: 1.0,
        });
        assert_eq!(ledger.pending_count(), 1);
        ledger.advance_session();
        assert_eq!(ledger.pending_count(), 1);
        ledger.advance_session();
        assert_eq!(ledger.pending_count(), 0);
        assert_eq!(
            ledger.events().last().unwrap().status,
            SettlementStatus::Settled
        );
    }
}
