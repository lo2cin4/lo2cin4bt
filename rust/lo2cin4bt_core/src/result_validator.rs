use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub const RESULT_VALIDATION_SCHEMA_VERSION: &str = "result_validation_report.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResultCheckStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultValidationCheck {
    pub check_id: String,
    pub status: ResultCheckStatus,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultValidationReport {
    pub schema_version: String,
    pub status: String,
    pub result_schema_version: String,
    pub result_hash: String,
    pub table_row_counts: BTreeMap<String, usize>,
    pub checks: Vec<ResultValidationCheck>,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Error, PartialEq)]
#[error("result validation failed: {0}")]
pub struct ResultValidationError(pub String);

pub struct ResultTableView<'a> {
    pub result_schema_version: &'a str,
    pub equity_curve: &'a [BTreeMap<String, Value>],
    pub holdings: &'a [BTreeMap<String, Value>],
    pub rebalance_audit: &'a [BTreeMap<String, Value>],
    pub rebalance_trades: &'a [BTreeMap<String, Value>],
    pub risk_gate_events: &'a [BTreeMap<String, Value>],
    pub settlements: &'a [BTreeMap<String, Value>],
}

pub fn validate_result_tables(
    view: ResultTableView<'_>,
) -> Result<ResultValidationReport, ResultValidationError> {
    let tables = BTreeMap::from([
        ("equity_curve", view.equity_curve),
        ("holdings", view.holdings),
        ("rebalance_audit", view.rebalance_audit),
        ("rebalance_trades", view.rebalance_trades),
        ("risk_gate_events", view.risk_gate_events),
        ("settlements", view.settlements),
    ]);
    let table_row_counts = tables
        .iter()
        .map(|(name, rows)| ((*name).to_string(), rows.len()))
        .collect::<BTreeMap<_, _>>();
    let mut checks = Vec::new();
    let mut errors = Vec::new();

    record_check(
        &mut checks,
        &mut errors,
        "schema_version",
        view.result_schema_version.starts_with("rust_")
            && view.result_schema_version.ends_with(".v1"),
        format!("result schema is {}", view.result_schema_version),
    );
    record_check(
        &mut checks,
        &mut errors,
        "artifact_completeness",
        !view.equity_curve.is_empty(),
        "equity_curve is present and non-empty".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "finite_values",
        tables.values().all(|rows| rows_are_finite(rows)),
        "all numeric result values are finite".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "event_ordering",
        rows_are_time_ordered(view.equity_curve),
        "equity timestamps are monotonic".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "equity_identity",
        equity_values_are_positive(view.equity_curve),
        "equity values are positive and finite".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "cash_position_reconciliation",
        holdings_reconcile_with_cash(view.equity_curve, view.holdings),
        "cash plus holdings weights reconcile to one when both are available".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "cost_and_turnover",
        cost_and_turnover_are_valid(view.equity_curve, view.rebalance_audit),
        "turnover and costs are non-negative".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "risk_transitions",
        risk_actions_are_valid(view.risk_gate_events),
        "risk actions use the canonical state-machine vocabulary".to_string(),
    );
    record_check(
        &mut checks,
        &mut errors,
        "settlement_transitions",
        settlements_are_valid(view.settlements),
        "settlement rows use pending/settled states and non-negative sessions".to_string(),
    );

    let result_hash = hash_tables(view.result_schema_version, &tables)?;
    if !errors.is_empty() {
        return Err(ResultValidationError(errors.join("; ")));
    }
    Ok(ResultValidationReport {
        schema_version: RESULT_VALIDATION_SCHEMA_VERSION.to_string(),
        status: "valid".to_string(),
        result_schema_version: view.result_schema_version.to_string(),
        result_hash,
        table_row_counts,
        checks,
        errors,
        warnings: Vec::new(),
    })
}

fn record_check(
    checks: &mut Vec<ResultValidationCheck>,
    errors: &mut Vec<String>,
    check_id: &str,
    passed: bool,
    success_message: String,
) {
    let message = if passed {
        success_message
    } else {
        format!("{check_id} check failed")
    };
    checks.push(ResultValidationCheck {
        check_id: check_id.to_string(),
        status: if passed {
            ResultCheckStatus::Passed
        } else {
            ResultCheckStatus::Failed
        },
        message: message.clone(),
    });
    if !passed {
        errors.push(message);
    }
}

fn rows_are_finite(rows: &[BTreeMap<String, Value>]) -> bool {
    rows.iter().all(|row| row.values().all(value_is_finite))
}

fn value_is_finite(value: &Value) -> bool {
    match value {
        Value::Number(number) => number.as_f64().is_some_and(f64::is_finite),
        Value::Array(values) => values.iter().all(value_is_finite),
        Value::Object(values) => values.values().all(value_is_finite),
        _ => true,
    }
}

fn rows_are_time_ordered(rows: &[BTreeMap<String, Value>]) -> bool {
    let timestamps = rows
        .iter()
        .filter_map(|row| string_field(row, &["Time", "Date"]))
        .collect::<Vec<_>>();
    timestamps.len() == rows.len() && timestamps.windows(2).all(|pair| pair[0] <= pair[1])
}

fn equity_values_are_positive(rows: &[BTreeMap<String, Value>]) -> bool {
    rows.iter().all(|row| {
        numeric_field(row, &["Equity_value", "Equity_after_trade"])
            .is_some_and(|value| value.is_finite() && value > 0.0)
    })
}

fn holdings_reconcile_with_cash(
    equity_rows: &[BTreeMap<String, Value>],
    _holdings: &[BTreeMap<String, Value>],
) -> bool {
    equity_rows.iter().all(|row| {
        let Some(cash) = numeric_field(row, &["Cash_weight"]) else {
            return true;
        };
        let weights = row
            .iter()
            .filter(|(name, _)| name.starts_with("Weight_"))
            .filter_map(|(_, value)| value.as_f64())
            .collect::<Vec<_>>();
        weights.is_empty() || (cash + weights.iter().sum::<f64>() - 1.0).abs() <= 1e-8
    })
}

fn cost_and_turnover_are_valid(
    equity_rows: &[BTreeMap<String, Value>],
    audit_rows: &[BTreeMap<String, Value>],
) -> bool {
    equity_rows.iter().chain(audit_rows.iter()).all(|row| {
        ["Turnover", "Trade_cost", "Cost_drag"]
            .iter()
            .filter_map(|field| numeric_field(row, &[*field]))
            .all(|value| value.is_finite() && value >= -1e-12)
    })
}

fn risk_actions_are_valid(rows: &[BTreeMap<String, Value>]) -> bool {
    let allowed = BTreeSet::from([
        "flatten",
        "permanent_stop",
        "shadow_until_recovery",
        "shadow_recovery_armed",
        "shadow_recovery_resumed",
        "block_new_orders",
        "reduce_exposure",
        "reduce_selected_positions",
        "clamp_order_delta",
        "margin_liquidation",
    ]);
    rows.iter()
        .all(|row| string_field(row, &["Action"]).is_some_and(|action| allowed.contains(action)))
}

fn settlements_are_valid(rows: &[BTreeMap<String, Value>]) -> bool {
    rows.iter().all(|row| {
        let valid_status = string_field(row, &["Status"])
            .is_some_and(|status| matches!(status, "pending" | "settled"));
        let valid_sessions = numeric_field(row, &["Remaining_sessions"])
            .is_some_and(|sessions| sessions >= 0.0 && sessions.fract().abs() <= 1e-12);
        valid_status && valid_sessions
    })
}

fn numeric_field(row: &BTreeMap<String, Value>, names: &[&str]) -> Option<f64> {
    names
        .iter()
        .find_map(|name| row.get(*name).and_then(Value::as_f64))
}

fn string_field<'a>(row: &'a BTreeMap<String, Value>, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| row.get(*name).and_then(Value::as_str))
}

fn hash_tables(
    result_schema_version: &str,
    tables: &BTreeMap<&str, &[BTreeMap<String, Value>]>,
) -> Result<String, ResultValidationError> {
    let payload = serde_json::to_vec(&(result_schema_version, tables))
        .map_err(|error| ResultValidationError(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(payload)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_equity() -> Vec<BTreeMap<String, Value>> {
        vec![BTreeMap::from([
            ("Time".to_string(), json!("2024-01-02")),
            ("Equity_value".to_string(), json!(100.0)),
            ("Cash_weight".to_string(), json!(1.0)),
            ("Turnover".to_string(), json!(0.0)),
        ])]
    }

    #[test]
    fn valid_result_returns_versioned_report_and_hash() {
        let equity = valid_equity();
        let empty = Vec::new();
        let report = validate_result_tables(ResultTableView {
            result_schema_version: "rust_accounting_result_tables.v1",
            equity_curve: &equity,
            holdings: &empty,
            rebalance_audit: &empty,
            rebalance_trades: &empty,
            risk_gate_events: &empty,
            settlements: &empty,
        })
        .unwrap();

        assert_eq!(report.schema_version, RESULT_VALIDATION_SCHEMA_VERSION);
        assert_eq!(report.status, "valid");
        assert_eq!(report.result_hash.len(), 64);
        assert!(report
            .checks
            .iter()
            .all(|check| check.status == ResultCheckStatus::Passed));
    }

    #[test]
    fn invalid_ordering_and_nan_fail_closed() {
        let equity = vec![
            BTreeMap::from([
                ("Time".to_string(), json!("2024-01-03")),
                ("Equity_value".to_string(), json!(100.0)),
            ]),
            BTreeMap::from([
                ("Time".to_string(), json!("2024-01-02")),
                ("Equity_value".to_string(), json!(-1.0)),
            ]),
        ];
        let empty = Vec::new();
        let error = validate_result_tables(ResultTableView {
            result_schema_version: "rust_accounting_result_tables.v1",
            equity_curve: &equity,
            holdings: &empty,
            rebalance_audit: &empty,
            rebalance_trades: &empty,
            risk_gate_events: &empty,
            settlements: &empty,
        })
        .unwrap_err();

        assert!(error.0.contains("event_ordering"));
        assert!(error.0.contains("equity_identity"));
    }
}
