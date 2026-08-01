use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

use crate::bar_aggregation::{parse_utc_nanos, ExecutionBarIndex, SourceBar};

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
    pub execution_equity_curve: &'a [BTreeMap<String, Value>],
    pub holdings: &'a [BTreeMap<String, Value>],
    pub rebalance_audit: &'a [BTreeMap<String, Value>],
    pub rebalance_trades: &'a [BTreeMap<String, Value>],
    pub risk_gate_events: &'a [BTreeMap<String, Value>],
    pub settlements: &'a [BTreeMap<String, Value>],
}

pub struct BarTimeValidationContext<'a> {
    pub expected_bar_time_contract_id: &'a str,
    pub expected_bar_time_contract_hash: &'a str,
    pub expected_stream_graph_hash: &'a str,
    pub expected_execution_stream_id: &'a str,
    pub expected_decision_stream_id: &'a str,
    pub execution_bars: &'a [SourceBar],
    pub expected_decisions: &'a [BarTimeExpectedDecisionEvidence],
    pub trusted_actions: &'a [BarTimeTrustedActionEvidence],
    pub rebalance_audit: &'a [BTreeMap<String, Value>],
    pub rebalance_trades: &'a [BTreeMap<String, Value>],
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BarTimeExpectedAggregationLineage {
    pub stream_id: String,
    pub parent_stream_id: String,
    pub parent_bar_count: usize,
    pub external_source_bar_count: usize,
    pub source_first_external_execution_sequence: u64,
    pub source_last_external_execution_sequence: u64,
    pub partial: bool,
}

#[derive(Debug, Clone)]
pub struct BarTimeExpectedDecisionEvidence {
    pub symbol: String,
    pub decision_index: usize,
    pub decision_id: String,
    pub decision_bar_open_time: String,
    pub decision_event_time: String,
    pub decision_available_time: String,
    pub source_count: usize,
    pub source_first_external_execution_sequence: u64,
    pub decision_source_sequence: u64,
    pub next_execution_index: Option<usize>,
    pub aggregation_lineage: Vec<BarTimeExpectedAggregationLineage>,
    pub expected_signal_action: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BarTimeTrustedActionEvidence {
    pub action_id: String,
    pub action: String,
    pub status: String,
    pub reason: Option<String>,
    pub execution_bar_close_time: String,
    pub asset: String,
    pub fill_ids: Vec<String>,
}

pub fn validate_bar_time_audit(
    audit: &Value,
    context: BarTimeValidationContext<'_>,
) -> Result<ResultValidationCheck, ResultValidationError> {
    require_text(audit, "schema_version", "bar_time_audit.v1")?;
    require_text(audit, "contract_id", "lo2cin4bt.bar_time_audit.v1")?;
    require_text(
        audit,
        "bar_time_contract_id",
        context.expected_bar_time_contract_id,
    )?;
    require_text(
        audit,
        "bar_time_contract_hash",
        context.expected_bar_time_contract_hash,
    )?;
    require_text(
        audit,
        "stream_graph_hash",
        context.expected_stream_graph_hash,
    )?;
    require_text(
        audit,
        "execution_stream_id",
        context.expected_execution_stream_id,
    )?;
    require_text(
        audit,
        "decision_stream_id",
        context.expected_decision_stream_id,
    )?;
    require_text(
        audit,
        "ordering_contract",
        "available_time,event_time,external_execution_sequence,lifecycle_stage,stream_id,source_sequence",
    )?;
    let mappings = audit
        .get("mappings")
        .and_then(Value::as_array)
        .ok_or_else(|| audit_error("mappings must be an array"))?;
    let terminal_decisions = audit
        .get("terminal_decisions")
        .and_then(Value::as_array)
        .ok_or_else(|| audit_error("terminal_decisions must be an array"))?;
    let mapping_count = usize_field(audit, "mapping_count")?;
    if mapping_count != mappings.len() {
        return Err(audit_error("mapping_count does not match mappings"));
    }
    if usize_field(audit, "external_source_bar_count")? != context.execution_bars.len() {
        return Err(audit_error(
            "external_source_bar_count does not match execution evidence",
        ));
    }
    let expected_mapped_decisions = context
        .expected_decisions
        .iter()
        .filter(|decision| decision.next_execution_index.is_some())
        .collect::<Vec<_>>();
    let expected_terminal_count = context
        .expected_decisions
        .iter()
        .filter(|decision| decision.next_execution_index.is_none())
        .count();
    if mapping_count != expected_mapped_decisions.len()
        || usize_field(audit, "terminal_decision_count")? != expected_terminal_count
        || terminal_decisions.len() != expected_terminal_count
    {
        return Err(audit_error(
            "mapping/terminal counts do not reconcile with prepared decision evidence",
        ));
    }
    let expected_derived_count =
        if context.expected_decision_stream_id == context.expected_execution_stream_id {
            0
        } else {
            context.expected_decisions.len()
        };
    if usize_field(audit, "derived_decision_bar_count")? != expected_derived_count {
        return Err(audit_error(
            "derived_decision_bar_count does not reconcile with prepared decision evidence",
        ));
    }
    let expected_by_id = expected_mapped_decisions
        .iter()
        .map(|decision| (decision.decision_id.as_str(), *decision))
        .collect::<BTreeMap<_, _>>();
    if expected_by_id.len() != expected_mapped_decisions.len() {
        return Err(audit_error(
            "prepared decision evidence contains duplicate decision IDs",
        ));
    }
    let mut prior_key: Option<&str> = None;
    let mut seen_decisions = BTreeSet::new();
    let mut referenced_actions = BTreeSet::new();
    let mut referenced_fills = BTreeSet::new();
    let mut eligibility_rows_checked = 0usize;
    let mut action_rows_checked = 0usize;
    let mut aggregation_rows_checked = 0usize;
    let mut seen_terminal_decisions = BTreeSet::new();
    let execution_lookup = ExecutionBarIndex::new(context.execution_bars)
        .map_err(|error| audit_error(&error.to_string()))?;
    for (row_index, mapping) in mappings.iter().enumerate() {
        let decision_id = text_field(mapping, "decision_id")?;
        if !seen_decisions.insert(decision_id) {
            return Err(audit_row_error(row_index, "duplicate decision_id"));
        }
        let expected_decision = expected_by_id
            .get(decision_id)
            .ok_or_else(|| audit_row_error(row_index, "decision_id is not prepared evidence"))?;
        if text_field(mapping, "symbol")? != expected_decision.symbol
            || usize_field(mapping, "decision_index")? != expected_decision.decision_index
            || text_field(mapping, "decision_bar_open_time")?
                != expected_decision.decision_bar_open_time
            || text_field(mapping, "decision_event_time")? != expected_decision.decision_event_time
            || text_field(mapping, "decision_available_time")?
                != expected_decision.decision_available_time
            || usize_field(mapping, "source_count")? != expected_decision.source_count
            || u64_field(mapping, "source_first_external_execution_sequence")?
                != expected_decision.source_first_external_execution_sequence
            || u64_field(mapping, "decision_source_sequence")?
                != expected_decision.decision_source_sequence
            || Some(usize_field(mapping, "execution_index")?)
                != expected_decision.next_execution_index
        {
            return Err(audit_row_error(
                row_index,
                "decision fields do not reconcile with prepared decision evidence",
            ));
        }
        require_text(
            mapping,
            "decision_stream_id",
            context.expected_decision_stream_id,
        )?;
        require_text(
            mapping,
            "execution_stream_id",
            context.expected_execution_stream_id,
        )?;
        require_text(mapping, "lifecycle_stage", "eligible_execution")?;
        let decision_event = text_field(mapping, "decision_event_time")?;
        let decision_available = text_field(mapping, "decision_available_time")?;
        let execution_open = text_field(mapping, "execution_bar_open_time")?;
        let execution_close = text_field(mapping, "execution_bar_close_time")?;
        let execution_available = text_field(mapping, "execution_available_time")?;
        let decision_event_nanos = parse_utc_nanos(decision_event)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        let decision_available_nanos = parse_utc_nanos(decision_available)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        let execution_open_nanos = parse_utc_nanos(execution_open)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        let execution_close_nanos = parse_utc_nanos(execution_close)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        let execution_available_nanos = parse_utc_nanos(execution_available)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        if decision_event_nanos > decision_available_nanos
            || decision_available_nanos > execution_open_nanos
            || execution_open_nanos >= execution_close_nanos
            || execution_close_nanos > execution_available_nanos
        {
            return Err(audit_row_error(row_index, "lifecycle ordering is invalid"));
        }
        let decision_source_sequence = u64_field(mapping, "decision_source_sequence")?;
        let execution_sequence = u64_field(mapping, "external_execution_sequence")?;
        if execution_sequence <= decision_source_sequence {
            return Err(audit_row_error(
                row_index,
                "execution sequence is not strictly later than decision source",
            ));
        }
        let execution_index = usize_field(mapping, "execution_index")?;
        let execution = context
            .execution_bars
            .get(execution_index)
            .ok_or_else(|| audit_row_error(row_index, "execution_index is out of bounds"))?;
        if execution.external_execution_sequence != execution_sequence
            || execution.bar_open_timestamp != execution_open
            || execution.event_timestamp != execution_close
            || execution.available_timestamp != execution_available
            || execution.stream_id != context.expected_execution_stream_id
            || (execution.open - f64_field(mapping, "execution_open_price")?).abs() > 1e-12
        {
            return Err(audit_row_error(
                row_index,
                "eligible execution does not reconcile with execution evidence",
            ));
        }
        let expected_index = execution_lookup
            .next_eligible(decision_available, decision_source_sequence)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        if expected_index != execution_index {
            return Err(audit_row_error(
                row_index,
                "mapping is not the earliest eligible execution",
            ));
        }
        let expected_key = format!(
            "{execution_open}|{execution_open}|{execution_sequence:020}|04:eligible_execution|{}|{decision_source_sequence:020}",
            context.expected_execution_stream_id
        );
        let ordering_key = text_field(mapping, "ordering_key")?;
        if ordering_key != expected_key {
            return Err(audit_row_error(row_index, "ordering_key is not canonical"));
        }
        if prior_key.is_some_and(|prior| ordering_key <= prior) {
            return Err(audit_row_error(
                row_index,
                "ordering_key is duplicate or non-monotonic",
            ));
        }
        prior_key = Some(ordering_key);
        validate_lifecycle_rows(mapping, row_index, ordering_key)?;
        eligibility_rows_checked += 1;

        let lineage = mapping
            .get("aggregation_lineage")
            .and_then(Value::as_array)
            .ok_or_else(|| audit_row_error(row_index, "aggregation_lineage must be an array"))?;
        let expected_lineage = serde_json::to_value(&expected_decision.aggregation_lineage)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        if mapping.get("aggregation_lineage") != Some(&expected_lineage) {
            return Err(audit_row_error(
                row_index,
                "aggregation lineage does not reconcile with prepared decision evidence",
            ));
        }
        if context.expected_decision_stream_id != context.expected_execution_stream_id
            && lineage.is_empty()
        {
            return Err(audit_row_error(
                row_index,
                "derived decision is missing aggregation lineage",
            ));
        }
        for hop in lineage {
            if text_field(hop, "stream_id")?.is_empty()
                || text_field(hop, "parent_stream_id")?.is_empty()
                || usize_field(hop, "parent_bar_count")? == 0
                || usize_field(hop, "external_source_bar_count")? == 0
                || u64_field(hop, "source_first_external_execution_sequence")?
                    > u64_field(hop, "source_last_external_execution_sequence")?
            {
                return Err(audit_row_error(row_index, "aggregation lineage is invalid"));
            }
            aggregation_rows_checked += 1;
        }

        let action_id = optional_text_field(mapping, "action_id")?;
        let fill_id = optional_text_field(mapping, "eligible_fill_id")?;
        let signal_action = optional_text_field(mapping, "signal_action")?;
        let action_status = optional_text_field(mapping, "action_status")?;
        let action_reason = optional_text_field(mapping, "action_reason")?;
        match (action_id, signal_action, action_status) {
            (None, None, None) if fill_id.is_none() && action_reason.is_none() => {}
            (Some(action_id), Some(signal_action), Some(action_status)) => {
                let trusted = context
                    .trusted_actions
                    .iter()
                    .find(|action| action.action_id == action_id)
                    .ok_or_else(|| audit_row_error(row_index, "action_id is orphaned"))?;
                if !referenced_actions.insert(action_id) {
                    return Err(audit_row_error(row_index, "duplicate action_id reference"));
                }
                if trusted.action != signal_action
                    || trusted.status != action_status
                    || trusted.execution_bar_close_time != execution_close
                    || trusted.asset != text_field(mapping, "symbol")?
                {
                    return Err(audit_row_error(
                        row_index,
                        "action evidence does not reconcile",
                    ));
                }
                match action_status {
                    "filled" => {
                        let fill_id = fill_id.ok_or_else(|| {
                            audit_row_error(row_index, "filled action is missing eligible_fill_id")
                        })?;
                        if action_reason.is_some()
                            || trusted.fill_ids.len() != 1
                            || trusted.fill_ids[0] != fill_id
                            || !referenced_fills.insert(fill_id)
                        {
                            return Err(audit_row_error(
                                row_index,
                                "filled action evidence does not reconcile uniquely",
                            ));
                        }
                        if !table_has_time(context.rebalance_audit, execution_close)
                            || !trade_table_reconciles(
                                context.rebalance_trades,
                                execution_close,
                                &trusted.asset,
                                signal_action,
                                fill_id,
                            )
                        {
                            return Err(audit_row_error(
                                row_index,
                                "action/fill does not reconcile with canonical result tables",
                            ));
                        }
                    }
                    "no_op" => {
                        if fill_id.is_some()
                            || action_reason != trusted.reason.as_deref()
                            || action_reason.is_none()
                            || !trusted.fill_ids.is_empty()
                        {
                            return Err(audit_row_error(
                                row_index,
                                "no-op action evidence does not reconcile",
                            ));
                        }
                    }
                    _ => {
                        return Err(audit_row_error(
                            row_index,
                            "action_status must be filled or no_op",
                        ))
                    }
                }
                action_rows_checked += 1;
            }
            _ => {
                return Err(audit_row_error(
                    row_index,
                    "action_id, eligible_fill_id and signal_action must be all present or all null",
                ))
            }
        }
    }
    let expected_terminal_by_id = context
        .expected_decisions
        .iter()
        .filter(|decision| decision.next_execution_index.is_none())
        .map(|decision| (decision.decision_id.as_str(), decision))
        .collect::<BTreeMap<_, _>>();
    for (row_index, terminal) in terminal_decisions.iter().enumerate() {
        let decision_id = text_field(terminal, "decision_id")?;
        if !seen_terminal_decisions.insert(decision_id) {
            return Err(audit_row_error(row_index, "duplicate terminal decision_id"));
        }
        let expected = expected_terminal_by_id.get(decision_id).ok_or_else(|| {
            audit_row_error(row_index, "terminal decision is not prepared evidence")
        })?;
        if text_field(terminal, "symbol")? != expected.symbol
            || usize_field(terminal, "decision_index")? != expected.decision_index
            || text_field(terminal, "decision_stream_id")? != context.expected_decision_stream_id
            || text_field(terminal, "decision_bar_open_time")? != expected.decision_bar_open_time
            || text_field(terminal, "decision_event_time")? != expected.decision_event_time
            || text_field(terminal, "decision_available_time")? != expected.decision_available_time
            || usize_field(terminal, "source_count")? != expected.source_count
            || u64_field(terminal, "source_first_external_execution_sequence")?
                != expected.source_first_external_execution_sequence
            || u64_field(terminal, "decision_source_sequence")? != expected.decision_source_sequence
        {
            return Err(audit_row_error(
                row_index,
                "terminal decision does not reconcile with prepared evidence",
            ));
        }
        let signal_action = optional_text_field(terminal, "signal_action")?;
        let status = text_field(terminal, "status")?;
        let reason = optional_text_field(terminal, "reason")?;
        if optional_text_field(terminal, "action_id")?.is_some()
            || optional_text_field(terminal, "eligible_fill_id")?.is_some()
            || terminal.get("execution_index") != Some(&Value::Null)
        {
            return Err(audit_row_error(
                row_index,
                "terminal decision cannot reference execution, action or fill evidence",
            ));
        }
        match expected.expected_signal_action.as_deref() {
            None if signal_action.is_none() && status == "no_signal" && reason.is_none() => {}
            Some(expected_action)
                if signal_action == Some(expected_action)
                    && status == "skipped"
                    && reason == Some("no_eligible_next_execution_bar") => {}
            _ => {
                return Err(audit_row_error(
                    row_index,
                    "terminal signal status/reason does not reconcile",
                ))
            }
        }
        let expected_lineage = serde_json::to_value(&expected.aggregation_lineage)
            .map_err(|error| audit_row_error(row_index, &error.to_string()))?;
        if terminal.get("aggregation_lineage") != Some(&expected_lineage) {
            return Err(audit_row_error(
                row_index,
                "terminal aggregation lineage does not reconcile",
            ));
        }
    }
    let expected_terminal_ids = expected_terminal_by_id
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    if seen_terminal_decisions != expected_terminal_ids {
        return Err(audit_error(
            "prepared terminal decision evidence is orphaned or duplicated",
        ));
    }
    let expected_decision_ids = expected_by_id.keys().copied().collect::<BTreeSet<_>>();
    if seen_decisions != expected_decision_ids {
        return Err(audit_error(
            "prepared decision evidence is orphaned or not referenced exactly once",
        ));
    }
    let expected_actions = context
        .trusted_actions
        .iter()
        .map(|action| action.action_id.as_str())
        .collect::<BTreeSet<_>>();
    let expected_fills = context
        .trusted_actions
        .iter()
        .flat_map(|action| action.fill_ids.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    if referenced_actions != expected_actions {
        return Err(audit_error(
            "trusted action evidence is orphaned or not referenced exactly once",
        ));
    }
    if referenced_fills != expected_fills {
        return Err(audit_error(
            "trusted fill evidence is orphaned or not referenced exactly once",
        ));
    }
    let mut expected_rebalance_times = BTreeMap::new();
    for action in context
        .trusted_actions
        .iter()
        .filter(|action| action.status == "filled")
    {
        *expected_rebalance_times
            .entry(action.execution_bar_close_time.as_str())
            .or_insert(0usize) += 1;
    }
    let mut actual_rebalance_times = BTreeMap::new();
    for row in context.rebalance_audit {
        let time = row
            .get("Time")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| audit_error("rebalance_audit row is missing Time"))?;
        *actual_rebalance_times.entry(time).or_insert(0usize) += 1;
    }
    if actual_rebalance_times != expected_rebalance_times {
        return Err(audit_error(
            "rebalance_audit Time rows are not an exact filled-action bijection",
        ));
    }
    let mut table_fill_counts = BTreeMap::new();
    for row in context.rebalance_trades {
        let order_id = row
            .get("Order_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| audit_error("rebalance_trades row is missing Order_id"))?;
        *table_fill_counts.entry(order_id).or_insert(0usize) += 1;
    }
    if table_fill_counts.keys().copied().collect::<BTreeSet<_>>() != expected_fills
        || table_fill_counts.values().any(|count| *count != 1)
    {
        return Err(audit_error(
            "rebalance_trades Order_id rows are not an exact fill bijection",
        ));
    }
    Ok(ResultValidationCheck {
        check_id: "bar_time_no_look_ahead".to_string(),
        status: ResultCheckStatus::Passed,
        message: format!(
            "validated {eligibility_rows_checked} ordering/eligibility rows, {aggregation_rows_checked} aggregation lineage rows, {action_rows_checked} reconciled action/fill rows"
        ),
    })
}

fn audit_error(reason: &str) -> ResultValidationError {
    ResultValidationError(format!("bar_time_audit: {reason}"))
}

fn audit_row_error(row: usize, reason: &str) -> ResultValidationError {
    audit_error(&format!("row {row}: {reason}"))
}

fn text_field<'a>(value: &'a Value, field: &str) -> Result<&'a str, ResultValidationError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| audit_error(&format!("{field} must be a non-empty string")))
}

fn optional_text_field<'a>(
    value: &'a Value,
    field: &str,
) -> Result<Option<&'a str>, ResultValidationError> {
    match value.get(field) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::String(text)) if !text.is_empty() => Ok(Some(text)),
        _ => Err(audit_error(&format!(
            "{field} must be null or a non-empty string"
        ))),
    }
}

fn require_text(value: &Value, field: &str, expected: &str) -> Result<(), ResultValidationError> {
    if text_field(value, field)? == expected {
        Ok(())
    } else {
        Err(audit_error(&format!("{field} does not match authority")))
    }
}

fn usize_field(value: &Value, field: &str) -> Result<usize, ResultValidationError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|number| usize::try_from(number).ok())
        .ok_or_else(|| audit_error(&format!("{field} must be a non-negative integer")))
}

fn u64_field(value: &Value, field: &str) -> Result<u64, ResultValidationError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| audit_error(&format!("{field} must be a non-negative integer")))
}

fn f64_field(value: &Value, field: &str) -> Result<f64, ResultValidationError> {
    value
        .get(field)
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite())
        .ok_or_else(|| audit_error(&format!("{field} must be finite")))
}

fn table_has_time(rows: &[BTreeMap<String, Value>], expected: &str) -> bool {
    rows.iter()
        .any(|row| row.get("Time").and_then(Value::as_str) == Some(expected))
}

fn trade_table_reconciles(
    rows: &[BTreeMap<String, Value>],
    expected_time: &str,
    asset: &str,
    signal_action: &str,
    fill_id: &str,
) -> bool {
    rows.iter().any(|row| {
        let action = row.get("Action").and_then(Value::as_str);
        row.get("Time").and_then(Value::as_str) == Some(expected_time)
            && row.get("Asset").and_then(Value::as_str) == Some(asset)
            && row.get("Order_id").and_then(Value::as_str) == Some(fill_id)
            && match signal_action {
                "enter" => action == Some("buy"),
                "exit" => matches!(action, Some("exit" | "sell")),
                _ => false,
            }
    })
}

fn validate_lifecycle_rows(
    mapping: &Value,
    mapping_index: usize,
    eligibility_ordering_key: &str,
) -> Result<(), ResultValidationError> {
    let rows = mapping
        .get("lifecycle")
        .and_then(Value::as_array)
        .ok_or_else(|| audit_row_error(mapping_index, "lifecycle must be an array"))?;
    let action_status = optional_text_field(mapping, "action_status")?;
    let expected_stages: Vec<(&str, usize)> = match action_status {
        None => vec![
            ("decision_event", 0),
            ("decision_available", 1),
            ("eligible_execution", 4),
            ("execution_close", 6),
        ],
        Some("no_op") => vec![
            ("decision_event", 0),
            ("decision_available", 1),
            ("signal", 2),
            ("order", 3),
            ("eligible_execution", 4),
            ("execution_close", 6),
        ],
        Some("filled") => vec![
            ("decision_event", 0),
            ("decision_available", 1),
            ("signal", 2),
            ("order", 3),
            ("eligible_execution", 4),
            ("eligible_fill", 5),
            ("execution_close", 6),
        ],
        Some(_) => {
            return Err(audit_row_error(
                mapping_index,
                "action_status must be filled or no_op",
            ))
        }
    };
    if rows.len() != expected_stages.len() {
        return Err(audit_row_error(
            mapping_index,
            "lifecycle stage count does not match action evidence",
        ));
    }
    let mut previous: Option<(i64, i64, u64, usize, String, u64)> = None;
    let mut eligible_execution_key = None;
    for (row, (expected_stage, rank)) in rows.iter().zip(expected_stages) {
        require_text(row, "lifecycle_stage", expected_stage)?;
        if usize_field(row, "lifecycle_rank")? != rank {
            return Err(audit_row_error(
                mapping_index,
                "lifecycle rank does not match canonical stage order",
            ));
        }
        let available = text_field(row, "available_time")?;
        let event = text_field(row, "event_time")?;
        let sequence = u64_field(row, "external_execution_sequence")?;
        let stream_id = text_field(row, "stream_id")?;
        let source_sequence = u64_field(row, "source_sequence")?;
        let expected_key = format!(
            "{available}|{event}|{sequence:020}|{rank:02}:{expected_stage}|{stream_id}|{source_sequence:020}"
        );
        if text_field(row, "ordering_key")? != expected_key {
            return Err(audit_row_error(
                mapping_index,
                "lifecycle ordering_key is not canonical",
            ));
        }
        if expected_stage == "eligible_execution" {
            eligible_execution_key = Some(text_field(row, "ordering_key")?);
        }
        let tuple = (
            parse_utc_nanos(available)
                .map_err(|error| audit_row_error(mapping_index, &error.to_string()))?,
            parse_utc_nanos(event)
                .map_err(|error| audit_row_error(mapping_index, &error.to_string()))?,
            sequence,
            rank,
            stream_id.to_string(),
            source_sequence,
        );
        if previous.as_ref().is_some_and(|prior| tuple <= *prior) {
            return Err(audit_row_error(
                mapping_index,
                "lifecycle ordering is duplicate or non-monotonic",
            ));
        }
        previous = Some(tuple);
    }
    if eligible_execution_key != Some(eligibility_ordering_key) {
        return Err(audit_row_error(
            mapping_index,
            "mapping ordering_key does not equal eligible_execution lifecycle ordering_key",
        ));
    }
    Ok(())
}

pub fn validate_result_tables(
    view: ResultTableView<'_>,
) -> Result<ResultValidationReport, ResultValidationError> {
    let mut tables = BTreeMap::from([
        ("equity_curve", view.equity_curve),
        ("holdings", view.holdings),
        ("rebalance_audit", view.rebalance_audit),
        ("rebalance_trades", view.rebalance_trades),
        ("risk_gate_events", view.risk_gate_events),
        ("settlements", view.settlements),
    ]);
    if !view.execution_equity_curve.is_empty() {
        tables.insert("execution_equity_curve", view.execution_equity_curve);
    }
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
        "execution_equity_completeness",
        !view.result_schema_version.starts_with("rust_timeline")
            || !view.execution_equity_curve.is_empty(),
        "execution_equity_curve is present and non-empty".to_string(),
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
        "execution_equity_ordering",
        rows_are_time_ordered(view.execution_equity_curve),
        "execution equity timestamps are monotonic".to_string(),
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
        "execution_equity_identity",
        equity_values_are_positive(view.execution_equity_curve),
        "execution equity values are positive and finite".to_string(),
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

    type BarTimeFixture = (
        Value,
        Vec<SourceBar>,
        Vec<BarTimeTrustedActionEvidence>,
        Vec<BTreeMap<String, Value>>,
        Vec<BTreeMap<String, Value>>,
    );

    fn valid_equity() -> Vec<BTreeMap<String, Value>> {
        vec![BTreeMap::from([
            ("Time".to_string(), json!("2024-01-02")),
            ("Equity_value".to_string(), json!(100.0)),
            ("Cash_weight".to_string(), json!(1.0)),
            ("Turnover".to_string(), json!(0.0)),
        ])]
    }

    fn bar_time_fixture() -> BarTimeFixture {
        let execution = vec![
            SourceBar {
                stream_id: "execution_1m".to_string(),
                external_execution_sequence: 5,
                bar_open_timestamp: "2024-07-03T13:34:00Z".to_string(),
                event_timestamp: "2024-07-03T13:35:00Z".to_string(),
                available_timestamp: "2024-07-03T13:35:00Z".to_string(),
                session_label: "2024-07-03".to_string(),
                open: 104.0,
                high: 105.0,
                low: 103.0,
                close: 104.5,
                volume: 10.0,
            },
            SourceBar {
                stream_id: "execution_1m".to_string(),
                external_execution_sequence: 6,
                bar_open_timestamp: "2024-07-03T13:35:00Z".to_string(),
                event_timestamp: "2024-07-03T13:36:00Z".to_string(),
                available_timestamp: "2024-07-03T13:36:00Z".to_string(),
                session_label: "2024-07-03".to_string(),
                open: 105.0,
                high: 106.0,
                low: 104.0,
                close: 105.5,
                volume: 11.0,
            },
        ];
        let lifecycle = [
            (
                "decision_event",
                0,
                "2024-07-03T13:35:00Z",
                5,
                "decision_5m",
            ),
            (
                "decision_available",
                1,
                "2024-07-03T13:35:00Z",
                5,
                "decision_5m",
            ),
            ("signal", 2, "2024-07-03T13:35:00Z", 5, "decision_5m"),
            ("order", 3, "2024-07-03T13:35:00Z", 5, "decision_5m"),
            (
                "eligible_execution",
                4,
                "2024-07-03T13:35:00Z",
                6,
                "execution_1m",
            ),
            (
                "eligible_fill",
                5,
                "2024-07-03T13:35:00Z",
                6,
                "execution_1m",
            ),
            (
                "execution_close",
                6,
                "2024-07-03T13:36:00Z",
                6,
                "execution_1m",
            ),
        ]
        .into_iter()
        .map(|(stage, rank, timestamp, sequence, stream)| {
            json!({
                "lifecycle_stage": stage,
                "lifecycle_rank": rank,
                "available_time": timestamp,
                "event_time": timestamp,
                "external_execution_sequence": sequence,
                "stream_id": stream,
                "source_sequence": 5,
                "ordering_key": match rank {
                    0 => "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000005|00:decision_event|decision_5m|00000000000000000005",
                    1 => "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000005|01:decision_available|decision_5m|00000000000000000005",
                    2 => "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000005|02:signal|decision_5m|00000000000000000005",
                    3 => "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000005|03:order|decision_5m|00000000000000000005",
                    4 => "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000006|04:eligible_execution|execution_1m|00000000000000000005",
                    5 => "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000006|05:eligible_fill|execution_1m|00000000000000000005",
                    6 => "2024-07-03T13:36:00Z|2024-07-03T13:36:00Z|00000000000000000006|06:execution_close|execution_1m|00000000000000000005",
                    _ => unreachable!(),
                }
            })
        })
        .collect::<Vec<_>>();
        let audit = json!({
            "schema_version": "bar_time_audit.v1",
            "contract_id": "lo2cin4bt.bar_time_audit.v1",
            "bar_time_contract_id": "lo2cin4bt.bar_time_contract.v1",
            "bar_time_contract_hash": "contract-hash",
            "stream_graph_hash": "graph-hash",
            "ordering_contract": "available_time,event_time,external_execution_sequence,lifecycle_stage,stream_id,source_sequence",
            "execution_stream_id": "execution_1m",
            "decision_stream_id": "decision_5m",
            "aggregated": true,
            "external_source_bar_count": 2,
            "derived_decision_bar_count": 2,
            "first_external_available_time": "2024-07-03T13:35:00Z",
            "last_external_available_time": "2024-07-03T13:36:00Z",
            "first_decision_available_time": "2024-07-03T13:35:00Z",
            "last_decision_available_time": "2024-07-03T13:36:00Z",
            "empty_bar_policy": "omit",
            "partial_first_bar_policy": "omit",
            "partial_final_bar_policy": "omit",
            "mapping_count": 1,
            "terminal_decision_count": 1,
            "terminal_decisions": [{
                "symbol": "QQQ",
                "decision_index": 1,
                "decision_id": "QQQ:decision_5m:1",
                "decision_stream_id": "decision_5m",
                "decision_bar_open_time": "2024-07-03T13:35:00Z",
                "decision_event_time": "2024-07-03T13:36:00Z",
                "decision_available_time": "2024-07-03T13:36:00Z",
                "source_count": 1,
                "source_first_external_execution_sequence": 6,
                "decision_source_sequence": 6,
                "aggregation_lineage": [{
                    "stream_id": "decision_5m",
                    "parent_stream_id": "execution_1m",
                    "parent_bar_count": 1,
                    "external_source_bar_count": 1,
                    "source_first_external_execution_sequence": 6,
                    "source_last_external_execution_sequence": 6,
                    "partial": true
                }],
                "signal_action": "enter",
                "status": "skipped",
                "reason": "no_eligible_next_execution_bar",
                "action_id": null,
                "eligible_fill_id": null,
                "execution_index": null
            }],
            "mappings": [{
                "symbol": "QQQ",
                "decision_index": 0,
                "decision_id": "QQQ:decision_5m:0",
                "decision_stream_id": "decision_5m",
                "decision_bar_open_time": "2024-07-03T13:30:00Z",
                "decision_event_time": "2024-07-03T13:35:00Z",
                "decision_available_time": "2024-07-03T13:35:00Z",
                "source_count": 5,
                "source_first_external_execution_sequence": 1,
                "decision_source_sequence": 5,
                "execution_stream_id": "execution_1m",
                "execution_index": 1,
                "execution_bar_open_time": "2024-07-03T13:35:00Z",
                "execution_bar_close_time": "2024-07-03T13:36:00Z",
                "execution_available_time": "2024-07-03T13:36:00Z",
                "external_execution_sequence": 6,
                "execution_open_price": 105.0,
                "lifecycle_stage": "eligible_execution",
                "ordering_key": "2024-07-03T13:35:00Z|2024-07-03T13:35:00Z|00000000000000000006|04:eligible_execution|execution_1m|00000000000000000005",
                "lifecycle": lifecycle,
                "signal_action": "enter",
                "action_id": "action-1",
                "action_status": "filled",
                "action_reason": null,
                "eligible_fill_id": "fill-1",
                "aggregation_lineage": [{
                    "stream_id": "decision_5m",
                    "parent_stream_id": "execution_1m",
                    "parent_bar_count": 5,
                    "external_source_bar_count": 5,
                    "source_first_external_execution_sequence": 1,
                    "source_last_external_execution_sequence": 5,
                    "partial": false
                }]
            }]
        });
        let actions = vec![BarTimeTrustedActionEvidence {
            action_id: "action-1".to_string(),
            action: "enter".to_string(),
            status: "filled".to_string(),
            reason: None,
            execution_bar_close_time: "2024-07-03T13:36:00Z".to_string(),
            asset: "QQQ".to_string(),
            fill_ids: vec!["fill-1".to_string()],
        }];
        let rebalance = vec![BTreeMap::from([(
            "Time".to_string(),
            json!("2024-07-03T13:36:00Z"),
        )])];
        let trades = vec![BTreeMap::from([
            ("Time".to_string(), json!("2024-07-03T13:36:00Z")),
            ("Asset".to_string(), json!("QQQ")),
            ("Action".to_string(), json!("buy")),
            ("Order_id".to_string(), json!("fill-1")),
        ])];
        (audit, execution, actions, rebalance, trades)
    }

    fn validate_bar_time_fixture(
        audit: &Value,
        execution: &[SourceBar],
        actions: &[BarTimeTrustedActionEvidence],
        rebalance: &[BTreeMap<String, Value>],
        trades: &[BTreeMap<String, Value>],
    ) -> Result<ResultValidationCheck, ResultValidationError> {
        let expected_decisions = vec![
            BarTimeExpectedDecisionEvidence {
                symbol: "QQQ".to_string(),
                decision_index: 0,
                decision_id: "QQQ:decision_5m:0".to_string(),
                decision_bar_open_time: "2024-07-03T13:30:00Z".to_string(),
                decision_event_time: "2024-07-03T13:35:00Z".to_string(),
                decision_available_time: "2024-07-03T13:35:00Z".to_string(),
                source_count: 5,
                source_first_external_execution_sequence: 1,
                decision_source_sequence: 5,
                next_execution_index: Some(1),
                aggregation_lineage: vec![BarTimeExpectedAggregationLineage {
                    stream_id: "decision_5m".to_string(),
                    parent_stream_id: "execution_1m".to_string(),
                    parent_bar_count: 5,
                    external_source_bar_count: 5,
                    source_first_external_execution_sequence: 1,
                    source_last_external_execution_sequence: 5,
                    partial: false,
                }],
                expected_signal_action: Some("enter".to_string()),
            },
            BarTimeExpectedDecisionEvidence {
                symbol: "QQQ".to_string(),
                decision_index: 1,
                decision_id: "QQQ:decision_5m:1".to_string(),
                decision_bar_open_time: "2024-07-03T13:35:00Z".to_string(),
                decision_event_time: "2024-07-03T13:36:00Z".to_string(),
                decision_available_time: "2024-07-03T13:36:00Z".to_string(),
                source_count: 1,
                source_first_external_execution_sequence: 6,
                decision_source_sequence: 6,
                next_execution_index: None,
                aggregation_lineage: vec![BarTimeExpectedAggregationLineage {
                    stream_id: "decision_5m".to_string(),
                    parent_stream_id: "execution_1m".to_string(),
                    parent_bar_count: 1,
                    external_source_bar_count: 1,
                    source_first_external_execution_sequence: 6,
                    source_last_external_execution_sequence: 6,
                    partial: true,
                }],
                expected_signal_action: Some("enter".to_string()),
            },
        ];
        validate_bar_time_audit(
            audit,
            BarTimeValidationContext {
                expected_bar_time_contract_id: "lo2cin4bt.bar_time_contract.v1",
                expected_bar_time_contract_hash: "contract-hash",
                expected_stream_graph_hash: "graph-hash",
                expected_execution_stream_id: "execution_1m",
                expected_decision_stream_id: "decision_5m",
                execution_bars: execution,
                expected_decisions: &expected_decisions,
                trusted_actions: actions,
                rebalance_audit: rebalance,
                rebalance_trades: trades,
            },
        )
    }

    #[test]
    fn bar_time_validator_rejects_hash_order_action_fill_and_result_reconciliation_tampering() {
        let (audit, execution, actions, rebalance, trades) = bar_time_fixture();
        assert_eq!(
            validate_bar_time_fixture(&audit, &execution, &actions, &rebalance, &trades)
                .unwrap()
                .status,
            ResultCheckStatus::Passed
        );

        let mut bad_hash = audit.clone();
        bad_hash["stream_graph_hash"] = json!("tampered");
        assert!(
            validate_bar_time_fixture(&bad_hash, &execution, &actions, &rebalance, &trades)
                .is_err()
        );

        let mut bad_order = audit.clone();
        bad_order["mappings"][0]["lifecycle"][3]["ordering_key"] = json!("tampered");
        assert!(
            validate_bar_time_fixture(&bad_order, &execution, &actions, &rebalance, &trades)
                .is_err()
        );

        let mut bad_action = audit.clone();
        bad_action["mappings"][0]["action_id"] = json!("orphan-action");
        assert!(
            validate_bar_time_fixture(&bad_action, &execution, &actions, &rebalance, &trades)
                .is_err()
        );

        for (field, value) in [
            ("status", json!("no_signal")),
            ("reason", Value::Null),
            ("action_id", json!("fabricated-action")),
            ("eligible_fill_id", json!("fabricated-fill")),
            ("execution_index", json!(1)),
        ] {
            let mut bad_terminal = audit.clone();
            bad_terminal["terminal_decisions"][0][field] = value;
            assert!(
                validate_bar_time_fixture(&bad_terminal, &execution, &actions, &rebalance, &trades)
                    .is_err(),
                "tampered terminal field {field} must fail"
            );
        }

        let mut bad_fill = audit.clone();
        bad_fill["mappings"][0]["eligible_fill_id"] = json!("orphan-fill");
        assert!(
            validate_bar_time_fixture(&bad_fill, &execution, &actions, &rebalance, &trades)
                .is_err()
        );

        let mut bad_trade_order = trades.clone();
        bad_trade_order[0].insert("Order_id".to_string(), json!("other-fill"));
        assert!(validate_bar_time_fixture(
            &audit,
            &execution,
            &actions,
            &rebalance,
            &bad_trade_order
        )
        .is_err());

        let mut duplicate_trade = trades.clone();
        duplicate_trade.push(trades[0].clone());
        assert!(validate_bar_time_fixture(
            &audit,
            &execution,
            &actions,
            &rebalance,
            &duplicate_trade
        )
        .is_err());

        let mut duplicate_rebalance = rebalance.clone();
        duplicate_rebalance.push(rebalance[0].clone());
        assert!(validate_bar_time_fixture(
            &audit,
            &execution,
            &actions,
            &duplicate_rebalance,
            &trades
        )
        .is_err());

        let mut orphan_rebalance = rebalance.clone();
        orphan_rebalance[0].insert("Time".to_string(), json!("2024-07-03T13:37:00Z"));
        assert!(validate_bar_time_fixture(
            &audit,
            &execution,
            &actions,
            &orphan_rebalance,
            &trades
        )
        .is_err());

        let mut extra_trade = trades.clone();
        let mut orphan = trades[0].clone();
        orphan.insert("Order_id".to_string(), json!("orphan-fill"));
        extra_trade.push(orphan);
        assert!(
            validate_bar_time_fixture(&audit, &execution, &actions, &rebalance, &extra_trade)
                .is_err()
        );

        for (field, value) in [
            ("stream_id", json!("other_5m")),
            ("parent_stream_id", json!("other_1m")),
            ("parent_bar_count", json!(4)),
            ("external_source_bar_count", json!(4)),
            ("source_first_external_execution_sequence", json!(2)),
            ("source_last_external_execution_sequence", json!(4)),
        ] {
            let mut bad_lineage = audit.clone();
            bad_lineage["mappings"][0]["aggregation_lineage"][0][field] = value;
            assert!(
                validate_bar_time_fixture(&bad_lineage, &execution, &actions, &rebalance, &trades)
                    .is_err(),
                "tampered lineage field {field} must fail"
            );
        }

        for (field, value) in [
            ("decision_bar_open_time", json!("2024-07-03T13:31:00Z")),
            ("decision_event_time", json!("2024-07-03T13:34:00Z")),
            ("decision_available_time", json!("2024-07-03T13:34:00Z")),
            ("source_count", json!(4)),
            ("source_first_external_execution_sequence", json!(2)),
            ("decision_source_sequence", json!(4)),
        ] {
            let mut bad_decision = audit.clone();
            bad_decision["mappings"][0][field] = value;
            assert!(
                validate_bar_time_fixture(&bad_decision, &execution, &actions, &rebalance, &trades)
                    .is_err(),
                "tampered decision field {field} must fail"
            );
        }

        let mut erased_action = audit.clone();
        erased_action["mappings"][0]["signal_action"] = Value::Null;
        erased_action["mappings"][0]["action_id"] = Value::Null;
        erased_action["mappings"][0]["action_status"] = Value::Null;
        erased_action["mappings"][0]["eligible_fill_id"] = Value::Null;
        assert!(validate_bar_time_fixture(
            &erased_action,
            &execution,
            &actions,
            &rebalance,
            &trades
        )
        .is_err());

        assert!(validate_bar_time_fixture(&audit, &execution, &actions, &rebalance, &[]).is_err());
    }

    #[test]
    fn valid_result_returns_versioned_report_and_hash() {
        let equity = valid_equity();
        let empty = Vec::new();
        let report = validate_result_tables(ResultTableView {
            result_schema_version: "rust_accounting_result_tables.v1",
            equity_curve: &equity,
            execution_equity_curve: &[],
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
            execution_equity_curve: &[],
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
