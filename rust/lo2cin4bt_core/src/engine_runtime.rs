use crate::bar_aggregation::parse_utc_nanos;
use crate::computed_fields::returns::simple_return;
use crate::daily_rank::{compute_feature_fields_with_market_fields, evaluate_condition};
use crate::{
    aggregate_time_bars, run_accounting, run_calendar_overlay_batch,
    run_daily_rank_accounting_batch, run_single_asset_calendar_same_session_batch,
    validate_bar_time_audit, AccountingConfig, AccountingInput, AccountingRiskGateConfig,
    AggregationRequest, BarAggregationError, BarAlignment as RuntimeBarAlignment, BarPriceBasisV1,
    BarSpec as RuntimeBarSpec, BarStreamSourceV1, BarTimeExpectedAggregationLineage,
    BarTimeExpectedDecisionEvidence, BarTimeTrustedActionEvidence, BarTimeValidationContext,
    BarTimestampConventionV1, BarUnit as RuntimeBarUnit, CalendarOverlayBatchInput,
    CalendarSameSessionBatchInput, CalendarSameSessionCandidateInput, CheckpointInput,
    ContractBarAlignmentV1, ContractBarSpecV1, ContractBarUnitV1, DailyRankBatchCandidateInput,
    DailyRankBatchInput, DailyRankConditionInput, DailyRankFeatureSpec, DecisionPlanV1, DerivedBar,
    EmptyBarPolicyV1, EngineRequestV2, ExecutionBarIndex, FinalPartialBarPolicyV1,
    MarketDataBundleV2, MarketDataIndexKind, OperationId,
    PartialBarPolicy as RuntimePartialBarPolicy, PartialBarPolicyV1, ResetTimerBatchInput,
    ResetTimerCandidateInput, SessionWindow, SingleAssetSignalBatchInput,
    SingleAssetSignalCandidateInput, SourceBar, TimelineAccountingConfig, TimelinePositionPolicy,
};
use polars::io::parquet::read::ParquetReader;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestExecutionInput {
    pub engine_request: EngineRequestV2,
    pub market_data_bundle: MarketDataBundleV2,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestBatchExecutionInput {
    pub engine_requests: Vec<EngineRequestV2>,
    pub market_data_bundle: MarketDataBundleV2,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ResetTimerSpec {
    signal_field: String,
    baseline_weights: BTreeMap<String, f64>,
    event_weights: BTreeMap<String, f64>,
    restore_weights: BTreeMap<String, f64>,
    entry_offset_bars: usize,
    entry_phase: String,
    restore_phase: String,
    hold_bars: usize,
}

type AssetWeights = BTreeMap<String, f64>;

#[derive(Debug, Clone)]
struct PreparedDecisionBar {
    stream_id: String,
    bar_open_timestamp: String,
    event_timestamp: String,
    available_timestamp: String,
    session_label: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    source_count: usize,
    source_first_execution_sequence: u64,
    source_execution_sequence: u64,
    aggregation_lineage: Vec<BarTimeAggregationLineage>,
    next_execution_index: Option<usize>,
}

#[derive(Debug, Clone)]
struct PreparedRuntimeStreams {
    execution_stream_id: String,
    decision_stream_id: String,
    execution_bars_by_symbol: BTreeMap<String, Vec<SourceBar>>,
    decision_bars_by_symbol: BTreeMap<String, Vec<PreparedDecisionBar>>,
    aggregated: bool,
}

#[derive(Debug, Clone)]
struct PreparedGraphBar {
    bar: SourceBar,
    aggregation_lineage: Vec<BarTimeAggregationLineage>,
}

#[derive(Debug, Clone, Serialize)]
struct BarTimeAudit {
    schema_version: &'static str,
    contract_id: &'static str,
    bar_time_contract_id: String,
    bar_time_contract_hash: String,
    stream_graph_hash: String,
    ordering_contract: &'static str,
    execution_stream_id: String,
    decision_stream_id: String,
    aggregated: bool,
    external_source_bar_count: usize,
    derived_decision_bar_count: usize,
    first_external_available_time: String,
    last_external_available_time: String,
    first_decision_available_time: String,
    last_decision_available_time: String,
    empty_bar_policy: String,
    partial_first_bar_policy: String,
    partial_final_bar_policy: String,
    mapping_count: usize,
    terminal_decision_count: usize,
    mappings: Vec<BarTimeAuditMapping>,
    terminal_decisions: Vec<BarTimeTerminalDecisionAudit>,
}

#[derive(Debug, Clone, Serialize)]
struct BarTimeAuditMapping {
    symbol: String,
    decision_index: usize,
    decision_id: String,
    decision_stream_id: String,
    decision_bar_open_time: String,
    decision_event_time: String,
    decision_available_time: String,
    source_count: usize,
    source_first_external_execution_sequence: u64,
    decision_source_sequence: u64,
    execution_stream_id: String,
    execution_index: usize,
    execution_bar_open_time: String,
    execution_bar_close_time: String,
    execution_available_time: String,
    external_execution_sequence: u64,
    execution_open_price: f64,
    lifecycle_stage: &'static str,
    ordering_key: String,
    lifecycle: Vec<BarTimeLifecycleAuditRow>,
    signal_action: Option<String>,
    action_id: Option<String>,
    action_status: Option<String>,
    action_reason: Option<String>,
    eligible_fill_id: Option<String>,
    aggregation_lineage: Vec<BarTimeAggregationLineage>,
}

#[derive(Debug, Clone, Serialize)]
struct BarTimeLifecycleAuditRow {
    lifecycle_stage: &'static str,
    lifecycle_rank: u8,
    available_time: String,
    event_time: String,
    external_execution_sequence: u64,
    stream_id: String,
    source_sequence: u64,
    ordering_key: String,
}

#[derive(Debug, Clone, Serialize)]
struct BarTimeAggregationLineage {
    stream_id: String,
    parent_stream_id: String,
    parent_bar_count: usize,
    external_source_bar_count: usize,
    source_first_external_execution_sequence: u64,
    source_last_external_execution_sequence: u64,
    partial: bool,
}

#[derive(Debug, Clone, Serialize)]
struct BarTimeTerminalDecisionAudit {
    symbol: String,
    decision_index: usize,
    decision_id: String,
    decision_stream_id: String,
    decision_bar_open_time: String,
    decision_event_time: String,
    decision_available_time: String,
    source_count: usize,
    source_first_external_execution_sequence: u64,
    decision_source_sequence: u64,
    aggregation_lineage: Vec<BarTimeAggregationLineage>,
    signal_action: Option<String>,
    status: String,
    reason: Option<String>,
    action_id: Option<String>,
    eligible_fill_id: Option<String>,
    execution_index: Option<usize>,
}

#[derive(Debug, Error)]
pub enum EngineRuntimeError {
    #[error("invalid EngineRequest: {0}")]
    InvalidRequest(String),
    #[error("invalid MarketDataBundle: {0}")]
    InvalidBundle(String),
    #[error("unsupported EngineRequest profile: {0}")]
    UnsupportedProfile(String),
    #[error("invalid fixed-allocation strategy: {0}")]
    InvalidAllocation(String),
    #[error("failed to read market data: {0}")]
    MarketData(String),
    #[error("accounting failed: {0}")]
    Accounting(String),
}

impl PreparedRuntimeStreams {
    fn validate(&self) -> Result<(), EngineRuntimeError> {
        if self.execution_stream_id.trim().is_empty() || self.decision_stream_id.trim().is_empty() {
            return Err(EngineRuntimeError::InvalidRequest(
                "prepared runtime stream IDs must not be empty".to_string(),
            ));
        }
        if self.execution_bars_by_symbol.is_empty()
            || self.execution_bars_by_symbol.len() != self.decision_bars_by_symbol.len()
        {
            return Err(EngineRuntimeError::MarketData(
                "prepared runtime streams must cover the same non-empty symbol set".to_string(),
            ));
        }
        for (symbol, execution_bars) in &self.execution_bars_by_symbol {
            let decisions = self.decision_bars_by_symbol.get(symbol).ok_or_else(|| {
                EngineRuntimeError::MarketData(format!(
                    "prepared decision stream is missing symbol {symbol}"
                ))
            })?;
            if execution_bars.is_empty() || decisions.is_empty() {
                return Err(EngineRuntimeError::MarketData(format!(
                    "prepared runtime stream for {symbol} must not be empty"
                )));
            }
            for decision in decisions {
                if decision.stream_id != self.decision_stream_id
                    || decision.bar_open_timestamp.trim().is_empty()
                    || decision.event_timestamp.trim().is_empty()
                    || decision.available_timestamp.trim().is_empty()
                    || decision.session_label.trim().is_empty()
                    || ![
                        decision.open,
                        decision.high,
                        decision.low,
                        decision.close,
                        decision.volume,
                    ]
                    .iter()
                    .all(|value| value.is_finite())
                {
                    return Err(EngineRuntimeError::MarketData(format!(
                        "prepared decision stream metadata is invalid for {symbol}"
                    )));
                }
                if decision.source_count == 0
                    || decision.source_first_execution_sequence > decision.source_execution_sequence
                {
                    return Err(EngineRuntimeError::MarketData(format!(
                        "prepared decision lineage is invalid for {symbol}"
                    )));
                }
                if let Some(index) = decision.next_execution_index {
                    let execution = execution_bars.get(index).ok_or_else(|| {
                        EngineRuntimeError::MarketData(format!(
                            "prepared next execution index is out of bounds for {symbol}"
                        ))
                    })?;
                    if execution.external_execution_sequence <= decision.source_execution_sequence {
                        return Err(EngineRuntimeError::MarketData(format!(
                            "prepared next execution sequence is not strictly later for {symbol}"
                        )));
                    }
                }
            }
        }
        if !self.aggregated && self.execution_stream_id != self.decision_stream_id {
            return Err(EngineRuntimeError::InvalidRequest(
                "non-aggregated prepared streams require decision=execution".to_string(),
            ));
        }
        Ok(())
    }
}

fn build_bar_time_audit(
    request: &EngineRequestV2,
    prepared: &PreparedRuntimeStreams,
) -> Result<BarTimeAudit, EngineRuntimeError> {
    let mut mappings = Vec::new();
    let mut terminal_decisions = Vec::new();
    let mut terminal_decision_count = 0;
    for (symbol, decisions) in &prepared.decision_bars_by_symbol {
        let execution_bars = prepared
            .execution_bars_by_symbol
            .get(symbol)
            .ok_or_else(|| {
                EngineRuntimeError::MarketData(format!(
                    "bar-time audit execution stream is missing symbol {symbol}"
                ))
            })?;
        for (decision_index, decision) in decisions.iter().enumerate() {
            let Some(execution_index) = decision.next_execution_index else {
                terminal_decision_count += 1;
                terminal_decisions.push(BarTimeTerminalDecisionAudit {
                    symbol: symbol.clone(),
                    decision_index,
                    decision_id: format!(
                        "{symbol}:{}:{decision_index}",
                        prepared.decision_stream_id
                    ),
                    decision_stream_id: decision.stream_id.clone(),
                    decision_bar_open_time: decision.bar_open_timestamp.clone(),
                    decision_event_time: decision.event_timestamp.clone(),
                    decision_available_time: decision.available_timestamp.clone(),
                    source_count: decision.source_count,
                    source_first_external_execution_sequence: decision
                        .source_first_execution_sequence,
                    decision_source_sequence: decision.source_execution_sequence,
                    aggregation_lineage: decision.aggregation_lineage.clone(),
                    signal_action: None,
                    status: "no_signal".to_string(),
                    reason: None,
                    action_id: None,
                    eligible_fill_id: None,
                    execution_index: None,
                });
                continue;
            };
            let execution = execution_bars.get(execution_index).ok_or_else(|| {
                EngineRuntimeError::MarketData(format!(
                    "bar-time audit execution index is out of bounds for {symbol}"
                ))
            })?;
            mappings.push(BarTimeAuditMapping {
                symbol: symbol.clone(),
                decision_index,
                decision_id: format!("{symbol}:{}:{decision_index}", prepared.decision_stream_id),
                decision_stream_id: decision.stream_id.clone(),
                decision_bar_open_time: decision.bar_open_timestamp.clone(),
                decision_event_time: decision.event_timestamp.clone(),
                decision_available_time: decision.available_timestamp.clone(),
                source_count: decision.source_count,
                source_first_external_execution_sequence: decision.source_first_execution_sequence,
                decision_source_sequence: decision.source_execution_sequence,
                execution_stream_id: execution.stream_id.clone(),
                execution_index,
                execution_bar_open_time: execution.bar_open_timestamp.clone(),
                execution_bar_close_time: execution.event_timestamp.clone(),
                execution_available_time: execution.available_timestamp.clone(),
                external_execution_sequence: execution.external_execution_sequence,
                execution_open_price: execution.open,
                lifecycle_stage: "eligible_execution",
                ordering_key: canonical_ordering_key(
                    &execution.bar_open_timestamp,
                    &execution.bar_open_timestamp,
                    execution.external_execution_sequence,
                    4,
                    "eligible_execution",
                    &execution.stream_id,
                    decision.source_execution_sequence,
                ),
                lifecycle: bar_time_lifecycle_rows(decision, execution, None),
                signal_action: None,
                action_id: None,
                action_status: None,
                action_reason: None,
                eligible_fill_id: None,
                aggregation_lineage: decision.aggregation_lineage.clone(),
            });
        }
    }
    let execution_bars = prepared
        .execution_bars_by_symbol
        .values()
        .next()
        .ok_or_else(|| EngineRuntimeError::MarketData("execution stream is empty".to_string()))?;
    let decision_bars = prepared
        .decision_bars_by_symbol
        .values()
        .next()
        .ok_or_else(|| EngineRuntimeError::MarketData("decision stream is empty".to_string()))?;
    let first_execution = execution_bars
        .first()
        .ok_or_else(|| EngineRuntimeError::MarketData("execution stream is empty".to_string()))?;
    let last_execution = execution_bars
        .last()
        .ok_or_else(|| EngineRuntimeError::MarketData("execution stream is empty".to_string()))?;
    let first_decision = decision_bars
        .first()
        .ok_or_else(|| EngineRuntimeError::MarketData("decision stream is empty".to_string()))?;
    let last_decision = decision_bars
        .last()
        .ok_or_else(|| EngineRuntimeError::MarketData("decision stream is empty".to_string()))?;
    let (empty_bar_policy, partial_first_bar_policy, partial_final_bar_policy) =
        decision_policy_labels(request)?;
    Ok(BarTimeAudit {
        schema_version: "bar_time_audit.v1",
        contract_id: "lo2cin4bt.bar_time_audit.v1",
        bar_time_contract_id: request.data_requirements.bar_time.contract_id.clone(),
        bar_time_contract_hash: canonical_json_hash(&request.data_requirements.bar_time)?,
        stream_graph_hash: canonical_json_hash(&serde_json::json!({
            "streams": request.data_requirements.bar_time.streams,
            "binding": request.strategy.stream_binding,
        }))?,
        ordering_contract:
            "available_time,event_time,external_execution_sequence,lifecycle_stage,stream_id,source_sequence",
        execution_stream_id: prepared.execution_stream_id.clone(),
        decision_stream_id: prepared.decision_stream_id.clone(),
        aggregated: prepared.aggregated,
        external_source_bar_count: execution_bars.len(),
        derived_decision_bar_count: if prepared.aggregated {
            decision_bars.len()
        } else {
            0
        },
        first_external_available_time: first_execution.available_timestamp.clone(),
        last_external_available_time: last_execution.available_timestamp.clone(),
        first_decision_available_time: first_decision.available_timestamp.clone(),
        last_decision_available_time: last_decision.available_timestamp.clone(),
        empty_bar_policy,
        partial_first_bar_policy,
        partial_final_bar_policy,
        mapping_count: mappings.len(),
        terminal_decision_count,
        mappings,
        terminal_decisions,
    })
}

fn expected_bar_time_decisions(
    prepared: &PreparedRuntimeStreams,
    candidate: &SingleAssetSignalCandidateInput,
) -> Result<Vec<BarTimeExpectedDecisionEvidence>, EngineRuntimeError> {
    let decisions = prepared
        .decision_bars_by_symbol
        .iter()
        .flat_map(|(symbol, decisions)| {
            decisions
                .iter()
                .enumerate()
                .map(
                    move |(decision_index, decision)| BarTimeExpectedDecisionEvidence {
                        symbol: symbol.clone(),
                        decision_index,
                        decision_id: format!(
                            "{symbol}:{}:{decision_index}",
                            prepared.decision_stream_id
                        ),
                        decision_bar_open_time: decision.bar_open_timestamp.clone(),
                        decision_event_time: decision.event_timestamp.clone(),
                        decision_available_time: decision.available_timestamp.clone(),
                        source_count: decision.source_count,
                        source_first_external_execution_sequence: decision
                            .source_first_execution_sequence,
                        decision_source_sequence: decision.source_execution_sequence,
                        next_execution_index: decision.next_execution_index,
                        aggregation_lineage: decision
                            .aggregation_lineage
                            .iter()
                            .map(|lineage| BarTimeExpectedAggregationLineage {
                                stream_id: lineage.stream_id.clone(),
                                parent_stream_id: lineage.parent_stream_id.clone(),
                                parent_bar_count: lineage.parent_bar_count,
                                external_source_bar_count: lineage.external_source_bar_count,
                                source_first_external_execution_sequence: lineage
                                    .source_first_external_execution_sequence,
                                source_last_external_execution_sequence: lineage
                                    .source_last_external_execution_sequence,
                                partial: lineage.partial,
                            })
                            .collect(),
                        expected_signal_action: match (
                            candidate.entry_signal[decision_index],
                            candidate.exit_signal[decision_index],
                        ) {
                            (true, false) => Some("enter".to_string()),
                            (false, true) => Some("exit".to_string()),
                            (false, false) => None,
                            (true, true) => Some("invalid_simultaneous".to_string()),
                        },
                    },
                )
        })
        .collect::<Vec<_>>();
    if decisions
        .iter()
        .any(|decision| decision.expected_signal_action.as_deref() == Some("invalid_simultaneous"))
    {
        return Err(EngineRuntimeError::InvalidRequest(
            "simultaneous entry and exit signals cannot produce canonical bar-time evidence"
                .to_string(),
        ));
    }
    Ok(decisions)
}

fn validate_prepared_profile_binding(
    request: &EngineRequestV2,
    prepared: &PreparedRuntimeStreams,
) -> Result<(), EngineRuntimeError> {
    if prepared.aggregated && !is_single_asset_signal_request(request) {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "derived decision streams are active only for the single-asset next-open signal profile"
                .to_string(),
        ));
    }
    Ok(())
}

fn canonical_json_hash<T: Serialize>(value: &T) -> Result<String, EngineRuntimeError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn canonical_ordering_key(
    available_time: &str,
    event_time: &str,
    external_execution_sequence: u64,
    lifecycle_rank: u8,
    lifecycle_stage: &str,
    stream_id: &str,
    source_sequence: u64,
) -> String {
    format!(
        "{available_time}|{event_time}|{external_execution_sequence:020}|{lifecycle_rank:02}:{lifecycle_stage}|{stream_id}|{source_sequence:020}"
    )
}

fn bar_time_lifecycle_rows(
    decision: &PreparedDecisionBar,
    execution: &SourceBar,
    action_status: Option<&str>,
) -> Vec<BarTimeLifecycleAuditRow> {
    let mut specs = vec![
        (
            "decision_event",
            0,
            decision.event_timestamp.as_str(),
            decision.event_timestamp.as_str(),
            decision.source_execution_sequence,
            decision.stream_id.as_str(),
            decision.source_execution_sequence,
        ),
        (
            "decision_available",
            1,
            decision.available_timestamp.as_str(),
            decision.event_timestamp.as_str(),
            decision.source_execution_sequence,
            decision.stream_id.as_str(),
            decision.source_execution_sequence,
        ),
        (
            "eligible_execution",
            4,
            execution.bar_open_timestamp.as_str(),
            execution.bar_open_timestamp.as_str(),
            execution.external_execution_sequence,
            execution.stream_id.as_str(),
            decision.source_execution_sequence,
        ),
        (
            "execution_close",
            6,
            execution.available_timestamp.as_str(),
            execution.event_timestamp.as_str(),
            execution.external_execution_sequence,
            execution.stream_id.as_str(),
            decision.source_execution_sequence,
        ),
    ];
    if action_status.is_some() {
        specs.push((
            "signal",
            2,
            decision.available_timestamp.as_str(),
            decision.event_timestamp.as_str(),
            decision.source_execution_sequence,
            decision.stream_id.as_str(),
            decision.source_execution_sequence,
        ));
        specs.push((
            "order",
            3,
            decision.available_timestamp.as_str(),
            decision.event_timestamp.as_str(),
            decision.source_execution_sequence,
            decision.stream_id.as_str(),
            decision.source_execution_sequence,
        ));
    }
    if action_status == Some("filled") {
        specs.push((
            "eligible_fill",
            5,
            execution.bar_open_timestamp.as_str(),
            execution.bar_open_timestamp.as_str(),
            execution.external_execution_sequence,
            execution.stream_id.as_str(),
            decision.source_execution_sequence,
        ));
    }
    specs.sort_by_key(|spec| spec.1);
    specs
        .into_iter()
        .map(
            |(
                lifecycle_stage,
                lifecycle_rank,
                available_time,
                event_time,
                external_execution_sequence,
                stream_id,
                source_sequence,
            )| BarTimeLifecycleAuditRow {
                lifecycle_stage,
                lifecycle_rank,
                available_time: available_time.to_string(),
                event_time: event_time.to_string(),
                external_execution_sequence,
                stream_id: stream_id.to_string(),
                source_sequence,
                ordering_key: canonical_ordering_key(
                    available_time,
                    event_time,
                    external_execution_sequence,
                    lifecycle_rank,
                    lifecycle_stage,
                    stream_id,
                    source_sequence,
                ),
            },
        )
        .collect()
}

fn refresh_audit_mapping_lifecycle(mapping: &mut BarTimeAuditMapping) {
    let mut rows = vec![
        audit_lifecycle_row(
            "decision_event",
            0,
            &mapping.decision_event_time,
            &mapping.decision_event_time,
            mapping.decision_source_sequence,
            &mapping.decision_stream_id,
            mapping.decision_source_sequence,
        ),
        audit_lifecycle_row(
            "decision_available",
            1,
            &mapping.decision_available_time,
            &mapping.decision_event_time,
            mapping.decision_source_sequence,
            &mapping.decision_stream_id,
            mapping.decision_source_sequence,
        ),
        audit_lifecycle_row(
            "eligible_execution",
            4,
            &mapping.execution_bar_open_time,
            &mapping.execution_bar_open_time,
            mapping.external_execution_sequence,
            &mapping.execution_stream_id,
            mapping.decision_source_sequence,
        ),
        audit_lifecycle_row(
            "execution_close",
            6,
            &mapping.execution_available_time,
            &mapping.execution_bar_close_time,
            mapping.external_execution_sequence,
            &mapping.execution_stream_id,
            mapping.decision_source_sequence,
        ),
    ];
    if mapping.action_status.is_some() {
        rows.push(audit_lifecycle_row(
            "signal",
            2,
            &mapping.decision_available_time,
            &mapping.decision_event_time,
            mapping.decision_source_sequence,
            &mapping.decision_stream_id,
            mapping.decision_source_sequence,
        ));
        rows.push(audit_lifecycle_row(
            "order",
            3,
            &mapping.decision_available_time,
            &mapping.decision_event_time,
            mapping.decision_source_sequence,
            &mapping.decision_stream_id,
            mapping.decision_source_sequence,
        ));
    }
    if mapping.action_status.as_deref() == Some("filled") {
        rows.push(audit_lifecycle_row(
            "eligible_fill",
            5,
            &mapping.execution_bar_open_time,
            &mapping.execution_bar_open_time,
            mapping.external_execution_sequence,
            &mapping.execution_stream_id,
            mapping.decision_source_sequence,
        ));
    }
    rows.sort_by_key(|row| row.lifecycle_rank);
    mapping.lifecycle = rows;
}

fn audit_lifecycle_row(
    lifecycle_stage: &'static str,
    lifecycle_rank: u8,
    available_time: &str,
    event_time: &str,
    external_execution_sequence: u64,
    stream_id: &str,
    source_sequence: u64,
) -> BarTimeLifecycleAuditRow {
    BarTimeLifecycleAuditRow {
        lifecycle_stage,
        lifecycle_rank,
        available_time: available_time.to_string(),
        event_time: event_time.to_string(),
        external_execution_sequence,
        stream_id: stream_id.to_string(),
        source_sequence,
        ordering_key: canonical_ordering_key(
            available_time,
            event_time,
            external_execution_sequence,
            lifecycle_rank,
            lifecycle_stage,
            stream_id,
            source_sequence,
        ),
    }
}

fn decision_policy_labels(
    request: &EngineRequestV2,
) -> Result<(String, String, String), EngineRuntimeError> {
    let decision = request
        .data_requirements
        .bar_time
        .streams
        .iter()
        .find(|stream| stream.stream_id == request.strategy.stream_binding.decision_stream_id)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest("decision stream is missing".to_string())
        })?;
    match &decision.source {
        BarStreamSourceV1::External { .. } => Ok((
            "not_applicable".to_string(),
            "not_applicable".to_string(),
            "not_applicable".to_string(),
        )),
        BarStreamSourceV1::Derived {
            empty_bar_policy,
            partial_first_bar_policy,
            partial_final_bar_policy,
            ..
        } => Ok((
            match empty_bar_policy {
                EmptyBarPolicyV1::Omit => "omit".to_string(),
            },
            match partial_first_bar_policy {
                PartialBarPolicyV1::Omit => "omit".to_string(),
                PartialBarPolicyV1::Emit => "emit".to_string(),
            },
            match partial_final_bar_policy {
                FinalPartialBarPolicyV1::Omit => "omit".to_string(),
                FinalPartialBarPolicyV1::Emit => "emit".to_string(),
            },
        )),
    }
}

fn validate_bundle_request_binding(
    request: &EngineRequestV2,
    bundle: &MarketDataBundleV2,
) -> Result<(), EngineRuntimeError> {
    let binding = &request.strategy.stream_binding;
    let execution = request
        .data_requirements
        .bar_time
        .streams
        .iter()
        .find(|stream| stream.stream_id == binding.execution_stream_id)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "execution stream binding is missing from bar_time".to_string(),
            )
        })?;
    let BarStreamSourceV1::External { provider_id } = &execution.source else {
        return Err(EngineRuntimeError::InvalidRequest(
            "execution stream binding must be external".to_string(),
        ));
    };
    if bundle.execution_stream.stream_id != execution.stream_id
        || bundle.execution_stream.source.provider_id != *provider_id
        || bundle.execution_stream.bar_spec != execution.bar_spec
        || bundle.execution_stream.session_scope
            != request
                .data_requirements
                .bar_time
                .session_model
                .session_scope
        || bundle.calendar != request.data_requirements.bar_time.session_model.calendar_id
        || bundle.timezone != request.data_requirements.bar_time.session_model.timezone
    {
        return Err(EngineRuntimeError::InvalidBundle(
            "MarketDataBundle execution stream does not exactly match EngineRequest".to_string(),
        ));
    }
    let request_semantics = &execution.timestamp_semantics;
    let bundle_semantics = &bundle.execution_stream.timestamp_semantics;
    if bundle_semantics.timestamp_convention != request_semantics.timestamp_convention
        || bundle_semantics.interval_boundary != request_semantics.interval_boundary
        || bundle_semantics.bar_open_time_column != request_semantics.bar_open_time_column
        || bundle_semantics.bar_close_time_column != request_semantics.bar_close_time_column
        || bundle_semantics.available_time_column != request_semantics.available_time_column
        || bundle_semantics.session_label_column != request_semantics.session_label_column
        || bundle_semantics.availability_policy != request_semantics.availability_policy
    {
        return Err(EngineRuntimeError::InvalidBundle(
            "MarketDataBundle timestamp semantics do not exactly match EngineRequest".to_string(),
        ));
    }
    let expected_adjustment = match request.data_requirements.bar_time.price_model.price_basis {
        BarPriceBasisV1::Raw => "raw",
        BarPriceBasisV1::SplitAdjusted => "split_adjusted",
        BarPriceBasisV1::SplitDividendAdjusted => "split_dividend_adjusted",
    };
    if bundle.lineage.adjustment_policy != expected_adjustment {
        return Err(EngineRuntimeError::InvalidBundle(
            "MarketDataBundle adjustment_policy does not match EngineRequest price_basis"
                .to_string(),
        ));
    }
    Ok(())
}

fn prepare_runtime_streams(
    request: &EngineRequestV2,
    bundle: &MarketDataBundleV2,
) -> Result<PreparedRuntimeStreams, EngineRuntimeError> {
    let mut frames = BTreeMap::new();
    for name in [
        bundle.execution_stream.ohlcv_tables.open.as_str(),
        bundle.execution_stream.ohlcv_tables.high.as_str(),
        bundle.execution_stream.ohlcv_tables.low.as_str(),
        bundle.execution_stream.ohlcv_tables.close.as_str(),
        bundle.execution_stream.ohlcv_tables.volume.as_str(),
        bundle.execution_stream.timeline_table.as_str(),
    ] {
        frames.insert(name.to_string(), read_bundle_table(bundle, name)?);
    }
    prepare_runtime_streams_from_frames(request, bundle, &frames)
}

fn prepare_runtime_streams_from_frames(
    request: &EngineRequestV2,
    bundle: &MarketDataBundleV2,
    frames: &BTreeMap<String, DataFrame>,
) -> Result<PreparedRuntimeStreams, EngineRuntimeError> {
    validate_bundle_request_binding(request, bundle)?;
    let execution = &bundle.execution_stream;
    let timeline = frames.get(&execution.timeline_table).ok_or_else(|| {
        EngineRuntimeError::InvalidBundle("execution_timeline frame is missing".to_string())
    })?;
    let row_keys = time_strings(timeline, &bundle.time_column, execution.row_key_kind)?;
    let sequence = table_u64_column(
        timeline,
        &execution
            .timestamp_semantics
            .external_execution_sequence_column,
    )?;
    let open_timestamps = table_timestamp_column(
        timeline,
        &execution.timestamp_semantics.bar_open_time_column,
    )?;
    let close_timestamps = table_timestamp_column(
        timeline,
        &execution.timestamp_semantics.bar_close_time_column,
    )?;
    let available_timestamps = table_timestamp_column(
        timeline,
        &execution.timestamp_semantics.available_time_column,
    )?;
    let session_labels = table_string_column(
        timeline,
        &execution.timestamp_semantics.session_label_column,
    )?;
    let height = timeline.height();
    if [
        row_keys.len(),
        sequence.len(),
        open_timestamps.len(),
        close_timestamps.len(),
        available_timestamps.len(),
        session_labels.len(),
    ]
    .iter()
    .any(|length| *length != height)
    {
        return Err(EngineRuntimeError::MarketData(
            "execution_timeline columns have inconsistent lengths".to_string(),
        ));
    }
    validate_available_timeline(
        timeline,
        &close_timestamps,
        &execution.timestamp_semantics.available_time_column,
    )?;
    validate_timeline_row_keys(
        execution.row_key_kind,
        execution.timestamp_semantics.timestamp_convention,
        &row_keys,
        &open_timestamps,
        &close_timestamps,
        &session_labels,
    )?;

    let mut execution_bars_by_symbol = BTreeMap::new();
    for symbol in &bundle.symbols {
        let open = table_price_column(
            required_frame(frames, &execution.ohlcv_tables.open)?,
            symbol,
        )?;
        let high = table_price_column(
            required_frame(frames, &execution.ohlcv_tables.high)?,
            symbol,
        )?;
        let low = table_price_column(required_frame(frames, &execution.ohlcv_tables.low)?, symbol)?;
        let close = table_price_column(
            required_frame(frames, &execution.ohlcv_tables.close)?,
            symbol,
        )?;
        let volume = table_numeric_column(
            required_frame(frames, &execution.ohlcv_tables.volume)?,
            symbol,
        )?;
        for name in [
            &execution.ohlcv_tables.open,
            &execution.ohlcv_tables.high,
            &execution.ohlcv_tables.low,
            &execution.ohlcv_tables.close,
            &execution.ohlcv_tables.volume,
        ] {
            let keys = bundle_time_strings(bundle, required_frame(frames, name)?)?;
            if keys != row_keys {
                return Err(EngineRuntimeError::MarketData(format!(
                    "{name} row keys do not match execution_timeline"
                )));
            }
        }
        let bars = (0..height)
            .map(|index| SourceBar {
                stream_id: execution.stream_id.clone(),
                external_execution_sequence: sequence[index],
                bar_open_timestamp: open_timestamps[index].clone(),
                event_timestamp: close_timestamps[index].clone(),
                available_timestamp: available_timestamps[index].clone(),
                session_label: session_labels[index].clone(),
                open: open[index],
                high: high[index],
                low: low[index],
                close: close[index],
                volume: volume[index],
            })
            .collect::<Vec<_>>();
        validate_decoded_source_bars(bundle, &bars)?;
        execution_bars_by_symbol.insert(symbol.clone(), bars);
    }

    let decision_id = &request.strategy.stream_binding.decision_stream_id;
    let mut decision_bars_by_symbol = BTreeMap::new();
    let aggregated = decision_id != &execution.stream_id;
    if !aggregated {
        for (symbol, bars) in &execution_bars_by_symbol {
            let execution_index = ExecutionBarIndex::new(bars)
                .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
            decision_bars_by_symbol.insert(
                symbol.clone(),
                bars.iter()
                    .map(|bar| {
                        let next_execution_index = match execution_index.next_eligible(
                            &bar.available_timestamp,
                            bar.external_execution_sequence,
                        ) {
                            Ok(index) => Some(index),
                            Err(BarAggregationError::NoEligibleExecutionBar { .. }) => None,
                            Err(error) => {
                                return Err(EngineRuntimeError::MarketData(error.to_string()))
                            }
                        };
                        Ok(PreparedDecisionBar {
                            stream_id: execution.stream_id.clone(),
                            bar_open_timestamp: bar.bar_open_timestamp.clone(),
                            event_timestamp: bar.event_timestamp.clone(),
                            available_timestamp: bar.available_timestamp.clone(),
                            session_label: bar.session_label.clone(),
                            open: bar.open,
                            high: bar.high,
                            low: bar.low,
                            close: bar.close,
                            volume: bar.volume,
                            source_count: 1,
                            source_first_execution_sequence: bar.external_execution_sequence,
                            source_execution_sequence: bar.external_execution_sequence,
                            aggregation_lineage: Vec::new(),
                            next_execution_index,
                        })
                    })
                    .collect::<Result<Vec<_>, EngineRuntimeError>>()?,
            );
        }
    } else {
        let sessions = bundle
            .session_windows
            .iter()
            .map(|window| SessionWindow {
                session_label: window.session_label.clone(),
                open_timestamp: window.open_timestamp.clone(),
                close_timestamp: window.close_timestamp.clone(),
            })
            .collect::<Vec<_>>();
        for (symbol, execution_bars) in &execution_bars_by_symbol {
            let prepared = prepare_decision_graph(request, decision_id, execution_bars, &sessions)?;
            decision_bars_by_symbol.insert(symbol.clone(), prepared);
        }
    }

    let prepared = PreparedRuntimeStreams {
        execution_stream_id: execution.stream_id.clone(),
        decision_stream_id: decision_id.clone(),
        execution_bars_by_symbol,
        decision_bars_by_symbol,
        aggregated,
    };
    prepared.validate()?;
    Ok(prepared)
}

pub fn execute_engine_request(
    input: EngineRequestExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    input
        .engine_request
        .validate()
        .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
    input
        .market_data_bundle
        .validate()
        .map_err(|error| EngineRuntimeError::InvalidBundle(error.to_string()))?;
    if input.engine_request.data_requirements.symbols != input.market_data_bundle.symbols {
        return Err(EngineRuntimeError::InvalidBundle(
            "bundle symbols do not match EngineRequest".to_string(),
        ));
    }
    let prepared_runtime_streams =
        prepare_runtime_streams(&input.engine_request, &input.market_data_bundle)?;
    validate_prepared_profile_binding(&input.engine_request, &prepared_runtime_streams)?;

    let decision = &input.engine_request.strategy.decision_plan;
    let allocation_method = decision
        .allocation
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if is_reset_timer_request(&input.engine_request) {
        execute_reset_timer(input)
    } else if decision
        .required_operations
        .contains(&OperationId::SessionSameSessionClose)
    {
        execute_calendar_same_session(input)
    } else if calendar_entry(decision).is_some() && has_event_weight_actions(&input.engine_request)
    {
        execute_calendar_overlay(input)
    } else if is_single_asset_signal_request(&input.engine_request) {
        execute_single_asset_signal(input, &prepared_runtime_streams)
    } else {
        match allocation_method {
            "fixed_weights" => execute_fixed_allocation(input),
            "equal_weight" | "equal_weight_long_short" => execute_daily_rank(input),
            _ => Err(EngineRuntimeError::UnsupportedProfile(format!(
                "decision plan with allocation method {allocation_method} requires a later StrategyIR runtime slice"
            ))),
        }
    }
}

pub fn execute_engine_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    if input.engine_requests.is_empty() {
        return Err(EngineRuntimeError::InvalidRequest(
            "engine_requests must not be empty".to_string(),
        ));
    }
    let has_derived_binding = input.engine_requests.iter().any(|request| {
        request.strategy.stream_binding.decision_stream_id
            != request.strategy.stream_binding.execution_stream_id
    });
    if has_derived_binding
        && !input
            .engine_requests
            .iter()
            .all(is_single_asset_signal_request)
    {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "derived decision streams are active only for the single-asset next-open signal profile"
                .to_string(),
        ));
    }
    if input.engine_requests.iter().all(is_daily_rank_request) {
        return execute_daily_rank_request_batch(input);
    }
    if input.engine_requests.iter().all(is_reset_timer_request) {
        return execute_reset_timer_request_batch(input);
    }
    if input
        .engine_requests
        .iter()
        .all(is_calendar_same_session_request)
    {
        return execute_calendar_same_session_request_batch(input);
    }
    if input
        .engine_requests
        .iter()
        .all(is_calendar_overlay_request)
    {
        return execute_calendar_overlay_request_batch(input);
    }
    if input
        .engine_requests
        .iter()
        .all(is_single_asset_signal_request)
        && signal_requests_share_prepared_stream_contract(&input.engine_requests)?
    {
        return execute_signal_request_batch(input);
    }
    let batch_run_id = input
        .artifact_run_id
        .as_deref()
        .unwrap_or("engine_request_batch");
    let mut results = Vec::with_capacity(input.engine_requests.len());
    for (index, engine_request) in input.engine_requests.into_iter().enumerate() {
        let request_id = engine_request.request_id.clone();
        let strategy_id = engine_request.strategy.strategy_id.clone();
        let result = execute_engine_request(EngineRequestExecutionInput {
            engine_request,
            market_data_bundle: input.market_data_bundle.clone(),
            artifact_output_dir: input.artifact_output_dir.clone(),
            artifact_run_id: input
                .artifact_output_dir
                .as_ref()
                .map(|_| format!("{batch_run_id}_{index}")),
        })?;
        results.push(serde_json::json!({
            "request_id": request_id,
            "strategy_id": strategy_id,
            "result": result,
        }));
    }
    Ok(serde_json::json!({
        "request_count": results.len(),
        "execution_mode": "sequential",
        "results": results,
    }))
}

fn signal_requests_share_prepared_stream_contract(
    requests: &[EngineRequestV2],
) -> Result<bool, EngineRuntimeError> {
    let Some(first) = requests.first() else {
        return Ok(false);
    };
    let expected_data_requirements = serde_json::to_value(&first.data_requirements)
        .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
    let expected_binding = serde_json::to_value(&first.strategy.stream_binding)
        .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
    for request in &requests[1..] {
        let current_data_requirements = serde_json::to_value(&request.data_requirements)
            .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
        let current_binding = serde_json::to_value(&request.strategy.stream_binding)
            .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
        if current_data_requirements != expected_data_requirements
            || current_binding != expected_binding
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn execute_signal_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    validate_batch_bundle_and_requests(&input)?;
    let market_data_bundle_hash = input.market_data_bundle.content_hash.clone();
    if input.market_data_bundle.symbols.len() != 1 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "cross signal execution requires one symbol".to_string(),
        ));
    }
    if !signal_requests_share_prepared_stream_contract(&input.engine_requests)? {
        return Err(EngineRuntimeError::InvalidRequest(
            "grouped signal requests require identical data requirements and stream binding"
                .to_string(),
        ));
    }
    let first_request = input.engine_requests.first().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("engine_requests must not be empty".to_string())
    })?;
    let prepared = prepare_runtime_streams(first_request, &input.market_data_bundle)?;
    for request in &input.engine_requests {
        validate_prepared_profile_binding(request, &prepared)?;
        validate_next_open_signal_actions(request)?;
    }
    let asset = input.market_data_bundle.symbols[0].clone();
    let execution_bars = prepared
        .execution_bars_by_symbol
        .get(&asset)
        .ok_or_else(|| {
            EngineRuntimeError::MarketData("prepared execution stream is missing asset".to_string())
        })?;
    let decision_bars = prepared
        .decision_bars_by_symbol
        .get(&asset)
        .ok_or_else(|| {
            EngineRuntimeError::MarketData("prepared decision stream is missing asset".to_string())
        })?;
    let decision_close = decision_bars
        .iter()
        .map(|bar| bar.close)
        .collect::<Vec<_>>();
    let decision_market_fields = BTreeMap::from([
        (
            "open".to_string(),
            decision_bars.iter().map(|bar| bar.open).collect(),
        ),
        (
            "high".to_string(),
            decision_bars.iter().map(|bar| bar.high).collect(),
        ),
        (
            "low".to_string(),
            decision_bars.iter().map(|bar| bar.low).collect(),
        ),
        (
            "volume".to_string(),
            decision_bars.iter().map(|bar| bar.volume).collect(),
        ),
    ]);
    let mut decision_candidates = Vec::with_capacity(input.engine_requests.len());
    let mut execution_candidates = Vec::with_capacity(input.engine_requests.len());
    for request in &input.engine_requests {
        let mut decision_candidate = single_signal_candidate(
            request,
            &decision_close,
            &decision_market_fields,
            request.strategy.strategy_id.clone(),
        )?;
        mask_signal_candidate_to_workflow_window(request, decision_bars, &mut decision_candidate)?;
        execution_candidates.push(remap_signal_candidate_to_execution(
            decision_candidate.clone(),
            decision_bars,
            execution_bars.len(),
        )?);
        decision_candidates.push(decision_candidate);
    }
    let mut config = identical_timeline_config(&input.engine_requests)?;
    config.session_label_by_event_time = execution_bars
        .iter()
        .map(|bar| (bar.event_timestamp.clone(), bar.session_label.clone()))
        .collect();
    let mut summary = crate::run_single_asset_next_open_signal_batch(SingleAssetSignalBatchInput {
        config,
        asset: asset.clone(),
        dates: execution_bars
            .iter()
            .map(|bar| bar.event_timestamp.clone())
            .collect(),
        open: execution_bars.iter().map(|bar| bar.open).collect(),
        close: execution_bars.iter().map(|bar| bar.close).collect(),
        include_full_results: true,
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates: execution_candidates,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    if summary.results.len() != input.engine_requests.len()
        || summary.trusted_timelines.len() != input.engine_requests.len()
    {
        return Err(EngineRuntimeError::Accounting(
            "grouped signal result cardinality does not match requests".to_string(),
        ));
    }
    let mut candidate_audits = Vec::with_capacity(input.engine_requests.len());
    for (index, request) in input.engine_requests.iter().enumerate() {
        let result = &mut summary.results[index];
        if result.result_validation.status != "valid" || !result.result_validation.errors.is_empty()
        {
            return Err(EngineRuntimeError::Accounting(
                "cannot attach a passed bar-time check to a failed result validation".to_string(),
            ));
        }
        if result
            .result_validation
            .checks
            .iter()
            .any(|check| check.check_id == "bar_time_no_look_ahead")
        {
            return Err(EngineRuntimeError::Accounting(
                "bar-time validation check must be attached exactly once".to_string(),
            ));
        }
        let timeline = &summary.trusted_timelines[index];
        let expected_decisions =
            expected_bar_time_decisions(&prepared, &decision_candidates[index])?;
        let mut audit = build_bar_time_audit(request, &prepared)?;
        let trusted_actions = attach_bar_time_action_evidence(
            &mut audit,
            &decision_candidates[index],
            timeline,
            &asset,
        )?;
        let audit_value = serde_json::to_value(&audit)
            .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
        let expected_contract_hash = canonical_json_hash(&request.data_requirements.bar_time)?;
        let expected_graph_hash = canonical_json_hash(&serde_json::json!({
            "streams": request.data_requirements.bar_time.streams,
            "binding": request.strategy.stream_binding,
        }))?;
        let check = validate_bar_time_audit(
            &audit_value,
            BarTimeValidationContext {
                expected_bar_time_contract_id: &audit.bar_time_contract_id,
                expected_bar_time_contract_hash: &expected_contract_hash,
                expected_stream_graph_hash: &expected_graph_hash,
                expected_execution_stream_id: &prepared.execution_stream_id,
                expected_decision_stream_id: &prepared.decision_stream_id,
                execution_bars,
                expected_decisions: &expected_decisions,
                trusted_actions: &trusted_actions,
                rebalance_audit: &timeline.result_tables.rebalance_audit,
                rebalance_trades: &timeline.result_tables.rebalance_trades,
            },
        )
        .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
        result.result_validation.checks.push(check);
        candidate_audits.push(serde_json::json!({
            "candidate_id": result.candidate_id,
            "request_id": request.request_id,
            "audit": audit,
        }));
    }
    let request_ids = input
        .engine_requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<Vec<_>>();
    let request_count = request_ids.len();
    Ok(serde_json::json!({
        "request_count": request_count,
        "execution_mode": "grouped",
        "shape": "signal_timeline",
        "request_ids": request_ids,
        "bar_time_audits": candidate_audits,
        "derived_bar_cache": {
            "schema_version": "derived_bar_cache.v1",
            "enabled": prepared.aggregated,
            "build_count": 1,
            "candidate_count": request_count,
            "market_data_bundle_hash": market_data_bundle_hash,
            "stream_graph_hash": canonical_json_hash(&serde_json::json!({
                "streams": first_request.data_requirements.bar_time.streams,
                "binding": first_request.strategy.stream_binding,
            }))?,
        },
        "result": summary,
    }))
}

fn execute_calendar_same_session_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    validate_batch_bundle_and_requests(&input)?;
    if input.market_data_bundle.symbols.len() != 1 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "same-session calendar execution requires one symbol".to_string(),
        ));
    }
    let mut config = identical_timeline_config(&input.engine_requests)?;
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    attach_timeline_session_labels(&mut config, &input.market_data_bundle, &close_frame)?;
    let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
    let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
    let asset = input.market_data_bundle.symbols[0].clone();
    let candidates = input
        .engine_requests
        .iter()
        .map(|request| {
            calendar_same_session_candidate(request, request.strategy.strategy_id.clone())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request_ids = input
        .engine_requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<Vec<_>>();
    let summary = run_single_asset_calendar_same_session_batch(CalendarSameSessionBatchInput {
        config,
        asset: asset.clone(),
        dates,
        open: table_price_column(&open_frame, &asset)?,
        close: table_price_column(&close_frame, &asset)?,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    Ok(serde_json::json!({
        "request_count": request_ids.len(),
        "execution_mode": "grouped",
        "shape": "calendar_same_session",
        "request_ids": request_ids,
        "result": summary,
    }))
}

fn execute_calendar_overlay_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    validate_batch_bundle_and_requests(&input)?;
    let mut config = identical_timeline_config(&input.engine_requests)?;
    let first_request = input.engine_requests.first().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("engine_requests must not be empty".to_string())
    })?;
    let (baseline_weights, event_weights) = calendar_overlay_weights(first_request)?;
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    attach_timeline_session_labels(&mut config, &input.market_data_bundle, &close_frame)?;
    let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
    let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
    let open = input
        .market_data_bundle
        .symbols
        .iter()
        .map(|symbol| Ok((symbol.clone(), table_price_column(&open_frame, symbol)?)))
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let close = input
        .market_data_bundle
        .symbols
        .iter()
        .map(|symbol| Ok((symbol.clone(), table_price_column(&close_frame, symbol)?)))
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let candidates = input
        .engine_requests
        .iter()
        .map(|request| {
            let weights = calendar_overlay_weights(request)?;
            if weights != (baseline_weights.clone(), event_weights.clone()) {
                return Err(EngineRuntimeError::InvalidRequest(
                    "grouped calendar-overlay requests require identical baseline and event weights"
                        .to_string(),
                ));
            }
            calendar_overlay_candidate(request, request.strategy.strategy_id.clone())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request_ids = input
        .engine_requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<Vec<_>>();
    let summary = run_calendar_overlay_batch(CalendarOverlayBatchInput {
        config,
        assets: input.market_data_bundle.symbols,
        dates,
        open,
        close,
        baseline_weights,
        event_weights,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    Ok(serde_json::json!({
        "request_count": request_ids.len(),
        "execution_mode": "grouped",
        "shape": "calendar_overlay",
        "request_ids": request_ids,
        "result": summary,
    }))
}

fn execute_reset_timer_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    validate_batch_bundle_and_requests(&input)?;
    let mut config = identical_timeline_config(&input.engine_requests)?;
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    attach_timeline_session_labels(&mut config, &input.market_data_bundle, &close_frame)?;
    let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
    let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
    let first_request = input.engine_requests.first().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("engine_requests must not be empty".to_string())
    })?;
    let reset_spec = reset_timer_spec(first_request)?;
    let feature_values = if reset_spec.signal_field.is_empty() {
        None
    } else {
        let feature_frame = read_bundle_table(&input.market_data_bundle, &reset_spec.signal_field)?;
        let feature_dates = bundle_time_strings(&input.market_data_bundle, &feature_frame)?;
        if feature_dates != dates {
            return Err(EngineRuntimeError::MarketData(
                "reset-timer feature timestamps are not aligned with price tables".to_string(),
            ));
        }
        Some(table_feature_column(
            &feature_frame,
            &reset_spec.signal_field,
            &input.market_data_bundle.symbols,
        )?)
    };
    let open = input
        .market_data_bundle
        .symbols
        .iter()
        .map(|symbol| Ok((symbol.clone(), table_price_column(&open_frame, symbol)?)))
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let close = input
        .market_data_bundle
        .symbols
        .iter()
        .map(|symbol| Ok((symbol.clone(), table_price_column(&close_frame, symbol)?)))
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let candidates = input
        .engine_requests
        .iter()
        .map(|request| {
            let spec = reset_timer_spec(request)?;
            if spec.baseline_weights != reset_spec.baseline_weights
                || spec.event_weights != reset_spec.event_weights
                || spec.restore_weights != reset_spec.restore_weights
                || spec.entry_offset_bars != reset_spec.entry_offset_bars
                || spec.entry_phase != reset_spec.entry_phase
                || spec.restore_phase != reset_spec.restore_phase
                || spec.signal_field != reset_spec.signal_field
            {
                return Err(EngineRuntimeError::InvalidRequest(
                    "grouped reset-timer requests require identical action weights and phases"
                        .to_string(),
                ));
            }
            reset_timer_candidate(
                request,
                feature_values.as_deref(),
                &dates,
                request.strategy.strategy_id.clone(),
                spec.hold_bars,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request_ids = input
        .engine_requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<Vec<_>>();
    let summary = crate::run_reset_timer_batch(ResetTimerBatchInput {
        config,
        assets: input.market_data_bundle.symbols,
        dates,
        open,
        close,
        baseline_weights: reset_spec.baseline_weights,
        event_weights: reset_spec.event_weights,
        restore_weights: reset_spec.restore_weights,
        entry_offset_bars: reset_spec.entry_offset_bars,
        entry_phase: reset_spec.entry_phase,
        restore_phase: reset_spec.restore_phase,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    Ok(serde_json::json!({
        "request_count": request_ids.len(),
        "execution_mode": "grouped",
        "shape": "reset_timer",
        "request_ids": request_ids,
        "result": summary,
    }))
}

fn execute_reset_timer(input: EngineRequestExecutionInput) -> Result<Value, EngineRuntimeError> {
    let grouped = execute_reset_timer_request_batch(EngineRequestBatchExecutionInput {
        engine_requests: vec![input.engine_request],
        market_data_bundle: input.market_data_bundle,
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
    })?;
    grouped
        .get("result")
        .cloned()
        .ok_or_else(|| EngineRuntimeError::Accounting("reset-timer result is missing".to_string()))
}

fn validate_batch_bundle_and_requests(
    input: &EngineRequestBatchExecutionInput,
) -> Result<(), EngineRuntimeError> {
    input
        .market_data_bundle
        .validate()
        .map_err(|error| EngineRuntimeError::InvalidBundle(error.to_string()))?;
    for request in &input.engine_requests {
        request
            .validate()
            .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
        if request.data_requirements.symbols != input.market_data_bundle.symbols {
            return Err(EngineRuntimeError::InvalidBundle(
                "bundle symbols do not match EngineRequest batch".to_string(),
            ));
        }
        let _prepared_runtime_streams =
            prepare_runtime_streams(request, &input.market_data_bundle)?;
    }
    Ok(())
}

fn identical_timeline_config(
    requests: &[EngineRequestV2],
) -> Result<TimelineAccountingConfig, EngineRuntimeError> {
    let first = requests.first().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("engine_requests must not be empty".to_string())
    })?;
    let config = timeline_accounting_config(first)?;
    let expected = serde_json::to_value(&config)
        .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
    for request in &requests[1..] {
        let current = serde_json::to_value(timeline_accounting_config(request)?)
            .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
        if current != expected {
            return Err(EngineRuntimeError::InvalidRequest(
                "grouped EngineRequest batch requires identical account, cost, risk and position policy settings"
                    .to_string(),
            ));
        }
    }
    Ok(config)
}

fn is_single_asset_signal_request(request: &EngineRequestV2) -> bool {
    let decision = &request.strategy.decision_plan;
    decision.allocation.get("method").and_then(Value::as_str) == Some("position_state")
        && decision.signals.get("entry").is_some_and(Value::is_object)
        && decision.signals.get("exit").is_some_and(Value::is_object)
}

fn is_calendar_same_session_request(request: &EngineRequestV2) -> bool {
    request
        .strategy
        .decision_plan
        .required_operations
        .contains(&OperationId::SessionSameSessionClose)
}

fn is_calendar_overlay_request(request: &EngineRequestV2) -> bool {
    calendar_entry(&request.strategy.decision_plan).is_some() && has_event_weight_actions(request)
}

fn is_reset_timer_request(request: &EngineRequestV2) -> bool {
    request
        .simulation
        .fill_model
        .get("position_policy")
        .and_then(|value| value.get("on_entry_signal_while_holding"))
        .and_then(Value::as_str)
        == Some("reset_timer")
}

fn reset_timer_spec(request: &EngineRequestV2) -> Result<ResetTimerSpec, EngineRuntimeError> {
    let entry = request
        .strategy
        .decision_plan
        .signals
        .get("entry")
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile(
                "reset-timer entry signal is required".to_string(),
            )
        })?;
    let signal_field = entry
        .get("field")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_default();
    if signal_field.is_empty()
        && entry.get("op").and_then(Value::as_str) != Some("calendar.session_offset_from_month_end")
    {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "reset-timer entry signal requires a feature field or supported calendar signal"
                .to_string(),
        ));
    }
    let actions = request
        .simulation
        .fill_model
        .get("actions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile("reset-timer actions are required".to_string())
        })?;
    let baseline = actions
        .iter()
        .find(|action| action.get("signal").and_then(Value::as_str) == Some("rebalance"))
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile(
                "reset-timer baseline action is required".to_string(),
            )
        })?;
    let entry_actions = actions
        .iter()
        .filter(|action| action.get("signal").and_then(Value::as_str) == Some("entry"))
        .collect::<Vec<_>>();
    if entry_actions.len() != 2 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "reset-timer requires one event action and one restore action".to_string(),
        ));
    }
    let event = entry_actions[0];
    let restore = entry_actions[1];
    Ok(ResetTimerSpec {
        signal_field,
        baseline_weights: required_action_weights(baseline)?,
        event_weights: required_action_weights(event)?,
        restore_weights: required_action_weights(restore)?,
        entry_offset_bars: resolved_i64(event.get("offset_bars"), request)? as usize,
        entry_phase: event
            .get("price")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(
                    "reset-timer event action requires price".to_string(),
                )
            })?
            .to_string(),
        restore_phase: restore
            .get("price")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(
                    "reset-timer restore action requires price".to_string(),
                )
            })?
            .to_string(),
        hold_bars: resolved_i64(restore.get("offset_bars"), request)? as usize,
    })
}

fn required_action_weights(action: &Value) -> Result<BTreeMap<String, f64>, EngineRuntimeError> {
    action
        .get("weights")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                "timeline action weights are required".to_string(),
            )
        })?
        .iter()
        .map(|(symbol, value)| {
            value
                .as_f64()
                .map(|weight| (symbol.clone(), weight))
                .ok_or_else(|| {
                    EngineRuntimeError::InvalidAllocation(format!(
                        "timeline action weight for {symbol} must be numeric"
                    ))
                })
        })
        .collect()
}

fn reset_timer_candidate(
    request: &EngineRequestV2,
    feature_values: Option<&[f64]>,
    dates: &[String],
    candidate_id: String,
    hold_bars: usize,
) -> Result<ResetTimerCandidateInput, EngineRuntimeError> {
    let entry = request
        .strategy
        .decision_plan
        .signals
        .get("entry")
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile(
                "reset-timer entry signal is required".to_string(),
            )
        })?;
    let entry_signal = if let Some(feature_values) = feature_values {
        let field = entry
            .get("field")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(
                    "reset-timer entry signal requires field".to_string(),
                )
            })?
            .to_lowercase();
        let mut rule = condition_from_value(entry);
        rule.value = Some(resolved_f64(
            entry.get("value").or_else(|| entry.get("right")),
            request,
        )?);
        let fields = BTreeMap::from([(field, feature_values.to_vec())]);
        evaluate_condition(&rule, &fields, feature_values.len(), 1)
            .map_err(|error| EngineRuntimeError::UnsupportedProfile(error.to_string()))?
    } else {
        calendar_session_offset_signal(entry, request, dates)?
    };
    Ok(ResetTimerCandidateInput {
        candidate_id,
        resolved_params: resolved_params(request),
        entry_signal,
        hold_bars,
    })
}

fn calendar_session_offset_signal(
    entry: &Value,
    request: &EngineRequestV2,
    dates: &[String],
) -> Result<Vec<bool>, EngineRuntimeError> {
    if entry.get("op").and_then(Value::as_str) != Some("calendar.session_offset_from_month_end") {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "unsupported reset-timer calendar signal".to_string(),
        ));
    }
    let offset = resolved_i64(
        entry.get("offset_sessions").or_else(|| entry.get("offset")),
        request,
    )?;
    if offset > 0 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "calendar session offset must be zero or negative".to_string(),
        ));
    }
    let mut signal = vec![false; dates.len()];
    let mut start = 0usize;
    while start < dates.len() {
        let month = dates[start].get(..7).unwrap_or(dates[start].as_str());
        let mut end = start + 1;
        while end < dates.len() && dates[end].get(..7).unwrap_or(dates[end].as_str()) == month {
            end += 1;
        }
        let target = end as i64 - 1 + offset;
        if target >= start as i64 && target < end as i64 {
            signal[target as usize] = true;
        }
        start = end;
    }
    Ok(signal)
}

fn execute_daily_rank_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    input
        .market_data_bundle
        .validate()
        .map_err(|error| EngineRuntimeError::InvalidBundle(error.to_string()))?;
    let first_request = input.engine_requests.first().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("engine_requests must not be empty".to_string())
    })?;
    let expected_config = serde_json::to_value(accounting_config(first_request)?)
        .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
    for request in &input.engine_requests {
        request
            .validate()
            .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
        if request.data_requirements.symbols != input.market_data_bundle.symbols {
            return Err(EngineRuntimeError::InvalidBundle(
                "bundle symbols do not match EngineRequest batch".to_string(),
            ));
        }
        let config = serde_json::to_value(accounting_config(request)?)
            .map_err(|error| EngineRuntimeError::InvalidRequest(error.to_string()))?;
        if config != expected_config {
            return Err(EngineRuntimeError::InvalidRequest(
                "grouped EngineRequest batch requires identical account, cost and risk settings"
                    .to_string(),
            ));
        }
        validate_daily_rank_trigger(request)?;
    }
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    let requires_open = input
        .engine_requests
        .iter()
        .any(daily_rank_executes_next_open);
    let (dates, open) = if requires_open {
        let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
        let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
        let prices = close_prices(&open_frame, &input.market_data_bundle.symbols)?;
        let flattened = (0..open_frame.height())
            .flat_map(|row| prices.iter().map(move |column| column[row]))
            .collect::<Vec<_>>();
        (dates, flattened)
    } else {
        (
            bundle_time_strings(&input.market_data_bundle, &close_frame)?,
            Vec::new(),
        )
    };
    let prices = close_prices(&close_frame, &input.market_data_bundle.symbols)?;
    let close = (0..close_frame.height())
        .flat_map(|row| prices.iter().map(move |column| column[row]))
        .collect::<Vec<_>>();
    let market_fields = feature_market_fields(
        &input.market_data_bundle,
        &close_frame,
        required_market_field_names(input.engine_requests.iter()),
    )?;
    let request_ids = input
        .engine_requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<Vec<_>>();
    let candidates = input
        .engine_requests
        .iter()
        .map(|request| daily_rank_candidate(request, request.strategy.strategy_id.clone(), &dates))
        .collect::<Result<Vec<_>, _>>()?;
    let mut config = accounting_config(first_request)?;
    attach_accounting_session_labels(&mut config, &input.market_data_bundle, &close_frame)?;
    let summary = run_daily_rank_accounting_batch(DailyRankBatchInput {
        config,
        dates,
        symbols: input.market_data_bundle.symbols,
        close,
        open,
        market_fields,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    Ok(serde_json::json!({
        "request_count": request_ids.len(),
        "execution_mode": "grouped",
        "shape": "daily_rank",
        "request_ids": request_ids,
        "result": summary,
    }))
}

fn execute_single_asset_signal(
    input: EngineRequestExecutionInput,
    prepared: &PreparedRuntimeStreams,
) -> Result<Value, EngineRuntimeError> {
    let request = &input.engine_request;
    let bundle = &input.market_data_bundle;
    if bundle.symbols.len() != 1 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "cross signal execution requires one symbol".to_string(),
        ));
    }
    validate_next_open_signal_actions(request)?;
    let asset = bundle.symbols[0].clone();
    let execution_bars = prepared
        .execution_bars_by_symbol
        .get(&asset)
        .ok_or_else(|| {
            EngineRuntimeError::MarketData("prepared execution stream is missing asset".to_string())
        })?;
    let decision_bars = prepared
        .decision_bars_by_symbol
        .get(&asset)
        .ok_or_else(|| {
            EngineRuntimeError::MarketData("prepared decision stream is missing asset".to_string())
        })?;
    let decision_close = decision_bars
        .iter()
        .map(|bar| bar.close)
        .collect::<Vec<_>>();
    let decision_market_fields = BTreeMap::from([
        (
            "open".to_string(),
            decision_bars.iter().map(|bar| bar.open).collect(),
        ),
        (
            "high".to_string(),
            decision_bars.iter().map(|bar| bar.high).collect(),
        ),
        (
            "low".to_string(),
            decision_bars.iter().map(|bar| bar.low).collect(),
        ),
        (
            "volume".to_string(),
            decision_bars.iter().map(|bar| bar.volume).collect(),
        ),
    ]);
    let mut decision_candidate = single_signal_candidate(
        request,
        &decision_close,
        &decision_market_fields,
        request.strategy.strategy_id.clone(),
    )?;
    mask_signal_candidate_to_workflow_window(request, decision_bars, &mut decision_candidate)?;
    let candidate = remap_signal_candidate_to_execution(
        decision_candidate.clone(),
        decision_bars,
        execution_bars.len(),
    )?;
    let mut config = timeline_accounting_config(request)?;
    config.session_label_by_event_time = execution_bars
        .iter()
        .map(|bar| (bar.event_timestamp.clone(), bar.session_label.clone()))
        .collect();
    let mut summary = crate::run_single_asset_next_open_signal_batch(SingleAssetSignalBatchInput {
        config,
        asset: asset.clone(),
        dates: execution_bars
            .iter()
            .map(|bar| bar.event_timestamp.clone())
            .collect(),
        open: execution_bars.iter().map(|bar| bar.open).collect(),
        close: execution_bars.iter().map(|bar| bar.close).collect(),
        include_full_results: true,
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates: vec![candidate],
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    let mut bar_time_audit = build_bar_time_audit(request, prepared)?;
    let expected_decisions = expected_bar_time_decisions(prepared, &decision_candidate)?;
    for (result_index, result) in summary.results.iter_mut().enumerate() {
        if result.result_validation.status != "valid" || !result.result_validation.errors.is_empty()
        {
            return Err(EngineRuntimeError::Accounting(
                "cannot attach a passed bar-time check to a failed result validation".to_string(),
            ));
        }
        let timeline = summary.trusted_timelines.get(result_index).ok_or_else(|| {
            EngineRuntimeError::Accounting(
                "mandatory bar-time validation requires trusted timeline evidence".to_string(),
            )
        })?;
        let trusted_actions = attach_bar_time_action_evidence(
            &mut bar_time_audit,
            &decision_candidate,
            timeline,
            &asset,
        )?;
        let audit_value = serde_json::to_value(&bar_time_audit)
            .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
        let check = validate_bar_time_audit(
            &audit_value,
            BarTimeValidationContext {
                expected_bar_time_contract_id: &bar_time_audit.bar_time_contract_id,
                expected_bar_time_contract_hash: &canonical_json_hash(
                    &request.data_requirements.bar_time,
                )?,
                expected_stream_graph_hash: &canonical_json_hash(&serde_json::json!({
                    "streams": request.data_requirements.bar_time.streams,
                    "binding": request.strategy.stream_binding,
                }))?,
                expected_execution_stream_id: &prepared.execution_stream_id,
                expected_decision_stream_id: &prepared.decision_stream_id,
                execution_bars,
                expected_decisions: &expected_decisions,
                trusted_actions: &trusted_actions,
                rebalance_audit: &timeline.result_tables.rebalance_audit,
                rebalance_trades: &timeline.result_tables.rebalance_trades,
            },
        )
        .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
        result.result_validation.checks.push(check);
    }
    let mut result = serde_json::to_value(summary)
        .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    let fields = result.as_object_mut().ok_or_else(|| {
        EngineRuntimeError::Accounting(
            "single-asset signal result must serialize as an object".to_string(),
        )
    })?;
    fields.insert(
        "bar_time_audit".to_string(),
        serde_json::to_value(bar_time_audit)
            .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?,
    );
    Ok(result)
}

fn execute_calendar_same_session(
    input: EngineRequestExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    let request = &input.engine_request;
    let bundle = &input.market_data_bundle;
    if bundle.symbols.len() != 1 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "same-session calendar execution requires one symbol".to_string(),
        ));
    }
    let close_frame = read_bundle_table(bundle, "close")?;
    let open_frame = read_bundle_table(bundle, "open")?;
    let dates = aligned_dates(bundle, &close_frame, &open_frame)?;
    let asset = bundle.symbols[0].clone();
    let candidate = calendar_same_session_candidate(request, request.strategy.strategy_id.clone())?;
    let mut config = timeline_accounting_config(request)?;
    attach_timeline_session_labels(&mut config, bundle, &close_frame)?;
    let summary = run_single_asset_calendar_same_session_batch(CalendarSameSessionBatchInput {
        config,
        asset: asset.clone(),
        dates,
        open: table_price_column(&open_frame, &asset)?,
        close: table_price_column(&close_frame, &asset)?,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates: vec![candidate],
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    serde_json::to_value(summary).map_err(|error| EngineRuntimeError::Accounting(error.to_string()))
}

fn execute_calendar_overlay(
    input: EngineRequestExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    let request = &input.engine_request;
    let bundle = &input.market_data_bundle;
    let mut config = timeline_accounting_config(request)?;
    let (baseline_weights, event_weights) = calendar_overlay_weights(request)?;
    let close_frame = read_bundle_table(bundle, "close")?;
    attach_timeline_session_labels(&mut config, bundle, &close_frame)?;
    let open_frame = read_bundle_table(bundle, "open")?;
    let dates = aligned_dates(bundle, &close_frame, &open_frame)?;
    let open = bundle
        .symbols
        .iter()
        .map(|symbol| Ok((symbol.clone(), table_price_column(&open_frame, symbol)?)))
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let close = bundle
        .symbols
        .iter()
        .map(|symbol| Ok((symbol.clone(), table_price_column(&close_frame, symbol)?)))
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let candidate = calendar_overlay_candidate(request, request.strategy.strategy_id.clone())?;
    let summary = run_calendar_overlay_batch(CalendarOverlayBatchInput {
        config,
        assets: bundle.symbols.clone(),
        dates,
        open,
        close,
        baseline_weights,
        event_weights,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates: vec![candidate],
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    serde_json::to_value(summary).map_err(|error| EngineRuntimeError::Accounting(error.to_string()))
}

fn execute_fixed_allocation(
    input: EngineRequestExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    let request = &input.engine_request;
    let bundle = &input.market_data_bundle;
    let weights = fixed_weights(&request.strategy.decision_plan.allocation, &bundle.symbols)?;
    let trigger = request
        .strategy
        .decision_plan
        .rebalance
        .get("trigger")
        .and_then(Value::as_object)
        .and_then(|value| value.get("op"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation("rebalance.trigger.op is required".to_string())
        })?;
    let close_table = bundle
        .tables
        .get("close")
        .ok_or_else(|| EngineRuntimeError::InvalidBundle("close table is missing".to_string()))?;
    let close_path = close_table.path.as_deref().ok_or_else(|| {
        EngineRuntimeError::InvalidBundle("close table requires parquet path".to_string())
    })?;
    let frame = read_parquet(close_path)?;
    if frame.height() != close_table.row_count || frame.height() != bundle.row_count {
        return Err(EngineRuntimeError::MarketData(
            "close parquet row_count does not match bundle manifest".to_string(),
        ));
    }
    let dates = bundle_time_strings(bundle, &frame)?;
    let prices = close_prices(&frame, &bundle.symbols)?;
    let rebalance = rebalance_flags(&dates, trigger)?;
    let selected_assets = weights
        .iter()
        .filter(|(_, weight)| weight.abs() > 1e-12)
        .map(|(symbol, _)| symbol.clone())
        .collect::<Vec<_>>();
    let mut checkpoints = Vec::with_capacity(frame.height());
    for row in 0..frame.height() {
        let returns = bundle
            .symbols
            .iter()
            .enumerate()
            .map(|(column, symbol)| {
                let value = if row == 0 {
                    0.0
                } else {
                    simple_return(prices[column][row], prices[column][row - 1])
                };
                (symbol.clone(), value)
            })
            .collect::<BTreeMap<_, _>>();
        let is_rebalance = rebalance[row];
        checkpoints.push(CheckpointInput {
            time: dates[row].clone(),
            rebalance: is_rebalance,
            returns,
            target_weights: if is_rebalance {
                weights.clone()
            } else {
                BTreeMap::new()
            },
            selected_assets: if is_rebalance {
                selected_assets.clone()
            } else {
                Vec::new()
            },
            ranked_assets: if is_rebalance {
                selected_assets.clone()
            } else {
                Vec::new()
            },
            score: if is_rebalance {
                weights.clone()
            } else {
                BTreeMap::new()
            },
            eligible: if is_rebalance {
                selected_assets
                    .iter()
                    .map(|asset| (asset.clone(), true))
                    .collect()
            } else {
                BTreeMap::new()
            },
            rank_by: is_rebalance.then(|| "fixed_weight".to_string()),
        });
    }
    let mut config = accounting_config(request)?;
    attach_accounting_session_labels(&mut config, bundle, &frame)?;
    let summary = run_accounting(AccountingInput {
        config,
        checkpoints,
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    serde_json::to_value(summary).map_err(|error| EngineRuntimeError::Accounting(error.to_string()))
}

fn execute_daily_rank(input: EngineRequestExecutionInput) -> Result<Value, EngineRuntimeError> {
    let request = &input.engine_request;
    let bundle = &input.market_data_bundle;
    validate_daily_rank_trigger(request)?;
    let close_table = bundle
        .tables
        .get("close")
        .ok_or_else(|| EngineRuntimeError::InvalidBundle("close table is missing".to_string()))?;
    let close_path = close_table.path.as_deref().ok_or_else(|| {
        EngineRuntimeError::InvalidBundle("close table requires parquet path".to_string())
    })?;
    let frame = read_parquet(close_path)?;
    let (dates, open) = if daily_rank_executes_next_open(request) {
        let open_frame = read_bundle_table(bundle, "open")?;
        let dates = aligned_dates(bundle, &frame, &open_frame)?;
        let prices = close_prices(&open_frame, &bundle.symbols)?;
        let flattened = (0..open_frame.height())
            .flat_map(|row| prices.iter().map(move |column| column[row]))
            .collect::<Vec<_>>();
        (dates, flattened)
    } else {
        (bundle_time_strings(bundle, &frame)?, Vec::new())
    };
    let prices = close_prices(&frame, &bundle.symbols)?;
    let close = (0..frame.height())
        .flat_map(|row| prices.iter().map(move |column| column[row]))
        .collect::<Vec<_>>();
    let market_fields = feature_market_fields(
        bundle,
        &frame,
        required_market_field_names(std::iter::once(request)),
    )?;
    let candidate = daily_rank_candidate(request, request.strategy.strategy_id.clone(), &dates)?;
    let mut config = accounting_config(request)?;
    attach_accounting_session_labels(&mut config, bundle, &frame)?;
    let summary = run_daily_rank_accounting_batch(DailyRankBatchInput {
        config,
        dates,
        symbols: bundle.symbols.clone(),
        close,
        open,
        market_fields,
        include_full_results: true,
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates: vec![candidate],
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    serde_json::to_value(summary).map_err(|error| EngineRuntimeError::Accounting(error.to_string()))
}

fn is_daily_rank_request(request: &EngineRequestV2) -> bool {
    matches!(
        request
            .strategy
            .decision_plan
            .allocation
            .get("method")
            .and_then(Value::as_str),
        Some("equal_weight" | "equal_weight_long_short")
    )
}

fn daily_rank_executes_next_open(request: &EngineRequestV2) -> bool {
    request
        .simulation
        .fill_model
        .get("timing")
        .and_then(Value::as_str)
        == Some("signal_close_for_next_bar")
        && request
            .simulation
            .fill_model
            .get("price")
            .and_then(Value::as_str)
            == Some("next_open")
}

fn validate_daily_rank_trigger(request: &EngineRequestV2) -> Result<(), EngineRuntimeError> {
    let trigger = request
        .strategy
        .decision_plan
        .rebalance
        .get("trigger")
        .and_then(|value| value.get("op"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation("rebalance.trigger.op is required".to_string())
        })?;
    match trigger {
        "calendar.every_session"
        | "every_session"
        | "calendar.first_session"
        | "calendar.month_start"
        | "calendar.month_end"
        | "calendar.year_start"
        | "calendar.year_end"
        | "target.change" => Ok(()),
        _ => Err(EngineRuntimeError::UnsupportedProfile(format!(
            "daily-rank rebalance trigger {trigger} is not implemented"
        ))),
    }
}

fn daily_rank_candidate(
    request: &EngineRequestV2,
    candidate_id: String,
    dates: &[String],
) -> Result<DailyRankBatchCandidateInput, EngineRuntimeError> {
    let decision = &request.strategy.decision_plan;
    let selection = &decision.selection;
    let allocation = &decision.allocation;
    let allocation_method = allocation
        .get("method")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation("allocation.method is required".to_string())
        })?;
    let top_n = selection
        .get(if allocation_method == "equal_weight_long_short" {
            "long_top_n"
        } else {
            "top_n"
        })
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                if allocation_method == "equal_weight_long_short" {
                    "selection.long_top_n is required".to_string()
                } else {
                    "selection.top_n is required".to_string()
                },
            )
        })? as usize;
    let short_bottom_n = allocation_method
        .eq("equal_weight_long_short")
        .then(|| {
            selection
                .get("short_bottom_n")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    EngineRuntimeError::InvalidAllocation(
                        "selection.short_bottom_n is required".to_string(),
                    )
                })
        })
        .transpose()?
        .unwrap_or(0) as usize;
    let long_gross_exposure = allocation
        .get("long_gross_exposure")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                "allocation.long_gross_exposure is required".to_string(),
            )
        })?;
    let short_gross_exposure = allocation
        .get("short_gross_exposure")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                "allocation.short_gross_exposure is required".to_string(),
            )
        })?;
    let position_limit = allocation
        .get("position_limit")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                "allocation.position_limit is required".to_string(),
            )
        })?;
    let rank_by = selection
        .get("rank_by")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation("selection.rank_by is required".to_string())
        })?
        .to_string();
    let trigger = decision
        .rebalance
        .get("trigger")
        .and_then(|value| value.get("op"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation("rebalance.trigger.op is required".to_string())
        })?;
    Ok(DailyRankBatchCandidateInput {
        candidate_id,
        resolved_params: resolved_params(request),
        eligible: Vec::new(),
        score: Vec::new(),
        rebalance: if trigger == "target.change" {
            Vec::new()
        } else {
            rebalance_flags(dates, trigger)?
        },
        ascending: selection
            .get("rank_order")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("asc")),
        top_n,
        short_bottom_n,
        long_gross_exposure,
        short_gross_exposure,
        position_limit,
        feature_specs: feature_specs(request)?,
        eligible_rule: selection.get("eligible").map(condition_from_value),
        rank_by: Some(rank_by),
        target_change: trigger == "target.change",
        execute_next_open: daily_rank_executes_next_open(request),
    })
}

fn single_signal_candidate(
    request: &EngineRequestV2,
    close: &[f64],
    market_fields: &BTreeMap<String, Vec<f64>>,
    candidate_id: String,
) -> Result<SingleAssetSignalCandidateInput, EngineRuntimeError> {
    validate_next_open_signal_actions(request)?;
    let specs = feature_specs(request)?;
    let fields =
        compute_feature_fields_with_market_fields(close, market_fields, close.len(), 1, &specs)
            .map_err(|error| EngineRuntimeError::UnsupportedProfile(error.to_string()))?;
    let signals = &request.strategy.decision_plan.signals;
    let entry_rule = condition_from_value(signals.get("entry").ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("entry signal is required".to_string())
    })?);
    let exit_rule = condition_from_value(signals.get("exit").ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("exit signal is required".to_string())
    })?);
    let entry_signal = evaluate_condition(&entry_rule, &fields, close.len(), 1)
        .map_err(|error| EngineRuntimeError::UnsupportedProfile(error.to_string()))?;
    let exit_signal = evaluate_condition(&exit_rule, &fields, close.len(), 1)
        .map_err(|error| EngineRuntimeError::UnsupportedProfile(error.to_string()))?;
    let target_weight = signals
        .get("target_weight")
        .and_then(Value::as_f64)
        .or_else(|| {
            request
                .strategy
                .decision_plan
                .allocation
                .get("target_weight")
                .and_then(Value::as_f64)
        })
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                "single-asset signal target_weight is required".to_string(),
            )
        })?;
    Ok(SingleAssetSignalCandidateInput {
        candidate_id,
        resolved_params: resolved_params(request),
        entry_signal,
        exit_signal,
        target_weight,
    })
}

fn mask_signal_candidate_to_workflow_window(
    request: &EngineRequestV2,
    decision_bars: &[PreparedDecisionBar],
    candidate: &mut SingleAssetSignalCandidateInput,
) -> Result<(), EngineRuntimeError> {
    let Some(window) = request.workflow.window.as_ref() else {
        return Ok(());
    };
    if window.start.len() != 10
        || window.end.len() != 10
        || window.start > window.end
        || candidate.entry_signal.len() != decision_bars.len()
        || candidate.exit_signal.len() != decision_bars.len()
    {
        return Err(EngineRuntimeError::InvalidRequest(
            "validation workflow window requires ordered canonical session labels".to_string(),
        ));
    }
    for (index, bar) in decision_bars.iter().enumerate() {
        let in_window = bar.session_label.as_str() >= window.start.as_str()
            && bar.session_label.as_str() <= window.end.as_str();
        if !in_window {
            candidate.entry_signal[index] = false;
            candidate.exit_signal[index] = false;
        }
    }
    Ok(())
}

fn remap_signal_candidate_to_execution(
    mut candidate: SingleAssetSignalCandidateInput,
    decision_bars: &[PreparedDecisionBar],
    execution_count: usize,
) -> Result<SingleAssetSignalCandidateInput, EngineRuntimeError> {
    if candidate.entry_signal.len() != decision_bars.len()
        || candidate.exit_signal.len() != decision_bars.len()
    {
        return Err(EngineRuntimeError::InvalidRequest(
            "decision signals do not match prepared decision bars".to_string(),
        ));
    }
    let mut entry_signal = vec![false; execution_count];
    let mut exit_signal = vec![false; execution_count];
    for (decision_index, decision) in decision_bars.iter().enumerate() {
        let Some(execution_index) = decision.next_execution_index else {
            continue;
        };
        if execution_index == 0 || execution_index >= execution_count {
            return Err(EngineRuntimeError::MarketData(
                "strict next execution index cannot be represented by next-open kernel".to_string(),
            ));
        }
        let kernel_signal_index = execution_index - 1;
        entry_signal[kernel_signal_index] |= candidate.entry_signal[decision_index];
        exit_signal[kernel_signal_index] |= candidate.exit_signal[decision_index];
    }
    candidate.entry_signal = entry_signal;
    candidate.exit_signal = exit_signal;
    Ok(candidate)
}

fn attach_bar_time_action_evidence(
    audit: &mut BarTimeAudit,
    decision_candidate: &SingleAssetSignalCandidateInput,
    timeline: &crate::TimelineAccountingSummary,
    asset: &str,
) -> Result<Vec<BarTimeTrustedActionEvidence>, EngineRuntimeError> {
    let mut trusted_actions = Vec::new();
    for event in &timeline.events {
        for (action_index, action) in event.actions.iter().enumerate() {
            let action_id = format!("{}:{}:{action_index}", event.date, event.phase);
            let fill_ids = action
                .orders
                .iter()
                .filter(|order| order.asset == asset && order.filled_delta.abs() > 1e-12)
                .map(|order| order.order_id.clone())
                .collect::<Vec<_>>();
            trusted_actions.push(BarTimeTrustedActionEvidence {
                action_id,
                action: action.action.clone(),
                status: if fill_ids.is_empty() {
                    "no_op".to_string()
                } else {
                    "filled".to_string()
                },
                reason: if fill_ids.is_empty() {
                    action
                        .skip_reason
                        .clone()
                        .or_else(|| Some("no_fill_generated".to_string()))
                } else {
                    None
                },
                execution_bar_close_time: event.date.clone(),
                asset: asset.to_string(),
                fill_ids,
            });
        }
    }
    for mapping in &mut audit.mappings {
        let entry = *decision_candidate
            .entry_signal
            .get(mapping.decision_index)
            .ok_or_else(|| {
                EngineRuntimeError::InvalidRequest(
                    "bar-time audit decision index is missing from entry signals".to_string(),
                )
            })?;
        let exit = *decision_candidate
            .exit_signal
            .get(mapping.decision_index)
            .ok_or_else(|| {
                EngineRuntimeError::InvalidRequest(
                    "bar-time audit decision index is missing from exit signals".to_string(),
                )
            })?;
        if entry && exit {
            return Err(EngineRuntimeError::InvalidRequest(
                "simultaneous entry and exit signals cannot produce one canonical bar-time audit row"
                    .to_string(),
            ));
        }
        let expected_action = if entry {
            Some("enter")
        } else if exit {
            Some("exit")
        } else {
            None
        };
        let Some(expected_action) = expected_action else {
            continue;
        };
        let matches = trusted_actions
            .iter()
            .filter(|evidence| {
                evidence.execution_bar_close_time == mapping.execution_bar_close_time
                    && evidence.action == expected_action
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(EngineRuntimeError::Accounting(format!(
                "decision {} signal must reconcile to exactly one filled action, found {}",
                mapping.decision_id,
                matches.len()
            )));
        }
        let trusted = matches[0];
        if trusted.status == "filled" && trusted.fill_ids.len() != 1 {
            return Err(EngineRuntimeError::Accounting(format!(
                "decision {} action must reconcile to exactly one fill, found {}",
                mapping.decision_id,
                trusted.fill_ids.len()
            )));
        }
        if trusted.status == "no_op"
            && (!trusted.fill_ids.is_empty() || trusted.reason.as_deref().unwrap_or("").is_empty())
        {
            return Err(EngineRuntimeError::Accounting(format!(
                "decision {} no-op action evidence is incomplete",
                mapping.decision_id
            )));
        }
        mapping.signal_action = Some(expected_action.to_string());
        mapping.action_id = Some(trusted.action_id.clone());
        mapping.action_status = Some(trusted.status.clone());
        mapping.action_reason = trusted.reason.clone();
        mapping.eligible_fill_id = trusted.fill_ids.first().cloned();
        refresh_audit_mapping_lifecycle(mapping);
    }
    for terminal in &mut audit.terminal_decisions {
        let entry = *decision_candidate
            .entry_signal
            .get(terminal.decision_index)
            .ok_or_else(|| {
                EngineRuntimeError::InvalidRequest(
                    "terminal audit decision index is missing from entry signals".to_string(),
                )
            })?;
        let exit = *decision_candidate
            .exit_signal
            .get(terminal.decision_index)
            .ok_or_else(|| {
                EngineRuntimeError::InvalidRequest(
                    "terminal audit decision index is missing from exit signals".to_string(),
                )
            })?;
        terminal.signal_action =
            match (entry, exit) {
                (true, false) => Some("enter".to_string()),
                (false, true) => Some("exit".to_string()),
                (false, false) => None,
                (true, true) => return Err(EngineRuntimeError::InvalidRequest(
                    "simultaneous entry and exit signals cannot produce terminal audit evidence"
                        .to_string(),
                )),
            };
        if terminal.signal_action.is_some() {
            terminal.status = "skipped".to_string();
            terminal.reason = Some("no_eligible_next_execution_bar".to_string());
        } else {
            terminal.status = "no_signal".to_string();
            terminal.reason = None;
        }
    }
    Ok(trusted_actions)
}

fn calendar_same_session_candidate(
    request: &EngineRequestV2,
    candidate_id: String,
) -> Result<CalendarSameSessionCandidateInput, EngineRuntimeError> {
    let entry = calendar_entry(&request.strategy.decision_plan).ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("calendar entry signal is required".to_string())
    })?;
    Ok(CalendarSameSessionCandidateInput {
        candidate_id,
        resolved_params: resolved_params(request),
        ordinal: resolved_i64(entry.get("ordinal"), request)? as i32,
        weekday: resolved_string(entry.get("weekday"), request)?,
        months: entry
            .get("months")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_u64)
                    .map(|value| value as u32)
                    .collect()
            })
            .ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(
                    "calendar entry requires explicit months".to_string(),
                )
            })?,
        target_weight: request
            .strategy
            .decision_plan
            .signals
            .get("target_weight")
            .and_then(Value::as_f64)
            .or_else(|| {
                request
                    .strategy
                    .decision_plan
                    .allocation
                    .get("target_weight")
                    .and_then(Value::as_f64)
            })
            .ok_or_else(|| {
                EngineRuntimeError::InvalidAllocation(
                    "calendar signal target_weight is required".to_string(),
                )
            })?,
    })
}

fn calendar_overlay_candidate(
    request: &EngineRequestV2,
    candidate_id: String,
) -> Result<CalendarSameSessionCandidateInput, EngineRuntimeError> {
    let entry = calendar_entry(&request.strategy.decision_plan).ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("calendar entry signal is required".to_string())
    })?;
    Ok(CalendarSameSessionCandidateInput {
        candidate_id,
        resolved_params: resolved_params(request),
        ordinal: resolved_i64(entry.get("ordinal"), request)? as i32,
        weekday: resolved_string(entry.get("weekday"), request)?,
        months: entry
            .get("months")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_u64)
                    .map(|value| value as u32)
                    .collect()
            })
            .ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(
                    "calendar entry requires explicit months".to_string(),
                )
            })?,
        target_weight: 1.0,
    })
}

fn calendar_entry(decision: &DecisionPlanV1) -> Option<&Value> {
    decision.signals.get("entry").filter(|entry| {
        entry
            .get("op")
            .and_then(Value::as_str)
            .is_some_and(|op| op.starts_with("calendar."))
    })
}

fn feature_specs(
    request: &EngineRequestV2,
) -> Result<Vec<DailyRankFeatureSpec>, EngineRuntimeError> {
    request
        .strategy
        .decision_plan
        .computed_fields
        .iter()
        .map(|value| {
            let name = value.get("name").and_then(Value::as_str).ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(
                    "computed field name is required".to_string(),
                )
            })?;
            let op = value.get("op").and_then(Value::as_str).ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile("computed field op is required".to_string())
            })?;
            let resolved_usize = |field: &str| -> Result<Option<usize>, EngineRuntimeError> {
                value
                    .get(field)
                    .map(|raw| {
                        resolved_value(Some(raw), request)?
                            .as_u64()
                            .filter(|number| *number > 0)
                            .map(|number| number as usize)
                            .ok_or_else(|| {
                                EngineRuntimeError::UnsupportedProfile(format!(
                                    "computed field {field} must be a positive integer"
                                ))
                            })
                    })
                    .transpose()
            };
            let resolved_nonnegative_usize =
                |field: &str| -> Result<Option<usize>, EngineRuntimeError> {
                    value
                        .get(field)
                        .map(|raw| {
                            resolved_value(Some(raw), request)?
                                .as_u64()
                                .map(|number| number as usize)
                                .ok_or_else(|| {
                                    EngineRuntimeError::UnsupportedProfile(format!(
                                        "computed field {field} must be a non-negative integer"
                                    ))
                                })
                        })
                        .transpose()
                };
            let resolved_f64_param = |field: &str| -> Result<Option<f64>, EngineRuntimeError> {
                value
                    .get(field)
                    .map(|raw| {
                        resolved_value(Some(raw), request)?.as_f64().ok_or_else(|| {
                            EngineRuntimeError::UnsupportedProfile(format!(
                                "computed field {field} must be numeric"
                            ))
                        })
                    })
                    .transpose()
            };
            Ok(DailyRankFeatureSpec {
                name: name.to_string(),
                op: op.to_string(),
                source: value
                    .get("source")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                right_source: value
                    .get("right_source")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                value: resolved_f64_param("value")?,
                period: resolved_usize("period")?,
                fastperiod: resolved_usize("fastperiod")?,
                slowperiod: resolved_usize("slowperiod")?,
                signalperiod: resolved_usize("signalperiod")?,
                stddev: resolved_f64_param("stddev")?,
                percentile: resolved_f64_param("percentile")?,
                annualize: value.get("annualize").and_then(Value::as_bool),
                output: value
                    .get("output")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                band: value
                    .get("band")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                high_source: value
                    .get("high_source")
                    .or_else(|| value.get("high"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                low_source: value
                    .get("low_source")
                    .or_else(|| value.get("low"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                close_source: value
                    .get("close_source")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                method: value
                    .get("method")
                    .or_else(|| value.get("average"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                sampling: value
                    .get("sampling")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                start_lag: resolved_usize("start_lag")?,
                end_lag: resolved_nonnegative_usize("end_lag")?,
                lower: resolved_f64_param("lower")?,
                upper: resolved_f64_param("upper")?,
                ascending: value.get("ascending").and_then(Value::as_bool),
                condition: value
                    .get("condition")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                true_source: value
                    .get("true_source")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                true_value: resolved_f64_param("true_value")?,
                false_source: value
                    .get("false_source")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                false_value: resolved_f64_param("false_value")?,
            })
        })
        .collect()
}

fn required_market_field_names<'a>(
    requests: impl Iterator<Item = &'a EngineRequestV2>,
) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for request in requests {
        let decision = &request.strategy.decision_plan;
        let computed_names = decision
            .computed_fields
            .iter()
            .filter_map(|field| field.get("name").and_then(Value::as_str))
            .map(|name| name.trim().to_lowercase())
            .collect::<BTreeSet<_>>();
        for field in &decision.computed_fields {
            for key in [
                "source",
                "right_source",
                "high_source",
                "high",
                "low_source",
                "low",
                "close_source",
                "true_source",
                "false_source",
            ] {
                if let Some(name) = field.get(key).and_then(Value::as_str) {
                    let normalized = name.trim().to_lowercase();
                    if !computed_names.contains(&normalized) {
                        names.insert(normalized);
                    }
                }
            }
            if field.get("op").and_then(Value::as_str) == Some("indicator.atr") {
                names.extend(["high".to_string(), "low".to_string()]);
            }
        }
        if let Some(name) = decision.selection.get("rank_by").and_then(Value::as_str) {
            names.insert(name.trim().to_lowercase());
        }
        if let Some(rule) = decision.selection.get("eligible") {
            collect_condition_field_names(rule, &mut names);
        }
        for key in ["entry", "exit"] {
            if let Some(rule) = decision.signals.get(key) {
                collect_condition_field_names(rule, &mut names);
            }
        }
    }
    names.remove("close");
    names
}

fn collect_condition_field_names(value: &Value, names: &mut BTreeSet<String>) {
    for key in ["field", "left", "right_field"] {
        if let Some(name) = value.get(key).and_then(Value::as_str) {
            names.insert(name.trim().to_lowercase());
        }
    }
    for key in ["all", "any", "nodes"] {
        if let Some(children) = value.get(key).and_then(Value::as_array) {
            for child in children {
                collect_condition_field_names(child, names);
            }
        }
    }
    for key in ["not", "node"] {
        if let Some(child) = value.get(key) {
            collect_condition_field_names(child, names);
        }
    }
}

fn feature_market_fields(
    bundle: &MarketDataBundleV2,
    close_frame: &DataFrame,
    required_names: BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<f64>>, EngineRuntimeError> {
    let mut fields = BTreeMap::new();
    let close_dates = bundle_time_strings(bundle, close_frame)?;
    for role in required_names {
        if !bundle.tables.contains_key(&role) {
            continue;
        }
        let frame = read_bundle_table(bundle, &role)?;
        if bundle_time_strings(bundle, &frame)? != close_dates {
            return Err(EngineRuntimeError::MarketData(format!(
                "{role} and close table timestamps are not aligned"
            )));
        }
        let columns = close_prices(&frame, &bundle.symbols)?;
        let values = (0..frame.height())
            .flat_map(|row| columns.iter().map(move |column| column[row]))
            .collect::<Vec<_>>();
        fields.insert(role, values);
    }
    Ok(fields)
}

fn condition_from_value(value: &Value) -> DailyRankConditionInput {
    let children = |key: &str| {
        value
            .get(key)
            .or_else(|| {
                (value.get("op").and_then(Value::as_str) == Some(key))
                    .then(|| value.get("nodes"))
                    .flatten()
            })
            .and_then(Value::as_array)
            .map(|items| items.iter().map(condition_from_value).collect())
            .unwrap_or_default()
    };
    let not_value = value.get("not").or_else(|| {
        (value.get("op").and_then(Value::as_str) == Some("not"))
            .then(|| value.get("node"))
            .flatten()
    });
    DailyRankConditionInput {
        field: value
            .get("field")
            .and_then(Value::as_str)
            .map(str::to_string),
        left: value
            .get("left")
            .and_then(Value::as_str)
            .map(str::to_string),
        op: value.get("op").and_then(Value::as_str).map(str::to_string),
        right_field: value
            .get("right_field")
            .and_then(Value::as_str)
            .map(str::to_string),
        right: value.get("right").and_then(Value::as_f64),
        value: value.get("value").and_then(Value::as_f64),
        all: children("all"),
        any: children("any"),
        not: not_value.map(|child| Box::new(condition_from_value(child))),
    }
}

fn validate_next_open_signal_actions(request: &EngineRequestV2) -> Result<(), EngineRuntimeError> {
    let actions = request
        .simulation
        .fill_model
        .get("actions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile(
                "signal timeline actions are required".to_string(),
            )
        })?;
    let valid = actions.len() == 2
        && actions.iter().all(|action| {
            matches!(
                action.get("signal").and_then(Value::as_str),
                Some("entry" | "exit")
            ) && action.get("offset_bars").and_then(Value::as_u64) == Some(1)
                && action.get("price").and_then(Value::as_str) == Some("open")
        });
    if !valid {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "cross signal runtime requires entry/exit actions at next-bar open".to_string(),
        ));
    }
    if request.simulation.account.account_type == crate::engine_request::AccountType::Margin {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "bar-time audit v1 does not support maintenance-margin-generated signal timeline fills"
                .to_string(),
        ));
    }
    if request
        .simulation
        .risk
        .get("max_daily_loss")
        .is_some_and(|value| !value.is_null())
        || request
            .simulation
            .risk
            .get("max_drawdown")
            .is_some_and(|value| !value.is_null())
    {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "bar-time audit v1 does not support risk-generated signal timeline fills".to_string(),
        ));
    }
    Ok(())
}

fn has_event_weight_actions(request: &EngineRequestV2) -> bool {
    request
        .simulation
        .fill_model
        .get("actions")
        .and_then(Value::as_array)
        .is_some_and(|actions| {
            actions.iter().any(|action| {
                action.get("signal").and_then(Value::as_str) == Some("entry")
                    && (action.get("weights").is_some()
                        || action.get("action").and_then(Value::as_str) == Some("flatten"))
            })
        })
}

fn calendar_overlay_weights(
    request: &EngineRequestV2,
) -> Result<(AssetWeights, AssetWeights), EngineRuntimeError> {
    let fill_model = &request.simulation.fill_model;
    let actions = fill_model
        .get("actions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile("timeline actions are required".to_string())
        })?;
    let baseline_weights = fill_model
        .get("baseline_weights")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidAllocation(
                "calendar overlay requires explicit baseline_weights".to_string(),
            )
        })?
        .iter()
        .map(|(symbol, value)| {
            value
                .as_f64()
                .map(|weight| (symbol.clone(), weight))
                .ok_or_else(|| {
                    EngineRuntimeError::InvalidAllocation(format!(
                        "calendar overlay baseline weight for {symbol} must be numeric"
                    ))
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let event_weights = action_weights(actions, "entry").ok_or_else(|| {
        EngineRuntimeError::InvalidAllocation("entry target weights are required".to_string())
    })?;
    Ok((baseline_weights, event_weights))
}

fn action_weights(actions: &[Value], signal: &str) -> Option<BTreeMap<String, f64>> {
    let action = actions
        .iter()
        .find(|action| action.get("signal").and_then(Value::as_str) == Some(signal))?;
    if action.get("action").and_then(Value::as_str) == Some("flatten") {
        return Some(BTreeMap::new());
    }
    action
        .get("weights")
        .and_then(Value::as_object)
        .map(|weights| {
            weights
                .iter()
                .filter_map(|(symbol, value)| Some((symbol.clone(), value.as_f64()?)))
                .collect()
        })
}

fn resolved_params(request: &EngineRequestV2) -> BTreeMap<String, String> {
    request
        .workflow
        .resolved_parameters
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string()),
            )
        })
        .collect()
}

fn resolved_value<'a>(
    value: Option<&'a Value>,
    request: &'a EngineRequestV2,
) -> Result<&'a Value, EngineRuntimeError> {
    let value = value.ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("required decision value is missing".to_string())
    })?;
    if let Some(reference) = value.get("param_ref").and_then(Value::as_str) {
        return request
            .workflow
            .resolved_parameters
            .get(reference)
            .ok_or_else(|| {
                EngineRuntimeError::UnsupportedProfile(format!(
                    "parameter {reference} is unresolved for this EngineRequest"
                ))
            });
    }
    Ok(value)
}

fn resolved_i64(
    value: Option<&Value>,
    request: &EngineRequestV2,
) -> Result<i64, EngineRuntimeError> {
    resolved_value(value, request)?.as_i64().ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("integer value is required".to_string())
    })
}

fn resolved_f64(
    value: Option<&Value>,
    request: &EngineRequestV2,
) -> Result<f64, EngineRuntimeError> {
    resolved_value(value, request)?.as_f64().ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("numeric value is required".to_string())
    })
}

fn resolved_string(
    value: Option<&Value>,
    request: &EngineRequestV2,
) -> Result<String, EngineRuntimeError> {
    resolved_value(value, request)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile("string value is required".to_string())
        })
}

fn read_bundle_table(
    bundle: &MarketDataBundleV2,
    role: &str,
) -> Result<DataFrame, EngineRuntimeError> {
    let table = bundle
        .tables
        .get(role)
        .ok_or_else(|| EngineRuntimeError::InvalidBundle(format!("{role} table is missing")))?;
    let path = table.path.as_deref().ok_or_else(|| {
        EngineRuntimeError::InvalidBundle(format!("{role} table requires parquet path"))
    })?;
    let frame = read_parquet(path)?;
    if frame.height() != table.row_count || frame.height() != bundle.row_count {
        return Err(EngineRuntimeError::MarketData(format!(
            "{role} parquet row_count does not match bundle manifest"
        )));
    }
    Ok(frame)
}

fn required_frame<'a>(
    frames: &'a BTreeMap<String, DataFrame>,
    name: &str,
) -> Result<&'a DataFrame, EngineRuntimeError> {
    frames
        .get(name)
        .ok_or_else(|| EngineRuntimeError::InvalidBundle(format!("{name} frame is missing")))
}

fn table_string_column(frame: &DataFrame, column: &str) -> Result<Vec<String>, EngineRuntimeError> {
    let source = frame
        .column(column)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    if source.dtype() != &DataType::String {
        return Err(EngineRuntimeError::MarketData(format!(
            "execution_timeline column {column} must use the v2 String transport"
        )));
    }
    source
        .str()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
        .iter()
        .map(|value| {
            value.map(str::to_string).ok_or_else(|| {
                EngineRuntimeError::MarketData(format!("{column} contains a null value"))
            })
        })
        .collect()
}

fn table_timestamp_column(
    frame: &DataFrame,
    column: &str,
) -> Result<Vec<String>, EngineRuntimeError> {
    table_string_column(frame, column)?
        .into_iter()
        .map(|value| normalize_utc_timestamp(&value))
        .collect()
}

fn table_u64_column(frame: &DataFrame, column: &str) -> Result<Vec<u64>, EngineRuntimeError> {
    let casted = frame
        .column(column)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
        .cast(&DataType::UInt64)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    casted
        .u64()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
        .iter()
        .map(|value| {
            value.ok_or_else(|| {
                EngineRuntimeError::MarketData(format!("{column} contains a null value"))
            })
        })
        .collect()
}

fn validate_timeline_row_keys(
    row_key_kind: MarketDataIndexKind,
    timestamp_convention: BarTimestampConventionV1,
    row_keys: &[String],
    open_timestamps: &[String],
    close_timestamps: &[String],
    session_labels: &[String],
) -> Result<(), EngineRuntimeError> {
    match row_key_kind {
        MarketDataIndexKind::SessionLabel => {
            if row_keys != session_labels {
                return Err(EngineRuntimeError::MarketData(
                    "session_label row keys do not match execution_timeline labels".to_string(),
                ));
            }
        }
        MarketDataIndexKind::EventTimestamp => {
            let authoritative = match timestamp_convention {
                BarTimestampConventionV1::BarOpen => open_timestamps,
                BarTimestampConventionV1::BarClose => close_timestamps,
            };
            for (row_key, timestamp) in row_keys.iter().zip(authoritative) {
                let row_key_nanos = parse_utc_nanos(row_key)
                    .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
                let timestamp_nanos = parse_utc_nanos(timestamp)
                    .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
                if row_key_nanos != timestamp_nanos {
                    return Err(EngineRuntimeError::MarketData(
                        "event_timestamp row keys do not match timestamp_convention".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_decoded_source_bars(
    bundle: &MarketDataBundleV2,
    bars: &[SourceBar],
) -> Result<(), EngineRuntimeError> {
    let windows = bundle
        .session_windows
        .iter()
        .map(|window| {
            let open = parse_utc_nanos(&window.open_timestamp)
                .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
            let close = parse_utc_nanos(&window.close_timestamp)
                .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
            Ok((window.session_label.as_str(), (open, close)))
        })
        .collect::<Result<BTreeMap<_, _>, EngineRuntimeError>>()?;
    let mut seen_sessions = Vec::new();
    let mut previous_sequence = None;
    let mut previous_close = None;
    for bar in bars {
        let (session_open, session_close) =
            windows.get(bar.session_label.as_str()).ok_or_else(|| {
                EngineRuntimeError::MarketData(format!(
                    "execution bar references unknown session {}",
                    bar.session_label
                ))
            })?;
        let open = parse_utc_nanos(&bar.bar_open_timestamp)
            .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
        let close = parse_utc_nanos(&bar.event_timestamp)
            .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
        let available = parse_utc_nanos(&bar.available_timestamp)
            .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
        if open >= close
            || close > available
            || open < *session_open
            || close > *session_close
            || previous_close.is_some_and(|prior| open < prior)
            || previous_sequence.is_some_and(|prior| bar.external_execution_sequence <= prior)
        {
            return Err(EngineRuntimeError::MarketData(format!(
                "execution bar timeline is invalid at {}",
                bar.event_timestamp
            )));
        }
        if ![bar.open, bar.high, bar.low, bar.close, bar.volume]
            .iter()
            .all(|value| value.is_finite())
            || bar.open <= 0.0
            || bar.high <= 0.0
            || bar.low <= 0.0
            || bar.close <= 0.0
            || bar.volume < 0.0
            || bar.high < bar.open.max(bar.close)
            || bar.low > bar.open.min(bar.close)
            || bar.low > bar.high
        {
            return Err(EngineRuntimeError::MarketData(format!(
                "execution bar OHLCV is invalid at {}",
                bar.event_timestamp
            )));
        }
        if seen_sessions.last().copied() != Some(bar.session_label.as_str()) {
            seen_sessions.push(bar.session_label.as_str());
        }
        previous_sequence = Some(bar.external_execution_sequence);
        previous_close = Some(close);
    }
    let expected_sessions = bundle
        .session_windows
        .iter()
        .map(|window| window.session_label.as_str())
        .collect::<Vec<_>>();
    if seen_sessions != expected_sessions {
        return Err(EngineRuntimeError::MarketData(
            "session_windows must exactly cover execution_timeline sessions".to_string(),
        ));
    }
    Ok(())
}

fn runtime_bar_spec(spec: &ContractBarSpecV1) -> Result<RuntimeBarSpec, EngineRuntimeError> {
    let unit = match spec.unit {
        ContractBarUnitV1::Minute => RuntimeBarUnit::Minute,
        ContractBarUnitV1::Hour => RuntimeBarUnit::Hour,
        ContractBarUnitV1::Day => RuntimeBarUnit::Day,
        ContractBarUnitV1::Week => RuntimeBarUnit::Week,
        ContractBarUnitV1::Month => RuntimeBarUnit::Month,
    };
    let alignment = match spec.alignment {
        ContractBarAlignmentV1::SessionOpen => RuntimeBarAlignment::SessionOpen,
        ContractBarAlignmentV1::CalendarPeriodStart => RuntimeBarAlignment::CalendarPeriodStart,
    };
    Ok(RuntimeBarSpec {
        step: spec.step,
        unit,
        alignment,
    })
}

fn runtime_partial_policy(policy: PartialBarPolicyV1) -> RuntimePartialBarPolicy {
    match policy {
        PartialBarPolicyV1::Omit => RuntimePartialBarPolicy::Omit,
        PartialBarPolicyV1::Emit => RuntimePartialBarPolicy::Emit,
    }
}

fn prepare_decision_graph(
    request: &EngineRequestV2,
    decision_stream_id: &str,
    execution_bars: &[SourceBar],
    sessions: &[SessionWindow],
) -> Result<Vec<PreparedDecisionBar>, EngineRuntimeError> {
    let execution_stream_id = &request.strategy.stream_binding.execution_stream_id;
    let mut cache = BTreeMap::from([(
        execution_stream_id.clone(),
        execution_bars
            .iter()
            .cloned()
            .map(|bar| PreparedGraphBar {
                bar,
                aggregation_lineage: Vec::new(),
            })
            .collect::<Vec<_>>(),
    )]);
    let mut visiting = BTreeSet::new();
    materialize_graph_stream(
        request,
        decision_stream_id,
        sessions,
        &mut cache,
        &mut visiting,
    )?;
    let decisions = cache.get(decision_stream_id).ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("decision graph did not materialize".to_string())
    })?;
    let execution_index = ExecutionBarIndex::new(execution_bars)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    decisions
        .iter()
        .map(|decision| {
            let next_execution_index = match execution_index.next_eligible(
                &decision.bar.available_timestamp,
                graph_last_execution_sequence(decision),
            ) {
                Ok(index) => Some(index),
                Err(BarAggregationError::NoEligibleExecutionBar { .. }) => None,
                Err(error) => return Err(EngineRuntimeError::MarketData(error.to_string())),
            };
            Ok(PreparedDecisionBar {
                stream_id: decision.bar.stream_id.clone(),
                bar_open_timestamp: decision.bar.bar_open_timestamp.clone(),
                event_timestamp: decision.bar.event_timestamp.clone(),
                available_timestamp: decision.bar.available_timestamp.clone(),
                session_label: decision.bar.session_label.clone(),
                open: decision.bar.open,
                high: decision.bar.high,
                low: decision.bar.low,
                close: decision.bar.close,
                volume: decision.bar.volume,
                source_count: graph_external_source_count(decision),
                source_first_execution_sequence: graph_first_execution_sequence(decision),
                source_execution_sequence: graph_last_execution_sequence(decision),
                aggregation_lineage: decision.aggregation_lineage.clone(),
                next_execution_index,
            })
        })
        .collect()
}

fn materialize_graph_stream(
    request: &EngineRequestV2,
    stream_id: &str,
    sessions: &[SessionWindow],
    cache: &mut BTreeMap<String, Vec<PreparedGraphBar>>,
    visiting: &mut BTreeSet<String>,
) -> Result<(), EngineRuntimeError> {
    if cache.contains_key(stream_id) {
        return Ok(());
    }
    if !visiting.insert(stream_id.to_string()) {
        return Err(EngineRuntimeError::InvalidRequest(format!(
            "bar-time graph contains a cycle at {stream_id}"
        )));
    }
    let stream = request
        .data_requirements
        .bar_time
        .streams
        .iter()
        .find(|stream| stream.stream_id == stream_id)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(format!("bar-time stream {stream_id} is missing"))
        })?;
    let BarStreamSourceV1::Derived {
        parent_stream_id,
        partial_first_bar_policy,
        partial_final_bar_policy,
        ..
    } = &stream.source
    else {
        return Err(EngineRuntimeError::InvalidRequest(format!(
            "non-execution stream {stream_id} must be derived"
        )));
    };
    materialize_graph_stream(request, parent_stream_id, sessions, cache, visiting)?;
    let parent_stream = request
        .data_requirements
        .bar_time
        .streams
        .iter()
        .find(|candidate| candidate.stream_id == *parent_stream_id)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(format!(
                "parent stream {parent_stream_id} is missing"
            ))
        })?;
    let parent_bars = cache.get(parent_stream_id).cloned().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest(format!(
            "parent stream {parent_stream_id} did not materialize"
        ))
    })?;
    let derived = aggregate_time_bars(AggregationRequest {
        source_stream_id: parent_stream_id.clone(),
        target_stream_id: stream.stream_id.clone(),
        parent_spec: runtime_bar_spec(&parent_stream.bar_spec)?,
        target_spec: runtime_bar_spec(&stream.bar_spec)?,
        sessions: sessions.to_vec(),
        partial_first_bar_policy: runtime_partial_policy(*partial_first_bar_policy),
        partial_final_bar_policy: match partial_final_bar_policy {
            FinalPartialBarPolicyV1::Omit => RuntimePartialBarPolicy::Omit,
            FinalPartialBarPolicyV1::Emit => RuntimePartialBarPolicy::Emit,
        },
        source_bars: parent_bars
            .iter()
            .map(|prepared| prepared.bar.clone())
            .collect(),
    })
    .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    let prepared = derived
        .iter()
        .map(|bar| prepare_graph_bar(bar, &parent_bars))
        .collect::<Result<Vec<_>, _>>()?;
    cache.insert(stream.stream_id.clone(), prepared);
    visiting.remove(stream_id);
    Ok(())
}

fn prepare_graph_bar(
    derived: &DerivedBar,
    parent_bars: &[PreparedGraphBar],
) -> Result<PreparedGraphBar, EngineRuntimeError> {
    let first = parent_bars
        .iter()
        .position(|bar| bar.bar.event_timestamp == derived.lineage.source_first_timestamp)
        .ok_or_else(|| {
            EngineRuntimeError::MarketData(
                "derived lineage first parent timestamp is missing".to_string(),
            )
        })?;
    let last = parent_bars
        .iter()
        .rposition(|bar| bar.bar.event_timestamp == derived.lineage.source_last_timestamp)
        .ok_or_else(|| {
            EngineRuntimeError::MarketData(
                "derived lineage last parent timestamp is missing".to_string(),
            )
        })?;
    if first > last || last - first + 1 != derived.lineage.source_count {
        return Err(EngineRuntimeError::MarketData(
            "derived lineage parent count does not reconcile".to_string(),
        ));
    }
    let parents = &parent_bars[first..=last];
    let mut lineage = merge_parent_lineage(parents)?;
    let external_source_bar_count = parents
        .iter()
        .map(graph_external_source_count)
        .sum::<usize>();
    let source_first = graph_first_execution_sequence(&parents[0]);
    let source_last = graph_last_execution_sequence(&parents[parents.len() - 1]);
    lineage.push(BarTimeAggregationLineage {
        stream_id: derived.stream_id.clone(),
        parent_stream_id: derived.lineage.parent_stream_id.clone(),
        parent_bar_count: parents.len(),
        external_source_bar_count,
        source_first_external_execution_sequence: source_first,
        source_last_external_execution_sequence: source_last,
        partial: derived.lineage.partial,
    });
    Ok(PreparedGraphBar {
        bar: SourceBar {
            stream_id: derived.stream_id.clone(),
            external_execution_sequence: source_last,
            bar_open_timestamp: derived.bar_open_timestamp.clone(),
            event_timestamp: derived.event_timestamp.clone(),
            available_timestamp: derived.available_timestamp.clone(),
            session_label: derived.session_label.clone(),
            open: derived.open,
            high: derived.high,
            low: derived.low,
            close: derived.close,
            volume: derived.volume,
        },
        aggregation_lineage: lineage,
    })
}

fn merge_parent_lineage(
    parents: &[PreparedGraphBar],
) -> Result<Vec<BarTimeAggregationLineage>, EngineRuntimeError> {
    let depth = parents
        .first()
        .map(|parent| parent.aggregation_lineage.len())
        .unwrap_or(0);
    if parents
        .iter()
        .any(|parent| parent.aggregation_lineage.len() != depth)
    {
        return Err(EngineRuntimeError::MarketData(
            "parent aggregation lineage depths do not match".to_string(),
        ));
    }
    let mut result = Vec::with_capacity(depth);
    for index in 0..depth {
        let first = &parents[0].aggregation_lineage[index];
        if parents.iter().any(|parent| {
            let hop = &parent.aggregation_lineage[index];
            hop.stream_id != first.stream_id || hop.parent_stream_id != first.parent_stream_id
        }) {
            return Err(EngineRuntimeError::MarketData(
                "parent aggregation lineage graph does not match".to_string(),
            ));
        }
        result.push(BarTimeAggregationLineage {
            stream_id: first.stream_id.clone(),
            parent_stream_id: first.parent_stream_id.clone(),
            parent_bar_count: parents
                .iter()
                .map(|parent| parent.aggregation_lineage[index].parent_bar_count)
                .sum(),
            external_source_bar_count: parents
                .iter()
                .map(|parent| parent.aggregation_lineage[index].external_source_bar_count)
                .sum(),
            source_first_external_execution_sequence: parents[0].aggregation_lineage[index]
                .source_first_external_execution_sequence,
            source_last_external_execution_sequence: parents[parents.len() - 1].aggregation_lineage
                [index]
                .source_last_external_execution_sequence,
            partial: parents
                .iter()
                .any(|parent| parent.aggregation_lineage[index].partial),
        });
    }
    Ok(result)
}

fn graph_external_source_count(bar: &PreparedGraphBar) -> usize {
    bar.aggregation_lineage
        .last()
        .map(|lineage| lineage.external_source_bar_count)
        .unwrap_or(1)
}

fn graph_first_execution_sequence(bar: &PreparedGraphBar) -> u64 {
    bar.aggregation_lineage
        .last()
        .map(|lineage| lineage.source_first_external_execution_sequence)
        .unwrap_or(bar.bar.external_execution_sequence)
}

fn graph_last_execution_sequence(bar: &PreparedGraphBar) -> u64 {
    bar.aggregation_lineage
        .last()
        .map(|lineage| lineage.source_last_external_execution_sequence)
        .unwrap_or(bar.bar.external_execution_sequence)
}

fn aligned_dates(
    bundle: &MarketDataBundleV2,
    close: &DataFrame,
    open: &DataFrame,
) -> Result<Vec<String>, EngineRuntimeError> {
    let close_dates = bundle_time_strings(bundle, close)?;
    let open_dates = bundle_time_strings(bundle, open)?;
    if close_dates != open_dates {
        return Err(EngineRuntimeError::MarketData(
            "open and close table timestamps are not aligned".to_string(),
        ));
    }
    Ok(close_dates)
}

fn table_price_column(frame: &DataFrame, symbol: &str) -> Result<Vec<f64>, EngineRuntimeError> {
    close_prices(frame, &[symbol.to_string()]).map(|mut values| values.remove(0))
}

fn table_numeric_column(frame: &DataFrame, column: &str) -> Result<Vec<f64>, EngineRuntimeError> {
    let casted = frame
        .column(column)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
        .cast(&DataType::Float64)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    casted
        .f64()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
        .iter()
        .map(|value| {
            value.filter(|value| value.is_finite()).ok_or_else(|| {
                EngineRuntimeError::MarketData(format!(
                    "feature table contains invalid value for {column}"
                ))
            })
        })
        .collect()
}

fn table_feature_column(
    frame: &DataFrame,
    field: &str,
    symbols: &[String],
) -> Result<Vec<f64>, EngineRuntimeError> {
    if frame.column(field).is_ok() {
        return table_numeric_column(frame, field);
    }
    let symbol = symbols
        .iter()
        .find(|symbol| frame.column(symbol).is_ok())
        .ok_or_else(|| {
            EngineRuntimeError::MarketData(format!(
                "feature table {field} has neither a {field} column nor a universe symbol column"
            ))
        })?;
    table_numeric_column(frame, symbol)
}

fn timeline_accounting_config(
    request: &EngineRequestV2,
) -> Result<TimelineAccountingConfig, EngineRuntimeError> {
    let accounting = accounting_config(request)?;
    let overlap = request
        .simulation
        .fill_model
        .get("position_policy")
        .and_then(|value| value.get("on_entry_signal_while_holding"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "fill_model.position_policy.on_entry_signal_while_holding is required".to_string(),
            )
        })?
        .to_string();
    Ok(TimelineAccountingConfig {
        starting_equity: accounting.starting_equity,
        cost_rate: accounting.cost_rate,
        max_gross_exposure: accounting.max_gross_exposure,
        allow_short: accounting.allow_short,
        short_borrow_rate_annual: accounting.short_borrow_rate_annual,
        borrow_day_count: accounting.borrow_day_count,
        session_label_by_event_time: accounting.session_label_by_event_time,
        position_policy: TimelinePositionPolicy {
            on_entry_signal_while_holding: overlap,
        },
        risk_gates: crate::timeline::TimelineRiskGateConfig {
            max_positions: accounting.risk_gates.max_positions,
            max_daily_loss: accounting.risk_gates.max_daily_loss,
            max_order_size: accounting.risk_gates.max_order_size,
            max_drawdown: accounting.risk_gates.max_drawdown,
            gate_action: accounting.risk_gates.gate_action,
            reduce_exposure_factor: accounting.risk_gates.reduce_exposure_factor,
        },
        simulated_venue: accounting.simulated_venue,
        simulated_account: accounting.simulated_account,
    })
}

fn read_parquet(path: &str) -> Result<DataFrame, EngineRuntimeError> {
    let file = File::open(Path::new(path))
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    ParquetReader::new(file)
        .finish()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))
}

fn bundle_time_strings(
    bundle: &MarketDataBundleV2,
    frame: &DataFrame,
) -> Result<Vec<String>, EngineRuntimeError> {
    time_strings(
        frame,
        &bundle.time_column,
        bundle.execution_stream.row_key_kind,
    )
}

fn validate_available_timeline(
    frame: &DataFrame,
    event_times: &[String],
    available_time_column: &str,
) -> Result<(), EngineRuntimeError> {
    let available_times = time_strings(
        frame,
        available_time_column,
        MarketDataIndexKind::EventTimestamp,
    )?;
    if event_times.len() != available_times.len() {
        return Err(EngineRuntimeError::MarketData(
            "event timestamps and available timestamps have different lengths".to_string(),
        ));
    }
    let mut previous = None;
    for (event_time, available_time) in event_times.iter().zip(available_times) {
        let event = parse_utc_nanos(event_time)
            .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
        let available = parse_utc_nanos(&available_time)
            .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
        if available < event {
            return Err(EngineRuntimeError::MarketData(format!(
                "available timestamp {available_time} precedes event timestamp {event_time}"
            )));
        }
        if previous.is_some_and(|prior| available < prior) {
            return Err(EngineRuntimeError::MarketData(format!(
                "available timestamps move backwards at {available_time}"
            )));
        }
        previous = Some(available);
    }
    Ok(())
}

fn attach_accounting_session_labels(
    config: &mut AccountingConfig,
    bundle: &MarketDataBundleV2,
    frame: &DataFrame,
) -> Result<(), EngineRuntimeError> {
    config.session_label_by_event_time = bundle_session_label_map(bundle, frame)?;
    Ok(())
}

fn attach_timeline_session_labels(
    config: &mut TimelineAccountingConfig,
    bundle: &MarketDataBundleV2,
    frame: &DataFrame,
) -> Result<(), EngineRuntimeError> {
    config.session_label_by_event_time = bundle_session_label_map(bundle, frame)?;
    Ok(())
}

fn bundle_session_label_map(
    bundle: &MarketDataBundleV2,
    frame: &DataFrame,
) -> Result<BTreeMap<String, String>, EngineRuntimeError> {
    let event_times = time_strings(
        frame,
        &bundle.time_column,
        bundle.execution_stream.row_key_kind,
    )?;
    let session_labels = match bundle.execution_stream.row_key_kind {
        MarketDataIndexKind::SessionLabel => event_times.clone(),
        MarketDataIndexKind::EventTimestamp => {
            let timeline = read_bundle_table(bundle, &bundle.execution_stream.timeline_table)?;
            let timeline_keys = time_strings(
                &timeline,
                &bundle.time_column,
                MarketDataIndexKind::EventTimestamp,
            )?;
            if timeline_keys != event_times {
                return Err(EngineRuntimeError::MarketData(
                    "execution_timeline row keys do not match OHLCV row keys".to_string(),
                ));
            }
            session_label_strings(
                &timeline,
                &bundle
                    .execution_stream
                    .timestamp_semantics
                    .session_label_column,
            )?
        }
    };
    if event_times.len() != session_labels.len() {
        return Err(EngineRuntimeError::MarketData(
            "event timestamps and session labels have different lengths".to_string(),
        ));
    }
    let mut result = BTreeMap::new();
    for (event_time, session_label) in event_times.into_iter().zip(session_labels) {
        if result
            .insert(event_time.clone(), session_label.clone())
            .is_some()
        {
            return Err(EngineRuntimeError::MarketData(format!(
                "duplicate event timestamp while building session map: {event_time}"
            )));
        }
    }
    Ok(result)
}

fn time_strings(
    frame: &DataFrame,
    time_column: &str,
    index_kind: MarketDataIndexKind,
) -> Result<Vec<String>, EngineRuntimeError> {
    let source = frame
        .column(time_column)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    if source.dtype() != &DataType::String {
        return Err(EngineRuntimeError::MarketData(format!(
            "row-key column {time_column} must use the v2 String transport"
        )));
    }
    let values = source
        .str()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    values
        .iter()
        .map(|value| {
            let value = value
                .ok_or_else(|| EngineRuntimeError::MarketData("invalid Time value".to_string()))?;
            match index_kind {
                MarketDataIndexKind::SessionLabel if valid_runtime_session_label(value) => {
                    Ok(value.to_string())
                }
                MarketDataIndexKind::SessionLabel => Err(EngineRuntimeError::MarketData(
                    "session_label Time values must use canonical YYYY-MM-DD strings".to_string(),
                )),
                MarketDataIndexKind::EventTimestamp => normalize_utc_timestamp(value),
            }
        })
        .collect()
}

fn normalize_utc_timestamp(value: &str) -> Result<String, EngineRuntimeError> {
    parse_utc_nanos(value).map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    Ok(value.to_string())
}

fn valid_runtime_session_label(value: &str) -> bool {
    value.len() == 10
        && value.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
}

fn session_label_strings(
    frame: &DataFrame,
    session_label_column: &str,
) -> Result<Vec<String>, EngineRuntimeError> {
    let column = frame
        .column(session_label_column)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    if column.dtype() != &DataType::String {
        return Err(EngineRuntimeError::MarketData(format!(
            "session label column {session_label_column} must use the v2 String transport"
        )));
    }
    let values = column
        .str()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    values
        .iter()
        .map(|value| {
            value
                .filter(|value| valid_runtime_session_label(value))
                .map(str::to_string)
                .ok_or_else(|| EngineRuntimeError::MarketData("invalid session label".to_string()))
        })
        .collect()
}

fn close_prices(
    frame: &DataFrame,
    symbols: &[String],
) -> Result<Vec<Vec<f64>>, EngineRuntimeError> {
    symbols
        .iter()
        .map(|symbol| {
            let casted = frame
                .column(symbol)
                .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
                .cast(&DataType::Float64)
                .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
            casted
                .f64()
                .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
                .iter()
                .map(|value| {
                    value
                        .filter(|value| value.is_finite() && *value > 0.0)
                        .ok_or_else(|| {
                            EngineRuntimeError::MarketData(format!(
                                "close table contains invalid price for {symbol}"
                            ))
                        })
                })
                .collect()
        })
        .collect()
}

fn fixed_weights(
    allocation: &Value,
    symbols: &[String],
) -> Result<BTreeMap<String, f64>, EngineRuntimeError> {
    let raw = allocation
        .get("weights")
        .and_then(Value::as_object)
        .ok_or_else(|| EngineRuntimeError::InvalidAllocation("weights are required".to_string()))?;
    let mut weights = BTreeMap::new();
    for symbol in symbols {
        let weight = raw.get(symbol).and_then(Value::as_f64).unwrap_or(0.0);
        if !weight.is_finite() || weight < 0.0 {
            return Err(EngineRuntimeError::InvalidAllocation(format!(
                "invalid long-only weight for {symbol}"
            )));
        }
        weights.insert(symbol.clone(), weight);
    }
    let gross = weights.values().sum::<f64>();
    if gross <= 0.0 || gross > 1.0 + 1e-12 {
        return Err(EngineRuntimeError::InvalidAllocation(format!(
            "fixed weight gross exposure must be in (0, 1], got {gross}"
        )));
    }
    Ok(weights)
}

fn rebalance_flags(dates: &[String], trigger: &str) -> Result<Vec<bool>, EngineRuntimeError> {
    let mut flags = vec![false; dates.len()];
    for (index, date) in dates.iter().enumerate() {
        let month = date.get(..7).unwrap_or(date.as_str());
        let year = date.get(..4).unwrap_or(date.as_str());
        flags[index] = match trigger {
            "calendar.every_session" | "every_session" => true,
            "calendar.first_session" => index == 0,
            "calendar.month_start" => {
                index == 0
                    || month
                        != dates[index - 1]
                            .get(..7)
                            .unwrap_or(dates[index - 1].as_str())
            }
            "calendar.month_end" => {
                index + 1 == dates.len()
                    || month
                        != dates[index + 1]
                            .get(..7)
                            .unwrap_or(dates[index + 1].as_str())
            }
            "calendar.year_start" => {
                index == 0
                    || year
                        != dates[index - 1]
                            .get(..4)
                            .unwrap_or(dates[index - 1].as_str())
            }
            "calendar.year_end" => {
                index + 1 == dates.len()
                    || year
                        != dates[index + 1]
                            .get(..4)
                            .unwrap_or(dates[index + 1].as_str())
            }
            other => {
                return Err(EngineRuntimeError::UnsupportedProfile(format!(
                    "unsupported rebalance trigger {other}"
                )))
            }
        };
    }
    Ok(flags)
}

fn accounting_config(request: &EngineRequestV2) -> Result<AccountingConfig, EngineRuntimeError> {
    let fill_model = &request.simulation.fill_model;
    let cost = fill_model.get("cost").ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("simulation.fill_model.cost is required".to_string())
    })?;
    let transaction_cost = cost
        .get("transaction_cost")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.cost.transaction_cost is required".to_string(),
            )
        })?;
    let slippage = cost
        .get("slippage")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.cost.slippage is required".to_string(),
            )
        })?;
    let cost_rate = transaction_cost + slippage;
    let risk = &request.simulation.risk;
    let max_gross_exposure = risk
        .get("max_gross_exposure")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.risk.max_gross_exposure is required".to_string(),
            )
        })?;
    let allow_short = risk
        .get("allow_short")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.risk.allow_short is required".to_string(),
            )
        })?;
    let short_borrow_rate_annual = cost
        .get("short_borrow_rate_annual")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.cost.short_borrow_rate_annual is required".to_string(),
            )
        })?;
    let borrow_day_count = cost
        .get("borrow_day_count")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.cost.borrow_day_count is required".to_string(),
            )
        })? as u32;
    Ok(AccountingConfig {
        starting_equity: request.simulation.account.starting_balance,
        cost_rate,
        max_gross_exposure,
        allow_short,
        short_borrow_rate_annual,
        borrow_day_count,
        session_label_by_event_time: BTreeMap::new(),
        risk_gates: AccountingRiskGateConfig {
            max_positions: risk
                .get("max_positions")
                .and_then(Value::as_u64)
                .map(|value| value as usize),
            max_daily_loss: risk.get("max_daily_loss").and_then(Value::as_f64),
            max_order_size: risk.get("max_order_size").and_then(Value::as_f64),
            max_drawdown: risk.get("max_drawdown").and_then(Value::as_f64),
            gate_action: risk
                .get("gate_action")
                .and_then(Value::as_str)
                .map(str::to_string),
            reduce_exposure_factor: risk.get("reduce_exposure_factor").and_then(Value::as_f64),
        },
        simulated_venue: simulated_venue_config(request)?,
        simulated_account: simulated_account_config(request)?,
    })
}

fn simulated_venue_config(
    request: &EngineRequestV2,
) -> Result<crate::simulation::SimulatedVenueConfig, EngineRuntimeError> {
    let max_fill_fraction = request
        .simulation
        .fill_model
        .get("liquidity")
        .and_then(|value| value.get("max_fill_fraction"))
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.liquidity.max_fill_fraction is required".to_string(),
            )
        })?;
    let min_order_delta = request
        .simulation
        .fill_model
        .get("min_order_delta")
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.min_order_delta is required".to_string(),
            )
        })?;
    let time_in_force = match request
        .simulation
        .fill_model
        .get("time_in_force")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.time_in_force is required".to_string(),
            )
        })? {
        "gtc" => crate::simulation::TimeInForce::Gtc,
        "ioc" => crate::simulation::TimeInForce::Ioc,
        "fok" => crate::simulation::TimeInForce::Fok,
        "day" => crate::simulation::TimeInForce::Day,
        other => {
            return Err(EngineRuntimeError::InvalidRequest(format!(
                "unsupported simulation.fill_model.time_in_force: {other}"
            )))
        }
    };
    let atomic_batch = request
        .simulation
        .fill_model
        .get("atomic_batch")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.atomic_batch is required".to_string(),
            )
        })?;
    Ok(crate::simulation::SimulatedVenueConfig {
        max_fill_fraction,
        min_order_delta,
        time_in_force,
        atomic_batch,
    })
}

fn simulated_account_config(
    request: &EngineRequestV2,
) -> Result<crate::simulation::SimulatedAccountConfig, EngineRuntimeError> {
    let is_cash =
        request.simulation.account.account_type == crate::engine_request::AccountType::Cash;
    let maintenance_margin_ratio = request
        .simulation
        .fill_model
        .get("margin")
        .and_then(|value| value.get("maintenance_margin_ratio"))
        .and_then(Value::as_f64)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.fill_model.margin.maintenance_margin_ratio is required".to_string(),
            )
        })?;
    let allow_short_borrow = request
        .simulation
        .risk
        .get("allow_short")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            EngineRuntimeError::InvalidRequest(
                "simulation.risk.allow_short is required".to_string(),
            )
        })?;
    Ok(crate::simulation::SimulatedAccountConfig {
        account_type: if is_cash {
            crate::simulation::SimulatedAccountType::Cash
        } else {
            crate::simulation::SimulatedAccountType::Margin
        },
        leverage_limit: request.simulation.account.leverage_limit,
        initial_margin_ratio: if is_cash {
            1.0
        } else {
            1.0 / request.simulation.account.leverage_limit
        },
        maintenance_margin_ratio,
        allow_short_borrow,
        settlement_days: request.simulation.venue.settlement_days,
    })
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;

    const REQUEST_FIXTURE: &str = include_str!(
        "../../../backtester/contracts/runtime/examples/engine-request-profile-fixtures-v2.json"
    );
    const BUNDLE_FIXTURE: &str = include_str!(
        "../../../backtester/contracts/runtime/examples/market-data-bundle-v2.example.json"
    );

    #[test]
    fn calendar_session_offset_signal_marks_t_minus_eight_sessions() {
        let request = direct_daily_request();
        let entry = serde_json::json!({
            "op": "calendar.session_offset_from_month_end",
            "offset_sessions": -2
        });
        let dates = vec![
            "2024-01-25".to_string(),
            "2024-01-26".to_string(),
            "2024-01-29".to_string(),
            "2024-01-30".to_string(),
            "2024-01-31".to_string(),
            "2024-02-27".to_string(),
            "2024-02-28".to_string(),
            "2024-02-29".to_string(),
        ];

        let signal = calendar_session_offset_signal(&entry, &request, &dates).unwrap();

        assert_eq!(
            signal,
            vec![false, false, true, false, false, true, false, false]
        );
    }

    fn direct_daily_request() -> EngineRequestV2 {
        let fixture: Value = serde_json::from_str(REQUEST_FIXTURE).unwrap();
        let mut request: EngineRequestV2 =
            serde_json::from_value(fixture["requests"][0].clone()).unwrap();
        request.data_requirements.symbols = vec!["QQQ".to_string()];
        request
    }

    fn direct_daily_bundle() -> MarketDataBundleV2 {
        serde_json::from_str(BUNDLE_FIXTURE).unwrap()
    }

    fn next_open_signal_request(request: EngineRequestV2) -> EngineRequestV2 {
        let mut value = serde_json::to_value(request).unwrap();
        value["strategy"]["decision_plan"] = serde_json::json!({
            "factor_pipeline": {},
            "computed_fields": [],
            "signals": {
                "entry": {"field": "close", "op": "lt", "right": 105.0},
                "exit": {"field": "close", "op": "lt", "right": 0.0},
                "side": "long",
                "target_weight": 1.0
            },
            "selection": {},
            "allocation": {
                "method": "position_state",
                "target_weight": 1.0,
                "cash_policy": "keep_unallocated_cash"
            },
            "rebalance": {"trigger": {"op": "calendar.every_session"}},
            "required_operations": ["calendar.every_session", "gt", "lt"],
            "required_actions": ["enter", "exit"]
        });
        value["simulation"]["fill_model"]["timing"] = serde_json::json!("timeline");
        value["simulation"]["fill_model"]["actions"] = serde_json::json!([
            {
                "signal": "entry",
                "offset_bars": 1,
                "price": "open",
                "action": "enter"
            },
            {
                "signal": "exit",
                "offset_bars": 1,
                "price": "open",
                "action": "exit"
            }
        ]);
        serde_json::from_value(value).unwrap()
    }

    fn multi_level_session_request() -> EngineRequestV2 {
        let mut value = serde_json::to_value(direct_daily_request()).unwrap();
        let provider_id = value["data_requirements"]["provider"]
            .as_str()
            .unwrap()
            .to_string();
        let streams = value["data_requirements"]["bar_time"]["streams"]
            .as_array_mut()
            .unwrap();
        streams.clear();
        let timestamp_semantics = serde_json::json!({
            "timestamp_convention": "bar_close",
            "interval_boundary": "left_open_right_closed",
            "bar_open_time_column": "bar_open_timestamp",
            "bar_close_time_column": "bar_close_timestamp",
            "available_time_column": "available_timestamp",
            "session_label_column": "session_label",
            "availability_policy": "bar_close"
        });
        streams.push(serde_json::json!({
            "stream_id": "execution_1m",
            "role": "execution",
            "source": {"kind": "external", "provider_id": provider_id},
            "bar_spec": {
                "aggregation": "time", "step": 1, "unit": "minute",
                "price_type": "last", "alignment": "session_open"
            },
            "timestamp_semantics": timestamp_semantics.clone()
        }));
        for (stream_id, parent_stream_id, step, unit) in [
            ("derived_5m", "execution_1m", 5, "minute"),
            ("derived_1h", "derived_5m", 1, "hour"),
            ("decision_session", "derived_1h", 1, "day"),
        ] {
            let partial_final_bar_policy = if stream_id == "derived_1h" {
                "emit"
            } else {
                "omit"
            };
            streams.push(serde_json::json!({
                "stream_id": stream_id,
                "role": "decision",
                "source": {
                    "kind": "derived",
                    "parent_stream_id": parent_stream_id,
                    "aggregation_engine": "shared_rust",
                    "empty_bar_policy": "omit",
                    "partial_first_bar_policy": "omit",
                    "partial_final_bar_policy": partial_final_bar_policy
                },
                "bar_spec": {
                    "aggregation": "time", "step": step, "unit": unit,
                    "price_type": "last", "alignment": "session_open"
                },
                "timestamp_semantics": timestamp_semantics.clone()
            }));
        }
        value["strategy"]["stream_binding"] = serde_json::json!({
            "execution_stream_id": "execution_1m",
            "decision_stream_id": "decision_session"
        });
        let mut request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        request = next_open_signal_request(request);
        request.simulation.fill_model["cost"]["transaction_cost"] = serde_json::json!(0.001);
        request.simulation.fill_model["cost"]["slippage"] = serde_json::json!(0.0005);
        request.request_hash = request.computed_hash().unwrap();
        request
    }

    fn calendar_week_request() -> EngineRequestV2 {
        let mut value = serde_json::to_value(direct_daily_request()).unwrap();
        let streams = value["data_requirements"]["bar_time"]["streams"]
            .as_array_mut()
            .unwrap();
        let timestamp_semantics = streams[0]["timestamp_semantics"].clone();
        streams.push(serde_json::json!({
            "stream_id": "decision_1w",
            "role": "decision",
            "source": {
                "kind": "derived",
                "parent_stream_id": "execution_daily",
                "aggregation_engine": "shared_rust",
                "empty_bar_policy": "omit",
                "partial_first_bar_policy": "omit",
                "partial_final_bar_policy": "omit"
            },
            "bar_spec": {
                "aggregation": "time",
                "step": 1,
                "unit": "week",
                "price_type": "last",
                "alignment": "calendar_period_start"
            },
            "timestamp_semantics": timestamp_semantics
        }));
        value["strategy"]["stream_binding"] = serde_json::json!({
            "execution_stream_id": "execution_daily",
            "decision_stream_id": "decision_1w"
        });
        let request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        next_open_signal_request(request)
    }

    fn calendar_month_request() -> EngineRequestV2 {
        let mut value = serde_json::to_value(calendar_week_request()).unwrap();
        let stream = value["data_requirements"]["bar_time"]["streams"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|stream| stream["stream_id"] == "decision_1w")
            .unwrap();
        stream["stream_id"] = serde_json::json!("decision_1mo");
        stream["bar_spec"]["unit"] = serde_json::json!("month");
        value["strategy"]["stream_binding"]["decision_stream_id"] =
            serde_json::json!("decision_1mo");
        let mut request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        request.request_hash = request.computed_hash().unwrap();
        request
    }

    fn minute_timestamp(day: &str, minute_offset: usize) -> String {
        let total_minutes = 13 * 60 + 30 + minute_offset;
        format!(
            "{day}T{:02}:{:02}:00Z",
            total_minutes / 60,
            total_minutes % 60
        )
    }

    fn xnys_normal_and_half_day_minute_frames() -> BTreeMap<String, DataFrame> {
        let mut row_keys = Vec::new();
        let mut open_times = Vec::new();
        let mut session_labels = Vec::new();
        let mut sequence = Vec::new();
        let mut open = Vec::new();
        let mut high = Vec::new();
        let mut low = Vec::new();
        let mut close = Vec::new();
        let mut volume = Vec::new();
        let mut global_index = 0usize;
        for (day, minute_count) in [("2024-07-02", 390usize), ("2024-07-03", 210usize)] {
            for minute in 0..minute_count {
                let price = 100.0 + global_index as f64 * 0.01;
                open_times.push(minute_timestamp(day, minute));
                row_keys.push(minute_timestamp(day, minute + 1));
                session_labels.push(day.to_string());
                sequence.push((global_index + 1) as u64);
                open.push(price);
                high.push(price + 0.02);
                low.push(price - 0.02);
                close.push(price + 0.01);
                volume.push(100.0 + minute as f64);
                global_index += 1;
            }
        }
        BTreeMap::from([
            (
                "open".to_string(),
                df!("Time" => row_keys.clone(), "QQQ" => open).unwrap(),
            ),
            (
                "high".to_string(),
                df!("Time" => row_keys.clone(), "QQQ" => high).unwrap(),
            ),
            (
                "low".to_string(),
                df!("Time" => row_keys.clone(), "QQQ" => low).unwrap(),
            ),
            (
                "close".to_string(),
                df!("Time" => row_keys.clone(), "QQQ" => close).unwrap(),
            ),
            (
                "volume".to_string(),
                df!("Time" => row_keys.clone(), "QQQ" => volume).unwrap(),
            ),
            (
                "execution_timeline".to_string(),
                df!(
                    "Time" => row_keys.clone(),
                    "external_execution_sequence" => sequence,
                    "bar_open_timestamp" => open_times,
                    "bar_close_timestamp" => row_keys.clone(),
                    "available_timestamp" => row_keys,
                    "session_label" => session_labels,
                )
                .unwrap(),
            ),
        ])
    }

    fn daily_frames() -> BTreeMap<String, DataFrame> {
        BTreeMap::from([
            (
                "open".to_string(),
                df!("Time" => &["2024-07-03"], "QQQ" => &[100.0]).unwrap(),
            ),
            (
                "high".to_string(),
                df!("Time" => &["2024-07-03"], "QQQ" => &[102.0]).unwrap(),
            ),
            (
                "low".to_string(),
                df!("Time" => &["2024-07-03"], "QQQ" => &[99.0]).unwrap(),
            ),
            (
                "close".to_string(),
                df!("Time" => &["2024-07-03"], "QQQ" => &[101.0]).unwrap(),
            ),
            (
                "volume".to_string(),
                df!("Time" => &["2024-07-03"], "QQQ" => &[1_000.0]).unwrap(),
            ),
            (
                "execution_timeline".to_string(),
                df!(
                    "Time" => &["2024-07-03"],
                    "external_execution_sequence" => &[1_u64],
                    "bar_open_timestamp" => &["2024-07-03T13:30:00Z"],
                    "bar_close_timestamp" => &["2024-07-03T17:00:00Z"],
                    "available_timestamp" => &["2024-07-03T17:00:00Z"],
                    "session_label" => &["2024-07-03"],
                )
                .unwrap(),
            ),
        ])
    }

    fn two_session_daily_frames() -> BTreeMap<String, DataFrame> {
        BTreeMap::from([
            (
                "open".to_string(),
                df!("Time" => &["2024-07-03", "2024-07-05"], "QQQ" => &[100.0, 110.0]).unwrap(),
            ),
            (
                "high".to_string(),
                df!("Time" => &["2024-07-03", "2024-07-05"], "QQQ" => &[102.0, 112.0]).unwrap(),
            ),
            (
                "low".to_string(),
                df!("Time" => &["2024-07-03", "2024-07-05"], "QQQ" => &[99.0, 109.0]).unwrap(),
            ),
            (
                "close".to_string(),
                df!("Time" => &["2024-07-03", "2024-07-05"], "QQQ" => &[101.0, 111.0]).unwrap(),
            ),
            (
                "volume".to_string(),
                df!("Time" => &["2024-07-03", "2024-07-05"], "QQQ" => &[1_000.0, 1_100.0]).unwrap(),
            ),
            (
                "execution_timeline".to_string(),
                df!(
                    "Time" => &["2024-07-03", "2024-07-05"],
                    "external_execution_sequence" => &[1_u64, 2],
                    "bar_open_timestamp" => &[
                        "2024-07-03T13:30:00Z",
                        "2024-07-05T13:30:00Z",
                    ],
                    "bar_close_timestamp" => &[
                        "2024-07-03T17:00:00Z",
                        "2024-07-05T20:00:00Z",
                    ],
                    "available_timestamp" => &[
                        "2024-07-03T17:00:00Z",
                        "2024-07-05T20:00:00Z",
                    ],
                    "session_label" => &["2024-07-03", "2024-07-05"],
                )
                .unwrap(),
            ),
        ])
    }

    fn xnys_calendar_week_daily_frames() -> BTreeMap<String, DataFrame> {
        let dates = [
            "2024-03-08",
            "2024-03-11",
            "2024-03-12",
            "2024-03-13",
            "2024-03-14",
            "2024-03-15",
            "2024-03-18",
        ];
        let opens = [
            "2024-03-08T14:30:00Z",
            "2024-03-11T13:30:00Z",
            "2024-03-12T13:30:00Z",
            "2024-03-13T13:30:00Z",
            "2024-03-14T13:30:00Z",
            "2024-03-15T13:30:00Z",
            "2024-03-18T13:30:00Z",
        ];
        let closes = [
            "2024-03-08T21:00:00Z",
            "2024-03-11T20:00:00Z",
            "2024-03-12T20:00:00Z",
            "2024-03-13T20:00:00Z",
            "2024-03-14T20:00:00Z",
            "2024-03-15T20:00:00Z",
            "2024-03-18T20:00:00Z",
        ];
        BTreeMap::from([
            (
                "open".to_string(),
                df!("Time" => &dates, "QQQ" => &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0])
                    .unwrap(),
            ),
            (
                "high".to_string(),
                df!("Time" => &dates, "QQQ" => &[102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0])
                    .unwrap(),
            ),
            (
                "low".to_string(),
                df!("Time" => &dates, "QQQ" => &[99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
                    .unwrap(),
            ),
            (
                "close".to_string(),
                df!("Time" => &dates, "QQQ" => &[101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0])
                    .unwrap(),
            ),
            (
                "volume".to_string(),
                df!("Time" => &dates, "QQQ" => &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0])
                    .unwrap(),
            ),
            (
                "execution_timeline".to_string(),
                df!(
                    "Time" => &dates,
                    "external_execution_sequence" => &[1_u64, 2, 3, 4, 5, 6, 7],
                    "bar_open_timestamp" => &opens,
                    "bar_close_timestamp" => &closes,
                    "available_timestamp" => &closes,
                    "session_label" => &dates,
                )
                .unwrap(),
            ),
        ])
    }

    fn xnys_calendar_month_daily_frames() -> BTreeMap<String, DataFrame> {
        let dates = ["2024-01-31", "2024-02-01", "2024-02-02", "2024-03-01"];
        let opens = [
            "2024-01-31T14:30:00Z",
            "2024-02-01T14:30:00Z",
            "2024-02-02T14:30:00Z",
            "2024-03-01T14:30:00Z",
        ];
        let closes = [
            "2024-01-31T21:00:00Z",
            "2024-02-01T21:00:00Z",
            "2024-02-02T21:00:00Z",
            "2024-03-01T21:00:00Z",
        ];
        BTreeMap::from([
            (
                "open".to_string(),
                df!("Time" => &dates, "QQQ" => &[100.0, 101.0, 102.0, 103.0]).unwrap(),
            ),
            (
                "high".to_string(),
                df!("Time" => &dates, "QQQ" => &[102.0, 103.0, 104.0, 105.0]).unwrap(),
            ),
            (
                "low".to_string(),
                df!("Time" => &dates, "QQQ" => &[99.0, 100.0, 101.0, 102.0]).unwrap(),
            ),
            (
                "close".to_string(),
                df!("Time" => &dates, "QQQ" => &[101.0, 102.0, 103.0, 104.0]).unwrap(),
            ),
            (
                "volume".to_string(),
                df!("Time" => &dates, "QQQ" => &[1_000.0, 1_100.0, 1_200.0, 1_300.0]).unwrap(),
            ),
            (
                "execution_timeline".to_string(),
                df!(
                    "Time" => &dates,
                    "external_execution_sequence" => &[1_u64, 2, 3, 4],
                    "bar_open_timestamp" => &opens,
                    "bar_close_timestamp" => &closes,
                    "available_timestamp" => &closes,
                    "session_label" => &dates,
                )
                .unwrap(),
            ),
        ])
    }

    #[test]
    fn direct_daily_preparation_binds_decision_to_execution_without_aggregation() {
        let request = direct_daily_request();
        let bundle = direct_daily_bundle();
        let prepared =
            prepare_runtime_streams_from_frames(&request, &bundle, &daily_frames()).unwrap();

        assert!(!prepared.aggregated);
        assert_eq!(prepared.execution_stream_id, "execution_daily");
        assert_eq!(prepared.decision_stream_id, "execution_daily");
        assert_eq!(prepared.execution_bars_by_symbol["QQQ"].len(), 1);
        assert_eq!(prepared.decision_bars_by_symbol["QQQ"].len(), 1);
        assert_eq!(
            prepared.decision_bars_by_symbol["QQQ"][0].next_execution_index,
            None
        );
    }

    #[test]
    fn terminal_signal_is_not_mapped_into_the_execution_kernel() {
        let request = next_open_signal_request(direct_daily_request());
        let mut bundle = direct_daily_bundle();
        bundle.row_count = 2;
        for table in bundle.tables.values_mut() {
            table.row_count = 2;
        }
        let mut second_session = bundle.session_windows[0].clone();
        second_session.session_label = "2024-07-05".to_string();
        second_session.open_timestamp = "2024-07-05T13:30:00Z".to_string();
        second_session.close_timestamp = "2024-07-05T20:00:00Z".to_string();
        bundle.session_windows.push(second_session);
        let prepared =
            prepare_runtime_streams_from_frames(&request, &bundle, &two_session_daily_frames())
                .unwrap();
        let decisions = &prepared.decision_bars_by_symbol["QQQ"];
        let candidate = SingleAssetSignalCandidateInput {
            candidate_id: "terminal_contract:single_backtest:fixed".to_string(),
            resolved_params: BTreeMap::new(),
            entry_signal: vec![true, false],
            exit_signal: vec![false, false],
            target_weight: 1.0,
        };
        let remapped =
            remap_signal_candidate_to_execution(candidate.clone(), decisions, 2).unwrap();
        assert_eq!(remapped.entry_signal, vec![true, false]);

        let mut terminal_signal = candidate;
        terminal_signal.entry_signal[1] = true;
        let remapped_terminal =
            remap_signal_candidate_to_execution(terminal_signal, decisions, 2).unwrap();
        assert_eq!(remapped_terminal.entry_signal, vec![true, false]);
    }

    #[test]
    fn signal_bar_time_audit_rejects_unsupported_risk_generated_fill_profiles_upfront() {
        let mut drawdown = next_open_signal_request(direct_daily_request());
        drawdown.simulation.risk["max_drawdown"] = serde_json::json!(0.1);
        let error = validate_next_open_signal_actions(&drawdown).unwrap_err();
        assert!(matches!(error, EngineRuntimeError::UnsupportedProfile(_)));
        assert!(error.to_string().contains("risk-generated"));

        let mut margin = next_open_signal_request(direct_daily_request());
        margin.simulation.account.account_type = crate::engine_request::AccountType::Margin;
        let error = validate_next_open_signal_actions(&margin).unwrap_err();
        assert!(matches!(error, EngineRuntimeError::UnsupportedProfile(_)));
        assert!(error.to_string().contains("maintenance-margin-generated"));
    }

    #[test]
    fn direct_daily_active_signal_uses_validated_next_execution_open_audit() {
        let request = next_open_signal_request(direct_daily_request());
        let mut bundle = direct_daily_bundle();
        bundle.row_count = 2;
        for table in bundle.tables.values_mut() {
            table.row_count = 2;
        }
        let mut second_session = bundle.session_windows[0].clone();
        second_session.session_label = "2024-07-05".to_string();
        second_session.open_timestamp = "2024-07-05T13:30:00Z".to_string();
        second_session.close_timestamp = "2024-07-05T20:00:00Z".to_string();
        bundle.session_windows.push(second_session);
        let prepared =
            prepare_runtime_streams_from_frames(&request, &bundle, &two_session_daily_frames())
                .unwrap();

        let result = execute_single_asset_signal(
            EngineRequestExecutionInput {
                engine_request: request,
                market_data_bundle: bundle,
                artifact_output_dir: None,
                artifact_run_id: None,
            },
            &prepared,
        )
        .unwrap();

        let audit = &result["bar_time_audit"];
        assert_eq!(audit["aggregated"], false);
        assert_eq!(audit["mapping_count"], 1);
        assert_eq!(audit["terminal_decision_count"], 1);
        assert_eq!(
            audit["mappings"][0]["decision_event_time"],
            "2024-07-03T17:00:00Z"
        );
        assert_eq!(
            audit["mappings"][0]["execution_bar_open_time"],
            "2024-07-05T13:30:00Z"
        );
        assert_eq!(audit["mappings"][0]["external_execution_sequence"], 2);
        assert_eq!(audit["mappings"][0]["execution_open_price"], 110.0);
        assert_eq!(result["results"][0]["active_rebalances"], 1);
    }

    #[test]
    fn grouped_signal_batch_keeps_candidate_specific_audits_and_validation() {
        let mut first = next_open_signal_request(direct_daily_request());
        first.request_id = "request-active".to_string();
        first.strategy.base_strategy_id = "candidate-active".to_string();
        first.strategy.strategy_id = "candidate-active:single_backtest:fixed".to_string();
        first.strategy.decision_plan.signals["entry"]["right"] = serde_json::json!(200.0);
        first.request_hash = first.computed_hash().unwrap();
        let mut second = first.clone();
        second.request_id = "request-inactive".to_string();
        second.strategy.base_strategy_id = "candidate-inactive".to_string();
        second.strategy.strategy_id = "candidate-inactive:single_backtest:fixed".to_string();
        second.strategy.decision_plan.signals["entry"]["right"] = serde_json::json!(100.0);
        second.request_hash = second.computed_hash().unwrap();

        let mut bundle = direct_daily_bundle();
        bundle.row_count = 2;
        bundle.time_range.start = "2024-07-03".to_string();
        bundle.time_range.end = "2024-07-05".to_string();
        for table in bundle.tables.values_mut() {
            table.row_count = 2;
        }
        let mut second_session = bundle.session_windows[0].clone();
        second_session.session_label = "2024-07-05".to_string();
        second_session.open_timestamp = "2024-07-05T13:30:00Z".to_string();
        second_session.close_timestamp = "2024-07-05T20:00:00Z".to_string();
        bundle.session_windows.push(second_session);
        let temp =
            std::env::temp_dir().join(format!("lo2cin4bt-grouped-signal-{}", std::process::id()));
        std::fs::create_dir_all(&temp).unwrap();
        for (name, frame) in two_session_daily_frames() {
            let path = temp.join(format!("{name}.parquet"));
            let mut frame = frame;
            ParquetWriter::new(File::create(&path).unwrap())
                .finish(&mut frame)
                .unwrap();
            bundle.tables.get_mut(&name).unwrap().path = Some(path.to_string_lossy().to_string());
        }
        bundle.content_hash = bundle.computed_content_hash().unwrap();
        bundle.bundle_id = format!("mdb-{}", &bundle.content_hash[..16]);

        let result = execute_engine_request_batch(EngineRequestBatchExecutionInput {
            engine_requests: vec![first, second],
            market_data_bundle: bundle,
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .unwrap();
        assert_eq!(result["execution_mode"], "grouped");
        assert_eq!(result["shape"], "signal_timeline");
        assert_eq!(result["derived_bar_cache"]["build_count"], 1);
        assert_eq!(result["derived_bar_cache"]["candidate_count"], 2);
        assert_eq!(result["derived_bar_cache"]["enabled"], false);
        assert!(result["derived_bar_cache"]["market_data_bundle_hash"]
            .as_str()
            .is_some_and(|value| value.len() == 64));
        assert!(result["derived_bar_cache"]["stream_graph_hash"]
            .as_str()
            .is_some_and(|value| value.len() == 64));
        assert_eq!(result["bar_time_audits"].as_array().unwrap().len(), 2);
        assert_eq!(
            result["bar_time_audits"][0]["candidate_id"],
            "candidate-active:single_backtest:fixed"
        );
        assert_eq!(
            result["bar_time_audits"][0]["audit"]["mappings"][0]["signal_action"],
            "enter"
        );
        assert_eq!(
            result["bar_time_audits"][0]["audit"]["terminal_decisions"][0]["status"],
            "skipped"
        );
        assert_eq!(
            result["bar_time_audits"][0]["audit"]["terminal_decisions"][0]["reason"],
            "no_eligible_next_execution_bar"
        );
        assert!(
            result["bar_time_audits"][0]["audit"]["terminal_decisions"][0]["action_id"].is_null()
        );
        assert!(
            result["bar_time_audits"][0]["audit"]["terminal_decisions"][0]["eligible_fill_id"]
                .is_null()
        );
        assert!(result["bar_time_audits"][1]["audit"]["mappings"][0]["signal_action"].is_null());
        assert_eq!(
            result["bar_time_audits"][1]["audit"]["terminal_decisions"][0]["status"],
            "no_signal"
        );
        for candidate in result["result"]["results"].as_array().unwrap() {
            let checks = candidate["result_validation"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|check| check["check_id"] == "bar_time_no_look_ahead")
                .count();
            assert_eq!(checks, 1);
        }
        std::fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn one_minute_to_five_minute_decision_maps_to_strict_next_one_minute_open() {
        let request = direct_daily_request();
        let mut value = serde_json::to_value(request).unwrap();
        let streams = value["data_requirements"]["bar_time"]["streams"]
            .as_array_mut()
            .unwrap();
        streams[0]["stream_id"] = serde_json::json!("execution_1m");
        streams[0]["bar_spec"]["unit"] = serde_json::json!("minute");
        streams.push(serde_json::json!({
            "stream_id": "decision_5m",
            "role": "decision",
            "source": {
                "kind": "derived",
                "parent_stream_id": "execution_1m",
                "aggregation_engine": "shared_rust",
                "empty_bar_policy": "omit",
                "partial_first_bar_policy": "omit",
                "partial_final_bar_policy": "omit"
            },
            "bar_spec": {
                "aggregation": "time",
                "step": 5,
                "unit": "minute",
                "price_type": "last",
                "alignment": "session_open"
            },
            "timestamp_semantics": {
                "timestamp_convention": "bar_close",
                "interval_boundary": "left_open_right_closed",
                "bar_open_time_column": "bar_open_timestamp",
                "bar_close_time_column": "bar_close_timestamp",
                "available_time_column": "available_timestamp",
                "session_label_column": "session_label",
                "availability_policy": "bar_close"
            }
        }));
        value["strategy"]["stream_binding"] = serde_json::json!({
            "execution_stream_id": "execution_1m",
            "decision_stream_id": "decision_5m"
        });
        let request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        let request = next_open_signal_request(request);

        let mut bundle = direct_daily_bundle();
        bundle.execution_stream.stream_id = "execution_1m".to_string();
        bundle.execution_stream.row_key_kind = MarketDataIndexKind::EventTimestamp;
        bundle.execution_stream.bar_spec.step = 1;
        bundle.execution_stream.bar_spec.unit = ContractBarUnitV1::Minute;
        bundle.row_count = 6;
        bundle.session_windows[0].close_timestamp = "2024-07-03T13:36:00Z".to_string();
        for table in bundle.tables.values_mut() {
            table.row_count = 6;
        }
        let row_keys = [
            "2024-07-03T13:31:00Z",
            "2024-07-03T13:32:00Z",
            "2024-07-03T13:33:00Z",
            "2024-07-03T13:34:00Z",
            "2024-07-03T13:35:00Z",
            "2024-07-03T13:36:00Z",
        ];
        let frames = BTreeMap::from([
            (
                "open".to_string(),
                df!("Time" => &row_keys, "QQQ" => &[100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
                    .unwrap(),
            ),
            (
                "high".to_string(),
                df!("Time" => &row_keys, "QQQ" => &[101.0, 102.0, 103.0, 104.0, 105.0, 106.0])
                    .unwrap(),
            ),
            (
                "low".to_string(),
                df!("Time" => &row_keys, "QQQ" => &[99.0, 100.0, 101.0, 102.0, 103.0, 104.0])
                    .unwrap(),
            ),
            (
                "close".to_string(),
                df!("Time" => &row_keys, "QQQ" => &[100.5, 101.5, 102.5, 103.5, 104.5, 105.5])
                    .unwrap(),
            ),
            (
                "volume".to_string(),
                df!("Time" => &row_keys, "QQQ" => &[10.0, 11.0, 12.0, 13.0, 14.0, 15.0]).unwrap(),
            ),
            (
                "execution_timeline".to_string(),
                df!(
                    "Time" => &row_keys,
                    "external_execution_sequence" => &[1_u64, 2, 3, 4, 5, 6],
                    "bar_open_timestamp" => &[
                        "2024-07-03T13:30:00Z",
                        "2024-07-03T13:31:00Z",
                        "2024-07-03T13:32:00Z",
                        "2024-07-03T13:33:00Z",
                        "2024-07-03T13:34:00Z",
                        "2024-07-03T13:35:00Z",
                    ],
                    "bar_close_timestamp" => &row_keys,
                    "available_timestamp" => &row_keys,
                    "session_label" => &[
                        "2024-07-03",
                        "2024-07-03",
                        "2024-07-03",
                        "2024-07-03",
                        "2024-07-03",
                        "2024-07-03",
                    ],
                )
                .unwrap(),
            ),
        ]);

        let prepared = prepare_runtime_streams_from_frames(&request, &bundle, &frames).unwrap();
        let decisions = &prepared.decision_bars_by_symbol["QQQ"];
        assert!(prepared.aggregated);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].event_timestamp, "2024-07-03T13:35:00Z");
        assert_eq!(decisions[0].next_execution_index, Some(5));
        assert_eq!(
            prepared.execution_bars_by_symbol["QQQ"][5].bar_open_timestamp,
            "2024-07-03T13:35:00Z"
        );

        let result = execute_single_asset_signal(
            EngineRequestExecutionInput {
                engine_request: request,
                market_data_bundle: bundle,
                artifact_output_dir: None,
                artifact_run_id: None,
            },
            &prepared,
        )
        .unwrap();
        let audit = &result["bar_time_audit"];
        assert_eq!(audit["aggregated"], true);
        assert_eq!(audit["mapping_count"], 1);
        assert_eq!(audit["mappings"][0]["source_count"], 5);
        assert_eq!(
            audit["mappings"][0]["source_first_external_execution_sequence"],
            1
        );
        assert_eq!(audit["mappings"][0]["decision_source_sequence"], 5);
        assert_eq!(
            audit["mappings"][0]["execution_bar_open_time"],
            "2024-07-03T13:35:00Z"
        );
        assert_eq!(audit["mappings"][0]["external_execution_sequence"], 6);
        assert_eq!(audit["mappings"][0]["execution_open_price"], 105.0);
        assert_eq!(result["results"][0]["active_rebalances"], 1);
        let bar_time_check = result["results"][0]["result_validation"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["check_id"] == "bar_time_no_look_ahead")
            .unwrap();
        assert_eq!(bar_time_check["status"], "passed");
        assert!(bar_time_check["message"]
            .as_str()
            .unwrap()
            .contains("1 ordering/eligibility rows"));
    }

    #[test]
    fn one_minute_to_five_minute_to_hour_to_session_executes_at_next_external_open() {
        let request = multi_level_session_request();
        let mut bundle = direct_daily_bundle();
        bundle.execution_stream.stream_id = "execution_1m".to_string();
        bundle.execution_stream.row_key_kind = MarketDataIndexKind::EventTimestamp;
        bundle.execution_stream.bar_spec.step = 1;
        bundle.execution_stream.bar_spec.unit = ContractBarUnitV1::Minute;
        bundle.row_count = 600;
        bundle.time_range.start = "2024-07-02T13:31:00Z".to_string();
        bundle.time_range.end = "2024-07-03T17:00:00Z".to_string();
        bundle.session_windows = vec![
            crate::MarketDataSessionWindowV2 {
                session_label: "2024-07-02".to_string(),
                open_timestamp: "2024-07-02T13:30:00Z".to_string(),
                close_timestamp: "2024-07-02T20:00:00Z".to_string(),
            },
            crate::MarketDataSessionWindowV2 {
                session_label: "2024-07-03".to_string(),
                open_timestamp: "2024-07-03T13:30:00Z".to_string(),
                close_timestamp: "2024-07-03T17:00:00Z".to_string(),
            },
        ];
        for table in bundle.tables.values_mut() {
            table.row_count = 600;
        }
        let frames = xnys_normal_and_half_day_minute_frames();
        let prepared = prepare_runtime_streams_from_frames(&request, &bundle, &frames).unwrap();
        let decisions = &prepared.decision_bars_by_symbol["QQQ"];
        let golden: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/golden/canonical_pipeline_golden_v1.json"
        ))
        .unwrap();
        let expected = &golden["phase2_multitimeframe"];
        let graph = request
            .data_requirements
            .bar_time
            .streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<Vec<_>>()
            .join("->");
        assert_eq!(graph, expected["graph"]);
        let one_hour_policy = request
            .data_requirements
            .bar_time
            .streams
            .iter()
            .find(|stream| stream.stream_id == "derived_1h")
            .and_then(|stream| match &stream.source {
                BarStreamSourceV1::Derived {
                    partial_final_bar_policy,
                    ..
                } => Some(match partial_final_bar_policy {
                    FinalPartialBarPolicyV1::Omit => "omit",
                    FinalPartialBarPolicyV1::Emit => "emit",
                }),
                BarStreamSourceV1::External { .. } => None,
            })
            .unwrap();
        assert_eq!(one_hour_policy, expected["partial_final_policy_1h"]);
        assert_eq!(decisions.len(), 2);
        assert_eq!(decisions[0].aggregation_lineage.len(), 3);
        assert_eq!(
            decisions[0]
                .aggregation_lineage
                .iter()
                .map(|lineage| lineage.parent_bar_count)
                .collect::<Vec<_>>(),
            vec![390, 78, 7]
        );
        assert_eq!(
            decisions[1]
                .aggregation_lineage
                .iter()
                .map(|lineage| lineage.parent_bar_count)
                .collect::<Vec<_>>(),
            vec![210, 42, 4]
        );
        assert_eq!(
            decisions
                .iter()
                .map(|decision| decision.source_count)
                .collect::<Vec<_>>(),
            vec![390, 210]
        );
        assert_eq!(decisions[0].open, 100.0);
        assert!((decisions[0].high - 103.91).abs() < 1e-12);
        assert!((decisions[0].low - 99.98).abs() < 1e-12);
        assert!((decisions[0].close - 103.9).abs() < 1e-12);
        assert_eq!(
            decisions[0].volume,
            (100..490).map(|value| value as f64).sum::<f64>()
        );
        assert_eq!(decisions[0].event_timestamp, "2024-07-02T20:00:00Z");
        assert_eq!(decisions[0].available_timestamp, "2024-07-02T20:00:00Z");
        assert!((decisions[1].open - 103.9).abs() < 1e-12);
        assert!((decisions[1].high - 106.01).abs() < 1e-12);
        assert!((decisions[1].low - 103.88).abs() < 1e-12);
        assert!((decisions[1].close - 106.0).abs() < 1e-12);
        assert_eq!(
            decisions[1].volume,
            (100..310).map(|value| value as f64).sum::<f64>()
        );
        assert_eq!(decisions[1].event_timestamp, "2024-07-03T17:00:00Z");
        assert_eq!(decisions[1].available_timestamp, "2024-07-03T17:00:00Z");
        assert_eq!(decisions[0].next_execution_index, Some(390));
        let next_execution_index = decisions[0].next_execution_index.unwrap();
        let next_execution = &prepared.execution_bars_by_symbol["QQQ"][next_execution_index];
        assert_eq!(
            next_execution_index,
            expected["next_execution"]["index"].as_u64().unwrap() as usize
        );
        assert_eq!(
            next_execution.bar_open_timestamp,
            expected["next_execution"]["open_timestamp"]
        );
        assert_eq!(
            next_execution.external_execution_sequence,
            expected["next_execution"]["external_execution_sequence"]
        );
        assert!(
            (next_execution.open - expected["next_execution"]["open_price"].as_f64().unwrap())
                .abs()
                < 1e-12
        );
        assert_eq!(
            next_execution.session_label,
            expected["next_execution"]["session_label"]
        );
        for (index, decision) in decisions.iter().enumerate() {
            let expected_decision = &expected["decisions"][index];
            assert_eq!(decision.session_label, expected_decision["session_label"]);
            for (actual, field) in [
                (decision.open, "open"),
                (decision.high, "high"),
                (decision.low, "low"),
                (decision.close, "close"),
                (decision.volume, "volume"),
            ] {
                assert!(
                    (actual - expected_decision[field].as_f64().unwrap()).abs() < 1e-12,
                    "golden decision {index} field {field} differs"
                );
            }
            assert_eq!(
                decision.event_timestamp,
                expected_decision["event_timestamp"]
            );
            assert_eq!(
                decision.available_timestamp,
                expected_decision["available_timestamp"]
            );
            assert_eq!(decision.source_count, expected_decision["source_count"]);
            assert_eq!(
                serde_json::json!(decision
                    .aggregation_lineage
                    .iter()
                    .map(|lineage| lineage.parent_bar_count)
                    .collect::<Vec<_>>()),
                expected_decision["lineage_parent_bar_counts"]
            );
            assert_eq!(
                serde_json::json!(decision
                    .aggregation_lineage
                    .iter()
                    .map(|lineage| lineage.external_source_bar_count)
                    .collect::<Vec<_>>()),
                expected_decision["lineage_external_source_bar_counts"]
            );
        }

        let temp =
            std::env::temp_dir().join(format!("lo2cin4bt-multilevel-{}", std::process::id(),));
        std::fs::create_dir_all(&temp).unwrap();
        for (name, frame) in &frames {
            let path = temp.join(format!("{name}.parquet"));
            let mut frame = frame.clone();
            ParquetWriter::new(File::create(&path).unwrap())
                .finish(&mut frame)
                .unwrap();
            bundle.tables.get_mut(name).unwrap().path = Some(path.to_string_lossy().to_string());
        }
        bundle.content_hash = bundle.computed_content_hash().unwrap();
        bundle.bundle_id = format!("mdb-{}", &bundle.content_hash[..16]);

        let result = execute_engine_request(EngineRequestExecutionInput {
            engine_request: request.clone(),
            market_data_bundle: bundle.clone(),
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .unwrap();
        assert_eq!(result["results"][0]["active_rebalances"], 1);
        assert_eq!(
            result["results"][0]["active_rebalances"],
            expected["result"]["active_rebalances"]
        );
        assert_eq!(
            result["bar_time_audit"]["mappings"][0]["execution_bar_open_time"],
            "2024-07-03T13:30:00Z"
        );
        assert_eq!(
            result["bar_time_audit"]["mappings"][0]["aggregation_lineage"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        let events = result["results"][0]["timeline"]["events"]
            .as_array()
            .unwrap();
        let cost_events = events
            .iter()
            .filter(|event| event["trade_cost"].as_f64().unwrap_or(0.0) > 0.0)
            .collect::<Vec<_>>();
        assert_eq!(cost_events.len(), 1);
        assert_eq!(
            cost_events.len(),
            expected["result"]["cost_event_count"].as_u64().unwrap() as usize
        );
        assert_eq!(cost_events[0]["date"], "2024-07-03T13:31:00Z");
        assert_eq!(
            cost_events[0]["date"],
            expected["result"]["cost_event_timestamp"]
        );
        assert_eq!(
            prepared.execution_bars_by_symbol["QQQ"][390].session_label,
            "2024-07-03"
        );
        let turnover = cost_events[0]["turnover"].as_f64().unwrap();
        let equity_before_trade = cost_events[0]["equity_before_trade"].as_f64().unwrap();
        let actual_cost_rate = request.simulation.fill_model["cost"]["transaction_cost"]
            .as_f64()
            .unwrap()
            + request.simulation.fill_model["cost"]["slippage"]
                .as_f64()
                .unwrap();
        assert!(
            (actual_cost_rate - expected["result"]["cost_rate"].as_f64().unwrap()).abs() < 1e-12
        );
        let expected_trade_cost = equity_before_trade * turnover * actual_cost_rate;
        assert!(
            (cost_events[0]["trade_cost"].as_f64().unwrap() - expected_trade_cost).abs() < 1e-12
        );
        assert!((turnover - expected["result"]["turnover"].as_f64().unwrap()).abs() < 1e-12);
        assert!(
            (cost_events[0]["trade_cost"].as_f64().unwrap()
                - expected["result"]["trade_cost"].as_f64().unwrap())
            .abs()
                < 1e-12
        );
        assert!(cost_events[0]["actions"][0]["orders"]
            .as_array()
            .is_some_and(|orders| !orders.is_empty()));
        let fill_id = result["bar_time_audit"]["mappings"][0]["eligible_fill_id"]
            .as_str()
            .unwrap();
        assert_eq!(fill_id, expected["result"]["eligible_fill_id"]);
        assert_eq!(
            cost_events[0]["actions"][0]["orders"][0]["order_id"],
            fill_id
        );
        let settlements = result["results"][0]["timeline"]["result_tables"]["settlements"]
            .as_array()
            .unwrap();
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0]["Order_id"], fill_id);
        assert_eq!(settlements[0]["Asset"], "QQQ");
        assert_eq!(settlements[0]["Status"], "settled");
        assert_eq!(
            settlements[0]["Status"],
            expected["result"]["settlement_status"]
        );
        assert_eq!(
            result["bar_time_audit"]["terminal_decisions"][0]["status"],
            expected["result"]["terminal_status"]
        );
        assert_eq!(
            result["bar_time_audit"]["terminal_decisions"][0]["signal_action"],
            expected["result"]["terminal_signal_action"]
        );
        let check = result["results"][0]["result_validation"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["check_id"] == "bar_time_no_look_ahead")
            .unwrap();
        assert_eq!(check["status"], expected["result"]["bar_time_check_status"]);

        let artifact_dir = temp.join("artifacts");
        let artifact_result = execute_engine_request(EngineRequestExecutionInput {
            engine_request: request,
            market_data_bundle: bundle,
            artifact_output_dir: Some(artifact_dir.to_string_lossy().to_string()),
            artifact_run_id: Some("multilevel".to_string()),
        })
        .unwrap();
        assert!(artifact_result["artifact_bundle"].is_object());
        assert!(artifact_result["results"][0]["timeline"].is_null());
        assert_eq!(
            artifact_result["results"][0]["result_validation"]["checks"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|check| check["check_id"] == "bar_time_no_look_ahead")
                .count(),
            1
        );
        std::fs::remove_dir_all(&temp).unwrap();
    }

    #[test]
    fn direct_daily_to_calendar_week_preserves_lineage_and_next_session_fill() {
        let request = calendar_week_request();
        let mut bundle = direct_daily_bundle();
        let frames = xnys_calendar_week_daily_frames();
        let dates = [
            "2024-03-08",
            "2024-03-11",
            "2024-03-12",
            "2024-03-13",
            "2024-03-14",
            "2024-03-15",
            "2024-03-18",
        ];
        let opens = [
            "2024-03-08T14:30:00Z",
            "2024-03-11T13:30:00Z",
            "2024-03-12T13:30:00Z",
            "2024-03-13T13:30:00Z",
            "2024-03-14T13:30:00Z",
            "2024-03-15T13:30:00Z",
            "2024-03-18T13:30:00Z",
        ];
        let closes = [
            "2024-03-08T21:00:00Z",
            "2024-03-11T20:00:00Z",
            "2024-03-12T20:00:00Z",
            "2024-03-13T20:00:00Z",
            "2024-03-14T20:00:00Z",
            "2024-03-15T20:00:00Z",
            "2024-03-18T20:00:00Z",
        ];
        bundle.row_count = dates.len();
        bundle.time_range.start = dates[0].to_string();
        bundle.time_range.end = dates[dates.len() - 1].to_string();
        bundle.session_windows = dates
            .iter()
            .zip(opens)
            .zip(closes)
            .map(|((session_label, open_timestamp), close_timestamp)| {
                crate::MarketDataSessionWindowV2 {
                    session_label: (*session_label).to_string(),
                    open_timestamp: open_timestamp.to_string(),
                    close_timestamp: close_timestamp.to_string(),
                }
            })
            .collect();
        for table in bundle.tables.values_mut() {
            table.row_count = dates.len();
        }

        let prepared = prepare_runtime_streams_from_frames(&request, &bundle, &frames).unwrap();
        let decisions = &prepared.decision_bars_by_symbol["QQQ"];

        assert!(prepared.aggregated);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].session_label, "2024-03-15");
        assert_eq!(decisions[0].bar_open_timestamp, "2024-03-11T13:30:00Z");
        assert_eq!(decisions[0].event_timestamp, "2024-03-15T20:00:00Z");
        assert_eq!(decisions[0].source_count, 5);
        assert_eq!(decisions[0].aggregation_lineage.len(), 1);
        assert_eq!(decisions[0].aggregation_lineage[0].parent_bar_count, 5);
        assert_eq!(decisions[0].next_execution_index, Some(6));
        assert_eq!(
            prepared.execution_bars_by_symbol["QQQ"][6].bar_open_timestamp,
            "2024-03-18T13:30:00Z"
        );
    }

    #[test]
    fn direct_daily_to_calendar_month_runs_through_validator_with_next_session_fill() {
        let request = calendar_month_request();
        let mut bundle = direct_daily_bundle();
        let frames = xnys_calendar_month_daily_frames();
        let dates = ["2024-01-31", "2024-02-01", "2024-02-02", "2024-03-01"];
        let opens = [
            "2024-01-31T14:30:00Z",
            "2024-02-01T14:30:00Z",
            "2024-02-02T14:30:00Z",
            "2024-03-01T14:30:00Z",
        ];
        let closes = [
            "2024-01-31T21:00:00Z",
            "2024-02-01T21:00:00Z",
            "2024-02-02T21:00:00Z",
            "2024-03-01T21:00:00Z",
        ];
        bundle.row_count = dates.len();
        bundle.time_range.start = dates[0].to_string();
        bundle.time_range.end = dates[dates.len() - 1].to_string();
        bundle.session_windows = dates
            .iter()
            .zip(opens)
            .zip(closes)
            .map(|((session_label, open_timestamp), close_timestamp)| {
                crate::MarketDataSessionWindowV2 {
                    session_label: (*session_label).to_string(),
                    open_timestamp: open_timestamp.to_string(),
                    close_timestamp: close_timestamp.to_string(),
                }
            })
            .collect();
        for table in bundle.tables.values_mut() {
            table.row_count = dates.len();
        }

        let prepared = prepare_runtime_streams_from_frames(&request, &bundle, &frames).unwrap();
        let decisions = &prepared.decision_bars_by_symbol["QQQ"];

        assert!(prepared.aggregated);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].session_label, "2024-02-02");
        assert_eq!(decisions[0].bar_open_timestamp, "2024-02-01T14:30:00Z");
        assert_eq!(decisions[0].event_timestamp, "2024-02-02T21:00:00Z");
        assert_eq!(decisions[0].source_count, 2);
        assert_eq!(decisions[0].aggregation_lineage[0].parent_bar_count, 2);
        assert!(!decisions[0].aggregation_lineage[0].partial);
        assert_eq!(decisions[0].next_execution_index, Some(3));
        assert_eq!(
            prepared.execution_bars_by_symbol["QQQ"][3].bar_open_timestamp,
            "2024-03-01T14:30:00Z"
        );

        let temp =
            std::env::temp_dir().join(format!("lo2cin4bt-calendar-month-{}", std::process::id()));
        std::fs::create_dir_all(&temp).unwrap();
        for (name, frame) in &frames {
            let path = temp.join(format!("{name}.parquet"));
            let mut frame = frame.clone();
            ParquetWriter::new(File::create(&path).unwrap())
                .finish(&mut frame)
                .unwrap();
            bundle.tables.get_mut(name).unwrap().path = Some(path.to_string_lossy().to_string());
        }
        bundle.content_hash = bundle.computed_content_hash().unwrap();
        bundle.bundle_id = format!("mdb-{}", &bundle.content_hash[..16]);

        let result = execute_engine_request(EngineRequestExecutionInput {
            engine_request: request,
            market_data_bundle: bundle,
            artifact_output_dir: None,
            artifact_run_id: None,
        })
        .unwrap();
        assert_eq!(result["results"][0]["active_rebalances"], 1);
        assert_eq!(
            result["bar_time_audit"]["mappings"][0]["decision_event_time"],
            "2024-02-02T21:00:00Z"
        );
        assert_eq!(
            result["bar_time_audit"]["mappings"][0]["execution_bar_open_time"],
            "2024-03-01T14:30:00Z"
        );
        let check = result["results"][0]["result_validation"]["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["check_id"] == "bar_time_no_look_ahead")
            .unwrap();
        assert_eq!(check["status"], "passed");
        std::fs::remove_dir_all(&temp).unwrap();
    }

    #[test]
    fn bar_time_audit_rejects_same_or_earlier_execution_sequence_mapping() {
        let request = next_open_signal_request(direct_daily_request());
        let mut bundle = direct_daily_bundle();
        bundle.row_count = 2;
        for table in bundle.tables.values_mut() {
            table.row_count = 2;
        }
        let mut second_session = bundle.session_windows[0].clone();
        second_session.session_label = "2024-07-05".to_string();
        second_session.open_timestamp = "2024-07-05T13:30:00Z".to_string();
        second_session.close_timestamp = "2024-07-05T20:00:00Z".to_string();
        bundle.session_windows.push(second_session);
        let mut prepared =
            prepare_runtime_streams_from_frames(&request, &bundle, &two_session_daily_frames())
                .unwrap();

        prepared.decision_bars_by_symbol.get_mut("QQQ").unwrap()[0].next_execution_index = Some(0);
        let same_bar_error = prepared.validate().unwrap_err();
        assert!(same_bar_error.to_string().contains("not strictly later"));

        prepared.decision_bars_by_symbol.get_mut("QQQ").unwrap()[0].next_execution_index = Some(1);
        prepared.decision_bars_by_symbol.get_mut("QQQ").unwrap()[1].next_execution_index = Some(0);
        let earlier_sequence_error = prepared.validate().unwrap_err();
        assert!(earlier_sequence_error
            .to_string()
            .contains("not strictly later"));
    }

    #[test]
    fn derived_binding_is_rejected_for_profiles_without_an_active_prepared_route() {
        let request = direct_daily_request();
        let bundle = direct_daily_bundle();
        let mut prepared =
            prepare_runtime_streams_from_frames(&request, &bundle, &daily_frames()).unwrap();
        prepared.aggregated = true;
        prepared.decision_stream_id = "derived_decision".to_string();

        let error = validate_prepared_profile_binding(&request, &prepared).unwrap_err();
        assert!(error
            .to_string()
            .contains("derived decision streams are active only"));
    }

    #[test]
    fn event_timestamp_values_are_preserved_separately_from_session_labels() {
        let frame = df!(
            "Time" => &[
                "2024-07-03T13:31:00Z",
                "2024-07-03T13:32:00Z",
            ],
            "Session" => &["2024-07-03", "2024-07-03"],
        )
        .unwrap();

        assert_eq!(
            time_strings(&frame, "Time", MarketDataIndexKind::EventTimestamp).unwrap(),
            vec![
                "2024-07-03T13:31:00Z".to_string(),
                "2024-07-03T13:32:00Z".to_string(),
            ]
        );
        assert_eq!(
            session_label_strings(&frame, "Session").unwrap(),
            vec!["2024-07-03".to_string(), "2024-07-03".to_string()]
        );
    }

    #[test]
    fn session_label_values_require_exact_daily_string_transport() {
        let frame = df!(
            "Time" => &[
                "2024-01-02",
                "2024-01-03",
            ],
        )
        .unwrap();

        assert_eq!(
            time_strings(&frame, "Time", MarketDataIndexKind::SessionLabel).unwrap(),
            vec!["2024-01-02".to_string(), "2024-01-03".to_string()]
        );
    }

    #[test]
    fn v2_row_keys_reject_datetime_transport_and_noncanonical_utc_offsets() {
        let datetime = Series::new("Time".into(), &[1_704_153_600_000_i64])
            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            .unwrap();
        let datetime_frame = DataFrame::new(1, vec![datetime.into()]).unwrap();
        assert!(
            time_strings(&datetime_frame, "Time", MarketDataIndexKind::SessionLabel)
                .unwrap_err()
                .to_string()
                .contains("v2 String transport")
        );

        let offset_frame = df!("Time" => &["2024-01-02T00:00:00+00:00"]).unwrap();
        assert!(time_strings(&offset_frame, "Time", MarketDataIndexKind::EventTimestamp).is_err());
    }

    #[test]
    fn event_availability_must_be_utc_ordered_and_not_precede_the_event() {
        let frame = df!(
            "Time" => &[
                "2024-07-03T13:31:00Z",
                "2024-07-03T13:32:00Z",
            ],
            "Available" => &[
                "2024-07-03T13:31:00Z",
                "2024-07-03T13:31:59Z",
            ],
        )
        .unwrap();
        let events = time_strings(&frame, "Time", MarketDataIndexKind::EventTimestamp).unwrap();

        assert!(validate_available_timeline(&frame, &events, "Available").is_err());
        assert!(time_strings(
            &df!("Time" => &["2024-07-03 13:31:00"]).unwrap(),
            "Time",
            MarketDataIndexKind::EventTimestamp,
        )
        .is_err());
    }
}
