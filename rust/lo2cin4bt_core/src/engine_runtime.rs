use crate::daily_rank::{compute_feature_fields_with_market_fields, evaluate_condition};
use crate::{
    run_accounting, run_calendar_overlay_batch, run_daily_rank_accounting_batch,
    run_single_asset_calendar_same_session_batch, AccountingConfig, AccountingInput,
    AccountingRiskGateConfig, CalendarOverlayBatchInput, CalendarSameSessionBatchInput,
    CalendarSameSessionCandidateInput, CheckpointInput, DailyRankBatchCandidateInput,
    DailyRankBatchInput, DailyRankConditionInput, DailyRankFeatureSpec, DecisionPlanV1,
    EngineRequestV1, MarketDataBundleV1, OperationId, ResetTimerBatchInput,
    ResetTimerCandidateInput, SingleAssetSignalBatchInput, SingleAssetSignalCandidateInput,
    TimelineAccountingConfig, TimelinePositionPolicy,
};
use polars::io::parquet::read::ParquetReader;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestExecutionInput {
    pub engine_request: EngineRequestV1,
    pub market_data_bundle: MarketDataBundleV1,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestBatchExecutionInput {
    pub engine_requests: Vec<EngineRequestV1>,
    pub market_data_bundle: MarketDataBundleV1,
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
        execute_single_asset_signal(input)
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
        .all(is_single_asset_signal_request)
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

fn execute_signal_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    validate_batch_bundle_and_requests(&input)?;
    if input.market_data_bundle.symbols.len() != 1 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "cross signal execution requires one symbol".to_string(),
        ));
    }
    let config = identical_timeline_config(&input.engine_requests)?;
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
    let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
    let asset = input.market_data_bundle.symbols[0].clone();
    let close = table_price_column(&close_frame, &asset)?;
    let market_fields = feature_market_fields(
        &input.market_data_bundle,
        &close_frame,
        required_market_field_names(input.engine_requests.iter()),
    )?;
    let candidates = input
        .engine_requests
        .iter()
        .map(|request| {
            single_signal_candidate(request, &close, &market_fields, request.request_id.clone())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request_ids = input
        .engine_requests
        .iter()
        .map(|request| request.request_id.clone())
        .collect::<Vec<_>>();
    let summary = crate::run_single_asset_next_open_signal_batch(SingleAssetSignalBatchInput {
        config,
        asset: asset.clone(),
        dates,
        open: table_price_column(&open_frame, &asset)?,
        close,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates,
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    Ok(serde_json::json!({
        "request_count": request_ids.len(),
        "execution_mode": "grouped",
        "shape": "signal_timeline",
        "request_ids": request_ids,
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
    let config = identical_timeline_config(&input.engine_requests)?;
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
    let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
    let asset = input.market_data_bundle.symbols[0].clone();
    let candidates = input
        .engine_requests
        .iter()
        .map(|request| calendar_same_session_candidate(request, request.request_id.clone()))
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

fn execute_reset_timer_request_batch(
    input: EngineRequestBatchExecutionInput,
) -> Result<Value, EngineRuntimeError> {
    validate_batch_bundle_and_requests(&input)?;
    let config = identical_timeline_config(&input.engine_requests)?;
    let close_frame = read_bundle_table(&input.market_data_bundle, "close")?;
    let open_frame = read_bundle_table(&input.market_data_bundle, "open")?;
    let dates = aligned_dates(&input.market_data_bundle, &close_frame, &open_frame)?;
    let first_request = input.engine_requests.first().ok_or_else(|| {
        EngineRuntimeError::InvalidRequest("engine_requests must not be empty".to_string())
    })?;
    let reset_spec = reset_timer_spec(first_request)?;
    let feature_frame = read_bundle_table(&input.market_data_bundle, &reset_spec.signal_field)?;
    let feature_dates = date_strings(&feature_frame, &input.market_data_bundle.time_column)?;
    if feature_dates != dates {
        return Err(EngineRuntimeError::MarketData(
            "reset-timer feature timestamps are not aligned with price tables".to_string(),
        ));
    }
    let feature_values = table_feature_column(
        &feature_frame,
        &reset_spec.signal_field,
        &input.market_data_bundle.symbols,
    )?;
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
                &feature_values,
                request.request_id.clone(),
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
    }
    Ok(())
}

fn identical_timeline_config(
    requests: &[EngineRequestV1],
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

fn is_single_asset_signal_request(request: &EngineRequestV1) -> bool {
    let decision = &request.strategy.decision_plan;
    decision.allocation.get("method").and_then(Value::as_str) == Some("position_state")
        && decision.signals.get("entry").is_some_and(Value::is_object)
        && decision.signals.get("exit").is_some_and(Value::is_object)
}

fn is_calendar_same_session_request(request: &EngineRequestV1) -> bool {
    request
        .strategy
        .decision_plan
        .required_operations
        .contains(&OperationId::SessionSameSessionClose)
}

fn is_reset_timer_request(request: &EngineRequestV1) -> bool {
    request
        .simulation
        .fill_model
        .get("position_policy")
        .and_then(|value| value.get("on_entry_signal_while_holding"))
        .and_then(Value::as_str)
        == Some("reset_timer")
}

fn reset_timer_spec(request: &EngineRequestV1) -> Result<ResetTimerSpec, EngineRuntimeError> {
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
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile(
                "reset-timer entry signal requires a feature field".to_string(),
            )
        })?
        .to_string();
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
    request: &EngineRequestV1,
    feature_values: &[f64],
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
    let entry_signal = evaluate_condition(&rule, &fields, feature_values.len(), 1)
        .map_err(|error| EngineRuntimeError::UnsupportedProfile(error.to_string()))?;
    Ok(ResetTimerCandidateInput {
        candidate_id,
        resolved_params: resolved_params(request),
        entry_signal,
        hold_bars,
    })
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
            date_strings(&close_frame, &input.market_data_bundle.time_column)?,
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
        .map(|request| daily_rank_candidate(request, request.request_id.clone(), &dates))
        .collect::<Result<Vec<_>, _>>()?;
    let summary = run_daily_rank_accounting_batch(DailyRankBatchInput {
        config: accounting_config(first_request)?,
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
) -> Result<Value, EngineRuntimeError> {
    let request = &input.engine_request;
    let bundle = &input.market_data_bundle;
    if bundle.symbols.len() != 1 {
        return Err(EngineRuntimeError::UnsupportedProfile(
            "cross signal execution requires one symbol".to_string(),
        ));
    }
    validate_next_open_signal_actions(request)?;
    let close_frame = read_bundle_table(bundle, "close")?;
    let open_frame = read_bundle_table(bundle, "open")?;
    let dates = aligned_dates(bundle, &close_frame, &open_frame)?;
    let asset = bundle.symbols[0].clone();
    let close = table_price_column(&close_frame, &asset)?;
    let market_fields = feature_market_fields(
        bundle,
        &close_frame,
        required_market_field_names(std::iter::once(request)),
    )?;
    let candidate = single_signal_candidate(
        request,
        &close,
        &market_fields,
        request.strategy.strategy_id.clone(),
    )?;
    let summary = crate::run_single_asset_next_open_signal_batch(SingleAssetSignalBatchInput {
        config: timeline_accounting_config(request)?,
        asset: asset.clone(),
        dates,
        open: table_price_column(&open_frame, &asset)?,
        close,
        include_full_results: input.artifact_output_dir.is_none(),
        artifact_output_dir: input.artifact_output_dir,
        artifact_run_id: input.artifact_run_id,
        candidates: vec![candidate],
    })
    .map_err(|error| EngineRuntimeError::Accounting(error.to_string()))?;
    serde_json::to_value(summary).map_err(|error| EngineRuntimeError::Accounting(error.to_string()))
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
    let summary = run_single_asset_calendar_same_session_batch(CalendarSameSessionBatchInput {
        config: timeline_accounting_config(request)?,
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
    let close_frame = read_bundle_table(bundle, "close")?;
    let open_frame = read_bundle_table(bundle, "open")?;
    let dates = aligned_dates(bundle, &close_frame, &open_frame)?;
    let entry = calendar_entry(&request.strategy.decision_plan).ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("calendar entry signal is required".to_string())
    })?;
    let actions = request
        .simulation
        .fill_model
        .get("actions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile("timeline actions are required".to_string())
        })?;
    let baseline_weights = request
        .simulation
        .fill_model
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
    let candidate = CalendarSameSessionCandidateInput {
        candidate_id: request.strategy.strategy_id.clone(),
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
    };
    let summary = run_calendar_overlay_batch(CalendarOverlayBatchInput {
        config: timeline_accounting_config(request)?,
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
    let dates = date_strings(&frame, &bundle.time_column)?;
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
                    prices[column][row] / prices[column][row - 1] - 1.0
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
    let config = accounting_config(request)?;
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
        (date_strings(&frame, &bundle.time_column)?, Vec::new())
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
    let summary = run_daily_rank_accounting_batch(DailyRankBatchInput {
        config: accounting_config(request)?,
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

fn is_daily_rank_request(request: &EngineRequestV1) -> bool {
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

fn daily_rank_executes_next_open(request: &EngineRequestV1) -> bool {
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

fn validate_daily_rank_trigger(request: &EngineRequestV1) -> Result<(), EngineRuntimeError> {
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
    request: &EngineRequestV1,
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
    request: &EngineRequestV1,
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

fn calendar_same_session_candidate(
    request: &EngineRequestV1,
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

fn calendar_entry(decision: &DecisionPlanV1) -> Option<&Value> {
    decision.signals.get("entry").filter(|entry| {
        entry
            .get("op")
            .and_then(Value::as_str)
            .is_some_and(|op| op.starts_with("calendar."))
    })
}

fn feature_specs(
    request: &EngineRequestV1,
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
    requests: impl Iterator<Item = &'a EngineRequestV1>,
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
    bundle: &MarketDataBundleV1,
    close_frame: &DataFrame,
    required_names: BTreeSet<String>,
) -> Result<BTreeMap<String, Vec<f64>>, EngineRuntimeError> {
    let mut fields = BTreeMap::new();
    let close_dates = date_strings(close_frame, &bundle.time_column)?;
    for role in required_names {
        if !bundle.tables.contains_key(&role) {
            continue;
        }
        let frame = read_bundle_table(bundle, &role)?;
        if date_strings(&frame, &bundle.time_column)? != close_dates {
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

fn validate_next_open_signal_actions(request: &EngineRequestV1) -> Result<(), EngineRuntimeError> {
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
    Ok(())
}

fn has_event_weight_actions(request: &EngineRequestV1) -> bool {
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

fn resolved_params(request: &EngineRequestV1) -> BTreeMap<String, String> {
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
    request: &'a EngineRequestV1,
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
    request: &EngineRequestV1,
) -> Result<i64, EngineRuntimeError> {
    resolved_value(value, request)?.as_i64().ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("integer value is required".to_string())
    })
}

fn resolved_f64(
    value: Option<&Value>,
    request: &EngineRequestV1,
) -> Result<f64, EngineRuntimeError> {
    resolved_value(value, request)?.as_f64().ok_or_else(|| {
        EngineRuntimeError::UnsupportedProfile("numeric value is required".to_string())
    })
}

fn resolved_string(
    value: Option<&Value>,
    request: &EngineRequestV1,
) -> Result<String, EngineRuntimeError> {
    resolved_value(value, request)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| {
            EngineRuntimeError::UnsupportedProfile("string value is required".to_string())
        })
}

fn read_bundle_table(
    bundle: &MarketDataBundleV1,
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

fn aligned_dates(
    bundle: &MarketDataBundleV1,
    close: &DataFrame,
    open: &DataFrame,
) -> Result<Vec<String>, EngineRuntimeError> {
    let close_dates = date_strings(close, &bundle.time_column)?;
    let open_dates = date_strings(open, &bundle.time_column)?;
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
    request: &EngineRequestV1,
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

fn date_strings(frame: &DataFrame, time_column: &str) -> Result<Vec<String>, EngineRuntimeError> {
    let column = frame
        .column(time_column)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?
        .cast(&DataType::String)
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    let values = column
        .str()
        .map_err(|error| EngineRuntimeError::MarketData(error.to_string()))?;
    values
        .iter()
        .map(|value| {
            value
                .filter(|value| value.len() >= 10)
                .map(|value| value[..10].to_string())
                .ok_or_else(|| EngineRuntimeError::MarketData("invalid Time value".to_string()))
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

fn accounting_config(request: &EngineRequestV1) -> Result<AccountingConfig, EngineRuntimeError> {
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
    request: &EngineRequestV1,
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
    request: &EngineRequestV1,
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
