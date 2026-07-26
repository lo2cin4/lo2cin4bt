use crate::{
    execute_engine_request, execute_engine_request_batch, project_backtest_detail_bundle,
    project_plot_bundle, run_accounting, run_calendar_overlay_batch, run_daily_rank_accounting,
    run_daily_rank_accounting_batch, run_metrics_batch, run_metrics_parquet, run_rank_selection,
    run_reset_timer_batch, run_single_asset_calendar_same_session_batch,
    run_single_asset_next_open_signal_batch, run_single_asset_next_open_signal_timeline,
    run_timeline_accounting, AccountingInput, BacktestDetailProjectionInput,
    CalendarOverlayBatchInput, CalendarSameSessionBatchInput, DailyRankAccountingInput,
    DailyRankBatchInput, EngineRequestBatchExecutionInput, EngineRequestExecutionInput,
    EngineRequestV1, MetricsBatchInput, MetricsParquetInput, PlotProjectionInput,
    RankSelectionInput, ResetTimerBatchInput, SingleAssetNextOpenSignalInput,
    SingleAssetSignalBatchInput, TimelineAccountingInput,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub const ENGINE_SERVICE_PROTOCOL_VERSION: &str = "engine_service.v1";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EngineServiceCommand {
    Health,
    Capabilities,
    ValidateEngineRequest,
    Execute,
    ExecuteEngineRequest,
    ExecuteEngineRequestBatch,
    Cancel,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EngineOperation {
    Accounting,
    TimelineAccounting,
    SignalTimeline,
    SignalTimelineBatch,
    CalendarSameSessionBatch,
    CalendarOverlayBatch,
    ResetTimerBatch,
    MetricsBatch,
    MetricsParquet,
    RankSelection,
    DailyRankAccounting,
    DailyRankBatch,
    PlotBundle,
    BacktestDetailBundle,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EngineResourceBudget {
    #[serde(default)]
    pub max_payload_bytes: Option<usize>,
    #[serde(default)]
    pub max_operation_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineServiceRequest {
    pub protocol_version: String,
    pub request_id: String,
    pub command: EngineServiceCommand,
    #[serde(default)]
    pub operation: Option<EngineOperation>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub deadline_unix_ms: Option<u64>,
    #[serde(default)]
    pub resource_budget: EngineResourceBudget,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EngineServiceStatus {
    Progress,
    Ok,
    Error,
    ShuttingDown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineServiceError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineServiceResponse {
    pub protocol_version: String,
    pub request_id: String,
    pub status: EngineServiceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<EngineOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<EngineServiceError>,
}

impl EngineServiceResponse {
    pub fn progress(request: &EngineServiceRequest, result: Value) -> Self {
        Self {
            protocol_version: ENGINE_SERVICE_PROTOCOL_VERSION.to_string(),
            request_id: request.request_id.clone(),
            status: EngineServiceStatus::Progress,
            operation: request.operation,
            result: Some(result),
            error: None,
        }
    }

    pub fn success(request: &EngineServiceRequest, result: Value) -> Self {
        Self {
            protocol_version: ENGINE_SERVICE_PROTOCOL_VERSION.to_string(),
            request_id: request.request_id.clone(),
            status: EngineServiceStatus::Ok,
            operation: request.operation,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(request: &EngineServiceRequest, code: &str, message: String) -> Self {
        Self {
            protocol_version: ENGINE_SERVICE_PROTOCOL_VERSION.to_string(),
            request_id: request.request_id.clone(),
            status: EngineServiceStatus::Error,
            operation: request.operation,
            result: None,
            error: Some(EngineServiceError {
                code: code.to_string(),
                message,
                retryable: false,
            }),
        }
    }
}

pub fn handle_engine_service_request(request: EngineServiceRequest) -> EngineServiceResponse {
    if request.protocol_version != ENGINE_SERVICE_PROTOCOL_VERSION {
        return EngineServiceResponse::failure(
            &request,
            "unsupported_protocol",
            format!("unsupported protocol_version: {}", request.protocol_version),
        );
    }
    if request.request_id.trim().is_empty() {
        return EngineServiceResponse::failure(
            &request,
            "invalid_request",
            "request_id cannot be empty".to_string(),
        );
    }
    if let Some(deadline) = request.deadline_unix_ms {
        if now_unix_ms() > deadline {
            return EngineServiceResponse::failure(
                &request,
                "deadline_exceeded",
                "request deadline elapsed before execution".to_string(),
            );
        }
    }
    if let Some(max_bytes) = request.resource_budget.max_payload_bytes {
        let payload_bytes =
            serde_json::to_vec(&request.payload).map_or(usize::MAX, |row| row.len());
        if payload_bytes > max_bytes {
            return EngineServiceResponse::failure(
                &request,
                "resource_budget_exceeded",
                format!("payload uses {payload_bytes} bytes; budget is {max_bytes}"),
            );
        }
    }

    match request.command {
        EngineServiceCommand::Health => EngineServiceResponse::success(
            &request,
            serde_json::json!({"ready": true, "protocol_version": ENGINE_SERVICE_PROTOCOL_VERSION}),
        ),
        EngineServiceCommand::Capabilities => EngineServiceResponse::success(
            &request,
            serde_json::json!({
                "operations": [
                    "accounting", "timeline_accounting", "signal_timeline",
                    "signal_timeline_batch", "calendar_same_session_batch",
                    "calendar_overlay_batch", "reset_timer_batch", "metrics_batch",
                    "metrics_parquet", "rank_selection", "daily_rank_accounting",
                    "daily_rank_batch", "plot_bundle"
                    , "backtest_detail_bundle"
                ],
                "commands": ["health", "capabilities", "validate_engine_request", "execute", "execute_engine_request", "execute_engine_request_batch", "cancel", "shutdown"]
            }),
        ),
        EngineServiceCommand::ValidateEngineRequest => {
            let result = serde_json::from_value::<EngineRequestV1>(request.payload.clone())
                .map_err(|exc| exc.to_string())
                .and_then(|engine_request| {
                    engine_request.validate().map_err(|exc| exc.to_string())?;
                    Ok(serde_json::json!({
                        "valid": true,
                        "request_id": engine_request.request_id,
                        "request_hash": engine_request.request_hash
                    }))
                });
            result_or_failure(&request, result)
        }
        EngineServiceCommand::Execute => {
            let started = Instant::now();
            let response = match request.operation {
                Some(operation) => result_or_failure(
                    &request,
                    execute_operation(operation, request.payload.clone()),
                ),
                None => EngineServiceResponse::failure(
                    &request,
                    "invalid_request",
                    "execute command requires operation".to_string(),
                ),
            };
            if let Some(max_operation_ms) = request.resource_budget.max_operation_ms {
                if started.elapsed().as_millis() > u128::from(max_operation_ms) {
                    return EngineServiceResponse::failure(
                        &request,
                        "resource_budget_exceeded",
                        format!("operation exceeded {max_operation_ms}ms budget"),
                    );
                }
            }
            if request
                .deadline_unix_ms
                .is_some_and(|deadline| now_unix_ms() > deadline)
            {
                return EngineServiceResponse::failure(
                    &request,
                    "deadline_exceeded",
                    "request deadline elapsed during execution".to_string(),
                );
            }
            response
        }
        EngineServiceCommand::ExecuteEngineRequest => {
            let started = Instant::now();
            let result =
                serde_json::from_value::<EngineRequestExecutionInput>(request.payload.clone())
                    .map_err(|error| error.to_string())
                    .and_then(|input| {
                        execute_engine_request(input).map_err(|error| error.to_string())
                    });
            let response = result_or_failure(&request, result);
            if let Some(max_operation_ms) = request.resource_budget.max_operation_ms {
                if started.elapsed().as_millis() > u128::from(max_operation_ms) {
                    return EngineServiceResponse::failure(
                        &request,
                        "resource_budget_exceeded",
                        format!("operation exceeded {max_operation_ms}ms budget"),
                    );
                }
            }
            if request
                .deadline_unix_ms
                .is_some_and(|deadline| now_unix_ms() > deadline)
            {
                return EngineServiceResponse::failure(
                    &request,
                    "deadline_exceeded",
                    "request deadline elapsed during execution".to_string(),
                );
            }
            response
        }
        EngineServiceCommand::ExecuteEngineRequestBatch => {
            let started = Instant::now();
            let result =
                serde_json::from_value::<EngineRequestBatchExecutionInput>(request.payload.clone())
                    .map_err(|error| error.to_string())
                    .and_then(|input| {
                        execute_engine_request_batch(input).map_err(|error| error.to_string())
                    });
            let response = result_or_failure(&request, result);
            if let Some(max_operation_ms) = request.resource_budget.max_operation_ms {
                if started.elapsed().as_millis() > u128::from(max_operation_ms) {
                    return EngineServiceResponse::failure(
                        &request,
                        "resource_budget_exceeded",
                        format!("operation exceeded {max_operation_ms}ms budget"),
                    );
                }
            }
            if request
                .deadline_unix_ms
                .is_some_and(|deadline| now_unix_ms() > deadline)
            {
                return EngineServiceResponse::failure(
                    &request,
                    "deadline_exceeded",
                    "request deadline elapsed during execution".to_string(),
                );
            }
            response
        }
        EngineServiceCommand::Cancel => {
            let target = request
                .payload
                .get("target_request_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if target.trim().is_empty() {
                EngineServiceResponse::failure(
                    &request,
                    "invalid_request",
                    "cancel command requires payload.target_request_id".to_string(),
                )
            } else {
                EngineServiceResponse::success(
                    &request,
                    serde_json::json!({"accepted": true, "target_request_id": target}),
                )
            }
        }
        EngineServiceCommand::Shutdown => EngineServiceResponse {
            protocol_version: ENGINE_SERVICE_PROTOCOL_VERSION.to_string(),
            request_id: request.request_id,
            status: EngineServiceStatus::ShuttingDown,
            operation: None,
            result: Some(serde_json::json!({"accepted": true})),
            error: None,
        },
    }
}

fn result_or_failure(
    request: &EngineServiceRequest,
    result: Result<Value, String>,
) -> EngineServiceResponse {
    match result {
        Ok(value) => EngineServiceResponse::success(request, value),
        Err(message) => EngineServiceResponse::failure(request, "operation_failed", message),
    }
}

fn parse_and_run<T, R, F>(payload: Value, run: F) -> Result<Value, String>
where
    T: for<'de> Deserialize<'de>,
    R: Serialize,
    F: FnOnce(T) -> Result<R, String>,
{
    let input = serde_json::from_value::<T>(payload).map_err(|exc| exc.to_string())?;
    let output = run(input)?;
    serde_json::to_value(output).map_err(|exc| exc.to_string())
}

fn execute_operation(operation: EngineOperation, payload: Value) -> Result<Value, String> {
    match operation {
        EngineOperation::Accounting => parse_and_run::<AccountingInput, _, _>(payload, |input| {
            run_accounting(input).map_err(|exc| exc.to_string())
        }),
        EngineOperation::TimelineAccounting => {
            parse_and_run::<TimelineAccountingInput, _, _>(payload, |input| {
                run_timeline_accounting(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::SignalTimeline => {
            parse_and_run::<SingleAssetNextOpenSignalInput, _, _>(payload, |input| {
                run_single_asset_next_open_signal_timeline(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::SignalTimelineBatch => {
            parse_and_run::<SingleAssetSignalBatchInput, _, _>(payload, |input| {
                run_single_asset_next_open_signal_batch(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::CalendarSameSessionBatch => {
            parse_and_run::<CalendarSameSessionBatchInput, _, _>(payload, |input| {
                run_single_asset_calendar_same_session_batch(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::CalendarOverlayBatch => {
            parse_and_run::<CalendarOverlayBatchInput, _, _>(payload, |input| {
                run_calendar_overlay_batch(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::ResetTimerBatch => {
            parse_and_run::<ResetTimerBatchInput, _, _>(payload, |input| {
                run_reset_timer_batch(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::MetricsBatch => {
            parse_and_run::<MetricsBatchInput, _, _>(payload, |input| {
                run_metrics_batch(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::MetricsParquet => {
            parse_and_run::<MetricsParquetInput, _, _>(payload, |input| {
                run_metrics_parquet(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::RankSelection => {
            parse_and_run::<RankSelectionInput, _, _>(payload, |input| {
                run_rank_selection(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::DailyRankAccounting => {
            parse_and_run::<DailyRankAccountingInput, _, _>(payload, |input| {
                run_daily_rank_accounting(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::DailyRankBatch => {
            parse_and_run::<DailyRankBatchInput, _, _>(payload, |input| {
                run_daily_rank_accounting_batch(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::PlotBundle => {
            parse_and_run::<PlotProjectionInput, _, _>(payload, |input| {
                project_plot_bundle(input).map_err(|exc| exc.to_string())
            })
        }
        EngineOperation::BacktestDetailBundle => {
            parse_and_run::<BacktestDetailProjectionInput, _, _>(payload, |input| {
                project_backtest_detail_bundle(input).map_err(|exc| exc.to_string())
            })
        }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        command: EngineServiceCommand,
        operation: Option<EngineOperation>,
        payload: Value,
    ) -> EngineServiceRequest {
        EngineServiceRequest {
            protocol_version: ENGINE_SERVICE_PROTOCOL_VERSION.to_string(),
            request_id: "request-1".to_string(),
            command,
            operation,
            payload,
            deadline_unix_ms: None,
            resource_budget: EngineResourceBudget::default(),
        }
    }

    #[test]
    fn health_uses_versioned_response_envelope() {
        let response =
            handle_engine_service_request(request(EngineServiceCommand::Health, None, Value::Null));
        assert_eq!(response.status, EngineServiceStatus::Ok);
        assert_eq!(response.request_id, "request-1");
    }

    #[test]
    fn execute_requires_an_operation() {
        let response = handle_engine_service_request(request(
            EngineServiceCommand::Execute,
            None,
            Value::Null,
        ));
        assert_eq!(response.status, EngineServiceStatus::Error);
        assert_eq!(response.error.unwrap().code, "invalid_request");
    }

    #[test]
    fn expired_deadline_fails_before_kernel_execution() {
        let mut input = request(
            EngineServiceCommand::Execute,
            Some(EngineOperation::RankSelection),
            Value::Null,
        );
        input.deadline_unix_ms = Some(1);
        let response = handle_engine_service_request(input);
        assert_eq!(response.status, EngineServiceStatus::Error);
        assert_eq!(response.error.unwrap().code, "deadline_exceeded");
    }

    #[test]
    fn cancel_requires_target_request_id() {
        let response = handle_engine_service_request(request(
            EngineServiceCommand::Cancel,
            None,
            serde_json::json!({}),
        ));
        assert_eq!(response.status, EngineServiceStatus::Error);
        assert_eq!(response.error.unwrap().code, "invalid_request");

        let response = handle_engine_service_request(request(
            EngineServiceCommand::Cancel,
            None,
            serde_json::json!({"target_request_id": "request-running"}),
        ));
        assert_eq!(response.status, EngineServiceStatus::Ok);
        assert_eq!(
            response.result.unwrap()["target_request_id"],
            "request-running"
        );
    }
}
