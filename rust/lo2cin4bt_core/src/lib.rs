//! Deterministic Rust core for lo2cin4bt.
//!
//! The first slice keeps Rust independent from the Python runtime. Python can
//! keep orchestrating data loading and UI payloads while this crate owns typed
//! contracts and the sequential accounting state machine.

pub mod accounting;
mod artifact_tables;
pub mod computed_fields;
pub mod config;
pub mod daily_rank;
pub mod detail;
pub mod engine_request;
pub mod engine_runtime;
pub mod engine_service;
pub mod metrics;
pub mod metrics_parquet;
pub mod plot;
pub mod result_validator;
pub mod risk;
pub mod selection;
pub mod signal_timeline;
pub mod simulation;
pub mod timeline;

pub use accounting::{
    run_accounting, AccountingConfig, AccountingEvent, AccountingInput, AccountingRiskGateConfig,
    AccountingSummary, CheckpointInput,
};
pub use computed_fields::returns::{
    period_return_series, session_return_series, PeriodReturnSeries, ReturnSeriesError,
    SessionReturnSeries,
};
pub use config::{
    FactorCompositeMethod, FactorPreprocessOp, StrategyModeId, StrategyPresetId, StrategyProfileId,
    WorkflowId,
};
pub use daily_rank::{
    run_daily_rank_accounting, run_daily_rank_accounting_batch, DailyRankAccountingInput,
    DailyRankAccountingSummary, DailyRankBatchCandidateInput, DailyRankBatchInput,
    DailyRankBatchSummary, DailyRankConditionInput, DailyRankFeatureSpec,
};
pub use detail::{
    project_backtest_detail_bundle, BacktestDetailBundle, BacktestDetailProjectionError,
    BacktestDetailProjectionInput, BACKTEST_DETAIL_SCHEMA_VERSION,
};
pub use engine_request::{
    AccountRequestV1, AccountType, BalanceMode, BookType, ClockMode, ClockRequestV1,
    DataRequirementsV1, DecisionPlanV1, DuplicateTimePolicy, EngineRequestV1, EventOrdering,
    EventTieBreaker, MarketDataBundleV1, MarketDataIndexKind, MarketDataLineageV1,
    MarketDataOrdering, MarketDataQualityV1, MarketDataRole, MarketDataTableV1,
    MarketDataTimeSemanticsV1, MarketDataTransport, MissingValuePolicy, OperationId,
    OutOfOrderPolicy, OutputRequestV1, PositionMode, RequestLineageV1, RequestWindowV1,
    RoutingMode, RunScopeId, SimulationRequestV1, StaleValuePolicy, StrategyRequestV1,
    TimelineActionId, VenueRequestV1, WorkflowRequestV1,
};
pub use engine_runtime::{
    execute_engine_request, execute_engine_request_batch, EngineRequestBatchExecutionInput,
    EngineRequestExecutionInput, EngineRuntimeError,
};
pub use engine_service::{
    handle_engine_service_request, EngineOperation, EngineResourceBudget, EngineServiceCommand,
    EngineServiceError, EngineServiceRequest, EngineServiceResponse, EngineServiceStatus,
    ENGINE_SERVICE_PROTOCOL_VERSION,
};
pub use metrics::{run_metrics_batch, EquityMetricRow, MetricsBatchInput, MetricsBatchSummary};
pub use metrics_parquet::{run_metrics_parquet, MetricsParquetInput};
pub use plot::{
    project_plot_bundle, PlotAxes, PlotBundle, PlotInputSeries, PlotProjectionError,
    PlotProjectionInput, PlotSeries, PLOT_BUNDLE_SCHEMA_VERSION,
};
pub use result_validator::{
    validate_result_tables, ResultCheckStatus, ResultTableView, ResultValidationCheck,
    ResultValidationError, ResultValidationReport, RESULT_VALIDATION_SCHEMA_VERSION,
};
pub use risk::{
    canonical_stateful_action, RiskControlError, RiskControlState, RiskRunMode, FLATTEN_ACTION,
    PERMANENT_STOP_ACTION, SHADOW_ACTION, SHADOW_RECOVERY_ARMED_ACTION,
    SHADOW_RECOVERY_RESUMED_ACTION,
};
pub use selection::{run_rank_selection, RankSelectionInput, RankSelectionSummary};
pub use signal_timeline::{
    run_calendar_overlay_batch, run_reset_timer_batch,
    run_single_asset_calendar_same_session_batch, run_single_asset_next_open_signal_batch,
    run_single_asset_next_open_signal_timeline, CalendarOverlayBatchInput,
    CalendarSameSessionBatchInput, CalendarSameSessionCandidateInput, ResetTimerBatchInput,
    ResetTimerCandidateInput, SingleAssetNextOpenSignalInput, SingleAssetSignalBatchInput,
    SingleAssetSignalBatchSummary, SingleAssetSignalCandidateInput, SingleAssetSignalCompactResult,
};
pub use simulation::{
    execute_target_weight_orders, maintenance_margin_breached, OrderStatus, SettlementEvent,
    SettlementInstruction, SettlementLedger, SettlementStatus, SimulatedAccountConfig,
    SimulatedAccountType, SimulatedExecutionResult, SimulatedOrderEvent, SimulatedVenueConfig,
    SimulationError, TimeInForce,
};
pub use timeline::{
    run_timeline_accounting, TimelineAccountingConfig, TimelineAccountingInput,
    TimelineAccountingSummary, TimelineActionEvent, TimelineActionInput, TimelineCheckpointEvent,
    TimelineCheckpointInput, TimelineDailyEvent, TimelinePositionPolicy,
};
