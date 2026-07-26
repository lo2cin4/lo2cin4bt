use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{ConfigError, StrategyModeId, StrategyPresetId, StrategyProfileId, WorkflowId};

pub const ENGINE_REQUEST_SCHEMA_VERSION: &str = "engine_request.v1";
pub const ENGINE_REQUEST_CONTRACT_ID: &str = "lo2cin4bt.engine_request.v1";
pub const MARKET_DATA_BUNDLE_SCHEMA_VERSION: &str = "market_data_bundle.v1";
pub const MARKET_DATA_BUNDLE_CONTRACT_ID: &str = "lo2cin4bt.market_data_bundle.v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestV1 {
    pub schema_version: String,
    pub contract_id: String,
    pub request_id: String,
    pub request_hash: String,
    pub strategy: StrategyRequestV1,
    pub workflow: WorkflowRequestV1,
    pub data_requirements: DataRequirementsV1,
    pub simulation: SimulationRequestV1,
    pub outputs: OutputRequestV1,
    pub lineage: RequestLineageV1,
}

impl EngineRequestV1 {
    /// Validates contract invariants which JSON shape validation alone cannot prove.
    pub fn validate(&self) -> Result<(), ConfigError> {
        ensure(
            self.schema_version == ENGINE_REQUEST_SCHEMA_VERSION,
            format!("schema_version must be {ENGINE_REQUEST_SCHEMA_VERSION}"),
        )?;
        ensure(
            self.contract_id == ENGINE_REQUEST_CONTRACT_ID,
            format!("contract_id must be {ENGINE_REQUEST_CONTRACT_ID}"),
        )?;
        ensure(
            !self.request_id.trim().is_empty(),
            "request_id must not be empty",
        )?;
        ensure(
            !self.strategy.strategy_id.trim().is_empty(),
            "strategy.strategy_id must not be empty",
        )?;
        ensure(
            !self.data_requirements.symbols.is_empty(),
            "data_requirements.symbols must not be empty",
        )?;
        ensure(
            self.data_requirements.bundle_schema_version == MARKET_DATA_BUNDLE_SCHEMA_VERSION,
            format!(
                "data_requirements.bundle_schema_version must be {MARKET_DATA_BUNDLE_SCHEMA_VERSION}"
            ),
        )?;
        ensure(
            self.simulation.account.starting_balance.is_finite()
                && self.simulation.account.starting_balance > 0.0,
            "simulation.account.starting_balance must be positive and finite",
        )?;
        ensure(
            self.simulation.account.leverage_limit.is_finite()
                && self.simulation.account.leverage_limit >= 1.0,
            "simulation.account.leverage_limit must be at least 1",
        )?;
        if self.simulation.account.account_type == AccountType::Cash {
            ensure(
                self.simulation.account.leverage_limit == 1.0,
                "cash accounts require leverage_limit=1",
            )?;
        }
        ensure(
            self.simulation.account.position_mode == self.simulation.venue.oms_type,
            "simulation.account.position_mode must match venue.oms_type",
        )?;
        ensure(
            !self.simulation.venue.venue_id.trim().is_empty(),
            "simulation.venue.venue_id must not be empty",
        )?;
        ensure(
            self.outputs.result_contract == "canonical_result_bundle.v1",
            "outputs.result_contract must be canonical_result_bundle.v1",
        )?;
        ensure(
            self.outputs.validation_contract == "result_validation_report.v1",
            "outputs.validation_contract must be result_validation_report.v1",
        )?;

        let symbol_count = self
            .data_requirements
            .symbols
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            .len();
        ensure(
            symbol_count == self.data_requirements.symbols.len(),
            "data_requirements.symbols must be unique",
        )?;
        let operation_count = self
            .strategy
            .decision_plan
            .required_operations
            .iter()
            .collect::<BTreeSet<_>>()
            .len();
        ensure(
            operation_count == self.strategy.decision_plan.required_operations.len(),
            "strategy.decision_plan.required_operations must be unique",
        )?;
        let action_count = self
            .strategy
            .decision_plan
            .required_actions
            .iter()
            .collect::<BTreeSet<_>>()
            .len();
        ensure(
            action_count == self.strategy.decision_plan.required_actions.len(),
            "strategy.decision_plan.required_actions must be unique",
        )?;

        if self.workflow.run_scope == RunScopeId::MatrixBatch {
            ensure(
                !self.workflow.parameter_domains.is_empty(),
                "matrix_batch requires parameter_domains",
            )?;
        }
        if matches!(
            self.workflow.run_scope,
            RunScopeId::ValidationTrainWindow | RunScopeId::ValidationTestWindow
        ) {
            ensure(
                self.workflow.window.is_some(),
                "validation window requests require workflow.window",
            )?;
        }
        for key in self.workflow.resolved_parameters.keys() {
            ensure(
                self.workflow.parameter_domains.contains_key(key),
                format!("resolved parameter is not declared: {key}"),
            )?;
        }

        let computed_hash = self.computed_hash()?;
        ensure(
            self.request_hash == computed_hash,
            "request_hash does not match canonical request content",
        )
    }

    pub fn computed_hash(&self) -> Result<String, ConfigError> {
        let mut value = serde_json::to_value(self)
            .map_err(|error| ConfigError::EngineRequestSerialization(error.to_string()))?;
        let Value::Object(ref mut fields) = value else {
            return Err(ConfigError::EngineRequestSerialization(
                "request did not serialize to an object".to_string(),
            ));
        };
        fields.remove("request_hash");
        let encoded = serde_json::to_vec(&value)
            .map_err(|error| ConfigError::EngineRequestSerialization(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyRequestV1 {
    pub strategy_id: String,
    pub strategy_mode_id: StrategyModeId,
    pub strategy_profile_id: StrategyProfileId,
    pub strategy_preset_id: Option<StrategyPresetId>,
    pub plan_hash: String,
    pub profile_contract: Value,
    pub decision_plan: DecisionPlanV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionPlanV1 {
    pub factor_pipeline: Value,
    pub computed_fields: Vec<Value>,
    pub signals: Value,
    pub selection: Value,
    pub allocation: Value,
    pub rebalance: Value,
    pub required_operations: Vec<OperationId>,
    pub required_actions: Vec<TimelineActionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowRequestV1 {
    pub workflow_id: WorkflowId,
    pub run_scope: RunScopeId,
    pub parameter_domains: BTreeMap<String, Value>,
    pub resolved_parameters: BTreeMap<String, Value>,
    pub combo_guard: Value,
    pub window: Option<RequestWindowV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestWindowV1 {
    pub start: String,
    pub end: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataRequirementsV1 {
    pub bundle_schema_version: String,
    pub provider: String,
    pub symbols: Vec<String>,
    pub provider_config: Value,
    pub universe_config: Value,
    pub frequency: String,
    pub calendar: String,
    pub timezone: String,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub start_policy: Option<String>,
    pub external_features: Vec<Value>,
    pub benchmark: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRequestV1 {
    pub account: AccountRequestV1,
    pub venue: VenueRequestV1,
    pub clock: ClockRequestV1,
    pub fill_model: Value,
    pub risk: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRequestV1 {
    pub base_currency: String,
    pub balance_mode: BalanceMode,
    pub starting_balance: f64,
    pub position_mode: PositionMode,
    pub account_type: AccountType,
    pub leverage_limit: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueRequestV1 {
    pub venue_id: String,
    pub oms_type: PositionMode,
    pub book_type: BookType,
    pub routing: RoutingMode,
    pub settlement_days: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockRequestV1 {
    pub mode: ClockMode,
    pub event_ordering: EventOrdering,
    pub tie_breaker: EventTieBreaker,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputRequestV1 {
    pub result_contract: String,
    pub validation_contract: String,
    pub requested: Value,
    pub metricstracker: Value,
    pub statanalyser: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLineageV1 {
    pub source_schema_version: String,
    pub source_config_hash: String,
    pub plan_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunScopeId {
    Single,
    MatrixBatch,
    ValidationTrainWindow,
    ValidationTestWindow,
    Statistics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BalanceMode {
    NormalizedEquity,
    Cash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionMode {
    Netting,
    Hedging,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountType {
    Cash,
    Margin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookType {
    Bar,
    L1Mbp,
    L2Mbp,
    L3Mbo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingMode {
    Simulated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockMode {
    HistoricalEventTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOrdering {
    EventTimeThenSequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventTieBreaker {
    SourceThenSequence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TimelineActionId {
    #[serde(rename = "enter")]
    Enter,
    #[serde(rename = "exit")]
    Exit,
    #[serde(rename = "flatten")]
    Flatten,
    #[serde(rename = "set_target_weights")]
    SetTargetWeights,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OperationId {
    #[serde(rename = "and")]
    And,
    #[serde(rename = "calendar.event_date")]
    CalendarEventDate,
    #[serde(rename = "calendar.every_session")]
    CalendarEverySession,
    #[serde(rename = "calendar.first_session")]
    CalendarFirstSession,
    #[serde(rename = "calendar.last_weekday_of_month")]
    CalendarLastWeekdayOfMonth,
    #[serde(rename = "calendar.month_end")]
    CalendarMonthEnd,
    #[serde(rename = "calendar.month_in")]
    CalendarMonthIn,
    #[serde(rename = "calendar.month_start")]
    CalendarMonthStart,
    #[serde(rename = "calendar.nth_weekday_of_month")]
    CalendarNthWeekdayOfMonth,
    #[serde(rename = "calendar.quarter_end")]
    CalendarQuarterEnd,
    #[serde(rename = "calendar.quarter_start")]
    CalendarQuarterStart,
    #[serde(rename = "calendar.weekday_eq")]
    CalendarWeekdayEq,
    #[serde(rename = "calendar.year_end")]
    CalendarYearEnd,
    #[serde(rename = "calendar.year_start")]
    CalendarYearStart,
    #[serde(rename = "cross_down")]
    CrossDown,
    #[serde(rename = "cross_up")]
    CrossUp,
    #[serde(rename = "eq")]
    Eq,
    #[serde(rename = "ge")]
    Ge,
    #[serde(rename = "gt")]
    Gt,
    #[serde(rename = "indicator.atr")]
    IndicatorAtr,
    #[serde(rename = "indicator.bollinger")]
    IndicatorBollinger,
    #[serde(rename = "indicator.calendar_return")]
    IndicatorCalendarReturn,
    #[serde(rename = "indicator.ema")]
    IndicatorEma,
    #[serde(rename = "indicator.macd")]
    IndicatorMacd,
    #[serde(rename = "indicator.momentum")]
    IndicatorMomentum,
    #[serde(rename = "indicator.percentile")]
    IndicatorPercentile,
    #[serde(rename = "indicator.rsi")]
    IndicatorRsi,
    #[serde(rename = "indicator.sma")]
    IndicatorSma,
    #[serde(rename = "indicator.volatility")]
    IndicatorVolatility,
    #[serde(rename = "indicator.zscore")]
    IndicatorZscore,
    #[serde(rename = "math.abs")]
    MathAbs,
    #[serde(rename = "math.add")]
    MathAdd,
    #[serde(rename = "math.clip")]
    MathClip,
    #[serde(rename = "math.divide")]
    MathDivide,
    #[serde(rename = "math.multiply")]
    MathMultiply,
    #[serde(rename = "math.negate")]
    MathNegate,
    #[serde(rename = "math.subtract")]
    MathSubtract,
    #[serde(rename = "rolling.correlation")]
    RollingCorrelation,
    #[serde(rename = "rolling.max")]
    RollingMax,
    #[serde(rename = "rolling.median")]
    RollingMedian,
    #[serde(rename = "rolling.min")]
    RollingMin,
    #[serde(rename = "rolling.sum")]
    RollingSum,
    #[serde(rename = "transform.fill_missing")]
    TransformFillMissing,
    #[serde(rename = "transform.lag")]
    TransformLag,
    #[serde(rename = "transform.where")]
    TransformWhere,
    #[serde(rename = "cross_section.percentile")]
    CrossSectionPercentile,
    #[serde(rename = "cross_section.rank")]
    CrossSectionRank,
    #[serde(rename = "cross_section.winsorize")]
    CrossSectionWinsorize,
    #[serde(rename = "cross_section.zscore")]
    CrossSectionZscore,
    #[serde(rename = "le")]
    Le,
    #[serde(rename = "lt")]
    Lt,
    #[serde(rename = "ne")]
    Ne,
    #[serde(rename = "not")]
    Not,
    #[serde(rename = "or")]
    Or,
    #[serde(rename = "session.same_session_close")]
    SessionSameSessionClose,
    #[serde(rename = "target.change")]
    TargetChange,
    #[serde(rename = "template.fixed_allocation_rebalance")]
    TemplateFixedAllocationRebalance,
    #[serde(rename = "template.momentum_rotation")]
    TemplateMomentumRotation,
    #[serde(rename = "template.monthly_nth_weekday_same_session")]
    TemplateMonthlyNthWeekdaySameSession,
    #[serde(rename = "template.single_asset_ma_cross")]
    TemplateSingleAssetMaCross,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataBundleV1 {
    pub schema_version: String,
    pub contract_id: String,
    pub bundle_id: String,
    pub content_hash: String,
    pub symbols: Vec<String>,
    pub frequency: String,
    pub calendar: String,
    pub timezone: String,
    pub time_range: RequestWindowV1,
    pub row_count: usize,
    pub time_column: String,
    pub time_semantics: MarketDataTimeSemanticsV1,
    pub quality: MarketDataQualityV1,
    pub tables: BTreeMap<String, MarketDataTableV1>,
    pub lineage: MarketDataLineageV1,
}

impl MarketDataBundleV1 {
    pub fn validate(&self) -> Result<(), ConfigError> {
        ensure(
            self.schema_version == MARKET_DATA_BUNDLE_SCHEMA_VERSION,
            format!("schema_version must be {MARKET_DATA_BUNDLE_SCHEMA_VERSION}"),
        )?;
        ensure(
            self.contract_id == MARKET_DATA_BUNDLE_CONTRACT_ID,
            format!("contract_id must be {MARKET_DATA_BUNDLE_CONTRACT_ID}"),
        )?;
        ensure(
            !self.bundle_id.trim().is_empty(),
            "bundle_id must not be empty",
        )?;
        ensure(
            is_sha256(&self.content_hash),
            "content_hash must be lowercase sha256",
        )?;
        ensure(!self.symbols.is_empty(), "symbols must not be empty")?;
        ensure(
            self.symbols.iter().collect::<BTreeSet<_>>().len() == self.symbols.len(),
            "symbols must be unique",
        )?;
        ensure(
            !self.frequency.trim().is_empty(),
            "frequency must not be empty",
        )?;
        ensure(
            !self.time_column.trim().is_empty(),
            "time_column must not be empty",
        )?;
        ensure(self.row_count > 0, "row_count must be positive")?;
        ensure(!self.tables.is_empty(), "tables must not be empty")?;
        ensure(
            self.tables.contains_key("close"),
            "tables must include close",
        )?;
        for (name, table) in &self.tables {
            ensure(!name.trim().is_empty(), "table name must not be empty")?;
            ensure(
                is_sha256(&table.content_hash),
                format!("table {name} content_hash must be lowercase sha256"),
            )?;
            ensure(
                table.row_count > 0,
                format!("table {name} row_count must be positive"),
            )?;
            ensure(
                !table.columns.is_empty(),
                format!("table {name} columns must not be empty"),
            )?;
            if table.transport != MarketDataTransport::InMemoryArrow {
                ensure(
                    table
                        .path
                        .as_ref()
                        .is_some_and(|path| !path.trim().is_empty()),
                    format!("table {name} path is required for file transport"),
                )?;
            }
        }
        let computed_hash = self.computed_content_hash()?;
        ensure(
            self.content_hash == computed_hash,
            "content_hash does not match canonical manifest",
        )?;
        ensure(
            self.bundle_id == format!("mdb-{}", &computed_hash[..16]),
            "bundle_id does not match content_hash",
        )?;
        Ok(())
    }

    pub fn computed_content_hash(&self) -> Result<String, ConfigError> {
        let tables = self
            .tables
            .iter()
            .map(|(name, table)| {
                let payload = BTreeMap::from([
                    ("columns", serde_json::to_value(&table.columns)),
                    ("content_hash", serde_json::to_value(&table.content_hash)),
                    ("role", serde_json::to_value(table.role)),
                    ("row_count", serde_json::to_value(table.row_count)),
                    ("transport", serde_json::to_value(table.transport)),
                ]);
                let value = payload
                    .into_iter()
                    .map(|(key, value)| {
                        value
                            .map(|value| (key.to_string(), value))
                            .map_err(|error| {
                                ConfigError::EngineRequestSerialization(error.to_string())
                            })
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                Ok((name.clone(), value))
            })
            .collect::<Result<BTreeMap<String, BTreeMap<String, Value>>, ConfigError>>()?;
        let canonical = BTreeMap::from([
            ("calendar", serde_json::to_value(&self.calendar)),
            ("contract_id", serde_json::to_value(&self.contract_id)),
            ("frequency", serde_json::to_value(&self.frequency)),
            ("lineage", serde_json::to_value(&self.lineage)),
            ("quality", serde_json::to_value(&self.quality)),
            ("row_count", serde_json::to_value(self.row_count)),
            ("schema_version", serde_json::to_value(&self.schema_version)),
            ("symbols", serde_json::to_value(&self.symbols)),
            ("tables", serde_json::to_value(tables)),
            ("time_column", serde_json::to_value(&self.time_column)),
            ("time_range", serde_json::to_value(&self.time_range)),
            ("time_semantics", serde_json::to_value(&self.time_semantics)),
            ("timezone", serde_json::to_value(&self.timezone)),
        ]);
        let canonical = canonical
            .into_iter()
            .map(|(key, value)| {
                value
                    .map(|value| (key.to_string(), value))
                    .map_err(|error| ConfigError::EngineRequestSerialization(error.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let encoded = serde_json::to_vec(&canonical)
            .map_err(|error| ConfigError::EngineRequestSerialization(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataTableV1 {
    pub role: MarketDataRole,
    pub transport: MarketDataTransport,
    pub path: Option<String>,
    pub content_hash: String,
    pub columns: Vec<String>,
    pub row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataTimeSemanticsV1 {
    pub index_kind: MarketDataIndexKind,
    pub event_time_column: String,
    pub available_time_column: Option<String>,
    pub availability_policy: String,
    pub ordering: MarketDataOrdering,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataQualityV1 {
    pub missing_value_policy: MissingValuePolicy,
    pub duplicate_time_policy: DuplicateTimePolicy,
    pub out_of_order_policy: OutOfOrderPolicy,
    pub stale_value_policy: StaleValuePolicy,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataLineageV1 {
    pub provider: String,
    pub source_hashes: BTreeMap<String, String>,
    pub point_in_time: bool,
    pub adjustment_policy: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataRole {
    Bars,
    Features,
    Benchmarks,
    Instruments,
    CorporateActions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataTransport {
    Parquet,
    ArrowIpc,
    InMemoryArrow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataIndexKind {
    SessionLabel,
    EventTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketDataOrdering {
    #[serde(rename = "event_time_then_table_name")]
    EventTimeThenTableName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingValuePolicy {
    Preserve,
    DropRows,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateTimePolicy {
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutOfOrderPolicy {
    SortThenValidate,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaleValuePolicy {
    Preserve,
    Fail,
}

fn ensure(condition: bool, message: impl Into<String>) -> Result<(), ConfigError> {
    if condition {
        Ok(())
    } else {
        Err(ConfigError::InvalidEngineRequest(message.into()))
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE_FIXTURES: &str = include_str!(
        "../../../backtester/contracts/runtime/examples/engine-request-profile-fixtures-v1.json"
    );
    const MARKET_DATA_BUNDLE_FIXTURE: &str = include_str!(
        "../../../backtester/contracts/runtime/examples/market-data-bundle-v1.example.json"
    );

    fn fixture_requests() -> Vec<Value> {
        let payload: Value = serde_json::from_str(PROFILE_FIXTURES).unwrap();
        payload["requests"].as_array().unwrap().clone()
    }

    #[test]
    fn python_profile_fixtures_deserialize_and_validate_in_rust() {
        let mut profiles = BTreeSet::new();
        for value in fixture_requests() {
            let request: EngineRequestV1 = serde_json::from_value(value).unwrap();
            request.validate().unwrap();
            profiles.insert(format!("{:?}", request.strategy.strategy_profile_id));
        }
        assert_eq!(profiles.len(), 6);
    }

    #[test]
    fn unknown_operation_is_rejected_during_deserialization() {
        let mut requests = fixture_requests();
        let mut value = requests.remove(0);
        value["strategy"]["decision_plan"]["required_operations"] =
            serde_json::json!(["indicator.magic"]);
        assert!(serde_json::from_value::<EngineRequestV1>(value).is_err());
    }

    #[test]
    fn unknown_request_field_is_rejected_during_deserialization() {
        let mut requests = fixture_requests();
        let mut value = requests.remove(0);
        value["producer_family"] = serde_json::json!("daily_rank");
        assert!(serde_json::from_value::<EngineRequestV1>(value).is_err());
    }

    #[test]
    fn request_hash_detects_content_tampering() {
        let mut requests = fixture_requests();
        let mut request: EngineRequestV1 = serde_json::from_value(requests.remove(0)).unwrap();
        request.request_id.push_str("-tampered");
        assert!(matches!(
            request.validate(),
            Err(ConfigError::InvalidEngineRequest(message))
                if message.contains("request_hash")
        ));
    }

    #[test]
    fn python_market_data_bundle_fixture_deserializes_and_validates_in_rust() {
        let bundle: MarketDataBundleV1 = serde_json::from_str(MARKET_DATA_BUNDLE_FIXTURE).unwrap();

        bundle.validate().unwrap();
        assert_eq!(
            bundle.content_hash,
            "1af6214920966802e161df7a47f989a94814dc04e1ac22278a663459b816d875"
        );
    }

    #[test]
    fn benchmark_role_deserializes_without_joining_the_trading_universe() {
        let role: MarketDataRole = serde_json::from_str("\"benchmarks\"").unwrap();

        assert_eq!(role, MarketDataRole::Benchmarks);
    }

    #[test]
    fn market_data_bundle_requires_positive_rows_and_tables() {
        let bundle = MarketDataBundleV1 {
            schema_version: MARKET_DATA_BUNDLE_SCHEMA_VERSION.to_string(),
            contract_id: MARKET_DATA_BUNDLE_CONTRACT_ID.to_string(),
            bundle_id: "bundle-1".to_string(),
            content_hash: "a".repeat(64),
            symbols: vec!["QQQ".to_string()],
            frequency: "1D".to_string(),
            calendar: "XNYS".to_string(),
            timezone: "America/New_York".to_string(),
            time_range: RequestWindowV1 {
                start: "2020-01-01".to_string(),
                end: "2020-12-31".to_string(),
            },
            row_count: 0,
            time_column: "Time".to_string(),
            time_semantics: MarketDataTimeSemanticsV1 {
                index_kind: MarketDataIndexKind::SessionLabel,
                event_time_column: "Time".to_string(),
                available_time_column: None,
                availability_policy: "bar_close".to_string(),
                ordering: MarketDataOrdering::EventTimeThenTableName,
            },
            quality: MarketDataQualityV1 {
                missing_value_policy: MissingValuePolicy::Preserve,
                duplicate_time_policy: DuplicateTimePolicy::Fail,
                out_of_order_policy: OutOfOrderPolicy::SortThenValidate,
                stale_value_policy: StaleValuePolicy::Preserve,
                warnings: Vec::new(),
            },
            tables: BTreeMap::new(),
            lineage: MarketDataLineageV1 {
                provider: "yfinance".to_string(),
                source_hashes: BTreeMap::new(),
                point_in_time: false,
                adjustment_policy: "adjusted_close".to_string(),
            },
        };
        assert!(bundle.validate().is_err());
    }
}
