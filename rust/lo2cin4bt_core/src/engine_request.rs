use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::candidate_identity::{
    canonical_parameter_suffix, parse_candidate_id, validate_base_strategy_id,
};
use crate::config::{ConfigError, StrategyModeId, StrategyPresetId, StrategyProfileId, WorkflowId};

pub const ENGINE_REQUEST_SCHEMA_VERSION: &str = "engine_request.v2";
pub const ENGINE_REQUEST_CONTRACT_ID: &str = "lo2cin4bt.engine_request.v2";
pub const BAR_TIME_CONTRACT_SCHEMA_VERSION: &str = "bar_time_contract.v1";
pub const BAR_TIME_CONTRACT_ID: &str = "lo2cin4bt.bar_time_contract.v1";
pub const MARKET_DATA_BUNDLE_SCHEMA_VERSION: &str = "market_data_bundle.v2";
pub const MARKET_DATA_BUNDLE_CONTRACT_ID: &str = "lo2cin4bt.market_data_bundle.v2";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestV2 {
    pub schema_version: String,
    pub contract_id: String,
    pub request_id: String,
    pub request_hash: String,
    pub strategy: StrategyRequestV2,
    pub workflow: WorkflowRequestV1,
    pub data_requirements: DataRequirementsV2,
    pub simulation: SimulationRequestV2,
    pub outputs: OutputRequestV1,
    pub lineage: RequestLineageV1,
}

impl EngineRequestV2 {
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
        validate_base_strategy_id(&self.strategy.base_strategy_id)
            .map_err(ConfigError::InvalidEngineRequest)?;
        let (candidate_base, candidate_workflow, candidate_suffix) =
            parse_candidate_id(&self.strategy.strategy_id)
                .map_err(ConfigError::InvalidEngineRequest)?;
        ensure(
            candidate_base == self.strategy.base_strategy_id,
            "strategy.strategy_id base segment must match strategy.base_strategy_id",
        )?;
        ensure(
            candidate_workflow == workflow_id_str(self.workflow.workflow_id),
            "strategy.strategy_id workflow segment must match workflow.workflow_id",
        )?;
        let expected_parameter_suffix =
            canonical_parameter_suffix(&self.workflow.resolved_parameters)
                .map_err(ConfigError::InvalidEngineRequest)?;
        ensure(
            candidate_suffix == expected_parameter_suffix,
            "strategy.strategy_id parameter suffix must match workflow.resolved_parameters",
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
        let mut legacy_provider_paths = Vec::new();
        collect_legacy_time_field_paths(
            &self.data_requirements.provider_config,
            "data_requirements.provider_config",
            &mut legacy_provider_paths,
        );
        legacy_provider_paths.sort();
        ensure(
            legacy_provider_paths.is_empty(),
            format!(
                "data_requirements.provider_config must not contain legacy time fields: {}",
                legacy_provider_paths.join(", ")
            ),
        )?;
        self.data_requirements.bar_time.validate(
            &self.data_requirements.provider,
            &self.strategy.stream_binding,
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
pub struct StrategyRequestV2 {
    pub base_strategy_id: String,
    pub strategy_id: String,
    pub strategy_mode_id: StrategyModeId,
    pub strategy_profile_id: StrategyProfileId,
    pub strategy_preset_id: Option<StrategyPresetId>,
    pub plan_hash: String,
    pub profile_contract: Value,
    pub stream_binding: StrategyStreamBindingV1,
    pub decision_plan: DecisionPlanV1,
}

fn workflow_id_str(workflow_id: WorkflowId) -> &'static str {
    match workflow_id {
        WorkflowId::SingleBacktest => "single_backtest",
        WorkflowId::ParameterMatrix => "parameter_matrix",
        WorkflowId::WalkForwardAnalysis => "walk_forward_analysis",
        WorkflowId::RollingValidation => "rolling_validation",
        WorkflowId::Statanalyser => "statanalyser",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyStreamBindingV1 {
    pub execution_stream_id: String,
    pub decision_stream_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarTimeContractV1 {
    pub schema_version: String,
    pub contract_id: String,
    pub session_model: BarSessionModelV1,
    pub timestamp_model: BarTimestampModelV1,
    pub price_model: BarPriceModelV1,
    pub streams: Vec<BarStreamV1>,
}

impl BarTimeContractV1 {
    fn validate(
        &self,
        provider: &str,
        binding: &StrategyStreamBindingV1,
    ) -> Result<(), ConfigError> {
        ensure(
            self.schema_version == BAR_TIME_CONTRACT_SCHEMA_VERSION,
            format!("bar_time.schema_version must be {BAR_TIME_CONTRACT_SCHEMA_VERSION}"),
        )?;
        ensure(
            self.contract_id == BAR_TIME_CONTRACT_ID,
            format!("bar_time.contract_id must be {BAR_TIME_CONTRACT_ID}"),
        )?;
        ensure(
            !self.session_model.calendar_id.trim().is_empty(),
            "bar_time.session_model.calendar_id must not be empty",
        )?;
        ensure(
            !self.session_model.timezone.trim().is_empty(),
            "bar_time.session_model.timezone must not be empty",
        )?;
        ensure(
            !self.streams.is_empty(),
            "bar_time.streams must not be empty",
        )?;

        let mut streams_by_id = BTreeMap::new();
        for stream in &self.streams {
            ensure(
                valid_stream_id(&stream.stream_id),
                format!("invalid bar_time stream_id: {}", stream.stream_id),
            )?;
            ensure(
                streams_by_id
                    .insert(stream.stream_id.as_str(), stream)
                    .is_none(),
                format!("duplicate bar_time stream_id: {}", stream.stream_id),
            )?;
            stream.validate()?;
        }

        let execution_streams = self
            .streams
            .iter()
            .filter(|stream| stream.role == BarStreamRoleV1::Execution)
            .collect::<Vec<_>>();
        ensure(
            execution_streams.len() == 1,
            "bar_time requires exactly one execution stream",
        )?;
        let execution_stream = execution_streams[0];
        let BarStreamSourceV1::External { provider_id } = &execution_stream.source else {
            return Err(ConfigError::InvalidEngineRequest(
                "bar_time execution stream source must be external".to_string(),
            ));
        };
        ensure(
            !provider.trim().is_empty() && provider_id == provider,
            "bar_time execution provider_id must match data_requirements.provider",
        )?;
        ensure(
            binding.execution_stream_id == execution_stream.stream_id,
            "strategy.stream_binding.execution_stream_id must identify the execution stream",
        )?;

        for stream in &self.streams {
            let BarStreamSourceV1::Derived {
                parent_stream_id, ..
            } = &stream.source
            else {
                continue;
            };
            ensure(
                stream.role == BarStreamRoleV1::Decision,
                format!(
                    "derived stream {} must have decision role",
                    stream.stream_id
                ),
            )?;
            let parent = streams_by_id
                .get(parent_stream_id.as_str())
                .ok_or_else(|| {
                    ConfigError::InvalidEngineRequest(format!(
                        "derived stream {} parent_stream_id {} is missing",
                        stream.stream_id, parent_stream_id
                    ))
                })?;
            ensure(
                stream.bar_spec.is_strictly_coarser_than(&parent.bar_spec),
                format!(
                    "derived stream {} must be strictly coarser than parent {}",
                    stream.stream_id, parent_stream_id
                ),
            )?;

            let mut seen = BTreeSet::new();
            let mut current = stream;
            loop {
                ensure(
                    seen.insert(current.stream_id.as_str()),
                    format!(
                        "derived stream lineage contains a cycle at {}",
                        current.stream_id
                    ),
                )?;
                let BarStreamSourceV1::Derived {
                    parent_stream_id, ..
                } = &current.source
                else {
                    break;
                };
                current = streams_by_id
                    .get(parent_stream_id.as_str())
                    .copied()
                    .ok_or_else(|| {
                        ConfigError::InvalidEngineRequest(format!(
                            "derived stream {} parent_stream_id {} is missing",
                            current.stream_id, parent_stream_id
                        ))
                    })?;
            }
        }

        let decision_stream = streams_by_id
            .get(binding.decision_stream_id.as_str())
            .copied()
            .ok_or_else(|| {
                ConfigError::InvalidEngineRequest(format!(
                    "strategy.stream_binding.decision_stream_id {} is missing",
                    binding.decision_stream_id
                ))
            })?;
        if decision_stream.stream_id != execution_stream.stream_id {
            ensure(
                decision_stream.role == BarStreamRoleV1::Decision,
                "strategy.stream_binding.decision_stream_id must identify a decision stream",
            )?;
            let mut seen = BTreeSet::new();
            let mut current = decision_stream;
            loop {
                ensure(
                    seen.insert(current.stream_id.as_str()),
                    "strategy.stream_binding decision lineage contains a cycle",
                )?;
                let BarStreamSourceV1::Derived {
                    parent_stream_id, ..
                } = &current.source
                else {
                    return Err(ConfigError::InvalidEngineRequest(
                        "strategy.stream_binding decision lineage must terminate at the execution stream"
                            .to_string(),
                    ));
                };
                current = streams_by_id
                    .get(parent_stream_id.as_str())
                    .copied()
                    .ok_or_else(|| {
                        ConfigError::InvalidEngineRequest(format!(
                            "strategy.stream_binding decision parent {} is missing",
                            parent_stream_id
                        ))
                    })?;
                if current.stream_id == execution_stream.stream_id {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarSessionModelV1 {
    pub calendar_id: String,
    pub timezone: String,
    pub session_scope: BarSessionScopeV1,
    pub session_label_policy: SessionLabelPolicy,
    pub non_session_bar_policy: NonSessionBarPolicyV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarSessionScopeV1 {
    Regular,
    #[serde(rename = "24x7")]
    TwentyFourSeven,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonSessionBarPolicyV1 {
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarTimestampModelV1 {
    pub time_standard: BarTimeStandardV1,
    pub precision: BarTimestampPrecisionV1,
    pub clock: BarClockV1,
    pub ordering: BarOrderingV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarTimeStandardV1 {
    #[serde(rename = "UTC")]
    Utc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarTimestampPrecisionV1 {
    Nanosecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarClockV1 {
    HistoricalAvailableTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarOrderingV1 {
    AvailableTimeThenEventTimeThenExternalExecutionSequenceThenLifecycleStageThenStreamIdThenSourceSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarPriceModelV1 {
    pub price_basis: BarPriceBasisV1,
    pub corporate_action_policy: CorporateActionPolicyV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarPriceBasisV1 {
    Raw,
    SplitAdjusted,
    SplitDividendAdjusted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorporateActionPolicyV1 {
    ProviderApplied,
    SeparateEvents,
    NotAvailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarStreamV1 {
    pub stream_id: String,
    pub role: BarStreamRoleV1,
    pub source: BarStreamSourceV1,
    pub bar_spec: ContractBarSpecV1,
    pub timestamp_semantics: BarTimestampSemanticsV1,
}

impl BarStreamV1 {
    fn validate(&self) -> Result<(), ConfigError> {
        ensure(
            self.bar_spec.step > 0,
            format!("stream {} bar_spec.step must be positive", self.stream_id),
        )?;
        if matches!(
            self.bar_spec.unit,
            ContractBarUnitV1::Week | ContractBarUnitV1::Month
        ) {
            ensure(
                self.bar_spec.alignment == ContractBarAlignmentV1::CalendarPeriodStart,
                format!(
                    "weekly/monthly stream {} requires calendar_period_start alignment",
                    self.stream_id
                ),
            )?;
        }
        for (field, value) in [
            (
                "bar_open_time_column",
                self.timestamp_semantics.bar_open_time_column.as_str(),
            ),
            (
                "bar_close_time_column",
                self.timestamp_semantics.bar_close_time_column.as_str(),
            ),
            (
                "available_time_column",
                self.timestamp_semantics.available_time_column.as_str(),
            ),
            (
                "session_label_column",
                self.timestamp_semantics.session_label_column.as_str(),
            ),
        ] {
            ensure(
                !value.trim().is_empty(),
                format!("stream {} {field} must not be empty", self.stream_id),
            )?;
        }
        match &self.source {
            BarStreamSourceV1::External { provider_id } => ensure(
                !provider_id.trim().is_empty(),
                format!(
                    "external stream {} provider_id must not be empty",
                    self.stream_id
                ),
            ),
            BarStreamSourceV1::Derived {
                parent_stream_id, ..
            } => ensure(
                valid_stream_id(parent_stream_id),
                format!(
                    "derived stream {} has invalid parent_stream_id",
                    self.stream_id
                ),
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarStreamRoleV1 {
    Execution,
    Decision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BarStreamSourceV1 {
    External {
        provider_id: String,
    },
    Derived {
        parent_stream_id: String,
        aggregation_engine: AggregationEngineV1,
        empty_bar_policy: EmptyBarPolicyV1,
        partial_first_bar_policy: PartialBarPolicyV1,
        partial_final_bar_policy: FinalPartialBarPolicyV1,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationEngineV1 {
    SharedRust,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmptyBarPolicyV1 {
    Omit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialBarPolicyV1 {
    Omit,
    Emit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalPartialBarPolicyV1 {
    Omit,
    Emit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractBarSpecV1 {
    pub aggregation: BarAggregationKindV1,
    pub step: u32,
    pub unit: ContractBarUnitV1,
    pub price_type: BarPriceTypeV1,
    pub alignment: ContractBarAlignmentV1,
}

impl ContractBarSpecV1 {
    fn is_strictly_coarser_than(&self, parent: &Self) -> bool {
        if self.step == 0 || parent.step == 0 {
            return false;
        }
        if matches!(
            (self.unit, parent.unit),
            (
                ContractBarUnitV1::Minute | ContractBarUnitV1::Hour,
                ContractBarUnitV1::Minute | ContractBarUnitV1::Hour
            )
        ) {
            let child_minutes = u64::from(self.step)
                * if self.unit == ContractBarUnitV1::Hour {
                    60
                } else {
                    1
                };
            let parent_minutes = u64::from(parent.step)
                * if parent.unit == ContractBarUnitV1::Hour {
                    60
                } else {
                    1
                };
            return child_minutes > parent_minutes && child_minutes % parent_minutes == 0;
        }
        if self.unit == ContractBarUnitV1::Month && parent.unit == ContractBarUnitV1::Week {
            return false;
        }
        let child_order = self.unit.order();
        let parent_order = parent.unit.order();
        if child_order != parent_order {
            return child_order > parent_order;
        }
        self.step > parent.step && self.step.is_multiple_of(parent.step)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarAggregationKindV1 {
    Time,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractBarUnitV1 {
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

impl ContractBarUnitV1 {
    fn order(self) -> u8 {
        match self {
            Self::Minute => 0,
            Self::Hour => 1,
            Self::Day => 2,
            Self::Week => 3,
            Self::Month => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarPriceTypeV1 {
    Last,
    Bid,
    Ask,
    Mid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractBarAlignmentV1 {
    SessionOpen,
    CalendarPeriodStart,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarTimestampSemanticsV1 {
    pub timestamp_convention: BarTimestampConventionV1,
    pub interval_boundary: BarIntervalBoundaryV1,
    pub bar_open_time_column: String,
    pub bar_close_time_column: String,
    pub available_time_column: String,
    pub session_label_column: String,
    pub availability_policy: BarAvailabilityPolicyV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarTimestampConventionV1 {
    BarOpen,
    BarClose,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarIntervalBoundaryV1 {
    LeftOpenRightClosed,
    LeftClosedRightOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarAvailabilityPolicyV1 {
    BarClose,
    ExplicitTimestamp,
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
pub struct DataRequirementsV2 {
    pub bundle_schema_version: String,
    pub provider: String,
    pub symbols: Vec<String>,
    pub provider_config: Value,
    pub universe_config: Value,
    pub bar_time: BarTimeContractV1,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub start_policy: Option<String>,
    pub external_features: Vec<Value>,
    pub benchmark: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRequestV2 {
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
    #[serde(rename = "calendar.session_offset_from_month_end")]
    CalendarSessionOffsetFromMonthEnd,
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
pub struct MarketDataBundleV2 {
    pub schema_version: String,
    pub contract_id: String,
    pub bundle_id: String,
    pub content_hash: String,
    pub symbols: Vec<String>,
    pub calendar: String,
    pub timezone: String,
    pub time_range: RequestWindowV1,
    pub row_count: usize,
    pub time_column: String,
    pub execution_stream: MarketDataExecutionStreamV2,
    pub session_windows: Vec<MarketDataSessionWindowV2>,
    pub quality: MarketDataQualityV2,
    pub tables: BTreeMap<String, MarketDataTableV2>,
    pub lineage: MarketDataLineageV2,
}

impl MarketDataBundleV2 {
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
            !self.calendar.trim().is_empty(),
            "calendar must not be empty",
        )?;
        ensure(
            !self.timezone.trim().is_empty(),
            "timezone must not be empty",
        )?;
        ensure(
            !self.time_range.start.trim().is_empty() && !self.time_range.end.trim().is_empty(),
            "time_range start and end must not be empty",
        )?;
        ensure(self.time_column == "Time", "time_column must be Time")?;
        ensure(self.row_count > 0, "row_count must be positive")?;
        self.execution_stream.validate()?;
        ensure(
            self.execution_stream.source.provider_id == self.lineage.provider,
            "execution_stream.source.provider_id must match lineage.provider",
        )?;
        self.validate_session_windows()?;

        for required in [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "execution_timeline",
        ] {
            ensure(
                self.tables.contains_key(required),
                format!("tables must include {required}"),
            )?;
        }
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
            if let Some(path) = &table.path {
                ensure(
                    !path.trim().is_empty(),
                    format!("table {name} path must not be empty when present"),
                )?;
            }
        }
        for name in ["open", "high", "low", "close", "volume"] {
            let table = &self.tables[name];
            ensure(
                table.role == MarketDataRoleV2::Bars,
                format!("table {name} role must be bars"),
            )?;
            ensure(
                table.columns == self.symbols,
                format!("table {name} columns must match symbols in order"),
            )?;
            ensure(
                table.row_count == self.row_count,
                format!("table {name} row_count must match bundle row_count"),
            )?;
        }
        let timeline = &self.tables["execution_timeline"];
        ensure(
            timeline.role == MarketDataRoleV2::BarTimeline,
            "execution_timeline role must be bar_timeline",
        )?;
        ensure(
            timeline.columns
                == [
                    "external_execution_sequence",
                    "bar_open_timestamp",
                    "bar_close_timestamp",
                    "available_timestamp",
                    "session_label",
                ],
            "execution_timeline columns must match the v2 contract exactly",
        )?;
        ensure(
            timeline.row_count == self.row_count,
            "execution_timeline row_count must match bundle row_count",
        )?;
        for (name, hash) in &self.lineage.source_hashes {
            ensure(
                !name.trim().is_empty() && is_sha256(hash),
                "lineage source hashes must have non-empty names and lowercase sha256 values",
            )?;
        }
        ensure(
            self.quality
                .warnings
                .iter()
                .all(|warning| !warning.trim().is_empty()),
            "quality warnings must not contain empty values",
        )?;
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
        let mut canonical = serde_json::to_value(self)
            .map_err(|error| ConfigError::EngineRequestSerialization(error.to_string()))?;
        let Value::Object(fields) = &mut canonical else {
            return Err(ConfigError::EngineRequestSerialization(
                "MarketDataBundle did not serialize to an object".to_string(),
            ));
        };
        fields.remove("bundle_id");
        fields.remove("content_hash");
        let Some(Value::Object(tables)) = fields.get_mut("tables") else {
            return Err(ConfigError::EngineRequestSerialization(
                "MarketDataBundle tables did not serialize to an object".to_string(),
            ));
        };
        for table in tables.values_mut() {
            let Value::Object(table_fields) = table else {
                return Err(ConfigError::EngineRequestSerialization(
                    "MarketDataBundle table did not serialize to an object".to_string(),
                ));
            };
            table_fields.remove("path");
        }
        let encoded = serde_json::to_vec(&canonical)
            .map_err(|error| ConfigError::EngineRequestSerialization(error.to_string()))?;
        Ok(format!("{:x}", Sha256::digest(encoded)))
    }

    fn validate_session_windows(&self) -> Result<(), ConfigError> {
        ensure(
            !self.session_windows.is_empty(),
            "session_windows must not be empty",
        )?;
        let mut previous_label: Option<&str> = None;
        let mut previous_close = None;
        for window in &self.session_windows {
            ensure(
                valid_session_label(&window.session_label),
                "session_window.session_label must use YYYY-MM-DD",
            )?;
            let open = crate::bar_aggregation::parse_utc_nanos(&window.open_timestamp)
                .map_err(|error| ConfigError::InvalidEngineRequest(error.to_string()))?;
            let close = crate::bar_aggregation::parse_utc_nanos(&window.close_timestamp)
                .map_err(|error| ConfigError::InvalidEngineRequest(error.to_string()))?;
            ensure(open < close, "session window open must precede close")?;
            ensure(
                previous_label.is_none_or(|label| window.session_label.as_str() > label),
                "session_windows must be unique and ordered by session_label",
            )?;
            ensure(
                previous_close.is_none_or(|prior| open >= prior),
                "session_windows must not overlap",
            )?;
            previous_label = Some(&window.session_label);
            previous_close = Some(close);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataExecutionStreamV2 {
    pub stream_id: String,
    pub role: MarketDataExecutionRoleV2,
    pub source: MarketDataExternalSourceV2,
    pub session_scope: BarSessionScopeV1,
    pub row_key_kind: MarketDataIndexKind,
    pub bar_spec: ContractBarSpecV1,
    pub timestamp_semantics: MarketDataTimestampSemanticsV2,
    pub timeline_table: String,
    pub ohlcv_tables: MarketDataOhlcvBindingsV2,
}

impl MarketDataExecutionStreamV2 {
    fn validate(&self) -> Result<(), ConfigError> {
        ensure(
            valid_stream_id(&self.stream_id),
            "execution_stream.stream_id is invalid",
        )?;
        ensure(
            !self.source.provider_id.trim().is_empty(),
            "execution_stream.source.provider_id must not be empty",
        )?;
        ensure(
            self.bar_spec.step > 0,
            "execution_stream.bar_spec.step must be positive",
        )?;
        if matches!(
            self.bar_spec.unit,
            ContractBarUnitV1::Week | ContractBarUnitV1::Month
        ) {
            ensure(
                self.bar_spec.alignment == ContractBarAlignmentV1::CalendarPeriodStart,
                "weekly/monthly execution streams require calendar_period_start alignment",
            )?;
        }
        ensure(
            self.timestamp_semantics.external_execution_sequence_column
                == "external_execution_sequence"
                && self.timestamp_semantics.bar_open_time_column == "bar_open_timestamp"
                && self.timestamp_semantics.bar_close_time_column == "bar_close_timestamp"
                && self.timestamp_semantics.available_time_column == "available_timestamp"
                && self.timestamp_semantics.session_label_column == "session_label",
            "execution_stream timestamp column bindings must match the v2 contract",
        )?;
        ensure(
            self.timeline_table == "execution_timeline",
            "execution_stream.timeline_table must be execution_timeline",
        )?;
        ensure(
            self.ohlcv_tables
                == MarketDataOhlcvBindingsV2 {
                    open: "open".to_string(),
                    high: "high".to_string(),
                    low: "low".to_string(),
                    close: "close".to_string(),
                    volume: "volume".to_string(),
                },
            "execution_stream.ohlcv_tables must bind the five canonical tables",
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataExecutionRoleV2 {
    Execution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataExternalSourceV2 {
    pub kind: MarketDataExternalSourceKindV2,
    pub provider_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataExternalSourceKindV2 {
    External,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataTimestampSemanticsV2 {
    pub timestamp_convention: BarTimestampConventionV1,
    pub interval_boundary: BarIntervalBoundaryV1,
    pub external_execution_sequence_column: String,
    pub bar_open_time_column: String,
    pub bar_close_time_column: String,
    pub available_time_column: String,
    pub session_label_column: String,
    pub availability_policy: BarAvailabilityPolicyV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataOhlcvBindingsV2 {
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataSessionWindowV2 {
    pub session_label: String,
    pub open_timestamp: String,
    pub close_timestamp: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataTableV2 {
    pub role: MarketDataRoleV2,
    pub transport: MarketDataTransportV2,
    pub path: Option<String>,
    pub content_hash: String,
    pub columns: Vec<String>,
    pub row_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataQualityV2 {
    pub missing_value_policy: MarketDataMissingValuePolicyV2,
    pub duplicate_time_policy: DuplicateTimePolicy,
    pub out_of_order_policy: MarketDataOutOfOrderPolicyV2,
    pub stale_value_policy: StaleValuePolicy,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataLineageV2 {
    pub provider: String,
    pub source_hashes: BTreeMap<String, String>,
    pub point_in_time: bool,
    pub adjustment_policy: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataRoleV2 {
    Bars,
    BarTimeline,
    Features,
    Benchmarks,
    Instruments,
    CorporateActions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataTransportV2 {
    Parquet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataIndexKind {
    SessionLabel,
    EventTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLabelPolicy {
    ExchangeLocalDate,
    UtcDate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataMissingValuePolicyV2 {
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateTimePolicy {
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataOutOfOrderPolicyV2 {
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaleValuePolicy {
    Preserve,
    Fail,
}

fn valid_stream_id(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_session_label(value: &str) -> bool {
    value.len() == 10
        && value.bytes().enumerate().all(|(index, byte)| match index {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
}

fn collect_legacy_time_field_paths(value: &Value, prefix: &str, paths: &mut Vec<String>) {
    match value {
        Value::Object(fields) => {
            for (key, item) in fields {
                let path = format!("{prefix}.{key}");
                if matches!(
                    key.as_str(),
                    "frequency" | "interval" | "calendar" | "timezone"
                ) {
                    paths.push(path);
                } else {
                    collect_legacy_time_field_paths(item, &path, paths);
                }
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                collect_legacy_time_field_paths(item, &format!("{prefix}[{index}]"), paths);
            }
        }
        _ => {}
    }
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
        "../../../backtester/contracts/runtime/examples/engine-request-profile-fixtures-v2.json"
    );
    const MARKET_DATA_BUNDLE_FIXTURE: &str = include_str!(
        "../../../backtester/contracts/runtime/examples/market-data-bundle-v2.example.json"
    );

    fn fixture_requests() -> Vec<Value> {
        let payload: Value = serde_json::from_str(PROFILE_FIXTURES).unwrap();
        payload["requests"].as_array().unwrap().clone()
    }

    fn request_v2_value() -> Value {
        fixture_requests().remove(0)
    }

    #[test]
    fn engine_request_v2_direct_daily_deserializes_and_validates() {
        let request: EngineRequestV2 = serde_json::from_value(request_v2_value()).unwrap();

        request.validate().unwrap();
        assert_eq!(
            request.strategy.stream_binding.execution_stream_id,
            "execution_daily"
        );
        assert_eq!(
            request.strategy.stream_binding.decision_stream_id,
            "execution_daily"
        );
    }

    #[test]
    fn python_profile_v2_fixtures_deserialize_and_validate_in_rust() {
        let mut profiles = BTreeSet::new();
        for value in fixture_requests() {
            let request: EngineRequestV2 = serde_json::from_value(value).unwrap();
            request.validate().unwrap();
            profiles.insert(format!("{:?}", request.strategy.strategy_profile_id));
        }

        assert_eq!(profiles.len(), 6);
    }

    #[test]
    fn engine_request_v2_accepts_one_minute_execution_and_five_minute_decision() {
        let mut value = request_v2_value();
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
        value["request_hash"] = serde_json::json!("");
        let mut request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        request.request_hash = request.computed_hash().unwrap();

        request.validate().unwrap();
    }

    #[test]
    fn unknown_operation_is_rejected_during_deserialization() {
        let mut value = request_v2_value();
        value["strategy"]["decision_plan"]["required_operations"] =
            serde_json::json!(["indicator.magic"]);
        assert!(serde_json::from_value::<EngineRequestV2>(value).is_err());
    }

    #[test]
    fn unknown_request_field_is_rejected_during_deserialization() {
        let mut value = request_v2_value();
        value["producer_family"] = serde_json::json!("daily_rank");
        assert!(serde_json::from_value::<EngineRequestV2>(value).is_err());
    }

    #[test]
    fn engine_request_v1_is_rejected_during_validation() {
        let mut value = request_v2_value();
        value["schema_version"] = serde_json::json!("engine_request.v1");
        value["contract_id"] = serde_json::json!("lo2cin4bt.engine_request.v1");
        let request: EngineRequestV2 = serde_json::from_value(value).unwrap();

        assert!(matches!(
            request.validate(),
            Err(ConfigError::InvalidEngineRequest(message))
                if message.contains("schema_version")
        ));
    }

    #[test]
    fn retired_frequency_calendar_and_timezone_are_rejected() {
        let mut value = request_v2_value();
        value["data_requirements"]["frequency"] = serde_json::json!("1D");
        value["data_requirements"]["calendar"] = serde_json::json!("XNYS");
        value["data_requirements"]["timezone"] = serde_json::json!("America/New_York");

        assert!(serde_json::from_value::<EngineRequestV2>(value).is_err());
    }

    #[test]
    fn nested_provider_config_legacy_time_fields_are_rejected() {
        let mut value = request_v2_value();
        value["data_requirements"]["provider_config"]["benchmark"]["interval"] =
            serde_json::json!("1d");
        value["request_hash"] = serde_json::json!("");
        let mut request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        request.request_hash = request.computed_hash().unwrap();

        assert!(matches!(
            request.validate(),
            Err(ConfigError::InvalidEngineRequest(message))
                if message.contains("data_requirements.provider_config.benchmark.interval")
        ));
    }

    #[test]
    fn decision_binding_must_resolve_to_the_execution_lineage() {
        let mut value = request_v2_value();
        value["strategy"]["stream_binding"]["decision_stream_id"] = serde_json::json!("missing_5m");
        value["request_hash"] = serde_json::json!("");
        let mut request: EngineRequestV2 = serde_json::from_value(value).unwrap();
        request.request_hash = request.computed_hash().unwrap();

        assert!(matches!(
            request.validate(),
            Err(ConfigError::InvalidEngineRequest(message))
                if message.contains("decision_stream_id")
        ));
    }

    #[test]
    fn request_hash_detects_content_tampering() {
        let mut request: EngineRequestV2 = serde_json::from_value(request_v2_value()).unwrap();
        request.request_id.push_str("-tampered");
        assert!(matches!(
            request.validate(),
            Err(ConfigError::InvalidEngineRequest(message))
                if message.contains("request_hash")
        ));
    }

    #[test]
    fn python_market_data_bundle_fixture_deserializes_and_validates_in_rust() {
        let bundle: MarketDataBundleV2 = serde_json::from_str(MARKET_DATA_BUNDLE_FIXTURE).unwrap();

        bundle.validate().unwrap();
        assert_eq!(
            bundle.content_hash,
            "28fc4b7daf3f24f1e86d5cf6882dbcc5cb861954b0714119dcca34dec3c52b86"
        );
    }

    #[test]
    fn benchmark_role_deserializes_without_joining_the_trading_universe() {
        let role: MarketDataRoleV2 = serde_json::from_str("\"benchmarks\"").unwrap();

        assert_eq!(role, MarketDataRoleV2::Benchmarks);
    }

    #[test]
    fn market_data_bundle_requires_positive_rows_and_tables() {
        let mut value: Value = serde_json::from_str(MARKET_DATA_BUNDLE_FIXTURE).unwrap();
        value["row_count"] = serde_json::json!(0);
        let bundle: MarketDataBundleV2 = serde_json::from_value(value).unwrap();

        assert!(bundle.validate().is_err());
    }

    #[test]
    fn market_data_bundle_v1_shape_is_rejected_during_deserialization() {
        let mut value: Value = serde_json::from_str(MARKET_DATA_BUNDLE_FIXTURE).unwrap();
        value["schema_version"] = serde_json::json!("market_data_bundle.v1");
        value["contract_id"] = serde_json::json!("lo2cin4bt.market_data_bundle.v1");
        value["frequency"] = serde_json::json!("1D");
        value["time_semantics"] = serde_json::json!({});

        assert!(serde_json::from_value::<MarketDataBundleV2>(value).is_err());
    }
}
