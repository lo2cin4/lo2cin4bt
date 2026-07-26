use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("unknown strategy mode id: {0}")]
    UnknownStrategyMode(String),
    #[error("unknown strategy profile id: {0}")]
    UnknownStrategyProfile(String),
    #[error("unknown strategy preset id: {0}")]
    UnknownStrategyPreset(String),
    #[error("unknown workflow id: {0}")]
    UnknownWorkflow(String),
    #[error("unknown factor preprocessing op: {0}")]
    UnknownFactorPreprocessOp(String),
    #[error("unknown factor composite method: {0}")]
    UnknownFactorCompositeMethod(String),
    #[error("invalid engine request: {0}")]
    InvalidEngineRequest(String),
    #[error("engine request serialization failed: {0}")]
    EngineRequestSerialization(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyModeId {
    MultiAssetPortfolio,
}

impl FromStr for StrategyModeId {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "multi_asset_portfolio" => Ok(Self::MultiAssetPortfolio),
            other => Err(ConfigError::UnknownStrategyMode(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyProfileId {
    SelectionTimingPortfolio,
    AllocationPortfolio,
    RotationPortfolio,
    CalendarEventPortfolio,
    PairSpreadPortfolio,
    MultiLegEventPortfolio,
}

impl FromStr for StrategyProfileId {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "selection_timing_portfolio" => Ok(Self::SelectionTimingPortfolio),
            "allocation_portfolio" => Ok(Self::AllocationPortfolio),
            "rotation_portfolio" => Ok(Self::RotationPortfolio),
            "calendar_event_portfolio" => Ok(Self::CalendarEventPortfolio),
            "pair_spread_portfolio" => Ok(Self::PairSpreadPortfolio),
            "multi_leg_event_portfolio" => Ok(Self::MultiLegEventPortfolio),
            other => Err(ConfigError::UnknownStrategyProfile(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyPresetId {
    SingleAssetSignal,
}

impl FromStr for StrategyPresetId {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "single_asset_signal" => Ok(Self::SingleAssetSignal),
            other => Err(ConfigError::UnknownStrategyPreset(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowId {
    SingleBacktest,
    ParameterMatrix,
    WalkForwardAnalysis,
    RollingValidation,
    Statanalyser,
}

impl FromStr for WorkflowId {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "single_backtest" => Ok(Self::SingleBacktest),
            "parameter_matrix" => Ok(Self::ParameterMatrix),
            "walk_forward_analysis" => Ok(Self::WalkForwardAnalysis),
            "rolling_validation" => Ok(Self::RollingValidation),
            "statanalyser" => Ok(Self::Statanalyser),
            other => Err(ConfigError::UnknownWorkflow(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorPreprocessOp {
    Winsorize,
    Standardize,
    Neutralize,
    Rank,
    FillMissing,
    LagAudit,
    DropUnavailable,
}

impl FromStr for FactorPreprocessOp {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "winsorize" => Ok(Self::Winsorize),
            "standardize" => Ok(Self::Standardize),
            "neutralize" => Ok(Self::Neutralize),
            "rank" => Ok(Self::Rank),
            "fill_missing" => Ok(Self::FillMissing),
            "lag_audit" => Ok(Self::LagAudit),
            "drop_unavailable" => Ok(Self::DropUnavailable),
            other => Err(ConfigError::UnknownFactorPreprocessOp(other.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorCompositeMethod {
    EqualWeight,
    ManualWeight,
    IcWeight,
    RegressionWeight,
    RankerModel,
    None,
}

impl FromStr for FactorCompositeMethod {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "equal_weight" => Ok(Self::EqualWeight),
            "manual_weight" => Ok(Self::ManualWeight),
            "ic_weight" => Ok(Self::IcWeight),
            "regression_weight" => Ok(Self::RegressionWeight),
            "ranker_model" => Ok(Self::RankerModel),
            "none" => Ok(Self::None),
            other => Err(ConfigError::UnknownFactorCompositeMethod(other.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_mode_is_fail_fast() {
        assert_eq!(
            "multi_asset_portfolio".parse::<StrategyModeId>().unwrap(),
            StrategyModeId::MultiAssetPortfolio
        );
        assert!("single_asset_signal".parse::<StrategyModeId>().is_err());
        assert!("calendar_event_session".parse::<StrategyModeId>().is_err());
        assert!("dynamic_allocation_rules"
            .parse::<StrategyModeId>()
            .is_err());
        assert!("walk_forward_analysis".parse::<StrategyModeId>().is_err());
    }

    #[test]
    fn strategy_profiles_are_typed_separately_from_engine_mode() {
        assert_eq!(
            "selection_timing_portfolio"
                .parse::<StrategyProfileId>()
                .unwrap(),
            StrategyProfileId::SelectionTimingPortfolio
        );
        assert_eq!(
            "multi_leg_event_portfolio"
                .parse::<StrategyProfileId>()
                .unwrap(),
            StrategyProfileId::MultiLegEventPortfolio
        );
        assert!("momentum_strategy".parse::<StrategyProfileId>().is_err());
    }

    #[test]
    fn strategy_presets_are_authoring_shortcuts_not_engine_modes() {
        assert_eq!(
            "single_asset_signal".parse::<StrategyPresetId>().unwrap(),
            StrategyPresetId::SingleAssetSignal
        );
        assert!("fixed_allocation_basic"
            .parse::<StrategyPresetId>()
            .is_err());
        assert!("simple_rotation".parse::<StrategyPresetId>().is_err());
        assert!("owner_private_strategy"
            .parse::<StrategyPresetId>()
            .is_err());
    }

    #[test]
    fn workflow_supports_rolling_validation() {
        assert_eq!(
            "rolling_validation".parse::<WorkflowId>().unwrap(),
            WorkflowId::RollingValidation
        );
    }

    #[test]
    fn factor_preprocess_ops_are_fail_fast() {
        assert_eq!(
            "neutralize".parse::<FactorPreprocessOp>().unwrap(),
            FactorPreprocessOp::Neutralize
        );
        assert!("normalize".parse::<FactorPreprocessOp>().is_err());
    }

    #[test]
    fn factor_composite_methods_are_fail_fast() {
        assert_eq!(
            "ic_weight".parse::<FactorCompositeMethod>().unwrap(),
            FactorCompositeMethod::IcWeight
        );
        assert!("magic_weight".parse::<FactorCompositeMethod>().is_err());
    }
}
