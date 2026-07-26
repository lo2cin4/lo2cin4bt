use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const FLATTEN_ACTION: &str = "flatten";
pub const PERMANENT_STOP_ACTION: &str = "permanent_stop";
pub const SHADOW_ACTION: &str = "shadow_until_recovery";
pub const SHADOW_RECOVERY_ARMED_ACTION: &str = "shadow_recovery_armed";
pub const SHADOW_RECOVERY_RESUMED_ACTION: &str = "shadow_recovery_resumed";

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskRunMode {
    #[default]
    Live,
    PermanentStopped,
    Shadow,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskControlState {
    mode: RiskRunMode,
    recovery_target: f64,
    recovery_armed: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum RiskControlError {
    #[error("unsupported stateful risk action: {0}")]
    UnsupportedAction(String),
    #[error("shadow recovery target must be positive and finite")]
    InvalidRecoveryTarget,
}

impl RiskControlState {
    pub fn mode(&self) -> RiskRunMode {
        self.mode
    }

    pub fn live_orders_allowed(&self) -> bool {
        self.mode == RiskRunMode::Live
    }

    pub fn is_shadow(&self) -> bool {
        self.mode == RiskRunMode::Shadow
    }

    pub fn recovery_target(&self) -> f64 {
        self.recovery_target
    }

    pub fn recovery_armed(&self) -> bool {
        self.recovery_armed
    }

    pub fn activate(&mut self, action: &str, equity_peak: f64) -> Result<(), RiskControlError> {
        match action {
            FLATTEN_ACTION => Ok(()),
            PERMANENT_STOP_ACTION => {
                self.mode = RiskRunMode::PermanentStopped;
                self.recovery_target = 0.0;
                self.recovery_armed = false;
                Ok(())
            }
            SHADOW_ACTION => {
                if !equity_peak.is_finite() || equity_peak <= 0.0 {
                    return Err(RiskControlError::InvalidRecoveryTarget);
                }
                self.mode = RiskRunMode::Shadow;
                self.recovery_target = equity_peak;
                self.recovery_armed = false;
                Ok(())
            }
            other => Err(RiskControlError::UnsupportedAction(other.to_string())),
        }
    }

    pub fn observe_shadow_equity(&mut self, shadow_equity: f64) -> bool {
        if self.mode != RiskRunMode::Shadow
            || self.recovery_armed
            || !shadow_equity.is_finite()
            || shadow_equity < self.recovery_target - 1e-12
        {
            return false;
        }
        self.recovery_armed = true;
        true
    }

    pub fn resume_on_next_action(&mut self) -> bool {
        if self.mode != RiskRunMode::Shadow || !self.recovery_armed {
            return false;
        }
        *self = Self::default();
        true
    }
}

pub fn canonical_stateful_action(action: Option<&str>) -> &str {
    match action.map(str::trim).filter(|value| !value.is_empty()) {
        None | Some("none") => FLATTEN_ACTION,
        Some(value) => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_stop_never_reopens_live_orders() {
        let mut state = RiskControlState::default();
        state
            .activate(PERMANENT_STOP_ACTION, 100.0)
            .expect("permanent stop should activate");
        assert_eq!(state.mode(), RiskRunMode::PermanentStopped);
        assert!(!state.live_orders_allowed());
        assert!(!state.resume_on_next_action());
    }

    #[test]
    fn shadow_waits_for_recovery_and_next_action() {
        let mut state = RiskControlState::default();
        state
            .activate(SHADOW_ACTION, 100.0)
            .expect("shadow should activate");
        assert!(!state.observe_shadow_equity(99.0));
        assert!(state.observe_shadow_equity(100.0));
        assert!(state.recovery_armed());
        assert!(state.resume_on_next_action());
        assert_eq!(state.mode(), RiskRunMode::Live);
    }
}
