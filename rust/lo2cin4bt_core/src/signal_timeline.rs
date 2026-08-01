use crate::artifact_tables::write_result_rows_parquet;
use crate::candidate_identity::parse_candidate_id;
use crate::computed_fields::returns::{
    annualized_return, session_return_series, simple_return, ReturnSeriesError, SessionReturnSeries,
};
use crate::result_validator::ResultValidationReport;
use crate::timeline::{
    run_timeline_accounting, TimelineAccountingConfig, TimelineAccountingError,
    TimelineAccountingSummary, TimelineActionInput, TimelineCheckpointInput,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SignalTimelineError {
    #[error("signal timeline input arrays must have equal non-zero length")]
    InvalidLength,
    #[error("asset symbol is required")]
    MissingAsset,
    #[error("target weight must be finite and non-negative")]
    InvalidTargetWeight,
    #[error("non-finite price in {field} at row {row}")]
    NonFinitePrice { field: &'static str, row: usize },
    #[error(transparent)]
    ReturnSeries(#[from] ReturnSeriesError),
    #[error(transparent)]
    Accounting(#[from] TimelineAccountingError),
    #[error("artifact export failed: {0}")]
    ArtifactExport(String),
    #[error("invalid canonical candidate_id: {0}")]
    InvalidCandidateId(String),
    #[error("duplicate canonical candidate_id: {0}")]
    DuplicateCandidateId(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleAssetNextOpenSignalInput {
    pub config: TimelineAccountingConfig,
    pub asset: String,
    pub dates: Vec<String>,
    pub open: Vec<f64>,
    pub close: Vec<f64>,
    pub entry_signal: Vec<bool>,
    pub exit_signal: Vec<bool>,
    #[serde(default = "default_target_weight")]
    pub target_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleAssetSignalCandidateInput {
    pub candidate_id: String,
    #[serde(default)]
    pub resolved_params: BTreeMap<String, String>,
    pub entry_signal: Vec<bool>,
    pub exit_signal: Vec<bool>,
    #[serde(default = "default_target_weight")]
    pub target_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleAssetSignalBatchInput {
    pub config: TimelineAccountingConfig,
    pub asset: String,
    pub dates: Vec<String>,
    pub open: Vec<f64>,
    pub close: Vec<f64>,
    #[serde(default)]
    pub include_full_results: bool,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
    pub candidates: Vec<SingleAssetSignalCandidateInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleAssetSignalCompactResult {
    pub candidate_id: String,
    pub resolved_params: BTreeMap<String, String>,
    pub final_equity: f64,
    pub total_return: f64,
    pub cagr: f64,
    pub sharpe: f64,
    pub max_drawdown: f64,
    pub intraday_max_drawdown: Option<f64>,
    pub days: usize,
    pub active_rebalances: usize,
    pub average_turnover: f64,
    pub average_gross_exposure: f64,
    pub result_validation: ResultValidationReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline: Option<TimelineAccountingSummary>,
}

fn register_candidate_id(
    candidate_id: String,
    seen_ids: &mut HashSet<String>,
) -> Result<String, SignalTimelineError> {
    parse_candidate_id(&candidate_id).map_err(SignalTimelineError::InvalidCandidateId)?;
    if !seen_ids.insert(candidate_id.clone()) {
        return Err(SignalTimelineError::DuplicateCandidateId(candidate_id));
    }
    Ok(candidate_id)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleAssetSignalBatchSummary {
    pub candidate_count: usize,
    pub results: Vec<SingleAssetSignalCompactResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_bundle: Option<RustArtifactBundle>,
    #[serde(skip)]
    pub(crate) trusted_timelines: Vec<TimelineAccountingSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RustArtifactBundle {
    pub schema_version: String,
    pub artifact_type: String,
    pub run_id: String,
    pub candidate_count: usize,
    pub bundle_paths: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarSameSessionCandidateInput {
    pub candidate_id: String,
    #[serde(default)]
    pub resolved_params: BTreeMap<String, String>,
    pub ordinal: i32,
    pub weekday: String,
    #[serde(default)]
    pub months: Vec<u32>,
    #[serde(default = "default_target_weight")]
    pub target_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarSameSessionBatchInput {
    pub config: TimelineAccountingConfig,
    pub asset: String,
    pub dates: Vec<String>,
    pub open: Vec<f64>,
    pub close: Vec<f64>,
    #[serde(default)]
    pub include_full_results: bool,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
    pub candidates: Vec<CalendarSameSessionCandidateInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarOverlayBatchInput {
    pub config: TimelineAccountingConfig,
    pub assets: Vec<String>,
    pub dates: Vec<String>,
    pub open: BTreeMap<String, Vec<f64>>,
    pub close: BTreeMap<String, Vec<f64>>,
    pub baseline_weights: BTreeMap<String, f64>,
    pub event_weights: BTreeMap<String, f64>,
    #[serde(default)]
    pub include_full_results: bool,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
    pub candidates: Vec<CalendarSameSessionCandidateInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetTimerCandidateInput {
    pub candidate_id: String,
    #[serde(default)]
    pub resolved_params: BTreeMap<String, String>,
    pub entry_signal: Vec<bool>,
    pub hold_bars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResetTimerBatchInput {
    pub config: TimelineAccountingConfig,
    pub assets: Vec<String>,
    pub dates: Vec<String>,
    pub open: BTreeMap<String, Vec<f64>>,
    pub close: BTreeMap<String, Vec<f64>>,
    pub baseline_weights: BTreeMap<String, f64>,
    pub event_weights: BTreeMap<String, f64>,
    pub restore_weights: BTreeMap<String, f64>,
    #[serde(default)]
    pub entry_offset_bars: usize,
    #[serde(default)]
    pub entry_phase: String,
    #[serde(default)]
    pub restore_phase: String,
    #[serde(default)]
    pub include_full_results: bool,
    #[serde(default)]
    pub artifact_output_dir: Option<String>,
    #[serde(default)]
    pub artifact_run_id: Option<String>,
    pub candidates: Vec<ResetTimerCandidateInput>,
}

fn default_target_weight() -> f64 {
    1.0
}

pub fn run_single_asset_next_open_signal_timeline(
    input: SingleAssetNextOpenSignalInput,
) -> Result<TimelineAccountingSummary, SignalTimelineError> {
    validate_signal_input(&input)?;
    let asset = input.asset.trim().to_string();
    let mut checkpoints = Vec::with_capacity(input.dates.len() * 2);
    let returns = session_return_series(&input.open, &input.close)?;

    for row_idx in 0..input.dates.len() {
        let mut open_actions = Vec::new();
        if row_idx > 0 {
            if input.entry_signal[row_idx - 1] {
                open_actions.push(TimelineActionInput {
                    action: "enter".to_string(),
                    target_weights: weights(&asset, input.target_weight),
                    reason: Some("entry +1 open".to_string()),
                });
            }
            if input.exit_signal[row_idx - 1] {
                open_actions.push(TimelineActionInput {
                    action: "exit".to_string(),
                    target_weights: BTreeMap::new(),
                    reason: Some("exit +1 open".to_string()),
                });
            }
        }

        checkpoints.push(TimelineCheckpointInput {
            date: input.dates[row_idx].clone(),
            phase: "open".to_string(),
            returns: asset_return(&asset, returns.overnight[row_idx]),
            actions: open_actions,
        });
        checkpoints.push(TimelineCheckpointInput {
            date: input.dates[row_idx].clone(),
            phase: "close".to_string(),
            returns: asset_return(&asset, returns.intraday[row_idx]),
            actions: Vec::new(),
        });
    }

    Ok(run_timeline_accounting(
        crate::timeline::TimelineAccountingInput {
            config: input.config,
            checkpoints,
        },
    )?)
}

pub fn run_single_asset_next_open_signal_batch(
    input: SingleAssetSignalBatchInput,
) -> Result<SingleAssetSignalBatchSummary, SignalTimelineError> {
    let row_count = input.dates.len();
    validate_common_series(&input.asset, &input.dates, &input.open, &input.close)?;
    let mut seen_ids = HashSet::new();
    let mut results = Vec::with_capacity(input.candidates.len());
    let mut full_summaries: Vec<(String, TimelineAccountingSummary)> = Vec::new();
    let mut trusted_timelines = Vec::with_capacity(input.candidates.len());
    let export_artifacts = input
        .artifact_output_dir
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    for candidate in input.candidates {
        if candidate.entry_signal.len() != row_count || candidate.exit_signal.len() != row_count {
            return Err(SignalTimelineError::InvalidLength);
        }
        let candidate_id = register_candidate_id(candidate.candidate_id, &mut seen_ids)?;

        let summary = run_single_asset_next_open_signal_timeline(SingleAssetNextOpenSignalInput {
            config: input.config.clone(),
            asset: input.asset.clone(),
            dates: input.dates.clone(),
            open: input.open.clone(),
            close: input.close.clone(),
            entry_signal: candidate.entry_signal,
            exit_signal: candidate.exit_signal,
            target_weight: candidate.target_weight,
        })?;
        if export_artifacts {
            full_summaries.push((candidate_id.clone(), summary.clone()));
        }
        trusted_timelines.push(summary.clone());
        results.push(SingleAssetSignalCompactResult {
            candidate_id,
            resolved_params: candidate.resolved_params,
            final_equity: summary.final_equity,
            total_return: summary.total_return,
            cagr: summary_cagr(&summary),
            sharpe: summary_sharpe(&summary),
            max_drawdown: summary_max_drawdown(&summary),
            intraday_max_drawdown: summary.intraday_max_drawdown,
            days: summary.days,
            active_rebalances: summary.active_rebalances,
            average_turnover: summary.average_turnover,
            average_gross_exposure: summary.average_gross_exposure,
            result_validation: summary.result_validation.clone(),
            timeline: if input.include_full_results && !export_artifacts {
                Some(summary)
            } else {
                None
            },
        });
    }
    let artifact_bundle = if export_artifacts {
        Some(export_single_asset_signal_bundle(
            input.artifact_output_dir.as_deref().unwrap_or_default(),
            input.artifact_run_id.as_deref().unwrap_or("signal_matrix"),
            &full_summaries,
        )?)
    } else {
        None
    };

    Ok(SingleAssetSignalBatchSummary {
        candidate_count: results.len(),
        results,
        artifact_bundle,
        trusted_timelines,
    })
}

fn export_single_asset_signal_bundle(
    output_dir: &str,
    run_id: &str,
    summaries: &[(String, TimelineAccountingSummary)],
) -> Result<RustArtifactBundle, SignalTimelineError> {
    let output_path = PathBuf::from(output_dir);
    fs::create_dir_all(&output_path)
        .map_err(|exc| SignalTimelineError::ArtifactExport(exc.to_string()))?;
    let safe_run_id = slugify(run_id);
    let mut bundle_paths = BTreeMap::new();
    let table_specs = [
        ("equity_curve", "equity_curve"),
        ("execution_equity_curve", "execution_equity_curve"),
        ("holdings", "holdings"),
        ("rebalance_audit", "rebalance_audit"),
        ("rebalance_trades", "rebalance_trades"),
        ("risk_gate_events", "risk_gate_events"),
        ("settlements", "settlements"),
    ];
    for (table_key, file_key) in table_specs {
        let rows = combined_table_rows(summaries, table_key);
        let path = output_path.join(format!("{safe_run_id}_{file_key}.parquet"));
        write_result_rows_parquet(&path, &rows, table_key)
            .map_err(SignalTimelineError::ArtifactExport)?;
        bundle_paths.insert(file_key.to_string(), path.to_string_lossy().to_string());
    }
    Ok(RustArtifactBundle {
        schema_version: "rust_portfolio_result_bundle.v1".to_string(),
        artifact_type: "rust_single_asset_signal_matrix_bundle".to_string(),
        run_id: safe_run_id,
        candidate_count: summaries.len(),
        bundle_paths,
    })
}

fn combined_table_rows(
    summaries: &[(String, TimelineAccountingSummary)],
    table_key: &str,
) -> Vec<BTreeMap<String, Value>> {
    let mut out = Vec::new();
    for (candidate_id, summary) in summaries {
        let rows = match table_key {
            "equity_curve" => &summary.result_tables.equity_curve,
            "execution_equity_curve" => &summary.result_tables.execution_equity_curve,
            "holdings" => &summary.result_tables.holdings,
            "rebalance_audit" => &summary.result_tables.rebalance_audit,
            "rebalance_trades" => &summary.result_tables.rebalance_trades,
            "risk_gate_events" => &summary.result_tables.risk_gate_events,
            "settlements" => &summary.result_tables.settlements,
            _ => continue,
        };
        for row in rows {
            let mut enriched = row.clone();
            enriched.insert(
                "Backtest_id".to_string(),
                Value::String(candidate_id.clone()),
            );
            out.push(enriched);
        }
    }
    out
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut previous_underscore = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            previous_underscore = false;
        } else if !previous_underscore {
            out.push('_');
            previous_underscore = true;
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "signal_matrix".to_string()
    } else {
        trimmed
    }
}

pub fn run_single_asset_calendar_same_session_batch(
    input: CalendarSameSessionBatchInput,
) -> Result<SingleAssetSignalBatchSummary, SignalTimelineError> {
    validate_common_series(&input.asset, &input.dates, &input.open, &input.close)?;
    let parsed_dates = input
        .dates
        .iter()
        .map(|date| parse_ymd(date))
        .collect::<Result<Vec<_>, _>>()?;
    let mut seen_ids = HashSet::new();
    let mut results = Vec::with_capacity(input.candidates.len());
    let mut full_summaries: Vec<(String, TimelineAccountingSummary)> = Vec::new();
    let export_artifacts = input
        .artifact_output_dir
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    for candidate in input.candidates {
        if !candidate.target_weight.is_finite() || candidate.target_weight < 0.0 {
            return Err(SignalTimelineError::InvalidTargetWeight);
        }
        let weekday =
            parse_weekday(&candidate.weekday).ok_or(SignalTimelineError::InvalidLength)?;
        let candidate_id = register_candidate_id(candidate.candidate_id, &mut seen_ids)?;

        let summary = run_calendar_same_session_candidate(
            &input.config,
            &input.asset,
            &input.dates,
            &parsed_dates,
            &input.open,
            &input.close,
            candidate.ordinal,
            weekday,
            &candidate.months,
            candidate.target_weight,
        )?;
        if export_artifacts {
            full_summaries.push((candidate_id.clone(), summary.clone()));
        }
        results.push(SingleAssetSignalCompactResult {
            candidate_id,
            resolved_params: candidate.resolved_params,
            final_equity: summary.final_equity,
            total_return: summary.total_return,
            cagr: summary_cagr(&summary),
            sharpe: summary_sharpe(&summary),
            max_drawdown: summary_max_drawdown(&summary),
            intraday_max_drawdown: summary.intraday_max_drawdown,
            days: summary.days,
            active_rebalances: summary.active_rebalances,
            average_turnover: summary.average_turnover,
            average_gross_exposure: summary.average_gross_exposure,
            result_validation: summary.result_validation.clone(),
            timeline: if input.include_full_results && !export_artifacts {
                Some(summary)
            } else {
                None
            },
        });
    }
    let artifact_bundle = if export_artifacts {
        Some(export_single_asset_signal_bundle(
            input.artifact_output_dir.as_deref().unwrap_or_default(),
            input
                .artifact_run_id
                .as_deref()
                .unwrap_or("calendar_same_session_matrix"),
            &full_summaries,
        )?)
    } else {
        None
    };

    Ok(SingleAssetSignalBatchSummary {
        candidate_count: results.len(),
        results,
        artifact_bundle,
        trusted_timelines: Vec::new(),
    })
}

pub fn run_calendar_overlay_batch(
    input: CalendarOverlayBatchInput,
) -> Result<SingleAssetSignalBatchSummary, SignalTimelineError> {
    validate_overlay_input(&input)?;
    let parsed_dates = input
        .dates
        .iter()
        .map(|date| parse_ymd(date))
        .collect::<Result<Vec<_>, _>>()?;
    let mut seen_ids = HashSet::new();
    let mut results = Vec::with_capacity(input.candidates.len());
    let mut full_summaries: Vec<(String, TimelineAccountingSummary)> = Vec::new();
    let export_artifacts = input
        .artifact_output_dir
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);

    for candidate in input.candidates {
        let weekday =
            parse_weekday(&candidate.weekday).ok_or(SignalTimelineError::InvalidLength)?;
        let candidate_id = register_candidate_id(candidate.candidate_id, &mut seen_ids)?;

        let summary = run_calendar_overlay_candidate(
            &input.config,
            &input.assets,
            &input.dates,
            &parsed_dates,
            &input.open,
            &input.close,
            &input.baseline_weights,
            &input.event_weights,
            candidate.ordinal,
            weekday,
            &candidate.months,
        )?;
        if export_artifacts {
            full_summaries.push((candidate_id.clone(), summary.clone()));
        }
        results.push(SingleAssetSignalCompactResult {
            candidate_id,
            resolved_params: candidate.resolved_params,
            final_equity: summary.final_equity,
            total_return: summary.total_return,
            cagr: summary_cagr(&summary),
            sharpe: summary_sharpe(&summary),
            max_drawdown: summary_max_drawdown(&summary),
            intraday_max_drawdown: summary.intraday_max_drawdown,
            days: summary.days,
            active_rebalances: summary.active_rebalances,
            average_turnover: summary.average_turnover,
            average_gross_exposure: summary.average_gross_exposure,
            result_validation: summary.result_validation.clone(),
            timeline: if input.include_full_results && !export_artifacts {
                Some(summary)
            } else {
                None
            },
        });
    }
    let artifact_bundle = if export_artifacts {
        Some(export_single_asset_signal_bundle(
            input.artifact_output_dir.as_deref().unwrap_or_default(),
            input
                .artifact_run_id
                .as_deref()
                .unwrap_or("calendar_overlay_matrix"),
            &full_summaries,
        )?)
    } else {
        None
    };

    Ok(SingleAssetSignalBatchSummary {
        candidate_count: results.len(),
        results,
        artifact_bundle,
        trusted_timelines: Vec::new(),
    })
}

pub fn run_reset_timer_batch(
    input: ResetTimerBatchInput,
) -> Result<SingleAssetSignalBatchSummary, SignalTimelineError> {
    validate_reset_timer_input(&input)?;
    let mut seen_ids = HashSet::new();
    let mut results = Vec::with_capacity(input.candidates.len());
    let mut full_summaries: Vec<(String, TimelineAccountingSummary)> = Vec::new();
    let export_artifacts = input
        .artifact_output_dir
        .as_ref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    let entry_phase = normalize_reset_phase(&input.entry_phase, "open")?;
    let restore_phase = normalize_reset_phase(&input.restore_phase, "close")?;
    let row_count = input.dates.len();

    for candidate in input.candidates {
        if candidate.entry_signal.len() != row_count {
            return Err(SignalTimelineError::InvalidLength);
        }
        let candidate_id = register_candidate_id(candidate.candidate_id, &mut seen_ids)?;

        let summary = run_reset_timer_candidate(
            &input.config,
            &input.assets,
            &input.dates,
            &input.open,
            &input.close,
            &input.baseline_weights,
            &input.event_weights,
            &input.restore_weights,
            &candidate.entry_signal,
            input.entry_offset_bars,
            &entry_phase,
            candidate.hold_bars,
            &restore_phase,
        )?;
        if export_artifacts {
            full_summaries.push((candidate_id.clone(), summary.clone()));
        }
        results.push(SingleAssetSignalCompactResult {
            candidate_id,
            resolved_params: candidate.resolved_params,
            final_equity: summary.final_equity,
            total_return: summary.total_return,
            cagr: summary_cagr(&summary),
            sharpe: summary_sharpe(&summary),
            max_drawdown: summary_max_drawdown(&summary),
            intraday_max_drawdown: summary.intraday_max_drawdown,
            days: summary.days,
            active_rebalances: summary.active_rebalances,
            average_turnover: summary.average_turnover,
            average_gross_exposure: summary.average_gross_exposure,
            result_validation: summary.result_validation.clone(),
            timeline: if input.include_full_results && !export_artifacts {
                Some(summary)
            } else {
                None
            },
        });
    }
    let artifact_bundle = if export_artifacts {
        Some(export_single_asset_signal_bundle(
            input.artifact_output_dir.as_deref().unwrap_or_default(),
            input
                .artifact_run_id
                .as_deref()
                .unwrap_or("reset_timer_matrix"),
            &full_summaries,
        )?)
    } else {
        None
    };

    Ok(SingleAssetSignalBatchSummary {
        candidate_count: results.len(),
        results,
        artifact_bundle,
        trusted_timelines: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn run_calendar_same_session_candidate(
    config: &TimelineAccountingConfig,
    asset: &str,
    dates: &[String],
    parsed_dates: &[(i32, u32, u32)],
    open: &[f64],
    close: &[f64],
    ordinal: i32,
    weekday: u32,
    months: &[u32],
    target_weight: f64,
) -> Result<TimelineAccountingSummary, SignalTimelineError> {
    let mut checkpoints = Vec::with_capacity(dates.len() * 2);
    let returns = session_return_series(open, close)?;
    for row_idx in 0..dates.len() {
        let is_entry = month_allowed(parsed_dates[row_idx].1, months)
            && is_nth_weekday_of_month(parsed_dates[row_idx], ordinal, weekday);
        let open_actions = if is_entry {
            vec![TimelineActionInput {
                action: "enter".to_string(),
                target_weights: weights(asset, target_weight),
                reason: Some("calendar same-session open entry".to_string()),
            }]
        } else {
            Vec::new()
        };
        let close_actions = if is_entry {
            vec![TimelineActionInput {
                action: "exit".to_string(),
                target_weights: BTreeMap::new(),
                reason: Some("calendar same-session close exit".to_string()),
            }]
        } else {
            Vec::new()
        };
        checkpoints.push(TimelineCheckpointInput {
            date: dates[row_idx].clone(),
            phase: "open".to_string(),
            returns: asset_return(asset, returns.overnight[row_idx]),
            actions: open_actions,
        });
        checkpoints.push(TimelineCheckpointInput {
            date: dates[row_idx].clone(),
            phase: "close".to_string(),
            returns: asset_return(asset, returns.intraday[row_idx]),
            actions: close_actions,
        });
    }

    Ok(run_timeline_accounting(
        crate::timeline::TimelineAccountingInput {
            config: config.clone(),
            checkpoints,
        },
    )?)
}

#[allow(clippy::too_many_arguments)]
fn run_calendar_overlay_candidate(
    config: &TimelineAccountingConfig,
    assets: &[String],
    dates: &[String],
    parsed_dates: &[(i32, u32, u32)],
    open: &BTreeMap<String, Vec<f64>>,
    close: &BTreeMap<String, Vec<f64>>,
    baseline_weights: &BTreeMap<String, f64>,
    event_weights: &BTreeMap<String, f64>,
    ordinal: i32,
    weekday: u32,
    months: &[u32],
) -> Result<TimelineAccountingSummary, SignalTimelineError> {
    let mut checkpoints = Vec::with_capacity(dates.len() * 2);
    let returns = session_returns_by_asset(assets, open, close)?;
    for row_idx in 0..dates.len() {
        let mut open_returns = BTreeMap::new();
        let mut close_returns = BTreeMap::new();
        for asset in assets {
            let series = returns
                .get(asset)
                .ok_or(SignalTimelineError::MissingAsset)?;
            let open_return = series.overnight[row_idx];
            let close_return = series.intraday[row_idx];
            open_returns.insert(asset.clone(), open_return);
            close_returns.insert(asset.clone(), close_return);
        }

        let is_entry = month_allowed(parsed_dates[row_idx].1, months)
            && is_nth_weekday_of_month(parsed_dates[row_idx], ordinal, weekday);
        let mut open_actions = vec![TimelineActionInput {
            action: "set_target_weights".to_string(),
            target_weights: baseline_weights.clone(),
            reason: Some("session open: restore baseline".to_string()),
        }];
        if is_entry {
            open_actions.push(TimelineActionInput {
                action: "set_target_weights".to_string(),
                target_weights: event_weights.clone(),
                reason: Some("event open: enter overlay".to_string()),
            });
        }
        let close_actions = if is_entry {
            vec![TimelineActionInput {
                action: "set_target_weights".to_string(),
                target_weights: baseline_weights.clone(),
                reason: Some("event close: restore baseline".to_string()),
            }]
        } else {
            Vec::new()
        };

        checkpoints.push(TimelineCheckpointInput {
            date: dates[row_idx].clone(),
            phase: "open".to_string(),
            returns: open_returns,
            actions: open_actions,
        });
        checkpoints.push(TimelineCheckpointInput {
            date: dates[row_idx].clone(),
            phase: "close".to_string(),
            returns: close_returns,
            actions: close_actions,
        });
    }

    Ok(run_timeline_accounting(
        crate::timeline::TimelineAccountingInput {
            config: config.clone(),
            checkpoints,
        },
    )?)
}

#[allow(clippy::too_many_arguments)]
fn run_reset_timer_candidate(
    config: &TimelineAccountingConfig,
    assets: &[String],
    dates: &[String],
    open: &BTreeMap<String, Vec<f64>>,
    close: &BTreeMap<String, Vec<f64>>,
    baseline_weights: &BTreeMap<String, f64>,
    event_weights: &BTreeMap<String, f64>,
    restore_weights: &BTreeMap<String, f64>,
    entry_signal: &[bool],
    entry_offset_bars: usize,
    entry_phase: &str,
    hold_bars: usize,
    restore_phase: &str,
) -> Result<TimelineAccountingSummary, SignalTimelineError> {
    let mut checkpoints = Vec::with_capacity(dates.len() * 2);
    let mut pending_restore_idx: Option<usize> = None;
    let returns = session_returns_by_asset(assets, open, close)?;
    for row_idx in 0..dates.len() {
        let mut open_returns = BTreeMap::new();
        let mut close_returns = BTreeMap::new();
        for asset in assets {
            let series = returns
                .get(asset)
                .ok_or(SignalTimelineError::MissingAsset)?;
            open_returns.insert(asset.clone(), series.overnight[row_idx]);
            close_returns.insert(asset.clone(), series.intraday[row_idx]);
        }

        let mut open_actions = Vec::new();
        let mut close_actions = Vec::new();
        if row_idx == 0 && !baseline_weights.is_empty() {
            let target = if entry_phase == "open" {
                &mut open_actions
            } else {
                &mut close_actions
            };
            target.push(TimelineActionInput {
                action: "set_target_weights".to_string(),
                target_weights: baseline_weights.clone(),
                reason: Some("baseline first session".to_string()),
            });
        }

        if row_idx >= entry_offset_bars {
            let signal_idx = row_idx - entry_offset_bars;
            if entry_signal[signal_idx] {
                let target = if entry_phase == "open" {
                    &mut open_actions
                } else {
                    &mut close_actions
                };
                target.push(TimelineActionInput {
                    action: "set_target_weights".to_string(),
                    target_weights: event_weights.clone(),
                    reason: Some("reset timer entry".to_string()),
                });
            }
        }

        if let Some(restore_idx) = pending_restore_idx {
            if restore_idx == row_idx {
                let target = if restore_phase == "open" {
                    &mut open_actions
                } else {
                    &mut close_actions
                };
                target.push(TimelineActionInput {
                    action: "set_target_weights".to_string(),
                    target_weights: restore_weights.clone(),
                    reason: Some("reset timer restore".to_string()),
                });
                pending_restore_idx = None;
            }
        }

        checkpoints.push(TimelineCheckpointInput {
            date: dates[row_idx].clone(),
            phase: "open".to_string(),
            returns: open_returns,
            actions: open_actions,
        });
        checkpoints.push(TimelineCheckpointInput {
            date: dates[row_idx].clone(),
            phase: "close".to_string(),
            returns: close_returns,
            actions: close_actions,
        });

        if entry_signal[row_idx] {
            let restore_idx = row_idx.saturating_add(hold_bars);
            pending_restore_idx = Some(restore_idx);
        }
    }

    Ok(run_timeline_accounting(
        crate::timeline::TimelineAccountingInput {
            config: config.clone(),
            checkpoints,
        },
    )?)
}

fn session_returns_by_asset(
    assets: &[String],
    open: &BTreeMap<String, Vec<f64>>,
    close: &BTreeMap<String, Vec<f64>>,
) -> Result<BTreeMap<String, SessionReturnSeries>, SignalTimelineError> {
    assets
        .iter()
        .map(|asset| {
            let open_series = open.get(asset).ok_or(SignalTimelineError::MissingAsset)?;
            let close_series = close.get(asset).ok_or(SignalTimelineError::MissingAsset)?;
            Ok((
                asset.clone(),
                session_return_series(open_series, close_series)?,
            ))
        })
        .collect()
}

fn validate_signal_input(
    input: &SingleAssetNextOpenSignalInput,
) -> Result<(), SignalTimelineError> {
    validate_common_series(&input.asset, &input.dates, &input.open, &input.close)?;
    if input.entry_signal.len() != input.dates.len() || input.exit_signal.len() != input.dates.len()
    {
        return Err(SignalTimelineError::InvalidLength);
    }
    if !input.target_weight.is_finite() || input.target_weight < 0.0 {
        return Err(SignalTimelineError::InvalidTargetWeight);
    }
    Ok(())
}

fn validate_overlay_input(input: &CalendarOverlayBatchInput) -> Result<(), SignalTimelineError> {
    let row_count = input.dates.len();
    if row_count == 0 || input.assets.is_empty() {
        return Err(SignalTimelineError::InvalidLength);
    }
    for asset in &input.assets {
        if asset.trim().is_empty() {
            return Err(SignalTimelineError::MissingAsset);
        }
        let open = input
            .open
            .get(asset)
            .ok_or(SignalTimelineError::MissingAsset)?;
        let close = input
            .close
            .get(asset)
            .ok_or(SignalTimelineError::MissingAsset)?;
        if open.len() != row_count || close.len() != row_count {
            return Err(SignalTimelineError::InvalidLength);
        }
        for (row, value) in open.iter().enumerate() {
            if !value.is_finite() {
                return Err(SignalTimelineError::NonFinitePrice { field: "open", row });
            }
        }
        for (row, value) in close.iter().enumerate() {
            if !value.is_finite() {
                return Err(SignalTimelineError::NonFinitePrice {
                    field: "close",
                    row,
                });
            }
        }
    }
    validate_weights(&input.baseline_weights)?;
    validate_weights(&input.event_weights)?;
    Ok(())
}

fn validate_reset_timer_input(input: &ResetTimerBatchInput) -> Result<(), SignalTimelineError> {
    if input.assets.is_empty() {
        return Err(SignalTimelineError::MissingAsset);
    }
    if input.dates.is_empty() {
        return Err(SignalTimelineError::InvalidLength);
    }
    let row_count = input.dates.len();
    for asset in &input.assets {
        if asset.trim().is_empty() {
            return Err(SignalTimelineError::MissingAsset);
        }
        let open = input
            .open
            .get(asset)
            .ok_or(SignalTimelineError::MissingAsset)?;
        let close = input
            .close
            .get(asset)
            .ok_or(SignalTimelineError::MissingAsset)?;
        if open.len() != row_count || close.len() != row_count {
            return Err(SignalTimelineError::InvalidLength);
        }
        for (idx, value) in open.iter().enumerate() {
            if !value.is_finite() {
                return Err(SignalTimelineError::NonFinitePrice {
                    field: "open",
                    row: idx,
                });
            }
        }
        for (idx, value) in close.iter().enumerate() {
            if !value.is_finite() {
                return Err(SignalTimelineError::NonFinitePrice {
                    field: "close",
                    row: idx,
                });
            }
        }
    }
    normalize_reset_phase(&input.entry_phase, "open")?;
    normalize_reset_phase(&input.restore_phase, "close")?;
    Ok(())
}

fn normalize_reset_phase(value: &str, default_phase: &str) -> Result<String, SignalTimelineError> {
    let normalized = if value.trim().is_empty() {
        default_phase.to_string()
    } else {
        value.trim().to_ascii_lowercase()
    };
    if matches!(normalized.as_str(), "open" | "close") {
        Ok(normalized)
    } else {
        Err(SignalTimelineError::InvalidLength)
    }
}

fn validate_weights(weights: &BTreeMap<String, f64>) -> Result<(), SignalTimelineError> {
    for (asset, weight) in weights {
        if asset.trim().is_empty() {
            return Err(SignalTimelineError::MissingAsset);
        }
        if !weight.is_finite() {
            return Err(SignalTimelineError::InvalidTargetWeight);
        }
    }
    Ok(())
}

fn validate_common_series(
    asset: &str,
    dates: &[String],
    open: &[f64],
    close: &[f64],
) -> Result<(), SignalTimelineError> {
    let row_count = dates.len();
    if row_count == 0 || open.len() != row_count || close.len() != row_count {
        return Err(SignalTimelineError::InvalidLength);
    }
    if asset.trim().is_empty() {
        return Err(SignalTimelineError::MissingAsset);
    }
    for (row, value) in open.iter().enumerate() {
        if !value.is_finite() {
            return Err(SignalTimelineError::NonFinitePrice { field: "open", row });
        }
    }
    for (row, value) in close.iter().enumerate() {
        if !value.is_finite() {
            return Err(SignalTimelineError::NonFinitePrice {
                field: "close",
                row,
            });
        }
    }
    Ok(())
}

fn weights(asset: &str, value: f64) -> BTreeMap<String, f64> {
    let mut weights = BTreeMap::new();
    if value.abs() > 1e-12 {
        weights.insert(asset.to_string(), value);
    }
    weights
}

fn asset_return(asset: &str, value: f64) -> BTreeMap<String, f64> {
    BTreeMap::from([(asset.to_string(), value)])
}

fn month_allowed(month: u32, months: &[u32]) -> bool {
    months.is_empty() || months.contains(&month)
}

fn parse_ymd(date: &str) -> Result<(i32, u32, u32), SignalTimelineError> {
    let mut parts = date.split('-');
    let year = parts
        .next()
        .and_then(|value| value.parse::<i32>().ok())
        .ok_or(SignalTimelineError::InvalidLength)?;
    let month = parts
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(SignalTimelineError::InvalidLength)?;
    let day = parts
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(SignalTimelineError::InvalidLength)?;
    if parts.next().is_some()
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
    {
        return Err(SignalTimelineError::InvalidLength);
    }
    Ok((year, month, day))
}

fn parse_weekday(value: &str) -> Option<u32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "0" | "monday" | "mon" => Some(0),
        "1" | "tuesday" | "tue" | "tues" => Some(1),
        "2" | "wednesday" | "wed" => Some(2),
        "3" | "thursday" | "thu" | "thur" | "thurs" => Some(3),
        "4" | "friday" | "fri" => Some(4),
        "5" | "saturday" | "sat" => Some(5),
        "6" | "sunday" | "sun" => Some(6),
        _ => None,
    }
}

fn is_nth_weekday_of_month(date: (i32, u32, u32), ordinal: i32, weekday: u32) -> bool {
    if ordinal == 0 {
        return false;
    }
    let (year, month, day) = date;
    if weekday_monday0(year, month, day) != weekday {
        return false;
    }
    if ordinal > 0 {
        let first_weekday = weekday_monday0(year, month, 1);
        let first_target = 1 + ((weekday + 7 - first_weekday) % 7);
        return day == first_target + ((ordinal as u32 - 1) * 7);
    }
    let end_day = days_in_month(year, month);
    let end_weekday = weekday_monday0(year, month, end_day);
    let last_target = end_day - ((end_weekday + 7 - weekday) % 7);
    day == last_target - ((ordinal.unsigned_abs() - 1) * 7)
}

fn weekday_monday0(year: i32, month: u32, day: u32) -> u32 {
    let mut y = year;
    let mut m = month as i32;
    if m < 3 {
        m += 12;
        y -= 1;
    }
    let k = y % 100;
    let j = y / 100;
    let h = (day as i32 + ((13 * (m + 1)) / 5) + k + (k / 4) + (j / 4) + (5 * j)) % 7;
    ((h + 5) % 7) as u32
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn summary_daily_returns(summary: &TimelineAccountingSummary) -> Vec<f64> {
    let mut returns = Vec::with_capacity(summary.daily_events.len());
    let mut previous_equity = summary.start_equity;
    for event in &summary.daily_events {
        let equity = event.equity_after_trade;
        if previous_equity > 0.0 && equity.is_finite() {
            returns.push(simple_return(equity, previous_equity));
        } else {
            returns.push(0.0);
        }
        previous_equity = equity;
    }
    returns
}

fn summary_sharpe(summary: &TimelineAccountingSummary) -> f64 {
    let returns = summary_daily_returns(summary);
    if returns.is_empty() {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / returns.len() as f64;
    let std = variance.sqrt();
    if std > 0.0 {
        mean / std * 252.0_f64.sqrt()
    } else {
        0.0
    }
}

fn summary_cagr(summary: &TimelineAccountingSummary) -> f64 {
    if summary.start_equity <= 0.0 || summary.final_equity <= 0.0 || summary.days == 0 {
        return 0.0;
    }
    let years = summary.days as f64 / 252.0;
    if years > 0.0 {
        annualized_return(
            simple_return(summary.final_equity, summary.start_equity),
            years,
        )
    } else {
        0.0
    }
}

fn summary_max_drawdown(summary: &TimelineAccountingSummary) -> f64 {
    let mut peak = summary.start_equity;
    let mut max_drawdown = 0.0;
    for event in &summary.daily_events {
        let equity = event.equity_after_trade;
        if !equity.is_finite() {
            continue;
        }
        if equity > peak {
            peak = equity;
        }
        if peak > 0.0 {
            let drawdown = simple_return(equity, peak);
            if drawdown < max_drawdown {
                max_drawdown = drawdown;
            }
        }
    }
    max_drawdown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_asset_next_open_signal_produces_timeline_summary() {
        let summary = run_single_asset_next_open_signal_timeline(SingleAssetNextOpenSignalInput {
            config: TimelineAccountingConfig::default(),
            asset: "AAA".to_string(),
            dates: vec![
                "2024-01-02".to_string(),
                "2024-01-03".to_string(),
                "2024-01-04".to_string(),
            ],
            open: vec![100.0, 110.0, 120.0],
            close: vec![100.0, 115.0, 118.0],
            entry_signal: vec![true, false, false],
            exit_signal: vec![false, true, false],
            target_weight: 1.0,
        })
        .expect("signal timeline should run");

        assert_eq!(summary.days, 3);
        assert_eq!(summary.active_rebalances, 2);
        assert!(summary.final_equity > 100.0);
        assert_eq!(summary.events[2].actions[0].action, "enter");
        assert_eq!(summary.events[4].actions[0].action, "exit");
    }

    #[test]
    fn single_asset_next_open_signal_batch_returns_summary_results() {
        let input = SingleAssetSignalBatchInput {
            config: TimelineAccountingConfig::default(),
            asset: "AAA".to_string(),
            dates: vec![
                "2024-01-02".to_string(),
                "2024-01-03".to_string(),
                "2024-01-04".to_string(),
            ],
            open: vec![100.0, 110.0, 120.0],
            close: vec![100.0, 115.0, 118.0],
            include_full_results: false,
            artifact_output_dir: None,
            artifact_run_id: None,
            candidates: vec![
                SingleAssetSignalCandidateInput {
                    candidate_id: "signal_probe:parameter_matrix:a".to_string(),
                    resolved_params: BTreeMap::from([("short".to_string(), "10".to_string())]),
                    entry_signal: vec![true, false, false],
                    exit_signal: vec![false, true, false],
                    target_weight: 1.0,
                },
                SingleAssetSignalCandidateInput {
                    candidate_id: "signal_probe:parameter_matrix:b".to_string(),
                    resolved_params: BTreeMap::from([("short".to_string(), "20".to_string())]),
                    entry_signal: vec![false, false, false],
                    exit_signal: vec![false, false, false],
                    target_weight: 1.0,
                },
            ],
        };

        let summary = run_single_asset_next_open_signal_batch(input).expect("batch should run");

        assert_eq!(summary.candidate_count, 2);
        assert_eq!(
            summary.results[0].candidate_id,
            "signal_probe:parameter_matrix:a"
        );
        assert_eq!(summary.results[0].active_rebalances, 2);
        assert_eq!(summary.results[1].final_equity, 100.0);
    }

    #[test]
    fn single_asset_next_open_signal_batch_exports_parquet_bundle() {
        let output_dir =
            std::env::temp_dir().join(format!("lo2cin4bt_signal_bundle_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&output_dir);
        let input = SingleAssetSignalBatchInput {
            config: TimelineAccountingConfig::default(),
            asset: "AAA".to_string(),
            dates: vec![
                "2024-01-02".to_string(),
                "2024-01-03".to_string(),
                "2024-01-04".to_string(),
            ],
            open: vec![100.0, 110.0, 120.0],
            close: vec![100.0, 115.0, 118.0],
            include_full_results: true,
            artifact_output_dir: Some(output_dir.to_string_lossy().to_string()),
            artifact_run_id: Some("bundle test".to_string()),
            candidates: vec![SingleAssetSignalCandidateInput {
                candidate_id: "signal_probe:single_backtest:fixed".to_string(),
                resolved_params: BTreeMap::new(),
                entry_signal: vec![true, false, false],
                exit_signal: vec![false, true, false],
                target_weight: 1.0,
            }],
        };

        let summary = run_single_asset_next_open_signal_batch(input)
            .expect("signal batch artifact export should run");
        let bundle = summary.artifact_bundle.expect("bundle should be returned");

        assert_eq!(bundle.candidate_count, 1);
        assert!(bundle.bundle_paths.contains_key("equity_curve"));
        for path in bundle.bundle_paths.values() {
            assert!(std::path::Path::new(path).exists());
        }
        assert!(summary.results[0].timeline.is_none());
        let _ = std::fs::remove_dir_all(&output_dir);
    }

    #[test]
    fn signal_batch_rejects_missing_or_duplicate_candidate_identity() {
        let candidate = SingleAssetSignalCandidateInput {
            candidate_id: "signal_probe:parameter_matrix:short_10".to_string(),
            resolved_params: BTreeMap::new(),
            entry_signal: vec![false],
            exit_signal: vec![false],
            target_weight: 1.0,
        };
        let input = |candidates| SingleAssetSignalBatchInput {
            config: TimelineAccountingConfig::default(),
            asset: "AAA".to_string(),
            dates: vec!["2024-01-02".to_string()],
            open: vec![100.0],
            close: vec![100.0],
            include_full_results: false,
            artifact_output_dir: None,
            artifact_run_id: None,
            candidates,
        };

        let mut invalid = candidate.clone();
        invalid.candidate_id.clear();
        assert!(matches!(
            run_single_asset_next_open_signal_batch(input(vec![invalid])),
            Err(SignalTimelineError::InvalidCandidateId(_))
        ));
        assert!(matches!(
            run_single_asset_next_open_signal_batch(input(vec![candidate.clone(), candidate])),
            Err(SignalTimelineError::DuplicateCandidateId(_))
        ));
    }
}
