use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsBatchInput {
    pub time_unit: usize,
    pub risk_free_rate: f64,
    pub backtest_ids: Vec<String>,
    pub equity: Vec<f64>,
    pub bah_equity: Vec<f64>,
    #[serde(default)]
    pub close: Vec<Option<f64>>,
    #[serde(default)]
    pub trade_actions: Vec<Option<f64>>,
    #[serde(default)]
    pub trade_returns: Vec<Option<f64>>,
    #[serde(default)]
    pub position_size: Vec<Option<f64>>,
    pub group_start: Vec<usize>,
    pub group_end: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsBatchSummary {
    pub row_count: usize,
    pub metrics: Vec<EquityMetricRow>,
    pub enriched_rows: Vec<MetricEnrichedRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricEnrichedRow {
    pub row_index: usize,
    #[serde(rename = "Drawdown")]
    pub drawdown: f64,
    #[serde(rename = "BAH_Equity")]
    pub bah_equity: Option<f64>,
    #[serde(rename = "BAH_Return")]
    pub bah_return: Option<f64>,
    #[serde(rename = "BAH_Drawdown")]
    pub bah_drawdown: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EquityMetricRow {
    #[serde(rename = "Backtest_id")]
    pub backtest_id: String,
    #[serde(rename = "Total_return")]
    pub total_return: f64,
    #[serde(rename = "Annualized_return (CAGR)")]
    pub annualized_return: f64,
    #[serde(rename = "Std")]
    pub std: f64,
    #[serde(rename = "Annualized_std")]
    pub annualized_std: f64,
    #[serde(rename = "Downside_risk")]
    pub downside_risk: f64,
    #[serde(rename = "Annualized_downside_risk")]
    pub annualized_downside_risk: f64,
    #[serde(rename = "Max_drawdown")]
    pub max_drawdown: f64,
    #[serde(rename = "Average_drawdown")]
    pub average_drawdown: f64,
    #[serde(rename = "Recovery_factor")]
    pub recovery_factor: f64,
    #[serde(rename = "Sharpe")]
    pub sharpe: f64,
    #[serde(rename = "Sortino")]
    pub sortino: f64,
    #[serde(rename = "Calmar")]
    pub calmar: f64,
    #[serde(rename = "Information_ratio")]
    pub information_ratio: f64,
    #[serde(rename = "Alpha")]
    pub alpha: f64,
    #[serde(rename = "Beta")]
    pub beta: f64,
    #[serde(rename = "BAH_Total_return")]
    pub bah_total_return: f64,
    #[serde(rename = "BAH_Annualized_return (CAGR)")]
    pub bah_annualized_return: f64,
    #[serde(rename = "BAH_Std")]
    pub bah_std: f64,
    #[serde(rename = "BAH_Annualized_std")]
    pub bah_annualized_std: f64,
    #[serde(rename = "BAH_Downside_risk")]
    pub bah_downside_risk: f64,
    #[serde(rename = "BAH_Annualized_downside_risk")]
    pub bah_annualized_downside_risk: f64,
    #[serde(rename = "BAH_Max_drawdown")]
    pub bah_max_drawdown: f64,
    #[serde(rename = "BAH_Average_drawdown")]
    pub bah_average_drawdown: f64,
    #[serde(rename = "BAH_Recovery_factor")]
    pub bah_recovery_factor: f64,
    #[serde(rename = "BAH_Sharpe")]
    pub bah_sharpe: f64,
    #[serde(rename = "BAH_Sortino")]
    pub bah_sortino: f64,
    #[serde(rename = "BAH_Calmar")]
    pub bah_calmar: f64,
    #[serde(rename = "Trade_count")]
    pub trade_count: f64,
    #[serde(rename = "Win_rate")]
    pub win_rate: f64,
    #[serde(rename = "Profit_factor")]
    pub profit_factor: f64,
    #[serde(rename = "Avg_trade_return")]
    pub avg_trade_return: f64,
    #[serde(rename = "Max_consecutive_losses")]
    pub max_consecutive_losses: f64,
    #[serde(rename = "Exposure_time")]
    pub exposure_time: f64,
    #[serde(rename = "Max_holding_period_ratio")]
    pub max_holding_ratio: f64,
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("metrics input requires a positive time_unit")]
    InvalidTimeUnit,
    #[error("equity and bah_equity must have identical lengths")]
    InvalidSeriesLength,
    #[error("group arrays must have identical lengths")]
    InvalidGroupLength,
    #[error("metrics input requires at least one group")]
    EmptyGroups,
    #[error("backtest_ids must match the number of groups")]
    InvalidBacktestIdLength,
    #[error("group index is out of bounds")]
    GroupOutOfBounds,
    #[error("risk_free_rate must be finite")]
    InvalidRiskFreeRate,
    #[error("invalid {series} value at index {index}")]
    InvalidEquityValue { series: &'static str, index: usize },
}

pub fn run_metrics_batch(input: MetricsBatchInput) -> Result<MetricsBatchSummary, MetricsError> {
    validate_metrics_input(&input)?;
    let mut metrics = Vec::with_capacity(input.group_start.len());
    let sqrt_time_unit = (input.time_unit as f64).sqrt();
    let rf_per_period = input.risk_free_rate / input.time_unit as f64;

    for group_idx in 0..input.group_start.len() {
        let start = input.group_start[group_idx];
        let end = input.group_end[group_idx];
        let id = input.backtest_ids[group_idx].clone();
        let equity = &input.equity[start..end];
        let bah_equity = &input.bah_equity[start..end];
        let trade_stats = trade_stats_for_group(&input, start, end);
        let years = ((end - start) as f64 / input.time_unit as f64).max(1.0);
        let returns = pct_change(equity);
        let bah_returns = pct_change(bah_equity);
        let drawdown = build_drawdown(equity);
        let bah_drawdown = build_drawdown(bah_equity);

        let total_return = calc_total_return(equity);
        let bah_total_return = calc_total_return(bah_equity);
        let annualized_return = annualized_total_return(total_return, years);
        let bah_annualized_return = annualized_total_return(bah_total_return, years);
        let std = sample_std(&returns);
        let bah_std = sample_std(&bah_returns);
        let annualized_std = calc_annualized_std(std, sqrt_time_unit);
        let bah_annualized_std = calc_annualized_std(bah_std, sqrt_time_unit);
        let downside_risk = calc_downside_risk(&returns, 0.0);
        let bah_downside_risk = calc_downside_risk(&bah_returns, 0.0);
        let annualized_downside_risk = calc_annualized_std(downside_risk, sqrt_time_unit);
        let bah_annualized_downside_risk = calc_annualized_std(bah_downside_risk, sqrt_time_unit);
        let max_drawdown = nan_min(&drawdown);
        let bah_max_drawdown = nan_min(&bah_drawdown);
        let average_drawdown = calc_average_drawdown(&drawdown);
        let bah_average_drawdown = calc_average_drawdown(&bah_drawdown);
        let recovery_factor = safe_div(total_return, max_drawdown.abs(), f64::NAN);
        let bah_recovery_factor = safe_div(bah_total_return, bah_max_drawdown.abs(), f64::NAN);
        let sharpe = calc_sharpe(&returns, rf_per_period, sqrt_time_unit);
        let bah_sharpe = calc_sharpe(&bah_returns, rf_per_period, sqrt_time_unit);
        let sortino = calc_sortino(&returns, rf_per_period, sqrt_time_unit);
        let bah_sortino = calc_sortino(&bah_returns, rf_per_period, sqrt_time_unit);
        let calmar = safe_div(
            annualized_return - input.risk_free_rate,
            max_drawdown.abs(),
            f64::NAN,
        );
        let bah_calmar = safe_div(
            bah_annualized_return - input.risk_free_rate,
            bah_max_drawdown.abs(),
            f64::NAN,
        );
        let information_ratio = information_ratio(&returns, &bah_returns);
        let beta = beta(&returns, &bah_returns);
        let alpha = alpha(
            &returns,
            &bah_returns,
            input.risk_free_rate,
            input.time_unit,
            beta,
        );

        metrics.push(EquityMetricRow {
            backtest_id: id,
            total_return,
            annualized_return,
            std,
            annualized_std,
            downside_risk,
            annualized_downside_risk,
            max_drawdown,
            average_drawdown,
            recovery_factor,
            sharpe,
            sortino,
            calmar,
            information_ratio,
            alpha,
            beta,
            bah_total_return,
            bah_annualized_return,
            bah_std,
            bah_annualized_std,
            bah_downside_risk,
            bah_annualized_downside_risk,
            bah_max_drawdown,
            bah_average_drawdown,
            bah_recovery_factor,
            bah_sharpe,
            bah_sortino,
            bah_calmar,
            trade_count: trade_stats.trade_count,
            win_rate: trade_stats.win_rate,
            profit_factor: trade_stats.profit_factor,
            avg_trade_return: trade_stats.avg_trade_return,
            max_consecutive_losses: trade_stats.max_consecutive_losses,
            exposure_time: trade_stats.exposure_time,
            max_holding_ratio: trade_stats.max_holding_ratio,
        });
    }

    let enriched_rows = build_enriched_rows(&input);
    Ok(MetricsBatchSummary {
        row_count: metrics.len(),
        metrics,
        enriched_rows,
    })
}

fn validate_metrics_input(input: &MetricsBatchInput) -> Result<(), MetricsError> {
    if input.time_unit == 0 {
        return Err(MetricsError::InvalidTimeUnit);
    }
    if !input.risk_free_rate.is_finite() {
        return Err(MetricsError::InvalidRiskFreeRate);
    }
    if input.equity.len() != input.bah_equity.len() {
        return Err(MetricsError::InvalidSeriesLength);
    }
    if (!input.trade_actions.is_empty() && input.trade_actions.len() != input.equity.len())
        || (!input.trade_returns.is_empty() && input.trade_returns.len() != input.equity.len())
        || (!input.position_size.is_empty() && input.position_size.len() != input.equity.len())
        || (!input.close.is_empty() && input.close.len() != input.equity.len())
    {
        return Err(MetricsError::InvalidSeriesLength);
    }
    if input.group_start.len() != input.group_end.len() {
        return Err(MetricsError::InvalidGroupLength);
    }
    if input.group_start.is_empty() {
        return Err(MetricsError::EmptyGroups);
    }
    if input.backtest_ids.len() != input.group_start.len() {
        return Err(MetricsError::InvalidBacktestIdLength);
    }
    validate_equity_series(&input.equity, "equity")?;
    validate_equity_series(&input.bah_equity, "bah_equity")?;
    for (&start, &end) in input.group_start.iter().zip(input.group_end.iter()) {
        if start >= end || end > input.equity.len() {
            return Err(MetricsError::GroupOutOfBounds);
        }
    }
    Ok(())
}

fn validate_equity_series(values: &[f64], series: &'static str) -> Result<(), MetricsError> {
    for (index, value) in values.iter().enumerate() {
        if !value.is_finite() || *value <= 0.0 {
            return Err(MetricsError::InvalidEquityValue { series, index });
        }
    }
    Ok(())
}

fn build_enriched_rows(input: &MetricsBatchInput) -> Vec<MetricEnrichedRow> {
    let mut rows = Vec::with_capacity(input.equity.len());
    for group_idx in 0..input.group_start.len() {
        let start = input.group_start[group_idx];
        let end = input.group_end[group_idx];
        if end <= start || end > input.equity.len() {
            continue;
        }
        let mut equity_peak = f64::NAN;
        let mut bah_peak = f64::NAN;
        let mut previous_bah_equity: Option<f64> = None;
        for row_index in start..end {
            let equity = input.equity[row_index];
            if equity.is_finite() {
                equity_peak = if equity_peak.is_finite() {
                    equity_peak.max(equity)
                } else {
                    equity
                };
            }
            let drawdown = if equity.is_finite() && equity_peak.is_finite() && equity_peak != 0.0 {
                (equity - equity_peak) / equity_peak
            } else {
                f64::NAN
            };
            let bah_equity = input.bah_equity.get(row_index).copied().and_then(|value| {
                if value.is_finite() {
                    Some(value)
                } else {
                    None
                }
            });
            if let Some(value) = bah_equity {
                bah_peak = if bah_peak.is_finite() {
                    bah_peak.max(value)
                } else {
                    value
                };
            }
            let bah_return = match (bah_equity, previous_bah_equity) {
                (Some(current), Some(previous)) if previous != 0.0 => {
                    Some(current / previous - 1.0)
                }
                (Some(_), None) => Some(0.0),
                _ => None,
            };
            let bah_drawdown = match bah_equity {
                Some(value) if bah_peak.is_finite() && bah_peak != 0.0 => {
                    Some((value - bah_peak) / bah_peak)
                }
                _ => None,
            };
            previous_bah_equity = bah_equity;
            rows.push(MetricEnrichedRow {
                row_index,
                drawdown,
                bah_equity,
                bah_return,
                bah_drawdown,
            });
        }
    }
    rows
}

struct TradeStats {
    trade_count: f64,
    win_rate: f64,
    profit_factor: f64,
    avg_trade_return: f64,
    max_consecutive_losses: f64,
    exposure_time: f64,
    max_holding_ratio: f64,
}

fn trade_stats_for_group(input: &MetricsBatchInput, start: usize, end: usize) -> TradeStats {
    if input.trade_actions.is_empty()
        || input.trade_returns.is_empty()
        || input.position_size.is_empty()
        || end <= start
    {
        return TradeStats {
            trade_count: f64::NAN,
            win_rate: f64::NAN,
            profit_factor: f64::NAN,
            avg_trade_return: f64::NAN,
            max_consecutive_losses: f64::NAN,
            exposure_time: f64::NAN,
            max_holding_ratio: f64::NAN,
        };
    }

    let actions = &input.trade_actions[start..end];
    let returns = &input.trade_returns[start..end];
    let positions = &input.position_size[start..end];
    let trade_count = actions
        .iter()
        .flatten()
        .filter(|value| value.is_finite() && **value == 1.0)
        .count() as f64;
    let closed_returns: Vec<f64> = actions
        .iter()
        .zip(returns.iter())
        .filter_map(|(action, value)| {
            if action.unwrap_or(f64::NAN) == 4.0 {
                value.and_then(|item| if item.is_nan() { None } else { Some(item) })
            } else {
                None
            }
        })
        .collect();
    let win_rate = if closed_returns.is_empty() {
        f64::NAN
    } else {
        closed_returns.iter().filter(|value| **value > 0.0).count() as f64
            / closed_returns.len() as f64
    };
    let profits: f64 = closed_returns
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .sum();
    let losses: f64 = closed_returns
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .sum();
    let profit_factor = if losses == 0.0 {
        f64::NAN
    } else {
        profits / losses.abs()
    };
    let avg_trade_return = if closed_returns.is_empty() {
        f64::NAN
    } else {
        closed_returns.iter().sum::<f64>() / closed_returns.len() as f64
    };
    let max_consecutive_losses = max_consecutive_negative(&closed_returns) as f64;
    let exposure_time = if positions.is_empty() {
        f64::NAN
    } else {
        positions
            .iter()
            .flatten()
            .filter(|value| !value.is_nan() && **value != 0.0)
            .count() as f64
            / positions.len() as f64
            * 100.0
    };
    let max_holding_ratio = max_nonzero_run_ratio(positions);

    TradeStats {
        trade_count,
        win_rate,
        profit_factor,
        avg_trade_return,
        max_consecutive_losses,
        exposure_time,
        max_holding_ratio,
    }
}

fn max_consecutive_negative(values: &[f64]) -> usize {
    let mut max_count = 0usize;
    let mut count = 0usize;
    for value in values.iter().copied() {
        if value.is_nan() {
            continue;
        }
        if value < 0.0 {
            count += 1;
            max_count = max_count.max(count);
        } else {
            count = 0;
        }
    }
    max_count
}

fn max_nonzero_run_ratio(values: &[Option<f64>]) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut max_run = 0usize;
    let mut run = 0usize;
    for value in values.iter().copied() {
        let Some(value) = value else {
            run = 0;
            continue;
        };
        if value.is_nan() {
            run = 0;
            continue;
        }
        if value != 0.0 {
            run += 1;
            max_run = max_run.max(run);
        } else {
            run = 0;
        }
    }
    max_run as f64 / values.len() as f64
}

fn pct_change(values: &[f64]) -> Vec<f64> {
    let mut out = vec![0.0; values.len()];
    for idx in 1..values.len() {
        out[idx] = values[idx] / values[idx - 1] - 1.0;
    }
    out
}

fn build_drawdown(values: &[f64]) -> Vec<f64> {
    let mut out = vec![f64::NAN; values.len()];
    let mut peak = f64::NAN;
    for (idx, value) in values.iter().copied().enumerate() {
        if !value.is_finite() {
            continue;
        }
        if !peak.is_finite() || value > peak {
            peak = value;
        }
        if peak != 0.0 {
            out[idx] = (value - peak) / peak;
        }
    }
    out
}

fn calc_total_return(equity: &[f64]) -> f64 {
    equity[equity.len() - 1] / equity[0] - 1.0
}

fn annualized_total_return(total_return: f64, years: f64) -> f64 {
    (1.0 + total_return).powf(1.0 / years) - 1.0
}

fn sample_std(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return f64::NAN;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
}

fn calc_annualized_std(std: f64, sqrt_time_unit: f64) -> f64 {
    if std.is_nan() {
        f64::NAN
    } else {
        std * sqrt_time_unit
    }
}

fn calc_downside_risk(values: &[f64], target: f64) -> f64 {
    let downside: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| *value < target)
        .collect();
    if downside.is_empty() {
        return 0.0;
    }
    let mean = downside
        .iter()
        .map(|value| {
            let diff = value - target;
            diff * diff
        })
        .sum::<f64>()
        / downside.len() as f64;
    mean.sqrt()
}

fn calc_average_drawdown(drawdown: &[f64]) -> f64 {
    let mut in_drawdown = false;
    let mut current_min = 0.0;
    let mut total = 0.0;
    let mut count = 0_usize;
    for value in drawdown.iter().copied() {
        if value.is_nan() {
            continue;
        }
        if value < 0.0 {
            if !in_drawdown {
                in_drawdown = true;
                current_min = value;
            } else if value < current_min {
                current_min = value;
            }
        } else if in_drawdown {
            total += current_min;
            count += 1;
            in_drawdown = false;
            current_min = 0.0;
        }
    }
    if in_drawdown {
        total += current_min;
        count += 1;
    }
    if count == 0 {
        0.0
    } else {
        total / count as f64
    }
}

fn nan_min(values: &[f64]) -> f64 {
    let mut found = false;
    let mut min_value = f64::NAN;
    for value in values.iter().copied() {
        if value.is_nan() {
            continue;
        }
        if !found || value < min_value {
            min_value = value;
            found = true;
        }
    }
    min_value
}

fn calc_sharpe(returns: &[f64], rf_per_period: f64, sqrt_time_unit: f64) -> f64 {
    if returns.len() < 2 {
        return f64::NAN;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let std = sample_std(returns);
    if std == 0.0 || std.is_nan() {
        f64::NAN
    } else {
        ((mean - rf_per_period) / std) * sqrt_time_unit
    }
}

fn calc_sortino(returns: &[f64], rf_per_period: f64, sqrt_time_unit: f64) -> f64 {
    let mean = if returns.is_empty() {
        f64::NAN
    } else {
        returns.iter().sum::<f64>() / returns.len() as f64
    };
    let downside = calc_downside_risk(returns, 0.0);
    if downside == 0.0 || downside.is_nan() {
        f64::NAN
    } else {
        ((mean - rf_per_period) / downside) * sqrt_time_unit
    }
}

fn information_ratio(strategy_returns: &[f64], benchmark_returns: &[f64]) -> f64 {
    if strategy_returns.is_empty() || benchmark_returns.is_empty() {
        return f64::NAN;
    }
    let len = strategy_returns.len().min(benchmark_returns.len());
    let diff: Vec<f64> = (0..len)
        .map(|idx| strategy_returns[idx] - benchmark_returns[idx])
        .collect();
    let tracking_error = sample_std(&diff);
    if tracking_error == 0.0 || tracking_error.is_nan() {
        f64::NAN
    } else {
        diff.iter().sum::<f64>() / diff.len() as f64 / tracking_error
    }
}

fn beta(strategy_returns: &[f64], benchmark_returns: &[f64]) -> f64 {
    let len = strategy_returns.len().min(benchmark_returns.len());
    if len < 2 {
        return f64::NAN;
    }
    let strategy_mean = strategy_returns[..len].iter().sum::<f64>() / len as f64;
    let benchmark_mean = benchmark_returns[..len].iter().sum::<f64>() / len as f64;
    let mut cov = 0.0;
    let mut var = 0.0;
    for idx in 0..len {
        let sx = strategy_returns[idx] - strategy_mean;
        let bx = benchmark_returns[idx] - benchmark_mean;
        cov += sx * bx;
        var += bx * bx;
    }
    cov /= (len - 1) as f64;
    var /= (len - 1) as f64;
    if var == 0.0 || var.is_nan() {
        f64::NAN
    } else {
        cov / var
    }
}

fn alpha(
    strategy_returns: &[f64],
    benchmark_returns: &[f64],
    risk_free_rate: f64,
    time_unit: usize,
    beta_value: f64,
) -> f64 {
    if strategy_returns.is_empty() || benchmark_returns.is_empty() || beta_value.is_nan() {
        return f64::NAN;
    }
    let rf = risk_free_rate / time_unit as f64;
    let strategy_mean = strategy_returns.iter().sum::<f64>() / strategy_returns.len() as f64;
    let benchmark_mean = benchmark_returns.iter().sum::<f64>() / benchmark_returns.len() as f64;
    strategy_mean - (rf + beta_value * (benchmark_mean - rf))
}

fn safe_div(numerator: f64, denominator: f64, fallback: f64) -> f64 {
    if denominator == 0.0 || numerator.is_nan() || denominator.is_nan() {
        fallback
    } else {
        numerator / denominator
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    #[test]
    fn invalid_equity_values_fail_closed() {
        let error = run_metrics_batch(MetricsBatchInput {
            time_unit: 252,
            risk_free_rate: 0.02,
            backtest_ids: vec!["invalid".to_string()],
            equity: vec![100.0, f64::NAN],
            bah_equity: vec![100.0, 101.0],
            close: Vec::new(),
            trade_actions: Vec::new(),
            trade_returns: Vec::new(),
            position_size: Vec::new(),
            group_start: vec![0],
            group_end: vec![2],
        })
        .expect_err("non-finite equity must fail");

        assert!(matches!(
            error,
            MetricsError::InvalidEquityValue {
                series: "equity",
                index: 1
            }
        ));
    }

    #[test]
    fn non_positive_equity_values_fail_closed() {
        let error = run_metrics_batch(MetricsBatchInput {
            time_unit: 252,
            risk_free_rate: 0.02,
            backtest_ids: vec!["invalid".to_string()],
            equity: vec![100.0, 0.0],
            bah_equity: vec![100.0, 101.0],
            close: Vec::new(),
            trade_actions: Vec::new(),
            trade_returns: Vec::new(),
            position_size: Vec::new(),
            group_start: vec![0],
            group_end: vec![2],
        })
        .expect_err("non-positive equity must fail");

        assert!(matches!(
            error,
            MetricsError::InvalidEquityValue {
                series: "equity",
                index: 1
            }
        ));
    }

    #[test]
    fn metrics_batch_computes_equity_and_benchmark_rows() {
        let summary = run_metrics_batch(MetricsBatchInput {
            time_unit: 252,
            risk_free_rate: 0.02,
            backtest_ids: vec!["a".to_string(), "b".to_string()],
            equity: vec![100.0, 101.0, 103.0, 100.0, 99.0, 102.0],
            bah_equity: vec![100.0, 100.5, 101.0, 100.0, 101.0, 102.0],
            close: Vec::new(),
            trade_actions: Vec::new(),
            trade_returns: Vec::new(),
            position_size: Vec::new(),
            group_start: vec![0, 3],
            group_end: vec![3, 6],
        })
        .expect("metrics batch should run");

        assert_eq!(summary.row_count, 2);
        assert_eq!(summary.metrics[0].backtest_id, "a");
        assert_relative_eq!(summary.metrics[0].total_return, 0.03, epsilon = 1e-12);
        assert_relative_eq!(summary.metrics[1].total_return, 0.02, epsilon = 1e-12);
        assert!(summary.metrics[1].max_drawdown < 0.0);
        assert_eq!(summary.enriched_rows.len(), 6);
        assert_relative_eq!(summary.enriched_rows[1].drawdown, 0.0, epsilon = 1e-12);
    }
}
