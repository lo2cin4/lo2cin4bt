use super::returns::simple_return;
use super::{field, quantile, ComputedFieldError, ComputedFieldSpec};
use std::collections::BTreeMap;

pub(crate) fn compute(
    op: &str,
    spec: &ComputedFieldSpec,
    fields: &BTreeMap<String, Vec<f64>>,
    dates: &[String],
    rows: usize,
    cols: usize,
) -> Result<Vec<f64>, ComputedFieldError> {
    let source_name = spec
        .source
        .as_deref()
        .ok_or_else(|| ComputedFieldError::InvalidParameter(format!("{op} requires source")))?;
    let source = field(fields, source_name)?;
    match op {
        "indicator.sma" => Ok(rolling_sma(source, rows, cols, spec.required_period()?)),
        "indicator.ema" => Ok(ema(source, rows, cols, spec.required_period()?)),
        "indicator.momentum" => Ok(momentum(source, rows, cols, spec.required_period()?)),
        "indicator.calendar_return" => calendar_return(
            source,
            dates,
            rows,
            cols,
            spec.sampling.as_deref().ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.calendar_return requires sampling".to_string(),
                )
            })?,
            spec.start_lag.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.calendar_return requires start_lag".to_string(),
                )
            })?,
            spec.end_lag.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.calendar_return requires end_lag".to_string(),
                )
            })?,
        ),
        "indicator.volatility" => Ok(rolling_volatility(
            source,
            rows,
            cols,
            spec.required_period()?,
            spec.annualize.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.volatility requires annualize".to_string(),
                )
            })?,
        )),
        "indicator.zscore" => Ok(rolling_zscore(source, rows, cols, spec.required_period()?)),
        "indicator.percentile" => Ok(rolling_percentile(
            source,
            rows,
            cols,
            spec.required_period()?,
            spec.percentile.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.percentile requires percentile".to_string(),
                )
            })?,
        )),
        "indicator.bollinger" => rolling_bollinger(
            source,
            rows,
            cols,
            spec.required_period()?,
            spec.stddev.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.bollinger requires stddev".to_string(),
                )
            })?,
            spec.band.as_deref().ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.bollinger requires band".to_string(),
                )
            })?,
        ),
        "indicator.rsi" => Ok(rolling_rsi(source, rows, cols, spec.required_period()?)),
        "indicator.macd" => macd(
            source,
            rows,
            cols,
            spec.fastperiod.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.macd requires fastperiod".to_string(),
                )
            })?,
            spec.slowperiod.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.macd requires slowperiod".to_string(),
                )
            })?,
            spec.signalperiod.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "indicator.macd requires signalperiod".to_string(),
                )
            })?,
            spec.output.as_deref().ok_or_else(|| {
                ComputedFieldError::InvalidParameter("indicator.macd requires output".to_string())
            })?,
        ),
        "indicator.atr" => average_true_range(
            field(
                fields,
                spec.high_source.as_deref().ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "indicator.atr requires high_source".to_string(),
                    )
                })?,
            )?,
            field(
                fields,
                spec.low_source.as_deref().ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "indicator.atr requires low_source".to_string(),
                    )
                })?,
            )?,
            field(
                fields,
                spec.close_source.as_deref().ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "indicator.atr requires close_source".to_string(),
                    )
                })?,
            )?,
            rows,
            cols,
            spec.required_period()?,
            spec.method.as_deref().ok_or_else(|| {
                ComputedFieldError::InvalidParameter("indicator.atr requires method".to_string())
            })?,
        ),
        "rolling.min" | "rolling.max" | "rolling.sum" | "rolling.median" => Ok(rolling_aggregate(
            source,
            rows,
            cols,
            spec.required_period()?,
            op,
        )),
        "rolling.correlation" => Ok(rolling_correlation(
            source,
            field(
                fields,
                spec.right_source.as_deref().ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "rolling.correlation requires right_source".to_string(),
                    )
                })?,
            )?,
            rows,
            cols,
            spec.required_period()?,
        )),
        _ => Err(ComputedFieldError::UnsupportedOperation(op.to_string())),
    }
}

fn rolling_sma(source: &[f64], rows: usize, cols: usize, period: usize) -> Vec<f64> {
    rolling_aggregate(source, rows, cols, period, "rolling.mean")
}

fn rolling_aggregate(
    source: &[f64],
    rows: usize,
    cols: usize,
    period: usize,
    op: &str,
) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    for row in period.saturating_sub(1)..rows {
        for col in 0..cols {
            let mut values = (row + 1 - period..=row)
                .map(|idx| source[idx * cols + col])
                .collect::<Vec<_>>();
            if values.iter().any(|value| !value.is_finite()) {
                continue;
            }
            output[row * cols + col] = match op {
                "rolling.min" => values.into_iter().fold(f64::INFINITY, f64::min),
                "rolling.max" => values.into_iter().fold(f64::NEG_INFINITY, f64::max),
                "rolling.sum" => values.into_iter().sum(),
                "rolling.median" => {
                    values.sort_by(f64::total_cmp);
                    quantile(&values, 0.5)
                }
                _ => values.into_iter().sum::<f64>() / period as f64,
            };
        }
    }
    output
}

fn momentum(source: &[f64], rows: usize, cols: usize, period: usize) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    for row in period..rows {
        for col in 0..cols {
            let previous = source[(row - period) * cols + col];
            let current = source[row * cols + col];
            if previous.is_finite() && previous != 0.0 && current.is_finite() {
                output[row * cols + col] = simple_return(current, previous);
            }
        }
    }
    output
}

fn calendar_return(
    source: &[f64],
    dates: &[String],
    rows: usize,
    cols: usize,
    sampling: &str,
    start_lag: usize,
    end_lag: usize,
) -> Result<Vec<f64>, ComputedFieldError> {
    if sampling.trim().to_lowercase() != "month_end" || dates.len() != rows || start_lag <= end_lag
    {
        return Err(ComputedFieldError::InvalidParameter(
            "indicator.calendar_return requires month_end and start_lag > end_lag >= 0".to_string(),
        ));
    }
    let mut output = vec![f64::NAN; rows * cols];
    let mut completed_month_ends = Vec::new();
    for row in 0..rows {
        let current_month = dates[row].get(..7).unwrap_or(dates[row].as_str());
        let is_month_end = row + 1 == rows
            || current_month != dates[row + 1].get(..7).unwrap_or(dates[row + 1].as_str());
        if is_month_end {
            completed_month_ends.push(row);
        }
        if completed_month_ends.len() <= start_lag {
            continue;
        }
        let anchor = completed_month_ends.len() - 1;
        let denominator_row = completed_month_ends[anchor - start_lag];
        let numerator_row = completed_month_ends[anchor - end_lag];
        for col in 0..cols {
            let denominator = source[denominator_row * cols + col];
            let numerator = source[numerator_row * cols + col];
            if denominator.is_finite() && denominator != 0.0 && numerator.is_finite() {
                output[row * cols + col] = simple_return(numerator, denominator);
            }
        }
    }
    Ok(output)
}

fn ema(source: &[f64], rows: usize, cols: usize, period: usize) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    let alpha = 2.0 / (period as f64 + 1.0);
    for col in 0..cols {
        let mut previous = f64::NAN;
        let mut count = 0usize;
        for row in 0..rows {
            let value = source[row * cols + col];
            if !value.is_finite() {
                continue;
            }
            count += 1;
            previous = if previous.is_finite() {
                alpha * value + (1.0 - alpha) * previous
            } else {
                value
            };
            if count >= period {
                output[row * cols + col] = previous;
            }
        }
    }
    output
}

fn rolling_stats(source: &[f64], rows: usize, cols: usize, period: usize) -> (Vec<f64>, Vec<f64>) {
    let means = rolling_sma(source, rows, cols, period);
    let mut stds = vec![f64::NAN; rows * cols];
    if period <= 1 {
        return (means, stds);
    }
    for row in period - 1..rows {
        for col in 0..cols {
            let mean = means[row * cols + col];
            if !mean.is_finite() {
                continue;
            }
            let values = (row + 1 - period..=row).map(|idx| source[idx * cols + col]);
            stds[row * cols + col] = (values.map(|value| (value - mean).powi(2)).sum::<f64>()
                / (period - 1) as f64)
                .sqrt();
        }
    }
    (means, stds)
}

fn rolling_zscore(source: &[f64], rows: usize, cols: usize, period: usize) -> Vec<f64> {
    let (means, stds) = rolling_stats(source, rows, cols, period);
    source
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            if stds[idx].is_finite() && stds[idx] != 0.0 {
                (value - means[idx]) / stds[idx]
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn rolling_percentile(
    source: &[f64],
    rows: usize,
    cols: usize,
    period: usize,
    percentile: f64,
) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    let q = percentile.clamp(0.0, 100.0) / 100.0;
    for row in period.saturating_sub(1)..rows {
        for col in 0..cols {
            let mut values = (row + 1 - period..=row)
                .map(|idx| source[idx * cols + col])
                .collect::<Vec<_>>();
            if values.iter().any(|value| !value.is_finite()) {
                continue;
            }
            values.sort_by(f64::total_cmp);
            output[row * cols + col] = quantile(&values, q);
        }
    }
    output
}

fn rolling_bollinger(
    source: &[f64],
    rows: usize,
    cols: usize,
    period: usize,
    width: f64,
    band: &str,
) -> Result<Vec<f64>, ComputedFieldError> {
    let (means, stds) = rolling_stats(source, rows, cols, period);
    means
        .iter()
        .zip(stds.iter())
        .enumerate()
        .map(|(idx, (mean, stddev))| {
            let deviation = stddev * width;
            match band.trim().to_lowercase().as_str() {
                "middle" => Ok(*mean),
                "upper" | "high" => Ok(mean + deviation),
                "lower" | "low" => Ok(mean - deviation),
                "width" | "bandwidth" => Ok(if *mean != 0.0 {
                    2.0 * deviation / mean
                } else {
                    f64::NAN
                }),
                "percent_b" | "pct_b" => Ok(if deviation != 0.0 {
                    (source[idx] - (mean - deviation)) / (2.0 * deviation)
                } else {
                    f64::NAN
                }),
                value => Err(ComputedFieldError::InvalidParameter(format!(
                    "indicator.bollinger unsupported band={value}"
                ))),
            }
        })
        .collect()
}

fn rolling_volatility(
    source: &[f64],
    rows: usize,
    cols: usize,
    period: usize,
    annualize: bool,
) -> Vec<f64> {
    let mut returns = vec![f64::NAN; rows * cols];
    for row in 1..rows {
        for col in 0..cols {
            let previous = source[(row - 1) * cols + col];
            let current = source[row * cols + col];
            if previous.is_finite() && previous != 0.0 && current.is_finite() {
                returns[row * cols + col] = simple_return(current, previous);
            }
        }
    }
    let (_, mut stds) = rolling_stats(&returns, rows, cols, period);
    if annualize {
        for value in &mut stds {
            *value *= 252.0_f64.sqrt();
        }
    }
    stds
}

fn rolling_rsi(source: &[f64], rows: usize, cols: usize, period: usize) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    for row in period..rows {
        for col in 0..cols {
            let mut gain = 0.0;
            let mut loss = 0.0;
            let mut valid = true;
            for idx in row + 1 - period..=row {
                let current = source[idx * cols + col];
                let previous = source[(idx - 1) * cols + col];
                if !current.is_finite() || !previous.is_finite() {
                    valid = false;
                    break;
                }
                let delta = current - previous;
                gain += delta.max(0.0);
                loss += (-delta).max(0.0);
            }
            if valid {
                let average_gain = gain / period as f64;
                let average_loss = loss / period as f64;
                output[row * cols + col] = if average_loss == 0.0 {
                    100.0
                } else {
                    100.0 - 100.0 / (1.0 + average_gain / average_loss)
                };
            }
        }
    }
    output
}

fn macd(
    source: &[f64],
    rows: usize,
    cols: usize,
    fast: usize,
    slow: usize,
    signal_period: usize,
    output: &str,
) -> Result<Vec<f64>, ComputedFieldError> {
    if fast == 0 || slow == 0 || signal_period == 0 {
        return Err(ComputedFieldError::InvalidParameter(
            "indicator.macd periods must be positive".to_string(),
        ));
    }
    let fast_values = ema(source, rows, cols, fast);
    let slow_values = ema(source, rows, cols, slow);
    let line = fast_values
        .iter()
        .zip(slow_values)
        .map(|(left, right)| {
            if left.is_finite() && right.is_finite() {
                left - right
            } else {
                f64::NAN
            }
        })
        .collect::<Vec<_>>();
    match output.trim().to_lowercase().as_str() {
        "line" => Ok(line),
        "signal" => Ok(ema(&line, rows, cols, signal_period)),
        "histogram" => {
            let signal = ema(&line, rows, cols, signal_period);
            Ok(line
                .iter()
                .zip(signal)
                .map(|(value, signal)| {
                    if value.is_finite() && signal.is_finite() {
                        value - signal
                    } else {
                        f64::NAN
                    }
                })
                .collect())
        }
        value => Err(ComputedFieldError::InvalidParameter(format!(
            "indicator.macd unsupported output={value}"
        ))),
    }
}

fn average_true_range(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    rows: usize,
    cols: usize,
    period: usize,
    method: &str,
) -> Result<Vec<f64>, ComputedFieldError> {
    let mut true_range = vec![f64::NAN; rows * cols];
    for row in 0..rows {
        for col in 0..cols {
            let idx = row * cols + col;
            if !high[idx].is_finite() || !low[idx].is_finite() {
                continue;
            }
            let mut value = high[idx] - low[idx];
            if row > 0 {
                let previous = close[(row - 1) * cols + col];
                if previous.is_finite() {
                    value = value
                        .max((high[idx] - previous).abs())
                        .max((low[idx] - previous).abs());
                }
            }
            true_range[idx] = value;
        }
    }
    match method.trim().to_lowercase().as_str() {
        "simple" | "sma" | "rolling" => Ok(rolling_sma(&true_range, rows, cols, period)),
        "ema" => Ok(ema(&true_range, rows, cols, period)),
        "wilder" | "rma" => Ok(wilder_average(&true_range, rows, cols, period)),
        value => Err(ComputedFieldError::InvalidParameter(format!(
            "indicator.atr unsupported method={value}"
        ))),
    }
}

fn wilder_average(source: &[f64], rows: usize, cols: usize, period: usize) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    for col in 0..cols {
        let (mut sum, mut count, mut previous) = (0.0, 0usize, f64::NAN);
        for row in 0..rows {
            let idx = row * cols + col;
            let value = source[idx];
            if !value.is_finite() {
                continue;
            }
            if count < period {
                sum += value;
                count += 1;
                if count == period {
                    previous = sum / period as f64;
                    output[idx] = previous;
                }
            } else {
                previous = (previous * (period - 1) as f64 + value) / period as f64;
                output[idx] = previous;
            }
        }
    }
    output
}

fn rolling_correlation(
    left: &[f64],
    right: &[f64],
    rows: usize,
    cols: usize,
    period: usize,
) -> Vec<f64> {
    let mut output = vec![f64::NAN; rows * cols];
    for row in period.saturating_sub(1)..rows {
        for col in 0..cols {
            let pairs = (row + 1 - period..=row)
                .map(|idx| (left[idx * cols + col], right[idx * cols + col]))
                .collect::<Vec<_>>();
            if pairs.iter().any(|(x, y)| !x.is_finite() || !y.is_finite()) {
                continue;
            }
            let mean_x = pairs.iter().map(|(x, _)| x).sum::<f64>() / period as f64;
            let mean_y = pairs.iter().map(|(_, y)| y).sum::<f64>() / period as f64;
            let covariance = pairs
                .iter()
                .map(|(x, y)| (x - mean_x) * (y - mean_y))
                .sum::<f64>();
            let variance_x = pairs.iter().map(|(x, _)| (x - mean_x).powi(2)).sum::<f64>();
            let variance_y = pairs.iter().map(|(_, y)| (y - mean_y).powi(2)).sum::<f64>();
            let denominator = (variance_x * variance_y).sqrt();
            if denominator > 0.0 {
                output[row * cols + col] = covariance / denominator;
            }
        }
    }
    output
}
