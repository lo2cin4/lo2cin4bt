use std::fs::File;
use std::path::Path;

use polars::io::parquet::read::ParquetReader;
use polars::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use thiserror::Error;

use crate::metrics::{run_metrics_batch, MetricsBatchInput, MetricsBatchSummary};

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsParquetInput {
    pub parquet_path: String,
    #[serde(default)]
    pub benchmark_parquet_path: Option<String>,
    #[serde(default)]
    pub benchmark_symbol: Option<String>,
    pub time_unit: usize,
    pub risk_free_rate: f64,
}

#[derive(Debug, Error)]
pub enum MetricsParquetError {
    #[error("failed to read parquet: {0}")]
    Read(String),
    #[error("parquet metrics input requires Equity_value")]
    MissingEquity,
    #[error("parquet metrics input requires Backtest_id")]
    MissingBacktestId,
    #[error("parquet metrics input requires Session_label")]
    MissingSessionLabel,
    #[error("parquet column is incomplete or non-finite: {0}")]
    InvalidColumn(String),
    #[error("parquet contains no rows")]
    Empty,
    #[error("failed to compute metrics from parquet: {0}")]
    Metrics(String),
    #[error("benchmark parquet is invalid: {0}")]
    InvalidBenchmark(String),
}

pub fn run_metrics_parquet(
    input: MetricsParquetInput,
) -> Result<MetricsBatchSummary, MetricsParquetError> {
    let df = read_metrics_parquet(&input.parquet_path)?;
    let benchmark = match input.benchmark_parquet_path.as_deref() {
        Some(path) => Some(read_metrics_parquet(path)?),
        None => None,
    };
    let benchmark_symbol = input.benchmark_symbol.as_deref().unwrap_or("");
    let batch = metrics_batch_from_dataframe(
        &df,
        benchmark.as_ref(),
        benchmark_symbol,
        input.time_unit,
        input.risk_free_rate,
    )?;
    run_metrics_batch(batch).map_err(|exc| MetricsParquetError::Metrics(exc.to_string()))
}

fn read_metrics_parquet(path: &str) -> Result<DataFrame, MetricsParquetError> {
    let file =
        File::open(Path::new(path)).map_err(|exc| MetricsParquetError::Read(exc.to_string()))?;
    ParquetReader::new(file)
        .finish()
        .map_err(|exc| MetricsParquetError::Read(exc.to_string()))
}

fn metrics_batch_from_dataframe(
    df: &DataFrame,
    benchmark_df: Option<&DataFrame>,
    benchmark_symbol: &str,
    time_unit: usize,
    risk_free_rate: f64,
) -> Result<MetricsBatchInput, MetricsParquetError> {
    let row_count = df.height();
    if row_count == 0 {
        return Err(MetricsParquetError::Empty);
    }

    let equity = required_complete_numeric_column(df, "Equity_value")?
        .ok_or(MetricsParquetError::MissingEquity)?;
    if equity.iter().any(|value| *value <= 0.0) {
        return Err(MetricsParquetError::InvalidColumn(
            "Equity_value must be positive".to_string(),
        ));
    }
    let backtest_id_values =
        string_column(df, "Backtest_id")?.ok_or(MetricsParquetError::MissingBacktestId)?;
    let session_labels =
        string_column(df, "Session_label")?.ok_or(MetricsParquetError::MissingSessionLabel)?;
    let (backtest_ids, group_start, group_end) = group_boundaries(&backtest_id_values, row_count);

    let close = required_complete_numeric_column(df, "Close")?;
    if close
        .as_ref()
        .is_some_and(|values| values.iter().any(|value| *value <= 0.0))
    {
        return Err(MetricsParquetError::InvalidColumn(
            "Close must be positive".to_string(),
        ));
    }
    let embedded_bah = required_complete_numeric_column(df, "BAH_Equity")?;
    if embedded_bah
        .as_ref()
        .is_some_and(|values| values.iter().any(|value| *value <= 0.0))
    {
        return Err(MetricsParquetError::InvalidColumn(
            "BAH_Equity must be positive".to_string(),
        ));
    }
    let bah_equity = if let Some(values) = embedded_bah {
        values
    } else if let Some(benchmark) = benchmark_df {
        if benchmark_symbol.trim().is_empty() {
            return Err(MetricsParquetError::InvalidBenchmark(
                "benchmark_symbol is required when benchmark_parquet_path is set".to_string(),
            ));
        }
        build_external_bah_equity(
            df,
            benchmark,
            benchmark_symbol,
            &equity,
            &group_start,
            &group_end,
        )?
    } else if let Some(close_values) = close.as_deref() {
        build_bah_equity(&equity, close_values, &group_start, &group_end)
    } else {
        vec![f64::NAN; equity.len()]
    };

    Ok(MetricsBatchInput {
        time_unit,
        risk_free_rate,
        backtest_ids,
        equity,
        bah_equity,
        session_labels,
        close: close
            .map(|values| values.into_iter().map(Some).collect())
            .unwrap_or_default(),
        trade_actions: numeric_column(df, "Trade_action")?.unwrap_or_default(),
        trade_returns: numeric_column(df, "Trade_return")?.unwrap_or_default(),
        position_size: numeric_column(df, "Position_size")?.unwrap_or_default(),
        group_start,
        group_end,
    })
}

fn build_external_bah_equity(
    source: &DataFrame,
    benchmark: &DataFrame,
    benchmark_symbol: &str,
    equity: &[f64],
    starts: &[usize],
    ends: &[usize],
) -> Result<Vec<f64>, MetricsParquetError> {
    let source_times = time_keys(source)?;
    let benchmark_times = time_keys(benchmark)?;
    let benchmark_values = required_complete_numeric_column(benchmark, benchmark_symbol)?
        .ok_or_else(|| {
            MetricsParquetError::InvalidBenchmark(format!(
                "symbol column {benchmark_symbol} is missing"
            ))
        })?;
    if benchmark_values.iter().any(|value| *value <= 0.0) {
        return Err(MetricsParquetError::InvalidBenchmark(format!(
            "symbol column {benchmark_symbol} must contain positive prices"
        )));
    }
    if benchmark_times.len() != benchmark_values.len() {
        return Err(MetricsParquetError::InvalidBenchmark(
            "Time and symbol columns have different lengths".to_string(),
        ));
    }
    let price_by_time: HashMap<String, f64> =
        benchmark_times.into_iter().zip(benchmark_values).collect();
    let mut out = vec![f64::NAN; equity.len()];
    for (&start, &end) in starts.iter().zip(ends.iter()) {
        let base_equity = equity.get(start).copied();
        let base_price = source_times
            .get(start)
            .and_then(|time| price_by_time.get(time))
            .copied();
        for (idx, output) in out.iter_mut().enumerate().take(end).skip(start) {
            let current_price = source_times
                .get(idx)
                .and_then(|time| price_by_time.get(time))
                .copied();
            *output = match (base_equity, base_price, current_price) {
                (Some(first_equity), Some(first_price), Some(price))
                    if first_price.is_finite() && first_price != 0.0 && price.is_finite() =>
                {
                    first_equity * price / first_price
                }
                _ => {
                    return Err(MetricsParquetError::InvalidBenchmark(
                        "benchmark prices do not cover every strategy timestamp".to_string(),
                    ))
                }
            };
        }
    }
    Ok(out)
}

fn time_keys(df: &DataFrame) -> Result<Vec<String>, MetricsParquetError> {
    let column = df
        .column("Time")
        .map_err(|_| MetricsParquetError::InvalidBenchmark("Time column is missing".to_string()))?;
    let casted = column
        .cast(&DataType::String)
        .map_err(|exc| MetricsParquetError::InvalidBenchmark(format!("Time: {exc}")))?;
    let strings = casted
        .str()
        .map_err(|exc| MetricsParquetError::InvalidBenchmark(format!("Time: {exc}")))?;
    strings
        .iter()
        .map(|value| {
            value
                .filter(|item| !item.trim().is_empty())
                .map(normalize_time_key)
                .ok_or_else(|| {
                    MetricsParquetError::InvalidBenchmark(
                        "Time column contains an empty value".to_string(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn normalize_time_key(value: &str) -> String {
    value.trim().chars().take(10).collect()
}

fn numeric_column(
    df: &DataFrame,
    name: &str,
) -> Result<Option<Vec<Option<f64>>>, MetricsParquetError> {
    let Ok(column) = df.column(name) else {
        return Ok(None);
    };
    let casted = column
        .cast(&DataType::Float64)
        .map_err(|exc| MetricsParquetError::Read(format!("{name}: {exc}")))?;
    let values = casted
        .f64()
        .map_err(|exc| MetricsParquetError::Read(format!("{name}: {exc}")))?
        .iter()
        .map(|value| value.filter(|item| item.is_finite()))
        .collect();
    Ok(Some(values))
}

fn required_complete_numeric_column(
    df: &DataFrame,
    name: &str,
) -> Result<Option<Vec<f64>>, MetricsParquetError> {
    let Some(values) = numeric_column(df, name)? else {
        return Ok(None);
    };
    values
        .into_iter()
        .map(|value| value.ok_or_else(|| MetricsParquetError::InvalidColumn(name.to_string())))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn string_column(df: &DataFrame, name: &str) -> Result<Option<Vec<String>>, MetricsParquetError> {
    let Ok(column) = df.column(name) else {
        return Ok(None);
    };
    let strings = column
        .str()
        .map_err(|exc| MetricsParquetError::Read(format!("{name}: {exc}")))?;
    Ok(Some(
        strings
            .iter()
            .map(|value| {
                value
                    .filter(|item| !item.trim().is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| MetricsParquetError::InvalidColumn(name.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn group_boundaries(
    backtest_id_values: &[String],
    row_count: usize,
) -> (Vec<String>, Vec<usize>, Vec<usize>) {
    let mut ids = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    let mut current_id = backtest_id_values[0].clone();
    ids.push(current_id.clone());
    starts.push(0);
    for (idx, next_id) in backtest_id_values
        .iter()
        .enumerate()
        .take(row_count)
        .skip(1)
    {
        if next_id != &current_id {
            ends.push(idx);
            starts.push(idx);
            ids.push(next_id.clone());
            current_id = next_id.clone();
        }
    }
    ends.push(row_count);
    (ids, starts, ends)
}

fn build_bah_equity(equity: &[f64], close: &[f64], starts: &[usize], ends: &[usize]) -> Vec<f64> {
    let mut out = vec![f64::NAN; equity.len()];
    for (&start, &end) in starts.iter().zip(ends.iter()) {
        let base_equity = equity[start];
        let base_close = close[start];
        for (idx, output) in out.iter_mut().enumerate().take(end).skip(start) {
            *output = base_equity * close[idx] / base_close;
        }
    }
    out
}
