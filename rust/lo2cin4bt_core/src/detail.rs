use crate::bar_aggregation::parse_canonical_date;
use crate::candidate_identity::parse_candidate_id;
use crate::computed_fields::returns::{excess_return, period_return_series, simple_return};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub const BACKTEST_DETAIL_SCHEMA_VERSION: &str = "backtest_detail_bundle.v3";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestDetailProjectionInput {
    pub run_id: String,
    pub backtest_id: String,
    pub label: String,
    pub asset: String,
    pub time: Vec<String>,
    pub session_labels: Vec<String>,
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    pub equity: Vec<f64>,
    #[serde(default)]
    pub benchmark_equity: Vec<f64>,
    #[serde(default)]
    pub trade_action: Vec<i64>,
    #[serde(default)]
    pub result_type: String,
    #[serde(default)]
    pub portfolio_returns: Vec<f64>,
    #[serde(default)]
    pub turnover: Vec<f64>,
    #[serde(default)]
    pub trade_cost: Vec<f64>,
    #[serde(default)]
    pub gross_exposure: Vec<f64>,
    #[serde(default)]
    pub contribution_series: BTreeMap<String, Vec<f64>>,
    #[serde(default)]
    pub weight_series: BTreeMap<String, Vec<f64>>,
    #[serde(default)]
    pub holding_rows: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    pub rebalance_rows: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    pub allocation_change_rows: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    pub strategy_summary: BTreeMap<String, Value>,
    #[serde(default)]
    pub parameter_summary: BTreeMap<String, Value>,
    #[serde(default)]
    pub semantic_fields: Vec<Value>,
    #[serde(default)]
    pub data_quality: BTreeMap<String, Value>,
    #[serde(default)]
    pub risk_gate_rows: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    pub risk_gate_summary: BTreeMap<String, Value>,
    #[serde(default)]
    pub ohlc_by_asset: BTreeMap<String, Vec<OhlcPoint>>,
    #[serde(default)]
    pub benchmark_label: String,
    #[serde(default)]
    pub metrics_matrix: BTreeMap<String, Value>,
    pub source_hashes: Vec<String>,
    pub artifact_source_refs: Vec<String>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DetailPoint {
    pub time: String,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DrawdownPoint {
    pub time: String,
    pub drawdown: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OhlcPoint {
    pub time: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeMarker {
    pub time: String,
    pub price: f64,
    pub action: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClosedTradeRow {
    pub rank: usize,
    pub asset: String,
    pub side: String,
    pub entry_time: String,
    pub exit_time: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub trade_return: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeOutcomeSummary {
    pub available: bool,
    pub closed_trade_count: usize,
    pub profitable_trade_count: usize,
    pub losing_trade_count: usize,
    pub breakeven_trade_count: usize,
    pub average_win: Option<f64>,
    pub average_loss: Option<f64>,
    pub profit_factor: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestDetailBundle {
    pub schema_version: String,
    pub contract_id: String,
    pub run_id: String,
    pub backtest_id: String,
    pub label: String,
    pub result_type: String,
    pub date_range_start: String,
    pub date_range_end: String,
    pub ohlc: Vec<OhlcPoint>,
    pub buy_markers: Vec<TradeMarker>,
    pub sell_markers: Vec<TradeMarker>,
    pub equity_series: Vec<DetailPoint>,
    pub benchmark_series: Vec<DetailPoint>,
    pub metrics_matrix: BTreeMap<String, Value>,
    pub monthly_return_rows: Vec<Value>,
    pub yearly_return_rows: Vec<Value>,
    pub drawdown_series: Vec<DrawdownPoint>,
    pub turnover_distribution: Vec<Value>,
    pub turnover_summary: BTreeMap<String, Value>,
    pub holding_rows: Vec<BTreeMap<String, Value>>,
    pub rebalance_rows: Vec<BTreeMap<String, Value>>,
    pub allocation_change_rows: Vec<BTreeMap<String, Value>>,
    pub asset_contribution_rows: Vec<Value>,
    pub asset_contribution_summary: BTreeMap<String, Value>,
    pub portfolio_visual_availability: BTreeMap<String, Value>,
    pub strategy_summary: BTreeMap<String, Value>,
    pub data_quality: BTreeMap<String, Value>,
    pub risk_gate_rows: Vec<BTreeMap<String, Value>>,
    pub risk_gate_summary: BTreeMap<String, Value>,
    pub ohlc_by_asset: BTreeMap<String, Vec<OhlcPoint>>,
    pub benchmark_label: String,
    pub trade_rows: Vec<ClosedTradeRow>,
    pub closed_trade_rows: Vec<ClosedTradeRow>,
    pub trade_outcome_summary: TradeOutcomeSummary,
    pub risk_diagnostics: BTreeMap<String, Value>,
    pub parameter_summary: BTreeMap<String, Value>,
    pub semantic_fields: Vec<Value>,
    pub source_hashes: Vec<String>,
    pub artifact_source_refs: Vec<String>,
    pub generated_at: String,
}

#[derive(Debug, Error, PartialEq)]
pub enum BacktestDetailProjectionError {
    #[error("detail projection requires run, backtest, label, asset and rows")]
    MissingInput,
    #[error("detail projection arrays must have equal lengths")]
    InvalidLength,
    #[error("portfolio detail requires matching weight and contribution assets")]
    InvalidPortfolioSeries,
    #[error("detail projection values must be finite and OHLC-consistent")]
    InvalidValue,
    #[error("detail projection requires valid source hashes and artifact refs")]
    InvalidSource,
    #[error("detail projection requires canonical YYYY-MM-DD session labels")]
    InvalidSessionLabel,
    #[error("detail projection requires a canonical Backtest_id: {0}")]
    InvalidBacktestId(String),
}

pub fn project_backtest_detail_bundle(
    input: BacktestDetailProjectionInput,
) -> Result<BacktestDetailBundle, BacktestDetailProjectionError> {
    let row_count = input.time.len();
    if input.run_id.trim().is_empty()
        || input.label.trim().is_empty()
        || input.asset.trim().is_empty()
        || row_count == 0
    {
        return Err(BacktestDetailProjectionError::MissingInput);
    }
    parse_candidate_id(&input.backtest_id)
        .map_err(BacktestDetailProjectionError::InvalidBacktestId)?;
    let required_lengths = [
        input.open.len(),
        input.high.len(),
        input.low.len(),
        input.close.len(),
        input.equity.len(),
        input.session_labels.len(),
    ];
    if required_lengths.iter().any(|length| *length != row_count)
        || (!input.benchmark_equity.is_empty() && input.benchmark_equity.len() != row_count)
        || (!input.trade_action.is_empty() && input.trade_action.len() != row_count)
        || optional_series_have_invalid_length(&input, row_count)
    {
        return Err(BacktestDetailProjectionError::InvalidLength);
    }
    if !portfolio_series_assets_match(&input.contribution_series, &input.weight_series) {
        return Err(BacktestDetailProjectionError::InvalidPortfolioSeries);
    }
    if input
        .open
        .iter()
        .chain(input.high.iter())
        .chain(input.low.iter())
        .chain(input.close.iter())
        .chain(input.equity.iter())
        .chain(input.benchmark_equity.iter())
        .chain(input.portfolio_returns.iter())
        .chain(input.turnover.iter())
        .chain(input.trade_cost.iter())
        .chain(input.gross_exposure.iter())
        .chain(input.contribution_series.values().flatten())
        .chain(input.weight_series.values().flatten())
        .any(|value| !value.is_finite())
        || input.equity.iter().any(|value| *value <= 0.0)
        || (0..row_count).any(|index| {
            input.high[index] < input.open[index].max(input.close[index])
                || input.low[index] > input.open[index].min(input.close[index])
                || input.high[index] < input.low[index]
        })
    {
        return Err(BacktestDetailProjectionError::InvalidValue);
    }
    if input.source_hashes.is_empty()
        || input
            .source_hashes
            .iter()
            .any(|value| value.len() != 64 || !value.chars().all(|item| item.is_ascii_hexdigit()))
        || input.artifact_source_refs.is_empty()
    {
        return Err(BacktestDetailProjectionError::InvalidSource);
    }

    let mut buy_markers = Vec::new();
    let mut sell_markers = Vec::new();
    let mut trade_rows = Vec::new();
    let mut open_trade: Option<(String, f64)> = None;
    for index in 0..row_count {
        match input.trade_action.get(index).copied().unwrap_or_default() {
            1 => {
                buy_markers.push(TradeMarker {
                    time: input.time[index].clone(),
                    price: input.close[index],
                    action: "buy".to_string(),
                });
                open_trade = Some((input.time[index].clone(), input.close[index]));
            }
            4 => {
                sell_markers.push(TradeMarker {
                    time: input.time[index].clone(),
                    price: input.close[index],
                    action: "sell".to_string(),
                });
                if let Some((entry_time, entry_price)) = open_trade.take() {
                    trade_rows.push(ClosedTradeRow {
                        rank: trade_rows.len() + 1,
                        asset: input.asset.clone(),
                        side: "long".to_string(),
                        entry_time,
                        exit_time: input.time[index].clone(),
                        entry_price,
                        exit_price: input.close[index],
                        trade_return: simple_return(input.close[index], entry_price),
                    });
                }
            }
            _ => {}
        }
    }
    if trade_rows.is_empty() && input.result_type == "portfolio" {
        trade_rows = portfolio_episode_rows(&input);
    }
    let profitable: Vec<f64> = trade_rows
        .iter()
        .map(|row| row.trade_return)
        .filter(|value| *value > 0.0)
        .collect();
    let losing: Vec<f64> = trade_rows
        .iter()
        .map(|row| row.trade_return)
        .filter(|value| *value < 0.0)
        .collect();
    let gross_profit: f64 = profitable.iter().sum();
    let gross_loss: f64 = losing.iter().map(|value| value.abs()).sum();
    let outcome = TradeOutcomeSummary {
        available: !trade_rows.is_empty(),
        closed_trade_count: trade_rows.len(),
        profitable_trade_count: profitable.len(),
        losing_trade_count: losing.len(),
        breakeven_trade_count: trade_rows
            .iter()
            .filter(|row| row.trade_return == 0.0)
            .count(),
        average_win: mean(&profitable),
        average_loss: mean(&losing),
        profit_factor: (gross_loss > 0.0).then_some(gross_profit / gross_loss),
    };
    let ohlc = (0..row_count)
        .map(|index| OhlcPoint {
            time: input.time[index].clone(),
            open: input.open[index],
            high: input.high[index],
            low: input.low[index],
            close: input.close[index],
        })
        .collect();
    let equity_series = input
        .time
        .iter()
        .cloned()
        .zip(input.equity.iter().copied())
        .map(|(time, value)| DetailPoint { time, value })
        .collect();
    let benchmark_series = input
        .time
        .iter()
        .cloned()
        .zip(input.benchmark_equity.iter().copied())
        .map(|(time, value)| DetailPoint { time, value })
        .collect();
    let monthly_return_rows =
        period_return_rows(&input.session_labels, &input.equity, ReturnPeriod::Month)?;
    let yearly_return_rows =
        period_return_rows(&input.session_labels, &input.equity, ReturnPeriod::Year)?;
    let drawdown_series = drawdown_series(&input.time, &input.equity);
    let turnover_distribution = turnover_distribution(&input.time, &input.turnover);
    let turnover_summary = turnover_summary(
        &input.turnover,
        &input.trade_cost,
        input.rebalance_rows.len(),
        input.allocation_change_rows.len(),
    );
    let mut asset_contribution_rows =
        asset_contribution_rows(&input.contribution_series, &input.weight_series);
    let asset_contribution_summary =
        asset_contribution_summary(&mut asset_contribution_rows, &input.equity);
    let risk_diagnostics =
        risk_diagnostics(&input.portfolio_returns, &drawdown_series, &trade_rows);
    let portfolio_visual_availability = portfolio_visual_availability(&input);
    let metrics_matrix = enriched_metrics_matrix(
        input.metrics_matrix,
        MetricsEnrichmentContext {
            returns: &input.portfolio_returns,
            benchmark_equity: &input.benchmark_equity,
            monthly_rows: &monthly_return_rows,
            drawdown: &drawdown_series,
            trades: &trade_rows,
            weights: &input.weight_series,
            holding_rows: &input.holding_rows,
        },
    );
    let result_type = if input.result_type.trim().is_empty() {
        "single_asset".to_string()
    } else {
        input.result_type.clone()
    };

    Ok(BacktestDetailBundle {
        schema_version: BACKTEST_DETAIL_SCHEMA_VERSION.to_string(),
        contract_id: "lo2cin4bt.backtest_detail_bundle.v3".to_string(),
        run_id: input.run_id,
        backtest_id: input.backtest_id,
        label: input.label,
        result_type,
        date_range_start: input.time.first().cloned().unwrap_or_default(),
        date_range_end: input.time.last().cloned().unwrap_or_default(),
        ohlc,
        buy_markers,
        sell_markers,
        equity_series,
        benchmark_series,
        metrics_matrix,
        monthly_return_rows,
        yearly_return_rows,
        drawdown_series,
        turnover_distribution,
        turnover_summary,
        holding_rows: normalize_rows(input.holding_rows),
        rebalance_rows: normalize_rows(input.rebalance_rows),
        allocation_change_rows: normalize_rows(input.allocation_change_rows),
        asset_contribution_rows,
        asset_contribution_summary,
        portfolio_visual_availability,
        strategy_summary: input.strategy_summary,
        data_quality: input.data_quality,
        risk_gate_rows: normalize_rows(input.risk_gate_rows),
        risk_gate_summary: input.risk_gate_summary,
        ohlc_by_asset: input.ohlc_by_asset,
        benchmark_label: input.benchmark_label,
        closed_trade_rows: trade_rows.clone(),
        trade_rows,
        trade_outcome_summary: outcome,
        risk_diagnostics,
        parameter_summary: input.parameter_summary,
        semantic_fields: input.semantic_fields,
        source_hashes: input.source_hashes,
        artifact_source_refs: input.artifact_source_refs,
        generated_at: input.generated_at,
    })
}

fn optional_series_have_invalid_length(
    input: &BacktestDetailProjectionInput,
    row_count: usize,
) -> bool {
    [
        &input.portfolio_returns,
        &input.turnover,
        &input.trade_cost,
        &input.gross_exposure,
    ]
    .iter()
    .any(|values| !values.is_empty() && values.len() != row_count)
        || input
            .contribution_series
            .values()
            .chain(input.weight_series.values())
            .any(|values| values.len() != row_count)
}

fn portfolio_series_assets_match(
    contribution: &BTreeMap<String, Vec<f64>>,
    weights: &BTreeMap<String, Vec<f64>>,
) -> bool {
    contribution.keys().eq(weights.keys())
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn row_text(row: &BTreeMap<String, Value>, names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| row.get(*name).and_then(Value::as_str))
        .unwrap_or_default()
        .to_string()
}

fn row_number(row: &BTreeMap<String, Value>, names: &[&str]) -> Option<f64> {
    names
        .iter()
        .find_map(|name| row.get(*name).and_then(Value::as_f64))
}

fn portfolio_episode_rows(input: &BacktestDetailProjectionInput) -> Vec<ClosedTradeRow> {
    input
        .rebalance_rows
        .windows(2)
        .enumerate()
        .filter_map(|(index, pair)| {
            let entry_time = row_text(&pair[0], &["Time", "time"]);
            let exit_time = row_text(&pair[1], &["Time", "time"]);
            let entry_price = row_number(&pair[0], &["Equity_value", "equity_value"])?;
            let exit_price = row_number(&pair[1], &["Equity_value", "equity_value"])?;
            if entry_time.is_empty() || exit_time.is_empty() || entry_price == 0.0 {
                return None;
            }
            Some(ClosedTradeRow {
                rank: index + 1,
                asset: "PORTFOLIO".to_string(),
                side: "portfolio_episode".to_string(),
                entry_time,
                exit_time,
                entry_price,
                exit_price,
                trade_return: simple_return(exit_price, entry_price),
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum ReturnPeriod {
    Month,
    Year,
}

fn period_return_rows(
    session_labels: &[String],
    equity: &[f64],
    period_kind: ReturnPeriod,
) -> Result<Vec<Value>, BacktestDetailProjectionError> {
    let mut closes: BTreeMap<String, (i64, Option<i64>, f64)> = BTreeMap::new();
    for (session_label, value) in session_labels.iter().zip(equity.iter()) {
        let (year, month, _) = parse_canonical_date(session_label)
            .map_err(|_| BacktestDetailProjectionError::InvalidSessionLabel)?;
        let (key, row_month) = match period_kind {
            ReturnPeriod::Month => (format!("{year:04}-{month:02}"), Some(month)),
            ReturnPeriod::Year => (format!("{year:04}"), None),
        };
        closes.insert(key, (year, row_month, *value));
    }
    let mut anchors = Vec::with_capacity(closes.len() + 1);
    anchors.push(equity[0]);
    anchors.extend(closes.values().map(|(_, _, end_equity)| *end_equity));
    let returns =
        period_return_series(&anchors).map_err(|_| BacktestDetailProjectionError::InvalidValue)?;

    Ok(closes
        .into_iter()
        .zip(returns.simple.into_iter().skip(1))
        .scan(
            equity[0],
            |start_equity, ((period, (year, month, end_equity)), period_return)| {
                let row = serde_json::json!({
                    "period": period,
                    "year": year,
                    "month": month,
                    "return": period_return,
                    "start_equity": *start_equity,
                    "end_equity": end_equity
                });
                *start_equity = end_equity;
                Some(row)
            },
        )
        .collect())
}

fn drawdown_series(time: &[String], equity: &[f64]) -> Vec<DrawdownPoint> {
    let mut peak = f64::NEG_INFINITY;
    time.iter()
        .cloned()
        .zip(equity.iter().copied())
        .map(|(time, value)| {
            peak = peak.max(value);
            DrawdownPoint {
                time,
                drawdown: if peak > 0.0 {
                    simple_return(value, peak)
                } else {
                    0.0
                },
            }
        })
        .collect()
}

fn turnover_distribution(time: &[String], turnover: &[f64]) -> Vec<Value> {
    time.iter()
        .zip(turnover.iter())
        .filter(|(_, value)| **value > 0.0)
        .map(|(time, value)| serde_json::json!({"time": time, "turnover": value}))
        .collect()
}

fn turnover_summary(
    turnover: &[f64],
    trade_cost: &[f64],
    checkpoint_events: usize,
    trade_events: usize,
) -> BTreeMap<String, Value> {
    let active: Vec<f64> = turnover
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .collect();
    BTreeMap::from([
        (
            "active_rebalance_events".to_string(),
            serde_json::json!(active.len()),
        ),
        (
            "total_turnover".to_string(),
            serde_json::json!(turnover.iter().sum::<f64>()),
        ),
        (
            "average_active_turnover".to_string(),
            serde_json::json!(mean(&active)),
        ),
        (
            "total_trade_cost".to_string(),
            serde_json::json!(trade_cost.iter().sum::<f64>()),
        ),
        (
            "checkpoint_events".to_string(),
            serde_json::json!(checkpoint_events),
        ),
        ("trade_events".to_string(), serde_json::json!(trade_events)),
    ])
}

fn asset_contribution_rows(
    contribution: &BTreeMap<String, Vec<f64>>,
    weights: &BTreeMap<String, Vec<f64>>,
) -> Vec<Value> {
    contribution
        .iter()
        .map(|(asset, values)| {
            let asset_weights = weights
                .get(asset)
                .expect("weight and contribution assets were validated")
                .as_slice();
            serde_json::json!({
                "asset": asset,
                "return_contribution": values.iter().sum::<f64>(),
                "avg_weight": mean(asset_weights),
                "active_days": asset_weights.iter().filter(|value| **value != 0.0).count()
            })
        })
        .collect()
}

fn asset_contribution_summary(rows: &mut [Value], equity: &[f64]) -> BTreeMap<String, Value> {
    let total = rows
        .iter()
        .filter_map(|row| row.get("return_contribution").and_then(Value::as_f64))
        .sum::<f64>();
    if total.abs() > 1e-12 {
        for row in rows.iter_mut() {
            if let Some(contribution) = row.get("return_contribution").and_then(Value::as_f64) {
                row["contribution_share"] = serde_json::json!(contribution / total);
            }
        }
    }
    let portfolio_total_return = equity
        .first()
        .zip(equity.last())
        .and_then(|(first, last)| (*first > 0.0).then_some(simple_return(*last, *first)));
    BTreeMap::from([
        ("asset_count".to_string(), serde_json::json!(rows.len())),
        (
            "total_asset_contribution".to_string(),
            serde_json::json!(total),
        ),
        (
            "portfolio_total_return".to_string(),
            serde_json::json!(portfolio_total_return),
        ),
        (
            "residual_and_compounding".to_string(),
            serde_json::json!(portfolio_total_return.map(|value| value - total)),
        ),
    ])
}

fn risk_diagnostics(
    returns: &[f64],
    drawdown: &[DrawdownPoint],
    trades: &[ClosedTradeRow],
) -> BTreeMap<String, Value> {
    let positive = returns.iter().filter(|value| **value > 0.0).count();
    let max_drawdown = drawdown
        .iter()
        .map(|point| point.drawdown)
        .fold(0.0, f64::min);
    let trade_returns: Vec<f64> = trades.iter().map(|row| row.trade_return).collect();
    let serial_correlation = serial_correlation_diagnostics(&trade_returns);
    let profit_concentration = profit_concentration_diagnostics(&trade_returns);
    let recovery_time = recovery_time_diagnostics(drawdown);
    BTreeMap::from([
        (
            "observation_count".to_string(),
            serde_json::json!(returns.len()),
        ),
        (
            "positive_return_ratio".to_string(),
            serde_json::json!(if returns.is_empty() {
                None
            } else {
                Some(positive as f64 / returns.len() as f64)
            }),
        ),
        ("max_drawdown".to_string(), serde_json::json!(max_drawdown)),
        ("serial_correlation".to_string(), serial_correlation),
        ("profit_concentration".to_string(), profit_concentration),
        ("recovery_time".to_string(), recovery_time),
    ])
}

fn serial_correlation_diagnostics(returns: &[f64]) -> Value {
    let max_lag = returns.len().saturating_sub(1).min(10);
    let lags: Vec<Value> = (1..=max_lag)
        .filter_map(|lag| {
            correlation(&returns[..returns.len() - lag], &returns[lag..])
                .map(|acf| serde_json::json!({"lag": lag, "acf": acf}))
        })
        .collect();
    serde_json::json!({
        "return_source": "closed_trades",
        "sample_count": returns.len(),
        "lag1": lags.first().and_then(|row| row.get("acf")).and_then(Value::as_f64),
        "significance_band": if returns.is_empty() { None } else { Some(1.96 / (returns.len() as f64).sqrt()) },
        "lags": lags,
    })
}

fn profit_concentration_diagnostics(returns: &[f64]) -> Value {
    let mut profits: Vec<f64> = returns
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .collect();
    profits.sort_by(f64::total_cmp);
    let total = profits.iter().sum::<f64>();
    if profits.is_empty() || total <= 0.0 {
        return serde_json::json!({
            "return_source": "closed_trades",
            "profitable_trade_count": 0,
            "top_20_contribution": null,
            "gini": null,
            "lorenz_curve": [],
        });
    }
    let mut cumulative = 0.0;
    let mut lorenz = vec![serde_json::json!({"trade_share": 0.0, "profit_share": 0.0})];
    for (index, profit) in profits.iter().enumerate() {
        cumulative += profit;
        lorenz.push(serde_json::json!({
            "trade_share": (index + 1) as f64 / profits.len() as f64,
            "profit_share": cumulative / total,
        }));
    }
    let area = lorenz
        .windows(2)
        .map(|pair| {
            let x0 = pair[0]["trade_share"]
                .as_f64()
                .expect("generated Lorenz trade_share is numeric");
            let x1 = pair[1]["trade_share"]
                .as_f64()
                .expect("generated Lorenz trade_share is numeric");
            let y0 = pair[0]["profit_share"]
                .as_f64()
                .expect("generated Lorenz profit_share is numeric");
            let y1 = pair[1]["profit_share"]
                .as_f64()
                .expect("generated Lorenz profit_share is numeric");
            (x1 - x0) * (y0 + y1) / 2.0
        })
        .sum::<f64>();
    let top_count = ((profits.len() as f64 * 0.2).ceil() as usize).max(1);
    let top_share = profits.iter().rev().take(top_count).sum::<f64>() / total;
    serde_json::json!({
        "return_source": "closed_trades",
        "profitable_trade_count": profits.len(),
        "top_20_contribution": top_share,
        "gini": 1.0 - 2.0 * area,
        "lorenz_curve": lorenz,
    })
}

fn recovery_time_diagnostics(drawdown: &[DrawdownPoint]) -> Value {
    let mut episodes = Vec::new();
    let mut start: Option<usize> = None;
    for (index, point) in drawdown.iter().enumerate() {
        if point.drawdown < 0.0 && start.is_none() {
            start = Some(index.saturating_sub(1));
        } else if point.drawdown >= 0.0 {
            if let Some(peak_index) = start.take() {
                episodes.push(serde_json::json!({
                    "peak_time": drawdown[peak_index].time,
                    "recovery_time": point.time,
                    "periods": index.saturating_sub(peak_index),
                    "recovered": true,
                }));
            }
        }
    }
    if let Some(peak_index) = start {
        episodes.push(serde_json::json!({
            "peak_time": drawdown[peak_index].time,
            "recovery_time": null,
            "periods": drawdown.len().saturating_sub(1).saturating_sub(peak_index),
            "recovered": false,
        }));
    }
    let mut recovered: Vec<f64> = episodes
        .iter()
        .filter(|row| row["recovered"].as_bool() == Some(true))
        .filter_map(|row| row["periods"].as_f64())
        .collect();
    recovered.sort_by(f64::total_cmp);
    let percentile =
        |probability| (!recovered.is_empty()).then(|| quantile(&recovered, probability));
    serde_json::json!({
        "recovered_count": recovered.len(),
        "unrecovered_count": episodes.len().saturating_sub(recovered.len()),
        "percentiles": {
            "p50_periods": percentile(0.50),
            "p75_periods": percentile(0.75),
            "p90_periods": percentile(0.90),
            "max_periods": recovered.last().copied(),
        },
        "episodes": episodes,
    })
}

struct MetricsEnrichmentContext<'a> {
    returns: &'a [f64],
    benchmark_equity: &'a [f64],
    monthly_rows: &'a [Value],
    drawdown: &'a [DrawdownPoint],
    trades: &'a [ClosedTradeRow],
    weights: &'a BTreeMap<String, Vec<f64>>,
    holding_rows: &'a [BTreeMap<String, Value>],
}

fn enriched_metrics_matrix(
    mut metrics: BTreeMap<String, Value>,
    context: MetricsEnrichmentContext<'_>,
) -> BTreeMap<String, Value> {
    let MetricsEnrichmentContext {
        returns,
        benchmark_equity,
        monthly_rows,
        drawdown,
        trades,
        weights,
        holding_rows,
    } = context;
    let finite: Vec<f64> = returns
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect();
    if !finite.is_empty() {
        let average = finite.iter().sum::<f64>() / finite.len() as f64;
        let variance = finite
            .iter()
            .map(|value| (value - average).powi(2))
            .sum::<f64>()
            / finite.len() as f64;
        let std = variance.sqrt();
        if std > 0.0 {
            insert_metric(
                &mut metrics,
                "skewness",
                standardized_moment(&finite, average, std, 3),
            );
            insert_metric(
                &mut metrics,
                "kurtosis",
                standardized_moment(&finite, average, std, 4) - 3.0,
            );
        }
        let mut ordered = finite.clone();
        ordered.sort_by(f64::total_cmp);
        insert_metric(&mut metrics, "var_95", quantile(&ordered, 0.05));
        insert_metric(&mut metrics, "var_99", quantile(&ordered, 0.01));
        insert_metric(&mut metrics, "cvar_95", lower_tail_mean(&ordered, 0.05));
        insert_metric(&mut metrics, "cvar_99", lower_tail_mean(&ordered, 0.01));
    }

    let max_drawdown = drawdown
        .iter()
        .map(|point| point.drawdown)
        .fold(0.0, f64::min);
    insert_metric(
        &mut metrics,
        "max_drawdown_duration_days",
        max_drawdown_duration(drawdown) as f64,
    );
    if max_drawdown < 0.0 {
        if let Some(value) = metric_value(&metrics, "total_return") {
            insert_metric(&mut metrics, "recovery_factor", value / max_drawdown.abs());
        }
        if let Some(value) = metric_value(&metrics, "cagr") {
            insert_metric(&mut metrics, "calmar", value / max_drawdown.abs());
        }
    }

    let monthly: Vec<f64> = monthly_rows
        .iter()
        .filter_map(|row| row.get("return").and_then(Value::as_f64))
        .collect();
    if !monthly.is_empty() {
        insert_metric(
            &mut metrics,
            "worst_month",
            monthly.iter().copied().fold(f64::INFINITY, f64::min),
        );
        insert_metric(
            &mut metrics,
            "best_month",
            monthly.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        );
        insert_metric(
            &mut metrics,
            "positive_month_ratio",
            monthly.iter().filter(|value| **value > 0.0).count() as f64 / monthly.len() as f64,
        );
    }

    enrich_trade_quality(&mut metrics, trades);
    enrich_operations(&mut metrics, weights, holding_rows);
    enrich_benchmark(&mut metrics, &finite, benchmark_equity);
    metrics
}

fn insert_metric(metrics: &mut BTreeMap<String, Value>, key: &str, value: f64) {
    if value.is_finite() {
        metrics.insert(key.to_string(), serde_json::json!(value));
    }
}

fn metric_value(metrics: &BTreeMap<String, Value>, key: &str) -> Option<f64> {
    metrics.get(key).and_then(Value::as_f64)
}

fn standardized_moment(values: &[f64], average: f64, std: f64, order: i32) -> f64 {
    values
        .iter()
        .map(|value| ((value - average) / std).powi(order))
        .sum::<f64>()
        / values.len() as f64
}

fn quantile(values: &[f64], probability: f64) -> f64 {
    if values.len() == 1 {
        return values[0];
    }
    let position = probability.clamp(0.0, 1.0) * (values.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    values[lower] + (values[upper] - values[lower]) * (position - lower as f64)
}

fn lower_tail_mean(values: &[f64], probability: f64) -> f64 {
    let count = ((values.len() as f64 * probability).ceil() as usize).max(1);
    values[..count].iter().sum::<f64>() / count as f64
}

fn max_drawdown_duration(drawdown: &[DrawdownPoint]) -> usize {
    let (mut longest, mut current) = (0, 0);
    for point in drawdown {
        if point.drawdown < 0.0 {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    longest
}

fn enrich_trade_quality(metrics: &mut BTreeMap<String, Value>, trades: &[ClosedTradeRow]) {
    if trades.is_empty() {
        return;
    }
    let wins: Vec<f64> = trades
        .iter()
        .map(|trade| trade.trade_return)
        .filter(|value| *value > 0.0)
        .collect();
    let losses: Vec<f64> = trades
        .iter()
        .map(|trade| trade.trade_return)
        .filter(|value| *value < 0.0)
        .collect();
    let gross_profit = wins.iter().sum::<f64>();
    let gross_loss = losses.iter().sum::<f64>();
    insert_metric(metrics, "win_rate", wins.len() as f64 / trades.len() as f64);
    if let Some(value) = mean(&wins) {
        insert_metric(metrics, "average_win", value);
    }
    if let Some(value) = mean(&losses) {
        insert_metric(metrics, "average_loss", value);
    }
    insert_metric(metrics, "gross_profit", gross_profit);
    insert_metric(metrics, "gross_loss", gross_loss);
    if gross_loss < 0.0 {
        insert_metric(metrics, "profit_factor", gross_profit / gross_loss.abs());
    }
    if let (Some(win), Some(loss)) = (mean(&wins), mean(&losses)) {
        if loss < 0.0 {
            insert_metric(metrics, "average_win_loss_ratio", win / loss.abs());
        }
    }
    let (wins_run, losses_run) = consecutive_outcomes(trades);
    insert_metric(metrics, "max_consecutive_wins", wins_run as f64);
    insert_metric(metrics, "max_consecutive_losses", losses_run as f64);
}

fn consecutive_outcomes(trades: &[ClosedTradeRow]) -> (usize, usize) {
    let (mut wins, mut losses, mut current_wins, mut current_losses) = (0, 0, 0, 0);
    for trade in trades {
        if trade.trade_return > 0.0 {
            current_wins += 1;
            current_losses = 0;
            wins = wins.max(current_wins);
        } else if trade.trade_return < 0.0 {
            current_losses += 1;
            current_wins = 0;
            losses = losses.max(current_losses);
        }
    }
    (wins, losses)
}

fn enrich_operations(
    metrics: &mut BTreeMap<String, Value>,
    weights: &BTreeMap<String, Vec<f64>>,
    holding_rows: &[BTreeMap<String, Value>],
) {
    let row_count = weights.values().map(Vec::len).max().unwrap_or(0);
    if row_count > 0 {
        let active_sum = (0..row_count)
            .map(|index| {
                weights
                    .values()
                    .filter(|values| values.get(index).copied().unwrap_or(0.0).abs() > 1e-12)
                    .count() as f64
            })
            .sum::<f64>();
        insert_metric(metrics, "avg_holdings", active_sum / row_count as f64);
        return;
    }

    let mut active_by_time: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in holding_rows {
        let time = row
            .get("Time")
            .or_else(|| row.get("time"))
            .and_then(Value::as_str);
        let weight = row
            .get("Target_weight")
            .or_else(|| row.get("target_weight"))
            .or_else(|| row.get("Weight"))
            .or_else(|| row.get("weight"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let asset = row
            .get("Asset")
            .or_else(|| row.get("asset"))
            .and_then(Value::as_str);
        if let Some(time) = time.filter(|_| weight.abs() > 1e-12) {
            if let Some(asset) = asset {
                active_by_time
                    .entry(time.to_string())
                    .or_default()
                    .insert(asset.to_string());
            }
        }
    }
    if !active_by_time.is_empty() {
        let active_sum = active_by_time.values().map(BTreeSet::len).sum::<usize>() as f64;
        insert_metric(
            metrics,
            "avg_holdings",
            active_sum / active_by_time.len() as f64,
        );
    }
}

fn enrich_benchmark(
    metrics: &mut BTreeMap<String, Value>,
    returns: &[f64],
    benchmark_equity: &[f64],
) {
    if let (Some(total), Some(benchmark)) = (
        metric_value(metrics, "total_return"),
        metric_value(metrics, "bah_total_return"),
    ) {
        insert_metric(metrics, "excess_return", excess_return(total, benchmark));
    }
    if returns.len() < 2 || benchmark_equity.len() != returns.len() {
        return;
    }
    let benchmark_returns: Vec<f64> = benchmark_equity
        .windows(2)
        .map(|pair| {
            if pair[0] == 0.0 {
                0.0
            } else {
                simple_return(pair[1], pair[0])
            }
        })
        .collect();
    if let Some(value) = correlation(&returns[1..], &benchmark_returns) {
        insert_metric(metrics, "benchmark_correlation", value);
    }
}

fn correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let left_mean = left.iter().sum::<f64>() / left.len() as f64;
    let right_mean = right.iter().sum::<f64>() / right.len() as f64;
    let covariance = left
        .iter()
        .zip(right.iter())
        .map(|(a, b)| (a - left_mean) * (b - right_mean))
        .sum::<f64>();
    let left_variance = left
        .iter()
        .map(|value| (value - left_mean).powi(2))
        .sum::<f64>();
    let right_variance = right
        .iter()
        .map(|value| (value - right_mean).powi(2))
        .sum::<f64>();
    let denominator = (left_variance * right_variance).sqrt();
    (denominator > 0.0).then_some(covariance / denominator)
}

fn portfolio_visual_availability(input: &BacktestDetailProjectionInput) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "allocation_timeline".to_string(),
            serde_json::json!(!input.holding_rows.is_empty()),
        ),
        (
            "allocation_changes".to_string(),
            serde_json::json!(!input.allocation_change_rows.is_empty()),
        ),
        (
            "asset_contribution".to_string(),
            serde_json::json!(!input.contribution_series.is_empty()),
        ),
        (
            "turnover".to_string(),
            serde_json::json!(!input.turnover.is_empty()),
        ),
        (
            "asset_ohlc".to_string(),
            serde_json::json!(!input.ohlc_by_asset.is_empty()),
        ),
    ])
}

fn normalize_rows(rows: Vec<BTreeMap<String, Value>>) -> Vec<BTreeMap<String, Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(key, value)| (key.to_ascii_lowercase(), value))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portfolio_detail_requires_matching_weight_and_contribution_assets() {
        let contribution = BTreeMap::from([("AAA".to_string(), vec![0.0, 0.1])]);
        let mismatched_weights = BTreeMap::from([("BBB".to_string(), vec![0.0, 1.0])]);

        assert!(!portfolio_series_assets_match(
            &contribution,
            &mismatched_weights
        ));
    }

    #[test]
    fn detail_projector_pairs_closed_trade_without_recomputing_equity() {
        let bundle = project_backtest_detail_bundle(BacktestDetailProjectionInput {
            run_id: "run-1".to_string(),
            backtest_id: "candidate-a:single_backtest:fixed".to_string(),
            label: "Candidate A".to_string(),
            asset: "AAA".to_string(),
            time: vec!["2024-01-01".to_string(), "2024-01-02".to_string()],
            session_labels: vec!["2024-01-01".to_string(), "2024-01-02".to_string()],
            open: vec![100.0, 110.0],
            high: vec![101.0, 112.0],
            low: vec![99.0, 109.0],
            close: vec![100.0, 111.0],
            equity: vec![100.0, 111.0],
            benchmark_equity: vec![100.0, 110.0],
            trade_action: vec![1, 4],
            result_type: "single_asset".to_string(),
            portfolio_returns: Vec::new(),
            turnover: Vec::new(),
            trade_cost: Vec::new(),
            gross_exposure: Vec::new(),
            contribution_series: BTreeMap::new(),
            weight_series: BTreeMap::new(),
            holding_rows: Vec::new(),
            rebalance_rows: Vec::new(),
            allocation_change_rows: Vec::new(),
            strategy_summary: BTreeMap::new(),
            parameter_summary: BTreeMap::new(),
            semantic_fields: Vec::new(),
            data_quality: BTreeMap::new(),
            risk_gate_rows: Vec::new(),
            risk_gate_summary: BTreeMap::new(),
            ohlc_by_asset: BTreeMap::new(),
            benchmark_label: "Benchmark".to_string(),
            metrics_matrix: BTreeMap::new(),
            source_hashes: vec!["a".repeat(64)],
            artifact_source_refs: vec!["canonical.json".to_string()],
            generated_at: "2026-07-11T00:00:00Z".to_string(),
        })
        .unwrap();

        assert_eq!(bundle.equity_series[1].value, 111.0);
        assert_eq!(bundle.trade_rows.len(), 1);
        assert!((bundle.trade_rows[0].trade_return - 0.11).abs() < 1e-12);
    }

    #[test]
    fn period_rows_use_session_labels_and_central_return_series_for_intraday_equity() {
        let monthly = period_return_rows(
            &[
                "2024-01-02".to_string(),
                "2024-01-02".to_string(),
                "2024-01-31".to_string(),
                "2024-02-01".to_string(),
            ],
            &[100.0, 101.0, 110.0, 121.0],
            ReturnPeriod::Month,
        )
        .expect("canonical session labels");

        assert_eq!(monthly.len(), 2);
        assert_eq!(monthly[0]["period"], "2024-01");
        assert_eq!(monthly[0]["start_equity"], 100.0);
        assert_eq!(monthly[0]["end_equity"], 110.0);
        assert!((monthly[0]["return"].as_f64().unwrap() - 0.1).abs() < 1e-12);
        assert_eq!(monthly[1]["period"], "2024-02");
        assert_eq!(monthly[1]["start_equity"], 110.0);
        assert_eq!(monthly[1]["end_equity"], 121.0);
        assert!((monthly[1]["return"].as_f64().unwrap() - 0.1).abs() < 1e-12);
    }

    #[test]
    fn period_rows_reject_noncanonical_session_labels() {
        assert_eq!(
            period_return_rows(
                &["2024-01-02T16:00:00Z".to_string()],
                &[100.0],
                ReturnPeriod::Month,
            ),
            Err(BacktestDetailProjectionError::InvalidSessionLabel)
        );
    }

    #[test]
    fn operations_metrics_accept_canonical_long_form_holdings() {
        let rows = vec![
            BTreeMap::from([
                ("Time".to_string(), serde_json::json!("2024-01-01")),
                ("Asset".to_string(), serde_json::json!("AAA")),
                ("Target_weight".to_string(), serde_json::json!(0.5)),
            ]),
            BTreeMap::from([
                ("Time".to_string(), serde_json::json!("2024-01-01")),
                ("Asset".to_string(), serde_json::json!("BBB")),
                ("Target_weight".to_string(), serde_json::json!(0.5)),
            ]),
            BTreeMap::from([
                ("Time".to_string(), serde_json::json!("2024-02-01")),
                ("Asset".to_string(), serde_json::json!("AAA")),
                ("Target_weight".to_string(), serde_json::json!(1.0)),
            ]),
        ];
        let mut metrics = BTreeMap::new();

        enrich_operations(&mut metrics, &BTreeMap::new(), &rows);

        assert_eq!(
            metrics.get("avg_holdings").and_then(Value::as_f64),
            Some(1.5)
        );
    }

    #[test]
    fn rich_diagnostics_and_contributions_are_complete() {
        let trades = vec![0.10, -0.05, 0.20, 0.15]
            .into_iter()
            .enumerate()
            .map(|(index, trade_return)| ClosedTradeRow {
                rank: index + 1,
                asset: "AAA".to_string(),
                side: "long".to_string(),
                entry_time: format!("2024-01-0{}", index + 1),
                exit_time: format!("2024-01-0{}", index + 2),
                entry_price: 100.0,
                exit_price: 100.0 * (1.0 + trade_return),
                trade_return,
            })
            .collect::<Vec<_>>();
        let drawdown = vec![
            DrawdownPoint {
                time: "d0".to_string(),
                drawdown: 0.0,
            },
            DrawdownPoint {
                time: "d1".to_string(),
                drawdown: -0.1,
            },
            DrawdownPoint {
                time: "d2".to_string(),
                drawdown: 0.0,
            },
        ];
        let diagnostics = risk_diagnostics(&[0.0, -0.1, 0.2], &drawdown, &trades);
        assert!(diagnostics["serial_correlation"]["lag1"].is_number());
        assert!(diagnostics["profit_concentration"]["gini"].is_number());
        assert_eq!(diagnostics["recovery_time"]["recovered_count"], 1);

        let mut rows = vec![serde_json::json!({"return_contribution": 0.1})];
        let summary = asset_contribution_summary(&mut rows, &[100.0, 121.0]);
        assert_eq!(rows[0]["contribution_share"], 1.0);
        assert!((summary["portfolio_total_return"].as_f64().unwrap() - 0.21).abs() < 1e-12);
    }
}
