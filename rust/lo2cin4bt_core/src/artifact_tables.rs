use polars::prelude::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

#[derive(Clone, Copy)]
enum ColumnKind {
    Float,
    Bool,
    String,
}

pub(crate) fn write_result_rows_parquet(
    path: &Path,
    rows: &[BTreeMap<String, Value>],
    table_key: &str,
) -> Result<(), String> {
    let schema = table_schema(table_key);
    if schema.is_empty() {
        return Err(format!("unknown canonical result table: {table_key}"));
    }
    let mut columns: Vec<String> = rows
        .iter()
        .flat_map(|row| row.keys().cloned())
        .collect::<Vec<_>>();
    if rows.is_empty() {
        columns.extend(schema.iter().map(|(name, _)| (*name).to_string()));
    }
    columns.sort();
    columns.dedup();

    let mut series = Vec::with_capacity(columns.len());
    for column in columns {
        let schema_kind = schema
            .iter()
            .find_map(|(name, kind)| (*name == column).then_some(*kind));
        let kind = schema_kind.unwrap_or_else(|| infer_column_kind(rows, &column));
        let output = match kind {
            ColumnKind::Float => Series::new(
                (&column).into(),
                rows.iter()
                    .map(|row| row.get(&column).and_then(value_as_f64))
                    .collect::<Vec<_>>(),
            )
            .into(),
            ColumnKind::Bool => Series::new(
                (&column).into(),
                rows.iter()
                    .map(|row| row.get(&column).and_then(value_as_bool))
                    .collect::<Vec<_>>(),
            )
            .into(),
            ColumnKind::String => Series::new(
                (&column).into(),
                rows.iter()
                    .map(|row| row.get(&column).and_then(value_as_string))
                    .collect::<Vec<_>>(),
            )
            .into(),
        };
        series.push(output);
    }

    let mut frame = DataFrame::new(rows.len(), series).map_err(|exc| exc.to_string())?;
    let file = File::create(path).map_err(|exc| exc.to_string())?;
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut frame)
        .map_err(|exc| exc.to_string())?;
    Ok(())
}

fn infer_column_kind(rows: &[BTreeMap<String, Value>], column: &str) -> ColumnKind {
    if rows
        .iter()
        .any(|row| row.get(column).and_then(value_as_f64).is_some())
    {
        ColumnKind::Float
    } else if rows
        .iter()
        .any(|row| row.get(column).and_then(value_as_bool).is_some())
    {
        ColumnKind::Bool
    } else {
        ColumnKind::String
    }
}

fn table_schema(table_key: &str) -> &'static [(&'static str, ColumnKind)] {
    match table_key {
        "equity_curve" | "execution_equity_curve" => &[
            ("Backtest_id", ColumnKind::String),
            ("Time", ColumnKind::String),
            ("Session_label", ColumnKind::String),
            ("Equity_value", ColumnKind::Float),
            ("Portfolio_return", ColumnKind::Float),
            ("Turnover", ColumnKind::Float),
            ("Trade_cost", ColumnKind::Float),
            ("Borrow_cost", ColumnKind::Float),
            ("Cost_drag", ColumnKind::Float),
            ("Selected_count", ColumnKind::Float),
            ("Gross_exposure", ColumnKind::Float),
            ("Cash_weight", ColumnKind::Float),
        ],
        "holdings" => &[
            ("Backtest_id", ColumnKind::String),
            ("Time", ColumnKind::String),
            ("Asset", ColumnKind::String),
            ("Rank", ColumnKind::Float),
            ("Selected", ColumnKind::Bool),
            ("Eligible", ColumnKind::Bool),
            ("Score", ColumnKind::Float),
            ("Target_weight", ColumnKind::Float),
        ],
        "rebalance_audit" => &[
            ("Backtest_id", ColumnKind::String),
            ("Time", ColumnKind::String),
            ("Rebalance", ColumnKind::Bool),
            ("Selected_assets", ColumnKind::String),
            ("Selected_count", ColumnKind::Float),
            ("Ranked_candidates", ColumnKind::String),
            ("Turnover", ColumnKind::Float),
            ("Cost_rate", ColumnKind::Float),
            ("Trade_cost", ColumnKind::Float),
            ("Borrow_cost", ColumnKind::Float),
            ("Equity_value", ColumnKind::Float),
        ],
        "rebalance_trades" => &[
            ("Backtest_id", ColumnKind::String),
            ("Time", ColumnKind::String),
            ("Asset", ColumnKind::String),
            ("Before_weight", ColumnKind::Float),
            ("Target_weight", ColumnKind::Float),
            ("Trade_delta", ColumnKind::Float),
            ("Action", ColumnKind::String),
            ("Trade_turnover", ColumnKind::Float),
            ("Allocated_cost", ColumnKind::Float),
            ("Selected", ColumnKind::Bool),
            ("Eligible", ColumnKind::Bool),
            ("Rank", ColumnKind::Float),
            ("Score", ColumnKind::Float),
            ("Reason", ColumnKind::String),
        ],
        "risk_gate_events" => &[
            ("Backtest_id", ColumnKind::String),
            ("Time", ColumnKind::String),
            ("Gate", ColumnKind::String),
            ("Threshold", ColumnKind::Float),
            ("Observed", ColumnKind::Float),
            ("Action", ColumnKind::String),
            ("Affected_assets", ColumnKind::String),
            ("Resulting_target_weights", ColumnKind::String),
        ],
        "settlements" => &[
            ("Backtest_id", ColumnKind::String),
            ("Order_id", ColumnKind::String),
            ("Asset", ColumnKind::String),
            ("Remaining_sessions", ColumnKind::Float),
            ("Cash_delta", ColumnKind::Float),
            ("Status", ColumnKind::String),
        ],
        _ => &[],
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value.as_f64().filter(|item| item.is_finite())
}

fn value_as_bool(value: &Value) -> Option<bool> {
    value.as_bool()
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn empty_canonical_table_keeps_its_schema() {
        let path = std::env::temp_dir().join(format!(
            "lo2cin4bt-empty-risk-table-{}.parquet",
            std::process::id()
        ));
        write_result_rows_parquet(&path, &[], "risk_gate_events").unwrap();
        let file = File::open(&path).unwrap();
        let frame = ParquetReader::new(file).finish().unwrap();
        assert_eq!(frame.height(), 0);
        assert!(frame
            .get_column_names()
            .iter()
            .any(|name| name.as_str() == "Gate"));
        fs::remove_file(path).unwrap();
    }
}
