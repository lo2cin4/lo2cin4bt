use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PLOT_BUNDLE_SCHEMA_VERSION: &str = "plot_bundle.v1";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlotProjectionInput {
    pub run_id: String,
    pub chart_type: String,
    pub title: String,
    pub series: Vec<PlotInputSeries>,
    pub x_axis: String,
    pub y_axis: String,
    pub source_hashes: Vec<String>,
    pub artifact_source_refs: Vec<String>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlotInputSeries {
    pub series_id: String,
    pub label: String,
    pub x: Vec<String>,
    pub y: Vec<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlotSeries {
    pub series_id: String,
    pub label: String,
    pub x: Vec<String>,
    pub y: Vec<f64>,
    pub annotations: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlotBundle {
    pub schema_version: String,
    pub contract_id: String,
    pub run_id: String,
    pub chart_type: String,
    pub title: String,
    pub series: Vec<PlotSeries>,
    pub axes: PlotAxes,
    pub legend: Vec<String>,
    pub source_hashes: Vec<String>,
    pub artifact_source_refs: Vec<String>,
    pub generated_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PlotAxes {
    pub x: String,
    pub y: String,
}

#[derive(Debug, Error, PartialEq)]
pub enum PlotProjectionError {
    #[error("plot projection requires chart identity, axes and non-empty x/y values")]
    MissingInput,
    #[error("plot x/y arrays must have equal lengths")]
    InvalidLength,
    #[error("plot values must be finite")]
    NonFiniteValue,
    #[error("plot projection requires valid source hashes and artifact source refs")]
    InvalidSource,
}

pub fn project_plot_bundle(input: PlotProjectionInput) -> Result<PlotBundle, PlotProjectionError> {
    if input.run_id.trim().is_empty()
        || input.chart_type.trim().is_empty()
        || input.title.trim().is_empty()
        || input.x_axis.trim().is_empty()
        || input.y_axis.trim().is_empty()
        || input.series.is_empty()
    {
        return Err(PlotProjectionError::MissingInput);
    }
    if input.series.iter().any(|series| {
        series.series_id.trim().is_empty()
            || series.label.trim().is_empty()
            || series.x.is_empty()
            || series.y.is_empty()
    }) {
        return Err(PlotProjectionError::MissingInput);
    }
    if input
        .series
        .iter()
        .any(|series| series.x.len() != series.y.len())
    {
        return Err(PlotProjectionError::InvalidLength);
    }
    if input
        .series
        .iter()
        .flat_map(|series| series.y.iter())
        .any(|value| !value.is_finite())
    {
        return Err(PlotProjectionError::NonFiniteValue);
    }
    if input.source_hashes.is_empty()
        || input
            .source_hashes
            .iter()
            .any(|value| value.len() != 64 || !value.chars().all(|item| item.is_ascii_hexdigit()))
        || input.artifact_source_refs.is_empty()
    {
        return Err(PlotProjectionError::InvalidSource);
    }
    let legend = input
        .series
        .iter()
        .map(|series| series.label.clone())
        .collect();
    let series = input
        .series
        .into_iter()
        .map(|series| PlotSeries {
            series_id: series.series_id,
            label: series.label,
            x: series.x,
            y: series.y,
            annotations: Vec::new(),
        })
        .collect();
    Ok(PlotBundle {
        schema_version: PLOT_BUNDLE_SCHEMA_VERSION.to_string(),
        contract_id: "lo2cin4bt.plot_bundle.v1".to_string(),
        run_id: input.run_id,
        chart_type: input.chart_type,
        title: input.title,
        series,
        axes: PlotAxes {
            x: input.x_axis,
            y: input.y_axis,
        },
        legend,
        source_hashes: input.source_hashes,
        artifact_source_refs: input.artifact_source_refs,
        generated_at: input.generated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projector_preserves_strategy_and_benchmark_values() {
        let bundle = project_plot_bundle(PlotProjectionInput {
            run_id: "run-1".to_string(),
            chart_type: "asset_curve_compare".to_string(),
            title: "Equity".to_string(),
            series: vec![
                PlotInputSeries {
                    series_id: "strategy".to_string(),
                    label: "Strategy".to_string(),
                    x: vec!["2024-01-02".to_string(), "2024-01-03".to_string()],
                    y: vec![100.0, 101.0],
                },
                PlotInputSeries {
                    series_id: "benchmark".to_string(),
                    label: "Buy and Hold".to_string(),
                    x: vec!["2024-01-02".to_string(), "2024-01-03".to_string()],
                    y: vec![100.0, 100.5],
                },
            ],
            x_axis: "time".to_string(),
            y_axis: "equity".to_string(),
            source_hashes: vec!["a".repeat(64)],
            artifact_source_refs: vec!["canonical.json".to_string()],
            generated_at: "2026-07-11T00:00:00Z".to_string(),
        })
        .unwrap();

        assert_eq!(bundle.schema_version, PLOT_BUNDLE_SCHEMA_VERSION);
        assert_eq!(bundle.series[0].y, vec![100.0, 101.0]);
        assert_eq!(bundle.series[1].y, vec![100.0, 100.5]);
    }
}
