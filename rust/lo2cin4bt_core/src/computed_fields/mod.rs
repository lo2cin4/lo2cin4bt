mod cross_section;
mod indicators;
mod math;
pub mod returns;
mod transforms;

use serde::Deserialize;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ComputedFieldSpec {
    pub name: String,
    pub op: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub right_source: Option<String>,
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub period: Option<usize>,
    #[serde(default)]
    pub fastperiod: Option<usize>,
    #[serde(default)]
    pub slowperiod: Option<usize>,
    #[serde(default)]
    pub signalperiod: Option<usize>,
    #[serde(default)]
    pub stddev: Option<f64>,
    #[serde(default)]
    pub percentile: Option<f64>,
    #[serde(default)]
    pub annualize: Option<bool>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub band: Option<String>,
    #[serde(default)]
    pub high_source: Option<String>,
    #[serde(default)]
    pub low_source: Option<String>,
    #[serde(default)]
    pub close_source: Option<String>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub sampling: Option<String>,
    #[serde(default)]
    pub start_lag: Option<usize>,
    #[serde(default)]
    pub end_lag: Option<usize>,
    #[serde(default)]
    pub lower: Option<f64>,
    #[serde(default)]
    pub upper: Option<f64>,
    #[serde(default)]
    pub ascending: Option<bool>,
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub true_source: Option<String>,
    #[serde(default)]
    pub true_value: Option<f64>,
    #[serde(default)]
    pub false_source: Option<String>,
    #[serde(default)]
    pub false_value: Option<f64>,
}

impl ComputedFieldSpec {
    pub(crate) fn required_period(&self) -> Result<usize, ComputedFieldError> {
        self.period.filter(|period| *period > 0).ok_or_else(|| {
            ComputedFieldError::InvalidParameter(format!("{} requires a positive period", self.op))
        })
    }
}

#[derive(Debug, Error)]
pub enum ComputedFieldError {
    #[error("unsupported computed field operation: {0}")]
    UnsupportedOperation(String),
    #[error("unknown computed field source: {0}")]
    UnknownField(String),
    #[error("invalid computed field parameter: {0}")]
    InvalidParameter(String),
}

pub(crate) fn compute_fields(
    close: &[f64],
    market_fields: &BTreeMap<String, Vec<f64>>,
    dates: &[String],
    rows: usize,
    cols: usize,
    specs: &[ComputedFieldSpec],
) -> Result<BTreeMap<String, Vec<f64>>, ComputedFieldError> {
    let expected_len = rows * cols;
    let mut fields = market_fields
        .iter()
        .map(|(name, values)| (name.trim().to_lowercase(), values.clone()))
        .collect::<BTreeMap<_, _>>();
    for (name, values) in &fields {
        if values.len() != expected_len {
            return Err(ComputedFieldError::InvalidParameter(format!(
                "{name} length {} does not match {expected_len}",
                values.len()
            )));
        }
    }
    fields.insert("close".to_string(), close.to_vec());
    for spec in specs {
        let name = spec.name.trim().to_lowercase();
        if name.is_empty() {
            return Err(ComputedFieldError::InvalidParameter(
                "computed field name is required".to_string(),
            ));
        }
        if fields.contains_key(&name) {
            return Err(ComputedFieldError::InvalidParameter(format!(
                "computed field name already exists: {name}"
            )));
        }
        let op = spec.op.trim().to_lowercase();
        let values = if op.starts_with("indicator.") || op.starts_with("rolling.") {
            indicators::compute(&op, spec, &fields, dates, rows, cols)?
        } else if op.starts_with("math.") {
            math::compute(&op, spec, &fields, expected_len)?
        } else if op.starts_with("transform.") {
            transforms::compute(&op, spec, &fields, rows, cols)?
        } else if op.starts_with("cross_section.") {
            cross_section::compute(&op, spec, &fields, rows, cols)?
        } else {
            return Err(ComputedFieldError::UnsupportedOperation(op));
        };
        fields.insert(name, values);
    }
    Ok(fields)
}

pub(crate) fn field<'a>(
    fields: &'a BTreeMap<String, Vec<f64>>,
    name: &str,
) -> Result<&'a [f64], ComputedFieldError> {
    let key = name.trim().to_lowercase();
    fields
        .get(&key)
        .map(Vec::as_slice)
        .ok_or(ComputedFieldError::UnknownField(key))
}

pub(crate) fn scalar_or_field(
    fields: &BTreeMap<String, Vec<f64>>,
    source: Option<&str>,
    value: Option<f64>,
    len: usize,
) -> Result<Vec<f64>, ComputedFieldError> {
    if let Some(name) = source {
        return Ok(field(fields, name)?.to_vec());
    }
    value.map(|scalar| vec![scalar; len]).ok_or_else(|| {
        ComputedFieldError::InvalidParameter("right_source or value is required".to_string())
    })
}

pub(crate) fn quantile(sorted: &[f64], q: f64) -> f64 {
    let position = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, op: &str, source: &str) -> ComputedFieldSpec {
        ComputedFieldSpec {
            name: name.to_string(),
            op: op.to_string(),
            source: Some(source.to_string()),
            ..ComputedFieldSpec::default()
        }
    }

    #[test]
    fn composes_recent_return_and_twelve_to_one_style_score() {
        let dates = vec![
            "2024-01-31".to_string(),
            "2024-02-29".to_string(),
            "2024-03-28".to_string(),
        ];
        let mut recent = spec("recent_return", "indicator.calendar_return", "close");
        recent.sampling = Some("month_end".to_string());
        recent.start_lag = Some(1);
        recent.end_lag = Some(0);

        let mut twelve_to_one = spec("twelve_to_one", "indicator.calendar_return", "close");
        twelve_to_one.sampling = Some("month_end".to_string());
        twelve_to_one.start_lag = Some(2);
        twelve_to_one.end_lag = Some(1);

        let mut one_plus_recent = spec("one_plus_recent", "math.add", "recent_return");
        one_plus_recent.value = Some(1.0);

        let mut adjusted = spec("adjusted", "math.multiply", "one_plus_recent");
        adjusted.right_source = Some("twelve_to_one".to_string());

        let fields = compute_fields(
            &[100.0, 120.0, 108.0],
            &BTreeMap::new(),
            &dates,
            3,
            1,
            &[recent, twelve_to_one, one_plus_recent, adjusted],
        )
        .expect("computed field chain should succeed");

        assert!((fields["recent_return"][2] + 0.1).abs() < 1e-12);
        assert!((fields["twelve_to_one"][2] - 0.2).abs() < 1e-12);
        assert!((fields["adjusted"][2] - 0.18).abs() < 1e-12);
    }

    #[test]
    fn rolling_and_transform_operations_respect_asset_columns() {
        let dates = (1..=3)
            .map(|day| format!("2024-01-0{day}"))
            .collect::<Vec<_>>();
        let mut rolling = spec("rolling_sum", "rolling.sum", "close");
        rolling.period = Some(2);
        let mut lagged = spec("lagged", "transform.lag", "rolling_sum");
        lagged.period = Some(1);
        let mut selected = spec("selected", "transform.where", "close");
        selected.condition = Some("gt".to_string());
        selected.value = Some(3.0);
        selected.true_source = Some("lagged".to_string());
        selected.false_value = Some(0.0);

        let fields = compute_fields(
            &[1.0, 10.0, 2.0, 20.0, 4.0, 40.0],
            &BTreeMap::new(),
            &dates,
            3,
            2,
            &[rolling, lagged, selected],
        )
        .expect("rolling transform chain should succeed");

        assert_eq!(fields["rolling_sum"][2..], [3.0, 30.0, 6.0, 60.0]);
        assert!(fields["lagged"][2].is_nan());
        assert_eq!(fields["lagged"][4..], [3.0, 30.0]);
        assert_eq!(fields["selected"][4..], [3.0, 30.0]);
    }

    #[test]
    fn cross_section_rank_is_calculated_within_each_date() {
        let mut descending = spec("rank", "cross_section.rank", "close");
        descending.ascending = Some(false);
        descending.method = Some("average".to_string());
        let fields = compute_fields(
            &[30.0, 10.0, 20.0, 2.0, 3.0, 1.0],
            &BTreeMap::new(),
            &["2024-01-01".to_string(), "2024-01-02".to_string()],
            2,
            3,
            &[descending],
        )
        .expect("cross-sectional rank should succeed");

        assert_eq!(fields["rank"], [1.0, 3.0, 2.0, 2.0, 1.0, 3.0]);
    }

    #[test]
    fn runtime_rejects_unmaterialized_registry_defaults() {
        let error = compute_fields(
            &[1.0, 2.0, 3.0],
            &BTreeMap::new(),
            &[
                "2024-01-01".to_string(),
                "2024-01-02".to_string(),
                "2024-01-03".to_string(),
            ],
            3,
            1,
            &[spec("macd", "indicator.macd", "close")],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ComputedFieldError::InvalidParameter(message)
                if message == "indicator.macd requires fastperiod"
        ));
    }
}
