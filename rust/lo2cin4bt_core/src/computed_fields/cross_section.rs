use super::{field, quantile, ComputedFieldError, ComputedFieldSpec};
use std::collections::BTreeMap;

pub(crate) fn compute(
    op: &str,
    spec: &ComputedFieldSpec,
    fields: &BTreeMap<String, Vec<f64>>,
    rows: usize,
    cols: usize,
) -> Result<Vec<f64>, ComputedFieldError> {
    let source = field(
        fields,
        spec.source
            .as_deref()
            .ok_or_else(|| ComputedFieldError::InvalidParameter(format!("{op} requires source")))?,
    )?;
    let mut output = vec![f64::NAN; rows * cols];
    for row in 0..rows {
        let offset = row * cols;
        let valid = (0..cols)
            .filter(|col| source[offset + col].is_finite())
            .collect::<Vec<_>>();
        if valid.is_empty() {
            continue;
        }
        match op {
            "cross_section.rank" | "cross_section.percentile" => {
                let ascending = spec.ascending.ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(format!("{op} requires ascending"))
                })?;
                let mut ordered = valid.clone();
                ordered.sort_by(|left, right| {
                    let ordering = source[offset + *left].total_cmp(&source[offset + *right]);
                    if ascending {
                        ordering
                    } else {
                        ordering.reverse()
                    }
                    .then_with(|| left.cmp(right))
                });
                let method = spec
                    .method
                    .as_deref()
                    .ok_or_else(|| {
                        ComputedFieldError::InvalidParameter(format!("{op} requires method"))
                    })?
                    .to_lowercase();
                let mut position = 0usize;
                let mut dense_rank = 1.0;
                while position < ordered.len() {
                    let start = position;
                    let value = source[offset + ordered[position]];
                    while position + 1 < ordered.len()
                        && source[offset + ordered[position + 1]]
                            .total_cmp(&value)
                            .is_eq()
                    {
                        position += 1;
                    }
                    let end = position;
                    let rank = match method.as_str() {
                        "average" => (start + end) as f64 / 2.0 + 1.0,
                        "dense" => dense_rank,
                        "ordinal" => start as f64 + 1.0,
                        _ => {
                            return Err(ComputedFieldError::InvalidParameter(format!(
                                "{op} unsupported method={method}"
                            )))
                        }
                    };
                    for (ordinal, col) in ordered[start..=end].iter().enumerate() {
                        let raw_rank = if method == "ordinal" {
                            rank + ordinal as f64
                        } else {
                            rank
                        };
                        output[offset + *col] = if op == "cross_section.percentile" {
                            if ordered.len() == 1 {
                                0.5
                            } else {
                                (raw_rank - 1.0) / (ordered.len() - 1) as f64
                            }
                        } else {
                            raw_rank
                        };
                    }
                    dense_rank += 1.0;
                    position += 1;
                }
            }
            "cross_section.zscore" => {
                let mean =
                    valid.iter().map(|col| source[offset + *col]).sum::<f64>() / valid.len() as f64;
                let variance = if valid.len() > 1 {
                    valid
                        .iter()
                        .map(|col| (source[offset + *col] - mean).powi(2))
                        .sum::<f64>()
                        / (valid.len() - 1) as f64
                } else {
                    0.0
                };
                let stddev = variance.sqrt();
                for col in valid {
                    output[offset + col] = if stddev > 0.0 {
                        (source[offset + col] - mean) / stddev
                    } else {
                        0.0
                    };
                }
            }
            "cross_section.winsorize" => {
                let lower = spec.lower.ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "cross_section.winsorize requires lower".to_string(),
                    )
                })?;
                let upper = spec.upper.ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "cross_section.winsorize requires upper".to_string(),
                    )
                })?;
                if !(0.0..=1.0).contains(&lower) || !(0.0..=1.0).contains(&upper) || lower > upper {
                    return Err(ComputedFieldError::InvalidParameter(
                        "cross_section.winsorize requires 0 <= lower <= upper <= 1".to_string(),
                    ));
                }
                let mut values = valid
                    .iter()
                    .map(|col| source[offset + *col])
                    .collect::<Vec<_>>();
                values.sort_by(f64::total_cmp);
                let low = quantile(&values, lower);
                let high = quantile(&values, upper);
                for col in valid {
                    output[offset + col] = source[offset + col].clamp(low, high);
                }
            }
            _ => return Err(ComputedFieldError::UnsupportedOperation(op.to_string())),
        }
    }
    Ok(output)
}
