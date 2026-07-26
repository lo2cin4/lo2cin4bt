use super::{field, scalar_or_field, ComputedFieldError, ComputedFieldSpec};
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
    match op {
        "transform.lag" => {
            let period = spec.required_period()?;
            let mut output = vec![f64::NAN; rows * cols];
            for row in period..rows {
                for col in 0..cols {
                    output[row * cols + col] = source[(row - period) * cols + col];
                }
            }
            Ok(output)
        }
        "transform.fill_missing" => {
            let fill = spec.value.ok_or_else(|| {
                ComputedFieldError::InvalidParameter(
                    "transform.fill_missing requires value".to_string(),
                )
            })?;
            Ok(source
                .iter()
                .map(|value| if value.is_finite() { *value } else { fill })
                .collect())
        }
        "transform.where" => {
            let right = scalar_or_field(
                fields,
                spec.right_source.as_deref(),
                spec.value,
                rows * cols,
            )?;
            let when_true = scalar_or_field(
                fields,
                spec.true_source.as_deref(),
                spec.true_value,
                rows * cols,
            )?;
            let when_false = scalar_or_field(
                fields,
                spec.false_source.as_deref(),
                spec.false_value,
                rows * cols,
            )?;
            let comparator = spec
                .condition
                .as_deref()
                .ok_or_else(|| {
                    ComputedFieldError::InvalidParameter(
                        "transform.where requires condition".to_string(),
                    )
                })?
                .trim()
                .to_lowercase();
            source
                .iter()
                .zip(right)
                .zip(when_true)
                .zip(when_false)
                .map(|(((left, right), when_true), when_false)| {
                    if !left.is_finite() || !right.is_finite() {
                        return Ok(f64::NAN);
                    }
                    let selected = match comparator.as_str() {
                        "gt" => left > &right,
                        "ge" => left >= &right,
                        "lt" => left < &right,
                        "le" => left <= &right,
                        "eq" => left == &right,
                        "ne" => left != &right,
                        _ => {
                            return Err(ComputedFieldError::InvalidParameter(format!(
                                "transform.where unsupported condition={comparator}"
                            )))
                        }
                    };
                    Ok(if selected { when_true } else { when_false })
                })
                .collect()
        }
        _ => Err(ComputedFieldError::UnsupportedOperation(op.to_string())),
    }
}
