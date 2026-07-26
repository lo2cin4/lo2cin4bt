use super::{field, scalar_or_field, ComputedFieldError, ComputedFieldSpec};
use std::collections::BTreeMap;

pub(crate) fn compute(
    op: &str,
    spec: &ComputedFieldSpec,
    fields: &BTreeMap<String, Vec<f64>>,
    len: usize,
) -> Result<Vec<f64>, ComputedFieldError> {
    let left = field(
        fields,
        spec.source
            .as_deref()
            .ok_or_else(|| ComputedFieldError::InvalidParameter(format!("{op} requires source")))?,
    )?;
    match op {
        "math.negate" => Ok(left.iter().map(|value| -value).collect()),
        "math.abs" => Ok(left.iter().map(|value| value.abs()).collect()),
        "math.clip" => {
            let lower = spec.lower.ok_or_else(|| {
                ComputedFieldError::InvalidParameter("math.clip requires lower".to_string())
            })?;
            let upper = spec.upper.ok_or_else(|| {
                ComputedFieldError::InvalidParameter("math.clip requires upper".to_string())
            })?;
            if lower > upper {
                return Err(ComputedFieldError::InvalidParameter(
                    "math.clip requires lower <= upper".to_string(),
                ));
            }
            Ok(left.iter().map(|value| value.clamp(lower, upper)).collect())
        }
        "math.add" | "math.subtract" | "math.multiply" | "math.divide" => {
            let right = scalar_or_field(fields, spec.right_source.as_deref(), spec.value, len)?;
            Ok(left
                .iter()
                .zip(right)
                .map(|(left, right)| {
                    if !left.is_finite() || !right.is_finite() {
                        return f64::NAN;
                    }
                    match op {
                        "math.add" => left + right,
                        "math.subtract" => left - right,
                        "math.multiply" => left * right,
                        "math.divide" if right != 0.0 => left / right,
                        "math.divide" => f64::NAN,
                        _ => unreachable!(),
                    }
                })
                .collect())
        }
        _ => Err(ComputedFieldError::UnsupportedOperation(op.to_string())),
    }
}
