use std::collections::BTreeMap;

use serde_json::Value;

pub const FIXED_PARAMETER_SUFFIX: &str = "fixed";

pub fn validate_base_strategy_id(value: &str) -> Result<&str, String> {
    validate_component(value, "base_strategy_id")
}

pub fn parse_candidate_id(value: &str) -> Result<(&str, &str, &str), String> {
    let parts = value.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(
            "candidate_id must use base_strategy_id:workflow_id:parameter_suffix".to_string(),
        );
    }
    let base_strategy_id = validate_component(parts[0], "base_strategy_id")?;
    let workflow_id = validate_component(parts[1], "workflow_id")?;
    let parameter_suffix = validate_component(parts[2], "parameter_suffix")?;
    if !matches!(
        workflow_id,
        "single_backtest"
            | "parameter_matrix"
            | "walk_forward_analysis"
            | "rolling_validation"
            | "statanalyser"
    ) {
        return Err(format!(
            "candidate_id contains unsupported workflow_id: {workflow_id}"
        ));
    }
    Ok((base_strategy_id, workflow_id, parameter_suffix))
}

pub fn canonical_parameter_suffix(parameters: &BTreeMap<String, Value>) -> Result<String, String> {
    if parameters.is_empty() {
        return Ok(FIXED_PARAMETER_SUFFIX.to_string());
    }
    let mut parts = Vec::with_capacity(parameters.len());
    for (key, value) in parameters {
        parts.push(format!(
            "{}_{}",
            slug_component(&Value::String(key.clone()))?,
            slug_component(value)?
        ));
    }
    let suffix = parts.join("_");
    validate_component(&suffix, "parameter_suffix")?;
    Ok(suffix)
}

fn validate_component<'a>(value: &'a str, field: &str) -> Result<&'a str, String> {
    let text = value.trim();
    let mut chars = text.chars();
    let first = chars
        .next()
        .ok_or_else(|| format!("{field} must not be empty"))?;
    if !first.is_ascii_alphanumeric()
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(format!(
            "{field} must start with an alphanumeric character and contain only \
letters, numbers, '.', '_' or '-'"
        ));
    }
    Ok(text)
}

fn slug_component(value: &Value) -> Result<String, String> {
    let text = match value {
        Value::Bool(flag) => flag.to_string(),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).map_err(|error| error.to_string())?
        }
        Value::String(text) => text.trim().to_lowercase(),
        Value::Number(number) => number.to_string().to_lowercase(),
        Value::Null => "none".to_string(),
    };
    let replaced = text.replace('-', "m").replace('.', "p");
    let mut slug = String::with_capacity(replaced.len());
    let mut previous_underscore = false;
    for ch in replaced.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            previous_underscore = false;
        } else if !previous_underscore && !slug.is_empty() {
            slug.push('_');
            previous_underscore = true;
        }
    }
    while slug.ends_with('_') {
        slug.pop();
    }
    if slug.is_empty() {
        return Err("parameter values must produce a non-empty canonical suffix".to_string());
    }
    Ok(slug)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn candidate_identity_requires_exactly_three_canonical_segments() {
        assert_eq!(
            parse_candidate_id("alpha:single_backtest:fixed").unwrap(),
            ("alpha", "single_backtest", "fixed")
        );
        assert!(parse_candidate_id("alpha").is_err());
        assert!(parse_candidate_id("alpha:single_backtest").is_err());
        assert!(parse_candidate_id("alpha:unknown:fixed").is_err());
        assert!(parse_candidate_id("alpha:single_backtest:bad suffix").is_err());
    }

    #[test]
    fn parameter_suffix_is_stable_and_fixed_when_empty() {
        assert_eq!(
            canonical_parameter_suffix(&BTreeMap::new()).unwrap(),
            "fixed"
        );
        let params = BTreeMap::from([
            ("long_ma".to_string(), json!(200)),
            ("short_ma".to_string(), json!(10)),
        ]);
        assert_eq!(
            canonical_parameter_suffix(&params).unwrap(),
            "long_ma_200_short_ma_10"
        );
    }
}
