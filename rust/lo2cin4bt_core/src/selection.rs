use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct RankSelectionInput {
    pub rows: usize,
    pub cols: usize,
    pub eligible: Vec<bool>,
    pub score: Vec<f64>,
    #[serde(default)]
    pub ascending: bool,
    pub top_n: usize,
    #[serde(default)]
    pub short_bottom_n: usize,
    #[serde(default = "default_long_gross_exposure")]
    pub long_gross_exposure: f64,
    #[serde(default)]
    pub short_gross_exposure: f64,
    #[serde(default = "default_position_limit")]
    pub position_limit: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RankSelectionSummary {
    pub rows: usize,
    pub cols: usize,
    pub target_weights: Vec<f64>,
    pub selected_indices: Vec<Vec<usize>>,
    pub ranked_indices: Vec<Vec<usize>>,
}

#[derive(Debug, Error)]
pub enum RankSelectionError {
    #[error("rank selection rows and cols must be positive")]
    InvalidShape,
    #[error("eligible and score arrays must match rows * cols")]
    InvalidArrayLength,
    #[error("position_limit must be finite and non-negative")]
    InvalidPositionLimit,
    #[error("long and short gross exposure must be finite and non-negative")]
    InvalidGrossExposure,
}

fn default_position_limit() -> f64 {
    1.0
}

fn default_long_gross_exposure() -> f64 {
    1.0
}

pub fn run_rank_selection(
    input: RankSelectionInput,
) -> Result<RankSelectionSummary, RankSelectionError> {
    validate_rank_selection_input(&input)?;
    let mut target_weights = vec![0.0; input.rows * input.cols];
    let mut selected_indices = Vec::with_capacity(input.rows);
    let mut ranked_indices = Vec::with_capacity(input.rows);
    let top_n = input.top_n.min(input.cols);

    for row in 0..input.rows {
        let start = row * input.cols;
        let mut valid_indices = Vec::new();
        for col in 0..input.cols {
            let idx = start + col;
            let score = input.score[idx];
            if input.eligible[idx] && score.is_finite() {
                valid_indices.push(col);
            }
        }
        valid_indices.sort_by(|left, right| {
            let left_score = input.score[start + *left];
            let right_score = input.score[start + *right];
            left_score
                .partial_cmp(&right_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.cmp(right))
        });
        if !input.ascending {
            valid_indices.reverse();
        }
        let long_selected = valid_indices
            .iter()
            .copied()
            .take(top_n)
            .collect::<Vec<_>>();
        let short_selected = valid_indices
            .iter()
            .rev()
            .copied()
            .filter(|col| !long_selected.contains(col))
            .take(
                input
                    .short_bottom_n
                    .min(input.cols.saturating_sub(long_selected.len())),
            )
            .collect::<Vec<_>>();
        if !long_selected.is_empty() {
            let per_asset_weight =
                (input.long_gross_exposure / long_selected.len() as f64).min(input.position_limit);
            for col in &long_selected {
                target_weights[start + *col] = per_asset_weight;
            }
        }
        if !short_selected.is_empty() {
            let per_asset_weight = (input.short_gross_exposure / short_selected.len() as f64)
                .min(input.position_limit);
            for col in &short_selected {
                target_weights[start + *col] = -per_asset_weight;
            }
        }
        let selected = long_selected
            .into_iter()
            .chain(short_selected)
            .collect::<Vec<_>>();
        selected_indices.push(selected);
        ranked_indices.push(valid_indices);
    }

    Ok(RankSelectionSummary {
        rows: input.rows,
        cols: input.cols,
        target_weights,
        selected_indices,
        ranked_indices,
    })
}

fn validate_rank_selection_input(input: &RankSelectionInput) -> Result<(), RankSelectionError> {
    if input.rows == 0 || input.cols == 0 {
        return Err(RankSelectionError::InvalidShape);
    }
    let expected_len = input.rows * input.cols;
    if input.eligible.len() != expected_len || input.score.len() != expected_len {
        return Err(RankSelectionError::InvalidArrayLength);
    }
    if !input.position_limit.is_finite() || input.position_limit < 0.0 {
        return Err(RankSelectionError::InvalidPositionLimit);
    }
    if !input.long_gross_exposure.is_finite()
        || input.long_gross_exposure < 0.0
        || !input.short_gross_exposure.is_finite()
        || input.short_gross_exposure < 0.0
    {
        return Err(RankSelectionError::InvalidGrossExposure);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_selection_matches_daily_rank_desc_tie_order() {
        let summary = run_rank_selection(RankSelectionInput {
            rows: 2,
            cols: 3,
            eligible: vec![true, true, true, true, false, true],
            score: vec![2.0, 3.0, 3.0, f64::NAN, 9.0, 1.0],
            ascending: false,
            top_n: 2,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 0.4,
        })
        .expect("rank selection should run");

        assert_eq!(summary.ranked_indices[0], vec![2, 1, 0]);
        assert_eq!(summary.selected_indices[0], vec![2, 1]);
        assert_eq!(summary.target_weights[0..3], [0.0, 0.4, 0.4]);
        assert_eq!(summary.ranked_indices[1], vec![2]);
        assert_eq!(summary.selected_indices[1], vec![2]);
        assert_eq!(summary.target_weights[3..6], [0.0, 0.0, 0.4]);
    }

    #[test]
    fn rank_selection_supports_ascending_order() {
        let summary = run_rank_selection(RankSelectionInput {
            rows: 1,
            cols: 3,
            eligible: vec![true, true, true],
            score: vec![2.0, 3.0, 1.0],
            ascending: true,
            top_n: 2,
            short_bottom_n: 0,
            long_gross_exposure: 1.0,
            short_gross_exposure: 0.0,
            position_limit: 1.0,
        })
        .expect("rank selection should run");

        assert_eq!(summary.ranked_indices[0], vec![2, 0, 1]);
        assert_eq!(summary.selected_indices[0], vec![2, 0]);
        assert_eq!(summary.target_weights, vec![0.5, 0.0, 0.5]);
    }

    #[test]
    fn rank_selection_supports_disjoint_long_and_short_tails() {
        let summary = run_rank_selection(RankSelectionInput {
            rows: 1,
            cols: 6,
            eligible: vec![true; 6],
            score: vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
            ascending: false,
            top_n: 2,
            short_bottom_n: 2,
            long_gross_exposure: 0.5,
            short_gross_exposure: 0.5,
            position_limit: 0.25,
        })
        .expect("long-short rank selection should run");

        assert_eq!(summary.selected_indices[0], vec![0, 1, 5, 4]);
        assert_eq!(
            summary.target_weights,
            vec![0.25, 0.25, 0.0, 0.0, -0.25, -0.25]
        );
    }
}
