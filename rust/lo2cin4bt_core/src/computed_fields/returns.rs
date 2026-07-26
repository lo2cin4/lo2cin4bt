use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ReturnSeriesError {
    #[error("return series requires at least one price")]
    EmptySeries,
    #[error("open and close price arrays must have equal non-zero length")]
    InvalidSessionLength,
    #[error("non-finite price in {field} at row {row}")]
    NonFinitePrice { field: &'static str, row: usize },
    #[error("price must be positive in {field} at row {row}")]
    NonPositivePrice { field: &'static str, row: usize },
}

#[derive(Clone, Debug, PartialEq)]
pub struct PeriodReturnSeries {
    pub simple: Vec<f64>,
    pub logarithmic: Vec<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionReturnSeries {
    pub overnight: Vec<f64>,
    pub intraday: Vec<f64>,
}

pub fn period_return_series(prices: &[f64]) -> Result<PeriodReturnSeries, ReturnSeriesError> {
    validate_prices(prices, "price")?;
    let mut simple = vec![0.0; prices.len()];
    let mut logarithmic = vec![0.0; prices.len()];
    for row in 1..prices.len() {
        simple[row] = simple_return(prices[row], prices[row - 1]);
        logarithmic[row] = logarithmic_return(prices[row], prices[row - 1]);
    }
    Ok(PeriodReturnSeries {
        simple,
        logarithmic,
    })
}

pub fn session_return_series(
    open: &[f64],
    close: &[f64],
) -> Result<SessionReturnSeries, ReturnSeriesError> {
    if open.is_empty() || open.len() != close.len() {
        return Err(ReturnSeriesError::InvalidSessionLength);
    }
    validate_prices(open, "open")?;
    validate_prices(close, "close")?;

    let mut overnight = vec![0.0; open.len()];
    let mut intraday = vec![0.0; open.len()];
    for row in 0..open.len() {
        if row > 0 {
            overnight[row] = simple_return(open[row], close[row - 1]);
        }
        intraday[row] = simple_return(close[row], open[row]);
    }
    Ok(SessionReturnSeries {
        overnight,
        intraday,
    })
}

fn validate_prices(prices: &[f64], field: &'static str) -> Result<(), ReturnSeriesError> {
    if prices.is_empty() {
        return Err(ReturnSeriesError::EmptySeries);
    }
    for (row, price) in prices.iter().enumerate() {
        if !price.is_finite() {
            return Err(ReturnSeriesError::NonFinitePrice { field, row });
        }
        if *price <= 0.0 {
            return Err(ReturnSeriesError::NonPositivePrice { field, row });
        }
    }
    Ok(())
}

fn simple_return(current: f64, previous: f64) -> f64 {
    current / previous - 1.0
}

fn logarithmic_return(current: f64, previous: f64) -> f64 {
    (current / previous).ln()
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn period_returns_match_simple_and_logarithmic_contracts() {
        let result = period_return_series(&[100.0, 110.0, 99.0]).expect("valid prices");

        assert_eq!(result.simple[0], 0.0);
        assert_abs_diff_eq!(result.simple[1], 0.1, epsilon = 1e-12);
        assert_abs_diff_eq!(result.simple[2], -0.1, epsilon = 1e-12);
        assert_abs_diff_eq!(result.logarithmic[1], 1.1_f64.ln(), epsilon = 1e-12);
        assert_abs_diff_eq!(result.logarithmic[2], 0.9_f64.ln(), epsilon = 1e-12);
    }

    #[test]
    fn session_returns_separate_overnight_and_intraday_movements() {
        let result = session_return_series(&[100.0, 108.0, 99.0], &[105.0, 110.0, 101.0])
            .expect("valid session prices");

        assert_eq!(result.overnight[0], 0.0);
        assert_abs_diff_eq!(result.overnight[1], 108.0 / 105.0 - 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(result.overnight[2], 99.0 / 110.0 - 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(result.intraday[0], 0.05, epsilon = 1e-12);
        assert_abs_diff_eq!(result.intraday[1], 110.0 / 108.0 - 1.0, epsilon = 1e-12);
        assert_abs_diff_eq!(result.intraday[2], 101.0 / 99.0 - 1.0, epsilon = 1e-12);
    }

    #[test]
    fn non_positive_prices_fail_closed() {
        assert_eq!(
            period_return_series(&[0.0, 10.0]),
            Err(ReturnSeriesError::NonPositivePrice {
                field: "price",
                row: 0
            })
        );
        assert_eq!(
            session_return_series(&[0.0, 10.0], &[5.0, 12.0]),
            Err(ReturnSeriesError::NonPositivePrice {
                field: "open",
                row: 0
            })
        );
    }

    #[test]
    fn non_finite_prices_fail_closed() {
        assert_eq!(
            period_return_series(&[100.0, f64::NAN]),
            Err(ReturnSeriesError::NonFinitePrice {
                field: "price",
                row: 1
            })
        );
    }
}
