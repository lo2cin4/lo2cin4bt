from __future__ import annotations

import pandas as pd
import pytest

from statanalyser.AutocorrelationTest_statanalyser import AutocorrelationTest
from statanalyser.Base_statanalyser import BaseStatAnalyser
from statanalyser.StationarityTest_statanalyser import StationarityTest


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Time": pd.date_range("2026-01-01", periods=6, freq="D"),
            "predictor": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "returns": [0.0, 0.01, -0.01, 0.02, -0.02, 0.01],
        }
    )


def test_invalid_autocorrelation_lags_do_not_fall_back_to_frequency_default() -> None:
    analyser = AutocorrelationTest(
        _frame(),
        "predictor",
        "returns",
        bar_spec={
            "aggregation": "time",
            "step": 1,
            "unit": "day",
            "price_type": "last",
            "alignment": "session_open",
        },
        analysis_config={"lags": ["invalid"]},
    )

    with pytest.raises(ValueError, match="lags"):
        analyser.analyze()


def test_relative_diff_requires_a_canonical_rust_return_column() -> None:
    with pytest.raises(ValueError, match="retired from Python"):
        BaseStatAnalyser._normalize_diff_mode("relative")  # noqa: SLF001


def test_failed_stationarity_tests_are_unavailable_not_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_test(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("insufficient statistical input")

    monkeypatch.setattr(
        "statanalyser.StationarityTest_statanalyser.adfuller",
        fail_test,
    )
    monkeypatch.setattr(
        "statanalyser.StationarityTest_statanalyser.kpss",
        fail_test,
    )

    result = StationarityTest(_frame(), "predictor", "returns").analyze()

    assert result["predictor"]["adf_stationary"] is None
    assert result["predictor"]["kpss_stationary"] is None
    assert "RuntimeError: insufficient statistical input" in result["predictor"]["adf_error"]
    assert "RuntimeError: insufficient statistical input" in result["predictor"]["kpss_error"]
    assert result["return"]["adf_stationary"] is None
    assert result["return"]["kpss_stationary"] is None
    assert "RuntimeError: insufficient statistical input" in result["return"]["adf_error"]
    assert "RuntimeError: insufficient statistical input" in result["return"]["kpss_error"]
