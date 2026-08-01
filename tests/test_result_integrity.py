from __future__ import annotations

import pandas as pd
import pytest

from backtester.result_integrity import canonical_equity_summary


def test_canonical_equity_summary_rejects_empty_equity_curve() -> None:
    with pytest.raises(ValueError, match="equity curve is empty"):
        canonical_equity_summary(pd.DataFrame(), rust_total_return=0.0)


def test_canonical_equity_summary_rejects_missing_or_nonfinite_values() -> None:
    with pytest.raises(ValueError, match="Equity_value"):
        canonical_equity_summary(
            pd.DataFrame({"Time": ["2026-01-01"]}),
            rust_total_return=0.0,
        )

    with pytest.raises(ValueError, match="finite"):
        canonical_equity_summary(
            pd.DataFrame({"Equity_value": [100.0, float("nan")]}),
            rust_total_return=0.0,
        )


def test_canonical_equity_summary_rejects_nonpositive_values() -> None:
    with pytest.raises(ValueError, match="positive"):
        canonical_equity_summary(
            pd.DataFrame({"Equity_value": [100.0, 0.0]}),
            rust_total_return=-1.0,
        )


def test_canonical_equity_summary_returns_observed_values() -> None:
    assert canonical_equity_summary(
        pd.DataFrame({"Equity_value": [100.0, 110.0]}),
        rust_total_return=0.1,
    ) == {
        "start_equity": 100.0,
        "end_equity": 110.0,
        "total_return": pytest.approx(0.1),
    }


def test_canonical_equity_summary_requires_authoritative_rust_return() -> None:
    with pytest.raises(ValueError, match="Rust total_return"):
        canonical_equity_summary(
            pd.DataFrame({"Equity_value": [100.0, 110.0]}),
            rust_total_return=None,
        )
