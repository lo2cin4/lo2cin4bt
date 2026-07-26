from pathlib import Path
import json
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from metricstracker.MetricsArtifactWriter_metricstracker import export_metrics_artifacts
from autorunner.MetricsRunner_autorunner import MetricsRunnerAutorunner


def _assert_metric_close(actual, expected) -> None:
    if actual is None and pd.isna(expected):
        return
    assert actual == pytest.approx(expected, rel=1e-9, abs=1e-9, nan_ok=True)


def _sample_metric_frame() -> pd.DataFrame:
    rows = []
    for backtest_id, closes in [("b2", [100, 110, 105, 120]), ("a1", [100, 95, 98, 102])]:
        equity = [100.0]
        actions = [0.0, 1.0, 0.0, 4.0]
        trade_returns = [None, None, None, (closes[-1] / closes[1]) - 1.0]
        positions = [0.0, 1.0, 1.0, 0.0]
        for idx, close in enumerate(closes):
            if idx > 0:
                exposure = 1.0 if positions[idx - 1] != 0.0 else 0.0
                equity.append(equity[-1] * (1.0 + ((close / closes[idx - 1]) - 1.0) * exposure))
            rows.append(
                {
                    "Time": pd.Timestamp("2024-01-01") + pd.Timedelta(days=idx),
                    "Backtest_id": backtest_id,
                    "Equity_value": equity[idx],
                    "Close": float(close),
                    "Trade_action": actions[idx],
                    "Trade_return": trade_returns[idx],
                    "Position_size": positions[idx],
                }
            )
    return pd.DataFrame(rows)


def test_rust_metrics_parquet_service_matches_existing_rust_batch_path(tmp_path: Path) -> None:
    bridge = pytest.importorskip("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    source = _sample_metric_frame()
    parquet_path = tmp_path / "metrics_source.parquet"
    source.to_parquet(parquet_path, index=False)

    parquet_summary = bridge.run_metrics_parquet_via_cli(
        {
            "parquet_path": str(parquet_path),
            "time_unit": 252,
            "risk_free_rate": 0.02,
        },
        timeout=120,
    )

    assert parquet_summary["row_count"] == 2
    assert len(parquet_summary["metrics"]) == 2
    assert {row["Backtest_id"] for row in parquet_summary["metrics"]} == {"a1", "b2"}
    assert len(parquet_summary["enriched_rows"]) == len(source)


def test_metrics_exporter_can_export_directly_from_parquet(tmp_path: Path) -> None:
    bridge = pytest.importorskip("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    source = _sample_metric_frame()
    source_dir = tmp_path / "backtester"
    source_dir.mkdir()
    parquet_path = source_dir / "sample.parquet"
    source.to_parquet(parquet_path, index=False)

    export_metrics_artifacts(str(parquet_path), time_unit=252, risk_free_rate=0.02)

    out_dir = tmp_path / "metricstracker"
    metadata_path = out_dir / "sample_metadata.json"
    metrics_path = out_dir / "sample_metrics.parquet"
    assert metadata_path.exists()
    assert metrics_path.exists()

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert {row["Backtest_id"] for row in metadata} == {"a1", "b2"}
    assert {row["Metrics_kernel"] for row in metadata} == {"rust_metrics_parquet_v1"}

    metrics_frame = pd.read_parquet(metrics_path)
    assert {"Time", "Equity_value", "BAH_Equity", "BAH_Return", "Drawdown", "Backtest_id"}.issubset(
        metrics_frame.columns
    )
    assert len(metrics_frame) == len(source)


def test_metrics_exporter_bounds_long_output_filenames(tmp_path: Path) -> None:
    bridge = pytest.importorskip("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    source = _sample_metric_frame()
    source_dir = tmp_path / "backtester"
    source_dir.mkdir()
    long_stem = (
        "backtest_20260630_QQQ_PRICE_ma-cross_matrix_portfolio-equity_"
        "qqq_daily_sma_cross_yfinance_example_equity_curve_with_many_parameters_"
        "short_ma_20_30_40_50_long_ma_100_150_200_250_300_variant"
    )
    parquet_path = source_dir / f"{long_stem}.parquet"
    source.to_parquet(parquet_path, index=False)

    export_metrics_artifacts(str(parquet_path), time_unit=252, risk_free_rate=0.02)

    out_dir = tmp_path / "metricstracker"
    metadata_files = list(out_dir.glob("*_metadata.json"))
    metrics_files = list(out_dir.glob("*_metrics.parquet"))
    assert len(metadata_files) == 1
    assert len(metrics_files) == 1
    assert len(metadata_files[0].name) <= 120
    assert len(metrics_files[0].name) <= 120
    assert long_stem not in metadata_files[0].name
    derived_path = MetricsRunnerAutorunner()._derive_output_path(str(parquet_path))
    assert Path(derived_path).name == metrics_files[0].name


def test_rust_metrics_parquet_rejects_incomplete_close_instead_of_using_equity(
    tmp_path: Path,
) -> None:
    bridge = pytest.importorskip("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    source = _sample_metric_frame()
    source.loc[2, "Close"] = float("nan")
    parquet_path = tmp_path / "invalid-close.parquet"
    source.to_parquet(parquet_path, index=False)

    with pytest.raises(Exception, match="Close"):
        bridge.run_metrics_parquet_via_cli(
            {
                "parquet_path": str(parquet_path),
                "time_unit": 252,
                "risk_free_rate": 0.02,
            },
            timeout=120,
        )
