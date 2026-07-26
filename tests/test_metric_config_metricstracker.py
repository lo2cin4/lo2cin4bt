from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from autorunner.MetricsRunner_autorunner import MetricsRunnerAutorunner
from metricstracker.MetricConfig_metricstracker import resolve_metric_config


pytestmark = pytest.mark.regression


def test_metric_config_defaults_to_252_for_traditional_assets() -> None:
    resolved = resolve_metric_config(
        {"enable_metrics_analysis": True},
        source_config={
            "data": {"provider": "yfinance", "calendar": "XNYS"},
            "universe": {"symbols": ["QQQ"]},
        },
    )

    assert resolved["time_unit"] == 252
    assert resolved["risk_free_rate"] == 0.04


def test_metric_config_defaults_to_365_for_crypto_assets() -> None:
    resolved = resolve_metric_config(
        {"enable_metrics_analysis": True},
        source_config={
            "data": {"provider": "binance", "calendar": "CRYPTO_24_7"},
            "universe": {"symbols": ["BTCUSDT"]},
        },
    )

    assert resolved["time_unit"] == 365
    assert resolved["risk_free_rate"] == 0.04


def test_metric_config_accepts_percent_or_decimal_risk_free_rate() -> None:
    assert resolve_metric_config({"risk_free_rate": 4})["risk_free_rate"] == 0.04
    assert resolve_metric_config({"risk_free_rate": "0.04"})["risk_free_rate"] == 0.04


def test_strategy_run_loader_exposes_metric_assumptions(tmp_path: Path) -> None:
    from autorunner.ConfigLoader_autorunner import ConfigLoader

    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "backtester/contracts/strategy/examples/strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json"
    config = json.loads(config_path.read_text(encoding="utf-8-sig"))
    config.pop("metricstracker", None)

    config_file = tmp_path / "strategy_run.json"
    config_file.write_text(json.dumps(config), encoding="utf-8")
    loaded = ConfigLoader().load_config(str(config_file))

    assert loaded is not None
    assert loaded.metricstracker_config["time_unit"] == 365
    assert loaded.metricstracker_config["risk_free_rate"] == 0.04


def test_metrics_runner_only_reads_validated_canonical_bundle(tmp_path: Path) -> None:
    from backtester.RuntimeContracts_backtester import build_canonical_result_bundle

    metric = tmp_path / "current_run_equity_curve.parquet"
    pd.DataFrame(
        {"Backtest_id": ["bt-1"], "Equity_value": [100.0], "Portfolio_return": [0.0]}
    ).to_parquet(metric)
    result_validation = {
        "schema_version": "result_validation_report.v1",
        "status": "valid",
        "result_hash": "a" * 64,
    }
    bundle = build_canonical_result_bundle(
        run_id="bt-1",
        candidates=[{"run_validation": {"result_validation": result_validation}}],
        table_paths={"equity_curve": str(metric)},
        artifact_paths=[str(metric)],
        result_table_kernel="rust_accounting_result_tables.v1",
    )
    metadata = tmp_path / "canonical_metadata.json"
    metadata.write_text(json.dumps(bundle), encoding="utf-8")
    runner = MetricsRunnerAutorunner()

    loaded = runner._load_validated_canonical_bundle({"exported_files": [str(metadata)]})
    assert loaded["bundle_paths"]["equity_curve"] == str(metric)


def test_metrics_runner_rejects_bare_parquet_without_canonical_bundle(tmp_path: Path) -> None:
    metric = tmp_path / "equity_curve.parquet"
    pd.DataFrame({"Equity_value": [100.0]}).to_parquet(metric)

    with pytest.raises(ValueError, match="validated canonical_result_bundle"):
        MetricsRunnerAutorunner()._load_validated_canonical_bundle(
            {"exported_files": [str(metric)]}
        )


def test_metrics_runner_resolves_auxiliary_benchmark_table(tmp_path: Path) -> None:
    close_path = tmp_path / "close.parquet"
    benchmark_path = tmp_path / "benchmark_close.parquet"
    pd.DataFrame({"VOO": [100.0]}).to_parquet(close_path)
    pd.DataFrame({"SPY": [101.0]}).to_parquet(benchmark_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "tables": {
                    "close": {"path": str(close_path), "columns": ["VOO"]},
                    "benchmark_close": {
                        "path": str(benchmark_path),
                        "columns": ["SPY"],
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    path, symbol = MetricsRunnerAutorunner._benchmark_context(
        {
            "market_data_bundle_manifest": str(manifest_path),
            "benchmark_symbol": "SPY",
        }
    )

    assert path == str(benchmark_path)
    assert symbol == "SPY"
