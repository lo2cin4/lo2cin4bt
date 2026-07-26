from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backtester.UnifiedBacktestRunner_backtester import UnifiedBacktestRunnerBacktester
from factorhandler.FactorArtifactExporter_factorhandler import FactorArtifactExporter
from factorhandler.FactorHandler_factorhandler import FactorHandlerResult
from statanalyser.ReportGenerator_statanalyser import ReportGenerator


def _long_text(prefix: str) -> str:
    return prefix + "_" + "_".join(f"segment_{idx:03d}" for idx in range(40))


def test_factor_artifact_exporter_bounds_long_run_id_and_factor_name(tmp_path: Path) -> None:
    dates = pd.date_range("2024-01-01", periods=2, freq="D")
    frame = pd.DataFrame({"AAA": [1.0, 2.0]}, index=dates)
    factor_name = _long_text("composite_factor_score")
    result = FactorHandlerResult(
        factor_frame={factor_name: frame},
        clean_factor_frame={},
        factor_score_frame={factor_name: frame},
        factor_quality_report={},
        point_in_time_audit={},
        cache_report={},
    )

    paths = FactorArtifactExporter(result, tmp_path, run_id=_long_text("factor_probe")).export()

    assert paths
    for raw_path in paths:
        assert Path(raw_path).exists()
        assert len(Path(raw_path).name) <= 150
    assert any(path.endswith("_factorhandler-reports.json") for path in paths)


def test_statanalyser_save_data_bounds_long_filename(tmp_path: Path) -> None:
    generator = ReportGenerator(output_dir=str(tmp_path))

    generator.save_data(pd.DataFrame({"value": [1]}), filename=_long_text("processed_data"))
    generator.save_report({}, filename=f"{_long_text('stats_report')}.txt")

    files = list(tmp_path.glob("*.csv"))
    assert len(files) == 1
    assert len(files[0].name) <= 110
    reports = list(tmp_path.glob("*.txt"))
    assert len(reports) == 1
    assert len(reports[0].name) <= 110


def test_statanalyser_does_not_invent_missing_statistics(tmp_path: Path) -> None:
    generator = ReportGenerator(output_dir=str(tmp_path))

    recommendations = generator.generate_strategy_recommendations(
        {
            "CorrelationTest_probe": {"correlation_results": {}},
            "StationarityTest_probe": {"predictor": {}},
            "AutocorrelationTest_probe": {},
            "DistributionTest_probe": {"is_normal": False},
            "SeasonalAnalysis_probe": {"has_seasonal": True},
        }
    )

    assert any("相關性分析資料不完整" in item for item in recommendations)
    assert any("平穩性分析資料不完整" in item for item in recommendations)
    assert any("分佈分析資料不完整" in item for item in recommendations)
    assert any("季節性分析資料不完整" in item for item in recommendations)
    assert all("ADF p=1.0000" not in item for item in recommendations)


def test_unified_rust_direct_bundle_metadata_bounds_long_run_id(tmp_path: Path) -> None:
    output_dir = tmp_path / "portfolio"
    equity_path = output_dir / "equity_curve.parquet"
    risk_gate_events_path = output_dir / "risk_gate_events.parquet"
    output_dir.mkdir()
    pd.DataFrame({"Time": pd.date_range("2024-01-01", periods=1), "Equity_value": [100.0]}).to_parquet(
        equity_path,
        index=False,
    )
    pd.DataFrame(columns=["Time", "gate"]).to_parquet(
        risk_gate_events_path,
        index=False,
    )

    runner = UnifiedBacktestRunnerBacktester()
    paths = runner._export_rust_direct_signal_bundle_metadata(
            artifact_bundle={
                "run_id": _long_text("portfolio_matrix"),
                "bundle_paths": {
                    "equity_curve": str(equity_path),
                    "risk_gate_events": str(risk_gate_events_path),
                },
            },
        items=[
            {
                "candidate_id": "candidate_a",
                "final_equity": 100.0,
                "total_return": 0.0,
                "cagr": 0.0,
                "sharpe": 0.0,
                "max_drawdown": 0.0,
                "active_rebalances": 0,
                "average_turnover": 0.0,
                "average_gross_exposure": 0.0,
                "days": 1,
                "result_validation": {
                    "schema_version": "result_validation_report.v1",
                    "status": "valid",
                    "result_hash": "0" * 64,
                    "errors": [],
                },
            }
        ],
        variants=[{"config": {"strategy_id": "candidate_a"}}],
        cost_rate=0.0,
    )

    metadata_files = [Path(path) for path in paths if path.endswith("_portfolio_matrix_metadata.json")]
    validation_files = [
        Path(path) for path in paths if path.endswith("_portfolio_matrix_run_validation_report.json")
    ]
    assert len(metadata_files) == 1
    assert len(validation_files) == 1
    assert len(metadata_files[0].name) <= 130
    assert len(validation_files[0].name) <= 160
    payload = json.loads(metadata_files[0].read_text(encoding="utf-8"))
    assert payload["run_id"].startswith("portfolio_matrix")
