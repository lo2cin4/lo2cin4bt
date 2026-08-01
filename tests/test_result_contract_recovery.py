import json
from pathlib import Path

import pandas as pd
import pytest

from app.api.metrics_contract_payload import MetricsContractPayloadService
from app.api.payloads import AppPayloadService
from app.api.shared_chart_series import SharedChartSeriesStore
from app.runtime.registry import AppRegistry


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_metrics_parquet_uses_explicit_external_benchmark(tmp_path: Path) -> None:
    bridge = pytest.importorskip("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    source_path = tmp_path / "portfolio_equity.parquet"
    benchmark_path = tmp_path / "benchmark_close.parquet"
    pd.DataFrame(
        {
            "Time": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Session_label": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Backtest_id": ["benchmark_probe:single_backtest:fixed"] * 3,
            "Equity_value": [100.0, 120.0, 110.0],
        }
    ).to_parquet(source_path, index=False)
    pd.DataFrame(
        {
            "Time": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "QQQ": [50.0, 51.0, 52.0],
        }
    ).to_parquet(benchmark_path, index=False)

    summary = bridge.run_metrics_parquet_via_cli(
        {
            "parquet_path": str(source_path),
            "benchmark_parquet_path": str(benchmark_path),
            "benchmark_symbol": "QQQ",
            "time_unit": 252,
            "risk_free_rate": 0.0,
        },
        timeout=120,
    )

    metric = summary["metrics"][0]
    enriched = summary["enriched_rows"]
    assert metric["BAH_Total_return"] == pytest.approx(0.04)
    assert metric["BAH_Total_return"] != metric["Total_return"]
    assert [row["BAH_Equity"] for row in enriched] == pytest.approx(
        [100.0, 102.0, 104.0]
    )


def _portfolio_contract_run(
    tmp_path: Path,
) -> tuple[MetricsContractPayloadService, AppPayloadService, AppRegistry, str]:
    registry = AppRegistry(tmp_path)
    run_id = "portfolio-contract-run"
    paths = registry.build_run_paths(run_id)
    result_hash = "a" * 64
    canonical_path = paths["snapshot_dir"] / "canonical.json"
    _write_json(
        canonical_path,
        {
            "schema_version": "canonical_result_bundle.v1",
            "validation": {"status": "valid"},
            "result_hashes": [result_hash],
        },
    )
    series = []
    metadata = []
    matrix_rows = []
    trade_rows = []
    for index in range(6):
        backtest_id = f"result_contract:parameter_matrix:index_{index}"
        series.append(
            {
                "series_id": backtest_id,
                "label": backtest_id,
                "x": ["2024-01-01", "2024-01-02"],
                "y": [100.0, 101.0 + index],
            }
        )
        metadata.append(
            {
                "Backtest_id": backtest_id,
                "Total_return": 0.01 + index / 100,
                "Sharpe": float(index),
                "Max_drawdown": -0.1,
                "BAH_Total_return": 0.02,
                "Annualization": {
                    "schema_version": "metrics_annualization.v1",
                    "basis": "session_close_projection",
                    "projection_policy": "last_accepted_equity_per_session",
                    "periods_per_year": 252,
                    "risk_free_rate_annual": 0.0,
                },
                "Projected_session_count": 2,
                "Projected_return_interval_count": 1,
            }
        )
        matrix_rows.append(
            {
                "backtest_id": backtest_id,
                "strategy_id": backtest_id,
                "label": backtest_id,
                "result_type": "portfolio",
                "result_materialization": "full",
                "semantic_combo": {"window": index + 1, "threshold": 10 + index},
                "semantic_fields": ["window", "threshold"],
                "total_return": 0.01 + index / 100,
                "sharpe": float(index),
                "max_drawdown": -0.1,
                "exposure_time": 100.0,
                "rebalance_count": index + 1,
                "trade_count": index + 1,
                "avg_turnover": 0.1,
                "avg_gross_exposure": 1.0,
            }
        )
        trade_rows.append(
            {
                "Backtest_id": backtest_id,
                "Time": "2024-01-02",
                "Asset": "QQQ",
                "Action": "buy",
                "Before_weight": 0.0,
                "Target_weight": 1.0,
                "Trade_delta": 1.0,
            }
        )
    series.append(
        {
            "series_id": "benchmark",
            "label": "QQQ Buy & Hold",
            "x": ["2024-01-01", "2024-01-02"],
            "y": [100.0, 102.0],
        }
    )
    plot_path = paths["chart_payload_dir"] / "asset_curve_compare.json"
    plot_payload = {
            "schema_version": "plot_bundle.v1",
            "contract_id": "lo2cin4bt.plot_bundle.v1",
            "run_id": run_id,
            "series": series,
            "source_hashes": [result_hash],
            "artifact_source_refs": [str(canonical_path)],
            "generated_at": "2026-07-12T00:00:00Z",
        }
    store = SharedChartSeriesStore(registry)
    store.write_json(plot_path, store.compact_plot_bundle(run_id, plot_payload))
    metadata_path = paths["snapshot_dir"] / "metrics_metadata.json"
    matrix_path = paths["snapshot_dir"] / "portfolio_matrix_summary.json"
    trades_path = paths["snapshot_dir"] / "rebalance_trades.parquet"
    metrics_path = paths["snapshot_dir"] / "metrics.parquet"
    _write_json(metadata_path, metadata)
    _write_json(
        matrix_path,
        {
            "schema_version": "portfolio_matrix_summary.v1",
            "variant_count": 6,
            "row_count": 6,
            "retained_result_count": 6,
            "compact_result_count": 0,
            "coverage": "all_candidates",
            "rows": matrix_rows,
        },
    )
    pd.DataFrame(trade_rows).to_parquet(trades_path, index=False)
    pd.DataFrame({"placeholder": [1]}).to_parquet(metrics_path, index=False)
    registry.write_artifact_manifest(
        run_id,
        {
            "artifacts": [
                {"artifact_type": "metricstracker_metadata", "path": str(metadata_path)},
                {"artifact_type": "metricstracker_parquet", "path": str(metrics_path)},
                {"artifact_type": "portfolio_matrix_summary_json", "path": str(matrix_path)},
                {
                    "artifact_type": "portfolio_rebalance_trades_parquet",
                    "path": str(trades_path),
                },
            ]
        },
    )
    registry.write_snapshot_file(
        run_id,
        "execution_plan.json",
        {
            "param_axes": [
                {"name": "window"},
                {"name": "threshold"},
            ]
        },
    )
    return (
        MetricsContractPayloadService(registry),
        AppPayloadService(tmp_path, registry),
        registry,
        run_id,
    )


def test_portfolio_metrics_contract_restores_rich_top_three_payload(
    tmp_path: Path,
) -> None:
    metrics_service, _payloads, _registry, run_id = _portfolio_contract_run(tmp_path)

    payload = metrics_service.load(run_id)

    assert payload["result_type"] == "portfolio"
    assert payload["default_category"] == "top_3_sharpe"
    assert len(payload["categories"]["top_3_sharpe"]) == 3
    assert len(payload["portfolio"]["runs"]) == 6
    assert payload["portfolio"]["runs"][0]["allocation_change_rows"]
    assert payload["benchmark_series"]["x"] == ["2024-01-01", "2024-01-02"]


def test_parameter_matrix_uses_complete_canonical_summary(
    tmp_path: Path,
) -> None:
    metrics_service, payloads, _registry, run_id = _portfolio_contract_run(tmp_path)
    metrics_service.ensure(run_id)

    payload = payloads.build_parameter_matrix_payload(run_id, force=True)

    assert payload["result_type"] == "portfolio"
    assert len(payload["rows"]) == 6
    assert payload["matrix_summary"]["coverage"] == "all_candidates"
    assert payload["axis_values"]["window"] == [1, 2, 3, 4, 5, 6]


def test_rust_detail_contract_projects_rich_portfolio_fields() -> None:
    bridge = pytest.importorskip("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = bridge.run_backtest_detail_bundle_via_cli(
        {
            "run_id": "run-portfolio",
            "backtest_id": "result_contract:parameter_matrix:detail",
            "label": "Candidate A",
            "asset": "PORTFOLIO_NAV",
            "result_type": "portfolio",
            "time": ["2024-01-02", "2024-01-31", "2024-02-01", "2024-02-29"],
            "session_labels": [
                "2024-01-02",
                "2024-01-31",
                "2024-02-01",
                "2024-02-29",
            ],
            "open": [100.0, 110.0, 105.0, 120.0],
            "high": [100.0, 110.0, 105.0, 120.0],
            "low": [100.0, 110.0, 105.0, 120.0],
            "close": [100.0, 110.0, 105.0, 120.0],
            "equity": [100.0, 110.0, 105.0, 120.0],
            "benchmark_equity": [100.0, 102.0, 101.0, 106.0],
            "trade_action": [],
            "portfolio_returns": [0.0, 0.1, -0.045454545, 0.142857143],
            "turnover": [1.0, 0.0, 2.0, 0.0],
            "trade_cost": [0.1, 0.0, 0.2, 0.0],
            "gross_exposure": [1.0, 1.0, 1.0, 1.0],
            "contribution_series": {
                "SPY": [0.0, 0.1, 0.0, 0.0],
                "GLD": [0.0, 0.0, -0.045454545, 0.142857143],
            },
            "weight_series": {
                "SPY": [1.0, 1.0, 0.0, 0.0],
                "GLD": [0.0, 0.0, 1.0, 1.0],
            },
            "holding_rows": [
                {"Time": "2024-01-02", "Asset": "SPY", "Target_weight": 1.0},
                {"Time": "2024-02-01", "Asset": "GLD", "Target_weight": 1.0},
            ],
            "rebalance_rows": [
                {"Time": "2024-01-02", "Equity_value": 100.0, "Turnover": 1.0},
                {"Time": "2024-02-01", "Equity_value": 105.0, "Turnover": 2.0},
                {"Time": "2024-02-29", "Equity_value": 120.0, "Turnover": 2.0},
            ],
            "allocation_change_rows": [
                {"Time": "2024-01-02", "Asset": "SPY", "Action": "buy"},
                {"Time": "2024-02-01", "Asset": "SPY", "Action": "exit"},
                {"Time": "2024-02-01", "Asset": "GLD", "Action": "buy"},
            ],
            "strategy_summary": {"strategy_profile_id": "multi_leg_event_portfolio"},
            "data_quality": {"status": "valid"},
            "risk_gate_rows": [],
            "risk_gate_summary": {"event_count": 0, "enabled": False},
            "ohlc_by_asset": {},
            "benchmark_label": "SPY Buy & Hold",
            "metrics_matrix": {
                "total_return": 0.2,
                "cagr": 0.2,
                "sharpe": 1.0,
                "max_drawdown": -0.045454545,
                "trade_count": 2,
                "avg_holdings": 1.0,
                "avg_gross_exposure": 1.0,
                "avg_turnover": 0.75,
                "annualized_std": 0.25,
                "sortino": 1.5,
            },
            "source_hashes": ["a" * 64],
            "artifact_source_refs": ["canonical.json"],
            "generated_at": "2026-07-12T00:00:00Z",
        },
        timeout=120,
    )

    assert payload["result_type"] == "portfolio"
    assert payload["holding_rows"][0]["asset"] == "SPY"
    assert payload["allocation_change_rows"]
    assert payload["rebalance_rows"]
    assert payload["monthly_return_rows"]
    assert payload["yearly_return_rows"]
    assert payload["drawdown_series"]
    assert payload["turnover_distribution"]
    assert payload["asset_contribution_rows"]
    assert payload["asset_contribution_summary"]
    assert payload["risk_diagnostics"]
    assert payload["closed_trade_rows"]
    assert "drawdown" in payload["drawdown_series"][0]
    assert payload["metrics_matrix"]["annualized_std"] > 0
    assert payload["metrics_matrix"]["var_95"] <= payload["metrics_matrix"]["best_month"]
