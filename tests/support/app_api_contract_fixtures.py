"""Canonical App API fixtures that do not depend on ignored local run outputs."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.api.metrics_contract_payload import MetricsContractPayloadService
from app.api.payloads import AppPayloadService
from app.api.service import AppAPIService
from app.api.shared_chart_series import SharedChartSeriesStore
from app.runtime.module_identity import VALIDATION_WORKFLOW_CANONICAL
from app.runtime.registry import AppRegistry


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def build_portfolio_contract_run(
    root: Path,
) -> tuple[MetricsContractPayloadService, AppPayloadService, AppRegistry, str]:
    """Build a complete canonical metrics and Parameter Matrix artifact set."""

    registry = AppRegistry(root)
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
    strategy_path = paths["snapshot_dir"] / "strategy_contract.json"
    _write_json(
        strategy_path,
        {
            "schema_version": "strategy_contract.v1",
            "strategy_id": "result_contract",
            "name": "Deterministic Portfolio Contract",
            "data_context": {
                "primary_instrument": "QQQ",
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
            },
            "parameter_domains": {
                "window": {"values": [1, 2, 3, 4, 5, 6]},
                "threshold": {"values": [10, 11, 12, 13, 14, 15]},
            },
        },
    )
    run_config_path = paths["snapshot_dir"] / "strategy_run.json"
    repo_root = Path(__file__).resolve().parent.parent.parent
    example_config_path = (
        repo_root
        / "backtester"
        / "contracts"
        / "strategy"
        / "examples"
        / "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"
    )
    _write_json(
        run_config_path,
        json.loads(example_config_path.read_text(encoding="utf-8")),
    )
    registry.write_snapshot_file(
        run_id,
        "run_snapshot.json",
        {
            "resolved_configs": {
                "run_config": {"config_path": str(run_config_path)},
                "backtester_config": {
                    "strategy_contract_path": str(strategy_path),
                    "trading_params": {
                        "transaction_cost": 0.001,
                        "slippage": 0.0005,
                    },
                },
            },
            "contract_refs": {"strategy_contract": {"path": str(strategy_path)}},
        },
    )
    series: list[dict[str, object]] = []
    metadata: list[dict[str, object]] = []
    matrix_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
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
                {
                    "artifact_type": "metricstracker_metadata",
                    "path": str(metadata_path),
                },
                {"artifact_type": "metricstracker_parquet", "path": str(metrics_path)},
                {
                    "artifact_type": "portfolio_matrix_summary_json",
                    "path": str(matrix_path),
                },
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
        {"param_axes": [{"name": "window"}, {"name": "threshold"}]},
    )
    return (
        MetricsContractPayloadService(registry),
        AppPayloadService(root, registry),
        registry,
        run_id,
    )


def build_wfa_contract_run(root: Path) -> tuple[AppAPIService, str]:
    """Build a canonical selected-optimum WFA artifact and dashboard source."""

    service = AppAPIService(root)
    run_id = "wfa-contract-run"
    paths = service.registry.build_run_paths(run_id)
    artifact_dir = (
        paths["snapshot_dir"] / "managed_artifacts" / VALIDATION_WORKFLOW_CANONICAL
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    wfa_path = artifact_dir / "wfa_contract_selected_optimum.parquet"
    pd.DataFrame(
        {
            "window_id": [1],
            "objective": ["calmar"],
            "semantic_combo": [json.dumps({"fast": 10, "slow": 200}, sort_keys=True)],
            "train_start": [pd.Timestamp("2024-01-01")],
            "train_end": [pd.Timestamp("2024-01-20")],
            "test_start": [pd.Timestamp("2024-01-21")],
            "test_end": [pd.Timestamp("2024-01-31")],
            "is_sharpe": [0.8],
            "is_calmar": [0.4],
            "oos_sharpe": [0.6],
            "oos_calmar": [0.3],
            "oos_total_return": [0.08],
            "selection_source": ["canonical_wfa"],
            "selection_rank": [1],
            "selection_metric": ["calmar"],
            "selection_evidence": ["rank=1 by IS Calmar"],
            "candidate_count": [1],
            "workflow": ["rolling_validation"],
            "wfa_row_type": ["selected_optimum"],
        }
    ).to_parquet(wfa_path, index=False)
    _write_json(
        wfa_path.with_name("wfa_contract_metadata.json"),
        {
            "row_contract": "selected_optimum_per_window",
            "annualization": {
                "schema_version": "metrics_annualization.v1",
                "basis": "session_close_projection",
                "projection_policy": "last_accepted_equity_per_session",
                "periods_per_year": 365.0,
                "risk_free_rate_annual": 0.0,
            },
        },
    )
    _write_json(
        paths["snapshot_dir"] / "wfa_run.json",
        {
            "schema_version": "wfa_run",
            "optimizer": {"primary_objective": "calmar"},
        },
    )
    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "entrypoint": "test",
            "status": "completed",
            "created_at": "2026-07-31T00:00:00Z",
            "completed_at": "2026-07-31T00:00:01Z",
            "config_filename": "wfa_contract.json",
            "strategy_mode": "single_asset",
            "run_type": "test",
        }
    )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "schema_version": "1.0",
            "artifacts": [
                {
                    "artifact_type": "wfa_parquet",
                    "path": str(wfa_path),
                    "status": "ready",
                }
            ],
        },
    )
    return service, run_id


def build_stat_contract_run(root: Path) -> tuple[AppAPIService, str]:
    """Build a deterministic StatAnalyser summary snapshot."""

    service = AppAPIService(root)
    run_id = "stat-contract-run"
    paths = service.registry.build_run_paths(run_id)
    _write_json(
        paths["snapshot_dir"] / "statanalyser_summary.json",
        {
            "status": "completed",
            "sample_count": 30,
            "mean_return": 0.001,
            "confidence_level": 0.95,
        },
    )
    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "statanalyser",
            "entrypoint": "test",
            "status": "completed",
            "created_at": "2026-07-31T00:00:00Z",
            "completed_at": "2026-07-31T00:00:01Z",
            "config_filename": "stat_contract.json",
            "run_type": "test",
        }
    )
    return service, run_id
