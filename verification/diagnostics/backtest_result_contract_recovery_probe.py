"""Read-only probe for the P0 backtest result contract recovery evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_RUN_ID = "20260718_b64dcb042889"
DEFAULT_CANDIDATE_ID = (
    "btcusd_monthly_nth_weekday_same_session_coinbase_example:"
    "parameter_matrix:month_week_1_weekday_wednesday"
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _artifact_paths(repo_root: Path, run_id: str) -> dict[str, Path]:
    manifest_path = repo_root / "outputs" / "app" / "artifact_manifests" / f"{run_id}.json"
    manifest = _read_json(manifest_path)
    return {
        str(item["artifact_type"]): Path(str(item["path"]))
        for item in manifest.get("artifacts", [])
        if isinstance(item, dict) and item.get("artifact_type") and item.get("path")
    }


def _candidate_rows(frame: pd.DataFrame, candidate_id: str) -> pd.DataFrame:
    if "Backtest_id" not in frame.columns:
        return frame.iloc[0:0]
    return frame.loc[frame["Backtest_id"].astype(str) == candidate_id].copy()


def _find_row(rows: Any, candidate_id: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        return {}
    return next(
        (
            dict(row)
            for row in rows
            if isinstance(row, dict)
            and candidate_id
            in {
                str(row.get("Backtest_id") or ""),
                str(row.get("backtest_id") or ""),
                str(row.get("strategy_id") or ""),
            }
        ),
        {},
    )


def collect_trace(repo_root: Path, run_id: str, candidate_id: str) -> dict[str, Any]:
    artifacts = _artifact_paths(repo_root, run_id)
    snapshot = repo_root / "outputs" / "app" / "run_snapshots" / run_id
    chart_dir = repo_root / "outputs" / "app" / "chart_payloads" / run_id

    strategy_run = _read_json(snapshot / "strategy_run.json")
    matrix_summary = _read_json(artifacts["portfolio_matrix_summary_json"])
    metrics_metadata = _read_json(artifacts["metricstracker_metadata"])
    metrics_overview = _read_json(chart_dir / "metrics_overview_payload.json")
    heatmap = _read_json(chart_dir / "parameter_heatmap_payload.json")
    detail_path = next(chart_dir.glob("backtest_detail_*.json"))
    detail = _read_json(detail_path)

    equity = _candidate_rows(
        pd.read_parquet(artifacts["portfolio_equity_curve_parquet"]), candidate_id
    )
    holdings = _candidate_rows(
        pd.read_parquet(artifacts["portfolio_holdings_parquet"]), candidate_id
    )
    rebalance_audit = _candidate_rows(
        pd.read_parquet(artifacts["portfolio_rebalance_audit_parquet"]), candidate_id
    )
    rebalance_trades = _candidate_rows(
        pd.read_parquet(artifacts["portfolio_rebalance_trades_parquet"]), candidate_id
    )
    metrics_frame = _candidate_rows(
        pd.read_parquet(artifacts["metricstracker_parquet"]), candidate_id
    )

    canonical_row = _find_row(matrix_summary.get("rows"), candidate_id)
    metrics_row = _find_row(metrics_metadata, candidate_id)
    overview_row = _find_row(metrics_overview.get("rows"), candidate_id)
    benchmark = ((strategy_run.get("data") or {}).get("benchmark") or {})

    return {
        "schema_version": "backtest_result_contract_recovery_probe.v1",
        "run_id": run_id,
        "candidate_id": candidate_id,
        "paths": {
            "strategy_run": str(snapshot / "strategy_run.json"),
            "artifact_manifest": str(
                repo_root / "outputs" / "app" / "artifact_manifests" / f"{run_id}.json"
            ),
            "matrix_summary": str(artifacts["portfolio_matrix_summary_json"]),
            "metrics_metadata": str(artifacts["metricstracker_metadata"]),
            "metrics_parquet": str(artifacts["metricstracker_parquet"]),
            "metrics_overview": str(chart_dir / "metrics_overview_payload.json"),
            "parameter_heatmap": str(chart_dir / "parameter_heatmap_payload.json"),
            "detail": str(detail_path),
        },
        "config_intent": {
            "result_type": "portfolio",
            "workflow_id": (strategy_run.get("platform") or {}).get("workflow_id"),
            "benchmark": benchmark,
            "starting_balance": (
                ((strategy_run.get("simulation") or {}).get("account") or {}).get(
                    "starting_balance"
                )
            ),
            "outputs": strategy_run.get("outputs") or {},
        },
        "canonical_result": {
            "matrix_variant_count": matrix_summary.get("variant_count"),
            "matrix_row_count": matrix_summary.get("row_count"),
            "retained_result_count": matrix_summary.get("retained_result_count"),
            "compact_result_count": matrix_summary.get("compact_result_count"),
            "candidate": {
                key: canonical_row.get(key)
                for key in (
                    "semantic_combo",
                    "result_type",
                    "result_materialization",
                    "final_equity",
                    "total_return",
                    "cagr",
                    "sharpe",
                    "max_drawdown",
                    "trade_count",
                    "rebalance_count",
                    "avg_turnover",
                    "avg_gross_exposure",
                    "days",
                )
            },
            "source_rows": {
                "equity": len(equity),
                "holdings": len(holdings),
                "rebalance_audit": len(rebalance_audit),
                "rebalance_trades": len(rebalance_trades),
            },
        },
        "metrics_projection": {
            "metadata": {
                key: metrics_row.get(key)
                for key in (
                    "Total_return",
                    "Annualized_return (CAGR)",
                    "Sharpe",
                    "Max_drawdown",
                    "Trade_count",
                    "BAH_Total_return",
                    "BAH_Annualized_return (CAGR)",
                    "BAH_Sharpe",
                    "BAH_Max_drawdown",
                )
            },
            "overview": {
                key: overview_row.get(key)
                for key in (
                    "semantic_combo",
                    "total_return",
                    "cagr",
                    "sharpe",
                    "max_drawdown",
                    "trade_count",
                    "bah_total_return",
                    "bah_cagr",
                    "bah_sharpe",
                    "bah_max_drawdown",
                    "excess_return",
                )
            },
            "overview_result_type": metrics_overview.get("result_type"),
            "overview_rows": len(metrics_overview.get("rows") or []),
            "default_category_size": len(
                (metrics_overview.get("categories") or {}).get(
                    metrics_overview.get("default_category"), []
                )
            ),
            "benchmark_series_present": bool(metrics_overview.get("benchmark_series")),
            "bah_equity_non_null": int(metrics_frame.get("BAH_Equity", pd.Series(dtype=float)).notna().sum()),
        },
        "heatmap_projection": {
            "rows": len(heatmap.get("rows") or []),
            "axis_value_counts": {
                key: len(values)
                for key, values in (heatmap.get("axis_values") or {}).items()
                if isinstance(values, list)
            },
            "result_type": heatmap.get("result_type"),
        },
        "detail_projection": {
            "contract_id": detail.get("contract_id"),
            "result_type": detail.get("result_type"),
            "metrics_keys": sorted((detail.get("metrics_matrix") or {}).keys()),
            "equity_points": len(detail.get("equity_series") or []),
            "benchmark_points": len(detail.get("benchmark_series") or []),
            "buy_markers": len(detail.get("buy_markers") or []),
            "sell_markers": len(detail.get("sell_markers") or []),
            "trade_rows": len(detail.get("trade_rows") or []),
            "monthly_rows": len(detail.get("monthly_return_rows") or []),
            "yearly_rows": len(detail.get("yearly_return_rows") or []),
            "risk_diagnostics_keys": sorted((detail.get("risk_diagnostics") or {}).keys()),
            "rich_portfolio_fields": {
                key: len(detail.get(key) or []) if isinstance(detail.get(key), list) else bool(detail.get(key))
                for key in (
                    "holding_rows",
                    "allocation_change_rows",
                    "rebalance_rows",
                    "asset_contribution_rows",
                    "drawdown_series",
                    "turnover_distribution",
                )
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--candidate-id", default=DEFAULT_CANDIDATE_ID)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    trace = collect_trace(args.repo_root.resolve(), args.run_id, args.candidate_id)
    rendered = json.dumps(trace, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
