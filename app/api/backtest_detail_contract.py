"""Strict reader for Rust-projected backtest detail bundles."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict

from app.runtime.registry import AppRegistry
from .shared_chart_series import SharedChartSeriesStore


class BacktestDetailContractService:
    def __init__(self, registry: AppRegistry):
        self.registry = registry
        self.shared_series = SharedChartSeriesStore(registry)

    def path(self, run_id: str, backtest_id: str) -> Path:
        safe_id = re.sub(r"[^A-Za-z0-9._-]+", "_", str(backtest_id)).strip("._-")
        path = (
            self.registry.resolve_run_paths(run_id)["chart_payload_dir"]
            / f"backtest_detail_{safe_id or 'result'}.json"
        )
        index = self._read_json(path)
        payload = self.shared_series.materialize_backtest_detail(run_id, index)
        self._validate(payload, run_id=run_id, backtest_id=backtest_id)
        self._validate_canonical_source(payload)
        return path

    def load(self, run_id: str, backtest_id: str) -> Dict[str, Any]:
        path = self.path(run_id, backtest_id)
        return self.shared_series.materialize_backtest_detail(
            run_id,
            self._read_json(path),
        )

    @staticmethod
    def _validate(payload: Dict[str, Any], *, run_id: str, backtest_id: str) -> None:
        if payload.get("schema_version") != "backtest_detail_bundle.v3":
            raise ValueError("backtest detail requires BacktestDetailBundle.v3")
        if payload.get("contract_id") != "lo2cin4bt.backtest_detail_bundle.v3":
            raise ValueError("backtest detail contract_id is invalid")
        if payload.get("run_id") != run_id or str(payload.get("backtest_id")) != str(backtest_id):
            raise ValueError("backtest detail identity does not match request")
        if not isinstance(payload.get("ohlc"), list) or not payload["ohlc"]:
            raise ValueError("backtest detail OHLC is empty")
        if payload.get("result_type") == "portfolio":
            metrics = payload.get("metrics_matrix")
            required_metrics = {
                "total_return",
                "cagr",
                "sharpe",
                "max_drawdown",
                "trade_count",
                "avg_holdings",
                "avg_gross_exposure",
                "avg_turnover",
                "annualized_std",
                "sortino",
                "calmar",
                "max_drawdown_duration_days",
                "recovery_factor",
                "skewness",
                "kurtosis",
                "var_95",
                "cvar_95",
                "var_99",
                "cvar_99",
                "worst_month",
                "best_month",
                "positive_month_ratio",
                "win_rate",
                "average_win",
                "average_loss",
                "gross_profit",
                "gross_loss",
                "max_consecutive_wins",
                "max_consecutive_losses",
                "excess_return",
            }
            if not isinstance(metrics, dict) or not required_metrics.issubset(metrics):
                raise ValueError("portfolio detail metrics contract is incomplete")
            if not isinstance(payload.get("parameter_summary"), dict):
                raise ValueError("portfolio detail parameter summary is invalid")
            for field in (
                "holding_rows",
                "rebalance_rows",
                "allocation_change_rows",
                "monthly_return_rows",
                "yearly_return_rows",
                "drawdown_series",
            ):
                if not isinstance(payload.get(field), list):
                    raise ValueError(f"portfolio detail field is invalid: {field}")
            data_quality = payload.get("data_quality")
            if not isinstance(data_quality, dict) or not {
                "expected_symbols",
                "loaded_symbols",
                "missing_symbols",
            }.issubset(data_quality):
                raise ValueError("portfolio detail data-quality contract is incomplete")
            turnover_summary = payload.get("turnover_summary")
            if not isinstance(turnover_summary, dict) or not {
                "checkpoint_events",
                "trade_events",
            }.issubset(turnover_summary):
                raise ValueError("portfolio detail turnover contract is incomplete")
            diagnostics = payload.get("risk_diagnostics")
            if not isinstance(diagnostics, dict) or not {
                "serial_correlation",
                "profit_concentration",
                "recovery_time",
            }.issubset(diagnostics):
                raise ValueError("portfolio detail risk-diagnostics contract is incomplete")
            contribution_summary = payload.get("asset_contribution_summary")
            if not isinstance(contribution_summary, dict) or not {
                "portfolio_total_return",
                "total_asset_contribution",
                "residual_and_compounding",
            }.issubset(contribution_summary):
                raise ValueError("portfolio detail contribution contract is incomplete")
            contribution_rows = payload.get("asset_contribution_rows")
            if not isinstance(contribution_rows, list) or any(
                not isinstance(row, dict) or "contribution_share" not in row
                for row in contribution_rows
            ):
                raise ValueError("portfolio detail contribution rows are incomplete")
        hashes = payload.get("source_hashes")
        if not isinstance(hashes, list) or not hashes or any(len(str(value)) != 64 for value in hashes):
            raise ValueError("backtest detail source hashes are invalid")

    @staticmethod
    def _validate_canonical_source(payload: Dict[str, Any]) -> None:
        for raw_path in payload.get("artifact_source_refs", []):
            path = Path(str(raw_path))
            if path.suffix.lower() != ".json" or not path.is_file():
                continue
            source = BacktestDetailContractService._read_json(path)
            if (
                source.get("schema_version") == "canonical_result_bundle.v1"
                and isinstance(source.get("validation"), dict)
                and source["validation"].get("status") == "valid"
                and source.get("result_hashes") == payload.get("source_hashes")
            ):
                return
        raise ValueError("backtest detail does not reference a matching validated canonical result")

    @staticmethod
    def _read_json(path: Path) -> Dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FileNotFoundError(f"backtest detail contract is unavailable: {path}") from exc
        if not isinstance(payload, dict):
            raise ValueError("backtest detail contract must be an object")
        return payload
