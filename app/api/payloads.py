from __future__ import annotations

import copy
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd

from app.api.metrics_contract_payload import METRICS_OVERVIEW_SCHEMA_VERSION
from app.api.shared_chart_series import SharedChartSeriesStore
from app.runtime.module_identity import (
    VALIDATION_WORKFLOW_CANONICAL,
    canonical_module_id,
)
from app.runtime.registry import AppRegistry
from backtester.StrategyRunConfig_backtester import (
    is_wfa_run_schema_version,
    normalize_strategy_run_config,
    plan_strategy_execution,
)

CATEGORY_MAP: Dict[str, Dict[str, Any]] = {
    "top_20_sharpe": {"label": "Top 20 Sharpe", "key": "sharpe", "ascending": False},
    "top_20_return": {"label": "Top 20 Return", "key": "total_return", "ascending": False},
    "top_20_cagr": {"label": "Top 20 CAGR", "key": "cagr", "ascending": False},
    "top_20_calmar": {"label": "Top 20 Calmar", "key": "calmar", "ascending": False},
    "top_20_sortino": {"label": "Top 20 Sortino", "key": "sortino", "ascending": False},
    "top_20_recovery_factor": {
        "label": "Top 20 Recovery Factor",
        "key": "recovery_factor",
        "ascending": False,
    },
    "top_20_information_ratio": {
        "label": "Top 20 Information Ratio",
        "key": "information_ratio",
        "ascending": False,
    },
    "top_20_profit_factor": {
        "label": "Top 20 Profit Factor",
        "key": "profit_factor",
        "ascending": False,
    },
    "top_20_lowest_mdd": {
        "label": "Top 20 Lowest MDD",
        "key": "max_drawdown",
        "ascending": False,
    },
    "top_20_excess_return": {
        "label": "Top 20 Excess Return",
        "key": "excess_return",
        "ascending": False,
    },
}

METRICS_OVERVIEW_MAX_POINTS = 240
PARAMETER_HEATMAP_SCHEMA_VERSION = "3.7"
WFA_DASHBOARD_SCHEMA_VERSION = "3.7"
BACKTEST_DETAIL_SCHEMA_VERSION = "1.19"
AI_READABLE_OUTPUT_SCHEMA_VERSION = "1.0"
AI_REVIEW_NUMERIC_FIELD_LIMIT = 5000
AI_REVIEW_LIST_SAMPLE_LIMIT = 5
AI_REVIEW_ARTIFACT_PROFILE_LIMIT_PER_TYPE = 3

METRIC_KEY_MAP: Dict[str, str] = {
    "total_return": "Total_return",
    "cagr": "Annualized_return (CAGR)",
    "sharpe": "Sharpe",
    "sortino": "Sortino",
    "calmar": "Calmar",
    "max_drawdown": "Max_drawdown",
    "average_drawdown": "Average_drawdown",
    "recovery_factor": "Recovery_factor",
    "std": "Std",
    "annualized_std": "Annualized_std",
    "downside_risk": "Downside_risk",
    "annualized_downside_risk": "Annualized_downside_risk",
    "information_ratio": "Information_ratio",
    "alpha": "Alpha",
    "beta": "Beta",
    "trade_count": "Trade_count",
    "win_rate": "Win_rate",
    "profit_factor": "Profit_factor",
    "avg_trade_return": "Avg_trade_return",
    "max_consecutive_losses": "Max_consecutive_losses",
    "exposure_time": "Exposure_time",
    "max_holding_period_ratio": "Max_holding_period_ratio",
    "bah_total_return": "BAH_Total_return",
    "bah_cagr": "BAH_Annualized_return (CAGR)",
    "bah_sharpe": "BAH_Sharpe",
    "bah_calmar": "BAH_Calmar",
    "bah_max_drawdown": "BAH_Max_drawdown",
}


class AppPayloadService:
    def __init__(self, repo_root: Path, registry: AppRegistry):
        self.repo_root = Path(repo_root).resolve()
        self.registry = registry
        self._heatmap_builder = None
        self._robust_selector = None

    @property
    def heatmap_builder(self):
        if self._heatmap_builder is None:
            from validation_workflow.HeatmapMatrixBuilder_validation_workflow import (
                HeatmapMatrixBuilder,
            )

            self._heatmap_builder = HeatmapMatrixBuilder()
        return self._heatmap_builder

    @property
    def robust_selector(self):
        if self._robust_selector is None:
            from validation_workflow.RobustSelector_validation_workflow import (
                RobustSelector,
            )

            self._robust_selector = RobustSelector()
        return self._robust_selector

    def ensure_run_payloads(
        self,
        run_id: str,
        module: Optional[str] = None,
    ) -> Dict[str, str]:
        registry_entry = self.registry.load_registry_entry(run_id)
        module_name = canonical_module_id(module or str(registry_entry.get("module", "")))
        created: Dict[str, str] = {}
        if module_name == "autorunner":
            from app.api.metrics_contract_payload import MetricsContractPayloadService

            created["metrics_overview"] = str(
                MetricsContractPayloadService(self.registry).ensure(run_id)
            )
            try:
                created["parameter_matrix"] = str(self.ensure_parameter_matrix_payload(run_id))
            except FileNotFoundError as exc:
                message = str(exc)
                if (
                    "at least two semantic parameter axes" not in message
                    and "portfolio parameter matrix requires at least two varied parameters" not in message
                ):
                    raise
                created["parameter_matrix"] = "skipped: not a parameter-matrix run"
        elif module_name == VALIDATION_WORKFLOW_CANONICAL:
            created["wfa_dashboard"] = str(self.ensure_wfa_dashboard_payload(run_id))
        elif module_name == "statanalyser":
            created["statanalyser_summary"] = str(
                self.ensure_statanalyser_summary_payload(run_id)
            )
        created["ai_readable_output"] = str(
            self.ensure_ai_readable_output(run_id, module=module_name)
        )
        return created










    @classmethod
    def _consecutive_return_streaks(cls, returns: pd.Series) -> tuple[int, int]:
        max_wins = 0
        max_losses = 0
        current_wins = 0
        current_losses = 0
        for value in pd.to_numeric(returns, errors="coerce").dropna().tolist():
            numeric = float(value)
            if numeric > 0:
                current_wins += 1
                current_losses = 0
            elif numeric < 0:
                current_losses += 1
                current_wins = 0
            else:
                current_wins = 0
                current_losses = 0
            max_wins = max(max_wins, current_wins)
            max_losses = max(max_losses, current_losses)
        return max_wins, max_losses

    def _drawdown_duration(self, drawdown: pd.Series, time_values: Any) -> Dict[str, Any]:
        numeric_drawdown = pd.to_numeric(drawdown, errors="coerce")
        if (
            numeric_drawdown.isna().any()
            or not np.isfinite(numeric_drawdown.to_numpy()).all()
        ):
            raise ValueError("Drawdown series contains missing or non-finite values")
        in_drawdown = numeric_drawdown < 0.0
        times = pd.to_datetime(time_values, errors="coerce") if time_values is not None else pd.Series(dtype="datetime64[ns]")
        max_periods = 0
        max_days: Optional[int] = None
        current_start_index: Optional[int] = None
        current_periods = 0
        for index, active in enumerate(in_drawdown.tolist()):
            if active:
                if current_start_index is None:
                    current_start_index = index
                    current_periods = 0
                current_periods += 1
                if current_periods > max_periods:
                    max_periods = current_periods
                    if len(times) > index and current_start_index is not None:
                        start_time = times.iloc[current_start_index]
                        end_time = times.iloc[index]
                        if pd.notna(start_time) and pd.notna(end_time):
                            max_days = int(max(0, (end_time - start_time).days))
            else:
                current_start_index = None
                current_periods = 0
        return {"periods": max_periods, "days": max_days}

    def _tail_mean(self, returns: pd.Series, threshold: Optional[float]) -> Optional[float]:
        if threshold is None:
            return None
        tail = pd.to_numeric(returns, errors="coerce").dropna()
        tail = tail[tail <= float(threshold)]
        if tail.empty:
            return None
        return self._finite_or_none(tail.mean())

    @staticmethod
    def _coerce_datetime_series(values: Any, index: Any = None) -> pd.Series:
        parsed = pd.to_datetime(values, errors="coerce", utc=True)
        if isinstance(parsed, pd.Series):
            return parsed.dt.tz_convert(None)
        return pd.Series(pd.DatetimeIndex(parsed).tz_convert(None), index=index)

    def _coerce_equity_frame(self, equity_df: pd.DataFrame) -> pd.DataFrame:
        if equity_df.empty:
            return pd.DataFrame(columns=["Time", "Equity_value"])
        frame = equity_df.copy()
        time_source: Any = frame["Time"] if "Time" in frame.columns else None
        if time_source is None:
            for candidate in ("time", "Date", "date", "Datetime", "datetime", "Timestamp", "timestamp"):
                if candidate in frame.columns:
                    time_source = frame[candidate]
                    break
        time_series = (
            self._coerce_datetime_series(time_source, index=frame.index)
            if time_source is not None
            else pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
        )
        if time_series.notna().sum() == 0 and not isinstance(frame.index, pd.RangeIndex):
            time_series = self._coerce_datetime_series(frame.index, index=frame.index)
        if "Equity_value" not in frame.columns:
            for candidate in ("equity_value", "Equity", "equity", "Value", "value"):
                if candidate in frame.columns:
                    frame["Equity_value"] = frame[candidate]
                    break
        if "Equity_value" not in frame.columns:
            return pd.DataFrame(columns=["Time", "Equity_value"])
        frame["Time"] = time_series
        frame["Equity_value"] = pd.to_numeric(frame["Equity_value"], errors="coerce")
        return frame.dropna(subset=["Time", "Equity_value"]).sort_values("Time").reset_index(drop=True)

    def _period_return_rows(self, equity_df: pd.DataFrame, freq: str) -> List[Dict[str, Any]]:
        frame = self._coerce_equity_frame(equity_df)
        if frame.empty:
            return []
        indexed = frame.set_index("Time")["Equity_value"].sort_index()
        indexed = indexed[~indexed.index.duplicated(keep="last")]
        if indexed.empty:
            return []
        grouped = {period: values.dropna() for period, values in indexed.resample(freq)}
        period_close = indexed.resample(freq).last().ffill().dropna()
        rows: List[Dict[str, Any]] = []
        previous_close: Optional[float] = None
        for period, end_value in period_close.items():
            values = grouped.get(period, pd.Series(dtype=float))
            end_equity = self._finite_or_none(end_value)
            if end_equity is None:
                continue
            start_equity = previous_close
            if start_equity is None:
                if len(values) < 2:
                    previous_close = end_equity
                    continue
                start_equity = self._finite_or_none(values.iloc[0])
            period_return = None
            if start_equity not in (None, 0.0):
                period_return = float(end_equity / cast(float, start_equity) - 1.0)
            is_monthly = freq.upper().startswith("M")
            rows.append(
                {
                    "period": period.strftime("%Y-%m") if is_monthly else period.strftime("%Y"),
                    "year": int(period.year),
                    "month": int(period.month) if is_monthly else None,
                    "return": self._finite_or_none(period_return),
                    "start_equity": self._finite_or_none(start_equity),
                    "end_equity": end_equity,
                }
            )
            previous_close = end_equity
        return rows

    def _portfolio_strategy_summary(self, run_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        base = self._strategy_summary(run_id)
        config = metadata.get("config", {}) if isinstance(metadata.get("config"), dict) else {}
        universe = config.get("universe", {}) if isinstance(config.get("universe"), dict) else {}
        rebalance = config.get("rebalance", {}) if isinstance(config.get("rebalance"), dict) else {}
        selection = config.get("selection", {}) if isinstance(config.get("selection"), dict) else {}
        allocation = config.get("allocation", {}) if isinstance(config.get("allocation"), dict) else {}
        benchmark = config.get("benchmark", {}) if isinstance(config.get("benchmark"), dict) else {}
        benchmark_label = benchmark.get("label") or benchmark.get("symbol") or base.get("benchmark_label") or ""
        source_config = self._source_strategy_config(run_id)
        source_platform = source_config.get("platform", {}) if isinstance(source_config.get("platform"), dict) else {}
        display_rules = self._strategy_rule_display_overrides(source_config, config)
        strategy_mode_id = str(
            config.get("strategy_mode_id")
            or source_platform.get("strategy_mode_id")
            or base.get("strategy_mode_id")
            or "multi_asset_portfolio"
        ).strip()
        strategy_profile_id = str(
            config.get("strategy_profile_id")
            or source_platform.get("strategy_profile_id")
            or base.get("strategy_profile_id")
            or ""
        ).strip()
        strategy_preset_id = str(
            config.get("strategy_preset_id")
            or source_platform.get("strategy_preset_id")
            or base.get("strategy_preset_id")
            or ""
        ).strip()
        parameter_domains = (
            config.get("parameter_domains", {})
            if isinstance(config.get("parameter_domains"), dict)
            else {}
        )
        symbols = universe.get("symbols", []) if isinstance(universe.get("symbols"), list) else []
        trigger = rebalance.get("trigger", {}) if isinstance(rebalance.get("trigger"), dict) else {}
        workflow_id = (
            "parameter_matrix"
            if parameter_domains
            else "single_backtest"
        )
        if parameter_domains:
            selection_label = self._render_parameter_domains(parameter_domains)
        elif allocation.get("method") in {"fixed_weight", "fixed_weights", "static_weight", "static_weights"}:
            selection_label = "fixed weights"
        elif isinstance(config.get("signals"), dict) and config.get("signals", {}).get("entry"):
            selection_label = self._render_parameter_domains({})
        else:
            selection_label = (
                f"rank_by={selection.get('rank_by', '-')}; "
                f"top_n={selection.get('top_n', '-')}; "
                f"position_limit={allocation.get('position_limit', '-')}"
            )
        base.update(
            {
                "strategy_id": metadata.get("strategy_id") or config.get("strategy_id") or base.get("strategy_id"),
                "name": metadata.get("strategy_id") or config.get("strategy_id") or base.get("name"),
                "asset_label": ", ".join(str(item) for item in symbols) if symbols else base.get("asset_label", "Multi-asset"),
                "strategy_mode_id": strategy_mode_id,
                "strategy_profile_id": strategy_profile_id,
                "strategy_preset_id": strategy_preset_id,
                "mode_label": display_rules.get("mode_label")
                or self._render_strategy_identity_label(
                    strategy_profile_id=strategy_profile_id,
                    strategy_preset_id=strategy_preset_id,
                    strategy_mode_id=strategy_mode_id,
                )
                or "Multi-asset portfolio",
                "workflow_label": self._workflow_label(workflow_id),
                "workflow_id": workflow_id,
                "entry_rule": display_rules.get("entry_rule")
                or self._render_normalized_entry_rule(config.get("signals", {}), selection, rebalance)
                or f"Rebalance on {trigger.get('op', 'calendar trigger')}",
                "exit_rule": display_rules.get("exit_rule")
                or self._render_normalized_exit_rule(config.get("signals", {}), rebalance, allocation)
                or "Replaced or resized at next rebalance",
                "parameter_domains": parameter_domains,
                "parameter_domain_label": display_rules.get("parameter_domain_label") or selection_label,
                "benchmark_label": benchmark_label,
                "source": "multi_asset_portfolio_config",
            }
        )
        return base

    @classmethod
    def _strategy_rule_display_overrides(cls, *configs: Any) -> Dict[str, str]:
        candidates: List[Any] = []
        for config in configs:
            if not isinstance(config, dict):
                continue
            candidates.extend(
                [
                    config.get("strategy_rules"),
                    config.get("presentation", {}).get("strategy_rules")
                    if isinstance(config.get("presentation"), dict)
                    else None,
                    config.get("display", {}).get("strategy_rules")
                    if isinstance(config.get("display"), dict)
                    else None,
                    config.get("metadata", {}).get("strategy_rules")
                    if isinstance(config.get("metadata"), dict)
                    else None,
                ]
            )
        out: Dict[str, str] = {}
        key_aliases = {
            "mode": "mode_label",
            "entry": "entry_rule",
            "exit": "exit_rule",
            "domain": "parameter_domain_label",
            "parameter_domain": "parameter_domain_label",
        }
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            for key, value in candidate.items():
                normalized_key = key_aliases.get(str(key), str(key))
                if normalized_key not in {
                    "mode_label",
                    "entry_rule",
                    "exit_rule",
                    "parameter_domain_label",
                    "execution_label",
                    "cost_label",
                    "risk_label",
                }:
                    continue
                if normalized_key in {"entry_rule", "exit_rule"}:
                    if isinstance(value, dict):
                        text = cls._render_rule_node(value).strip()
                    else:
                        text = str(value or "").strip()
                        if text.startswith("{") and text.endswith("}"):
                            try:
                                parsed_rule = json.loads(text)
                            except json.JSONDecodeError:
                                parsed_rule = None
                            if isinstance(parsed_rule, dict):
                                text = cls._render_rule_node(parsed_rule).strip()
                else:
                    text = str(value or "").strip()
                if text:
                    out[normalized_key] = text
        return out

    def _frame_preview(self, frame: pd.DataFrame, *, limit: int) -> List[Dict[str, Any]]:
        if frame.empty:
            return []
        preview = frame.head(limit).copy()
        for column in preview.columns:
            if pd.api.types.is_datetime64_any_dtype(preview[column]):
                preview[column] = preview[column].map(self._to_iso)
            else:
                preview[column] = preview[column].map(self._json_safe_value)
        return self._records_from_frame(preview.where(pd.notna(preview), None))

    @staticmethod
    def _records_from_frame(frame: pd.DataFrame) -> List[Dict[str, Any]]:
        return [
            {str(key): value for key, value in dict(cast(Dict[Any, Any], record)).items()}
            for record in frame.to_dict(orient="records")
        ]

    def _finite_or_none(self, value: Any) -> Optional[float]:
        parsed = self._as_float(value)
        return parsed if math.isfinite(parsed) else None

    def _required_record_number(
        self,
        record: Dict[str, Any],
        *keys: str,
    ) -> float:
        for key in keys:
            if key in record and record[key] is not None:
                parsed = self._finite_or_none(record[key])
                if parsed is None:
                    raise ValueError(f"Invalid numeric result field: {key}")
                return parsed
        raise ValueError(f"Missing numeric result field: {'/'.join(keys)}")

    @staticmethod
    def _required_numeric_column(
        frame: pd.DataFrame,
        column: str,
    ) -> pd.Series:
        if column not in frame.columns:
            raise ValueError(f"Canonical result is missing required column: {column}")
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise ValueError(
                f"Canonical result contains invalid numeric values in {column}"
            )
        return values.astype(float)

    def _optional_bool(self, value: Any) -> Optional[bool]:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, str):
            text = value.strip().lower()
            if not text:
                return None
            if text in {"1", "true", "yes", "y", "on"}:
                return True
            if text in {"0", "false", "no", "n", "off"}:
                return False
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return bool(value)

    def _json_safe_value(self, value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return [self._json_safe_value(item) for item in value]
        if hasattr(value, "tolist"):
            converted = value.tolist()
            if converted is not value:
                return self._json_safe_value(converted)
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    def _downsample_xy(
        self,
        x_values: List[Any],
        y_values: List[Any],
        *,
        max_points: int,
    ) -> tuple[List[Any], List[Any]]:
        if len(x_values) <= max_points or len(y_values) <= max_points:
            return x_values, y_values
        if max_points <= 2:
            return [x_values[0], x_values[-1]], [y_values[0], y_values[-1]]
        stride = max(1, len(x_values) // (max_points - 1))
        sampled_x = x_values[::stride]
        sampled_y = y_values[::stride]
        if sampled_x[-1] != x_values[-1]:
            sampled_x.append(x_values[-1])
            sampled_y.append(y_values[-1])
        return sampled_x, sampled_y

    def ensure_parameter_matrix_payload(
        self,
        run_id: str,
        *,
        force: bool = False,
    ) -> Path:
        payload = self._build_parameter_matrix_payload(run_id, force=force)
        path = self._chart_path(run_id, "parameter_heatmap_payload.json")
        self._write_json(path, payload)
        return path

    def build_parameter_matrix_payload(
        self,
        run_id: str,
        *,
        force: bool = False,
        ranking_config_override: Optional[Dict[str, Any]] = None,
        acceptance_config_override: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._build_parameter_matrix_payload(
            run_id,
            force=force,
            ranking_config_override=ranking_config_override,
            acceptance_config_override=acceptance_config_override,
        )

    def _build_parameter_matrix_payload(
        self,
        run_id: str,
        *,
        force: bool = False,
        ranking_config_override: Optional[Dict[str, Any]] = None,
        acceptance_config_override: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        path = self._chart_path(run_id, "parameter_heatmap_payload.json")
        if (
            path.exists()
            and not force
            and ranking_config_override is None
            and acceptance_config_override is None
        ):
            cached_payload = self._load_json(path, {})
            if (
                isinstance(cached_payload, dict)
                and cached_payload.get("schema_version") == PARAMETER_HEATMAP_SCHEMA_VERSION
            ):
                return cached_payload
        matrix_summary_path, matrix_summary = self._load_portfolio_matrix_summary(run_id)
        has_portfolio_matrix_rows = (
            matrix_summary_path is not None
            and isinstance(matrix_summary, dict)
            and isinstance(matrix_summary.get("rows"), list)
            and bool(matrix_summary.get("rows"))
        )
        # Parameter review reuses the strict Rust-backed metrics contract. A
        # summary-only matrix reads its canonical matrix summary artifact.
        try:
            from app.api.metrics_contract_payload import MetricsContractPayloadService

            overview_path = MetricsContractPayloadService(self.registry).ensure(run_id)
            overview = self._load_json(overview_path, {})
        except (FileNotFoundError, ValueError):
            if not has_portfolio_matrix_rows:
                raise
            overview_path = matrix_summary_path
            overview = {
                "schema_version": METRICS_OVERVIEW_SCHEMA_VERSION,
                "artifact_type": "multi_asset_portfolio_matrix_bundle",
                "result_type": "portfolio",
                "rows": matrix_summary.get("rows", []),
            }
        rows = overview.get("rows", []) if isinstance(overview, dict) else []
        if not rows:
            if has_portfolio_matrix_rows:
                rows = matrix_summary.get("rows", [])
                if isinstance(overview, dict):
                    overview["rows"] = rows
            if not rows:
                raise FileNotFoundError(
                    "metrics overview rows missing for parameter matrix payload"
                )
        strategy_summary = self._parameter_strategy_summary(run_id, overview)
        execution_plan_path = self._snapshot_path(run_id, "execution_plan.json")
        if has_portfolio_matrix_rows:
            payload = self._build_portfolio_parameter_matrix_payload(
                run_id=run_id,
                overview=overview,
                overview_path=overview_path,
                ranking_config_override=ranking_config_override,
                acceptance_config_override=acceptance_config_override,
            )
            payload["schema_version"] = PARAMETER_HEATMAP_SCHEMA_VERSION
            payload["generated_at"] = self._now_iso()
            payload["strategy_summary"] = strategy_summary
            return payload
        if not execution_plan_path.exists():
            payload = self._build_no_parameter_matrix_payload(
                run_id=run_id,
                overview=overview,
                overview_path=overview_path,
                reason=(
                    "No parameter domain is available for this run. "
                    "Parameter Research only applies to runs with at least two varied parameters."
                ),
                result_type=str(overview.get("result_type") or "single_asset"),
                artifact_type=str(overview.get("artifact_type") or ""),
            )
            payload["strategy_summary"] = strategy_summary
            return payload

        plan = self._load_json(execution_plan_path, {})
        index_map = self._load_backtest_index_map(run_id)
        param_axes = [
            axis.get("name")
            for axis in plan.get("param_axes", [])
            if axis.get("name")
        ]
        matrix_rows: List[Dict[str, Any]] = []
        sorted_rows = sorted(
            rows,
            key=lambda item: item.get("sharpe") or float("-inf"),
            reverse=True,
        )
        for rank, row in enumerate(sorted_rows, start=1):
            backtest_id = str(row.get("backtest_id", ""))
            combo = index_map.get(backtest_id, {})
            payload_row: Dict[str, Any] = dict(row)
            payload_row["rank"] = rank
            payload_row["semantic_combo"] = combo.get("semantic_combo", {}) or row.get("semantic_combo", {})
            payload_row["strategy_id"] = combo.get("strategy_id") or row.get("strategy_id")
            payload_row["strategy_display_label"] = combo.get("strategy_display_label") or row.get("label")
            matrix_rows.append(payload_row)

        if len(param_axes) < 2:
            param_axes = self._infer_param_axes(matrix_rows)
        if len(param_axes) < 2:
            payload = self._build_no_parameter_matrix_payload(
                run_id=run_id,
                overview=overview,
                overview_path=overview_path,
                reason=(
                    "This run does not expose enough varied semantic parameters for Parameter Research. "
                    "Open the Backtests tab to review the fixed strategy result."
                ),
                result_type=str(overview.get("result_type") or "single_asset"),
                artifact_type=str(overview.get("artifact_type") or ""),
            )
            payload["strategy_summary"] = strategy_summary
            return payload

        future_live_search_config = self._load_future_live_search_config(run_id)
        ranking_config = copy.deepcopy(future_live_search_config.get("ranking") or {})
        acceptance_config = copy.deepcopy(future_live_search_config.get("acceptance") or {})
        if isinstance(ranking_config_override, dict) and ranking_config_override:
            ranking_config = self._deep_merge(ranking_config, ranking_config_override)
        if isinstance(acceptance_config_override, dict):
            acceptance_config = self._deep_merge(acceptance_config, acceptance_config_override)
        payload = self.heatmap_builder.build_payload(
            run_id=run_id,
            rows=matrix_rows,
            param_axes=param_axes,
            ranking_config=ranking_config,
            acceptance_config=acceptance_config,
        )
        payload["schema_version"] = PARAMETER_HEATMAP_SCHEMA_VERSION
        payload["generated_at"] = self._now_iso()
        payload["future_live_search_config"] = future_live_search_config
        payload["strategy_summary"] = strategy_summary
        payload["artifact_source_refs"] = [str(execution_plan_path), str(overview_path)]
        if future_live_search_config.get("config_path"):
            payload["artifact_source_refs"].append(str(future_live_search_config["config_path"]))
        return payload

    def _build_no_parameter_matrix_payload(
        self,
        *,
        run_id: str,
        overview: Dict[str, Any],
        overview_path: Path,
        reason: str,
        result_type: str,
        artifact_type: str,
    ) -> Dict[str, Any]:
        rows = overview.get("rows", []) if isinstance(overview, dict) else []
        return {
            "schema_version": PARAMETER_HEATMAP_SCHEMA_VERSION,
            "contract_id": "lo2cin4bt-app-parameter-heatmap-payload-v2",
            "run_id": run_id,
            "availability": "no_parameter_domain",
            "reason": reason,
            "result_type": result_type,
            "artifact_type": artifact_type,
            "rows": [],
            "source_row_count": len(rows) if isinstance(rows, list) else 0,
            "shortlist_rows": [],
            "cluster_summary": [],
            "parameter_importance": [],
            "study_summary": {
                "sampler": "not_applicable",
                "mode": "not_applicable",
                "objective": "",
                "n_trials": 0,
                "n_startup_trials": 0,
                "completed_trials": 0,
                "pruned_trials": 0,
                "best_robust_score": None,
                "accepted_candidate_count": 0,
                "cluster_count": 0,
                "warnings": ["no_parameter_domain"],
            },
            "objectives": [],
            "param_axes": [],
            "default_x_axis": "",
            "default_y_axis": "",
            "aggregation_modes": [],
            "reduction_modes": [],
            "axis_values": {},
            "search_source_options": [],
            "default_search_source": "all_existing_results",
            "ml_search_status": "not_applicable",
            "selected_representative_mode": "",
            "future_live_search_config": {
                "label": "No parameter domain",
                "mode": "not_applicable",
                "note": reason,
            },
            "artifact_source_refs": [str(overview_path)],
            "generated_at": self._now_iso(),
        }

    def _build_portfolio_parameter_matrix_payload(
        self,
        *,
        run_id: str,
        overview: Dict[str, Any],
        overview_path: Path,
        ranking_config_override: Optional[Dict[str, Any]],
        acceptance_config_override: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        matrix_summary_path, matrix_summary = self._load_portfolio_matrix_summary(run_id)
        rows = (
            matrix_summary.get("rows", [])
            if isinstance(matrix_summary, dict) and isinstance(matrix_summary.get("rows"), list)
            else overview.get("rows", [])
            if isinstance(overview, dict)
            else []
        )
        source_path = matrix_summary_path or overview_path
        matrix_rows: List[Dict[str, Any]] = []
        sorted_rows = sorted(
            rows,
            key=lambda item: item.get("sharpe") or float("-inf"),
            reverse=True,
        )
        for rank, row in enumerate(sorted_rows, start=1):
            combo = row.get("semantic_combo", {}) if isinstance(row, dict) else {}
            payload_row = dict(row)
            payload_row["rank"] = rank
            payload_row["semantic_combo"] = combo if isinstance(combo, dict) else {}
            payload_row["strategy_display_label"] = row.get("label")
            matrix_rows.append(payload_row)
        param_axes = self._infer_param_axes(matrix_rows)
        if len(param_axes) < 2:
            if len(param_axes) == 1 and len(matrix_rows) > 1:
                return self._build_table_only_parameter_matrix_payload(
                    run_id=run_id,
                    rows=matrix_rows,
                    param_axes=param_axes,
                    overview_path=source_path,
                    result_type="portfolio",
                    artifact_type=str(overview.get("artifact_type", "multi_asset_portfolio_backtest")),
                )
            return self._build_no_parameter_matrix_payload(
                run_id=run_id,
                overview=overview,
                overview_path=overview_path,
                reason=(
                    "No varied portfolio parameter domain is available for this run. "
                    "Fixed portfolios and single-policy portfolio runs should be reviewed in Backtests."
                ),
                result_type="portfolio",
                artifact_type=str(overview.get("artifact_type", "multi_asset_portfolio_backtest")),
            )
        ranking_config = copy.deepcopy(ranking_config_override or {})
        acceptance_config = copy.deepcopy(acceptance_config_override or {})
        payload = self.heatmap_builder.build_payload(
            run_id=run_id,
            rows=matrix_rows,
            param_axes=param_axes,
            ranking_config=ranking_config,
            acceptance_config=acceptance_config,
        )
        payload["result_type"] = "portfolio"
        payload["artifact_type"] = overview.get("artifact_type", "multi_asset_portfolio_backtest")
        payload["future_live_search_config"] = {
            "label": "Portfolio parameter matrix",
            "source_filename": source_path.name,
            "mode": "post_run_review",
            "note": (
                "Derived from portfolio matrix summary rows."
                if matrix_summary_path is not None
                else "Derived from multi-asset portfolio variants inside the selected metrics run."
            ),
        }
        payload["artifact_source_refs"] = [str(source_path)]
        if source_path != overview_path:
            payload["artifact_source_refs"].append(str(overview_path))
        if matrix_summary_path is not None:
            materialization_summary = self._matrix_row_materialization_summary(matrix_rows)
            payload["matrix_summary"] = {
                "schema_version": matrix_summary.get("schema_version"),
                "row_count": matrix_summary.get("row_count"),
                "variant_count": matrix_summary.get("variant_count"),
                "retained_result_count": matrix_summary.get("retained_result_count"),
                "compact_result_count": matrix_summary.get("compact_result_count"),
                "coverage": matrix_summary.get("coverage"),
                "materialization_summary": materialization_summary,
            }
        return payload

    def _parameter_strategy_summary(
        self, run_id: str, overview: Dict[str, Any]
    ) -> Dict[str, Any]:
        summary = overview.get("strategy_summary") if isinstance(overview, dict) else None
        if isinstance(summary, dict) and summary.get("asset_label"):
            return copy.deepcopy(summary)

        config = self._source_strategy_config(run_id)
        universe = config.get("universe", {}) if isinstance(config, dict) else {}
        symbols = universe.get("symbols", []) if isinstance(universe, dict) else []
        platform = config.get("platform", {}) if isinstance(config, dict) else {}
        return {
            "asset_label": ", ".join(
                str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
            ),
            "display_label": (
                platform.get("display_label") if isinstance(platform, dict) else ""
            ),
        }

    @staticmethod
    def _matrix_row_materialization_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        for row in rows:
            materialization = str((row or {}).get("result_materialization") or "unknown").strip() or "unknown"
            counts[materialization] = counts.get(materialization, 0) + 1
        return {
            "full_count": counts.get("full", 0),
            "summary_only_count": counts.get("summary_only", 0),
            "other_count": sum(
                count
                for key, count in counts.items()
                if key not in {"full", "summary_only"}
            ),
            "has_summary_only_rows": counts.get("summary_only", 0) > 0,
            "counts": counts,
        }

    def _load_portfolio_matrix_summary(self, run_id: str) -> tuple[Optional[Path], Dict[str, Any]]:
        candidate_paths = list(self._artifact_paths(run_id, "portfolio_matrix_summary_json"))
        manifest_paths = [
            path
            for path in self._artifact_paths(run_id, "portfolio_metadata_json")
            if "portfolio_matrix_summary" in path.name.lower()
            or "portfolio-matrix-summary" in path.name.lower()
        ]
        candidate_paths.extend(manifest_paths)
        snapshot_dir = self.registry.resolve_run_paths(run_id)["snapshot_dir"]
        if snapshot_dir.exists():
            candidate_paths.extend(snapshot_dir.rglob("*portfolio_matrix_summary*.json"))
            candidate_paths.extend(snapshot_dir.rglob("*portfolio-matrix-summary*.json"))
        seen: set[Path] = set()
        for path in candidate_paths:
            if path in seen:
                continue
            seen.add(path)
            payload = self._load_json(path, {})
            if (
                isinstance(payload, dict)
                and payload.get("schema_version") == "portfolio_matrix_summary.v1"
                and isinstance(payload.get("rows"), list)
            ):
                return path, payload
        return None, {}

    def _build_table_only_parameter_matrix_payload(
        self,
        *,
        run_id: str,
        rows: List[Dict[str, Any]],
        param_axes: List[str],
        overview_path: Path,
        result_type: str,
        artifact_type: str,
    ) -> Dict[str, Any]:
        values = sorted(
            {
                str((row.get("semantic_combo") or {}).get(param_axes[0]))
                for row in rows
                if isinstance(row.get("semantic_combo"), dict)
                and (row.get("semantic_combo") or {}).get(param_axes[0]) is not None
            }
        )
        return {
            "schema_version": PARAMETER_HEATMAP_SCHEMA_VERSION,
            "contract_id": "lo2cin4bt-app-parameter-heatmap-payload-v2",
            "run_id": run_id,
            "availability": "table_only_single_axis",
            "reason": "This run varies one parameter axis, so it is shown as ranked candidates rather than a two-axis heatmap.",
            "result_type": result_type,
            "artifact_type": artifact_type,
            "rows": rows,
            "source_row_count": len(rows),
            "shortlist_rows": rows[: min(20, len(rows))],
            "cluster_summary": [],
            "parameter_importance": [],
            "study_summary": {
                "sampler": "post_run_review",
                "mode": "single_axis_parameter_review",
                "objective": "sharpe",
                "n_trials": len(rows),
                "n_startup_trials": 0,
                "completed_trials": len(rows),
                "pruned_trials": 0,
                "best_robust_score": rows[0].get("sharpe") if rows else None,
                "accepted_candidate_count": len(rows),
                "cluster_count": 0,
                "warnings": ["single_axis_table_only"],
            },
            "objectives": ["sharpe", "total_return", "cagr", "max_drawdown"],
            "param_axes": param_axes,
            "default_x_axis": param_axes[0] if param_axes else "",
            "default_y_axis": "",
            "aggregation_modes": ["ranked_table"],
            "reduction_modes": [],
            "axis_values": {param_axes[0]: values} if param_axes else {},
            "search_source_options": [],
            "default_search_source": "all_existing_results",
            "ml_search_status": "not_applicable",
            "selected_representative_mode": "ranked_table",
            "future_live_search_config": {
                "label": "Single-axis parameter review",
                "source_filename": "portfolio metrics artifacts",
                "mode": "post_run_review",
                "note": "Derived from variants that differ by one categorical parameter.",
            },
            "artifact_source_refs": [str(overview_path)],
            "generated_at": self._now_iso(),
        }






    def _portfolio_ohlc_by_asset(
        self,
        trades_df: pd.DataFrame,
        *,
        market_frames: Dict[str, pd.DataFrame],
    ) -> Dict[str, List[Dict[str, Any]]]:
        if trades_df.empty or "Asset" not in trades_df.columns:
            return {}
        open_frame = self._normalized_market_frame(market_frames.get("open"))
        high_frame = self._normalized_market_frame(market_frames.get("high"))
        low_frame = self._normalized_market_frame(market_frames.get("low"))
        close_frame = self._normalized_market_frame(market_frames.get("close"))
        if close_frame is None:
            return {}
        assets = [
            asset
            for asset in trades_df["Asset"].dropna().astype(str).str.strip().unique().tolist()
            if asset
        ]
        output: Dict[str, List[Dict[str, Any]]] = {}
        for asset in assets:
            if asset not in close_frame.columns and len(close_frame.columns) != 1:
                continue
            series_rows: List[Dict[str, Any]] = []
            for timestamp in close_frame.index:
                open_source = open_frame if open_frame is not None else close_frame
                high_source = high_frame if high_frame is not None else close_frame
                low_source = low_frame if low_frame is not None else close_frame
                row = {
                    "time": self._to_iso(timestamp),
                    "open": self._market_price_at(open_source, timestamp, asset),
                    "high": self._market_price_at(high_source, timestamp, asset),
                    "low": self._market_price_at(low_source, timestamp, asset),
                    "close": self._market_price_at(close_frame, timestamp, asset),
                }
                if row["close"] is not None:
                    series_rows.append(row)
            if series_rows:
                output[asset] = series_rows
        return output

    @staticmethod
    def _normalized_market_frame(frame: Any) -> Optional[pd.DataFrame]:
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            return None
        normalized = frame.copy()
        normalized.index = pd.to_datetime(normalized.index, errors="coerce").normalize()
        normalized = normalized[~normalized.index.isna()].sort_index()
        return normalized.apply(pd.to_numeric, errors="coerce")

    def _market_price_at(
        self,
        frame: Optional[pd.DataFrame],
        raw_time: Any,
        asset: str,
    ) -> Optional[float]:
        if frame is None or not asset:
            return None
        timestamp = pd.to_datetime(raw_time, errors="coerce")
        if pd.isna(timestamp):
            return None
        date_key = pd.Timestamp(timestamp).normalize()
        if date_key not in frame.index:
            return None
        if asset in frame.columns:
            return self._finite_or_none(frame.at[date_key, asset])
        if len(frame.columns) == 1:
            return self._finite_or_none(frame.iloc[frame.index.get_loc(date_key), 0])
        return None

    def _row_price(
        self,
        row: Dict[str, Any],
        *,
        prefer_exit: bool = False,
    ) -> Optional[float]:
        specific_keys = (
            ("Exit_price", "exit_price", "Close_price", "close_price")
            if prefer_exit
            else ("Entry_price", "entry_price", "Open_price", "open_price")
        )
        for key in (
            *specific_keys,
            "Execution_price",
            "execution_price",
            "Fill_price",
            "fill_price",
            "Price",
            "price",
        ):
            value = self._finite_or_none(row.get(key))
            if value is not None:
                return value
        return None

    def _portfolio_allocation_change_events(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        if trades_df.empty:
            return trades_df
        mask: pd.Series = pd.Series(False, index=trades_df.index, dtype=bool)
        if "Action" in trades_df.columns:
            actions = trades_df["Action"].astype(str).str.strip().str.lower()
            mask = mask | ~actions.isin({"", "hold", "noop", "no_op", "none"})
        if "Trade_delta" in trades_df.columns:
            delta = self._required_numeric_column(trades_df, "Trade_delta")
            mask = mask | (delta.abs() > 1e-12)
        if {"Before_weight", "Target_weight"}.issubset(trades_df.columns):
            before = self._required_numeric_column(trades_df, "Before_weight")
            target = self._required_numeric_column(trades_df, "Target_weight")
            mask = mask | ((target - before).abs() > 1e-12)
        filtered = trades_df.loc[mask].copy()
        if filtered.empty:
            return filtered
        return self._annotate_allocation_change_trade_pnl(filtered)

    def _annotate_allocation_change_trade_pnl(self, trades_df: pd.DataFrame) -> pd.DataFrame:
        if trades_df.empty:
            return trades_df
        frame = trades_df.copy()
        frame["Trade_pnl_pct"] = None
        frame["Trade_side"] = None
        open_by_asset: Dict[str, Dict[str, Any]] = {}
        for index, row in frame.iterrows():
            record = row.to_dict()
            asset = str(record.get("Asset") or record.get("asset") or "").strip()
            if not asset:
                continue
            action = str(record.get("Action") or record.get("action") or "").strip().lower()
            before_weight = self._required_record_number(
                record,
                "Before_weight",
                "before_weight",
            )
            target_weight = self._required_record_number(
                record,
                "Target_weight",
                "target_weight",
            )
            is_entry = action in {"buy", "entry", "short", "new_short", "sell_short"} or (
                abs(before_weight) <= 1e-12 and abs(target_weight) > 1e-12
            )
            is_exit = action in {"exit", "sell", "close", "close_short", "cover"} or (
                abs(before_weight) > 1e-12 and abs(target_weight) <= 1e-12
            )
            side = "short" if (target_weight < 0.0 or action in {"short", "new_short", "sell_short"}) else "long"
            if is_entry and not is_exit:
                frame.at[index, "Trade_side"] = side
                open_by_asset[asset] = record
                continue
            if not is_exit:
                continue
            entry = open_by_asset.pop(asset, None)
            if not isinstance(entry, dict):
                continue
            entry_price = self._row_price(entry)
            exit_price = self._row_price(record, prefer_exit=True)
            entry_target_weight = self._required_record_number(
                entry,
                "Target_weight",
                "target_weight",
            )
            trade_side = "short" if (
                entry_target_weight < 0.0
                or str(entry.get("Action") or entry.get("action") or "").strip().lower() in {"short", "new_short", "sell_short"}
            ) else "long"
            frame.at[index, "Trade_side"] = trade_side
            if entry_price is None or exit_price is None or abs(entry_price) <= 1e-12:
                continue
            raw_return = (exit_price - entry_price) / entry_price
            trade_return = -raw_return if trade_side == "short" else raw_return
            frame.at[index, "Trade_pnl_pct"] = self._finite_or_none(trade_return)
        return frame

    @classmethod
    def _annotate_rebalance_event_display(
        cls,
        rebalance_df: pd.DataFrame,
        rebalance_trades_df: pd.DataFrame,
        *,
        timezone_label: str,
    ) -> pd.DataFrame:
        if rebalance_df.empty or "Time" not in rebalance_df.columns:
            return rebalance_df
        frame = rebalance_df.copy()
        frame["_event_date"] = pd.to_datetime(frame["Time"], errors="coerce").dt.normalize()
        frame["_event_order"] = frame.groupby("_event_date", dropna=False).cumcount()
        if not rebalance_trades_df.empty and "Time" in rebalance_trades_df.columns:
            trades = rebalance_trades_df.copy()
            trades["_event_date"] = pd.to_datetime(trades["Time"], errors="coerce").dt.normalize()
            trades["_event_order"] = trades.groupby("_event_date", dropna=False).cumcount()
            merge_cols = [
                col
                for col in ["_event_date", "_event_order", "Asset", "Action", "Reason"]
                if col in trades.columns
            ]
            frame = frame.merge(
                trades[merge_cols].rename(
                    columns={
                        "Asset": "Display_asset",
                        "Action": "Display_action",
                        "Reason": "Display_reason",
                    }
                ),
                on=["_event_date", "_event_order"],
                how="left",
            )
        phases = frame.apply(cls._event_phase_from_row, axis=1)
        frame["Event_phase"] = phases
        frame["Event_time_local"] = [
            cls._event_local_time_for_phase(phase) for phase in phases
        ]
        frame["Event_timezone"] = timezone_label or "America/New_York"
        frame["Event_timestamp_local"] = [
            cls._event_timestamp_label(date_value, phase, timezone_label)
            for date_value, phase in zip(frame["_event_date"], phases)
        ]
        return frame.drop(columns=["_event_date", "_event_order"], errors="ignore")

    @staticmethod
    def _event_phase_from_row(row: pd.Series) -> str:
        reason = str(row.get("Display_reason") or row.get("Reason") or "").lower()
        action = str(row.get("Display_action") or row.get("Action") or "").lower()
        if "event close" in reason or action == "close_short":
            return "event_close"
        if "event open" in reason or action in {"exit", "new_short"}:
            return "event_open"
        if "session open" in reason or action == "buy":
            return "session_open"
        return ""

    @staticmethod
    def _event_local_time_for_phase(phase: str) -> str:
        if phase == "event_close":
            return "16:00"
        if phase in {"event_open", "session_open"}:
            return "09:30"
        return ""

    @classmethod
    def _event_timestamp_label(
        cls,
        date_value: Any,
        phase: str,
        timezone_label: str,
    ) -> str:
        timestamp = pd.to_datetime(date_value, errors="coerce")
        if pd.isna(timestamp):
            return ""
        local_time = cls._event_local_time_for_phase(phase)
        if not local_time:
            return timestamp.date().isoformat()
        suffix = "ET" if str(timezone_label).strip() == "America/New_York" else str(timezone_label).strip()
        return f"{timestamp.date().isoformat()} {local_time} {suffix}".strip()

    def _portfolio_asset_contribution_rows(self, equity_df: pd.DataFrame) -> List[Dict[str, Any]]:
        contribution_cols = [str(col) for col in equity_df.columns if str(col).startswith("Contribution_")]
        rows: List[Dict[str, Any]] = []
        for contribution_col in contribution_cols:
            asset = contribution_col.removeprefix("Contribution_")
            contribution = self._required_numeric_column(
                equity_df,
                contribution_col,
            )
            weight = self._required_numeric_column(equity_df, f"Weight_{asset}")
            rows.append(
                {
                    "asset": asset,
                    "return_contribution": self._finite_or_none(contribution.sum()),
                    "avg_weight": self._finite_or_none(weight.mean()),
                    "active_days": int((weight.abs() > 1e-12).sum()),
                }
            )
        rows.sort(
            key=lambda item: abs(
                self._required_record_number(item, "return_contribution")
            ),
            reverse=True,
        )
        return rows

    @staticmethod
    def _portfolio_universe_provenance(
        metadata: Dict[str, Any],
        data_quality: Dict[str, Any],
    ) -> Dict[str, Any]:
        candidates = [
            metadata.get("universe_provenance"),
            data_quality.get("universe_provenance") if isinstance(data_quality, dict) else None,
        ]
        validation = metadata.get("run_validation", {}) if isinstance(metadata.get("run_validation"), dict) else {}
        candidates.append(validation.get("universe_provenance"))
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate:
                return dict(candidate)

        config = metadata.get("config", {}) if isinstance(metadata.get("config"), dict) else {}
        universe = config.get("universe", {}) if isinstance(config.get("universe"), dict) else {}
        symbols = universe.get("symbols", []) if isinstance(universe.get("symbols"), list) else []
        source_ref = (
            universe.get("historical_constituents_path")
            or universe.get("universe_constituents_path")
            or universe.get("constituents_path")
            or universe.get("constituents_path")
            or universe.get("source_path")
            or universe.get("universe_path")
            or universe.get("source_ref")
            or universe.get("source")
        )
        return {
            "schema_version": "universe_provenance.v1",
            "source_type": "explicit_config_symbols" if symbols else "unknown",
            "source_ref": str(source_ref) if source_ref not in (None, "", []) else None,
            "policy": str(
                universe.get("universe_policy")
                or universe.get("survivorship_policy")
                or ""
            ) or None,
            "as_of_date": str(
                universe.get("as_of_date") or universe.get("as_of") or ""
            ) or None,
            "configured_symbols": [str(item) for item in symbols],
            "runtime_symbols": [str(item) for item in symbols],
            "window_count": 0,
            "window_source_snapshots": [],
            "point_in_time_constituents": False,
            "constituents_validation": {
                "schema_version": "historical_universe_constituents_validation.v1",
                "status": "not_available",
                "path": str(source_ref) if source_ref not in (None, "", []) else None,
                "warnings": ["portfolio_metadata_missing_constituents_validation"],
                "errors": [],
            },
            "delisted_policy": str(universe.get("delisted_policy") or "") or None,
            "survivorship_bias_risk": "high" if symbols else "unknown",
            "provenance_status": "review",
            "warnings": ["legacy_metadata_missing_universe_provenance"],
        }



    @staticmethod
    def _truth_source_policy() -> Dict[str, Any]:
        return {
            "schema_version": "truth_source_policy.v1",
            "mode": "artifact_only",
            "source_refs_required": True,
            "silent_recompute_allowed": False,
            "warning_propagation": "required",
        }






    @staticmethod
    def _is_trade_style_portfolio(strategy_summary: Dict[str, Any]) -> bool:
        profile = str(strategy_summary.get("strategy_profile_id") or "").strip()
        preset = str(strategy_summary.get("strategy_preset_id") or "").strip()
        return (
            profile == "selection_timing_portfolio"
            or profile == "calendar_event_portfolio"
            or preset == "single_asset_signal"
        )





    def ensure_wfa_dashboard_payload(
        self,
        run_id: str,
        *,
        force: bool = False,
    ) -> Path:
        path = self._chart_path(run_id, "wfa_dashboard_payload.json")
        if path.exists() and not force:
            cached_payload = self._load_json(path, {})
            if (
                isinstance(cached_payload, dict)
                and cached_payload.get("schema_version") == WFA_DASHBOARD_SCHEMA_VERSION
                and self._payload_source_refs_exist(cached_payload)
            ):
                return path
        wfa_path = self._wfa_dashboard_artifact_path(run_id)
        if wfa_path is None:
            raise FileNotFoundError("wfa dashboard requires selected-optimum WFA parquet")
        wfa_metadata = self._wfa_sidecar_metadata(wfa_path)
        df = pd.read_parquet(wfa_path)
        if df.empty:
            raise FileNotFoundError("wfa parquet is empty")
        source_row_count = int(len(df))
        diagnostic_rows: List[Dict[str, Any]] = []
        if "wfa_row_type" not in df.columns:
            raise ValueError("WFA artifact requires canonical wfa_row_type")
        row_type = df["wfa_row_type"].fillna("").astype(str)
        diagnostic_df = df[row_type != "selected_optimum"].copy()
        if not diagnostic_df.empty:
            diagnostic_rows = self._records_from_frame(diagnostic_df)
        df = df[row_type == "selected_optimum"].copy()
        if df.empty:
            raise ValueError("WFA artifact contains no selected_optimum rows")
        diagnostic_artifacts = [
            str(path)
            for path in sorted(wfa_path.parent.glob("*candidate_diagnostics*.parquet"))
        ]
        diagnostic_artifacts.extend(
            str(path)
            for path in sorted(wfa_path.parent.glob("*candidate-diagnostics*.parquet"))
            if str(path) not in diagnostic_artifacts
        )
        metric_priority = ["oos_sharpe", "oos_calmar", "oos_total_return", "is_sharpe", "is_calmar"]
        objective = "is_sharpe"
        for key in metric_priority:
            if key not in df.columns:
                continue
            numeric = pd.to_numeric(df[key], errors="coerce")
            if numeric.notna().any():
                objective = key
                break
        evidence_metric = "Sharpe" if "sharpe" in objective else "Calmar" if "calmar" in objective else "Metric"
        rows: List[Dict[str, Any]] = []
        for record in self._records_from_frame(df):
            combo = self._parse_semantic_combo(record.get("semantic_combo"))
            row_objective = str(record.get("objective") or objective)
            row_selection_metric = record.get("selection_metric") or row_objective
            oos_portfolio = self._parse_json_object(record.get("oos_portfolio_json"))
            oos_risk_gate_summary = self._parse_json_object(record.get("oos_risk_gate_summary_json"))
            rows.append(
                {
                    "window_id": int(record.get("window_id", 0) or 0),
                    "semantic_combo": combo,
                    "objective": row_objective,
                    "is_sharpe": self._as_float(record.get("is_sharpe")),
                    "is_calmar": self._as_float(record.get("is_calmar")),
                    "oos_sharpe": self._as_float(record.get("oos_sharpe")),
                    "oos_calmar": self._as_float(record.get("oos_calmar")),
                    "oos_total_return": self._as_float(record.get("oos_total_return")),
                    "train_start_date": self._to_iso(record.get("train_start_date") or record.get("train_start")),
                    "train_end_date": self._to_iso(record.get("train_end_date") or record.get("train_end")),
                    "test_start_date": self._to_iso(record.get("test_start_date") or record.get("test_start")),
                    "test_end_date": self._to_iso(record.get("test_end_date") or record.get("test_end")),
                    "strategy_mode": record.get("strategy_mode"),
                    "execution_plan_hash": record.get("execution_plan_hash"),
                    "linked_backtest": self._record_backtest_ref(record)
                    or self._match_backtest_ref(
                        execution_plan_hash=record.get("execution_plan_hash"),
                        semantic_combo=combo,
                    ),
                    "selection_source": record.get("selection_source"),
                    "selection_rank": self._as_float(record.get("selection_rank")),
                    "selection_metric": row_selection_metric,
                    "selection_evidence": record.get("selection_evidence") or f"rank=1 by IS {evidence_metric}",
                    "candidate_count": self._as_float(record.get("candidate_count")),
                    "total_candidate_count": self._as_float(record.get("total_candidate_count")),
                    "candidate_budget": self._finite_or_none(record.get("candidate_budget")),
                    "candidate_budget_applied": self._optional_bool(record.get("candidate_budget_applied")),
                    "candidate_budget_policy": record.get("candidate_budget_policy"),
                    "candidate_budget_method": record.get("candidate_budget_method"),
                    "candidate_budget_seed": self._finite_or_none(record.get("candidate_budget_seed")),
                    "selection_pool_count": self._as_float(record.get("selection_pool_count")),
                    "selection_pool_total_count": self._as_float(record.get("selection_pool_total_count")),
                    "selection_constraints_applied": self._optional_bool(
                        record.get("selection_constraints_applied")
                    ) or False,
                    "candidate_viability_pass": (
                        True
                        if self._optional_bool(record.get("candidate_viability_pass")) is None
                        else self._optional_bool(record.get("candidate_viability_pass"))
                    ),
                    "candidate_viability_reasons": record.get("candidate_viability_reasons"),
                    "is_active_rebalance_count": self._as_float(record.get("is_active_rebalance_count")),
                    "is_exposure_ratio": self._as_float(record.get("is_exposure_ratio")),
                    "is_nonzero_return_days": self._as_float(record.get("is_nonzero_return_days")),
                    "candidate_max_lookback": self._as_float(record.get("candidate_max_lookback")),
                    "accepted": self._optional_bool(record.get("accepted")),
                    "review_status": record.get("review_status"),
                    "acceptance_reasons": record.get("acceptance_reasons"),
                    "wfa_row_type": record.get("wfa_row_type", "selected_optimum"),
                    "workflow": record.get("workflow"),
                    "oos_profit_factor": self._as_float(record.get("oos_profit_factor")),
                    "oos_win_rate": self._as_float(record.get("oos_win_rate")),
                    "oos_max_drawdown": self._as_float(record.get("oos_max_drawdown")),
                    "oos_portfolio": oos_portfolio,
                    "oos_rebalance_count": self._finite_or_none(
                        oos_portfolio.get("active_rebalance_count")
                    ),
                    "oos_avg_exposure": self._finite_or_none(oos_portfolio.get("avg_exposure")),
                    "oos_avg_holdings": self._finite_or_none(oos_portfolio.get("avg_holdings")),
                    "oos_total_turnover": self._finite_or_none(oos_portfolio.get("total_turnover")),
                    "oos_cost_drag": self._finite_or_none(oos_portfolio.get("cost_drag")),
                    "is_risk_gate_event_count": self._finite_or_none(
                        record.get("is_risk_gate_event_count")
                    ),
                    "oos_risk_gate_event_count": self._finite_or_none(
                        record.get("oos_risk_gate_event_count")
                        if record.get("oos_risk_gate_event_count") is not None
                        else oos_portfolio.get("risk_gate_event_count")
                    ),
                    "oos_risk_gate_summary": oos_risk_gate_summary
                    or oos_portfolio.get("risk_gate_summary")
                    or {},
                }
            )
        combo_groups: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            combo_key = json.dumps(row.get("semantic_combo", {}), sort_keys=True)
            combo_groups.setdefault(combo_key, []).append(row)
        from validation_workflow.WFAAcceptanceEvaluator_validation_workflow import (
            WFAAcceptanceEvaluator,
        )

        acceptance = WFAAcceptanceEvaluator()
        grouped_rows: List[Dict[str, Any]] = []
        clustering_input: List[Dict[str, Any]] = []
        for combo_key, combo_rows in combo_groups.items():
            summary = {
                "combo_key": combo_key,
                "label": self._semantic_combo_label(combo_rows[0].get("semantic_combo", {})),
                "params": combo_rows[0].get("semantic_combo", {}),
                "mean_is_sharpe": self._mean(combo_rows, "is_sharpe"),
                "mean_is_calmar": self._mean(combo_rows, "is_calmar"),
                "mean_oos_sharpe": self._mean(combo_rows, "oos_sharpe"),
                "mean_oos_calmar": self._mean(combo_rows, "oos_calmar"),
                "profit_factor": self._mean(combo_rows, "oos_profit_factor"),
                "win_rate": self._mean(combo_rows, "oos_win_rate"),
                "trade_count": len(combo_rows),
                "oos_std": self._std(combo_rows, "oos_sharpe"),
                "max_drawdown": self._mean(combo_rows, "oos_max_drawdown"),
                "selection_evidence": combo_rows[0].get("selection_evidence"),
                "selected_window_count": len(combo_rows),
            }
            acceptance_result = acceptance.evaluate(summary)
            selected_row_reasons = [
                str(row.get("acceptance_reasons"))
                for row in combo_rows
                if str(row.get("acceptance_reasons") or "").strip()
            ]
            selected_row_review_gate = any(
                self._optional_bool(row.get("accepted")) is False
                or str(row.get("review_status") or "").strip().lower() == "review"
                for row in combo_rows
            )
            summary["oos_is_ratio"] = acceptance_result.metrics.get("oos_is_ratio")
            summary["robust_score"] = acceptance_result.robust_score
            summary["accepted"] = bool(acceptance_result.accepted and not selected_row_review_gate)
            summary["review_status"] = "Pass" if summary["accepted"] else "Review"
            summary["acceptance_reasons"] = sorted(
                {
                    *acceptance_result.reasons,
                    *selected_row_reasons,
                    *(["selected_window_review_gate"] if selected_row_review_gate else []),
                }
            )
            summary["selected_window_review_count"] = sum(
                1
                for row in combo_rows
                if self._optional_bool(row.get("accepted")) is False
                or str(row.get("review_status") or "").strip().lower() == "review"
            )
            grouped_rows.append({**summary, "rows": combo_rows})
            clustering_input.append(summary)
        cluster_summary = self.robust_selector.cluster_candidates(
            clustering_input,
            representative_mode="cluster_median",
        )
        linked_backtest_run_ids = sorted(
            {
                str(row.get("linked_backtest", {}).get("run_id"))
                for row in rows
                if isinstance(row.get("linked_backtest"), dict)
                and str(row.get("linked_backtest", {}).get("run_id", "")).strip()
            }
        )
        cluster_lookup: Dict[str, Dict[str, Any]] = {}
        for cluster in cluster_summary.get("clusters", []):
            cluster_rows = cluster.get("rows", []) if isinstance(cluster, dict) else []
            selected_window_count = 0
            for cluster_row in cluster_rows:
                if not isinstance(cluster_row, dict):
                    continue
                raw_combo_key = cluster_row.get("combo_key")
                if raw_combo_key:
                    cluster_lookup[str(raw_combo_key)] = cluster
                selected_window_count += int(cluster_row.get("selected_window_count") or 0)
            if isinstance(cluster, dict):
                cluster["unique_set_count"] = cluster.get("size", len(cluster_rows))
                cluster["selected_window_count"] = selected_window_count
        enriched_grouped_rows: List[Dict[str, Any]] = []
        for group in grouped_rows:
            cluster = cluster_lookup.get(str(group.get("combo_key")))
            enriched_grouped_rows.append(
                {
                    **group,
                    "representative_type": "IS Window Optimum",
                    "source": "Walk-Forward IS optimization",
                    "cluster_id": cluster.get("cluster_id") if cluster else None,
                    "cluster_size": cluster.get("unique_set_count") if cluster else None,
                    "local_plateau_score": None,
                    "candidate_key": group.get("combo_key"),
                    "wfa_pack_inclusion_reason": group.get("selection_evidence"),
                }
            )

        workflow_values = {
            str(row.get("workflow") or "").strip()
            for row in rows
            if str(row.get("workflow") or "").strip()
        }
        batch_workflow = (
            "rolling_validation"
            if workflow_values == {"rolling_validation"}
            else "window_is_optimization"
        )
        windowing_metadata = self._wfa_windowing_metadata(df, rows, wfa_metadata)
        selection_constraints_metadata = self._wfa_selection_constraints_metadata(df, rows, wfa_metadata)
        candidate_budget_metadata = self._wfa_candidate_budget_metadata(df, rows, wfa_metadata)
        batch_metadata = {
            "workflow": batch_workflow,
            "source_workflows": sorted(workflow_values),
            "row_contract": "selected_optimum_per_window",
            "source_run_id": linked_backtest_run_ids[0] if len(linked_backtest_run_ids) == 1 else None,
            "linked_backtest_run_ids": linked_backtest_run_ids,
            "review_mode": "run_center_wfa",
            "pack_strategy": None,
            "candidate_count": len(grouped_rows),
            "source_row_count": source_row_count,
            "selected_row_count": len(rows),
            "diagnostic_row_count": len(diagnostic_rows),
            "diagnostic_artifacts": diagnostic_artifacts,
            "legacy_grid_detected": False,
            "windowing": windowing_metadata,
            "selection_constraints": selection_constraints_metadata,
            "candidate_budget": candidate_budget_metadata,
        }
        metric_columns = [
            col
            for col in metric_priority
            if col in df.columns and pd.to_numeric(df[col], errors="coerce").notna().any()
        ]
        if rows:
            timeline_df = df.groupby("window_id", as_index=False)[metric_columns].mean(
                numeric_only=True
            )
        else:
            timeline_df = pd.DataFrame(columns=["window_id", *metric_columns])
        date_fields = [
            ("train_start_date", ["train_start_date", "train_start"]),
            ("train_end_date", ["train_end_date", "train_end"]),
            ("test_start_date", ["test_start_date", "test_start"]),
            ("test_end_date", ["test_end_date", "test_end"]),
        ]
        for output_field, candidates in date_fields:
            field = next((candidate for candidate in candidates if candidate in df.columns), "")
            if field:
                first_values = (
                    df.groupby("window_id", as_index=False)[[field]]
                    .first()
                    .rename(columns={field: f"__{output_field}"})
                )
                timeline_df = timeline_df.merge(first_values, on="window_id", how="left")
        timeline: List[Dict[str, Any]] = []
        for record in self._records_from_frame(timeline_df):
            base_record = {
                key: value for key, value in record.items() if not str(key).startswith("__")
            }
            timeline.append(
                {
                    **base_record,
                    "train_start_date": self._to_iso(record.get("__train_start_date")),
                    "train_end_date": self._to_iso(record.get("__train_end_date")),
                    "test_start_date": self._to_iso(record.get("__test_start_date")),
                    "test_end_date": self._to_iso(record.get("__test_end_date")),
                }
            )
        portfolio_window_summary = self._wfa_portfolio_window_summary(rows)
        payload = {
            "schema_version": WFA_DASHBOARD_SCHEMA_VERSION,
            "contract_id": "lo2cin4bt-app-wfa-dashboard-payload-v1",
            "run_id": run_id,
            "objective": objective,
            "strategy_summary": self._strategy_summary(run_id),
            "rows": rows,
            "combo_groups": enriched_grouped_rows,
            "cluster_summary": cluster_summary,
            "timeline": timeline,
            "portfolio_window_summary": portfolio_window_summary,
            "batch_metadata": batch_metadata,
            "diagnostic_rows": diagnostic_rows[:500],
            "truth_source_policy": self._truth_source_policy(),
            "truth_warnings": sorted(
                {
                    str(value)
                    for row in rows
                    if isinstance(row, dict)
                    for value in row.get("warnings", [])
                    if value not in (None, "")
                }
            ),
            "generated_at": self._now_iso(),
            "artifact_source_refs": [str(wfa_path)],
        }
        self._write_json(path, payload)
        return path

    def ensure_statanalyser_summary_payload(
        self,
        run_id: str,
        *,
        force: bool = False,
    ) -> Path:
        path = self._chart_path(run_id, "statanalyser_summary_payload.json")
        if path.exists() and not force:
            return path
        summary_path = self._snapshot_path(run_id, "statanalyser_summary.json")
        if not summary_path.exists():
            raise FileNotFoundError("statanalyser summary snapshot missing")
        payload = {
            "schema_version": "1.0",
            "contract_id": "lo2cin4bt-app-statanalyser-summary-payload-v1",
            "run_id": run_id,
            "summary": self._load_json(summary_path, {}),
            "generated_at": self._now_iso(),
            "artifact_source_refs": [str(summary_path)],
        }
        self._write_json(path, payload)
        return path

    def ensure_ai_readable_output(
        self,
        run_id: str,
        *,
        module: Optional[str] = None,
        force: bool = True,
    ) -> Path:
        path = self.registry.build_run_paths(run_id)["ai_readable_output"]
        if path.exists() and not force:
            return path
        payload = self._build_ai_readable_output(run_id, module=module)
        self._write_json(path, payload)
        self._register_ai_readable_output_artifact(run_id, path)
        return path

    def _build_ai_readable_output(
        self,
        run_id: str,
        *,
        module: Optional[str] = None,
    ) -> Dict[str, Any]:
        paths = self.registry.build_run_paths(run_id)
        registry_entry = self.registry.load_registry_entry(run_id)
        stage_status = self.registry.load_stage_status(run_id)
        artifact_manifest = self.registry.load_artifact_manifest(run_id)
        source_payloads, payload_index = self._load_ai_payload_directory(
            paths["chart_payload_dir"]
        )
        snapshot_payloads, snapshot_index = self._load_ai_snapshot_payloads(
            paths["snapshot_dir"]
        )
        artifact_index, artifact_summary, table_profiles, json_profiles = (
            self._build_ai_artifact_profiles(artifact_manifest)
        )
        metric_field_catalog = self._build_ai_metric_field_catalog(
            {
                "source_payloads": source_payloads,
                "snapshot_payloads": snapshot_payloads,
                "artifact_table_profiles": table_profiles,
                "artifact_json_profiles": json_profiles,
            }
        )
        resolved_module = module or str(registry_entry.get("module", "") or "")
        return {
            "schema_version": AI_READABLE_OUTPUT_SCHEMA_VERSION,
            "contract_id": "lo2cin4bt-app-ai-readable-output-v1",
            "run_id": run_id,
            "module": resolved_module,
            "generated_at": self._now_iso(),
            "auto_inclusion_policy": {
                "chart_payloads": "Every JSON file under this run's chart_payloads directory is embedded under source_payloads.",
                "snapshots": "Every direct JSON snapshot for this run is embedded under snapshot_payloads.",
                "artifacts": "The full artifact manifest is included, and ready table artifacts are schema-profiled by artifact type.",
                "future_metrics": (
                    "New performance scores are automatically present when they appear "
                    "as JSON payload fields or artifact table columns."
                ),
            },
            "review_guidance": {
                "primary_inputs": [
                    "run_registry",
                    "stage_status",
                    "artifact_manifest",
                    "source_payloads",
                    "artifact_table_profiles",
                    "metric_field_catalog",
                ],
                "do_not_infer": (
                    "Do not treat absent metrics as zero. Report them as missing or not generated."
                ),
                "recommended_review_order": [
                    "data health and run status",
                    "strategy summary and execution plan",
                    "headline performance and benchmark comparison",
                    "parameter or WFA robustness evidence",
                    "risk diagnostics and trade/allocation diagnostics",
                    "missing artifacts, warnings, and unsupported views",
                ],
            },
            "source_paths": {
                "run_registry": str(paths["run_registry"]),
                "artifact_manifest": str(paths["artifact_manifest"]),
                "stage_status": str(paths["stage_status"]),
                "run_snapshots": str(paths["snapshot_dir"]),
                "chart_payloads": str(paths["chart_payload_dir"]),
                "ai_readable_output": str(paths["ai_readable_output"]),
            },
            "run_registry": registry_entry,
            "stage_status": stage_status,
            "artifact_manifest": artifact_manifest,
            "artifact_summary": artifact_summary,
            "artifact_index": artifact_index,
            "payload_index": payload_index,
            "snapshot_index": snapshot_index,
            "source_payloads": source_payloads,
            "snapshot_payloads": snapshot_payloads,
            "artifact_table_profiles": table_profiles,
            "artifact_json_profiles": json_profiles,
            "metric_field_catalog": metric_field_catalog,
        }

    def _load_ai_payload_directory(self, directory: Path) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
        payloads: Dict[str, Any] = {}
        index: List[Dict[str, Any]] = []
        if not directory.exists():
            return payloads, index
        for path in sorted(directory.glob("*.json")):
            loaded = self._load_json(path, {})
            loaded = self._materialize_chart_storage(directory.name, loaded)
            key = path.stem
            payloads[key] = loaded
            index.append(self._json_payload_index_row(path, loaded))
        return payloads, index

    def _materialize_chart_storage(self, run_id: str, loaded: Any) -> Any:
        if not isinstance(loaded, dict):
            return loaded
        store = SharedChartSeriesStore(self.registry)
        schema_version = loaded.get("schema_version")
        if schema_version == "plot_bundle_index.v1":
            return store.materialize_plot_bundle(run_id, loaded)
        if schema_version == "metrics_overview_index.v1":
            return store.materialize_metrics_overview(run_id, loaded)
        if schema_version == "backtest_detail_index.v1":
            return store.materialize_backtest_detail(run_id, loaded)
        return loaded

    def _load_ai_snapshot_payloads(self, snapshot_dir: Path) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
        payloads: Dict[str, Any] = {}
        index: List[Dict[str, Any]] = []
        if not snapshot_dir.exists():
            return payloads, index
        for path in sorted(snapshot_dir.glob("*.json")):
            loaded = self._load_json(path, {})
            key = path.stem
            payloads[key] = loaded
            index.append(self._json_payload_index_row(path, loaded))
        return payloads, index

    def _json_payload_index_row(self, path: Path, loaded: Any) -> Dict[str, Any]:
        top_level_keys = sorted(loaded.keys()) if isinstance(loaded, dict) else []
        return {
            "name": path.name,
            "path": str(path),
            "size_bytes": self._file_size(path),
            "schema_version": loaded.get("schema_version") if isinstance(loaded, dict) else None,
            "contract_id": loaded.get("contract_id") if isinstance(loaded, dict) else None,
            "top_level_keys": top_level_keys,
        }

    def _build_ai_artifact_profiles(
        self,
        artifact_manifest: Dict[str, Any],
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        artifacts = artifact_manifest.get("artifacts", []) if isinstance(artifact_manifest, dict) else []
        if not isinstance(artifacts, list):
            artifacts = []
        artifact_index: List[Dict[str, Any]] = []
        summary_by_type: Dict[str, Dict[str, Any]] = {}
        profile_counts: Dict[tuple[str, str], int] = {}
        table_profiles: List[Dict[str, Any]] = []
        json_profiles: List[Dict[str, Any]] = []

        for item in artifacts:
            if not isinstance(item, dict):
                continue
            artifact_type = str(item.get("artifact_type", "") or "unknown")
            status = str(item.get("status", "") or "")
            path_text = str(item.get("path", "") or "")
            path = self._safe_manifest_artifact_path(path_text)
            exists = bool(path is not None)
            suffix = path.suffix.lower() if path is not None else (Path(path_text).suffix.lower() if path_text else "")
            size_bytes = self._file_size(path) if path is not None else None
            artifact_index.append(
                {
                    "artifact_type": artifact_type,
                    "status": status,
                    "path": path_text,
                    "exists": exists,
                    "extension": suffix,
                    "size_bytes": size_bytes,
                    "content_contract": item.get("content_contract"),
                    "source_stage": item.get("source_stage"),
                }
            )
            bucket = summary_by_type.setdefault(
                artifact_type,
                {"count": 0, "ready": 0, "missing": 0, "failed": 0, "extensions": {}},
            )
            bucket["count"] += 1
            if status == "ready":
                bucket["ready"] += 1
            elif status == "missing":
                bucket["missing"] += 1
            elif status == "failed":
                bucket["failed"] += 1
            extensions = bucket["extensions"]
            extensions[suffix or "none"] = int(extensions.get(suffix or "none", 0)) + 1

            if not exists or status != "ready" or artifact_type == "ai_readable_output_json":
                continue
            profile_key = (artifact_type, suffix)
            sampled = profile_counts.get(profile_key, 0)
            if sampled >= AI_REVIEW_ARTIFACT_PROFILE_LIMIT_PER_TYPE:
                continue
            if path is None:
                continue
            if suffix in {".parquet", ".csv"}:
                table_profiles.append(self._profile_table_artifact(path, item))
                profile_counts[profile_key] = sampled + 1
            elif suffix == ".json":
                json_profiles.append(self._profile_json_artifact(path, item))
                profile_counts[profile_key] = sampled + 1

        return (
            artifact_index,
            {
                "total": len(artifact_index),
                "by_type": summary_by_type,
                "profile_limit_per_type": AI_REVIEW_ARTIFACT_PROFILE_LIMIT_PER_TYPE,
            },
            table_profiles,
            json_profiles,
        )

    def _profile_table_artifact(self, path: Path, manifest_item: Dict[str, Any]) -> Dict[str, Any]:
        profile: Dict[str, Any] = {
            "artifact_type": manifest_item.get("artifact_type"),
            "path": str(path),
            "extension": path.suffix.lower(),
            "status": "profiled",
        }
        try:
            if path.suffix.lower() == ".parquet":
                frame = pd.read_parquet(path)
            else:
                frame = pd.read_csv(path)
            profile.update(
                {
                    "row_count": int(len(frame)),
                    "column_count": int(len(frame.columns)),
                    "columns": [str(column) for column in frame.columns],
                    "dtypes": {str(column): str(dtype) for column, dtype in frame.dtypes.items()},
                }
            )
            numeric_summary: Dict[str, Any] = {}
            for column in frame.columns:
                series = pd.to_numeric(frame[column], errors="coerce")
                if series.notna().sum() == 0:
                    continue
                finite = series.dropna()
                numeric_summary[str(column)] = {
                    "count": int(finite.count()),
                    "min": self._finite_or_none(finite.min()),
                    "max": self._finite_or_none(finite.max()),
                    "mean": self._finite_or_none(finite.mean()),
                    "last": self._finite_or_none(finite.iloc[-1]) if not finite.empty else None,
                }
            profile["numeric_summary"] = numeric_summary
        except Exception as exc:
            profile.update({"status": "profile_failed", "error": str(exc)})
        return profile

    def _profile_json_artifact(self, path: Path, manifest_item: Dict[str, Any]) -> Dict[str, Any]:
        loaded = self._load_json(path, {})
        numeric_fields = self._build_ai_metric_field_catalog(loaded)
        return {
            "artifact_type": manifest_item.get("artifact_type"),
            "path": str(path),
            "extension": path.suffix.lower(),
            "status": "profiled" if loaded else "empty_or_unreadable",
            "top_level_keys": sorted(loaded.keys()) if isinstance(loaded, dict) else [],
            "numeric_field_catalog": numeric_fields,
        }

    def _build_ai_metric_field_catalog(self, payload: Any) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}
        self._collect_ai_numeric_fields(payload, "", fields)
        rows = [
            {"path": path, "sample_value": value}
            for path, value in list(fields.items())[:AI_REVIEW_NUMERIC_FIELD_LIMIT]
        ]
        return {
            "numeric_field_count": len(fields),
            "included_count": len(rows),
            "truncated": len(fields) > AI_REVIEW_NUMERIC_FIELD_LIMIT,
            "fields": rows,
        }

    def _collect_ai_numeric_fields(
        self,
        value: Any,
        prefix: str,
        fields: Dict[str, Any],
    ) -> None:
        if len(fields) >= AI_REVIEW_NUMERIC_FIELD_LIMIT:
            return
        if isinstance(value, dict):
            for key, child in value.items():
                child_key = str(key)
                child_prefix = f"{prefix}.{child_key}" if prefix else child_key
                self._collect_ai_numeric_fields(child, child_prefix, fields)
                if len(fields) >= AI_REVIEW_NUMERIC_FIELD_LIMIT:
                    return
        elif isinstance(value, list):
            for child in value[:AI_REVIEW_LIST_SAMPLE_LIMIT]:
                child_prefix = f"{prefix}[]" if prefix else "[]"
                self._collect_ai_numeric_fields(child, child_prefix, fields)
                if len(fields) >= AI_REVIEW_NUMERIC_FIELD_LIMIT:
                    return
        elif isinstance(value, bool):
            return
        elif isinstance(value, (int, float)):
            if math.isfinite(float(value)) and prefix and prefix not in fields:
                fields[prefix] = value

    def _register_ai_readable_output_artifact(self, run_id: str, path: Path) -> None:
        manifest = self.registry.load_artifact_manifest(run_id)
        if not isinstance(manifest, dict):
            manifest = {"schema_version": "1.0", "artifacts": []}
        artifacts = manifest.get("artifacts", [])
        if not isinstance(artifacts, list):
            artifacts = []
        artifacts = [
            item
            for item in artifacts
            if not (
                isinstance(item, dict)
                and item.get("artifact_type") == "ai_readable_output_json"
            )
        ]
        artifacts.append(
            {
                "artifact_type": "ai_readable_output_json",
                "path": str(path),
                "required_by_pages": ["results_library"],
                "status": "ready",
                "generated_at": self._now_iso(),
                "content_contract": "lo2cin4bt-app-ai-readable-output-v1",
                "source_stage": "app_export",
                "optional": True,
                "notes": "AI-readable aggregate pack assembled from app payloads, snapshots, and artifact profiles.",
            }
        )
        manifest["artifacts"] = artifacts
        self.registry.write_artifact_manifest(run_id, manifest)

        registry_entry = self.registry.load_registry_entry(run_id)
        if isinstance(registry_entry, dict) and registry_entry:
            registry_entry["artifacts_total"] = len(artifacts)
            registry_entry["artifacts_ready"] = sum(
                1
                for item in artifacts
                if isinstance(item, dict) and item.get("status") == "ready"
            )
            self.registry.write_registry_entry(registry_entry)

    @staticmethod
    def _file_size(path: Path) -> Optional[int]:
        try:
            return int(path.stat().st_size)
        except Exception:
            return None


















    def _build_pcp_dimensions(
        self,
        rows: List[Dict[str, Any]],
        param_axes: List[str],
        metric_axes: List[str],
    ) -> List[Dict[str, Any]]:
        dimensions: List[Dict[str, Any]] = []
        for axis in [*param_axes, *metric_axes]:
            values = [row.get(axis) for row in rows]
            numeric_values = [self._as_float(value) for value in values if value is not None]
            numeric_values = [value for value in numeric_values if not math.isnan(value)]
            dimension: Dict[str, Any] = {
                "key": axis,
                "label": axis,
                "values": values,
                "kind": "metric" if axis in metric_axes else "parameter",
            }
            if numeric_values:
                dimension["range"] = [min(numeric_values), max(numeric_values)]
            dimensions.append(dimension)
        return dimensions

    @staticmethod
    def _mean(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
        numeric = [
            AppPayloadService._as_float(row.get(key))
            for row in rows
            if not math.isnan(AppPayloadService._as_float(row.get(key)))
        ]
        if not numeric:
            return None
        return float(sum(numeric) / len(numeric))

    @staticmethod
    def _std(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
        numeric = [
            AppPayloadService._as_float(row.get(key))
            for row in rows
            if not math.isnan(AppPayloadService._as_float(row.get(key)))
        ]
        if not numeric:
            return None
        mean = sum(numeric) / len(numeric)
        variance = sum((value - mean) ** 2 for value in numeric) / len(numeric)
        return float(variance ** 0.5)

    @staticmethod
    def _semantic_combo_label(combo: Dict[str, Any]) -> str:
        if not isinstance(combo, dict) or not combo:
            return "Combo not recorded"
        return " | ".join(f"{key}={combo[key]}" for key in sorted(combo.keys()))

    @classmethod
    def _portfolio_strategy_table_label(
        cls,
        *,
        run_id: str,
        metadata: Dict[str, Any],
        config: Dict[str, Any],
        strategy_id: str,
        params: Dict[str, Any],
    ) -> str:
        asset_label = cls._portfolio_asset_label(metadata, config, strategy_id)
        display_rules = cls._strategy_rule_display_overrides(metadata, config)
        mode_label = (
            display_rules.get("mode_label")
            or cls._portfolio_strategy_identity_label(strategy_id, asset_label, params, config)
        )
        param_parts = cls._semantic_combo_parts(params)
        parts = [
            part.strip()
            for part in [asset_label, mode_label, *param_parts]
            if isinstance(part, str) and part.strip()
        ]
        if not parts:
            return f"Strategy {str(run_id or strategy_id)[:8]}"
        deduped: List[str] = []
        seen: set[str] = set()
        for part in parts:
            key = part.lower()
            if key in seen:
                continue
            deduped.append(part)
            seen.add(key)
        return " | ".join(deduped)

    @classmethod
    def _portfolio_asset_label(
        cls,
        metadata: Dict[str, Any],
        config: Dict[str, Any],
        strategy_id: str,
    ) -> str:
        universe = config.get("universe", {}) if isinstance(config.get("universe"), dict) else {}
        symbols = universe.get("symbols", []) if isinstance(universe.get("symbols"), list) else []
        clean_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
        if clean_symbols:
            return "-".join(clean_symbols)
        summary = metadata.get("summary", {}) if isinstance(metadata.get("summary"), dict) else {}
        summary_symbols = summary.get("symbols", []) if isinstance(summary.get("symbols"), list) else []
        clean_summary_symbols = [
            str(symbol).strip().upper() for symbol in summary_symbols if str(symbol).strip()
        ]
        if clean_summary_symbols:
            return "-".join(clean_summary_symbols)
        known_symbols = {
            "agg",
            "btc",
            "dia",
            "dbc",
            "eem",
            "efa",
            "eth",
            "gld",
            "iau",
            "ief",
            "iwm",
            "qqq",
            "qqqm",
            "shy",
            "slv",
            "smh",
            "soxx",
            "spy",
            "sso",
            "tlt",
            "tmf",
            "ung",
            "upro",
            "usd",
            "usdt",
            "uso",
            "voo",
        }
        leading_symbols: List[str] = []
        for token in re.split(r"[^A-Za-z0-9]+", str(strategy_id or "")):
            clean = token.strip().lower()
            if clean in known_symbols:
                leading_symbols.append(clean.upper())
                continue
            break
        if leading_symbols:
            return "-".join(leading_symbols)
        first_token = str(strategy_id or "").split("_", 1)[0].strip()
        return first_token.upper() if first_token else "Portfolio"

    @classmethod
    def _portfolio_strategy_identity_label(
        cls,
        strategy_id: str,
        asset_label: str,
        params: Dict[str, Any],
        config: Dict[str, Any],
    ) -> str:
        tokens = [token for token in re.split(r"[^A-Za-z0-9]+", str(strategy_id or "")) if token]
        filtered = cls._filtered_strategy_id_tokens(tokens, asset_label, params)
        if filtered:
            return cls._humanize_strategy_tokens(filtered)

        platform = config.get("platform", {}) if isinstance(config.get("platform"), dict) else {}
        strategy_profile_id = str(
            platform.get("strategy_profile_id") or config.get("strategy_profile_id") or ""
        ).strip()
        strategy_preset_id = str(
            platform.get("strategy_preset_id") or config.get("strategy_preset_id") or ""
        ).strip()
        strategy_mode_id = str(
            platform.get("strategy_mode_id") or config.get("strategy_mode_id") or ""
        ).strip()
        identity_label = cls._render_strategy_identity_label_static(
            strategy_profile_id=strategy_profile_id,
            strategy_preset_id=strategy_preset_id,
            strategy_mode_id=strategy_mode_id,
        )
        if identity_label:
            return identity_label

        signals = config.get("signals", {}) if isinstance(config.get("signals"), dict) else {}
        entry = signals.get("entry", {}) if isinstance(signals.get("entry"), dict) else {}
        field_tokens = " ".join(
            str(entry.get(key, ""))
            for key in ("field", "right_field", "op")
        ).lower()
        if "ma" in field_tokens or "moving" in field_tokens:
            return "MA Cross"
        return "Portfolio Strategy"

    @classmethod
    def _filtered_strategy_id_tokens(
        cls,
        tokens: List[str],
        asset_label: str,
        params: Dict[str, Any],
    ) -> List[str]:
        if not tokens:
            return []
        lowered = [token.lower() for token in tokens]
        param_start = len(tokens)
        if isinstance(params, dict):
            for key in params.keys():
                key_tokens = [token for token in str(key).lower().split("_") if token]
                if not key_tokens:
                    continue
                for index in range(0, len(lowered) - len(key_tokens) + 1):
                    if lowered[index : index + len(key_tokens)] == key_tokens:
                        param_start = min(param_start, index)
        core_tokens = tokens[:param_start]

        asset_tokens = {
            token.lower()
            for token in re.split(r"[^A-Za-z0-9]+", str(asset_label or ""))
            if token.strip()
        }
        stop_tokens = {
            "backtest",
            "demo",
            "example",
            "matrix",
            "portfolio",
            "price",
            "prices",
            "run",
            "selection",
            "single",
            "strategy",
            "sweep",
            "test",
            "yf",
            "yfinance",
        }
        return [
            token
            for token in core_tokens
            if token.lower() not in asset_tokens
            and token.lower() not in stop_tokens
            and not re.fullmatch(r"v\d+|\d+", token.lower())
        ]

    @staticmethod
    def _semantic_combo_parts(combo: Dict[str, Any]) -> List[str]:
        if not isinstance(combo, dict) or not combo:
            return []
        preferred_order = [
            "short_ma",
            "long_ma",
            "fast_ma",
            "slow_ma",
            "entry_ma",
            "exit_ma",
            "lookback",
            "sma_period",
            "threshold",
            "vix_threshold",
            "vix_max",
            "target_weight",
        ]
        order_rank = {key: index for index, key in enumerate(preferred_order)}
        original_rank = {str(key): index for index, key in enumerate(combo.keys())}
        keys = sorted(
            [str(key) for key in combo.keys()],
            key=lambda key: (
                order_rank.get(key, len(preferred_order) + original_rank.get(key, 0)),
                original_rank.get(key, 0),
            ),
        )
        return [f"{key}={combo[key]}" for key in keys]

    @staticmethod
    def _humanize_strategy_tokens(tokens: List[str]) -> str:
        acronyms = {
            "adx",
            "atr",
            "btc",
            "ema",
            "etf",
            "gld",
            "ma",
            "macd",
            "ohlcv",
            "qqq",
            "rsi",
            "sma",
            "spy",
            "usdt",
            "vix",
            "voo",
            "wfa",
        }
        words: List[str] = []
        for token in tokens:
            clean = str(token).strip()
            if not clean:
                continue
            lower = clean.lower()
            if lower in acronyms:
                words.append(lower.upper())
            else:
                words.append(lower[:1].upper() + lower[1:])
        return " ".join(words).strip()

    @classmethod
    def _clean_strategy_id_label(cls, strategy_id: Any, params: Dict[str, Any]) -> str:
        text = str(strategy_id or "").strip()
        if not text:
            return ""
        return cls._portfolio_strategy_table_label(
            run_id=text,
            metadata={},
            config={},
            strategy_id=text,
            params=params if isinstance(params, dict) else {},
        )

    @staticmethod
    def _looks_like_internal_strategy_label(label: Any) -> bool:
        text = str(label or "").strip()
        if not text or "_" not in text:
            return False
        return bool(re.fullmatch(r"[A-Za-z0-9_-]+", text))

    def _build_category_map(self, rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for category_id, config in CATEGORY_MAP.items():
            key = config["key"]
            sorted_rows = sorted(
                rows,
                key=lambda item: self._sort_value(
                    item.get(key),
                    ascending=bool(config["ascending"]),
                ),
                reverse=not bool(config["ascending"]),
            )
            mapping[category_id] = [
                str(item.get("backtest_id")) for item in sorted_rows[:20]
            ]
        return mapping

    def _infer_param_axes(self, rows: List[Dict[str, Any]]) -> List[str]:
        axis_counts: Dict[str, int] = {}
        for row in rows:
            combo = row.get("semantic_combo", {})
            if not isinstance(combo, dict):
                continue
            for key, value in combo.items():
                if value is None:
                    continue
                axis_counts[key] = axis_counts.get(key, 0) + 1
        ranked = sorted(axis_counts.items(), key=lambda item: (-item[1], item[0]))
        return [key for key, _count in ranked]

    def _metric_float(self, metric_row: Dict[str, Any], key: str) -> float:
        return self._as_float(metric_row.get(METRIC_KEY_MAP[key]))

    def _extract_last_trade_time(self, subset: pd.DataFrame) -> Optional[str]:
        if subset.empty or "Trade_action" not in subset.columns:
            return None
        actions = self._required_numeric_column(subset, "Trade_action").astype(int)
        closed = subset[actions == 4]
        if closed.empty:
            return None
        return self._to_iso(closed.iloc[-1]["Time"])

    def _build_last_trade_time_map(self, trade_df: pd.DataFrame) -> Dict[str, Optional[str]]:
        if trade_df.empty:
            return {}
        action_series = self._required_numeric_column(
            trade_df,
            "Trade_action",
        ).astype(int)
        closed = trade_df.loc[action_series == 4, ["Backtest_id", "Time", "Close_time"]].copy()
        if closed.empty:
            return {}
        closed["event_time"] = closed["Close_time"].where(closed["Close_time"].notna(), closed["Time"])
        closed = closed.sort_values(["Backtest_id", "event_time"])
        latest = closed.groupby("Backtest_id", sort=False)["event_time"].last()
        return {str(backtest_id): self._to_iso(value) for backtest_id, value in latest.items()}

    def _sort_value(self, value: Any, *, ascending: bool) -> float:
        cast = self._as_float(value)
        if math.isnan(cast):
            return float("inf") if ascending else float("-inf")
        return cast

    def _build_backtest_label(self, backtest_id: str, combo: Dict[str, Any]) -> str:
        semantic_combo = combo.get("semantic_combo", {}) if isinstance(combo, dict) else {}
        strategy_display_label = combo.get("strategy_display_label") if isinstance(combo, dict) else None
        if (
            isinstance(strategy_display_label, str)
            and strategy_display_label.strip()
            and not strategy_display_label.startswith("Strategy ")
        ):
            if self._looks_like_internal_strategy_label(strategy_display_label):
                return self._clean_strategy_id_label(strategy_display_label, semantic_combo)
            return strategy_display_label
        semantic_run_label = combo.get("semantic_run_label") if isinstance(combo, dict) else None
        if isinstance(semantic_run_label, str) and semantic_run_label.strip():
            if self._looks_like_internal_strategy_label(semantic_run_label):
                return self._clean_strategy_id_label(semantic_run_label, semantic_combo)
            return semantic_run_label
        if isinstance(semantic_combo, dict) and semantic_combo:
            return " | ".join(self._semantic_combo_parts(semantic_combo))
        strategy_id = combo.get("strategy_id") if isinstance(combo, dict) else None
        if (
            isinstance(strategy_id, str)
            and strategy_id.strip()
            and not strategy_id.startswith("_")
        ):
            if self._looks_like_internal_strategy_label(strategy_id):
                return self._clean_strategy_id_label(strategy_id, semantic_combo)
            return str(strategy_id)
        return f"Strategy {str(backtest_id)[:8]}"

    def _label_source(self, combo: Dict[str, Any]) -> str:
        if not isinstance(combo, dict) or not combo:
            return "internal_id_fallback"
        if combo.get("strategy_display_label") and not str(
            combo.get("strategy_display_label")
        ).startswith("Strategy "):
            return "strategy_display_label"
        if combo.get("semantic_run_label"):
            return "semantic_run_label"
        if combo.get("semantic_combo"):
            return "semantic_combo"
        if combo.get("strategy_id") and not str(combo.get("strategy_id")).startswith("_"):
            return "strategy_id"
        return "internal_id_fallback"

    def _load_backtest_index_map(self, run_id: str) -> Dict[str, Dict[str, Any]]:
        index_path = self._snapshot_path(run_id, "backtest_result_index.json")
        payload = self._load_json(index_path, {})
        rows = payload.get("backtests", []) if isinstance(payload, dict) else []
        mapping: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            mapping[str(row.get("backtest_id", ""))] = row
        return mapping

    def _load_future_live_search_config(self, run_id: str) -> Dict[str, Any]:
        wfa_dir = self.repo_root / "workspace" / "wfa"
        candidates = [
            wfa_dir / f"wfa-shortlist-{run_id}.user.json",
            wfa_dir / "wfa-latest.user.json",
        ]
        for path in candidates:
            config = self._load_json(path, None)
            if not isinstance(config, dict):
                continue
            optimizer = config.get("wfa_config", {}).get("optimizer", {})
            if not isinstance(optimizer, dict) or not optimizer:
                continue
            return {
                "label": "Future live-search config",
                "source_filename": path.name,
                "config_path": str(path),
                "mode": str(optimizer.get("mode", "") or "").strip() or None,
                "sampler": str(optimizer.get("sampler", "") or "").strip() or None,
                "n_trials": optimizer.get("n_trials"),
                "n_startup_trials": optimizer.get("n_startup_trials"),
                "multivariate": optimizer.get("multivariate"),
                "timeout_seconds": optimizer.get("timeout_seconds"),
                "note": "This is configuration for a future live search, not something consumed by this completed sweep.",
                "ranking": config.get("wfa_config", {}).get("ranking", {}),
                "acceptance": config.get("wfa_config", {}).get("acceptance", {}),
                "robust_selection": config.get("wfa_config", {}).get("robust_selection", {}),
            }
        return {}

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        merged = copy.deepcopy(base or {})
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = AppPayloadService._deep_merge(merged.get(key, {}), value)
            else:
                merged[key] = value
        return merged

    def _match_backtest_ref(
        self,
        execution_plan_hash: Any,
        semantic_combo: Dict[str, Any],
    ) -> Optional[Dict[str, str]]:
        target_hash = str(execution_plan_hash or "")
        if not target_hash:
            return None
        for run in self.registry.list_runs(module="autorunner"):
            if str(run.get("status", "")) not in {"completed", "partial"}:
                continue
            index_map = self._load_backtest_index_map(str(run.get("run_id")))
            for backtest_id, row in index_map.items():
                if str(row.get("execution_plan_hash", "")) != target_hash:
                    continue
                if row.get("semantic_combo", {}) == semantic_combo:
                    return {
                        "run_id": str(run.get("run_id")),
                        "backtest_id": backtest_id,
                }
        return None

    @staticmethod
    def _record_backtest_ref(record: Dict[str, Any]) -> Optional[Dict[str, str]]:
        run_id = str(record.get("linked_backtest_run_id") or "").strip()
        backtest_id = str(record.get("linked_backtest_id") or "").strip()
        if run_id and backtest_id:
            return {"run_id": run_id, "backtest_id": backtest_id}
        linked = record.get("linked_backtest")
        if isinstance(linked, dict):
            run_id = str(linked.get("run_id") or "").strip()
            backtest_id = str(linked.get("backtest_id") or "").strip()
            if run_id and backtest_id:
                return {"run_id": run_id, "backtest_id": backtest_id}
        return None

    @staticmethod
    def _parse_semantic_combo(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            if not raw:
                raise ValueError("WFA semantic_combo cannot be empty")
            return raw
        if isinstance(raw, str) and raw.strip():
            text = raw.strip()
            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    "WFA semantic_combo must be a JSON object"
                ) from exc
            if not isinstance(value, dict) or not value:
                raise ValueError("WFA semantic_combo must be a non-empty JSON object")
            return value
        raise ValueError("WFA semantic_combo is required")

    @staticmethod
    def _parse_json_object(raw: Any) -> Dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return {}
        if isinstance(raw, str) and raw.strip():
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("WFA payload field must be a JSON object") from exc
            if not isinstance(value, dict):
                raise ValueError("WFA payload field must be a JSON object")
            return value
        raise ValueError("WFA payload field must be a JSON object")

    def _wfa_portfolio_window_summary(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        allocation_by_window: List[Dict[str, Any]] = []
        contribution_by_window: List[Dict[str, Any]] = []
        asset_summary: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            snapshot = row.get("oos_portfolio", {}) if isinstance(row.get("oos_portfolio"), dict) else {}
            allocation = snapshot.get("allocation", []) if isinstance(snapshot.get("allocation"), list) else []
            contribution = snapshot.get("contribution", []) if isinstance(snapshot.get("contribution"), list) else []
            if allocation:
                allocation_by_window.append(
                    {
                        "window_id": row.get("window_id"),
                        "semantic_combo": row.get("semantic_combo", {}),
                        "test_start_date": row.get("test_start_date"),
                        "test_end_date": row.get("test_end_date"),
                        "avg_exposure": row.get("oos_avg_exposure"),
                        "avg_holdings": row.get("oos_avg_holdings"),
                        "active_rebalance_count": row.get("oos_rebalance_count"),
                        "risk_gate_event_count": row.get("oos_risk_gate_event_count"),
                        "weights": allocation,
                    }
                )
            if contribution:
                contribution_by_window.append(
                    {
                        "window_id": row.get("window_id"),
                        "semantic_combo": row.get("semantic_combo", {}),
                        "test_start_date": row.get("test_start_date"),
                        "test_end_date": row.get("test_end_date"),
                        "contributions": contribution,
                    }
                )
            for item in allocation:
                if not isinstance(item, dict):
                    continue
                asset = str(item.get("asset") or "").strip()
                if not asset:
                    continue
                bucket = asset_summary.setdefault(
                    asset,
                    {
                        "asset": asset,
                        "avg_weight_values": [],
                        "last_weight_values": [],
                        "active_windows": 0,
                        "return_contribution": 0.0,
                    },
                )
                avg_weight = self._finite_or_none(item.get("avg_weight"))
                last_weight = self._finite_or_none(item.get("last_weight"))
                if avg_weight is not None:
                    bucket["avg_weight_values"].append(avg_weight)
                    if abs(avg_weight) > 1e-12:
                        bucket["active_windows"] += 1
                if last_weight is not None:
                    bucket["last_weight_values"].append(last_weight)
            for item in contribution:
                if not isinstance(item, dict):
                    continue
                asset = str(item.get("asset") or "").strip()
                if not asset:
                    continue
                bucket = asset_summary.setdefault(
                    asset,
                    {
                        "asset": asset,
                        "avg_weight_values": [],
                        "last_weight_values": [],
                        "active_windows": 0,
                        "return_contribution": 0.0,
                    },
                )
                contribution_value = self._finite_or_none(
                    item.get("return_contribution")
                )
                if contribution_value is None:
                    raise ValueError(
                        "WFA asset contribution is missing or non-finite"
                    )
                bucket["return_contribution"] += contribution_value

        summary_rows: List[Dict[str, Any]] = []
        for bucket in asset_summary.values():
            avg_weights = bucket.pop("avg_weight_values", [])
            last_weights = bucket.pop("last_weight_values", [])
            bucket["mean_avg_weight"] = self._finite_or_none(
                sum(avg_weights) / len(avg_weights) if avg_weights else None
            )
            bucket["mean_last_weight"] = self._finite_or_none(
                sum(last_weights) / len(last_weights) if last_weights else None
            )
            bucket["return_contribution"] = self._finite_or_none(bucket.get("return_contribution"))
            summary_rows.append(bucket)
        summary_rows.sort(
            key=lambda item: abs(
                self._required_record_number(item, "return_contribution")
            ),
            reverse=True,
        )
        return {
            "is_portfolio_wfa": any(len(item.get("weights", [])) > 1 for item in allocation_by_window),
            "allocation_by_window": sorted(
                allocation_by_window,
                key=lambda item: int(item.get("window_id") or 0),
            ),
            "contribution_by_window": sorted(
                contribution_by_window,
                key=lambda item: int(item.get("window_id") or 0),
            ),
            "asset_summary": summary_rows,
        }

    def _artifact_path(self, run_id: str, artifact_type: str) -> Optional[Path]:
        manifest = self.registry.load_artifact_manifest(run_id)
        candidates: List[Path] = []
        snapshot_root = self.registry.resolve_run_paths(run_id)["snapshot_dir"].resolve()
        for artifact in manifest.get("artifacts", []) if isinstance(manifest, dict) else []:
            if artifact.get("artifact_type") != artifact_type:
                continue
            path = self._safe_manifest_artifact_path(str(artifact.get("path", "")))
            if path is not None:
                candidates.append(path)
        if not candidates:
            return None
        candidates.sort(
            key=lambda value: (
                0 if snapshot_root in value.resolve().parents else 1,
                "_audit" in value.name.lower(),
                "_metadata" in value.name.lower(),
                value.name.lower().endswith(".json"),
                value.name.lower(),
            )
        )
        return candidates[0]

    def _artifact_paths(self, run_id: str, artifact_type: str) -> List[Path]:
        manifest = self.registry.load_artifact_manifest(run_id)
        candidates: List[Path] = []
        snapshot_root = self.registry.resolve_run_paths(run_id)["snapshot_dir"].resolve()
        for artifact in manifest.get("artifacts", []) if isinstance(manifest, dict) else []:
            if artifact.get("artifact_type") != artifact_type:
                continue
            path = self._safe_manifest_artifact_path(str(artifact.get("path", "")))
            if path is not None:
                candidates.append(path)
        candidates.sort(
            key=lambda value: (
                0 if snapshot_root in value.resolve().parents else 1,
                "_audit" in value.name.lower(),
                "_metadata" in value.name.lower(),
                value.name.lower().endswith(".json"),
                value.name.lower(),
            )
        )
        return candidates

    def _safe_manifest_artifact_path(self, path_text: str) -> Optional[Path]:
        text = str(path_text or "").strip()
        if not text:
            return None
        try:
            path = Path(text)
            if not path.is_absolute():
                path = self.repo_root / path
            resolved = path.resolve()
            repo_root = self.repo_root.resolve()
            if resolved != repo_root and repo_root not in resolved.parents:
                return None
            if not resolved.exists() or not resolved.is_file():
                return None
            return resolved
        except Exception:
            return None

    def _wfa_sidecar_metadata(self, wfa_path: Path) -> Dict[str, Any]:
        stem = wfa_path.stem
        candidates = [wfa_path.with_name(f"{stem}_metadata.json")]
        if stem.endswith("_selected_optimum"):
            candidates.insert(0, wfa_path.with_name(f"{stem[:-len('_selected_optimum')]}_metadata.json"))
        candidates.extend(sorted(wfa_path.parent.glob("*metadata*.json")))
        for candidate in candidates:
            loaded = self._load_json(candidate, {})
            if (
                isinstance(loaded, dict)
                and loaded
                and (
                    loaded.get("row_contract") == "selected_optimum_per_window"
                    or "windowing" in loaded
                    or "selection_constraints" in loaded
                )
            ):
                return loaded
        return {}

    def _wfa_windowing_metadata(
        self,
        df: pd.DataFrame,
        rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        from_metadata = metadata.get("windowing", {}) if isinstance(metadata.get("windowing"), dict) else {}
        if from_metadata:
            return {**from_metadata, "metadata_source": "sidecar_metadata"}

        def first_number(column: str) -> Optional[float]:
            if column not in df.columns:
                return None
            series = pd.to_numeric(df[column], errors="coerce").dropna()
            return float(series.iloc[0]) if not series.empty else None

        train_sizes: List[int] = []
        test_sizes: List[int] = []
        for row in rows:
            train_start = pd.to_datetime(row.get("train_start_date"), errors="coerce")
            train_end = pd.to_datetime(row.get("train_end_date"), errors="coerce")
            test_start = pd.to_datetime(row.get("test_start_date"), errors="coerce")
            test_end = pd.to_datetime(row.get("test_end_date"), errors="coerce")
            if pd.notna(train_start) and pd.notna(train_end):
                train_start_date = cast(Any, train_start).date()
                train_end_date = cast(Any, train_end).date()
                train_sizes.append(max(1, int(np.busday_count(train_start_date, train_end_date)) + 1))
            if pd.notna(test_start) and pd.notna(test_end):
                test_start_date = cast(Any, test_start).date()
                test_end_date = cast(Any, test_end).date()
                test_sizes.append(max(1, int(np.busday_count(test_start_date, test_end_date)) + 1))
        inferred_train = int(round(float(np.median(train_sizes)))) if train_sizes else None
        inferred_test = int(round(float(np.median(test_sizes)))) if test_sizes else None
        return {
            "size_mode": "unknown",
            "sizing_source": "artifact_dates",
            "metadata_source": "artifact_dates",
            "effective_train_size": first_number("effective_train_size") or inferred_train,
            "effective_test_size": first_number("effective_test_size") or inferred_test,
            "effective_step_size": first_number("effective_step_size"),
            "actual_window_count": len(rows),
            "auto_indicators": {},
        }

    def _wfa_selection_constraints_metadata(
        self,
        df: pd.DataFrame,
        rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        from_metadata = (
            metadata.get("selection_constraints", {})
            if isinstance(metadata.get("selection_constraints"), dict)
            else {}
        )
        if from_metadata:
            return {**from_metadata, "metadata_source": "sidecar_metadata"}
        applied = any(bool(row.get("selection_constraints_applied")) for row in rows)
        pool_counts: List[float] = []
        total_counts: List[float] = []
        for row in rows:
            pool_count = self._finite_or_none(row.get("selection_pool_count"))
            if pool_count is not None:
                pool_counts.append(pool_count)
            total_count = self._finite_or_none(row.get("selection_pool_total_count"))
            if total_count is not None:
                total_counts.append(total_count)
        return {
            "enabled": applied,
            "metadata_source": "artifact_rows",
            "observed_min_pool_count": min(pool_counts) if pool_counts else None,
            "observed_max_total_count": max(total_counts) if total_counts else None,
        }

    def _wfa_candidate_budget_metadata(
        self,
        df: pd.DataFrame,
        rows: List[Dict[str, Any]],
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        fields = [
            "candidate_budget",
            "candidate_budget_applied",
            "candidate_budget_policy",
            "candidate_budget_method",
            "candidate_budget_seed",
            "candidate_count",
            "total_candidate_count",
        ]
        from_metadata = {key: metadata.get(key) for key in fields if key in metadata}
        if from_metadata:
            return {**from_metadata, "metadata_source": "sidecar_metadata"}

        def first_present(key: str) -> Any:
            for row in rows:
                value = row.get(key)
                if value is None:
                    continue
                if isinstance(value, float) and math.isnan(value):
                    continue
                if isinstance(value, str) and not value.strip():
                    continue
                return value
            return None

        candidate_count_values: List[float] = []
        total_candidate_count_values: List[float] = []
        for row in rows:
            candidate_count = self._finite_or_none(row.get("candidate_count"))
            if candidate_count is not None:
                candidate_count_values.append(candidate_count)
            total_candidate_count = self._finite_or_none(row.get("total_candidate_count"))
            if total_candidate_count is not None:
                total_candidate_count_values.append(total_candidate_count)
        applied_values = [
            self._optional_bool(row.get("candidate_budget_applied"))
            for row in rows
            if self._optional_bool(row.get("candidate_budget_applied")) is not None
        ]
        applied = any(value is True for value in applied_values)
        policy = first_present("candidate_budget_policy")
        method = first_present("candidate_budget_method")
        if not policy and (applied_values or candidate_count_values or total_candidate_count_values):
            policy = "seeded_random_sample" if applied else "full_grid"
        if not method and policy:
            method = policy
        return {
            "candidate_budget": first_present("candidate_budget"),
            "candidate_budget_applied": applied if applied_values else None,
            "candidate_budget_policy": policy,
            "candidate_budget_method": method,
            "candidate_budget_seed": first_present("candidate_budget_seed"),
            "candidate_count": max(candidate_count_values) if candidate_count_values else None,
            "total_candidate_count": max(total_candidate_count_values) if total_candidate_count_values else None,
            "metadata_source": "artifact_rows",
        }

    def _wfa_dashboard_artifact_path(self, run_id: str) -> Optional[Path]:
        candidates = self._artifact_paths(run_id, "wfa_parquet")
        if not candidates:
            return None

        for path in candidates:
            try:
                df = pd.read_parquet(path)
            except Exception as exc:
                raise ValueError(
                    f"WFA artifact cannot be read: {path}"
                ) from exc
            if df.empty:
                continue
            if "wfa_row_type" in df.columns:
                row_type = df["wfa_row_type"].fillna("").astype(str)
                if (row_type == "selected_optimum").any():
                    return path
                continue
        raise ValueError(
            f"WFA run {run_id} has no canonical selected_optimum artifact"
        )

    @staticmethod
    def _payload_source_refs_exist(payload: Dict[str, Any]) -> bool:
        refs = payload.get("artifact_source_refs", [])
        if not isinstance(refs, list):
            return True
        return all(Path(str(ref)).exists() for ref in refs if str(ref).strip())

    def _chart_path(self, run_id: str, name: str) -> Path:
        return self.registry.resolve_run_paths(run_id)["chart_payload_dir"] / name

    def _snapshot_path(self, run_id: str, name: str) -> Path:
        return self.registry.resolve_run_paths(run_id)["snapshot_dir"] / name

    def _source_strategy_config(self, run_id: str) -> Dict[str, Any]:
        snapshot = self._load_json(self._snapshot_path(run_id, "run_snapshot.json"), {})
        resolved = snapshot.get("resolved_configs", {}) if isinstance(snapshot, dict) else {}
        for key in ("run_config", "strategy_run"):
            ref = resolved.get(key, {}) if isinstance(resolved, dict) else {}
            raw_path = str(ref.get("config_path", "")).strip() if isinstance(ref, dict) else ""
            if not raw_path:
                continue
            path = Path(raw_path)
            if path.is_file():
                loaded = self._load_json(path, {})
                if isinstance(loaded, dict):
                    return loaded
        return {}

    def _strategy_summary(self, run_id: str) -> Dict[str, Any]:
        snapshot = self._load_json(self._snapshot_path(run_id, "run_snapshot.json"), {})
        resolved = snapshot.get("resolved_configs", {}) if isinstance(snapshot, dict) else {}
        run_config = resolved.get("run_config", {}) if isinstance(resolved, dict) else {}
        run_config_ref = (
            str(run_config.get("config_path", "")).strip()
            if isinstance(run_config, dict)
            else ""
        )
        run_config_path = Path(run_config_ref) if run_config_ref else None
        source_config = (
            self._load_json(run_config_path, {})
            if run_config_path is not None and run_config_path.is_file()
            else {}
        )
        if not isinstance(source_config, dict):
            source_config = {}
        source_is_wfa_run = bool(
            is_wfa_run_schema_version(source_config.get("schema_version"))
        )
        forced_workflow_id = ""
        strategy_source_config = source_config
        strategy_source_path = run_config_path
        if source_is_wfa_run:
            wfa_platform = (
                source_config.get("platform", {})
                if isinstance(source_config.get("platform"), dict)
                else {}
            )
            forced_workflow_id = str(wfa_platform.get("workflow_id") or "").strip()
            embedded_backtester = (
                resolved.get("backtester_config", {}) if isinstance(resolved, dict) else {}
            )
            if isinstance(embedded_backtester, dict):
                embedded_strategy = embedded_backtester.get("strategy_run_config")
                if isinstance(embedded_strategy, dict) and embedded_strategy:
                    strategy_source_config = embedded_strategy
                    strategy_source_path = Path("__embedded_strategy_run__.json")
        normalized_config = self._normalized_strategy_config(
            strategy_source_config,
            strategy_source_path,
        )
        platform_config = source_config.get("platform", {})
        if not isinstance(platform_config, dict):
            platform_config = {}
        dataloader_config = (
            resolved.get("dataloader_config", {}) if isinstance(resolved, dict) else {}
        )
        backtester_config = (
            resolved.get("backtester_config", {}) if isinstance(resolved, dict) else {}
        )
        contract_refs = snapshot.get("contract_refs", {}) if isinstance(snapshot, dict) else {}
        strategy_ref = contract_refs.get("strategy_contract", {}) if isinstance(contract_refs, dict) else {}
        strategy_ref_path = (
            str(strategy_ref.get("path", "")).strip()
            if isinstance(strategy_ref, dict)
            else ""
        )
        strategy_path = Path(strategy_ref_path) if strategy_ref_path else None
        if strategy_path is None or not strategy_path.is_file():
            raw_path = (
                backtester_config.get("strategy_contract_path")
                if isinstance(backtester_config, dict)
                else None
            )
            if isinstance(raw_path, str) and raw_path.strip():
                candidate = self.repo_root / raw_path
                if candidate.is_file():
                    strategy_path = candidate
        strategy = (
            self._load_json(strategy_path, {})
            if strategy_path is not None and strategy_path.is_file()
            else {}
        )
        if not isinstance(strategy, dict):
            strategy = {}
        data_context = strategy.get("data_context", {})
        if not isinstance(data_context, dict):
            data_context = {}
        trading_params = (
            backtester_config.get("trading_params", {})
            if isinstance(backtester_config, dict)
            else {}
        )
        if not isinstance(trading_params, dict):
            trading_params = {}
        parameter_domains = strategy.get("parameter_domains", {})
        source = (
            str(dataloader_config.get("source", "")).strip()
            if isinstance(dataloader_config, dict)
            else ""
        )
        yfinance_config = (
            dataloader_config.get("yfinance_config", {})
            if isinstance(dataloader_config, dict)
            else {}
        )
        asset_label = (
            str(data_context.get("primary_instrument") or "").strip()
            or (
                str(yfinance_config.get("symbol") or "").strip()
                if isinstance(yfinance_config, dict)
                else ""
            )
            or ("Dataset" if source and source.lower() != "yfinance" else "")
        )
        frequency_label = (
            str(data_context.get("frequency") or "").strip()
            or (
                str(yfinance_config.get("interval") or "").strip()
                if isinstance(yfinance_config, dict)
                else ""
            )
        )
        period_label = self._render_period_label(dataloader_config)
        transaction_cost = trading_params.get("transaction_cost")
        slippage = trading_params.get("slippage")
        trade_delay = trading_params.get("trade_delay")
        trade_price = trading_params.get("trade_price")
        strategy_platform_config = (
            strategy_source_config.get("platform", {})
            if isinstance(strategy_source_config, dict)
            else {}
        )
        if not isinstance(strategy_platform_config, dict):
            strategy_platform_config = {}
        strategy_mode_id = str(
            strategy_platform_config.get("strategy_mode_id")
            or platform_config.get("strategy_mode_id")
            or platform_config.get("product_mode_id")
            or ""
        ).strip()
        workflow_id = str(platform_config.get("workflow_id") or "").strip()
        if forced_workflow_id:
            workflow_id = forced_workflow_id
        if not workflow_id:
            workflow_id = self._infer_workflow_id(
                run_config_path=run_config_path,
                source_config=source_config,
                parameter_domains=parameter_domains,
            )
        mode_label = self._render_strategy_mode_label(
            strategy=strategy,
            backtester_config=backtester_config,
            parameter_domains=parameter_domains,
            strategy_mode_id=strategy_mode_id,
        )
        summary = {
            "strategy_id": strategy.get("strategy_id"),
            "name": (
                strategy.get("name")
                or strategy.get("strategy_id")
                or (strategy_path.stem if strategy_path is not None else "")
            ),
            "description": strategy.get("description"),
            "strategy_contract_path": (
                str(strategy_path)
                if strategy_path is not None and strategy_path.is_file()
                else ""
            ),
            "asset_label": asset_label,
            "period_label": period_label,
            "frequency_label": frequency_label,
            "calendar_label": data_context.get("calendar") or data_context.get("market_calendar"),
            "timezone_label": data_context.get("timezone"),
            "strategy_mode_id": strategy_mode_id,
            "mode_label": mode_label,
            "workflow_id": workflow_id,
            "workflow_label": self._workflow_label(workflow_id),
            "execution_label": self._render_execution_label(trade_delay, trade_price),
            "cost_label": self._render_cost_label(transaction_cost, slippage),
            "entry_rule": self._render_rule_node(strategy.get("entry")),
            "exit_rule": self._render_rule_node(strategy.get("exit")),
            "parameter_domains": parameter_domains if isinstance(parameter_domains, dict) else {},
            "parameter_domain_label": self._render_parameter_domains(parameter_domains),
            "mode_registry_path": str(self._mode_registry_path()),
            "available_mode_labels": self._strategy_mode_labels(status="planned"),
            "source": "strategy_contract",
        }
        if normalized_config:
            summary = self._apply_normalized_strategy_summary(summary, normalized_config)
        raw_display_rules = self._strategy_rule_display_overrides(strategy_source_config)
        raw_parameter_domains = (
            strategy_source_config.get("parameter_domains", {})
            if isinstance(strategy_source_config, dict)
            else {}
        )
        if raw_display_rules:
            if not str(summary.get("mode_label") or "").strip():
                summary["mode_label"] = raw_display_rules.get("mode_label", "")
            if not str(summary.get("entry_rule") or "").strip():
                summary["entry_rule"] = raw_display_rules.get("entry_rule", "")
            if not str(summary.get("exit_rule") or "").strip():
                summary["exit_rule"] = raw_display_rules.get("exit_rule", "")
            if not str(summary.get("parameter_domain_label") or "").strip():
                summary["parameter_domain_label"] = raw_display_rules.get(
                    "parameter_domain_label", ""
                )
        if not isinstance(summary.get("parameter_domains"), dict) or not summary.get(
            "parameter_domains"
        ):
            if isinstance(raw_parameter_domains, dict) and raw_parameter_domains:
                summary["parameter_domains"] = raw_parameter_domains
                if not str(summary.get("parameter_domain_label") or "").strip():
                    summary["parameter_domain_label"] = self._render_parameter_domains(
                        raw_parameter_domains
                    )
        if forced_workflow_id:
            summary["workflow_id"] = forced_workflow_id
            summary["workflow_label"] = self._workflow_label(forced_workflow_id)
        return summary

    def _normalized_strategy_config(
        self,
        source_config: Any,
        source_path: Optional[Path],
    ) -> Dict[str, Any]:
        if not isinstance(source_config, dict) or not source_config:
            return {}
        return normalize_strategy_run_config(
            source_config,
            source_path=(
                source_path
                if source_path is not None and source_path.is_file()
                else None
            ),
            repo_root=self.repo_root,
        )

    def _apply_normalized_strategy_summary(
        self,
        summary: Dict[str, Any],
        normalized: Dict[str, Any],
    ) -> Dict[str, Any]:
        out = dict(summary)
        platform = normalized.get("platform", {}) if isinstance(normalized.get("platform"), dict) else {}
        data = normalized.get("data", {}) if isinstance(normalized.get("data"), dict) else {}
        universe = normalized.get("universe", {}) if isinstance(normalized.get("universe"), dict) else {}
        signals = normalized.get("signals", {}) if isinstance(normalized.get("signals"), dict) else {}
        selection = normalized.get("selection", {}) if isinstance(normalized.get("selection"), dict) else {}
        allocation = normalized.get("allocation", {}) if isinstance(normalized.get("allocation"), dict) else {}
        rebalance = normalized.get("rebalance", {}) if isinstance(normalized.get("rebalance"), dict) else {}
        fill_model = normalized.get("fill_model", {}) if isinstance(normalized.get("fill_model"), dict) else {}
        if not fill_model:
            fill_model = normalized.get("execution", {}) if isinstance(normalized.get("execution"), dict) else {}
        risk = normalized.get("risk", {}) if isinstance(normalized.get("risk"), dict) else {}
        metadata = normalized.get("metadata", {}) if isinstance(normalized.get("metadata"), dict) else {}
        parameter_domains = (
            normalized.get("parameter_domains", {})
            if isinstance(normalized.get("parameter_domains"), dict)
            else {}
        )
        display_rules = self._strategy_rule_display_overrides(normalized)
        symbols = [str(item).strip().upper() for item in universe.get("symbols", []) if str(item).strip()]
        strategy_mode_id = str(platform.get("strategy_mode_id") or out.get("strategy_mode_id") or "").strip()
        strategy_profile_id = str(platform.get("strategy_profile_id") or out.get("strategy_profile_id") or "").strip()
        strategy_preset_id = str(platform.get("strategy_preset_id") or out.get("strategy_preset_id") or "").strip()
        workflow_id = str(platform.get("workflow_id") or out.get("workflow_id") or "").strip()
        cost = fill_model.get("cost", {}) if isinstance(fill_model.get("cost"), dict) else {}
        benchmark = data.get("benchmark")
        if isinstance(benchmark, dict):
            benchmark_label = benchmark.get("label") or benchmark.get("symbol") or ""
        else:
            benchmark_label = str(benchmark or "")

        out.update(
            {
                "strategy_id": metadata.get("strategy_id") or out.get("strategy_id"),
                "asset_label": ", ".join(symbols) if symbols else out.get("asset_label", ""),
                "frequency_label": data.get("frequency") or out.get("frequency_label", ""),
                "calendar_label": data.get("calendar") or out.get("calendar_label"),
                "timezone_label": data.get("timezone") or out.get("timezone_label"),
                "strategy_mode_id": strategy_mode_id,
                "strategy_profile_id": strategy_profile_id,
                "strategy_preset_id": strategy_preset_id,
                "mode_label": display_rules.get("mode_label")
                or self._render_strategy_identity_label(
                    strategy_profile_id=strategy_profile_id,
                    strategy_preset_id=strategy_preset_id,
                    strategy_mode_id=strategy_mode_id,
                )
                or out.get("mode_label", ""),
                "workflow_id": workflow_id,
                "workflow_label": self._workflow_label(workflow_id),
                "execution_label": self._render_normalized_execution_label(fill_model)
                or out.get("execution_label", ""),
                "cost_label": self._render_cost_label(
                    cost.get("transaction_cost"),
                    cost.get("slippage"),
                ),
                "entry_rule": display_rules.get("entry_rule")
                or self._render_normalized_entry_rule(signals, selection, rebalance),
                "exit_rule": display_rules.get("exit_rule")
                or self._render_normalized_exit_rule(signals, rebalance, allocation),
                "parameter_domains": parameter_domains,
                "parameter_domain_label": display_rules.get("parameter_domain_label")
                or self._render_parameter_domains(parameter_domains),
                "benchmark_label": benchmark_label,
                "risk_label": self._render_risk_label(risk),
                "source": "strategy_run",
            }
        )
        out["execution_plan"] = plan_strategy_execution(normalized)
        return out

    def _mode_registry_path(self) -> Path:
        return self.repo_root / "backtester" / "contracts" / "strategy" / "mode-registry-v1.json"

    @staticmethod
    def _workflow_label(workflow_id: str) -> str:
        labels = {
            "single_backtest": "Single backtest",
            "parameter_matrix": "Parameter matrix",
            "walk_forward_analysis": "Validation workflow (WFA)",
            "rolling_validation": "Validation workflow (rolling)",
            "statanalyser": "Stat analyser",
        }
        workflow = str(workflow_id or "").strip()
        return labels.get(workflow, workflow.replace("_", " ").title() if workflow else "")

    @staticmethod
    def _infer_workflow_id(
        *,
        run_config_path: Optional[Path],
        source_config: Any,
        parameter_domains: Any,
    ) -> str:
        del run_config_path, parameter_domains
        platform = source_config.get("platform", {}) if isinstance(source_config, dict) else {}
        if isinstance(platform, dict):
            workflow_id = str(platform.get("workflow_id") or "").strip()
            if workflow_id:
                return workflow_id
        return "single_backtest"

    def _strategy_mode_labels(self, *, status: str) -> List[str]:
        registry = self._load_json(self._mode_registry_path(), {})
        modes = registry.get("modes", []) if isinstance(registry, dict) else []
        labels: List[str] = []
        for mode in modes:
            if not isinstance(mode, dict):
                continue
            if str(mode.get("status", "")).strip().lower() != status:
                continue
            label = str(mode.get("label", "")).strip()
            if label:
                labels.append(label)
        return labels

    def _strategy_mode_label(self, mode_id: str) -> str:
        if not mode_id:
            return ""
        registry = self._load_json(self._mode_registry_path(), {})
        modes = registry.get("modes", []) if isinstance(registry, dict) else []
        for mode in modes:
            if not isinstance(mode, dict):
                continue
            if str(mode.get("id", "")).strip() == mode_id:
                return str(mode.get("label", "")).strip()
        return mode_id.replace("_", " ").title()

    def _strategy_profile_label(self, profile_id: str) -> str:
        if not profile_id:
            return ""
        registry = self._load_json(self._mode_registry_path(), {})
        profiles = registry.get("strategy_profiles", []) if isinstance(registry, dict) else []
        for profile in profiles:
            if not isinstance(profile, dict):
                continue
            if str(profile.get("id", "")).strip() == profile_id:
                return self._humanize_strategy_tokens(str(profile_id).split("_"))
        return profile_id.replace("_", " ").title()

    def _strategy_preset_label(self, preset_id: str) -> str:
        if not preset_id:
            return ""
        registry = self._load_json(self._mode_registry_path(), {})
        presets = registry.get("strategy_presets", []) if isinstance(registry, dict) else []
        for preset in presets:
            if not isinstance(preset, dict):
                continue
            if str(preset.get("id", "")).strip() == preset_id:
                return self._humanize_strategy_tokens(str(preset_id).split("_"))
        return preset_id.replace("_", " ").title()

    def _render_strategy_identity_label(
        self,
        *,
        strategy_profile_id: str = "",
        strategy_preset_id: str = "",
        strategy_mode_id: str = "",
    ) -> str:
        if strategy_profile_id:
            return self._strategy_profile_label(strategy_profile_id)
        if strategy_preset_id:
            return self._strategy_preset_label(strategy_preset_id)
        return self._strategy_mode_label(strategy_mode_id)

    @classmethod
    def _render_strategy_identity_label_static(
        cls,
        *,
        strategy_profile_id: str = "",
        strategy_preset_id: str = "",
        strategy_mode_id: str = "",
    ) -> str:
        if strategy_profile_id:
            return cls._humanize_strategy_tokens(strategy_profile_id.split("_"))
        if strategy_preset_id:
            return cls._humanize_strategy_tokens(strategy_preset_id.split("_"))
        if strategy_mode_id:
            return cls._humanize_strategy_tokens(strategy_mode_id.split("_"))
        return ""

    @staticmethod
    def _render_period_label(dataloader_config: Any) -> str:
        if not isinstance(dataloader_config, dict):
            return ""
        start = str(dataloader_config.get("start_date") or "").strip()
        end = str(dataloader_config.get("end_date") or "").strip()
        if start and end:
            return f"{start} -> {end}"
        if start:
            return f"{start} -> latest available"
        if end:
            return f"through {end}"
        return ""

    def _render_strategy_mode_label(
        self,
        *,
        strategy: Dict[str, Any],
        backtester_config: Any,
        parameter_domains: Any,
        strategy_mode_id: str = "",
    ) -> str:
        platform = strategy.get("platform", {}) if isinstance(strategy.get("platform"), dict) else {}
        explicit_label = self._render_strategy_identity_label(
            strategy_profile_id=str(platform.get("strategy_profile_id") or "").strip(),
            strategy_preset_id=str(platform.get("strategy_preset_id") or "").strip(),
            strategy_mode_id=strategy_mode_id,
        )
        if explicit_label:
            return explicit_label
        if isinstance(parameter_domains, dict) and parameter_domains:
            return "Selection Timing Portfolio"
        strategy_mode = (
            str(backtester_config.get("strategy_mode") or "").strip()
            if isinstance(backtester_config, dict)
            else ""
        )
        return f"{strategy_mode} semantic backtest" if strategy_mode else "Semantic backtest"

    @classmethod
    def _render_execution_label(cls, trade_delay: Any, trade_price: Any) -> str:
        price = str(trade_price or "").strip() or "configured price"
        delay = cls._as_float(trade_delay)
        if math.isnan(delay):
            return f"Execute at {price}"
        if delay == 0:
            timing = "signal bar"
        elif delay == 1:
            timing = "next bar after signal"
        else:
            timing = f"{int(delay)} bars after signal" if delay.is_integer() else f"{delay:g} bars after signal"
        return f"{timing} at {price}"

    @staticmethod
    def _render_normalized_execution_label(execution: Any) -> str:
        if not isinstance(execution, dict):
            return ""
        timing = str(execution.get("timing") or "").strip()
        price = str(execution.get("price") or execution.get("entry_price") or "").strip()
        if timing and price:
            return f"{timing.replace('_', ' ')} at {price.replace('_', ' ')}"
        if timing:
            return timing.replace("_", " ")
        if price:
            return f"Execute at {price.replace('_', ' ')}"
        return ""

    @classmethod
    def _render_cost_label(cls, transaction_cost: Any, slippage: Any) -> str:
        cost = cls._as_float(transaction_cost)
        slip = cls._as_float(slippage)
        parts: List[str] = []
        if not math.isnan(cost):
            parts.append(f"transaction cost {cost:.4g}")
        if not math.isnan(slip):
            parts.append(f"slippage {slip:.4g}")
        return "; ".join(parts)

    @classmethod
    def _render_rule_node(cls, node: Any) -> str:
        if not isinstance(node, dict):
            return str(node) if node is not None else ""
        op = str(node.get("op", "") or "").strip()
        if op in {"and", "or"}:
            rows = [cls._render_rule_node(item) for item in node.get("nodes", []) if item is not None]
            rows = [item for item in rows if item]
            joiner = " AND " if op == "and" else " OR "
            return f"({joiner.join(rows)})" if rows else op.upper()
        left = node.get("left")
        if left is None and "field" in node:
            left = node.get("field")
        right = node.get("right")
        if right is None and "right_field" in node:
            right = node.get("right_field")
        if op in {"lt", "lte", "gt", "gte", "eq", "neq"}:
            symbols = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "eq": "=", "neq": "!="}
            return f"{cls._render_operand(left)} {symbols[op]} {cls._render_operand(right)}"
        if op in {"cross_up", "cross_down", "crosses_above", "crosses_below"}:
            verb = "crosses above" if op in {"cross_up", "crosses_above"} else "crosses below"
            return f"{cls._render_operand(left)} {verb} {cls._render_operand(right)}"
        if op == "timer_bars":
            return f"hold for {cls._render_operand(node.get('value'))} bars"
        return json.dumps(node, ensure_ascii=False, sort_keys=True)

    @classmethod
    def _render_normalized_entry_rule(cls, signals: Any, selection: Any, rebalance: Any) -> str:
        if isinstance(signals, dict) and isinstance(signals.get("entry"), dict) and signals.get("entry"):
            rendered = cls._render_rule_node(signals.get("entry"))
            side = str(signals.get("side") or "").replace("_", " ").strip()
            if side:
                return f"{side} entry on {rendered}"
            return rendered
        if isinstance(selection, dict) and selection:
            pieces: List[str] = []
            eligible = selection.get("eligible")
            if eligible:
                pieces.append(f"eligible: {cls._render_rule_node(eligible)}")
            rank_by = selection.get("rank_by")
            top_n = selection.get("top_n")
            if rank_by or top_n:
                pieces.append(f"select top {top_n or '-'} by {rank_by or '-'}")
            return "; ".join(pieces)
        if isinstance(rebalance, dict) and isinstance(rebalance.get("trigger"), dict):
            op = rebalance["trigger"].get("op")
            if op:
                return f"Rebalance on {op}"
        return ""

    @classmethod
    def _render_normalized_exit_rule(cls, signals: Any, rebalance: Any, allocation: Any) -> str:
        if isinstance(signals, dict) and isinstance(signals.get("exit"), dict) and signals.get("exit"):
            return cls._render_rule_node(signals.get("exit"))
        if isinstance(rebalance, dict) and rebalance:
            return "Replaced or resized at next rebalance"
        if isinstance(allocation, dict) and allocation.get("method"):
            return f"Target allocation by {allocation.get('method')}"
        return ""

    @staticmethod
    def _render_risk_label(risk: Any) -> str:
        if not isinstance(risk, dict) or not risk:
            return ""
        pieces: List[str] = []
        if risk.get("max_positions") is not None:
            pieces.append(f"max positions {risk.get('max_positions')}")
        if risk.get("max_gross_exposure") is not None:
            pieces.append(f"max gross {risk.get('max_gross_exposure')}")
        if risk.get("long_short"):
            pieces.append(str(risk.get("long_short")).replace("_", " "))
        return "; ".join(pieces)

    @classmethod
    def _render_operand(cls, value: Any) -> str:
        if isinstance(value, dict):
            if "field" in value:
                return str(value.get("field"))
            if "param_ref" in value:
                return f"${value.get('param_ref')}"
            if "feature" in value:
                source = str(value.get("source", "") or "").strip()
                params = value.get("params", {})
                if isinstance(params, dict) and params:
                    rendered_params = ", ".join(
                        f"{key}={cls._render_operand(param_value)}"
                        for key, param_value in sorted(params.items())
                    )
                    if source:
                        return f"{value.get('feature')}({source}, {rendered_params})"
                    return f"{value.get('feature')}({rendered_params})"
                return str(value.get("feature"))
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    @staticmethod
    def _render_parameter_domains(domains: Any) -> str:
        if not isinstance(domains, dict) or not domains:
            return "No tunable parameter domain recorded."
        rows: List[str] = []
        for name, spec in sorted(domains.items()):
            if not isinstance(spec, dict):
                rows.append(f"{name}: {spec}")
                continue
            if spec.get("type") == "range":
                rows.append(
                    f"{name}: {spec.get('start')} to {spec.get('end')} step {spec.get('step')}"
                )
            elif spec.get("type") == "set":
                values = spec.get("values", [])
                if isinstance(values, list):
                    rows.append(f"{name}: {', '.join(str(value) for value in values)}")
                else:
                    rows.append(f"{name}: {values}")
            else:
                rows.append(f"{name}: {json.dumps(spec, ensure_ascii=False, sort_keys=True)}")
        return "; ".join(rows)

    @staticmethod
    def _position_side(position_size: Any) -> str:
        size = AppPayloadService._as_float(position_size)
        if math.isnan(size) or size == 0:
            return "flat"
        return "long" if size > 0 else "short"

    @staticmethod
    def _to_iso(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        return str(value)

    @staticmethod
    def _as_float(value: Any) -> float:
        if value is None:
            return float("nan")
        try:
            return float(value)
        except Exception:
            return float("nan")

    @staticmethod
    def _load_json(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read JSON artifact {path}: {exc}") from exc

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            AppPayloadService._json_cache_safe_value(payload),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        path.write_bytes(encoded)

    @staticmethod
    def _json_cache_safe_value(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                str(key): AppPayloadService._json_cache_safe_value(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [AppPayloadService._json_cache_safe_value(item) for item in value]
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            numeric = float(value)
            return numeric if math.isfinite(numeric) else None
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        try:
            if value is not None and pd.isna(value):
                return None
        except (TypeError, ValueError):
            return value
        return value

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")
