"""Strict metrics overview projection from versioned JSON contracts only."""

from __future__ import annotations

import json
import math
import copy
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from app.runtime.registry import AppRegistry
from .shared_chart_series import SharedChartSeriesStore
from .time_context import strategy_time_summary
from backtester.EngineRequest_backtester import validate_canonical_candidate_id
from backtester.StrategyRunConfig_backtester import normalize_strategy_run_config
from backtester.ops.support_checker import timeline_action_is_pre_session_known


METRICS_OVERVIEW_SCHEMA_VERSION = "1.28"
METRIC_KEYS = {
    "total_return": "Total_return",
    "cagr": "Annualized_return (CAGR)",
    "sharpe": "Sharpe",
    "sortino": "Sortino",
    "calmar": "Calmar",
    "max_drawdown": "Max_drawdown",
    "intraday_max_drawdown": "Intraday_max_drawdown",
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
    "excess_return": "Excess_return",
    "bah_cagr": "BAH_Annualized_return (CAGR)",
    "bah_sharpe": "BAH_Sharpe",
    "bah_calmar": "BAH_Calmar",
    "bah_max_drawdown": "BAH_Max_drawdown",
}
CATEGORIES = {
    "top_3_sharpe": ("Top 3 Sharpe", "sharpe"),
    "top_3_return": ("Top 3 Return", "total_return"),
    "top_3_cagr": ("Top 3 CAGR", "cagr"),
    "top_3_calmar": ("Top 3 Calmar", "calmar"),
    "top_3_sortino": ("Top 3 Sortino", "sortino"),
    "top_3_recovery_factor": ("Top 3 Recovery Factor", "recovery_factor"),
    "top_3_information_ratio": ("Top 3 Information Ratio", "information_ratio"),
    "top_3_profit_factor": ("Top 3 Profit Factor", "profit_factor"),
    "top_3_lowest_mdd": ("Top 3 Lowest MDD", "max_drawdown"),
    "top_3_excess_return": ("Top 3 Excess Return", "excess_return"),
}


class MetricsContractPayloadService:
    def __init__(self, registry: AppRegistry):
        self.registry = registry
        self.shared_series = SharedChartSeriesStore(registry)

    def ensure(self, run_id: str, *, force: bool = False) -> Path:
        output_path = self.registry.resolve_run_paths(run_id)["chart_payload_dir"] / "metrics_overview_payload.json"
        if output_path.exists() and not force:
            cached = self._read_json(output_path, {})
            if (
                isinstance(cached, dict)
                and cached.get("schema_version") == "metrics_overview_index.v1"
                and cached.get("contract_id") == "lo2cin4bt.metrics_overview_index.v1"
                and cached.get("run_id") == run_id
            ):
                materialized = self.shared_series.materialize_metrics_overview(
                    run_id, cached
                )
                if (
                    materialized.get("schema_version")
                    == METRICS_OVERVIEW_SCHEMA_VERSION
                    and isinstance(materialized.get("annualization"), dict)
                    and isinstance(materialized.get("strategy_summary"), dict)
                    and isinstance(
                        materialized["strategy_summary"].get("time_context"), dict
                    )
                ):
                    return output_path

        plot_path = self.registry.resolve_run_paths(run_id)["chart_payload_dir"] / "asset_curve_compare.json"
        if not plot_path.is_file():
            raise FileNotFoundError(
                f"metrics overview PlotBundle index not found for run {run_id}"
            )
        plot_index = self._read_json(plot_path, {})
        plot_bundle = self.shared_series.materialize_plot_bundle(run_id, plot_index)
        self._validate_plot_bundle(plot_bundle)
        canonical_path = self._validated_canonical_source(plot_bundle)
        metadata_path = self._artifact_path(run_id, "metricstracker_metadata")
        if metadata_path is None:
            raise FileNotFoundError("metrics contract requires metricstracker artifact metadata")
        metadata_rows = self._read_json(metadata_path, [])
        if not isinstance(metadata_rows, list) or not metadata_rows:
            raise FileNotFoundError("metrics contract metadata JSON is missing or empty")
        annualization = self._annualization_contract(metadata_rows)

        matrix_path = self._artifact_path(run_id, "portfolio_matrix_summary_json")
        matrix_summary = self._read_json(matrix_path, {}) if matrix_path is not None else {}
        matrix_rows = (
            matrix_summary.get("rows", [])
            if isinstance(matrix_summary, dict)
            and matrix_summary.get("schema_version") == "portfolio_matrix_summary.v1"
            and isinstance(matrix_summary.get("rows"), list)
            else []
        )
        matrix_by_id = {
            str(item.get("backtest_id") or ""): item
            for item in matrix_rows
            if isinstance(item, dict)
        }
        rows = [self._metric_row(item) for item in metadata_rows if isinstance(item, dict)]
        for row in rows:
            matrix_row = matrix_by_id.get(str(row.get("backtest_id") or ""))
            if not isinstance(matrix_row, dict):
                continue
            row["label"] = str(matrix_row.get("label") or row["label"])
            row["label_source"] = "canonical_matrix_summary"
            matrix_strategy_id = validate_canonical_candidate_id(
                matrix_row.get("strategy_id")
            )
            if matrix_strategy_id != row["backtest_id"]:
                raise ValueError(
                    "Parameter Matrix strategy_id does not match Rust metrics Backtest_id"
                )
            row["strategy_id"] = matrix_strategy_id
            for key in (
                "semantic_combo",
                "semantic_fields",
                "result_materialization",
                "rebalance_count",
                "trade_count",
                "avg_turnover",
                "avg_gross_exposure",
                "result_validation",
            ):
                if key in matrix_row:
                    row[key] = matrix_row[key]
            # metricstracker is the canonical metrics authority. The matrix
            # summary is generated earlier and may contain null placeholders,
            # so it can only fill values that metricstracker did not produce.
            for key in (
                "total_return",
                "cagr",
                "sharpe",
                "max_drawdown",
                "intraday_max_drawdown",
            ):
                if row.get(key) is None and matrix_row.get(key) is not None:
                    row[key] = matrix_row[key]
        series = []
        benchmark_series: Optional[Dict[str, Any]] = None
        for item in plot_bundle["series"]:
            projected = {
                "backtest_id": str(item["series_id"]),
                "label": str(item["label"]),
                "x": list(item["x"]),
                "y": list(item["y"]),
            }
            if projected["backtest_id"] == "benchmark":
                benchmark_series = {
                    "series_id": "benchmark",
                    "label": projected["label"],
                    "x": projected["x"],
                    "y": projected["y"],
                }
            else:
                series.append(projected)
        series_by_id = {
            str(item.get("backtest_id") or ""): item
            for item in series
            if isinstance(item, dict)
        }
        for row in rows:
            source_series = series_by_id.get(str(row.get("backtest_id") or ""), {})
            x_values = source_series.get("x", []) if isinstance(source_series, dict) else []
            if isinstance(x_values, list) and x_values:
                row["date_range_start"] = str(x_values[0])
                row["date_range_end"] = str(x_values[-1])
        categories = {
            category_id: [
                str(row["backtest_id"])
                for row in sorted(
                    rows,
                    key=lambda row: self._sort_value(row.get(metric_key)),
                    reverse=True,
                )[:3]
            ]
            for category_id, (_label, metric_key) in CATEGORIES.items()
        }
        registry_entry = self.registry.load_registry_entry(run_id)
        result_type = "portfolio" if matrix_rows else "single_asset"
        strategy_summary = self._strategy_summary(run_id, registry_entry)
        strategy_summary["annualization"] = copy.deepcopy(annualization)
        portfolio_runs = (
            self._portfolio_runs(run_id, rows, strategy_summary)
            if result_type == "portfolio"
            else []
        )
        payload = {
            "schema_version": METRICS_OVERVIEW_SCHEMA_VERSION,
            "contract_id": "lo2cin4bt-app-metrics-overview-payload-v1",
            "projection_source": "validated_json_contracts",
            "run_id": run_id,
            "result_type": result_type,
            "artifact_type": (
                "multi_asset_portfolio_matrix_bundle"
                if result_type == "portfolio"
                else "single_asset_backtest"
            ),
            "strategy_summary": strategy_summary,
            "time_context": copy.deepcopy(
                strategy_summary.get("time_context")
            ),
            "annualization": annualization,
            "default_category": "top_3_sharpe",
            "available_categories": [
                {"id": category_id, "label": label}
                for category_id, (label, _metric_key) in CATEGORIES.items()
            ],
            "rows": rows,
            "series": series,
            "benchmark_series": benchmark_series,
            "categories": categories,
            "generated_at": plot_bundle["generated_at"],
            "source_hashes": list(plot_bundle["source_hashes"]),
            "artifact_source_refs": [
                str(plot_path),
                str(metadata_path),
                str(canonical_path),
            ],
        }
        if matrix_path is not None:
            payload["artifact_source_refs"].append(str(matrix_path))
        if result_type == "portfolio":
            payload["portfolio"] = {
                "summary": rows[0] if rows else {},
                "runs": portfolio_runs,
                "matrix_summary": {
                    "row_count": matrix_summary.get("row_count"),
                    "variant_count": matrix_summary.get("variant_count"),
                    "retained_result_count": matrix_summary.get("retained_result_count"),
                    "compact_result_count": matrix_summary.get("compact_result_count"),
                    "coverage": matrix_summary.get("coverage"),
                },
            }
        self.shared_series.write_json(
            output_path,
            self.shared_series.compact_metrics_overview(run_id, payload),
        )
        return output_path

    def load(self, run_id: str, *, force: bool = False) -> Dict[str, Any]:
        path = self.ensure(run_id, force=force)
        return self.shared_series.materialize_metrics_overview(
            run_id,
            self._read_json(path, {}),
        )

    def _portfolio_runs(
        self,
        run_id: str,
        rows: list[Dict[str, Any]],
        strategy_summary: Dict[str, Any],
    ) -> list[Dict[str, Any]]:
        trade_path = self._artifact_path(run_id, "portfolio_rebalance_trades_parquet")
        trades_by_id: Dict[str, list[Dict[str, Any]]] = {}
        if trade_path is not None:
            frame = pd.read_parquet(trade_path)
            if "Backtest_id" in frame.columns:
                for backtest_id, group in frame.groupby("Backtest_id", sort=False):
                    trades_by_id[str(backtest_id)] = self._records(group)
        benchmark_label = str(strategy_summary.get("benchmark_label") or "Benchmark")
        runs: list[Dict[str, Any]] = []
        for row in rows:
            backtest_id = str(row.get("backtest_id") or "")
            allocation_rows = trades_by_id.get(backtest_id, [])
            validation = row.get("result_validation")
            runs.append(
                {
                    "summary": row,
                    "allocation_change_rows": allocation_rows,
                    "turnover_summary": {
                        "active_rebalance_events": row.get("rebalance_count"),
                        "trade_events": len(allocation_rows),
                        "avg_trade_turnover": row.get("avg_turnover"),
                    },
                    "data_quality": {
                        "status": (
                            validation.get("status")
                            if isinstance(validation, dict)
                            else "unknown"
                        ),
                        "result_validation": validation,
                    },
                    "benchmark_label": benchmark_label,
                }
            )
        return runs

    def _strategy_summary(
        self, run_id: str, registry_entry: Dict[str, Any]
    ) -> Dict[str, Any]:
        strategy_path = self.registry.resolve_run_paths(run_id)["snapshot_dir"] / "strategy_run.json"
        config = self._read_json(strategy_path, {})
        if (
            isinstance(config, dict)
            and config.get("schema_version") == "strategy_run"
            and isinstance(config.get("platform"), dict)
        ):
            config = normalize_strategy_run_config(config)
        platform = config.get("platform", {}) if isinstance(config, dict) else {}
        data = config.get("data", {}) if isinstance(config, dict) else {}
        universe = config.get("universe", {}) if isinstance(config, dict) else {}
        symbols = universe.get("symbols", []) if isinstance(universe, dict) else []
        benchmark = data.get("benchmark", {}) if isinstance(data, dict) else {}
        signals = config.get("signals", {}) if isinstance(config, dict) else {}
        fill_model = config.get("fill_model", {}) if isinstance(config, dict) else {}
        time_summary = strategy_time_summary(data)
        return {
            "strategy_id": (
                config.get("metadata", {}).get("strategy_id")
                if isinstance(config.get("metadata"), dict)
                else registry_entry.get("strategy_id")
            ),
            "display_label": registry_entry.get("display_label"),
            "symbol": registry_entry.get("symbol"),
            **time_summary,
            "strategy_mode_id": platform.get("strategy_mode_id"),
            "strategy_profile_id": platform.get("strategy_profile_id"),
            "workflow_id": platform.get("workflow_id"),
            "asset_label": ", ".join(str(item) for item in symbols),
            "mode_label": self._title(platform.get("strategy_profile_id")),
            "workflow_label": self._title(platform.get("workflow_id")),
            "entry_rule": self._render_rule(signals.get("entry")),
            "exit_rule": self._render_exit_rule(fill_model.get("actions")),
            "parameter_domain_label": ", ".join(
                str(key) for key in (config.get("parameter_domains", {}) or {})
            ),
            "execution_label": str(fill_model.get("timing") or ""),
            "cost_label": self._cost_label(fill_model.get("cost")),
            "logic_steps": self._strategy_logic_steps(config),
            "benchmark_label": (
                benchmark.get("label") or benchmark.get("symbol")
                if isinstance(benchmark, dict)
                else str(benchmark or "")
            ),
        }

    @classmethod
    def _strategy_logic_steps(cls, config: Any) -> list[Dict[str, str]]:
        """Project executable config into complete, ordered strategy semantics."""

        if not isinstance(config, dict):
            return []
        steps: list[Dict[str, str]] = []
        universe = config.get("universe")
        if isinstance(universe, dict) and universe.get("symbols"):
            steps.append({
                "kind": "Universe",
                "label": "Tradable universe",
                "detail": ", ".join(str(value) for value in universe["symbols"]),
            })
        computed_fields = config.get("computed_fields")
        if isinstance(computed_fields, list):
            for field in computed_fields:
                if not isinstance(field, dict) or not field.get("name"):
                    continue
                steps.append({
                    "kind": "Indicator",
                    "label": str(field["name"]),
                    "detail": cls._render_config_value(
                        {key: value for key, value in field.items() if key != "name"}
                    ),
                })
        signals = config.get("signals")
        if isinstance(signals, dict):
            for name, rule in signals.items():
                if rule:
                    steps.append({
                        "kind": "Signal",
                        "label": str(name),
                        "detail": cls._render_config_value(rule),
                    })
        for section, kind in (
            ("selection", "Selection"),
            ("allocation", "Allocation"),
            ("rebalance", "Rebalance"),
        ):
            value = config.get(section)
            if isinstance(value, dict) and value:
                steps.append({
                    "kind": kind,
                    "label": section,
                    "detail": cls._render_config_value(value),
                })
        fill_model = config.get("fill_model")
        if isinstance(fill_model, dict):
            execution_model = {
                key: fill_model[key]
                for key in ("timing", "price", "position_policy")
                if key in fill_model
            }
            if execution_model:
                steps.append({
                    "kind": "Execution",
                    "label": "Execution model",
                    "detail": cls._render_config_value(execution_model),
                })
            for index, action in enumerate(fill_model.get("actions") or [], start=1):
                if isinstance(action, dict):
                    steps.append({
                        "kind": "Action",
                        "label": f"Execution {index}",
                        "detail": cls._render_timeline_action(action, config),
                    })
            cost = fill_model.get("cost")
            if isinstance(cost, dict) and cost:
                steps.append({
                    "kind": "Costs",
                    "label": "Trading costs",
                    "detail": cls._render_config_value(cost),
                })
        risk = config.get("risk")
        if isinstance(risk, dict) and risk:
            steps.append({
                "kind": "Risk",
                "label": "Risk controls",
                "detail": cls._render_config_value(risk),
            })
        parameters = config.get("parameter_domains")
        if isinstance(parameters, dict) and parameters:
            steps.append({
                "kind": "Parameters",
                "label": "Search domains",
                "detail": cls._render_config_value(parameters),
            })
        return steps

    @classmethod
    def _render_timeline_action(cls, action: Dict[str, Any], config: Dict[str, Any]) -> str:
        signal = str(action.get("signal") or "").strip()
        price = str(action.get("price") or "").strip()
        offset = action.get("offset_bars")
        if isinstance(offset, dict) and offset.get("param_ref"):
            timing = f"{offset['param_ref']} bars after signal at {price}"
        elif offset == 0 and signal == "rebalance":
            timing = f"current session {price} (pre-scheduled initialization or rebalance)"
        elif offset == 0 and timeline_action_is_pre_session_known(action, config):
            timing = f"current session {price} (calendar event known before session)"
        elif isinstance(offset, (int, float)):
            timing = f"{int(offset)} bar{'s' if int(offset) != 1 else ''} after signal at {price}"
        else:
            timing = price

        parts = [f"signal: {signal}", f"execution timing: {timing}"]
        action_name = action.get("action")
        if action_name:
            parts.append(f"action: {str(action_name).replace('_', ' ')}")
        weights = action.get("weights")
        if isinstance(weights, dict):
            parts.append(
                "weights: "
                + ", ".join(
                    f"{symbol} {float(weight):.0%}" for symbol, weight in weights.items()
                )
            )
        return "; ".join(parts)

    @classmethod
    def _render_config_value(cls, value: Any) -> str:
        if isinstance(value, dict):
            if set(value) == {"param_ref"}:
                return f"parameter {value['param_ref']}"
            parts = []
            for key, item in value.items():
                if key == "weights" and isinstance(item, dict):
                    rendered = ", ".join(
                        f"{symbol} {float(weight):.0%}" for symbol, weight in item.items()
                    )
                else:
                    rendered = cls._render_config_value(item)
                parts.append(f"{str(key).replace('_', ' ')}: {rendered}")
            return "; ".join(parts)
        if isinstance(value, list):
            return ", ".join(cls._render_config_value(item) for item in value)
        if isinstance(value, bool):
            return "yes" if value else "no"
        return str(value).replace("_", " ")

    @staticmethod
    def _render_rule(value: Any) -> str:
        if not isinstance(value, dict):
            return ""
        field = str(value.get("field") or "")
        operation = str(value.get("op") or "").replace("_", " ")
        target = value.get("value")
        if isinstance(target, dict) and target.get("param_ref"):
            target = target["param_ref"]
        return " ".join(str(item) for item in (field, operation, target) if item not in (None, ""))

    @classmethod
    def _render_exit_rule(cls, actions: Any) -> str:
        if not isinstance(actions, list):
            return ""
        for action in reversed(actions):
            if not isinstance(action, dict):
                continue
            offset = action.get("offset_bars")
            if isinstance(offset, dict) and offset.get("param_ref"):
                offset = offset["param_ref"]
            if offset in (None, ""):
                continue
            weights = action.get("weights", {})
            target = ", ".join(
                f"{asset}={float(weight):.0%}"
                for asset, weight in weights.items()
            ) if isinstance(weights, dict) else ""
            return f"after {offset} bars -> {target}".strip()
        return ""

    @staticmethod
    def _cost_label(value: Any) -> str:
        if not isinstance(value, dict):
            return ""
        return ", ".join(f"{key}={amount}" for key, amount in value.items())

    @staticmethod
    def _title(value: Any) -> str:
        return str(value or "").replace("_", " ").strip().title()

    @staticmethod
    def _records(frame: pd.DataFrame) -> list[Dict[str, Any]]:
        sanitized = frame.astype(object).where(pd.notna(frame), None)
        return [
            {str(key): value for key, value in row.items()}
            for row in sanitized.to_dict("records")
        ]

    def _validated_canonical_source(self, plot_bundle: Dict[str, Any]) -> Path:
        for raw_path in plot_bundle.get("artifact_source_refs", []):
            path = Path(str(raw_path))
            if path.suffix.lower() != ".json" or not path.is_file():
                continue
            payload = self._read_json(path, {})
            if (
                isinstance(payload, dict)
                and payload.get("schema_version") == "canonical_result_bundle.v1"
                and isinstance(payload.get("validation"), dict)
                and payload["validation"].get("status") == "valid"
                and payload.get("result_hashes") == plot_bundle.get("source_hashes")
            ):
                return path
        raise ValueError("PlotBundle does not reference a matching validated canonical result")

    @staticmethod
    def _validate_plot_bundle(payload: Any) -> None:
        if not isinstance(payload, dict) or payload.get("schema_version") != "plot_bundle.v1":
            raise ValueError("metrics overview requires PlotBundle.v1")
        if payload.get("contract_id") != "lo2cin4bt.plot_bundle.v1":
            raise ValueError("PlotBundle contract_id is invalid")
        if not isinstance(payload.get("series"), list) or not payload["series"]:
            raise ValueError("PlotBundle series is empty")
        hashes = payload.get("source_hashes")
        if not isinstance(hashes, list) or not hashes or any(
            len(str(value)) != 64 for value in hashes
        ):
            raise ValueError("PlotBundle source hashes are invalid")

    @staticmethod
    def _metric_row(metric_row: Dict[str, Any]) -> Dict[str, Any]:
        candidate_id = validate_canonical_candidate_id(
            metric_row.get("Backtest_id")
        )
        row: Dict[str, Any] = {
            "backtest_id": candidate_id,
            "label": candidate_id,
            "label_source": "rust_metrics_contract",
            "strategy_id": candidate_id,
            "semantic_combo": {},
            "semantic_fields": [],
            "date_range_start": metric_row.get("Date_start"),
            "date_range_end": metric_row.get("Date_end"),
            "last_trade_time": None,
            "annualization": copy.deepcopy(metric_row.get("Annualization")),
        }
        for public_key, rust_key in METRIC_KEYS.items():
            row[public_key] = MetricsContractPayloadService._finite(metric_row.get(rust_key))
        if row["trade_count"] is not None:
            row["trade_count"] = int(row["trade_count"])
        for public_key, rust_key in (
            ("projected_session_count", "Projected_session_count"),
            (
                "projected_return_interval_count",
                "Projected_return_interval_count",
            ),
        ):
            value = MetricsContractPayloadService._finite(metric_row.get(rust_key))
            row[public_key] = int(value) if value is not None else None
        return row

    @staticmethod
    def _annualization_contract(metadata_rows: list[Any]) -> Dict[str, Any]:
        contracts = [
            row.get("Annualization")
            for row in metadata_rows
            if isinstance(row, dict)
        ]
        if (
            len(contracts) != len(metadata_rows)
            or not contracts
            or any(not isinstance(contract, dict) for contract in contracts)
        ):
            raise ValueError(
                "metrics overview requires canonical Rust Annualization metadata"
            )
        expected = contracts[0]
        if not isinstance(expected, dict):
            raise ValueError(
                "metrics overview requires canonical Rust Annualization metadata"
            )
        if any(contract != expected for contract in contracts):
            raise ValueError(
                "metrics overview requires one consistent Annualization contract"
            )
        if (
            expected.get("schema_version") != "metrics_annualization.v1"
            or expected.get("basis") != "session_close_projection"
            or expected.get("projection_policy")
            != "last_accepted_equity_per_session"
        ):
            raise ValueError("metrics overview Annualization contract is invalid")
        periods_per_year = MetricsContractPayloadService._finite(
            expected.get("periods_per_year")
        )
        risk_free_rate = MetricsContractPayloadService._finite(
            expected.get("risk_free_rate_annual")
        )
        if (
            periods_per_year is None
            or periods_per_year <= 0
            or risk_free_rate is None
        ):
            raise ValueError("metrics overview Annualization values are invalid")
        return copy.deepcopy(expected)

    def _artifact_path(self, run_id: str, artifact_type: str) -> Optional[Path]:
        manifest = self.registry.load_artifact_manifest(run_id)
        for item in manifest.get("artifacts", []) if isinstance(manifest, dict) else []:
            if isinstance(item, dict) and item.get("artifact_type") == artifact_type:
                path = Path(str(item.get("path") or ""))
                if path.is_file():
                    return path
        return None

    @staticmethod
    def _finite(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    @staticmethod
    def _sort_value(value: Any) -> float:
        parsed = MetricsContractPayloadService._finite(value)
        return parsed if parsed is not None else float("-inf")

    @staticmethod
    def _read_json(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read JSON artifact {path}: {exc}") from exc
