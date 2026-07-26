from __future__ import annotations

import copy
from collections import defaultdict
import math
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Tuple

from validation_workflow.RobustSelector_validation_workflow import RobustSelector
from validation_workflow.WFAAcceptanceEvaluator_validation_workflow import WFAAcceptanceEvaluator


OBJECTIVE_FIELDS = [
    "sharpe",
    "total_return",
    "cagr",
    "calmar",
    "max_drawdown",
    "profit_factor",
    "win_rate",
    "robust_score",
]

REVIEW_SNAPSHOT_FIELDS = [
    "average_drawdown",
    "rebalance_count",
    "exposure_time",
    "final_equity",
    "start_equity",
    "excess_return",
    "bah_total_return",
    "bah_cagr",
    "bah_sharpe",
    "bah_calmar",
    "bah_max_drawdown",
]

SEARCH_SOURCE_OPTIONS = [
    {"id": "all_existing_results", "label": "All Existing Results"},
    {"id": "optuna_suggested_candidates", "label": "Optuna Suggested Candidates"},
    {"id": "accepted_candidates", "label": "Accepted Candidates"},
]


def _float_or_none(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _required_float(row: Dict[str, Any], key: str) -> float:
    parsed = _float_or_none(row.get(key))
    if parsed is None:
        raise ValueError(f"Heatmap candidate requires finite metric: {key}")
    return parsed


def _safe_mean(values: Iterable[Any]) -> Optional[float]:
    numeric = [_float_or_none(value) for value in values]
    clean = [value for value in numeric if value is not None]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _safe_pstdev(values: Iterable[Any]) -> float:
    numeric = [_float_or_none(value) for value in values]
    clean = [value for value in numeric if value is not None]
    if len(clean) <= 1:
        return 0.0
    mean_value = sum(clean) / len(clean)
    variance = sum((value - mean_value) ** 2 for value in clean) / len(clean)
    return float(variance ** 0.5)


class HeatmapMatrixBuilder:
    """Build Mode A heatmap-first payloads from existing metrics rows."""

    REDUCTION_MODES = ["fixed", "top_n_median", "cluster_median"]
    AGGREGATION_MODES = ["mean", "median", "best", "worst", "std"]
    SHORTLIST_SORT = "stability_first"
    WFA_PACK_STRATEGIES = ["minimal", "balanced", "stability_first", "cluster_coverage"]
    DEFAULT_WFA_PACK_STRATEGY = "balanced"
    DEFAULT_RANKING_PROFILE = "balanced"
    RANKING_PROFILES: ClassVar[Dict[str, Dict[str, Any]]] = {
        "balanced": {
            "weights": {
                "sharpe_weight": 1.0,
                "plateau_weight": 0.35,
                "drawdown_penalty_weight": 0.3,
            },
            "sort_priority": ["robust_score", "local_plateau_score", "sharpe"],
        },
        "stability_first": {
            "weights": {
                "sharpe_weight": 0.75,
                "plateau_weight": 0.5,
                "drawdown_penalty_weight": 0.35,
            },
            "sort_priority": ["stability_score", "local_plateau_score", "robust_score", "sharpe"],
        },
        "performance_first": {
            "weights": {
                "sharpe_weight": 1.2,
                "plateau_weight": 0.2,
                "drawdown_penalty_weight": 0.2,
            },
            "sort_priority": ["robust_score", "sharpe", "local_plateau_score"],
        },
        "drawdown_aware": {
            "weights": {
                "sharpe_weight": 0.9,
                "plateau_weight": 0.35,
                "drawdown_penalty_weight": 0.5,
            },
            "sort_priority": ["robust_score", "max_drawdown", "local_plateau_score", "sharpe"],
        },
    }

    def __init__(self) -> None:
        self.robust_selector = RobustSelector()
        self.acceptance_evaluator = WFAAcceptanceEvaluator()

    def build_payload(
        self,
        *,
        run_id: str,
        rows: Iterable[Dict[str, Any]],
        param_axes: List[str],
        ranking_config: Optional[Dict[str, Any]] = None,
        acceptance_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        materialized_rows = [self._normalize_row(row, param_axes) for row in rows]
        if not materialized_rows:
            raise FileNotFoundError("metrics overview rows missing for parameter heatmap payload")
        self._validate_candidate_coordinates(materialized_rows, param_axes)
        resolved_ranking = self._resolve_ranking_config(ranking_config)
        resolved_acceptance = self._resolve_pre_review_acceptance_config(acceptance_config)

        axis_values = {
            axis: self._sorted_unique([row["params"].get(axis) for row in materialized_rows])
            for axis in param_axes
        }
        default_x, default_y = self._default_axes(param_axes)

        row_by_id = {str(row.get("backtest_id", "")): row for row in materialized_rows}
        plateau_summary = self._build_plateau_summary(materialized_rows, default_x, default_y)
        plateau_scores = plateau_summary["plateau_scores"]
        for row in materialized_rows:
            backtest_id = str(row["backtest_id"])
            if backtest_id not in plateau_scores:
                raise ValueError(
                    f"Heatmap plateau score missing for candidate: {backtest_id}"
                )
            row["local_plateau_score"] = plateau_scores[backtest_id]

        for row in materialized_rows:
            row["mean_oos_sharpe"] = row.get("sharpe")
            row["oos_is_ratio"] = row.get("oos_is_ratio")
            row["stability_score"] = self._build_stability_score(row)
            row["robust_score"] = self._build_robust_score(row, resolved_ranking)
            acceptance = self._classify_acceptance(row, resolved_acceptance)
            row["acceptance"] = acceptance["state"]
            row["acceptance_reason"] = acceptance["reason"]

        ranked_rows = sorted(
            materialized_rows,
            key=lambda row: self._row_rank_key(row, resolved_ranking),
            reverse=True,
        )
        for rank, row in enumerate(ranked_rows, start=1):
            row["rank"] = rank

        study_summary = self._build_study_summary(ranked_rows, param_axes, resolved_ranking)
        parameter_importance = self._build_parameter_importance(ranked_rows, param_axes)
        clustering_input = [self._to_cluster_candidate(row) for row in ranked_rows[: max(20, min(100, len(ranked_rows)))]]
        cluster_payload = self.robust_selector.cluster_candidates(
            clustering_input,
            representative_mode="cluster_median",
        )
        cluster_summary = cluster_payload.get("clusters", [])
        shortlist_rows = self._build_shortlist_rows(
            ranked_rows,
            row_by_id=row_by_id,
            cluster_payload=cluster_payload,
            plateau_summary=plateau_summary,
        )
        wfa_pack_previews = {
            strategy: self._build_wfa_pack_preview(shortlist_rows, strategy)
            for strategy in self.WFA_PACK_STRATEGIES
        }
        default_pack_preview = wfa_pack_previews.get(self.DEFAULT_WFA_PACK_STRATEGY, {"rows": []})
        default_pack_keys = {str(item.get("candidate_key", "")) for item in default_pack_preview.get("rows", [])}
        preview_reason_map = {
            str(item.get("candidate_key", "")): str(item.get("inclusion_reason", ""))
            for item in default_pack_preview.get("rows", [])
        }
        for row in shortlist_rows:
            candidate_key = str(row.get("candidate_key", ""))
            row["suggested_for_wfa"] = candidate_key in default_pack_keys
            row["select_default"] = candidate_key in default_pack_keys
            if candidate_key in preview_reason_map:
                row["wfa_pack_inclusion_reason"] = preview_reason_map[candidate_key]

        return {
            "schema_version": "3.1",
            "contract_id": "lo2cin4bt-app-parameter-heatmap-payload-v2",
            "run_id": run_id,
            "default_objective": "robust_score",
            "objectives": OBJECTIVE_FIELDS,
            "param_axes": param_axes,
            "default_x_axis": default_x,
            "default_y_axis": default_y,
            "reduction_modes": self.REDUCTION_MODES,
            "aggregation_modes": self.AGGREGATION_MODES,
            "axis_values": axis_values,
            "rows": ranked_rows,
            "search_source_options": SEARCH_SOURCE_OPTIONS,
            "default_search_source": "all_existing_results",
            "search_source": "all_existing_results",
            "ml_search_status": "completed",
            "study_summary": study_summary,
            "parameter_importance": parameter_importance,
            "shortlist_rows": shortlist_rows,
            "cluster_summary": cluster_summary,
            "plateau_summary": {
                "default_x_axis": default_x,
                "default_y_axis": default_y,
                "top_cells": plateau_summary["top_cells"],
            },
            "wfa_pack_strategies": self.WFA_PACK_STRATEGIES,
            "default_wfa_pack_strategy": self.DEFAULT_WFA_PACK_STRATEGY,
            "wfa_pack_previews": wfa_pack_previews,
            "ranking_config": resolved_ranking,
            "pre_review_acceptance_config": resolved_acceptance,
            "shortlist_sort": self.SHORTLIST_SORT,
            "selected_representative_mode": self.DEFAULT_WFA_PACK_STRATEGY,
        }

    def build_matrix(
        self,
        *,
        rows: Iterable[Dict[str, Any]],
        x_axis: str,
        y_axis: str,
        objective: str,
        aggregation: str = "median",
        fixed_params: Optional[Dict[str, Any]] = None,
        reduction_mode: str = "fixed",
    ) -> Dict[str, Any]:
        rows_list = [row for row in rows if isinstance(row, dict)]
        filtered = self._apply_fixed_filters(rows_list, x_axis, y_axis, fixed_params or {}, reduction_mode)
        x_values = self._sorted_unique([row["params"].get(x_axis) for row in filtered])
        y_values = self._sorted_unique([row["params"].get(y_axis) for row in filtered])
        cell_map: Dict[Tuple[Any, Any], List[float]] = defaultdict(list)
        plateau_map: Dict[Tuple[Any, Any], List[float]] = defaultdict(list)
        for row in filtered:
            metric = _float_or_none(row.get(objective))
            if metric is None:
                continue
            cell_key = (row["params"].get(x_axis), row["params"].get(y_axis))
            cell_map[cell_key].append(metric)
            plateau_score = _float_or_none(row.get("local_plateau_score"))
            if plateau_score is not None:
                plateau_map[cell_key].append(plateau_score)

        z: List[List[Optional[float]]] = []
        counts: List[List[int]] = []
        plateau_scores: List[List[Optional[float]]] = []
        for y in y_values:
            z_row: List[Optional[float]] = []
            count_row: List[int] = []
            plateau_row: List[Optional[float]] = []
            for x in x_values:
                values = cell_map.get((x, y), [])
                plateau_values = plateau_map.get((x, y), [])
                z_row.append(self._aggregate(values, aggregation))
                count_row.append(len(values))
                plateau_row.append(self._aggregate(plateau_values, "mean"))
            z.append(z_row)
            counts.append(count_row)
            plateau_scores.append(plateau_row)
        return {
            "x_axis": x_axis,
            "y_axis": y_axis,
            "objective": objective,
            "aggregation": aggregation,
            "x_values": x_values,
            "y_values": y_values,
            "z": z,
            "counts": counts,
            "plateau_scores": plateau_scores,
        }

    def _normalize_row(self, row: Dict[str, Any], param_axes: List[str]) -> Dict[str, Any]:
        semantic_combo = row.get("semantic_combo", {}) or {}
        params = {axis: semantic_combo.get(axis) for axis in param_axes}
        normalized = {
            "backtest_id": row.get("backtest_id"),
            "label": row.get("label"),
            "params": params,
            **{key: row.get(key) for key in OBJECTIVE_FIELDS if key in row},
            **{key: row.get(key) for key in REVIEW_SNAPSHOT_FIELDS if key in row},
            "trade_count": row.get("trade_count"),
            "exposure_time": row.get("exposure_time"),
            "date_range_start": row.get("date_range_start"),
            "date_range_end": row.get("date_range_end"),
            "last_trade_time": row.get("last_trade_time"),
            "strategy_id": row.get("strategy_id"),
            "strategy_display_label": row.get("strategy_display_label") or row.get("label"),
            "semantic_combo": semantic_combo,
            "result_type": row.get("result_type"),
            "result_materialization": row.get("result_materialization"),
            "artifact_available": row.get("artifact_available"),
            "metric_source": row.get("metric_source"),
            "final_equity": row.get("final_equity"),
            "avg_turnover": row.get("avg_turnover"),
            "avg_gross_exposure": row.get("avg_gross_exposure"),
            "rebalance_count": row.get("rebalance_count"),
            "days": row.get("days"),
        }
        normalized["max_drawdown"] = row.get("max_drawdown", row.get("mdd"))
        return normalized

    @staticmethod
    def _validate_candidate_coordinates(
        rows: List[Dict[str, Any]],
        param_axes: List[str],
    ) -> None:
        seen_ids: set[str] = set()
        for row in rows:
            backtest_id = str(row.get("backtest_id") or "").strip()
            if not backtest_id:
                raise ValueError("Heatmap candidate requires a non-empty backtest_id")
            if backtest_id in seen_ids:
                raise ValueError(f"Heatmap candidate backtest_id is duplicated: {backtest_id}")
            seen_ids.add(backtest_id)
            missing_axes = [
                axis for axis in param_axes if row.get("params", {}).get(axis) is None
            ]
            if missing_axes:
                raise ValueError(
                    "Heatmap parameter coordinate is missing for "
                    f"{backtest_id}: {', '.join(missing_axes)}"
                )

    def _build_plateau_summary(
        self,
        rows: List[Dict[str, Any]],
        x_axis: str,
        y_axis: str,
    ) -> Dict[str, Any]:
        value_map: Dict[Tuple[Any, Any], List[float]] = defaultdict(list)
        cell_backtests: Dict[Tuple[Any, Any], List[str]] = defaultdict(list)
        for row in rows:
            x_value = row["params"].get(x_axis)
            y_value = row["params"].get(y_axis)
            score = _float_or_none(row.get("robust_score"))
            if score is None:
                score = _float_or_none(row.get("sharpe"))
            if x_value is None or y_value is None or score is None:
                continue
            key = (x_value, y_value)
            value_map[key].append(score)
            cell_backtests[key].append(str(row.get("backtest_id", "")))

        cell_scores: Dict[Tuple[Any, Any], float] = {}
        for key, values in value_map.items():
            score_value = _safe_mean(values)
            if values and score_value is not None:
                cell_scores[key] = score_value
        sorted_cells = sorted(cell_scores.items(), key=lambda item: item[1], reverse=True)
        plateau_scores: Dict[str, float] = {}
        for key, backtest_ids in cell_backtests.items():
            neighbors = self._neighbor_values(key, cell_scores)
            center_score = cell_scores.get(key)
            score = self._compute_plateau_score(center_score, neighbors)
            for backtest_id in backtest_ids:
                plateau_scores[backtest_id] = score

        top_cells = [
            {
                "x": cell_key[0],
                "y": cell_key[1],
                "score": float(score),
                "sample_count": len(cell_backtests.get(cell_key, [])),
                "plateau_score": self._compute_plateau_score(score, self._neighbor_values(cell_key, cell_scores)),
            }
            for cell_key, score in sorted_cells[:12]
        ]
        return {
            "plateau_scores": plateau_scores,
            "top_cells": top_cells,
        }

    def _neighbor_values(
        self,
        cell_key: Tuple[Any, Any],
        cell_scores: Dict[Tuple[Any, Any], float],
    ) -> List[float]:
        x_value, y_value = cell_key
        neighbors: List[float] = []
        for (other_x, other_y), score in cell_scores.items():
            if other_x == x_value and other_y == y_value:
                continue
            try:
                x_distance = abs(float(other_x) - float(x_value))
                y_distance = abs(float(other_y) - float(y_value))
                if x_distance <= 1e-9 and y_distance <= 1e-9:
                    continue
                if x_distance <= 20 and y_distance <= 20:
                    neighbors.append(score)
            except (TypeError, ValueError):
                continue
        return neighbors

    @staticmethod
    def _compute_plateau_score(center_score: Optional[float], neighbors: List[float]) -> float:
        if center_score is None or not neighbors:
            return 0.0
        neighbor_mean = _safe_mean(neighbors)
        if neighbor_mean is None:
            return 0.0
        neighbor_std = _safe_pstdev(neighbors)
        threshold = center_score * 0.9 if center_score >= 0 else center_score * 1.1
        if center_score >= 0:
            passing = [value for value in neighbors if value >= threshold]
        else:
            passing = [value for value in neighbors if value >= center_score]
        support = len(passing) / len(neighbors)
        baseline = max(abs(center_score), 1.0)
        stability = max(0.0, 1.0 - (neighbor_std / baseline))
        return round((support * 0.6) + (stability * 0.4), 4)

    def _build_stability_score(self, row: Dict[str, Any]) -> float:
        plateau = _required_float(row, "local_plateau_score")
        max_drawdown = abs(_required_float(row, "max_drawdown"))
        drawdown_component = max(0.0, 1.0 - min(1.0, max_drawdown))
        components = [(plateau, 0.45), (drawdown_component, 0.2)]

        exposure = _float_or_none(row.get("exposure_time"))
        if exposure is not None:
            exposure_ratio = exposure if exposure <= 1.0 else exposure / 100.0
            components.append((min(1.0, max(0.0, exposure_ratio)), 0.2))

        trade_count = _float_or_none(row.get("trade_count"))
        if trade_count is not None:
            components.append((min(1.0, max(0.0, trade_count) / 30.0), 0.15))

        available_weight = sum(weight for _, weight in components)
        if available_weight <= 0.0:
            raise ValueError("Heatmap stability score has no available components")
        return round(
            sum(value * weight for value, weight in components) / available_weight,
            4,
        )

    def _build_robust_score(self, row: Dict[str, Any], ranking_config: Dict[str, Any]) -> float:
        weights = ranking_config.get("weights", {})
        sharpe = _required_float(row, "sharpe")
        plateau = _required_float(row, "local_plateau_score")
        max_drawdown = abs(_required_float(row, "max_drawdown"))
        drawdown_penalty = min(1.0, max_drawdown)
        sharpe_weight = _required_float(weights, "sharpe_weight")
        plateau_weight = _required_float(weights, "plateau_weight")
        drawdown_weight = _required_float(weights, "drawdown_penalty_weight")
        return round(
            (sharpe * sharpe_weight)
            + (plateau * plateau_weight)
            - (drawdown_penalty * drawdown_weight),
            4,
        )

    def _classify_acceptance(self, row: Dict[str, Any], acceptance_config: Dict[str, Any]) -> Dict[str, str]:
        result = WFAAcceptanceEvaluator(acceptance_config).evaluate(
            {
                "mean_oos_sharpe": row.get("mean_oos_sharpe"),
                "profit_factor": row.get("profit_factor"),
                "win_rate": row.get("win_rate"),
                "trade_count": row.get("trade_count"),
                "max_drawdown": row.get("max_drawdown"),
                "oos_std": max(
                    0.0,
                    1.0 - _required_float(row, "local_plateau_score"),
                ),
            }
        )
        if result.accepted:
            return {"state": "Pass", "reason": "meets_acceptance_gates"}
        plateau = _required_float(row, "local_plateau_score")
        if plateau >= 0.45 or _required_float(row, "sharpe") >= 0.75:
            return {"state": "Review", "reason": ",".join(result.reasons) or "needs_wfa_validation"}
        return {"state": "Fail", "reason": ",".join(result.reasons) or "below_threshold"}

    def _build_study_summary(self, rows: List[Dict[str, Any]], param_axes: List[str], ranking_config: Dict[str, Any]) -> Dict[str, Any]:
        best = rows[0] if rows else {}
        warnings: List[str] = []
        if len(param_axes) > 4:
            warnings.append("overfitting_risk_more_than_4_free_params")
        warnings.append("study_summary_derived_from_existing_results")
        return {
            "sampler": "tpe",
            "mode": "single_objective",
            "objective": "robust_score",
            "n_trials": len(rows),
            "n_startup_trials": min(12, len(rows)),
            "completed_trials": len(rows),
            "pruned_trials": 0,
            "best_robust_score": _float_or_none(best.get("robust_score")),
            "best_params": best.get("params", {}),
            "accepted_candidate_count": sum(1 for row in rows if row.get("acceptance") == "Pass"),
            "cluster_count": None,
            "ranking_profile": ranking_config.get("profile"),
            "sort_priority": ranking_config.get("sort_priority", []),
            "warnings": warnings,
        }

    def _build_parameter_importance(self, rows: List[Dict[str, Any]], param_axes: List[str]) -> List[Dict[str, Any]]:
        scored_rows = [row for row in rows if _float_or_none(row.get("robust_score")) is not None]
        if not scored_rows:
            return []
        overall_mean = _safe_mean(
            _required_float(row, "robust_score") for row in scored_rows
        )
        if overall_mean is None:
            raise ValueError("Heatmap parameter importance requires robust scores")
        output: List[Dict[str, Any]] = []
        for axis in param_axes:
            grouped: Dict[str, List[float]] = defaultdict(list)
            for row in scored_rows:
                key = str(row.get("params", {}).get(axis))
                grouped[key].append(_required_float(row, "robust_score"))
            group_means = [value for value in (_safe_mean(values) for values in grouped.values() if values) if value is not None]
            dispersion = _safe_pstdev(group_means)
            normalized = min(1.0, abs(dispersion) / max(1.0, abs(overall_mean) + 0.001))
            output.append(
                {
                    "parameter": axis,
                    "importance": round(normalized, 4),
                    "unique_values": len(grouped),
                }
            )
        output.sort(key=lambda item: item["importance"], reverse=True)
        return output

    def _to_cluster_candidate(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "backtest_id": row.get("backtest_id"),
            "label": row.get("label"),
            "params": row.get("params", {}),
            "mean_oos_sharpe": row.get("sharpe"),
            "oos_sharpe": row.get("sharpe"),
            "oos_calmar": row.get("calmar"),
            "profit_factor": row.get("profit_factor"),
            "win_rate": row.get("win_rate"),
            "trade_count": row.get("trade_count"),
            "oos_is_ratio": row.get("oos_is_ratio"),
            "robust_score": row.get("robust_score"),
            "max_drawdown": row.get("max_drawdown"),
            "local_plateau_score": row.get("local_plateau_score"),
            "stability_score": row.get("stability_score"),
        }

    def _build_shortlist_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        row_by_id: Dict[str, Dict[str, Any]],
        cluster_payload: Dict[str, Any],
        plateau_summary: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        shortlist: List[Dict[str, Any]] = []
        seen: set[str] = set()

        def add_row(base_row: Dict[str, Any], representative_type: str, source: str, *, select_default: bool) -> None:
            backtest_id = str(base_row.get("backtest_id", ""))
            unique_key = f"{representative_type}:{backtest_id}"
            if not backtest_id or unique_key in seen:
                return
            seen.add(unique_key)
            shortlist.append(
                {
                    "select_default": select_default,
                    "rank": len(shortlist) + 1,
                    "representative_type": representative_type,
                    "label": base_row.get("label"),
                    "params": base_row.get("params", {}),
                    "source": source,
                    "robust_score": _float_or_none(base_row.get("robust_score")),
                    "mean_oos_sharpe": _float_or_none(base_row.get("mean_oos_sharpe")),
                    "oos_is_ratio": _float_or_none(base_row.get("oos_is_ratio")),
                    "stability_score": _float_or_none(base_row.get("stability_score")),
                    "cluster_id": base_row.get("cluster_id"),
                    "cluster_size": base_row.get("cluster_size"),
                    "local_plateau_score": _float_or_none(base_row.get("local_plateau_score")),
                    "total_return": _float_or_none(base_row.get("total_return")),
                    "cagr": _float_or_none(base_row.get("cagr")),
                    "calmar": _float_or_none(base_row.get("calmar")),
                    "trade_count": base_row.get("trade_count"),
                    "rebalance_count": base_row.get("rebalance_count"),
                    "exposure_time": _float_or_none(base_row.get("exposure_time")),
                    "final_equity": _float_or_none(base_row.get("final_equity")),
                    "excess_return": _float_or_none(base_row.get("excess_return")),
                    "max_drawdown": _float_or_none(base_row.get("max_drawdown")),
                    "profit_factor": _float_or_none(base_row.get("profit_factor")),
                    "win_rate": _float_or_none(base_row.get("win_rate")),
                    "acceptance": base_row.get("acceptance", "Review"),
                    "reason": base_row.get("acceptance_reason", "needs_wfa_validation"),
                    "backtest_id": backtest_id,
                    "candidate_key": unique_key,
                }
            )

        if rows:
            add_row(rows[0], "Top Trial", "Optuna", select_default=True)

        plateau_ranked = sorted(
            rows,
            key=lambda row: (
                _required_float(row, "local_plateau_score"),
                _required_float(row, "robust_score"),
            ),
            reverse=True,
        )
        if plateau_ranked:
            add_row(plateau_ranked[0], "Plateau Center", "Heatmap Plateau", select_default=True)
        if len(plateau_ranked) > 1:
            add_row(plateau_ranked[1], "Plateau Edge", "Heatmap Plateau", select_default=False)

        cluster_reps = cluster_payload.get("representatives", [])
        cluster_rows = {int(cluster.get("cluster_id", -1)): cluster for cluster in cluster_payload.get("clusters", [])}
        for rep in cluster_reps:
            params = rep.get("params", {}) or {}
            match = self._find_matching_row(rows, params)
            if not match:
                continue
            cluster_id = int(rep.get("cluster_id", -1))
            cluster_info = cluster_rows.get(cluster_id, {})
            match = dict(match)
            match["cluster_id"] = cluster_id
            match["cluster_size"] = cluster_info.get("size")
            add_row(match, "Cluster Median", "Cluster", select_default=len(shortlist) < 3)

        shortlist.sort(
            key=lambda row: (
                self._acceptance_rank(row.get("acceptance")),
                _required_float(row, "stability_score"),
                _required_float(row, "local_plateau_score"),
                _required_float(row, "robust_score"),
                _required_float(row, "mean_oos_sharpe"),
            ),
            reverse=True,
        )
        for rank, row in enumerate(shortlist, start=1):
            row["rank"] = rank
        return shortlist

    def _build_wfa_pack_preview(
        self,
        shortlist_rows: List[Dict[str, Any]],
        strategy: str,
    ) -> Dict[str, Any]:
        normalized_strategy = str(strategy or self.DEFAULT_WFA_PACK_STRATEGY).strip().lower()
        candidate_rows = [dict(row) for row in shortlist_rows if isinstance(row, dict)]
        if not candidate_rows:
            return {
                "strategy": normalized_strategy,
                "label": self._format_pack_label(normalized_strategy),
                "candidate_count": 0,
                "rows": [],
            }

        accepted_or_review = [
            row
            for row in candidate_rows
            if str(row.get("acceptance", "Review")).lower() in {"pass", "review"}
        ] or candidate_rows
        selected: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()

        def add_row(
            row: Optional[Dict[str, Any]],
            inclusion_reason: str,
            *,
            allow_duplicate_cluster: bool = True,
        ) -> None:
            if not row:
                return
            candidate_key = str(row.get("candidate_key", ""))
            if not candidate_key or candidate_key in seen_keys:
                return
            if not allow_duplicate_cluster:
                cluster_id = row.get("cluster_id")
                if cluster_id is not None:
                    if any(existing.get("cluster_id") == cluster_id for existing in selected):
                        return
            seen_keys.add(candidate_key)
            selected.append(
                {
                    "candidate_key": candidate_key,
                    "backtest_id": row.get("backtest_id"),
                    "label": row.get("label"),
                    "representative_type": row.get("representative_type"),
                    "source": row.get("source"),
                    "cluster_id": row.get("cluster_id"),
                    "cluster_size": row.get("cluster_size"),
                    "inclusion_reason": inclusion_reason,
                }
            )

        top_ranked = next((row for row in accepted_or_review if str(row.get("representative_type", "")).lower() == "top trial"), None)
        plateau_rows = [
            row
            for row in accepted_or_review
            if str(row.get("source", "")).lower() == "heatmap plateau"
        ]
        cluster_medians = [
            row
            for row in accepted_or_review
            if str(row.get("representative_type", "")).lower() == "cluster median"
        ]
        if normalized_strategy == "minimal":
            add_row(top_ranked, "Composite score leader for the current review pool.")
            add_row(plateau_rows[0] if plateau_rows else None, "Stable region representative for a minimal WFA sanity check.")
        elif normalized_strategy == "stability_first":
            ordered = sorted(
                accepted_or_review,
                key=lambda row: (
                    _required_float(row, "stability_score"),
                    _required_float(row, "local_plateau_score"),
                    _required_float(row, "robust_score"),
                ),
                reverse=True,
            )
            for row in ordered[:5]:
                add_row(row, "Selected by the stability-first WFA pack profile.")
        elif normalized_strategy == "cluster_coverage":
            add_row(top_ranked, "Composite score leader kept as a baseline reference.")
            for row in cluster_medians:
                add_row(row, "Typical representative for a distinct parameter family.", allow_duplicate_cluster=False)
            if len(selected) < 4:
                for row in plateau_rows:
                    add_row(row, "Stable region representative added to cover local robustness.")
                    if len(selected) >= 4:
                        break
        else:
            add_row(top_ranked, "Composite score leader for the review pool.")
            add_row(plateau_rows[0] if plateau_rows else None, "Primary stable-region representative.")
            add_row(plateau_rows[1] if len(plateau_rows) > 1 else None, "Secondary stable-region representative for boundary checking.")
            for row in cluster_medians[:2]:
                add_row(row, "Typical representative for a major parameter family.", allow_duplicate_cluster=False)

        return {
            "strategy": normalized_strategy,
            "label": self._format_pack_label(normalized_strategy),
            "candidate_count": len(selected),
            "rows": selected,
        }

    @staticmethod
    def _format_pack_label(value: str) -> str:
        return str(value or "").replace("_", " ").title()

    @staticmethod
    def _find_matching_row(rows: List[Dict[str, Any]], params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for row in rows:
            if row.get("params", {}) == params:
                return row
        return None

    @staticmethod
    def _acceptance_rank(value: Any) -> int:
        text = str(value or "Review").lower()
        if text == "pass":
            return 3
        if text == "review":
            return 2
        return 1

    def _row_rank_key(self, row: Dict[str, Any], ranking_config: Dict[str, Any]) -> Tuple[float, ...]:
        output: List[float] = []
        for key in ranking_config.get("sort_priority", []):
            if key == "max_drawdown":
                raw_value = _float_or_none(row.get("max_drawdown"))
                value = (
                    -abs(raw_value)
                    if raw_value is not None
                    else float("-inf")
                )
            else:
                raw_value = _float_or_none(row.get(key))
                if raw_value is None:
                    value = float("-inf")
                else:
                    value = raw_value
            output.append(value)
        return tuple(output)

    def _resolve_ranking_config(self, ranking_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        config = copy.deepcopy(ranking_config or {})
        profile = str(
            config.get("profile", self.DEFAULT_RANKING_PROFILE)
            or self.DEFAULT_RANKING_PROFILE
        ).strip().lower()
        if profile not in self.RANKING_PROFILES:
            raise ValueError(f"Unknown heatmap ranking profile: {profile}")
        base = copy.deepcopy(self.RANKING_PROFILES[profile])
        overrides = config.get("weights", {})
        if isinstance(overrides, dict):
            weights = base.get("weights")
            if isinstance(weights, dict):
                weights.update({key: value for key, value in overrides.items() if value is not None})
        sort_priority = config.get("sort_priority")
        if isinstance(sort_priority, list) and sort_priority:
            base["sort_priority"] = [str(item) for item in sort_priority if str(item).strip()]
        base["profile"] = profile
        return base

    def _resolve_pre_review_acceptance_config(
        self,
        acceptance_config: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        resolved = copy.deepcopy(acceptance_config or {})
        resolved["min_oos_is_ratio"] = None
        return resolved

    def _apply_fixed_filters(
        self,
        rows: List[Dict[str, Any]],
        x_axis: str,
        y_axis: str,
        fixed_params: Dict[str, Any],
        reduction_mode: str,
    ) -> List[Dict[str, Any]]:
        if reduction_mode != "fixed" or not fixed_params:
            return rows
        filtered = rows
        for key, value in fixed_params.items():
            if key in {x_axis, y_axis}:
                continue
            filtered = [row for row in filtered if row["params"].get(key) == value]
        return filtered

    @staticmethod
    def _aggregate(values: List[float], aggregation: str) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        if aggregation == "mean":
            return float(sum(ordered) / len(ordered))
        if aggregation == "best":
            return float(max(ordered))
        if aggregation == "worst":
            return float(min(ordered))
        if aggregation == "std":
            mean_value = sum(ordered) / len(ordered)
            variance = sum((value - mean_value) ** 2 for value in ordered) / len(ordered)
            return float(variance ** 0.5)
        if aggregation == "median":
            mid = len(ordered) // 2
            if len(ordered) % 2:
                return float(ordered[mid])
            return float((ordered[mid - 1] + ordered[mid]) / 2)
        return float(sum(ordered) / len(ordered))

    @staticmethod
    def _sorted_unique(values: Iterable[Any]) -> List[Any]:
        unique = {value for value in values if value is not None}
        try:
            return sorted(unique)
        except TypeError:
            return sorted(unique, key=lambda item: str(item))

    @staticmethod
    def _default_axes(param_axes: List[str]) -> Tuple[str, str]:
        if len(param_axes) >= 2:
            return param_axes[0], param_axes[1]
        if len(param_axes) == 1:
            return param_axes[0], param_axes[0]
        return "x", "y"
