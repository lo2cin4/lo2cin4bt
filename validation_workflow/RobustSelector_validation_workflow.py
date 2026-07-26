from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
from sklearn.cluster import KMeans


def _float_or_none(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class ClusterRepresentative:
    cluster_id: int
    params: Dict[str, Any]
    score: Optional[float]
    size: int


class RobustSelector:
    """Cluster and summarize robust WFA candidates across windows."""

    def __init__(self, random_seed: int = 42) -> None:
        self.random_seed = random_seed

    def cluster_candidates(
        self,
        candidates: Iterable[Dict[str, Any]],
        *,
        representative_mode: str = "cluster_median",
        n_clusters: Optional[int] = None,
    ) -> Dict[str, Any]:
        rows = [dict(candidate) for candidate in candidates if isinstance(candidate, dict)]
        if not rows:
            return {"clusters": [], "representatives": []}

        param_keys = sorted(
            {
                key
                for row in rows
                for key in (row.get("params", {}) or {}).keys()
            }
        )
        if not param_keys:
            return {"clusters": [], "representatives": []}

        encoded = np.array([self._encode_params(row.get("params", {}), param_keys) for row in rows], dtype=float)
        cluster_count = n_clusters or self._default_cluster_count(len(rows))
        cluster_count = max(1, min(cluster_count, len(rows)))

        labels: np.ndarray
        if cluster_count == 1:
            labels = np.zeros(len(rows), dtype=int)
        else:
            model = KMeans(n_clusters=cluster_count, random_state=self.random_seed, n_init="auto")
            labels = model.fit_predict(encoded)

        grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for label, row in zip(labels.tolist(), rows):
            grouped[label].append(row)

        cluster_summaries: List[Dict[str, Any]] = []
        representatives: List[Dict[str, Any]] = []
        for cluster_id, cluster_rows in sorted(grouped.items()):
            representative = self._select_representative(cluster_rows, representative_mode)
            cluster_summary = self._build_cluster_summary(cluster_id, cluster_rows, representative)
            cluster_summaries.append(cluster_summary)
            representatives.append(
                {
                    "cluster_id": cluster_id,
                    "params": representative.get("params", {}),
                    "score": _float_or_none(representative.get("robust_score")),
                    "size": len(cluster_rows),
                }
            )

        return {
            "clusters": cluster_summaries,
            "representatives": representatives,
            "param_keys": param_keys,
        }

    def _build_cluster_summary(
        self,
        cluster_id: int,
        rows: List[Dict[str, Any]],
        representative: Dict[str, Any],
    ) -> Dict[str, Any]:
        def mean_for(key: str) -> Optional[float]:
            values = [_float_or_none(row.get(key)) for row in rows]
            numeric = [value for value in values if value is not None]
            if not numeric:
                return None
            return float(sum(numeric) / len(numeric))

        def std_for(key: str) -> Optional[float]:
            values = [_float_or_none(row.get(key)) for row in rows]
            numeric = [value for value in values if value is not None]
            if len(numeric) < 2:
                return 0.0 if numeric else None
            return float(np.std(np.array(numeric, dtype=float)))

        mean_oos_sharpe = mean_for("mean_oos_sharpe")
        if mean_oos_sharpe is None:
            mean_oos_sharpe = mean_for("oos_sharpe")
        mean_oos_calmar = mean_for("mean_oos_calmar")
        if mean_oos_calmar is None:
            mean_oos_calmar = mean_for("oos_calmar")
        stability_std = std_for("mean_oos_sharpe")
        if stability_std is None:
            stability_std = std_for("oos_sharpe")

        return {
            "cluster_id": cluster_id,
            "size": len(rows),
            "representative_params": representative.get("params", {}),
            "representative_combo_label": representative.get("label"),
            "mean_oos_sharpe": mean_oos_sharpe,
            "mean_oos_calmar": mean_oos_calmar,
            "mean_profit_factor": mean_for("profit_factor"),
            "mean_win_rate": mean_for("win_rate"),
            "mean_oos_is_ratio": mean_for("oos_is_ratio"),
            "stability_std": stability_std,
            "rows": rows,
        }

    def _select_representative(
        self,
        rows: List[Dict[str, Any]],
        representative_mode: str,
    ) -> Dict[str, Any]:
        if representative_mode == "cluster_center":
            ranked = sorted(rows, key=lambda row: _float_or_none(row.get("robust_score")) or float("-inf"), reverse=True)
            return ranked[0]

        params_list = [row.get("params", {}) for row in rows]
        median_params: Dict[str, Any] = {}
        for key in sorted({k for params in params_list for k in params.keys()}):
            values = [params.get(key) for params in params_list if key in params]
            numeric = []
            all_numeric = True
            for value in values:
                try:
                    numeric.append(float(value))
                except (TypeError, ValueError):
                    all_numeric = False
                    break
            if all_numeric and numeric:
                median_params[key] = float(np.median(np.array(numeric, dtype=float)))
            elif values:
                median_params[key] = values[len(values) // 2]
        def distance(row: Dict[str, Any]) -> float:
            params = row.get("params", {})
            total = 0.0
            for key, median_value in median_params.items():
                value = params.get(key)
                try:
                    total += abs(float(value) - float(median_value))
                except (TypeError, ValueError):
                    total += 0.0 if value == median_value else 1.0
            return total

        ranked = sorted(rows, key=lambda row: (distance(row), -(_float_or_none(row.get("robust_score")) or float("-inf"))))
        return ranked[0]

    @staticmethod
    def _encode_params(params: Dict[str, Any], param_keys: List[str]) -> List[float]:
        encoded: List[float] = []
        for key in param_keys:
            value = params.get(key)
            try:
                if value is None:
                    raise TypeError
                encoded.append(float(value))
            except (TypeError, ValueError):
                encoded.append(float(abs(hash((key, str(value)))) % 1000))
        return encoded

    @staticmethod
    def _default_cluster_count(size: int) -> int:
        if size <= 3:
            return 1
        if size <= 8:
            return 2
        return min(5, max(2, int(round(size ** 0.5))))
