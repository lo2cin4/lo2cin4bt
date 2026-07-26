from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, List, Optional


def _float_or_none(value: Any) -> Optional[float]:
    try:
        cast = float(value)
    except (TypeError, ValueError):
        return None
    return cast if math.isfinite(cast) else None


def _first_present(metrics: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in metrics and metrics[key] is not None:
            return metrics[key]
    return None


@dataclass(slots=True)
class AcceptanceResult:
    accepted: bool
    reasons: List[str]
    robust_score: Optional[float]
    metrics: Dict[str, Optional[float]]


class WFAAcceptanceEvaluator:
    """Evaluate robust WFA candidates against acceptance gates."""

    DEFAULTS: Dict[str, Any] = {
        "min_oos_is_ratio": 0.7,
        "min_oos_sharpe": 0.0,
        "min_oos_calmar": 0.0,
        "max_drawdown_floor": None,
        "min_profit_factor": None,
        "min_win_rate": None,
        "min_trade_count": None,
        "stability_penalty_weight": 0.25,
        "drawdown_penalty_weight": 0.25,
    }

    def __init__(self, acceptance_config: Optional[Dict[str, Any]] = None) -> None:
        self.acceptance_config = {**self.DEFAULTS, **(acceptance_config or {})}

    def evaluate(self, metrics: Dict[str, Any]) -> AcceptanceResult:
        reasons: List[str] = []
        normalized = self._normalize(metrics)
        min_oos_sharpe = self.acceptance_config.get("min_oos_sharpe")
        mean_oos_sharpe = normalized.get("mean_oos_sharpe")
        if min_oos_sharpe is not None and (
            mean_oos_sharpe is None or mean_oos_sharpe <= float(min_oos_sharpe)
        ):
            reasons.append("oos_sharpe_not_positive")

        min_oos_calmar = self.acceptance_config.get("min_oos_calmar")
        oos_calmar = normalized.get("mean_oos_calmar")
        if (
            min_oos_calmar is not None
            and (oos_calmar is None or oos_calmar <= float(min_oos_calmar))
        ):
            reasons.append("oos_calmar_not_positive")

        min_oos_is_ratio = self.acceptance_config.get("min_oos_is_ratio")
        if min_oos_is_ratio is not None and self._ratio_gate_applies(normalized):
            ratio = normalized.get("oos_is_ratio")
            if ratio is None or ratio < float(min_oos_is_ratio):
                reasons.append("oos_is_ratio_below_threshold")

        max_drawdown_floor = self.acceptance_config.get("max_drawdown_floor")
        max_drawdown = normalized.get("max_drawdown")
        if max_drawdown_floor is not None and (
            max_drawdown is None or max_drawdown < float(max_drawdown_floor)
        ):
            reasons.append("max_drawdown_floor_breached")

        min_profit_factor = self.acceptance_config.get("min_profit_factor")
        profit_factor = normalized.get("profit_factor")
        if min_profit_factor is not None and (
            profit_factor is None or profit_factor < float(min_profit_factor)
        ):
            reasons.append("profit_factor_below_threshold")

        min_win_rate = self.acceptance_config.get("min_win_rate")
        win_rate = normalized.get("win_rate")
        if min_win_rate is not None and (
            win_rate is None or win_rate < float(min_win_rate)
        ):
            reasons.append("win_rate_below_threshold")

        min_trade_count = self.acceptance_config.get("min_trade_count")
        trade_count = normalized.get("trade_count")
        if min_trade_count is not None and (
            trade_count is None or trade_count < float(min_trade_count)
        ):
            reasons.append("trade_count_below_threshold")

        robust_score = self.compute_robust_score(normalized)
        return AcceptanceResult(
            accepted=not reasons,
            reasons=reasons,
            robust_score=robust_score,
            metrics=normalized,
        )

    def compute_robust_score(self, metrics: Dict[str, Optional[float]]) -> Optional[float]:
        mean_oos_sharpe = metrics.get("mean_oos_sharpe")
        if mean_oos_sharpe is None:
            return None
        oos_std = metrics.get("oos_std")
        max_drawdown = metrics.get("max_drawdown")
        if oos_std is None or max_drawdown is None:
            return None
        ratio_weight = (
            1.0
            if self.acceptance_config.get("min_oos_is_ratio") is not None
            and self._ratio_gate_applies(metrics)
            else 0.0
        )
        ratio_value = metrics.get("oos_is_ratio")
        if ratio_weight and ratio_value is None:
            return None
        ratio = (ratio_value if ratio_value is not None else 0.0) * ratio_weight
        stability_weight = float(self.acceptance_config["stability_penalty_weight"])
        drawdown_weight = float(self.acceptance_config["drawdown_penalty_weight"])
        stability_penalty = abs(oos_std) * stability_weight
        drawdown_penalty = abs(min(max_drawdown, 0.0)) * drawdown_weight
        return float(mean_oos_sharpe + ratio - stability_penalty - drawdown_penalty)

    def _normalize(self, metrics: Dict[str, Any]) -> Dict[str, Optional[float]]:
        is_metric = _float_or_none(
            _first_present(metrics, "mean_is_sharpe", "is_sharpe")
        )
        oos_metric = _float_or_none(
            _first_present(metrics, "mean_oos_sharpe", "oos_sharpe")
        )
        ratio = _float_or_none(metrics.get("oos_is_ratio"))
        if (
            ratio is None
            and is_metric is not None
            and is_metric != 0.0
            and oos_metric is not None
            and is_metric > 0
            and oos_metric > 0
        ):
            ratio = oos_metric / is_metric
        return {
            "mean_oos_sharpe": oos_metric,
            "mean_is_sharpe": is_metric,
            "oos_is_ratio": ratio,
            "mean_oos_calmar": _float_or_none(
                _first_present(metrics, "mean_oos_calmar", "oos_calmar")
            ),
            "oos_calmar": _float_or_none(
                _first_present(metrics, "mean_oos_calmar", "oos_calmar")
            ),
            "profit_factor": _float_or_none(
                _first_present(metrics, "oos_profit_factor", "profit_factor")
            ),
            "win_rate": _float_or_none(
                _first_present(metrics, "oos_win_rate", "win_rate")
            ),
            "trade_count": _float_or_none(metrics.get("trade_count")),
            "oos_std": _float_or_none(metrics.get("oos_std")),
            "max_drawdown": _float_or_none(
                _first_present(metrics, "max_drawdown", "oos_max_drawdown")
            ),
        }

    @staticmethod
    def _ratio_gate_applies(metrics: Dict[str, Optional[float]]) -> bool:
        is_metric = metrics.get("mean_is_sharpe")
        oos_metric = metrics.get("mean_oos_sharpe")
        return (
            isinstance(is_metric, (int, float))
            and isinstance(oos_metric, (int, float))
            and is_metric > 0
            and oos_metric > 0
        )
