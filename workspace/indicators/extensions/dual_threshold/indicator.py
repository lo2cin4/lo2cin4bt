from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np

from backtester.IndicatorParams_backtester import IndicatorParams


class DualThresholdIndicator:
    """Example extension indicator for multi-column entry confirmation."""

    STRATEGY_DESCRIPTIONS = [
        (
            "Emit a long-entry confirmation when both configured column conditions "
            "become true. Exit is expected to be defined separately."
        )
    ]

    @staticmethod
    def get_strategy_descriptions() -> Dict[str, str]:
        return {"DUAL1": DualThresholdIndicator.STRATEGY_DESCRIPTIONS[0]}

    @staticmethod
    def _require_config(params_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if params_config is None:
            raise ValueError("DUAL extension requires params_config")
        return params_config

    @classmethod
    def get_params(
        cls,
        strat_idx: Optional[int] = None,
        params_config: Optional[Dict[str, Any]] = None,
    ) -> List[IndicatorParams]:
        if strat_idx not in (1, None):
            raise ValueError("DUAL currently supports only DUAL1")

        config = cls._require_config(params_config)
        primary_column = str(config.get("primary_column", "")).strip()
        confirm_column = str(config.get("confirm_column", "")).strip()
        if not primary_column or not confirm_column:
            raise ValueError("DUAL requires primary_column and confirm_column")

        params = IndicatorParams("DUAL")
        params.add_param("primary_column", primary_column, param_type="string")
        params.add_param("confirm_column", confirm_column, param_type="string")
        params.add_param(
            "primary_threshold", float(config.get("primary_threshold")), param_type="numeric"
        )
        params.add_param(
            "confirm_threshold", float(config.get("confirm_threshold")), param_type="numeric"
        )
        params.add_param(
            "primary_op", str(config.get("primary_op", "gt")).lower(), param_type="string"
        )
        params.add_param(
            "confirm_op", str(config.get("confirm_op", "gt")).lower(), param_type="string"
        )
        params.add_param("strat_idx", 1, param_type="numeric")
        return [params]

    def __init__(self, data, params: IndicatorParams, logger=None) -> None:
        self.data = data
        self.params = params
        self.logger = logger

    @staticmethod
    def _compare(values: np.ndarray, threshold: float, op: str) -> np.ndarray:
        if op == "gt":
            return values > threshold
        if op == "gte":
            return values >= threshold
        if op == "lt":
            return values < threshold
        if op == "lte":
            return values <= threshold
        raise ValueError(f"unsupported DUAL operator: {op}")

    def generate_signals(self, _predictor: Optional[str] = None) -> np.ndarray:
        primary_column = self.params.get_param("primary_column")
        confirm_column = self.params.get_param("confirm_column")
        primary_threshold = float(self.params.get_param("primary_threshold"))
        confirm_threshold = float(self.params.get_param("confirm_threshold"))
        primary_op = str(self.params.get_param("primary_op", "gt")).lower()
        confirm_op = str(self.params.get_param("confirm_op", "gt")).lower()

        if primary_column not in self.data.columns:
            raise ValueError(f"DUAL primary column not found: {primary_column}")
        if confirm_column not in self.data.columns:
            raise ValueError(f"DUAL confirm column not found: {confirm_column}")

        primary_values = self.data[primary_column].astype(float).to_numpy()
        confirm_values = self.data[confirm_column].astype(float).to_numpy()
        combined = self._compare(primary_values, primary_threshold, primary_op) & self._compare(
            confirm_values, confirm_threshold, confirm_op
        )
        previous = np.roll(combined, 1)
        previous[0] = False

        signals = np.zeros(len(combined), dtype=np.float64)
        signals[combined & ~previous] = 1.0
        return signals
