"""Config-driven base class for statanalyser modules."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


class BaseStatAnalyser(ABC):
    """Shared validation and config normalization for statanalyser tests."""

    _PREDICTOR_EXCLUDES = {
        "Time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "open_return",
        "close_return",
        "open_logreturn",
        "close_logreturn",
    }

    _RETURN_CANDIDATES = [
        "close_return",
        "open_return",
        "close_logreturn",
        "open_logreturn",
    ]

    def __init__(
        self,
        data: pd.DataFrame,
        predictor_col: str,
        return_col: str,
        analysis_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.analysis_config = analysis_config or {}
        self.data = self._validate_data(data, predictor_col, return_col)
        self.predictor_col = predictor_col
        self.return_col = return_col
        self.results: Dict[str, Any] = {}

    @staticmethod
    def _available_predictor_factors(data: pd.DataFrame) -> list[str]:
        return [col for col in data.columns if col not in BaseStatAnalyser._PREDICTOR_EXCLUDES]

    @staticmethod
    def _default_return_column(data: pd.DataFrame) -> str:
        for column in BaseStatAnalyser._RETURN_CANDIDATES:
            if column in data.columns:
                return column
        raise ValueError("No supported return column found in dataframe")

    @staticmethod
    def _normalize_diff_mode(diff_mode: Optional[Any]) -> str:
        if diff_mode is None:
            return "none"
        normalized = str(diff_mode).strip().lower()
        if normalized in {"", "none", "false", "0", "off"}:
            return "none"
        if normalized in {"absolute", "abs", "diff"}:
            return "absolute"
        if normalized in {"relative", "rel", "ratio"}:
            return "relative"
        raise ValueError(f"Unsupported diff_mode: {diff_mode}")

    @staticmethod
    def _apply_diff_column(
        df: pd.DataFrame, predictor_col: str, diff_mode: str
    ) -> Tuple[str, pd.DataFrame]:
        if diff_mode == "none":
            return predictor_col, df

        source = pd.to_numeric(df[predictor_col], errors="coerce")
        if source.isna().any() or not np.isfinite(source.to_numpy()).all():
            raise ValueError(f"Predictor column contains invalid values: {predictor_col}")
        diff_col = f"{predictor_col}_{'abs' if diff_mode == 'absolute' else 'rel'}_diff"
        if diff_mode == "absolute":
            df[diff_col] = source.diff()
        else:
            if (source <= 0.0).any():
                raise ValueError(
                    f"Relative difference requires positive values: {predictor_col}"
                )
            df[diff_col] = source.pct_change(fill_method=None)
        invalid = ~np.isfinite(df[diff_col].iloc[1:].to_numpy())
        if invalid.any():
            raise ValueError(f"Difference calculation produced invalid values: {diff_col}")
        return diff_col, df

    @staticmethod
    def select_predictor_factor(
        data: pd.DataFrame,
        default_factor: Optional[str] = None,
        for_diff: bool = False,
        config: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Select a predictor column without interactive prompts."""

        available_factors = BaseStatAnalyser._available_predictor_factors(data)
        if not available_factors:
            raise ValueError("No predictor factors available")

        selected_factor = None
        if config:
            selected_factor = config.get("predictor_column") or config.get(
                "selected_predictor"
            )

        if selected_factor not in available_factors:
            if default_factor in available_factors:
                selected_factor = default_factor
            else:
                selected_factor = available_factors[0]

        if not for_diff:
            return selected_factor

        diff_mode = BaseStatAnalyser._normalize_diff_mode(
            None if not config else config.get("diff_mode")
        )
        if diff_mode == "none":
            return selected_factor

        diff_col, _ = BaseStatAnalyser._apply_diff_column(
            data.copy(), selected_factor, diff_mode
        )
        return diff_col

    @classmethod
    def get_user_config(
        cls, data: pd.DataFrame, config: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, pd.DataFrame]:
        """Normalize predictor/return selection from config or defaults."""

        df = data.copy()
        available_factors = cls._available_predictor_factors(df)
        if not available_factors:
            raise ValueError("No predictor factor available for statanalyser")

        config = config or {}
        target = config.get("target", config)

        predictor_col = target.get("predictor_column") or target.get(
            "selected_predictor"
        )
        if predictor_col not in available_factors:
            predictor_col = available_factors[0]

        diff_mode = cls._normalize_diff_mode(target.get("diff_mode"))
        predictor_col, df = cls._apply_diff_column(df, predictor_col, diff_mode)
        if diff_mode != "none":
            df = df.iloc[1:].copy()

        return_col = target.get("return_column") or cls._default_return_column(df)
        if return_col not in df.columns:
            raise ValueError(f"Return column not found: {return_col}")

        return predictor_col, df

    def _validate_data(
        self, data: pd.DataFrame, predictor_col: str, return_col: str
    ) -> pd.DataFrame:
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected pandas.DataFrame, got {type(data)}")

        df = data.copy()
        if not all(isinstance(col, str) for col in df.columns):
            raise TypeError("DataFrame columns must be strings")

        if "Time" in df.columns:
            try:
                df["Time"] = pd.to_datetime(df["Time"])
                df.set_index("Time", inplace=True)
                df = df.infer_objects()
            except ValueError as exc:
                raise ValueError("Time column must be convertible to datetime") from exc
        elif not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must use DatetimeIndex or include a 'Time' column")

        if predictor_col not in df.columns or return_col not in df.columns:
            raise ValueError(f"Missing predictor/return columns: {predictor_col}, {return_col}")

        if np.isnan(df[[predictor_col, return_col]].values).any():
            raise ValueError("Predictor/return columns contain NaN")
        if np.isinf(df[[predictor_col, return_col]].values).any():
            raise ValueError("Predictor/return columns contain infinite values")

        return df

    @abstractmethod
    def analyze(self) -> Dict[str, Any]:
        """Run the concrete statistical test."""

    def get_results(self) -> Dict[str, Any]:
        return self.results

    @staticmethod
    def default_output_dir() -> Path:
        return Path(__file__).resolve().parent.parent / "records" / "statanalyser"

    def get_output_dir(self) -> Path:
        configured = self.analysis_config.get("output_dir")
        if configured:
            candidate = Path(str(configured))
            if candidate.is_absolute():
                output_dir = candidate
            else:
                output_dir = Path(__file__).resolve().parent.parent / candidate
        else:
            output_dir = self.default_output_dir()
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir
