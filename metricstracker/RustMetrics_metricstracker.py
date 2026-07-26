"""Thin transport adapter for the canonical Rust metrics kernel."""

from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd

from backtester.RustCoreBridge_backtester import run_metrics_batch_via_cli


def compute_metrics_for_frame(
    frame: pd.DataFrame,
    *,
    time_unit: int,
    risk_free_rate: float,
    backtest_id: str = "default",
) -> Dict[str, Any]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {}
    if "Equity_value" not in frame.columns:
        raise ValueError("Rust metrics frame requires Equity_value")
    equity = pd.to_numeric(frame["Equity_value"], errors="coerce").to_numpy(dtype=np.float64)
    def optional(name: str) -> list[float | None]:
        values = pd.to_numeric(
            frame.get(name, pd.Series(np.nan, index=frame.index)), errors="coerce"
        ).to_numpy(dtype=np.float64)
        return [float(value) if np.isfinite(value) else None for value in values]
    summary = run_metrics_batch_via_cli(
        {
            "time_unit": int(time_unit),
            "risk_free_rate": float(risk_free_rate),
            "backtest_ids": [str(backtest_id)],
            "equity": [float(value) for value in equity],
            "bah_equity": [float(value) for value in equity],
            "trade_actions": optional("Trade_action"),
            "trade_returns": optional("Trade_return"),
            "position_size": optional("Position_size"),
            "group_start": [0],
            "group_end": [len(equity)],
        },
        timeout=60,
    )
    rows = summary.get("metrics")
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise RuntimeError("Rust metrics batch returned an invalid metrics contract")
    return {
        key: (float("nan") if value is None else value)
        for key, value in rows[0].items()
    }
