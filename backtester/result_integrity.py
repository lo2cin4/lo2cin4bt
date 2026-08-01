"""Shared integrity checks for canonical Rust result tables."""

from __future__ import annotations

import math
from typing import Dict

import pandas as pd


def canonical_equity_summary(
    equity_curve: pd.DataFrame,
    *,
    rust_total_return: object,
) -> Dict[str, float]:
    """Return observed endpoints plus the authoritative Rust total return."""
    if not isinstance(equity_curve, pd.DataFrame) or equity_curve.empty:
        raise ValueError("Canonical equity curve is empty")
    if "Equity_value" not in equity_curve.columns:
        raise ValueError("Canonical equity curve requires Equity_value")

    values = pd.to_numeric(equity_curve["Equity_value"], errors="coerce")
    if values.isna().any() or not all(math.isfinite(float(value)) for value in values):
        raise ValueError("Canonical Equity_value values must be finite")
    if (values <= 0.0).any():
        raise ValueError("Canonical Equity_value values must be positive")

    start_equity = float(values.iloc[0])
    end_equity = float(values.iloc[-1])
    try:
        total_return = float(rust_total_return)
    except (TypeError, ValueError) as exc:
        raise ValueError("Canonical equity summary requires Rust total_return") from exc
    if not math.isfinite(total_return):
        raise ValueError("Canonical Rust total_return must be finite")
    return {
        "start_equity": start_equity,
        "end_equity": end_equity,
        "total_return": total_return,
    }
