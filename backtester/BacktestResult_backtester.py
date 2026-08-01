"""Canonical Python-side view of already-computed Rust backtest artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

import pandas as pd


@dataclass
class MultiAssetBacktestResult:
    strategy_id: str
    equity_curve: pd.DataFrame
    holdings: pd.DataFrame
    rebalance_audit: pd.DataFrame
    rebalance_trades: pd.DataFrame
    feature_cache: Dict[str, Any]
    config: Dict[str, Any]
    validation_report: Dict[str, Any]
    risk_gate_events: pd.DataFrame = field(default_factory=pd.DataFrame)
    execution_equity_curve: pd.DataFrame = field(default_factory=pd.DataFrame)
