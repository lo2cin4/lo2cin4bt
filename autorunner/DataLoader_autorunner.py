"""Autorunner data loading through the current market-data boundary."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from autorunner.utils import get_console
from backtester.timeframe_contracts import validate_bar_time_contract
from dataloader.market_data_bundle import MarketDataBundle
from dataloader.market_data_loader import (
    MultiAssetMarketDataLoader,
    market_data_spec_from_requirements,
)
from utils import show_error, show_info

console = get_console()

class DataLoaderAutorunner:
    """Current autorunner data boundary.

    Legacy single-asset dataloader config shapes are no longer supported. Current
    strategy_run configs delegate market data loading to the unified runner.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("DataLoaderAutorunner")
        self.data: Optional[pd.DataFrame] = None
        self.execution_stream_id: Optional[str] = None
        self.bar_spec: Optional[Dict[str, Any]] = None
        self.source: Optional[str] = None
        self.loading_summary: Dict[str, Any] = {}
        self.project_root = Path(__file__).resolve().parent.parent

    def load_market_data_bundle(
        self,
        engine_request: Dict[str, Any],
        *,
        output_root: Path,
        config_file_path: Optional[str] = None,
    ) -> MarketDataBundle:
        """Own the complete data stage and return its immutable runtime artifact."""

        from backtester.EngineRequest_backtester import validate_engine_request

        validate_engine_request(engine_request)
        requirements = engine_request.get("data_requirements")
        if not isinstance(requirements, dict):
            raise ValueError("EngineRequest data_requirements must be an object")
        strategy = engine_request.get("strategy")
        if not isinstance(strategy, dict):
            raise ValueError("EngineRequest strategy must be an object")
        stream_binding = strategy.get("stream_binding")
        if not isinstance(stream_binding, dict):
            raise ValueError("EngineRequest strategy.stream_binding must be an object")
        spec = market_data_spec_from_requirements(requirements, stream_binding)
        bundle = MultiAssetMarketDataLoader(repo_root=self.project_root).load_bundle(
            spec,
            output_root=output_root,
            config_file_path=config_file_path,
        )
        bundle.validate_against_engine_request(engine_request)
        manifest = bundle.read_manifest()
        execution_stream = manifest["execution_stream"]
        self.execution_stream_id = str(execution_stream["stream_id"])
        self.bar_spec = dict(execution_stream["bar_spec"])
        self.source = str(manifest["lineage"]["provider"])
        self.loading_summary = {
            "schema_version": manifest["schema_version"],
            "bundle_id": manifest["bundle_id"],
            "content_hash": manifest["content_hash"],
            "symbols": list(manifest["symbols"]),
            "row_count": int(manifest["row_count"]),
            "time_range": dict(manifest["time_range"]),
            "execution_stream_id": self.execution_stream_id,
            "bar_spec": dict(self.bar_spec),
            "manifest_path": str(bundle.manifest_path),
        }
        return bundle

    def load_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Prepare the unified runner boundary; market frames come from MarketDataBundle."""
        source = str(config.get("source", "yfinance")).strip().lower()
        if source not in {"strategy_run_market_data", "multi_asset"}:
            raise ValueError(
                "Legacy single-asset dataloader configs are no longer supported. "
                "Use a strategy_run config with data/universe sections."
            )
        legacy_fields = sorted(
            {"frequency", "interval", "calendar", "timezone"} & set(config)
        )
        if legacy_fields:
            raise ValueError(
                "DataLoaderAutorunner rejects legacy time fields: "
                + ", ".join(legacy_fields)
            )
        execution_stream = self._execution_stream_contract(config)
        self.data = pd.DataFrame()
        self.execution_stream_id = str(execution_stream["stream_id"])
        self.bar_spec = dict(execution_stream["bar_spec"])
        self.source = source
        self._update_loading_summary(config)
        return self.data

    @staticmethod
    def _execution_stream_contract(config: Dict[str, Any]) -> Dict[str, Any]:
        bar_time = config.get("bar_time")
        if not isinstance(bar_time, dict):
            raise ValueError("DataLoaderAutorunner requires typed bar_time")
        validate_bar_time_contract(bar_time)
        binding = config.get("stream_binding")
        if not isinstance(binding, dict):
            raise ValueError("DataLoaderAutorunner requires stream_binding")
        execution_stream_id = str(binding.get("execution_stream_id") or "")
        for stream in bar_time["streams"]:
            if (
                stream.get("stream_id") == execution_stream_id
                and stream.get("role") == "execution"
            ):
                return dict(stream)
        raise ValueError(
            "stream_binding.execution_stream_id must reference the execution stream"
        )

    def _update_loading_summary(self, config: Dict[str, Any]) -> None:
        data = self.data
        self.loading_summary = {
            "source": self.source,
            "execution_stream_id": self.execution_stream_id,
            "bar_spec": dict(self.bar_spec or {}),
            "data_shape": data.shape if data is not None else (0, 0),
            "columns": list(data.columns) if data is not None else [],
            "date_range": self._get_date_range(),
            "config_used": {
                "source": config.get("source"),
                "start_date": config.get("start_date"),
                "end_date": config.get("end_date"),
            },
        }

    def _get_date_range(self) -> Tuple[str, str]:
        if self.data is None or "Time" not in self.data.columns or self.data.empty:
            return "N/A", "N/A"
        start = pd.Timestamp(self.data["Time"].min())
        end = pd.Timestamp(self.data["Time"].max())
        if pd.isna(start) or pd.isna(end):
            raise ValueError("Dataloader Time column must contain a valid date range")
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def get_loading_summary(self) -> Dict[str, Any]:
        return self.loading_summary.copy()

    def display_loading_summary(self) -> None:
        if not self.loading_summary:
            show_error("AUTORUNNER", "No data loading summary is available.")
            return
        shape = self.loading_summary.get("data_shape", (0, 0))
        show_info(
            "DATALOADER",
            "Data loading summary:\n"
            f"   source: {self.loading_summary.get('source', 'N/A')}\n"
            f"   execution stream: {self.loading_summary.get('execution_stream_id', 'N/A')}\n"
            f"   bar spec: {self.loading_summary.get('bar_spec', {})}\n"
            f"   rows x columns: {shape}\n"
            f"   date range: {self.loading_summary.get('date_range', ('N/A', 'N/A'))}\n"
            f"   predictor: {self.loading_summary.get('predictor_column', 'N/A')}",
        )
