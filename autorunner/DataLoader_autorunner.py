"""Autorunner data loading through the current market-data boundary."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from autorunner.utils import get_console
from backtester.timeframe_utils import is_subdaily_timeframe
from dataloader.market_data_bundle import MarketDataBundle, build_market_data_bundle
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
        self.frequency: Optional[str] = None
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
        spec = market_data_spec_from_requirements(requirements)
        bundle = MultiAssetMarketDataLoader(repo_root=self.project_root).load_bundle(
            spec,
            output_root=output_root,
            config_file_path=config_file_path,
        )
        factor_pipeline = (
            engine_request.get("strategy", {})
            .get("decision_plan", {})
            .get("factor_pipeline", {})
        )
        if factor_pipeline:
            bundle = self._materialize_factor_bundle(
                bundle,
                factor_pipeline=factor_pipeline,
                spec=spec,
                output_root=output_root,
            )
        bundle.validate_against_engine_request(engine_request)
        manifest = bundle.read_manifest()
        self.frequency = str(manifest["frequency"])
        self.source = str(manifest["lineage"]["provider"])
        self.loading_summary = {
            "schema_version": manifest["schema_version"],
            "bundle_id": manifest["bundle_id"],
            "content_hash": manifest["content_hash"],
            "symbols": list(manifest["symbols"]),
            "row_count": int(manifest["row_count"]),
            "time_range": dict(manifest["time_range"]),
            "manifest_path": str(bundle.manifest_path),
        }
        return bundle

    @staticmethod
    def _materialize_factor_bundle(
        bundle: MarketDataBundle,
        *,
        factor_pipeline: Dict[str, Any],
        spec: Dict[str, Any],
        output_root: Path,
    ) -> MarketDataBundle:
        from factorhandler import FactorHandler

        frames = bundle.load_frames()
        result = FactorHandler(frames, factor_pipeline).run()
        produced: Dict[str, pd.DataFrame] = {}
        for collection in (
            result.factor_frame,
            result.clean_factor_frame,
            result.factor_score_frame,
        ):
            for raw_name, frame in collection.items():
                name = str(raw_name).strip().lower()
                if name in frames:
                    raise ValueError(f"Factor pipeline produced duplicate market field: {name}")
                produced[name] = frame
        if not produced:
            raise ValueError("Factor pipeline did not produce any market fields")
        return build_market_data_bundle(
            {**frames, **produced},
            spec={**spec, "point_in_time": True},
            output_root=output_root,
        )

    def load_data(self, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Prepare the unified runner boundary; market frames come from MarketDataBundle."""
        source = str(config.get("source", "yfinance")).strip().lower()
        if source not in {"strategy_run_market_data", "multi_asset"}:
            raise ValueError(
                "Legacy single-asset dataloader configs are no longer supported. "
                "Use a strategy_run config with data/universe sections."
            )
        for field_name in ("frequency", "interval"):
            value = config.get(field_name)
            if is_subdaily_timeframe(value):
                raise ValueError(
                    "multi_asset/session-level portfolio runtime only supports daily-or-slower bars; "
                    f"{field_name}={value!r} is not supported"
                )
        self.data = pd.DataFrame()
        self.frequency = str(config.get("frequency") or config.get("interval") or "1D")
        self.source = source
        self._update_loading_summary(config)
        return self.data

    def _update_loading_summary(self, config: Dict[str, Any]) -> None:
        data = self.data
        self.loading_summary = {
            "source": self.source,
            "frequency": self.frequency,
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
            f"   frequency: {self.loading_summary.get('frequency', 'N/A')}\n"
            f"   rows x columns: {shape}\n"
            f"   date range: {self.loading_summary.get('date_range', ('N/A', 'N/A'))}\n"
            f"   predictor: {self.loading_summary.get('predictor_column', 'N/A')}",
        )
