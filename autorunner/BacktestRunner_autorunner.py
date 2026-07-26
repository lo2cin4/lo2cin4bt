#!/usr/bin/env python3
"""Submit canonical EngineRequest payloads to the unified backtest runner."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from backtester.EngineRequest_backtester import validate_engine_request
from dataloader.market_data_bundle import MarketDataBundle
from utils import show_error
from utils.path_resolver import resolve_input_path


class BacktestRunnerAutorunner:
    """Thin runtime adapter; strategy configs are compiled before this boundary."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("lo2cin4bt.autorunner.backtest")
        self.project_root = Path(__file__).resolve().parent.parent

    def run_backtest(
        self,
        market_data_bundle: MarketDataBundle,
        engine_request: Dict[str, Any],
        *,
        config_file_path: Optional[str] = None,
        export_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        try:
            validate_engine_request(engine_request)
            if not isinstance(market_data_bundle, MarketDataBundle):
                raise TypeError("BacktestRunner requires a MarketDataBundle")
            market_data_bundle.validate_against_engine_request(engine_request)
            from backtester.UnifiedBacktestRunner_backtester import (
                UnifiedBacktestRunnerBacktester,
            )

            return UnifiedBacktestRunnerBacktester(
                logger=self.logger,
                path_resolver=(
                    lambda raw_path, source_path: self._resolve_optional_path(
                        raw_path,
                        config_file_path=source_path,
                    )
                ),
            ).run(
                market_data_bundle=market_data_bundle,
                engine_request=engine_request,
                config_file_path=config_file_path,
                export_config=export_config or {},
            )
        except Exception as exc:  # pragma: no cover - defensive logging boundary
            show_error("BACKTESTER", f"回測執行失敗: {exc}")
            self.logger.exception("Backtest runner failed")
            raise

    def _resolve_optional_path(
        self,
        raw_path: Any,
        *,
        config_file_path: Optional[str],
    ) -> Optional[Path]:
        if not isinstance(raw_path, str) or not raw_path.strip():
            return None
        resolved = resolve_input_path(
            raw_path,
            repo_root=self.project_root,
            config_file_path=config_file_path,
        )
        return resolved.path
