"""Thin orchestrator for canonical WFA and rolling validation workflows."""

from __future__ import annotations

from copy import deepcopy
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from autorunner.DataLoader_autorunner import DataLoaderAutorunner
from backtester.EngineRequest_backtester import build_engine_request
from utils import show_error
from validation_workflow.UnifiedPortfolioWFAExporter_validation_workflow import (
    UnifiedPortfolioWFAExporter,
)
from validation_workflow.UnifiedPortfolioWFARunner_validation_workflow import (
    UnifiedPortfolioWFARunner,
)


class WalkForwardEngine:
    """Route one canonical validation workflow through the shared portfolio runner."""

    def __init__(
        self,
        config_data: Any,
        logger: Optional[logging.Logger] = None,
        *,
        market_data_bundle_root: Optional[Path] = None,
    ):
        self.config_data = config_data
        self.logger = logger or logging.getLogger("lo2cin4bt.validation_workflow.engine")
        self.market_data_bundle_root = market_data_bundle_root
        self.wfa_config = deepcopy(getattr(config_data, "wfa_config", {}) or {})
        self.results: List[Dict[str, Any]] = []

    def run(self) -> Optional[Dict[str, Any]]:
        try:
            payload = self._run_canonical_workflow()
            self.results = [payload]
            return payload
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            show_error("VALIDATION_WORKFLOW", f"Validation workflow failed: {exc}")
            self.logger.exception("Validation workflow failed")
            return None

    def _run_canonical_workflow(self) -> Dict[str, Any]:
        backtester_config = getattr(self.config_data, "backtester_config", {}) or {}
        if not isinstance(backtester_config, dict):
            raise TypeError("backtester_config must be a mapping")

        strategy_run_config = backtester_config.get("strategy_run_config")
        if not isinstance(strategy_run_config, dict) or not strategy_run_config:
            raise ValueError("Validation workflow requires backtester.strategy_run_config")

        workflow_config_file_path = str(
            backtester_config.get("__config_file_path")
            or self.wfa_config.get("__config_file_path")
            or getattr(self.config_data, "file_path", "")
        )
        strategy_config_file_path = str(
            backtester_config.get("strategy_config_file_path")
            or workflow_config_file_path
        )
        data_request = build_engine_request(
            strategy_run_config,
            request_id="validation_workflow:market_data",
            run_scope="single",
        )
        bundle_root = self.market_data_bundle_root or (
            self._output_dir(workflow_config_file_path) / "market_data_bundle"
        )
        market_data_bundle = DataLoaderAutorunner(logger=self.logger).load_market_data_bundle(
            data_request,
            output_root=bundle_root,
            config_file_path=strategy_config_file_path or None,
        )
        workflow_config = self._canonical_workflow_config()
        result = UnifiedPortfolioWFARunner(
            market_data_bundle=market_data_bundle,
            strategy_config=strategy_run_config,
            wfa_config=workflow_config,
        ).run()

        output_dir = self._output_dir(workflow_config_file_path)
        metadata = (
            strategy_run_config.get("metadata", {})
            if isinstance(strategy_run_config.get("metadata"), dict)
            else {}
        )
        default_run_id = Path(workflow_config_file_path).stem if workflow_config_file_path else str(
            metadata.get("strategy_id") or "validation_workflow"
        )
        run_id = str(workflow_config.get("run_id") or default_run_id)
        outputs = workflow_config.get("outputs", {})
        if not isinstance(outputs, dict):
            outputs = {}
        exported_files = UnifiedPortfolioWFAExporter(
            result=result,
            output_dir=output_dir,
            run_id=run_id,
            export_diagnostics=bool(outputs.get("candidate_diagnostics", True)),
        ).export()

        return {
            "wfa_config": workflow_config,
            "total_windows": int(result.metadata.get("window_count", 0)),
            "selected_optimum": result.selected_optimum,
            "candidate_diagnostics": result.candidate_diagnostics,
            "window_backtests": result.window_backtests,
            "metadata": result.metadata,
            "market_data_bundle": {
                "bundle_id": market_data_bundle.bundle_id,
                "content_hash": market_data_bundle.content_hash,
                "manifest_path": str(market_data_bundle.manifest_path),
            },
            "exported_files": exported_files,
            "contract_audit": {
                "runtime": "unified_portfolio_wfa",
                "row_contract": "selected_optimum_per_window",
                "workflow_id": workflow_config["workflow_id"],
            },
        }

    def _canonical_workflow_config(self) -> Dict[str, Any]:
        workflow_id = str(self.wfa_config.get("workflow_id") or "").strip()
        if workflow_id not in {"walk_forward_analysis", "rolling_validation"}:
            raise ValueError(
                "wfa_config.workflow_id must be walk_forward_analysis or rolling_validation"
            )
        return {
            "workflow_id": workflow_id,
            "engine": self.wfa_config.get("engine"),
            "windowing": deepcopy(self._dict(self.wfa_config.get("windowing"))),
            "optimizer": deepcopy(self._dict(self.wfa_config.get("optimizer"))),
            "acceptance": deepcopy(self._dict(self.wfa_config.get("acceptance"))),
            "outputs": deepcopy(self._dict(self.wfa_config.get("outputs"))),
        }

    def _output_dir(self, config_file_path: str) -> Path:
        outputs = self._dict(self.wfa_config.get("outputs"))
        configured = outputs.get("output_dir")
        if isinstance(configured, str) and configured.strip():
            raw = Path(configured)
            if raw.is_absolute():
                return raw
            if config_file_path:
                return (Path(config_file_path).parent / raw).resolve()
            return raw.resolve()
        return Path("outputs") / "validation_workflow"

    @staticmethod
    def _dict(value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}
