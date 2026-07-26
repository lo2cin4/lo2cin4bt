"""Canonical WFA run config loader.

The public input is always a ``wfa_run`` config referencing one canonical
``strategy_run``. This loader resolves that reference and prepares the thin
runtime packet consumed by the unified validation workflow.
"""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtester.StrategyRunConfig_backtester import (
    StrategyRunConfigError,
    is_wfa_run_schema_version,
    normalize_strategy_run_config,
    normalize_wfa_run_config,
    plan_strategy_execution,
    validate_repo_relative_json_path,
)
from metricstracker.MetricConfig_metricstracker import resolve_metric_config
from utils import show_error


class WFAConfigData:
    """Resolved runtime packet for one canonical validation workflow."""

    def __init__(self, config_dict: Dict[str, Any], file_path: str):
        self.file_path = str(file_path)
        self.file_name = Path(file_path).name
        self.raw_config = deepcopy(config_dict)
        self.wfa_config = deepcopy(config_dict.get("wfa_config", {}))
        self.dataloader_config = deepcopy(config_dict.get("dataloader", {}))
        self.backtester_config = deepcopy(config_dict.get("backtester", {}))
        self.metricstracker_config = deepcopy(config_dict.get("metricstracker", {}))

        for section in (
            self.wfa_config,
            self.dataloader_config,
            self.backtester_config,
            self.metricstracker_config,
        ):
            section.setdefault("__config_file_path", self.file_path)

    def get_summary(self) -> Dict[str, Any]:
        windowing = (
            self.wfa_config.get("windowing", {})
            if isinstance(self.wfa_config.get("windowing"), dict)
            else {}
        )
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "workflow_id": str(self.wfa_config.get("workflow_id") or ""),
            "window_mode": str(windowing.get("mode") or "rolling"),
            "train_ratio": windowing.get("train_ratio"),
            "test_ratio": windowing.get("test_ratio"),
            "step_size": windowing.get("step_size"),
            "dataloader_source": self.dataloader_config.get("source", "unknown"),
            "strategy_mode": self.backtester_config.get("strategy_mode", "unknown"),
        }


class ConfigLoader:
    """Load only canonical ``wfa_run`` configs."""

    def load_config(self, config_file: str) -> Optional[WFAConfigData]:
        try:
            path = Path(config_file)
            with path.open("r", encoding="utf-8-sig") as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict) or not is_wfa_run_schema_version(
                payload.get("schema_version")
            ):
                raise StrategyRunConfigError(
                    "Validation workflow requires a canonical wfa_run config"
                )
            runtime_packet = self._runtime_shell_from_wfa_run(payload, str(path))
            return WFAConfigData(runtime_packet, str(path))
        except (OSError, json.JSONDecodeError, StrategyRunConfigError, ValueError) as exc:
            show_error("VALIDATION_WORKFLOW", f"Unable to load config: {exc}")
            return None

    def load_configs(self, config_files: List[str]) -> List[WFAConfigData]:
        loaded: List[WFAConfigData] = []
        for config_file in config_files:
            config_data = self.load_config(config_file)
            if config_data is not None:
                loaded.append(config_data)
        return loaded

    def _runtime_shell_from_wfa_run(
        self,
        config_dict: Dict[str, Any],
        config_file: str,
    ) -> Dict[str, Any]:
        normalized_wfa = normalize_wfa_run_config(
            config_dict,
            source_path=Path(config_file),
        )
        strategy_path = self._resolve_wfa_strategy_run_path(
            str(normalized_wfa.get("strategy_run_path") or ""),
            config_file,
        )
        strategy_run_config = self._load_wfa_strategy_run(strategy_path)
        wfa_platform = self._dict(normalized_wfa.get("platform"))

        strategy_platform = deepcopy(self._dict(strategy_run_config.get("platform")))
        strategy_platform["workflow_id"] = wfa_platform["workflow_id"]
        strategy_run_config["platform"] = strategy_platform
        strategy_run_config = normalize_strategy_run_config(strategy_run_config)

        data_config = self._dict(strategy_run_config.get("data"))
        universe = self._dict(strategy_run_config.get("universe"))
        symbols = [
            str(item).strip().upper()
            for item in list(universe.get("symbols") or [])
            if str(item).strip()
        ]
        primary_symbol = symbols[0] if symbols else str(data_config.get("symbol") or "AAPL")

        return {
            "schema_version": "wfa_run",
            "strategy_run_path": normalized_wfa["strategy_run_path"],
            "platform": deepcopy(wfa_platform),
            "wfa_config": self._wfa_config_from_strategy_run(normalized_wfa),
            "dataloader": self._dataloader_config_from_strategy_run(
                data_config,
                primary_symbol,
            ),
            "backtester": self._backtester_config_from_strategy_run(
                strategy_run_config,
                strategy_path=strategy_path,
            ),
            "metricstracker": resolve_metric_config(
                strategy_run_config.get("metricstracker", {}),
                source_config=strategy_run_config,
                default_enable=True,
            ),
        }

    def _load_wfa_strategy_run(
        self,
        strategy_path: Path,
    ) -> Dict[str, Any]:
        with strategy_path.open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise StrategyRunConfigError("Referenced strategy_run must be a JSON object")
        return normalize_strategy_run_config(payload, source_path=strategy_path)

    @staticmethod
    def _resolve_wfa_strategy_run_path(strategy_path: str, config_file: str) -> Path:
        validate_repo_relative_json_path(strategy_path, field_name="strategy_run_path")
        config_parent = Path(config_file).resolve().parent
        candidates = [config_parent / strategy_path]
        if "/" in strategy_path.replace("\\", "/"):
            for parent in [config_parent, *config_parent.parents]:
                if (parent / "workspace").exists() or (parent / "backtester").exists():
                    candidates.append(parent / strategy_path)
        candidates.append(Path.cwd() / strategy_path)
        for candidate in candidates:
            if candidate.exists():
                return candidate.resolve()
        raise FileNotFoundError(f"Referenced strategy_run does not exist: {strategy_path}")

    @staticmethod
    def _wfa_config_from_strategy_run(normalized_wfa: Dict[str, Any]) -> Dict[str, Any]:
        platform = ConfigLoader._dict(normalized_wfa.get("platform"))
        optimizer = ConfigLoader._dict(normalized_wfa.get("optimizer"))
        return {
            "workflow_id": str(platform.get("workflow_id") or ""),
            "engine": normalized_wfa.get("engine") or optimizer.get("engine"),
            "windowing": deepcopy(ConfigLoader._dict(normalized_wfa.get("windowing"))),
            "optimizer": deepcopy(optimizer),
            "acceptance": deepcopy(ConfigLoader._dict(normalized_wfa.get("acceptance"))),
            "outputs": deepcopy(ConfigLoader._dict(normalized_wfa.get("outputs"))),
        }

    @staticmethod
    def _dataloader_config_from_strategy_run(
        data_config: Dict[str, Any],
        primary_symbol: str,
    ) -> Dict[str, Any]:
        provider = str(data_config.get("provider") or "yfinance").strip().lower()
        if provider in {"local", "multi_asset"}:
            source = "multi_asset"
        elif provider in {"file", "csv", "parquet"}:
            source = "file"
        else:
            source = "yfinance"
        dataloader: Dict[str, Any] = {
            "source": source,
            "start_date": str(data_config.get("start_date") or "2020-01-01"),
            "frequency": data_config.get("frequency") or "1D",
        }
        if source == "yfinance":
            dataloader["yfinance_config"] = {
                "symbol": primary_symbol,
                "period": data_config.get("period") or "max",
                "interval": data_config.get("interval") or "1d",
            }
        if source == "file":
            dataloader["file_config"] = deepcopy(
                ConfigLoader._dict(data_config.get("file_config"))
            )
        return dataloader

    @staticmethod
    def _backtester_config_from_strategy_run(
        strategy_run_config: Dict[str, Any],
        *,
        strategy_path: Path,
    ) -> Dict[str, Any]:
        return {
            "strategy_mode": "multi_asset_portfolio",
            "strategy_run_config": deepcopy(strategy_run_config),
            "strategy_config_file_path": str(strategy_path),
            "execution_plan": plan_strategy_execution(strategy_run_config),
        }

    @staticmethod
    def _dict(value: Any) -> Dict[str, Any]:
        return value if isinstance(value, dict) else {}
