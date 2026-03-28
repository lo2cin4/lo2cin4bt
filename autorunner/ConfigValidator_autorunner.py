"""Validation helpers for autorunner configs."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.table import Table

from utils import show_error, show_success, show_warning


class ConfigValidator:
    def __init__(self) -> None:
        self.required_fields = ["dataloader", "backtester", "metricstracker"]
        self.module_required_fields = {
            "dataloader": ["source", "start_date"],
            "backtester": ["condition_pairs"],
            "metricstracker": ["enable_metrics_analysis"],
        }

    def validate_config(self, config_file: str) -> bool:
        try:
            config = self._load_config(config_file)
            if config is None:
                return False
            if not self._validate_structure(config):
                return False
            if not self._validate_content(config):
                return False
            return True
        except Exception as e:  # pragma: no cover - defensive
            show_error("AUTORUNNER", f"Validation failed: {e}")
            self._display_validation_error(f"Validation failed: {e}", Path(config_file).name)
            return False

    def validate_configs(self, config_files: List[str]) -> List[bool]:
        return [self.validate_config(config_file) for config_file in config_files]

    def get_validation_errors(self, config_file: str) -> List[str]:
        errors: List[str] = []

        try:
            config = self._load_config(config_file)
            if config is None:
                errors.append("Failed to load config")
                return errors

            errors.extend(self._check_structure_errors(config))
            errors.extend(self._check_content_errors(config))
            return errors
        except Exception as e:  # pragma: no cover - defensive
            errors.append(f"Validation error: {e}")
            return errors

    def _load_config(self, config_file: str) -> Optional[Dict[str, Any]]:
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            self._display_validation_error("Config file not found", Path(config_file).name)
            return None
        except json.JSONDecodeError as e:
            self._display_validation_error(f"Invalid JSON: {e}", Path(config_file).name)
            return None
        except Exception as e:  # pragma: no cover - defensive
            self._display_validation_error(f"Failed to load config: {e}", Path(config_file).name)
            return None

    def _validate_structure(self, config: Dict[str, Any]) -> bool:
        for field in self.required_fields:
            if field not in config:
                self._display_validation_error(f"Missing top-level field: {field}", "structure")
                return False

        for module, required_fields in self.module_required_fields.items():
            module_config = config.get(module, {})
            if not isinstance(module_config, dict):
                self._display_validation_error(f"{module} must be a dict", "structure")
                return False
            for field in required_fields:
                if field not in module_config:
                    self._display_validation_error(
                        f"Missing {module}.{field}", "structure"
                    )
                    return False
        return True

    def _validate_content(self, config: Dict[str, Any]) -> bool:
        return all(
            [
                self._validate_dataloader_config(config.get("dataloader", {})),
                self._validate_backtester_config(config.get("backtester", {})),
                self._validate_metricstracker_config(config.get("metricstracker", {})),
            ]
        )

    def _validate_dataloader_config(self, config: Dict[str, Any]) -> bool:
        try:
            source = config.get("source")
            valid_sources = ["yfinance", "binance", "coinbase", "file"]
            if source not in valid_sources:
                self._display_validation_error(
                    f"Unsupported dataloader source: {source}",
                    "dataloader",
                )
                return False

            start_date = config.get("start_date")
            if start_date and not self._validate_date_format(str(start_date)):
                return False

            if source == "file":
                file_config = config.get("file_config", {})
                file_path = file_config.get("file_path")
                if not isinstance(file_path, str) or not file_path.strip():
                    self._display_validation_error(
                        "file source requires file_config.file_path",
                        "dataloader",
                    )
                    return False

            return True
        except Exception as e:  # pragma: no cover - defensive
            self._display_validation_error(f"dataloader validation failed: {e}", "dataloader")
            return False

    def _validate_backtester_config(self, config: Dict[str, Any]) -> bool:
        try:
            condition_pairs = config.get("condition_pairs", [])
            if not isinstance(condition_pairs, list) or not condition_pairs:
                self._display_validation_error(
                    "backtester.condition_pairs must be a non-empty list",
                    "backtester",
                )
                return False

            trading_params = config.get("trading_params", {})
            if not isinstance(trading_params, dict):
                self._display_validation_error(
                    "backtester.trading_params must be a dict",
                    "backtester",
                )
                return False

            for param in ["transaction_cost", "slippage", "trade_delay"]:
                value = trading_params.get(param)
                if value is not None and (not isinstance(value, (int, float)) or value < 0):
                    self._display_validation_error(
                        f"backtester.trading_params.{param} must be non-negative numeric",
                        "backtester",
                    )
                    return False

            export_config = config.get("export_config", {})
            if not isinstance(export_config, dict):
                self._display_validation_error(
                    "backtester.export_config must be a dict",
                    "backtester",
                )
                return False

            for field in ["export_csv", "export_excel"]:
                value = export_config.get(field)
                # Backward compatible: accept bool-like strings/ints (older JSON configs
                # may store these values as "true"/"false").
                if value is None:
                    continue
                if isinstance(value, bool):
                    continue
                if isinstance(value, (int, float)) and value in (0, 1):
                    continue
                if isinstance(value, str) and value.strip().lower() in (
                    "0",
                    "1",
                    "true",
                    "false",
                    "yes",
                    "no",
                    "y",
                    "n",
                ):
                    continue
                if value is not None and not isinstance(value, bool):
                    self._display_validation_error(
                        f"backtester.export_config.{field} must be bool",
                        "backtester",
                    )
                    return False

            return True
        except Exception as e:  # pragma: no cover - defensive
            self._display_validation_error(f"backtester validation failed: {e}", "backtester")
            return False

    def _validate_metricstracker_config(self, config: Dict[str, Any]) -> bool:
        try:
            enable = config.get("enable_metrics_analysis")
            if enable is not None and not isinstance(enable, bool):
                self._display_validation_error(
                    "metricstracker.enable_metrics_analysis must be bool",
                    "metricstracker",
                )
                return False
            if not enable:
                return True

            for field in ["risk_free_rate", "time_unit"]:
                value = config.get(field)
                if value is not None and not isinstance(value, (int, float, str)):
                    self._display_validation_error(
                        f"metricstracker.{field} must be numeric or string",
                        "metricstracker",
                    )
                    return False

            return True
        except Exception as e:  # pragma: no cover - defensive
            self._display_validation_error(f"metricstracker validation failed: {e}", "metricstracker")
            return False

    def _validate_date_format(self, date_str: str) -> bool:
        if not isinstance(date_str, str):
            self._display_validation_error("start_date must be a string", "dataloader")
            return False
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            self._display_validation_error(
                f"Invalid date format: {date_str}; expected YYYY-MM-DD",
                "dataloader",
            )
            return False
        return True

    def _check_structure_errors(self, config: Dict[str, Any]) -> List[str]:
        errors: List[str] = []

        for field in self.required_fields:
            if field not in config:
                errors.append(f"Missing top-level field: {field}")

        for module, required_fields in self.module_required_fields.items():
            module_config = config.get(module, {})
            if not isinstance(module_config, dict):
                errors.append(f"{module} must be a dict")
                continue
            for field in required_fields:
                if field not in module_config:
                    errors.append(f"Missing {module}.{field}")

        return errors

    def _check_content_errors(self, config: Dict[str, Any]) -> List[str]:
        errors: List[str] = []

        dataloader = config.get("dataloader", {})
        source = dataloader.get("source")
        if source not in ["yfinance", "binance", "coinbase", "file"]:
            errors.append(f"Unsupported dataloader source: {source}")

        backtester = config.get("backtester", {})
        condition_pairs = backtester.get("condition_pairs", [])
        if not isinstance(condition_pairs, list) or not condition_pairs:
            errors.append("backtester.condition_pairs must be a non-empty list")

        export_config = backtester.get("export_config", {})
        if isinstance(export_config, dict):
            for field in ["export_csv", "export_excel"]:
                value = export_config.get(field)
                if value is not None and not isinstance(value, bool):
                    errors.append(f"backtester.export_config.{field} must be bool")

        return errors

    def _display_validation_error(self, message: str, context: str = "") -> None:
        show_error("AUTORUNNER", message)

    def display_validation_summary(
        self, config_files: List[str], results: List[bool]
    ) -> None:
        success_count = sum(results)
        total_count = len(results)

        table = Table(title="Config validation summary")
        table.add_column("File", style="magenta")
        table.add_column("Status", style="cyan")
        table.add_column("Errors", style="red")

        for config_file, validation_result in zip(config_files, results):
            file_name = Path(config_file).name
            status = "PASS" if validation_result else "FAIL"
            errors = []
            if not validation_result:
                errors = self.get_validation_errors(config_file)
            error_text = "\n".join(errors) if errors else "-"
            table.add_row(file_name, status, error_text)

        show_warning("AUTORUNNER", f"Validation complete: {success_count}/{total_count} passed")
        from autorunner.utils import get_console

        get_console().print(table)
        if success_count == total_count:
            show_success("AUTORUNNER", "All config files passed validation")
