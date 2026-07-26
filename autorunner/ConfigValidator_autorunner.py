"""Canonical strategy_run validation for autorunner entrypoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.table import Table

from backtester.StrategyRunConfig_backtester import (
    StrategyRunConfigError,
    is_strategy_run_schema_version,
    validate_strategy_run_config,
)
from utils import show_error, show_success, show_warning


CANONICAL_CONFIG_REQUIRED = (
    "Autorunner configs must use schema_version=strategy_run; "
    "legacy dataloader/backtester runtime shells are not supported."
)


class ConfigValidator:
    """Validate the public strategy_run contract before runtime compilation."""

    _KNOWN_STAT_TESTS = {
        "stationarity",
        "correlation",
        "autocorrelation",
        "distribution",
        "seasonality",
    }

    def validate_config(self, config_file: str) -> bool:
        try:
            config = self._load_config(config_file)
            if config is None:
                return False
            errors = self._check_config_errors(config)
            if errors:
                self._display_validation_error(errors[0], Path(config_file).name)
                return False
            return True
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Validation failed: {exc}"
            self._display_validation_error(message, Path(config_file).name)
            return False

    def validate_configs(self, config_files: List[str]) -> List[bool]:
        return [self.validate_config(config_file) for config_file in config_files]

    def get_validation_errors(self, config_file: str) -> List[str]:
        try:
            config = self._load_config(config_file)
            if config is None:
                return ["Failed to load config"]
            return self._check_config_errors(config)
        except Exception as exc:  # pragma: no cover - defensive
            return [f"Validation error: {exc}"]

    def _load_config(self, config_file: str) -> Optional[Dict[str, Any]]:
        try:
            with open(config_file, "r", encoding="utf-8-sig") as file_obj:
                payload = json.load(file_obj)
            if not isinstance(payload, dict):
                self._display_validation_error("Config root must be an object", Path(config_file).name)
                return None
            return payload
        except FileNotFoundError:
            self._display_validation_error("Config file not found", Path(config_file).name)
            return None
        except json.JSONDecodeError as exc:
            self._display_validation_error(f"Invalid JSON: {exc}", Path(config_file).name)
            return None
        except Exception as exc:  # pragma: no cover - defensive
            self._display_validation_error(f"Failed to load config: {exc}", Path(config_file).name)
            return None

    def _check_config_errors(self, config: Dict[str, Any]) -> List[str]:
        if not is_strategy_run_schema_version(config.get("schema_version")):
            return [CANONICAL_CONFIG_REQUIRED]

        errors = self._check_strategy_run_errors(config)
        errors.extend(self._check_metricstracker_errors(config.get("metricstracker")))
        errors.extend(self._check_statanalyser_errors(config.get("statanalyser")))

        platform = config.get("platform")
        workflow_id = str(platform.get("workflow_id") or "") if isinstance(platform, dict) else ""
        statanalyser = config.get("statanalyser")
        if workflow_id == "statanalyser" and not (
            isinstance(statanalyser, dict) and statanalyser.get("enabled") is True
        ):
            errors.append("workflow_id=statanalyser requires statanalyser.enabled=true")
        return errors

    @staticmethod
    def _check_strategy_run_errors(config: Dict[str, Any]) -> List[str]:
        try:
            validate_strategy_run_config(config)
            return []
        except StrategyRunConfigError as exc:
            return [str(exc)]
        except Exception as exc:  # pragma: no cover - defensive
            return [f"strategy_run validation failed: {exc}"]

    @staticmethod
    def _check_metricstracker_errors(raw_config: Any) -> List[str]:
        if raw_config is None:
            return []
        if not isinstance(raw_config, dict):
            return ["metricstracker must be an object"]

        errors: List[str] = []
        enabled = raw_config.get("enable_metrics_analysis")
        if enabled is not None and not isinstance(enabled, bool):
            errors.append("metricstracker.enable_metrics_analysis must be boolean")
        for field in ("risk_free_rate", "time_unit"):
            value = raw_config.get(field)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, (int, float, str))
            ):
                errors.append(f"metricstracker.{field} must be numeric or string")
        return errors

    def _check_statanalyser_errors(self, raw_config: Any) -> List[str]:
        if raw_config is None:
            return []
        if not isinstance(raw_config, dict):
            return ["statanalyser must be an object"]

        errors: List[str] = []
        enabled = raw_config.get("enabled", False)
        if not isinstance(enabled, bool):
            return ["statanalyser.enabled must be boolean"]
        if not enabled:
            return errors

        target = raw_config.get("target", {})
        if not isinstance(target, dict):
            errors.append("statanalyser.target must be an object")
        else:
            for field in ("predictor_column", "return_column", "diff_mode"):
                value = target.get(field)
                if value is not None and not isinstance(value, str):
                    errors.append(f"statanalyser.target.{field} must be a string")

        tests = raw_config.get("tests", {})
        if not isinstance(tests, dict):
            errors.append("statanalyser.tests must be an object")
        else:
            errors.extend(self._check_statanalyser_test_errors(tests))

        report = raw_config.get("report", {})
        if not isinstance(report, dict):
            errors.append("statanalyser.report must be an object")
        else:
            for field in ("include_plots", "include_raw_tables", "fail_on_error"):
                value = report.get(field)
                if value is not None and not isinstance(value, bool):
                    errors.append(f"statanalyser.report.{field} must be boolean")
            formats = report.get("formats", [])
            if formats and (
                not isinstance(formats, list)
                or any(not isinstance(item, str) for item in formats)
            ):
                errors.append("statanalyser.report.formats must be a list of strings")
        return errors

    def _check_statanalyser_test_errors(self, tests: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        enabled_any = False
        for test_name, test_config in tests.items():
            if test_name not in self._KNOWN_STAT_TESTS:
                errors.append(f"Unsupported statanalyser test: {test_name}")
                continue
            if not isinstance(test_config, dict):
                errors.append(f"statanalyser.tests.{test_name} must be an object")
                continue
            enabled = test_config.get("enabled", True)
            if not isinstance(enabled, bool):
                errors.append(f"statanalyser.tests.{test_name}.enabled must be boolean")
                continue
            if not enabled:
                continue
            enabled_any = True

            output = test_config.get("output", [])
            if output and (
                not isinstance(output, list)
                or any(not isinstance(item, str) for item in output)
            ):
                errors.append(
                    f"statanalyser.tests.{test_name}.output must be a list of strings"
                )
            if test_name == "autocorrelation":
                lags = test_config.get("lags", [])
                if lags and (
                    not isinstance(lags, list)
                    or any(isinstance(item, bool) or not isinstance(item, int) or item <= 0 for item in lags)
                ):
                    errors.append(
                        "statanalyser.tests.autocorrelation.lags must be a list of positive integers"
                    )
            if test_name == "stationarity":
                methods = test_config.get("methods", [])
                if methods and (
                    not isinstance(methods, list)
                    or any(not isinstance(item, str) for item in methods)
                ):
                    errors.append(
                        "statanalyser.tests.stationarity.methods must be a list of strings"
                    )
        if tests and not enabled_any:
            errors.append("statanalyser.enabled is true but no tests are enabled")
        return errors

    def _display_validation_error(self, message: str, context: str = "") -> None:
        del context
        show_error("AUTORUNNER", message)

    def display_validation_summary(
        self,
        config_files: List[str],
        results: List[bool],
    ) -> None:
        success_count = sum(results)
        total_count = len(results)

        table = Table(title="Config validation summary")
        table.add_column("File", style="magenta")
        table.add_column("Status", style="cyan")
        table.add_column("Errors", style="red")

        for config_file, validation_result in zip(config_files, results):
            errors = [] if validation_result else self.get_validation_errors(config_file)
            table.add_row(
                Path(config_file).name,
                "PASS" if validation_result else "FAIL",
                "\n".join(errors) if errors else "-",
            )

        from autorunner.utils import get_console

        get_console().print(table)
        if success_count == total_count:
            show_success("AUTORUNNER", "All config files passed validation")
        else:
            show_warning("AUTORUNNER", f"Validation complete: {success_count}/{total_count} passed")
