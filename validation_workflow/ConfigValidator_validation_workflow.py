"""Canonical wfa_run validation for the validation workflow."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.table import Table

from backtester.StrategyRunConfig_backtester import (
    StrategyRunConfigError,
    is_wfa_run_schema_version,
    normalize_wfa_run_config,
    validate_repo_relative_json_path,
    validate_strategy_run_config,
)
from utils import show_error, show_success, show_warning


CANONICAL_WFA_CONFIG_REQUIRED = (
    "WFA configs must use schema_version=wfa_run; legacy WFA runtime shells are not supported."
)
REPO_ROOT = Path(__file__).resolve().parents[1]


class ConfigValidator:
    """Validate the sole public wfa_run contract and its strategy reference."""

    def validate_config(self, config_file: str) -> bool:
        errors = self.get_validation_errors(config_file)
        if errors:
            self._display_validation_error(errors[0], Path(config_file).name)
            return False
        return True

    def validate_configs(self, config_files: List[str]) -> List[bool]:
        return [self.validate_config(config_file) for config_file in config_files]

    def get_validation_errors(self, config_file: str) -> List[str]:
        config_path = Path(config_file)
        config, load_error = self._load_config(config_path)
        if load_error:
            return [load_error]
        if config is None:
            return ["Failed to load config"]
        if not is_wfa_run_schema_version(config.get("schema_version")):
            return [CANONICAL_WFA_CONFIG_REQUIRED]
        return self._check_wfa_run_errors(config, config_file=config_path)

    @staticmethod
    def _load_config(config_file: Path) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            with config_file.open("r", encoding="utf-8-sig") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return None, "Config file not found"
        except json.JSONDecodeError as exc:
            return None, f"Invalid JSON: {exc}"
        except OSError as exc:
            return None, f"Failed to load config: {exc}"
        if not isinstance(payload, dict):
            return None, "Config must be a JSON object"
        return payload, None

    @classmethod
    def _check_wfa_run_errors(
        cls,
        config: Dict[str, Any],
        *,
        config_file: Path,
    ) -> List[str]:
        try:
            normalized = normalize_wfa_run_config(config)
            strategy_path = str(normalized.get("strategy_run_path") or "").strip()
            resolved = cls._resolve_strategy_run_path(strategy_path, config_file)
            with resolved.open("r", encoding="utf-8-sig") as handle:
                strategy_payload = json.load(handle)
            if not isinstance(strategy_payload, dict):
                return ["strategy_run_path must point to a JSON object config"]
            validate_strategy_run_config(strategy_payload)
            return []
        except StrategyRunConfigError as exc:
            return [str(exc)]
        except FileNotFoundError as exc:
            return [f"strategy_run_path does not exist: {exc.filename}"]
        except json.JSONDecodeError as exc:
            return [f"strategy_run_path contains invalid JSON: {exc}"]
        except OSError as exc:
            return [f"wfa_run validation failed: {exc}"]

    @staticmethod
    def _resolve_strategy_run_path(strategy_path: str, config_file: Path) -> Path:
        validate_repo_relative_json_path(strategy_path, field_name="strategy_run_path")
        normalized = Path(strategy_path.replace("\\", "/"))
        if len(normalized.parts) == 1:
            return (config_file.resolve().parent / normalized).resolve()
        return (REPO_ROOT / normalized).resolve()

    def _display_validation_error(self, message: str, context: str = "") -> None:
        del context
        show_error("WFANALYSER", message)

    def display_validation_summary(self, config_files: List[str], results: List[bool]) -> None:
        success_count = sum(results)
        total_count = len(results)

        table = Table(title="WFA config validation summary")
        table.add_column("File", style="magenta")
        table.add_column("Status", style="cyan")
        table.add_column("Errors", style="red")

        for config_file, validation_result in zip(config_files, results):
            errors = [] if validation_result else self.get_validation_errors(config_file)
            error_text = "; ".join(errors[:3]) if errors else "-"
            if len(errors) > 3:
                error_text += f" ... ({len(errors)} total)"
            table.add_row(
                Path(config_file).name,
                "PASS" if validation_result else "FAIL",
                error_text,
            )

        from .utils.ConsoleUtils_utils_validation_workflow import get_console

        get_console().print(table)
        if success_count == total_count:
            show_success("WFANALYSER", f"All {total_count} config files passed validation")
        else:
            show_warning(
                "WFANALYSER",
                f"Validation complete: {success_count}/{total_count} passed",
            )
