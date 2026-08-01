"""Config-driven statanalyser stage for autorunner."""

from __future__ import annotations

import logging
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple

import json

import numpy as np
import pandas as pd

from statanalyser.AutocorrelationTest_statanalyser import AutocorrelationTest
from statanalyser.CorrelationTest_statanalyser import CorrelationTest
from statanalyser.DistributionTest_statanalyser import DistributionTest
from statanalyser.Base_statanalyser import BaseStatAnalyser
from statanalyser.ReportGenerator_statanalyser import ReportGenerator
from statanalyser.SeasonalAnalysis_statanalyser import SeasonalAnalysis
from statanalyser.StationarityTest_statanalyser import StationarityTest
from backtester.timeframe_contracts import validate_bar_time_contract


class StatAnalyserRunnerAutorunner:
    """Run statanalyser tests without interactive prompts."""

    TEST_ORDER = [
        "stationarity",
        "correlation",
        "autocorrelation",
        "distribution",
        "seasonality",
    ]

    TEST_CLASS_MAP = {
        "stationarity": StationarityTest,
        "correlation": CorrelationTest,
        "autocorrelation": AutocorrelationTest,
        "distribution": DistributionTest,
        "seasonality": SeasonalAnalysis,
    }

    DEFAULT_OUTPUTS = {
        "stationarity": ["summary", "decision", "pvalue", "critical_values"],
        "correlation": ["matrix", "top_pairs", "summary"],
        "autocorrelation": ["acf", "pacf", "summary", "plots"],
        "distribution": ["summary", "histogram", "qq_plot"],
        "seasonality": ["summary", "heatmap"],
    }

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("lo2cin4bt.autorunner.statanalyser")
        self.summary: Dict[str, Any] = {}

    def run(self, data: pd.DataFrame, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        stage_config = config.get("statanalyser", {})
        if not self._as_bool(stage_config.get("enabled", False)):
            self.logger.info("Statanalyser disabled in config, skip.")
            return None

        target_config = stage_config.get("target", {})
        analysis_df, predictor_col, return_col = self._prepare_data(data, target_config)
        report_config = stage_config.get("report", {})
        tests_config = self._normalize_tests_config(stage_config.get("tests", {}), stage_config)
        report_output_dir = self._resolve_output_dir(report_config.get("output_dir"))

        results: Dict[str, Dict[str, Any]] = {}
        tasks: List[Dict[str, Any]] = []
        success_count = 0
        failure_count = 0

        for test_name in self.TEST_ORDER:
            test_config = tests_config.get(test_name, {})
            if not self._as_bool(test_config.get("enabled", False)):
                continue

            try:
                analyzer = self._build_analyzer(
                    test_name=test_name,
                    data=analysis_df.copy(),
                    predictor_col=predictor_col,
                    return_col=return_col,
                    test_config=test_config,
                    stage_config=stage_config,
                    data_config=config.get("data", {}),
                    output_dir=report_output_dir,
                )
                result = analyzer.analyze()
                result = result if isinstance(result, dict) else getattr(analyzer, "results", {})
                results[analyzer.__class__.__name__] = result
                tasks.append(
                    {
                        "test": test_name,
                        "status": "success",
                        "outputs": self._selected_outputs(test_name, test_config),
                    }
                )
                success_count += 1
            except Exception as exc:  # pragma: no cover - defensive
                failure_count += 1
                error = {"success": False, "error": str(exc)}
                results[self.TEST_CLASS_MAP[test_name].__name__] = error
                tasks.append(
                    {
                        "test": test_name,
                        "status": "failed",
                        "error": str(exc),
                        "outputs": self._selected_outputs(test_name, test_config),
                    }
                )
                self.logger.exception("Statanalyser %s failed", test_name)
                if self._as_bool(report_config.get("fail_on_error", False)):
                    raise

        self.summary = {
            "enabled": True,
            "executed": bool(tasks),
            "success": success_count,
            "failed": failure_count,
            "tasks": tasks,
            "results": results,
            "target": {
                "predictor_column": predictor_col,
                "return_column": return_col,
            },
        }

        report_paths = self._write_reports(
            results=results,
            data=analysis_df,
            report_config=report_config,
            output_dir=report_output_dir,
        )
        if report_paths:
            self.summary["report_paths"] = report_paths

        return self.summary

    def _build_analyzer(
        self,
        test_name: str,
        data: pd.DataFrame,
        predictor_col: str,
        return_col: str,
        test_config: Dict[str, Any],
        stage_config: Dict[str, Any],
        data_config: Dict[str, Any],
        output_dir: Path,
    ):
        analyzer_cls = self.TEST_CLASS_MAP[test_name]
        effective_test_config = dict(test_config)
        effective_test_config["output_dir"] = str(output_dir)
        effective_test_config["include_plots"] = self._as_bool(
            effective_test_config.get("include_plots", False)
        )
        if test_name == "autocorrelation":
            bar_spec = self._resolve_decision_bar_spec(stage_config, data_config)
            analyzer = analyzer_cls(
                data,
                predictor_col,
                return_col,
                bar_spec=bar_spec,
                analysis_config=effective_test_config,
            )
        else:
            analyzer = analyzer_cls(
                data,
                predictor_col,
                return_col,
            )
            analyzer.analysis_config = effective_test_config
        return analyzer

    def _normalize_tests_config(
        self, tests_config: Dict[str, Any], stage_config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        normalized = {
            name: {
                "enabled": False,
                "output": list(self.DEFAULT_OUTPUTS[name]),
            }
            for name in self.TEST_ORDER
        }

        if isinstance(stage_config.get("analysis_types"), list):
            for name in stage_config["analysis_types"]:
                if name in normalized:
                    normalized[name]["enabled"] = True

        for name, test_config in tests_config.items():
            if name not in normalized:
                continue
            if isinstance(test_config, bool):
                normalized[name]["enabled"] = test_config
                continue
            if not isinstance(test_config, dict):
                continue
            merged = dict(normalized[name])
            merged.update(test_config)
            if "output" not in merged or not merged["output"]:
                merged["output"] = list(self.DEFAULT_OUTPUTS[name])
            normalized[name] = merged

        return normalized

    def _prepare_data(
        self, data: pd.DataFrame, target_config: Dict[str, Any]
    ) -> Tuple[pd.DataFrame, str, str]:
        predictor_col, prepared_df = BaseStatAnalyser.get_user_config(
            data,
            {
                "target": {
                    "predictor_column": target_config.get("predictor_column"),
                    "return_column": target_config.get("return_column"),
                    "diff_mode": target_config.get("diff_mode", "none"),
                }
            },
        )
        return_col = target_config.get("return_column") or BaseStatAnalyser._default_return_column(
            prepared_df
        )
        return prepared_df, predictor_col, return_col

    def _resolve_decision_bar_spec(
        self,
        stage_config: Dict[str, Any],
        data_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        legacy_keys = sorted({"frequency", "freq"}.intersection(stage_config))
        if legacy_keys:
            raise ValueError(
                "statanalyser cadence must come from the typed decision stream; "
                f"legacy fields are not supported: {', '.join(legacy_keys)}"
            )

        bar_time = data_config.get("bar_time")
        stream_binding = data_config.get("stream_binding")
        if not isinstance(bar_time, Mapping) or not isinstance(stream_binding, Mapping):
            raise ValueError(
                "statanalyser autocorrelation requires bar_time and stream_binding"
            )
        validate_bar_time_contract(bar_time)
        decision_stream_id = str(stream_binding.get("decision_stream_id") or "")
        if not decision_stream_id:
            raise ValueError(
                "statanalyser stream_binding.decision_stream_id is required"
            )
        stream = next(
            (
                item
                for item in bar_time["streams"]
                if item["stream_id"] == decision_stream_id
            ),
            None,
        )
        if stream is None:
            raise ValueError(
                "statanalyser decision_stream_id must reference a bar_time stream"
            )
        return dict(stream["bar_spec"])

    def _selected_outputs(self, test_name: str, test_config: Dict[str, Any]) -> List[str]:
        output = test_config.get("output")
        if isinstance(output, list) and output:
            return [str(item) for item in output]
        return list(self.DEFAULT_OUTPUTS[test_name])

    def _resolve_output_dir(self, output_dir: Optional[str]) -> Path:
        if output_dir:
            return Path(output_dir).resolve()
        raise ValueError(
            "statanalyser report.output_dir must be assigned by the app runtime "
            "or an explicit standalone caller"
        )

    def _write_reports(
        self,
        results: Dict[str, Dict[str, Any]],
        data: pd.DataFrame,
        report_config: Dict[str, Any],
        output_dir: Path,
    ) -> List[str]:
        output_dir.mkdir(parents=True, exist_ok=True)
        report = ReportGenerator(output_dir=str(output_dir))
        formats = report_config.get("formats", ["md", "json"])
        if isinstance(formats, str):
            formats = [formats]

        report_paths: List[str] = []
        if "md" in formats or "txt" in formats:
            report_name = "statanalyser_report.md" if "md" in formats else "statanalyser_report.txt"
            report.save_report(results, filename=report_name)
            report_paths.append(str(output_dir / report_name))

        if self._as_bool(report_config.get("include_raw_tables", True)):
            data_name = "statanalyser_processed_data.csv"
            report.save_data(data, format="csv", filename="statanalyser_processed_data")
            report_paths.append(str(output_dir / data_name))

        if "json" in formats:
            json_path = output_dir / "statanalyser_summary.json"
            json_path.write_text(
                json.dumps(self._to_json_safe(self.summary), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            report_paths.append(str(json_path))

        return report_paths

    def _to_json_safe(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): self._to_json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._to_json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [self._to_json_safe(item) for item in value]
        if isinstance(value, (np.bool_, np.integer, np.floating)):
            return value.item()
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        return value

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y"}:
                return True
            if normalized in {"0", "false", "no", "n", ""}:
                return False
        return bool(value)
