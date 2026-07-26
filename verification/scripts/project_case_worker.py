from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import numpy as np


def _prepare_sys_path(project_root: Path) -> None:
    project_root = project_root.resolve()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))


def _replace_tokens(value: Any, replacements: Dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: _replace_tokens(inner, replacements) for key, inner in value.items()}
    if isinstance(value, list):
        return [_replace_tokens(item, replacements) for item in value]
    if isinstance(value, str):
        result = value
        for token, replacement in replacements.items():
            result = result.replace(token, replacement)
        return result
    return value


def _safe_case_filename(value: Any, *, default: str) -> str:
    text = str(value or default).strip()
    if not text:
        text = default
    path = Path(text)
    if (
        path.name != text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
        or ":" in text
    ):
        raise ValueError(f"Unsafe case filename: {text}")
    return text


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


def _dataframe_payload(df: pd.DataFrame) -> Dict[str, Any]:
    preview = df.head(5).copy()
    if "Time" in preview.columns:
        preview["Time"] = preview["Time"].astype(str)
    numeric_preview: Dict[str, list] = {}
    for column in preview.columns:
        series = preview[column]
        if pd.api.types.is_numeric_dtype(series):
            numeric_preview[column] = [None if pd.isna(v) else float(v) for v in series.tolist()]
        else:
            numeric_preview[column] = [None if pd.isna(v) else str(v) for v in series.tolist()]

    payload = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "head": numeric_preview,
    }
    if "Time" in df.columns:
        payload["time_min"] = str(pd.to_datetime(df["Time"]).min())
        payload["time_max"] = str(pd.to_datetime(df["Time"]).max())
    if "X" in df.columns:
        payload["predictor_non_null"] = int(df["X"].notna().sum())
    if "close_return" in df.columns:
        payload["close_return_preview"] = [
            None if pd.isna(v) else float(v) for v in df["close_return"].head(5).tolist()
        ]
    return payload


def run_dataloader_case(project_root: Path, case_config: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    _prepare_sys_path(project_root)
    from autorunner.DataLoader_autorunner import DataLoaderAutorunner

    loader = DataLoaderAutorunner()
    data = loader.load_data(case_config["dataloader"])
    if data is None:
        raise RuntimeError("DataLoader returned None")

    payload = {
        "module": "dataloader",
        "dataframe": _dataframe_payload(data),
        "loading_summary": loader.get_loading_summary(),
        "current_predictor_column": loader.current_predictor_column,
        "using_price_predictor_only": loader.using_price_predictor_only,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "dataloader_snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    data.to_parquet(output_root / "dataloader_output.parquet", index=False)
    (output_root / "worker_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def run_statanalyser_case(project_root: Path, case_config: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    _prepare_sys_path(project_root)
    import utils as utils_module

    stat_dataset = Path(case_config["statanalyser"]["fixture_dataset"])
    data = pd.read_csv(stat_dataset)

    target = case_config["statanalyser"]["target"]
    predictor_col = target["predictor_column"]
    return_col = target["return_column"]
    tests_config = case_config["statanalyser"].get("tests", {})
    enabled_tests = [name for name, cfg in tests_config.items() if isinstance(cfg, dict) and cfg.get("enabled")]
    if len(enabled_tests) != 1:
        raise ValueError(f"Expected exactly one enabled statanalyser test, got {enabled_tests}")

    test_name = enabled_tests[0]
    analysis_config = tests_config.get(test_name, {})

    def _silent(*args: Any, **kwargs: Any) -> None:
        return None

    class _SilentConsole:
        def print(self, *args: Any, **kwargs: Any) -> None:
            return None

        def input(self, *args: Any, **kwargs: Any) -> str:
            return "n"

    utils_module.get_console = lambda: _SilentConsole()
    utils_module.show_step_panel = _silent
    utils_module.show_info = _silent
    utils_module.show_warning = _silent
    utils_module.show_error = _silent

    normalized_rows: list[dict[str, Any]]
    summary: dict[str, Any]

    if test_name == "correlation":
        import statanalyser.CorrelationTest_statanalyser as test_module

        test_module.show_step_panel = _silent
        test_module.show_info = _silent
        test_module.show_warning = _silent
        test_module.show_error = _silent
        analyzer = test_module.CorrelationTest(data, predictor_col, return_col)
        results = analyzer.analyze()
        normalized_rows = []
        for lag, values in sorted(results.get("correlation_results", {}).items(), key=lambda item: int(item[0])):
            row: dict[str, Any] = {"lag": int(lag)}
            for key, value in values.items():
                row[key] = float(value)
            normalized_rows.append(row)
        summary = {
            "best_lag": results.get("best_lag"),
            "best_spearman": results.get("best_spearman"),
            "best_chatterjee_lag": results.get("best_chatterjee_lag"),
            "best_chatterjee": results.get("best_chatterjee"),
            "skipped_lags": results.get("skipped_lags", []),
        }
    elif test_name == "stationarity":
        import statanalyser.StationarityTest_statanalyser as test_module

        test_module.get_console = lambda: _SilentConsole()
        test_module.show_step_panel = _silent
        test_module.show_info = _silent
        analyzer = test_module.StationarityTest(data, predictor_col, return_col)
        results = analyzer.analyze()
        normalized_rows = []
        for series_name in ["predictor", "return"]:
            result_row: dict[str, Any] = {"series": series_name}
            result_row.update(results.get(series_name, {}))
            normalized_rows.append(result_row)
        summary = {
            "predictor_adf_stationary": results.get("predictor", {}).get("adf_stationary"),
            "predictor_kpss_stationary": results.get("predictor", {}).get("kpss_stationary"),
            "return_adf_stationary": results.get("return", {}).get("adf_stationary"),
            "return_kpss_stationary": results.get("return", {}).get("kpss_stationary"),
        }
    elif test_name == "autocorrelation":
        import statanalyser.AutocorrelationTest_statanalyser as test_module

        try:
            analyzer = test_module.AutocorrelationTest(
                data,
                predictor_col,
                return_col,
                analysis_config=analysis_config,
            )
        except TypeError:
            analyzer = test_module.AutocorrelationTest(
                data,
                predictor_col,
                return_col,
            )
        results = analyzer.analyze()
        normalized_rows = [
            {"series": "acf", "lag": int(lag)} for lag in results.get("acf_lags", [])
        ] + [
            {"series": "pacf", "lag": int(lag)} for lag in results.get("pacf_lags", [])
        ]
        summary = {
            "success": results.get("success"),
            "has_autocorr": results.get("has_autocorr"),
            "plots_generated": results.get("plots_generated", False),
            "acf_lags": results.get("acf_lags", []),
            "pacf_lags": results.get("pacf_lags", []),
        }
    elif test_name == "distribution":
        import statanalyser.DistributionTest_statanalyser as test_module

        analyzer = test_module.DistributionTest(data, predictor_col, return_col)
        results = analyzer.analyze()
        normalized_rows = [
            {
                "ks_stat": results.get("ks_stat"),
                "ks_p": results.get("ks_p"),
                "ad_stat": results.get("ad_stat"),
                "ad_critical": results.get("ad_critical"),
                "skewness": results.get("skewness"),
                "kurtosis": results.get("kurtosis"),
            }
        ]
        summary = normalized_rows[0].copy()
    elif test_name == "seasonality":
        import statanalyser.SeasonalAnalysis_statanalyser as test_module

        test_module.show_error = _silent
        analyzer = test_module.SeasonalAnalysis(data, predictor_col, return_col)
        results = analyzer.analyze()
        normalized_rows = [
            {
                "success": results.get("success"),
                "has_seasonal": results.get("has_seasonal"),
                "period": results.get("period"),
                "strength": results.get("strength"),
            }
        ]
        summary = normalized_rows[0].copy()
    else:
        raise NotImplementedError(f"Unsupported statanalyser test in worker: {test_name}")

    normalized_rows = _json_safe(normalized_rows)
    summary = _json_safe(summary)
    output_df = pd.DataFrame(normalized_rows)
    if output_df.empty:
        output_df = pd.DataFrame(columns=["empty"])

    payload = {
        "module": "statanalyser",
        "test": test_name,
        "fixture_dataset": str(stat_dataset),
        "predictor_column": predictor_col,
        "return_column": return_col,
        "summary": summary,
        "row_count": int(len(normalized_rows)),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "statanalyser_snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    output_df.to_parquet(output_root / "statanalyser_output.parquet", index=False)
    (output_root / "worker_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def run_backtester_case(project_root: Path, case_config: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    _prepare_sys_path(project_root)
    from autorunner.DataLoader_autorunner import DataLoaderAutorunner
    from autorunner.BacktestRunner_autorunner import BacktestRunnerAutorunner

    loader = DataLoaderAutorunner()
    data = loader.load_data(case_config["dataloader"])
    if data is None:
        raise RuntimeError("DataLoader returned None for backtester case")

    runner = BacktestRunnerAutorunner()
    results = runner.run_backtest(data, case_config)
    if not results or not results.get("success"):
        raise RuntimeError("BacktestRunner returned unsuccessful result")

    export_root = Path(case_config["backtester"]["export_config"]["output_dir"])
    parquet_files = sorted(export_root.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet exported under {export_root}")
    parquet_path = parquet_files[-1]
    df = pd.read_parquet(parquet_path)

    payload = {
        "module": "backtester",
        "parquet_path": str(parquet_path),
        "dataframe": _dataframe_payload(df),
        "success": bool(results.get("success")),
        "requested_engine_mode": results.get("requested_engine_mode"),
        "resolved_engine_mode": results.get("resolved_engine_mode", "unknown"),
        "symbol": results.get("symbol"),
        "predictor_column": results.get("predictor_column"),
        "frequency": results.get("frequency"),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "backtester_snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    df.to_parquet(output_root / "backtester_output.parquet", index=False)
    (output_root / "worker_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def run_metrics_case(project_root: Path, case_config: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    _prepare_sys_path(project_root)
    from metricstracker.MetricsArtifactWriter_metricstracker import export_metrics_artifacts

    backtester_payload = run_backtester_case(project_root, case_config, output_root)
    backtester_parquet = output_root / "backtester_output.parquet"
    if not backtester_parquet.exists():
        raise FileNotFoundError(f"Missing backtester parquet: {backtester_parquet}")

    metric_config = case_config["metricstracker"]
    time_unit = int(metric_config.get("time_unit", 252))
    risk_free_rate = float(metric_config.get("risk_free_rate", 0.0))
    if risk_free_rate > 1:
        risk_free_rate = risk_free_rate / 100.0

    export_metrics_artifacts(
        str(backtester_parquet),
        time_unit=time_unit,
        risk_free_rate=risk_free_rate,
    )

    metrics_dir = output_root.parent / "metricstracker"
    metrics_parquet_candidates = sorted(metrics_dir.glob("*_metrics.parquet"))
    metadata_candidates = sorted(metrics_dir.glob("*_metadata.json"))
    if not metrics_parquet_candidates or not metadata_candidates:
        raise FileNotFoundError(f"Metrics artifacts missing under {metrics_dir}")

    metrics_parquet = metrics_parquet_candidates[-1]
    metadata_json = metadata_candidates[-1]
    metrics_df = pd.read_parquet(metrics_parquet)
    metadata_payload = json.loads(metadata_json.read_text(encoding="utf-8"))

    payload = {
        "module": "metrics",
        "backtester": backtester_payload,
        "metrics_parquet_path": str(metrics_parquet),
        "metadata_json_path": str(metadata_json),
        "metrics_dataframe": _dataframe_payload(metrics_df),
        "metadata_rows": len(metadata_payload) if isinstance(metadata_payload, list) else 1,
    }
    (output_root / "metrics_snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_root / "worker_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def run_wfa_case(project_root: Path, case_config: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    _prepare_sys_path(project_root)
    import utils as utils_module
    import validation_workflow.WalkForwardEngine_validation_workflow as engine_module
    import validation_workflow.ResultsExporter_validation_workflow as exporter_module

    from validation_workflow.ConfigLoader_validation_workflow import ConfigLoader

    def _silent(*args: Any, **kwargs: Any) -> None:
        return None

    class _SilentConsole:
        def print(self, *args: Any, **kwargs: Any) -> None:
            return None

        def input(self, *args: Any, **kwargs: Any) -> str:
            return "n"

    utils_module.show_step_panel = _silent
    utils_module.show_warning = _silent
    utils_module.show_info = _silent
    utils_module.show_error = _silent
    utils_module.show_success = _silent
    utils_module.get_console = lambda: _SilentConsole()
    engine_module.show_step_panel = _silent
    engine_module.show_warning = _silent
    engine_module.show_info = _silent
    engine_module.show_error = _silent
    engine_module.show_success = _silent
    engine_module.console = _SilentConsole()
    exporter_module.show_info = _silent
    exporter_module.show_error = _silent
    exporter_module.show_success = _silent
    exporter_module.console = _SilentConsole()

    wfa_case = case_config["wfa"]
    config_name = _safe_case_filename(
        wfa_case.get("config_name", "inline_wfa_case.json"),
        default="inline_wfa_case.json",
    )
    requested_mode = wfa_case.get("mode")
    inline_config = wfa_case.get("inline_config")
    if isinstance(inline_config, dict):
        config_path = output_root / config_name
        output_root.mkdir(parents=True, exist_ok=True)
        config_path.write_text(json.dumps(inline_config, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        candidates = [
            project_root / "records" / "autorunner" / "validation_workflow_autorunner" / config_name,
            project_root / "autorunner" / "templates" / config_name,
        ]
        config_path = next((path for path in candidates if path.exists()), candidates[0])
    if not config_path.exists():
        raise FileNotFoundError(f"WFA config missing: {config_path}")

    loader = ConfigLoader()
    config_data = loader.load_config(str(config_path))
    if config_data is None:
        raise RuntimeError(f"Unable to load WFA config: {config_path}")

    if requested_mode:
        config_data.wfa_config["mode"] = requested_mode

    engine = engine_module.WalkForwardEngine(config_data)
    results = engine.run()
    if not results:
        raise RuntimeError("WalkForwardEngine returned no results")

    export_dir = output_root / "records" / "validation_workflow"
    exporter = exporter_module.ResultsExporter(
        results,
        output_dir=export_dir,
        config_data=config_data,
        data=results.get("data") if isinstance(results, dict) else None,
    )
    exporter.export()

    parquet_files = sorted(export_dir.glob("*.parquet"))
    csv_files = sorted(export_dir.glob("*.csv"))
    if not parquet_files:
        raise FileNotFoundError(f"No WFA parquet exported under {export_dir}")

    window_boundaries = []
    for window in engine.windows:
        window_boundaries.append(
            {
                "window_id": int(window["window_id"]),
                "train_start": int(window["train_start"]),
                "train_end": int(window["train_end"]),
                "test_start": int(window["test_start"]),
                "test_end": int(window["test_end"]),
                "train_rows": int(len(window["train_data"])),
                "test_rows": int(len(window["test_data"])),
            }
        )

    objective_exports = []
    for parquet_path in parquet_files:
        objective = "unknown"
        lower_name = parquet_path.name.lower()
        if "_wfa_sharpe_" in lower_name:
            objective = "sharpe"
        elif "_wfa_calmar_" in lower_name:
            objective = "calmar"

        df = pd.read_parquet(parquet_path)
        objective_exports.append(
            {
                "objective": objective,
                "path": str(parquet_path),
                "rows": int(len(df)),
                "columns": list(df.columns),
            }
        )
        df.to_parquet(export_dir / f"{objective}_snapshot.parquet", index=False)

    objective_names = sorted(results.get("results_by_objective", {}).keys())
    objective_result_counts = {
        key: int(len(value))
        for key, value in results.get("results_by_objective", {}).items()
    }
    if not objective_names and isinstance(results.get("metadata"), dict):
        objective_names = list(results["metadata"].get("objectives", []))
    if not objective_result_counts and isinstance(results.get("selected_optimum"), pd.DataFrame):
        selected = results["selected_optimum"]
        if "objective" in selected.columns:
            objective_result_counts = {
                str(key): int(value)
                for key, value in selected["objective"].value_counts().sort_index().items()
            }

    window_count = int(len(window_boundaries))
    metadata = results.get("metadata") if isinstance(results.get("metadata"), dict) else {}
    if window_count == 0 and metadata.get("window_count") is not None:
        window_count = int(metadata["window_count"])

    payload = {
        "module": "wfa",
        "config_name": config_name,
        "requested_mode": requested_mode,
        "resolved_mode": config_data.wfa_config.get("mode"),
        "window_count": window_count,
        "window_boundaries": window_boundaries,
        "objective_names": objective_names,
        "objective_result_counts": objective_result_counts,
        "parquet_exports": objective_exports,
        "csv_export_count": int(len(csv_files)),
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "wfa_snapshot.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_root / "worker_payload.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single truth-validation case inside a project root.")
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--case-file", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--fixture-root", required=True)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    case_file = Path(args.case_file).resolve()
    output_root = Path(args.output_root).resolve()
    fixture_root = Path(args.fixture_root).resolve()

    case = json.loads(case_file.read_text(encoding="utf-8"))
    replacements = {
        "${FIXTURE_ROOT}": str(fixture_root),
        "${OUTPUT_ROOT}": str(output_root),
    }
    resolved_case = _replace_tokens(case, replacements)

    module_name = resolved_case["module"]
    if module_name == "dataloader":
        payload = run_dataloader_case(project_root, resolved_case["config"], output_root)
    elif module_name == "statanalyser":
        payload = run_statanalyser_case(project_root, resolved_case["config"], output_root)
    elif module_name == "backtester":
        payload = run_backtester_case(project_root, resolved_case["config"], output_root)
    elif module_name == "metrics":
        payload = run_metrics_case(project_root, resolved_case["config"], output_root)
    elif module_name == "wfa":
        payload = run_wfa_case(project_root, resolved_case["config"], output_root)
    else:
        raise NotImplementedError(
            f"Worker currently supports dataloader, statanalyser, backtester, metrics, and wfa only, got {module_name}"
        )
    summary = {"status": "ok", "module": module_name}
    if "dataframe" in payload:
        summary["rows"] = payload["dataframe"]["rows"]
    elif "correlation_rows" in payload:
        summary["rows"] = len(payload["correlation_rows"])
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
