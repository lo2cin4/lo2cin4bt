"""
MetricsRunner_autorunner.py

【功能說明】
------------------------------------------------------------
本模組為 lo2cin4bt Autorunner 的績效分析封裝器，負責在自動化流程中
根據配置載入回測交易記錄（Parquet），並調用 metricstracker 的匯出邏輯，
於無需用戶互動的前提下完成績效指標匯出與摘要顯示。

【流程與數據流】
------------------------------------------------------------
- 由 AppRuntimeService / canonical autorunner path 調用，接收回測結果與 metrics 配置
- 解析配置 → 選擇目標 Parquet → 計算績效 → 匯出結果 → 顯示摘要

【維護與擴充重點】
------------------------------------------------------------
- 新增績效輸出格式或額外統計時，請同步更新本模組與配置文件
- 若 metricstracker 介面有變動，需調整匯出與摘要流程
- 顯示樣式需符合專案 CLI 美化規範
"""

from __future__ import annotations

import logging
import os
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from rich.table import Table

from autorunner.utils import get_console
from metricstracker.MetricConfig_metricstracker import resolve_metric_config
from metricstracker.MetricsArtifactWriter_metricstracker import export_metrics_artifacts
from utils import show_error, show_info, show_success
from utils.filename_utils import bounded_filename_stem

console = get_console()


@dataclass
class MetricsTaskResult:
    """指標計算任務結果數據類"""
    source_path: str
    output_path: Optional[str]
    status: str
    error: Optional[str] = None


class MetricsRunnerAutorunner:
    """自動化績效分析封裝器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("lo2cin4bt.autorunner.metrics")
        from autorunner.utils import get_console
        self.console = get_console()
        self.panel_title = "[bold #8f1511]Metrics Analysis[/bold #8f1511]"
        self.panel_error_style = "#8f1511"
        self.panel_success_style = "#dbac30"
        self.summary: Dict[str, Any] = {}
        self.required_metric_columns = [
            "Time",
            "Backtest_id",
            "Equity_value",
            "Close",
            "Trade_action",
            "Trade_return",
            "Position_size",
            "BAH_Equity",
            "BAH_Return",
            "Drawdown",
            "BAH_Drawdown",
        ]

    def run(
        self,
        backtest_results: Dict[str, Any],
        config: Dict[str, Any],
        *,
        metrics_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """執行績效分析主流程"""

        enable_metrics = config.get("enable_metrics_analysis", False)
        if not enable_metrics:
            raise ValueError(
                "metricstracker is mandatory for every canonical backtest; "
                "enable_metrics_analysis must be true"
            )

        canonical_bundle = self._load_validated_canonical_bundle(backtest_results)
        equity_path = str(
            canonical_bundle["bundle_paths"].get("execution_equity_curve")
            or canonical_bundle["bundle_paths"].get("equity_curve")
            or ""
        )
        target_files = [os.path.abspath(equity_path)] if equity_path else []

        if not target_files:
            raise ValueError(
                "validated canonical result bundle is missing execution_equity_curve "
                "and equity_curve"
            )

        resolved_metrics = resolve_metric_config(config)
        time_unit = self._resolve_time_unit(resolved_metrics)
        risk_free_rate = self._resolve_risk_free_rate(resolved_metrics)
        benchmark_path, benchmark_symbol = self._benchmark_context(metrics_context)

        task_results: List[MetricsTaskResult] = []
        success_count = 0
        failure_count = 0

        self._display_info(
            "開始績效分析",
            details=[
                f"分析檔案數：{len(target_files)}",
                f"年化時間單位：{time_unit}",
                f"無風險利率：{risk_free_rate:.4f}",
            ],
        )

        for file_path in target_files:
            result = self._process_single_file(
                file_path=file_path,
                time_unit=time_unit,
                risk_free_rate=risk_free_rate,
                benchmark_parquet_path=benchmark_path,
                benchmark_symbol=benchmark_symbol,
            )
            task_results.append(result)
            if result.status == "success":
                success_count += 1
            else:
                failure_count += 1

        self.summary = {
            "enabled": True,
            "executed": True,
            "success": success_count,
            "failed": failure_count,
            "tasks": [result.__dict__ for result in task_results],
        }

        self._display_summary(task_results)
        return self.summary

    def _load_validated_canonical_bundle(
        self, backtest_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        for raw_path in backtest_results.get("exported_files", []) or []:
            path = str(raw_path or "").strip()
            if not path.lower().endswith(".json") or not os.path.isfile(path):
                continue
            try:
                with open(path, encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict) or payload.get("schema_version") != "canonical_result_bundle.v1":
                continue
            validation = payload.get("validation")
            result_hashes = payload.get("result_hashes")
            candidates = payload.get("candidates")
            candidate_count = int(payload.get("candidate_count") or 0)
            if (
                not isinstance(validation, dict)
                or validation.get("status") != "valid"
                or not isinstance(result_hashes, list)
                or not isinstance(candidates, list)
                or candidate_count <= 0
                or len(result_hashes) != candidate_count
                or len(candidates) != candidate_count
                or len(str(payload.get("bundle_hash") or "")) != 64
            ):
                raise ValueError("canonical result bundle validation contract is invalid")
            for candidate in candidates:
                run_validation = candidate.get("run_validation") if isinstance(candidate, dict) else None
                report = (
                    run_validation.get("result_validation")
                    if isinstance(run_validation, dict)
                    else None
                )
                if (
                    not isinstance(report, dict)
                    or report.get("schema_version") != "result_validation_report.v1"
                    or report.get("status") != "valid"
                    or report.get("result_hash") not in result_hashes
                ):
                    raise ValueError("canonical result candidate is not validated")
            bundle_paths = payload.get("bundle_paths")
            if not isinstance(bundle_paths, dict):
                raise ValueError("canonical result bundle_paths is invalid")
            return payload
        raise ValueError("metrics requires a validated canonical_result_bundle.v1")

    # ------------------------------------------------------------------
    # NOTE: translated to English.
    # ------------------------------------------------------------------

    def _resolve_time_unit(self, config: Dict[str, Any]) -> int:
        value = config.get("time_unit")
        if value is None:
            raise ValueError("配置缺少 time_unit 設定")

        if isinstance(value, (int, float)):
            return int(value)

        if isinstance(value, str):
            value = value.strip()
            if value.isdigit():
                return int(value)
            raise ValueError(f"time_unit 必須是數字: {value}")

        raise ValueError(f"time_unit 必須是數字: {value}")

    def _resolve_risk_free_rate(self, config: Dict[str, Any]) -> float:
        value = config.get("risk_free_rate")
        if value is None:
            raise ValueError("配置缺少 risk_free_rate 設定")

        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError as exc:
                raise ValueError(f"risk_free_rate 必須是數字: {value}") from exc

        if value > 1:
            return float(value) / 100.0
        return float(value)

    def _process_single_file(
        self,
        file_path: str,
        time_unit: int,
        risk_free_rate: float,
        benchmark_parquet_path: Optional[str] = None,
        benchmark_symbol: Optional[str] = None,
    ) -> MetricsTaskResult:
        abs_path = os.path.abspath(file_path)
        self.logger.info("Processing metrics for %s", abs_path)

        if not os.path.exists(abs_path):
            warning = f"找不到檔案：{abs_path}"
            self.logger.error(warning)
            self._display_warning(warning)
            return MetricsTaskResult(abs_path, None, "failed", warning)

        try:
            export_metrics_artifacts(
                abs_path,
                time_unit=time_unit,
                risk_free_rate=risk_free_rate,
                benchmark_parquet_path=benchmark_parquet_path,
                benchmark_symbol=benchmark_symbol,
            )
            output_path = self._derive_output_path(abs_path)
            self._display_success(f"已匯出績效：{os.path.basename(output_path)}")

            return MetricsTaskResult(abs_path, output_path, "success")
        except Exception as exc:  # NOTE: translated to English.
            error_msg = f"績效分析失敗：{exc}"
            self.logger.exception(error_msg)
            self._display_error(error_msg)
            return MetricsTaskResult(abs_path, None, "failed", str(exc))

    @staticmethod
    def _benchmark_context(
        metrics_context: Optional[Dict[str, Any]],
    ) -> tuple[Optional[str], Optional[str]]:
        context = metrics_context or {}
        manifest_path = str(context.get("market_data_bundle_manifest") or "").strip()
        symbol = str(context.get("benchmark_symbol") or "").strip()
        if not manifest_path or not symbol:
            return None, None
        with open(manifest_path, encoding="utf-8") as handle:
            manifest = json.load(handle)
        tables = manifest.get("tables") or {}
        benchmark_table = tables.get("benchmark_close") or {}
        close_table = tables.get("close") or {}
        selected_table = (
            benchmark_table
            if symbol in [str(value) for value in benchmark_table.get("columns") or []]
            else close_table
        )
        close_path = str(selected_table.get("path") or "").strip()
        columns = [str(value) for value in selected_table.get("columns") or []]
        if not close_path or symbol not in columns:
            raise ValueError(
                f"configured benchmark {symbol} is absent from MarketDataBundle benchmark data"
            )
        return close_path, symbol

    def _derive_output_path(self, parquet_path: str) -> str:
        orig_name = os.path.splitext(os.path.basename(parquet_path))[0]
        output_stem = bounded_filename_stem(orig_name, max_length=96, fallback="metrics")
        out_dir = os.path.join(
            os.path.dirname(os.path.dirname(parquet_path)), "metricstracker"
        )
        return os.path.join(out_dir, f"{output_stem}_metrics.parquet")

    def _display_summary(self, task_results: List[MetricsTaskResult]) -> None:
        table = Table(title="Metrics Analysis Summary", show_lines=True, border_style="#dbac30")
        table.add_column("檔案", style="white")
        table.add_column("輸出", style="#1e90ff")
        table.add_column("狀態", style="white")

        for result in task_results:
            status_display = "SUCCESS" if result.status == "success" else "FAILED"
            output_display = (
                os.path.basename(result.output_path) if result.output_path else "—"
            )
            table.add_row(
                os.path.basename(result.source_path), output_display, status_display
            )

        self.console.print(table)


    def _display_info(self, title: str, details: Optional[List[str]] = None) -> None:
        content = title
        if details:
            content += "\n" + "\n".join(details)
        show_info("METRICSTRACKER", content)

    def _display_success(self, message: str) -> None:
        show_success("METRICSTRACKER", message)

    def _display_warning(self, message: str) -> None:
        from utils import show_warning as ui_show_warning
        ui_show_warning("METRICSTRACKER", message)

    def _display_error(self, message: str) -> None:
        show_error("METRICSTRACKER", message)
