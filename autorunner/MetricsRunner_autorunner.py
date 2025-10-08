"""
MetricsRunner_autorunner.py

【功能說明】
------------------------------------------------------------
本模組為 lo2cin4bt Autorunner 的績效分析封裝器，負責在自動化流程中
根據配置載入回測交易記錄（Parquet），並調用 metricstracker 的匯出邏輯，
於無需使用者互動的前提下完成績效指標匯出與摘要顯示。

【流程與數據流】
------------------------------------------------------------
- 由 Base_autorunner 調用，接收回測結果與 metrics 配置
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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from metricstracker.MetricsExporter_metricstracker import MetricsExporter


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
        self.console = Console()
        self.panel_title = "[bold #8f1511]📈 績效分析 MetricsTracker[/bold #8f1511]"
        self.panel_error_style = "#8f1511"
        self.panel_success_style = "#dbac30"
        self.summary: Dict[str, Any] = {}

    def run(
        self, backtest_results: Dict[str, Any], config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """執行績效分析主流程"""

        enable_metrics = config.get("enable_metrics_analysis", False)
        if not enable_metrics:
            self.logger.info("Metrics analysis disabled in config, skip.")
            return None

        exported_files = backtest_results.get("exported_files", [])
        target_files = self._collect_target_files(exported_files, config)

        if not target_files:
            self._display_warning("找不到可用的回測 Parquet 檔，已跳過績效分析。")
            self.summary = {
                "enabled": True,
                "executed": False,
                "message": "No parquet files available",
            }
            return self.summary

        time_unit = self._resolve_time_unit(config)
        risk_free_rate = self._resolve_risk_free_rate(config)

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

    # ------------------------------------------------------------------
    # 私有工具方法
    # ------------------------------------------------------------------

    def _collect_target_files(
        self, exported_files: List[str], config: Dict[str, Any]
    ) -> List[str]:
        file_selection_mode = config.get("file_selection_mode", "auto").lower()

        # 固定路徑：records/backtester/
        parquet_directory = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "records", "backtester"
        )
        parquet_directory = os.path.abspath(parquet_directory)

        # 先確保路徑存在
        if not os.path.exists(parquet_directory):
            self.logger.warning(
                "Parquet directory does not exist: %s", parquet_directory
            )
            return exported_files

        if file_selection_mode not in {"auto", "all"}:
            self.logger.info(
                "Unsupported file_selection_mode=%s, fallback to auto.",
                file_selection_mode,
            )
            file_selection_mode = "auto"

        if exported_files:
            exported_abs = [os.path.abspath(path) for path in exported_files]
        else:
            exported_abs = []

        if file_selection_mode == "auto":
            if exported_abs:
                exported_abs.sort(key=os.path.getmtime, reverse=True)
                return [exported_abs[0]]
            candidate = self._find_latest_parquet(parquet_directory)
            return [candidate] if candidate else []

        if exported_abs:
            exported_abs.sort(key=os.path.getmtime, reverse=True)
            return exported_abs
        return self._list_all_parquet(parquet_directory)

    def _find_latest_parquet(self, directory: str) -> Optional[str]:
        parquet_files: List[str] = []
        for entry in os.listdir(directory):
            if entry.endswith(".parquet"):
                parquet_files.append(os.path.join(directory, entry))
        if not parquet_files:
            return None
        parquet_files.sort(key=os.path.getmtime, reverse=True)
        return parquet_files[0]

    def _list_all_parquet(self, directory: str) -> List[str]:
        parquet_files: List[str] = []
        for entry in os.listdir(directory):
            if entry.endswith(".parquet"):
                parquet_files.append(os.path.join(directory, entry))
        parquet_files.sort(key=os.path.getmtime, reverse=True)
        return parquet_files

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
    ) -> MetricsTaskResult:
        abs_path = os.path.abspath(file_path)
        self.logger.info("Processing metrics for %s", abs_path)

        if not os.path.exists(abs_path):
            warning = f"找不到檔案：{abs_path}"
            self.logger.error(warning)
            self._display_warning(warning)
            return MetricsTaskResult(abs_path, None, "failed", warning)

        try:
            df = pd.read_parquet(abs_path)
            MetricsExporter.export(df, abs_path, time_unit, risk_free_rate)
            output_path = self._derive_output_path(abs_path)
            self._display_success(f"已匯出績效：{os.path.basename(output_path)}")
            return MetricsTaskResult(abs_path, output_path, "success")
        except Exception as exc:  # pragma: no cover (防止執行時意外)
            error_msg = f"績效分析失敗：{exc}"
            self.logger.exception(error_msg)
            self._display_error(error_msg)
            return MetricsTaskResult(abs_path, None, "failed", str(exc))

    def _derive_output_path(self, parquet_path: str) -> str:
        orig_name = os.path.splitext(os.path.basename(parquet_path))[0]
        out_dir = os.path.join(
            os.path.dirname(os.path.dirname(parquet_path)), "metricstracker"
        )
        return os.path.join(out_dir, f"{orig_name}_metrics.parquet")

    def _display_summary(self, task_results: List[MetricsTaskResult]) -> None:
        table = Table(title="📈 績效分析摘要", show_lines=True, border_style="#dbac30")
        table.add_column("檔案", style="white")
        table.add_column("輸出", style="#1e90ff")
        table.add_column("狀態", style="white")

        for result in task_results:
            status_display = "✅ 成功" if result.status == "success" else "❌ 失敗"
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
        self.console.print(
            Panel(
                content, title=self.panel_title, border_style=self.panel_success_style
            )
        )

    def _display_success(self, message: str) -> None:
        self.console.print(
            Panel(
                message, title=self.panel_title, border_style=self.panel_success_style
            )
        )

    def _display_warning(self, message: str) -> None:
        self.console.print(
            Panel(message, title=self.panel_title, border_style="#8f1511"),
        )

    def _display_error(self, message: str) -> None:
        self.console.print(
            Panel(
                message,
                title=self.panel_title,
                border_style=self.panel_error_style,
            )
        )
