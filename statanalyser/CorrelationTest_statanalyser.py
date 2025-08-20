"""
CorrelationTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組，負責對預測因子與收益率進行相關性檢定（如 Pearson、Spearman、Kendall 等），評估預測因子的預測能力與線性關係強度。

【流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[CorrelationTest] -->|檢定結果| B[ReportGenerator/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改檢定類型、參數、圖表邏輯時，請同步更新頂部註解與下游流程
- 若介面、欄位、分析流程有變動，需同步更新本檔案與 Base_statanalyser
- 統計結果格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 數據點不足或缺失值過多會導致檢定結果異常
- 欄位型態錯誤或數據對齊問題會影響分析正確性
- 統計結果格式不符會影響下游報表或流程

【範例】
------------------------------------------------------------
- test = CorrelationTest(data, predictor_col, return_col)
  result = test.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 繼承 Base_statanalyser，檢定結果傳遞給 ReportGenerator 或下游模組
- 需與 ReportGenerator、主流程等下游結構保持一致

【參考】
------------------------------------------------------------
- scipy、pandas 官方文件
- Base_statanalyser.py、ReportGenerator_statanalyser.py
- 專案 README
"""

from typing import Dict

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from scipy.stats import pearsonr, spearmanr

from .Base_statanalyser import BaseStatAnalyser


class CorrelationTest(BaseStatAnalyser):
    """相關性測試模組，評估因子預測能力"""

    def __init__(
        self,
        data: pd.DataFrame,
        predictor_col: str,
        return_col: str,
    ):
        super().__init__(data, predictor_col, return_col)
        self.lags = [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            45,
            60,
        ]

    def _cal_maxCCC(self, X: np.ndarray, Y: np.ndarray) -> float:
        """
        計算 Chatterjee 相關系數 (ξ) 的簡潔實現

        Args:
            X: 第一個變數的數組
            Y: 第二個變數的數組

        Returns:
            Chatterjee 相關系數值 (0 到 1 之間)
        """

        def _CCC(X, Y):
            Y_sort_by_X = Y[np.argsort(X)]
            Y_ranks = np.argsort(np.argsort(Y_sort_by_X))
            ccc = 1 - 3 * np.abs(np.diff(Y_ranks)).sum() / (len(Y) ** 2 - 1)
            return ccc

        return max(_CCC(X, Y), _CCC(Y, X))

    def analyze(self) -> Dict:
        # 步驟說明
        console = Console()
        console.print(
            Panel(
                "🟢 選擇用於統計分析的預測因子\n"
                "🟢 收益率相關性檢驗[自動]\n"
                "🔴 ADF/KPSS 平穩性檢驗[自動]\n"
                "🔴 ACF/PACF 自相關性檢驗[自動]\n"
                "🔴 生成 ACF 或 PACF 互動圖片\n"
                "🔴 統計分佈檢驗[自動]\n"
                "🔴 季節性檢驗[自動]\n\n"
                "[bold #dbac30]說明[/bold #dbac30]\n"
                "1.因子收益率相關性檢驗\n檢驗功能：通過計算因子與未來收益率的相關性，評估因子對資產收益的預測能力，避免後續分析無效因子。\n成功/失敗標準：\n   - |Spearman| < 0.2：因子預測能力微弱，建議更換因子。\n   - |Spearman| ≥ 0.2 且 < 0.4：因子具有輕微預測能力，適合輔助策略。\n   - |Spearman| ≥ 0.4 且 < 0.7：因子具有良好預測能力，可作為主要策略因子。\n   - |Spearman| ≥ 0.7：因子具有優秀預測能力，適合核心交易策略。\n   - 注意：Spearman 相關係數衡量因子與收益率的單調關係，適合非正態數據（如 BTC 收益率的尖峰厚尾特性）。\n           係數絕對值越大，預測能力越強；p 值 < 0.05 表示相關性統計顯著。\n   - Chatterjee 相關系數（ξ）檢測非線性相關性，值域 0-1，不受單調性限制。\n       - |ξ| < 0.2：非線性相關性極弱\n       - |ξ| ≥ 0.2 且 < 0.4：非線性相關性較弱\n       - |ξ| ≥ 0.4 且 < 0.7：非線性相關性中等\n       - |ξ| ≥ 0.7：非線性相關性強",
                title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：收益率相關性檢驗[自動][/bold #dbac30]",
                border_style="#dbac30",
            )
        )
        # 數據完整性
        console.print(
            Panel(
                f"數據完整性檢查\n原始數據行數：{len(self.data)}\n因子列（{self.predictor_col}）NaN 數：{self.data[self.predictor_col].isna().sum()}\n收益率列（{self.return_col}）NaN 數：{self.data[self.return_col].isna().sum()}",
                title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                border_style="#dbac30",
            )
        )
        correlation_results = {}
        skipped_lags = []
        for lag in self.lags:
            return_series = (
                self.data[self.return_col]
                if lag == 0
                else self.data[self.return_col].shift(-lag)
            )
            temp_df = pd.DataFrame(
                {"factor": self.data[self.predictor_col], "return": return_series}
            ).dropna()
            if len(temp_df) < 30:
                skipped_lags.append(lag)
                continue
            try:
                pearson_corr, pearson_p = pearsonr(temp_df["factor"], temp_df["return"])
                spearman_corr, spearman_p = spearmanr(
                    temp_df["factor"], temp_df["return"]
                )
                chatterjee_corr = self._cal_maxCCC(
                    temp_df["factor"].to_numpy(), temp_df["return"].to_numpy()
                )
                correlation_results[lag] = {
                    "Pearson": pearson_corr,
                    "Pearson_p": pearson_p,
                    "Spearman": spearman_corr,
                    "Spearman_p": spearman_p,
                    "Chatterjee": chatterjee_corr,
                }
            except ValueError:
                skipped_lags.append(lag)
                continue
        # 警告 Panel
        for lag in skipped_lags:
            if lag in correlation_results:
                continue
            console.print(
                Panel(
                    f"滯後期 {lag} 日的數據不足（{len(self.data) if lag == 0 else len(self.data) - lag} 筆，需至少 30 筆），跳過此滯後期。",
                    title="[bold yellow]資料不足[/bold yellow]",
                    border_style="yellow",
                )
            )
        # 結果表格
        corr_df = pd.DataFrame(correlation_results).T.round(4)
        # 若有 index，將其作為第一欄顯示，且 index 欄用白色
        show_index = corr_df.index.name or corr_df.index.names[0] or "lag"
        table = Table(title="相關性分析結果", border_style="#dbac30", show_lines=True)
        table.add_column(str(show_index), style="bold white")
        for col in corr_df.columns:
            table.add_column(str(col), style="bold white")
        for idx, row in corr_df.iterrows():
            row_cells = [str(idx)]
            for v in row:
                if isinstance(v, (int, float)) or (
                    isinstance(v, str) and v.replace(".", "", 1).isdigit()
                ):
                    row_cells.append(f"[#1e90ff]{v}[/#1e90ff]")
                else:
                    row_cells.append(str(v))
            table.add_row(*row_cells)
        console.print(table)
        # 最佳 lag 與 Chatterjee
        best_lag = None
        best_spearman = 0
        for lag, vals in correlation_results.items():
            if abs(vals["Spearman"]) > abs(best_spearman):
                best_spearman = vals["Spearman"]
                best_lag = lag
        best_chatterjee_lag = None
        best_chatterjee = 0
        for lag, vals in correlation_results.items():
            if vals["Chatterjee"] > best_chatterjee:
                best_chatterjee = vals["Chatterjee"]
                best_chatterjee_lag = lag
        # 結論與建議
        summary = ""
        if best_lag is None:
            summary += f"無法計算任何滯後期的相關性，數據可能不足或無效。\n已跳過滯後期：{skipped_lags if skipped_lags else '無'}\n建議：檢查數據完整性（因子和收益率序列），或更換因子。"
        else:
            spearman_p = correlation_results[best_lag]["Spearman_p"]
            # Spearman 判斷
            if abs(best_spearman) < 0.2:
                strength = "微弱"
                summary += f"因子預測能力{strength}（最佳 Spearman = {best_spearman:.4f} @ lag={best_lag}, p 值={spearman_p:.4f}）\n"
            else:
                strength = (
                    "輕微"
                    if abs(best_spearman) < 0.4
                    else "良好" if abs(best_spearman) < 0.7 else "優秀"
                )
                significance = "顯著" if spearman_p < 0.05 else "不顯著"
                summary += f"因子具有{strength}預測能力（最佳 Spearman = {best_spearman:.4f} @ lag={best_lag}, p 值={spearman_p:.4f}，統計{significance}）\n"
            # Chatterjee 判斷
            if best_chatterjee is not None:
                if abs(best_chatterjee) < 0.2:
                    c_level = "極弱"
                elif abs(best_chatterjee) < 0.4:
                    c_level = "較弱"
                elif abs(best_chatterjee) < 0.7:
                    c_level = "中等"
                else:
                    c_level = "強"
                summary += f"Chatterjee 非線性相關性{c_level}（最佳 ξ = {best_chatterjee:.4f} @ lag={best_chatterjee_lag}）"
        console = Console()
        console.print(
            Panel(
                summary,
                title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                border_style="#dbac30",
            )
        )
        self.results = {
            "correlation_results": correlation_results,
            "skipped_lags": skipped_lags,
            "best_lag": best_lag,
            "best_spearman": best_spearman,
            "best_chatterjee_lag": best_chatterjee_lag,
            "best_chatterjee": best_chatterjee,
        }
        return self.results
