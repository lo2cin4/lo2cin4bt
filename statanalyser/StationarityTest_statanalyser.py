"""
StationarityTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 lo2cin4bt 統計分析模組，負責對時序數據進行定態性檢定（如 ADF、KPSS、PP 等），評估時間序列的平穩性，輔助模型選擇與差分策略。

【流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[StationarityTest] -->|檢定結果| B[ReportGenerator/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改檢定類型、參數、圖表邏輯時，請同步更新頂部註解與下游流程
- 若介面、欄位、分析流程有變動，需同步更新本檔案與 Base_statanalyser
- 統計結果格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 檢定參數設置錯誤或數據點不足會導致結果異常
- 頻率設定不符或欄位型態錯誤會影響分析正確性
- 統計結果格式不符會影響下游報表或流程

【範例】
------------------------------------------------------------
- test = StationarityTest(data, predictor_col, return_col)
  result = test.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 繼承 Base_statanalyser，檢定結果傳遞給 ReportGenerator 或下游模組
- 需與 ReportGenerator、主流程等下游結構保持一致

【參考】
------------------------------------------------------------
- statsmodels 官方文件
- Base_statanalyser.py、ReportGenerator_statanalyser.py
- 專案 README
"""

import warnings
from typing import Dict

import pandas as pd
from utils import get_console, show_info, show_step_panel
from rich.table import Table
from statsmodels.tsa.stattools import adfuller, kpss

from .Base_statanalyser import BaseStatAnalyser


class StationarityTest(BaseStatAnalyser):
    """平穩性檢驗模組"""

    def __init__(self, data: pd.DataFrame, predictor_col: str, return_col: str):
        super().__init__(data, predictor_col, return_col)

    def analyze(self) -> Dict:
        step_content = (
            "🟢 選擇用於統計分析的預測因子\n"
            "🟢 收益率相關性檢驗[自動]\n"
            "🟢 ADF/KPSS 平穩性檢驗[自動]\n"
            "🔴 ACF/PACF 自相關性檢驗[自動]\n"
            "🔴 生成 ACF 或 PACF 互動圖片\n"
            "🔴 統計分佈檢驗[自動]\n"
            "🔴 季節性檢驗[自動]\n\n"
            "[bold #dbac30]說明[/bold #dbac30]\n"
            f"2. '{self.predictor_col}' 平穩性檢驗（ADF/KPSS）\n"
            "檢驗功能：判斷序列是否為平穩過程，適合用於傳統時間序列建模。如序列非平穩，很多模型如自回歸 (AR)、ARIMA 模型、線性回歸分析等效果將大打折扣。\n"
            "成功/失敗標準：ADF p<0.05 為平穩，KPSS p>0.05 為平穩。"
        )
        # NOTE: translated to English.
        show_step_panel("STATANALYSER", 1, ["ADF/KPSS 平穩性檢驗[自動]"], step_content)

        # NOTE: translated to English.
        def run_stationarity_tests(series):
            result = {}
            try:
                adf_stat, adf_p, _, _, _, _ = adfuller(series.dropna(), autolag="AIC")
                result["adf_stat"] = adf_stat
                result["adf_p"] = adf_p
                result["adf_stationary"] = adf_p < 0.05
            except Exception as exc:
                result["adf_stat"] = "N/A"
                result["adf_p"] = "N/A"
                result["adf_stationary"] = None
                result["adf_error"] = f"{type(exc).__name__}: {exc}"
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    kpss_stat, kpss_p, _, _ = kpss(series.dropna(), nlags="auto")
                result["kpss_stat"] = kpss_stat
                result["kpss_p"] = kpss_p
                result["kpss_stationary"] = kpss_p > 0.05
            except Exception as exc:
                result["kpss_stat"] = "N/A"
                result["kpss_p"] = "N/A"
                result["kpss_stationary"] = None
                result["kpss_error"] = f"{type(exc).__name__}: {exc}"
            return result

        self.results["predictor"] = run_stationarity_tests(
            self.data[self.predictor_col]
        )
        self.results["return"] = run_stationarity_tests(self.data[self.return_col])
        # NOTE: translated to English.
        pred_adf = self.results["predictor"].get("adf_stat", "N/A")
        pred_adf_p = self.results["predictor"].get("adf_p", "N/A")
        pred_kpss = self.results["predictor"].get("kpss_stat", "N/A")
        pred_kpss_p = self.results["predictor"].get("kpss_p", "N/A")
        ret_adf = self.results["return"].get("adf_stat", "N/A")
        ret_adf_p = self.results["return"].get("adf_p", "N/A")
        ret_kpss = self.results["return"].get("kpss_stat", "N/A")
        ret_kpss_p = self.results["return"].get("kpss_p", "N/A")
        df = pd.DataFrame(
            {
                "指標": ["因子ADF", "因子KPSS", "收益率ADF", "收益率KPSS"],
                "統計量": [pred_adf, pred_kpss, ret_adf, ret_kpss],
                "p值": [pred_adf_p, pred_kpss_p, ret_adf_p, ret_kpss_p],
            }
        )
        # NOTE: translated to English.
        console = get_console()
        table = Table(title="平穩性檢驗結果", border_style="#dbac30", show_lines=True)
        for col in df.columns:
            table.add_column(str(col), style="bold white")
        for _, row in df.iterrows():
            row_cells = []
            for v in row:
                if isinstance(v, (int, float)) or (
                    isinstance(v, str) and v.replace(".", "", 1).isdigit()
                ):
                    row_cells.append(f"[#1e90ff]{v}[/#1e90ff]")
                else:
                    row_cells.append(str(v))
            table.add_row(*row_cells)
        console.print(table)
        # NOTE: translated to English.
        pred_adf_bool = self.results["predictor"].get("adf_stationary")
        pred_kpss_bool = self.results["predictor"].get("kpss_stationary")
        ret_adf_bool = self.results["return"].get("adf_stationary")
        ret_kpss_bool = self.results["return"].get("kpss_stationary")

        def status(value: object) -> str:
            if value is True:
                return "[bold green]是[/bold green]"
            if value is False:
                return "[bold red]否[/bold red]"
            return "[bold yellow]資料不足[/bold yellow]"

        summary = (
            f"因子 ADF 平穩：{status(pred_adf_bool)}，"
            f"KPSS 平穩：{status(pred_kpss_bool)}\n"
            f"收益率 ADF 平穩：{status(ret_adf_bool)}，"
            f"KPSS 平穩：{status(ret_kpss_bool)}\n"
        )
        if pred_adf_bool is None or pred_kpss_bool is None:
            summary += "[bold yellow]因子序列資料不足，未能判定平穩性。[/bold yellow]\n"
        elif pred_adf_bool and pred_kpss_bool:
            summary += "[bold #dbac30]因子序列平穩[/bold #dbac30]，[bold]適合用於傳統時間序列建模（如ARMA/ARIMA）[/bold]\n"
        else:
            summary += "[bold red]因子序列非平穩[/bold red]，[bold]建議差分或轉換後再建模[/bold]\n"
        if ret_adf_bool is None or ret_kpss_bool is None:
            summary += "[bold yellow]收益率序列資料不足，未能判定平穩性。[/bold yellow]"
        elif ret_adf_bool and ret_kpss_bool:
            summary += "[bold #dbac30]收益率序列平穩[/bold #dbac30]，[bold green]可直接用於收益率建模[/bold green]"
        else:
            summary += "[bold red]收益率序列非平穩[/bold red]，[bold]建議差分或轉換後再建模[/bold]"
        show_info("STATANALYSER", summary)
        return self.results
