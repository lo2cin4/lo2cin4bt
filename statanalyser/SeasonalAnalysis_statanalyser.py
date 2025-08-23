"""
SeasonalAnalysis_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組，負責對時序數據進行季節性分析（如週期性、趨勢分解等），評估時間序列的季節性模式，輔助模型選擇與策略設計。

【流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[SeasonalAnalysis] -->|檢定結果| B[ReportGenerator/下游模組]
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
- test = SeasonalAnalysis(data, predictor_col, return_col)
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

from typing import Dict

import numpy as np
from rich.console import Console
from rich.panel import Panel

from .Base_statanalyser import BaseStatAnalyser


class SeasonalAnalysis(BaseStatAnalyser):
    """季節性分析模組，檢測時間序列的週期性模式"""

    def analyze(self) -> Dict:
        console = Console()
        # 步驟說明 Panel
        panel_content = (
            "🟢 選擇用於統計分析的預測因子\n"
            "🟢 收益率相關性檢驗[自動]\n"
            "🟢 ADF/KPSS平穩性檢驗[自動]\n"
            "🟢 ACF/PACF 自相關性檢驗[自動]\n"
            "🟢 輸出 ACF 或 PACF 互動圖片\n"
            "🟢 統計分佈檢驗[自動]\n"
            "🟢 季節性檢驗[自動]\n\n"
            "[bold #dbac30]說明[/bold #dbac30]\n"
            f"5. '{self.predictor_col}' 季節性分析\n"
            "檢驗功能：檢測時間序列中的週期性模式，判斷是否存在顯著季節性。\n"
            "成功/失敗標準：檢測到顯著季節性（強度>0.1且週期>1）視為有季節性。"
        )
        console.print(
            Panel(
                panel_content,
                title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：季節性分析[自動][/bold #dbac30]",
                border_style="#dbac30",
            )
        )

        series = self.data[self.predictor_col].dropna()
        min_lags = 100
        if len(series) < min_lags:
            msg = f"資料點數不足（{len(series)} < {min_lags}），無法進行季節性分析。建議補充更多數據。"
            console.print(
                Panel(
                    msg,
                    title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return {"success": False, "has_seasonal": False, "period": 0}

        # 檢測週期
        max_lag = min(100, len(series) // 2)
        from statsmodels.tsa.stattools import acf

        acf_vals = acf(series, nlags=max_lag, fft=True)
        peaks = [
            i
            for i in range(1, len(acf_vals) - 1)
            if acf_vals[i] > acf_vals[i - 1] and acf_vals[i] > acf_vals[i + 1]
        ]
        best_period = 0
        if peaks:
            abs_acf = [float(abs(acf_vals[i])) for i in peaks]
            idx = np.argmax(abs_acf)
            best_period = int(peaks[idx])
        else:
            best_period = 0

        if best_period <= 1:
            msg = f"未檢測到有效週期（best_period={best_period}），無法進行季節性分析。可忽略季節性因子。"
            console.print(
                Panel(
                    msg,
                    title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return {"success": False, "has_seasonal": False, "period": 0}

        min_data_length = best_period * 3
        if len(series) < min_data_length:
            msg = f"資料長度不足以支持週期 {best_period}（需至少 {min_data_length} 點，實際 {len(series)} 點），建議補充更多數據。"
            console.print(
                Panel(
                    msg,
                    title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return {"success": False, "has_seasonal": False, "period": 0}

        from statsmodels.tsa.seasonal import seasonal_decompose

        try:
            result = seasonal_decompose(series, model="additive", period=best_period)
            var_residual = np.nanvar(result.resid)
            var_total = series.var()
            seasonal_strength = (
                max(0, 1 - var_residual / var_total) if var_total > 0 else 0
            )
        except ValueError as e:
            msg = f"分解失敗，錯誤訊息：{e}。請檢查數據品質或週期設置。"
            console.print(
                Panel(
                    msg,
                    title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return {"success": False, "has_seasonal": False, "period": 0}

        has_seasonal = seasonal_strength > 0.1
        self.results = {
            "success": True,
            "has_seasonal": has_seasonal,
            "period": best_period,
            "strength": seasonal_strength,
        }

        # 合併結果與策略建議 Panel
        merged_content = (
            "季節性分析結果\n"
            f"週期 = {best_period}\n"
            f"強度 = {seasonal_strength:.2f}\n"
            f"判斷：{'檢測到顯著季節性' if has_seasonal else '未檢測到顯著季節性'}\n"
        )
        if has_seasonal:
            if seasonal_strength > 0.3:
                merged_content += f"[bold green]強烈季節性（週期={best_period}），建議優先納入策略模型[/bold green]"
            else:
                merged_content += f"[bold yellow]季節性（週期={best_period}），可考慮納入策略模型[/bold yellow]"
        else:
            merged_content += "[bold]無顯著季節性，可忽略季節性因子[/bold]"
        console.print(
            Panel(
                merged_content,
                title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

        return self.results
