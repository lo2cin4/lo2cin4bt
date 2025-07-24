"""
SeasonalAnalysis_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析子模組，專責對時序資料進行季節性分析（如週期性、月份效應等），協助判斷資料中是否存在顯著的季節性規律，並提供策略建議。

【關聯流程與數據流】
------------------------------------------------------------
- 由 Base_statanalyser 繼承，接收主流程傳入的資料
- 分析結果傳遞給 ReportGenerator_statanalyser 產生報表
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|調用| B[SeasonalAnalysis_statanalyser]
    B -->|分析結果| C[ReportGenerator_statanalyser]
```

【主控流程細節】
------------------------------------------------------------
- analyze() 為主入口，執行週期性檢定、季節性分解等分析
- 根據自動偵測的週期與強度，給出是否納入策略模型的建議
- 分析結果以 dict 格式回傳，供報表模組與下游流程使用
- 支援自訂分析參數，並可擴充其他季節性檢定方法

【維護與擴充提醒】
------------------------------------------------------------
- 新增分析方法、參數時，請同步更新 analyze() 及頂部註解
- 若數據結構或欄位有變動，需同步調整與 Base_statanalyser、ReportGenerator_statanalyser 的介面
- 分析指標、臨界值如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- 分析樣本數不足時，無法有效檢測季節性
- 週期偵測錯誤會導致分析失準
- 統計結果格式不符會影響下游報表產生

【範例】
------------------------------------------------------------
- analysis = SeasonalAnalysis(data, predictor_col="因子欄位")
  result = analysis.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 由主流程或 Base_statanalyser 調用，分析結果傳遞給 ReportGenerator_statanalyser
- 依賴 pandas、statsmodels 等第三方庫

【維護重點】
------------------------------------------------------------
- 新增/修改分析方法、參數時，務必同步更新本檔案、Base_statanalyser 及 README
- 分析結果格式需與 ReportGenerator_statanalyser 保持一致

【參考】
------------------------------------------------------------
- 詳細分析規範與指標定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""
import pandas as pd
import numpy as np
from .Base_statanalyser import BaseStatAnalyser
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import acf
from typing import Dict
from rich.panel import Panel
from rich.console import Console

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
            f"5. '{self.predictor_col}' 季節性分析\n"
            "檢驗功能：檢測時間序列中的週期性模式，判斷是否存在顯著季節性。\n"
            "成功/失敗標準：檢測到顯著季節性（強度>0.1且週期>1）視為有季節性。"
        )
        console.print(Panel(panel_content, title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：季節性分析[自動][/bold #dbac30]", border_style="#dbac30"))

        series = self.data[self.predictor_col].dropna()
        min_lags = 100
        if len(series) < min_lags:
            msg = f"資料點數不足（{len(series)} < {min_lags}），無法進行季節性分析。建議補充更多數據。"
            console.print(Panel(msg, title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]", border_style="#8f1511"))
            return {'success': False, 'has_seasonal': False, 'period': 0}

        # 檢測週期
        max_lag = min(100, len(series) // 2)
        from statsmodels.tsa.stattools import acf
        acf_vals = acf(series, nlags=max_lag, fft=True)
        peaks = [i for i in range(1, len(acf_vals) - 1) if acf_vals[i] > acf_vals[i - 1] and acf_vals[i] > acf_vals[i + 1]]
        best_period = 0
        if peaks:
            abs_acf = [float(abs(acf_vals[i])) for i in peaks]
            idx = np.argmax(abs_acf)
            best_period = int(peaks[idx])
        else:
            best_period = 0

        if best_period <= 1:
            msg = f"未檢測到有效週期（best_period={best_period}），無法進行季節性分析。可忽略季節性因子。"
            console.print(Panel(msg, title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]", border_style="#8f1511"))
            return {'success': False, 'has_seasonal': False, 'period': 0}

        min_data_length = best_period * 3
        if len(series) < min_data_length:
            msg = f"資料長度不足以支持週期 {best_period}（需至少 {min_data_length} 點，實際 {len(series)} 點），建議補充更多數據。"
            console.print(Panel(msg, title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]", border_style="#8f1511"))
            return {'success': False, 'has_seasonal': False, 'period': 0}

        from statsmodels.tsa.seasonal import seasonal_decompose
        try:
            result = seasonal_decompose(series, model='additive', period=best_period)
            var_residual = np.nanvar(result.resid)
            var_total = series.var()
            seasonal_strength = max(0, 1 - var_residual / var_total) if var_total > 0 else 0
        except ValueError as e:
            msg = f"分解失敗，錯誤訊息：{e}。請檢查數據品質或週期設置。"
            console.print(Panel(msg, title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]", border_style="#8f1511"))
            return {'success': False, 'has_seasonal': False, 'period': 0}

        has_seasonal = seasonal_strength > 0.1
        self.results = {
            'success': True,
            'has_seasonal': has_seasonal,
            'period': best_period,
            'strength': seasonal_strength
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
        console.print(Panel(merged_content, title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]", border_style="#dbac30"))

        return self.results