"""
DistributionTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析子模組，專責對指定數據欄位進行分布檢定（如常態性、偏態、峰態等），協助判斷資料是否適合用於標準化、Z-Score 或分位數策略。

【關聯流程與數據流】
------------------------------------------------------------
- 由 Base_statanalyser 繼承，接收主流程傳入的資料
- 檢定結果傳遞給 ReportGenerator_statanalyser 產生報表
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|調用| B[DistributionTest_statanalyser]
    B -->|檢定結果| C[ReportGenerator_statanalyser]
```

【主控流程細節】
------------------------------------------------------------
- analyze() 為主入口，執行常態性（KS/AD）、偏態、峰態等檢定
- 根據檢定結果自動給出建議（如建議轉換、分位數分析等）
- 檢定結果以 dict 格式回傳，供報表模組與下游流程使用
- 支援自訂檢定參數，並可擴充其他分布檢定方法

【維護與擴充提醒】
------------------------------------------------------------
- 新增檢定方法、參數時，請同步更新 analyze() 及頂部註解
- 若數據結構或欄位有變動，需同步調整與 Base_statanalyser、ReportGenerator_statanalyser 的介面
- 檢定指標、臨界值如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- 檢定樣本數不足時，結果不具統計意義
- 檢定參數設置錯誤會導致判斷失準
- 統計結果格式不符會影響下游報表產生

【範例】
------------------------------------------------------------
- test = DistributionTest(data, predictor_col="因子欄位")
  result = test.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 由主流程或 Base_statanalyser 調用，檢定結果傳遞給 ReportGenerator_statanalyser
- 依賴 pandas、scipy.stats 等第三方庫

【維護重點】
------------------------------------------------------------
- 新增/修改檢定方法、參數時，務必同步更新本檔案、Base_statanalyser 及 README
- 檢定結果格式需與 ReportGenerator_statanalyser 保持一致

【參考】
------------------------------------------------------------
- 詳細檢定規範與指標定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""
import pandas as pd
import numpy as np
from .Base_statanalyser import BaseStatAnalyser
from scipy.stats import kstest, anderson
from typing import Dict
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

class DistributionTest(BaseStatAnalyser):
    """分佈檢驗模組，評估數據是否符合正態分佈"""

    def analyze(self) -> Dict:
        console = Console()
        # 美化步驟說明 Panel
        panel_content = (
            "🟢 選擇用於統計分析的預測因子\n"
            "🟢 收益率相關性檢驗[自動]\n"
            "🟢 ADF/KPSS平穩性檢驗[自動]\n"
            "🟢 ACF/PACF 自相關性檢驗[自動]\n"
            "🟢 輸出 ACF 或 PACF 互動圖片\n"
            "🟢 統計分佈檢驗[自動]\n"
            "🔴 季節性檢驗[自動]\n\n"
            "[bold #dbac30]說明[/bold #dbac30]\n"
            f"4. '{self.predictor_col}' 分布檢驗\n"
            "檢驗功能：評估數據是否符合常態分布，判斷是否適合用於標準化、Z-Score 等分析。\n"
            "成功/失敗標準：同時滿足以下條件視為常態分布：\n"
            "- KS檢驗 p值 > 0.05（無法拒絕常態假設）\n"
            "- AD統計量 < 臨界值（通過Anderson-Darling常態性檢驗）\n"
            "- 偏度、峰度在合理範圍內（偏度約-1~1，峰度約2.5~3.5）"
        )
        panel = Panel(panel_content, title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：分布檢驗[自動][/bold #dbac30]", border_style="#dbac30")
        console.print(panel)
        # robust 計算 skewness/kurtosis 並存入 self.results
        from scipy.stats import skew, kurtosis
        series = self.data[self.predictor_col].dropna()
        if len(series) > 1:
            try:
                skewness = skew(series)
            except Exception:
                skewness = 'N/A'
            try:
                kurt = kurtosis(series)
            except Exception:
                kurt = 'N/A'
        else:
            skewness = 'N/A'
            kurt = 'N/A'
        self.results['skewness'] = skewness
        self.results['kurtosis'] = kurt
        # robust 計算 KS/AD 統計量與 p 值，並存入 self.results
        if len(series) > 1:
            try:
                ks_stat, ks_p = kstest(series, 'norm')
            except Exception:
                ks_stat, ks_p = 'N/A', 'N/A'
            try:
                ad_result = anderson(series, 'norm')
                ad_stat = ad_result.statistic
                ad_critical = ad_result.critical_values[2]  # 5% 臨界值
            except Exception:
                ad_stat, ad_critical = 'N/A', 'N/A'
        else:
            ks_stat, ks_p, ad_stat, ad_critical = 'N/A', 'N/A', 'N/A', 'N/A'
        self.results['ks_stat'] = ks_stat
        self.results['ks_p'] = ks_p
        self.results['ad_stat'] = ad_stat
        self.results['ad_critical'] = ad_critical
        # 結果數據
        ks_stat = self.results.get('ks_stat', 'N/A')
        ks_p = self.results.get('ks_p', 'N/A')
        ad_stat = self.results.get('ad_stat', 'N/A')
        ad_critical = self.results.get('ad_critical', 'N/A')
        skewness = self.results.get('skewness', 'N/A')
        kurtosis = self.results.get('kurtosis', 'N/A')
        df = pd.DataFrame({
            '指標': ['KS統計量', 'KS p值', 'AD統計量', 'AD臨界值', '偏度', '峰度'],
            '數值': [ks_stat, ks_p, ad_stat, ad_critical, skewness, kurtosis]
        })
        table = Table(title="分布檢驗結果", border_style="#dbac30", show_lines=True)
        for col in df.columns:
            table.add_column(str(col), style="bold white")
        for _, row in df.iterrows():
            row_cells = []
            for v in row:
                # 數值型態四捨五入到小數點後3位
                if isinstance(v, (int, float)):
                    row_cells.append(f"[#1e90ff]{v:.3f}[/#1e90ff]")
                elif isinstance(v, str) and v.replace('.', '', 1).isdigit():
                    try:
                        row_cells.append(f"[#1e90ff]{float(v):.3f}[/#1e90ff]")
                    except Exception:
                        row_cells.append(str(v))
                elif isinstance(v, float) and (abs(v) < 1e-3 or abs(v) > 1e3):
                    row_cells.append(f"[#1e90ff]{v:.3e}[/#1e90ff]")
                else:
                    row_cells.append(str(v))
            table.add_row(*row_cells)
        console.print(table)
        # 判斷
        is_normal = self.results.get('is_normal', False)
        summary = f"是否近似常態分布：{'是' if is_normal else '否'}\n"
        suggestions = []
        if is_normal:
            suggestions.append("[bold green]資料近似常態分布，適合使用 Z-Score、標準化等方法進行分析。[/bold green]\n你可以直接用均值、標準差等統計量來描述資料，或用 Z-Score 進行異常值檢測。")
        else:
            if isinstance(skewness, (int, float)) and abs(skewness) > 1:
                suggestions.append(f"[bold yellow]資料偏度較高（偏度={skewness:.2f}），分布不對稱。這在金融數據中常見。\n建議：\n- 可嘗試對數轉換、分位數分析，或觀察資料分布圖。[/bold yellow]")
            if isinstance(kurtosis, (int, float)) and kurtosis > 3.5:
                suggestions.append(f"[bold yellow]資料呈現厚尾分佈（峰度={kurtosis:.2f}），極端值較多。這於金融數據中常見。\n建議：\n- 可嘗試分位數分析、非參數方法，或直接觀察資料分布圖。[/bold yellow]")
            elif isinstance(kurtosis, (int, float)) and kurtosis < 2.5:
                suggestions.append(f"[bold yellow]資料呈現平峰分佈（峰度={kurtosis:.2f}），比常態分布更扁平，極端值較少。這於金融數據中常見。\n建議：\n- 可嘗試分位數分析、非參數方法，或直接觀察資料分布圖。[/bold yellow]")
            if not suggestions:
                suggestions.append("[bold yellow]非正態分佈，建議分位數分析或觀察資料分布圖。[/bold yellow]")
        summary += "\n".join(suggestions)
        console.print(Panel(summary, title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]", border_style="#dbac30"))
        return self.results