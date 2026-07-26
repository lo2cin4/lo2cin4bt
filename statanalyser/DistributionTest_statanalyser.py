"""
DistributionTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 lo2cin4bt 統計分析模組，負責對預測因子進行分布檢定（如常態性、偏態、峰態等），評估數據分布特性，輔助模型選擇與風險評估。

【流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[DistributionTest] -->|檢定結果| B[ReportGenerator/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改檢定類型、參數、圖表邏輯時，請同步更新頂部註解與下游流程
- 若介面、欄位、分析流程有變動，需同步更新本檔案與 Base_statanalyser
- 統計結果格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 數據點不足或極端值過多會導致檢定結果異常
- 欄位型態錯誤或缺失值處理不當會影響分析正確性
- 統計結果格式不符會影響下游報表或流程

【範例】
------------------------------------------------------------
- test = DistributionTest(data, predictor_col, return_col)
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

import pandas as pd
from rich.table import Table
from scipy.stats import anderson, kstest

from .Base_statanalyser import BaseStatAnalyser


class DistributionTest(BaseStatAnalyser):
    """分佈檢驗模組，評估數據是否符合正態分佈"""

    def analyze(self) -> Dict:
        from utils import get_console, show_info, show_step_panel
        console = get_console()
        # NOTE: translated to English.
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
        show_step_panel("STATANALYSER", 1, ["分布檢驗[自動]"], panel_content)
        # NOTE: translated to English.
        from scipy.stats import kurtosis, skew

        series = self.data[self.predictor_col].dropna()
        diagnostic_errors = []
        if len(series) > 1:
            try:
                skewness = skew(series)
            except Exception as exc:
                skewness = "N/A"
                diagnostic_errors.append(
                    f"skewness:{type(exc).__name__}: {exc}"
                )
            try:
                kurt = kurtosis(series)
            except Exception as exc:
                kurt = "N/A"
                diagnostic_errors.append(
                    f"kurtosis:{type(exc).__name__}: {exc}"
                )
        else:
            skewness = "N/A"
            kurt = "N/A"
        self.results["skewness"] = skewness
        self.results["kurtosis"] = kurt
        # NOTE: translated to English.
        if len(series) > 1:
            try:
                ks_stat, ks_p = kstest(series, "norm")
            except Exception as exc:
                ks_stat, ks_p = "N/A", "N/A"
                diagnostic_errors.append(
                    f"kstest:{type(exc).__name__}: {exc}"
                )
            try:
                try:
                    ad_result = anderson(series, "norm", method="interpolated")
                except TypeError:
                    ad_result = anderson(series, "norm")
                ad_stat = ad_result.statistic
                ad_critical = ad_result.critical_values[2]  # NOTE: translated to English.
            except Exception as exc:
                ad_stat, ad_critical = "N/A", "N/A"
                diagnostic_errors.append(
                    f"anderson:{type(exc).__name__}: {exc}"
                )
        else:
            ks_stat, ks_p, ad_stat, ad_critical = "N/A", "N/A", "N/A", "N/A"
        self.results["ks_stat"] = ks_stat
        self.results["ks_p"] = ks_p
        self.results["ad_stat"] = ad_stat
        self.results["ad_critical"] = ad_critical
        self.results["diagnostic_errors"] = diagnostic_errors
        # NOTE: translated to English.
        ks_stat = self.results.get("ks_stat", "N/A")
        ks_p = self.results.get("ks_p", "N/A")
        ad_stat = self.results.get("ad_stat", "N/A")
        ad_critical = self.results.get("ad_critical", "N/A")
        skewness = self.results.get("skewness", "N/A")
        kurtosis = self.results.get("kurtosis", "N/A")
        df = pd.DataFrame(
            {
                "指標": ["KS統計量", "KS p值", "AD統計量", "AD臨界值", "偏度", "峰度"],
                "數值": [ks_stat, ks_p, ad_stat, ad_critical, skewness, kurtosis],
            }
        )
        table = Table(title="分布檢驗結果", border_style="#dbac30", show_lines=True)
        for col in df.columns:
            table.add_column(str(col), style="bold white")
        for _, row in df.iterrows():
            row_cells = []
            for v in row:
                # NOTE: translated to English.
                if isinstance(v, (int, float)):
                    row_cells.append(f"[#1e90ff]{v:.3f}[/#1e90ff]")
                elif isinstance(v, str) and v.replace(".", "", 1).isdigit():
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
        # NOTE: translated to English.
        is_normal = self.results.get("is_normal", False)
        summary = f"是否近似常態分布：{'是' if is_normal else '否'}\n"
        suggestions = []
        if is_normal:
            suggestions.append(
                "[bold green]資料近似常態分布，適合使用 Z-Score、標準化等方法進行分析。[/bold green]\n你可以直接用均值、標準差等統計量來描述資料，或用 Z-Score 進行異常值檢測。"
            )
        else:
            if isinstance(skewness, (int, float)) and abs(skewness) > 1:
                suggestions.append(
                    f"[bold yellow]資料偏度較高（偏度={skewness:.2f}），分布不對稱。這在金融數據中常見。\n建議：\n- 可嘗試對數轉換、分位數分析，或觀察資料分布圖。[/bold yellow]"
                )
            if isinstance(kurtosis, (int, float)) and kurtosis > 3.5:
                suggestions.append(
                    f"[bold yellow]資料呈現厚尾分佈（峰度={kurtosis:.2f}），極端值較多。這於金融數據中常見。\n建議：\n- 可嘗試分位數分析、非參數方法，或直接觀察資料分布圖。[/bold yellow]"
                )
            elif isinstance(kurtosis, (int, float)) and kurtosis < 2.5:
                suggestions.append(
                    f"[bold yellow]資料呈現平峰分佈（峰度={kurtosis:.2f}），比常態分布更扁平，極端值較少。這於金融數據中常見。\n建議：\n- 可嘗試分位數分析、非參數方法，或直接觀察資料分布圖。[/bold yellow]"
                )
            if not suggestions:
                suggestions.append(
                    "[bold yellow]非正態分佈，建議分位數分析或觀察資料分布圖。[/bold yellow]"
                )
        summary += "\n".join(suggestions)
        show_info("STATANALYSER", summary)
        return self.results
