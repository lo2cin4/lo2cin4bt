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

class DistributionTest(BaseStatAnalyser):
    """分佈檢驗模組，評估數據是否符合正態分佈"""

    def analyze(self) -> Dict:
        self.print_step_panel(f"{self.predictor_col} 分布檢驗\n1. 檢驗名稱：分布檢驗\n2. 檢驗功能：評估數據是否符合常態分布，判斷是否適合用於標準化、Z-Score等分析。\n3. 成功/失敗標準：KS檢驗p值>0.05且AD統計量<臨界值，且偏度、峰度在合理範圍內視為常態。","步驟說明","🔬")
        # 結果數據
        ks_stat = self.results.get('ks_stat', 'N/A')
        ks_p = self.results.get('ks_p', 'N/A')
        ad_stat = self.results.get('ad_stat', 'N/A')
        ad_critical = self.results.get('ad_critical', 'N/A')
        skewness = self.results.get('skewness', 'N/A')
        kurtosis = self.results.get('kurtosis', 'N/A')
        df = pd.DataFrame({
            '指標': ['KS統計量', 'KS p值', 'AD統計量', 'AD 5%臨界值', '偏度', '峰度'],
            '數值': [ks_stat, ks_p, ad_stat, ad_critical, skewness, kurtosis]
        })
        self.print_result_table(df, "分布檢驗結果", "🔬")
        # 判斷
        is_normal = self.results.get('is_normal', False)
        summary = f"是否近似常態分布：{'是' if is_normal else '否'}\n"
        suggestions = []
        if is_normal:
            suggestions.append("數據符合正態分佈，適合 Z-Score 分析")
        else:
            if abs(skewness) != 'N/A' and abs(skewness) > 1:
                suggestions.append(f"高偏度（{skewness:.2f}），建議對數轉換或分位數分析")
            if kurtosis != 'N/A' and kurtosis > 3.5:
                suggestions.append(f"尖峰厚尾（峰度={kurtosis:.2f}），建議分位數分析")
            elif kurtosis != 'N/A' and kurtosis < 2.5:
                suggestions.append(f"平峰分佈（峰度={kurtosis:.2f}），檢查數據異常")
            if not suggestions:
                suggestions.append("非正態分佈，建議分位數分析")
        summary += "\n".join(suggestions)
        self.print_info_panel(summary, "結論與建議", "🔬")
        return self.results