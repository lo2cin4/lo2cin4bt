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
        print(f"\n=== 檢驗：{self.predictor_col} 分布檢驗 ===")
        # print("1. 檢驗名稱：分佈檢驗")
        # print("2. 檢驗功能：評估數據是否符合正態分佈")

        series = self.data[self.predictor_col].dropna()
        if len(series) < 50:
            # print(f"3. 檢驗結果：數據不足（{len(series)} 點，需至少 50 點）")
            return {'success': False, 'is_normal': False}

        # 標準化數據
        series_std = (series - series.mean()) / series.std()

        # KS 和 AD 檢驗
        ks_stat, ks_p = kstest(series_std, 'norm')
        ad_result = anderson(series_std, dist='norm')
        ad_stat = ad_result.statistic
        ad_critical = ad_result.critical_values[2]  # 5% 臨界值

        # 偏度和峰度
        skewness = series.skew()
        kurt_excess = series.kurtosis()
        kurt_actual = kurt_excess + 3
        skew_threshold = 1.5 if len(series) < 100 else 1.0
        kurt_threshold_upper = 3.7 if len(series) < 100 else 3.5
        kurt_threshold_lower = 2.3 if len(series) < 100 else 2.5

        # print("3. 檢驗結果數據：")
        # print(f"   - KS 統計量: {ks_stat:.4f} (p={ks_p:.4f})")
        # print(f"   - AD 統計量: {ad_stat:.2f} (5% 臨界值={ad_critical:.2f})")
        is_normal = ks_p > 0.05 and ad_stat < ad_critical
        # print(f"4. 是否符合正態分佈：{'是' if is_normal else '否'}")
        # print(f"   - 偏度：{skewness:.2f}（理想值=0，閾值=±{skew_threshold}）")
        # print(f"   - 峰度：{kurt_excess:.2f}（實際峰度={kurt_actual:.2f}，閾值=[{kurt_threshold_lower}, {kurt_threshold_upper}）")

        # print("5. 建議：")
        suggestions = []
        if is_normal:
            suggestions.append("   - 數據符合正態分佈，適合 Z-Score 分析")
        else:
            if abs(skewness) > skew_threshold:
                suggestions.append(f"   - 高偏度（{skewness:.2f}），建議對數轉換或分位數分析")
            if kurt_actual > kurt_threshold_upper:
                suggestions.append(f"   - 尖峰厚尾（峰度={kurt_actual:.2f}），建議分位數分析")
            elif kurt_actual < kurt_threshold_lower:
                suggestions.append(f"   - 平峰分佈（峰度={kurt_actual:.2f}），檢查數據異常")
            if not suggestions:
                suggestions.append("   - 非正態分佈，建議分位數分析")
        # for suggestion in suggestions:
        #     print(suggestion)

        self.results = {
            'success': True,
            'is_normal': is_normal,
            'ks_stat': ks_stat,
            'ks_p': ks_p,
            'ad_stat': ad_stat,
            'ad_critical': ad_critical,
            'skewness': skewness,
            'kurtosis': kurt_actual
        }

        print("1. 檢驗名稱：分布檢驗")
        print("2. 檢驗功能：評估數據是否符合常態分布，判斷是否適合用於標準化、Z-Score等分析。")
        print("3. 成功/失敗標準：KS檢驗p值>0.05且AD統計量<臨界值，且偏度、峰度在合理範圍內視為常態。")
        print(f"4. 檢驗結果數據：KS={ks_stat:.4f} (p={ks_p:.4g})，AD={ad_stat:.2f} (5%臨界值={ad_critical:.2f})，偏度={skewness:.2f}，峰度={kurt_actual:.2f}")
        print(f"5. 檢驗結果判斷：{'近似常態分布' if is_normal else '非常態分布'}")
        print("6. 量化策略開發建議：")
        for suggestion in suggestions:
            print(suggestion)
        print("=============================")

        return self.results