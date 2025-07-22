"""
StationarityTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析子模組，專責對時序資料進行定態性檢定（如 ADF、KPSS），判斷資料是否為平穩過程，協助決定是否需進行差分或轉換以利後續建模。

【關聯流程與數據流】
------------------------------------------------------------
- 由 Base_statanalyser 繼承，接收主流程傳入的資料
- 檢定結果傳遞給 ReportGenerator_statanalyser 產生報表
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|調用| B[StationarityTest_statanalyser]
    B -->|檢定結果| C[ReportGenerator_statanalyser]
```

【主控流程細節】
------------------------------------------------------------
- analyze() 為主入口，執行 ADF、KPSS 等平穩性檢定
- 同時對因子欄位與收益率欄位進行檢定，並回傳詳細結果
- 根據檢定結果自動給出建議（如建議差分、直接建模等）
- 檢定結果以 dict 格式回傳，供報表模組與下游流程使用

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
- 欄位名稱錯誤或資料為常數會導致檢定失敗

【範例】
------------------------------------------------------------
- test = StationarityTest(data, predictor_col="因子欄位", return_col="收益率欄位")
  result = test.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 由主流程或 Base_statanalyser 調用，檢定結果傳遞給 ReportGenerator_statanalyser
- 依賴 pandas、statsmodels 等第三方庫

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
from statsmodels.tsa.stattools import adfuller, kpss
import warnings
from typing import Dict

class StationarityTest(BaseStatAnalyser):
    """平穩性檢驗模組"""

    def __init__(
            self,
            data: pd.DataFrame,
            predictor_col: str,
            return_col: str
    ):
        super().__init__(data, predictor_col, return_col)

    def analyze(self) -> Dict:
        """執行ADF和KPSS平穩性檢驗"""
        print(f"\n=== 檢驗：{self.predictor_col} 平穩性檢驗（ADF/KPSS） ===")
        series = self.data[self.predictor_col]
        series_clean = series.dropna()

        if len(series_clean) < 30:
            raise ValueError(f"數據點數不足（{len(series_clean)}）於平穩性檢驗")

        if series_clean.var() == 0:
            raise ValueError("序列為常數（無方差）")

        self.results = {"predictor": {}, "return": {}}

        # ADF Test for predictor
        try:
            adf_result = adfuller(series_clean, autolag="AIC")
            self.results["predictor"]["adf_stat"] = adf_result[0]
            self.results["predictor"]["adf_p"] = adf_result[1]
            self.results["predictor"]["adf_stationary"] = adf_result[1] < 0.05
            # print(f"\n因子 ({self.predictor_col}) ADF 檢驗：")
            # print(f"  統計量：{adf_result[0]:.4f}")
            # print(f"  p 值：{adf_result[1]:.4f}")
            # print(f"  是否平穩：{'是' if adf_result[1] < 0.05 else '否'} (p < 0.05 表示平穩)")
        except Exception as e:
            self.results["predictor"]["adf_error"] = str(e)
            # print(f"因子 ADF 檢驗錯誤：{e}")

        # KPSS Test for predictor
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                kpss_result = kpss(series_clean, regression="c", nlags="auto")
                self.results["predictor"]["kpss_stat"] = kpss_result[0]
                self.results["predictor"]["kpss_p"] = kpss_result[1]
                self.results["predictor"]["kpss_stationary"] = kpss_result[1] > 0.05
                # print(f"\n因子 ({self.predictor_col}) KPSS 檢驗：")
                # print(f"  統計量：{kpss_result[0]:.4f}")
                # print(f"  p 值：{kpss_result[1]:.4f}")
                # print(f"  是否平穩：{'是' if kpss_result[1] > 0.05 else '否'} (p > 0.05 表示平穩)")
        except Exception as e:
            self.results["predictor"]["kpss_error"] = str(e)
            # print(f"因子 KPSS 檢驗錯誤：{e}")

        # 對收益率進行檢驗
        series_return = self.data[self.return_col]
        series_return_clean = series_return.dropna()

        if len(series_return_clean) >= 30 and series_return_clean.var() > 0:
            try:
                adf_result = adfuller(series_return_clean, autolag="AIC")
                self.results["return"]["adf_stat"] = adf_result[0]
                self.results["return"]["adf_p"] = adf_result[1]
                self.results["return"]["adf_stationary"] = adf_result[1] < 0.05
                # print(f"\n收益率 ({self.return_col}) ADF 檢驗：")
                # print(f"  統計量：{adf_result[0]:.4f}")
                # print(f"  p 值：{adf_result[1]:.4f}")
                # print(f"  是否平穩：{'是' if adf_result[1] < 0.05 else '否'} (p < 0.05 表示平穩)")
            except Exception as e:
                self.results["return"]["adf_error"] = str(e)
                # print(f"收益率 ADF 檢驗錯誤：{e}")

            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore")
                    kpss_result = kpss(series_return_clean, regression="c", nlags="auto")
                    self.results["return"]["kpss_stat"] = kpss_result[0]
                    self.results["return"]["kpss_p"] = kpss_result[1]
                    self.results["return"]["kpss_stationary"] = kpss_result[1] > 0.05
                    # print(f"\n收益率 ({self.return_col}) KPSS 檢驗：")
                    # print(f"  統計量：{kpss_result[0]:.4f}")
                    # print(f"  p 值：{kpss_result[1]:.4f}")
                    # print(f"  是否平穩：{'是' if kpss_result[1] > 0.05 else '否'} (p > 0.05 表示平穩)")
            except Exception as e:
                self.results["return"]["kpss_error"] = str(e)
                # print(f"收益率 KPSS 檢驗錯誤：{e}")

        print("1. 檢驗名稱：平穩性檢驗（ADF/KPSS）")
        print("2. 檢驗功能：判斷序列是否為平穩過程，適合用於傳統時間序列建模。")
        print("3. 成功/失敗標準：ADF p<0.05 為平穩，KPSS p>0.05 為平穩。")
        print("4. 檢驗結果數據：")
        print(f"   - 因子ADF統計量={self.results['predictor'].get('adf_stat', 'N/A'):.4f}，p={self.results['predictor'].get('adf_p', 'N/A')}")
        print(f"   - 因子KPSS統計量={self.results['predictor'].get('kpss_stat', 'N/A'):.4f}，p={self.results['predictor'].get('kpss_p', 'N/A')}")
        print(f"   - 收益率ADF統計量={self.results['return'].get('adf_stat', 'N/A'):.4f}，p={self.results['return'].get('adf_p', 'N/A')}")
        print(f"   - 收益率KPSS統計量={self.results['return'].get('kpss_stat', 'N/A'):.4f}，p={self.results['return'].get('kpss_p', 'N/A')}")
        print("5. 檢驗結果判斷：")
        pred_adf = self.results['predictor'].get('adf_stationary', False)
        pred_kpss = self.results['predictor'].get('kpss_stationary', False)
        ret_adf = self.results['return'].get('adf_stationary', False)
        ret_kpss = self.results['return'].get('kpss_stationary', False)
        print(f"   - 因子ADF平穩：{'是' if pred_adf else '否'}，KPSS平穩：{'是' if pred_kpss else '否'}")
        print(f"   - 收益率ADF平穩：{'是' if ret_adf else '否'}，KPSS平穩：{'是' if ret_kpss else '否'}")
        print("6. 量化策略開發建議：")
        if pred_adf and pred_kpss:
            print("   - 因子序列平穩，適合用於傳統時間序列建模（如ARMA/ARIMA）")
        else:
            print("   - 因子序列非平穩，建議差分或轉換後再建模")
        if ret_adf and ret_kpss:
            print("   - 收益率序列平穩，可直接用於收益率建模")
        else:
            print("   - 收益率序列非平穩，建議差分或轉換後再建模")
        print("=============================")

        return self.results