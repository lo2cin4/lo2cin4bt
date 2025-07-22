"""
CorrelationTest_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組，負責對多變數資料進行相關性檢定（如皮爾森、斯皮爾曼、Chatterjee 等），評估因子與收益率間的線性與非線性關聯，輔助預測能力篩選與策略設計。

【關聯流程與數據流】
------------------------------------------------------------
- 繼承 Base_statanalyser，作為統計分析子類之一
- 檢定結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[CorrelationTest] -->|檢定結果| B[ReportGenerator/下游模組]
```

【主控流程細節】
------------------------------------------------------------
- 實作 analyze 方法，支援多種滯後期（lag）相關性分析
- 計算皮爾森、斯皮爾曼、Chatterjee 相關係數及 p 值
- 自動尋找最佳滯後期與相關性衰減分析，輔助因子有效性判斷
- 結果以字典格式返回，便於下游報表與自動化流程

【維護與擴充提醒】
------------------------------------------------------------
- 新增/修改檢定類型、滯後期、相關性指標時，請同步更新頂部註解與下游流程
- 若介面、欄位、分析流程有變動，需同步更新本檔案與 Base_statanalyser
- 統計結果格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 檢定參數設置錯誤或數據點不足會導致結果異常
- 欄位型態錯誤或滯後期資料不足會影響分析正確性
- 統計結果格式不符會影響下游報表或流程

【範例】
------------------------------------------------------------
- test = CorrelationTest(data, predictor_col, return_col)
  result = test.analyze()

【與其他模組的關聯】
------------------------------------------------------------
- 繼承 Base_statanalyser，檢定結果傳遞給 ReportGenerator 或下游模組
- 需與 ReportGenerator、主流程等下游結構保持一致

【維護重點】
------------------------------------------------------------
- 新增/修改檢定類型、滯後期、相關性指標、結果格式時，務必同步更新本檔案與 Base_statanalyser
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- scipy.stats、pandas 官方文件
- Base_statanalyser.py、ReportGenerator_statanalyser.py
- 專案 README
"""
import pandas as pd
import numpy as np
from .Base_statanalyser import BaseStatAnalyser
from scipy.stats import pearsonr, spearmanr
from typing import Dict

class CorrelationTest(BaseStatAnalyser):
    """相關性測試模組，評估因子預測能力"""

    def __init__(
            self,
            data: pd.DataFrame,
            predictor_col: str,
            return_col: str,
    ):
        super().__init__(data, predictor_col, return_col)
        self.lags = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 45, 60]

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
        """執行因子-收益率相關性分析"""
        print("\n=== 檢驗：因子預測能力初篩 ===")
        print("1. 檢驗名稱：因子-收益率相關性初篩")
        print("2. 檢驗功能：通過計算因子與未來收益率的相關性，評估因子對資產收益的預測能力，避免後續分析無效因子。")
        print("3. 成功/失敗標準：")
        print("   - |Spearman| < 0.2：因子預測能力微弱，建議更換因子。")
        print("   - |Spearman| ≥ 0.2 且 < 0.4：因子具有輕微預測能力，適合輔助策略。")
        print("   - |Spearman| ≥ 0.4 且 < 0.7：因子具有良好預測能力，可作為主要策略因子。")
        print("   - |Spearman| ≥ 0.7：因子具有優秀預測能力，適合核心交易策略。")
        print("   - 注意：Spearman 相關係數衡量因子與收益率的單調關係，適合非正態數據（如 BTC 收益率的尖峰厚尾特性）。")
        print("           係數絕對值越大，預測能力越強；p 值 < 0.05 表示相關性統計顯著。")
        print("   - Chatterjee 相關系數（ξ）檢測非線性相關性，值域 0-1，不受單調性限制。")

        # 檢查數據完整性
        print(f"\n原始數據行數：{len(self.data)}")
        print(f"因子列（{self.predictor_col}）NaN 數：{self.data[self.predictor_col].isna().sum()}")
        print(f"收益率列（{self.return_col}）NaN 數：{self.data[self.return_col].isna().sum()}")

        # 計算相關性
        correlation_results = {}
        skipped_lags = []
        for lag in self.lags:
            return_series = self.data[self.return_col] if lag == 0 else self.data[self.return_col].shift(-lag)

            temp_df = pd.DataFrame({
                'factor': self.data[self.predictor_col],
                'return': return_series
            }).dropna()

            # 調試輸出
            if lag <= 1:
                # print(f"\n調試：lag={lag}")
                # print(f"temp_df 長度：{len(temp_df)}")
                # print(f"temp_df 頭 5 行：\n{temp_df.head()}")
                # print(f"因子統計：均值={temp_df['factor'].mean():.2f}, 標準差={temp_df['factor'].std():.2f}")
                # print(f"收益率統計：均值={temp_df['return'].mean():.6f}, 標準差={temp_df['return'].std():.6f}")
                pass

            if len(temp_df) < 30:
                print(f"警告：滯後期 {lag} 日的數據不足（{len(temp_df)} 筆，需至少 30 筆），跳過此滯後期。")
                skipped_lags.append(lag)
                continue

            try:
                pearson_corr, pearson_p = pearsonr(temp_df['factor'], temp_df['return'])
                spearman_corr, spearman_p = spearmanr(temp_df['factor'], temp_df['return'])
                
                # 計算 Chatterjee 相關系數
                chatterjee_corr = self._cal_maxCCC(temp_df['factor'].to_numpy(), temp_df['return'].to_numpy())
                
                correlation_results[lag] = {
                    'Pearson': pearson_corr,
                    'Pearson_p': pearson_p,
                    'Spearman': spearman_corr,
                    'Spearman_p': spearman_p,
                    'Chatterjee': chatterjee_corr
                }
            except ValueError as e:
                print(f"警告：滯後期 {lag} 相關性計算失敗（{e}），跳過此滯後期。")
                skipped_lags.append(lag)
                continue

        if skipped_lags:
            print(f"\n已跳過以下滯後期（數據不足或無效）：{skipped_lags}")

        # 展示相關性結果
        print("\n4. 檢驗結果數據：")
        corr_df = pd.DataFrame(correlation_results).T
        corr_df = corr_df.round(4)
        print(corr_df)

        # 尋找最佳滯後期（基於 Spearman）
        best_lag = None
        best_spearman = 0
        for lag, vals in correlation_results.items():
            if abs(vals['Spearman']) > abs(best_spearman):
                best_spearman = vals['Spearman']
                best_lag = lag

        # 尋找最佳 Chatterjee 滯後期
        best_chatterjee_lag = None
        best_chatterjee = 0
        for lag, vals in correlation_results.items():
            if vals['Chatterjee'] > best_chatterjee:
                best_chatterjee = vals['Chatterjee']
                best_chatterjee_lag = lag

        # 相關性衰減分析
        print("\n5. 相關性衰減分析：")
        if correlation_results and 0 in correlation_results:
            print("   - 計算從 lag=0 到 lag=10 的 Spearman 絕對值數量趨勢：")
            decay_spearman = {}
            for lag in range(11):
                if lag in correlation_results:
                    decay_spearman[lag] = abs(correlation_results[lag]['Spearman'])
                    # print(f"     lag={lag}: |Spearman|={decay_spearman[lag]:.4f}")
            decay_point = None
            for lag in range(1, 11):
                if lag in decay_spearman and decay_spearman[lag] < 0.05:
                    decay_point = lag
                    break
            if 5 in decay_spearman:
                spearman_0 = decay_spearman[0]
                spearman_5 = decay_spearman[5]
                if abs(spearman_0) < 1e-10:
                    decay_rate = 0
                    # print("警告：lag=0 的 Spearman 相關係數接近 0，無法計算衰減率，設置為 0。")
                else:
                    decay_rate = (spearman_0 - spearman_5) / spearman_0
                    # print(f"   - lag=0 的 |Spearman| = {spearman_0:.4f}")
                    # print(f"   - lag=5 的 |Spearman| = {spearman_5:.4f}")
                    # print(f"   - 衰減率（lag=0 到 lag=5）：{decay_rate:.2%}")
                    if decay_point:
                        # print(f"   - 相關性在 lag={decay_point} 首次低於 0.05，表明預測能力集中於短期。")
                        pass
                    elif spearman_5 < 0.05 or decay_rate > 0.5:
                        print("   - 相關性迅速衰減，因子預測能力集中於當天或短期滯後（lag=0 或 1）。")
                        print("   - 建議：lag=0 對應當日收益率，優先考慮當日交易策略，基於 lag=0 的因子值。")
                    else:
                        print("   - 相關性衰減緩慢，因子可能具有中期或長期預測能力。")
                        print("   - 建議：考慮趨勢跟蹤策略，關注 lag=5 之後的滯後期（如 lag=10、15）。")
        else:
            print("   - 無法進行衰減分析，lag=0 或後續滯後期的數據不足。")

        # Chatterjee 相關性分析
        print("\n6. Chatterjee 非線性相關性分析：")
        if best_chatterjee_lag is not None:
            print(f"   - 最佳 Chatterjee ξ = {best_chatterjee:.4f} @ lag={best_chatterjee_lag}")
            # 解釋 Chatterjee 係數強度
            if best_chatterjee < 0.1:
                strength = "極弱"
            elif best_chatterjee < 0.2:
                strength = "弱"
            elif best_chatterjee < 0.4:
                strength = "中等"
            elif best_chatterjee < 0.6:
                strength = "強"
            elif best_chatterjee < 0.8:
                strength = "很強"
            else:
                strength = "極強"
            print(f"   - 非線性相關性強度：{strength}")

        # 評估因子有效性
        print("\n7. 檢驗結果判斷：")
        if best_lag is None:
            print("   - 錯誤：無法計算任何滯後期的相關性，數據可能不足或無效。")
            print(f"   - 已跳過滯後期：{skipped_lags if skipped_lags else '無'}")
            print("   - 建議：檢查數據完整性（因子和收益率序列），或更換因子。")
        else:
            spearman_p = correlation_results[best_lag]['Spearman_p']
            if abs(best_spearman) < 0.2:
                strength = "微弱"
                print(f"   - 因子預測能力{strength}（最佳 Spearman = {best_spearman:.4f} @ lag={best_lag}，p 值={spearman_p:.4f}）")
            else:
                strength = "輕微" if abs(best_spearman) < 0.4 else "良好" if abs(best_spearman) < 0.7 else "優秀"
                significance = "顯著" if spearman_p < 0.05 else "不顯著"
                print(f"   - 因子具有{strength}預測能力（最佳 Spearman = {best_spearman:.4f} @ lag={best_lag}，p 值={spearman_p:.4f}，統計{significance}）")

        self.results = {
            'correlation_results': correlation_results,
            'skipped_lags': skipped_lags,
            'best_lag': best_lag,
            'best_spearman': best_spearman,
            'best_chatterjee_lag': best_chatterjee_lag,
            'best_chatterjee': best_chatterjee
        }
        return self.results