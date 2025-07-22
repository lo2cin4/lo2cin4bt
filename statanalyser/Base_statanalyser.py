"""
Base_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組的抽象基底類，統一規範所有統計檢定/分析器（如自相關、相關性、分布、定態性等）的介面與繼承結構，確保分析流程一致、易於擴充。

【關聯流程與數據流】
------------------------------------------------------------
- 由各統計分析子類（AutocorrelationTest、CorrelationTest、DistributionTest、StationarityTest、SeasonalAnalysis 等）繼承
- 提供標準化的統計檢定、分析、報表產生流程，分析結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[Base_statanalyser] -->|繼承| B[各統計分析子類]
    B -->|分析結果| C[ReportGenerator/下游模組]
```

【主控流程細節】
------------------------------------------------------------
- 定義 run_test、summary、analyze 等抽象方法，所有子類必須實作
- 提供用戶互動式選擇預測因子、差分選項、收益率欄位等
- 驗證數據結構、欄位型態與缺失值，確保分析前數據合格
- 分析結果以字典或標準格式返回，便於下游報表生成

【維護與擴充提醒】
------------------------------------------------------------
- 新增統計分析子類時，請務必繼承本類並實作必要方法
- 若介面、欄位、分析流程有變動，需同步更新所有子類與本檔案頂部註解
- 分析結果格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 子類未正確實作抽象方法會導致分析失敗
- 預測因子、收益率欄位缺失或型態錯誤會導致分析異常
- 統計結果格式不符會影響下游報表或流程

【範例】
------------------------------------------------------------
- class MyTest(BaseStatAnalyser):
      def run_test(self): ...
- predictor_col, df = BaseStatAnalyser.get_user_config(df)

【與其他模組的關聯】
------------------------------------------------------------
- 由 statanalyser 目錄下各統計分析子類繼承，分析結果傳遞給 ReportGenerator 或下游模組
- 需與 ReportGenerator、主流程等下游結構保持一致

【維護重點】
------------------------------------------------------------
- 新增/修改介面、欄位、分析流程、結果格式時，務必同步更新所有子類與本檔案
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- pandas、numpy、abc 官方文件
- statanalyser/ReportGenerator_statanalyser.py
- 專案 README
"""
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Tuple,Dict

class BaseStatAnalyser(ABC):
    """統計分析基類，處理數據輸入與公共方法"""

    @staticmethod
    def select_predictor_factor(data, default_factor=None):
        """
        讓用戶選擇要用於統計分析的預測因子
        """
        available_factors = [col for col in data.columns if col not in ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'open_return', 'close_return', 'open_logreturn', 'close_logreturn']]
        if not available_factors:
            raise ValueError("數據中沒有可用的預測因子")
        if default_factor and default_factor in available_factors:
            default = default_factor
        else:
            default = available_factors[0]
        print(f"\n可用預測因子欄位：{available_factors}")
        while True:
            selected_factor = input(f"請選擇要用於統計分析的預測因子（預設 {default}）：").strip() or default
            if selected_factor not in available_factors:
                print(f"輸入錯誤，請重新輸入（可選: {available_factors}，預設 {default}）")
                continue
            print(f"已選擇預測因子: {selected_factor}")
            return selected_factor

    @classmethod
    def get_user_config(cls, data: pd.DataFrame) -> Tuple[str, pd.DataFrame]:
        """
        交互式獲取用戶配置參數，包括差分計算選項

        Args:
            data: 輸入DataFrame，用於驗證欄位存在

        Returns:
            Tuple containing (predictor_col, updated_data)
        """
        df = data.copy()
        print(f"可用列名：{list(df.columns)}")

        # 預測因子列名
        while True:
            predictor_col = input("請輸入預測因子列名：").strip()
            if predictor_col in df.columns:
                break
            print(f"錯誤：'{predictor_col}' 不在數據欄位中，可用欄位：{list(df.columns)}")

        # 差分選擇
        diff_choice = input("\n是否對預測因子進行差分計算(除前值或減前值)？(y/n)：").strip().lower()
        if diff_choice == 'y':
            has_zeros = (df[predictor_col] == 0).any()
            if has_zeros:
                print(f"警告：因子 '{predictor_col}' 包含零值，僅支援絕對差分 (t-1) - t。")
                diff_type = 'absolute'
            else:
                diff_type = input(
                    "\n請選擇差分方式：\n  1：絕對差分 (t-1) - t\n  2：相對差分 (t-1) / t\n輸入選擇（1 或 2）：").strip()
                diff_type = 'absolute' if diff_type == '1' else 'relative'

            diff_col = f"{predictor_col}_{'abs' if diff_type == 'absolute' else 'rel'}_diff"
            df[diff_col] = df[predictor_col].diff() if diff_type == 'absolute' else df[predictor_col].shift(1) / df[predictor_col]
            df[diff_col] = df[diff_col].fillna(0).replace([np.inf, -np.inf], 0)
            predictor_col = diff_col
            print(f"已生成差分欄位：{diff_col}")

        # 收益率列名
        valid_returns = ["close_return", "close_logreturn", "open_return", "open_logreturn"]
        while True:
            return_col = input("請輸入收益率列名（close_return/close_logreturn/open_return/open_logreturn）：").strip()
            if return_col in valid_returns and return_col in df.columns:
                break
            print(f"錯誤：'{return_col}' 無效或不在數據中，可用欄位：{list(df.columns)}")

        return predictor_col, df

    def __init__(self, data: pd.DataFrame, predictor_col: str, return_col: str):
        self.data = self._validate_data(data, predictor_col, return_col)
        self.predictor_col = predictor_col
        self.return_col = return_col
        self.results = {}

    def _validate_data(self, data: pd.DataFrame, predictor_col: str, return_col: str) -> pd.DataFrame:
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected pandas.DataFrame, got {type(data)}")

        df = data.copy()
        if not all(isinstance(col, str) for col in df.columns):
            raise TypeError("列名必須為字符串")

        if "Time" in df.columns:
            try:
                df["Time"] = pd.to_datetime(df["Time"])
                df.set_index("Time", inplace=True)
            except ValueError:
                raise ValueError("無法將 'Time' 欄位轉換為 DatetimeIndex")
        elif not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("索引必須為 DatetimeIndex 或包含 'Time' 欄位")

        if predictor_col not in df.columns or return_col not in df.columns:
            raise ValueError(f"缺少欄位：{predictor_col} 或 {return_col}")

        if np.isnan(df[[predictor_col, return_col]].values).any():
            raise ValueError("數據中包含 NaN 值")
        if np.isinf(df[[predictor_col, return_col]].values).any():
            raise ValueError("數據中包含無限值。")

        return df

    @abstractmethod
    def analyze(self) -> Dict:
        """執行分析，返回結果字典"""
        pass

    def get_results(self) -> Dict:
        """獲取分析結果"""
        return self.results