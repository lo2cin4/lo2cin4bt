"""
Base_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析模組的抽象基底類，統一規範所有統計檢定/分析器（如自相關、相關性、分布、定態性等）的介面與繼承結構，確保分析流程一致、易於擴充。

【流程與數據流】
------------------------------------------------------------
- 由各統計分析子類（AutocorrelationTest、CorrelationTest、DistributionTest、StationarityTest、SeasonalAnalysis 等）繼承
- 提供標準化的統計檢定、分析、報表產生流程，分析結果傳遞給 ReportGenerator 或下游模組

```mermaid
flowchart TD
    A[Base_statanalyser] -->|繼承| B[各統計分析子類]
    B -->|分析結果| C[ReportGenerator/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增統計分析子類時，請務必繼承本類並實作必要方法
- 若介面、欄位、分析流程有變動，需同步更新所有子類與本檔案頂部註解
- 分析結果格式如有調整，請同步通知協作者
- 新增/修改介面、欄位、分析流程、結果格式時，務必同步更新所有子類與本檔案
- 欄位名稱、型態需與下游模組協調一致

【常見易錯點】
------------------------------------------------------------
- 子類未正確實作抽象方法會導致分析失敗
- 預測因子、收益率欄位缺失或型態錯誤會導致分析異常
- 統計結果格式不符會影響下游報表或流程

【錯誤處理】
------------------------------------------------------------
- 數據結構驗證失敗時提供詳細錯誤訊息
- 欄位缺失時自動提示用戶選擇
- 分析結果異常時提供診斷建議

【範例】
------------------------------------------------------------
- class MyTest(BaseStatAnalyser):
      def run_test(self): ...
- predictor_col, df = BaseStatAnalyser.get_user_config(df)

【與其他模組的關聯】
------------------------------------------------------------
- 由 statanalyser 目錄下各統計分析子類繼承，分析結果傳遞給 ReportGenerator 或下游模組
- 需與 ReportGenerator、主流程等下游結構保持一致

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，定義基本介面
- v1.1: 新增用戶互動式預測因子選擇
- v1.2: 支援 Rich Panel 顯示和步驟跟蹤

【參考】
------------------------------------------------------------
- pandas、numpy、abc 官方文件
- statanalyser/ReportGenerator_statanalyser.py
- 專案 README
"""

from abc import ABC, abstractmethod
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel

console = Console()


class BaseStatAnalyser(ABC):
    """統計分析基類，處理數據輸入與公共方法"""

    @staticmethod
    def select_predictor_factor(data, default_factor=None, for_diff=False):
        """
        讓用戶選擇要用於統計分析的預測因子
        """
        available_factors = [
            col
            for col in data.columns
            if col
            not in [
                "Time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "open_return",
                "close_return",
                "open_logreturn",
                "close_logreturn",
            ]
        ]
        if not available_factors:
            raise ValueError("數據中沒有可用的預測因子")
        if default_factor and default_factor in available_factors:
            default = default_factor
        else:
            default = available_factors[0]
        # 詳細新手說明
        detail = (
            "🟢 選擇用於統計分析的預測因子\n"
            "🔴 收益率相關性檢驗[自動]\n"
            "🔴 ADF/KPSS 平穩性檢驗[自動]\n"
            "🔴 ACF/PACF 自相關性檢驗[自動]\n"
            "🔴 生成 ACF 或 PACF 互動圖片\n"
            "🔴 統計分佈檢驗[自動]\n"
            "🔴 季節性檢驗[自動]\n\n"
            "[bold #dbac30]說明[/bold #dbac30]\n"
            "選擇要用作統計分析的因子，可以是原因子，也可以是差分後的因子。\n"
            "統計分析將協助你尋找預測因子與收益率的關係，有助於建立策略。\n"
            "系統會自動對齊時間並進行後續統計檢驗，協助你判斷該因子是否具備預測能力。\n"
            "然而統計分析的建議僅作參考，開發量化策略時仍然需具備交易邏輯，才能避免數據發掘而導致的過度擬合。"
        )
        msg = f"[bold #dbac30]請選擇要用於統計分析的預測因子（可選: {available_factors}, 預設 {default}）：[/bold #dbac30]"
        panel_content = detail
        while True:
            console.print(
                Panel(
                    panel_content,
                    title="[bold #dbac30]🔬 統計分析 StatAnalyser 步驟：選擇預測因子[/bold #dbac30]",
                    border_style="#dbac30",
                )
            )
            console.print(msg)
            selected_factor = input().strip() or default
            if selected_factor not in available_factors:
                console.print(
                    Panel(
                        f"輸入錯誤，請重新輸入（可選: {available_factors}，預設 {default}）",
                        title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                continue
            console.print(
                Panel(
                    f"已選擇預測因子: {selected_factor}",
                    title="[bold #8f1511]🔬 統計分析 StatAnalyser[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
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
            print(
                f"錯誤：'{predictor_col}' 不在數據欄位中，可用欄位：{list(df.columns)}"
            )

        # 差分選擇
        diff_choice = (
            input("\n是否對預測因子進行差分計算(除前值或減前值)？(y/n)：")
            .strip()
            .lower()
        )
        if diff_choice == "y":
            has_zeros = (df[predictor_col] == 0).any()
            if has_zeros:
                print(
                    f"警告：因子 '{predictor_col}' 包含零值，僅支援絕對差分 (t-1) - t。"
                )
                diff_type = "absolute"
            else:
                diff_type = input(
                    "\n請選擇差分方式：\n  1：絕對差分 (t-1) - t\n  2：相對差分 (t-1) / t\n輸入選擇（1 或 2）："
                ).strip()
                diff_type = "absolute" if diff_type == "1" else "relative"

            diff_col = (
                f"{predictor_col}_{'abs' if diff_type == 'absolute' else 'rel'}_diff"
            )
            df[diff_col] = (
                df[predictor_col].diff()
                if diff_type == "absolute"
                else df[predictor_col].shift(1) / df[predictor_col]
            )
            df[diff_col] = df[diff_col].fillna(0).replace([np.inf, -np.inf], 0)
            predictor_col = diff_col
            print(f"已生成差分欄位：{diff_col}")

        # 收益率列名
        valid_returns = [
            "close_return",
            "close_logreturn",
            "open_return",
            "open_logreturn",
        ]
        while True:
            return_col = input(
                "請輸入收益率列名（close_return/close_logreturn/open_return/open_logreturn）："
            ).strip()
            if return_col in valid_returns and return_col in df.columns:
                break
            print(
                f"錯誤：'{return_col}' 無效或不在數據中，可用欄位：{list(df.columns)}"
            )

        return predictor_col, df

    def __init__(self, data: pd.DataFrame, predictor_col: str, return_col: str):
        self.data = self._validate_data(data, predictor_col, return_col)
        self.predictor_col = predictor_col
        self.return_col = return_col
        self.results = {}

    def _validate_data(
        self, data: pd.DataFrame, predictor_col: str, return_col: str
    ) -> pd.DataFrame:
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected pandas.DataFrame, got {type(data)}")

        df = data.copy()
        if not all(isinstance(col, str) for col in df.columns):
            raise TypeError("列名必須為字符串")

        if "Time" in df.columns:
            try:
                df["Time"] = pd.to_datetime(df["Time"])
                df.set_index("Time", inplace=True)
                df = df.infer_objects()  # 消除 dtype inference 警告
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

    def get_results(self) -> Dict:
        """獲取分析結果"""
        return self.results
