"""
DataImporter_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的數據導入工具，負責從外部來源載入回測所需的行情數據，支援多種格式與來源，並確保數據結構與回測引擎相容。
- 整合 dataloader 模組，提供統一的數據載入接口
- 自動檢測數據頻率（日線、週線、月線、小時線、分鐘線等）
- 標準化數據格式，確保與回測引擎相容
- 提供數據驗證與錯誤處理機制

【流程與數據流】
------------------------------------------------------------
- 由 BaseBacktester 調用，載入回測所需的行情數據
- 載入數據後傳遞給 BacktestEngine 進行回測

```mermaid
flowchart TD
    A[BaseBacktester] -->|調用| B[DataImporter]
    B -->|載入數據| C[dataloader.base_loader]
    C -->|原始數據| D[標準化處理]
    D -->|頻率檢測| E[detect_frequency]
    D -->|格式驗證| F[數據驗證]
    E & F -->|標準化數據| G[BacktestEngine]
```

【支援的數據格式】
------------------------------------------------------------
- 時間序列數據：包含 Time, Open, High, Low, Close, Volume
- 預測因子：支援多種技術指標與衍生數據
- 頻率支援：日線、週線、月線、小時線、分鐘線、15分鐘線、4小時線等
- 來源支援：檔案、API、資料庫等多種數據來源

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改數據來源、格式時，請同步更新頂部註解與下游流程
- 若數據結構有變動，需同步更新本檔案與 BacktestEngine
- 數據格式如有調整，請同步通知協作者
- 頻率檢測邏輯需要支援新的時間間隔
- 數據驗證規則需要與回測引擎需求保持一致

【常見易錯點】
------------------------------------------------------------
- 數據來源錯誤或格式不符會導致載入失敗
- 欄位缺失或型態錯誤會影響回測執行
- 數據對齊問題會導致信號產生異常
- 頻率檢測不準確會影響回測結果
- 數據驗證不完整會導致後續處理錯誤

【錯誤處理】
------------------------------------------------------------
- 數據載入失敗時提供詳細錯誤信息
- 格式不匹配時提供修正建議
- 頻率檢測失敗時提供手動設定選項
- 數據缺失時提供插值或跳過選項

【範例】
------------------------------------------------------------
- 創建導入器：importer = DataImporter()
- 載入並標準化數據：data, freq = importer.load_and_standardize_data(Backtest_id)
- 檢測數據頻率：freq = importer._detect_frequency()

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseBacktester 調用，數據傳遞給 BacktestEngine
- 需與 BacktestEngine 的數據結構保持一致
- 依賴 dataloader.base_loader 進行實際數據載入
- 與 TradeRecordExporter 配合記錄數據來源信息

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本數據載入功能
- v1.1: 新增頻率自動檢測
- v1.2: 完善數據標準化處理
- v2.0: 整合 dataloader 模組
- v2.1: 新增數據驗證與錯誤處理
- v2.2: 優化頻率檢測算法

【參考】
------------------------------------------------------------
- pandas 官方文件：https://pandas.pydata.org/
- dataloader 模組文檔
- Base_backtester.py、BacktestEngine_backtester.py
- 專案 README
"""

import logging
from typing import Tuple, Union

import numpy as np
import pandas as pd

try:
    from dataloader.base_loader import DataLoader
except ImportError as e:
    logging.error(f"無法導入 DataLoader: {str(e)}")
    raise ImportError("請確認 dataloader.base_loader 模組存在並可導入。")

# 移除硬編碼的日誌路徑設置，使用 main.py 中的日誌配置


class DataImporter:
    """從 dataloader 載入數據，標準化格式，檢測頻率。

    Attributes:
        data (pd.DataFrame): 標準化數據，包含 Time, Open, High, Low, Close, Volume, predictors。
        frequency (str): 自動檢測的數據頻率（day, week, month, hour, minute, 15m, 4h 等）。

    Example:
        >>> importer = DataImporter_backtester()
        >>> data, freq = importer.load_and_standardize_data()
        >>> print(data.head())
        >>> print(f"Frequency: {freq}")
    """

    def __init__(self) -> None:
        self.data: pd.DataFrame | None = None
        self.frequency: str | None = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def _load_data_from_loader(self) -> Union[pd.DataFrame, str]:
        """從 DataLoader 載入數據"""
        loader = DataLoader()
        result = loader.load_data()
        if isinstance(result, str) and result == "__SKIP_STATANALYSER__":
            self.data = loader.data
            self.frequency = loader.frequency
            return "__SKIP_STATANALYSER__"
        else:
            self.data = result
            self.frequency = loader.frequency
            return result

    def _validate_data(self) -> None:
        """驗證數據的有效性"""
        if self.data is None or (
            isinstance(self.data, pd.DataFrame) and self.data.empty
        ):
            raise ValueError("數據載入失敗或數據為空")

    def _validate_required_columns(self) -> None:
        """驗證必要欄位是否存在"""
        if self.data is None:
            raise ValueError("數據未載入，無法驗證欄位")

        required_cols = ["time", "open", "high", "low", "close", "volume"]
        missing_cols = [
            col
            for col in required_cols
            if col.lower() not in [c.lower() for c in self.data.columns]
        ]
        if missing_cols:
            raise ValueError(f"缺少必要欄位: {missing_cols}")

    def _standardize_column_names(self) -> None:
        """標準化欄位名稱"""
        if self.data is None:
            raise ValueError("數據未載入，無法標準化欄位名稱")

        column_mapping = {}
        new_columns = []

        for col in self.data.columns:
            col_lower = col.lower()
            if col_lower == "time":
                column_mapping[col] = "Time"
                new_columns.append("Time")
            elif col_lower == "open":
                column_mapping[col] = "Open"
                new_columns.append("Open")
            elif col_lower == "high":
                column_mapping[col] = "High"
                new_columns.append("High")
            elif col_lower == "low":
                column_mapping[col] = "Low"
                new_columns.append("Low")
            elif col_lower == "close":
                column_mapping[col] = "Close"
                new_columns.append("Close")
            elif col_lower == "volume":
                column_mapping[col] = "Volume"
                new_columns.append("Volume")
            else:
                # 保留預測因子欄位的原始大小寫
                new_columns.append(col)

        # 應用欄位重命名
        self.data = self.data.rename(columns=column_mapping)

    def _process_time_column(self) -> None:
        """處理時間欄位"""
        if self.data is not None:
            self.data["Time"] = pd.to_datetime(self.data["Time"])
            if self.data["Time"].duplicated().any():
                raise ValueError("Time 欄位包含重複值")

    def load_and_standardize_data(
        self, Backtest_id: str = "unknown"
    ) -> Union[Tuple[pd.DataFrame, str], Tuple[str, str]]:
        """載入並標準化數據，自動檢測頻率。

        Args:
            Backtest_id (str): 回測唯一 ID，用於日誌記錄。

        Returns:
            tuple: (pd.DataFrame, str) - 標準化數據與自動檢測頻率。

        Raises:
            ValueError: 數據載入失敗或格式不正確。
            ImportError: DataLoader 模組無法導入。
        """
        try:
            # 載入數據
            result = self._load_data_from_loader()
            if result == "__SKIP_STATANALYSER__":
                return "__SKIP_STATANALYSER__", str(self.frequency or "unknown")

            # 驗證數據
            self._validate_data()
            self._validate_required_columns()

            # 標準化處理
            self._standardize_column_names()
            self._process_time_column()

            # 檢測頻率
            self.frequency = self._detect_frequency()

            return self.data, str(self.frequency or "unknown")

        except Exception as e:
            self.logger.error(
                f"數據載入或標準化失敗: {e}", extra={"Backtest_id": Backtest_id}
            )
            raise

    def _detect_frequency(self) -> str:
        """
        自動檢測數據頻率，支援非標準頻率

        Returns:
            str: 檢測到的頻率（day, week, month, hour, minute, 15m, 4h 等）

        Raises:
            ValueError: 當數據未載入或數據過少時
        """
        try:
            if self.data is None:
                raise ValueError("數據未載入")

            # 取前 100 筆數據（或全部）計算時間差
            time_diffs = self.data["Time"].diff().dropna().dt.total_seconds()
            if len(time_diffs) == 0:
                raise ValueError("無法計算時間差，數據過少")
            median_diff = np.median(time_diffs)

            # 定義頻率映射（秒）
            freq_map = {
                60: "minute",
                60 * 15: "15m",
                60 * 60: "hour",
                60 * 60 * 4: "4h",
                60 * 60 * 24: "day",
                60 * 60 * 24 * 7: "week",
                60 * 60 * 24 * 30: "month",
            }

            # 尋找最接近的頻率
            closest_diff = min(freq_map.keys(), key=lambda x: abs(x - median_diff))
            return freq_map.get(closest_diff, "custom")

        except Exception as e:
            self.logger.error(f"頻率檢測失敗: {e}", extra={"Backtest_id": "unknown"})
            return "custom"
