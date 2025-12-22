"""
calculator_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據計算與衍生欄位模組，負責對行情數據進行批次技術指標、收益率、衍生欄位等計算，並確保數據結構與下游模組一致。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader、DataImporter 或 BacktestEngine 調用，對原始數據進行批次計算
- 計算後數據傳遞給 Predictor、Validator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter/BacktestEngine] -->|調用| B(calculator_loader)
    B -->|計算/衍生欄位| C[數據DataFrame]
    C -->|傳遞| D[Predictor/Validator/BacktestEngine]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改計算類型、欄位時，請同步更新頂部註解與下游流程
- 若計算邏輯、欄位結構有變動，需同步更新本檔案與下游模組
- 技術指標公式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 欄位名稱拼寫錯誤或型態不符會導致計算失敗
- 批次運算未同步更新會影響下游流程
- 輸入數據缺失 open/close 欄位會導致收益率計算失敗

【範例】
------------------------------------------------------------
- calculator = ReturnCalculator(df)
  df = calculator.calculate_returns()

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader、DataImporter、BacktestEngine 調用，數據傳遞給 Predictor、Validator、BacktestEngine
- 需與下游欄位結構保持一致

【參考】
------------------------------------------------------------
- numpy、numba 官方文件
- base_loader.py、DataValidator、Predictor_loader
- 專案 README
"""

import numpy as np
import pandas as pd
from numba import jit

from utils import show_error, show_success, get_console

console = get_console()


class ReturnCalculator:
    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data.copy()

    def calculate_returns(self) -> pd.DataFrame:
        """計算 open_return, close_return, open_logreturn, close_logreturn"""
        # 同時允許 'Open'/'Close' 或 'open'/'close' 欄位
        open_col = None
        close_col = None
        for cand in ["Open", "open"]:
            if cand in self.data.columns:
                open_col = cand
                break
        for cand in ["Close", "close"]:
            if cand in self.data.columns:
                close_col = cand
                break
        if open_col is None or close_col is None:
            show_error("DATALOADER", "錯誤：缺少 open/Open 或 close/Close 欄位，無法計算收益率")
            return self.data

        # 使用 numpy 向量化計算
        open_prices = self.data[open_col].to_numpy()
        close_prices = self.data[close_col].to_numpy()

        # 計算簡單收益率
        self.data["open_return"] = self._calc_simple_return(open_prices)
        self.data["close_return"] = self._calc_simple_return(close_prices)

        # 計算對數收益率
        self.data["open_logreturn"] = self._calc_log_return(open_prices)
        self.data["close_logreturn"] = self._calc_log_return(close_prices)
        show_success("DATALOADER", "已計算收益率：open_return, close_return, open_logreturn, close_logreturn")
        return self.data

    @staticmethod
    @jit(nopython=True)
    def _calc_simple_return(prices: np.ndarray) -> np.ndarray:
        """使用 numba 加速簡單收益率計算"""
        returns = np.zeros(len(prices))
        for i in range(1, len(prices)):
            if prices[i - 1] != 0:
                returns[i] = (prices[i] - prices[i - 1]) / prices[i - 1]
        return returns

    @staticmethod
    @jit(nopython=True)
    def _calc_log_return(prices: np.ndarray) -> np.ndarray:
        """使用 numba 加速對數收益率計算"""
        returns = np.zeros(len(prices))
        for i in range(1, len(prices)):
            if prices[i] > 0 and prices[i - 1] > 0:
                returns[i] = np.log(prices[i] / prices[i - 1])
        return returns
