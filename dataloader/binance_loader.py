"""
binance_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責連接 Binance API 下載行情數據，支援多種頻率、資料欄位與收益率自動計算，並標準化為統一格式供下游模組使用。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader 或 DataImporter 調用，作為行情數據來源之一
- 下載數據後傳遞給 DataValidator、ReturnCalculator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter] -->|選擇 Binance| B(binance_loader)
    B -->|下載數據| C[DataValidator]
    C -->|驗證清洗| D[ReturnCalculator]
    D -->|計算收益率| E[BacktestEngine/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改 API 參數、欄位、頻率時，請同步更新頂部註解與下游流程
- 若 Binance API 介面有變動，需同步更新本檔案與 base_loader
- 欄位標準化、收益率計算邏輯如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- API 金鑰或頻率錯誤會導致下載失敗
- 欄位缺失或型態錯誤會影響下游驗證與計算
- 時間格式未正確轉換會導致資料錯位

【範例】
------------------------------------------------------------
- loader = BinanceLoader()
  df = loader.load()
- 可於 DataLoader 互動式選擇 Binance 作為行情來源

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader/DataImporter 調用，數據傳遞給 DataValidator、ReturnCalculator、BacktestEngine
- 需與 base_loader 介面保持一致

【參考】
------------------------------------------------------------
- Binance API 官方文件
- base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""

from typing import Optional, Tuple

import pandas as pd
from binance.client import Client

from .base_loader import AbstractDataLoader
from .calculator_loader import ReturnCalculator


class BinanceLoader(AbstractDataLoader):
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """從 Binance API 載入數據"""
        symbol = getattr(self, "symbol", None)
        interval = getattr(self, "interval", None)
        start_date = getattr(self, "start_date", None)
        end_date = getattr(self, "end_date", None)

        if symbol is None:
            symbol = self.get_user_input("請輸入交易對（例如 BTCUSDT", "BTCUSDT")
        if interval is None:
            interval = self.get_frequency("1d")
        if start_date is None or end_date is None:
            start_date, end_date = self.get_date_range()

        try:
            # 使用無憑證的 Client
            client = Client()
            klines = client.get_historical_klines(
                symbol, interval, start_date, end_date
            )
            if not klines:
                self.show_error(f"無法獲取 '{symbol}' 的數據")
                return None, interval

            # 轉換為 DataFrame
            data = pd.DataFrame(
                klines,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume",
                    "ignore",
                ],
            )

            # 重命名欄位為標準格式
            data = data.rename(
                columns={
                    "timestamp": "Time",
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                }
            )

            # 轉換時間格式
            data["Time"] = pd.to_datetime(data["Time"], unit="ms")

            # 選擇需要的欄位
            data = data[["Time", "Open", "High", "Low", "Close", "Volume"]]

            # 轉換為數值類型
            data[["Open", "High", "Low", "Close", "Volume"]] = data[
                ["Open", "High", "Low", "Close", "Volume"]
            ].astype(float)

            # 使用 ReturnCalculator 計算收益率
            calculator = ReturnCalculator(data)
            data = calculator.calculate_returns()

            # 檢查缺失值
            self.display_missing_values(data)
            self.show_success(f"從 Binance 載入 '{symbol}' 成功，行數：{len(data)}")
            # 保存 symbol 供後續使用
            self.symbol = symbol
            return data, interval
        except Exception as err:  # pylint: disable=broad-exception-caught
            self.show_error(str(err))
            return None, interval
