"""
coinbase_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責連接 Coinbase API 下載行情數據，支援多種頻率、資料欄位與收益率自動計算，並標準化為統一格式供下游模組使用。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader 或 DataImporter 調用，作為行情數據來源之一
- 下載數據後傳遞給 DataValidator、ReturnCalculator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter] -->|選擇 Coinbase| B(coinbase_loader)
    B -->|下載數據| C[DataValidator]
    C -->|驗證清洗| D[ReturnCalculator]
    D -->|計算收益率| E[BacktestEngine/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改 API 參數、欄位、頻率時，請同步更新頂部註解與下游流程
- 若 Coinbase API 介面有變動，需同步更新本檔案與 base_loader
- 欄位標準化、收益率計算邏輯如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- API 金鑰或頻率錯誤會導致下載失敗
- 欄位缺失或型態錯誤會影響下游驗證與計算
- 時間格式未正確轉換會導致資料錯位

【範例】
------------------------------------------------------------
- loader = CoinbaseLoader()
  df = loader.load()
- 可於 DataLoader 互動式選擇 Coinbase 作為行情來源

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader/DataImporter 調用，數據傳遞給 DataValidator、ReturnCalculator、BacktestEngine
- 需與 base_loader 介面保持一致

【參考】
------------------------------------------------------------
- Coinbase API 官方文件
- base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import requests

from .base_loader import AbstractDataLoader
from .calculator_loader import ReturnCalculator


class CoinbaseLoader(AbstractDataLoader):
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """從 Coinbase API 載入數據"""
        symbol = getattr(self, "symbol", None)
        if symbol is None:
            symbol = self.get_user_input("請輸入交易對（例如 BTC-USD", "BTC-USD")

        # Coinbase 支援的時間間隔
        interval_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "1d": 86400,
        }

        interval_input = getattr(self, "interval", None)
        if interval_input is None:
            interval_input = self.get_user_input(
                "輸入價格數據的周期 (1m, 5m, 15m, 1h, 6h, 1d", "1d"
            )

        if interval_input not in interval_map:
            self.show_warning(f"不支援的時間周期 '{interval_input}'，將使用預設值 1d")
            interval_input = "1d"

        granularity = interval_map[interval_input]

        # 獲取日期範圍
        start_date_str = getattr(self, "start_date", None)
        end_date_str = getattr(self, "end_date", None)

        if start_date_str is None or end_date_str is None:
            start_date_str, end_date_str = self.get_date_range()

        try:
            # 轉換日期為 timestamp
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

            # Coinbase API 有限制，每次請求最多 300 個數據點
            # 需要分批請求數據
            all_data = []

            # 計算每批的時間範圍
            max_candles = 300
            seconds_per_candle = granularity
            batch_seconds = max_candles * seconds_per_candle

            current_start = start_date

            self.show_info(f"正在從 Coinbase 下載 {symbol} 數據...")

            while current_start < end_date:
                current_end = min(
                    current_start + timedelta(seconds=batch_seconds), end_date
                )

                # Coinbase Exchange API endpoint (public, no auth required)
                # Note: api.exchange.coinbase.com is the current public endpoint
                # The old api.pro.coinbase.com has been deprecated
                url = f"https://api.exchange.coinbase.com/products/{symbol}/candles"
                params = {
                    "start": current_start.isoformat(),
                    "end": current_end.isoformat(),
                    "granularity": granularity,
                }

                response = requests.get(url, params=params)

                if response.status_code != 200:
                    self.show_error(
                        f"API 請求失敗：{response.status_code} - {response.text}"
                    )
                    return None, interval_input

                candles = response.json()

                if candles:
                    all_data.extend(candles)

                # 移動到下一批
                current_start = current_end

            if not all_data:
                self.show_error(f"無法獲取 '{symbol}' 的數據")
                return None, interval_input

            # 轉換為 DataFrame
            # Coinbase API 返回格式: [timestamp, low, high, open, close, volume]
            data = pd.DataFrame(
                all_data,
                columns=["timestamp", "low", "high", "open", "close", "volume"],
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

            # 轉換時間格式 (Coinbase 返回 Unix timestamp)
            data["Time"] = pd.to_datetime(data["Time"], unit="s")

            # 重新排序欄位
            data = data[["Time", "Open", "High", "Low", "Close", "Volume"]]

            # 按時間排序
            data = data.sort_values("Time").reset_index(drop=True)

            # 轉換為數值類型
            numeric_columns = ["Open", "High", "Low", "Close", "Volume"]
            data[numeric_columns] = data[numeric_columns].astype(float)

            # 使用 ReturnCalculator 計算收益率
            calculator = ReturnCalculator(data)
            data = calculator.calculate_returns()

            # 檢查缺失值
            self.display_missing_values(data)
            self.show_success(f"從 Coinbase 載入 '{symbol}' 成功，行數：{len(data)}")
            # 保存 symbol 供後續使用
            self.symbol = symbol
            return data, interval_input

        except Exception as e:
            self.show_error(str(e))
            return None, interval_input
