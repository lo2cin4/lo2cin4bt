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

from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from binance.client import Client
from rich.console import Console
from rich.panel import Panel

from .calculator_loader import ReturnCalculator

console = Console()


class BinanceLoader:
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """從 Binance API 載入數據"""
        console.print(
            "[bold #dbac30]請輸入交易對（例如 BTCUSDT，預設 BTCUSDT）：[/bold #dbac30]"
        )
        symbol = input().strip() or "BTCUSDT"
        console.print(
            "[bold #dbac30]輸入價格數據的周期 (例如 1d 代替日線，1h 代表 1小時線，預設 1d)：[/bold #dbac30]"
        )
        interval = input().strip() or "1d"

        # 預設開始日期為 2020-01-01，結束日期為運行當日
        default_start = "2020-01-01"
        default_end = datetime.now().strftime("%Y-%m-%d")

        console.print(
            f"[bold #dbac30]請輸入開始日期（例如 2023-01-01，預設 {default_start}）：[/bold #dbac30]"
        )
        start_date = input().strip() or default_start
        console.print(
            f"[bold #dbac30]請輸入結束日期（例如 2023-12-31，預設 {default_end}）：[/bold #dbac30]"
        )
        end_date = input().strip() or default_end

        try:
            # 使用無憑證的 Client
            client = Client()
            klines = client.get_historical_klines(
                symbol, interval, start_date, end_date
            )
            if not klines:
                console.print(
                    Panel(
                        f"❌ 無法獲取 '{symbol}' 的數據",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
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
            # 缺失值比例 Panel
            missing_msgs = []
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                missing_ratio = data[col].isna().mean()
                missing_msgs.append(f"{col} 缺失值比例：{missing_ratio:.2%}")
            console.print(
                Panel(
                    "\n".join(missing_msgs),
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

            console.print(
                Panel(
                    f"從 Binance 載入 '{symbol}' 成功，行數：{len(data)}",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
            return data, interval
        except Exception as err:  # pylint: disable=broad-exception-caught
            console.print(
                Panel(
                    f"❌ {err}",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return None, interval
