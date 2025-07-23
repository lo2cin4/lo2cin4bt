"""
Binance_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責連接 Binance API 下載行情數據，支援多種頻率、資料欄位與收益率自動計算，並標準化為統一格式供下游模組使用。

【關聯流程與數據流】
------------------------------------------------------------
- 由 DataLoader 或 DataImporter 調用，作為行情數據來源之一
- 下載數據後傳遞給 DataValidator、ReturnCalculator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter] -->|選擇 Binance| B(Binance_loader)
    B -->|下載數據| C[DataValidator]
    C -->|驗證清洗| D[ReturnCalculator]
    D -->|計算收益率| E[BacktestEngine/下游模組]
```

【主控流程細節】
------------------------------------------------------------
- 互動式輸入交易對、時間間隔、起訖日期，支援預設值
- 調用 Binance API 下載 K 線數據，轉為 DataFrame 並標準化欄位
- 自動計算 open/close return 及 logreturn
- 檢查主要欄位缺失值比例，並提示用戶
- 僅保留標準欄位（Time, Open, High, Low, Close, Volume）

【維護與擴充提醒】
------------------------------------------------------------
- 新增/修改 API 參數、欄位、頻率時，請同步更新頂部註解與下游流程
- 若 Binance API 介面有變動，需同步更新本檔案與 Base_loader
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
- 需與 Base_loader 介面保持一致

【維護重點】
------------------------------------------------------------
- 新增/修改 API 參數、欄位、標準化邏輯時，務必同步更新本檔案與 Base_loader
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- Binance API 官方文件
- Base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""
import pandas as pd
import numpy as np
from binance.client import Client
from datetime import datetime


class BinanceLoader:
    def load(self):
        """從 Binance API 載入數據"""
        symbol = input("請輸入交易對（例如 BTCUSDT，預設 BTCUSDT）：").strip() or "BTCUSDT"
        interval = input("請輸入時間間隔（例如 1d, 1h，預設 1d）：").strip() or "1d"
        
        # 預設開始日期為 2020-01-01，結束日期為運行當日
        default_start = "2020-01-01"
        default_end = datetime.now().strftime("%Y-%m-%d")
        
        start_date = input(f"請輸入開始日期（例如 2023-01-01，預設 {default_start}）：").strip() or default_start
        end_date = input(f"請輸入結束日期（例如 2023-12-31，預設 {default_end}）：").strip() or default_end

        try:
            # 使用無憑證的 Client
            client = Client()
            klines = client.get_historical_klines(symbol, interval, start_date, end_date)
            if not klines:
                print(f"錯誤：無法獲取 '{symbol}' 的數據")
                return None, interval

            # 轉換為 DataFrame
            data = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])
            
            # 重命名欄位為標準格式
            data = data.rename(columns={
                'timestamp': 'Time',
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            })
            
            # 轉換時間格式
            data['Time'] = pd.to_datetime(data['Time'], unit='ms')
            
            # 選擇需要的欄位
            data = data[['Time', 'Open', 'High', 'Low', 'Close', 'Volume']]
            
            # 轉換為數值類型
            data[['Open', 'High', 'Low', 'Close', 'Volume']] = data[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)

            # 計算收益率
            data['open_return'] = data['Open'].pct_change().fillna(0)
            data['close_return'] = data['Close'].pct_change().fillna(0)
            data['open_logreturn'] = np.log(data['Open'] / data['Open'].shift(1)).fillna(0)
            data['close_logreturn'] = np.log(data['Close'] / data['Close'].shift(1)).fillna(0)

            # 檢查缺失值
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                missing_ratio = data[col].isna().mean()
                print(f"{col} 缺失值比例：{missing_ratio:.2%}")

            print(f"從 Binance 載入 '{symbol}' 成功，行數：{len(data)}")
            print("已計算收益率：open_return, close_return, open_logreturn, close_logreturn")
            return data, interval
        except Exception as e:
            print(f"錯誤：{e}")
            return None, interval