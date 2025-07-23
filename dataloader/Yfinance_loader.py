"""
Yfinance_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責連接 Yahoo Finance API 下載行情數據，支援多種頻率、資料欄位自動標準化，並確保數據結構與下游模組一致。

【關聯流程與數據流】
------------------------------------------------------------
- 由 DataLoader 或 DataImporter 調用，作為行情數據來源之一
- 下載數據後傳遞給 DataValidator、ReturnCalculator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter] -->|選擇 Yahoo Finance| B(Yfinance_loader)
    B -->|下載數據| C[DataValidator]
    C -->|驗證清洗| D[ReturnCalculator]
    D -->|計算收益率| E[BacktestEngine/下游模組]
```

【主控流程細節】
------------------------------------------------------------
- 互動式輸入股票/指數代碼、起訖日期，支援多種頻率
- 調用 yfinance 下載行情數據，轉為 DataFrame 並標準化欄位
- 自動檢查並補齊必要欄位（Time, Open, High, Low, Close, Volume）
- 檢查主要欄位型態與缺失值，並提示用戶
- 僅保留標準欄位，確保與下游一致

【維護與擴充提醒】
------------------------------------------------------------
- 新增/修改支援頻率、欄位時，請同步更新頂部註解與下游流程
- 若 yfinance API 介面有變動，需同步更新本檔案與 Base_loader
- 欄位標準化、資料清洗邏輯如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 股票代碼或日期範圍錯誤會導致下載失敗
- 欄位缺失或型態錯誤會影響下游驗證與計算
- 多級索引未正確展平會導致資料結構異常

【範例】
------------------------------------------------------------
- loader = YahooFinanceLoader()
  df = loader.load()
- 可於 DataLoader 互動式選擇 Yahoo Finance 作為行情來源

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader/DataImporter 調用，數據傳遞給 DataValidator、ReturnCalculator、BacktestEngine
- 需與 Base_loader 介面保持一致

【維護重點】
------------------------------------------------------------
- 新增/修改支援頻率、欄位、標準化邏輯時，務必同步更新本檔案與 Base_loader
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- yfinance 官方文件
- Base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""
import pandas as pd
import yfinance as yf
import logging


class YahooFinanceLoader:
    def __init__(self):
        self.logger = logging.getLogger("lo2cin4bt.dataloader.YahooFinanceLoader")

    def load(self):
        """從 Yahoo Finance 載入數據，參考 vectorbt 的標準化處理"""
        from datetime import datetime
        default_ticker = "TSLA"
        default_start = "2020-01-01"
        default_end = datetime.now().strftime("%Y-%m-%d")
        ticker = input(f"請輸入股票或指數代碼（例如 TSLA，預設 {default_ticker}）：").strip() or default_ticker
        frequency = input("請輸入時間間隔（例如 1d, 1h，預設 1d）：").strip() or "1d"
        start_date = input(f"請輸入開始日期（例如 2020-01-01，預設 {default_start}）：").strip() or default_start
        end_date = input(f"請輸入結束日期（例如 2024-12-31，預設 {default_end}）：").strip() or default_end

        try:
            # 下載數據，設置參數模仿 vectorbt
            data = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                progress=False
            )

            # 檢查數據是否為 DataFrame 並非空
            if not isinstance(data, pd.DataFrame) or data.empty:
                self.logger.error(f"無法獲取 '{ticker}' 的數據，可能股票代碼無效或日期範圍錯誤")
                print(f"錯誤：無法獲取 '{ticker}' 的數據，可能股票代碼無效或日期範圍錯誤")
                return None, frequency

            # 處理可能的數據結構
            if isinstance(data, pd.Series):
                # 單股票返回 Series，轉為 DataFrame
                data = pd.DataFrame({
                    'Close': data
                }).reset_index()
            elif isinstance(data, pd.DataFrame):
                # 展平多級索引（如果存在）
                if isinstance(data.columns, pd.MultiIndex):
                    # 保留第一級欄名（Open, High 等）
                    data.columns = [col[0] for col in data.columns]
                data = data.reset_index()
            else:
                self.logger.error(f"意外的數據類型 {type(data)}")
                print(f"錯誤：意外的數據類型 {type(data)}")
                return None, frequency

            # 標準化欄位名稱（首字大寫）
            col_map = {}
            for col in data.columns:
                col_lower = str(col).lower()
                if col_lower in ['date', 'time', 'timestamp']:
                    col_map[col] = 'Time'
                elif col_lower in ['open', 'o']:
                    col_map[col] = 'Open'
                elif col_lower in ['high', 'h']:
                    col_map[col] = 'High'
                elif col_lower in ['low', 'l']:
                    col_map[col] = 'Low'
                elif col_lower in ['close', 'c']:
                    col_map[col] = 'Close'
                elif col_lower in ['volume', 'vol']:
                    col_map[col] = 'Volume'

            data = data.rename(columns=col_map)

            # 檢查必要欄位
            required_cols = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                print(f"警告：缺少欄位 {missing_cols}，將設置為缺失值")
                for col in missing_cols:
                    data[col] = pd.NA

            # 僅保留必要欄位
            data = data[required_cols]

            # 驗證並轉換數值欄位
            for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                if not isinstance(data[col], pd.Series):
                    print(f"警告：欄位 '{col}' 不是 Series，類型為 {type(data[col])}，轉為 Series")
                    data[col] = pd.Series(data[col], index=data.index)
                try:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                except Exception as e:
                    print(f"警告：無法轉換欄位 '{col}' 為數值：{e}")
                    data[col] = pd.NA

            # 檢查數據有效性（大寫欄位）
            if isinstance(data, pd.DataFrame):
                try:
                    invalid_rows = data[['Open', 'High', 'Low', 'Close']].isna().all(axis=1)
                except Exception as e:
                    self.logger.warning(f"檢查無效行時出錯：{e}")
                    print(f"檢查無效行時出錯：{e}")
                    invalid_rows = None
                if isinstance(invalid_rows, pd.Series):
                    if invalid_rows.any():
                        self.logger.warning(f"'{ticker}' 數據包含 {invalid_rows.sum()} 個無效行，將移除")
                        print(f"警告：'{ticker}' 數據包含 {invalid_rows.sum()} 個無效行，將移除")
                        data = data[~invalid_rows]
                else:
                    self.logger.warning("invalid_rows 不是 Series，跳過無效行移除")
                    print("警告：invalid_rows 不是 Series，跳過無效行移除")
            else:
                self.logger.warning("data 不是 DataFrame，跳過無效行檢查")
                print("警告：data 不是 DataFrame，跳過無效行檢查")

            if not isinstance(data, pd.DataFrame) or data.empty:
                self.logger.error(f"'{ticker}' 數據在清洗後為空")
                print(f"錯誤：'{ticker}' 數據在清洗後為空")
                return None, frequency

            self.logger.info(f"從 Yahoo Finance 載入 '{ticker}' 成功，行數：{len(data)}")
            return data, frequency

        except Exception as e:
            self.logger.error(f"Yahoo Finance 載入錯誤：{e}")
            print(f"Yahoo Finance 載入錯誤：{e}")
            return None, frequency