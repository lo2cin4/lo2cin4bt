"""
yfinance_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責連接 Yahoo Finance API 下載行情數據，支援多種頻率、資料欄位自動標準化，並確保數據結構與下游模組一致。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader 或 DataImporter 調用，作為行情數據來源之一
- 下載數據後傳遞給 DataValidator、ReturnCalculator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter] -->|選擇 Yahoo Finance| B(yfinance_loader)
    B -->|下載數據| C[DataValidator]
    C -->|驗證清洗| D[ReturnCalculator]
    D -->|計算收益率| E[BacktestEngine/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改支援頻率、欄位時，請同步更新頂部註解與下游流程
- 若 yfinance API 介面有變動，需同步更新本檔案與 base_loader
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
- 需與 base_loader 介面保持一致

【參考】
------------------------------------------------------------
- yfinance 官方文件
- base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""

import io
import sys
from typing import Optional, Tuple

import pandas as pd
import yfinance as yf

from dataloader.validator_loader import print_dataframe_table

from .base_loader import AbstractDataLoader


class YahooFinanceLoader(AbstractDataLoader):
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """從 Yahoo Finance 載入數據，參考 vectorbt 的標準化處理"""

        # Get user inputs
        ticker = self._get_ticker()
        frequency = self._get_frequency()
        start_date, end_date = self._get_date_range()

        try:
            # Download data from Yahoo Finance
            data, error_msg = self._download_data(ticker, start_date, end_date)
            if data is None:
                return None, frequency

            # Print raw data structure for diagnosis
            print_dataframe_table(data.head(), title="原始數據預覽（前5行）")

            # Process data structure
            data = self._process_data_structure(data)
            if data is None:
                return None, frequency

            # Standardize column names
            data = self._standardize_columns(data)

            # Check and add required columns
            data = self._ensure_required_columns(data)

            # Validate and convert numeric columns
            data = self._convert_numeric_columns(data)

            # Check and remove invalid rows
            data = self._remove_invalid_rows(data, ticker)

            # Final validation
            if not isinstance(data, pd.DataFrame) or data.empty:
                self.show_error(f"'{ticker}' 數據在清洗後為空")
                return None, frequency

            self.show_success(
                f"從 Yahoo Finance 載入 '{ticker}' 成功，行數：{len(data)}"
            )
            # 保存 symbol 供後續使用
            self.symbol = ticker
            return data, frequency

        except Exception as e:
            self.show_error(f"Yahoo Finance 載入錯誤：{e}")
            return None, frequency

    def _get_ticker(self) -> str:
        """Get ticker symbol from user input"""
        return self.get_user_input("請輸入股票或指數代碼（例如 TSLA", "TSLA")

    def _get_frequency(self) -> str:
        """Get data frequency from user input"""
        return self.get_frequency("1d")

    def _get_date_range(self) -> Tuple[str, str]:
        """Get date range from user input"""
        return self.get_date_range()

    def _download_data(
        self, ticker: str, start_date: str, end_date: str
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """Download data from Yahoo Finance API"""
        # Capture yfinance stderr output
        old_stderr = sys.stderr
        sys.stderr = io.StringIO()

        # Download data with vectorbt-like parameters
        data = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
        )

        yf_err = sys.stderr.getvalue()
        sys.stderr = old_stderr

        # Add error messages if any
        extra_msg = ""
        if yf_err.strip():
            extra_msg = f"\n[red]{yf_err.strip()}[/red]"

        # Check if data is valid
        if not isinstance(data, pd.DataFrame) or data.empty:
            self.show_error(
                f"無法獲取 '{ticker}' 的數據，可能股票代碼無效或日期範圍錯誤。{extra_msg}"
            )
            return None, ""

        return data, extra_msg

    def _process_data_structure(self, data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Process and flatten data structure"""
        if isinstance(data, pd.Series):
            # Single ticker returns Series, convert to DataFrame
            return pd.DataFrame({"Close": data}).reset_index()
        elif isinstance(data, pd.DataFrame):
            # Flatten multi-level index if exists
            if isinstance(data.columns, pd.MultiIndex):
                # Keep first level column names (Open, High, etc.)
                data.columns = [col[0] for col in data.columns]
            return data.reset_index()
        else:
            self.show_error(f"意外的數據型別 {type(data)}")
            return None

    def _standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to capitalized format"""
        return self.standardize_columns(data)

    def _ensure_required_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Ensure all required columns exist"""
        return self.ensure_required_columns(data)

    def _convert_numeric_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to numeric types"""
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in data.columns and not isinstance(data[col], pd.Series):
                self.show_warning(
                    f"欄位 '{col}' 不是 Series，型別為 {type(data[col])}，轉為 Series"
                )
                data[col] = pd.Series(data[col], index=data.index)

        return self.convert_numeric_columns(data)

    def _remove_invalid_rows(
        self, data: pd.DataFrame, ticker: str
    ) -> Optional[pd.DataFrame]:
        """Remove rows with all NaN values in price columns"""
        if not isinstance(data, pd.DataFrame):
            self.show_warning("data 不是 DataFrame，跳過無效行檢查")
            return data

        try:
            invalid_rows = data[["Open", "High", "Low", "Close"]].isna().all(axis=1)

            if not isinstance(invalid_rows, pd.Series):
                self.show_warning("invalid_rows 不是 Series，跳過無效行移除")
                return data

            if invalid_rows.any():
                self.show_warning(
                    f"'{ticker}' 數據包含 {invalid_rows.sum()} 個無效行，將移除"
                )
                data = data[~invalid_rows]

        except Exception as e:
            self.show_warning(f"檢查無效行時出錯：{e}")

        return data
