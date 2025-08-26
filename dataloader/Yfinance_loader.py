"""
Yfinance_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責連接 Yahoo Finance API 下載行情數據，支援多種頻率、資料欄位自動標準化，並確保數據結構與下游模組一致。

【流程與數據流】
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

import pandas as pd
import yfinance as yf
from rich.console import Console
from rich.panel import Panel

console = Console()
from dataloader.Validator_loader import print_dataframe_table


class YahooFinanceLoader:
    def load(self):
        """從 Yahoo Finance 載入數據，參考 vectorbt 的標準化處理"""
        from datetime import datetime

        default_ticker = "TSLA"
        default_start = "2020-01-01"
        default_end = datetime.now().strftime("%Y-%m-%d")
        console.print(
            "[bold #dbac30]請輸入股票或指數代碼（例如 TSLA，預設 TSLA）：[/bold #dbac30]"
        )
        ticker = input().strip() or default_ticker
        console.print(
            "[bold #dbac30]輸入價格數據的周期 (例如 1d 代替日線，1h 代表 1小時線，預設 1d)：[/bold #dbac30]"
        )
        frequency = input().strip() or "1d"
        console.print(
            f"[bold #dbac30]請輸入開始日期（例如 2020-01-01，預設 {default_start}）：[/bold #dbac30]"
        )
        start_date = input().strip() or default_start
        console.print(
            f"[bold #dbac30]請輸入結束日期（例如 2024-12-31，預設 {default_end}）：[/bold #dbac30]"
        )
        end_date = input().strip() or default_end

        try:
            # 捕捉 yfinance 的 stderr 輸出
            old_stderr = sys.stderr
            sys.stderr = io.StringIO()
            # 下載數據，設置參數模仿 vectorbt
            data = yf.download(
                ticker,
                start=start_date,
                end=end_date,
                auto_adjust=False,
                progress=False,
            )
            yf_err = sys.stderr.getvalue()
            sys.stderr = old_stderr

            # 若有 Failed download 等訊息，加入 Panel 錯誤顯示
            extra_msg = ""
            if yf_err.strip():
                extra_msg = f"\n[red]{yf_err.strip()}[/red]"

            # 檢查數據是否為 DataFrame 並非空
            if not isinstance(data, pd.DataFrame) or data.empty:
                console.print(
                    Panel(
                        f"❌ 無法獲取 '{ticker}' 的數據，可能股票代碼無效或日期範圍錯誤。{extra_msg}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                return None, frequency

            # 打印原始數據結構以便診斷
            print_dataframe_table(data.head(), title="原始數據預覽（前5行）")

            # 處理可能的數據結構
            if isinstance(data, pd.Series):
                # 單股票返回 Series，轉為 DataFrame
                data = pd.DataFrame({"Close": data}).reset_index()
            elif isinstance(data, pd.DataFrame):
                # 展平多級索引（如果存在）
                if isinstance(data.columns, pd.MultiIndex):
                    # 保留第一級欄名（Open, High 等）
                    data.columns = [col[0] for col in data.columns]
                data = data.reset_index()
            else:
                console.print(
                    Panel(
                        f"❌ 意外的數據型別 {type(data)}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                return None, frequency

            # 標準化欄位名稱（首字大寫）
            col_map = {}
            for col in data.columns:
                col_lower = str(col).lower()
                if col_lower in ["date", "time", "timestamp"]:
                    col_map[col] = "Time"
                elif col_lower in ["open", "o"]:
                    col_map[col] = "Open"
                elif col_lower in ["high", "h"]:
                    col_map[col] = "High"
                elif col_lower in ["low", "l"]:
                    col_map[col] = "Low"
                elif col_lower in ["close", "c"]:
                    col_map[col] = "Close"
                elif col_lower in ["volume", "vol"]:
                    col_map[col] = "Volume"

            data = data.rename(columns=col_map)

            # 檢查必要欄位
            required_cols = ["Time", "Open", "High", "Low", "Close", "Volume"]
            missing_cols = [col for col in required_cols if col not in data.columns]
            if missing_cols:
                console.print(
                    Panel(
                        f"⚠️ 缺少欄位 {missing_cols}，將設為缺失值",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                for col in missing_cols:
                    data[col] = pd.NA

            # 僅保留必要欄位
            data = data[required_cols]

            # 驗證並轉換數值欄位
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if not isinstance(data[col], pd.Series):
                    console.print(
                        Panel(
                            f"⚠️ 欄位 '{col}' 不是 Series，型別為 {type(data[col])}，轉為 Series",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    data[col] = pd.Series(data[col], index=data.index)
                try:
                    data[col] = pd.to_numeric(data[col], errors="coerce")
                except Exception as e:
                    console.print(
                        Panel(
                            f"⚠️ 無法轉換欄位 '{col}' 為數值：{e}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    data[col] = pd.NA

            # 檢查數據有效性（大寫欄位）
            if isinstance(data, pd.DataFrame):
                try:
                    invalid_rows = (
                        data[["Open", "High", "Low", "Close"]].isna().all(axis=1)
                    )
                except Exception as e:
                    console.print(
                        Panel(
                            f"檢查無效行時出錯：{e}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    invalid_rows = None
                if isinstance(invalid_rows, pd.Series):
                    if invalid_rows.any():
                        console.print(
                            Panel(
                                f"⚠️ '{ticker}' 數據包含 {invalid_rows.sum()} 個無效行，將移除",
                                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                                border_style="#8f1511",
                            )
                        )
                        data = data[~invalid_rows]
                else:
                    console.print(
                        Panel(
                            "⚠️ invalid_rows 不是 Series，跳過無效行移除",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
            else:
                console.print(
                    Panel(
                        "⚠️ data 不是 DataFrame，跳過無效行檢查",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

            if not isinstance(data, pd.DataFrame) or data.empty:
                console.print(
                    Panel(
                        f"❌ '{ticker}' 數據在清洗後為空",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                return None, frequency

            console.print(
                Panel(
                    f"從 Yahoo Finance 載入 '{ticker}' 成功，行數：{len(data)}",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
            return data, frequency

        except Exception as e:
            console.print(
                Panel(
                    f"❌ Yahoo Finance 載入錯誤：{e}",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return None, frequency
