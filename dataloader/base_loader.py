"""
base_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入模組的抽象基底類，統一規範所有數據來源載入器（如 Binance、File、Yfinance）的介面與繼承結構，確保數據載入、驗證、轉換流程一致。

【流程與數據流】
------------------------------------------------------------
- 由各數據來源子類（binance_loader、file_loader、yfinance_loader）繼承
- 提供標準化數據載入、驗證、轉換流程，數據傳遞給 DataImporter/BacktestEngine

```mermaid
flowchart TD
    A[base_loader] -->|繼承| B[binance_loader/file_loader/yfinance_loader]
    B -->|載入數據| C[DataImporter/BacktestEngine]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增數據來源時，請務必繼承本類並實作必要方法
- 若介面、欄位有變動，需同步更新所有子類與本檔案頂部註解
- 介面規範變動時，請同步通知協作者並於 README 記錄
- 新增/修改介面、欄位時，務必同步更新所有子類與本檔案
- 介面規範、欄位名稱、型態需與下游模組協調一致

【常見易錯點】
------------------------------------------------------------
- 子類未正確實作抽象方法會導致載入失敗
- 數據格式或欄位不符會影響下游流程
- 忽略欄位型態轉換，易導致驗證失敗

【錯誤處理】
------------------------------------------------------------
- 檔案不存在時提供明確錯誤訊息
- 欄位缺失時自動提示用戶補充
- 數據型態錯誤時提供轉換建議

【範例】
------------------------------------------------------------
- class BinanceLoader(BaseLoader):
      def load_data(self): ...
- class FileLoader(BaseLoader):
      def load_data(self): ...

【與其他模組的關聯】
------------------------------------------------------------
- 由 dataloader 目錄下各數據來源子類繼承
- 載入數據傳遞給 DataImporter/BacktestEngine
- 依賴 DataValidator、ReturnCalculator 等輔助模組

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，定義基本介面
- v1.1: 新增步驟跟蹤功能，支援 Rich Panel 顯示
- v1.2: 重構為 BaseDataLoader 和 DataLoader 雙類結構

【參考】
------------------------------------------------------------
- 詳細介面規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Tuple, Union

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from dataloader.validator_loader import print_dataframe_table

console = Console()


class AbstractDataLoader(ABC):
    """Abstract base class for all data loaders with common functionality"""

    def __init__(self) -> None:
        self.console = Console()
        self.panel_title = "[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]"
        self.panel_error_style = "#8f1511"
        self.panel_success_style = "#dbac30"

    def show_error(self, message: str) -> None:
        """Display error message in standardised panel"""
        self.console.print(
            Panel(
                f"❌ {message}",
                title=self.panel_title,
                border_style=self.panel_error_style,
            )
        )

    def show_success(self, message: str) -> None:
        """Display success message in standardised panel"""
        self.console.print(
            Panel(
                message,
                title=self.panel_title,
                border_style=self.panel_success_style,
            )
        )

    def show_warning(self, message: str) -> None:
        """Display warning message in standardised panel"""
        self.console.print(
            Panel(
                f"⚠️ {message}",
                title=self.panel_title,
                border_style=self.panel_error_style,
            )
        )

    def show_info(self, message: str) -> None:
        """Display informational message in standardised panel"""
        self.console.print(
            Panel(
                message,
                title=self.panel_title,
                border_style=self.panel_success_style,
            )
        )

    def get_user_input(self, prompt: str, default: Optional[str] = None) -> str:
        """Get user input with optional default value

        Args:
            prompt: The prompt message to display. If empty, no prompt is shown.
            default: Optional default value if user provides no input.

        Returns:
            User input string, or default if provided and user enters nothing.
        """
        if prompt:  # Only print if prompt is not empty
            if default:
                self.console.print(
                    f"[bold #dbac30]{prompt}（預設 {default}）：[/bold #dbac30]"
                )
            else:
                self.console.print(f"[bold #dbac30]{prompt}：[/bold #dbac30]")

        user_input = input().strip()
        return user_input if user_input else (default or user_input)

    def get_date_range(
        self, default_start: str = "2020-01-01", default_end: Optional[str] = None
    ) -> Tuple[str, str]:
        """Get date range from user input with defaults"""
        if default_end is None:
            default_end = datetime.now().strftime("%Y-%m-%d")

        start_date = self.get_user_input(
            f"請輸入開始日期（例如 {default_start}", default_start
        )
        end_date = self.get_user_input(
            f"請輸入結束日期（例如 {default_end}", default_end
        )

        return start_date, end_date

    def get_frequency(self, default: str = "1d") -> str:
        """Get data frequency from user input"""
        return self.get_user_input(
            "輸入價格數據的周期 (例如 1d 代替日線，1h 代表 1小時線", default
        )

    def display_missing_values(
        self, data: pd.DataFrame, columns: Optional[List[str]] = None
    ) -> None:
        """Display missing value statistics for specified columns"""
        if columns is None:
            columns = ["Open", "High", "Low", "Close", "Volume"]

        missing_msgs = []
        for col in columns:
            if col in data.columns:
                missing_ratio = data[col].isna().mean()
                missing_msgs.append(f"{col} 缺失值比例：{missing_ratio:.2%}")

        if missing_msgs:
            self.console.print(
                Panel(
                    "\n".join(missing_msgs),
                    title=self.panel_title,
                    border_style=self.panel_success_style,
                )
            )

    def standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to expected format"""
        col_map = {}
        time_col_found = False  # 追蹤是否已找到時間欄位
        
        for col in data.columns:
            col_lower = str(col).lower()
            # 只轉換第一個找到的時間欄位，避免重複鍵
            if col_lower in ["date", "time", "timestamp", "datetime", "period"]:
                if not time_col_found:
                    col_map[col] = "Time"
                    time_col_found = True
            elif col_lower in ["open", "o"]:
                col_map[col] = "Open"
            elif col_lower in ["high", "h"]:
                col_map[col] = "High"
            elif col_lower in ["low", "l"]:
                col_map[col] = "Low"
            elif col_lower in ["close", "c"]:
                col_map[col] = "Close"
            elif col_lower in ["volume", "vol", "v"]:
                col_map[col] = "Volume"

        return data.rename(columns=col_map)

    def ensure_required_columns(
        self, data: pd.DataFrame, required_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Ensure all required columns exist in dataframe"""
        if required_cols is None:
            required_cols = ["Time", "Open", "High", "Low", "Close", "Volume"]

        missing_cols = [col for col in required_cols if col not in data.columns]

        if missing_cols:
            self.show_warning(f"缺少欄位 {missing_cols}，將設為缺失值")
            for col in missing_cols:
                data[col] = pd.NA

        # Keep only required columns
        return data[required_cols]

    def convert_numeric_columns(
        self, data: pd.DataFrame, numeric_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Convert specified columns to numeric types"""
        if numeric_cols is None:
            numeric_cols = ["Open", "High", "Low", "Close", "Volume"]

        for col in numeric_cols:
            if col in data.columns:
                try:
                    data[col] = pd.to_numeric(data[col], errors="coerce")
                except Exception as e:
                    self.show_warning(f"無法轉換欄位 '{col}' 為數值：{e}")
                    data[col] = pd.NA

        return data

    def detect_and_convert_timestamp(
        self, data: pd.DataFrame, time_col: str = "Time"
    ) -> pd.DataFrame:
        """
        檢測並轉換timestamp格式為標準datetime格式
        
        如果時間欄位是timestamp格式（Unix時間戳），自動轉換為datetime
        支援秒級和毫秒級timestamp
        
        Args:
            data: 數據DataFrame
            time_col: 時間欄位名稱，預設為 "Time"
            
        Returns:
            轉換後的DataFrame
        """
        if time_col not in data.columns:
            return data
            
        try:
            # 檢查是否已經是datetime格式
            if pd.api.types.is_datetime64_any_dtype(data[time_col]):
                return data
            
            # 嘗試轉換為數值，檢查是否為timestamp
            sample_value = data[time_col].iloc[0]
            
            # 如果是數值型態，可能是timestamp
            if pd.api.types.is_numeric_dtype(data[time_col]):
                # 判斷是秒級還是毫秒級timestamp
                # 秒級timestamp大約為10位數，毫秒級為13位數
                # 使用 numpy 的數值類型檢查（支援 numpy.int64 等類型）
                import numpy as np
                if isinstance(sample_value, (int, float, np.integer, np.floating)):
                    if sample_value > 1e10:  # 毫秒級timestamp
                        self.show_info("檢測到毫秒級timestamp格式，正在轉換...")
                        data[time_col] = pd.to_datetime(data[time_col], unit="ms")
                    else:  # 秒級timestamp
                        self.show_info("檢測到秒級timestamp格式，正在轉換...")
                        data[time_col] = pd.to_datetime(data[time_col], unit="s")
                    
                    self.show_success(f"timestamp轉換成功，格式為：{data[time_col].iloc[0]}")
            else:
                # 嘗試將字符串轉換為數值再判斷
                try:
                    numeric_value = pd.to_numeric(data[time_col].iloc[0])
                    if numeric_value > 1e10:  # 毫秒級
                        self.show_info("檢測到毫秒級timestamp格式，正在轉換...")
                        data[time_col] = pd.to_numeric(data[time_col])
                        data[time_col] = pd.to_datetime(data[time_col], unit="ms")
                    else:  # 秒級
                        self.show_info("檢測到秒級timestamp格式，正在轉換...")
                        data[time_col] = pd.to_numeric(data[time_col])
                        data[time_col] = pd.to_datetime(data[time_col], unit="s")
                    
                    self.show_success(f"timestamp轉換成功，格式為：{data[time_col].iloc[0]}")
                except (ValueError, TypeError):
                    # 不是timestamp，跳過轉換
                    pass
                    
        except Exception as e:
            self.show_warning(f"timestamp檢測時出錯：{e}，將嘗試其他方式解析時間")
        
        return data

    @abstractmethod
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """Abstract method that must be implemented by all subclasses"""


class BaseDataLoader:
    """
    重構後的數據載入框架核心協調器，負責調用各模組並統一管理步驟跟蹤
    """

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.data: Optional[pd.DataFrame] = None
        self.frequency: Optional[str] = None
        self.source: Optional[str] = None
        self.logger = logger or logging.getLogger("BaseDataLoader")

    @staticmethod
    def get_steps() -> List[str]:
        """Get the list of steps for data loading process."""
        return [
            "選擇價格數據來源",
            "輸入預測因子",
            "導出合併後數據",
            "選擇差分預測因子",
        ]

    def process_difference(
        self, data: pd.DataFrame, predictor_col: Optional[str] = None
    ) -> Tuple[pd.DataFrame, Optional[List[str]], Optional[pd.Series]]:
        """
        處理差分步驟，讓用戶選擇是否進行差分處理
        """
        # Step 4: 選擇差分預測因子
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

        # 檢查是否有可用的預測因子
        if not available_factors:
            self._print_step_panel(
                4,
                "檢測到僅有價格數據，無預測因子可進行差分處理。\n"
                "將直接進行回測，使用價格數據作為基礎。",
            )
            return data, None, None

        default = available_factors[0]
        self._print_step_panel(
            4,
            "差分（Differencing）是時間序列分析常用的預處理方法。\n"
            "可以消除數據中的趨勢與季節性，讓資料更穩定，有助於提升統計檢定與回測策略的準確性。\n"
            "在量化回測中，我們往往不會選擇價格(原始因子)，而是收益率(差分值)作為預測因子，因為收益率更能反映資產的實際表現。\n\n"
            "[bold #dbac30]選項說明：[/bold #dbac30]\n"
            "• 選擇預測因子：進行差分處理後回測\n"
            "• 輸入 'price'：僅使用價格數據進行回測",
        )

        while True:
            console.print(
                f"[bold #dbac30]請輸入要差分的預測因子（可選: {available_factors}，"
                f"預設 {default}，或輸入 'price' 僅使用價格數據）：[/bold #dbac30]"
            )
            predictor_col = input().strip() or default
            if predictor_col.lower() == "price":
                # 用戶選擇僅使用價格數據
                self._print_step_panel(
                    4, "已選擇僅使用價格數據進行回測，跳過差分處理。"
                )
                return data, None, None
            if predictor_col not in available_factors:
                console.print(
                    Panel(
                        f"輸入錯誤，請重新輸入（可選: {available_factors}，預設 {default}，或輸入 'price' 僅使用價格數據）",
                        title=Text("📊 數據載入 Dataloader", style="bold #8f1511"),
                        border_style="#8f1511",
                    )
                )
                continue
            break

        # 進行差分處理
        from .predictor_loader import PredictorLoader

        predictor_loader = PredictorLoader(data)
        data, diff_cols, used_series = predictor_loader.process_difference(
            data, predictor_col
        )
        return data, diff_cols, used_series

    @staticmethod
    def print_step_panel(current_step: int, desc: str = "") -> None:
        """Print a step panel with progress information."""
        steps = BaseDataLoader.get_steps()
        step_content = ""
        for idx, step in enumerate(steps):
            if idx < current_step:
                step_content += f"🟢 {step}\n"
            else:
                step_content += f"🔴 {step}\n"
        content = step_content.strip()
        if desc:
            content += f"\n\n[bold #dbac30]說明[/bold #dbac30]\n{desc}"
        panel_title = f"[bold #dbac30]📊 數據載入 Dataloader 步驟：{steps[current_step - 1]}[/bold #dbac30]"
        console.print(Panel(content.strip(), title=panel_title, border_style="#dbac30"))

    def _print_step_panel(self, current_step: int, desc: str = "") -> None:
        # 已被靜態方法取代，保留兼容性
        BaseDataLoader.print_step_panel(current_step, desc)

    def run(  # noqa: C901 # pylint: disable=too-many-statements, too-many-branches
        self,
    ) -> Optional[Union[pd.DataFrame, str]]:
        """
        主執行函數，協調數據載入、預測因子處理、數據導出等全流程
        """
        try:
            # Step 1: 選擇價格數據來源
            self._print_step_panel(
                1,
                "請選擇你要載入的價格數據來源，可選擇本地 Excel/CSV、Yahoo Finance 或 Binance API。\n"
                "這一步會決定後續所有分析與回測的基礎數據。\n"
                "[bold yellow]本地檔案讀取格式：Time | Open | High | Low | Close | "
                "Volume(可選)（首字母大寫）[/bold yellow]",
            )

            # 數據來源選單 Panel
            console.print(
                Panel(
                    "[bold white]請選擇價格數據來源：\n1. Excel/CSV 文件\n"
                    "2. Yahoo Finance\n3. Binance API\n4. Coinbase API[/bold white]",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

            while True:
                console.print(
                    "[bold #dbac30]輸入你的選擇（1, 2, 3, 4）：[/bold #dbac30]"
                )
                choice = input().strip()
                if choice in ["1", "2", "3", "4"]:
                    self.source = choice
                    break
                console.print(
                    Panel(
                        "錯誤：請輸入 1, 2, 3 或 4。",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

            # 載入價格數據
            while True:
                if self.source == "1":
                    from .file_loader import FileLoader

                    loader = FileLoader()
                elif self.source == "2":
                    from .yfinance_loader import YahooFinanceLoader

                    loader = YahooFinanceLoader()
                elif self.source == "3":
                    from .binance_loader import BinanceLoader

                    loader = BinanceLoader()
                else:
                    from .coinbase_loader import CoinbaseLoader

                    loader = CoinbaseLoader()

                self.data, self.frequency = loader.load()
                # 保存 symbol 信息（如果 loader 有設定）
                if hasattr(loader, 'symbol'):
                    self.symbol = loader.symbol
                else:
                    self.symbol = "X"  # File loader 預設值
                if self.data is not None:
                    break
                # 若 loader 回傳 (None, None)，直接回到數據來源選擇
                return self.run()

            # 驗證和清洗價格數據
            from .validator_loader import DataValidator

            validator = DataValidator(self.data)
            self.data = validator.validate_and_clean()
            if self.data is None:
                console.print(
                    Panel(
                        "[bold #8f1511]價格數據清洗失敗，程式終止[/bold #8f1511]",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                return None

            # 計算收益率
            from .calculator_loader import ReturnCalculator

            calculator = ReturnCalculator(self.data)
            self.data = calculator.calculate_returns()
            price_data = self.data

            # 價格數據載入完成 Panel
            print_dataframe_table(
                self.data.head(), title="價格數據載入完成，概覽"  # type: ignore[union-attr]
            )

            # Step 2: 輸入預測因子
            self._print_step_panel(
                2,
                "你可以提供一份你認為能預測價格的「預測因子」數據檔案（如 Excel/CSV/JSON），\n"
                "例如：BTC ETF 資金流入數據、Google Trends、其他資產價格等。\n\n"
                "系統會自動對齊時間，並用這些因子做後續的統計分析與回測。\n"
                "你也可以輸入另一份價格數據，並選擇用哪個欄位作為預測因子（例如用 AAPL 股價預測 NVDA 股價）。\n\n"
                "如果留空，系統只會用剛才載入的價格數據，適合用於技術分析策略（如均線回測），\n"
                "並會直接跳過統計分析，進行回測。",
            )

            # 載入預測因子數據
            from .predictor_loader import PredictorLoader

            predictor_loader = PredictorLoader(price_data=price_data)
            predictor_data = predictor_loader.load()

            # 存儲預測因子文件名供後續使用
            self.predictor_file_name = predictor_loader.predictor_file_name

            if (
                isinstance(predictor_data, str)
                and predictor_data == "__SKIP_STATANALYSER__"
            ):
                if not hasattr(self, "frequency") or self.frequency is None:
                    self.frequency = "1d"
                # 設置標記表示跳過統計分析，但繼續使用價格數據
                self.skip_statanalyser = True
                self.data = price_data
            elif predictor_data is not None:
                self.data = predictor_data
            else:
                console.print(
                    Panel(
                        "[bold #8f1511]未載入預測因子，僅使用價格數據。[/bold #8f1511]",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                self.data = price_data

            # 重新驗證合併數據
            from .validator_loader import DataValidator

            validator = DataValidator(self.data)
            self.data = validator.validate_and_clean()
            if self.data is None:
                console.print(
                    Panel(
                        "[bold #8f1511]合併數據清洗失敗，程式終止[/bold #8f1511]",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                return None

            # 最終數據載入完成 Panel
            print_dataframe_table(
                self.data.head(), title="最終數據（價格與預測因子）載入完成，概覽"
            )

            # Step 3: 導出合併後數據
            self._print_step_panel(
                3,
                "你可以將合併後的數據導出為 xlsx/csv/json 檔案，方便後續分析或保存。\n"
                "這一步可跳過，若不導出，數據仍會自動進入後續回測與分析流程。",
            )

            # 提示導出數據
            console.print(
                "[bold #dbac30]\n是否導出合併後數據(xlsx/csv/json)？(y/n，預設n)：[/bold #dbac30]"
            )
            export_choice = input().strip().lower() or "n"
            if export_choice == "y":
                from .data_exporter_loader import DataExporter

                exporter = DataExporter(self.data)
                exporter.export()
            else:
                console.print(
                    Panel(
                        "未導出合併後數據，數據將直接進入後續分析/回測流程。",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )

            return self.data

        except Exception as err:  # pylint: disable=broad-exception-caught
            self.logger.error(f"數據載入失敗: {err}")
            console.print(
                Panel(
                    f"[bold #8f1511]數據載入失敗: {err}[/bold #8f1511]",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return None


class DataLoader:  # pylint: disable=too-few-public-methods
    """Data loader wrapper class for backward compatibility."""

    def __init__(self) -> None:
        """初始化 DataLoader，設置數據和來源為 None
        使用模組: 無（僅標準 Python）
        """
        self.data: Optional[Union[pd.DataFrame, str]] = (
            None  # 儲存載入的數據（pandas DataFrame）
        )
        self.source: Optional[str] = (
            None  # 記錄價格數據來源（1: 文件, 2: Yahoo Finance, 3: Binance）
        )
        self.frequency: Optional[str] = None  # 資料頻率

    def load_data(self) -> Optional[Union[pd.DataFrame, str]]:
        """Load data using BaseDataLoader."""
        # 使用新的 BaseDataLoader
        loader = BaseDataLoader()
        result = loader.run()
        if isinstance(result, str) and result == "__SKIP_STATANALYSER__":
            self.data = loader.data
            self.frequency = loader.frequency
            return "__SKIP_STATANALYSER__"
        self.data = result
        self.frequency = loader.frequency
        return result
