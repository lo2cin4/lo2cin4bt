"""
Base_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入模組的抽象基底類，統一規範所有數據來源載入器（如 Binance、File、Yfinance）的介面與繼承結構，確保數據載入、驗證、轉換流程一致。

【流程與數據流】
------------------------------------------------------------
- 由各數據來源子類（Binance_loader、File_loader、Yfinance_loader）繼承
- 提供標準化數據載入、驗證、轉換流程，數據傳遞給 DataImporter/BacktestEngine

```mermaid
flowchart TD
    A[Base_loader] -->|繼承| B[Binance_loader/File_loader/Yfinance_loader]
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

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from dataloader.Validator_loader import print_dataframe_table

from .Binance_loader import BinanceLoader  # 自定義模組：從 Binance API 載入價格數據
from .Calculator_loader import ReturnCalculator  # 自定義模組：計算收益率
from .Coinbase_loader import CoinbaseLoader  # 自定義模組：從 Coinbase API 載入價格數據
from .DataExporter_loader import DataExporter  # 自定義模組：導出數據為 CSV/XLSX/JSON
from .File_loader import FileLoader  # 自定義模組：從 Excel/CSV 文件載入價格數據
from .Predictor_loader import (
    PredictorLoader,
)  # 自定義模組：載入預測因子數據（Excel/CSV/JSON）
from .Validator_loader import DataValidator  # 自定義模組：驗證和清洗數據
from .Yfinance_loader import (
    YahooFinanceLoader,
)  # 自定義模組：從 Yahoo Finance API 載入價格數據

console = Console()


class BaseDataLoader:
    """
    重構後的數據載入框架核心協調器，負責調用各模組並統一管理步驟跟蹤
    """

    def __init__(self, logger=None):
        self.data = None
        self.frequency = None
        self.source = None
        self.logger = logger or logging.getLogger("BaseDataLoader")

    @staticmethod
    def get_steps():
        return [
            "選擇價格數據來源",
            "輸入預測因子",
            "導出合併後數據",
            "選擇差分預測因子",
        ]

    def process_difference(self, data, predictor_col=None):
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
                f"[bold #dbac30]請輸入要差分的預測因子（可選: {available_factors}，預設 {default}，或輸入 'price' 僅使用價格數據）：[/bold #dbac30]"
            )
            predictor_col = input().strip() or default
            if predictor_col.lower() == "price":
                # 用戶選擇僅使用價格數據
                self._print_step_panel(
                    4, "已選擇僅使用價格數據進行回測，跳過差分處理。"
                )
                return data, None, None
            elif predictor_col not in available_factors:
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
        predictor_loader = PredictorLoader(data)
        data, diff_cols, used_series = predictor_loader.process_difference(
            data, predictor_col
        )
        return data, diff_cols, used_series

    @staticmethod
    def print_step_panel(current_step: int, desc: str = ""):
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
        panel_title = f"[bold #dbac30]📊 數據載入 Dataloader 步驟：{steps[current_step-1]}[/bold #dbac30]"
        console.print(Panel(content.strip(), title=panel_title, border_style="#dbac30"))

    def _print_step_panel(self, current_step: int, desc: str = ""):
        # 已被靜態方法取代，保留兼容性
        BaseDataLoader.print_step_panel(current_step, desc)

    def run(self):
        """
        主執行函數，協調數據載入、預測因子處理、數據導出等全流程
        """
        try:
            # Step 1: 選擇價格數據來源
            self._print_step_panel(
                1,
                "請選擇你要載入的價格數據來源，可選擇本地 Excel/CSV、Yahoo Finance 或 Binance API。\n"
                "這一步會決定後續所有分析與回測的基礎數據。\n"
                "[bold yellow]本地檔案讀取格式：Time | Open | High | Low | Close | Volume(可選)（首字母大寫）[/bold yellow]",
            )

            # 數據來源選單 Panel
            console.print(
                Panel(
                    "[bold white]請選擇價格數據來源：\n1. Excel/CSV 文件\n2. Yahoo Finance\n3. Binance API\n4. Coinbase API[/bold white]",
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
                    loader = FileLoader()
                elif self.source == "2":
                    loader = YahooFinanceLoader()
                elif self.source == "3":
                    loader = BinanceLoader()
                else:
                    loader = CoinbaseLoader()

                self.data, self.frequency = loader.load()
                if self.data is not None:
                    break
                # 若 loader 回傳 (None, None)，直接回到數據來源選擇
                return self.run()

            # 驗證和清洗價格數據
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
            calculator = ReturnCalculator(self.data)
            self.data = calculator.calculate_returns()
            price_data = self.data

            # 價格數據載入完成 Panel
            print_dataframe_table(self.data.head(), title="價格數據載入完成，概覽")

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
            predictor_loader = PredictorLoader(price_data=price_data)
            predictor_data = predictor_loader.load()

            if (
                isinstance(predictor_data, str)
                and predictor_data == "__SKIP_STATANALYSER__"
            ):
                if not hasattr(self, "frequency") or self.frequency is None:
                    self.frequency = "1d"
                return "__SKIP_STATANALYSER__"
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

        except Exception as e:
            self.logger.error(f"數據載入失敗: {e}")
            console.print(
                Panel(
                    f"[bold #8f1511]數據載入失敗: {e}[/bold #8f1511]",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return None


class DataLoader:
    def __init__(self):
        """初始化 DataLoader，設置數據和來源為 None
        使用模組: 無（僅標準 Python）
        """
        self.data = None  # 儲存載入的數據（pandas DataFrame）
        self.source = None  # 記錄價格數據來源（1: 文件, 2: Yahoo Finance, 3: Binance）

    def load_data(self):
        # 使用新的 BaseDataLoader
        loader = BaseDataLoader()
        result = loader.run()
        if isinstance(result, str) and result == "__SKIP_STATANALYSER__":
            self.data = loader.data
            self.frequency = loader.frequency
            return "__SKIP_STATANALYSER__"
        else:
            self.data = result
            self.frequency = loader.frequency
            return result
