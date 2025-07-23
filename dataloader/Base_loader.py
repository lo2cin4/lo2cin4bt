"""
Base_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入模組的抽象基底類，統一規範所有數據來源載入器（如 Binance、File、Yfinance）的介面與繼承結構，確保數據載入、驗證、轉換流程一致。

【關聯流程與數據流】
------------------------------------------------------------
- 由各數據來源子類（Binance_loader、File_loader、Yfinance_loader）繼承
- 提供標準化數據載入、驗證、轉換流程，數據傳遞給 DataImporter/BacktestEngine

```mermaid
flowchart TD
    A[Base_loader] -->|繼承| B[Binance_loader/File_loader/Yfinance_loader]
    B -->|載入數據| C[DataImporter/BacktestEngine]
```

【主控流程細節】
------------------------------------------------------------
- 定義 load_data、validate_data 等抽象方法，所有子類必須實作
- 子類可擴充自訂數據來源與格式，但需遵循統一介面
- 驗證與轉換流程需確保數據格式正確，便於下游模組處理

【維護與擴充提醒】
------------------------------------------------------------
- 新增數據來源時，請務必繼承本類並實作必要方法
- 若介面、欄位有變動，需同步更新所有子類與本檔案頂部註解
- 介面規範變動時，請同步通知協作者並於 README 記錄

【常見易錯點】
------------------------------------------------------------
- 子類未正確實作抽象方法會導致載入失敗
- 數據格式或欄位不符會影響下游流程
- 忽略欄位型態轉換，易導致驗證失敗

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

【維護重點】
------------------------------------------------------------
- 新增/修改介面、欄位時，務必同步更新所有子類與本檔案
- 介面規範、欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- 詳細介面規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
"""
import pandas as pd  # 用於數據處理（如 DataFrame 操作、數據概覽）
from .File_loader import FileLoader  # 自定義模組：從 Excel/CSV 文件載入價格數據
from .Yfinance_loader import YahooFinanceLoader  # 自定義模組：從 Yahoo Finance API 載入價格數據
from .Binance_loader import BinanceLoader  # 自定義模組：從 Binance API 載入價格數據
from .Predictor_loader import PredictorLoader  # 自定義模組：載入預測因子數據（Excel/CSV/JSON）
from .Validator_loader import DataValidator  # 自定義模組：驗證和清洗數據
from .Calculator_loader import ReturnCalculator  # 自定義模組：計算收益率
from .DataExporter_loader import DataExporter  # 自定義模組：導出數據為 CSV/XLSX/JSON

class DataLoader:
    def __init__(self):
        """初始化 DataLoader，設置數據和來源為 None
        使用模組: 無（僅標準 Python）
        """
        self.data = None  # 儲存載入的數據（pandas DataFrame）
        self.source = None  # 記錄價格數據來源（1: 文件, 2: Yahoo Finance, 3: Binance）

    def load_data(self):
        """交互式載入價格與預測因子數據，並提示問題
        使用模組:
            - pandas (pd): 數據處理和概覽顯示
            - FileLoader: 從文件載入價格數據
            - YahooFinanceLoader: 從 Yahoo Finance 載入價格數據
            - BinanceLoader: 從 Binance API 載入價格數據
            - PredictorLoader: 載入預測因子數據
            - DataValidator: 驗證和清洗數據
            - ReturnCalculator: 計算收益率
            - DataExporter: 導出最終數據
        返回: pandas DataFrame 或 None（若載入失敗）
        """
        print("\n=== 數據載入 ===")

        # 選擇價格數據來源
        print("請選擇價格數據來源：")
        print("1. Excel/CSV 文件")
        print("2. Yahoo Finance")
        print("3. Binance API")
        while True:
            choice = input("輸入你的選擇（1, 2, 3）：").strip()  # 標準 Python 輸入
            if choice in ['1', '2', '3']:
                self.source = choice
                break
            print("錯誤：請輸入 1, 2 或 3。")

        # 載入價格數據
        while True:
            if self.source == '1':
                loader = FileLoader()
            elif self.source == '2':
                loader = YahooFinanceLoader()
            else:
                loader = BinanceLoader()

            self.data, self.frequency = loader.load()
            if self.data is not None:
                break
            print("價格數據載入失敗，請重新選擇數據來源與輸入參數。\n")
            print("請選擇價格數據來源：")
            print("1. Excel/CSV 文件")
            print("2. Yahoo Finance")
            print("3. Binance API")
            while True:
                choice = input("輸入你的選擇（1, 2, 3）：").strip()
                if choice in ['1', '2', '3']:
                    self.source = choice
                    break
                print("錯誤：請輸入 1, 2 或 3。")

        # 驗證和清洗價格數據
        validator = DataValidator(self.data)  # 使用 DataValidator 模組驗證數據
        self.data = validator.validate_and_clean()  # 調用 validate_and_clean 方法清洗數據
        if self.data is None:
            print("價格數據清洗失敗，程式終止")
            return None

        # 計算收益率
        calculator = ReturnCalculator(self.data)  # 使用 ReturnCalculator 模組計算收益率
        self.data = calculator.calculate_returns()  # 調用 calculate_returns 方法更新數據
        price_data = self.data  # 儲存價格數據副本以供後續使用

        print("\n價格數據載入完成，概覽：")
        print(self.data.head())  # 使用 pandas 的 head 方法顯示數據前幾行

        # 載入預測因子數據
        predictor_loader = PredictorLoader(price_data=price_data)  # 使用 PredictorLoader 模組載入預測因子
        predictor_data = predictor_loader.load()  # 調用 load 方法合併預測因子數據
        if isinstance(predictor_data, str) and predictor_data == "__SKIP_STATANALYSER__":
            if not hasattr(self, "frequency") or self.frequency is None:
                self.frequency = "1d"
            return "__SKIP_STATANALYSER__"
        elif predictor_data is not None:
            self.data = predictor_data
        else:
            print("未載入預測因子，僅使用價格數據。")
            self.data = price_data

        # 重新驗證合併數據
        validator = DataValidator(self.data)  # 使用 DataValidator 模組驗證合併數據
        self.data = validator.validate_and_clean()  # 再次調用 validate_and_clean 方法
        if self.data is None:
            print("合併數據清洗失敗，程式終止")
            return None

        print("\n最終數據（價格與預測因子）載入完成，概覽：")
        print(self.data.head())  # 使用 pandas 的 head 方法顯示最終數據概覽

        # 提示導出數據
        export_choice = input("\n是否導出合併後數據(xlsx/csv/json)？(y/n，預設n)：").strip().lower() or 'n'
        if export_choice == 'y':
            exporter = DataExporter(self.data)  # 使用 DataExporter 模組導出數據
            exporter.export()  # 調用 export 方法交互式導出數據

        return self.data