"""
File_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責從本地 Excel、CSV 等檔案載入行情數據，支援多種格式、欄位自動標準化，並確保數據結構與下游模組一致。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader 或 DataImporter 調用，作為行情數據來源之一
- 載入數據後傳遞給 DataValidator、ReturnCalculator、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter] -->|選擇本地檔案| B(File_loader)
    B -->|載入數據| C[DataValidator]
    C -->|驗證清洗| D[ReturnCalculator]
    D -->|計算收益率| E[BacktestEngine/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改支援格式、欄位時，請同步更新頂部註解與下游流程
- 若欄位標準化邏輯有變動，需同步更新本檔案與 base_loader
- 檔案格式、欄位結構如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 檔案不存在或格式錯誤會導致載入失敗
- 欄位缺失或型態不符會影響下游驗證與計算
- 欄位標準化未同步更新，易導致資料結構不一致

【範例】
------------------------------------------------------------
- loader = FileLoader()
  df = loader.load()
- 可於 DataLoader 互動式選擇本地檔案作為行情來源

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader/DataImporter 調用，數據傳遞給 DataValidator、ReturnCalculator、BacktestEngine
- 需與 base_loader 介面保持一致

【參考】
------------------------------------------------------------
- base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""

import glob  # 用於檢測目錄內的文件
import os  # 用於檢查文件是否存在（os.path.exists）
from typing import List, Optional, Tuple

import pandas as pd  # 用於讀取 Excel/CSV 文件、數據處理（如重命名欄位、填充缺失值）
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dataloader.Validator_loader import print_dataframe_table

console = Console()


class FileLoader:
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """從 Excel 或 CSV 文件載入數據
        使用模組:
            - pandas (pd): 讀取 Excel/CSV 文件（read_excel, read_csv），數據處理
            - os: 檢查文件是否存在（os.path.exists）
            - openpyxl: 作為 pd.read_excel 的引擎支持 Excel 文件
            - glob: 檢測目錄內的文件
        功能: 交互式選擇文件來源，讀取 Excel/CSV 文件，標準化欄位並返回數據
        返回: pandas DataFrame 或 None（若載入失敗）
        """
        while True:
            # 檢測預設目錄內的文件
            import_dir = os.path.join("records", "dataloader", "import")
            available_files = self._get_available_files(import_dir)

            if available_files:
                # 顯示文件選擇選單
                console.print(
                    Panel(
                        "[bold white]請選擇文件來源：\n1. 從預設目錄選擇文件\n2. 輸入完整文件路徑[/bold white]",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )

                while True:
                    console.print(
                        "[bold #dbac30]請選擇（1 或 2，預設1）：[/bold #dbac30]"
                    )
                    source_choice = input().strip() or "1"
                    if source_choice == "1":
                        file_name = self._select_from_directory(
                            available_files, import_dir
                        )
                        break
                    elif source_choice == "2":
                        file_name = self._input_file_path()
                        break
                    else:
                        console.print(
                            Panel(
                                "❌ 請輸入 1 或 2",
                                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                                border_style="#8f1511",
                            )
                        )
            else:
                # 如果預設目錄沒有文件，直接要求輸入路徑
                file_name = self._input_file_path()

            if file_name is None:
                continue

            console.print(
                "[bold #dbac30]輸入價格數據的周期 (例如 1d 代替日線，1h 代表 1小時線，預設 1d)：[/bold #dbac30]"
            )
            frequency = input().strip() or "1d"

            try:
                # 檢查文件是否存在
                if not os.path.exists(file_name):
                    msg = f"❌ 找不到文件 '{file_name}'"
                    console.print(
                        Panel(
                            msg,
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    continue

                # 根據文件擴展名選擇讀取方式
                if file_name.endswith(".xlsx"):
                    data = pd.read_excel(file_name)
                elif file_name.endswith(".csv"):
                    data = pd.read_csv(file_name)
                else:
                    console.print(
                        Panel(
                            "❌ 僅支援 .xlsx 或 .csv 文件",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    continue

                # 標準化欄位名稱
                data = self._standardize_columns(data)
                print_dataframe_table(data.head(), title="數據加載成功，預覽（前5行）")
                console.print(
                    Panel(
                        f"數據加載成功，行數：{len(data)}",
                        title="[bold #8f1511]📁 FileLoader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )
                return data, frequency
            except Exception as e:
                console.print(
                    Panel(
                        f"❌ 讀取文件時出錯：{e}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

    def _get_available_files(self, directory: str) -> List[str]:
        """檢測目錄內可用的 xlsx 和 csv 文件
        參數:
            directory: str - 要檢測的目錄路徑
        返回: list - 可用文件列表
        """
        if not os.path.exists(directory):
            return []

        # 檢測 xlsx 和 csv 文件
        xlsx_files = glob.glob(os.path.join(directory, "*.xlsx"))
        csv_files = glob.glob(os.path.join(directory, "*.csv"))

        return xlsx_files + csv_files

    def _select_from_directory(
        self, available_files: List[str], import_dir: str
    ) -> Optional[str]:
        """從預設目錄中選擇文件
        參數:
            available_files: list - 可用文件列表
            import_dir: str - 預設目錄路徑
        返回: str - 選擇的文件路徑或 None
        """
        # 創建文件列表表格
        table = Table(
            title="📁 可用的數據文件",
            show_header=True,
            header_style="bold #dbac30",
            border_style="#dbac30",
        )
        table.add_column("編號", style="bold #dbac30", justify="center")
        table.add_column("文件名", style="bold white")
        table.add_column("類型", style="bold white", justify="center")

        for i, file_path in enumerate(available_files, 1):
            file_name = os.path.basename(file_path)
            file_type = "Excel" if file_path.endswith(".xlsx") else "CSV"
            table.add_row(str(i), file_name, file_type)

        console.print(table)

        while True:
            # 如果只有一個文件，自動選擇
            if len(available_files) == 1:
                selected_file = available_files[0]
                console.print(
                    Panel(
                        f"✅ 自動選擇唯一文件：{os.path.basename(selected_file)}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )
                return selected_file

            console.print(
                "[bold #dbac30]請選擇文件編號（1-{}，預設1）：[/bold #dbac30]".format(
                    len(available_files)
                )
            )
            try:
                choice_input = input().strip()
                choice = int(choice_input) if choice_input else 1
                if 1 <= choice <= len(available_files):
                    selected_file = available_files[choice - 1]
                    console.print(
                        Panel(
                            f"✅ 已選擇：{os.path.basename(selected_file)}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    return selected_file
                else:
                    console.print(
                        Panel(
                            f"❌ 請輸入 1-{len(available_files)} 之間的數字",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
            except ValueError:
                console.print(
                    Panel(
                        "❌ 請輸入有效的數字",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

    def _input_file_path(self) -> Optional[str]:
        """要求用戶輸入完整文件路徑
        返回: str - 文件路徑或 None
        """
        console.print(
            "[bold #dbac30]請輸入文件名稱（例如 D:/Python/data.xlsx 或 D:/Python/data.csv，按Enter跳過）：[/bold #dbac30]"
        )
        file_name = input().strip()
        if not file_name:
            console.print(
                Panel(
                    "ℹ️ 跳過文件路徑輸入",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return None
        return file_name

    def _standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """將數據欄位標準化為 Time, Open, High, Low, Close, Volume
        使用模組:
            - pandas (pd): 欄位重命名（rename）、缺失值填充（pd.NA）、數據處理
        參數:
            data: pandas DataFrame - 輸入的數據
        功能: 將輸入數據的欄位名稱映射為標準名稱，檢查必要欄位並處理缺失值
        返回: 標準化後的 pandas DataFrame
        """
        # 定義欄位名稱映射
        col_map = {
            "Time": ["time", "date", "timestamp", "Time", "Date", "Timestamp"],
            "Open": ["open", "o", "Open", "O"],
            "High": ["high", "h", "High", "H"],
            "Low": ["low", "l", "Low", "L"],
            "Close": ["close", "c", "Close", "C"],
            "Volume": ["volume", "vol", "Volume", "Vol"],
        }
        new_cols = {}
        # 遍歷映射，查找現有欄位並重命名
        for std_col, aliases in col_map.items():
            for alias in aliases:
                if alias in data.columns:  # 使用 pandas 的 columns 屬性檢查
                    new_cols[alias] = std_col
                    break
        data = data.rename(columns=new_cols)  # 使用 pandas 的 rename 方法重命名欄位

        # 檢查必要欄位
        required_cols = ["Time", "Open", "High", "Low", "Close"]
        missing_cols = [
            col for col in required_cols if col not in data.columns
        ]  # 標準 Python 列表推導式，檢查 pandas columns
        if missing_cols:
            console.print(
                Panel(
                    f"⚠️ 缺少欄位 {missing_cols}，將從用戶輸入補充",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            for col in missing_cols:
                data[col] = pd.NA  # 使用 pandas 的 pd.NA 填充缺失欄位

        # 處理 volume 欄位（可選）
        if "Volume" not in data.columns:  # 使用 pandas 的 columns 屬性檢查
            console.print(
                "[bold #dbac30]數據缺少 Volume 欄位，是否填充內容？(y/n)：[/bold #dbac30]"
            )
            choice = input().strip().lower()  # 標準 Python 輸入
            if choice == "y":
                data["Volume"] = pd.NA  # 使用 pandas 的 pd.NA 填充
            else:
                data["Volume"] = 0.0  # 使用 pandas 賦值 0.0

        return data
