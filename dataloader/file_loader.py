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
from rich.table import Table

from dataloader.validator_loader import print_dataframe_table

from .base_loader import AbstractDataLoader


class FileLoader(AbstractDataLoader):
    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """從 Excel 或 CSV 文件載入數據
        使用模組:
            - pandas (pd): 讀取 Excel/CSV 文件（read_excel, read_csv），數據處理
            - os: 檢查文件是否存在（os.path.exists）
            - glob: 檢測目錄內的文件
        功能: 交互式選擇文件來源，讀取 Excel/CSV 文件，標準化欄位並返回數據
        返回: pandas DataFrame 或 None（若載入失敗）
        """
        while True:
            # 獲取文件路徑
            file_name = getattr(self, "file_path", None)
            if not file_name:
                file_name = self._get_file_path()
            
            if file_name is None:
                continue

            # 獲取頻率設定
            frequency = getattr(self, "interval", None)
            if not frequency:
                frequency = self._get_frequency()

            # 讀取並處理文件
            result = self._read_and_process_file(file_name, frequency)
            if result is not None:
                return result

    def _get_file_path(self) -> Optional[str]:
        """獲取要載入的文件路徑
        返回: 文件路徑或 None
        """
        import_dir = os.path.join("records", "dataloader", "import")
        available_files = self._get_available_files(import_dir)

        if available_files:
            return self._choose_file_source(available_files, import_dir)
        else:
            # 如果預設目錄沒有文件，直接要求輸入路徑
            return self._input_file_path()

    def _choose_file_source(
        self, available_files: List[str], import_dir: str
    ) -> Optional[str]:
        """選擇文件來源（從目錄選擇或輸入路徑）
        參數:
            available_files: 可用文件列表
            import_dir: 預設目錄路徑
        返回: 文件路徑或 None
        """
        self.show_info("[bold white]請選擇文件來源：\n1. 從預設目錄選擇文件\n2. 輸入完整文件路徑[/bold white]")

        while True:
            source_choice = self.get_user_input("請選擇（1 或 2", "1")
            if source_choice == "1":
                return self._select_from_directory(available_files, import_dir)
            elif source_choice == "2":
                return self._input_file_path()
            else:
                self.show_error("請輸入 1 或 2")

    def _get_frequency(self) -> str:
        """獲取數據頻率設定
        返回: 頻率字符串
        """
        return self.get_frequency("1d")

    def _read_and_process_file(
        self, file_name: str, frequency: str
    ) -> Optional[Tuple[pd.DataFrame, str]]:
        """讀取並處理文件
        參數:
            file_name: 文件路徑
            frequency: 數據頻率
        返回: (DataFrame, frequency) 或 None
        """
        try:
            # 檢查文件是否存在
            if not os.path.exists(file_name):
                self.show_error(f"找不到文件 '{file_name}'")
                return None

            # 讀取文件
            data = self._read_file(file_name)
            if data is None:
                return None

            # 標準化欄位名稱
            data = self.standardize_columns(data)
            
            # 檢測並轉換timestamp格式
            data = self.detect_and_convert_timestamp(data, "Time")

            # 顯示成功信息
            self._show_success_info(data)
            return data, frequency

        except Exception as e:
            self.show_error(f"讀取文件時出錯：{e}")
            return None

    def _read_file(self, file_name: str) -> Optional[pd.DataFrame]:
        """根據文件擴展名讀取文件
        參數:
            file_name: 文件路徑
        返回: DataFrame 或 None
        """
        if file_name.endswith(".xlsx"):
            return pd.read_excel(file_name)
        elif file_name.endswith(".csv"):
            return pd.read_csv(file_name)
        else:
            self.show_error("僅支援 .xlsx 或 .csv 文件")
            return None

    def _show_success_info(self, data: pd.DataFrame) -> None:
        """顯示成功載入信息
        參數:
            data: 載入的數據
        """
        print_dataframe_table(data.head(), title="數據加載成功，預覽（前5行）")
        self.show_success(f"數據加載成功，行數：{len(data)}")

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

        # 確保排序一致性
        return sorted(xlsx_files + csv_files)

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

        self.console.print(table)

        while True:
            # 如果只有一個文件，自動選擇
            if len(available_files) == 1:
                selected_file = available_files[0]
                self.show_success(
                    f"自動選擇唯一文件：{os.path.basename(selected_file)}"
                )
                return selected_file

            try:
                choice_input = self.get_user_input(
                    f"請選擇文件編號（1-{len(available_files)}，預設1）", "1"
                ).strip()
                choice = int(choice_input) if choice_input else 1
                if 1 <= choice <= len(available_files):
                    selected_file = available_files[choice - 1]
                    self.show_success(f"已選擇：{os.path.basename(selected_file)}")
                    return selected_file
                else:
                    self.show_error(f"請輸入 1-{len(available_files)} 之間的數字")
            except ValueError:
                self.show_error("請輸入有效的數字")

    def _input_file_path(self) -> Optional[str]:
        """要求用戶輸入完整文件路徑
        返回: str - 文件路徑或 None
        """
        file_name = self.get_user_input(
            "請輸入文件名稱（例如 D:/Python/data.xlsx 或 D:/Python/data.csv，按Enter跳過）",
            "",
        ).strip()
        if not file_name:
            self.show_info("跳過文件路徑輸入")
            return None
        return file_name

    def _standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """將數據欄位標準化為 Time, Open, High, Low, Close, Volume - now delegates to base class"""
        # First use base class standardization
        data = super().standardize_columns(data)

        # 檢查必要欄位
        required_cols = ["Time", "Open", "High", "Low", "Close"]
        missing_cols = [
            col for col in required_cols if col not in data.columns
        ]  # 標準 Python 列表推導式，檢查 pandas columns
        if missing_cols:
            self.show_warning(f"缺少欄位 {missing_cols}，將從用戶輸入補充")
            for col in missing_cols:
                data[col] = pd.NA  # 使用 pandas 的 pd.NA 填充缺失欄位

        # 處理 volume 欄位（可選）
        if "Volume" not in data.columns:  # 使用 pandas 的 columns 屬性檢查
            choice = (
                self.get_user_input("數據缺少 Volume 欄位，是否填充內容？(y/n)", None)
                .strip()
                .lower()
            )  # 標準 Python 輸入
            if choice == "y":
                data["Volume"] = pd.NA  # 使用 pandas 的 pd.NA 填充
            else:
                data["Volume"] = 0.0  # 使用 pandas 賦值 0.0

        return data
