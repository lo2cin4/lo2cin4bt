"""
File_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據載入器，負責從本地 Excel、CSV 等檔案載入行情數據，支援多種格式、欄位自動標準化，並確保數據結構與下游模組一致。

【關聯流程與數據流】
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

【主控流程細節】
------------------------------------------------------------
- 互動式輸入檔案名稱，支援 .xlsx/.csv 格式
- 自動檢查檔案存在性，根據副檔名選擇讀取方式
- 內部自動標準化欄位名稱（Time, Open, High, Low, Close, Volume）
- 檢查必要欄位，缺失時提示用戶補充或自動填充
- 支援 Volume 欄位缺失時互動式補充

【維護與擴充提醒】
------------------------------------------------------------
- 新增/修改支援格式、欄位時，請同步更新頂部註解與下游流程
- 若欄位標準化邏輯有變動，需同步更新本檔案與 Base_loader
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
- 需與 Base_loader 介面保持一致

【維護重點】
------------------------------------------------------------
- 新增/修改支援格式、欄位、標準化邏輯時，務必同步更新本檔案與 Base_loader
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- Base_loader.py、DataValidator、ReturnCalculator
- 專案 README
"""
import pandas as pd  # 用於讀取 Excel/CSV 文件、數據處理（如重命名欄位、填充缺失值）
import os  # 用於檢查文件是否存在（os.path.exists）
import openpyxl  # 用於支持 Excel 文件讀取（pd.read_excel 的引擎）

class FileLoader:
    def load(self):
        """從 Excel 或 CSV 文件載入數據
        使用模組:
            - pandas (pd): 讀取 Excel/CSV 文件（read_excel, read_csv），數據處理
            - os: 檢查文件是否存在（os.path.exists）
            - openpyxl: 作為 pd.read_excel 的引擎支持 Excel 文件
        功能: 交互式輸入文件名，讀取 Excel/CSV 文件，標準化欄位並返回數據
        返回: pandas DataFrame 或 None（若載入失敗）
        """
        while True:
            file_name = input("請輸入文件名稱（例如 data.xlsx 或 data.csv）：").strip()
            frequency = input("請輸入時間間隔（例如 1d, 1h，預設 1d）：").strip() or "1d"
            try:
                # 檢查文件是否存在
                if not os.path.exists(file_name):  # 使用 os 模組檢查文件路徑
                    print(f"錯誤：找不到文件 '{file_name}'")
                    continue
                # 根據文件擴展名選擇讀取方式
                if file_name.endswith('.xlsx'):
                    data = pd.read_excel(file_name)  # 使用 pandas 的 read_excel，依賴 openpyxl
                elif file_name.endswith('.csv'):
                    data = pd.read_csv(file_name)  # 使用 pandas 的 read_csv
                else:
                    print("錯誤：僅支援 .xlsx 或 .csv 文件")
                    continue

                # 標準化欄位名稱
                data = self._standardize_columns(data)  # 調用內部方法，依賴 pandas
                print(f"數據加載成功，行數：{len(data)}")  # 使用標準 Python 的 len 函數
                return data, frequency
            except Exception as e:
                print(f"讀取文件時出錯：{e}")  # 標準 Python 異常處理

    def _standardize_columns(self, data):
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
            'Time': ['time', 'date', 'timestamp', 'Time', 'Date', 'Timestamp'],
            'Open': ['open', 'o', 'Open', 'O'],
            'High': ['high', 'h', 'High', 'H'],
            'Low': ['low', 'l', 'Low', 'L'],
            'Close': ['close', 'c', 'Close', 'C'],
            'Volume': ['volume', 'vol', 'Volume', 'Vol']
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
        required_cols = ['Time', 'Open', 'High', 'Low', 'Close']
        missing_cols = [col for col in required_cols if col not in data.columns]  # 標準 Python 列表推導式，檢查 pandas columns
        if missing_cols:
            print(f"警告：缺少欄位 {missing_cols}，將從用戶輸入補充")
            for col in missing_cols:
                data[col] = pd.NA  # 使用 pandas 的 pd.NA 填充缺失欄位

        # 處理 volume 欄位（可選）
        if 'Volume' not in data.columns:  # 使用 pandas 的 columns 屬性檢查
            choice = input("數據缺少 Volume 欄位，是否填充內容？(y/n)：").strip().lower()  # 標準 Python 輸入
            if choice == 'y':
                data['Volume'] = pd.NA  # 使用 pandas 的 pd.NA 填充
            else:
                data['Volume'] = 0.0  # 使用 pandas 賦值 0.0

        return data