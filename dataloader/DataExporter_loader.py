"""
DataExporter_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據導出模組，負責將處理後的行情數據、特徵、驗證結果等導出為 CSV、Excel、JSON 等格式，便於後續分析、保存與外部系統對接。

【關聯流程與數據流】
------------------------------------------------------------
- 由 DataLoader、DataImporter、BacktestEngine、Calculator、Predictor、Validator 等模組調用，負責將最終數據導出
- 支援多種導出格式與欄位自訂，導出結果供用戶或外部系統分析

```mermaid
flowchart TD
    A[DataLoader/DataImporter/BacktestEngine/Calculator/Predictor/Validator] -->|產生數據| B(DataExporter_loader)
    B -->|導出| C[CSV/Excel/JSON]
    C -->|分析/保存| D[用戶/外部系統]
```

【主控流程細節】
------------------------------------------------------------
- 互動式選擇導出格式（CSV、XLSX、JSON），自訂檔名
- 統一導出至 records 目錄，確保專案結構一致
- 支援 pandas to_csv、to_excel、to_json 等標準導出方法
- 欄位結構與上游模組同步，確保資料一致性

【維護與擴充提醒】
------------------------------------------------------------
- 新增/修改導出格式、欄位時，請同步更新頂部註解與下游流程
- 若導出結構、欄位有變動，需同步更新本檔案與上游模組
- 導出格式或欄位結構如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 欄位結構不符會導致導出失敗或資料遺失
- 導出格式未同步更新會影響下游分析或外部系統對接
- 權限不足或檔案被佔用會導致導出失敗

【範例】
------------------------------------------------------------
- exporter = DataExporter(df)
  exporter.export()

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader、DataImporter、BacktestEngine、Calculator、Predictor、Validator 調用，協調數據導出
- 欄位結構依賴上游模組，需保持同步

【維護重點】
------------------------------------------------------------
- 新增/修改導出格式、欄位、結構時，務必同步更新本檔案與上游模組
- 欄位名稱、型態需與上游模組協調一致

【參考】
------------------------------------------------------------
- pandas 官方文件
- Base_loader.py、Calculator_loader、Predictor_loader、Validator_loader
- 專案 README
"""
import pandas as pd  # 用於數據處理和導出為 CSV、JSON、XLSX
import os  # 用於檔案路徑操作（本例中未直接使用，但可能用於後續擴展）
import openpyxl  # 用於 Excel 文件寫入（to_excel 方法需要）

class DataExporter:
    def __init__(self, data):
        """初始化 DataExporter，接受最終數據
        參數:
            data: pandas.DataFrame - 要導出的數據
        使用模組: pandas (pd)
        """
        self.data = data  # 將傳入的 pandas DataFrame 存儲為實例變量

    def export(self):
        """交互式導出數據為 JSON, CSV 或 XLSX，統一導出到 records 目錄"""
        try:
            print("\n=== 數據導出 ===")
            print("請選擇導出格式：")
            print("1. CSV")
            print("2. XLSX (Excel)")
            print("3. JSON")
            while True:
                choice = input("輸入你的選擇（1, 2, 3）：").strip()
                if choice in ['1', '2', '3']:
                    break
                print("錯誤：請輸入 1, 2 或 3。")

            # 獲取輸出檔案名稱
            default_name = "output_data"
            file_name = input(f"請輸入輸出檔案名稱（預設：{default_name}，不含副檔名）：").strip() or default_name

            # 統一導出到 records 目錄
            records_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "records")
            os.makedirs(records_dir, exist_ok=True)

            if choice == '1':
                file_path = os.path.join(records_dir, f"{file_name}.csv")
                self.data.to_csv(file_path, index=False)
                print(f"數據成功導出為 CSV：{file_path}")
            elif choice == '2':
                file_path = os.path.join(records_dir, f"{file_name}.xlsx")
                self.data.to_excel(file_path, index=False, engine='openpyxl')
                print(f"數據成功導出為 XLSX：{file_path}")
            else:
                file_path = os.path.join(records_dir, f"{file_name}.json")
                self.data.to_json(file_path, orient='records', lines=True, date_format='iso')
                print(f"數據成功導出為 JSON：{file_path}")

        except PermissionError:
            print(f"錯誤：無法寫入檔案 '{file_path}'，請檢查權限或關閉已開啟的檔案")
        except Exception as e:
            print(f"數據導出錯誤：{e}")