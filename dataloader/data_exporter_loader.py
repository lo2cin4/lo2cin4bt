"""
data_exporter_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據導出模組，負責將處理後的行情數據、特徵、驗證結果等導出為 CSV、Excel、JSON 等格式，便於後續分析、保存與外部系統對接。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader、DataImporter、BacktestEngine、Calculator、Predictor、Validator 等模組調用，負責將最終數據導出
- 支援多種導出格式與欄位自訂，導出結果供用戶或外部系統分析

```mermaid
flowchart TD
    A[DataLoader/DataImporter/BacktestEngine/Calculator/Predictor/Validator] -->|產生數據| B(data_exporter_loader)
    B -->|導出| C[CSV/Excel/JSON]
    C -->|分析/保存| D[用戶/外部系統]
```

【維護與擴充重點】
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

【參考】
------------------------------------------------------------
- pandas 官方文件
- base_loader.py、calculator_loader、predictor_loader、validator_loader
- 專案 README
"""

import os  # 用於檔案路徑操作（本例中未直接使用，但可能用於後續擴展）

import pandas as pd
from rich.console import Console
from rich.panel import Panel

console = Console()


class DataExporter:
    def __init__(self, data: pd.DataFrame) -> None:
        """初始化 DataExporter，接受最終數據
        參數:
            data: pandas.DataFrame - 要導出的數據
        使用模組: pandas (pd)
        """
        self.data = data  # 將傳入的 pandas DataFrame 存儲為實例變量

    def export(self) -> None:
        """交互式導出數據為 JSON, CSV 或 XLSX，統一導出到 records 目錄"""
        try:
            console.print("[bold #dbac30]請選擇導出格式：[/bold #dbac30]")
            console.print("[bold white]1. CSV\n2. XLSX (Excel)\n3. JSON[/bold white]")
            while True:
                console.print("[bold #dbac30]輸入你的選擇（1, 2, 3）：[/bold #dbac30]")
                choice = input().strip()
                if choice in ["1", "2", "3"]:
                    break
                console.print(
                    Panel(
                        "❌ 錯誤：請輸入 1, 2 或 3。",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

            # 獲取輸出檔案名稱
            default_name = "output_data"
            console.print(
                f"[bold #dbac30]請輸入輸出檔案名稱（預設：{default_name}，不含副檔名）：[/bold #dbac30]"
            )
            file_name = input().strip() or default_name

            # 統一導出到 records 目錄
            records_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "records", "dataloader"
            )
            os.makedirs(records_dir, exist_ok=True)

            if choice == "1":
                file_path = os.path.join(records_dir, f"{file_name}.csv")
                self.data.to_csv(file_path, index=False)
                console.print(
                    Panel(
                        f"✅ 數據成功導出為 CSV：{file_path}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="green",
                    )
                )
            elif choice == "2":
                file_path = os.path.join(records_dir, f"{file_name}.xlsx")
                self.data.to_excel(file_path, index=False, engine="openpyxl")
                console.print(
                    Panel(
                        f"✅ 數據成功導出為 XLSX：{file_path}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="green",
                    )
                )
            else:
                file_path = os.path.join(records_dir, f"{file_name}.json")
                self.data.to_json(
                    file_path, orient="records", lines=True, date_format="iso"
                )
                console.print(
                    Panel(
                        f"✅ 數據成功導出為 JSON：{file_path}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="green",
                    )
                )

        except PermissionError:
            console.print(
                Panel(
                    f"❌ 錯誤：無法寫入檔案 '{file_path}'，請檢查權限或關閉已開啟的檔案",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="red",
                )
            )
        except Exception as e:
            console.print(
                Panel(
                    f"❌ 數據導出錯誤：{e}",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="red",
                )
            )
