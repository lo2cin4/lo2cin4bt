"""
DataImporter_metricstracker.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 績效分析框架的數據導入工具，負責從指定目錄載入 Parquet 格式的交易記錄檔案，支援檔案列表顯示和選擇功能。

【流程與數據流】
------------------------------------------------------------
- 由 BaseMetricTracker 調用，載入 Parquet 格式的交易記錄
- 載入數據後傳遞給 MetricsCalculator 進行績效計算

```mermaid
flowchart TD
    A[BaseMetricTracker] -->|調用| B[DataImporter]
    B -->|載入數據| C[MetricsCalculator]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改檔案格式支援時，請同步更新頂部註解與下游流程
- 若數據結構有變動，需同步更新本檔案與 MetricsCalculator
- 檔案格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 檔案路徑錯誤或檔案不存在會導致載入失敗
- 檔案格式不符會影響績效計算
- 數據結構變動會影響下游分析

【範例】
------------------------------------------------------------
- files = list_parquet_files(directory)
- selected = select_files(files, user_input)

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseMetricTracker 調用，數據傳遞給 MetricsCalculator
- 需與 MetricsCalculator 的數據結構保持一致

【參考】
------------------------------------------------------------
- pandas 官方文件
- Base_metricstracker.py、MetricsCalculator_metricstracker.py
- 專案 README
"""

import glob
import os

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def list_parquet_files(directory):
    """
    掃描指定資料夾下所有parquet檔案，回傳檔案路徑list。
    """
    pattern = os.path.join(directory, "*.parquet")
    return sorted(glob.glob(pattern))


def show_parquet_files(files):
    """
    列出所有parquet檔案，顯示編號與檔名。
    """
    table = Table(title="可用 Parquet 檔案", show_lines=True, border_style="#dbac30")
    table.add_column("編號", style="bold white", no_wrap=True)
    table.add_column("檔案名稱", style="bold white", no_wrap=True)

    for idx, file in enumerate(files, 1):
        table.add_row(
            f"[white]{idx}[/white]", f"[#1e90ff]{os.path.basename(file)}[/#1e90ff]"
        )

    console.print(table)


def select_files(files, user_input):
    """
    根據用戶輸入的編號字串，回傳所選檔案的完整路徑list。
    user_input: 字串，如 '1,2' 或 'all'
    """
    user_input = user_input.strip().lower()
    if user_input in ("all"):
        return files
    try:
        idxs = [int(x) for x in user_input.split(",") if x.strip().isdigit()]
        selected = [files[i - 1] for i in idxs if 1 <= i <= len(files)]
        if selected:
            return selected
        else:
            console.print(
                Panel(
                    "請輸入有效編號！\n建議：請確認編號在可用範圍內，或使用 'all' 選擇所有檔案。",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return []
    except Exception:
        console.print(
            Panel(
                "輸入格式錯誤，請重新輸入！\n建議：請使用數字編號（如 1,2,3）或 'all' 選擇所有檔案。",
                title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                border_style="#8f1511",
            )
        )
        return []
