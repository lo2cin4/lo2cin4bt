"""
plotter

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 量化回測框架的可視化平台，負責讀取 metricstracker 產生的 parquet 檔案，
並提供基於 Dash 的互動式可視化界面，顯示績效指標和權益曲線。

【關聯流程與數據流】
------------------------------------------------------------
- 主流程：讀取 parquet 檔案 → 解析參數 → 生成可視化界面 → 互動式分析
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[main.py] -->|調用| B[BasePlotter]
    B -->|讀取數據| C[DataImporter_plotter]
    B -->|生成界面| D[DashboardGenerator_plotter]
    B -->|處理回調| E[CallbackHandler_plotter]
    D -->|顯示圖表| F[ChartComponents_plotter]
    D -->|顯示指標| G[MetricsDisplay_plotter]
```

【主流程步驟與參數傳遞細節】
------------------------------------------------------------
- 由 main.py 啟動，依序調用數據載入、界面生成、回調處理
- BasePlotter 負責協調各子模組，管理整體流程
- **每次新增/修改主流程、參數結構、界面格式時，必須同步檢查本檔案與所有依賴模組**

【維護與擴充提醒】
------------------------------------------------------------
- 新增主流程步驟、參數、界面元素時，請同步更新頂部註解與對應模組
- 若參數結構有變動，需同步更新 BasePlotter、DashboardGenerator、CallbackHandler 等依賴模組

【常見易錯點】
------------------------------------------------------------
- 主流程與各模組流程不同步，導致參數遺漏或界面顯示錯誤
- 初始化環境未正確設置，導致下游模組報錯
- Dash 回調函數命名衝突或依賴關係錯誤

【範例】
------------------------------------------------------------
- 執行可視化平台：python main.py (選擇選項5)
- 自訂參數啟動：python main.py --plotter --config config.json

【與其他模組的關聯】
------------------------------------------------------------
- 調用 BasePlotter，協調 DataImporter_plotter、DashboardGenerator_plotter、CallbackHandler_plotter
- 參數結構依賴 metricstracker 產生的 parquet 檔案格式
- BasePlotter 負責界面生成與回調處理

【維護重點】
------------------------------------------------------------
- 新增/修改主流程、參數結構、界面格式時，務必同步更新本檔案與所有依賴模組
- DashboardGenerator 的界面生成邏輯與 CallbackHandler 的回調處理機制需要特別注意

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- DashboardGenerator 的界面生成與 CallbackHandler 的回調處理邏輯請參考對應模組
"""

from .Base_plotter import BasePlotter
from .DataImporter_plotter import DataImporterPlotter
from .DashboardGenerator_plotter import DashboardGenerator
from .CallbackHandler_plotter import CallbackHandler
from .ChartComponents_plotter import ChartComponents
from .MetricsDisplay_plotter import MetricsDisplay

__all__ = [
    'BasePlotter',
    'DataImporterPlotter', 
    'DashboardGenerator',
    'CallbackHandler',
    'ChartComponents',
    'MetricsDisplay'
] 