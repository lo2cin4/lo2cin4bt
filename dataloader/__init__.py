"""
Lo2cin4BT dataloader 目錄

【功能說明】
------------------------------------------------------------
本目錄為 Lo2cin4BT 數據載入與預處理核心，包含各類行情數據來源、驗證、導出、預測、計算等模組。

【流程與數據流】
------------------------------------------------------------
- 主流程或其他模組可直接從 dataloader 匯入所需數據載入類別
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|from dataloader import ...| B[各數據載入子模組]
    B -->|載入數據/驗證/導出| C[其他模組/導出]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改數據來源、驗證、導出、計算、預測等模組時，務必同步更新所有相關模組頂部註解與 README
- Mermaid 流程圖、數據流、易錯點、維護重點等請詳列於各檔案頂部註解
- 任何結構變動請同步通知協作者並於 README 記錄

【常見易錯點】
------------------------------------------------------------
- 忘記將新模組加入 __all__，導致外部無法正確匯入
- import 路徑錯誤會導致 ModuleNotFoundError

【範例】
------------------------------------------------------------
- from dataloader import DataLoader, PredictorLoader, DataExporter
- DataLoader().load_data()

【與其他模組的關聯】
------------------------------------------------------------
- 供 main.py 及其他模組統一匯入數據載入與導出功能
- 依賴各子模組的正確實作與命名

【參考】
------------------------------------------------------------
- 詳細模組結構與匯入規範請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

from .base_loader import DataLoader
from .coinbase_loader import CoinbaseLoader
from .data_exporter_loader import DataExporter
from .predictor_loader import PredictorLoader

__all__ = ["DataLoader", "PredictorLoader", "DataExporter", "CoinbaseLoader"]
