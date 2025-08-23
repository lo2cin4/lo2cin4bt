"""
Lo2cin4BT backtester 目錄

【功能說明】
------------------------------------------------------------
本目錄為 Lo2cin4BT 回測框架核心，包含回測引擎、指標系統、交易模擬、結果導出等模組。

【流程與數據流】
------------------------------------------------------------
- 主流程或其他模組可直接從 backtester 匯入所需回測類別
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|from backtester import ...| B[各回測子模組]
    B -->|回測結果| C[用戶/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/刪除回測子模組時，請同步更新本檔案的 import 與 __all__
- 若回測類別名稱或介面有變動，需同步調整本檔案與主流程

【常見易錯點】
------------------------------------------------------------
- 忘記將新模組加入 __all__，導致外部無法正確匯入
- import 路徑錯誤會導致 ModuleNotFoundError

【範例】
------------------------------------------------------------
- from backtester import BaseBacktester, BacktestEngine
- from backtester import MovingAverageIndicator

【與其他模組的關聯】
------------------------------------------------------------
- 供 main.py 及其他模組統一匯入回測功能
- 依賴各子模組的正確實作與命名

【參考】
------------------------------------------------------------
- 詳細模組結構與匯入規範請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

from .Base_backtester import BaseBacktester
from .DataImporter_backtester import DataImporter
from .Indicators_backtester import IndicatorsBacktester
from .TradeRecorder_backtester import TradeRecorder_backtester
from .TradeRecordExporter_backtester import TradeRecordExporter_backtester
from .TradeSimulator_backtester import TradeSimulator_backtester

__all__ = [
    "DataImporter",
    "BaseBacktester",
    "IndicatorsBacktester",
    "TradeSimulator_backtester",
    "TradeRecorder_backtester",
    "TradeRecordExporter_backtester",
]
