"""
__init__.py

【功能說明】
------------------------------------------------------------
本檔案為 Lo2cin4BT 回測框架的初始化入口，統一匯入所有回測相關子模組（數據導入、主流程、指標、交易模擬、紀錄、導出等），便於主流程與外部模組統一調用。

【關聯流程與數據流】
------------------------------------------------------------
- 主流程或其他模組可直接從 backtester 匯入所需回測類別
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|from backtester import ...| B[各回測子模組]
    B -->|回測結果/紀錄| C[其他模組/導出]
```

【主控流程細節】
------------------------------------------------------------
- 匯入 DataImporter、BaseBacktester、IndicatorsBacktester、TradeSimulator_backtester、TradeRecorder_backtester、TradeRecordExporter_backtester 等
- __all__ 明確列出可供外部調用的類別與方法，控制模組導出
- 新增子模組時，需同步更新 import 與 __all__

【維護與擴充提醒】
------------------------------------------------------------
- 新增/刪除回測子模組時，請同步更新本檔案的 import 與 __all__
- 若類別名稱或介面有變動，需同步調整本檔案與主流程

【常見易錯點】
------------------------------------------------------------
- 忘記將新模組加入 __all__，導致外部無法正確匯入
- import 路徑錯誤會導致 ModuleNotFoundError

【範例】
------------------------------------------------------------
- from backtester import BaseBacktester, TradeRecordExporter_backtester
- BaseBacktester().run()

【與其他模組的關聯】
------------------------------------------------------------
- 供 main.py 及其他模組統一匯入回測與紀錄導出功能
- 依賴各子模組的正確實作與命名

【維護重點】
------------------------------------------------------------
- 新增/刪除回測子模組時，務必同步更新 import 與 __all__
- 保持與主流程、README 的同步

【參考】
------------------------------------------------------------
- 詳細模組結構與匯入規範請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

from .DataImporter_backtester import DataImporter
from .Base_backtester import BaseBacktester
from .Indicators_backtester import IndicatorsBacktester
from .TradeSimulator_backtester import TradeSimulator_backtester
from .TradeRecorder_backtester import TradeRecorder_backtester
from .TradeRecordExporter_backtester import TradeRecordExporter_backtester

__all__ = [
    "DataImporter",
    "BaseBacktester",
    "IndicatorsBacktester",
    "TradeSimulator_backtester",
    "TradeRecorder_backtester",
    "TradeRecordExporter_backtester",
]