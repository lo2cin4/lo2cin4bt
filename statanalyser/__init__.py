"""
Lo2cin4BT statanalyser 目錄

【功能說明】
------------------------------------------------------------
本目錄為 Lo2cin4BT 統計分析核心，包含各類統計檢定、分析與報表產生模組。

【流程與數據流】
------------------------------------------------------------
- 主流程或其他模組可直接從 statanalyser 匯入所需分析類別
- 主要數據流：

```mermaid
flowchart TD
    A[main.py/主流程] -->|from statanalyser import ...| B[各分析子模組]
    B -->|分析結果| C[ReportGenerator_statanalyser]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/刪除分析子模組時，請同步更新本檔案的 import 與 __all__
- 若分析類別名稱或介面有變動，需同步調整本檔案與主流程

【常見易錯點】
------------------------------------------------------------
- 忘記將新模組加入 __all__，導致外部無法正確匯入
- import 路徑錯誤會導致 ModuleNotFoundError

【範例】
------------------------------------------------------------
- from statanalyser import CorrelationTest, ReportGenerator
- select_predictor_factor(df, ...)

【與其他模組的關聯】
------------------------------------------------------------
- 供 main.py 及其他模組統一匯入分析與報表功能
- 依賴各子模組的正確實作與命名

【參考】
------------------------------------------------------------
- 詳細模組結構與匯入規範請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

from .AutocorrelationTest_statanalyser import AutocorrelationTest
from .Base_statanalyser import BaseStatAnalyser
from .CorrelationTest_statanalyser import CorrelationTest
from .DistributionTest_statanalyser import DistributionTest
from .ReportGenerator_statanalyser import ReportGenerator
from .SeasonalAnalysis_statanalyser import SeasonalAnalysis
from .StationarityTest_statanalyser import StationarityTest

# 從基類匯入 select_predictor_factor 方法
select_predictor_factor = BaseStatAnalyser.select_predictor_factor

__all__ = [
    "BaseStatAnalyser",
    "CorrelationTest",
    "StationarityTest",
    "AutocorrelationTest",
    "DistributionTest",
    "SeasonalAnalysis",
    "ReportGenerator",
    "select_predictor_factor",
]
