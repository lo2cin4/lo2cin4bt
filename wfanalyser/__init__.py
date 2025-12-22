"""
wfanalyser 模組

【功能說明】
------------------------------------------------------------
Walk-Forward Analysis (WFA) 滾動前向分析模組
提供參數優化、窗口劃分、結果可視化等功能

【主要模組】
------------------------------------------------------------
- Base_wfanalyser: 主控制器
- WalkForwardEngine: WFA 核心引擎
- ParameterOptimizer: 參數優化器
- BaseWFAPlotter: 可視化平台
"""

__version__ = "1.0.0"

from wfanalyser.Base_wfanalyser import BaseWFAAnalyser

__all__ = ["BaseWFAAnalyser"]

