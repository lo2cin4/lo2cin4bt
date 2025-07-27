"""
Indicators_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「指標協調與信號產生器」，統一管理所有技術指標的註冊、調用、參數組合產生與信號產生接口，支援多種指標型態與細分策略。

【流程與數據流】
------------------------------------------------------------
- 由 BacktestEngine 調用，根據用戶參數產生對應指標信號
- 調用各指標模組（如 MovingAverage、BollingerBand、NDayCycle）產生信號
- 主要數據流：

```mermaid
flowchart TD
    A[BacktestEngine] -->|調用| B[IndicatorsBacktester]
    B -->|產生信號| C[各指標模組]
    C -->|信號| D[BacktestEngine]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增指標時，請同步更新 indicator_map/new_indicators/alias_map/頂部註解
- 若指標參數結構有變動，需同步更新 IndicatorParams、BacktestEngine 等依賴模組
- 指標描述與細分型態如有調整，請於 README 詳列
- 新增/修改指標、參數結構、細分型態時，務必同步更新本檔案、IndicatorParams、BacktestEngine
- 指標註冊與描述需與主流程保持一致

【常見易錯點】
------------------------------------------------------------
- 指標註冊或參數結構未同步更新，導致信號產生錯誤
- 預測因子與指標數據對齊錯誤，易產生 NaN 或信號偏移
- 新增指標未正確加入 alias_map，導致無法調用

【錯誤處理】
------------------------------------------------------------
- 指標參數錯誤時提供詳細診斷
- 信號產生失敗時提供備用方案
- 數據對齊問題時提供修正建議

【範例】
------------------------------------------------------------
- 取得所有細分型態：IndicatorsBacktester().get_all_indicator_aliases()
- 產生參數組合：IndicatorsBacktester().get_indicator_params('MA1', params_config)
- 產生信號：IndicatorsBacktester().run_indicator('ma', data, params)

【與其他模組的關聯】
------------------------------------------------------------
- 由 BacktestEngine 調用，協調各指標模組產生信號
- 參數結構依賴 IndicatorParams

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本指標
- v1.1: 新增指標註冊機制
- v1.2: 新增細分型態支援

【參考】
------------------------------------------------------------
- 詳細指標規範與參數定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

import pandas as pd
import numpy as np
import logging
import os
import importlib
from .IndicatorParams_backtester import IndicatorParams
from typing import Optional

# 移除重複的logger設置，使用main.py中設置的logger
logger = logging.getLogger("lo2cin4bt")

pd.set_option('future.no_silent_downcasting', True)

# 註冊指標
INDICATOR_REGISTRY = {
    # 其他指標
    "NDayCycle": None,  # 實際信號產生於TradeSimulator
}

class IndicatorsBacktester:
    """
    指標集合點，負責調用各個 indicator 並提供通用部件。
    """
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger("IndicatorsBacktester")
        self.indicator_map = {
            'ma': 'MovingAverage_Indicator_backtester',
            # 之後可擴充更多指標
        }
        
        # 新的指標協調器 - 使用現有的指標模組
        self.new_indicators = {
            'MA': 'MovingAverage_Indicator_backtester',
            'BOLL': 'BollingerBand_Indicator_backtester',
            'NDayCycle': 'NDayCycle_Indicator_backtester'
        }
        # 細分型態對應表
        self.indicator_alias_map = self._build_indicator_alias_map()

    def _build_indicator_alias_map(self):
        alias_map = {}
        # MA
        try:
            module = importlib.import_module('backtester.MovingAverage_Indicator_backtester')
            if hasattr(module, 'MovingAverageIndicator'):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    # code: 'MA1'~'MA12'
                    idx = int(str(code).replace('MA', ''))
                    alias_map[code.upper()] = ('MA', idx)
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        # BOLL
        try:
            module = importlib.import_module('backtester.BollingerBand_Indicator_backtester')
            if hasattr(module, 'BollingerBandIndicator') and hasattr(module.BollingerBandIndicator, 'STRATEGY_DESCRIPTIONS'):
                for i, desc in enumerate(module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1):
                    if i <= 4:
                        alias_map[f"BOLL{i}"] = ('BOLL', i)
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        # NDayCycle
        alias_map["NDAY1"] = ('NDayCycle', 1)  # 開倉後N日做多
        alias_map["NDAY2"] = ('NDayCycle', 2)  # 開倉後N日做空
        return alias_map

    def get_all_indicator_aliases(self):
        """回傳所有可用細分型態（如 MA1、BOLL2...）"""
        return list(self.indicator_alias_map.keys())

    def get_indicator_params(self, indicator_type: str, params_config: Optional[dict] = None):
        """
        取得指定指標的所有參數組合（list of IndicatorParams），支援細分型態與參數配置
        """
        alias = self.indicator_alias_map.get(indicator_type.upper())
        # print(f"[DEBUG] indicator_type={indicator_type}, alias={alias}")
        if alias:
            main_type, strat_idx = alias
            module_name = self.new_indicators[main_type]
            # print(f"[DEBUG] main_type={main_type}, strat_idx={strat_idx}, module_name={module_name}")
            module = importlib.import_module(f"backtester.{module_name}")
            # print(f"[DEBUG] dir(module): {dir(module)}")
            # 修正 class 名稱取得，避免大小寫錯誤
            indicator_cls_name_map = {
                'MA': 'MovingAverageIndicator',
                'BOLL': 'BollingerBandIndicator',
                'NDayCycle': 'NDayCycleIndicator'
            }
            indicator_cls_name = indicator_cls_name_map.get(main_type, main_type.capitalize() + 'Indicator')
            if not indicator_cls_name:
                raise ValueError(f"無法取得 {main_type} 的指標 class 名稱")
            if hasattr(module, indicator_cls_name):
                indicator_cls = getattr(module, indicator_cls_name)
                # print(f"[DEBUG] indicator_cls={indicator_cls}, dir={dir(indicator_cls)}")
                if hasattr(indicator_cls, 'get_params'):
                    return indicator_cls.get_params(strat_idx, params_config)
            raise ValueError(f"指標 {indicator_type} 未實作 get_params() 方法")
        # 傳統主指標
        if indicator_type in self.new_indicators:
            module_name = self.new_indicators[indicator_type]
            module = importlib.import_module(f"backtester.{module_name}")
            if hasattr(module, 'get_params'):
                return module.get_params(params_config=params_config)
            elif hasattr(module, indicator_type.capitalize() + 'Indicator'):
                indicator_cls = getattr(module, indicator_type.capitalize() + 'Indicator')
                if hasattr(indicator_cls, 'get_params'):
                    return indicator_cls.get_params(params_config=params_config)
            raise ValueError(f"指標 {indicator_type} 未實作 get_params() 方法")
        raise ValueError(f"不支援的指標類型: {indicator_type}")

    def run_indicator(self, indicator_name, data, params):
        """
        調用指定 indicator 並產生信號。
        indicator_name: 'ma' 等
        """
        if indicator_name not in self.indicator_map:
            raise ValueError(f"未知指標: {indicator_name}")
        module_name = self.indicator_map[indicator_name]
        module = importlib.import_module(f"backtester.{module_name}")
        # 預設每個 indicator class 叫 Indicator 或 MovingAverageIndicator
        if hasattr(module, 'MovingAverageIndicator'):
            indicator_cls = getattr(module, 'MovingAverageIndicator')
        else:
            indicator_cls = getattr(module, 'Indicator')
        indicator = indicator_cls(data, params, logger=self.logger)
        return indicator.generate_signals(params.get('predictor'))
    
    # 新的協調器方法 - 與現有架構兼容
    def get_available_indicators(self):
        """獲取可用指標列表，並列出每個指標的說明"""
        indicator_descs = {}
        # MA
        try:
            module = importlib.import_module('backtester.MovingAverage_Indicator_backtester')
            if hasattr(module, 'MovingAverageIndicator'):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs:
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        # BOLL
        try:
            module = importlib.import_module('backtester.BollingerBand_Indicator_backtester')
            if hasattr(module, 'BollingerBandIndicator') and hasattr(module.BollingerBandIndicator, 'STRATEGY_DESCRIPTIONS'):
                for i, desc in enumerate(module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1):
                    indicator_descs[f"BOLL{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        # print所有指標與說明
        print("\n可用技術指標與說明：")
        for code, desc in indicator_descs.items():
            print(f"{code}: {desc}")
        return list(self.new_indicators.keys())
    
    def calculate_signals(self, indicator_type: str, data: pd.DataFrame, params: IndicatorParams, predictor: Optional[str] = None, entry_signal: Optional[pd.Series] = None):
        """計算指定指標的信號 - 使用現有的信號生成邏輯"""
        # 處理 NDayCycle 細分型態
        if indicator_type in ['NDAY1', 'NDAY2']:
            try:
                module = importlib.import_module('backtester.NDayCycle_Indicator_backtester')
                if hasattr(module, 'NDayCycleIndicator'):
                    return module.NDayCycleIndicator.calculate_signals(data, params, predictor, entry_signal)
            except Exception as e:
                self.logger.warning(f"無法調用 NDayCycle 信號產生: {e}")
                return pd.Series(0, index=data.index)
        
        # 處理主指標類型
        if indicator_type == 'MA':
            signals = self._calculate_ma_signals(data, params, predictor)
        elif indicator_type == 'BOLL':
            signals = self._calculate_boll_signals(data, params, predictor)
        else:
            raise ValueError(f"不支持的指標類型: {indicator_type}")
        
        if isinstance(signals, pd.DataFrame):
            print(f"[DEBUG] signals 是 DataFrame, columns: {signals.columns.tolist()}, shape: {signals.shape}")
            signals = signals.iloc[:, 0]
        if not isinstance(signals, pd.Series):
            print(f"[DEBUG] signals 型別異常: {type(signals)}, 內容: {signals}")
        return signals
    
    def _calculate_ma_signals(self, data, params, predictor=None):
        # print(f"[DEBUG] _calculate_ma_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")
        # print(f"[DEBUG] 預測因子存在於數據中：{predictor in data.columns if predictor else False}")
        
        try:
            module = importlib.import_module('backtester.MovingAverage_Indicator_backtester')
            indicator_cls = getattr(module, 'MovingAverageIndicator')
            # print(f"[DEBUG] 成功導入 MovingAverageIndicator")
            
            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")
            
            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")
            
            return signals
        except Exception as e:
            print(f"[DEBUG] MA 信號計算失敗：{e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _calculate_boll_signals(self, data, params, predictor=None):
        # print(f"[DEBUG] _calculate_boll_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")
        
        try:
            module = importlib.import_module('backtester.BollingerBand_Indicator_backtester')
            indicator_cls = getattr(module, 'BollingerBandIndicator')
            # print(f"[DEBUG] 成功導入 BollingerBandIndicator")
            
            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")
            
            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")
            
            return signals
        except Exception as e:
            print(f"[DEBUG] BOLL 信號計算失敗：{e}")
            import traceback
            traceback.print_exc()
            raise