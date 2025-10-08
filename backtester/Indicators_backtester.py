"""
Indicators_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「指標協調與信號產生器」，統一管理所有技術指標的註冊、調用、參數組合產生與信號產生接口，支援多種指標型態與細分策略。
- 提供統一的指標註冊與管理機制
- 支援多種指標型態：MA（移動平均）、BOLL（布林通道）、NDAY（N日週期）
- 整合 Numba JIT 編譯優化，提升信號計算性能
- 提供向量化信號計算接口，支援批量處理

【流程與數據流】
------------------------------------------------------------
- 由 BacktestEngine 或 VectorBacktestEngine 調用，根據用戶參數產生對應指標信號
- 調用各指標模組（如 MovingAverage、BollingerBand）產生信號
- 主要數據流：

```mermaid
flowchart TD
    A[BacktestEngine/VBT] -->|調用| B[IndicatorsBacktester]
    B -->|產生信號| C[各指標模組]
    C -->|MA信號| D[MovingAverage_Indicator]
    C -->|BOLL信號| E[BollingerBand_Indicator]

    D & E & F -->|信號| G[BacktestEngine/VBT]
```

【指標支援】
------------------------------------------------------------
- MA 指標：支援 MA1-MA12，包含單均線、雙均線、多均線策略
- BOLL 指標：支援 BOLL1-BOLL4，包含突破、回歸等策略
- NDAY 指標：支援 NDAY1-NDAY2，包含順勢、反轉策略
- 所有指標支援 SMA、EMA、WMA 等均線型態

【維護與擴充重點】
------------------------------------------------------------
- 新增指標時，請同步更新 indicator_map/new_indicators/alias_map/頂部註解
- 若指標參數結構有變動，需同步更新 IndicatorParams、BacktestEngine 等依賴模組
- 指標描述與細分型態如有調整，請於 README 詳列
- 新增/修改指標、參數結構、細分型態時，務必同步更新本檔案、IndicatorParams、BacktestEngine
- 指標註冊與描述需與主流程保持一致
- 向量化計算邏輯需要與單個指標計算保持一致
- Numba 優化需要確保跨平台兼容性

【常見易錯點】
------------------------------------------------------------
- 指標註冊或參數結構未同步更新，導致信號產生錯誤
- 預測因子與指標數據對齊錯誤，易產生 NaN 或信號偏移
- 新增指標未正確加入 alias_map，導致無法調用
- 向量化計算與單個指標計算結果不一致
- Numba 編譯失敗時未提供備用方案

【錯誤處理】
------------------------------------------------------------
- 指標參數錯誤時提供詳細診斷
- 信號產生失敗時提供備用方案
- 數據對齊問題時提供修正建議
- Numba 編譯失敗時自動降級為標準 Python 計算
- 向量化計算錯誤時提供單個指標計算備用方案

【範例】
------------------------------------------------------------
- 取得所有細分型態：IndicatorsBacktester().get_all_indicator_aliases()
- 產生參數組合：IndicatorsBacktester().get_indicator_params('MA1', params_config)
- 產生信號：IndicatorsBacktester().run_indicator('ma', data, params)
- 向量化信號計算：IndicatorsBacktester().calculate_signals(indicator_type, data, params)

【與其他模組的關聯】
------------------------------------------------------------
- 由 BacktestEngine 或 VectorBacktestEngine 調用，協調各指標模組產生信號
- 參數結構依賴 IndicatorParams
- 與各指標模組（MovingAverage、BollingerBand）緊密耦合
- 支援向量化計算，與 VectorBacktestEngine 配合

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本指標
- v1.1: 新增指標註冊機制
- v1.2: 新增細分型態支援
- v2.0: 整合 Numba JIT 編譯優化
- v2.1: 新增向量化信號計算接口
- v2.2: 完善錯誤處理與備用方案

【參考】
------------------------------------------------------------
- 詳細指標規範與參數定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
- Numba 官方文檔：https://numba.pydata.org/
- 技術指標計算與信號產生最佳實踐
"""

import importlib
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .IndicatorParams_backtester import IndicatorParams

# 移除重複的logger設置，使用main.py中設置的logger
logger = logging.getLogger("lo2cin4bt")

pd.set_option("future.no_silent_downcasting", True)

# 優化：嘗試導入 Numba 進行 JIT 編譯加速
try:
    from numba import njit

    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False
    print("Numba 未安裝，將使用標準 Python 計算。建議安裝 numba 以獲得更好的性能。")

# 核心算法：純 Numba + ndarray 實現
if NUMBA_AVAILABLE:

    @njit(fastmath=True)
    def _combine_signals_njit(signals_list: List[np.ndarray]) -> np.ndarray:  # pylint: disable=unused-argument
        """
        使用 Numba 合併多個信號序列
        """
        if len(signals_list) == 0:
            return np.zeros(0)

        n = len(signals_list[0])
        result = np.zeros(n)

        for i in range(n):
            for signal in signals_list:
                if i < len(signal):
                    result[i] += signal[i]

        return result


# 註冊指標


class IndicatorsBacktester:
    """
    指標集合點，負責調用各個 indicator 並提供通用部件。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):  # pylint: disable=unused-argument
        self.logger = logger or logging.getLogger("IndicatorsBacktester")
        self.indicator_map = {
            "ma": "MovingAverage_Indicator_backtester",
            # 之後可擴充更多指標
        }

        # 新的指標協調器 - 使用現有的指標模組
        self.new_indicators = {
            "MA": "MovingAverage_Indicator_backtester",
            "BOLL": "BollingerBand_Indicator_backtester",
            "HL": "HL_Indicator_backtester",
            "PERC": "Percentile_Indicator_backtester",
            "VALUE": "VALUE_Indicator_backtester",
        }

        # 細分型態對應表
        self.indicator_alias_map = self._build_indicator_alias_map()

    def _build_indicator_alias_map(self) -> Dict[str, Tuple[str, int]]:  # pylint: disable=too-complex
        alias_map = {}
        # MA
        try:
            module = importlib.import_module(
                "backtester.MovingAverage_Indicator_backtester"
            )
            if hasattr(module, "MovingAverageIndicator"):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    # code: 'MA1'~'MA12'
                    idx = int(str(code).replace("MA", ""))
                    alias_map[code.upper()] = ("MA", idx)
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        # BOLL
        try:
            module = importlib.import_module(
                "backtester.BollingerBand_Indicator_backtester"
            )
            if hasattr(module, "BollingerBandIndicator") and hasattr(
                module.BollingerBandIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(
                    module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1
                ):
                    if i <= 4:
                        alias_map[f"BOLL{i}"] = ("BOLL", i)
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        # HL
        try:
            module = importlib.import_module("backtester.HL_Indicator_backtester")
            if hasattr(module, "HLIndicator") and hasattr(
                module.HLIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(module.HLIndicator.STRATEGY_DESCRIPTIONS, 1):
                    if i <= 4:
                        alias_map[f"HL{i}"] = ("HL", i)
        except Exception as e:
            self.logger.warning(f"無法獲取HL指標描述: {e}")
        # PERC
        try:
            module = importlib.import_module(
                "backtester.Percentile_Indicator_backtester"
            )
            if hasattr(module, "PercentileIndicator") and hasattr(
                module.PercentileIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(
                    module.PercentileIndicator.STRATEGY_DESCRIPTIONS, 1
                ):
                    if i <= 6:
                        alias_map[f"PERC{i}"] = ("PERC", i)
        except Exception as e:
            self.logger.warning(f"無法獲取PERC指標描述: {e}")
        # VALUE
        try:
            module = importlib.import_module("backtester.VALUE_Indicator_backtester")
            if hasattr(module, "VALUEIndicator") and hasattr(
                module.VALUEIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(
                    module.VALUEIndicator.STRATEGY_DESCRIPTIONS, 1
                ):
                    if i <= 6:
                        alias_map[f"VALUE{i}"] = ("VALUE", i)
        except Exception as e:
            self.logger.warning(f"無法獲取VALUE指標描述: {e}")

        return alias_map

    def get_all_indicator_aliases(self) -> List[str]:
        """
        回傳所有可用細分型態（如 MA1、BOLL2...）

        Returns:
            list: 所有可用指標細分型態的列表
        """
        return list(self.indicator_alias_map.keys())

    def get_indicator_params(  # pylint: disable=unused-argument
        self, indicator_type: str, params_config: Optional[dict] = None
    ) -> List[Any]:
        """
        取得指定指標的所有參數組合（list of IndicatorParams），支援細分型態與參數配置

        Args:
            indicator_type (str): 指標類型，如 'MA1', 'BOLL2' 等
            params_config (Optional[dict]): 參數配置字典

        Returns:
            list: IndicatorParams 物件列表，包含所有參數組合

        Raises:
            ValueError: 當指標類型不支援或未實作 get_params 方法時
        """
        alias = self.indicator_alias_map.get(indicator_type.upper())
        # print(f"[DEBUG] indicator_type={indicator_type}, alias={alias}")
        if alias:
            main_type: str
            strat_idx: int
            main_type, strat_idx = alias
            module_name = self.new_indicators[main_type]
            # print(f"[DEBUG] main_type={main_type}, strat_idx={strat_idx}, module_name={module_name}")
            module = importlib.import_module(f"backtester.{module_name}")
            # print(f"[DEBUG] dir(module): {dir(module)}")
            # 修正 class 名稱取得，避免大小寫錯誤
            indicator_cls_name_map = {
                "MA": "MovingAverageIndicator",
                "BOLL": "BollingerBandIndicator",
                "HL": "HLIndicator",
                "PERC": "PercentileIndicator",
                "VALUE": "VALUEIndicator",
            }
            indicator_cls_name = indicator_cls_name_map.get(
                main_type, main_type.capitalize() + "Indicator"
            )
            if not indicator_cls_name:
                raise ValueError(f"無法取得 {main_type} 的指標 class 名稱")
            if hasattr(module, indicator_cls_name):
                indicator_cls = getattr(module, indicator_cls_name)
                # print(f"[DEBUG] indicator_cls={indicator_cls}, dir={dir(indicator_cls)}")
                if hasattr(indicator_cls, "get_params"):
                    # 優先使用 params_config 中的 strat_idx，如果沒有則使用指標類型的 strat_idx
                    actual_strat_idx = strat_idx
                    if params_config and "strat_idx" in params_config:
                        actual_strat_idx = params_config["strat_idx"]
                    return indicator_cls.get_params(actual_strat_idx, params_config)
            raise ValueError(f"指標 {indicator_type} 未實作 get_params() 方法")
        raise ValueError(f"不支援的指標類型: {indicator_type}")

    def run_indicator(
        self, indicator_name: str, data: pd.DataFrame, params: Dict[str, Any]
    ) -> np.ndarray:  # pylint: disable=unused-argument
        """
        調用指定 indicator 並產生信號

        Args:
            indicator_name (str): 指標名稱，如 'ma' 等
            data (pd.DataFrame): 輸入數據
            params (dict): 指標參數

        Returns:
            pd.Series: 產生的信號序列

        Raises:
            ValueError: 當指標名稱未知時
        """
        if indicator_name not in self.indicator_map:
            raise ValueError(f"未知指標: {indicator_name}")
        module_name = self.indicator_map[indicator_name]
        module = importlib.import_module(f"backtester.{module_name}")
        # 預設每個 indicator class 叫 Indicator 或 MovingAverageIndicator
        if hasattr(module, "MovingAverageIndicator"):
            indicator_cls = getattr(module, "MovingAverageIndicator")
        else:
            indicator_cls = getattr(module, "Indicator")
        indicator = indicator_cls(data, params, logger=self.logger)
        return indicator.generate_signals(params.get("predictor"))

    # 新的協調器方法 - 與現有架構兼容
    def get_available_indicators(self) -> List[str]:  # pylint: disable=too-complex
        """獲取可用指標列表，並列出每個指標的說明"""
        indicator_descs = {}
        # MA
        try:
            module = importlib.import_module(
                "backtester.MovingAverage_Indicator_backtester"
            )
            if hasattr(module, "MovingAverageIndicator"):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs:
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        # BOLL
        try:
            module = importlib.import_module(
                "backtester.BollingerBand_Indicator_backtester"
            )
            if hasattr(module, "BollingerBandIndicator") and hasattr(
                module.BollingerBandIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(
                    module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1
                ):
                    indicator_descs[f"BOLL{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        # PERC (added)
        try:
            module = importlib.import_module(
                "backtester.Percentile_Indicator_backtester"
            )
            if hasattr(module, "PercentileIndicator") and hasattr(
                module.PercentileIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(
                    module.PercentileIndicator.STRATEGY_DESCRIPTIONS, 1
                ):
                    indicator_descs[f"PERC{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取PERC指標描述: {e}")
        # print所有指標與說明
        print("\n可用技術指標與說明：")
        for code, desc in indicator_descs.items():
            print(f"{code}: {desc}")
        return list(self.new_indicators.keys())

    def calculate_signals(  # pylint: disable=unused-argument
        self,
        indicator_type: str,
        data: pd.DataFrame,
        params: "IndicatorParams",
        predictor: Optional[str] = None,
        entry_signal: Optional[pd.Series] = None,
    ) -> np.ndarray:
        """
        計算指標信號，支援快取機制
        """
        # 根據指標類型調用對應的計算方法
        if indicator_type == "MA":
            signals = self._calculate_ma_signals(data, params, predictor)
        elif indicator_type == "BOLL":
            signals = self._calculate_boll_signals(data, params, predictor)
        elif indicator_type == "HL":
            signals = self._calculate_hl_signals(data, params, predictor)
        elif indicator_type == "VALUE":
            signals = self._calculate_value_signals(data, params, predictor)
        elif indicator_type == "PERC":
            signals = self._calculate_percentile_signals(data, params, predictor)
        else:
            raise ValueError(f"不支援的指標類型: {indicator_type}")

        return signals

    def _calculate_ma_signals(
        self, data: pd.DataFrame, params: "IndicatorParams", predictor: Optional[str] = None
    ) -> np.ndarray:  # pylint: disable=unused-argument
        # print(f"[DEBUG] _calculate_ma_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")
        # print(f"[DEBUG] 預測因子存在於數據中：{predictor in data.columns if predictor else False}")

        try:
            # 動態導入模組
            module = importlib.import_module(
                "backtester.MovingAverage_Indicator_backtester"
            )
            indicator_cls = getattr(module, "MovingAverageIndicator")
            # print(f"[DEBUG] 成功導入 MovingAverageIndicator")

            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")

            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")

            return signals
        except Exception:
            # MA 信號計算失敗：{e}
            import traceback

            traceback.print_exc()
            raise

    def _calculate_boll_signals(
        self, data: pd.DataFrame, params: "IndicatorParams", predictor: Optional[str] = None
    ) -> np.ndarray:  # pylint: disable=unused-argument
        # print(f"[DEBUG] _calculate_boll_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")

        try:
            # 動態導入模組
            module = importlib.import_module(
                "backtester.BollingerBand_Indicator_backtester"
            )
            indicator_cls = getattr(module, "BollingerBandIndicator")
            # print(f"[DEBUG] 成功導入 BollingerBandIndicator")

            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")

            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")

            return signals
        except Exception:
            # BOLL 信號計算失敗：{e}
            import traceback

            traceback.print_exc()
            raise

    def _calculate_hl_signals(
        self, data: pd.DataFrame, params: "IndicatorParams", predictor: Optional[str] = None
    ) -> np.ndarray:  # pylint: disable=unused-argument
        # print(f"[DEBUG] _calculate_hl_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")

        try:
            # 動態導入模組
            module = importlib.import_module("backtester.HL_Indicator_backtester")
            indicator_cls = getattr(module, "HLIndicator")
            # print(f"[DEBUG] 成功導入 HLIndicator")

            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")

            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")

            return signals
        except Exception:
            # HL 信號計算失敗：{e}
            import traceback

            traceback.print_exc()
            raise

    def _calculate_value_signals(
        self, data: pd.DataFrame, params: "IndicatorParams", predictor: Optional[str] = None
    ) -> np.ndarray:  # pylint: disable=unused-argument
        # print(f"[DEBUG] _calculate_value_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")

        try:
            # 動態導入模組
            module = importlib.import_module("backtester.VALUE_Indicator_backtester")
            indicator_cls = getattr(module, "VALUEIndicator")
            # print(f"[DEBUG] 成功導入 VALUEIndicator")

            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")

            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")

            return signals
        except Exception:
            # VALUE 信號計算失敗：{e}
            import traceback

            traceback.print_exc()
            raise

    def _calculate_percentile_signals(
        self, data: pd.DataFrame, params: "IndicatorParams", predictor: Optional[str] = None
    ) -> np.ndarray:  # pylint: disable=unused-argument
        # print(f"[DEBUG] _calculate_percentile_signals 開始")
        # print(f"[DEBUG] 數據形狀：{data.shape}")
        # print(f"[DEBUG] 預測因子：{predictor}")

        try:
            # 動態導入模組
            module = importlib.import_module(
                "backtester.Percentile_Indicator_backtester"
            )
            indicator_cls = getattr(module, "PercentileIndicator")
            # print(f"[DEBUG] 成功導入 PercentileIndicator")

            indicator = indicator_cls(data, params, logger=self.logger)
            # print(f"[DEBUG] 成功創建指標實例")

            signals = indicator.generate_signals(predictor)
            # print(f"[DEBUG] 信號生成完成，信號形狀：{signals.shape}")
            # print(f"[DEBUG] 信號分佈：{signals.value_counts().to_dict()}")

            return signals
        except Exception:
            # PERC 信號計算失敗：{e}
            import traceback

            traceback.print_exc()
            raise

