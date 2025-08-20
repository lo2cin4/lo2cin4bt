"""
NDayCycle_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的 N日週期指標工具，負責產生基於時間週期的交易信號，支援順勢與反轉策略。
- 支援 NDAY1-NDAY2 兩種細分策略，涵蓋不同交易邏輯
- 提供可調整的週期天數參數，支援範圍設定
- 整合 Numba JIT 編譯優化，大幅提升計算性能
- 支援向量化批量計算，適合大規模參數組合回測
- 提供平倉信號生成機制，支援基於開倉信號的平倉邏輯

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生 N日週期信號
- 信號傳遞給 BacktestEngine 進行交易模擬
- 支援開倉信號與平倉信號的獨立生成

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[NDayCycle_Indicator]
    B -->|NDAY1| C[順勢策略]
    B -->|NDAY2| D[反轉策略]
    C -->|開倉信號| E[generate_signals]
    D -->|開倉信號| E
    E -->|平倉信號| F[generate_exit_signal_from_entry]
    E & F -->|信號| G[BacktestEngine]
```

【策略型態】
------------------------------------------------------------
- NDAY1：順勢策略，當前價格高於N日前價格做多，低於做空
- NDAY2：反轉策略，當前價格高於N日前價格做空，低於做多
- 平倉信號：基於開倉信號自動生成對應的平倉信號

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改指標型態、參數時，請同步更新頂部註解與下游流程
- 若指標邏輯有變動，需同步更新本檔案與 IndicatorsBacktester
- 指標參數如有調整，請同步通知協作者
- 向量化計算邏輯需要與單個指標計算保持一致
- Numba 優化需要確保跨平台兼容性
- 平倉信號生成邏輯需要與開倉信號保持一致

【常見易錯點】
------------------------------------------------------------
- 參數設置錯誤會導致信號產生異常
- 數據對齊問題會影響信號準確性
- 指標邏輯變動會影響下游交易模擬
- 平倉信號與開倉信號邏輯不一致
- 週期參數設置不當導致信號過於頻繁或稀少

【錯誤處理】
------------------------------------------------------------
- 參數驗證失敗時提供詳細錯誤信息
- 數據格式錯誤時提供修正建議
- Numba 編譯失敗時自動降級為標準 Python 計算
- 平倉信號生成失敗時提供備用方案

【範例】
------------------------------------------------------------
- 單指標計算：indicator = NDayCycleIndicator(data)
  signals = indicator.generate_signals(params, predictor)
- 批量向量化計算：NDayCycleIndicator.vectorized_calculate_ndaycycle_signals(tasks, predictor, signals_matrix, data)
- 平倉信號生成：exit_signals = NDayCycleIndicator.generate_exit_signal_from_entry(entry_signal, n, strat_idx)
- 參數生成：params_list = NDayCycleIndicator.get_params(strat_idx, params_config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 需與 IndicatorsBacktester 的指標介面保持一致
- 支援向量化計算，與 VectorBacktestEngine 配合
- 與 TradeSimulator 配合處理平倉邏輯

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本 N日週期指標
- v1.1: 新增順勢與反轉策略支援
- v1.2: 完善平倉信號生成機制
- v2.0: 整合 Numba JIT 編譯優化
- v2.1: 新增向量化批量計算
- v2.2: 完善錯誤處理與邏輯驗證

【參考】
------------------------------------------------------------
- pandas 官方文件：https://pandas.pydata.org/
- Numba 官方文檔：https://numba.pydata.org/
- Indicators_backtester.py、BacktestEngine_backtester.py
- 專案 README
"""

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from numba import njit

# 避免循環導入
try:
    from .Base_backtester import BaseIndicator, IndicatorParams
except ImportError:
    # 如果無法導入，定義一個基本的 BaseIndicator 類
    class BaseIndicator:
        def __init__(self, data):
            self.data = data
            self.name = ""

        def calculate(self, params):
            pass

        def generate_signals(self, params, predictor="Close"):
            pass

        def get_min_valid_index(self, params):
            pass

    class IndicatorParams:
        def __init__(self, indicator_type):
            self.indicator_type = indicator_type
            self.params = {}

        def add_param(self, key, value):
            self.params[key] = value

        def get_param(self, key, default=None):
            return self.params.get(key, default)


# 檢查 Numba 是否可用
try:
    from numba import njit

    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False

    def njit(*args, **kwargs):
        def decorator(func):
            return func

        return decorator


class NDayCycleIndicator(BaseIndicator):
    """
    N-Day Cycle 指標
    根據 N 天週期生成交易信號
    """

    def __init__(self, data: pd.DataFrame):
        super().__init__(data)
        self.name = "NDayCycle"

    def calculate(self, params: Dict[str, Any]) -> pd.Series:
        """
        計算 N-Day Cycle 指標

        Args:
            params: 包含以下參數的字典：
                - n: 週期天數
                - strat_idx: 策略索引 (1: 順勢, 2: 反轉)

        Returns:
            包含信號的 Series
        """
        n = params.get("n", 20)
        strat_idx = params.get("strat_idx", 1)

        if NUMBA_AVAILABLE:
            signals = _calculate_nday_signals_njit(
                self.data["Close"].values, n, strat_idx
            )
        else:
            signals = self._calculate_nday_signals_fallback(n, strat_idx)

        return pd.Series(signals, index=self.data.index)

    def _calculate_nday_signals_fallback(self, n: int, strat_idx: int) -> np.ndarray:
        """備用的 N-Day Cycle 信號計算"""
        close_prices = self.data["Close"].values
        signals = np.zeros(len(close_prices))

        for i in range(n, len(close_prices)):
            # 計算 N 天前的價格
            price_n_days_ago = close_prices[i - n]
            current_price = close_prices[i]

            # 生成信號
            if strat_idx == 1:  # 順勢策略
                if current_price > price_n_days_ago:
                    signals[i] = 1.0  # 買入信號
                elif current_price < price_n_days_ago:
                    signals[i] = -1.0  # 賣出信號
            elif strat_idx == 2:  # 反轉策略
                if current_price > price_n_days_ago:
                    signals[i] = -1.0  # 賣出信號
                elif current_price < price_n_days_ago:
                    signals[i] = 1.0  # 買入信號

        return signals

    def generate_signals(
        self, params: Dict[str, Any], predictor: str = "Close"
    ) -> pd.Series:
        """
        生成交易信號

        Args:
            params: 指標參數
            predictor: 預測因子名稱

        Returns:
            包含信號的 Series
        """
        return self.calculate(params)

    def get_min_valid_index(self, params: Dict[str, Any]) -> int:
        """
        獲取最小有效索引

        Args:
            params: 指標參數

        Returns:
            最小有效索引
        """
        n = params.get("n", 20)
        return n

    @classmethod
    def get_params(cls, strat_idx=None, params_config=None):
        """
        根據 strat_idx 和 params_config 生成參數列表

        Args:
            strat_idx: 策略索引 (1: 順勢, 2: 反轉)
            params_config: 參數配置字典，包含 n_range

        Returns:
            參數列表
        """
        if params_config is None:
            raise ValueError("params_config 必須由 UserInterface 提供，且不得為 None")

        if "n_range" not in params_config:
            raise ValueError("n_range 必須由 UserInterface 提供")

        n_range = params_config["n_range"]
        n_start, n_end, n_step = map(int, n_range.split(":"))
        n_list = list(range(n_start, n_end + 1, n_step))

        param_list = []
        for n in n_list:
            param = IndicatorParams("NDayCycle")
            param.add_param("n", n)
            param.add_param("strat_idx", strat_idx)
            param_list.append(param)

        return param_list

    @staticmethod
    def generate_exit_signal_from_entry(
        entry_signal: pd.Series, n: int, strat_idx: int
    ) -> pd.Series:
        """
        根據開倉信號生成平倉信號

        Args:
            entry_signal: 開倉信號
            n: 週期天數
            strat_idx: 策略索引

        Returns:
            平倉信號
        """
        if NUMBA_AVAILABLE:
            exit_signals = _generate_exit_signal_njit(entry_signal.values, n, strat_idx)
        else:
            exit_signals = NDayCycleIndicator._generate_exit_signal_fallback(
                entry_signal, n, strat_idx
            )

        result = pd.Series(exit_signals, index=entry_signal.index)
        return result

    @staticmethod
    def _generate_exit_signal_fallback(
        entry_signal: pd.Series, n: int, strat_idx: int
    ) -> np.ndarray:
        """備用的平倉信號生成"""
        exit_signals = np.zeros(len(entry_signal))

        for i in range(n, len(entry_signal)):
            if entry_signal.iloc[i] != 0:  # 如果有開倉信號
                # 在 N 天後生成相反的平倉信號
                if i + n < len(entry_signal):
                    # 平倉信號總是與開倉信號相反，無論策略類型
                    exit_signals[i + n] = -entry_signal.iloc[i]

        return exit_signals

    # ==================== 新增向量化功能 ====================

    @staticmethod
    def vectorized_calculate_ndaycycle_signals(
        tasks: List[Tuple],
        predictor: str,
        signals_matrix: np.ndarray,
        data: pd.DataFrame,
    ) -> None:
        """
        向量化計算 NDayCycle 信號 - 只生成純粹的 +1/-1/0 信號，不區分開平倉

        Args:
            tasks: 任務列表，每個元素為 (task_idx, indicator_idx, param)
            predictor: 預測因子名稱
            signals_matrix: 信號矩陣 [時間點, 任務數, 指標數]
            data: 數據DataFrame
        """
        for task_idx, indicator_idx, param in tasks:
            try:
                n = param.get_param("n")
                param.get_param("strat_idx", 1)

                if n is None:
                    signals_matrix[:, task_idx, indicator_idx] = 0
                    continue

                # NDayCycle 作為平倉指標時，不生成自己的信號，而是依賴開倉信號
                # 這裡只生成零信號，真正的平倉信號會在 handle_ndaycycle_exit_signals 中生成
                signal_array = np.zeros(len(data))

                signals_matrix[:, task_idx, indicator_idx] = signal_array

            except Exception:
                signals_matrix[:, task_idx, indicator_idx] = 0

    @staticmethod
    def handle_ndaycycle_exit_signals(
        exit_params_list: List,
        entry_signals: np.ndarray,
        exit_signals: np.ndarray,
        data: pd.DataFrame,
    ) -> np.ndarray:
        """
        處理 NDayCycle 退出信號

        Args:
            exit_params_list: 平倉參數列表
            entry_signals: 開倉信號矩陣 [時間點, 任務數]
            exit_signals: 平倉信號矩陣 [時間點, 任務數]
            data: 數據DataFrame

        Returns:
            更新後的平倉信號矩陣
        """
        for task_idx, exit_params in enumerate(exit_params_list):

            # exit_params 是一個列表，包含 IndicatorParams 對象
            if isinstance(exit_params, list) and len(exit_params) > 0:
                exit_param = exit_params[0]  # 取第一個參數
                if (
                    hasattr(exit_param, "indicator_type")
                    and exit_param.indicator_type == "NDayCycle"
                ):

                    n = exit_param.get_param("n")
                    strat_idx = exit_param.get_param("strat_idx")

                    if n is not None and strat_idx is not None:
                        # 使用合併後的開倉信號來生成平倉信號
                        entry_signal_series = pd.Series(
                            entry_signals[:, task_idx], index=data.index
                        )

                        # 檢查是否有開倉信號
                        if np.count_nonzero(entry_signal_series.values) > 0:
                            exit_signal_series = (
                                NDayCycleIndicator.generate_exit_signal_from_entry(
                                    entry_signal_series, n, strat_idx
                                )
                            )
                            exit_signals[:, task_idx] = exit_signal_series.values

        return exit_signals

    @staticmethod
    def process_ndaycycle_exit_signals_in_batch(
        exit_params_list: List,
        entry_signals: np.ndarray,
        exit_signals: np.ndarray,
        data: pd.DataFrame,
    ) -> np.ndarray:
        """
        批量處理 NDayCycle 平倉信號 - 向量化版本

        Args:
            exit_params_list: 平倉參數列表
            entry_signals: 開倉信號矩陣 [時間點, 任務數]
            exit_signals: 平倉信號矩陣 [時間點, 任務數]
            data: 數據DataFrame

        Returns:
            更新後的平倉信號矩陣
        """
        # 檢查每個任務的平倉參數，如果是 NDayCycle，則基於合併後的開倉信號重新生成平倉信號
        for task_idx, exit_params in enumerate(exit_params_list):
            if (
                len(exit_params) == 1
                and hasattr(exit_params[0], "indicator_type")
                and exit_params[0].indicator_type == "NDayCycle"
            ):

                n = exit_params[0].get_param("n")
                strat_idx = exit_params[0].get_param("strat_idx")

                if n is not None and strat_idx is not None:
                    # 基於合併後的開倉信號生成平倉信號
                    entry_signal_series = pd.Series(
                        entry_signals[:, task_idx], index=data.index
                    )

                    if np.count_nonzero(entry_signal_series.values) > 0:
                        exit_signal_series = (
                            NDayCycleIndicator.generate_exit_signal_from_entry(
                                entry_signal_series, n, strat_idx
                            )
                        )
                        exit_signals[:, task_idx] = exit_signal_series.values

        return exit_signals


# Numba 優化函數
if NUMBA_AVAILABLE:

    @njit(fastmath=True)
    def _calculate_nday_signals_njit(
        close_prices: np.ndarray, n: int, strat_idx: int
    ) -> np.ndarray:
        """Numba 優化的 N-Day Cycle 信號計算"""
        signals = np.zeros(len(close_prices))

        for i in range(n, len(close_prices)):
            # 計算 N 天前的價格
            price_n_days_ago = close_prices[i - n]
            current_price = close_prices[i]

            # 生成信號
            if strat_idx == 1:  # 順勢策略
                if current_price > price_n_days_ago:
                    signals[i] = 1.0  # 買入信號
                elif current_price < price_n_days_ago:
                    signals[i] = -1.0  # 賣出信號
            elif strat_idx == 2:  # 反轉策略
                if current_price > price_n_days_ago:
                    signals[i] = -1.0  # 賣出信號
                elif current_price < price_n_days_ago:
                    signals[i] = 1.0  # 買入信號

        return signals

    @njit(fastmath=True)
    def _generate_exit_signal_njit(
        entry_signals: np.ndarray, n: int, strat_idx: int
    ) -> np.ndarray:
        """Numba 優化的平倉信號生成"""
        exit_signals = np.zeros(len(entry_signals))

        for i in range(n, len(entry_signals)):
            if entry_signals[i] != 0.0:  # 如果有開倉信號
                # 在 N 天後生成相反的平倉信號
                if i + n < len(entry_signals):
                    # 平倉信號總是與開倉信號相反，無論策略類型
                    exit_signals[i + n] = -entry_signals[i]

        return exit_signals
