"""
Percentile_Indicator_backtester.py

百分位技術指標模組

功能特點：
- 支援 PERC1-PERC6 六種細分策略，涵蓋不同交易邏輯
- 基於滾動百分位計算，提供動態閾值
- 支援單百分位和百分位區間兩種模式
- 內建 Numba JIT 優化，提升計算性能
- 支援向量化計算，適合批量參數優化

架構流程：
A[IndicatorsBacktester] -->|調用| B[Percentile_Indicator]
B -->|單指標計算| C[generate_signals]
B -->|批量向量化計算| D[vectorized_calculate_percentile_signals]
C -->|PERC1-PERC6| E[策略信號生成]

策略說明：
- PERC1：價格升穿 m 百分位做多
- PERC2：價格升穿 m 百分位做空
- PERC3：價格跌破 m 百分位做多
- PERC4：價格跌破 m 百分位做空
- PERC5：價格在 m1 與 m2 百分位之間做多
- PERC6：價格在 m1 與 m2 百分位之間做空

使用方式：
- 單指標計算：indicator = PercentileIndicator(data, params)
- 信號生成：signals = indicator.generate_signals(predictor)
- 批量向量化計算：signals_matrix = PercentileIndicator.vectorized_calculate_percentile_signals(tasks, predictor, signals_matrix, global_percentile_cache, data)  # noqa: E501
- 參數生成：params_list = PercentileIndicator.get_params(strat_idx, params_config)

依賴模組：
- numpy: 數值計算
- pandas: 數據處理
- numba: JIT 編譯優化
- backtester.IndicatorParams_backtester: 參數管理
"""

import logging

import numpy as np
import pandas as pd

from .IndicatorParams_backtester import IndicatorParams

# 優化：嘗試導入 Numba 進行 JIT 編譯加速
try:
    from numba import njit

    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False
    # Numba 未安裝，將使用標準 Python 計算

# 核心算法：純 Numba + ndarray 實現
if NUMBA_AVAILABLE:

    @njit(fastmath=True, cache=True)
    def _calculate_rolling_percentile_njit(data, window, percentile):
        """使用 Numba 計算滾動百分位"""
        n = len(data)
        result = np.zeros(n)

        for i in range(window - 1, n):
            # 提取窗口數據
            window_data = data[i - window + 1 : i + 1]
            # 使用 numpy 的百分位計算（更準確）
            result[i] = np.percentile(window_data, percentile)

        # 確保返回值的數據類型與輸入數據一致
        return result.astype(data.dtype)

    @njit(fastmath=True, cache=True)
    def _generate_percentile_signals_njit(
        predictor_values, percentile_values, strat_idx, window, m1=None, m2=None
    ):
        """
        使用 Numba 生成百分位信號
        全程使用 ndarray，無 pandas 依賴
        """
        n = len(predictor_values)
        signals = np.zeros(n)

        # 生成信號
        for i in range(1, n):
            if i < window - 1:
                continue

            prev_val = predictor_values[i - 1]  # 正確的前一日值
            curr_val = predictor_values[i]
            percentile_val = percentile_values[i]

            # 處理 NaN 值
            if np.isnan(prev_val) or np.isnan(curr_val) or np.isnan(percentile_val):
                continue

            if strat_idx == 1:  # PERC1：價格升穿 m 百分位做多
                if prev_val <= percentile_val and curr_val > percentile_val:
                    signals[i] = 1.0
            elif strat_idx == 2:  # PERC2：價格升穿 m 百分位做空
                if prev_val <= percentile_val and curr_val > percentile_val:
                    signals[i] = -1.0
            elif strat_idx == 3:  # PERC3：價格跌破 m 百分位做多
                if prev_val >= percentile_val and curr_val < percentile_val:
                    signals[i] = 1.0
            elif strat_idx == 4:  # PERC4：價格跌破 m 百分位做空
                if prev_val >= percentile_val and curr_val < percentile_val:
                    signals[i] = -1.0
            elif strat_idx == 5:  # PERC5：價格在 m1 與 m2 百分位之間做多
                if m1 is not None and m2 is not None:
                    if m1 < m2:  # 確保 m1 < m2
                        if curr_val >= m1 and curr_val <= m2:
                            signals[i] = 1.0
            elif strat_idx == 6:  # PERC6：價格在 m1 與 m2 百分位之間做空
                if m1 is not None and m2 is not None:
                    if m1 < m2:  # 確保 m1 < m2
                        if curr_val >= m1 and curr_val <= m2:
                            signals[i] = -1.0

        return signals


class PercentileIndicator:
    """
    百分位技術指標

    功能特點：
    - 支援 PERC1-PERC6 六種細分策略，涵蓋不同交易邏輯
    - 基於滾動百分位計算，提供動態閾值
    - 支援單百分位和百分位區間兩種模式
    - 內建 Numba JIT 優化，提升計算性能
    - 支援向量化計算，適合批量參數優化

    策略說明：
    - PERC1：價格升穿 m 百分位做多
    - PERC2：價格升穿 m 百分位做空
    - PERC3：價格跌破 m 百分位做多
    - PERC4：價格跌破 m 百分位做空
    - PERC5：價格在 m1 與 m2 百分位之間做多
    - PERC6：價格在 m1 與 m2 百分位之間做空
    """

    # 策略描述
    STRATEGY_DESCRIPTIONS = [
        "價格升穿 m 百分位做多",
        "價格升穿 m 百分位做空",
        "價格跌破 m 百分位做多",
        "價格跌破 m 百分位做空",
        "價格在 m1 與 m2 百分位之間做多",
        "價格在 m1 與 m2 百分位之間做空",
    ]

    # 策略別名映射
    STRATEGY_ALIASES = {
        "PERC1": 1,
        "PERC2": 2,
        "PERC3": 3,
        "PERC4": 4,
        "PERC5": 5,
        "PERC6": 6,
    }

    @staticmethod
    def get_strategy_descriptions():
        """獲取策略描述字典"""
        return {
            f"PERC{i + 1}": desc
            for i, desc in enumerate(PercentileIndicator.STRATEGY_DESCRIPTIONS)
        }

    @staticmethod
    def get_strategy_aliases():
        """獲取策略別名映射"""
        return PercentileIndicator.STRATEGY_ALIASES

    def __init__(self, data, params, logger=None):
        self.data = data  # 移除 .copy()，直接引用
        self.params = params
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @classmethod
    def get_params(cls, strat_idx=None, params_config=None):
        """
        參數必須完全由 UserInterface 層傳入，否則丟出 ValueError。
        不再於此處設定任何預設值。
        """
        if params_config is None:
            raise ValueError("params_config 必須由 UserInterface 提供，且不得為 None")

        # 如果沒有傳入 strat_idx，嘗試從 params_config 中提取
        if strat_idx is None:
            if "strat_idx" in params_config:
                strat_idx = params_config["strat_idx"]
            else:
                # 嘗試從別名中推斷策略索引
                if "alias" in params_config:
                    alias = params_config["alias"]
                    if alias in cls.STRATEGY_ALIASES:
                        strat_idx = cls.STRATEGY_ALIASES[alias]
                    else:
                        raise ValueError(f"無法從別名 {alias} 推斷策略索引")
                else:
                    raise ValueError(
                        "strat_idx 必須由 UserInterface 提供，或存在於 params_config 中"
                    )

        if "window_range" not in params_config:
            raise ValueError("window_range 必須由 UserInterface 提供")

        # 根據策略類型檢查不同的參數要求
        if strat_idx in [1, 2, 3, 4]:
            # 單百分位策略需要 percentile_range
            if "percentile_range" not in params_config:
                raise ValueError("percentile_range 必須由 UserInterface 提供")
        elif strat_idx in [5, 6]:
            # 雙百分位區間策略需要 m1_range 和 m2_range
            if "m1_range" not in params_config or "m2_range" not in params_config:
                raise ValueError("m1_range 與 m2_range 必須由 UserInterface 提供")
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")

        window_range = params_config["window_range"]

        # 處理窗口範圍參數
        start, end, step = map(int, window_range.split(":"))
        window_lengths = list(range(start, end + 1, step))

        param_list = []
        if strat_idx in [1, 2, 3, 4]:
            # 單百分位策略
            percentile_range = params_config["percentile_range"]
            # 處理百分位範圍參數
            p_start, p_end, p_step = map(int, percentile_range.split(":"))
            percentiles = list(range(p_start, p_end + 1, p_step))

            for window in window_lengths:
                for percentile in percentiles:
                    param = IndicatorParams("PERC")
                    param.add_param("strat_idx", strat_idx)
                    param.add_param("window", window)
                    param.add_param("percentile", percentile)
                    param_list.append(param)
        elif strat_idx in [5, 6]:
            # 雙百分位區間策略
            m1_range = params_config["m1_range"]
            m2_range = params_config["m2_range"]
            m1_start, m1_end, m1_step = map(int, m1_range.split(":"))
            m2_start, m2_end, m2_step = map(int, m2_range.split(":"))
            m1_list = list(range(m1_start, m1_end + 1, m1_step))
            m2_list = list(range(m2_start, m2_end + 1, m2_step))

            for window in window_lengths:
                for m1_val in m1_list:
                    for m2_val in m2_list:
                        if m1_val < m2_val:  # 確保 m1 < m2
                            param = IndicatorParams("PERC")
                            param.add_param("strat_idx", strat_idx)
                            param.add_param("window", window)
                            param.add_param("m1", m1_val)
                            param.add_param("m2", m2_val)
                            param_list.append(param)
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")
        return param_list

    def generate_signals(self, predictor=None):
        """
        根據 PERC 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算百分位，而非價格。

        strat=1: 預測因子升穿 m 百分位做多
        strat=2: 預測因子升穿 m 百分位做空
        strat=3: 預測因子跌破 m 百分位做多
        strat=4: 預測因子跌破 m 百分位做空
        strat=5: 預測因子在 m1 與 m2 百分位之間做多
        strat=6: 預測因子在 m1 與 m2 百分位之間做空
        """
        strat_idx = self.params.get_param("strat_idx")
        window = self.params.get_param("window")

        if strat_idx is None or window is None:
            raise ValueError("strat_idx, window 參數必須由外部提供")

        # 使用預測因子而非價格
        if predictor is None:
            predictor_series = self.data["Close"]
            self.logger.warning("未指定預測因子，使用 Close 價格作為預測因子")
        else:
            if predictor in self.data.columns:
                predictor_series = self.data[predictor]
            else:
                raise ValueError(
                    f"預測因子 '{predictor}' 不存在於數據中，可用欄位: {list(self.data.columns)}"
                )

        # 核心算法：使用純 Numba + ndarray
        if NUMBA_AVAILABLE:
            # 轉換為 ndarray，確保數據類型
            predictor_values = predictor_series.values.astype(np.float64)

            # 處理 NaN 值
            predictor_values = np.nan_to_num(predictor_values, nan=0.0)

            if strat_idx in [1, 2, 3, 4]:
                # 單百分位策略
                percentile = self.params.get_param("percentile")
                if percentile is None:
                    raise ValueError("percentile 參數必須由外部提供")

                # 計算滾動百分位
                percentile_values = _calculate_rolling_percentile_njit(
                    predictor_values, window, percentile
                )

                # 使用 Numba 計算信號
                signal_values = _generate_percentile_signals_njit(
                    predictor_values, percentile_values, strat_idx, window
                )
            else:
                # 雙百分位區間策略
                m1 = self.params.get_param("m1")
                m2 = self.params.get_param("m2")
                if m1 is None or m2 is None:
                    raise ValueError("m1, m2 參數必須由外部提供")

                # 計算兩個百分位值
                percentile1_values = _calculate_rolling_percentile_njit(
                    predictor_values, window, m1
                )
                _calculate_rolling_percentile_njit(predictor_values, window, m2)

                # 使用 Numba 計算信號
                signal_values = _generate_percentile_signals_njit(
                    predictor_values, percentile1_values, strat_idx, window, m1, m2
                )

            # 轉換回 pandas Series
            signal = pd.Series(signal_values, index=self.data.index)

            # 確保在有效期間之前不產生信號
            signal.iloc[: window - 1] = 0

            return signal

    def get_min_valid_index(self):
        window = self.params.get_param("window")
        if window is None:
            raise ValueError("window 參數必須由外部提供")
        return window - 1

    # ==================== 新增向量化批量計算功能 ====================

    @staticmethod
    def vectorized_calculate_percentile_signals(
        tasks, predictor, signals_matrix, global_percentile_cache=None, data=None
    ):
        """
        向量化計算百分位信號 - 批量處理多個參數組合，大幅提升計算效率

        Args:
            tasks: 任務列表，每個任務包含 (task_idx, indicator_idx, param)
            predictor: 預測因子名稱
            signals_matrix: 信號矩陣 [時間點, 任務數, 指標數]
            global_percentile_cache: 全局緩存字典，避免重複計算
            data: 數據DataFrame，如果為None則使用實例的data

        Returns:
            None (直接修改signals_matrix)
        """
        if data is None:
            raise ValueError("data參數必須提供")

        if global_percentile_cache is None:
            global_percentile_cache = {}

        # 提取所有PERC參數
        windows = []
        percentiles = []
        m1_values = []
        m2_values = []
        strat_indices = []
        task_indices = []
        indicator_indices = []

        for task_idx, indicator_idx, param in tasks:
            window = param.get_param("window")
            strat_idx = param.get_param("strat_idx", 1)

            if window is not None:
                windows.append(window)
                strat_indices.append(strat_idx)
                task_indices.append(task_idx)
                indicator_indices.append(indicator_idx)

                if strat_idx in [1, 2, 3, 4]:
                    percentile = param.get_param("percentile")
                    if percentile is not None:
                        percentiles.append(percentile)
                    else:
                        percentiles.append(50)  # 預設值
                else:  # strat_idx in [5, 6]
                    m1 = param.get_param("m1", 25)
                    m2 = param.get_param("m2", 75)
                    m1_values.append(m1)
                    m2_values.append(m2)
                    percentiles.append(50)  # 佔位符

        if not windows:
            return

        # 預處理數據 - 轉換為numpy數組並處理NaN值
        predictor_values = data[predictor].values.astype(np.float64)
        predictor_values = np.nan_to_num(predictor_values, nan=0.0)

        # 批量計算所有參數的百分位值 - 使用Numba優化
        unique_combinations = list(set(zip(windows, percentiles)))

        for window, percentile in unique_combinations:
            if window <= len(data):
                cache_key = (window, percentile, predictor)
                if cache_key not in global_percentile_cache:
                    # 使用Numba優化的函數計算百分位
                    if NUMBA_AVAILABLE:
                        percentile_values = _calculate_rolling_percentile_njit(
                            predictor_values, window, percentile
                        )
                    else:
                        # 備用方案：使用pandas rolling
                        percentile_values = (
                            data[predictor]
                            .rolling(window=window)
                            .quantile(percentile / 100.0)
                            .values
                        )
                        percentile_values = np.nan_to_num(percentile_values, nan=0.0)

                    global_percentile_cache[cache_key] = percentile_values

        # 為每個任務生成信號 - 使用Numba優化
        for i, (window, percentile, strat_idx, task_idx, indicator_idx) in enumerate(
            zip(windows, percentiles, strat_indices, task_indices, indicator_indices)
        ):
            try:
                if strat_idx in [1, 2, 3, 4]:
                    # 單百分位策略
                    cache_key = (window, percentile, predictor)
                    percentile_values = global_percentile_cache[cache_key]

                    # 使用Numba優化生成信號
                    if NUMBA_AVAILABLE:
                        signals = _generate_percentile_signals_njit(
                            predictor_values, percentile_values, strat_idx, window
                        )
                    else:
                        # 備用實現
                        signals = np.zeros(len(predictor_values))
                        prev_values = np.roll(predictor_values, 1)

                        for j in range(1, len(predictor_values)):
                            if j < window - 1:
                                continue

                            prev_val = prev_values[j]
                            curr_val = predictor_values[j]
                            percentile_val = percentile_values[j]

                            if strat_idx == 1:  # 升穿做多
                                if (
                                    prev_val <= percentile_val
                                    and curr_val > percentile_val
                                ):
                                    signals[j] = 1.0
                            elif strat_idx == 2:  # 升穿做空
                                if (
                                    prev_val <= percentile_val
                                    and curr_val > percentile_val
                                ):
                                    signals[j] = -1.0
                            elif strat_idx == 3:  # 跌破做多
                                if (
                                    prev_val >= percentile_val
                                    and curr_val < percentile_val
                                ):
                                    signals[j] = 1.0
                            elif strat_idx == 4:  # 跌破做空
                                if (
                                    prev_val >= percentile_val
                                    and curr_val < percentile_val
                                ):
                                    signals[j] = -1.0

                        # 確保在有效期間之前不產生信號
                        signals[: window - 1] = 0.0

                else:  # strat_idx in [5, 6]
                    # 雙百分位區間策略
                    m1 = m1_values[
                        i - len([x for x in strat_indices[:i] if x in [1, 2, 3, 4]])
                    ]
                    m2 = m2_values[
                        i - len([x for x in strat_indices[:i] if x in [1, 2, 3, 4]])
                    ]

                    # 計算兩個百分位值
                    cache_key1 = (window, m1, predictor)
                    cache_key2 = (window, m2, predictor)

                    if cache_key1 not in global_percentile_cache:
                        if NUMBA_AVAILABLE:
                            global_percentile_cache[cache_key1] = (
                                _calculate_rolling_percentile_njit(
                                    predictor_values, window, m1
                                )
                            )
                        else:
                            global_percentile_cache[cache_key1] = (
                                data[predictor]
                                .rolling(window=window)
                                .quantile(m1 / 100.0)
                                .values
                            )

                    if cache_key2 not in global_percentile_cache:
                        if NUMBA_AVAILABLE:
                            global_percentile_cache[cache_key2] = (
                                _calculate_rolling_percentile_njit(
                                    predictor_values, window, m2
                                )
                            )
                        else:
                            global_percentile_cache[cache_key2] = (
                                data[predictor]
                                .rolling(window=window)
                                .quantile(m2 / 100.0)
                                .values
                            )

                    percentile1_values = global_percentile_cache[cache_key1]
                    percentile2_values = global_percentile_cache[cache_key2]

                    # 生成區間信號
                    signals = np.zeros(len(predictor_values))

                    for j in range(window - 1, len(predictor_values)):
                        curr_val = predictor_values[j]
                        p1_val = percentile1_values[j]
                        p2_val = percentile2_values[j]

                        if m1 < m2:  # 確保 m1 < m2
                            if curr_val >= p1_val and curr_val <= p2_val:
                                signals[j] = 1.0 if strat_idx == 5 else -1.0

                    # 確保在有效期間之前不產生信號
                    signals[: window - 1] = 0.0

                signals_matrix[:, task_idx, indicator_idx] = signals

            except Exception as e:
                # 記錄錯誤但不中斷處理
                if hasattr(PercentileIndicator, "logger"):
                    PercentileIndicator.logger.warning(
                        f"百分位信號生成失敗 (task_idx={task_idx}, indicator_idx={indicator_idx}): {e}"
                    )
                signals_matrix[:, task_idx, indicator_idx] = 0

    @staticmethod
    def create_global_percentile_cache():
        """創建全局百分位緩存字典"""
        return {}

    @staticmethod
    def clear_global_percentile_cache(global_percentile_cache):
        """清理全局百分位緩存"""
        if global_percentile_cache is not None:
            global_percentile_cache.clear()

    @staticmethod
    def get_cache_info(global_percentile_cache):
        """獲取緩存信息"""
        if global_percentile_cache is None:
            return "緩存未初始化"

        cache_size = len(global_percentile_cache)
        cache_keys = list(global_percentile_cache.keys())

        info = f"緩存大小: {cache_size}"
        if cache_size > 0:
            info += f"\n緩存鍵: {cache_keys[:5]}"  # 只顯示前5個鍵
            if len(cache_keys) > 5:
                info += f" ... (還有 {len(cache_keys) - 5} 個)"

        return info
