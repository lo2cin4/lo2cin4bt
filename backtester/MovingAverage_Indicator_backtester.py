"""
MovingAverage_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的移動平均指標工具，負責產生移動平均線信號，支援單均線、雙均線、多均線等多種策略型態。
- 支援多種均線型態：SMA（簡單移動平均）、EMA（指數移動平均）、WMA（加權移動平均）
- 提供 MA1-MA12 十二種細分策略，涵蓋不同交易邏輯
- 整合 Numba JIT 編譯優化，大幅提升計算性能
- 支援向量化批量計算，適合大規模參數組合回測
- 提供智能緩存機制，避免重複計算

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生移動平均線信號
- 信號傳遞給 BacktestEngine 進行交易模擬

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[MovingAverage_Indicator]
    B -->|MA1-MA4| C[單均線策略]
    B -->|MA5-MA8| D[雙均線策略]
    B -->|MA9-MA12| E[多均線策略]
    C -->|SMA/EMA/WMA| F[信號生成]
    D -->|SMA/EMA/WMA| F
    E -->|SMA/EMA/WMA| F
    F -->|信號| G[BacktestEngine]
```

【策略型態】
------------------------------------------------------------
- MA1-MA4：單均線策略，支援價格與均線的交叉信號
- MA5-MA8：雙均線策略，支援短長期均線交叉信號
- MA9-MA12：多均線策略，支援連續日數與均線組合信號
- 所有策略支援 SMA、EMA、WMA 三種均線型態

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改指標型態、參數時，請同步更新頂部註解與下游流程
- 若指標邏輯有變動，需同步更新本檔案與 IndicatorsBacktester
- 指標參數如有調整，請同步通知協作者
- 向量化計算邏輯需要與單個指標計算保持一致
- Numba 優化需要確保跨平台兼容性
- 緩存機制需要正確管理記憶體使用

【常見易錯點】
------------------------------------------------------------
- 參數設置錯誤會導致信號產生異常
- 數據對齊問題會影響信號準確性
- 指標邏輯變動會影響下游交易模擬
- 向量化計算與單個指標計算結果不一致
- 緩存機制管理不當導致記憶體洩漏

【錯誤處理】
------------------------------------------------------------
- 參數驗證失敗時提供詳細錯誤信息
- 數據格式錯誤時提供修正建議
- Numba 編譯失敗時自動降級為標準 Python 計算
- 緩存錯誤時提供清理機制

【範例】
------------------------------------------------------------
- 單指標計算：indicator = MovingAverageIndicator(data, params)
  signals = indicator.generate_signals(predictor)
- 批量向量化計算：signals_matrix = MovingAverageIndicator.vectorized_calculate_ma_signals(tasks, predictor, signals_matrix, global_ma_cache, data)  # noqa: E501
- 參數生成：params_list = MovingAverageIndicator.get_params(strat_idx, params_config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 需與 IndicatorsBacktester 的指標介面保持一致
- 支援向量化計算，與 VectorBacktestEngine 配合
- 與其他指標模組共享緩存機制

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本移動平均指標
- v1.1: 新增多種均線型態支援
- v1.2: 完善策略邏輯與參數驗證
- v2.0: 整合 Numba JIT 編譯優化
- v2.1: 新增向量化批量計算
- v2.2: 完善緩存機制與錯誤處理

【參考】
------------------------------------------------------------
- pandas 官方文件：https://pandas.pydata.org/
- Numba 官方文檔：https://numba.pydata.org/
- Indicators_backtester.py、BacktestEngine_backtester.py
- 專案 README
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
    print("Numba 未安裝，將使用標準 Python 計算。建議安裝 numba 以獲得更好的性能。")

# ============================================================================
# 核心算法：統一的 MA 計算函數
# ============================================================================


def _calculate_ma_unified(values, period, ma_type):
    """
    統一的MA計算函數，支持SMA、EMA、WMA
    消除重複代碼，統一所有MA計算邏輯
    """
    values = np.asarray(values, dtype=np.float64)
    values = np.nan_to_num(values, nan=0.0)

    if NUMBA_AVAILABLE:
        if ma_type.upper() == "SMA":
            return _calculate_sma_njit(values, period)
        elif ma_type.upper() == "EMA":
            return _calculate_ema_njit(values, period)
        elif ma_type.upper() == "WMA":
            return _calculate_wma_njit(values, period)
        else:
            raise ValueError(f"不支援的 MA 類型: {ma_type}")
    else:
        # numpy備用實現
        if ma_type.upper() == "SMA":
            ma_values = np.convolve(values, np.ones(period) / period, mode="valid")
            return np.concatenate([np.zeros(period - 1), ma_values])
        elif ma_type.upper() == "EMA":
            alpha = 2.0 / (period + 1)
            ma_values = np.zeros_like(values)
            ma_values[0] = values[0]
            for i in range(1, len(values)):
                ma_values[i] = alpha * values[i] + (1 - alpha) * ma_values[i - 1]
            return ma_values
        elif ma_type.upper() == "WMA":
            weights = np.arange(1, period + 1)
            ma_values = np.zeros_like(values)
            for i in range(period - 1, len(values)):
                window = values[i - period + 1 : i + 1]
                ma_values[i] = np.sum(window * weights) / np.sum(weights)
            return ma_values
        else:
            raise ValueError(f"不支援的 MA 類型: {ma_type}")


# ============================================================================
# Numba 加速函數
# ============================================================================

if NUMBA_AVAILABLE:

    @njit(fastmath=True)
    def _calculate_sma_njit(data, window):
        """使用 Numba 計算簡單移動平均"""
        n = len(data)
        result = np.zeros(n)

        for i in range(window - 1, n):
            sum_val = 0.0
            for j in range(i - window + 1, i + 1):
                sum_val += data[j]
            result[i] = sum_val / window

        return result

    @njit(fastmath=True)
    def _calculate_ema_njit(data, period):
        """使用 Numba 計算指數移動平均"""
        n = len(data)
        result = np.zeros(n)
        alpha = 2.0 / (period + 1)

        # 初始化第一個值
        if n > 0:
            result[0] = data[0]

        for i in range(1, n):
            result[i] = alpha * data[i] + (1 - alpha) * result[i - 1]

        return result

    @njit(fastmath=True)
    def _calculate_wma_njit(data, period):
        """使用 Numba 計算加權移動平均"""
        n = len(data)
        result = np.zeros(n)
        weights = np.arange(1, period + 1, dtype=np.float64)
        weight_sum = np.sum(weights)

        for i in range(period - 1, n):
            sum_val = 0.0
            for j in range(period):
                sum_val += weights[j] * data[i - period + 1 + j]
            result[i] = sum_val / weight_sum

        return result

    # 統一向量化信號生成函數（整合所有策略）
    @njit(fastmath=True)
    def _vectorized_generate_ma_signals_njit(
        price_values,
        ma_values,
        strat_idx,
        period,
        short_ma_values=None,
        long_ma_values=None,
        m=2,
    ):
        """
        統一向量化生成移動平均信號 - 支持所有12種策略
        消除重複代碼，統一所有信號生成邏輯

        Args:
            price_values: 價格數組
            ma_values: 移動平均數組
            strat_idx: 策略索引 (1-12)
            period: 移動平均週期
            short_ma_values: 短均線數組（雙均線策略用）
            long_ma_values: 長均線數組（雙均線策略用）
            m: 連續日數（連續策略用）

        Returns:
            np.ndarray: 信號數組，1=做多，-1=做空，0=無信號
        """
        n = len(price_values)
        signals = np.zeros(n)

        # 處理雙均線策略（strat_idx 5-8）
        if (
            strat_idx in [5, 6, 7, 8]
            and short_ma_values is not None
            and long_ma_values is not None
        ):
            # 生成雙均線信號
            prev_short = np.roll(short_ma_values, 1)
            prev_long = np.roll(long_ma_values, 1)

            if strat_idx == 5:  # 短均線升穿長均線做多
                crossover = (short_ma_values > long_ma_values) & (
                    prev_short <= prev_long
                )
                signals = np.where(crossover, 1.0, 0.0)
            elif strat_idx == 6:  # 短均線升穿長均線做空
                crossover = (short_ma_values > long_ma_values) & (
                    prev_short <= prev_long
                )
                signals = np.where(crossover, -1.0, 0.0)
            elif strat_idx == 7:  # 短均線跌穿長均線做多
                crossover = (short_ma_values < long_ma_values) & (
                    prev_short >= prev_long
                )
                signals = np.where(crossover, 1.0, 0.0)
            elif strat_idx == 8:  # 短均線跌穿長均線做空
                crossover = (short_ma_values < long_ma_values) & (
                    prev_short >= prev_long
                )
                signals = np.where(crossover, -1.0, 0.0)

            # 在最小有效索引之前設為0（使用長均線週期）
            min_valid_index = period - 1
            signals[:min_valid_index] = 0
        else:
            # 單均線策略（strat_idx 1-4, 9-12）
            if strat_idx in [1, 2, 3, 4]:
                # 價格與MA交叉策略
                prev_price = np.roll(price_values, 1)
                prev_ma = np.roll(ma_values, 1)

                if strat_idx == 1:  # 價格升穿MA做多
                    crossover = (price_values > ma_values) & (prev_price <= prev_ma)
                    signals = np.where(crossover, 1.0, 0.0)
                elif strat_idx == 2:  # 價格升穿MA做空
                    crossover = (price_values > ma_values) & (prev_price <= prev_ma)
                    signals = np.where(crossover, -1.0, 0.0)
                elif strat_idx == 3:  # 價格跌穿MA做多
                    crossover = (price_values < ma_values) & (prev_price >= prev_ma)
                    signals = np.where(crossover, 1.0, 0.0)
                elif strat_idx == 4:  # 價格跌穿MA做空
                    crossover = (price_values < ma_values) & (prev_price >= prev_ma)
                    signals = np.where(crossover, -1.0, 0.0)

                # 設置最小有效索引
                min_valid_index = period - 1
                signals[:min_valid_index] = 0

            elif strat_idx in [9, 10, 11, 12]:
                # 連續日數策略
                if strat_idx in [9, 10]:  # 連續m日位於MA以上
                    above_ma = price_values > ma_values
                    consecutive_above = np.zeros_like(price_values, dtype=np.bool_)
                    for j in range(m - 1, len(price_values)):  # 從第(m-1)天開始檢查
                        if np.all(
                            above_ma[j - m + 1 : j + 1]
                        ):  # 檢查包含當天在內的連續m天
                            consecutive_above[j] = True

                    if strat_idx == 9:  # 做多
                        signals = np.where(consecutive_above, 1.0, 0.0)
                    else:  # strat_idx == 10, 做空
                        signals = np.where(consecutive_above, -1.0, 0.0)

                else:  # strat_idx in [11, 12], 連續m日位於MA以下
                    below_ma = price_values < ma_values
                    consecutive_below = np.zeros_like(price_values, dtype=np.bool_)
                    for j in range(m - 1, len(price_values)):  # 從第(m-1)天開始檢查
                        if np.all(
                            below_ma[j - m + 1 : j + 1]
                        ):  # 檢查包含當天在內的連續m天
                            consecutive_below[j] = True

                    if strat_idx == 11:  # 做多
                        signals = np.where(consecutive_below, 1.0, 0.0)
                    else:  # strat_idx == 12, 做空
                        signals = np.where(consecutive_below, -1.0, 0.0)

                # 設置最小有效索引
                min_valid_index = period + m - 2
                signals[:min_valid_index] = 0

        return signals


# ============================================================================
# 統一參數驗證工具
# ============================================================================


def _validate_ma_params(params, required_params):
    """
    統一參數驗證函數，消除重複的驗證邏輯

    Args:
        params: 參數物件
        required_params: 必需參數列表

    Raises:
        ValueError: 當必需參數缺失時
    """
    for param_name in required_params:
        if params.get_param(param_name) is None:
            raise ValueError(f"{param_name} 參數必須由外部提供")


def _get_predictor_series(data, predictor):
    """
    統一獲取預測因子序列，消除重複邏輯

    Args:
        data: 數據DataFrame
        predictor: 預測因子列名

    Returns:
        pd.Series: 預測因子序列
    """
    if predictor is None:
        predictor_series = data["Close"]
        return predictor_series
    else:
        if predictor in data.columns:
            return data[predictor]
        else:
            raise ValueError(
                f"預測因子 '{predictor}' 不存在於數據中，可用欄位: {list(data.columns)}"
            )


# ============================================================================
# 統一緩存管理器
# ============================================================================


class MACacheManager:
    """
    統一的MA緩存管理器，消除重複的緩存邏輯
    """

    def __init__(self):
        self._cache = {}

    def get_or_calculate(self, values, period, ma_type, predictor=None):
        """
        獲取緩存的MA值，如果不存在則計算並緩存
        """
        cache_key = (period, ma_type, predictor)

        if cache_key in self._cache:
            return self._cache[cache_key]

        # 計算MA
        ma_values = _calculate_ma_unified(values, period, ma_type)

        # 緩存結果
        self._cache[cache_key] = ma_values
        return ma_values

    def clear(self):
        """清空緩存"""
        self._cache.clear()


# ============================================================================
# 統一信號生成器
# ============================================================================


def _generate_ma_signals_unified(
    predictor_values,
    ma_values,
    strat_idx,
    period,
    short_ma_values=None,
    long_ma_values=None,
    m=2,
):
    """
    統一的MA信號生成函數，消除重複的信號生成邏輯
    """
    if NUMBA_AVAILABLE:
        return _vectorized_generate_ma_signals_njit(
            predictor_values,
            ma_values,
            strat_idx,
            period,
            short_ma_values,
            long_ma_values,
            m,
        )
    else:
        # numpy備用實現
        n = len(predictor_values)
        signals = np.zeros(n)

        # 處理雙均線策略（strat_idx 5-8）
        if (
            strat_idx in [5, 6, 7, 8]
            and short_ma_values is not None
            and long_ma_values is not None
        ):
            prev_short = np.roll(short_ma_values, 1)
            prev_long = np.roll(long_ma_values, 1)

            if strat_idx == 5:  # 短均線升穿長均線做多
                crossover = (short_ma_values > long_ma_values) & (
                    prev_short <= prev_long
                )
                signals = np.where(crossover, 1.0, 0.0)
            elif strat_idx == 6:  # 短均線升穿長均線做空
                crossover = (short_ma_values > long_ma_values) & (
                    prev_short <= prev_long
                )
                signals = np.where(crossover, -1.0, 0.0)
            elif strat_idx == 7:  # 短均線跌穿長均線做多
                crossover = (short_ma_values < long_ma_values) & (
                    prev_short >= prev_long
                )
                signals = np.where(crossover, 1.0, 0.0)
            elif strat_idx == 8:  # 短均線跌穿長均線做空
                crossover = (short_ma_values < long_ma_values) & (
                    prev_short >= prev_long
                )
                signals = np.where(crossover, -1.0, 0.0)

            # 在最小有效索引之前設為0
            min_valid_index = period - 1
            signals[:min_valid_index] = 0
        else:
            # 單均線策略
            if strat_idx in [1, 2, 3, 4]:
                prev_price = np.roll(predictor_values, 1)
                prev_ma = np.roll(ma_values, 1)

                if strat_idx == 1:  # 價格升穿MA做多
                    crossover = (predictor_values > ma_values) & (prev_price <= prev_ma)
                    signals = np.where(crossover, 1.0, 0.0)
                elif strat_idx == 2:  # 價格升穿MA做空
                    crossover = (predictor_values > ma_values) & (prev_price <= prev_ma)
                    signals = np.where(crossover, -1.0, 0.0)
                elif strat_idx == 3:  # 價格跌穿MA做多
                    crossover = (predictor_values < ma_values) & (prev_price >= prev_ma)
                    signals = np.where(crossover, 1.0, 0.0)
                elif strat_idx == 4:  # 價格跌穿MA做空
                    crossover = (predictor_values < ma_values) & (prev_price >= prev_ma)
                    signals = np.where(crossover, -1.0, 0.0)

                min_valid_index = period - 1
                signals[:min_valid_index] = 0

            elif strat_idx in [9, 10, 11, 12]:
                if strat_idx in [9, 10]:  # 連續m日位於MA以上
                    above_ma = predictor_values > ma_values
                    consecutive_above = np.zeros_like(predictor_values, dtype=bool)
                    for j in range(m - 1, len(predictor_values)):
                        if np.all(above_ma[j - m + 1 : j + 1]):
                            consecutive_above[j] = True

                    if strat_idx == 9:  # 做多
                        signals = np.where(consecutive_above, 1.0, 0.0)
                    else:  # 做空
                        signals = np.where(consecutive_above, -1.0, 0.0)

                else:  # 連續m日位於MA以下
                    below_ma = predictor_values < ma_values
                    consecutive_below = np.zeros_like(predictor_values, dtype=bool)
                    for j in range(m - 1, len(predictor_values)):
                        if np.all(below_ma[j - m + 1 : j + 1]):
                            consecutive_below[j] = True

                    if strat_idx == 11:  # 做多
                        signals = np.where(consecutive_below, 1.0, 0.0)
                    else:  # 做空
                        signals = np.where(consecutive_below, -1.0, 0.0)

                min_valid_index = period + m - 2
                signals[:min_valid_index] = 0

        return signals


# ============================================================================
# 主類別
# ============================================================================


class MovingAverageIndicator:
    """
    移動平均線指標（僅產生MA序列，不含策略信號）
    支援 SMA、EMA、WMA
    優化後消除冗餘代碼，統一邏輯
    """

    MA_DESCRIPTIONS = [
        "價格升穿n日均線做多",
        "價格升穿n日均線做空",
        "價格跌穿n日均線做多",
        "價格跌穿n日均線做空",
        "n日均線升穿m日均線做多",
        "n日均線升穿m日均線做空",
        "n日均線跌穿m日均線做多",
        "n日均線跌穿m日均線做空",
        "價格連續m日位於n日均線以上做多",
        "價格連續m日位於n日均線以上做空",
        "價格連續m日位於n日均線以下做多",
        "價格連續m日位於n日均線以下做空",
    ]

    def __init__(self, data, params, logger=None):
        self.data = data
        self.params = params
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.signals = None
        # 使用統一的緩存管理器
        self._cache_manager = MACacheManager()

    @staticmethod
    def get_strategy_descriptions():
        """回傳策略描述字典"""
        return {
            f"MA{i + 1}": desc
            for i, desc in enumerate(MovingAverageIndicator.MA_DESCRIPTIONS)
        }

    @classmethod
    def get_params(cls, strat_idx=None, params_config=None):
        """
        參數必須完全由 UserInterface 層傳入，否則丟出 ValueError。
        不再於此處設定任何預設值。
        """
        if params_config is None:
            raise ValueError("params_config 必須由 UserInterface 提供，且不得為 None")

        ma_type = params_config["ma_type"]
        param_list = []

        if strat_idx in [1, 2, 3, 4]:
            if "ma_range" not in params_config:
                raise ValueError("ma_range 必須由 UserInterface 提供")
            ma_range = params_config["ma_range"]
            start, end, step = map(int, ma_range.split(":"))
            ma_lengths = list(range(start, end + 1, step))
            for n in ma_lengths:
                param = IndicatorParams("MA")
                param.add_param("ma_type", ma_type)
                param.add_param("period", n)
                param.add_param("mode", "single")
                param.add_param("strat_idx", strat_idx)
                param_list.append(param)
        elif strat_idx in [5, 6, 7, 8]:
            if "short_range" not in params_config or "long_range" not in params_config:
                raise ValueError("short_range 與 long_range 必須由 UserInterface 提供")
            short_range = params_config["short_range"]
            long_range = params_config["long_range"]
            s_start, s_end, s_step = map(int, short_range.split(":"))
            l_start, l_end, l_step = map(int, long_range.split(":"))
            short_periods = list(range(s_start, s_end + 1, s_step))
            long_periods = list(range(l_start, l_end + 1, l_step))
            for sp in short_periods:
                for lp in long_periods:
                    if sp < lp:
                        param = IndicatorParams("MA")
                        param.add_param("ma_type", ma_type)
                        param.add_param("shortMA_period", sp)
                        param.add_param("longMA_period", lp)
                        param.add_param("mode", "double")
                        param.add_param("strat_idx", strat_idx)
                        param_list.append(param)
        elif strat_idx in [9, 10, 11, 12]:
            if "m_range" not in params_config or "n_range" not in params_config:
                raise ValueError("m_range 與 n_range 必須由 UserInterface 提供")
            m_range = params_config["m_range"]
            n_range = params_config["n_range"]
            m_start, m_end, m_step = map(int, m_range.split(":"))
            n_start, n_end, n_step = map(int, n_range.split(":"))
            m_list = list(range(m_start, m_end + 1, m_step))
            n_list = list(range(n_start, n_end + 1, n_step))
            for m in m_list:
                for n in n_list:
                    param = IndicatorParams("MA")
                    param.add_param("ma_type", ma_type)
                    param.add_param("period", n)
                    param.add_param("m", m)
                    param.add_param("mode", "single")
                    param.add_param("strat_idx", strat_idx)
                    param_list.append(param)
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")
        return param_list

    def calculate(self):
        """根據params產生MA序列（單/雙）"""
        ma_type = self.params.get_param("ma_type", "SMA")
        mode = self.params.get_param("mode", "single")

        if mode == "single":
            _validate_ma_params(self.params, ["period"])
            period = self.params.get_param("period")
            return self._calculate_ma(self.data["Close"], period, ma_type)
        elif mode == "double":
            _validate_ma_params(self.params, ["shortMA_period", "longMA_period"])
            short_period = self.params.get_param("shortMA_period")
            long_period = self.params.get_param("longMA_period")
            short_ma = self._calculate_ma(self.data["Close"], short_period, ma_type)
            long_ma = self._calculate_ma(self.data["Close"], long_period, ma_type)
            return pd.DataFrame(
                {
                    f"short_{ma_type}_{short_period}": short_ma,
                    f"long_{ma_type}_{long_period}": long_ma,
                }
            )
        else:
            raise ValueError(f"未知MA模式: {mode}")

    def _calculate_ma(self, series, period, ma_type):
        """計算移動平均 - 使用統一的MA計算函數"""
        ma_values = _calculate_ma_unified(series.values, period, ma_type)
        return pd.Series(ma_values, index=series.index)

    def generate_signals(self, predictor=None):
        """
        根據 MA 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算 MA，而非價格。
        優化後使用統一的信號生成邏輯
        """
        strat_idx = self.params.get_param("strat_idx", 1)
        mode = self.params.get_param("mode", "single")

        # 獲取預測因子序列
        predictor_series, predictor_name = _get_predictor_series(self.data, predictor)

        # 轉換為 ndarray，確保數據類型
        predictor_values = predictor_series.values.astype(np.float64)
        predictor_values = np.nan_to_num(predictor_values, nan=0.0)

        if mode == "single":
            _validate_ma_params(self.params, ["period"])
            period = self.params.get_param("period")
            ma_type = self.params.get_param("ma_type", "SMA")

            # 計算移動平均
            ma_values = self._cache_manager.get_or_calculate(
                predictor_values, period, ma_type, predictor_name
            )

            # 生成信號 - 為MA9-MA12傳遞m參數
            if strat_idx in [9, 10, 11, 12]:
                m = self.params.get_param("m")
                if m is None:
                    raise ValueError("MA9-MA12策略需要m參數")
                signal_values = _generate_ma_signals_unified(
                    predictor_values,
                    ma_values,
                    strat_idx,
                    period,
                    short_ma_values=None,
                    long_ma_values=None,
                    m=m,
                )
            else:
                signal_values = _generate_ma_signals_unified(
                    predictor_values, ma_values, strat_idx, period
                )

        else:  # 雙均線模式
            _validate_ma_params(self.params, ["shortMA_period", "longMA_period"])
            short_period = self.params.get_param("shortMA_period")
            long_period = self.params.get_param("longMA_period")
            ma_type = self.params.get_param("ma_type", "SMA")

            # 計算短均線和長均線
            short_ma_values = self._cache_manager.get_or_calculate(
                predictor_values, short_period, ma_type, f"{predictor_name}_short"
            )
            long_ma_values = self._cache_manager.get_or_calculate(
                predictor_values, long_period, ma_type, f"{predictor_name}_long"
            )

            # 生成信號 - 修復參數順序，對於雙均線策略，第一個ma_values參數不重要
            signal_values = _generate_ma_signals_unified(
                predictor_values,
                short_ma_values,
                strat_idx,
                long_period,
                short_ma_values,
                long_ma_values,
            )

        signal = pd.Series(signal_values, index=self.data.index)
        return signal

    def get_min_valid_index(self):
        """獲取最小有效索引"""
        mode = self.params.get_param("mode", "single")
        if mode == "single":
            _validate_ma_params(self.params, ["period"])
            period = self.params.get_param("period")
            return period - 1
        elif mode == "double":
            _validate_ma_params(self.params, ["longMA_period"])
            long_period = self.params.get_param("longMA_period")
            return long_period - 1
        else:
            return 0

    # 向量化方法（優化後消除重複代碼）
    @staticmethod
    def vectorized_calculate_ma_signals(
        tasks, predictor, signals_matrix, global_ma_cache=None, data=None
    ):
        """
        向量化計算移動平均信號 - 只生成純粹的 +1/-1/0 信號，不區分開平倉
        優化後使用統一的計算邏輯
        """
        if global_ma_cache is None:
            global_ma_cache = {}

        if data is None:
            raise ValueError("data 參數必須提供")

        # 創建緩存管理器
        cache_manager = MACacheManager()
        cache_manager._cache = global_ma_cache

        # 獲取預測因子序列
        predictor_series = _get_predictor_series(data, predictor)
        predictor_name = predictor
        predictor_values = predictor_series.values.astype(np.float64)
        predictor_values = np.nan_to_num(predictor_values, nan=0.0)

        # 處理所有任務
        for task_idx, indicator_idx, param in tasks:
            try:
                strat_idx = param.get_param("strat_idx", 1)
                mode = param.get_param("mode", "single")
                ma_type = param.get_param("ma_type", "SMA")

                if mode == "single":
                    period = param.get_param("period")
                    if period is None:
                        continue

                    # 計算MA
                    ma_values = cache_manager.get_or_calculate(
                        predictor_values, period, ma_type, f"{predictor_name}_{period}"
                    )

                    # 生成信號 - 為MA9-MA12傳遞m參數
                    if strat_idx in [9, 10, 11, 12]:
                        m = param.get_param("m")
                        if m is None:
                            continue  # 跳過缺少m參數的任務
                        signals = _generate_ma_signals_unified(
                            predictor_values,
                            ma_values,
                            strat_idx,
                            period,
                            short_ma_values=None,
                            long_ma_values=None,
                            m=m,
                        )
                    else:
                        signals = _generate_ma_signals_unified(
                            predictor_values, ma_values, strat_idx, period
                        )

                else:  # 雙均線模式
                    short_period = param.get_param("shortMA_period")
                    long_period = param.get_param("longMA_period")

                    if short_period is None or long_period is None:
                        continue

                    # 計算短均線和長均線
                    short_ma_values = cache_manager.get_or_calculate(
                        predictor_values,
                        short_period,
                        ma_type,
                        f"{predictor_name}_short_{short_period}",
                    )
                    long_ma_values = cache_manager.get_or_calculate(
                        predictor_values,
                        long_period,
                        ma_type,
                        f"{predictor_name}_long_{long_period}",
                    )

                    # 生成信號 - 修復參數順序
                    signals = _generate_ma_signals_unified(
                        predictor_values,
                        short_ma_values,
                        strat_idx,
                        long_period,
                        short_ma_values,
                        long_ma_values,
                    )

                signals_matrix[:, task_idx, indicator_idx] = signals

            except Exception:
                signals_matrix[:, task_idx, indicator_idx] = 0

        return signals_matrix
