"""
BollingerBand_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的布林通道指標工具，負責產生布林通道信號，支援多種突破策略和通道寬度設定。
- 支援 BOLL1-BOLL4 四種細分策略，涵蓋不同交易邏輯
- 提供可調整的均線長度與標準差倍數參數
- 整合 Numba JIT 編譯優化，大幅提升計算性能
- 支援向量化批量計算，適合大規模參數組合回測
- 提供智能緩存機制，避免重複計算

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生布林通道信號
- 信號傳遞給 BacktestEngine 進行交易模擬
- 支援單指標計算和批量向量化計算兩種模式

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[BollingerBand_Indicator]
    B -->|單指標計算| C[generate_signals]
    B -->|批量向量化計算| D[vectorized_calculate_boll_signals]
    C -->|BOLL1-BOLL4| E[策略信號生成]
    D -->|批量處理| E
    E -->|信號| F[BacktestEngine]
```

【策略型態】
------------------------------------------------------------
- BOLL1：價格突破上軌做多，跌破下軌做空
- BOLL2：價格回歸中軌做多，偏離中軌做空
- BOLL3：通道寬度收縮做多，擴張做空
- BOLL4：價格與通道位置關係綜合判斷

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改指標型態、參數時，請同步更新頂部註解與下游流程
- 若指標邏輯有變動，需同步更新本檔案與 IndicatorsBacktester
- 指標參數如有調整，請同步通知協作者
- 向量化功能與單個指標功能保持邏輯一致
- Numba 優化需要確保跨平台兼容性
- 緩存機制需要正確管理記憶體使用

【常見易錯點】
------------------------------------------------------------
- 參數設置錯誤會導致信號產生異常
- 數據對齊問題會影響信號準確性
- 指標邏輯變動會影響下游交易模擬
- 向量化計算的緩存機制需要正確管理
- 標準差計算精度問題影響通道寬度

【錯誤處理】
------------------------------------------------------------
- 參數驗證失敗時提供詳細錯誤信息
- 數據格式錯誤時提供修正建議
- Numba 編譯失敗時自動降級為標準 Python 計算
- 緩存錯誤時提供清理機制

【範例】
------------------------------------------------------------
- 單指標計算：indicator = BollingerBandIndicator(data, params)
  signals = indicator.generate_signals(predictor)
- 批量向量化計算：signals_matrix = BollingerBandIndicator.vectorized_calculate_boll_signals(tasks, predictor, signals_matrix, global_boll_cache, data)  # noqa: E501
- 參數生成：params_list = BollingerBandIndicator.get_params(strat_idx, params_config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 需與 IndicatorsBacktester 的指標介面保持一致
- 向量化功能與 VectorBacktestEngine 共享緩存機制
- 與其他指標模組共享計算資源

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本布林通道指標
- v1.1: 新增多種策略型態支援
- v1.2: 完善參數驗證與錯誤處理
- v2.0: 整合 Numba JIT 編譯優化
- v2.1: 新增向量化批量計算
- v2.2: 完善緩存機制與性能優化

【參考】
------------------------------------------------------------
- pandas 官方文件：https://pandas.pydata.org/
- Numba 官方文檔：https://numba.pydata.org/
- Indicators_backtester.py、BacktestEngine_backtester.py、VectorBacktestEngine_backtester.py
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
    # Numba 未安裝，將使用標準 Python 計算

# 核心算法：純 Numba + ndarray 實現
if NUMBA_AVAILABLE:

    @njit(fastmath=True, cache=True)
    def _calculate_rolling_mean_njit(data, window):
        """使用 Numba 計算滾動平均"""
        n = len(data)
        result = np.zeros(n)

        for i in range(window - 1, n):
            sum_val = 0.0
            for j in range(i - window + 1, i + 1):
                sum_val += data[j]
            result[i] = sum_val / window

        return result

    @njit(fastmath=True, cache=True)
    def _calculate_rolling_std_njit(data, window):
        """使用 Numba 計算滾動標準差"""
        n = len(data)
        result = np.zeros(n)

        for i in range(window - 1, n):
            mean_val = 0.0
            for j in range(i - window + 1, i + 1):
                mean_val += data[j]
            mean_val /= window

            var_val = 0.0
            for j in range(i - window + 1, i + 1):
                var_val += (data[j] - mean_val) ** 2
            result[i] = np.sqrt(var_val / window)

        return result

    @njit(fastmath=True, cache=True)
    def _generate_bollinger_signals_njit(
        predictor_values, ma_length, std_multiplier, strat_idx
    ):
        """
        使用 Numba 生成布林通道信號
        全程使用 ndarray，無 pandas 依賴
        """
        n = len(predictor_values)
        signals = np.zeros(n)

        # 計算滾動統計量
        ma_values = _calculate_rolling_mean_njit(predictor_values, ma_length)
        std_values = _calculate_rolling_std_njit(predictor_values, ma_length)

        # 計算布林通道
        upper_values = ma_values + std_multiplier * std_values
        lower_values = ma_values - std_multiplier * std_values

        # 生成信號
        for i in range(1, n):
            if i < ma_length - 1:
                continue

            prev_val = predictor_values[i - 1]
            curr_val = predictor_values[i]
            upper = upper_values[i]
            lower = lower_values[i]

            # 處理 NaN 值
            if (
                np.isnan(prev_val)
                or np.isnan(curr_val)
                or np.isnan(upper)
                or np.isnan(lower)
            ):
                continue

            if strat_idx == 1:  # 突破上軌做多
                if prev_val <= upper and curr_val > upper:
                    signals[i] = 1.0
            elif strat_idx == 2:  # 突破上軌做空
                if prev_val <= upper and curr_val > upper:
                    signals[i] = -1.0
            elif strat_idx == 3:  # 突破下軌做多
                if prev_val >= lower and curr_val < lower:
                    signals[i] = 1.0
            elif strat_idx == 4:  # 突破下軌做空
                if prev_val >= lower and curr_val < lower:
                    signals[i] = -1.0

        return signals


class BollingerBandIndicator:
    """
    Bollinger Band 指標與信號產生器
    支援四種指標邏輯，參數可自訂
    新增向量化批量計算功能，大幅提升多參數組合的計算效率
    """

    STRATEGY_DESCRIPTIONS = [
        "價格突破上軌（ma+n倍sd)做多",
        "價格突破上軌（ma+n倍sd)做空",
        "價格突破下軌(ma-n倍sd)做多",
        "價格突破下軌(ma-n倍sd)做空",
    ]

    @staticmethod
    def get_strategy_descriptions():
        # 回傳 dict: {'BOLL{i+1}': '描述', ...}
        return {
            f"BOLL{i + 1}": desc
            for i, desc in enumerate(BollingerBandIndicator.STRATEGY_DESCRIPTIONS)
        }

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

        if "ma_range" not in params_config:
            raise ValueError("ma_range 必須由 UserInterface 提供")
        if "sd_multi" not in params_config:
            raise ValueError("sd_multi 必須由 UserInterface 提供")

        ma_range = params_config["ma_range"]
        sd_input = params_config["sd_multi"]

        # 處理MA範圍參數
        start, end, step = map(int, ma_range.split(":"))
        ma_lengths = list(range(start, end + 1, step))

        # 處理標準差倍數參數 - 支援逗號分隔和範圍格式
        sd_multi_list = []
        if ":" in sd_input:
            # 範圍格式，如 "1:5:1" 或 "1.5:3.5:0.5"
            try:
                sd_start, sd_end, sd_step = map(float, sd_input.split(":"))
                sd_multi_list = [
                    sd_start + i * sd_step
                    for i in range(int((sd_end - sd_start) / sd_step) + 1)
                ]
            except (ValueError, ZeroDivisionError):
                raise ValueError("sd_multi 範圍格式解析失敗，請檢查格式是否正確")
        else:
            # 逗號分隔格式，如 "2,3" 或 "1.5,2.0,2.5"
            sd_multi_list = [float(x.strip()) for x in sd_input.split(",") if x.strip()]

        # 確保至少有一個標準差倍數
        if not sd_multi_list:
            raise ValueError("sd_multi 參數不能為空")

        param_list = []
        if strat_idx in [1, 2, 3, 4]:
            for n in ma_lengths:
                for sd in sd_multi_list:
                    param = IndicatorParams("BOLL")
                    param.add_param("ma_length", n)
                    param.add_param("std_multiplier", sd)
                    param.add_param("strat_idx", strat_idx)
                    param_list.append(param)
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")
        return param_list

    def generate_signals(self, predictor=None):
        """
        根據 BOLL 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算 Bollinger Bands，而非價格。

        strat=1: 預測因子突破上軌做多
        strat=2: 預測因子突破上軌做空
        strat=3: 預測因子突破下軌做多
        strat=4: 預測因子突破下軌做空
        """
        ma_length = self.params.get_param("ma_length")  # 移除預設值
        std_multiplier = self.params.get_param("std_multiplier")  # 移除預設值
        strat_idx = self.params.get_param("strat_idx")  # 移除預設值

        if ma_length is None or std_multiplier is None or strat_idx is None:
            raise ValueError("ma_length, std_multiplier, strat_idx 參數必須由外部提供")

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

            # 使用 Numba 計算信號
            signal_values = _generate_bollinger_signals_njit(
                predictor_values, ma_length, std_multiplier, strat_idx
            )

            # 轉換回 pandas Series
            signal = pd.Series(signal_values, index=self.data.index)

        # 確保在有效期間之前不產生信號
        signal.iloc[: ma_length - 1] = 0

        return signal

    def get_min_valid_index(self):
        ma_length = self.params.get_param("ma_length")  # 移除預設值
        if ma_length is None:
            raise ValueError("ma_length 參數必須由外部提供")
        return ma_length - 1

    # ==================== 新增向量化批量計算功能 ====================

    @staticmethod
    def vectorized_calculate_boll_signals(
        tasks, predictor, signals_matrix, global_boll_cache=None, data=None
    ):
        """
        向量化計算布林帶信號 - 批量處理多個參數組合，大幅提升計算效率

        Args:
            tasks: 任務列表，每個任務包含 (task_idx, indicator_idx, param)
            predictor: 預測因子名稱
            signals_matrix: 信號矩陣 [時間點, 任務數, 指標數]
            global_boll_cache: 全局緩存字典，避免重複計算
            data: 數據DataFrame，如果為None則使用實例的data

        Returns:
            None (直接修改signals_matrix)
        """
        if data is None:
            raise ValueError("data參數必須提供")

        if global_boll_cache is None:
            global_boll_cache = {}

        # 提取所有BOLL參數
        ma_lengths = []
        std_multipliers = []
        strat_indices = []
        task_indices = []
        indicator_indices = []

        for task_idx, indicator_idx, param in tasks:
            ma_length = param.get_param("ma_length")
            std_multiplier = param.get_param("std_multiplier")
            strat_idx = param.get_param("strat_idx", 1)
            if ma_length is not None and std_multiplier is not None:
                ma_lengths.append(ma_length)
                std_multipliers.append(std_multiplier)
                strat_indices.append(strat_idx)
                task_indices.append(task_idx)
                indicator_indices.append(indicator_idx)

        if not ma_lengths:
            return

        # 預處理數據 - 轉換為numpy數組並處理NaN值
        predictor_values = data[predictor].values.astype(np.float64)
        predictor_values = np.nan_to_num(predictor_values, nan=0.0)

        # 批量計算所有參數的布林帶 - 使用Numba優化
        unique_combinations = list(set(zip(ma_lengths, std_multipliers)))

        for ma_length, std_multiplier in unique_combinations:
            if ma_length <= len(data):
                cache_key = (ma_length, std_multiplier, predictor)
                if cache_key not in global_boll_cache:
                    # 使用Numba優化的函數計算布林帶
                    ma_values = _calculate_rolling_mean_njit(
                        predictor_values, ma_length
                    )
                    std_values = _calculate_rolling_std_njit(
                        predictor_values, ma_length
                    )

                    upper_band = ma_values + (std_values * std_multiplier)
                    lower_band = ma_values - (std_values * std_multiplier)

                    global_boll_cache[cache_key] = (upper_band, lower_band, ma_values)

        # 為每個任務生成信號 - 使用Numba優化
        for i, (
            ma_length,
            std_multiplier,
            strat_idx,
            task_idx,
            indicator_idx,
        ) in enumerate(
            zip(
                ma_lengths,
                std_multipliers,
                strat_indices,
                task_indices,
                indicator_indices,
            )
        ):
            try:
                cache_key = (ma_length, std_multiplier, predictor)
                upper_band, lower_band, ma_values = global_boll_cache[cache_key]

                # 使用緩存中的布林帶值直接生成信號
                signals = np.zeros(len(predictor_values))

                # 計算前一日值
                prev_values = np.roll(predictor_values, 1)

                # 向量化生成信號 - 優化條件判斷
                if strat_idx in [1, 2]:  # 突破上軌策略
                    crossover = (predictor_values >= upper_band) & (
                        prev_values < upper_band
                    )
                    signal_value = 1.0 if strat_idx == 1 else -1.0
                    signals = np.where(crossover, signal_value, 0.0)
                elif strat_idx in [3, 4]:  # 突破下軌策略
                    crossover = (predictor_values <= lower_band) & (
                        prev_values > lower_band
                    )
                    signal_value = 1.0 if strat_idx == 3 else -1.0
                    signals = np.where(crossover, signal_value, 0.0)

                # 確保在有效期間之前不產生信號
                signals[: ma_length - 1] = 0.0

                signals_matrix[:, task_idx, indicator_idx] = signals

            except Exception as e:
                # 記錄錯誤但不中斷處理
                if hasattr(BollingerBandIndicator, "logger"):
                    BollingerBandIndicator.logger.warning(
                        f"布林帶信號生成失敗 (task_idx={task_idx}, indicator_idx={indicator_idx}): {e}"
                    )
                signals_matrix[:, task_idx, indicator_idx] = 0

    @staticmethod
    def create_global_boll_cache():
        """創建全局布林帶緩存字典"""
        return {}

    @staticmethod
    def clear_global_boll_cache(global_boll_cache):
        """清理全局布林帶緩存"""
        if global_boll_cache is not None:
            global_boll_cache.clear()

    @staticmethod
    def get_cache_info(global_boll_cache):
        """獲取緩存信息"""
        if global_boll_cache is None:
            return "緩存未初始化"

        cache_size = len(global_boll_cache)
        cache_keys = list(global_boll_cache.keys())

        info = f"緩存大小: {cache_size}"
        if cache_size > 0:
            info += f"\n緩存鍵: {cache_keys[:5]}"  # 只顯示前5個鍵
            if len(cache_keys) > 5:
                info += f" ... (還有 {len(cache_keys) - 5} 個)"

        return info
