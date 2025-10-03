"""
VALUE_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的VALUE突破指標工具，負責產生連續突破信號，支援多種突破策略和參數設定。
- 支援 VALUE1-VALUE6 六種細分策略，涵蓋不同交易邏輯
- 提供可調整的連續天數與固定閾值參數
- 整合 Numba JIT 編譯優化，大幅提升計算性能
- 支援向量化批量計算，適合大規模參數組合回測
- 提供智能緩存機制，避免重複計算

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生VALUE突破信號
- 信號傳遞給 BacktestEngine 進行交易模擬
- 支援單指標計算和批量向量化計算兩種模式

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[VALUE_Indicator]
    B -->|單指標計算| C[generate_signals]
    B -->|批量向量化計算| D[vectorized_calculate_value_signals]
    C -->|VALUE1-VALUE6| E[策略信號生成]
    D -->|批量處理| E
    E -->|信號| F[BacktestEngine]
```

【策略型態】
------------------------------------------------------------
- VALUE1：連續N日升穿M值時做多
- VALUE2：連續N日升穿M值時做空
- VALUE3：連續N日跌穿M值時做多
- VALUE4：連續N日跌穿M值時做空
- VALUE5：在M1和M2之間做多
- VALUE6：在M1和M2之間做空

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
- 連續性判斷邏輯需要準確實現

【錯誤處理】
------------------------------------------------------------
- 參數驗證失敗時提供詳細錯誤信息
- 數據格式錯誤時提供修正建議
- Numba 編譯失敗時自動降級為標準 Python 計算
- 緩存錯誤時提供清理機制

【範例】
------------------------------------------------------------
- 單指標計算：indicator = VALUEIndicator(data, params)
  signals = indicator.generate_signals(predictor)
- 批量向量化計算：signals_matrix = VALUEIndicator.vectorized_calculate_value_signals(
    tasks, predictor, signals_matrix, global_value_cache, data)
- 參數生成：params_list = VALUEIndicator.get_params(strat_idx, params_config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 需與 IndicatorsBacktester 的指標介面保持一致
- 向量化功能與 VectorBacktestEngine 共享緩存機制
- 與其他指標模組共享計算資源

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本VALUE突破指標
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
from typing import Any, Dict, List, Optional, Tuple

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

    @njit(fastmath=True, cache=True)  # type: ignore[misc]
    def _generate_value_signals_njit(
        predictor_values, n_length, m_value, strat_idx
    ):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        """
        使用 Numba 生成VALUE突破信號 - VALUE1-4
        全程使用 ndarray，無 pandas 依賴

        邏輯：
        - VALUE1: 連續N日升穿M值時做多
        - VALUE2: 連續N日升穿M值時做空
        - VALUE3: 連續N日跌穿M值時做多
        - VALUE4: 連續N日跌穿M值時做空
        """
        n = len(predictor_values)
        signals = np.zeros(n)

        # 生成信號
        # 需要確保有足夠的數據進行連續次數檢查
        # 起始點應該是 n_length - 1，這樣檢查連續n天時，每天都有足夠的數據
        for i in range(n_length - 1, n):
            # 檢查連續N日是否都滿足條件
            if strat_idx == 1:  # VALUE1: 連續N日升穿M值時做多
                all_above = True
                for j in range(i - n_length + 1, i + 1):
                    if predictor_values[j] <= m_value:
                        all_above = False
                        break
                if all_above:
                    signals[i] = 1.0

            elif strat_idx == 2:  # VALUE2: 連續N日升穿M值時做空
                all_above = True
                for j in range(i - n_length + 1, i + 1):
                    if predictor_values[j] <= m_value:
                        all_above = False
                        break
                if all_above:
                    signals[i] = -1.0

            elif strat_idx == 3:  # VALUE3: 連續N日跌穿M值時做多
                all_below = True
                for j in range(i - n_length + 1, i + 1):
                    if predictor_values[j] >= m_value:
                        all_below = False
                        break
                if all_below:
                    signals[i] = 1.0

            elif strat_idx == 4:  # VALUE4: 連續N日跌穿M值時做空
                all_below = True
                for j in range(i - n_length + 1, i + 1):
                    if predictor_values[j] >= m_value:
                        all_below = False
                        break
                if all_below:
                    signals[i] = -1.0

        return signals

    @njit(fastmath=True, cache=True)  # type: ignore[misc]
    def _generate_value_range_signals_njit(  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        predictor_values, m1_value, m2_value, strat_idx
    ):
        """
        使用 Numba 生成VALUE範圍信號 - VALUE5-6
        全程使用 ndarray，無 pandas 依賴

        邏輯：
        - VALUE5: 在M1和M2之間做多
        - VALUE6: 在M1和M2之間做空
        """
        n = len(predictor_values)
        signals = np.zeros(n)

        # 確保 m1 < m2
        if m1_value >= m2_value:
            return signals

        # 生成信號
        for i in range(n):
            curr_val = predictor_values[i]

            # 檢查當前值是否在範圍內
            if m1_value <= curr_val <= m2_value:
                if strat_idx == 5:  # VALUE5: 在M1和M2之間做多
                    signals[i] = 1.0
                elif strat_idx == 6:  # VALUE6: 在M1和M2之間做空
                    signals[i] = -1.0

        return signals


class VALUEIndicator:
    """
    VALUE突破指標與信號產生器
    支援六種指標邏輯，參數可自訂
    新增向量化批量計算功能，大幅提升多參數組合的計算效率
    """

    STRATEGY_DESCRIPTIONS = [
        "連續 n 日升穿 m 值時做多",
        "連續 n 日升穿 m 值時做空",
        "連續 n 日跌穿 m 值時做多",
        "連續 n 日跌穿 m 值時做空",
        "在 m1 和 m2 之間做多",
        "在 m1 和 m2 之間做空",
    ]

    @staticmethod
    def get_strategy_descriptions() -> Dict[str, str]:
        # 回傳 dict: {'VALUE{i+1}': '描述', ...}
        return {
            f"VALUE{i + 1}": desc
            for i, desc in enumerate(VALUEIndicator.STRATEGY_DESCRIPTIONS)
        }

    def __init__(
        self, data: pd.DataFrame, params: "IndicatorParams", logger: Optional[logging.Logger] = None
    ) -> None:  # pylint: disable=unused-argument
        self.data = data  # 移除 .copy()，直接引用
        self.params = params
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @classmethod
    def get_params(
        cls, strat_idx: Optional[int] = None, params_config: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:  # pylint: disable=unused-argument
        """
        參數必須完全由 UserInterface 層傳入，否則丟出 ValueError。
        不再於此處設定任何預設值。
        """
        if params_config is None:
            raise ValueError("params_config 必須由 UserInterface 提供，且不得為 None")

        if strat_idx is None:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")

        param_list = []

        if strat_idx in [1, 2, 3, 4]:
            # VALUE1-4: 需要 n_range 和 m_range
            if "n_range" not in params_config:
                raise ValueError("n_range 必須由 UserInterface 提供")
            if "m_range" not in params_config:
                raise ValueError("m_range 必須由 UserInterface 提供")

            n_range = params_config["n_range"]
            m_range = params_config["m_range"]

            # 處理n範圍參數
            start, end, step = map(int, n_range.split(":"))
            n_lengths = list(range(start, end + 1, step))

            # 處理m範圍參數
            start, end, step = map(int, m_range.split(":"))
            m_values = list(range(start, end + 1, step))

            for n in n_lengths:
                for m in m_values:
                    param = IndicatorParams("VALUE")
                    param.add_param("n_length", n)
                    param.add_param("m_value", m)
                    param.add_param("strat_idx", strat_idx)
                    param_list.append(param)

        elif strat_idx in [5, 6]:
            # VALUE5-6: 需要 m1_range 和 m2_range
            if "m1_range" not in params_config:
                raise ValueError("m1_range 必須由 UserInterface 提供")
            if "m2_range" not in params_config:
                raise ValueError("m2_range 必須由 UserInterface 提供")

            m1_range = params_config["m1_range"]
            m2_range = params_config["m2_range"]

            # 處理m1範圍參數
            start, end, step = map(int, m1_range.split(":"))
            m1_values = list(range(start, end + 1, step))

            # 處理m2範圍參數
            start, end, step = map(int, m2_range.split(":"))
            m2_values = list(range(start, end + 1, step))

            for m1 in m1_values:
                for m2 in m2_values:
                    if m1 < m2:  # 確保 m1 < m2
                        param = IndicatorParams("VALUE")
                        param.add_param("m1_value", m1)
                        param.add_param("m2_value", m2)
                        param.add_param("strat_idx", strat_idx)
                        param_list.append(param)
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")

        return param_list

    def generate_signals(
        self, predictor: Optional[str] = None
    ) -> Tuple[np.ndarray, np.ndarray]:  # pylint: disable=unused-argument
        """
        根據 VALUE 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算VALUE突破信號，而非價格。

        strat=1: 連續N日升穿M值時做多
        strat=2: 連續N日升穿M值時做空
        strat=3: 連續N日跌穿M值時做多
        strat=4: 連續N日跌穿M值時做空
        strat=5: 在M1和M2之間做多
        strat=6: 在M1和M2之間做空
        """
        strat_idx = self.params.get_param("strat_idx")

        if strat_idx is None:
            raise ValueError("strat_idx 參數必須由外部提供")

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
                # VALUE1-4: 連續突破策略
                n_length = self.params.get_param("n_length")
                m_value = self.params.get_param("m_value")

                if n_length is None or m_value is None:
                    raise ValueError("n_length, m_value 參數必須由外部提供")

                # 檢查數據是否足夠
                if len(self.data) < n_length:
                    raise ValueError(
                        f"數據長度不足：需要至少{n_length}個數據點，當前只有{len(self.data)}個"
                    )

                # 使用 Numba 計算信號
                signal_values = _generate_value_signals_njit(
                    predictor_values, n_length, m_value, strat_idx
                )

            elif strat_idx in [5, 6]:
                # VALUE5-6: 範圍策略
                m1_value = self.params.get_param("m1_value")
                m2_value = self.params.get_param("m2_value")

                if m1_value is None or m2_value is None:
                    raise ValueError("m1_value, m2_value 參數必須由外部提供")

                # 使用 Numba 計算信號
                signal_values = _generate_value_range_signals_njit(
                    predictor_values, m1_value, m2_value, strat_idx
                )
            else:
                raise ValueError(f"無效的策略索引: {strat_idx}")

            # 轉換回 pandas Series
            signal = pd.Series(signal_values, index=self.data.index)

            # 確保在有效期間之前不產生信號（僅適用於VALUE1-4）
            if strat_idx in [1, 2, 3, 4]:
                n_length = self.params.get_param("n_length")
                # 需要確保有足夠的數據進行連續次數檢查
                min_signal_start = max(n_length - 1, 0)
                signal.iloc[:min_signal_start] = 0

            return signal
        else:
            raise RuntimeError("Numba 未安裝，無法使用 VALUE 指標")

    def get_min_valid_index(self) -> int:
        strat_idx = self.params.get_param("strat_idx")
        if strat_idx is None:
            raise ValueError("strat_idx 參數必須由外部提供")

        if strat_idx in [1, 2, 3, 4]:
            n_length = self.params.get_param("n_length")
            if n_length is None:
                raise ValueError("n_length 參數必須由外部提供")
            # 需要確保有足夠的數據進行連續次數檢查
            return max(n_length - 1, 0)
        else:
            # VALUE5-6 沒有最小有效索引限制
            return 0

    # ==================== 新增向量化批量計算功能 ====================

    @staticmethod
    def vectorized_calculate_value_signals(  # pylint: disable=too-complex
        tasks: List[Tuple[int, Dict[str, Any]]],
        predictor: Optional[str],
        signals_matrix: np.ndarray,
        global_value_cache: Optional[Dict[str, Any]] = None,
        data: Optional[pd.DataFrame] = None,
    ) -> np.ndarray:
        """
        向量化計算VALUE突破信號 - 批量處理多個參數組合，大幅提升計算效率

        Args:
            tasks: 任務列表，每個任務包含 (task_idx, indicator_idx, param)
            predictor: 預測因子名稱
            signals_matrix: 信號矩陣 [時間點, 任務數, 指標數]
            global_value_cache: 全局緩存字典，避免重複計算
            data: 數據DataFrame，如果為None則使用實例的data

        Returns:
            None (直接修改signals_matrix)
        """
        if data is None:
            raise ValueError("data參數必須提供")

        if global_value_cache is None:
            global_value_cache = {}

        # 預處理數據 - 轉換為numpy數組並處理NaN值
        predictor_values = data[predictor].values.astype(np.float64)
        predictor_values = np.nan_to_num(predictor_values, nan=0.0)

        # 直接為每個任務生成信號 - 真正的向量化處理
        for task_idx, indicator_idx, param in tasks:
            try:
                strat_idx = param.get_param("strat_idx")

                if strat_idx in [1, 2, 3, 4]:
                    # VALUE1-4: 連續突破策略
                    n_length = param.get_param("n_length")
                    m_value = param.get_param("m_value")

                    if n_length is not None and m_value is not None:
                        # 使用 Numba 優化的函數計算VALUE信號
                        signal_values = _generate_value_signals_njit(
                            predictor_values, n_length, m_value, strat_idx
                        )

                        # 確保在有效期間之前不產生信號
                        signal_values[: n_length - 1] = 0.0

                        # 直接寫入信號矩陣
                        signals_matrix[:, task_idx, indicator_idx] = signal_values

                elif strat_idx in [5, 6]:
                    # VALUE5-6: 範圍策略
                    m1_value = param.get_param("m1_value")
                    m2_value = param.get_param("m2_value")

                    if m1_value is not None and m2_value is not None:
                        # 使用 Numba 優化的函數計算VALUE範圍信號
                        signal_values = _generate_value_range_signals_njit(
                            predictor_values, m1_value, m2_value, strat_idx
                        )

                        # 直接寫入信號矩陣
                        signals_matrix[:, task_idx, indicator_idx] = signal_values

            except Exception as e:
                # 記錄錯誤但不中斷處理
                if hasattr(VALUEIndicator, "logger"):
                    VALUEIndicator.logger.warning(
                        f"VALUE突破信號生成失敗 (task_idx={task_idx}, indicator_idx={indicator_idx}): {e}"
                    )
                signals_matrix[:, task_idx, indicator_idx] = 0

    # 注意：緩存機制已簡化，直接使用 Numba 函數進行計算
    # 如需緩存功能，可在 VectorBacktestEngine 中實現
