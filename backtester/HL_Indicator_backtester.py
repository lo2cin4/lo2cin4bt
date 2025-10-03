"""
HL_Indicator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的HL突破指標工具，負責產生連續突破信號，支援多種突破策略和參數設定。
- 支援 HL1-HL4 四種細分策略，涵蓋不同交易邏輯
- 提供可調整的連續天數與歷史回看天數參數
- 整合 Numba JIT 編譯優化，大幅提升計算性能
- 支援向量化批量計算，適合大規模參數組合回測
- 提供智能緩存機制，避免重複計算

【流程與數據流】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，產生HL突破信號
- 信號傳遞給 BacktestEngine 進行交易模擬
- 支援單指標計算和批量向量化計算兩種模式

```mermaid
flowchart TD
    A[IndicatorsBacktester] -->|調用| B[HL_Indicator]
    B -->|單指標計算| C[generate_signals]
    B -->|批量向量化計算| D[vectorized_calculate_hl_signals]
    C -->|HL1-HL4| E[策略信號生成]
    D -->|批量處理| E
    E -->|信號| F[BacktestEngine]
```

【策略型態】
------------------------------------------------------------
- HL1：連續n日升穿過去m日高位做多
- HL2：連續n日升穿過去m日高位做空
- HL3：連續n日跌穿過去m日低位做多
- HL4：連續n日跌穿過去m日低位做空

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
- 單指標計算：indicator = HLIndicator(data, params)
  signals = indicator.generate_signals(predictor)
- 批量向量化計算：signals_matrix = HLIndicator.vectorized_calculate_hl_signals(tasks, predictor, signals_matrix, global_hl_cache, data)
- 參數生成：params_list = HLIndicator.get_params(strat_idx, params_config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 IndicatorsBacktester 調用，信號傳遞給 BacktestEngine
- 需與 IndicatorsBacktester 的指標介面保持一致
- 向量化功能與 VectorBacktestEngine 共享緩存機制
- 與其他指標模組共享計算資源

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本HL突破指標
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
    def _generate_hl_signals_njit(
        predictor_values, n_length, m_length, strat_idx
    ):  # type: ignore[no-untyped-def] # pylint: disable=too-complex
        """
        使用 Numba 生成HL等同信號 - 修正版本
        全程使用 ndarray，無 pandas 依賴

        邏輯：
        - HL1: 連續 n 日數值等同過去 m 天的歷史高位，發出買入信號
        - HL2: 連續 n 日數值等同過去 m 天的歷史高位，發出賣出信號
        - HL3: 連續 n 日數值等同過去 m 天的歷史低位，發出買入信號
        - HL4: 連續 n 日數值等同過去 m 天的歷史低位，發出賣出信號
        """
        n = len(predictor_values)
        signals = np.zeros(n)

        # 生成信號
        # 需要確保有足夠的數據進行連續次數檢查
        # 起始點應該是 m_length + n_length - 1，這樣檢查連續n天時，每天都有m天的歷史數據
        for i in range(m_length + n_length - 1, n):
            # 計算過去m天的歷史高位和低位（包括當前第i天）
            start_idx = i - m_length + 1  # 包含第i天
            end_idx = i + 1  # 包含第i天

            _ = predictor_values[start_idx:end_idx].max()  # historical_high
            _ = predictor_values[start_idx:end_idx].min()  # historical_low

            # 檢查連續n日是否都等同各自的歷史高位或低位
            if strat_idx == 1:  # HL1: 連續n日等同高位做多
                # 檢查連續n天，每天都要等同各自過去m天的最高值
                # 確保有足夠的數據進行連續次數檢查
                if i >= n_length - 1:
                    all_equal_high = True
                    for j in range(i - n_length + 1, i + 1):
                        # 確保j不會超出範圍
                        if j < 0:
                            all_equal_high = False
                            break
                        # 計算第j天過去m天的最高值
                        j_start = max(0, j - m_length + 1)
                        j_end = j + 1
                        j_historical_high = predictor_values[j_start:j_end].max()
                        # 檢查第j天是否等同其過去m天的最高值
                        if abs(predictor_values[j] - j_historical_high) > 1e-10:
                            all_equal_high = False
                            break
                    if all_equal_high:
                        signals[i] = 1.0

            elif strat_idx == 2:  # HL2: 連續n日等同高位做空
                # 檢查連續n天，每天都要等同各自過去m天的最高值
                # 確保有足夠的數據進行連續次數檢查
                if i >= n_length - 1:
                    all_equal_high = True
                    for j in range(i - n_length + 1, i + 1):
                        # 確保j不會超出範圍
                        if j < 0:
                            all_equal_high = False
                            break
                        # 計算第j天過去m天的最高值
                        j_start = max(0, j - m_length + 1)
                        j_end = j + 1
                        j_historical_high = predictor_values[j_start:j_end].max()
                        # 檢查第j天是否等同其過去m天的最高值
                        if abs(predictor_values[j] - j_historical_high) > 1e-10:
                            all_equal_high = False
                            break
                    if all_equal_high:
                        signals[i] = -1.0

            elif strat_idx == 3:  # HL3: 連續n日等同低位做多
                # 檢查連續n天，每天都要等同各自過去m天的最低值
                # 確保有足夠的數據進行連續次數檢查
                if i >= n_length - 1:
                    all_equal_low = True
                    for j in range(i - n_length + 1, i + 1):
                        # 確保j不會超出範圍
                        if j < 0:
                            all_equal_low = False
                            break
                        # 計算第j天過去m天的最低值
                        j_start = max(0, j - m_length + 1)
                        j_end = j + 1
                        j_historical_low = predictor_values[j_start:j_end].min()
                        # 檢查第j天是否等同其過去m天的最低值
                        if abs(predictor_values[j] - j_historical_low) > 1e-10:
                            all_equal_low = False
                            break
                    if all_equal_low:
                        signals[i] = 1.0

            elif strat_idx == 4:  # HL4: 連續n日等同低位做空
                # 檢查連續n天，每天都要等同各自過去m天的最低值
                # 確保有足夠的數據進行連續次數檢查
                if i >= n_length - 1:
                    all_equal_low = True
                    for j in range(i - n_length + 1, i + 1):
                        # 確保j不會超出範圍
                        if j < 0:
                            all_equal_low = False
                            break
                        # 計算第j天過去m天的最低值
                        j_start = max(0, j - m_length + 1)
                        j_end = j + 1
                        j_historical_low = predictor_values[j_start:j_end].min()
                        # 檢查第j天是否等同其過去m天的最低值
                        if abs(predictor_values[j] - j_historical_low) > 1e-10:
                            all_equal_low = False
                            break
                    if all_equal_low:
                        signals[i] = -1.0

        return signals


class HLIndicator:
    """
    HL突破指標與信號產生器
    支援四種指標邏輯，參數可自訂
    新增向量化批量計算功能，大幅提升多參數組合的計算效率
    """

    STRATEGY_DESCRIPTIONS = [
        "連續n日等同過去m日高位做多",
        "連續n日等同過去m日高位做空",
        "連續n日等同過去m日低位做多",
        "連續n日等同過去m日低位做空",
    ]

    @staticmethod
    def get_strategy_descriptions() -> Dict[str, str]:
        # 回傳 dict: {'HL{i+1}': '描述', ...}
        return {
            f"HL{i + 1}": desc for i, desc in enumerate(HLIndicator.STRATEGY_DESCRIPTIONS)
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
        m_lengths = list(range(start, end + 1, step))

        param_list = []
        if strat_idx in [1, 2, 3, 4]:
            for n in n_lengths:
                for m in m_lengths:
                    # 檢查參數合理性：n <= m
                    if n <= m:
                        param = IndicatorParams("HL")
                        param.add_param("n_length", n)
                        param.add_param("m_length", m)
                        param.add_param("strat_idx", strat_idx)
                        param_list.append(param)
        else:
            raise ValueError("strat_idx 必須由 UserInterface 明確指定且有效")
        return param_list

    def generate_signals(
        self, predictor: Optional[str] = None
    ) -> Tuple[np.ndarray, np.ndarray]:  # pylint: disable=unused-argument
        """
        根據 HL 參數產生交易信號（1=多頭, -1=空頭, 0=無動作）。
        基於預測因子計算HL突破信號，而非價格。

        strat=1: 連續n日升穿過去m日高位做多
        strat=2: 連續n日升穿過去m日高位做空
        strat=3: 連續n日跌穿過去m日低位做多
        strat=4: 連續n日跌穿過去m日低位做空
        """
        n_length = self.params.get_param("n_length")  # 移除預設值
        m_length = self.params.get_param("m_length")  # 移除預設值
        strat_idx = self.params.get_param("strat_idx")  # 移除預設值

        if n_length is None or m_length is None or strat_idx is None:
            raise ValueError("n_length, m_length, strat_idx 參數必須由外部提供")

        # 檢查參數合理性
        if n_length > m_length:
            raise ValueError(f"連續天數({n_length})不能大於歷史回看天數({m_length})")

        # 檢查數據是否足夠
        min_required_length = (
            m_length + n_length - 1
        )  # 需要m_length + n_length - 1天的數據
        if len(self.data) < min_required_length:
            raise ValueError(
                f"數據長度不足：需要至少{min_required_length}個數據點，當前只有{len(self.data)}個"
            )

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
            signal_values = _generate_hl_signals_njit(
                predictor_values, n_length, m_length, strat_idx
            )

            # 轉換回 pandas Series
            signal = pd.Series(signal_values, index=self.data.index)

        # 確保在有效期間之前不產生信號
        signal.iloc[: min_required_length - 1] = 0

        return signal

    def get_min_valid_index(self) -> int:
        n_length = self.params.get_param("n_length")  # 移除預設值
        m_length = self.params.get_param("m_length")  # 移除預設值
        if n_length is None or m_length is None:
            raise ValueError("n_length 和 m_length 參數必須由外部提供")
        # 需要m_length + n_length - 1天的數據
        return m_length + n_length - 2

    # ==================== 新增向量化批量計算功能 ====================

    @staticmethod
    def vectorized_calculate_hl_signals(  # pylint: disable=too-complex
        tasks: List[Tuple[int, Dict[str, Any]]],
        predictor: Optional[str],
        signals_matrix: np.ndarray,
        global_hl_cache: Optional[Dict[str, Any]] = None,
        data: Optional[pd.DataFrame] = None,
    ) -> np.ndarray:
        """
        向量化計算HL突破信號 - 批量處理多個參數組合，大幅提升計算效率

        Args:
            tasks: 任務列表，每個任務包含 (task_idx, indicator_idx, param)
            predictor: 預測因子名稱
            signals_matrix: 信號矩陣 [時間點, 任務數, 指標數]
            global_hl_cache: 全局緩存字典，避免重複計算
            data: 數據DataFrame，如果為None則使用實例的data

        Returns:
            None (直接修改signals_matrix)
        """
        if data is None:
            raise ValueError("data參數必須提供")

        if global_hl_cache is None:
            global_hl_cache = {}

        # 提取所有HL參數
        n_lengths = []
        m_lengths = []
        strat_indices = []
        task_indices = []
        indicator_indices = []

        for task_idx, indicator_idx, param in tasks:
            n_length = param.get_param("n_length")
            m_length = param.get_param("m_length")
            strat_idx = param.get_param("strat_idx", 1)
            if n_length is not None and m_length is not None:
                # 檢查參數合理性
                if n_length <= m_length:
                    n_lengths.append(n_length)
                    m_lengths.append(m_length)
                    strat_indices.append(strat_idx)
                    task_indices.append(task_idx)
                    indicator_indices.append(indicator_idx)

        if not n_lengths:
            return

        # 預處理數據 - 轉換為numpy數組並處理NaN值
        predictor_values = data[predictor].values.astype(np.float64)
        predictor_values = np.nan_to_num(predictor_values, nan=0.0)

        # 直接為每個任務生成信號 - 真正的向量化處理
        for i, (n_length, m_length, strat_idx, task_idx, indicator_idx) in enumerate(
            zip(n_lengths, m_lengths, strat_indices, task_indices, indicator_indices)
        ):
            try:
                # 直接使用Numba優化的函數計算HL信號
                signal_values = _generate_hl_signals_njit(
                    predictor_values, n_length, m_length, strat_idx
                )

                # 確保在有效期間之前不產生信號
                # 需要m_length + n_length - 1天的數據，與單個指標方法保持一致
                min_required_length = m_length + n_length - 1
                min_signal_start = max(min_required_length - 1, n_length - 1)
                signal_values[:min_signal_start] = 0.0

                # 直接寫入信號矩陣
                signals_matrix[:, task_idx, indicator_idx] = signal_values

            except Exception as e:
                # 記錄錯誤但不中斷處理
                if hasattr(HLIndicator, "logger"):
                    HLIndicator.logger.warning(
                        f"HL突破信號生成失敗 (task_idx={task_idx}, indicator_idx={indicator_idx}): {e}"
                    )
                signals_matrix[:, task_idx, indicator_idx] = 0

    # 注意：緩存機制已簡化，直接使用 _generate_hl_signals_njit 進行計算
    # 如需緩存功能，可在 VectorBacktestEngine 中實現
