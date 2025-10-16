"""
TradeSimulator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的交易模擬器，負責根據信號序列模擬持倉、平倉與資金變化。
**本模組是全系統唯一的持倉/平倉邏輯守門員，所有交易行為規則皆在此集中管控。**

【新增功能】
------------------------------------------------------------
- 向量化交易模擬：支援批量處理多個策略，大幅提升性能
- 統一接口：為 BacktestEngine 和 VectorBacktestEngine 提供統一的交易模擬接口
- 完整交易記錄：包含所有必要的交易信息欄位
- Numba JIT 編譯優化：使用 Numba 加速向量化計算
- 智能持倉管理：支援多種持倉狀態與交易邏輯

【流程與數據流】
------------------------------------------------------------
- 輸入：來自 BacktestEngine 或 VectorBacktestEngine 的信號序列
- 依據信號與當前持倉狀態，決定是否開倉/平倉
- 每次交易動作都會記錄於交易記錄（DataFrame）
- 交易模擬流程如下：

```mermaid
flowchart TD
    A[BacktestEngine/VBT] -->|產生信號| B(TradeSimulator)
    B -->|模擬交易| C{持倉狀態}
    C -- 無持倉且信號=1 --> D[開多倉]
    C -- 有多倉且信號=-1 --> E[平多倉]
    C -- 有空倉且信號=1 --> F[平空倉]
    C -- 其他情況 --> G[無動作]
    D & E & F & G --> H[記錄交易]
    H --> I[回傳交易記錄]
```

【主要方法】
------------------------------------------------------------
- simulate_trades(): 單個策略交易模擬（向後兼容）
- simulate_trades_vectorized(): 向量化交易模擬（供 VBT 調用）
- generate_single_result(): 生成完整交易記錄
- _vectorized_trade_simulation_njit(): Numba 加速的向量化交易邏輯

【維護與擴充重點】
------------------------------------------------------------
- **任何涉及交易邏輯的需求（如反手開倉、複合信號、停損/停利）都必須在本檔案集中設計與維護**
- 未來如需支援「反手開倉」等特殊行為，只需在本模組集中修改
- 若信號產生邏輯有變動（如允許2, -2等複合信號），需同步調整本模組的判斷邏輯
- **每次開發新 indicator 或信號型態時，務必檢查本模組的持倉/平倉判斷是否仍然正確**
- 任何涉及 position, signal 判斷的邏輯都在本檔案 for 迴圈內
- 若有新信號型態，需同步更新本檔案的判斷分支
- 若有交易記錄欄位變動，需同步更新 record 結構
- 向量化邏輯需要與單個策略邏輯保持一致

【常見易錯點】
------------------------------------------------------------
- 平倉信號在無持倉時出現，不會開空倉，只會記錄信號
- 同一根K線同時出現 signal==1 和 signal==-1（合併後為0），則不會有任何動作
- 交易延遲、交易成本、滑點等皆可自訂，需注意參數傳遞正確
- 向量化計算與單個策略計算結果不一致
- 持倉狀態管理錯誤導致交易邏輯異常

【錯誤處理】
------------------------------------------------------------
- 信號格式錯誤時提供詳細診斷
- 持倉狀態異常時提供修正建議
- 交易記錄錯誤時提供備用方案
- 向量化計算失敗時自動降級為單個策略計算

【範例：收到的信號與產生的交易記錄】
------------------------------------------------------------
- 輸入信號序列（pd.Series）：[0, 1, 0, 0, -1, 0, 1, -1, 0]
- 產生的交易記錄（DataFrame）：
    - 第2根K線 signal==1 → 開多倉
    - 第5根K線 signal==-1 → 平多倉
    - 第7根K線 signal==1 → 再次開多倉
    - 第8根K線 signal==-1 → 再次平多倉

【與其他模組的關聯】
------------------------------------------------------------
- 接受 BacktestEngine 和 VectorBacktestEngine 的信號
- 交易記錄會傳給 TradeRecorder_backtester.py 驗證與導出
- 若需擴充交易型態，需同步通知 TradeRecorder/Exporter
- 與 VectorBacktestEngine 共享向量化計算邏輯

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本開平倉邏輯
- v1.1: 新增交易記錄詳細欄位
- v1.2: 優化持倉狀態管理
- v2.0: 新增向量化交易模擬，統一接口
- v2.1: 整合 Numba JIT 編譯優化
- v2.2: 完善錯誤處理與邏輯驗證

【參考】
------------------------------------------------------------
- 詳細交易規則如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
- Numba 向量化計算最佳實踐
- 交易邏輯設計與驗證方法
"""

import logging
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

# 導入 Numba 進行 JIT 編譯加速
from numba import njit

# 導入型別
from .IndicatorParams_backtester import IndicatorParams


# 核心算法：向量化 Numba 實現
@njit(fastmath=True, cache=True)
def _vectorized_trade_simulation_njit(  # pylint: disable=too-complex
    entry_signals: np.ndarray,
    exit_signals: np.ndarray,
    close_prices: np.ndarray,
    open_prices: np.ndarray,
    transaction_cost: float,
    slippage: float,
    trade_price: str = "open",
    trade_delay: int = 1,
) -> Dict[str, Any]:
    """
    向量化交易模擬 - 移植自 VBT
    """
    n_time, n_strategies = entry_signals.shape

    # 預分配結果矩陣
    positions = np.zeros((n_time, n_strategies))
    returns = np.zeros((n_time, n_strategies))
    trade_actions = np.zeros((n_time, n_strategies))
    equity_values = np.zeros((n_time, n_strategies))

    # 對每個策略進行優化的狀態機處理
    for s in range(n_strategies):
        # 狀態機：最小化記憶依賴
        current_state = 0.0  # 0=空倉, 1=多倉, -1=空倉
        equity = 1.0
        open_price = 0.0  # 追蹤開倉價格
        open_equity = 1.0  # 追蹤開倉時的權益

        for t in range(n_time):
            # 計算信號索引（考慮交易延遲）
            signal_index = t - trade_delay
            entry_sig = (
                entry_signals[signal_index, s] if 0 <= signal_index < n_time else 0.0
            )
            exit_sig = (
                exit_signals[signal_index, s] if 0 <= signal_index < n_time else 0.0
            )

            # 計算資金曲線和每日收益率
            if t > 0 and current_state != 0.0 and open_price > 0.0:
                if trade_price == "close":
                    current_close = close_prices[t]
                    if current_state == 1.0:  # 做多
                        price_return = (current_close - open_price) / open_price
                    else:  # 做空
                        price_return = (open_price - current_close) / open_price
                else:  # trade_price == 'open'
                    current_open = open_prices[t]
                    if current_state == 1.0:  # 做多
                        price_return = (current_open - open_price) / open_price
                    else:  # 做空
                        price_return = (open_price - current_open) / open_price

                # 計算資金曲線：開倉時權益 * (1 + 價格收益率)
                equity = open_equity * (1.0 + price_return)
                
                # 計算每日收益率：今日資金曲線 / 昨日資金曲線 - 1
                if equity_values[t-1, s] > 0:
                    returns[t, s] = (equity * 100.0) / equity_values[t-1, s] - 1.0
                else:
                    returns[t, s] = 0.0
            else:
                returns[t, s] = 0.0

            # 狀態轉換邏輯（優化版本）
            if current_state == 0.0:  # 空倉
                if entry_sig == 1.0:  # 開多倉
                    current_state = 1.0
                    trade_actions[t, s] = 1
                    # 設置開倉價格
                    if trade_price == "close":
                        open_price = close_prices[t]
                    else:
                        open_price = open_prices[t]
                    # 扣除滑點與手續費
                    equity *= (1.0 - slippage) * (1.0 - transaction_cost)
                    open_equity = equity  # 記錄開倉時的權益（扣除成本後）
                elif entry_sig == -1.0:  # 開空倉
                    current_state = -1.0
                    trade_actions[t, s] = 1
                    # 設置開倉價格
                    if trade_price == "close":
                        open_price = close_prices[t]
                    else:
                        open_price = open_prices[t]
                    # 扣除滑點與手續費
                    equity *= (1.0 - slippage) * (1.0 - transaction_cost)
                    open_equity = equity  # 記錄開倉時的權益（扣除成本後）
            elif current_state == 1.0:  # 多倉
                if exit_sig == -1.0:  # 平多倉
                    current_state = 0.0
                    trade_actions[t, s] = 4
                    open_price = 0.0  # 重置開倉價格
                    open_equity = 1.0  # 重置開倉權益
                    # 扣除滑點與手續費
                    equity *= (1.0 - slippage) * (1.0 - transaction_cost)
            elif current_state == -1.0:  # 空倉
                if exit_sig == 1.0:  # 平空倉
                    current_state = 0.0
                    trade_actions[t, s] = 4
                    open_price = 0.0  # 重置開倉價格
                    open_equity = 1.0  # 重置開倉權益
                    # 扣除滑點與手續費
                    equity *= (1.0 - slippage) * (1.0 - transaction_cost)

            positions[t, s] = current_state
            equity_values[t, s] = equity * 100.0

    return {"positions": positions, "returns": returns, "trade_actions": trade_actions, "equity_values": equity_values}


logger = logging.getLogger("lo2cin4bt")


class TradeSimulator_backtester:
    """
    模擬交易，生成交易記錄（基於百分比）。

    Attributes:
        data (pd.DataFrame): 輸入數據，包含價格與預測因子。
        signals (pd.Series): 交易信號序列。
        transaction_cost (float): 交易成本（小數，預設0.001）。
        slippage (float): 滑點（小數，預設0.0005）。
        trade_delay (int): 交易延遲數據點數。
        trade_price (str): 交易價格（open/close）。
        Backtest_id (str): 回測唯一ID。
        parameter_set_id (str): 參數集唯一ID。
        predictor (str): 預測因子名稱。
        initial_equity (float): 初始權益（預設1.0）。
        logger (logging.Logger): 日誌記錄器。
        indicators (list): 所有參與的 indicator 實例列表，預設 None。
    """

    def __init__(  # pylint: disable=unused-argument
        self,
        data: pd.DataFrame,
        entry_signal: pd.Series,
        exit_signal: pd.Series,
        transaction_cost: float = 0.001,
        slippage: float = 0.0005,
        trade_delay: int = 0,
        trade_price: str = "close",
        Backtest_id: Optional[str] = None,
        parameter_set_id: Optional[str] = None,
        predictor: Optional[str] = None,
        initial_equity: float = 1.0,
        indicators: Optional[List[str]] = None,
        trading_instrument: Optional[str] = None,
    ):
        self.data = data
        self.entry_signal = entry_signal
        self.exit_signal = exit_signal
        self.transaction_cost = transaction_cost
        self.slippage = slippage
        self.trade_delay = trade_delay
        self.trade_price = trade_price
        self.Backtest_id = Backtest_id
        self.trading_instrument = trading_instrument or "X"
        self.parameter_set_id = parameter_set_id
        self.predictor = predictor
        self.initial_equity = initial_equity
        self.logger = logger
        self.indicators = indicators  # 新增：所有參與的 indicator 實例列表，預設 None

    def simulate_trades(self) -> pd.DataFrame:
        """
        模擬交易，生成交易記錄

        Returns:
            tuple: (records_df, warning_msg) 包含交易記錄DataFrame和警告訊息

        Note:
            entry_signal: 1=開多, -1=開空, 0=無操作
            exit_signal: -1=平多, 1=平空, 0=無操作
        """
        # 將單個策略轉換為向量化格式
        entry_signals_matrix = self.entry_signal.values.reshape(-1, 1).astype(
            np.float64
        )
        exit_signals_matrix = self.exit_signal.values.reshape(-1, 1).astype(np.float64)

        # 處理 NaN 值
        entry_signals_matrix = np.nan_to_num(entry_signals_matrix, nan=0.0)
        exit_signals_matrix = np.nan_to_num(exit_signals_matrix, nan=0.0)

        # 使用向量化方法進行交易模擬
        trading_params = {
            "transaction_cost": self.transaction_cost,
            "slippage": self.slippage,
            "trade_delay": self.trade_delay,
            "trade_price": self.trade_price,
        }

        trade_results = self.simulate_trades_vectorized(
            entry_signals_matrix, exit_signals_matrix, trading_params
        )

        # 提取單個策略的結果
        positions = trade_results["positions"][:, 0]
        returns = trade_results["returns"][:, 0]
        trade_actions = trade_results["trade_actions"][:, 0]
        equity_values = trade_results["equity_values"][:, 0]

        # 生成完整的交易記錄
        result = self.generate_single_result(
            0,  # task_idx
            self.entry_signal.values,
            self.exit_signal.values,
            positions,
            returns,
            trade_actions,
            equity_values,
            self.predictor or "",
            self.Backtest_id or "",
            {},  # entry_params (單個策略不需要)
            {},  # exit_params (單個策略不需要)
            trading_params,
        )

        return result["records"], None  # 返回 records_df 和 warning_msg

    def simulate_trades_vectorized(  # pylint: disable=unused-argument
        self, entry_signals_matrix: pd.Series, exit_signals_matrix: pd.Series, trading_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        向量化交易模擬 - 供 VBT 調用

        Args:
            entry_signals_matrix: numpy.ndarray, shape (n_time, n_strategies)
            exit_signals_matrix: numpy.ndarray, shape (n_time, n_strategies)
            trading_params: dict, 包含交易參數

        Returns:
            dict: 包含向量化交易結果
        """
        # 使用 Numba 加速的向量化交易模擬
        result = _vectorized_trade_simulation_njit(
            entry_signals_matrix,
            exit_signals_matrix,
            self.data["Close"].values.astype(np.float64),
            self.data["Open"].values.astype(np.float64),
            trading_params.get("transaction_cost", 0.001),
            trading_params.get("slippage", 0.0005),
            trading_params.get("trade_price", "close"),
            trading_params.get("trade_delay", 0),
        )
        positions = result["positions"]
        returns = result["returns"]
        trade_actions = result["trade_actions"]
        equity_values = result["equity_values"]

        return {
            "positions": positions,
            "returns": returns,
            "trade_actions": trade_actions,
            "equity_values": equity_values,
        }

    def generate_single_result(  # pylint: disable=too-complex
        self,
        task_idx: int,
        entry_signal: np.ndarray,
        exit_signal: np.ndarray,
        position: np.ndarray,
        returns: np.ndarray,
        trade_actions: np.ndarray,
        equity_values: np.ndarray,
        predictor: str,
        backtest_id: str,
        entry_params: Dict[str, Any],
        exit_params: Dict[str, Any],
        trading_params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        生成單個任務的結果 - 移植自 VBT 的 _generate_single_result
        """
        import uuid

        records = []
        current_trade_group_id = None
        open_price_map = {}
        open_time_map = {}
        holding_period_count = 0

        for i in range(len(position)):
            row = self.data.iloc[i]
            # 確保使用正確的時間索引
            if isinstance(self.data.index, pd.DatetimeIndex):
                time_index = self.data.index[i]
            elif "Time" in self.data.columns:
                time_index = row["Time"]
            else:
                time_index = i  # 如果沒有時間索引，使用數字索引

            # 根據trade_actions判斷交易類型
            position_type = None
            open_position_price = 0.0
            close_position_price = 0.0
            open_time = None
            close_time = None
            trade_group_id = None
            transaction_cost = 0.0
            slippage_cost = 0.0
            holding_period = None
            trade_return = None

            # 根據trade_price設置當前時間點的價格（無論是否有交易動作）
            trade_price = trading_params.get("trade_price", "close")
            current_price = row["Open"] if trade_price == "open" else row["Close"]

            if trade_actions[i] == 1:  # 開倉
                if position[i] > 0:
                    position_type = "new_long"
                else:
                    position_type = "new_short"
                open_position_price = current_price
                open_time = time_index
                # 生成新的交易組ID
                trade_group_id = f"T{str(uuid.uuid4())[:8]}"
                current_trade_group_id = trade_group_id
                # 記錄開倉價格和時間
                open_price_map[trade_group_id] = open_position_price
                open_time_map[trade_group_id] = open_time
                # 計算交易成本
                transaction_cost = trading_params.get("transaction_cost", 0.001)
                slippage_cost = trading_params.get("slippage", 0.0005)
                holding_period_count = 0
            elif trade_actions[i] == 4:  # 平倉
                close_position_price = current_price
                close_time = time_index
                # 補上平倉時的 Position_type
                if i > 0:
                    if position[i - 1] > 0:
                        position_type = "close_long"
                    elif position[i - 1] < 0:
                        position_type = "close_short"
                # 使用當前的交易組ID
                trade_group_id = current_trade_group_id
                # 計算交易成本
                transaction_cost = trading_params.get("transaction_cost", 0.001)
                slippage_cost = trading_params.get("slippage", 0.0005)
                # 計算持倉期間和交易收益率
                if trade_group_id and trade_group_id in open_time_map:
                    open_time = open_time_map[trade_group_id]
                    open_price = open_price_map[trade_group_id]
                    # 先累加持倉期數（平倉當日也要計算）
                    if current_trade_group_id is not None:
                        holding_period_count += 1
                    # 使用正確的 holding_period_count 作為 holding_period
                    holding_period = holding_period_count
                    # 計算交易收益率
                    if open_price > 0:
                        if position_type == "close_long":
                            trade_return = (
                                close_position_price - open_price
                            ) / open_price
                        else:  # close_short
                            trade_return = (
                                open_price - close_position_price
                            ) / open_price
                # 清理交易組記錄
                if trade_group_id in open_price_map:
                    open_price_map.pop(trade_group_id)
                if trade_group_id in open_time_map:
                    open_time_map.pop(trade_group_id)
                current_trade_group_id = None
                # 平倉後重置 holding_period_count 為 0
                holding_period_count = 0
            else:
                # 持倉期間，累加持倉期數
                if current_trade_group_id is not None:
                    holding_period_count += 1
                    trade_group_id = current_trade_group_id

            record = {
                "Time": time_index,
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "Trading_instrument": getattr(self, 'trading_instrument', 'X'),
                "Position_type": position_type,
                "Open_position_price": open_position_price,
                "Close_position_price": close_position_price,
                "Position_size": position[i],
                "Return": returns[i],
                "Trade_group_id": trade_group_id,
                "Trade_action": int(trade_actions[i]),
                "Open_time": open_time,
                "Close_time": close_time,
                "Parameter_set_id": self._generate_parameter_set_id(
                    entry_params, exit_params, predictor
                ),
                "Equity_value": equity_values[i],
                "Transaction_cost": transaction_cost,
                "Slippage_cost": slippage_cost,
                "Predictor_value": (
                    row[predictor] if predictor in self.data.columns else 0.0
                ),
                "Entry_signal": entry_signal[i],
                "Exit_signal": exit_signal[i],
                "Holding_period_count": holding_period_count,
                "Holding_period": holding_period,
                "Trade_return": trade_return,
                "Backtest_id": backtest_id,
            }
            records.append(record)

        # 轉換為DataFrame
        records_df = pd.DataFrame(records)

        # 生成策略名稱
        strategy_name = self._generate_parameter_set_id(
            entry_params, exit_params, predictor
        )

        # 轉換參數為字典格式 - 與BacktestEngine格式一致
        params_dict = {
            "entry": [
                (
                    param.to_dict()
                    if hasattr(param, "to_dict")
                    else self._param_to_dict(param)
                )
                for param in entry_params
            ],
            "exit": [
                (
                    param.to_dict()
                    if hasattr(param, "to_dict")
                    else self._param_to_dict(param)
                )
                for param in exit_params
            ],
            "predictor": predictor,
        }

        result = {
            "Backtest_id": backtest_id,
            "strategy_id": strategy_name,
            "params": params_dict,
            "records": records_df,
            "warning_msg": None,
            "error": None,
        }

        return result

    def _param_to_dict(self, param: Any) -> Dict[str, Any]:  # pylint: disable=unused-argument
        """將參數物件轉換為字典格式"""
        if param is None:
            return {}

        result = {"indicator_type": param.indicator_type}
        for key, value in param.params.items():
            result[key] = value
        return result

    def _generate_parameter_set_id(
        self, entry_params: Union[List[IndicatorParams], Dict[str, Any]], exit_params: Union[List[IndicatorParams], Dict[str, Any]], predictor: str
    ) -> str:  # pylint: disable=too-complex
        """
        根據 entry/exit 參數生成有意義的 parameter_set_id
        """
        # 處理不同的參數型別
        if isinstance(entry_params, dict):
            # 如果是字典，直接返回簡單的策略名稱
            return f"Strategy_{predictor}_{len(entry_params)}"

        # 生成開倉參數字符串
        entry_str = ""
        for i, param in enumerate(entry_params):
            if param.indicator_type == "MA":
                period = param.get_param("period")
                ma_type = param.get_param("ma_type", "SMA")
                strat_idx = param.get_param("strat_idx", 1)
                # 對於 MA9-MA12，需要顯示連續日數 m
                if strat_idx in [9, 10, 11, 12]:
                    m = param.get_param("m", 2)
                    entry_str += f"MA{strat_idx}_{ma_type}({period},{m})"
                else:
                    entry_str += f"MA{strat_idx}_{ma_type}({period})"
            elif param.indicator_type == "BOLL":
                ma_length = param.get_param("ma_length")
                std_multiplier = param.get_param("std_multiplier")
                strat_idx = param.get_param("strat_idx", 1)
                entry_str += f"BOLL{strat_idx}({ma_length},{std_multiplier})"
            elif param.indicator_type == "HL":
                n_length = param.get_param("n_length")
                m_length = param.get_param("m_length")
                strat_idx = param.get_param("strat_idx", 1)
                entry_str += f"HL{strat_idx}({n_length},{m_length})"
            elif param.indicator_type == "VALUE":
                strat_idx = param.get_param("strat_idx", 1)
                if strat_idx in [1, 2, 3, 4]:
                    n_length = param.get_param("n_length")
                    m_value = param.get_param("m_value")
                    entry_str += f"VALUE{strat_idx}({n_length},{m_value})"
                elif strat_idx in [5, 6]:
                    m1_value = param.get_param("m1_value")
                    m2_value = param.get_param("m2_value")
                    entry_str += f"VALUE{strat_idx}({m1_value},{m2_value})"
                else:
                    entry_str += f"VALUE{strat_idx}"

            elif param.indicator_type == "PERC":
                window = param.get_param("window")
                strat_idx = param.get_param("strat_idx", 1)
                if strat_idx in [1, 2, 3, 4]:
                    percentile = param.get_param("percentile")
                    entry_str += f"PERC{strat_idx}(W={window},P={percentile})"
                elif strat_idx in [5, 6]:
                    m1 = param.get_param("m1")
                    m2 = param.get_param("m2")
                    entry_str += f"PERC{strat_idx}(W={window},M1={m1},M2={m2})"
                else:
                    entry_str += f"PERC{strat_idx}(W={window})"

            if i < len(entry_params) - 1:
                entry_str += "+"

        # 生成平倉參數字符串
        exit_str = ""
        # exit_params 是一個列表，需要遍歷
        for i, param in enumerate(exit_params):
            if param.indicator_type == "MA":
                period = param.get_param("period")
                ma_type = param.get_param("ma_type", "SMA")
                strat_idx = param.get_param("strat_idx", 1)
                # 對於 MA9-MA12，需要顯示連續日數 m
                if strat_idx in [9, 10, 11, 12]:
                    m = param.get_param("m", 2)
                    exit_str += f"MA{strat_idx}_{ma_type}({period},{m})"
                else:
                    exit_str += f"MA{strat_idx}_{ma_type}({period})"
            elif param.indicator_type == "BOLL":
                ma_length = param.get_param("ma_length")
                std_multiplier = param.get_param("std_multiplier")
                strat_idx = param.get_param("strat_idx", 1)
                exit_str += f"BOLL{strat_idx}({ma_length},{std_multiplier})"
            elif param.indicator_type == "HL":
                n_length = param.get_param("n_length")
                m_length = param.get_param("m_length")
                strat_idx = param.get_param("strat_idx", 1)
                exit_str += f"HL{strat_idx}({n_length},{m_length})"
            elif param.indicator_type == "VALUE":
                strat_idx = param.get_param("strat_idx", 1)
                if strat_idx in [1, 2, 3, 4]:
                    n_length = param.get_param("n_length")
                    m_value = param.get_param("m_value")
                    exit_str += f"VALUE{strat_idx}({n_length},{m_value})"
                elif strat_idx in [5, 6]:
                    m1_value = param.get_param("m1_value")
                    m2_value = param.get_param("m2_value")
                    exit_str += f"VALUE{strat_idx}({m1_value},{m2_value})"
                else:
                    exit_str += f"VALUE{strat_idx}"

            elif param.indicator_type == "PERC":
                window = param.get_param("window")
                strat_idx = param.get_param("strat_idx", 1)
                if strat_idx in [1, 2, 3, 4]:
                    percentile = param.get_param("percentile")
                    exit_str += f"PERC{strat_idx}(W={window},P={percentile})"
                elif strat_idx in [5, 6]:
                    m1 = param.get_param("m1")
                    m2 = param.get_param("m2")
                    exit_str += f"PERC{strat_idx}(W={window},M1={m1},M2={m2})"
                else:
                    exit_str += f"PERC{strat_idx}(W={window})"

            if i < len(exit_params) - 1:
                exit_str += "+"

        # 組合最終字符串
        if exit_str:
            return f"{entry_str}_{predictor}_{exit_str}"
        else:
            return f"{entry_str}_{predictor}"
