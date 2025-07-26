"""
TradeSimulator_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的交易模擬器，負責根據信號序列模擬持倉、平倉與資金變化。
**本模組是全系統唯一的持倉/平倉邏輯守門員，所有交易行為規則皆在此集中管控。**

【關聯流程與數據流】
------------------------------------------------------------
- 輸入：來自 BacktestEngine 的信號序列（pd.Series, 僅允許 1, -1, 0）
- 依據信號與當前持倉狀態，決定是否開倉/平倉
- 每次交易動作都會記錄於交易記錄（DataFrame），並回傳給 TradeRecorder/Exporter
- 交易模擬流程如下：

```mermaid
flowchart TD
    A[BacktestEngine] -->|產生信號| B(TradeSimulator)
    B -->|模擬交易| C{持倉狀態}
    C -- 無持倉且信號=1 --> D[開多倉]
    C -- 有多倉且信號=-1 --> E[平多倉]
    C -- 有空倉且信號=1 --> F[平空倉]
    C -- 其他情況 --> G[無動作]
    D & E & F & G --> H[記錄交易]
    H --> I[回傳交易記錄]
```

【信號與持倉規則】
------------------------------------------------------------
- 1   ：做多信號（開多倉）
- -1  ：做空信號（開空倉）
- 0   ：無動作
- 只有 signal==1 能開多倉，signal==-1 只能在有多倉時平倉，不能直接開空
- 只有 signal==-1 能平多倉，signal==1 只能在有空倉時平空，不能直接開多
- **平倉信號永遠不會直接開反向倉位，只能在有持倉時觸發平倉**

【維護與擴充提醒】
------------------------------------------------------------
- **任何涉及交易邏輯的需求（如反手開倉、複合信號、停損/停利）都必須在本檔案集中設計與維護**
- 未來如需支援「反手開倉」等特殊行為，只需在本模組集中修改
- 若信號產生邏輯有變動（如允許2, -2等複合信號），需同步調整本模組的判斷邏輯
- **每次開發新 indicator 或信號型態時，務必檢查本模組的持倉/平倉判斷是否仍然正確**

【常見易錯點】
------------------------------------------------------------
- 平倉信號在無持倉時出現，不會開空倉，只會記錄信號
- 同一根K線同時出現 signal==1 和 signal==-1（合併後為0），則不會有任何動作
- 交易延遲、交易成本、滑點等皆可自訂，需注意參數傳遞正確

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
- 只接受 BacktestEngine 合併後的最終信號
- 交易記錄會傳給 TradeRecorder_backtester.py 驗證與導出
- 若需擴充交易型態，需同步通知 TradeRecorder/Exporter

【維護重點】
------------------------------------------------------------
- 任何涉及 position, signal 判斷的邏輯都在本檔案 for 迴圈內
- 若有新信號型態，需同步更新本檔案的判斷分支
- 若有交易記錄欄位變動，需同步更新 record 結構

【參考】
------------------------------------------------------------
- 詳細交易規則如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
"""

import pandas as pd
import numpy as np
import logging
import uuid
import os

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

    def __init__(self, data, entry_signal, exit_signal, transaction_cost=0.001, slippage=0.0005, trade_delay=0, trade_price='close', Backtest_id=None, parameter_set_id=None, predictor=None, initial_equity=1.0, indicators=None):
        self.data = data
        self.entry_signal = entry_signal
        self.exit_signal = exit_signal
        self.transaction_cost = transaction_cost
        self.slippage = slippage
        self.trade_delay = trade_delay
        self.trade_price = trade_price
        self.Backtest_id = Backtest_id
        self.parameter_set_id = parameter_set_id
        self.predictor = predictor
        self.initial_equity = initial_equity
        self.logger = logger
        self.indicators = indicators  # 新增：所有參與的 indicator 實例列表，預設 None

        entry_counts = entry_signal.value_counts().to_dict()
        exit_counts = exit_signal.value_counts().to_dict()


    def simulate_trades(self):
        """
        模擬交易，生成交易記錄。
        entry_signal: 1=開多, -1=開空, 0=無操作
        exit_signal: -1=平多, 1=平空, 0=無操作
        """
        if hasattr(self, 'indicators') and self.indicators:
            min_indices = [ind.get_min_valid_index() for ind in self.indicators]
            start_index = max(min_indices)
        else:
            start_index = 0

        records = []
        position = 0.0  # 0: 無持倉, 1: 多頭, -1: 空頭
        equity = self.initial_equity
        trade_group_id = None
        trading_instrument = "BTCUSDT"
        Holding_period_count = 0
        # 新增：追蹤每個trade_group_id的開倉價與開倉時間
        open_price_map = {}
        open_time_map = {}


        for i in range(start_index, len(self.data)):
            row = self.data.iloc[i]
            signal_index = i - self.trade_delay
            entry_sig = self.entry_signal.iloc[signal_index] if 0 <= signal_index < len(self.entry_signal) else 0
            exit_sig = self.exit_signal.iloc[signal_index] if 0 <= signal_index < len(self.exit_signal) else 0
            Predictor_value = row[self.predictor] if self.predictor and self.predictor in self.data.columns else 0.0

            if i == 0:
                Holding_period_count = 0
            elif position != 0:
                Holding_period_count += 1
            else:
                Holding_period_count = 0

            if i > 0 and position != 0:
                prev_close = self.data.iloc[i - 1]["Close"]
                ret = (row["Close"] - prev_close) / prev_close * position
                equity *= (1 + ret)
            else:
                ret = 0.0

            record = {
                "Time": row["Time"],
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "Trading_instrument": trading_instrument,
                "Position_type": None,
                "Open_position_price": 0.0,
                "Close_position_price": 0.0,
                "Position_size": position if position != 0 else 0.0,
                "Return": ret,
                "Trade_group_id": trade_group_id,
                "Trade_action": 0,
                "Open_time": None,
                "Close_time": None,
                "Parameter_set_id": self.parameter_set_id,
                "Equity_value": equity * 100,
                "Transaction_cost": 0.0,
                "Slippage_cost": 0.0,
                "Predictor_value": Predictor_value,
                "Entry_signal": entry_sig,
                "Exit_signal": exit_sig,
                "Holding_period_count": Holding_period_count,
                "Holding_period": None,
                "Trade_return": None,
                "Backtest_id": self.Backtest_id
            }

            trade_price_value = row[self.trade_price.capitalize()]

            # 無持倉時，只能根據 entry_signal 開倉
            if position == 0:
                if entry_sig == 1:
                    record["Trade_action"] = 1
                    record["Position_type"] = "new_long"
                    record["Open_position_price"] = trade_price_value
                    record["Position_size"] = 1.0
                    record["Open_time"] = row["Time"]
                    trade_group_id = f"T{str(uuid.uuid4())[:8]}"
                    record["Trade_group_id"] = trade_group_id
                    position = 1.0
                    # 新增：記錄開倉價與開倉時間
                    open_price_map[trade_group_id] = trade_price_value
                    open_time_map[trade_group_id] = row["Time"]
                    # === DEBUG: 開多倉時扣除滑點與手續費 ===
                    equity_before = equity
                    equity *= (1 - self.slippage)
                    equity *= (1 - self.transaction_cost)
                    record["Slippage_cost"] = self.slippage
                    record["Transaction_cost"] = self.transaction_cost
                    self.logger.info(f"開多頭倉: 價格={trade_price_value:.2f}, 時間={row['Time']}", extra={"Backtest_id": self.Backtest_id})
                elif entry_sig == -1:
                    record["Trade_action"] = 1
                    record["Position_type"] = "new_short"
                    record["Open_position_price"] = trade_price_value
                    record["Position_size"] = -1.0
                    record["Open_time"] = row["Time"]
                    trade_group_id = f"T{str(uuid.uuid4())[:8]}"
                    record["Trade_group_id"] = trade_group_id
                    position = -1.0
                    # 新增：記錄開倉價與開倉時間
                    open_price_map[trade_group_id] = trade_price_value
                    open_time_map[trade_group_id] = row["Time"]
                    # === DEBUG: 開空倉時扣除滑點與手續費 ===
                    equity_before = equity
                    equity *= (1 - self.slippage)
                    equity *= (1 - self.transaction_cost)
                    record["Slippage_cost"] = self.slippage
                    record["Transaction_cost"] = self.transaction_cost
                    self.logger.info(f"開空頭倉: 價格={trade_price_value:.2f}, 時間={row['Time']}", extra={"Backtest_id": self.Backtest_id})
                # 其他信號無動作
            # 有多倉時，只能根據 exit_signal 平多
            elif position == 1:
                if exit_sig == -1:
                    record["Trade_action"] = 4
                    record["Position_type"] = "close_long"
                    record["Close_position_price"] = trade_price_value
                    record["Position_size"] = 0.0
                    record["Close_time"] = row["Time"]
                    record["Trade_group_id"] = trade_group_id
                    # === DEBUG: 平多倉時扣除滑點與手續費 ===
                    equity_before = equity
                    equity *= (1 - self.slippage)
                    equity *= (1 - self.transaction_cost)
                    record["Slippage_cost"] = self.slippage
                    record["Transaction_cost"] = self.transaction_cost
                    position = 0.0
                    # 修改：僅在 Trade Action == 4 時計算 Holding_period
                    open_price = open_price_map.get(trade_group_id, None)
                    open_time = open_time_map.get(trade_group_id, None)
                    close_price = trade_price_value
                    close_time = row["Time"]
                    # 只有 Trade Action == 4 才計算 Holding_period
                    if record["Trade_action"] == 4:
                        if open_time is not None and close_time is not None:
                            try:
                                holding_days = (pd.to_datetime(close_time) - pd.to_datetime(open_time)).days
                                holding_days = max(holding_days, 1)
                            except Exception:
                                holding_days = 1
                        else:
                            holding_days = 1
                        record["Holding_period"] = holding_days
                    else:
                        record["Holding_period"] = None
                    # 平倉（3,4）都計算 Trade_return，其餘為 None
                    if record["Trade_action"] in [3, 4] and open_price and open_price != 0:
                        record["Trade_return"] = (close_price - open_price) / open_price * 100
                    else:
                        record["Trade_return"] = None
                    # 平倉後移除該trade_group_id的開倉資訊
                    open_price_map.pop(trade_group_id, None)
                    open_time_map.pop(trade_group_id, None)
                    self.logger.info(f"平多倉: 價格={trade_price_value:.2f}, 時間={row['Time']}", extra={"Backtest_id": self.Backtest_id})
                    Holding_period_count = 0
                # 其他信號無動作
            # 有空倉時，只能根據 exit_signal 平空
            elif position == -1:
                if exit_sig == 1:
                    record["Trade_action"] = 4
                    record["Position_type"] = "close_short"
                    record["Close_position_price"] = trade_price_value
                    record["Position_size"] = 0.0
                    record["Close_time"] = row["Time"]
                    record["Trade_group_id"] = trade_group_id
                    # === DEBUG: 平空倉時扣除滑點與手續費 ===
                    equity_before = equity
                    equity *= (1 - self.slippage)
                    equity *= (1 - self.transaction_cost)
                    record["Slippage_cost"] = self.slippage
                    record["Transaction_cost"] = self.transaction_cost
                    position = 0.0
                    # 修改：僅在 Trade Action == 4 時計算 Holding_period
                    open_price = open_price_map.get(trade_group_id, None)
                    open_time = open_time_map.get(trade_group_id, None)
                    close_price = trade_price_value
                    close_time = row["Time"]
                    # 只有 Trade Action == 4 才計算 Holding_period
                    if record["Trade_action"] == 4:
                        if open_time is not None and close_time is not None:
                            try:
                                holding_days = (pd.to_datetime(close_time) - pd.to_datetime(open_time)).days
                                holding_days = max(holding_days, 1)
                            except Exception:
                                holding_days = 1
                        else:
                            holding_days = 1
                        record["Holding_period"] = holding_days
                    else:
                        record["Holding_period"] = None
                    # 平倉（3,4）都計算 Trade_return，其餘為 None
                    if record["Trade_action"] in [3, 4] and open_price and open_price != 0:
                        record["Trade_return"] = -(close_price - open_price) / open_price * 100
                    else:
                        record["Trade_return"] = None
                    # 平倉後移除該trade_group_id的開倉資訊
                    open_price_map.pop(trade_group_id, None)
                    open_time_map.pop(trade_group_id, None)
                    self.logger.info(f"平空倉: 價格={trade_price_value:.2f}, 時間={row['Time']}", extra={"Backtest_id": self.Backtest_id})
                    Holding_period_count = 0
                # 其他信號無動作

            records.append(record)

        df = pd.DataFrame(records)
        trade_count = len(df[df["Trade_action"] != 0])
        # print(f"\n交易模擬完成 (回測 ID {self.Backtest_id}): 交易數={trade_count}, 最終權益={equity*100:.2f}%")
        self.logger.info(f"交易模擬完成，交易數: {trade_count}, 最終權益: {equity*100:.2f}%", extra={"Backtest_id": self.Backtest_id})
        
        # 不輸出警告，而是返回警告訊息
        warning_msg = None
        if trade_count == 0:
            warning_msg = f"回測 ID {self.Backtest_id} 無交易生成！請檢查信號分佈或參數設置。"
        
        return df, warning_msg