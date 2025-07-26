"""
TradeRecorder_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「交易紀錄器」，負責記錄每筆交易、驗證交易資料結構、計算績效指標，並生成交易明細，供下游模組導出與分析。

【關聯流程與數據流】
------------------------------------------------------------
- 由 BacktestEngine 調用，根據模擬交易結果記錄每筆交易
- 計算績效指標，並導出給 TradeRecordExporter
- 主要數據流：

```mermaid
flowchart TD
    A[BacktestEngine] -->|調用| B[TradeRecorder]
    B -->|記錄交易| C[交易明細]
    B -->|計算績效| D[績效指標]
    B -->|導出| E[TradeRecordExporter]
```

【主控流程細節】
------------------------------------------------------------
- record_trades() 為主入口，負責驗證並記錄所有交易資料
- 檢查欄位完整性、數據型態、邏輯正確性（如 Equity Value > 0）
- 支援自訂欄位（如 Holding_period、Trade_return）並保留於明細
- 計算績效指標供下游模組使用

【維護與擴充提醒】
------------------------------------------------------------
- 新增績效指標、紀錄欄位時，請同步更新 record_trades/頂部註解
- 若紀錄結構有變動，需同步更新 TradeRecordExporter、BacktestEngine 等依賴模組
- 欄位驗證與型態規則如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- 紀錄格式不符會導致導出失敗
- 欄位遺漏或型態錯誤會導致下游模組報錯
- 績效指標計算錯誤會影響回測結果

【範例】
------------------------------------------------------------
- 記錄交易：recorder = TradeRecorder_backtester(trade_records, Backtest_id); df = recorder.record_trades()
- 計算績效：calculate_performance()

【與其他模組的關聯】
------------------------------------------------------------
- 由 BacktestEngine 調用，協調交易紀錄與績效計算
- 紀錄結構依賴下游 TradeRecordExporter

【維護重點】
------------------------------------------------------------
- 新增/修改績效指標、紀錄格式時，務必同步更新本檔案與所有依賴模組
- 欄位驗證與型態規則需與主流程保持一致

【參考】
------------------------------------------------------------
- 詳細紀錄規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

# 移除重複的logging設置，使用main.py中設置的logger

class TradeRecorder_backtester:
    """記錄並驗證交易記錄。"""

    def __init__(self, trade_records, Backtest_id):
        self.trade_records = trade_records
        self.Backtest_id = Backtest_id
        self.logger = logging.getLogger(__name__)

        self.trade_record_schema = {
            "Time": "datetime64[ns]",
            "Open": float,
            "High": float,
            "Low": float,
            "Close": float,
            "Trading_instrument": str,
            "Position_type": str,
            "Open_position_price": float,
            "Close_position_price": float,
            "Position_size": float,
            "Return": float,
            "Trade_group_id": str,
            "Trade_action": int,
            "Open_time": "datetime64[ns]",
            "Close_time": "datetime64[ns]",
            "Parameter_set_id": str,
            "Equity_value": float,
            "Transaction_cost": float,
            "Predictor_value": float,
        }

    def record_trades(self):
        """記錄並驗證交易記錄。"""
        try:
            df = self.trade_records.copy()

            # 處理 NaN
            numeric_cols = [
                "Open_position_price",
                "Close_position_price",
                "Position_size",
                "Return",
                "Equity_value",
                "Transaction_cost",
                "Predictor_value",  # 新增欄位
            ]
            df[numeric_cols] = df[numeric_cols].fillna(0.0)
            df["Trade_action"] = df["Trade_action"].fillna(0).astype(int)

            # 檢查缺失值（允許Holding_period與Trade_return不存在，不報錯）
            missing_cols = [col for col in self.trade_record_schema if col not in df.columns]
            allowed_missing = set(["Holding_period", "Trade_return"])
            real_missing = [col for col in missing_cols if col not in allowed_missing]
            if real_missing:
                raise ValueError(f"缺少欄位: {real_missing}")

            # 驗證數據類型
            for col, dtype in self.trade_record_schema.items():
                if col in ["Time", "Open_time", "Close_time"]:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == float:
                    ser = pd.to_numeric(df[col], errors="coerce")
                    if isinstance(ser, pd.Series):
                        df[col] = ser.fillna(0.0)
                    else:
                        df[col] = ser
                elif dtype == int:
                    ser = pd.to_numeric(df[col], errors="coerce")
                    if isinstance(ser, pd.Series):
                        df[col] = ser.fillna(0).astype(int)
                    else:
                        df[col] = ser

            # 保留Holding_period與Trade_return欄位（如有）
            # 不做型別驗證，直接保留

            # 驗證邏輯
            invalid_rows = df[df["Equity_value"] <= 0]
            if not invalid_rows.empty:
                raise ValueError(f"發現 {len(invalid_rows)} 行 Equity_value 無效: {invalid_rows.index.tolist()}")

            # 日誌記錄
            self.logger.info(
                f"成功記錄 {len(df)} 筆交易記錄，Backtest_id: {self.Backtest_id}",
                extra={"Backtest_id": self.Backtest_id},
            )

            # 確保回傳的是 DataFrame 型態
            if not isinstance(df, pd.DataFrame):
                self.logger.warning(f"trade_records 不是 DataFrame 型態，轉換為空 DataFrame，Backtest_id: {self.Backtest_id}", extra={"Backtest_id": self.Backtest_id})
                return pd.DataFrame()
            
            return df

        except Exception as e:
            self.logger.error(f"交易記錄驗證失敗: {e}", extra={"Backtest_id": self.Backtest_id})
            # 確保異常時也回傳 DataFrame 型態
            return pd.DataFrame()