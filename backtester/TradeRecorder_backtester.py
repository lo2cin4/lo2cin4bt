"""
TradeRecorder_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的交易記錄工具，負責記錄和管理回測過程中的交易詳情，包括開倉、平倉、持倉變化等資訊。
- 提供完整的交易記錄結構，包含所有必要的交易信息欄位
- 支援交易記錄的驗證與清理，確保數據品質
- 提供標準化的交易記錄格式，便於後續分析
- 整合錯誤處理機制，確保記錄的完整性

【流程與數據流】
------------------------------------------------------------
- 由 BacktestEngine 調用，記錄交易詳情
- 記錄結果傳遞給 TradeRecordExporter 進行導出

```mermaid
flowchart TD
    A[BacktestEngine] -->|調用| B[TradeRecorder]
    B -->|記錄交易| C[record_trades]
    B -->|驗證數據| D[數據驗證]
    B -->|清理數據| E[數據清理]
    C & D & E -->|標準化記錄| F[TradeRecordExporter]
```

【記錄欄位】
------------------------------------------------------------
- 基本交易信息：Time, Open, High, Low, Close
- 交易詳情：Trading_instrument, Position_type, Position_size
- 價格信息：Open_position_price, Close_position_price
- 績效指標：Return, Equity_value, Transaction_cost
- 時間信息：Open_time, Close_time, Trade_action
- 參數信息：Parameter_set_id, Predictor_value

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改記錄欄位、格式時，請同步更新頂部註解與下游流程
- 若記錄結構有變動，需同步更新本檔案與 TradeRecordExporter
- 記錄格式如有調整，請同步通知協作者
- 數據驗證規則需要與交易邏輯保持一致
- 記錄結構需要支援新的交易型態

【常見易錯點】
------------------------------------------------------------
- 記錄欄位缺失或格式錯誤會導致導出失敗
- 交易記錄不完整會影響績效計算
- 記錄結構變動會影響下游分析
- 數據驗證不完整會導致記錄錯誤
- 時間格式不一致會影響分析結果

【錯誤處理】
------------------------------------------------------------
- 記錄格式錯誤時提供詳細錯誤信息
- 數據缺失時提供插值或默認值
- 驗證失敗時提供修正建議
- 記錄不完整時提供警告信息

【範例】
------------------------------------------------------------
- 創建記錄器：recorder = TradeRecorder_backtester(trade_records, Backtest_id)
- 記錄交易：recorder.record_trades()
- 驗證記錄：recorder.validate_records()

【與其他模組的關聯】
------------------------------------------------------------
- 由 BacktestEngine 調用，記錄結果傳遞給 TradeRecordExporter
- 需與 TradeRecordExporter 的記錄結構保持一致
- 與 TradeSimulator 配合記錄交易詳情
- 支援多種分析工具的下游處理

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本交易記錄功能
- v1.1: 新增詳細欄位支援
- v1.2: 完善數據驗證與清理
- v2.0: 新增標準化記錄格式
- v2.1: 完善錯誤處理機制
- v2.2: 優化記錄結構與性能

【參考】
------------------------------------------------------------
- pandas 官方文件：https://pandas.pydata.org/
- BacktestEngine_backtester.py、TradeRecordExporter_backtester.py
- 專案 README
"""

import logging

import pandas as pd

# 移除重複的logging設置，使用main.py中設置的logger


class TradeRecorder_backtester:
    """記錄並驗證交易記錄。"""

    def __init__(self, trade_records: pd.DataFrame, Backtest_id: str):
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

    def record_trades(self) -> pd.DataFrame:
        """
        記錄並驗證交易記錄

        Returns:
            pd.DataFrame: 驗證後的交易記錄DataFrame，驗證失敗時返回空DataFrame
        """
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
            missing_cols = [
                col for col in self.trade_record_schema if col not in df.columns
            ]
            allowed_missing = set(["Holding_period", "Trade_return"])
            real_missing = [col for col in missing_cols if col not in allowed_missing]
            if real_missing:
                raise ValueError(f"缺少欄位: {real_missing}")

            # 驗證數據類型
            for col, dtype in self.trade_record_schema.items():
                if col in ["Time", "Open_time", "Close_time"]:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype is float:
                    ser = pd.to_numeric(df[col], errors="coerce")
                    if isinstance(ser, pd.Series):
                        df[col] = ser.fillna(0.0)
                    else:
                        df[col] = ser
                elif dtype is int:
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
                raise ValueError(
                    f"發現 {len(invalid_rows)} 行 Equity_value 無效: {invalid_rows.index.tolist()}"
                )

            # 日誌記錄
            self.logger.info(
                f"成功記錄 {len(df)} 筆交易記錄，Backtest_id: {self.Backtest_id}",
                extra={"Backtest_id": self.Backtest_id},
            )

            # 確保回傳的是 DataFrame 型態
            if not isinstance(df, pd.DataFrame):
                self.logger.warning(
                    f"trade_records 不是 DataFrame 型態，轉換為空 DataFrame，Backtest_id: {self.Backtest_id}",
                    extra={"Backtest_id": self.Backtest_id},
                )
                return pd.DataFrame()

            return df

        except Exception as e:
            self.logger.error(
                f"交易記錄驗證失敗: {e}", extra={"Backtest_id": self.Backtest_id}
            )
            # 確保異常時也回傳 DataFrame 型態
            return pd.DataFrame()
