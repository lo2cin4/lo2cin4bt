"""
TradeRecordExporter_backtester.py

【功能說明】更新時間 16-07-2025
------------------------------------------------------------
| 欄位名稱               | 型態      | 出現時機           | 說明
|------------------------|-----------|--------------------|-----------------------------------------------
| Time                   | datetime  | 每筆皆有           | 交易紀錄發生時間
| Open                   | float     | 每筆皆有           | 開盤價
| High                   | float     | 每筆皆有           | 最高價
| Low                    | float     | 每筆皆有           | 最低價
| Close                  | float     | 每筆皆有           | 收盤價
| Trading_instrument     | str       | 每筆皆有           | 標的代碼
| Position_type          | str       | 開倉或平倉時有      | 持倉方向（如 new_long/new_short/close_long/close_short）
| Open_position_price    | float     | 每筆皆有           | 開倉價格 (平時為0)
| Close_position_price   | float     | 每筆皆有           | 平倉價格 (平時為0)
| Position_size          | float/int | 每筆皆有           | 持倉數量 (0=沒有持倉,1=有持倉)
| Return                 | float     | 每筆皆有           | 每日報酬率（小數）(沒有持倉=0)
| Trade_group_id         | int/str   | 持倉時有           | 交易分組流水號
| Trade_action           | str       | 每筆皆有           | 交易動作(1=開倉,2=平倉,3=減倉,4=平倉,0=沒有動作)
| Open_time              | datetime  | 開倉時有           | 只在開倉時有值
| Close_time             | datetime  | 平倉時有           | 只在平倉時有值
| Parameter_set_id       | int/str   | 每筆皆有           | 參數組流水號
| Equity_value           | float     | 每筆皆有           | 每筆後的資產淨值
| Transaction_cost       | float     | 每筆皆有           | 交易時固定百份比手續費
| Slippage_cost          | float     | 每筆皆有           | 交易時固定百份比滑價
| Predictor_value        | float     | 每筆皆有           | 預測器分數/值
| Entry_signal           | 任意      | 每筆皆有           | 進場訊號內容(觸發時為1,平時為0)
| Exit_signal            | 任意      | 每筆皆有           | 出場訊號內容(觸發時為-1,平時為0)
| Holding_period_count   | int       | 每筆皆有           | 持倉天數（每日計算），無持倉時為0
| Holding_period         | int/float | 平倉時有           | 持倉天數（最終持倉時間）
| Trade_return           | float     | 平倉時有           | 一筆交易的百分比報酬率（僅平倉時有值）
| Backtest_id            | str       | 每筆皆有           | 批次唯一識別碼，主表格與 meta 對應
------------------------------------------------------------
- 新增欄位、metadata 結構、導出格式時，請同步更新 export_to_csv/export_to_parquet/頂部註解
- 若欄位或參數結構有變動，需同步更新所有依賴模組
- 檔案命名與 metadata 結構如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- 欄位結構未同步更新，導致導出失敗或下游分析錯誤
- metadata 結構不一致，影響批次分析或視覺化
- 必要欄位缺失或型態錯誤導致導出異常

【範例】
------------------------------------------------------------
- 導出 CSV：exporter = TradeRecordExporter_backtester(...); exporter.export_to_csv()
- 導出 Parquet：exporter.export_to_parquet()

【與其他模組的關聯】
------------------------------------------------------------
- 由 BacktestEngine 或 TradeRecorder 調用，協調交易紀錄導出與 metadata 管理
- 欄位結構依賴主流程與下游分析工具（如 plotguy）

【維護重點】
------------------------------------------------------------
- 新增/修改欄位、metadata 結構、導出格式時，務必同步更新本檔案與所有依賴模組
- 欄位與 metadata 結構需與主流程、README、下游工具保持一致

【參考】
------------------------------------------------------------
- 詳細欄位與 metadata 規範請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

import pandas as pd
import numpy as np
import logging
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# 移除重複的logging設置，使用main.py中設置的logger

class TradeRecordExporter_backtester:
    """導出交易記錄至 CSV 或 Parquet。"""

    def __init__(
        self,
        trade_records,
        frequency,
        trade_params=None,
        predictor=None,
        Backtest_id="",
        results=None,
        transaction_cost=None,
        slippage=None,
        trade_delay=None,
        trade_price=None,
        data=None,
    ):
        self.trade_records = trade_records
        self.frequency = frequency
        self.trade_params = trade_params
        self.predictor = predictor
        self.Backtest_id = Backtest_id
        self.results = results or []
        self.transaction_cost = transaction_cost
        self.slippage = slippage
        self.trade_delay = trade_delay
        self.trade_price = trade_price
        self.data = data
        self.logger = logging.getLogger(self.__class__.__name__)

        self.output_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "lo2cin4bt",
            "records",
            "backtester",
        )
        os.makedirs(self.output_dir, exist_ok=True)

    def _get_strategy_name(self, params):
        """根據 entry/exit 參數產生 strategy 字串，格式為 entry1+entry2_exit1+exit2"""
        def param_to_str(param):
            # 支援dict或物件
            if isinstance(param, dict):
                indicator_type = param.get('indicator_type', '')
                if indicator_type == 'MA':
                    strat_idx = param.get('strat_idx', '')
                    ma_type = param.get('ma_type', '')
                    mode = param.get('mode', 'single')
                    if mode == 'double':
                        short_period = param.get('shortMA_period', '')
                        long_period = param.get('longMA_period', '')
                        return f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                    else:
                        period = param.get('period', '')
                        return f"MA{strat_idx}_{ma_type}({period})"
                elif indicator_type == 'BOLL':
                    strat = param.get('strat', '')
                    ma_length = param.get('ma_length', '')
                    std_multiplier = param.get('std_multiplier', '')
                    return f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                elif indicator_type == 'NDayCycle':
                    n = param.get('n', '')
                    strat_idx = param.get('strat_idx', '')
                    return f"NDayCycle(N={n},T={strat_idx})"
                else:
                    return indicator_type
            elif hasattr(param, 'indicator_type'):
                indicator_type = getattr(param, 'indicator_type', '')
                if indicator_type == 'MA':
                    strat_idx = getattr(param, 'strat_idx', '')
                    ma_type = getattr(param, 'ma_type', '')
                    mode = getattr(param, 'mode', 'single')
                    if mode == 'double':
                        short_period = getattr(param, 'shortMA_period', '')
                        long_period = getattr(param, 'longMA_period', '')
                        return f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                    else:
                        period = getattr(param, 'period', '')
                        return f"MA{strat_idx}_{ma_type}({period})"
                elif indicator_type == 'BOLL':
                    strat = getattr(param, 'strat', '')
                    ma_length = getattr(param, 'ma_length', '')
                    std_multiplier = getattr(param, 'std_multiplier', '')
                    return f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                elif indicator_type == 'NDayCycle':
                    n = getattr(param, 'n', '')
                    strat_idx = getattr(param, 'strat_idx', '')
                    return f"NDayCycle(N={n},T={strat_idx})"
                else:
                    return indicator_type
            return str(param)
        entry_str = '+'.join([param_to_str(p) for p in params.get('entry', [])])
        exit_str = '+'.join([param_to_str(p) for p in params.get('exit', [])])
        return f"{entry_str}_{exit_str}" if entry_str or exit_str else "Unknown"



    def export_to_csv(self):
        """導出交易記錄至 CSV。"""
        try:
            if not self.results:
                print("無回測結果可導出為CSV")
                return
            
            exported_count = 0
            for result in self.results:
                if "error" in result:
                    print(f"跳過失敗的回測 {result['Backtest_id']}: {result['error']}")
                    continue
                
                if "records" not in result or result["records"].empty:
                    print(f"跳過無交易記錄的回測 {result['Backtest_id']}")
                    continue
                
                date_str = datetime.now().strftime("%Y%m%d")
                Backtest_id = result["Backtest_id"]
                params = result.get("params")
                if params is None:
                    print(f"[DEBUG] result 無 params 欄位，跳過。result keys: {list(result.keys())}")
                    continue
                predictor = params.get("predictor", "unknown")
                
                # 生成策略名稱
                strategy = self._get_strategy_name(params)
                
                # 生成參數字符串
                params_str = ""
                if params:
                    entry_params = params.get("entry", [])
                    exit_params = params.get("exit", [])
                    entry_str = []
                    for param in entry_params:
                        if isinstance(param, dict):
                            indicator_type = param.get('indicator_type', '')
                            if indicator_type == 'MA':
                                strat_idx = param.get('strat_idx', '')
                                ma_type = param.get('ma_type', '')
                                mode = param.get('mode', 'single')
                                consecutive_days = param.get('consecutive_days', None)
                                if consecutive_days is not None:
                                    period = param.get('period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}_m{consecutive_days}_n{period}"
                                elif mode == 'double':
                                    short_period = param.get('shortMA_period', '')
                                    long_period = param.get('longMA_period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                                else:
                                    period = param.get('period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({period})"
                            elif indicator_type == 'BOLL':
                                strat = param.get('strat', '')
                                ma_length = param.get('ma_length', '')
                                std_multiplier = param.get('std_multiplier', '')
                                param_str = f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                            elif indicator_type == 'NDayCycle':
                                n = param.get_param('n') if hasattr(param, 'get_param') else param.get('n', '')
                                strat_idx = param.get_param('strat_idx') if hasattr(param, 'get_param') else param.get('strat_idx', '')
                                param_str = f"NDayCycle(N={n},T={strat_idx})"
                            else:
                                param_str = indicator_type
                            entry_str.append(param_str)
                        elif hasattr(param, 'indicator_type'):
                            indicator_type = getattr(param, 'indicator_type', '')
                            if indicator_type == 'MA':
                                strat_idx = getattr(param, 'strat_idx', '')
                                ma_type = getattr(param, 'ma_type', '')
                                mode = getattr(param, 'mode', 'single')
                                consecutive_days = getattr(param, 'consecutive_days', None)
                                if consecutive_days is not None:
                                    period = getattr(param, 'period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}_m{consecutive_days}_n{period}"
                                elif mode == 'double':
                                    short_period = getattr(param, 'shortMA_period', '')
                                    long_period = getattr(param, 'longMA_period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                                else:
                                    period = getattr(param, 'period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({period})"
                            elif indicator_type == 'BOLL':
                                strat = getattr(param, 'strat', '')
                                ma_length = getattr(param, 'ma_length', '')
                                std_multiplier = getattr(param, 'std_multiplier', '')
                                param_str = f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                            elif indicator_type == 'NDayCycle':
                                n = getattr(param, 'n', '')
                                strat_idx = getattr(param, 'strat_idx', '')
                                param_str = f"NDayCycle(N={n},T={strat_idx})"
                            else:
                                param_str = indicator_type
                            entry_str.append(param_str)
                    exit_str = []
                    for param in exit_params:
                        if isinstance(param, dict):
                            indicator_type = param.get('indicator_type', '')
                            if indicator_type == 'MA':
                                strat_idx = param.get('strat_idx', '')
                                ma_type = param.get('ma_type', '')
                                mode = param.get('mode', 'single')
                                consecutive_days = param.get('consecutive_days', None)
                                if consecutive_days is not None:
                                    period = param.get('period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}_m{consecutive_days}_n{period}"
                                elif mode == 'double':
                                    short_period = param.get('shortMA_period', '')
                                    long_period = param.get('longMA_period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                                else:
                                    period = param.get('period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({period})"
                            elif indicator_type == 'BOLL':
                                strat = param.get('strat', '')
                                ma_length = param.get('ma_length', '')
                                std_multiplier = param.get('std_multiplier', '')
                                param_str = f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                            elif indicator_type == 'NDayCycle':
                                n = param.get_param('n') if hasattr(param, 'get_param') else param.get('n', '')
                                strat_idx = param.get_param('strat_idx') if hasattr(param, 'get_param') else param.get('strat_idx', '')
                                param_str = f"NDayCycle(N={n},T={strat_idx})"
                            else:
                                param_str = indicator_type
                            exit_str.append(param_str)
                        elif hasattr(param, 'indicator_type'):
                            indicator_type = getattr(param, 'indicator_type', '')
                            if indicator_type == 'MA':
                                strat_idx = getattr(param, 'strat_idx', '')
                                ma_type = getattr(param, 'ma_type', '')
                                mode = getattr(param, 'mode', 'single')
                                consecutive_days = getattr(param, 'consecutive_days', None)
                                if consecutive_days is not None:
                                    period = getattr(param, 'period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}_m{consecutive_days}_n{period}"
                                elif mode == 'double':
                                    short_period = getattr(param, 'shortMA_period', '')
                                    long_period = getattr(param, 'longMA_period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                                else:
                                    period = getattr(param, 'period', '')
                                    param_str = f"MA{strat_idx}_{ma_type}({period})"
                            elif indicator_type == 'BOLL':
                                strat = getattr(param, 'strat', '')
                                ma_length = getattr(param, 'ma_length', '')
                                std_multiplier = getattr(param, 'std_multiplier', '')
                                param_str = f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                            elif indicator_type == 'NDayCycle':
                                n = getattr(param, 'n', '')
                                strat_idx = getattr(param, 'strat_idx', '')
                                param_str = f"NDayCycle(N={n},T={strat_idx})"
                            else:
                                param_str = indicator_type
                            exit_str.append(param_str)
                    params_str = f"{'+'.join(entry_str)}+{'+'.join(exit_str)}" if entry_str or exit_str else "unknown"
                
                # 生成文件名
                filename = f"{date_str}_{self.frequency}_{strategy}_{predictor}_{params_str}_{Backtest_id[:8]}.csv"
                filepath = os.path.join(self.output_dir, filename)
                
                # 導出CSV
                # 新增 Backtest_id 欄位，確保主表格與 metadata 一一對應
                result["records"] = result["records"].copy()
                result["records"]["Backtest_id"] = Backtest_id
                result["records"].to_csv(filepath, index=False)
                print(f"已導出: {filename}")
                exported_count += 1
            
            if exported_count == 0:
                print("沒有成功導出任何CSV文件")
            elif exported_count == 1:
                print(f"CSV導出完成，共導出 {exported_count} 個文件")
            else:
                print(f"CSV導出完成，共導出 {exported_count} 個文件")
            
        except Exception as e:
            self.logger.error(
                f"CSV 導出失敗: {e}",
                extra={"Backtest_id": self.Backtest_id},
            )
            raise

    def export_to_parquet(self):
        """導出交易記錄至 Parquet，包含 metadata。"""
        try:
            date_str = datetime.now().strftime("%Y%m%d")
            filename = f"{date_str}_{self.Backtest_id}.parquet"
            filepath = os.path.join(self.output_dir, filename)

            metadata = {}
            if self.results:
                batch_metadata = []
                for result in self.results:
                    if "Backtest_id" in result:
                        params = result.get("params")
                        if params is None:
                            print(f"[DEBUG] result 無 params 欄位，跳過。result keys: {list(result.keys())}")
                            continue
                        # entry/exit 參數完整記錄
                        def param_to_dict(param):
                            if isinstance(param, dict):
                                return {k: str(v) for k, v in param.items()}
                            elif hasattr(param, '__dict__'):
                                return {k: str(v) for k, v in param.__dict__.items()}
                            else:
                                return str(param)
                        entry_details = [param_to_dict(p) for p in params.get("entry", [])]
                        exit_details = [param_to_dict(p) for p in params.get("exit", [])]
                        meta = {
                            "Backtest_id": result["Backtest_id"],
                            "Frequency": self.frequency,
                            "Asset": (
                                result.get("records", pd.DataFrame()).get("Trading_instrument", pd.Series()).iloc[0]
                                if not result.get("records", pd.DataFrame()).empty and "Trading_instrument" in result.get("records", pd.DataFrame()).columns
                                else "ALL"
                            ),
                            # strategy 欄位用 entry+exit 組合格式
                            "Strategy": self._get_strategy_name(params),
                            "Predictor": params.get("predictor", ""),
                            "Entry_params": entry_details,
                            "Exit_params": exit_details,
                            "Transaction_cost": self.transaction_cost or 0.0,
                            "Slippage_cost": self.slippage or 0.0,
                            "Trade_delay": self.trade_delay or 0,
                            "Trade_price": self.trade_price or "",
                            "Data_start_time": (
                                str(self.data["Time"].min())
                                if self.data is not None
                                else ""
                            ),
                            "Data_end_time": (
                                str(self.data["Time"].max())
                                if self.data is not None
                                else ""
                            ),
                            "Backtest_date": date_str
                            # 不再寫入strategy_id
                        }
                        batch_metadata.append(meta)
                metadata["batch_metadata"] = json.dumps(batch_metadata, ensure_ascii=False)
            else:
                asset = (
                    self.trade_records["Trading_instrument"].iloc[0]
                    if "Trading_instrument" in self.trade_records.columns
                    else "Unknown"
                )
                strategy = self._get_strategy_name(self.trade_params)
                metadata = {
                    "Frequency": self.frequency,
                    "Asset": asset,
                    "Strategy": strategy,
                    "ma_type": (
                        self.trade_params.get("ma_type", "")
                        if self.trade_params
                        else ""
                    ),
                    "short_period": (
                        self.trade_params.get("short_period", "")
                        if self.trade_params
                        else ""
                    ),
                    "long_period": (
                        self.trade_params.get("long_period", "")
                        if self.trade_params
                        else ""
                    ),
                    "period": (
                        self.trade_params.get("period", "")
                        if self.trade_params
                        else ""
                    ),
                    "predictor": self.predictor or "",
                    "Transaction_cost": self.transaction_cost or 0.0,
                    "Slippage_cost": self.slippage or 0.0,
                    "Trade_delay": self.trade_delay or 0,
                    "Trade_price": self.trade_price or "",
                    "Data_start_time": (
                        str(self.data["Time"].min())
                        if self.data is not None
                        else ""
                    ),
                    "Data_end_time": (
                        str(self.data["Time"].max())
                        if self.data is not None
                        else ""
                    ),
                    "Backtest_date": date_str,
                    "Backtest_id": self.Backtest_id,
                    "shortMA_period": (
                        self.trade_params.get("shortMA_period", "")
                        if self.trade_params
                        else ""
                    ),
                    "longMA_period": (
                        self.trade_params.get("longMA_period", "")
                        if self.trade_params
                        else ""
                    ),
                }

            # 合併所有回測結果的交易記錄
            all_records = []
            if self.results:
                for result in self.results:
                    if "records" in result and not result["records"].empty:
                        # 強制補齊 Backtest_id 欄位
                        if "Backtest_id" not in result["records"].columns:
                            result["records"] = result["records"].copy()
                            result["records"]["Backtest_id"] = result["Backtest_id"]
                        all_records.append(result["records"])
                
                if all_records:
                    all_records = [df for df in all_records if not df.empty]
                    combined_records = pd.concat(all_records, ignore_index=True)
                else:
                    combined_records = pd.DataFrame()
            else:
                combined_records = self.trade_records

            # 將 DataFrame 轉為 pyarrow.Table
            table = pa.Table.from_pandas(combined_records)
            # 將 metadata 轉為字節（pyarrow 要求）
            metadata_bytes = {k: v.encode("utf-8") if isinstance(v, str) else str(v).encode("utf-8") for k, v in metadata.items()}

            # 合併 pandas schema 與自訂 metadata
            orig_meta = table.schema.metadata or {}
            all_meta = dict(orig_meta)
            all_meta.update(metadata_bytes)
            table = table.replace_schema_metadata(all_meta)

            # 儲存 Parquet
            pq.write_table(table, filepath)
            self.logger.info(
                f"交易記錄已導出至 Parquet: {filepath}",
                extra={"Backtest_id": self.Backtest_id},
            )
        except Exception as e:
            self.logger.error(
                f"Parquet 導出失敗: {e}",
                extra={"Backtest_id": self.Backtest_id},
            )
            raise