"""
TradeRecordExporter_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的交易記錄導出工具，負責將回測結果和交易記錄導出為多種格式，支援 CSV、Excel、Parquet 等格式，便於後續分析。
- 提供智能回測摘要顯示，包含策略績效統計
- 支援多種導出格式：CSV、Excel、Parquet
- 整合 Rich Panel 美化顯示，提供分頁與篩選功能
- 提供策略詳細分析與成功/失敗結果分類

【流程與數據流】
------------------------------------------------------------
- 由 BaseBacktester 調用，導出回測結果和交易記錄
- 導出結果供用戶或下游模組分析

```mermaid
flowchart TD
    A[BaseBacktester] -->|調用| B[TradeRecordExporter]
    B -->|智能摘要| C[display_backtest_summary]
    B -->|導出CSV| D[export_to_csv]
    B -->|導出Parquet| E[export_to_parquet]
    B -->|策略分析| F[display_results_by_strategy]
    C & D & E & F -->|結果| G[用戶/下游模組]
```

【主要功能】
------------------------------------------------------------
- 智能摘要顯示：自動生成回測結果摘要，包含關鍵績效指標
- 多格式導出：支援 CSV、Excel、Parquet 等多種格式
- 策略分析：按策略分類顯示結果，提供詳細分析
- 分頁顯示：支援大量結果的分頁顯示與篩選
- 美化界面：整合 Rich Panel 提供美觀的 CLI 界面

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改導出格式、欄位時，請同步更新頂部註解與下游流程
- 若導出結構有變動，需同步更新本檔案與上游模組
- 導出格式如有調整，請同步通知協作者
- 摘要顯示邏輯需要與回測結果結構保持一致
- 新增導出格式時需要確保跨平台兼容性

【常見易錯點】
------------------------------------------------------------
- 導出格式錯誤或欄位缺失會導致導出失敗
- 檔案權限不足會導致寫入失敗
- 數據結構變動會影響下游分析
- 大量數據顯示時記憶體使用過高
- 跨平台檔案路徑處理不當

【錯誤處理】
------------------------------------------------------------
- 檔案寫入失敗時提供詳細錯誤信息
- 數據格式錯誤時提供修正建議
- 記憶體不足時提供分頁處理方案
- 權限問題時提供解決方案

【範例】
------------------------------------------------------------
- 創建導出器：exporter = TradeRecordExporter(trade_records, frequency, results, data)
- 顯示智能摘要：exporter.display_backtest_summary()
- 導出CSV：exporter.export_to_csv(backtest_id)
- 導出Parquet：exporter.export_to_parquet(backtest_id)
- 策略分析：exporter.display_results_by_strategy()

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseBacktester 調用，導出結果供用戶或下游模組使用
- 需與上游模組的數據結構保持一致
- 與 TradeRecorder_backtester 配合驗證交易記錄
- 支援多種分析工具的下游處理

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本導出功能
- v1.1: 新增 Parquet 格式支援
- v1.2: 整合 Rich Panel 美化顯示
- v2.0: 新增智能摘要與策略分析
- v2.1: 完善分頁顯示與篩選功能
- v2.2: 優化記憶體使用與錯誤處理

【參考】
------------------------------------------------------------
- pandas 官方文件：https://pandas.pydata.org/
- pyarrow 官方文件：https://arrow.apache.org/docs/python/
- Rich 官方文件：https://rich.readthedocs.io/
- Base_backtester.py、TradeRecorder_backtester.py
- 專案 README
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.table import Table
from rich.text import Text

# 移除重複的logging設置，使用main.py中設置的logger
from .utils import get_console
from utils import show_error, show_info, show_success, show_warning

console = get_console()


class TradeRecordExporter_backtester:
    """導出交易記錄至 CSV 或 Parquet。"""

    def __init__(
        self,
        trade_records: pd.DataFrame,
        frequency: str,
        trade_params: Optional[dict] = None,
        predictor: Optional[str] = None,
        Backtest_id: str = "",
        results: Optional[List[dict]] = None,
        transaction_cost: Optional[float] = None,
        slippage: Optional[float] = None,
        trade_delay: Optional[int] = None,
        trade_price: Optional[str] = None,
        data: Optional[pd.DataFrame] = None,
        predictor_file_name: Optional[str] = None,
        predictor_column: Optional[str] = None,
        symbol: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> None:
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
        self.predictor_file_name = predictor_file_name
        self.predictor_column = predictor_column
        self.symbol = symbol  # 添加 symbol 參數
        self.logger = logging.getLogger(self.__class__.__name__)

        if output_dir:
            self.output_dir = os.fspath(output_dir)
        else:
            self.output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "records",
                "backtester",
            )
        os.makedirs(self.output_dir, exist_ok=True)
        self.last_exported_path: Optional[str] = None

    def _get_strategy_name(self, params: Optional[dict]) -> str:  # noqa: C901
        """根據 entry/exit 參數產生 strategy 字串，格式為 entry1+entry2_exit1+exit2"""

        def param_to_str(param: Union[dict, object, str, int, float, None]) -> str:
            # 支援dict或物件
            if isinstance(param, dict):
                indicator_type = param.get("indicator_type", "")
                if indicator_type == "MA":
                    strat_idx = param.get("strat_idx", "")
                    ma_type = param.get("ma_type", "")
                    mode = param.get("mode", "single")
                    if mode == "double":
                        short_period = param.get("shortMA_period", "")
                        long_period = param.get("longMA_period", "")
                        return f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                    else:
                        period = param.get("period", "")
                        # 對於 MA9-MA12，需要顯示連續日數 m
                        if strat_idx in [9, 10, 11, 12]:
                            m = param.get("m", 2)
                            return f"MA{strat_idx}_{ma_type}({period},{m})"
                        else:
                            return f"MA{strat_idx}_{ma_type}({period})"
                elif indicator_type == "BOLL":
                    strat = param.get("strat", "")
                    ma_length = param.get("ma_length", "")
                    std_multiplier = param.get("std_multiplier", "")
                    return f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                elif indicator_type == "HL":
                    strat_idx = param.get("strat_idx", "")
                    n_length = param.get("n_length", "")
                    m_length = param.get("m_length", "")
                    return f"HL{strat_idx}_N({n_length})_M({m_length})"
                elif indicator_type == "VALUE":
                    strat_idx = param.get("strat_idx", "")
                    if strat_idx in [1, 2, 3, 4]:
                        n_length = param.get("n_length", "")
                        m_value = param.get("m_value", "")
                        return f"VALUE{strat_idx}_N({n_length})_M({m_value})"
                    elif strat_idx in [5, 6]:
                        m1_value = param.get("m1_value", "")
                        m2_value = param.get("m2_value", "")
                        return f"VALUE{strat_idx}_M1({m1_value})_M2({m2_value})"
                    else:
                        return f"VALUE{strat_idx}"

                elif indicator_type == "PERC":
                    window = param.get("window", "")
                    strat_idx = param.get("strat_idx", 1)
                    if strat_idx in [1, 2, 3, 4]:
                        percentile = param.get("percentile", "")
                        return f"PERC{strat_idx}(W={window},P={percentile})"
                    elif strat_idx in [5, 6]:
                        m1 = param.get("m1", "")
                        m2 = param.get("m2", "")
                        return f"PERC{strat_idx}(W={window},M1={m1},M2={m2})"
                    else:
                        return f"PERC{strat_idx}(W={window})"
                else:
                    return str(indicator_type or "unknown")
            elif hasattr(param, "indicator_type"):
                indicator_type = getattr(param, "indicator_type", "")
                if indicator_type == "MA":
                    strat_idx = getattr(param, "strat_idx", "")
                    ma_type = getattr(param, "ma_type", "")
                    mode = getattr(param, "mode", "single")
                    if mode == "double":
                        short_period = getattr(param, "shortMA_period", "")
                        long_period = getattr(param, "longMA_period", "")
                        return f"MA{strat_idx}_{ma_type}({short_period},{long_period})"
                    else:
                        period = getattr(param, "period", "")
                        # 對於 MA9-MA12，需要顯示連續日數 m
                        if strat_idx in [9, 10, 11, 12]:
                            m = getattr(param, "m", 2)
                            return f"MA{strat_idx}_{ma_type}({period},{m})"
                        else:
                            return f"MA{strat_idx}_{ma_type}({period})"
                elif indicator_type == "BOLL":
                    strat = getattr(param, "strat", "")
                    ma_length = getattr(param, "ma_length", "")
                    std_multiplier = getattr(param, "std_multiplier", "")
                    return f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                elif indicator_type == "HL":
                    strat_idx = getattr(param, "strat_idx", "")
                    n_length = getattr(param, "n_length", "")
                    m_length = getattr(param, "m_length", "")
                    return f"HL{strat_idx}({n_length},{m_length})"
                elif indicator_type == "VALUE":
                    strat_idx = getattr(param, "strat_idx", "")
                    if strat_idx in [1, 2, 3, 4]:
                        n_length = getattr(param, "n_length", "")
                        m_value = getattr(param, "m_value", "")
                        return f"VALUE{strat_idx}({n_length},{m_value})"
                    elif strat_idx in [5, 6]:
                        m1_value = getattr(param, "m1_value", "")
                        m2_value = getattr(param, "m2_value", "")
                        return f"VALUE{strat_idx}({m1_value},{m2_value})"
                    else:
                        return f"VALUE{strat_idx}"

                elif indicator_type == "PERC":
                    window = getattr(param, "window", "")
                    strat_idx = getattr(param, "strat_idx", 1)
                    if strat_idx in [1, 2, 3, 4]:
                        percentile = getattr(param, "percentile", "")
                        return f"PERC{strat_idx}(W={window},P={percentile})"
                    elif strat_idx in [5, 6]:
                        m1 = getattr(param, "m1", "")
                        m2 = getattr(param, "m2", "")
                        return f"PERC{strat_idx}(W={window},M1={m1},M2={m2})"
                    else:
                        return f"PERC{strat_idx}(W={window})"
                else:
                    return str(indicator_type or "unknown")
            # 確保返回字符串類型
            try:
                return str(param)
            except (TypeError, ValueError):
                return "unknown"

        if params is None:
            return "Unknown"

        entry_str = "+".join([param_to_str(p) for p in params.get("entry", [])])
        exit_str = "+".join([param_to_str(p) for p in params.get("exit", [])])
        return f"{entry_str}_{exit_str}" if entry_str or exit_str else "Unknown"

    def export_to_csv(self, backtest_id: Optional[str] = None) -> None:  # noqa: C901
        """
        導出交易記錄至 CSV

        Args:
            backtest_id (str, optional): 指定要導出的回測ID，如果為None則導出所有結果

        Note:
            導出的CSV檔案會保存在 records/backtester/ 目錄下
        """
        try:
            if not self.results:
                show_warning("BACKTESTER", "無回測結果可導出為CSV")
                return

            # 如果指定了backtest_id，只導出該回測結果
            if backtest_id:
                results_to_export = [
                    r for r in self.results if r.get("Backtest_id") == backtest_id
                ]
                if not results_to_export:
                    show_error("BACKTESTER", f"找不到Backtest_id為 {backtest_id} 的回測結果")
                    return
            else:
                results_to_export = self.results

            exported_count = 0
            msg_lines = []
            for result in results_to_export:
                if result.get("error") is not None:
                    msg_lines.append(
                        f"跳過失敗的回測 {result['Backtest_id']}: {result['error']}"
                    )
                    continue

                if (
                    "records" not in result
                    or not isinstance(result["records"], pd.DataFrame)
                    or result["records"].empty
                    or (result["records"]["Trade_action"] == 1).sum() == 0
                ):
                    msg_lines.append(f"跳過無交易記錄的回測 {result['Backtest_id']}")
                    continue

                date_str = datetime.now().strftime("%Y%m%d")
                Backtest_id = result["Backtest_id"]
                params = result.get("params")
                if params is None:
                    msg_lines.append(
                        f"result 無 params 欄位，跳過。result keys: {list(result.keys())}"
                    )
                    continue
                predictor = params.get("predictor", "unknown")

                # 生成策略名稱
                strategy = self._get_strategy_name(params)

                # 生成文件名 - 移除重複的params_str，只使用strategy
                filename = f"{date_str}_{self.frequency}_{strategy}_{predictor}_{Backtest_id[:8]}.csv"
                filepath = os.path.join(self.output_dir, filename)

                # 導出CSV
                # 新增 Backtest_id 欄位，確保主表格與 metadata 一一對應
                # 優化：只在需要時才拷貝，避免不必要的記憶體使用
                if "Backtest_id" not in result["records"].columns:
                    records_to_export = result["records"].copy()
                    records_to_export["Backtest_id"] = Backtest_id
                else:
                    records_to_export = result["records"]
                records_to_export.to_csv(filepath, index=False)
                msg_lines.append(f"已導出: {filename}")
                exported_count += 1

            if exported_count == 0:
                msg_lines.append("沒有成功導出任何CSV文件")
            else:
                msg_lines.append(f"CSV導出完成，共導出 {exported_count} 個文件")

            if exported_count == 0:
                show_error("BACKTESTER", "\n".join(msg_lines))
            else:
                show_success("BACKTESTER", "\n".join(msg_lines))
        except Exception as e:
            self.logger.error(
                f"CSV 導出失敗: {e}",
                extra={"Backtest_id": self.Backtest_id},
            )
            raise

    def export_to_excel(self, backtest_id: Optional[str] = None) -> None:
        """
        導出交易記錄至 Excel

        Args:
            backtest_id (str, optional): 指定要導出的回測ID，如果為None則導出所有結果
        """
        try:
            if not self.results:
                show_warning("BACKTESTER", "無回測結果可導出為Excel")
                return

            # 如果指定了backtest_id，只導出該回測結果
            if backtest_id:
                results_to_export = [
                    r for r in self.results if r.get("Backtest_id") == backtest_id
                ]
                if not results_to_export:
                    show_error("BACKTESTER", f"找不到Backtest_id為 {backtest_id} 的回測結果")
                    return
            else:
                results_to_export = self.results

            exported_count = 0
            msg_lines = []
            for result in results_to_export:
                if result.get("error") is not None:
                    continue

                if (
                    "records" not in result
                    or not isinstance(result["records"], pd.DataFrame)
                    or result["records"].empty
                    or (result["records"]["Trade_action"] == 1).sum() == 0
                ):
                    continue

                date_str = datetime.now().strftime("%Y%m%d")
                Backtest_id = result["Backtest_id"]
                params = result.get("params")
                if params is None:
                    continue
                
                predictor = params.get("predictor", "unknown")
                strategy = self._get_strategy_name(params)

                # 生成文件名
                filename = f"{date_str}_{self.frequency}_{strategy}_{predictor}_{Backtest_id[:8]}.xlsx"
                filepath = os.path.join(self.output_dir, filename)

                # 導出Excel
                if "Backtest_id" not in result["records"].columns:
                    records_to_export = result["records"].copy()
                    records_to_export["Backtest_id"] = Backtest_id
                else:
                    records_to_export = result["records"]
                
                records_to_export.to_excel(filepath, index=False)
                msg_lines.append(f"已導出 Excel: {filename}")
                exported_count += 1

            if exported_count > 0:
                show_success("BACKTESTER", f"Excel 導出完成，共導出 {exported_count} 個文件")
            else:
                show_warning("BACKTESTER", "沒有成功導出任何Excel文件")
                
        except Exception as e:
            self.logger.error(f"Excel 導出失敗: {e}")
            show_error("BACKTESTER", f"Excel 導出失敗: {e}")

    def _create_parquet_filename(self) -> tuple[str, str]:
        """創建 Parquet 文件名和路徑

        格式: {date}_{Trading_instrument}_{predictor_file_name}_{predictor_column}_{random_id}_{Backtest_id}.parquet
        如果使用價格: {date}_{Trading_instrument}_{price}_{price}_{random_id}_{Backtest_id}.parquet
        """
        import uuid

        date_str = datetime.now().strftime("%Y%m%d")
        random_id = uuid.uuid4().hex[:8]

        # 獲取交易標的
        trading_instrument = self._get_trading_instrument()

        # 獲取預測因子文件名和列名
        if self.predictor_file_name and self.predictor_column:
            # 使用預測因子文件
            predictor_file = self.predictor_file_name
            predictor_col = self.predictor_column
        else:
            # 僅使用價格數據
            predictor_file = "price"
            predictor_col = "price"

        # 生成文件名（與原版一致：date + random_id + Backtest_id）
        filename = f"{date_str}_{trading_instrument}_{predictor_file}_{predictor_col}_{random_id}_{self.Backtest_id}.parquet"
        filepath = os.path.join(self.output_dir, filename)
        return filename, filepath

    def _get_trading_instrument(self) -> str:
        """獲取交易標的名稱"""
        # 優先使用直接傳入的 symbol
        if self.symbol and self.symbol != "X":
            return self.symbol
        
        # 從 results 中獲取
        if self.results:
            for result in self.results:
                records = result.get("records", pd.DataFrame())
                if not records.empty and "Trading_instrument" in records.columns:
                    instrument = records["Trading_instrument"].iloc[0]
                    if instrument and str(instrument) != "nan" and str(instrument) != "X":
                        return str(instrument)

        # 如果 symbol 有值，返回它（即使是 "X"）
        if self.symbol:
            return self.symbol
        
        # 預設值
        return "UNKNOWN"

    def _get_results_to_export(self, backtest_id: Optional[str] = None) -> List[dict]:
        """獲取要導出的回測結果"""
        if backtest_id:
            results_to_export = [
                r for r in self.results if r.get("Backtest_id") == backtest_id
            ]
            if not results_to_export:
                print(f"找不到Backtest_id為 {backtest_id} 的回測結果")
                return []
            return results_to_export
        else:
            return self.results

    def _create_batch_metadata(
        self, results_to_export: List[dict], date_str: str
    ) -> dict:
        """創建批次元數據"""
        batch_metadata = []
        for result in results_to_export:
            if "Backtest_id" not in result:
                continue

            params = result.get("params")
            if params is None:
                continue

            # entry/exit 參數完整記錄
            def param_to_dict(param: Union[dict, Any]) -> dict:
                if isinstance(param, dict):
                    return {k: str(v) for k, v in param.items()}
                elif hasattr(param, "__dict__"):
                    return {k: str(v) for k, v in param.__dict__.items()}
                else:
                    # 對於非dict/object類型，創建一個包含值的字典
                    return {"value": str(param)}

            entry_details = [param_to_dict(p) for p in params.get("entry", [])]
            exit_details = [param_to_dict(p) for p in params.get("exit", [])]

            meta = {
                "Backtest_id": result["Backtest_id"],
                "Frequency": self.frequency,
                "Asset": (
                    result.get("records", pd.DataFrame())
                    .get("Trading_instrument", pd.Series())
                    .iloc[0]
                    if not result.get("records", pd.DataFrame()).empty
                    and "Trading_instrument"
                    in result.get("records", pd.DataFrame()).columns
                    else "ALL"
                ),
                "Strategy": self._get_strategy_name(params),
                "Predictor": params.get("predictor", ""),
                "Entry_params": entry_details,
                "Exit_params": exit_details,
                "Transaction_cost": str(float(self.transaction_cost or 0.0)),
                "Slippage_cost": str(float(self.slippage or 0.0)),
                "Trade_delay": str(int(self.trade_delay or 0)),
                "Trade_price": self.trade_price or "",
                "Data_start_time": (
                    str(self.data["Time"].min()) if self.data is not None else ""
                ),
                "Data_end_time": (
                    str(self.data["Time"].max()) if self.data is not None else ""
                ),
                "Backtest_date": date_str,
            }
            batch_metadata.append(meta)

        return {"batch_metadata": json.dumps(batch_metadata, ensure_ascii=False)}

    def _create_single_metadata(self, date_str: str) -> dict:
        """創建單一元數據（當沒有 results_to_export 時）"""
        asset = (
            self.trade_records["Trading_instrument"].iloc[0]
            if "Trading_instrument" in self.trade_records.columns
            else "Unknown"
        )
        strategy = self._get_strategy_name(self.trade_params)

        return {
            "Frequency": self.frequency,
            "Asset": asset,
            "Strategy": strategy,
            "ma_type": (
                self.trade_params.get("ma_type", "") if self.trade_params else ""
            ),
            "short_period": (
                self.trade_params.get("short_period", "") if self.trade_params else ""
            ),
            "long_period": (
                self.trade_params.get("long_period", "") if self.trade_params else ""
            ),
            "period": (
                self.trade_params.get("period", "") if self.trade_params else ""
            ),
            "predictor": self.predictor or "",
            "Transaction_cost": str(float(self.transaction_cost or 0.0)),
            "Slippage_cost": str(float(self.slippage or 0.0)),
            "Trade_delay": str(int(self.trade_delay or 0)),
            "Trade_price": self.trade_price or "",
            "Data_start_time": (
                str(self.data["Time"].min()) if self.data is not None else ""
            ),
            "Data_end_time": (
                str(self.data["Time"].max()) if self.data is not None else ""
            ),
            "Backtest_date": date_str,
            "Backtest_id": self.Backtest_id,
            "shortMA_period": (
                self.trade_params.get("shortMA_period", "") if self.trade_params else ""
            ),
            "longMA_period": (
                self.trade_params.get("longMA_period", "") if self.trade_params else ""
            ),
        }

    def _filter_valid_records(
        self, all_records: List[pd.DataFrame]
    ) -> List[pd.DataFrame]:
        """過濾和清理記錄"""
        filtered_records = []
        for df in all_records:
            if not df.empty and len(df.columns) > 0:
                has_valid_data = False
                for col in df.columns:
                    if not df[col].isna().all():
                        has_valid_data = True
                        break
                if has_valid_data:
                    cleaned_df = df.dropna(axis=1, how="all")
                    if not cleaned_df.empty:
                        filtered_records.append(cleaned_df)
        return filtered_records

    def _concat_records_safely(
        self, filtered_records: List[pd.DataFrame]
    ) -> pd.DataFrame:
        """安全地合併記錄"""
        if not filtered_records:
            return pd.DataFrame()

        try:
            combined_records = pd.concat(
                filtered_records, ignore_index=True, sort=False
            )
        except Exception:
            combined_records = filtered_records[0]
            for df in filtered_records[1:]:
                combined_records = pd.concat(
                    [combined_records, df], ignore_index=True, sort=False
                )
        return combined_records

    def _combine_records(self, results_to_export: List[dict]) -> pd.DataFrame:
        """合併所有回測結果的交易記錄"""
        all_records = []
        if results_to_export:
            for result in results_to_export:
                if "records" in result and not result["records"].empty:
                    records_df = result["records"]
                    all_records.append(records_df)

            # 過濾和清理記錄
            filtered_records = self._filter_valid_records(all_records)

            # 安全合併
            combined_records = self._concat_records_safely(filtered_records)
        else:
            combined_records = self.trade_records

        return combined_records

    def _save_parquet_file(
        self, combined_records: pd.DataFrame, metadata: dict, filepath: str
    ) -> None:
        """保存 Parquet 文件"""
        # 將 DataFrame 轉為 pyarrow.Table
        table = pa.Table.from_pandas(combined_records)

        # 將 metadata 轉為字節（pyarrow 要求）
        metadata_bytes = {
            k: v.encode("utf-8") if isinstance(v, str) else str(v).encode("utf-8")
            for k, v in metadata.items()
        }

        # 合併 pandas schema 與自訂 metadata
        orig_meta = table.schema.metadata or {}
        all_meta = dict(orig_meta)
        all_meta.update(metadata_bytes)
        table = table.replace_schema_metadata(all_meta)

        # 儲存 Parquet
        pq.write_table(table, filepath)
        self.last_exported_path = filepath

    def export_to_parquet(self, backtest_id: Optional[str] = None) -> None:
        """導出交易記錄至 Parquet，包含 metadata。

        Args:
            backtest_id: 指定要導出的回測ID，如果為None則導出所有結果
        """
        try:
            # 創建文件名和路徑
            filename, filepath = self._create_parquet_filename()

            # 獲取要導出的結果
            results_to_export = self._get_results_to_export(backtest_id)
            if not results_to_export and backtest_id:
                return

            date_str = datetime.now().strftime("%Y%m%d")

            # 創建元數據
            if results_to_export:
                metadata = self._create_batch_metadata(results_to_export, date_str)
            else:
                metadata = self._create_single_metadata(date_str)

            # 合併記錄
            combined_records = self._combine_records(results_to_export)

            # 保存文件
            self._save_parquet_file(combined_records, metadata, filepath)

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

    def display_backtest_summary(self) -> None:
        """顯示回測摘要，包含預覽表格和操作選項。"""
        if not self.results:
            show_warning("BACKTESTER", "無回測結果可顯示摘要")
            return

        # 智能分頁：如果結果超過15個，使用分頁顯示
        if len(self.results) > 15:
            self._display_paginated_summary()
        else:
            self._display_full_summary()

    def _display_full_summary(self) -> None:
        """顯示完整摘要表格（結果數量 ≤ 15）"""

        table = Table(title="回測摘要", style="bold magenta")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)

        for i, result in enumerate(self.results, 1):
            if result.get("error") is not None:
                table.add_row(str(i), result["Backtest_id"], "失敗", "❌ 失敗")
                continue

            if (
                "records" not in result
                or not isinstance(result["records"], pd.DataFrame)
                or result["records"].empty
                or (result["records"]["Trade_action"] == 1).sum() == 0
            ):
                params = result.get("params")
                strategy = self._get_strategy_name(params) if params else "N/A"
                table.add_row(str(i), result["Backtest_id"], strategy, "⚠️ 無交易")
                continue

            params = result.get("params")
            if params is None:
                table.add_row(str(i), result["Backtest_id"], "N/A", "❌ 失敗")
                continue

            # 生成策略名稱
            strategy = self._get_strategy_name(params)

            table.add_row(str(i), result["Backtest_id"], strategy, "✅ 成功")

        console.print(table)
        
        # 分支邏輯：根據調用來源決定是否顯示用戶界面
        # 如果是在 autorunner 模式下，不顯示用戶選擇界面
        import sys
        if 'autorunner' in sys.modules:
            # autorunner 模式：只顯示摘要，不顯示用戶界面
            pass
        else:
            # 原版 backtester 模式：顯示用戶選擇界面
            self._show_operation_menu()

    def _display_paginated_summary(self) -> None:  # noqa: C901
        """分頁顯示摘要表格（結果數量 > 15）"""
        page_size = 15
        total_results = len(self.results)
        total_pages = (total_results + page_size - 1) // page_size
        page = 1
        
        # 檢查是否在 autorunner 模式下
        import sys
        is_autorunner = 'autorunner' in sys.modules
        
        while True:
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, total_results)

            table = Table(
                title=f"回測結果 - 第 {page} 頁 (共 {total_pages} 頁)",
                style="bold magenta",
            )
            table.add_column("序號", style="cyan", no_wrap=True)
            table.add_column("回測ID", style="green", no_wrap=True)
            table.add_column("策略", style="blue", no_wrap=True)
            table.add_column("狀態", style="yellow", no_wrap=True)

            for i in range(start_idx, end_idx):
                result = self.results[i]
                # 嚴格判斷成功/無交易/失敗 - 檢查實際交易行為
                is_success = (
                    result.get("error") is None
                    and "records" in result
                    and isinstance(result["records"], pd.DataFrame)
                    and not result["records"].empty
                    and (result["records"]["Trade_action"] == 1).sum() > 0
                )
                is_no_trade = (
                    result.get("error") is None
                    and "records" in result
                    and isinstance(result["records"], pd.DataFrame)
                    and not result["records"].empty
                    and (result["records"]["Trade_action"] == 1).sum() == 0
                )
                is_failed = result.get("error") is not None
                if is_failed:
                    table.add_row(
                        str(i + 1), result["Backtest_id"], "失敗", "[red]❌ 失敗[/red]"
                    )
                elif is_no_trade:
                    params = result.get("params")
                    strategy = self._get_strategy_name(params) if params else "N/A"
                    table.add_row(
                        str(i + 1),
                        result["Backtest_id"],
                        strategy,
                        "[yellow]⚠️ 無交易[/yellow]",
                    )
                elif is_success:
                    params = result.get("params")
                    strategy = self._get_strategy_name(params) if params else "N/A"
                    table.add_row(
                        str(i + 1),
                        result["Backtest_id"],
                        strategy,
                        "[green]✅ 成功[/green]",
                    )
                else:
                    # 其他異常情況也標示為失敗
                    table.add_row(
                        str(i + 1),
                        result.get("Backtest_id", "N/A"),
                        "異常",
                        "[red]❌ 失敗[/red]",
                    )

            console.print(table)

            # 分頁導航
            if total_pages > 1:
                if is_autorunner:
                    # autorunner 模式：顯示所有頁面但不要求用戶輸入
                    if page < total_pages:
                        page += 1
                        console.clear()
                        continue
                    else:
                        # 已經顯示完所有頁面，跳出循環
                        break
                else:
                    # 原版 backtester 模式：顯示分頁導航並要求用戶輸入
                    show_info("BACKTESTER", "📄 分頁導航: [m] 下一頁(m) | [n] 上一頁(n) | [數字] 跳轉到指定頁 | [q] 進入操作選單(q)")
                    console.print("[bold #dbac30]請輸入導航指令: [/bold #dbac30]", end="")
                    nav = input().lower()

                    if nav == "q":
                        break
                    elif nav == "m" and page < total_pages:
                        page += 1
                        console.clear()
                    elif nav == "n" and page > 1:
                        page -= 1
                        console.clear()
                    elif nav.isdigit():
                        page_num = int(nav)
                        if 1 <= page_num <= total_pages:
                            page = page_num
                            console.clear()
                        else:
                            console.print("❌ 頁碼超出範圍", style="red")
                    else:
                        console.print("❌ 無效命令", style="red")
            else:
                break

        # 分支邏輯：根據調用來源決定是否顯示用戶界面
        import sys
        if 'autorunner' in sys.modules:
            # autorunner 模式：只顯示摘要，不顯示用戶界面
            pass
        else:
            # 原版 backtester 模式：顯示用戶選擇界面
            self._show_operation_menu()

    def _show_operation_menu(self) -> None:  # noqa: C901
        """顯示操作選單"""
        # 提供操作選項
        menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""

        show_info("BACKTESTER", menu_text)

        while True:
            console.print("[bold #dbac30]請選擇操作: [/bold #dbac30]", end="")
            choice = input()
            if choice == "1":
                self.display_successful_results()
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                show_info("BACKTESTER", menu_text)
            elif choice == "2":
                self.display_failed_results()
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                show_info("BACKTESTER", menu_text)
            elif choice == "3":
                self.export_to_csv()
                console.print("✅ CSV 導出完成！", style="green")
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                show_info("BACKTESTER", menu_text)
            elif choice == "4":
                while True:
                    console.print(
                        "[bold #dbac30]請輸入Backtest ID（可用逗號分隔多個），或按Enter返回選單: [/bold #dbac30]",
                        end="",
                    )
                    backtest_id_input = input()
                    if not backtest_id_input:
                        # 直接返回選單
                        break
                    # 支援多個ID
                    backtest_ids = [
                        bid.strip()
                        for bid in backtest_id_input.split(",")
                        if bid.strip()
                    ]
                    not_found = [
                        bid
                        for bid in backtest_ids
                        if not any(r.get("Backtest_id") == bid for r in self.results)
                    ]
                    if not backtest_ids:
                        continue
                    if not_found:
                        show_error("BACKTESTER", f"找不到Backtest_id為 {', '.join(not_found)} 的回測結果")
                        continue
                    for bid in backtest_ids:
                        self.export_to_csv(backtest_id=bid)
                    console.print(
                        f"✅ 已導出 {len(backtest_ids)} 個特定回測 CSV！", style="green"
                    )
                    break
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                show_info("BACKTESTER", menu_text)
            elif choice == "5":
                console.print("結束交易回測，進入下一階段...", style="yellow")
                break
            else:
                console.print("無效選擇，請重新輸入。", style="red")

    def display_results_by_strategy(self) -> None:  # noqa: C901
        """按策略分組顯示結果。"""
        if not self.results:
            show_warning("BACKTESTER", "無回測結果可顯示")
            return

        # 按策略分組
        strategy_groups: Dict[str, List[dict]] = {}
        for result in self.results:
            # 使用與VectorBacktestEngine相同的判斷邏輯
            if result.get("error") is not None:
                strategy = "失敗"
            else:
                records = result.get("records")
                if records is None or not isinstance(records, pd.DataFrame):
                    strategy = "無交易"
                elif len(records) == 0:
                    strategy = "無交易"
                elif (records["Trade_action"] == 1).sum() == 0:
                    strategy = "無交易"
                else:
                    params = result.get("params", {})
                    strategy = self._get_strategy_name(params)

            if strategy not in strategy_groups:
                strategy_groups[strategy] = []
            strategy_groups[strategy].append(result)

        # 顯示策略列表
        console.print("\n=== 按策略分組 ===")
        for i, (strategy, results) in enumerate(strategy_groups.items(), 1):
            # 使用與VectorBacktestEngine相同的判斷邏輯
            success_count = 0
            for r in results:
                if r.get("error") is None:
                    records = r.get("records")
                    if (
                        records is not None
                        and isinstance(records, pd.DataFrame)
                        and not records.empty
                        and (records["Trade_action"] == 1).sum() > 0
                    ):
                        success_count += 1

            total_count = len(results)
            console.print(f"{i}. {strategy}: {success_count}/{total_count} 成功")

        # 選擇策略查看詳情
        while True:
            show_info("BACKTESTER", "⌨請選擇策略編號查看詳情")
            choice = input(" 策略編號 (或按 Enter 返回選單): ")
            if not choice:
                break

            try:
                choice_idx = int(choice) - 1
                strategy_list = list(strategy_groups.keys())
                if 0 <= choice_idx < len(strategy_list):
                    selected_strategy = strategy_list[choice_idx]
                    self.display_strategy_details(
                        selected_strategy, strategy_groups[selected_strategy]
                    )
                else:
                    console.print("策略編號超出範圍", style="red")
            except ValueError:
                console.print("請輸入有效的數字", style="red")

    def display_strategy_details(self, strategy: str, results: List[dict]) -> None:
        """顯示特定策略的詳細結果。"""
        console.print(f"\n=== {strategy} 策略詳情 ===")

        table = Table(title=f"{strategy} - 回測結果", style="bold magenta")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("預測因子", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)
        table.add_column("總回報", style="cyan", no_wrap=True)
        table.add_column("交易次數", style="green", no_wrap=True)

        for i, result in enumerate(results, 1):
            # 使用與VectorBacktestEngine相同的判斷邏輯
            if result.get("error") is not None:
                status = "❌ 失敗"
                total_return = "N/A"
                trade_count = "N/A"
            else:
                records = result.get("records")
                if records is None or not isinstance(records, pd.DataFrame):
                    status = "⚠️ 無交易"
                    total_return = "N/A"
                    trade_count = "0"
                elif len(records) == 0:
                    status = "⚠️ 無交易"
                    total_return = "N/A"
                    trade_count = "0"
                elif (records["Trade_action"] == 1).sum() == 0:
                    status = "⚠️ 無交易"
                    total_return = "N/A"
                    trade_count = "0"
                else:
                    status = "✅ 成功"
                    total_return = (
                        f"{result.get('total_return', 0):.2%}"
                        if result.get("total_return") is not None
                        else "N/A"
                    )
                    trade_count = str(result.get("total_trades", 0))

            params = result.get("params", {})
            predictor = params.get("predictor", "N/A")

            table.add_row(
                str(i),
                result["Backtest_id"][:8] + "...",
                predictor,
                status,
                total_return,
                trade_count,
            )

        console.print(table)
        console.print("[bold #dbac30]按 Enter 返回選單: [/bold #dbac30]", end="")
        input()

    def display_successful_results(self) -> None:
        """顯示成功的回測結果"""
        # 使用與VectorBacktestEngine相同的判斷邏輯
        # 成功：無錯誤且有實際開倉交易
        successful_results = []
        for r in self.results:
            if r.get("error") is None:
                records = r.get("records")
                if (
                    records is not None
                    and isinstance(records, pd.DataFrame)
                    and not records.empty
                    and (records["Trade_action"] == 1).sum() > 0
                ):
                    successful_results.append(r)

        if not successful_results:
            show_info("BACKTESTER", "成功結果：沒有")
            return

        table = Table(title="成功回測結果", style="bold green")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)

        for i, result in enumerate(successful_results, 1):
            params = result.get("params")
            strategy = self._get_strategy_name(params) if params else "N/A"

            table.add_row(str(i), result["Backtest_id"], strategy, "✅ 成功")

        console.print(table)

    def display_failed_results(self) -> None:
        """顯示失敗的回測結果"""
        # 使用與VectorBacktestEngine相同的判斷邏輯
        # 失敗：有錯誤
        failed_results = [r for r in self.results if r.get("error") is not None]

        if not failed_results:
            show_info("BACKTESTER", "失敗結果：沒有")
            return

        table = Table(title="失敗回測結果", style="bold red")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)

        for i, result in enumerate(failed_results, 1):
            params = result.get("params")
            strategy = self._get_strategy_name(params) if params else "N/A"

            status = "❌ 失敗"

            table.add_row(str(i), result["Backtest_id"], strategy, status)

        console.print(table)

    def debug_trade_actions(self) -> None:
        """調試方法：檢查Trade_action的實際值分布"""
        show_info("BACKTESTER", "🔍 調試：Trade_action值分布分析")

        # 統計所有Trade_action值的分布
        all_trade_actions = []
        for result in self.results:
            if (
                "error" not in result
                and "records" in result
                and isinstance(result["records"], pd.DataFrame)
                and not result["records"].empty
            ):
                trade_actions = result["records"]["Trade_action"].values
                all_trade_actions.extend(trade_actions)

        if all_trade_actions:
            unique_values, counts = np.unique(all_trade_actions, return_counts=True)
            console.print("📊 Trade_action值分布：")
            for value, count in zip(unique_values, counts):
                percentage = count / len(all_trade_actions) * 100
                console.print(f"   {value}: {count} 次 ({percentage:.1f}%)")

            # 檢查是否有NaN值
            nan_count = sum(1 for x in all_trade_actions if pd.isna(x))
            if nan_count > 0:
                console.print(f"⚠️  發現 {nan_count} 個NaN值")

            # 檢查是否有非預期值
            expected_values = {0, 1, 4}
            unexpected_values = set(all_trade_actions) - expected_values
            if unexpected_values:
                console.print(f"❌ 發現非預期值：{unexpected_values}")
        else:
            console.print("❌ 沒有找到有效的交易記錄")

        # 檢查每個回測的Trade_action分布
        console.print("\n📊 各回測Trade_action分布：")
        for i, result in enumerate(self.results[:5]):  # 只顯示前5個
            if (
                "error" not in result
                and "records" in result
                and isinstance(result["records"], pd.DataFrame)
                and not result["records"].empty
            ):
                trade_actions = result["records"]["Trade_action"].values
                unique_values, counts = np.unique(trade_actions, return_counts=True)
                console.print(
                    f"  回測 {i + 1} ({result.get('Backtest_id', 'N/A')}): {dict(zip(unique_values, counts))}"
                )

        console.print("\n[bold #dbac30]按 Enter 繼續: [/bold #dbac30]", end="")
        input()

    def display_no_trade_results(self) -> None:
        """顯示無交易的回測結果"""
        # 使用與VectorBacktestEngine相同的判斷邏輯
        # 無交易：沒有錯誤但沒有開倉交易的回測
        no_trade_results = []
        for r in self.results:
            if r.get("error") is None:
                records = r.get("records")
                if records is None or not isinstance(records, pd.DataFrame):
                    continue
                # 檢查是否有開倉交易（Trade_action == 1）
                if len(records) == 0:
                    no_trade_results.append(r)
                elif (records["Trade_action"] == 1).sum() == 0:
                    no_trade_results.append(r)

        if not no_trade_results:
            show_info("BACKTESTER", "無交易結果：沒有")
            return

        table = Table(title="無交易回測結果", style="bold yellow")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)

        for i, result in enumerate(no_trade_results, 1):
            params = result.get("params")
            strategy = self._get_strategy_name(params) if params else "N/A"

            status = "⚠️ 無交易"

            table.add_row(str(i), result["Backtest_id"], strategy, status)

        console.print(table)
