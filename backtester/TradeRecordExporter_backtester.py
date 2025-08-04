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

import pandas as pd
import numpy as np
import logging
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.console import Group
from rich.text import Text

# 移除重複的logging設置，使用main.py中設置的logger

console = Console()

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
                        # 對於 MA9-MA12，需要顯示連續日數 m
                        if strat_idx in [9, 10, 11, 12]:
                            m = param.get('m', 2)
                            return f"MA{strat_idx}_{ma_type}({period},{m})"
                        else:
                            return f"MA{strat_idx}_{ma_type}({period})"
                elif indicator_type == 'BOLL':
                    strat = param.get('strat', '')
                    ma_length = param.get('ma_length', '')
                    std_multiplier = param.get('std_multiplier', '')
                    return f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                elif indicator_type == 'NDayCycle':
                    n = param.get('n', '')
                    strat_idx = param.get('strat_idx', '')
                    return f"NDayCycle(N={n})"
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
                        # 對於 MA9-MA12，需要顯示連續日數 m
                        if strat_idx in [9, 10, 11, 12]:
                            m = getattr(param, 'm', 2)
                            return f"MA{strat_idx}_{ma_type}({period},{m})"
                        else:
                            return f"MA{strat_idx}_{ma_type}({period})"
                elif indicator_type == 'BOLL':
                    strat = getattr(param, 'strat', '')
                    ma_length = getattr(param, 'ma_length', '')
                    std_multiplier = getattr(param, 'std_multiplier', '')
                    return f"BOLL{strat}_MA({ma_length})_SD({std_multiplier})"
                elif indicator_type == 'NDayCycle':
                    n = getattr(param, 'n', '')
                    strat_idx = getattr(param, 'strat_idx', '')
                    return f"NDayCycle(N={n})"
                else:
                    return indicator_type
            return str(param)
        entry_str = '+'.join([param_to_str(p) for p in params.get('entry', [])])
        exit_str = '+'.join([param_to_str(p) for p in params.get('exit', [])])
        return f"{entry_str}_{exit_str}" if entry_str or exit_str else "Unknown"



    def export_to_csv(self, backtest_id=None):
        """導出交易記錄至 CSV。
        
        Args:
            backtest_id: 指定要導出的回測ID，如果為None則導出所有結果
        """
        try:
            if not self.results:
                console.print(Panel("無回測結果可導出為CSV", title="[bold #ff6b6b]👨‍💻 交易回測 Backtester[/bold #ff6b6b]", border_style="#dbac30"))
                return
            
            # 如果指定了backtest_id，只導出該回測結果
            if backtest_id:
                results_to_export = [r for r in self.results if r.get("Backtest_id") == backtest_id]
                if not results_to_export:
                    console.print(Panel(f"找不到Backtest_id為 {backtest_id} 的回測結果", title="[bold #ff6b6b]👨‍💻 交易回測 Backtester[/bold #ff6b6b]", border_style="#dbac30"))
                    return
            else:
                results_to_export = self.results
            
            exported_count = 0
            msg_lines = []
            for result in results_to_export:
                if result.get("error") is not None:
                    msg_lines.append(f"跳過失敗的回測 {result['Backtest_id']}: {result['error']}")
                    continue
                
                if "records" not in result or not isinstance(result["records"], pd.DataFrame) or result["records"].empty or (result["records"]["Trade_action"] != 0).sum() == 0:
                    msg_lines.append(f"跳過無交易記錄的回測 {result['Backtest_id']}")
                    continue
                
                date_str = datetime.now().strftime("%Y%m%d")
                Backtest_id = result["Backtest_id"]
                params = result.get("params")
                if params is None:
                    msg_lines.append(f"result 無 params 欄位，跳過。result keys: {list(result.keys())}")
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
            
            console.print(Panel("\n".join(msg_lines), title="[bold #8f1511]💾 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
        except Exception as e:
            self.logger.error(
                f"CSV 導出失敗: {e}",
                extra={"Backtest_id": self.Backtest_id},
            )
            raise

    def export_to_parquet(self, backtest_id=None):
        """導出交易記錄至 Parquet，包含 metadata。
        
        Args:
            backtest_id: 指定要導出的回測ID，如果為None則導出所有結果
        """
        try:
            import uuid
            date_str = datetime.now().strftime("%Y%m%d")
            random_id = uuid.uuid4().hex[:8]
            filename = f"{date_str}_{random_id}_{self.Backtest_id}.parquet"
            filepath = os.path.join(self.output_dir, filename)

            metadata = {}
            # 如果指定了backtest_id，只處理該回測結果
            if backtest_id:
                results_to_export = [r for r in self.results if r.get("Backtest_id") == backtest_id]
                if not results_to_export:
                    print(f"找不到Backtest_id為 {backtest_id} 的回測結果")
                    return
            else:
                results_to_export = self.results
                
            if results_to_export:
                batch_metadata = []
                for result in results_to_export:
                    if "Backtest_id" in result:
                        params = result.get("params")
                        if params is None:
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
            if results_to_export:
                for result in results_to_export:
                    # 優化：避免不必要的數據拷貝
                    if "records" in result and not result["records"].empty:
                        # 直接使用原始數據，不進行拷貝
                        records_df = result["records"]
                        all_records.append(records_df)
                
                # 再次檢查並過濾，確保沒有空的 DataFrame 或全為 NA 的 DataFrame
                filtered_records = []
                for df in all_records:
                    if not df.empty and len(df.columns) > 0:
                        # 檢查是否有至少一列包含非 NA 值
                        has_valid_data = False
                        for col in df.columns:
                            if not df[col].isna().all():
                                has_valid_data = True
                                break
                        if has_valid_data:
                            # 清理 DataFrame：移除全為 NA 的列
                            cleaned_df = df.dropna(axis=1, how='all')
                            if not cleaned_df.empty:
                                filtered_records.append(cleaned_df)
                
                if filtered_records:
                    # 使用更安全的 concat 方式
                    try:
                        combined_records = pd.concat(filtered_records, ignore_index=True, sort=False)
                    except Exception as e:
                        # 如果 concat 失敗，嘗試逐個合併
                        combined_records = filtered_records[0]
                        for df in filtered_records[1:]:
                            combined_records = pd.concat([combined_records, df], ignore_index=True, sort=False)
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

    def display_backtest_summary(self):
        """顯示回測摘要，包含預覽表格和操作選項。"""
        if not self.results:
            console.print(Panel("無回測結果可顯示摘要", title="警告", style="yellow"))
            return

        # 智能分頁：如果結果超過15個，使用分頁顯示
        if len(self.results) > 15:
            self._display_paginated_summary()
        else:
            self._display_full_summary()

    def _display_full_summary(self):
        """顯示完整摘要表格（結果數量 ≤ 15）"""
        
        table = Table(title="回測摘要", style="bold magenta")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)

        for i, result in enumerate(self.results, 1):
            if result.get("error") is not None:
                table.add_row(
                    str(i),
                    result["Backtest_id"],
                    "失敗",
                    "❌ 失敗"
                )
                continue

            if "records" not in result or not isinstance(result["records"], pd.DataFrame) or result["records"].empty or (result["records"]["Trade_action"] != 0).sum() == 0:
                params = result.get("params")
                strategy = self._get_strategy_name(params) if params else "N/A"
                table.add_row(
                    str(i),
                    result["Backtest_id"],
                    strategy,
                    "⚠️ 無交易"
                )
                continue

            params = result.get("params")
            if params is None:
                table.add_row(
                    str(i),
                    result["Backtest_id"],
                    "N/A",
                    "❌ 失敗"
                )
                continue

            # 生成策略名稱
            strategy = self._get_strategy_name(params)

            table.add_row(
                str(i),
                result["Backtest_id"],
                strategy,
                "✅ 成功"
            )

        console.print(table)
        self._show_operation_menu()

    def _display_paginated_summary(self):
        """分頁顯示摘要表格（結果數量 > 15）"""
        page_size = 15
        total_results = len(self.results)
        total_pages = (total_results + page_size - 1) // page_size
        page = 1
        while True:
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, total_results)
            
            table = Table(title=f"回測結果 - 第 {page} 頁 (共 {total_pages} 頁)", style="bold magenta")
            table.add_column("序號", style="cyan", no_wrap=True)
            table.add_column("回測ID", style="green", no_wrap=True)
            table.add_column("策略", style="blue", no_wrap=True)
            table.add_column("狀態", style="yellow", no_wrap=True)
            
            for i in range(start_idx, end_idx):
                result = self.results[i]
                # 嚴格判斷成功/無交易/失敗 - 檢查實際交易行為
                is_success = (
                    result.get("error") is None and
                    "records" in result and
                    isinstance(result["records"], pd.DataFrame) and
                    not result["records"].empty and
                    (result["records"]["Trade_action"] != 0).sum() > 0
                )
                is_no_trade = (
                    result.get("error") is None and
                    "records" in result and
                    isinstance(result["records"], pd.DataFrame) and
                    not result["records"].empty and
                    (result["records"]["Trade_action"] != 0).sum() == 0
                )
                is_failed = result.get("error") is not None
                if is_failed:
                    table.add_row(
                        str(i + 1),
                        result["Backtest_id"],
                        "失敗",
                        "[red]❌ 失敗[/red]"
                    )
                elif is_no_trade:
                    params = result.get("params")
                    strategy = self._get_strategy_name(params) if params else "N/A"
                    table.add_row(
                        str(i + 1),
                        result["Backtest_id"],
                        strategy,
                        "[yellow]⚠️ 無交易[/yellow]"
                    )
                elif is_success:
                    params = result.get("params")
                    strategy = self._get_strategy_name(params) if params else "N/A"
                    table.add_row(
                        str(i + 1),
                        result["Backtest_id"],
                        strategy,
                        "[green]✅ 成功[/green]"
                    )
                else:
                    # 其他異常情況也標示為失敗
                    table.add_row(
                        str(i + 1),
                        result.get("Backtest_id", "N/A"),
                        "異常",
                        "[red]❌ 失敗[/red]"
                    )
            
            console.print(table)
            
            # 分頁導航
            if total_pages > 1:
                console.print(Panel("📄 分頁導航: [m] 下一頁(m) | [n] 上一頁(n) | [數字] 跳轉到指定頁 | [q] 進入操作選單(q)", title="[bold #ff6b6b]📄 👨‍💻 交易回測 Backtester[/bold #ff6b6b]", border_style="#dbac30"))
                console.print("[bold #dbac30]請輸入導航指令: [/bold #dbac30]", end="")
                nav = input().lower()
                
                if nav == 'q':
                    break
                elif nav == 'm' and page < total_pages:
                    page += 1
                    console.clear()
                elif nav == 'n' and page > 1:
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
        
        self._show_operation_menu()

    def _show_operation_menu(self):
        """顯示操作選單"""
        # 提供操作選項
        menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
        
        console.print(Panel(menu_text, title=Text("👨‍💻 交易回測 Backtester", style="bold #ff6b6b"), border_style="#dbac30"))

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
                console.print(Panel(menu_text, title=Text("👨‍💻 交易回測 Backtester", style="bold #ff6b6b"), border_style="#dbac30"))
            elif choice == "2":
                self.display_failed_results()
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                console.print(Panel(menu_text, title=Text("👨‍💻 交易回測 Backtester", style="bold #ff6b6b"), border_style="#dbac30"))
            elif choice == "3":
                self.export_to_csv()
                console.print("✅ CSV 導出完成！", style="green")
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                console.print(Panel(menu_text, title=Text("👨‍💻 交易回測 Backtester", style="bold #ff6b6b"), border_style="#dbac30"))
            elif choice == "4":
                while True:
                    console.print("[bold #dbac30]請輸入Backtest ID（可用逗號分隔多個），或按Enter返回選單: [/bold #dbac30]", end="")
                    backtest_id_input = input()
                    if not backtest_id_input:
                        # 直接返回選單
                        break
                    # 支援多個ID
                    backtest_ids = [bid.strip() for bid in backtest_id_input.split(",") if bid.strip()]
                    not_found = [bid for bid in backtest_ids if not any(r.get("Backtest_id") == bid for r in self.results)]
                    if not backtest_ids:
                        continue
                    if not_found:
                        console.print(Panel(f"找不到Backtest_id為 {', '.join(not_found)} 的回測結果", title=Text("👨‍💻交易回測 Backtester", style="bold #ff6b6b"), border_style="#8f1511"))
                        continue
                    for bid in backtest_ids:
                        self.export_to_csv(backtest_id=bid)
                    console.print(f"✅ 已導出 {len(backtest_ids)} 個特定回測 CSV！", style="green")
                    break
                # 重新顯示選單
                menu_text = """1. 查看成功結果
2. 查看失敗結果
3. 導出所有回測結果為 CSV
4. 導出特定回測結果為 CSV (輸入 Backtest_id)
5. 結束交易回測，進入下一階段"""
                console.print(Panel(menu_text, title=Text("👨‍💻 交易回測 Backtester", style="bold #ff6b6b"), border_style="#dbac30"))
            elif choice == "5":
                console.print("結束交易回測，進入下一階段...", style="yellow")
                break
            else:
                console.print("無效選擇，請重新輸入。", style="red")

    def display_results_by_strategy(self):
        """按策略分組顯示結果。"""
        if not self.results:
            console.print(Panel("無回測結果可顯示", title="警告", style="yellow"))
            return
        
        # 按策略分組
        strategy_groups = {}
        for result in self.results:
            if "error" in result:
                strategy = "失敗"
            elif "records" not in result or result["records"].empty or (result["records"]["Trade_action"] != 0).sum() == 0:
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
            success_count = len([r for r in results if "error" not in r and "records" in r and not r["records"].empty and (r["records"]["Trade_action"] != 0).sum() > 0])
            total_count = len(results)
            console.print(f"{i}. {strategy}: {success_count}/{total_count} 成功")
        
        # 選擇策略查看詳情
        while True:
            console.print(Panel("⌨請選擇策略編號查看詳情", title=Text("👨‍💻 交易回測 Backtester", style="bold #ff6b6b"), border_style="#dbac30"))
            choice = input(" 策略編號 (或按 Enter 返回選單): ")
            if not choice:
                break
            
            try:
                choice_idx = int(choice) - 1
                strategy_list = list(strategy_groups.keys())
                if 0 <= choice_idx < len(strategy_list):
                    selected_strategy = strategy_list[choice_idx]
                    self.display_strategy_details(selected_strategy, strategy_groups[selected_strategy])
                else:
                    console.print("策略編號超出範圍", style="red")
            except ValueError:
                console.print("請輸入有效的數字", style="red")

    def display_strategy_details(self, strategy, results):
        """顯示特定策略的詳細結果。"""
        console.print(f"\n=== {strategy} 策略詳情 ===")
        
        table = Table(title=f"{strategy} - 回測結果", style="bold magenta")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)

        
        for i, result in enumerate(results, 1):
            if "error" in result:
                status = "❌ 失敗"
                total_return = "N/A"
                trade_count = "N/A"
            elif "records" not in result or result["records"].empty or (result["records"]["Trade_action"] != 0).sum() == 0:
                status = "⚠️ 無交易"
                total_return = "N/A"
                trade_count = "0"
            else:
                status = "✅ 成功"
                total_return = f"{result.get('total_return', 0):.2%}" if result.get('total_return') is not None else "N/A"
                trade_count = str(result.get('total_trades', 0))
            
            params = result.get("params", {})
            predictor = params.get("predictor", "N/A")
            
            table.add_row(
                str(i),
                result["Backtest_id"][:8] + "...",
                predictor,
                status,
                total_return,
                trade_count
            )
        
        console.print(table)
        # console.print(Panel("⌨️ 按 Enter 回到選單", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
        console.print("[bold #dbac30]按 Enter 返回選單: [/bold #dbac30]", end="")
        input()

    def display_successful_results(self):
        """顯示成功的回測結果"""
        successful_results = [r for r in self.results if "error" not in r and "records" in r and isinstance(r["records"], pd.DataFrame) and not r["records"].empty and (r["records"]["Trade_action"] != 0).sum() > 0]
        
        if not successful_results:
            console.print(Panel("成功結果：沒有", title="[bold #ff6b6b]👨‍💻 交易回測 Backtester[/bold #ff6b6b]", border_style="#dbac30"))
            return
        
        table = Table(title="成功回測結果", style="bold green")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)
        
        for i, result in enumerate(successful_results, 1):
            params = result.get("params")
            strategy = self._get_strategy_name(params) if params else "N/A"
            
            table.add_row(
                str(i),
                result["Backtest_id"],
                strategy,
                "✅ 成功"
            )
        
        console.print(table)

    def display_failed_results(self):
        """顯示失敗的回測結果"""
        failed_results = [r for r in self.results if "error" in r or "records" not in r or not isinstance(r["records"], pd.DataFrame) or r["records"].empty or (r["records"]["Trade_action"] != 0).sum() == 0]
        
        if not failed_results:
            console.print(Panel("失敗結果：沒有", title="[bold #ff6b6b]👨‍💻 交易回測 Backtester[/bold #ff6b6b]", border_style="#dbac30"))
            return
        
        table = Table(title="失敗回測結果", style="bold red")
        table.add_column("序號", style="cyan", no_wrap=True)
        table.add_column("回測ID", style="green", no_wrap=True)
        table.add_column("策略", style="blue", no_wrap=True)
        table.add_column("狀態", style="yellow", no_wrap=True)
        
        for i, result in enumerate(failed_results, 1):
            params = result.get("params")
            strategy = self._get_strategy_name(params) if params else "N/A"
            
            if "error" in result:
                status = "❌ 失敗"
                error_msg = result.get("error", "未知錯誤")
            elif "records" not in result or result["records"].empty:
                status = "⚠️ 無交易"
                error_msg = "無交易記錄"
            else:
                status = "❌ 失敗"
                error_msg = "未知錯誤"
            
            table.add_row(
                str(i),
                result["Backtest_id"],
                strategy,
                status
            )
        
        console.print(table)
