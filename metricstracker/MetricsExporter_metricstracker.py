"""
MetricsExporter_metricstracker.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 績效分析框架的績效指標導出工具，負責將績效分析結果導出為多種格式，支援 CSV、Excel、JSON 等格式，便於後續分析。

【流程與數據流】
------------------------------------------------------------
- 由 BaseMetricTracker 調用，導出績效分析結果
- 導出結果供用戶或下游模組分析

```mermaid
flowchart TD
    A[BaseMetricTracker] -->|調用| B[MetricsExporter]
    B -->|導出結果| C[CSV/Excel/JSON]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改導出格式、欄位時，請同步更新頂部註解與下游流程
- 若導出結構有變動，需同步更新本檔案與上游模組
- 導出格式如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 導出格式錯誤或欄位缺失會導致導出失敗
- 檔案權限不足會導致寫入失敗
- 數據結構變動會影響下游分析

【範例】
------------------------------------------------------------
- exporter = MetricsExporter()
  exporter.export_metrics(metrics, format='csv')

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseMetricTracker 調用，導出結果供用戶或下游模組使用
- 需與上游模組的數據結構保持一致

【參考】
------------------------------------------------------------
- pandas 官方文件
- Base_metricstracker.py、MetricsCalculator_metricstracker.py
- 專案 README
"""

import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console
from rich.panel import Panel

from .MetricsCalculator_metricstracker import MetricsCalculatorMetricTracker

console = Console()


class MetricsExporter:
    @staticmethod
    def add_drawdown_bah(df):
        # 避免不必要複製，只在需要時複製
        if "Drawdown" not in df.columns or "BAH_Equity" not in df.columns:
            df = df.copy()
        else:
            # 如果欄位已存在，直接返回
            return df
        
        equity = df["Equity_value"]
        roll_max = equity.cummax()
        df["Drawdown"] = (equity - roll_max) / roll_max
        if "Close" in df.columns:
            initial_equity = equity.iloc[0]
            initial_price = df["Close"].iloc[0]
            df["BAH_Equity"] = initial_equity * (df["Close"] / initial_price)
            df["BAH_Return"] = df["BAH_Equity"].pct_change().fillna(0)
            # 新增 BAH_Drawdown
            bah_roll_max = df["BAH_Equity"].cummax()
            df["BAH_Drawdown"] = (df["BAH_Equity"] - bah_roll_max) / bah_roll_max
        return df

    @staticmethod
    def export(df, orig_parquet_path, time_unit, risk_free_rate):
        # 嘗試讀取原始 parquet 檔案
        try:
            orig_table = pq.read_table(orig_parquet_path)
            orig_meta = orig_table.schema.metadata or {}
        except Exception as e:
            console.print(
                Panel(
                    f"⚠️ 讀取 Parquet 檔案時發生錯誤: {e}\n"
                    "這可能是由於 metadata 過大導致的。嘗試使用簡化模式...",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            # 使用簡化模式，不讀取 metadata
            orig_meta = {}
            console.print(
                Panel(
                    "✅ 已切換到簡化模式，將忽略舊的 metadata",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

        # 先讀取舊的 batch_metadata（從分離的 JSON 檔案）
        old_batch_metadata = []
        orig_name = os.path.splitext(os.path.basename(orig_parquet_path))[0]
        out_dir = os.path.join(
            os.path.dirname(os.path.dirname(orig_parquet_path)), "metricstracker"
        )
        metadata_json_path = os.path.join(out_dir, f"{orig_name}_metadata.json")

        # 嘗試從 JSON 檔案讀取舊的 batch_metadata
        if os.path.exists(metadata_json_path):
            try:
                with open(metadata_json_path, "r", encoding="utf-8") as f:
                    old_batch_metadata = json.load(f)
            except Exception as e:
                console.print(
                    Panel(
                        f"⚠️ 無法讀取舊的 metadata JSON 檔案: {e}",
                        title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

        # 如果 JSON 檔案不存在，嘗試從 Parquet metadata 讀取（向後相容）
        if not old_batch_metadata and b"batch_metadata" in orig_meta:
            try:
                old_batch_metadata = json.loads(orig_meta[b"batch_metadata"].decode())
                # 將舊的 metadata 遷移到 JSON 檔案
                os.makedirs(out_dir, exist_ok=True)
                with open(metadata_json_path, "w", encoding="utf-8") as f:
                    json.dump(old_batch_metadata, f, ensure_ascii=False, indent=2)
                console.print(
                    Panel(
                        f"✅ 已將舊的 batch_metadata 遷移到 JSON 檔案",
                        title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )
            except Exception as e:
                console.print(
                    Panel(
                        f"⚠️ 無法讀取舊的 Parquet metadata: {e}",
                        title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
        
        # 統一 batch_metadata 寫入，不論單/多策略
        # 優化：先添加 drawdown 欄位到整個 DataFrame，避免每個 group 都複製
        df = MetricsExporter.add_drawdown_bah(df)
        
        # 檢查是否有太多策略組合，如果是則分批次處理
        if "Backtest_id" in df.columns:
            unique_backtest_ids = df["Backtest_id"].nunique()
            grouped = df.groupby("Backtest_id")
            batch_metadata = []
            all_df = []
            
            if unique_backtest_ids > 10000:
                console.print(
                    Panel(
                        f"⚠️ 檢測到大量策略組合 ({unique_backtest_ids} 個)，將使用批次處理以優化記憶體使用（每批 1000 個）",
                        title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                # 分批處理，每批處理 1000 個
                batch_size = 1000
                all_backtest_ids = list(grouped.groups.keys())
                total_batches = (len(all_backtest_ids) + batch_size - 1) // batch_size
                
                for batch_idx in range(total_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, len(all_backtest_ids))
                    batch_ids = all_backtest_ids[start_idx:end_idx]
                    
                    console.print(
                        Panel(
                            f"處理批次 {batch_idx + 1}/{total_batches} (策略 {start_idx + 1}-{end_idx}/{unique_backtest_ids})",
                            title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    
                    for Backtest_id in batch_ids:
                        group = grouped.get_group(Backtest_id)
                        all_df.append(group)
                        calc = MetricsCalculatorMetricTracker(group, time_unit, risk_free_rate)
                        strategy_metrics = calc.calc_strategy_metrics()
                        bah_metrics = calc.calc_bah_metrics()
                        meta = {"Backtest_id": Backtest_id} if Backtest_id is not None else {}
                        for k in strategy_metrics:
                            meta[k] = strategy_metrics[k]
                        for k in bah_metrics:
                            meta[k] = bah_metrics[k]
                        batch_metadata.append(meta)
                        # 清理臨時對象以釋放記憶體
                        del calc, strategy_metrics, bah_metrics, meta, group
            else:
                grouped = df.groupby("Backtest_id")
                batch_metadata = []
                all_df = []
                for Backtest_id, group in grouped:
                    all_df.append(group)
                    calc = MetricsCalculatorMetricTracker(group, time_unit, risk_free_rate)
                    strategy_metrics = calc.calc_strategy_metrics()
                    bah_metrics = calc.calc_bah_metrics()
                    meta = {"Backtest_id": Backtest_id} if Backtest_id is not None else {}
                    for k in strategy_metrics:
                        meta[k] = strategy_metrics[k]
                    for k in bah_metrics:
                        meta[k] = bah_metrics[k]
                    batch_metadata.append(meta)
                    # 清理臨時對象以釋放記憶體
                    del calc, strategy_metrics, bah_metrics, meta
        else:
            # 沒有 Backtest_id，直接處理整個 DataFrame
            all_df = [df]
            calc = MetricsCalculatorMetricTracker(df, time_unit, risk_free_rate)
            strategy_metrics = calc.calc_strategy_metrics()
            bah_metrics = calc.calc_bah_metrics()
            meta = {}
            for k in strategy_metrics:
                meta[k] = strategy_metrics[k]
            for k in bah_metrics:
                meta[k] = bah_metrics[k]
            batch_metadata = [meta]
            del calc, strategy_metrics, bah_metrics
        # 合併舊的 batch_metadata（欄位級合併）
        if old_batch_metadata:
            old_map = {
                m.get("Backtest_id"): m for m in old_batch_metadata if "Backtest_id" in m
            }
            new_map = {
                m.get("Backtest_id"): m for m in batch_metadata if "Backtest_id" in m
            }
            all_ids = set(old_map.keys()) | set(new_map.keys())
            merged = []
            for bid in all_ids:
                if bid in old_map and bid in new_map:
                    merged_dict = dict(old_map[bid])
                    merged_dict.update(new_map[bid])  # 新欄位覆蓋舊欄位
                    merged.append(merged_dict)
                elif bid in new_map:
                    merged.append(new_map[bid])
                else:
                    merged.append(old_map[bid])
            batch_metadata = merged

        # 直接基於原始 df 構建精簡輸出，避免將所有分組 DataFrame 彙整到記憶體
        preferred_cols = [
            "Time",
            "Equity_value",
            "BAH_Equity",
            "BAH_Return",
            "Drawdown",
            "Backtest_id",
        ]
        available_cols = [c for c in preferred_cols if c in df.columns]
        if available_cols:
            df_out = df[available_cols].copy()
        else:
            fallback = [c for c in ["Time", "Equity_value"] if c in df.columns]
            df_out = df[fallback].copy() if fallback else df.head(0).copy()

        # 降精度降低記憶體佔用
        for col in df_out.columns:
            if df_out[col].dtype == "float64":
                df_out[col] = df_out[col].astype("float32")
        # 將 batch_metadata 儲存到獨立的 JSON 檔案
        os.makedirs(out_dir, exist_ok=True)
        with open(metadata_json_path, "w", encoding="utf-8") as f:
            json.dump(batch_metadata, f, ensure_ascii=False, indent=2)

        # 清理 Parquet metadata，移除可能過大的 batch_metadata
        new_meta = dict(orig_meta)
        new_meta = {
            k if isinstance(k, bytes) else str(k).encode(): v
            for k, v in new_meta.items()
        }
        # 移除 batch_metadata 以避免 Parquet 檔案過大
        if b"batch_metadata" in new_meta:
            del new_meta[b"batch_metadata"]
            console.print(
                Panel(
                    "✅ 已將 batch_metadata 從 Parquet 檔案中移除，改為儲存在 JSON 檔案中",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

        table = pa.Table.from_pandas(df_out)
        table = table.replace_schema_metadata(new_meta)
        out_path = os.path.join(out_dir, f"{orig_name}_metrics.parquet")
        pq.write_table(table, out_path)

        console.print(
            Panel(
                f"batch_metadata 已計算並輸出：\n📊 Parquet 檔案: {out_path}\n📋 Metadata JSON: {metadata_json_path}",
                title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

        # 立即讀回檢查
        try:
            pq.read_table(out_path)
            console.print(
                Panel(
                    "✅ Parquet 檔案驗證成功！",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
        except Exception as e:
            console.print(
                Panel(
                    f"⚠️ Parquet 檔案驗證失敗: {e}\n"
                    "但檔案已成功寫入，metadata 已分離到 JSON 檔案中",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )

        console.print(
            Panel(
                "✅ 交易績效分析完成！",
                title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                border_style="#dbac30",
            )
        )
