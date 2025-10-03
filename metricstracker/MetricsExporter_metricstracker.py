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
        df = df.copy()
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

        # 統一 batch_metadata 寫入，不論單/多策略
        grouped = (
            df.groupby("Backtest_id") if "Backtest_id" in df.columns else [(None, df)]
        )
        batch_metadata = []
        all_df = []

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
        for Backtest_id, group in grouped:
            group = MetricsExporter.add_drawdown_bah(group)
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
        # 合併舊的 batch_metadata（欄位級合併）
        if old_batch_metadata:
            old_map = {
                m["Backtest_id"]: m for m in old_batch_metadata if "Backtest_id" in m
            }
            new_map = {
                m["Backtest_id"]: m for m in batch_metadata if "Backtest_id" in m
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
        # 過濾空的 DataFrame 以避免 FutureWarning
        filtered_df = []
        for df_item in all_df:
            if not df_item.empty and len(df_item.columns) > 0:
                # 清理 DataFrame：移除全為 NA 的列
                cleaned_df = df_item.dropna(axis=1, how="all")
                if not cleaned_df.empty:
                    filtered_df.append(cleaned_df)

        if filtered_df:
            # 使用更安全的 concat 方式
            try:
                df = pd.concat(filtered_df, ignore_index=True, sort=False)
            except Exception:
                # 如果 concat 失敗，嘗試逐個合併
                df = filtered_df[0]
                for df_item in filtered_df[1:]:
                    df = pd.concat([df, df_item], ignore_index=True, sort=False)
        else:
            df = pd.DataFrame()
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

        table = pa.Table.from_pandas(df)
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
