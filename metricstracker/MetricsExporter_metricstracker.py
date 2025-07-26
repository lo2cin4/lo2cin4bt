"""
MetricsExporter_metricstracker.py

【功能說明】
------------------------------------------------------------
- 專責將每日指標（如 Drawdown, BAH_Equity, BAH_Return）與 summary 指標（依 README 對照表）寫入 DataFrame，並輸出新的 parquet。
- 所有 parquet 輸出、欄位補齊、metadata 寫入等流程全部集中於此，Calculator 僅負責指標計算。
- 支援多策略分群（Backtest_id）與單策略模式。

【使用方式】
------------------------------------------------------------
from metricstracker.MetricsExporter_metricstracker import MetricsExporter
MetricsExporter.export(df, orig_parquet_path, time_unit, risk_free_rate)

"""
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
from rich.console import Console
from rich.panel import Panel
from .MetricsCalculator_metricstracker import MetricsCalculatorMetricTracker

console = Console()

class MetricsExporter:
    @staticmethod
    def add_drawdown_bah(df):
        df = df.copy()
        equity = df['Equity_value']
        roll_max = equity.cummax()
        df['Drawdown'] = (equity - roll_max) / roll_max
        if 'Close' in df.columns:
            initial_equity = equity.iloc[0]
            initial_price = df['Close'].iloc[0]
            df['BAH_Equity'] = initial_equity * (df['Close'] / initial_price)
            df['BAH_Return'] = df['BAH_Equity'].pct_change().fillna(0)
            # 新增 BAH_Drawdown
            bah_roll_max = df['BAH_Equity'].cummax()
            df['BAH_Drawdown'] = (df['BAH_Equity'] - bah_roll_max) / bah_roll_max
        return df

    @staticmethod
    def export(df, orig_parquet_path, time_unit, risk_free_rate):
        orig_table = pq.read_table(orig_parquet_path)
        orig_meta = orig_table.schema.metadata or {}
        # 統一 batch_metadata 寫入，不論單/多策略
        grouped = df.groupby('Backtest_id') if 'Backtest_id' in df.columns else [(None, df)]
        batch_metadata = []
        all_df = []
        # 先讀取舊的 batch_metadata
        old_batch_metadata = []
        if b'batch_metadata' in orig_meta:
            try:
                old_batch_metadata = json.loads(orig_meta[b'batch_metadata'].decode())
            except Exception as e:
                pass
        for Backtest_id, group in grouped:
            group = MetricsExporter.add_drawdown_bah(group)
            all_df.append(group)
            calc = MetricsCalculatorMetricTracker(group, time_unit, risk_free_rate)
            strategy_metrics = calc.calc_strategy_metrics()
            bah_metrics = calc.calc_bah_metrics()
            meta = {'Backtest_id': Backtest_id} if Backtest_id is not None else {}
            for k in strategy_metrics:
                meta[k] = strategy_metrics[k]
            for k in bah_metrics:
                meta[k] = bah_metrics[k]
            batch_metadata.append(meta)
        # 合併舊的 batch_metadata（欄位級合併）
        if old_batch_metadata:
            old_map = {m['Backtest_id']: m for m in old_batch_metadata if 'Backtest_id' in m}
            new_map = {m['Backtest_id']: m for m in batch_metadata if 'Backtest_id' in m}
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
                cleaned_df = df_item.dropna(axis=1, how='all')
                if not cleaned_df.empty:
                    filtered_df.append(cleaned_df)
        
        if filtered_df:
            # 使用更安全的 concat 方式
            try:
                df = pd.concat(filtered_df, ignore_index=True, sort=False)
            except Exception as e:
                # 如果 concat 失敗，嘗試逐個合併
                df = filtered_df[0]
                for df_item in filtered_df[1:]:
                    df = pd.concat([df, df_item], ignore_index=True, sort=False)
        else:
            df = pd.DataFrame()
        new_meta = dict(orig_meta)
        new_meta = {k if isinstance(k, bytes) else str(k).encode(): v for k, v in new_meta.items()}
        new_meta[b'batch_metadata'] = json.dumps(batch_metadata, ensure_ascii=False).encode()
        table = pa.Table.from_pandas(df)
        table = table.replace_schema_metadata(new_meta)
        orig_name = os.path.splitext(os.path.basename(orig_parquet_path))[0]
        out_dir = os.path.join(os.path.dirname(os.path.dirname(orig_parquet_path)), 'metricstracker')
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f'{orig_name}_metrics.parquet')
        pq.write_table(table, out_path)
        
        console.print(Panel(
            f"batch_metadata 已計算並輸出：\n{out_path}",
            title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
            border_style="#dbac30"
        ))
        
        # 立即讀回檢查
        table2 = pq.read_table(out_path)
        meta2 = table2.schema.metadata
        
        console.print(Panel(
            "✅ 交易績效分析完成！",
            title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
            border_style="#dbac30"
        )) 