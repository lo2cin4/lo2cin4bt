"""Persist canonical Rust metrics results without recomputing in Python."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from backtester.RustCoreBridge_backtester import run_metrics_parquet_via_cli
from utils.filename_utils import bounded_filename_stem


def export_metrics_artifacts(
    source_path: str,
    *,
    time_unit: int,
    risk_free_rate: float,
    benchmark_parquet_path: str | None = None,
    benchmark_symbol: str | None = None,
) -> Dict[str, str]:
    source = Path(source_path).resolve()
    request = {
        "parquet_path": str(source),
        "time_unit": int(time_unit),
        "risk_free_rate": float(risk_free_rate),
    }
    if benchmark_parquet_path:
        request["benchmark_parquet_path"] = str(Path(benchmark_parquet_path).resolve())
        request["benchmark_symbol"] = str(benchmark_symbol or "")
    summary = run_metrics_parquet_via_cli(
        request,
        timeout=60,
    )
    metrics, enriched_rows = summary.get("metrics"), summary.get("enriched_rows")
    if not isinstance(metrics, list) or not isinstance(enriched_rows, list):
        raise RuntimeError("Rust metrics_parquet returned an invalid contract")
    annualization = summary.get("annualization")
    if not isinstance(annualization, dict):
        raise RuntimeError("Rust metrics_parquet returned no annualization contract")
    output_dir = source.parent.parent / "metricstracker"
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = bounded_filename_stem(source.stem, max_length=96, fallback="metrics")
    metadata_path = output_dir / f"{stem}_metadata.json"
    parquet_path = output_dir / f"{stem}_metrics.parquet"
    metadata_rows = [
        {
            **row,
            "Annualization": annualization,
            "Metrics_kernel": "rust_metrics_parquet_v1",
        }
        for row in metrics
        if isinstance(row, dict)
    ]
    metadata_path.write_text(
        json.dumps(metadata_rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    source_frame = pd.read_parquet(source)
    columns = [
        name
        for name in ["Time", "Session_label", "Equity_value", "Backtest_id"]
        if name in source_frame
    ]
    output = source_frame[columns].copy()
    enriched = pd.DataFrame(enriched_rows).sort_values("row_index")
    if len(enriched) != len(output):
        raise RuntimeError("Rust metrics_parquet enrichment row count mismatch")
    for name in ["BAH_Equity", "BAH_Return", "Drawdown", "BAH_Drawdown"]:
        if name in enriched:
            output[name] = pd.to_numeric(enriched[name], errors="coerce").to_numpy()
    metadata = {key: value for key, value in (pq.read_schema(source).metadata or {}).items() if key != b"batch_metadata"}
    table = pa.Table.from_pandas(output, preserve_index=False).replace_schema_metadata(metadata)
    pq.write_table(table, parquet_path)
    return {"parquet_path": str(parquet_path), "metadata_path": str(metadata_path)}
