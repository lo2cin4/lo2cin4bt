# metricstracker

`metricstracker` exports performance metrics for backtest equity parquet files.

The canonical metric math is implemented in Rust:

- Rust crate: `rust/lo2cin4bt_core/src/metrics.rs`
- Unified service: `rust/lo2cin4bt_core/src/bin/engine_service_cli.rs`
- Rust parquet reader: Polars `0.54.x`, pinned through repo Rust toolchain `1.96.0`
- Rust transport: `metricstracker/RustMetrics_metricstracker.py`
- Artifact writer: `metricstracker/MetricsArtifactWriter_metricstracker.py`
- Runtime boundary: persistent engine-service bridge through
  `backtester/RustCoreBridge_backtester.py`

Python remains responsible for orchestration only:

1. select/source backtester parquet files;
2. call the Rust parquet metrics kernel through the subprocess bridge for
   metric metadata;
3. read only lightweight output columns for the metrics parquet artifact;
4. write metrics parquet and JSON sidecar metadata.

This is intentionally not a PyO3/maturin wheel path. Production metric runs use
the repo-local persistent `engine_service_cli` process. If the binary is missing
or stale, the bridge may rebuild it through Cargo for local development, but
there is no shape-specific metrics CLI fallback.

The retired Python metrics calculator was removed because it duplicated Rust
logic and produced incompatible Calmar values on short samples. New runtime code
should use `compute_metrics_for_frame()` or `export_metrics_artifacts()`.

## Metric Assumptions

- `time_unit`: annualization periods, usually `252` for traditional daily assets
  and `365` for crypto.
- `risk_free_rate`: annual rate as decimal, for example `0.04` for 4%.
- Calmar is canonicalized as:

```text
(Annualized_return - risk_free_rate) / abs(Max_drawdown)
```

## Files

```text
metricstracker/
+-- __init__.py
+-- MetricConfig_metricstracker.py  # metric assumption resolver
+-- RustMetrics_metricstracker.py
+-- MetricsArtifactWriter_metricstracker.py
+-- README.md
\-- utils/
```
