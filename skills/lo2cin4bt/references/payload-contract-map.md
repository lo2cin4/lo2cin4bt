# Payload Contract Map

Use this to connect frontend pages, API endpoints, generated JSON, and source artifacts.

## API Endpoints

| Endpoint | Page | Generated payload |
| --- | --- | --- |
| `GET /api/app/health` | Setup check | none |
| `GET /api/app/command-center` | Command Center | live registry summary |
| `GET /api/app/run-center/configs` | Run Center | workspace config list |
| `POST /api/app/batches` | Run Center | batch id/status |
| `GET /api/app/metrics/runs` | Metrics selector | run registry |
| `GET /api/app/metrics/{run_id}/overview` | Metrics Overview | `outputs/app/chart_payloads/{run_id}/metrics_overview_payload.json` |
| `GET /api/app/metrics/{run_id}/parameter-matrix` | Parameter Matrix | `outputs/app/chart_payloads/{run_id}/parameter_heatmap_payload.json` |
| `GET /api/app/backtests/{run_id}/{backtest_id}` | Backtests | `outputs/app/chart_payloads/{run_id}/backtest_detail_{id}.json` or generated on demand |
| `GET /api/app/wfa/{run_id}/dashboard` | WFA | `outputs/app/chart_payloads/{run_id}/wfa_dashboard_payload.json` |
| `GET /api/app/statanalyser/{run_id}` | Optional statistical analysis API; no dedicated React route | diagnostic payload only when a separate statanalyser workflow generated it |
| `POST /api/app/screenshots` | Mosaic one-click PNG export | `outputs/app/screenshots/{run_id}/{capture_id}/` |
| `GET /api/app/ai-readable/{run_id}` | AI review | `outputs/app/ai_review/{run_id}/ai_review_pack.json` |

## Runtime Output Roots

- `outputs/app/run_registry/`: app run registry.
- `outputs/app/run_snapshots/{run_id}/`: generated snapshots and managed artifacts.
- `outputs/app/artifact_manifests/{run_id}.json`: artifact list and status.
- `outputs/app/chart_payloads/{run_id}/`: frontend JSON payloads.
- `outputs/app/chart_payloads/{run_id}/shared_series/`: content-addressed
  arrays shared by plot, overview, and detail indexes within the same run.
- `outputs/app/ai_review/{run_id}/`: AI-readable review pack.
- `outputs/app/screenshots/{run_id}/{capture_id}/`: user-requested Mosaic PNG export sets.

These folders are local runtime output and ignored by Git.

## Shared Series Storage

Rust producers still return complete `PlotBundle.v1` and
`BacktestDetailBundle.v3` values. The app I/O boundary stores repeated arrays
once under `shared_series/{sha256}.json` and persists small, fail-closed index
contracts. API and AI-review consumers materialize the original public payload
before use. This is a storage concern only: no strategy family, metric, or
backtest calculation receives a separate execution path.

## Strategy Identity Projection

Frontend result payloads expose a `strategy_summary` projected from the
immutable `outputs/app/run_snapshots/{run_id}/strategy_run.json` snapshot.
Its `display_label` is the saved four-part human-readable strategy identity;
its `asset_label` is derived from the normalized universe. Consumers must not
reconstruct either value from a filename, dropdown text, or strategy-family
fallback. Detailed Strategy Logic is projected from the same snapshot's
executable fields under the contract in
`skills/lo2cin4bt/references/strategy-identity-and-summary.md`.

## Source Artifact Types

- `CanonicalResultBundle`: shared simulation truth returned by the Rust engine and accepted by the mandatory Rust result validator.
- `PlotBundle.v1`: canonical Rust projection consumed by metrics/plotter payload builders.
- `metricstracker_parquet`: optional persisted metrics time series derived from the accepted result.
- `backtester_parquet`: optional persisted trade/action detail; it is not the universal result contract.
- `portfolio_equity_curve_parquet`: portfolio equity through time.
- `portfolio_holdings_parquet`: holdings/selection audit rows.
- `portfolio_rebalance_audit_parquet`: rebalance checkpoints.
- `portfolio_rebalance_trades_parquet`: per-asset rebalance trade rows.
- `portfolio_metadata_json`: config, data health, universe/provenance, summary.
- `wfa_parquet`: WFA/rolling validation selected and diagnostic rows.

Every strategy profile must reach the same accepted result contract. Payload builders may expose different sections when the strategy genuinely generated different evidence, but they must not substitute a family-specific result source or overwrite valid metrics with empty summaries.

## AI Review Procedure

1. Open `ai_review_pack.json`.
2. Confirm run status and artifact manifest readiness.
3. Read `source_payloads` for what the app shows.
4. Read `artifact_table_profiles` for column availability.
5. Use `metric_field_catalog` to discover numeric fields.
6. When a field is absent, report `not generated` or `not available`.
7. Cross-check any surprising UI value against the source artifact profile and payload path.

## Cache Note

The app may cache JSON payloads by schema version. If a payload schema changes, the schema version should bump so stale cached payloads rebuild automatically.
