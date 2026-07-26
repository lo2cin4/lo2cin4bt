# lo2cin4bt App Core Contracts

This directory defines the machine-readable truth layer for the future browser-first app.

## Scope
The foundation MVP app must consume these contracts before any large UI implementation begins.

## Decisions Locked In This Phase
- `main.py` targets immediate cutover to the new app.
- Foundation v1 manages only new runs created after app rollout.
- Existing legacy `outputs/*` are not adopted in this phase.
- The app is registry-first and does not scan raw outputs as its primary logic.

## Contracts
1. `run-registry-v1`
- One index record per managed run.
- Tracks status, snapshot location, health path, and manifest path.

2. `run-snapshot-v1`
- Captures the resolved config and contract state used by the run.
- Keeps future history review deterministic even if workspace configs change later.

3. `artifact-manifest-v1`
- Lists every produced artifact and whether the app may treat it as ready.
- `ready` is only legal after artifact finalize completes.

4. `dataloader-health-v1`
- Holds data coverage and alignment quality.
- Fill and stale quality belong here, not in the primary plotter surface.

5. `data-lineage-manifest-v1`
- Records input sources, hashes where available, transformations, validity flags, and honest lineage claims.
- Provider/API data without a reproducible content snapshot must be `partial` or `unknown`, never `complete`.
- Universe provenance must separate requested symbols from Historical Universe Constituents evidence; static/current symbol lists keep survivorship risk visible.

6. `chart-payload-v1`
- Standard machine-readable chart payload for browser rendering and AI consumption.
- Rust owns the complete plot projection. App storage externalizes repeated
  `x/y`, benchmark, and multi-asset OHLC arrays into run-level
  `shared_series/{sha256}.json` objects; index payloads are materialized back to
  the complete public contract at the API and AI-review boundaries.

6a. `chart-series-storage-v1`
- Defines the persisted shared-series envelope and the plot, metrics-overview,
  and backtest-detail index contracts. Every reference is a SHA-256 content
  address and is validated before materialization.

7. `page-artifact-matrix-v1`
- Declares page-level required artifacts, optional artifacts, blocking conditions, and fallback rules.

8. `ai-readable-output-v1`
- Aggregates registry, stage status, manifest, snapshots, chart payloads, and artifact profiles into one AI review pack.
- It intentionally embeds all run-local chart payload JSON so future metrics appear automatically when payload producers add them.

## App Storage Layout
Managed run outputs use:
- `outputs/app/run_registry/{run_id}.json`
- `outputs/app/latest_runs.json`
- `outputs/app/run_snapshots/{run_id}/...`
- `outputs/app/artifact_manifests/{run_id}.json`
- `outputs/app/chart_payloads/{run_id}/...`
- `outputs/app/chart_payloads/{run_id}/shared_series/{sha256}.json`
- `outputs/app/ai_review/{run_id}/ai_review_pack.json`
- `outputs/app/stage_status/{run_id}.json`

`dataloader_health.json` belongs inside `outputs/app/run_snapshots/{run_id}/` so the registry can point at a single run-local snapshot tree.
`data_lineage_manifest.json` also belongs inside the same run snapshot directory so AI review can inspect data provenance without scanning raw outputs.

## Lifecycle Rules
Run status:
- `queued`
- `running`
- `completed`
- `failed`
- `partial`

Artifact status:
- `pending`
- `writing`
- `ready`
- `missing`
- `failed`

Rules:
- `running` registry entries may exist before artifacts are ready.
- UI must not treat artifacts as loadable until the manifest marks them `ready`.
- `completed` means all required artifacts are ready.
- `partial` means core results exist, but at least one optional stage failed or is missing.
- `failed` means the run is not valid for a results page.

## Immediate Cutover Boundary
Foundation v1 uses the existing modules only as service layer:
- `autorunner`
- `backtester`
- `metricstracker`
- `statanalyser`
- `validation_workflow`
- `app.api` / `app.runtime` payload services

The old CLI menu is not the target user entry once cutover happens. `main.py` will open the unified app instead.

## StatAnalyser Foundation v1 Output Level
The first app version requires only:
- summary JSON
- tabular output
- report file
- chart payload if one already exists

The first app version does not require:
- PCA
- confusion matrix
- new multi-parameter analysis generation inside the UI

If a supported analysis has no generated artifact yet, the app should show `Not generated yet` rather than recompute it in the browser.
