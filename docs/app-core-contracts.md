# App Core Contracts

This document is the human-readable complement to `app/contracts/*`.

## Purpose
We are defining the canonical app truth layer before large UI work begins.
The first browser-first app must read machine-readable contracts instead of guessing state from raw filenames under `outputs/*`.

## Immediate Cutover Target
- `main.py` will ultimately launch the unified app.
- Existing autorunner / WFA / statanalyser functionality must be consumed through the canonical app/runtime and public service boundaries, not retired Python base controllers.
- The new app does not need to adopt legacy outputs in its first managed version.

## New Managed Paths
The app manages only new runs written after rollout.
The canonical layout is:
- `outputs/app/run_registry/{run_id}.json`
- `outputs/app/latest_runs.json`
- `outputs/app/run_snapshots/{run_id}/...`
- `outputs/app/artifact_manifests/{run_id}.json`
- `outputs/app/chart_payloads/{run_id}/...`
- `outputs/app/ai_review/{run_id}/ai_review_pack.json`
- `outputs/app/stage_status/{run_id}.json`

`dataloader_health.json` is stored inside the run snapshot directory.
`data_lineage_manifest.json` is stored beside it and is the canonical run-local record of data provenance, source hashes where available, transformation claims, and unknown validity assumptions.

## AI-Readable Output
Each completed or partial app run produces an AI-readable review pack:

- `outputs/app/ai_review/{run_id}/ai_review_pack.json`
- contract id: `lo2cin4bt-app-ai-readable-output-v1`
- artifact type: `ai_readable_output_json`

The pack is intentionally assembled from existing machine-readable truth rather
than a manually curated metric list. It embeds every JSON file in the run's
`chart_payloads` directory, embeds direct JSON snapshots, includes the full
artifact manifest, and profiles ready table artifacts by artifact type.
Because the data lineage manifest is a direct JSON snapshot, it is embedded
automatically under `snapshot_payloads.data_lineage_manifest`.

This is the forward-compatibility rule: if a future feature or performance
score is added to a app payload or artifact table column, it is picked up
automatically by the AI-readable output without adding a new field mapping.

## Data Lineage Discipline
Every managed run should write `data_lineage_manifest.json`.

The lineage manifest must distinguish proven facts from inferred or unknown
claims. Local files and frozen artifacts should carry content hashes. Provider
API sources may record provider identity, symbols, requested range, resolved
range, and cache references, but they must not claim `complete` unless the
actual consumed content is snapshotted and hashed.

Historical Universe Constituents are a separate validity claim. A configured symbol list,
`all_symbols`, or provider/current universe is recorded as usable provenance,
but it is not treated as survivorship-safe unless the manifest carries
point-in-time constituents evidence and a delisted-symbol policy.

## Statanalyser Foundation v1 Policy
The first app version accepts three mandatory output levels and one optional level.

Mandatory:
- summary JSON
- tabular output
- report file

Optional:
- chart payload

This means the app may display:
- current summary cards
- download links
- a missing or not-generated state for charts that do not yet exist

The app will not compute PCA, confusion matrix, or new heavy analysis in-browser during this phase.

## Page Artifact Discipline
All app pages must obey the page artifact matrix.
Each page must declare:
- required artifacts
- optional artifacts
- blocked conditions
- partial warning conditions
- whether UI-side rebuild is allowed

The goal is to prevent each page from inventing its own fallback rules.
