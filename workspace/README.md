# lo2cin4bt Workspace

This folder is the local working area for runnable research inputs.

- `runs/` stores strategy run configs shown in Run Center.
- `wfa/` stores rolling validation configs shown in Run Center.
- `datasets/`, `features/`, `indicators/`, `calendars/`, and `strategies/` are local inputs used by examples or custom research.
- `reports/` is for local working notes and agent evidence.

Keep public examples under `backtester/contracts/strategy/examples/` and copy them here only when they should appear in the local app.

App-managed backtest results live under `outputs/app/`. When a specific result
must be removed, delete the whole bundle for that `run_id` together:

- `outputs/app/run_registry/<run_id>.json`
- `outputs/app/stage_status/<run_id>.json`
- `outputs/app/artifact_manifests/<run_id>.json`
- `outputs/app/run_snapshots/<run_id>/`
- `outputs/app/chart_payloads/<run_id>/`
- `outputs/app/ai_review/<run_id>/`

Also remove the matching entry from `outputs/app/latest_runs.json` so the app UI
does not keep stale selectors.
