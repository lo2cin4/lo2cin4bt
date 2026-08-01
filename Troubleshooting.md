# Troubleshooting lo2cin4bt 2.2.1

This is the current browser-first troubleshooting guide. The app runs through
FastAPI + React at `http://127.0.0.1:2424/`.

For AI-assisted recovery, use:

```text
skills/lo2cin4bt/references/troubleshooting.md
```

## First Health Check

```bash
uv run --locked --exact python scripts/doctor.py
```

Expected:

- Python 3.12+.
- Required runtime packages installed.
- Node.js and npm available unless you intentionally skip frontend checks.
- `plotter/web/package-lock.json` exists.

## App Does Not Start Or Homepage Shows 503

1. Confirm dependencies are installed.
2. Rebuild frontend if `plotter/web/dist/` is missing.
3. Launch the app again.

```bash
cd plotter/web
npm ci
npm run build
cd ../..
uv run --locked --exact python main.py
```

Why this happens: `uv run --locked --exact python main.py` serves the React
production build from
`plotter/web/dist/`. A clean GitHub checkout does not include `dist/`, because it
is a generated build artifact. The normal `scripts/setup.ps1` / `scripts/setup.sh`
path creates it automatically unless you use the frontend-skip option.

If you built `dist/` while the app was already running, stop and restart
`uv run --locked --exact python main.py`. The app only mounts `/assets` at
startup, so a server that was
started before `plotter/web/dist/assets/` existed can show a blank white page.

Open:

```text
http://127.0.0.1:2424/
```

Check API health:

```text
http://127.0.0.1:2424/api/app/health
```

## Port 2424 Is Occupied

Windows:

```powershell
Get-NetTCPConnection -LocalPort 2424
```

Stop the old local app process only if it belongs to your lo2cin4bt session.

## Run Center Shows No Configs

Run Center reads local config folders:

- `workspace/runs/`
- `workspace/wfa/`

Public GitHub snapshots may intentionally omit local/user configs. Add examples
from the owner/community channel or create a supported `strategy_run` config
with Codex using `skills/lo2cin4bt/references/indicator-recipes.md`.

## Run Finished But Page Looks Empty

Inspect:

- `outputs/app/artifact_manifests/{run_id}.json`
- `outputs/app/run_snapshots/{run_id}/`
- `outputs/app/chart_payloads/{run_id}/`
- `outputs/app/ai_review/{run_id}/ai_review_pack.json`

If the artifact is from an older version and lacks required fields, rerun the
strategy with the current app instead of treating missing fields as zero.

## Data Provider Fails

- yfinance: check symbol spelling and network access.
- Binance/Coinbase: use provider-specific symbol formats.
- File-backed data: confirm `Time`, `Open`, `High`, `Low`, `Close`, and
  optionally `Volume` are present or mappable.
- FUTU/IBKR: optional market-data gateway work only; it is not part of the
  first run and this app does not place live orders. Missing gateway/account
  permissions are environment issues, not strategy config issues.

## Result Looks Wrong

Do not debug from a screenshot alone. Check in this order:

1. Selected config.
2. Normalized config/snapshot.
3. Provider, calendar, timezone, and benchmark.
4. Data health, effective start, missing assets, and universe provenance.
5. Costs and slippage.
6. Equity, holdings, rebalance audit, and rebalance trades artifacts.
7. Parameter Matrix or WFA payloads only if those workflows were generated.
8. Frontend payload JSON and page component.

See `skills/lo2cin4bt/references/quant-interpretation-risks.md` for result
interpretation traps.
