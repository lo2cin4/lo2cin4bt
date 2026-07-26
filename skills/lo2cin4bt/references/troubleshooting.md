# Troubleshooting lo2cin4bt 2.1.0

Use this for current browser-first lo2cin4bt.

## Fast Health Check

```bash
python scripts/doctor.py
```

Expected:

- Python 3.12+.
- Required Python packages available.
- Node and npm available unless `--skip-node` is used.
- `plotter/web/package-lock.json` exists.

## App Does Not Open

Check:

1. Did `python main.py` start without error?
2. Is the URL exactly `http://127.0.0.1:2424/`?
3. Is port 2424 already occupied?
4. Did `plotter/web/dist/` get built?

Fix:

```bash
cd plotter/web
npm ci
npm run build
cd ../..
python main.py
```

Windows port check:

```powershell
Get-NetTCPConnection -LocalPort 2424
```

## Frontend Looks Blank Or Old

- Rebuild `plotter/web`.
- Hard refresh the browser.
- Restart `python main.py`.
- Confirm API health: `http://127.0.0.1:2424/api/app/health`.

## No Configs In Run Center

Possible causes:

- The workspace is a clean public clone and no example configs have been initialized yet.
- The local example files were intentionally deleted from `workspace/runs/` or `workspace/wfa/`.
- Config JSON is invalid or does not use current supported contract.

Fix:

- Ask the AI agent to initialize supported example configs from bundled contracts into the ignored workspace folders, then refresh Run Center.
- Run `python scripts/doctor.py` if the folders are still empty after launch.
- If the user intentionally deleted the examples, ask Codex to create a supported `strategy_run` config using `indicator-recipes.md`.
- Refresh Run Center after saving the config.

## Run Completes But Metrics Page Missing

Check:

- `outputs/app/artifact_manifests/{run_id}.json`
- `outputs/app/run_snapshots/{run_id}/`
- `outputs/app/chart_payloads/{run_id}/`
- Run Center batch result status.

If artifacts are from an older contract and missing required fields, rerun with the current app.

## Deleted Config Still Shows Somewhere

- Deleted workspace config disappears from Run Center after refresh.
- Completed results remain in `outputs/app/` until local outputs/registry entries are cleaned.
- Do not delete user evidence without explicit approval.

## yfinance Or Internet Data Fails

- Check network access.
- Try again later.
- Confirm symbol spelling.
- Use file-backed data if public provider is unavailable.

## Binance/Coinbase Symbol Problem

- Binance examples usually use symbols such as `BTCUSDT`.
- Coinbase examples may use product style such as `BTC-USD`.
- Do not mix provider-specific symbol conventions without an adapter.

## FUTU / IBKR Does Not Work

These are not first-run features. They require:

- Optional Python packages from `requirements-brokers.lock`.
- Local gateway app.
- API permissions.
- Host/port config.
- Market-data entitlements.

If the gateway is missing, call it an environment issue, not a strategy config issue.

## Result Looks Wrong

Inspect in order:

1. Selected config.
2. Normalized config/snapshot.
3. Provider, frequency, calendar, timezone.
4. Data health and effective start.
5. Universe/provenance and missing assets.
6. Benchmark symbol/provider.
7. Costs and slippage.
8. Equity artifact.
9. Holdings, rebalance audit, and rebalance trades.
10. WFA selected rows or Parameter Matrix candidates.
11. App payload JSON.
12. Frontend component.

Never decide from a screenshot alone.
