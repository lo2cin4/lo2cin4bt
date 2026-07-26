---
name: lo2cin4bt-backtesting
description: Repo-local backtesting skill for lo2cin4bt. Use when running or troubleshooting local strategy_run, Parameter Matrix, WFA, rolling validation, Run Center discovery, frontend startup, payload refresh, screenshots, or generated artifacts.
version: 2.1.0
status: active
category: workflow
use_when:
  - "Running or troubleshooting local backtests, Parameter Matrix, WFA, or rolling validation."
  - "Checking app-managed artifacts, frontend payloads, screenshots, or run cleanup."
---

# lo2cin4bt Backtesting Skill

## Purpose

Operate and diagnose the one app-managed backtest route from normalized config through the persistent Rust engine to validated canonical artifacts and port 2424.

## Use When

Use for local execution, runtime failures, performance checks, output cleanup, and frontend visibility of completed runs.

## Inputs

- Exact config and requested workflow.
- Provider/data environment, runtime logs, registry, snapshots, manifests, and payloads.
- Current runtime architecture and expected result contract.

## Workflow

Use the Pre-Run Checklist and Procedure below. Reproduce before diagnosing, keep every strategy on the shared engine, and verify app/API/frontend evidence end to end.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
Backtests are local research artifacts only. They are not investment advice, trading advice, financial advice, or instructions to trade.

## Required Reads
- `skills/lo2cin4bt/references/runtime-architecture.md`
- `skills/lo2cin4bt/references/first-run.md`
- `skills/lo2cin4bt/references/troubleshooting.md`
- `skills/lo2cin4bt/references/payload-contract-map.md`
- `skills/lo2cin4bt/references/frontend-pages.md`
- `skills/lo2cin4bt/references/strategy-identity-and-summary.md`
- The exact config assigned by the PM agent

## Pre-Run Checklist
1. Confirm config path exists.
2. Confirm schema version and workflow.
3. Confirm provider, symbol, frequency, calendar, and benchmark.
4. Confirm costs and slippage are explicit or intentionally defaulted.
5. Confirm `metricstracker.time_unit` and `metricstracker.risk_free_rate`; defaults are 252 for traditional daily assets, 365 for crypto, and 0.04 risk-free rate.
6. Confirm output path is local.
7. Confirm no live trading or broker order path is involved.
8. Confirm `platform.display_label` follows the strategy identity contract and agrees with `universe.symbols`, `data.provider`, and the executable strategy concept.

## Procedure
1. Run the smallest relevant validation first.
2. Use the single public route: normalized `strategy_run` -> persistent Rust engine -> mandatory Rust result validator -> Rust metrics/PlotBundle -> app-managed artifacts and API payloads. Do not revive a strategy-family producer or Python result calculator.
3. Use repo commands from docs/tests; do not invent commands.
4. If the user expects a result to appear in Run Center, Metrics Overview,
   Backtests, Parameter Matrix, or WFA pages, run through the app runtime
   (`python main.py` + Run Center/API) so `outputs/app/latest_runs.json`,
   run registry, artifact manifests, snapshots, and chart payloads are written.
5. Write canonical artifacts into the app-managed run snapshot. Standalone
   tools must receive an explicit caller-owned output directory and must not
   recreate module-specific folders under `outputs/`. Script-only runs are
   diagnostic artifacts, not completed app-managed runs.
6. Record command, config path, stdout/stderr summary, artifact paths, registry
   paths, and payload paths.
7. If frontend is involved, verify `/api/app/metrics/runs`,
   `outputs/app/latest_runs.json`, and the expected page payloads are visible.
   Confirm the selector has `date | assets | strategy concept | workflow | run id`
   and the Strategy Logic panel was projected from the saved
   `outputs/app/run_snapshots/<run_id>/strategy_run.json`.
8. If artifacts are stale, rerun with current config instead of mixing old and new outputs.

## Cleanup App-Managed Run Traces
When the user asks to remove all traces of one completed or canceled run, do
not make them remember output folders. Use the registry cleanup path:

```bash
python scripts/cleanup_app_run.py <run_id>
python scripts/cleanup_app_run.py <run_id> --yes
```

The first command is a dry run. The `--yes` command removes the app-managed
registry entry, artifact manifest, stage status, run snapshot folder, chart
payload folder, AI review folder, screenshot bundle folder, and the matching
`latest_runs.json` row.
Do not delete `workspace/runs/` or `workspace/wfa/` unless the user explicitly
asks to remove runnable configs too.

## Repo-Local Report Output
When ProjectManager asks for a durable repo-local report, write Markdown under `workspace/reports/agents/<agent_name>/YYYY-MM-DD_<short-topic>.md` and return `repo_agent_report_path` in the output packet. Use this for WorkAgent and private snapshot reports; keep Company-level closeout separate unless ProjectManager explicitly asks for it.

## Output Format
```text
response_language:
run_goal:
configs_used:
commands_run:
artifacts_created_or_read:
payloads_checked:
failure_recovery:
remaining_blockers:
quant_review_required:
repo_agent_report_path:
not_trading_advice_notice:
```

## Outputs

- Completed or diagnosed app-managed run with traceable artifacts and payloads.
- Cleanup dry-run/deletion evidence when explicitly requested.

## Path Rules

- Runnable configs: ignored `Repo\workspace\runs\` and `Repo\workspace\wfa\`.
- Managed outputs: `Repo\outputs\app\` only.
- Product frontend: `Repo\plotter\web\`, served by the app on port `2424`.

## Validation

```powershell
python -m pytest tests/test_engine_request_contract.py tests/test_rust_accounting_golden.py tests/test_app_runtime_smoke.py tests/test_agent_skill_lecture_alignment.py -q
```

Pass criteria: selected tests pass and a completed run can be traced from snapshot through accepted result, metrics/PlotBundle, registry, API, and frontend payload.

Fail action: keep the run failed, preserve evidence, diagnose the shared boundary, and do not add a strategy-specific bypass.

## Report Requirements

Record config, command, timings, run id, stage failure or success, artifacts, API checks, validation, and the structural root cause of any repair.
