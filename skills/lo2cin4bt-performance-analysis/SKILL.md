---
name: lo2cin4bt-performance-analysis
description: Repo-local performance analysis skill for lo2cin4bt. Use when explaining generated metrics, equity curves, drawdowns, trades, rebalances, costs, slippage, benchmarks, Parameter Matrix, WFA, missing fields, and claims that require independent quantitative risk review.
version: 2.2.0
status: active
category: reference-backed
use_when:
  - "Explaining completed lo2cin4bt metrics, plots, trades, portfolio evidence, or WFA results."
  - "Auditing missing frontend fields or claims against generated artifacts."
---

# lo2cin4bt Performance Analysis Skill

## Purpose

Explain only generated, canonical result evidence and distinguish absent, not-applicable, matrix, WFA, benchmark, cost, and risk claims.

## Use When

Use for completed run analysis, frontend data completeness, benchmark comparison, risk interpretation, and public wording review.

## Inputs

- Exact run id and immutable run snapshot.
- Registry, artifact manifest, chart payloads, AI review pack, canonical result artifacts, and config.
- Runtime architecture, metric dictionary, and quant interpretation risks.

## Workflow

Apply the Frontend Completeness Gate and Procedure below. Trace values to canonical artifacts, never let an empty summary overwrite valid metrics, and state what cannot be concluded.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
Performance analysis explains generated artifacts only. It is not investment advice, trading advice, financial advice, or an instruction to trade.

## Required Reads
- `skills/lo2cin4bt/references/runtime-architecture.md`
- `skills/lo2cin4bt/references/frontend-pages.md`
- `skills/lo2cin4bt/references/metric-dictionary.md`
- `skills/lo2cin4bt/references/payload-contract-map.md`
- `skills/lo2cin4bt/references/quant-interpretation-risks.md`
- The exact config, payload, result folder, or frontend page assigned by the PM agent
- `skills/lo2cin4bt/references/strategy-identity-and-summary.md`

## Audience Rule
Default to a beginner-readable report. Put audit details in a technical appendix unless the user explicitly asks for raw evidence.

Beginner users usually want:

1. one-sentence summary
2. what the strategy did
3. which run is being analyzed
4. data and benchmark caveats
5. main results and benchmark comparison
6. risk and drawdown
7. trade, holding, rebalance, and portfolio evidence
8. cost and slippage evidence
9. frontend data completeness
10. what can and cannot be concluded
11. next research steps

Do not lead with raw field names such as `artifact_scope` or `configs_and_payloads_read` in user-facing reports.

## Writing Style
- Write in a professional, concise, artifact-backed style.
- Do not use rhetorical contrast templates that replace one idea with a dramatic opposing idea.
- Do not add motivational, moralizing, or AI-like commentary.
- Prefer direct caveats, short tables, and traceable numbers.

## Frontend Completeness Gate
Before producing a complete analysis, inspect every available frontend-facing payload and AI-readable pack for the run.

Required roots:

- `outputs/app/run_registry/{run_id}.json`
- `outputs/app/chart_payloads/{run_id}/`
- `outputs/app/ai_review/{run_id}/ai_review_pack.json`
- `outputs/app/artifact_manifests/{run_id}.json`
- `outputs/app/run_snapshots/{run_id}/`

Before marking Backtests Detail missing, check `backtest_result_index.json`. If it has a `backtest_id`, request or generate the detail payload through the local app API/service and then analyze the generated `backtest_detail_*.json`.

Frontend payloads to check when present:

- `metrics_overview_payload.json`
- `parameter_heatmap_payload.json`
- `parameter_matrix_payload.json`
- `backtest_detail_*.json`
- `wfa_dashboard_payload.json`
- `statanalyser_summary_payload.json` only when the optional diagnostic workflow generated it

AI review pack sections to check when present:

- `source_payloads`
- `snapshot_payloads`
- `payload_index`
- `artifact_table_profiles`
- `metric_field_catalog`

Snapshot and artifact evidence to check when present:

- `strategy_run.json`
- `run_config.json`
- `backtest_result_index.json`
- `data_lineage_manifest.json`
- `managed_artifacts/portfolio/*equity*`
- `managed_artifacts/portfolio/*holdings*`
- `managed_artifacts/portfolio/*rebalance*`
- WFA parquet/metadata artifacts

If a frontend page cannot be fully analyzed because its payload is missing, say so plainly:

- `Metrics Overview`: required for any performance summary
- `Backtests Detail`: required for complete single-run drilldown; generate/read it on demand when `backtest_result_index.json` has a `backtest_id`
- `Parameter Matrix`: required only when the run has parameter domains or matrix workflow
- `WFA`: required only for WFA/rolling runs or when discussing WFA, robustness, rolling validation, or OOS behavior; for `single_backtest`, mark it `not applicable`
- `Optional statistical analysis output`: not applicable unless a statanalyser payload exists; there is no dedicated public React page for it

Do not call a requested report complete when required payloads are missing. Use `partial analysis` and list exactly what must be generated, regenerated, or run through a separate workflow.

## Procedure
1. Identify the user-facing run facts from the immutable run snapshot: run id, status, created/completed time, display label, strategy mode, workflow, and config filename. Use the saved display label as identity and derive the plain-language strategy logic from normalized executable fields; never infer behavior from the label alone.
2. Inspect all frontend payloads listed in the Frontend Completeness Gate, including lazy-generated Backtests Detail when available.
3. State data and benchmark caveats before returns.
4. State the metric assumptions when annualized fields are discussed: `metricstracker.time_unit` and `metricstracker.risk_free_rate`; if absent, use the runtime default inference.
5. Explain only generated metrics and fields.
6. Mark missing fields as `not generated` or `not applicable`.
7. For Parameter Matrix, describe candidate ranking, not future performance.
8. For WFA, distinguish in-sample selection from out-of-sample evaluation.
9. List what can be concluded, what cannot be concluded, and what needs independent review by `lo2cin4btTradingRiskReviewAgent`.

## Metric Coverage Contract

- Treat `skills/lo2cin4bt/references/metric-dictionary.md` as the human interpretation contract and the selected run's `metric_field_catalog` as the generated-field inventory.
- Account for every generated metric field in the selected payload: explain it, group it under an explicitly named equivalent field, or state why it is technical metadata rather than a financial metric.
- For each financial metric, retain its value, unit, source artifact, annualization/benchmark assumptions, and misread warning. Never infer a missing value from another panel.
- New Rust or frontend-visible financial metrics require a dictionary entry and coverage-test update in the same change. An undocumented generated metric blocks a claim of complete analysis.
- “Can interpret all metrics” means every metric actually generated by the canonical run contracts. It does not authorize invented values or claims for fields that are not generated or not applicable.

## Analysis Categories
Performance analysis can cover these artifact-backed categories:

1. Run analyzed: run id, config filename, workflow, timestamp, display label, and whether outputs are current.
2. Data and benchmark caveats: provider, symbol mapping, frequency, calendar, timezone, effective start, missing assets, benchmark label, and lineage status.
3. Strategy/result summary: strategy label, generated metrics, equity curve, drawdown, benchmark comparison, and data health warnings.
4. Trade and portfolio evidence: trade rows, holdings, allocation changes, target weights, rebalance audit, asset contribution, turnover, and risk gate events.
5. Cost and slippage evidence: configured transaction costs, slippage assumptions, generated cost drag, gross/net availability, and missing cost fields.
6. Frontend completeness: which app pages have generated payloads and which are missing.
7. Parameter Matrix screening: parameter axes, objectives, ranking config, shortlist rows, cluster/plateau diagnostics, and parameter importance.
8. WFA or rolling validation: train/test window boundaries, selected optimum rows, diagnostic rows, OOS metrics, OOS/IS ratios, portfolio window summary, and truth warnings.
9. Claim gate: statements that are allowed, statements that are blocked, and whether final wording requires `lo2cin4btTradingRiskReviewAgent`.

## What The Analysis Is Not
- It is not a buy/sell recommendation.
- It is not proof that a strategy is profitable or suitable for live trading.
- It is not a replacement for payloads or artifacts; screenshots alone are not enough.
- It does not turn missing fields into zero.
- It does not treat Parameter Matrix ranking as OOS proof.
- It does not treat WFA as a guarantee of future performance.


## Repo-Local Report Output
When ProjectManager asks for a durable repo-local report, write Markdown under `workspace/reports/agents/<agent_name>/YYYY-MM-DD_<short-topic>.md` and return `repo_agent_report_path` in the output packet. Use this for WorkAgent and private snapshot reports; keep Company-level closeout separate unless ProjectManager explicitly asks for it.

## Independent Quantitative Risk Review Triggers
Require quant review before final wording when public docs or user conclusions mention strategy performance, WFA, robustness, overfitting, alpha, tradability, data lineage, cost/slippage impact, or look-ahead risk.

## Default User-Facing Output Format
```text
response_language:
title:
repo_agent_report_path:
not_trading_advice_notice:
one_sentence_summary:
strategy_plain_english:
run_analyzed:
data_and_benchmark_caveats:
main_results:
benchmark_comparison:
risk_and_drawdown:
trade_holding_rebalance_evidence:
cost_slippage_caveats:
frontend_data_completeness:
what_this_can_mean:
what_this_cannot_mean:
next_research_steps:
quant_review_required:
technical_appendix:
```

## Technical Appendix Fields
Use these in the appendix, not as the first section of a beginner report:

```text
artifact_scope:
configs_and_payloads_read:
payloads_read:
snapshots_read:
artifact_profiles_read:
missing_fields:
claims_allowed:
claims_blocked:
```

## Outputs

- Beginner-readable analysis and technical appendix grounded in current artifacts.
- Explicit missing/not-applicable fields, claim gates, and quant-review requirement.

## Path Rules

- Read managed results only from `Repo\outputs\app\` roots for the selected run.
- Reports belong under the assigned repo-local and Company report roots.
- Screenshots are supporting evidence only and never replace payload/artifact checks.

## Validation

```powershell
uv run --locked --exact --group dev python -m pytest tests/test_app_api_payloads.py tests/test_ai_readable_output.py tests/test_agent_skill_lecture_alignment.py -q
```

Pass criteria: values trace to canonical generated evidence, missing fields are not converted to zero, and matrix/WFA claims retain their correct boundaries.

Fail action: label the analysis partial, list the exact missing artifact or failed contract, and block unsupported conclusions.

## Report Requirements

Record run identity, artifacts and payloads read, data/benchmark caveats, generated metrics, missing fields, allowed and blocked claims, validation, and quant-review status.
