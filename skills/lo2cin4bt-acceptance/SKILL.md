---
name: lo2cin4bt-acceptance
description: Repo-local acceptance skill for lo2cin4bt. Use when checking whether a deliverable satisfies the user request, repo contracts, skills, public/GitHub boundary, tests, stale docs, forbidden paths, and no-trading-advice disclaimers.
version: 2.2.0
status: active
category: auditor
use_when:
  - "Checking a lo2cin4bt deliverable against user requirements and current contracts."
  - "Reviewing architecture drift, stale docs, output boundaries, or public claims."
---

# lo2cin4bt Acceptance Skill

## Purpose

Provide skill-owned acceptance for user intent, shared Rust architecture, artifacts, frontend contracts, safety, and evidence.

## Use When

Use before closing meaningful lo2cin4bt code, config, agent, skill, Lecture, frontend, or release work.

## Inputs

- User request and latest corrections.
- Changed files, current contracts, tests, payloads, artifacts, and reports.

## Workflow

Apply the Checklist below, run risk-matched validation, return pass/revise/block, and reject structural drift rather than accepting exceptions.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
Acceptance review checks deliverable quality only. It does not endorse any strategy and is not investment advice, trading advice, financial advice, or an instruction to trade.

## Required Reads
- The user request and latest corrections
- `skills/lo2cin4bt/references/runtime-architecture.md`
- The agent and skill contracts touched by the task
- Relevant README/doc/config/test artifacts
- `skills/lo2cin4bt/references/readme-acceptance-criteria.md` when README or public docs are touched
- `skills/lo2cin4bt/references/lo2cin4-agent-contract.md` when AI agent discoverability is touched
- `skills/lo2cin4bt/references/workspace-and-github-boundary.md` when GitHub/public boundaries are touched
- `skills/lo2cin4bt/references/strategy-identity-and-summary.md` when strategy config, selector, summary, or result payloads are touched
- Public GitHub boundary docs when release readiness is involved

## Checklist
- user wording followed
- latest correction supersedes older instructions
- no scope drift
- required agents/skills/references exist
- no unsupported strategy/config claim
- no public performance overclaim
- no live trading, broker action, external account action, or production deployment
- docs and skills agree on paths and names
- evidence is current
- tests or scans match change risk
- new strategy configs use a specific four-part `platform.display_label` with no `Backtest`, workflow, date, or run id prefix
- result-selector and strategy-logic previews are derived from the executable config, not copied from user prose or `metadata.notes`
- app-managed run identity and Strategy Logic agree with the saved `strategy_run.json` snapshot
- Python remains control-plane only and all result-changing calculations use the shared Rust engine, mandatory Rust validator, Rust metrics, and PlotBundle contract
- WFA and rolling validation remain explicit `validation_workflow/` workflows; Parameter Matrix is not mislabeled as validation
- `plotter/web` is the only frontend and the app serves it on port `2424`
- the resolved Git root is the product `Repo` directory itself
- product `origin` fetch/push URLs exactly match the approved GitHub repository
- product `main` is clean, contains the fetched remote history, and is neither
  behind nor diverged from `origin/main`
- when nested in Company, the parent tracks only the product commit as a Git
  submodule, has no product remote, and never pushes product code
- candidate and tracked-index release guards pass; otherwise publishing is
  blocked without exceptions

## Verdicts
- `pass`
- `revise`
- `block`

## Output Format
```text
response_language:
acceptance_verdict:
requirements_checked:
evidence_checked:
gaps:
scope_drift:
forbidden_or_out_of_scope_items:
required_followup_gate:
repo_agent_report_path:
not_trading_advice_notice:
```

## Outputs

- Acceptance verdict with requirements, evidence, gaps, blockers, and required follow-up.

## Path Rules

- Inspect product evidence under the current `<repo-root>`.
- Write acceptance evidence to the PM-assigned repo-local or Company report root.
- Do not move ignored runtime outputs into Git-tracked source paths.

## Validation

```powershell
uv run --locked --exact --group dev python -m pytest tests/test_agent_skill_lecture_alignment.py tests/test_strategy_run_config.py tests/test_app_api_payloads.py -q
```

Pass criteria: user requirements and current architecture agree, selected tests pass, and no unsupported claim or retired path remains.

Fail action: return `revise` or `block` with exact evidence; never waive a result-integrity or safety failure.

## Report Requirements

Record requirements checked, evidence paths, commands, verdict, scope drift, residual risks, and the next required owner.
