---
name: lo2cin4bt
description: Operate, teach, and troubleshoot the lo2cin4bt quantitative research/backtesting repo. Use when Codex needs to install or launch lo2cin4bt, create or review strategy run or WFA configs, run local backtests, Parameter Matrix, WFA or rolling validation, explain frontend metrics/artifacts/AI-readable packs, or recover beginner setup/runtime issues while respecting repo-only evidence and no-live-trading boundaries.
version: 2.2.0
status: active
category: workflow
use_when:
  - "Operating, teaching, reviewing, or troubleshooting the lo2cin4bt repository."
  - "A task must follow the shared Rust backtest architecture and canonical artifact contracts."
---

# lo2cin4bt

## Purpose

Provide the canonical project operating rules for local strategy authoring, Rust-backed simulation, explicit validation workflows, artifact interpretation, teaching, and troubleshooting.

## Use When

Use for any lo2cin4bt product task. Pair it with the narrower task skill selected by ProjectManager.

## Inputs

- PM task packet and requested response language.
- Current config, source code, tests, payloads, artifacts, or frontend page.
- `references/runtime-architecture.md` and the task-specific references.

## Workflow

1. Read the canonical runtime architecture and classify the task.
2. Load the exact task skill and inspect current repo evidence.
3. Perform the smallest structural change or operation that satisfies the request.
4. Run skill-owned validation and record evidence.
5. Close with a semantic summary and durable report when required.

Use this skill as the repo-local operating guide for lo2cin4bt. Keep answers grounded in the repository, generated artifacts, and current app payloads. Label outside finance or engineering context as external context or AI inference.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
This skill supports local research, education, and software operation only. It is not investment advice, trading advice, financial advice, or an instruction to trade.

## Safety Rules

- Do not deploy, place trades, enable live trading, move funds, change positions, or change external accounts. Broker/exchange accounts may be used only for read-only market-data access when the user asks for that setup.
- Do not invent config fields, strategy modes, metrics, provider behavior, WFA evidence, or UI features.
- Treat `strategy_run`, generated normalized snapshots, app payloads, and tests as the source of truth.
- If a result artifact is from an older contract and lacks fields required by current validation, call it stale and rerun with the current version instead of mixing old and new evidence.
- For public/GitHub guidance, assume runtime outputs, local configs, datasets, secrets, and broker credentials are not committed.

## Read Order

1. `AGENTS.md`
2. `README.md`
3. This `SKILL.md`
4. `references/runtime-architecture.md`
5. Load only the relevant reference below.
6. Inspect the actual config, payload, artifact, frontend component, or test before making a claim.

## Reference Map

- Public AI operator/teacher contract: `references/lo2cin4-agent-contract.md`
- Runtime architecture and language boundary: `references/runtime-architecture.md`
- Agent model: `agents/lo2cin4bt_PM.agent.md` routes one `agents/lo2cin4btWorkAgent.agent.md` through the matching task skills; use `agents/lo2cin4btTradingRiskReviewAgent.agent.md` only for independent quant-risk review
- Task skills: `skills/lo2cin4bt-pm/`, `skills/lo2cin4bt-teaching/`, `skills/lo2cin4bt-strategy-builder/`, `skills/lo2cin4bt-backtesting/`, `skills/lo2cin4bt-acceptance/`, and `skills/lo2cin4bt-performance-analysis/`
- Strategy authoring template and Strategy Building Blocks verdict flow: `references/strategy-authoring-template.md`
- README release acceptance criteria: `references/readme-acceptance-criteria.md`
- Beginner install and first successful run: `references/first-run.md`
- Supported indicator recipes and first-run strategy examples: `references/indicator-recipes.md`
- Complete computed-field operation catalog: `references/computed-field-building-blocks.md`
- Strategy config sections and field choices: `references/strategy-config-fields.md`
- Frontend page walkthroughs and what each page can/cannot prove: `references/frontend-pages.md`
- Metric and field dictionary: `references/metric-dictionary.md`
- API, chart payload, artifact, and AI review pack map: `references/payload-contract-map.md`
- Quant interpretation risks and evidence boundaries: `references/quant-interpretation-risks.md`
- Current 2.0 troubleshooting: `references/troubleshooting.md`
- Contracts and schema index: `references/contracts-index.md`
- Workspace, GitHub upload, ignored output, and privacy boundary: `references/workspace-and-github-boundary.md`
- Done definition for this skill and teaching coverage: `references/acceptance-criteria.md`

## Core Workflows

### lo2cin4 Operator / Teacher

When a user says "you are lo2cin4" or asks the AI to develop a strategy, load `agents/lo2cin4bt_PM.agent.md`, `agents/lo2cin4btWorkAgent.agent.md`, `skills/lo2cin4bt-pm/SKILL.md`, `references/runtime-architecture.md`, and `references/lo2cin4-agent-contract.md`. The WorkAgent loads the matching task skills before acting; do not create or route through family-specific agents. Strategy creation starts with a capability verdict before writing configs. Teaching must stay grounded in repo files, tests, docs, configs, artifacts, or lecture pages.

### New User Setup

Read `references/first-run.md`. Walk the user from clone or ZIP download to
`uv run --locked --exact python main.py`, `http://127.0.0.1:2424/`,
`uv run --locked --exact python scripts/doctor.py`, and one completed local
run. A clean public clone may have empty ignored `workspace/runs/` and
`workspace/wfa/` folders; when the user wants runnable defaults, initialize
supported example configs from bundled contracts into those folders. WFA
configs should reference strategy configs with explicit
`workspace/runs/<strategy-config>.json` paths.

### Strategy Creation

Read `references/strategy-authoring-template.md`, `references/strategy-config-fields.md`, `references/strategy-identity-and-summary.md`, `references/indicator-recipes.md`, and `references/computed-field-building-blocks.md`. First decide whether existing Strategy Building Blocks cover the request, then produce a capability verdict:

```text
supported | needs_clarification | unsupported_needs_new_building_block
```

Only write a runnable config after provider, typed execution/decision `BarSpec`, session calendar/timezone, universe, benchmark, entry/exit or allocation rules, fill timing, cost/slippage, workflow, and parameter domains are known and supported by current repo contracts. The AI must also write a specific `platform.display_label`, then derive the result-selector and strategy-logic previews from the completed config. For unsupported or undefined strategies, write no runnable config until building block code, tests, and quant safety metadata exist.

### Result Explanation

Read `references/frontend-pages.md`, `references/metric-dictionary.md`, and `references/quant-interpretation-risks.md`. Explain results in this order:

1. Config intent and workflow.
2. Data health, universe/provenance, benchmark, and truth warnings.
3. Strategy performance and risk metrics.
4. Portfolio holdings, rebalance, contribution, costs, and risk gates.
5. Parameter Matrix or WFA evidence only when those artifacts exist.
6. Missing or unavailable fields as `not generated`, never as zero.

### Troubleshooting

Read `references/troubleshooting.md`. Check setup, `scripts/doctor.py`, frontend build, port `2424`, Run Center config discovery, app registry, current payload JSON, and then source artifacts. Do not judge from screenshots alone.

When the user wants a result visible in Metrics Overview, Parameter Matrix, Backtests,
Parameter Matrix, or WFA, the run must be app-managed. Prefer Run Center or the
app runtime API. Canonical artifacts belong to the run-scoped
`outputs/app/run_snapshots/<run_id>/managed_artifacts/` tree. Standalone tools
must use an explicit caller-owned output directory and must not recreate
module-specific folders under `outputs/`.

## Closeout Checklist

- Cite files, configs, payload paths, tests, or artifacts used as evidence.
- State whether the action is local research only.
- State any stale artifact, missing output, benchmark mismatch, or survivorship/provenance risk.
- When teaching, give the next exact UI page or file the user should inspect.

## Outputs

- Implemented local change, runnable config, diagnosis, teaching response, or artifact-backed analysis.
- Validation evidence and any required repo-local or Company report.

## Path Rules

- Product source and product skills: the current `<repo-root>`.
- Local runtime evidence: `Repo\outputs\app\` and ignored `Repo\workspace\` paths.
- Repo-local reports: `Repo\workspace\reports\agents\<agent_name>\`.
- Reports and memory: the PM packet's `repo_agent_report_path`.
- Never write generated runtime output into source module folders.

## Validation

```powershell
uv run --locked --exact --group dev python -m pytest tests/test_agent_skill_lecture_alignment.py tests/test_strategy_run_config.py tests/test_app_api_payloads.py -q
```

Pass criteria: all selected tests pass, the current runtime architecture is cited, and no retired agent or runtime path is introduced.

Fail action: stop closeout, report the failing contract or test, and repair the shared structure rather than adding a case-specific exception.

## Report Requirements

Record scope, architecture decisions, changed paths, validation commands and results, unresolved risks, and a semantic before/after summary. Append reusable decisions to the assigned `memory.jsonl` only after evidence exists.
