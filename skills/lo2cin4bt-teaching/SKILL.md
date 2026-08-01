---
name: lo2cin4bt-teaching
description: Repo-local teaching skill for lo2cin4bt. Use when explaining setup, AI usage, repo structure, frontend pages, metrics, README, Lecture Skill, AI Manual Skill, terminology, or beginner next steps from repo evidence only.
version: 2.2.0
status: active
category: workflow
use_when:
  - "Explaining lo2cin4bt setup, architecture, frontend pages, metrics, or learning material."
  - "Updating beginner-facing Lecture or AI manuals from current repo evidence."
---

# lo2cin4bt Teaching Skill

## Purpose

Teach the current lo2cin4bt product without reviving retired Python architecture or overstating generated evidence.

## Use When

Use for onboarding, architecture explanations, Lecture updates, page walkthroughs, and terminology.

## Inputs

- Learner goal and language.
- Current runtime architecture, docs, tests, configs, payloads, and frontend pages.

## Workflow

Follow the Procedure below. Separate implemented behavior from general theory, cite current evidence, and end with one concrete next step.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
Teaching is educational only. It is not investment advice, trading advice, financial advice, or an instruction to trade.

## Required Reads
Load only what the lesson needs:

- `skills/lo2cin4bt/references/runtime-architecture.md`
- `README.md`
- `docs/ai/AI_MANUAL_SKILL.md`
- `docs/ai/AI_SKILL_LECTURE_GUIDE.md`
- `skills/lo2cin4bt/references/first-run.md`
- `skills/lo2cin4bt/references/frontend-pages.md`
- `skills/lo2cin4bt/references/metric-dictionary.md`
- `skills/lo2cin4bt/references/troubleshooting.md`

For reader-facing Traditional Chinese Lecture copy:

- Write in plain Traditional Chinese for readers who are new to backtesting.
- Introduce technical terms as `中文（English original）` on first use.
- Prefer direct verbs and concrete examples; remove optional adverbs and filler.
- Preserve config keys, commands, paths, metric names, and technical facts exactly.
- Keep this public skill self-contained. Do not depend on Company-local skills or
  expose local workstation paths.

## Current Timeline / Rust Teaching
- Teach current strategy execution as one plain flow:
  `strategy_run config -> Python normalization and data loading -> persistent Rust engine -> mandatory Rust result validation -> Rust metrics and PlotBundle -> outputs/app artifacts -> plotter/web on port 2424`.
- Explain profiles as simpler authoring forms that all compile into this same engine. Single-asset strategies are one-asset portfolios; they are not a separate backtester.
- Explain WFA and rolling validation as explicit `validation_workflow/` wrappers. They are not automatic stages for every backtest, and Parameter Matrix is candidate expansion rather than validation.
- For new examples, prefer `fill_model.timing = "timeline"` and explicit `actions[]`:
  `offset_bars` says how many bars after the signal/rebalance trigger, `price` is `open` or `close`, and `action` is `enter`, `exit`, `flatten`, or `set_target_weights`. Use `0` only for an action known before the session. Current-bar indicators, rankings, and signals must execute on a later bar through `offset_bars: 1` or `signal_close_for_next_bar`.
- Current public examples should use timeline `actions[]` directly and describe
  one shared Rust engine. Vector precomputation and sequential portfolio
  accounting are internal stages of that engine, not separate public routes.
- Metrics Overview, Parameter Matrix, Backtests, and WFA read completed app-managed runs from `outputs/app/latest_runs.json`; they do not list every config in `workspace/runs/` or `workspace/wfa/`. If configs exist but `latest_runs.json` is empty, the correct explanation is "no current completed runs yet", not "the configs cannot be read".
- If a user asks why old results disappeared after config/schema work, first check `/api/app/run-center/configs`, `/api/app/metrics/runs`, `/api/app/wfa/runs`, and `outputs/app/latest_runs.json`.

## Procedure
1. Identify the learner goal.
2. Name the repo evidence used.
3. Explain implemented lo2cin4bt behavior separately from general trading theory.
4. Define technical terms at first use.
5. Mention what a page or artifact can prove and cannot prove.
6. For complete beginners, provide a learning goal, prerequisites, expected
   result, common problems, and a copyable checklist.
7. When presenting hands-on work, separate the manual route from the
   Agent/Skill route. Explain ProjectManager, WorkAgent, and Skill before using
   those names.
8. End with one concrete next file, page, command, or artifact.
9. For Lecture copy, use documentation mode, keep technical facts and names,
   and remove optional adverbs. Record any meaning-critical exception; the
   expected exception list is empty.


## Repo-Local Report Output
When ProjectManager asks for a durable repo-local report, write Markdown under `workspace/reports/agents/<agent_name>/YYYY-MM-DD_<short-topic>.md` and return `repo_agent_report_path` in the output packet. Use this for WorkAgent and private snapshot reports; keep Company-level closeout separate unless ProjectManager explicitly asks for it.

## Stop Conditions
Stop when the request needs trading advice, unsupported repo behavior, unavailable private credentials, paid data access, or old artifacts that conflict with current contracts.

## Output Format
```text
response_language:
lesson_goal:
repo_evidence:
plain_explanation:
terms:
not_generated_or_not_applicable:
next_step:
repo_agent_report_path:
not_trading_advice_notice:
```

## Outputs

- Beginner-readable, evidence-backed lesson or updated teaching artifact.
- Clear `not generated`, `not applicable`, and safety boundaries.

## Path Rules

- Lecture source: `<repo-root>/Lecture/`.
- Product docs and skills remain under the Repo root.
- Teaching reports belong under the assigned repo-local and Company report roots.

## Validation

```powershell
uv run --locked --exact --group dev python -m pytest tests/test_agent_skill_lecture_alignment.py tests/test_lecture_contracts.py -q
```

Pass criteria: local links resolve, UTF-8 is valid, lessons teach the shared
Rust route and explicit validation workflows, and the beginner learning map,
glossary, two operation routes, checklist, copy controls, and progress
components remain present. Every changed Lecture HTML page must also follow the
plain-language rules above without optional adverbs or internal environment
references.

Fail action: correct the teaching source; never document a compatibility exception for retired behavior.

## Report Requirements

Record lessons changed, source evidence, terminology decisions, validation result, and any feature that remains optional or not implemented.
