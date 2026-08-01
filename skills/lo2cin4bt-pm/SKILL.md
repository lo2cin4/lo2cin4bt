---
name: lo2cin4bt-pm
description: Repo-local PM routing skill for lo2cin4bt. Use when ProjectManager must classify a request, assign one lo2cin4btWorkAgent, select the required task skills, enforce the current runtime architecture and safety boundaries, and identify out-of-scope work.
version: 2.2.0
status: active
category: workflow
use_when:
  - "ProjectManager needs to classify and route a lo2cin4bt task."
  - "A task requires selected skills, evidence, validation, and report ownership."
---

# lo2cin4bt PM Routing Skill

## Purpose

Route all ordinary lo2cin4bt work to one WorkAgent with explicit skills, evidence, validation, and safety boundaries.

## Use When

Use before assigning any lo2cin4bt implementation, teaching, backtesting, acceptance, or analysis task.

## Inputs

- User request and latest correction.
- Project workspace manifest, current reports, and memory.
- Current runtime architecture and available project-local skills.

## Workflow

Apply the Request Classification and Routing Procedure below, then issue one bounded task packet to `lo2cin4btWorkAgent`. Add independent trading-risk review only when its trigger is present.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
Every lo2cin4bt agent and skill is for local research, education, and software operation only. Do not present output as investment advice, trading advice, financial advice, or an instruction to trade.

## Required Reads
1. `agents/lo2cin4bt_PM.agent.md`
2. `agents/lo2cin4btWorkAgent.agent.md`
3. `skills/lo2cin4bt/SKILL.md`
4. `skills/lo2cin4bt/references/runtime-architecture.md`
5. `skills/lo2cin4bt/references/computed-field-building-blocks.md` for calculation or strategy-capability work
6. `docs/ai/AI_MANUAL_SKILL.md`
7. `docs/ai/AI_SKILL_LECTURE_GUIDE.md`
8. The task skills selected for this request

## Text Encoding Guardrail
Do not judge Chinese documentation by terminal rendering alone. PowerShell,
console fonts, tool transcripts, and code pages can display valid UTF-8
Traditional Chinese as mojibake.

If a Chinese file appears garbled, verify with byte-level evidence before making
a public-readiness claim:

```powershell
@'
from pathlib import Path
p = Path("docs/ai/AI_MANUAL_SKILL.md")
text = p.read_text(encoding="utf-8")
print("replacement_count", text.count("\ufffd"))
for i, line in enumerate(text.splitlines()[:8], 1):
    print(i, line.encode("unicode_escape").decode("ascii"))
'@ | python -
```

Rules:

- `replacement_count == 0` means UTF-8 decoding did not find replacement
  characters in file content.
- A garbled terminal preview is not enough evidence to call a file corrupted.
- Report `encoding_valid` separately from `terminal_rendering_unreliable`.
- Use `unicode_escape`, code points, or a browser/editor preview when public
  display quality matters.

## Request Classification
- `teaching`: setup, concepts, frontend pages, README, lecture, AI manual
- `strategy_building`: strategy idea, config, indicator, signal, allocation, rebalance, WFA plan
- `backtesting`: local run, Parameter Matrix, WFA, Run Center, artifact, screenshot, troubleshooting, and deleting all app-managed traces for a specific `run_id` through the registry cleanup helper dry run before deletion
- `acceptance`: final check against request, contracts, public boundary, evidence
- `performance_analysis`: metrics, charts, trades, rebalances, costs, slippage, WFA, claims
- `out_of_scope`: live trading, broker order, fund movement, account setting, production deploy, legal/tax/financial advice

## Routing Procedure
1. Summarize the user request in one sentence.
2. Assign one `lo2cin4btWorkAgent`.
3. Name every exact task skill the WorkAgent must read.
4. List evidence needed before acting.
5. Block unsupported or out-of-scope work.
6. For strategy/data/WFA/cost/slippage/look-ahead/result interpretation, require quant review before final claims.
7. For runtime code changes, require a bounded implementation patch plus tests; keep Python in the control plane and result-changing calculations in Rust as defined by `runtime-architecture.md`.
8. For every new or revised strategy config, require the Strategy Builder to apply `skills/lo2cin4bt/references/strategy-identity-and-summary.md`. The returned packet must include config-derived `result_selector_preview` and `strategy_logic_preview`; do not accept a name or summary copied only from user prose.
9. For new calculations, require evidence that the operation is present in the registry, both request schemas, the Rust operation enum, the shared computed-field runtime, and tests. Reject a config-only claim when any interface is missing.

## Host Tool Setup Policy
lo2cin4bt follows a developer-style GitHub setup model:

- The repo contains source code, configs, `pyproject.toml`, `uv.lock`, scripts,
  docs, skills, and
  tests.
- The repo must not bundle Python, Node.js, Rust, `.venv/`, `node_modules/`,
  Cargo registry caches, or Rust `target/` build output.
- Local setup may create `.venv/`, `plotter/web/node_modules/`, `outputs/`, and
  Rust `target/`; these are disposable local artifacts and must stay ignored.

When setup or first-run work is requested:

1. Run or request `uv run --locked --exact python scripts/doctor.py` before
   guessing.
2. Require uv 0.11.32 and run `uv sync --locked`; use `--group dev` or
   `--group brokers` only for those declared profiles.
3. If Python is missing or too old, let uv install/select the Python 3.12
   version declared by `.python-version`; do not introduce another route.
4. If Node is missing or too old, help install or point to a host Node location,
   available on `PATH`, `LO2CIN4BT_NODE_HOME`, or `NODE_HOME`.
5. If Rust is missing or too old, help install Rust 1.96.0 through rustup or
   point `LO2CIN4BT_RUST_HOME` to a host-managed directory outside the repo.
6. Rerun `uv run --locked --exact python scripts/doctor.py` after setup
   changes.
7. Do not copy host runtimes, package caches, or build output into the repo to
   make setup pass.

## GitHub Publishing Gate

For any release, mirror, or GitHub push request:

1. Read `skills/lo2cin4bt/references/workspace-and-github-boundary.md`.
2. Resolve the current Git root exactly to the product `Repo` directory.
3. Require the approved GitHub URL as an explicit task input and verify both
   product `origin` URLs against it.
4. Require a clean product `main`, fetched remote history, no behind/diverged
   state, and passing candidate/tracked release guards.
5. When nested in Company, verify that the parent tracks `Repo` only as a Git
   submodule. Never add a product remote to Company or push from Company.
6. If any gate cannot be proven, route the task as `blocked`; do not approve a
   manual copy, external-clone publisher, subtree push, force push, or exception.

## Output Format
```text
response_language:
request_classification:
selected_agent: lo2cin4btWorkAgent
skills_to_read:
scope:
evidence_required:
out_of_scope_items:
next_step:
repo_agent_report_path:
not_trading_advice_notice:
```

## Outputs

- PM routing packet with one selected agent, selected skills, evidence, validation, scope, and report path.
- Explicit escalation or out-of-scope decision when required.

## Path Rules

- Treat the Git root containing this skill as `<repo-root>`.
- Read product skills from `<repo-root>/skills/`.
- Use the PM task packet's `repo_agent_report_path` for reports; do not invent
  an absolute host path.

## Validation

```powershell
uv run --locked --exact --group dev python -m pytest tests/test_agent_skill_lecture_alignment.py -q
```

Pass criteria: routing names one `lo2cin4btWorkAgent`, selects exact skills, and contains no retired family-specific agent route.

Fail action: return the packet for revision; do not route through a fallback agent or global skill.

## Report Requirements

Record classification, selected skills, accepted scope, evidence requirements, validation owner, specialist-review decision, and final acceptance state.
