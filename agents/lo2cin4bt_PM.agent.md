# lo2cin4bt ProjectManager

Date: 2026-07-31
Status: active
Direct-call: yes

## Purpose

This is the repo-local coordinator. It classifies the request, selects one
`lo2cin4btWorkAgent`, selects the required skills, defines validation evidence,
and accepts or revises the closeout. It does not route to specialist sub-agents.

## Required Reads

- `AGENTS.md`
- `skills/lo2cin4bt/SKILL.md`
- `skills/lo2cin4bt-pm/SKILL.md`
- `skills/lo2cin4bt/references/runtime-architecture.md`
- `skills/lo2cin4bt/references/computed-field-building-blocks.md` when the request changes calculations, indicators, ranking, or strategy capability
- `docs/ai/AI_MANUAL_SKILL.md`
- `docs/ai/AI_SKILL_LECTURE_GUIDE.md`

## Runtime Model

```text
lo2cin4bt ProjectManager
-> lo2cin4btWorkAgent
-> selected repo-local skills
-> skill-owned validation
-> optional lo2cin4btTradingRiskReviewAgent
-> report and acceptance
```

Use the optional risk reviewer only for strategy validity, look-ahead,
survivorship, market-data timing, WFA, costs/slippage, backtest validity, or
public performance claims. Ordinary acceptance is the
`lo2cin4bt-acceptance` skill, not another agent.

## Skill Routing

| Request | Required lead skill |
| --- | --- |
| Setup, terminology, pages, lecture | `lo2cin4bt-teaching` |
| Create or review strategy config | `lo2cin4bt-strategy-builder` |
| Run, matrix, WFA, Run Center, screenshots | `lo2cin4bt-backtesting` |
| Metrics and generated-result explanation | `lo2cin4bt-performance-analysis` |
| Requirement and public-boundary check | `lo2cin4bt-acceptance` |

Code, agent, skill, or contract changes also require the project engineering
guardrails selected by the host ProjectManager.

## Architecture Rules

- New configs use canonical `strategy_run` or `wfa_run`; do not create legacy
  config mappings.
- Market-data timing is expressed by typed `data.bar_time` and stream bindings.
  Provider capability, timestamp semantics, session rules, availability, and
  data-quality policy must be validated before a run. Missing, duplicated, or
  out-of-order rows are blocking failures; do not repair, invent, or silently
  replace market data.
- Use provider capabilities exactly as implemented. `yfinance` is daily-only;
  Binance, Coinbase, FUTU, and IBKR intraday requests must match their declared
  intervals. FUTU and IBKR also require the user's gateway and data permission;
  adapter support is not proof that a live environment is ready.
- All strategies compile into the shared Rust execution contract. Profiles are
  authoring shapes, not separate engines.
- Direct daily data remains direct. When a declared decision stream needs a
  higher timeframe, the shared Rust runtime derives it from the execution
  stream and preserves timestamp, session, partial-bar, and no-look-ahead
  rules.
- Strategy capability decisions must read the generated operation registry and
  the computed-field catalog. PM must not approve an operation remembered from
  an older config or document.
- Python may orchestrate config, data, jobs, artifacts, and API I/O, but must not
  become a second result-changing backtester or metric calculator.
- Every candidate must produce a canonical result, pass the mandatory Rust
  validator, and feed the Rust metrics/plot contract before frontend success.
- Candidate identity is
  `base_strategy_id:workflow_id:parameter_suffix` (`fixed` when no parameter
  suffix exists). Ranking, objective rows, retained artifacts, WFA windows, and
  dashboards must match this identity exactly. A missing or mismatched artifact
  is a contract error, never a reason to select another result.
- Intraday result pages retain intraday equity and trade timestamps. Headline
  annualized metrics use one validated equity close per market session, while
  `intraday_max_drawdown` measures the largest decline inside sessions.
- WFA and rolling validation are explicit workflows. Parameter Matrix is
  optional parameter expansion, not mandatory validation.
- Large Parameter Matrix and WFA jobs run in bounded batches. Every candidate
  keeps its summary and rank; only the configured retained top candidates need
  heavyweight full artifacts.
- Python dependencies and commands use the locked `uv` route:
  `uv sync --locked` and `uv run --locked --exact`. Do not introduce a pip,
  requirements-file, Poetry, or unlocked fallback.
- The only supported app frontend is served on port `2424`.

## Result Deletion

Resolve a user-visible selector to its full `run_id`, then use
`uv run --locked --exact python scripts/cleanup_app_run.py <run_id>` for a dry
run. Execute with `--yes`
only after explicit approval. Cleanup must remove all registry, stage, snapshot,
manifest, chart payload, AI-review, screenshot references, and
`latest_runs.json` entries owned by that run.

## Product GitHub Boundary

- Resolve this `Repo` directory as the independent product Git root and the
  only product push source.
- The approved GitHub repository is an explicit input for each release; verify
  both product `origin` URLs against it.
- When nested in Company, the parent tracks only the product commit as a Git
  submodule and has no product remote.
- Require a clean product `main`, fetched non-diverged history, matching
  submodule pointer, and passing candidate/tracked release guards.
- Missing proof for product root, tracked state, parent relationship, remote
  identity, or release guards is a blocking failure, not a waivable warning.

## Output Packet

```yaml
response_language: zh-Hant|en
request_classification: teaching|strategy_building|backtesting|performance_analysis|acceptance|implementation|out_of_scope
selected_agent: lo2cin4btWorkAgent
selected_skills: []
scope: ""
evidence_required: []
risk_review_required: false
next_step: ""
not_trading_advice_notice: "Local research and software operation only."
```

No agent may place trades, move funds, modify broker accounts, or present
research output as investment advice.
