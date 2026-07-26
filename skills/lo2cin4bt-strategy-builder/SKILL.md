---
name: lo2cin4bt-strategy-builder
description: Repo-local strategy builder skill for lo2cin4bt. Use when converting plain-language strategy ideas into supported, needs-clarification, or unsupported verdicts, and when drafting strategy run or WFA configs from Strategy Building Blocks.
version: 2.1.0
status: active
category: workflow
use_when:
  - "Turning a strategy idea into a precise supported strategy_run config."
  - "Reviewing whether a new Strategy Building Block is required."
---

# lo2cin4bt Strategy Builder Skill

## Purpose

Translate strategy concepts into precise executable config semantics that compile into the shared Rust engine without family-specific runtime code.

## Use When

Use for new or revised strategy configs, profile selection, parameter domains, risk behavior, identity, and strategy summary generation.

## Inputs

- User strategy concept and constraints.
- Current schemas, op registry, examples, runtime architecture, and identity contract.
- Explicit data, timing, costs, benchmark, risk, and validation requirements.

## Workflow

Return a capability verdict, map the idea to supported building blocks, write config only when complete, derive identity and summary from executable fields, then validate.

## Language Policy
1. Use `response_language` from the PM packet when present; otherwise infer it from the latest user message.
2. If the user writes in Chinese or asks for Chinese, write Traditional Chinese for all non-specialist wording.
3. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact. For mixed technical terms, write Chinese first with English in parentheses where useful, e.g. 夏普率 (Sharpe), 前向分析 (WFA).
4. Return `response_language` in the output packet. When writing repo-local reports, apply the same language policy to the report body.

## Safety Notice
Strategy-building output is local research support only. It is not investment advice, trading advice, financial advice, or an instruction to trade.

## Required Reads
- `skills/lo2cin4bt/references/runtime-architecture.md`
- `skills/lo2cin4bt/references/strategy-authoring-template.md`
- `skills/lo2cin4bt/references/strategy-config-fields.md`
- `skills/lo2cin4bt/references/indicator-recipes.md`
- `skills/lo2cin4bt/references/computed-field-building-blocks.md`
- `skills/lo2cin4bt/references/strategy-identity-and-summary.md`
- `backtester/contracts/strategy_authoring/strategy-authoring-layers-v1.json`
- Relevant schemas under `backtester/contracts/strategy/`

## Capability Verdict
Return one verdict before writing any runnable config:

- `supported`
- `needs_clarification`
- `unsupported_needs_new_building_block`

## Parse The Strategy Into
- asset or universe
- data provider
- frequency
- calendar and timezone
- strategy mode and workflow
- short human strategy concept for `platform.display_label`
- computed fields
- signals
- selection
- allocation
- rebalance
- fill timing
- costs and slippage
- benchmark
- risk gates
- parameter domains
- outputs

## Rules
1. Define calculations in `computed_fields[]` before using them.
2. Read the generated operation registry and computed-field catalog. Use canonical operation names; do not infer support from memory.
3. Do not use inline feature nodes.
4. Treat condition logic, comparators, cross conditions, calendars, fill timing, and strategy templates as separate Strategy Building Block types.
5. For any named pattern, custom signal, or undefined setup, ask for observable OHLCV conditions first.
6. For same-session fills, prove the signal is known before the fill or reject it.
7. Do not write a runnable config for unsupported behavior.
8. Treat `platform.display_label` as a concise human summary only; never place executable behavior solely in the label or `metadata.notes`.
9. Derive `result_selector_preview` and `strategy_logic_preview` from the completed config before calling it runnable.
10. Treat every profile as an authoring and validation shape that compiles into the same engine request. Never add a per-family runtime path or a Python strategy calculator.

## Strategy Coverage Contract

- Read the current public profiles and authoring presets from `backtester/contracts/strategy/mode-registry-v1.json`; do not rely on a memorized family list.
- Read supported operations from `backtester/ops/registry.py` before returning `supported`.
- A strategy is writable only when its data, calculations, signals, selection/allocation, rebalance, fills, costs/slippage, risk, benchmark, parameter domains, and validation workflow are explicit and schema-valid.
- New public profiles, presets, or operations require updated registry/schema evidence and tests. The Skill must fail closed rather than silently approximate an unknown concept.
- “Can write a strategy” means producing a validated config and config-derived identity/summary. It does not mean every imaginable trading idea is already implemented.

## Default Strategy Development Workflow
1. Check existing Strategy Building Blocks and registry ops before proposing new code.
2. If existing blocks cover the idea, write a config-only `strategy_run` draft.
3. Put all derived values in `computed_fields[]`, then reference those names from `signals`, `selection`, or `allocation`.
4. Compose research formulas from generic math, transform, rolling-window, and cross-sectional operations before proposing a new strategy-specific block.
5. Use `fill_model` for timing, price basis, cost, slippage, accounting, and session assumptions.
6. Use `wfa_run` only as a wrapper that references a strategy config; do not duplicate strategy logic inside WFA.
7. Check the name and logic contract in `skills/lo2cin4bt/references/strategy-identity-and-summary.md`.
8. Run schema/support validation before calling any config runnable.
9. Route strategy, data, WFA, cost/slippage, look-ahead, or backtest validity changes to `lo2cin4btTradingRiskReviewAgent`.

## Missing OP / Building Block Flow
When an idea needs a missing OP, unsupported pattern/setup, order model, or calendar rule:

1. Return `needs_clarification` if the observable definition, data timing, entry, exit, invalidation, or fill timing is unclear.
2. Return `unsupported_needs_new_building_block` when no supported OP exists.
3. Do not write a runnable config until the missing block is implemented and reviewed.
4. Required implementation deliverables are:
   - registry entry and public op name
   - runtime implementation
   - schema or validator coverage
   - oracle tests for no-look-ahead behavior
   - golden/parity tests for expected signals or trades
   - docs/examples that label the feature accurately
   - quant safety metadata covering observation time, data availability time, earliest trade time, warmup/lookback, cost/slippage, WFA train/OOS behavior, and missing-data policy
5. For Pine Script translations, separate pattern detection, setup state, order model, exits, and visualization-only labels before deciding what lo2cin4bt must implement.


## Repo-Local Report Output
When ProjectManager asks for a durable repo-local report, write Markdown under `workspace/reports/agents/<agent_name>/YYYY-MM-DD_<short-topic>.md` and return `repo_agent_report_path` in the output packet. Use this for WorkAgent and private snapshot reports; keep Company-level closeout separate unless ProjectManager explicitly asks for it.

## Output Format
```text
response_language:
capability_verdict:
parsed_strategy_intent:
building_blocks_checked:
missing_or_ambiguous_items:
config_status:
config_paths:
result_selector_preview:
strategy_logic_preview:
validation_command:
quant_review_required:
repo_agent_report_path:
not_trading_advice_notice:
```

## Outputs

- Capability verdict and, only when supported, a validated `strategy_run` config.
- Config-derived result selector and Strategy Logic previews.

## Path Rules

- Schemas and examples: `Repo\backtester\contracts\`.
- Local runnable configs: ignored `Repo\workspace\runs\`.
- Never create a new strategy-family source folder or compatibility config mapper.

## Validation

```powershell
python -m pytest tests/test_strategy_run_config.py tests/test_strategy_authoring_layers.py tests/test_engine_request_contract.py tests/test_agent_skill_lecture_alignment.py -q
```

Pass criteria: schema and support checks pass, previews derive from executable config, and the config compiles toward the shared EngineRequest.

Fail action: return `needs_clarification` or `unsupported_needs_new_building_block`; do not emit a fake runnable config.

## Report Requirements

Record the capability verdict, building blocks checked, config paths, semantic summary, validation commands, quant-review trigger, and unresolved assumptions.
