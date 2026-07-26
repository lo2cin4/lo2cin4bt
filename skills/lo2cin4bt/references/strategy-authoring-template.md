# Strategy Authoring Template

Use this reference when a user describes a strategy in plain language and asks lo2cin4bt or `lo2cin4` to build it.

Beginner-facing language has one public term: **Strategy Building Blocks**. Broadly, this means repo-supported, tested parts of a strategy, such as data source choices, indicators, conditions, calendar rules, allocation methods, fill timing, cost/slippage assumptions, or validation workflow.

The machine registry currently covers the executable or authoring-op subset of Strategy Building Blocks: `indicator`, `condition_logic`, `condition_comparator`, `cross_condition`, `calendar`, `execution`, and `strategy_template`. Other config sections such as data provider, allocation method, cost model, slippage model, risk, and validation workflow remain `strategy_run` config fields unless they are promoted into the registry with code, tests, and quant safety metadata.

Do not tell a beginner that an unsupported indicator, chart pattern, or named setup is runnable just because the idea is common in trading. A strategy is runnable only when the repo has the required Strategy Building Blocks and contracts.

## Two-Layer Authoring Model

Use `backtester/contracts/strategy_authoring/strategy-authoring-layers-v1.json` as the contract for separating beginner/AI authoring from machine execution.

Layer 1 is the **Strategy Config DSL**. This is the human/AI writing surface. It should read like strategy intent: define named `computed_fields`, compose `signals`, choose `selection`, set `allocation`, `rebalance`, `fill_model`, `risk`, costs, slippage, and validation workflow. The DSL uses explicit op names such as `op: indicator.rsi` or `op: lt`; it must not require the beginner to know internal runtime paths.

Layer 2 is **Machine IR** plus the Strategy Building Block registry. The validator and AI support checker must normalize the DSL into canonical nodes with `canonical_id`, `block_kind`, `usage_site`, inputs, params, temporal metadata, implementation source hash, and evidence paths. Runtime adapters should only execute validated IR.

Keep this mental model:

```text
Natural language request
-> Strategy Config DSL
-> Machine IR
-> Strategy Building Block registry support checks
-> Runtime adapter
-> Verification artifacts
```

Read `strategy-identity-and-summary.md` before writing the config. The AI must
write a specific human-facing `platform.display_label`, but all detailed
strategy behavior must remain in executable structured fields. Before calling
the config runnable, derive both the result-selector preview and the complete
strategy-logic preview from the config itself.

The current `block_kind` taxonomy is:

- `indicator`: computes named numeric values in `computed_fields`
- `condition_logic`: combines conditions such as all/any/not
- `condition_comparator`: compares fields, constants, or computed values
- `cross_condition`: detects current/previous bar crossings
- `calendar`: creates date/session masks or event sessions
- `execution`: marks fill timing or accounting semantics
- `strategy_template`: authoring scaffold that expands into a config, not a runtime result

## Capability Verdicts

Return exactly one of these verdicts before writing any runnable config:

- `supported`: Current Strategy Building Blocks and schema contracts cover the request. Required data, fill timing, cost/slippage, benchmark, and validation workflow are known.
- `needs_clarification`: The request may be supportable, but at least one required item is missing or ambiguous. Ask targeted questions before writing a config.
- `unsupported_needs_new_building_block`: Current Strategy Building Blocks do not cover the requested behavior, or the user named a complex pattern without a repo-supported observable definition. Do not write a runnable config.

## First Pass Checklist

Map the user's text request to these fields:

- asset or universe
- data provider
- data frequency
- calendar and timezone
- Strategy Building Blocks needed
- entry rule
- exit rule
- invalidation or risk-off rule
- fill timing
- cost/slippage assumptions
- benchmark
- parameter ranges
- validation workflow
- short strategy concept and result-selector preview
- generated strategy-logic preview

If all required fields are known and every needed Strategy Building Block exists, use `supported`.

If the repo likely supports the building blocks but the request is incomplete, use `needs_clarification`.

If any required building block does not exist, use `unsupported_needs_new_building_block`.

## Question Flow For Complex Or Undefined Strategies

For complex or undefined strategies, including any named pattern, custom signal, or external strategy rule that lacks a repo-supported observable definition, ask the user questions in this order. Ask at most three questions per turn, then continue the same queue after the user answers.

1. Observable definition: What exact data conditions define the pattern using columns lo2cin4bt can observe, such as OHLCV, volume, moving averages, highs/lows, ranges, or calendar events?
2. Data frequency: Should the pattern be observed on daily, hourly, minute, or another bar frequency?
3. Entry: What exact condition triggers entry after the pattern is observed?
4. Exit: What exact condition exits a winning or neutral trade?
5. Invalidation: What condition proves the setup failed before or after entry?
6. Parameter ranges: Which lookbacks, thresholds, contraction counts, breakout levels, volume filters, or holding periods should be fixed or swept?
7. Fill timing: After a signal is known, should the earliest simulated fill happen at next open, next close, same-session close, or another supported timing?
8. Cost/slippage: What commission, spread, slippage, and turnover assumptions should be applied?
9. Benchmark: What benchmark should be used for comparison?
10. Validation workflow: Should this be a single backtest, Parameter Matrix, rolling validation, or WFA, and which result is allowed to count as out-of-sample evidence?

Do not skip the observable definition. A pattern name alone is not a Strategy Building Block.

## Safe Unsupported Flow

When the verdict is `unsupported_needs_new_building_block`:

1. State which Strategy Building Block is missing.
2. State that no runnable config will be written.
3. Ask only for the missing observable definition or implementation constraints needed to design the new block.
4. Create a bounded implementation plan only when asked.
5. Implement the new block only inside an approved coding task with registry entry, runtime implementation, schema/validator coverage, oracle/golden tests, docs/examples, and quant safety metadata.

AI writes no runnable config until building block registry entry + implementation + tests + docs/examples + quant safety metadata exist.

Quant safety metadata must at least document observation time, data availability time, earliest trade time, lookback/warmup, fill policy, missing-data handling, and WFA train/OOS behavior.

## Anti-Look-Ahead And Forward-Bias Guardrails

Use plain language in user-facing explanations:

- Observation time: the time when the market fact happened.
- Data availability time: the time when the strategy could actually know that fact.
- Earliest trade time: the first supported time the strategy can trade after the fact is known.
- No future bars: a signal for one bar must not depend on prices, volume, labels, or ranks from later bars.
- No bfill for tradable signals: do not backfill future values into past rows for entries, exits, rankings, or allocations.
- WFA train/OOS separation: parameters are selected using only the training window, then tested on the out-of-sample window without peeking.

If any of these are unclear for a proposed Strategy Building Block, the verdict is `unsupported_needs_new_building_block` or `needs_clarification`, not `supported`.

## Response Template

Use this structure for strategy authoring replies:

```text
capability_verdict: supported | needs_clarification | unsupported_needs_new_building_block
strategy_building_blocks_checked:
- ...
missing_or_ambiguous_items:
- ...
next_step:
- ...
config_status: runnable_config_written | waiting_for_answers | no_runnable_config_until_new_building_block_exists
evidence_paths:
- ...
```

For `supported`, `config_status` can be `runnable_config_written` only after the config is actually created or updated.

For `needs_clarification`, `config_status` must be `waiting_for_answers`.

For `unsupported_needs_new_building_block`, `config_status` must be `no_runnable_config_until_new_building_block_exists`.
