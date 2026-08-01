# Backtest Config and Contracts

## User Entry Points

Curated release examples live under `backtester/contracts/strategy/examples/`.
These files are the public examples that should be copied or adapted when a new
user, AI agent, README flow, or test needs a known-good config.

Workspace files are local working inputs:

- `workspace/datasets/` for local CSV / Parquet / research datasets
- `workspace/runs/` for local `strategy_run` backtest configs copied or generated during research
- `workspace/wfa/` for local `wfa_run` configs
- `workspace/indicators/extensions/` for custom indicator extension packages
- `workspace/strategies/` for local strategy drafts and experiments

`workspace/` is user working state. Do not treat it as the curated release
example root.

Current app-managed outputs are generated under `outputs/app/`:

- `outputs/app/run_registry/`
- `outputs/app/run_snapshots/`
- `outputs/app/artifact_manifests/`
- `outputs/app/chart_payloads/`
- `outputs/app/ai_review/`

`outputs/` is runtime state and is intentionally ignored by Git. Recreate it by
running app jobs from Run Center or by executing the relevant verification
scripts. Old module-specific output roots are legacy implementation details, not
the public app contract. `records/` is no longer part of the active input/output
path.

## Canonical Contract Roots

Backtest contract truth lives under `backtester/contracts/`:

- strategy run schema: `backtester/contracts/strategy/strategy-run.schema.json`
- WFA run schema: `backtester/contracts/strategy/wfa-run.schema.json`
- strategy examples: `backtester/contracts/strategy/examples/strategy-run-*.json`
- WFA examples: `backtester/contracts/strategy/examples/wfa-run-*.json`
- Strategy Building Block templates: `backtester/contracts/strategy_authoring/templates/`
- generated app registry mirror: `app/contracts/generated/op-registry-v1.json`

The runtime registry source is `backtester/ops/registry.py`. Regenerate the app
mirror with `python -m backtester.ops.export` after registry changes.

## Current Runtime Truth

For new backtest runs, the supported public config is `schema_version:
"strategy_run"`.

The current public strategy sections are:

- `computed_fields` for calculated values such as SMA, RSI, MACD, z-score,
  percentile, Bollinger bands, ATR, momentum, and custom supported indicators
- `signals` for entry, exit, and target-weight signal rules
- `selection` for eligibility filters, ranking, and top-N selection
- `allocation` for portfolio weight rules
- `rebalance` for calendar, signal-change, and portfolio rebalance triggers
- `fill_model` for fill timing, price basis, transaction cost, slippage, and
  same-session accounting assumptions
- `risk` for exposure, position, and long/short boundaries
- `parameter_domains` for matrix and WFA parameter ranges
- `outputs` for requested artifacts

The legacy top-level aliases `features`, `indicators`, and `execution` are
read-side compatibility only. New examples and AI-authored configs should use
`computed_fields` and `fill_model`. Validators reject mixed canonical and legacy
sections so a config cannot silently combine two names for the same concept.

Runtime adapts the public config into internal engine inputs, runs through the
backtester or WFA runner, and writes app-readable artifacts under `outputs/app/`.
Internal artifact names such as `execution_plan.v1` may still appear in runtime
outputs; they are not user-editable config sections.

## Legacy Semantic Contract v2

The retired strategy-contract-v2 semantic surface is not present in the active
contract tree. Canonical strategy authoring uses `strategy-run.schema.json`.

Feature contract v1 remains useful for external, non-price inputs. Each item in
the typed external feature-contract `features[]` declares:

- a semantic field name, for example `feature.vix.close`
- a source type and URI
- a source column
- typed `bar_spec` / timezone / fill policy / lag
- optional `source.source_id` for multi-source auditability
- optional `calendar` / `staleness_max_bars` for contract-safe alignment metadata

Runtime materialization support:

- node_ir/native execution can materialize local feature-contract sources into
  the runtime dataframe
- current supported runtime join modes are `left` and `asof`
- current supported source file types are CSV / Excel / parquet
- `fill_policy`, `lag_bars`, and `asof_tolerance_bars` are applied during
  runtime materialization
- if a declared source file is absent but the needed source column already
  exists in the loaded base dataframe, runtime can fallback to the base dataframe
  column as a compatibility bridge

## Current Limitation

Some downstream compatibility fields still use old single-label wording:

- `selected_predictor`
- `Predictor_value`

Those fields are kept only to preserve existing consumer/output contracts. They
should be understood as compatibility labels, not as a limit on semantic strategy
inputs.

## Indicator Extensions

Indicator extension manifests define indicator metadata and implementation
mapping.

They define:

- indicator family identity
- parameter metadata
- implementation binding
- extension package discovery under `workspace/indicators/extensions/*/manifest.json`
- optional input-contract metadata for multi-column indicators

Trading actions do not belong in indicator metadata. They belong in `signals`,
`selection`, `allocation`, `rebalance`, and `fill_model`.

Current extension direction:

- core indicators remain registry-backed through `backtester/ops/registry.py`
- user indicators live in workspace packages and can bind Python implementations
  through `artifact_path + entrypoint`
- multi-column custom indicators are supported through indicator params such as
  `primary_column` / `confirm_column`
- prefer custom indicators that emit calculated fields or confirmations; keep
  trading semantics in the strategy config

## Migration Notes

- `docs/contracts/*` is no longer a truth source for active backtest contracts
- `backtester/contracts/strategy/examples/` is the curated public example root
- `workspace/` is the local user input root
- `outputs/` is the only supported runtime output root
- legacy config semantics are transition-only and blocked or normalized before
  runtime where possible
