# Quality Gates

Use these gates before publishing a release, after changing the backtester,
metrics pipeline, runtime, config schema, agent instructions, or frontend.

## Local Setup Gate

```powershell
python scripts/doctor.py
```

`doctor.py` checks Python packages, Node/npm, Rust/Cargo, Rust release binaries,
workspace config folders, and the frontend build output.

## Complete Python And Golden Gate

```powershell
python -m pytest -q
```

This is the release authority. It includes the 15-case Golden suite, runtime
contracts, strategy examples, WFA, app payloads, Agent/Skill alignment, Lecture
contracts, and interface audits.

To isolate deterministic result regressions:

```powershell
python -m pytest -m golden -q
```

The Golden gate covers the content-addressed dataloader bundle, all eight public
strategy examples, Rust accounting and metrics, WFA selected optimum,
`PlotBundle.v1`, and the canonical end-to-end route.

## Runtime Example Gate

```powershell
python -m pytest tests/test_strategy_run_examples_runtime.py -q
```

This runs the built-in strategy examples through the runtime path and verifies
that result artifacts are generated as expected.

## WFA And App Runtime Gate

```powershell
python -m pytest tests/test_unified_portfolio_wfa_runner.py tests/test_app_runtime_smoke.py -q
```

This checks walk-forward analysis and the app runtime smoke path.

## Focused Agent, Skill, And Lecture Gate

```powershell
python -m pytest tests/test_ai_skill_docs.py tests/test_agent_skill_lecture_alignment.py tests/test_lecture_contracts.py -q
```

This checks repo-local AI instructions, skills, and documentation references.

## Type And Lint Gate

```powershell
python -m mypy
python -m ruff check .
```

The mypy scope is intentionally controlled by `pyproject.toml`. Ruff is used
here for high-signal unused import, unused variable, and redefinition failures.

## Rust Gate

```powershell
cargo test --manifest-path rust/lo2cin4bt_core/Cargo.toml --quiet
cargo build --manifest-path rust/lo2cin4bt_core/Cargo.toml --release --bins
```

Run this after changing Rust kernels, Rust metrics, direct parquet bundle output,
or any Python bridge that calls Rust binaries.

## Frontend Gate

```powershell
cd plotter/web
npm ci
npm run build
npx playwright install chromium
cd ../..
python main.py --no-browser
```

With the app listening on port `2424`, run this in another terminal:

```powershell
cd plotter/web
npm run test:e2e:production
npm run test:e2e:2424 -- e2e/backtest-result-contract-recovery.p0.spec.ts
```

Run this after changing the Vite/React UI, frontend routes, chart components,
or payload contracts consumed by the app. The production-route suite is
clean-clone reproducible and runs in CI. The P0 result-contract suite additionally
requires its registered local recovery fixture and is a release-workstation
gate.

## Release Checklist

- `python scripts/doctor.py` passes.
- `python -m pytest -m golden -q` reports 15 passed.
- Core regression, runtime example, WFA/app runtime, and AI docs tests pass.
- `python -m mypy` passes.
- Ruff high-signal check passes.
- Rust tests and release binary build pass when Rust-facing code changed.
- Frontend build passes when UI or payload contracts changed.
- Playwright route and contract tests pass when the frontend or payload
  contracts changed.
- Built-in examples and WFA configs remain present under `workspace/runs` and
  `workspace/wfa`.
- No generated run outputs, build caches, or local toolchains are staged.
