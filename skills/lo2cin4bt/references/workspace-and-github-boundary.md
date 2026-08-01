# Workspace And GitHub Boundary

Use this before telling a user what will or will not upload to GitHub.

## Non-Negotiable Publishing Invariant

This repository is the only product Git repository and the only GitHub push
source:

- Resolve the Git root exactly to this `Repo` directory before any release.
- Keep one explicit `origin` whose fetch and push URLs both match the approved
  GitHub repository.
- Require a clean `main`, no untracked public candidate files, no staged or
  unstaged changes, and a passing release guard before pushing.
- Fetch before push and stop when local `main` is behind or has diverged from
  `origin/main`; do not force push or create an alternate publication route.
- When nested inside a Company workspace, the parent tracks only the product
  commit as a Git submodule. The parent has no product remote and must never be
  used to push product code.
- Remote drift, source-root drift, dirty state, release-guard failure, or an
  unexpected parent relationship must stop the operation. There is no exception
  or fallback route.

## Public GitHub

These files are intended to be tracked in the public release branch and pushed
to GitHub:

- Source code.
- Tests.
- Contracts and schemas.
- Public documentation.
- Setup scripts.
- `workspace/README.md`.
- Indicator extension source under `workspace/indicators/extensions/`.
- The repo-local skill under `skills/lo2cin4bt/`.
- Stable public test and verification fixtures under `tests/fixtures/` and
  `verification/fixtures/`.
- Reviewed README visual assets under `assets/readme/`.

## Local Or Private Git Only

These files should not be pushed to the public GitHub release branch. They may
still be kept in a local-only Git repo, private branch, private remote, or
external archive if the owner wants Git history for them.

- `outputs/`
- `logs/`
- `plotter/web/dist/`
- `plotter/web/node_modules/`
- `.venv/`
- Python/TypeScript caches.
- `.env`, broker keys, private certs, sqlite/db files.
- `workspace/runs/**`
- `workspace/wfa/**`
- `workspace/datasets/**`
- `workspace/calendars/**`
- `workspace/indicators/**`
- generated feature-contract workspace files
- `workspace/strategies/*.json`
- `workspace/statanalyser/**`
- release-excluded planning or archive documentation folders
- planning notes such as
  `docs/phase6-factor-strategy-development-plan.md` unless explicitly promoted
  to a reviewed public document.

Included strategy and WFA examples live under `backtester/contracts/strategy/examples/`. The public repo does not track local runnable workspace copies, so a clean clone may need an AI agent to initialize supported examples into ignored `workspace/runs/` and `workspace/wfa/` folders. Runnable WFA configs must reference their strategy config with an explicit repo-relative `workspace/runs/<strategy-config>.json` path, not a bare filename. Extra private configs may still be distributed outside GitHub when needed.

## README Visual Assets

Public README screenshots and animations must live under `assets/readme/`.
Do not store public README media under `outputs/`, `plotter/web/dist/`,
`workspace/`, root `assets/`, or any release-excluded docs folder.

Screenshots and GIFs must be produced from deterministic demo/synthetic
evidence. They must not show:

- `.env`, tokens, API keys, broker credentials, account IDs, certificates,
  account balances, order tickets, or live broker screens
- local absolute filesystem paths
- local datasets, private configs, private reports, generated runtime output
  inventories, raw run snapshots, raw chart payload JSON, or AI review packs
- release-excluded planning or archive documentation folders
- claims that backtest, Parameter Matrix, or WFA screenshots prove future
  returns, strategy validity, broker readiness, or live-trading safety

## Runtime Output Meaning

- `outputs/app/run_snapshots/` stores local run snapshots and managed artifacts.
- `outputs/app/chart_payloads/` stores frontend payload JSON.
- `outputs/app/ai_review/` stores AI-readable evidence packs.

These are generated evidence, not source.

## Staging Warning

If a file was tracked before `.gitignore` changed, it may still be tracked.
Removing it from future GitHub uploads requires `git rm --cached <path>` or an
explicit staging decision. Do not run destructive cleanup without user approval.

Git and GitHub are not separate visibility layers inside one pushed branch. A
file committed to the branch that is pushed to GitHub will be downloadable from
GitHub even if it later appears in `.gitignore`. `.gitignore` only protects
untracked files from accidental staging.
