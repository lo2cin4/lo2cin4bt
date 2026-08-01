# Install And First Run

lo2cin4bt is designed to run locally without Docker. New users need:

- Python 3.12 or newer
- uv 0.11.32
- Node.js 24 LTS recommended. Minimum supported frontend runtime is Node.js
  20.19.0 or 22.12.0 because Vite 6 requires those patch levels.
- Rust 1.96.0 through rustup for the Rust backtester/metricstracker core
- Git
- An AI coding assistant is recommended, either in an IDE or CLI

## Host Tools Policy

The GitHub repository does not bundle Python, Node.js, Rust, `.venv/`,
`node_modules`, Cargo build output, or Rust registry caches. That keeps
downloads small and follows normal open-source project practice. Install host
tools once, then let `scripts/setup.*` and `scripts/doctor.py` detect them.

Install host tools through their official installers or package managers and
make them available on `PATH`. If you prefer managed tool directories on
Windows, keep them outside the cloned repository, for example:

```text
C:\dev-tools\python
C:\dev-tools\nodejs
C:\dev-tools\rust
```

The setup and doctor scripts do not assume those example locations. They
resolve tools from `PATH`, repo-local `.tools/`, or these environment variables:

```text
LO2CIN4BT_NODE_HOME
NODE_HOME
LO2CIN4BT_RUST_HOME
RUST_HOME
CARGO_HOME
RUSTUP_HOME
```

An AI CLI/IDE assistant can install or update Node/Rust for the user, but it
should install them outside this repo and rerun
`uv run --locked --exact python scripts/doctor.py` after
setup. Local caches such as `.venv/`, `plotter/web/node_modules/`, and
`rust/**/target/` are disposable build/runtime artifacts and are intentionally
ignored by Git.

## Windows Quick Start

```powershell
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
.\scripts\setup.ps1
uv run --locked --exact python main.py
```

Then open:

```text
http://127.0.0.1:2424/
```

On first app launch, Run Center seeds included examples into `workspace/runs/`.
If you ever need to recreate the QQQ SMA Cross example manually:

```powershell
New-Item -ItemType Directory -Force workspace\runs
Copy-Item backtester\contracts\strategy\examples\strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json workspace\runs\strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json
```

## macOS / Linux Quick Start

```bash
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
bash scripts/setup.sh
uv run --locked --exact python main.py
```

On first app launch, Run Center seeds included examples into `workspace/runs/`.
If you ever need to recreate the QQQ SMA Cross example manually:

```bash
mkdir -p workspace/runs
cp backtester/contracts/strategy/examples/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json workspace/runs/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json
```

## Manual Install

```bash
uv sync --locked
cd plotter/web
npm ci
npm run build
cd ../..
uv run --locked --exact python scripts/doctor.py
uv run --locked --exact python main.py
```

## Dependency Profiles

- `pyproject.toml`: the only maintainer-edited Python dependency authority.
- `uv.lock`: the reproducible resolution for all supported dependency groups.
- `[project].dependencies`: runtime used by the local app.
- `[dependency-groups].dev`: test, lint, type-checking, and CI tools.
- `[dependency-groups].brokers`: optional FUTU / IBKR adapter packages. Sync
  this group only when using `provider=futu` or `provider=ibkr`.
- `plotter/web/package-lock.json`: JavaScript dependency lockfile for the
  React/Vite frontend.
- `plotter/web/package.json`: records the supported Node.js and npm engine
  range for the React/Vite frontend.
- `rust-toolchain.toml`: Rust toolchain pin for the local Rust core. See
  `docs/RUST_TOOLCHAIN.md`.

Maintainers update the single Python lock for the supported Python 3.12 target:

```bash
uv lock
uv lock --check
```

The setup scripts run `uv sync --locked` by default. For dev tools, run
`.\scripts\setup.ps1 -Dev` or `bash scripts/setup.sh --dev`. Broker packages are
not part of the first run. Only add `-Brokers` or `--brokers` for optional
market-data gateway experiments; this app does not place live orders.

The setup scripts also run `npm ci` and `npm run build` in `plotter/web` by
default. That downloads Node frontend dependencies from `package-lock.json` and
builds the local React app. The lockfile pins versions and integrity hashes, but
npm packages can still run install scripts. If you want to inspect Node
dependencies first, use `.\scripts\setup.ps1 -SkipFrontend` or
`bash scripts/setup.sh --skip-frontend`; the browser app will need a later
manual `npm ci && npm run build` before
`uv run --locked --exact python main.py` can show the frontend.

On first app launch, lo2cin4bt copies included examples from
`backtester/contracts/strategy/examples/` into ignored local folders under
`workspace/runs/` and `workspace/wfa/`. This keeps GitHub clean while making Run
Center usable immediately after setup. WFA configs should reference executable
strategy configs with explicit repo-relative paths such as
`workspace/runs/my-strategy.json`, not bare filenames.

## Development Install

```bash
uv sync --locked --group dev
uv run --locked --exact --group dev python -m pytest -q
cargo test --manifest-path rust/lo2cin4bt_core/Cargo.toml --quiet
```

Optional FUTU / IBKR gateway packages:

```bash
uv sync --locked --group brokers
```

FUTU and IBKR also require local gateway applications, account login, API
permissions, and market-data entitlements. These are not solved by Python
dependencies alone. Account or gateway setup is allowed for read-only market
data, but do not enable live trading, enable order placement, move funds, change
positions, change account settings, or treat gateway setup as a release
requirement for local backtesting.

Optional FUTU market-data account note:

- Official redeem page: <https://redeem.futunn.com/redeem>.
- If you independently need a FUTU market-data account, the app flow is:
  download Futubull, register or log in, tap Discover, Me, Event Center,
  Redeem Center, then enter `AZ57KU`.
- In lo2cin4bt this is only for read-only market data. Do not enable trading,
  order placement, fund movement, position changes, or account-setting changes.

## AI Assistant Setup

Ask your AI CLI or IDE assistant to read these files first:

1. `AGENTS.md`
2. `README.md`
3. `README.en.md`
4. `skills/lo2cin4bt/SKILL.md`
5. `skills/lo2cin4bt/references/first-run.md`

Advanced references after the first run:

1. `docs/ai/AI_MANUAL_SKILL.md`
2. `docs/ai/AI_SKILL_LECTURE_GUIDE.md`
3. `workspace/README.md`

A good first prompt is:

```text
Use $lo2cin4bt. Read AGENTS.md, README.md, README.en.md,
skills/lo2cin4bt/SKILL.md, docs/ai/AI_MANUAL_SKILL.md,
docs/ai/AI_SKILL_LECTURE_GUIDE.md, agents/lo2cin4bt_PM.agent.md,
and skills/lo2cin4bt/references/first-run.md.
Help me run the simplest local backtest available in workspace/runs, then
explain the output using only repo files and generated artifacts. If workspace
configs are missing, create one supported beginner config first and tell me
where you saved it.
```

Expected first-run evidence:

- `http://127.0.0.1:2424/api/app/health` returns `{"status":"ok"}`.
- Run Center opens in the browser.
- One Backtest run completes, or Codex clearly explains that no local config is
  present and creates/imports one.
- Metrics Overview shows at least one strategy table row.
- Local runtime output appears under `outputs/app/`.

## Advanced AI Assistant References

For deeper automation or maintenance work, ask the assistant to also read:

1. `docs/ai/AI_MANUAL_SKILL.md`
2. `docs/ai/AI_SKILL_LECTURE_GUIDE.md`
3. `workspace/README.md`

## Troubleshooting

Run:

```bash
uv run --locked --exact python scripts/doctor.py
```

If the frontend is missing, run:

```bash
cd plotter/web
npm ci
npm run build
```

If Python dependencies fail to install, verify that uv is exactly 0.11.32,
`.python-version` selects Python 3.12, and `uv lock --check` succeeds. For
current troubleshooting, use `skills/lo2cin4bt/references/troubleshooting.md`.
