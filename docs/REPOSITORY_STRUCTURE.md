# Repository Structure Notes

This document clarifies the repo layout boundaries that are easiest to confuse
while preparing a public GitHub snapshot.

## Dependency Profiles

- `requirements.txt`, `requirements-dev.txt`, and `requirements-brokers.txt`
  are maintainer-edited dependency inputs.
- `requirements.lock`, `requirements-dev.lock`, and
  `requirements-brokers.lock` are reproducible installation profiles for the
  runtime, development/CI, and optional FUTU/IBKR data adapters.
- JavaScript dependencies are managed independently by
  `plotter/web/package-lock.json`.

Locked requirements files are the installation contract. The unpinned input
files declare supported dependency ranges, while `pyproject.toml` stores
project metadata and tool configuration rather than the full runtime list.

## App Boundary

- `main.py` starts the browser-first app on `127.0.0.1:2424`.
- `app/api/` is the FastAPI HTTP/WebSocket API and static frontend server.
- `app/runtime/` is the Python run execution and filesystem registry layer
  used by `app.api`.
- `plotter/` is the visualization namespace.
- `plotter/web/` is the current React + Vite frontend source. Its `dist/`
  output is generated and ignored by Git.
- Legacy Python Dash/Plotly plotter modules were removed after the React
  frontend became the supported visualization entrypoint.

## Scripts Boundary

Use `scripts/` only for commands a user needs to install, start, inspect, or
operate the local product. Owner release, one-time migration, fixture-generation,
README-media production, and private diagnosis tools are intentionally kept
outside the public repository.

Current scripts:

- `scripts/setup.ps1`
- `scripts/setup.sh`
- `scripts/doctor.py`
- `scripts/start_lo2cin4bt.ps1`
- `scripts/start_app_background.py`
- `scripts/create_windows_shortcut.ps1`
- `scripts/cleanup_app_run.py`
- `scripts/capture_screenshot_bundle.mjs`

## Generated Artifacts

Keep runtime outputs and local installation products out of Git:

- `outputs/`
- `logs/`
- `plotter/web/dist/`
- `plotter/web/node_modules/`
- Python caches and test caches
- `verification/baseline_old/`, `verification/candidate_new/`, and
  `verification/results/` generated outputs
