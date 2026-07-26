# Scripts

This directory contains commands a user needs to install, start, inspect, or
operate the local product. Owner release, one-time migration, fixture generation,
README media production, and private diagnosis tools do not belong in the public
Repo.

## Install And Health

- `setup.ps1`: Windows setup wrapper. Installs runtime dependencies, optionally
  dev and broker profiles, builds `plotter/web`, then runs `doctor.py`.
- `setup.sh`: macOS/Linux setup wrapper with the same behavior as `setup.ps1`.
- `doctor.py`: checks Python, Node/npm, Rust/Cargo, lockfiles, example folders,
  and built frontend assets.

Rust is pinned by `rust-toolchain.toml`; see `docs/RUST_TOOLCHAIN.md`.

## Start And Operate

- `start_lo2cin4bt.ps1`: starts the supported local app on port `2424`.
- `start_app_background.py`: background launcher used by the Windows start flow.
- `create_windows_shortcut.ps1`: creates the optional Windows shortcut.
- `cleanup_app_run.py`: removes one app-managed run and all related artifacts.
- `capture_screenshot_bundle.mjs`: creates the in-app screenshot bundle requested
  by the frontend.
