# Rust Toolchain

lo2cin4bt uses Rust for the deterministic backtester and metricstracker compute
core.

## Required Version

- Rust: `1.96.0`
- Cargo: `1.96.0`
- Repo pin: `rust-toolchain.toml`
- Key Rust dataframe/parquet dependency: Polars `0.54.x`

Use stable Rust, not nightly. Rust `1.96.0` is the project baseline because it
supports the current Rust 2024-era dependency ecosystem used by Polars.

## Install Or Update

Windows, macOS, and Linux should install Rust through rustup:

```bash
rustup toolchain install 1.96.0 --profile minimal
rustup component add clippy rustfmt --toolchain 1.96.0
```

Inside this repo, Cargo automatically reads `rust-toolchain.toml` and uses
`1.96.0`.

The normal rustup defaults are supported on every platform. If Windows users
want a host-managed tool directory outside the cloned repository, one possible
layout is:

```text
C:\dev-tools\rust
```

For that optional layout, set:

```powershell
$env:LO2CIN4BT_RUST_HOME = "C:\dev-tools\rust"
$env:CARGO_HOME = "C:\dev-tools\rust\cargo"
$env:RUSTUP_HOME = "C:\dev-tools\rust\rustup"
$env:Path = "$env:CARGO_HOME\bin;$env:Path"
```

The repo setup and doctor scripts resolve the environment variables above,
repo-local `.tools/`, and `PATH`. Do not commit `rust/**/target/`, host tool
directories, or Cargo registry caches into this repository; they are local
machine tools or rebuildable caches.

## Why Not Rust 1.82

Rust `1.82.0` cannot parse some modern transitive dependency manifests that use
the 2024 edition. That blocks current Polars. Upgrading the toolchain is the
clean route; avoiding Polars only because of an old local compiler would make
the compute layer harder to maintain.

## Validation

```bash
cargo check --manifest-path rust/lo2cin4bt_core/Cargo.toml --bin engine_service_cli
cargo test --manifest-path rust/lo2cin4bt_core/Cargo.toml --quiet
uv run --locked --exact --group dev python -m pytest tests/test_metricstracker_parquet_rust.py -q
```

`metricstracker` uses Rust/Polars to read parquet files directly before sending
equity arrays into the Rust metrics kernel. Python remains the orchestration,
registry, payload, and export layer.

## Runtime Integration Model

The active integration boundary is subprocess-based:

- Python launches Rust binaries from `rust/lo2cin4bt_core/target/...` or falls
  back to `cargo run --bin ...` during local development.
- Long-running kernels can stay alive as `--server` subprocesses managed by
  `backtester/RustCoreBridge_backtester.py`.
- Payloads cross the boundary as JSON or parquet-backed file references.

The supported production runtime is not a PyO3/maturin extension wheel.
