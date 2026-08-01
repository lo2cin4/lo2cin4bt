#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
REQUIRED_UV_VERSION="0.11.32"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv $REQUIRED_UV_VERSION is required. Install it from https://docs.astral.sh/uv/ and rerun setup." >&2
  exit 1
fi
UV_VERSION_LINE="$(uv --version)"
case "$UV_VERSION_LINE" in
  "uv $REQUIRED_UV_VERSION"|"uv $REQUIRED_UV_VERSION "*) ;;
  *)
    echo "uv $REQUIRED_UV_VERSION is required; found '$UV_VERSION_LINE'." >&2
    exit 1
    ;;
esac

for node_dir in "${LO2CIN4BT_NODE_HOME:-}" "${NODE_HOME:-}" "$REPO_ROOT/.tools/nodejs"; do
  if [ -n "$node_dir" ] && [ -x "$node_dir/node" ] && [ -x "$node_dir/npm" ]; then
    export PATH="$node_dir:$PATH"
    echo "Using Node from $node_dir"
    break
  fi
done

for rust_root in "${LO2CIN4BT_RUST_HOME:-}" "${RUST_HOME:-}" "$REPO_ROOT/.tools/rust" "/opt/lo2cin4bt/rust"; do
  if [ -n "$rust_root" ] && [ -x "$rust_root/cargo/bin/cargo" ]; then
    export CARGO_HOME="${CARGO_HOME:-$rust_root/cargo}"
    export RUSTUP_HOME="${RUSTUP_HOME:-$rust_root/rustup}"
    export PATH="$rust_root/cargo/bin:$PATH"
    echo "Using Rust from $rust_root"
    break
  fi
done

INSTALL_DEV=0
INSTALL_BROKERS=0
SKIP_FRONTEND=0

for arg in "$@"; do
  case "$arg" in
    --dev) INSTALL_DEV=1 ;;
    --brokers) INSTALL_BROKERS=1 ;;
    --skip-frontend) SKIP_FRONTEND=1 ;;
    *) echo "Unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [ "$INSTALL_BROKERS" -eq 1 ]; then
  echo "uv sync --locked --group brokers"
  uv sync --locked --group brokers
elif [ "$INSTALL_DEV" -eq 1 ]; then
  echo "uv sync --locked --group dev"
  uv sync --locked --group dev
else
  echo "uv sync --locked"
  uv sync --locked
fi

if [ "$SKIP_FRONTEND" -eq 0 ]; then
  (cd plotter/web && npm ci && npm run build)
fi

(cd rust/lo2cin4bt_core && cargo build --release --bins)

if [ "$INSTALL_BROKERS" -eq 1 ]; then
  uv run --locked --exact --group brokers python scripts/doctor.py
elif [ "$INSTALL_DEV" -eq 1 ]; then
  uv run --locked --exact --group dev python scripts/doctor.py
else
  uv run --locked --exact python scripts/doctor.py
fi
