from __future__ import annotations

import argparse
import importlib.util
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_UV_VERSION = (0, 11, 32)
REQUIRED_RUST_VERSION = (1, 96, 0)
MIN_NODE_20_VERSION = (20, 19, 0)
MIN_NODE_22_VERSION = (22, 12, 0)

PYTHON_MODULES = {
    "fastapi": "fastapi",
    "exchange-calendars": "exchange_calendars",
    "numpy": "numpy",
    "orjson": "orjson",
    "pandas": "pandas",
    "plotly": "plotly",
    "pyarrow": "pyarrow",
    "rich": "rich",
    "scikit-learn": "sklearn",
    "uvicorn": "uvicorn",
    "yfinance": "yfinance",
}

RUST_RELEASE_BINS = [
    "engine_service_cli",
]


def _candidate_node_dirs() -> list[Path]:
    paths: list[Path] = []
    for key in ("LO2CIN4BT_NODE_HOME", "NODE_HOME"):
        raw = os.environ.get(key, "").strip()
        if raw:
            paths.append(Path(raw))
    paths.append(ROOT / ".tools" / "nodejs")
    return paths


def _candidate_rust_roots() -> list[Path]:
    paths: list[Path] = []
    for key in ("LO2CIN4BT_RUST_HOME", "RUST_HOME"):
        raw = os.environ.get(key, "").strip()
        if raw:
            paths.append(Path(raw))
    paths.append(ROOT / ".tools" / "rust")
    return paths


def _candidate_rust_bin_dirs() -> list[Path]:
    paths: list[Path] = []
    for key in ("LO2CIN4BT_CARGO_HOME", "CARGO_HOME"):
        raw = os.environ.get(key, "").strip()
        if raw:
            paths.append(Path(raw) / "bin")
    for root in _candidate_rust_roots():
        paths.append(root / "cargo" / "bin")
        paths.append(root / "bin")
    return paths


def _configure_preferred_rust_home() -> None:
    if os.environ.get("CARGO_HOME") and os.environ.get("RUSTUP_HOME"):
        return
    for root in _candidate_rust_roots():
        cargo_home = root / "cargo"
        rustup_home = root / "rustup"
        cargo_bin = cargo_home / "bin"
        if (cargo_bin / ("cargo.exe" if sys.platform.startswith("win") else "cargo")).exists():
            os.environ.setdefault("CARGO_HOME", str(cargo_home))
            os.environ.setdefault("RUSTUP_HOME", str(rustup_home))
            os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"
            return


def _preferred_command(command: str) -> str | None:
    names = [command]
    if command == "npm" and sys.platform.startswith("win"):
        names = ["npm.cmd", "npm"]
    if command == "node" and sys.platform.startswith("win"):
        names = ["node.exe", "node"]
    if command in {"node", "npm"}:
        for folder in _candidate_node_dirs():
            for name in names:
                candidate = folder / name
                if candidate.exists():
                    return str(candidate)
    if command in {"rustc", "cargo"}:
        names = [f"{command}.exe", command] if sys.platform.startswith("win") else [command]
        for folder in _candidate_rust_bin_dirs():
            for name in names:
                candidate = folder / name
                if candidate.exists():
                    return str(candidate)
    return shutil.which(command)


def _rust_release_bins_ready() -> bool:
    suffix = ".exe" if sys.platform.startswith("win") else ""
    release_dir = ROOT / "rust" / "lo2cin4bt_core" / "target" / "release"
    return all((release_dir / f"{name}{suffix}").exists() for name in RUST_RELEASE_BINS)


def _ok(message: str) -> None:
    print(f"[OK] {message}")


def _warn(message: str) -> None:
    print(f"[WARN] {message}")


def _fail(message: str) -> None:
    print(f"[FAIL] {message}")


def _command_version(command: str) -> str | None:
    executable = _preferred_command(command)
    if not executable:
        return None
    try:
        result = subprocess.run(
            [executable, "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except Exception:
        return executable
    return (result.stdout or result.stderr or executable).strip().splitlines()[0]


def _parse_rust_version(version_line: str) -> tuple[int, int, int] | None:
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_line)
    if not match:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _uv_lock_is_current() -> bool:
    try:
        result = subprocess.run(
            ["uv", "lock", "--check", "--project", str(ROOT)],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except Exception:
        return False
    return result.returncode == 0


def _parse_node_version(version_line: str) -> tuple[int, int, int] | None:
    match = re.search(r"v?(\d+)\.(\d+)\.(\d+)", version_line)
    if not match:
        return None
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch)


def _is_supported_node_version(version: tuple[int, int, int]) -> bool:
    major = version[0]
    if major == 20:
        return version >= MIN_NODE_20_VERSION
    if major == 21:
        return False
    if major == 22:
        return version >= MIN_NODE_22_VERSION
    return major > 22


def _node_version_failure_is_fatal(
    *,
    frontend_dist_exists: bool,
    require_frontend_build: bool,
) -> bool:
    return require_frontend_build or not frontend_dist_exists


def main() -> int:
    parser = argparse.ArgumentParser(description="Check local lo2cin4bt setup.")
    parser.add_argument("--skip-node", action="store_true", help="Skip Node/npm checks.")
    parser.add_argument(
        "--require-frontend-build",
        action="store_true",
        help="Fail when Node/npm cannot rebuild the React frontend. Without this flag, an existing dist can still be served through the locked uv app route.",
    )
    args = parser.parse_args()

    failures = 0
    frontend_root = ROOT / "plotter" / "web"
    frontend_dist_exists = (frontend_root / "dist" / "index.html").exists()
    _configure_preferred_rust_home()

    if sys.version_info >= (3, 12):
        _ok(f"Python {sys.version.split()[0]}")
    else:
        _fail("Python 3.12+ is required")
        failures += 1

    uv_version = _command_version("uv")
    required_uv_text = ".".join(str(part) for part in REQUIRED_UV_VERSION)
    if uv_version is None:
        _fail(f"uv {required_uv_text} is required")
        failures += 1
    else:
        parsed_uv = _parse_rust_version(uv_version)
        if parsed_uv != REQUIRED_UV_VERSION:
            _fail(f"uv {required_uv_text} is required; found {uv_version}")
            failures += 1
        else:
            _ok(f"uv available: {uv_version}")
            if _uv_lock_is_current():
                _ok("uv.lock is current")
            else:
                _fail("uv.lock is stale")
                failures += 1

    for label, module in PYTHON_MODULES.items():
        if importlib.util.find_spec(module) is None:
            _fail(f"Missing Python package: {label}")
            failures += 1
        else:
            _ok(f"Python package available: {label}")

    if not args.skip_node:
        node_version = _command_version("node")
        npm_version = _command_version("npm")
        if node_version:
            parsed_node = _parse_node_version(node_version)
            if parsed_node is not None and _is_supported_node_version(parsed_node):
                _ok(f"Node available: {node_version}")
            else:
                message = "Node.js 20.19.0+, 22.12.0+, or 24 LTS+ is required for Vite rebuild/dev"
                if _node_version_failure_is_fatal(
                    frontend_dist_exists=frontend_dist_exists,
                    require_frontend_build=args.require_frontend_build,
                ):
                    _fail(message)
                    failures += 1
                else:
                    _warn(f"{message}; existing frontend dist can still be served through the locked uv app route")
        else:
            message = "Node.js is required for rebuilding the React frontend"
            if _node_version_failure_is_fatal(
                frontend_dist_exists=frontend_dist_exists,
                require_frontend_build=args.require_frontend_build,
            ):
                _fail(message)
                failures += 1
            else:
                _warn(f"{message}; existing frontend dist can still be served through the locked uv app route")
        if npm_version:
            _ok(f"npm available: {npm_version}")
        else:
            message = "npm is required for plotter/web rebuild/dev"
            if _node_version_failure_is_fatal(
                frontend_dist_exists=frontend_dist_exists,
                require_frontend_build=args.require_frontend_build,
            ):
                _fail(message)
                failures += 1
            else:
                _warn(f"{message}; existing frontend dist can still be served through the locked uv app route")

    rust_version = _command_version("rustc")
    cargo_version = _command_version("cargo")
    if rust_version:
        parsed = _parse_rust_version(rust_version)
        if parsed is not None and parsed >= REQUIRED_RUST_VERSION:
            _ok(f"Rust available: {rust_version}")
        else:
            _fail("Rust 1.96.0+ is required for the Polars-backed Rust metrics parquet path")
            failures += 1
    else:
        _fail("Rust toolchain is required; install with rustup and rerun setup")
        failures += 1
    if cargo_version:
        _ok(f"Cargo available: {cargo_version}")
    else:
        _fail("cargo is required for rust/lo2cin4bt_core")
        failures += 1
    if _rust_release_bins_ready():
        _ok("Optimized Rust release binaries are built")
    else:
        _warn(
            "Optimized Rust release binaries are missing; run setup or "
            "`cargo build --release --bins` in rust/lo2cin4bt_core before large matrices"
        )

    if (frontend_root / "package-lock.json").exists():
        _ok("Frontend lockfile exists")
    else:
        _fail("Missing plotter/web/package-lock.json")
        failures += 1

    for workspace_folder in [ROOT / "workspace" / "runs", ROOT / "workspace" / "wfa"]:
        if workspace_folder.exists():
            _ok(f"Workspace config folder exists: {workspace_folder.relative_to(ROOT)}")
        else:
            workspace_folder.mkdir(parents=True, exist_ok=True)
            _ok(f"Created workspace config folder: {workspace_folder.relative_to(ROOT)}")
        config_count = len(
            [
                path
                for path in workspace_folder.glob("*.json")
                if "template" not in path.stem.lower()
            ]
        )
        if config_count:
            _ok(
                f"Workspace config folder has {config_count} runnable JSON config(s): "
                f"{workspace_folder.relative_to(ROOT)}"
            )
        else:
            _warn(
                f"{workspace_folder.relative_to(ROOT)} is empty. This is normal after a fresh "
                "GitHub clone; ask the project agent to initialize the built-in example configs "
                "before expecting Run Center to show runnable jobs."
            )

    if frontend_dist_exists:
        _ok("Frontend dist exists for locked uv app serving")
    else:
        _warn(
            "Frontend dist is absent. This is normal after a fresh GitHub clone. "
            "Run `cd plotter/web && npm ci && npm run build` before "
            "`uv run --locked --exact python main.py`, or run "
            "`uv run --locked --exact python scripts/doctor.py --require-frontend-build` "
            "to make this a hard setup gate."
        )

    if failures:
        print(f"\nDoctor finished with {failures} failure(s).")
        return 1
    print("\nDoctor finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
