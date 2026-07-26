from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run quality gate for the current lo2cin4bt Rust-backed pipeline.")
    parser.add_argument(
        "--speed-profile",
        choices=["rust"],
        default="rust",
        help="Speed gate profile.",
    )
    parser.add_argument(
        "--gate-runs",
        type=int,
        default=3,
        help="Number of benchmark runs per speed profile (median aggregation).",
    )
    args = parser.parse_args()

    smoke_tests = [
        "tests/test_app_runtime_smoke.py",
        "tests/test_strategy_run_examples_runtime.py",
        "tests/test_rust_core_bridge.py",
        "tests/test_rust_accounting_golden.py",
        "tests/test_metricstracker_parquet_rust.py",
        "tests/test_unified_portfolio_wfa_runner.py",
    ]
    _run([sys.executable, "-m", "pytest", *smoke_tests, "-q"])
    _run(
        [
            sys.executable,
            str(PROJECT_ROOT / "verification" / "scripts" / "run_rust_pipeline_speed_gate.py"),
            "--profile",
            args.speed_profile,
            "--gate-runs",
            str(max(1, int(args.gate_runs))),
        ]
    )
    print(f"[quality-gate] PASS (speed_profile={args.speed_profile}, gate_runs={max(1, int(args.gate_runs))})")


if __name__ == "__main__":
    main()
