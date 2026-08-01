from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RESULT_DIR = PROJECT_ROOT / "verification" / "results" / "benchmarks" / "rust_pipeline_speed_gate"
THRESHOLD_PATH = PROJECT_ROOT / "verification" / "benchmarks" / "thresholds" / "rust_pipeline_speed_gate_thresholds.json"


def _load_thresholds() -> Dict[str, Any]:
    return json.loads(THRESHOLD_PATH.read_text(encoding="utf-8"))


def _build_rust_release() -> None:
    crate_dir = PROJECT_ROOT / "rust" / "lo2cin4bt_core"
    subprocess.run(["cargo", "build", "--release", "--bins"], cwd=str(crate_dir), check=True)


def _rank_selection_payload() -> Dict[str, Any]:
    rows = 1000
    cols = 8
    eligible: List[bool] = []
    score: List[float] = []
    for row in range(rows):
        for col in range(cols):
            eligible.append((row + col) % 7 != 0)
            score.append(float(((row * 17) + (col * 31)) % 997))
    return {
        "rows": rows,
        "cols": cols,
        "eligible": eligible,
        "score": score,
        "ascending": False,
        "top_n": 3,
        "position_limit": 0.25,
    }


def _daily_rank_accounting_payload() -> Dict[str, Any]:
    rows = 756
    symbols = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
    cols = len(symbols)
    close: List[float] = []
    eligible: List[bool] = []
    score: List[float] = []
    start_date = date(2021, 1, 1)
    dates = [(start_date + timedelta(days=idx)).isoformat() for idx in range(rows)]
    for row in range(rows):
        for col in range(cols):
            close.append(100.0 + row * (0.05 + col * 0.002) + ((row + col) % 11))
            eligible.append((row + col) % 13 != 0)
            score.append(float(((row + 3) * (col + 5)) % 101))
    return {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "dates": dates,
        "symbols": symbols,
        "close": close,
        "eligible": eligible,
        "score": score,
        "ascending": False,
        "top_n": 3,
        "position_limit": 0.25,
    }


def _metrics_batch_payload() -> Dict[str, Any]:
    backtest_count = 128
    points = 252
    equity: List[float] = []
    bah_equity: List[float] = []
    trade_actions: List[float] = []
    trade_returns: List[float | None] = []
    position_size: List[float] = []
    session_labels: List[str] = []
    group_start: List[int] = []
    group_end: List[int] = []
    backtest_ids: List[str] = []
    cursor = 0
    for run_idx in range(backtest_count):
        backtest_ids.append(f"bench_{run_idx:03d}")
        group_start.append(cursor)
        value = 100.0
        bah_value = 100.0
        for point in range(points):
            if point > 0:
                value *= 1.0 + ((((point + run_idx) % 9) - 4) * 0.0007)
                bah_value *= 1.0 + ((((point + run_idx) % 7) - 3) * 0.0005)
            equity.append(value)
            bah_equity.append(bah_value)
            is_entry = point % 63 == 1
            is_exit = point % 63 == 31
            trade_actions.append(1.0 if is_entry else 4.0 if is_exit else 0.0)
            trade_returns.append((value / equity[-30]) - 1.0 if is_exit and len(equity) >= 30 else None)
            position_size.append(1.0 if point % 63 < 31 else 0.0)
            session_labels.append(str(date(2020, 1, 1) + timedelta(days=point)))
            cursor += 1
        group_end.append(cursor)
    return {
        "time_unit": 252,
        "risk_free_rate": 0.04,
        "backtest_ids": backtest_ids,
        "equity": equity,
        "bah_equity": bah_equity,
        "session_labels": session_labels,
        "trade_actions": trade_actions,
        "trade_returns": trade_returns,
        "position_size": position_size,
        "group_start": group_start,
        "group_end": group_end,
    }


def _time_call(name: str, fn: Callable[[], Dict[str, Any]]) -> Dict[str, Any]:
    start = time.perf_counter()
    summary = fn()
    elapsed = time.perf_counter() - start
    return {
        "operation": name,
        "seconds": elapsed,
        "summary_keys": sorted(summary.keys())[:12],
    }


def _run_once() -> Dict[str, Any]:
    from backtester import RustCoreBridge_backtester as bridge

    if not bridge.rust_core_available():
        raise RuntimeError("Rust core is unavailable")
    operations = [
        _time_call(
            "daily_rank_accounting",
            lambda: bridge.run_daily_rank_accounting_via_cli(_daily_rank_accounting_payload(), timeout=60),
        ),
        _time_call(
            "rank_selection",
            lambda: bridge.run_rank_selection_via_cli(_rank_selection_payload(), timeout=60),
        ),
        _time_call(
            "metrics_batch",
            lambda: bridge.run_metrics_batch_via_cli(_metrics_batch_payload(), timeout=60),
        ),
    ]
    return {
        "operations": operations,
        "total_seconds": sum(float(item["seconds"]) for item in operations),
    }


def _median(values: List[float]) -> float:
    if not values:
        raise ValueError("cannot aggregate empty values")
    return float(statistics.median(values))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Rust pipeline speed gate.")
    parser.add_argument("--profile", choices=["rust"], default="rust")
    parser.add_argument("--gate-runs", type=int, default=3)
    parser.add_argument("--skip-build", action="store_true", help="Assume release Rust binaries are already built.")
    args = parser.parse_args()

    if not args.skip_build:
        _build_rust_release()

    gate_runs = max(1, int(args.gate_runs))
    runs = [_run_once() for _ in range(gate_runs)]
    thresholds = _load_thresholds()[args.profile]
    operation_names = [item["operation"] for item in runs[0]["operations"]]
    medians = {
        name: _median(
            [
                float(next(item for item in run["operations"] if item["operation"] == name)["seconds"])
                for run in runs
            ]
        )
        for name in operation_names
    }
    total_median = _median([float(run["total_seconds"]) for run in runs])
    summary = {
        "benchmark_id": "rust_pipeline_speed_gate",
        "profile": args.profile,
        "gate_runs": gate_runs,
        "operation_median_seconds": medians,
        "total_median_seconds": total_median,
        "thresholds": thresholds,
        "runs": runs,
    }

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    (RESULT_DIR / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    max_total = float(thresholds["max_total_seconds"])
    if total_median > max_total:
        raise SystemExit(
            f"Rust pipeline speed gate failed: total median {total_median:.6f}s > {max_total:.6f}s"
        )
    operation_thresholds = thresholds.get("max_operation_seconds", {})
    for name, seconds in medians.items():
        max_operation = float(operation_thresholds.get(name, max_total))
        if seconds > max_operation:
            raise SystemExit(
                f"Rust pipeline speed gate failed: {name} median {seconds:.6f}s > {max_operation:.6f}s"
            )

    print(
        "[rust-pipeline-speed-gate] PASS "
        f"runs={gate_runs} total_median={total_median:.6f}s "
        + " ".join(f"{name}={seconds:.6f}s" for name, seconds in medians.items())
    )


if __name__ == "__main__":
    main()
