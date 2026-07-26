"""Bridge to the Rust deterministic core.

This repo does not use a PyO3 or maturin extension boundary for production
runtime work. Python owns orchestration, DataFrame shaping, path resolution,
and artifact export. Rust owns the deterministic accounting, signal timeline,
rank, and metrics kernels. The integration boundary is process-based:
Python launches repo-pinned Rust binaries or long-lived `--server` processes
and exchanges JSON or parquet-backed payloads with them.
"""

from __future__ import annotations

import atexit
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtester.EngineServiceClient_backtester import EngineServiceClient

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CRATE_DIR = _REPO_ROOT / "rust" / "lo2cin4bt_core"
_RUST_PROFILE = os.getenv("LO2CIN4BT_RUST_PROFILE", "release").strip().lower() or "release"
if _RUST_PROFILE not in {"debug", "release"}:
    _RUST_PROFILE = "release"
_RUST_BIN_SUFFIX = ".exe" if os.name == "nt" else ""
_RUST_TARGET_DIR = _CRATE_DIR / "target" / _RUST_PROFILE


def _rust_bin_path(bin_name: str) -> Path:
    return _RUST_TARGET_DIR / f"{bin_name}{_RUST_BIN_SUFFIX}"


_ENGINE_SERVICE_BIN = _rust_bin_path("engine_service_cli")
_KNOWN_RUST_SERVER_NAMES = {
    "engine_service_cli",
    "engine_service_cli.exe",
}


def rust_core_crate_dir() -> Path:
    return _CRATE_DIR


def rust_core_available() -> bool:
    return bool(shutil.which("cargo")) and _CRATE_DIR.exists()


def rust_core_profile() -> str:
    return _RUST_PROFILE


def _rust_source_newer_than(binary: Path) -> bool:
    if not binary.exists():
        return True
    binary_mtime = binary.stat().st_mtime
    source_paths = [_CRATE_DIR / "Cargo.toml", *_CRATE_DIR.glob("src/**/*.rs")]
    return any(path.exists() and path.stat().st_mtime > binary_mtime for path in source_paths)


def _rust_bin_command(binary: Path, bin_name: str, *, args: Optional[List[str]] = None) -> List[str]:
    extra_args = list(args or [])
    if binary.exists() and not _rust_source_newer_than(binary):
        return [str(binary), *extra_args]
    profile_args = [] if _RUST_PROFILE == "debug" else ["--release"]
    cargo = shutil.which("cargo") or "cargo"
    return [
        cargo,
        "run",
        "--quiet",
        "--manifest-path",
        str(_CRATE_DIR / "Cargo.toml"),
        *profile_args,
        "--bin",
        bin_name,
        "--",
        *extra_args,
    ]


def prewarm_rust_batch_services(service_names: Optional[List[str]] = None) -> Dict[str, str]:
    """Start the unified Rust engine service ahead of the first job."""

    targets = set(
        service_names
        or [
            "signal_timeline_batch",
            "calendar_same_session_batch",
            "calendar_overlay_batch",
            "reset_timer_batch",
            "daily_rank_batch",
            "metrics_parquet",
        ]
    )
    if not rust_core_available():
        raise RuntimeError("Rust core is unavailable")

    process = _ENGINE_SERVICE_CLIENT.start()
    if process.poll() is not None:
        raise RuntimeError("Rust engine service exited during prewarm")
    return {name: "ready" for name in sorted(targets)}


def _close_engine_service() -> None:
    _ENGINE_SERVICE_CLIENT.close(graceful=True)


def _kill_process_tree(process: Optional[subprocess.Popen[str]]) -> None:
    if process is None:
        return
    pid = process.pid
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return
    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()


def _tracked_engine_processes() -> List[Optional[subprocess.Popen[str]]]:
    return [_ENGINE_SERVICE_CLIENT.process]


def _kill_orphaned_rust_descendants() -> None:
    if os.name != "nt":
        return
    try:
        query = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "$rootPid = [int]$env:LO2CIN4BT_CANCEL_ROOT_PID; "
                    "$all = Get-CimInstance Win32_Process | "
                    "Select-Object ProcessId,ParentProcessId,Name; "
                    "$byParent = @{}; "
                    "foreach ($row in $all) { "
                    "  $key = [string]$row.ParentProcessId; "
                    "  if (-not $byParent.ContainsKey($key)) { $byParent[$key] = @() }; "
                    "  $byParent[$key] += $row "
                    "} "
                    "$queue = New-Object System.Collections.Generic.Queue[object]; "
                    "$queue.Enqueue($rootPid); "
                    "$seen = New-Object System.Collections.Generic.HashSet[int]; "
                    "$targets = @(); "
                    "while ($queue.Count -gt 0) { "
                    "  $parent = $queue.Dequeue(); "
                    "  foreach ($child in ($byParent[[string]$parent] | ForEach-Object { $_ })) { "
                    "    if ($seen.Add([int]$child.ProcessId)) { "
                    "      $queue.Enqueue([int]$child.ProcessId); "
                    "      $targets += $child "
                    "    } "
                    "  } "
                    "} "
                    "$targets | ConvertTo-Json -Compress"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "LO2CIN4BT_CANCEL_ROOT_PID": str(os.getpid())},
        )
        if query.returncode != 0:
            return
        payload = str(query.stdout or "").strip()
        if not payload:
            return
        rows = json.loads(payload)
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows:
            name = str((row or {}).get("Name") or "").strip()
            pid = int((row or {}).get("ProcessId") or 0)
            if pid <= 0 or name not in _KNOWN_RUST_SERVER_NAMES:
                continue
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception:
        return


def cancel_active_rust_work() -> None:
    """Best-effort stop for long-running Rust server workloads."""

    _ENGINE_SERVICE_CLIENT.cancel_all(timeout=1)
    for process in _tracked_engine_processes():
        _kill_process_tree(process)
    _ENGINE_SERVICE_CLIENT.close()
    _kill_orphaned_rust_descendants()


atexit.register(_close_engine_service)


def _spawn_engine_service_process() -> subprocess.Popen[str]:
    command = _rust_bin_command(_ENGINE_SERVICE_BIN, "engine_service_cli")
    return subprocess.Popen(
        command,
        cwd=_CRATE_DIR,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


_ENGINE_SERVICE_CLIENT = EngineServiceClient(
    _spawn_engine_service_process,
    availability_check=rust_core_available,
)


def _run_engine_service_operation(
    operation: str,
    payload: Dict[str, Any],
    *,
    timeout: int,
) -> Dict[str, Any]:
    return _ENGINE_SERVICE_CLIENT.execute(operation, payload, timeout=timeout)


def run_accounting_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust accounting through the unified engine service."""

    return _run_engine_service_operation("accounting", payload, timeout=timeout)


def run_timeline_accounting_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust timeline accounting through the unified engine service."""

    return _run_engine_service_operation("timeline_accounting", payload, timeout=timeout)


def run_signal_timeline_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust single-asset signal timeline producer and accounting."""

    return _run_engine_service_operation("signal_timeline", payload, timeout=timeout)


def run_signal_timeline_batch_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust batch producer for single-asset signal timelines."""

    return _run_engine_service_operation("signal_timeline_batch", payload, timeout=timeout)


def run_calendar_same_session_batch_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust batch producer for single-asset calendar same-session timelines."""

    return _run_engine_service_operation("calendar_same_session_batch", payload, timeout=timeout)


def run_calendar_overlay_batch_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust batch producer for multi-asset calendar overlay timelines."""

    return _run_engine_service_operation("calendar_overlay_batch", payload, timeout=timeout)


def run_reset_timer_batch_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust batch producer for reset-timer timeline matrices."""

    return _run_engine_service_operation("reset_timer_batch", payload, timeout=timeout)


def run_metrics_batch_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust batch equity metrics through the JSON CLI."""

    return _run_engine_service_operation("metrics_batch", payload, timeout=timeout)


def run_metrics_parquet_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust parquet reader + metrics kernel through the JSON CLI."""

    return _run_engine_service_operation("metrics_parquet", payload, timeout=timeout)


def run_plot_bundle_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Project validated result/metrics arrays into PlotBundle.v1 in Rust."""

    return _run_engine_service_operation("plot_bundle", payload, timeout=timeout)


def run_backtest_detail_bundle_via_cli(
    payload: Dict[str, Any], *, timeout: int = 30
) -> Dict[str, Any]:
    """Project raw result columns into BacktestDetailBundle.v1 in Rust."""

    return _run_engine_service_operation(
        "backtest_detail_bundle", payload, timeout=timeout
    )


def run_rank_selection_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust multi-asset rank selection producer through the JSON CLI."""

    return _run_engine_service_operation("rank_selection", payload, timeout=timeout)


def run_daily_rank_accounting_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust combined daily-rank selection and accounting through the JSON CLI."""

    return _run_engine_service_operation("daily_rank_accounting", payload, timeout=timeout)


def run_daily_rank_batch_via_cli(payload: Dict[str, Any], *, timeout: int = 30) -> Dict[str, Any]:
    """Run Rust daily-rank matrix producer/accounting through the JSON CLI."""

    return _run_engine_service_operation("daily_rank_batch", payload, timeout=timeout)
