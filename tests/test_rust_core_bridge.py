import importlib
import subprocess
import sys
import threading
from pathlib import Path

import pandas as pd
import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def test_rust_accounting_bridge_computes_rotation_turnover():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "checkpoints": [
            {
                "time": "2024-01-02",
                "returns": {},
                "target_weights": {"VOO": 1.0},
            },
            {
                "time": "2024-02-01",
                "returns": {"VOO": 0.10, "GLD": 0.0},
                "target_weights": {"GLD": 1.0},
            },
        ],
    }

    summary = bridge.run_accounting_via_cli(payload, timeout=60)

    assert summary["events"][0]["turnover"] == pytest.approx(1.0)
    assert summary["events"][1]["turnover"] == pytest.approx(2.0)
    assert summary["final_equity"] == pytest.approx(110.0)


def test_rust_bridge_uses_release_profile_by_default(tmp_path):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    binary = tmp_path / "missing_binary.exe"

    command = bridge._rust_bin_command(binary, "engine_service_cli")

    assert Path(command[0]).name.lower() in {"cargo", "cargo.exe", "cargo.cmd"}
    assert command[1:3] == ["run", "--quiet"]
    assert "--release" in command
    assert "--bin" in command


def test_rust_timeline_bridge_reuses_server_for_multiple_requests():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "checkpoints": [
            {
                "date": "2024-01-02",
                "phase": "open",
                "returns": {"AAA": 0.0},
                "actions": [
                    {
                        "action": "set_target_weights",
                        "target_weights": {"AAA": 1.0},
                        "reason": "enter",
                    }
                ],
            },
            {
                "date": "2024-01-02",
                "phase": "close",
                "returns": {"AAA": 0.05},
                "actions": [],
            },
        ],
    }

    first = bridge.run_timeline_accounting_via_cli(payload, timeout=60)
    second = bridge.run_timeline_accounting_via_cli(payload, timeout=60)

    assert first["final_equity"] == pytest.approx(105.0)
    assert second["final_equity"] == pytest.approx(first["final_equity"])


def test_rust_signal_timeline_bridge_reuses_server_for_multiple_requests():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "asset": "AAA",
        "dates": ["2024-01-02", "2024-01-03", "2024-01-04"],
        "open": [100.0, 110.0, 120.0],
        "close": [100.0, 115.0, 118.0],
        "entry_signal": [True, False, False],
        "exit_signal": [False, True, False],
        "target_weight": 1.0,
    }

    first = bridge.run_signal_timeline_via_cli(payload, timeout=60)
    second = bridge.run_signal_timeline_via_cli(payload, timeout=60)

    assert first["active_rebalances"] == 2
    assert first["final_equity"] == pytest.approx(second["final_equity"])


def test_rust_signal_timeline_batch_bridge_returns_summary_and_full_results():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    common = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "asset": "AAA",
        "dates": ["2024-01-02", "2024-01-03", "2024-01-04"],
        "open": [100.0, 110.0, 120.0],
        "close": [100.0, 115.0, 118.0],
    }
    first_payload = {
        **common,
        "entry_signal": [True, False, False],
        "exit_signal": [False, True, False],
        "target_weight": 1.0,
    }
    batch_payload = {
        **common,
        "candidates": [
            {
                "candidate_id": "signal_probe:parameter_matrix:entry_exit",
                "resolved_params": {"short_ma": "10"},
                "entry_signal": [True, False, False],
                "exit_signal": [False, True, False],
                "target_weight": 1.0,
            },
            {
                "candidate_id": "signal_probe:parameter_matrix:no_trade",
                "resolved_params": {"short_ma": "20"},
                "entry_signal": [False, False, False],
                "exit_signal": [False, False, False],
                "target_weight": 1.0,
            },
        ],
    }

    single = bridge.run_signal_timeline_via_cli(first_payload, timeout=60)
    batch = bridge.run_signal_timeline_batch_via_cli(batch_payload, timeout=60)
    full_batch = bridge.run_signal_timeline_batch_via_cli(
        {**batch_payload, "include_full_results": True},
        timeout=60,
    )

    assert batch["candidate_count"] == 2
    assert (
        batch["results"][0]["candidate_id"]
        == "signal_probe:parameter_matrix:entry_exit"
    )
    assert batch["results"][0]["resolved_params"] == {"short_ma": "10"}
    assert batch["results"][0]["final_equity"] == pytest.approx(single["final_equity"])
    assert "sharpe" in batch["results"][0]
    assert "cagr" in batch["results"][0]
    assert "max_drawdown" in batch["results"][0]
    assert (
        batch["results"][1]["candidate_id"]
        == "signal_probe:parameter_matrix:no_trade"
    )
    assert batch["results"][1]["final_equity"] == pytest.approx(100.0)
    assert full_batch["results"][0]["timeline"]["final_equity"] == pytest.approx(single["final_equity"])
    assert len(full_batch["results"][0]["timeline"]["daily_events"]) == 3
    assert len(full_batch["results"][0]["timeline"]["events"]) == 6


def test_rust_reset_timer_batch_bridge_reuses_server_for_multiple_requests():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "assets": ["AAA"],
        "dates": ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "open": {"AAA": [100.0, 101.0, 103.0, 104.0]},
        "close": {"AAA": [100.0, 102.0, 104.0, 105.0]},
        "baseline_weights": {},
        "event_weights": {"AAA": 1.0},
        "restore_weights": {},
        "entry_offset_bars": 0,
        "entry_phase": "open",
        "restore_phase": "close",
        "include_full_results": False,
        "candidates": [
            {
                "candidate_id": "signal_probe:single_backtest:fixed",
                "resolved_params": {"reset_days": "2"},
                "entry_signal": [True, False, False, False],
                "hold_bars": 2,
            }
        ],
    }

    first = bridge.run_reset_timer_batch_via_cli(payload, timeout=300)
    second = bridge.run_reset_timer_batch_via_cli(payload, timeout=300)

    assert first["candidate_count"] == 1
    assert (
        first["results"][0]["candidate_id"]
        == "signal_probe:single_backtest:fixed"
    )
    assert second["results"][0]["final_equity"] == pytest.approx(first["results"][0]["final_equity"])


def test_rust_metrics_batch_bridge_returns_full_metric_rows():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "time_unit": 252,
        "risk_free_rate": 0.02,
        "backtest_ids": [
            "a:parameter_matrix:fixed",
            "b:parameter_matrix:fixed",
        ],
        "equity": [100.0, 110.0, 121.0, 100.0, 90.0, 99.0],
        "bah_equity": [100.0, 105.0, 110.0, 100.0, 95.0, 100.0],
        "session_labels": [
            "2024-01-01", "2024-01-02", "2024-01-03",
            "2024-01-01", "2024-01-02", "2024-01-03",
        ],
        "trade_actions": [0.0, 1.0, 4.0, 0.0, 1.0, 4.0],
        "trade_returns": [None, None, 0.10, None, None, -0.05],
        "position_size": [0.0, 1.0, 0.0, 0.0, 1.0, 0.0],
        "group_start": [0, 3],
        "group_end": [3, 6],
    }

    summary = bridge.run_metrics_batch_via_cli(payload, timeout=60)

    assert summary["row_count"] == 2
    assert summary["metrics"][0]["Backtest_id"] == "a:parameter_matrix:fixed"
    assert summary["metrics"][0]["Total_return"] == pytest.approx(0.21)
    assert summary["metrics"][0]["BAH_Total_return"] == pytest.approx(0.10)
    assert summary["metrics"][0]["Trade_count"] == pytest.approx(1.0)
    assert summary["metrics"][0]["Win_rate"] == pytest.approx(1.0)
    assert summary["metrics"][1]["Backtest_id"] == "b:parameter_matrix:fixed"
    assert summary["metrics"][1]["Total_return"] == pytest.approx(-0.01)
    assert summary["metrics"][1]["Trade_count"] == pytest.approx(1.0)


def test_calendar_same_session_bridge_uses_unified_engine_service(monkeypatch):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")

    monkeypatch.setattr(bridge, "rust_core_available", lambda: True)
    monkeypatch.setattr(
        bridge,
        "_run_engine_service_operation",
        lambda operation, payload, *, timeout: {
            "route": "engine_service",
            "operation": operation,
            "payload": payload,
            "timeout": timeout,
        },
    )

    def _unexpected_subprocess(*args, **kwargs):
        raise AssertionError("calendar same-session bridge should not fall back to subprocess CLI")

    monkeypatch.setattr(bridge.subprocess, "run", _unexpected_subprocess)

    payload = {"candidates": []}
    summary = bridge.run_calendar_same_session_batch_via_cli(payload, timeout=17)

    assert summary["route"] == "engine_service"
    assert summary["operation"] == "calendar_same_session_batch"
    assert summary["payload"] == payload
    assert summary["timeout"] == 17


def test_rank_selection_bridge_uses_unified_engine_service(monkeypatch):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")

    monkeypatch.setattr(bridge, "rust_core_available", lambda: True)
    monkeypatch.setattr(
        bridge,
        "_run_engine_service_operation",
        lambda operation, payload, *, timeout: {
            "route": "engine_service",
            "operation": operation,
            "payload": payload,
            "timeout": timeout,
        },
    )

    def _unexpected_subprocess(*args, **kwargs):
        raise AssertionError("rank-selection bridge should not fall back to subprocess CLI")

    monkeypatch.setattr(bridge.subprocess, "run", _unexpected_subprocess)

    payload = {"rows": 1, "cols": 1}
    summary = bridge.run_rank_selection_via_cli(payload, timeout=23)

    assert summary["route"] == "engine_service"
    assert summary["operation"] == "rank_selection"
    assert summary["payload"] == payload
    assert summary["timeout"] == 23


def test_engine_service_client_enforces_request_timeout():
    from backtester.EngineServiceClient_backtester import EngineServiceClient

    stopped = threading.Event()

    class _Input:
        def write(self, value):
            return len(value)

        def flush(self):
            return None

    class _Output:
        def __iter__(self):
            return self

        def __next__(self):
            stopped.wait()
            raise StopIteration

    class _Process:
        stdin = _Input()
        stdout = _Output()
        returncode = None

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = 0
            stopped.set()

        def wait(self, timeout):
            return self.returncode

        def kill(self):
            self.terminate()

    client = EngineServiceClient(lambda: _Process(), availability_check=lambda: True)

    with pytest.raises(subprocess.TimeoutExpired):
        client.execute("rank_selection", {}, timeout=1)
    client.close()


def test_engine_service_emits_progress_and_recovers_after_process_exit():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")
    client = bridge._ENGINE_SERVICE_CLIENT
    client.close()
    payload = {
        "rows": 1,
        "cols": 3,
        "eligible": [True, True, True],
        "score": [2.0, 3.0, 1.0],
        "ascending": False,
        "top_n": 2,
        "position_limit": 0.4,
    }
    progress: list[dict] = []

    first = client.execute(
        "rank_selection",
        payload,
        timeout=30,
        progress_callback=progress.append,
    )
    first_process = client.process
    assert first_process is not None
    first_process.terminate()
    first_process.wait(timeout=5)

    second = client.execute("rank_selection", payload, timeout=30)
    second_process = client.process

    assert progress == [{"stage": "accepted"}]
    assert first["selected_indices"] == [[1, 0]]
    assert second == first
    assert second_process is not None
    assert second_process is not first_process


def test_engine_service_multiplexes_concurrent_requests_by_request_id():
    from concurrent.futures import ThreadPoolExecutor

    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")
    client = bridge._ENGINE_SERVICE_CLIENT
    payloads = [
        {
            "rows": 1,
            "cols": 3,
            "eligible": [True, True, True],
            "score": [float(index), 2.0, 1.0],
            "ascending": False,
            "top_n": 1,
            "position_limit": 1.0,
        }
        for index in range(1, 9)
    ]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda row: client.execute("rank_selection", row, timeout=30),
                payloads,
            )
        )

    assert len(results) == 8
    assert all(result["rows"] == 1 for result in results)
    assert client.process is not None


def test_engine_service_accepts_cancel_while_request_is_running():
    from concurrent.futures import ThreadPoolExecutor

    from backtester.EngineServiceClient_backtester import EngineServiceError

    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")
    client = bridge._ENGINE_SERVICE_CLIENT
    rows = 2_000
    cols = 1_000
    accepted = threading.Event()
    payload = {
        "rows": rows,
        "cols": cols,
        "eligible": [True] * (rows * cols),
        "score": [float(index) for index in range(cols)] * rows,
        "ascending": False,
        "top_n": 10,
        "position_limit": 0.1,
    }
    request_id = "cancel-running-rank-selection"

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            client.execute,
            "rank_selection",
            payload,
            timeout=30,
            request_id=request_id,
            progress_callback=lambda row: accepted.set(),
        )
        assert accepted.wait(timeout=10)
        cancel_result = client.request(
            "cancel",
            {"target_request_id": request_id},
            timeout=5,
        )
        with pytest.raises(EngineServiceError) as exc_info:
            future.result(timeout=30)

    assert cancel_result["accepted"] is True
    assert cancel_result["target_request_id"] == request_id
    assert exc_info.value.code == "canceled"


def test_daily_rank_accounting_bridge_uses_unified_engine_service(monkeypatch):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")

    monkeypatch.setattr(bridge, "rust_core_available", lambda: True)
    monkeypatch.setattr(
        bridge,
        "_run_engine_service_operation",
        lambda operation, payload, *, timeout: {
            "route": "engine_service",
            "operation": operation,
            "payload": payload,
            "timeout": timeout,
        },
    )

    def _unexpected_subprocess(*args, **kwargs):
        raise AssertionError("daily-rank accounting bridge should not fall back to subprocess CLI")

    monkeypatch.setattr(bridge.subprocess, "run", _unexpected_subprocess)

    payload = {"dates": []}
    summary = bridge.run_daily_rank_accounting_via_cli(payload, timeout=29)

    assert summary["route"] == "engine_service"
    assert summary["operation"] == "daily_rank_accounting"
    assert summary["payload"] == payload
    assert summary["timeout"] == 29


def test_calendar_overlay_batch_bridge_uses_unified_engine_service(monkeypatch):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")

    monkeypatch.setattr(bridge, "rust_core_available", lambda: True)
    monkeypatch.setattr(
        bridge,
        "_run_engine_service_operation",
        lambda operation, payload, *, timeout: {
            "route": "engine_service",
            "operation": operation,
            "payload": payload,
            "timeout": timeout,
        },
    )

    def _unexpected_subprocess(*args, **kwargs):
        raise AssertionError("calendar-overlay batch bridge should not fall back to subprocess CLI")

    monkeypatch.setattr(bridge.subprocess, "run", _unexpected_subprocess)

    payload = {"candidates": []}
    summary = bridge.run_calendar_overlay_batch_via_cli(payload, timeout=31)

    assert summary["route"] == "engine_service"
    assert summary["operation"] == "calendar_overlay_batch"
    assert summary["payload"] == payload
    assert summary["timeout"] == 31


def test_daily_rank_batch_bridge_uses_unified_engine_service(monkeypatch):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")

    monkeypatch.setattr(bridge, "rust_core_available", lambda: True)
    monkeypatch.setattr(
        bridge,
        "_run_engine_service_operation",
        lambda operation, payload, *, timeout: {
            "route": "engine_service",
            "operation": operation,
            "payload": payload,
            "timeout": timeout,
        },
    )

    def _unexpected_subprocess(*args, **kwargs):
        raise AssertionError("daily-rank batch bridge should not fall back to subprocess CLI")

    monkeypatch.setattr(bridge.subprocess, "run", _unexpected_subprocess)

    payload = {"candidates": []}
    summary = bridge.run_daily_rank_batch_via_cli(payload, timeout=37)

    assert summary["route"] == "engine_service"
    assert summary["operation"] == "daily_rank_batch"
    assert summary["payload"] == payload
    assert summary["timeout"] == 37


def test_rust_metrics_trade_stats_only_use_closed_trade_returns():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "time_unit": 252,
        "risk_free_rate": 0.0,
        "backtest_ids": ["a:single_backtest:fixed"],
        "equity": [100.0, 100.0, 110.0, 110.0, 99.0],
        "bah_equity": [100.0, 100.0, 100.0, 100.0, 100.0],
        "session_labels": [
            "2024-01-01", "2024-01-02", "2024-01-03",
            "2024-01-04", "2024-01-05",
        ],
        "trade_actions": [0.0, 1.0, 4.0, 1.0, 4.0],
        "trade_returns": [None, 9.99, 0.10, -9.99, -0.10],
        "position_size": [0.0, 1.0, 0.0, 1.0, 0.0],
        "group_start": [0],
        "group_end": [5],
    }

    summary = bridge.run_metrics_batch_via_cli(payload, timeout=60)
    metrics = summary["metrics"][0]

    assert metrics["Trade_count"] == pytest.approx(2.0)
    assert metrics["Win_rate"] == pytest.approx(0.5)
    assert metrics["Profit_factor"] == pytest.approx(1.0)
    assert metrics["Avg_trade_return"] == pytest.approx(0.0)
    assert metrics["Max_consecutive_losses"] == pytest.approx(1.0)


def test_rust_metrics_parquet_bridge_reuses_server_for_multiple_requests(tmp_path):
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    frame = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-01", "2024-01-02", "2024-01-03"]),
            "Backtest_id": [
                "a:parameter_matrix:fixed",
                "a:parameter_matrix:fixed",
                "a:parameter_matrix:fixed",
                "b:parameter_matrix:fixed",
                "b:parameter_matrix:fixed",
                "b:parameter_matrix:fixed",
            ],
            "Session_label": [
                "2024-01-01", "2024-01-02", "2024-01-03",
                "2024-01-01", "2024-01-02", "2024-01-03",
            ],
            "Equity_value": [100.0, 110.0, 121.0, 100.0, 95.0, 100.0],
            "Close": [100.0, 110.0, 121.0, 100.0, 95.0, 100.0],
            "Trade_action": [0.0, 1.0, 4.0, 0.0, 1.0, 4.0],
            "Trade_return": [None, None, 0.10, None, None, -0.05],
            "Position_size": [0.0, 1.0, 0.0, 0.0, 1.0, 0.0],
        }
    )
    parquet_path = tmp_path / "metrics.parquet"
    frame.to_parquet(parquet_path, index=False)
    payload = {
        "parquet_path": str(parquet_path),
        "time_unit": 252,
        "risk_free_rate": 0.02,
    }

    first = bridge.run_metrics_parquet_via_cli(payload, timeout=300)
    second = bridge.run_metrics_parquet_via_cli(payload, timeout=300)

    assert first["row_count"] == 2
    assert second["row_count"] == first["row_count"]
    assert second["metrics"][0]["Backtest_id"] == first["metrics"][0]["Backtest_id"]


def test_rust_calendar_same_session_batch_bridge_returns_full_results():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "asset": "AAA",
        "dates": ["2024-01-01", "2024-01-02", "2024-01-08"],
        "open": [100.0, 110.0, 120.0],
        "close": [105.0, 111.0, 132.0],
        "include_full_results": True,
        "candidates": [
            {
                "candidate_id": "calendar_probe:parameter_matrix:first_monday",
                "resolved_params": {"month_week": "1", "weekday": "monday"},
                "ordinal": 1,
                "weekday": "monday",
                "target_weight": 1.0,
            },
            {
                "candidate_id": "calendar_probe:parameter_matrix:second_monday",
                "resolved_params": {"month_week": "2", "weekday": "monday"},
                "ordinal": 2,
                "weekday": "monday",
                "target_weight": 1.0,
            },
        ],
    }

    summary = bridge.run_calendar_same_session_batch_via_cli(payload, timeout=60)

    assert summary["candidate_count"] == 2
    assert (
        summary["results"][0]["candidate_id"]
        == "calendar_probe:parameter_matrix:first_monday"
    )
    assert summary["results"][0]["final_equity"] == pytest.approx(105.0)
    assert summary["results"][0]["timeline"]["active_rebalances"] == 2
    assert (
        summary["results"][1]["candidate_id"]
        == "calendar_probe:parameter_matrix:second_monday"
    )
    assert summary["results"][1]["final_equity"] == pytest.approx(110.0)


def test_rust_rank_selection_bridge_matches_daily_rank_rules():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "rows": 2,
        "cols": 3,
        "eligible": [True, True, True, True, False, True],
        "score": [2.0, 3.0, 3.0, 1.0, 99.0, 4.0],
        "ascending": False,
        "top_n": 2,
        "position_limit": 0.4,
    }

    summary = bridge.run_rank_selection_via_cli(payload, timeout=60)

    assert summary["rows"] == 2
    assert summary["cols"] == 3
    assert summary["ranked_indices"][0] == [2, 1, 0]
    assert summary["selected_indices"][0] == [2, 1]
    assert summary["target_weights"][:3] == pytest.approx([0.0, 0.4, 0.4])
    assert summary["ranked_indices"][1] == [2, 0]
    assert summary["selected_indices"][1] == [2, 0]
    assert summary["target_weights"][3:] == pytest.approx([0.4, 0.0, 0.4])


def test_rust_daily_rank_accounting_bridge_returns_equity_events():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "config": {
            "starting_equity": 100.0,
            "cost_rate": 0.0,
            "max_gross_exposure": 1.0,
            "allow_short": False,
        },
        "dates": ["2024-01-01", "2024-01-02"],
        "symbols": ["AAA", "BBB", "CCC"],
        "close": [100.0, 90.0, 80.0, 110.0, 95.0, 70.0],
        "eligible": [True, True, True, True, True, True],
        "score": [100.0, 90.0, 80.0, 110.0, 95.0, 70.0],
        "ascending": False,
        "top_n": 2,
        "position_limit": 0.4,
    }

    summary = bridge.run_daily_rank_accounting_via_cli(payload, timeout=60)

    assert summary["days"] == 2
    assert summary["events"][0]["selected_indices"] == [0, 1]
    assert summary["events"][0]["target_weights"] == pytest.approx([0.4, 0.4, 0.0])
    assert summary["events"][0]["cash_weight"] == pytest.approx(0.2)
    assert summary["final_equity"] > 100.0


def test_rust_plot_bundle_bridge_preserves_multiple_series_and_provenance():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    payload = {
        "run_id": "run-plot",
        "chart_type": "asset_curve_compare",
        "title": "Equity",
        "series": [
            {
                "series_id": "candidate-a",
                "label": "Candidate A",
                "x": ["2024-01-01", "2024-01-02"],
                "y": [100.0, 101.0],
            },
            {
                "series_id": "candidate-b",
                "label": "Candidate B",
                "x": ["2024-01-01", "2024-01-02"],
                "y": [100.0, 99.0],
            },
        ],
        "x_axis": "time",
        "y_axis": "equity",
        "source_hashes": ["a" * 64],
        "artifact_source_refs": ["canonical.json", "metrics.parquet"],
        "generated_at": "2026-07-11T00:00:00Z",
    }

    result = bridge.run_plot_bundle_via_cli(payload, timeout=60)

    assert result["schema_version"] == "plot_bundle.v1"
    assert [item["series_id"] for item in result["series"]] == [
        "candidate-a",
        "candidate-b",
    ]
    assert result["series"][1]["y"] == [100.0, 99.0]
    assert result["source_hashes"] == ["a" * 64]


def test_rust_backtest_detail_bridge_pairs_trade_and_preserves_equity():
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    if not bridge.rust_core_available():
        pytest.skip("Rust core is unavailable")

    result = bridge.run_backtest_detail_bundle_via_cli(
        {
            "run_id": "run-detail",
            "backtest_id": "candidate-a:single_backtest:fixed",
            "label": "Candidate A",
            "asset": "AAA",
            "time": ["2024-01-01", "2024-01-02"],
            "session_labels": ["2024-01-01", "2024-01-02"],
            "open": [100.0, 110.0],
            "high": [101.0, 112.0],
            "low": [99.0, 109.0],
            "close": [100.0, 111.0],
            "equity": [100.0, 111.0],
            "benchmark_equity": [100.0, 110.0],
            "trade_action": [1, 4],
            "metrics_matrix": {"Sharpe": 1.5},
            "source_hashes": ["a" * 64],
            "artifact_source_refs": ["canonical.json"],
            "generated_at": "2026-07-11T00:00:00Z",
        },
        timeout=60,
    )

    assert result["schema_version"] == "backtest_detail_bundle.v3"
    assert result["equity_series"][1]["value"] == 111.0
    assert result["trade_rows"][0]["trade_return"] == pytest.approx(0.11)
