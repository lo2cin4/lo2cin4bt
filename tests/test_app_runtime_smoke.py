import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from app.runtime.registry import AppRegistry
from app.runtime.module_identity import VALIDATION_WORKFLOW_CANONICAL
from app.runtime.runtime import AppRuntimeService
from dataloader.market_data_loader import MarketDataContractError


REPO_ROOT = Path(__file__).resolve().parents[1]


def _typed_daily_data(provider: str, **extra: object) -> dict[str, object]:
    stream_id = "execution_daily"
    return {
        "provider": provider,
        "bar_time": {
            "schema_version": "bar_time_contract.v1",
            "contract_id": "lo2cin4bt.bar_time_contract.v1",
            "session_model": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "session_scope": "regular",
                "session_label_policy": "exchange_local_date",
                "non_session_bar_policy": "reject",
            },
            "timestamp_model": {
                "time_standard": "UTC",
                "precision": "nanosecond",
                "clock": "historical_available_time",
                "ordering": (
                    "available_time_then_event_time_then_external_execution_sequence"
                    "_then_lifecycle_stage_then_stream_id_then_source_sequence"
                ),
            },
            "price_model": {
                "price_basis": "split_dividend_adjusted",
                "corporate_action_policy": "provider_applied",
            },
            "streams": [
                {
                    "stream_id": stream_id,
                    "role": "execution",
                    "source": {"kind": "external", "provider_id": provider},
                    "bar_spec": {
                        "aggregation": "time",
                        "step": 1,
                        "unit": "day",
                        "price_type": "last",
                        "alignment": "session_open",
                    },
                    "timestamp_semantics": {
                        "timestamp_convention": "bar_close",
                        "interval_boundary": "left_open_right_closed",
                        "bar_open_time_column": "bar_open_timestamp",
                        "bar_close_time_column": "bar_close_timestamp",
                        "available_time_column": "available_timestamp",
                        "session_label_column": "session_label",
                        "availability_policy": "bar_close",
                    },
                }
            ],
        },
        "stream_binding": {
            "execution_stream_id": stream_id,
            "decision_stream_id": stream_id,
        },
        **extra,
    }


def _typed_dataloader(
    provider: str,
    *,
    source: str,
    symbols: list[str],
    **extra: object,
) -> dict[str, object]:
    data = _typed_daily_data(provider)
    return {
        "source": source,
        "provider": provider,
        "asset_symbols": symbols,
        "bar_time": data["bar_time"],
        "stream_binding": data["stream_binding"],
        **extra,
    }


def _valid_result_report(result_hash: str = "a" * 64) -> dict:
    return {
        "schema_version": "result_validation_report.v1",
        "status": "valid",
        "result_schema_version": "rust_accounting_result_tables.v1",
        "result_hash": result_hash,
        "table_row_counts": {"equity_curve": 1},
        "checks": [{"check_id": "schema_version", "status": "passed", "message": "ok"}],
        "errors": [],
        "warnings": [],
    }


def test_result_validation_stage_is_mandatory_and_writes_batch_report(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    run_id = "validated_run"
    stage_status = runtime._new_stage_status(run_id, "autorunner")  # noqa: SLF001
    result = SimpleNamespace(
        validation_report={"result_validation": _valid_result_report()}
    )
    emitted = []

    payload = runtime._run_result_validation_stage(  # noqa: SLF001
        run_id=run_id,
        stage_status=stage_status,
        backtest_results={"portfolio_results": [result]},
        emit=lambda stage, message: emitted.append((stage, message)),
    )

    assert payload["status"] == "valid"
    assert payload["validated_results"] == 1
    valid_stage = next(item for item in stage_status["stages"] if item["stage"] == "valid")
    assert valid_stage["status"] == "completed"
    assert valid_stage["optional"] is False
    assert emitted[-1][0] == "valid"
    written = runtime.registry.build_run_paths(run_id)["snapshot_dir"] / "result_validation_report.json"
    assert json.loads(written.read_text(encoding="utf-8"))["validated_results"] == 1


def test_result_validation_stage_fails_closed_when_report_is_missing(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    stage_status = runtime._new_stage_status("invalid_run", "autorunner")  # noqa: SLF001
    result = SimpleNamespace(validation_report={})

    with pytest.raises(ValueError, match="ResultValidationReport"):
        runtime._run_result_validation_stage(  # noqa: SLF001
            run_id="invalid_run",
            stage_status=stage_status,
            backtest_results={"portfolio_results": [result]},
            emit=lambda _stage, _message: None,
        )


def test_empty_dataloader_health_does_not_report_zero_missing_ratio(
    tmp_path: Path,
) -> None:
    runtime = AppRuntimeService(tmp_path)

    health = runtime._build_dataloader_health(  # noqa: SLF001
        run_id="empty_data",
        dataloader_config={},
        data=pd.DataFrame(),
        primary_artifact=None,
    )
    lineage = runtime._lineage_audit(  # noqa: SLF001
        pd.DataFrame(),
        health,
    )

    assert health["missing_ratio"] is None
    assert lineage["missing_ratio"] is None


def test_metricstracker_stage_is_mandatory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AppRuntimeService(tmp_path)
    stage_status = runtime._new_stage_status("metrics_run", "autorunner")  # noqa: SLF001
    emitted = []

    class SuccessfulMetricsRunner:
        def __init__(self, logger: object) -> None:
            self.logger = logger

        def run(self, *_args: object, **_kwargs: object) -> dict:
            return {
                "enabled": True,
                "executed": True,
                "success": 1,
                "failed": 0,
                "tasks": [],
            }

    monkeypatch.setattr(
        "app.runtime.runtime._autorunner_metrics_runner_cls",
        lambda: SuccessfulMetricsRunner,
    )

    summary = runtime._run_metrics_stage(  # noqa: SLF001
        stage_status,
        {},
        {"enable_metrics_analysis": True},
        lambda stage, message: emitted.append((stage, message)),
    )

    metrics_stage = next(
        item for item in stage_status["stages"] if item["stage"] == "metricstracker"
    )
    assert metrics_stage["optional"] is False
    assert metrics_stage["status"] == "completed"
    assert summary["success"] == 1
    assert emitted[-1][0] == "metricstracker"


def test_metricstracker_stage_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AppRuntimeService(tmp_path)
    stage_status = runtime._new_stage_status("metrics_run", "autorunner")  # noqa: SLF001

    class FailedMetricsRunner:
        def __init__(self, logger: object) -> None:
            self.logger = logger

        def run(self, *_args: object, **_kwargs: object) -> dict:
            return {
                "enabled": True,
                "executed": True,
                "success": 0,
                "failed": 1,
                "tasks": [{"status": "failed"}],
            }

    monkeypatch.setattr(
        "app.runtime.runtime._autorunner_metrics_runner_cls",
        lambda: FailedMetricsRunner,
    )

    with pytest.raises(RuntimeError, match="exactly one successful"):
        runtime._run_metrics_stage(  # noqa: SLF001
            stage_status,
            {},
            {"enable_metrics_analysis": True},
            lambda _stage, _message: None,
        )

    metrics_stage = next(
        item for item in stage_status["stages"] if item["stage"] == "metricstracker"
    )
    assert metrics_stage["optional"] is False
    assert metrics_stage["status"] == "failed"


def test_result_validation_stage_taxonomy_keeps_wfa_as_optional_workflow(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    autorunner = runtime._new_stage_status("autorunner", "autorunner")  # noqa: SLF001
    wfa = runtime._new_stage_status("wfa", VALIDATION_WORKFLOW_CANONICAL)  # noqa: SLF001

    autorunner_valid = next(item for item in autorunner["stages"] if item["stage"] == "valid")
    wfa_valid = next(item for item in wfa["stages"] if item["stage"] == "valid")
    assert autorunner_valid == {
        "stage": "valid",
        "status": "pending",
        "optional": False,
        "message": None,
    }
    assert wfa_valid["status"] == "skipped"
    assert next(
        item for item in wfa["stages"] if item["stage"] == VALIDATION_WORKFLOW_CANONICAL
    )["status"] == "pending"


def test_app_registry_cleanup_removes_screenshot_bundle_with_run(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    run_id = "cleanup_with_screenshots"
    paths = registry.build_run_paths(run_id)
    registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "completed",
            "created_at": "2026-07-18T00:00:00+08:00",
        }
    )
    registry.write_stage_status(run_id, {"run_id": run_id, "status": "completed"})
    registry.write_artifact_manifest(run_id, {"run_id": run_id, "status": "completed"})
    registry.write_snapshot_file(run_id, "strategy_run.json", {"schema_version": "strategy_run"})
    (paths["chart_payload_dir"] / "payload.json").write_text("{}", encoding="utf-8")
    (paths["ai_review_dir"] / "ai_review_pack.json").write_text("{}", encoding="utf-8")
    capture_dir = paths["screenshot_dir"] / "capture"
    capture_dir.mkdir(parents=True)
    (capture_dir / "equity_curve.png").write_bytes(b"png")

    removed = set(registry.delete_run_artifacts(run_id))

    for key in (
        "run_registry",
        "artifact_manifest",
        "stage_status",
        "snapshot_dir",
        "chart_payload_dir",
        "ai_review_dir",
        "screenshot_dir",
    ):
        assert str(paths[key]) in removed
        assert not paths[key].exists()
    assert registry.list_runs() == []


def test_app_registry_closes_interrupted_runs(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    run_id = "stale_running"
    stage_status = {
        "run_id": run_id,
        "module": VALIDATION_WORKFLOW_CANONICAL,
        "status": "running",
        "current_stage": "config_validation",
        "stages": [
            {"stage": "config_validation", "status": "pending", "optional": False, "message": None},
            {
                "stage": VALIDATION_WORKFLOW_CANONICAL,
                "status": "pending",
                "optional": False,
                "message": None,
            },
        ],
    }
    registry.write_stage_status(run_id, stage_status)
    registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "entrypoint": "app-run-center",
            "status": "running",
            "created_at": "2026-06-24T12:00:00+08:00",
            "completed_at": None,
            "error_count": 0,
        }
    )

    closed = registry.fail_interrupted_runs(
        completed_at="2026-06-24T12:05:00+08:00",
        message="interrupted",
    )

    assert closed == 1
    assert registry.load_registry_entry(run_id)["status"] == "failed"
    assert registry.list_runs()[0]["status"] == "failed"
    updated_stage = registry.load_stage_status(run_id)
    assert updated_stage["status"] == "failed"
    assert updated_stage["stages"][0]["status"] == "failed"
    assert updated_stage["stages"][0]["message"] == "interrupted"


def test_app_registry_cleanup_skips_current_session_and_direct_runtime(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    keep_ids = ["same_session_run", "direct_runtime_run"]
    for run_id in keep_ids:
        registry.write_stage_status(
            run_id,
            {
                "run_id": run_id,
                "module": "autorunner",
                "status": "running",
                "current_stage": "backtester",
                "stages": [
                    {"stage": "config_validation", "status": "completed", "optional": False, "message": "ok"},
                    {"stage": "backtester", "status": "running", "optional": False, "message": "still computing"},
                ],
            },
        )

    registry.write_registry_entry(
        {
            "run_id": "same_session_run",
            "module": "autorunner",
            "entrypoint": "app-run-center",
            "owner_type": "app_server",
            "server_session_id": "app-current",
            "status": "running",
            "created_at": "2026-07-02T00:00:00+08:00",
            "completed_at": None,
            "error_count": 0,
        }
    )
    registry.write_registry_entry(
        {
            "run_id": "direct_runtime_run",
            "module": "autorunner",
            "entrypoint": "runtime-direct",
            "owner_type": "direct_runtime",
            "server_session_id": None,
            "status": "running",
            "created_at": "2026-07-02T00:00:00+08:00",
            "completed_at": None,
            "error_count": 0,
        }
    )

    closed = registry.fail_interrupted_runs(
        completed_at="2026-07-02T00:05:00+08:00",
        message="interrupted",
        current_server_session_id="app-current",
    )

    assert closed == 0
    assert registry.load_registry_entry("same_session_run")["status"] == "running"
    assert registry.load_registry_entry("direct_runtime_run")["status"] == "running"


def test_app_runtime_lists_workspace_configs() -> None:
    runtime = AppRuntimeService(REPO_ROOT)

    run_configs = runtime.list_run_configs()
    wfa_configs = runtime.list_wfa_configs()
    statanalyser_configs = runtime.list_statanalyser_configs()

    assert run_configs
    assert wfa_configs
    assert statanalyser_configs == []
    assert all(item["value"].endswith(".json") for item in run_configs)
    assert all(item["value"].endswith(".json") for item in wfa_configs)
    assert not any(
        item["value"].endswith("strategy-run-btcusdt-binance-daily-dual-ma-example.json")
        for item in run_configs
    )
    assert any(
        item["value"].endswith("strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json")
        for item in run_configs
    )
    assert any(
        item["value"].endswith(
            "strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json"
        )
        for item in run_configs
    )
    assert any(
        item["value"].endswith("wfa-run-qqq-yfinance-daily-sma-cross-example.json")
        for item in wfa_configs
    )
    assert any(
        item["value"].endswith("wfa-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json")
        for item in wfa_configs
    )
    assert any(
        item["value"].endswith("wfa-run-btcusdt-binance-monthly-nth-weekday-same-session-example.json")
        for item in wfa_configs
    )


def test_app_runtime_rejects_corrupt_workspace_config(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    config_dir = tmp_path / "workspace" / "runs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "broken.json").write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="broken.json"):
        runtime._list_configs(config_dir, "autorunner")  # noqa: SLF001


def test_app_runtime_rejects_non_object_workspace_config(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    config_dir = tmp_path / "workspace" / "runs"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "array.json").write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        runtime._list_configs(config_dir, "autorunner")  # noqa: SLF001


def test_app_runtime_rejects_corrupt_config_metadata(tmp_path: Path) -> None:
    config_path = tmp_path / "broken.json"
    config_path.write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="broken.json"):
        AppRuntimeService._load_app_config_metadata(config_path)  # noqa: SLF001


def test_app_runtime_materializes_included_examples_into_empty_workspace(tmp_path: Path) -> None:
    import shutil

    repo = tmp_path / "repo"
    examples = repo / "backtester" / "contracts" / "strategy" / "examples"
    examples.mkdir(parents=True)
    expected_runs = [
        "strategy-run-btcusdt-binance-1m-sma-10-20-example.json",
        "strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json",
        "strategy-run-qqq-tlt-gld-yfinance-monthly-hedge-overlay-example.json",
        "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
        "strategy-run-spy-qqq-yfinance-monthly-pair-spread-example.json",
        "strategy-run-us-etf-yfinance-daily-selection-timing-momentum-sma-example.json",
        "strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json",
        "strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json",
        "strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json",
    ]
    from app.runtime.runtime import INCLUDED_STRATEGY_EXAMPLE_FILENAMES

    assert set(expected_runs) == set(INCLUDED_STRATEGY_EXAMPLE_FILENAMES)
    assert len(INCLUDED_STRATEGY_EXAMPLE_FILENAMES) == 9
    public_examples = REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples"
    assert len(list(public_examples.glob("strategy-run-*.json"))) == 9
    assert "9 個公開內建回測範例" in (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    assert "nine backtest examples" in (REPO_ROOT / "README.en.md").read_text(
        encoding="utf-8"
    )
    expected_wfas = [
        "wfa-run-qqq-yfinance-daily-sma-cross-example.json",
        "wfa-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json",
        "wfa-run-btcusdt-binance-monthly-nth-weekday-same-session-example.json",
    ]
    for filename in [*expected_runs, *expected_wfas]:
        shutil.copy2(
            REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples" / filename,
            examples / filename,
        )

    runtime = AppRuntimeService(repo)

    run_configs = runtime.list_run_configs()
    wfa_configs = runtime.list_wfa_configs()
    for filename in expected_runs:
        run_example = repo / "workspace" / "runs" / filename
        assert run_example.exists()
        assert any(item["value"] == str(run_example.resolve()) for item in run_configs)
    for filename in expected_wfas:
        wfa_example = repo / "workspace" / "wfa" / filename
        assert wfa_example.exists()
        assert any(item["value"] == str(wfa_example.resolve()) for item in wfa_configs)


def test_app_runtime_compiles_single_strategy_run_to_engine_request() -> None:
    from backtester.EngineRequest_backtester import build_engine_request

    runtime = AppRuntimeService(REPO_ROOT)
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal",
            "workflow_id": "single_backtest",
        },
        "data": _typed_daily_data(
            "file",
            file_path="tests/fixtures/smoke/price_data_ma_cross.csv",
            date_column="Time",
            price_column="Close",
        ),
        "universe": {"symbols": ["TEST"]},
        "computed_fields": [],
        "signals": {
            "entry": {"field": "close", "op": "gt", "value": 0},
            "exit": {"field": "close", "op": "lt", "value": 0},
        },
        "allocation": {},
        "fill_model": {},
        "risk": {},
        "parameter_domains": {},
        "outputs": {},
        "metadata": {"strategy_id": "single_primary_probe"},
    }

    request = build_engine_request(config)

    assert request["schema_version"] == "engine_request.v2"
    assert request["strategy"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert request["strategy"]["strategy_profile_id"] == "selection_timing_portfolio"
    assert request["data_requirements"]["provider"] == "file"
    assert request["data_requirements"]["symbols"] == ["TEST"]
    assert "MarketDataBundle dataloader" in runtime._data_request_profile(config)  # pylint: disable=protected-access


def test_app_runtime_compiles_multi_strategy_run_to_same_engine_request() -> None:
    from backtester.EngineRequest_backtester import build_engine_request

    path = (
        REPO_ROOT
        / "backtester"
        / "contracts"
        / "strategy"
        / "examples"
        / "strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json"
    )
    config = json.loads(path.read_text(encoding="utf-8"))

    request = build_engine_request(config)

    assert request["schema_version"] == "engine_request.v2"
    assert request["strategy"]["strategy_mode_id"] == "multi_asset_portfolio"
    assert request["strategy"]["strategy_profile_id"] == "rotation_portfolio"
    assert request["data_requirements"]["symbols"] == ["VOO", "GLD"]
    assert request["workflow"]["run_scope"] == "single"


def test_app_runtime_routes_unified_selection_profile_through_bundle_dataloader() -> None:
    runtime = AppRuntimeService(REPO_ROOT)
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal",
            "workflow_id": "single_backtest",
        },
        "data": _typed_daily_data("yfinance", start_date="2020-01-01"),
        "universe": {"symbols": ["QQQ"]},
    }

    profile = runtime._data_request_profile(config)  # pylint: disable=protected-access

    assert "MarketDataBundle dataloader" in profile
    assert "provider=yfinance" in profile


def test_app_runtime_rejects_legacy_frequency_before_engine_request() -> None:
    from backtester.EngineRequest_backtester import build_engine_request

    config = {
        "schema_version": "strategy_run",
        "platform": {"strategy_mode_id": "multi_asset_portfolio", "workflow_id": "single_backtest"},
        "data": {"provider": "yfinance", "frequency": "5m", "start_date": "2020-01-01"},
        "universe": {"symbols": ["VOO", "GLD"]},
        "computed_fields": [],
        "selection": {},
        "allocation": {"method": "equal_weight", "position_limit": 1.0},
        "rebalance": {"trigger": {"op": "calendar.every_session"}},
        "fill_model": {"timing": "signal_close_for_next_bar", "price": "close_to_close"},
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "outputs": {"equity_curve": True},
        "metadata": {"strategy_id": "subdaily_app_runtime_probe"},
    }

    with pytest.raises(ValueError, match="legacy frequency fields"):
        build_engine_request(config)


def test_app_runtime_backtest_stage_message_describes_full_matrix_work() -> None:
    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal",
        },
        "parameter_domains": {
            "short_ma": {"type": "range", "start": 10, "end": 20, "step": 5},
            "long_ma": {"values": [100, 120]},
        },
    }

    message = AppRuntimeService._running_stage_message("Running backtest", config)  # pylint: disable=protected-access

    assert "provider market data" not in message
    assert "Rust full-matrix" in message
    assert "6 parameter candidates" in message
    assert "full result artifacts" in message


def test_app_runtime_counts_large_parameter_matrix_candidates() -> None:
    config = {
        "schema_version": "strategy_run",
        "platform": {"strategy_mode_id": "multi_asset_portfolio", "workflow_id": "parameter_matrix"},
        "parameter_domains": {
            "fast_window": {"type": "range", "start": 10, "end": 35, "step": 1},
            "slow_window": {"type": "range", "start": 100, "end": 200, "step": 10},
        },
    }

    count = AppRuntimeService._parameter_candidate_count(config)  # pylint: disable=protected-access

    assert count == 286


def test_app_runtime_wfa_stage_message_uses_sampled_candidate_budget() -> None:
    config = {
        "strategy_run_config": {
            "schema_version": "strategy_run",
            "platform": {
                "strategy_mode_id": "multi_asset_portfolio",
                "strategy_profile_id": "selection_timing_portfolio",
                "strategy_preset_id": "single_asset_signal",
                "workflow_id": "parameter_matrix",
            },
            "parameter_domains": {
                "short_ma": {"type": "range", "start": 20, "end": 100, "step": 10},
                "long_ma": {"type": "range", "start": 120, "end": 300, "step": 10},
            },
        },
        "optimizer": {"candidate_limit": 60},
    }

    message = AppRuntimeService._running_stage_message("Running WFA", config)  # pylint: disable=protected-access

    assert "60 parameter candidates" in message
    assert "171 parameter candidates" not in message


def test_app_runtime_default_init_does_not_close_live_runs(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    run_id = "live_run"
    registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "entrypoint": "app-run-center",
            "status": "running",
            "created_at": "2026-07-01T12:00:00+08:00",
            "completed_at": None,
            "error_count": 0,
        }
    )
    registry.write_stage_status(
        run_id,
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "running",
            "current_stage": "backtester",
            "stages": [
                {"stage": "config_validation", "status": "completed", "optional": False, "message": "ok"},
                {"stage": "dataloader", "status": "completed", "optional": False, "message": "ok"},
                {"stage": "backtester", "status": "running", "optional": False, "message": "still computing"},
            ],
        },
    )

    AppRuntimeService(tmp_path)

    assert registry.load_registry_entry(run_id)["status"] == "running"
    assert registry.load_stage_status(run_id)["status"] == "running"


def test_app_runtime_base_registry_marks_app_server_ownership(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path, app_server_session_id="app-session")

    payload = runtime._base_registry(  # pylint: disable=protected-access
        run_id="owned_run",
        module="autorunner",
        entrypoint="app-run-center",
        status="running",
    )

    assert payload["owner_type"] == "app_server"
    assert payload["server_session_id"] == "app-session"


def test_app_runtime_writes_portfolio_matrix_summary_artifact(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    path = runtime._write_portfolio_matrix_summary(  # pylint: disable=protected-access
        "matrix_summary_run",
        {
            "portfolio_matrix_summary": {
                "schema_version": "portfolio_matrix_summary.v1",
                "variant_count": 2,
                "row_count": 2,
                "retained_result_count": 2,
                "compact_result_count": 0,
                "coverage": "all_candidates",
                "rows": [
                    {
                        "backtest_id": "matrix_probe:parameter_matrix:candidate_1",
                        "strategy_id": "matrix_probe:parameter_matrix:candidate_1",
                        "semantic_combo": {"short_ma": "20", "long_ma": "100"},
                        "total_return": 0.01,
                    }
                ],
            }
        },
    )

    assert path is not None
    assert path.name == "portfolio_matrix_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "matrix_summary_run"
    assert payload["row_count"] == 2
    manifest = runtime._build_artifact_manifest("matrix_summary_run", [path])  # pylint: disable=protected-access
    assert manifest["artifacts"][0]["artifact_type"] == "portfolio_matrix_summary_json"


def test_app_runtime_classifies_shortened_portfolio_artifacts(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    portfolio_dir = tmp_path / "managed_artifacts" / "portfolio"
    metrics_dir = tmp_path / "managed_artifacts" / "metricstracker"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    summary = portfolio_dir / "backtest_demo_portfolio_matrix_summary_ab12cd.json"
    portfolio_metadata = portfolio_dir / "backtest_demo_portfolio-metadata_ab12cd.json"
    metrics_metadata = metrics_dir / "backtest_demo_portfolio-metadata_ab12cd.json"

    assert runtime._classify_artifact(summary)[0] == "portfolio_matrix_summary_json"  # pylint: disable=protected-access
    assert runtime._classify_artifact(portfolio_metadata)[0] == "portfolio_metadata_json"  # pylint: disable=protected-access
    assert runtime._classify_artifact(metrics_metadata)[0] != "portfolio_metadata_json"  # pylint: disable=protected-access


def test_data_lineage_manifest_hashes_local_file_source(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    data_path = tmp_path / "workspace" / "datasets" / "prices.csv"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_text("Time,Close\n2026-01-01,100\n2026-01-02,101\n", encoding="utf-8")
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Close": [100.0, 101.0],
        }
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_local",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "file",
            source="file",
            symbols=["TEST"],
            file_config={"file_path": "workspace/datasets/prices.csv"},
        ),
        data=data,
        raw_config={
            "data": _typed_daily_data(
                "file",
                file_path="workspace/datasets/prices.csv",
            ),
            "universe": {"symbols": ["TEST"], "universe_policy": "fixed_symbols"},
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["lineage_status"] == "partial"
    assert manifest["coverage_level"] == "run"
    source = manifest["input_sources"][0]
    assert source["source_type"] == "file"
    assert source["content_hash"]
    assert source["uri_or_path"] == "workspace\\datasets\\prices.csv" or source["uri_or_path"] == "workspace/datasets/prices.csv"
    assert source["actual_start"].startswith("2026-01-01")
    assert manifest["audit"]["row_count"] == 2
    assert manifest["validity_flags"]["survivorship_known"] is False
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "high"
    assert "Configured universe symbols may be a current/static list with survivorship bias." in manifest["lineage_claims"]["unknown"]


def test_data_lineage_manifest_keeps_provider_source_partial() -> None:
    runtime = AppRuntimeService(REPO_ROOT)
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Close": [100.0, 101.0],
        }
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_provider",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["QQQ"],
        ),
        data=data,
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {"symbols": ["QQQ"]},
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["lineage_status"] == "partial"
    assert manifest["input_sources"][0]["source_type"] == "provider"
    assert manifest["input_sources"][0]["content_hash"] is None
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "high"
    assert "Provider content hash is not available." in manifest["lineage_claims"]["unknown"]


def test_data_lineage_manifest_writes_consumed_provider_snapshot(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Close": [100.0, 101.0],
        }
    )

    manifest = runtime._write_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_snapshot",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["QQQ"],
        ),
        data=data,
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {"symbols": ["QQQ"]},
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    snapshot = manifest["consumed_data_snapshot"]
    assert snapshot["status"] == "captured"
    assert snapshot["format"] == "parquet"
    assert snapshot["content_hash"].startswith("sha256:")
    assert (tmp_path / snapshot["path"]).is_file()
    assert manifest["input_sources"][0]["content_hash"] == snapshot["content_hash"]
    assert manifest["input_sources"][0]["cache"]["status"] == "captured"
    assert "Provider content hash is not available." not in manifest["lineage_claims"]["unknown"]


def test_consumed_data_snapshot_has_no_csv_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AppRuntimeService(tmp_path)
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01"]),
            "Close": [100.0],
        }
    )

    def fail_parquet(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("parquet unavailable")

    monkeypatch.setattr(data, "to_parquet", fail_parquet)

    with pytest.raises(RuntimeError, match="parquet unavailable"):
        runtime._write_consumed_data_snapshot("snapshot_failure", data)  # noqa: SLF001

    snapshot_dir = runtime.registry.build_run_paths("snapshot_failure")["snapshot_dir"]
    assert not (snapshot_dir / "consumed_market_data.csv").exists()


def test_data_lineage_manifest_point_in_time_universe_sets_low_survivorship_risk(tmp_path: Path) -> None:
    runtime = AppRuntimeService(REPO_ROOT)
    constituents_path = tmp_path / "historical_constituents.csv"
    constituents_path.write_text(
        "symbol,effective_start,effective_end\n"
        "AAA,2019-01-01,\n"
        "BBB,2019-01-01,\n",
        encoding="utf-8",
    )
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Close": [100.0, 101.0],
        }
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_pit",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA", "BBB"],
        ),
        data=data,
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {
                "symbols": ["AAA", "BBB"],
                "universe_policy": "point_in_time_snapshot",
                "historical_constituents_path": str(constituents_path),
                "as_of_date": "2020-01-01",
                "delisted_policy": "include_when_historically_tradable",
            },
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["validity_flags"]["point_in_time_known"] is True
    assert manifest["validity_flags"]["survivorship_known"] is True
    assert manifest["universe_provenance"]["source_type"] == "historical_universe_constituents"
    assert manifest["universe_provenance"]["point_in_time_constituents"] is True
    assert manifest["universe_provenance"]["constituents_validation"]["status"] == "valid"
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "low"


def test_data_lineage_manifest_snapshot_date_only_constituents_require_exact_as_of(tmp_path: Path) -> None:
    runtime = AppRuntimeService(REPO_ROOT)
    constituents_path = tmp_path / "historical_constituents.csv"
    constituents_path.write_text(
        "symbol,snapshot_date\n"
        "AAA,2019-01-01\n"
        "BBB,2019-01-01\n",
        encoding="utf-8",
    )
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Close": [100.0, 101.0],
        }
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_stale_snapshot_constituents",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA", "BBB"],
        ),
        data=data,
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {
                "symbols": ["AAA", "BBB"],
                "universe_policy": "point_in_time_snapshot",
                "historical_constituents_path": str(constituents_path),
                "as_of_date": "2020-01-01",
                "delisted_policy": "include_when_historically_tradable",
            },
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    validation = manifest["universe_provenance"]["constituents_validation"]
    assert manifest["validity_flags"]["point_in_time_known"] is False
    assert manifest["validity_flags"]["survivorship_known"] is False
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "medium"
    assert validation["status"] == "invalid"
    assert "historical_constituents_exact_as_of_snapshot_missing" in validation["errors"]
    assert "historical_constituents_content_validation_failed" in manifest["audit"]["warnings"]


def test_data_lineage_manifest_current_provider_source_cannot_prove_survivorship() -> None:
    runtime = AppRuntimeService(REPO_ROOT)
    data = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "Close": [100.0, 101.0],
        }
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_current_provider",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA"],
        ),
        data=data,
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {
                "symbols": ["AAA"],
                "universe_policy": "point_in_time_snapshot",
                "source_type": "current_provider_list",
                "source": "sp500",
                "as_of_date": "2020-01-01",
                "delisted_policy": "include_when_historically_tradable",
            },
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["validity_flags"]["point_in_time_known"] is False
    assert manifest["validity_flags"]["survivorship_known"] is False
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "medium"
    assert "current_or_static_universe_source_not_point_in_time" in manifest["audit"]["warnings"]


def test_data_lineage_manifest_factor_audit_blocks_unproven_pit_factor() -> None:
    runtime = AppRuntimeService(REPO_ROOT)

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_factor_audit",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA"],
        ),
        data=pd.DataFrame({"Time": pd.to_datetime(["2026-01-01"]), "Close": [100.0]}),
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {"symbols": ["AAA"]},
            "factor_pipeline": {
                "schema_version": "factor_pipeline.v1",
                "data_requirements": {"point_in_time_required": True},
                "construction": [{"name": "value", "op": "factor.book_to_market"}],
            },
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["factor_feature_audit"]["status"] == "invalid"
    assert manifest["validity_flags"]["feature_lag_verified"] is False
    assert "factor_point_in_time_metadata_missing" in manifest["factor_feature_audit"]["errors"]
    assert "Factor point-in-time metadata or feature lag audit is not valid." in manifest["lineage_claims"]["unknown"]


def test_data_lineage_manifest_fixed_source_type_cannot_prove_survivorship() -> None:
    runtime = AppRuntimeService(REPO_ROOT)

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_fixed_source_type",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA"],
        ),
        data=pd.DataFrame({"Time": pd.to_datetime(["2026-01-01"]), "Close": [100.0]}),
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {
                "symbols": ["AAA"],
                "universe_policy": "point_in_time_snapshot",
                "source_type": "fixed_symbols",
                "historical_constituents_path": "workspace/universe/current_symbols.parquet",
                "as_of_date": "2020-01-01",
                "delisted_policy": "include_when_historically_tradable",
            },
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["validity_flags"]["survivorship_known"] is False
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "medium"
    assert "current_or_static_universe_source_not_point_in_time" in manifest["audit"]["warnings"]


def test_data_lineage_manifest_historical_constituents_path_requires_as_of_date() -> None:
    runtime = AppRuntimeService(REPO_ROOT)

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_missing_as_of",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA"],
        ),
        data=pd.DataFrame({"Time": pd.to_datetime(["2026-01-01"]), "Close": [100.0]}),
        raw_config={
            "data": _typed_daily_data("yfinance"),
            "universe": {
                "symbols": ["AAA"],
                "universe_policy": "point_in_time_snapshot",
                "historical_constituents_path": "workspace/universe/historical_constituents.parquet",
                "delisted_policy": "include_when_historically_tradable",
            },
            "fill_model": {"timing": "next_bar_after_signal"},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["validity_flags"]["point_in_time_known"] is False
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "medium"
    assert "point_in_time_universe_claim_missing_as_of_date" in manifest["audit"]["warnings"]


def test_data_lineage_manifest_internal_strategy_loader_is_partial() -> None:
    runtime = AppRuntimeService(REPO_ROOT)

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_internal",
        module="autorunner",
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="strategy_run_market_data",
            symbols=["VOO", "GLD"],
        ),
        data=pd.DataFrame(),
        raw_config={
            "schema_version": "strategy_run",
            "data": _typed_daily_data("yfinance"),
            "universe": {"symbols": ["VOO", "GLD"]},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
    )

    assert manifest["lineage_status"] == "partial"
    assert manifest["input_sources"][0]["source_type"] == "generated"
    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "high"
    assert "Internal market loader did not expose a consumed data content snapshot." in manifest["lineage_claims"]["unknown"]


def test_data_lineage_manifest_captures_wfa_windows() -> None:
    runtime = AppRuntimeService(REPO_ROOT)
    selected = pd.DataFrame(
        [
            {
                "window_id": 1,
                "train_start": pd.Timestamp("2020-01-01"),
                "train_end": pd.Timestamp("2020-06-30"),
                "test_start": pd.Timestamp("2020-07-01"),
                "test_end": pd.Timestamp("2020-12-31"),
            }
        ]
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_wfa",
        module=VALIDATION_WORKFLOW_CANONICAL,
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["QQQ"],
        ),
        data=pd.DataFrame({"Time": pd.to_datetime(["2020-01-01"])}),
        raw_config={
            "schema_version": "wfa_run",
            "data": _typed_daily_data("yfinance"),
            "universe": {"symbols": ["QQQ"]},
        },
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
        wfa_results={"selected_optimum": selected},
    )

    assert manifest["coverage_level"] == "window"
    assert manifest["windows"][0]["window_id"] == 1
    assert manifest["windows"][0]["train_start"].startswith("2020-01-01")
    assert manifest["universe_provenance"]["window_count"] == 1
    assert manifest["windows"][0]["universe_provenance"]["survivorship_bias_risk"] == "high"
    assert "wfa_windows_use_run_level_universe_without_point_in_time_constituents" in manifest["audit"]["warnings"]


def test_data_lineage_manifest_wfa_uses_referenced_strategy_universe(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    strategy_path = tmp_path / "workspace" / "runs" / "strategy_run.json"
    wfa_path = tmp_path / "workspace" / "wfa" / "wfa_run.json"
    constituents_path = tmp_path / "workspace" / "universe" / "historical_constituents.csv"
    strategy_path.parent.mkdir(parents=True, exist_ok=True)
    wfa_path.parent.mkdir(parents=True, exist_ok=True)
    constituents_path.parent.mkdir(parents=True, exist_ok=True)
    constituents_path.write_text(
        "symbol,effective_start,effective_end\n"
        "AAA,2019-01-01,\n"
        "BBB,2019-01-01,\n",
        encoding="utf-8",
    )
    strategy_path.write_text(
        json.dumps(
            {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "workflow_id": "single_backtest",
                },
                    "data": _typed_daily_data("yfinance"),
                    "universe": {
                        "symbols": ["AAA", "BBB"],
                        "universe_policy": "point_in_time_snapshot",
                        "historical_constituents_path": "workspace/universe/historical_constituents.csv",
                        "as_of_date": "2020-01-01",
                        "delisted_policy": "include_when_historically_tradable",
                },
                    "computed_fields": [],
                "selection": {},
                "allocation": {},
                "rebalance": {},
                "fill_model": {"timing": "next_bar_after_signal"},
                "risk": {},
                "parameter_domains": {},
                "outputs": {},
            }
        ),
        encoding="utf-8",
    )
    raw_wfa = {
        "schema_version": "wfa_run",
        "strategy_run_path": "workspace/runs/strategy_run.json",
        "platform": {"workflow_id": "rolling_validation"},
    }
    wfa_path.write_text(json.dumps(raw_wfa), encoding="utf-8")
    lineage_raw_config = runtime._lineage_raw_config_with_embedded_strategy_run(  # pylint: disable=protected-access
        raw_wfa,
        wfa_path,
    )
    selected = pd.DataFrame(
        [
            {
                "window_id": 1,
                "train_start": pd.Timestamp("2020-01-01"),
                "train_end": pd.Timestamp("2020-06-30"),
                "test_start": pd.Timestamp("2020-07-01"),
                "test_end": pd.Timestamp("2020-12-31"),
            }
        ]
    )

    manifest = runtime._build_data_lineage_manifest(  # pylint: disable=protected-access
        run_id="lineage_wfa_strategy_ref",
        module=VALIDATION_WORKFLOW_CANONICAL,
        dataloader_config=_typed_dataloader(
            "yfinance",
            source="multi_asset",
            symbols=["AAA", "BBB"],
        ),
        data=pd.DataFrame({"Time": pd.to_datetime(["2020-01-01"])}),
        raw_config=lineage_raw_config,
        primary_artifact=None,
        dataloader_health={"missing_ratio": 0.0, "warnings": [], "errors": []},
        wfa_results={"selected_optimum": selected},
    )

    assert manifest["universe_provenance"]["survivorship_bias_risk"] == "low"
    assert manifest["validity_flags"]["survivorship_known"] is True
    assert manifest["windows"][0]["symbols"] == ["AAA", "BBB"]
    assert manifest["windows"][0]["universe_provenance"]["survivorship_bias_risk"] == "low"


def test_failed_run_writes_unknown_data_lineage_manifest(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    run_id = "failed_lineage"
    paths = runtime.registry.build_run_paths(run_id)
    registry_payload = runtime._base_registry(  # pylint: disable=protected-access
        run_id=run_id,
        module="autorunner",
        entrypoint="test",
        status="running",
    )
    registry_payload["config_snapshot_dir"] = str(paths["snapshot_dir"])
    registry_payload["artifact_manifest_path"] = str(paths["artifact_manifest"])
    registry_payload["dataloader_health_path"] = str(paths["dataloader_health"])
    registry_payload["data_lineage_manifest_path"] = str(paths["data_lineage_manifest"])
    stage_status = runtime._new_stage_status(run_id, "autorunner")  # pylint: disable=protected-access

    runtime._fail_run(  # pylint: disable=protected-access
        run_id=run_id,
        registry_payload=registry_payload,
        stage_status=stage_status,
        stage_name="config_validation",
        message="validation failed",
    )

    lineage = json.loads(paths["data_lineage_manifest"].read_text(encoding="utf-8"))
    registry_entry = runtime.registry.load_registry_entry(run_id)
    assert lineage["lineage_status"] == "unknown"
    assert registry_entry["lineage_status"] == "unknown"


def test_failed_run_projects_structured_data_error_for_frontend_and_ai(
    tmp_path: Path,
) -> None:
    runtime = AppRuntimeService(tmp_path)
    run_id = "failed_data_contract"
    registry_payload = runtime._base_registry(  # pylint: disable=protected-access
        run_id=run_id,
        module="autorunner",
        entrypoint="test",
        status="running",
    )
    stage_status = runtime._new_stage_status(run_id, "autorunner")  # pylint: disable=protected-access
    error = MarketDataContractError(
        "missing_bar",
        "binance bars are incomplete",
        provider="binance",
        details={"missing_bar_count": 1},
    )

    result = runtime._fail_run(  # pylint: disable=protected-access
        run_id=run_id,
        registry_payload=registry_payload,
        stage_status=stage_status,
        stage_name="dataloader",
        message=f"dataloader failed: {error}",
        failure=error.to_payload(),
    )

    registry_entry = runtime.registry.load_registry_entry(run_id)
    written_stage = runtime.registry.load_stage_status(run_id)
    failed_stage = next(
        item for item in written_stage["stages"] if item["stage"] == "dataloader"
    )
    assert result["failure"]["error_code"] == "missing_bar"
    assert registry_entry["failure"]["provider"] == "binance"
    assert failed_stage["failure"]["details"]["missing_bar_count"] == 1


def test_app_export_reads_json_artifacts_through_fail_closed_reader(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    artifact = tmp_path / "artifact.json"
    artifact.write_text('{"schema_version":"canonical_result_bundle.v1"}', encoding="utf-8")

    assert runtime._read_json_artifact(artifact) == {  # noqa: SLF001
        "schema_version": "canonical_result_bundle.v1"
    }

    artifact.write_text("not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="Unable to read JSON artifact"):
        runtime._read_json_artifact(artifact)  # noqa: SLF001


def test_app_runtime_has_no_calls_to_retired_load_json_helper() -> None:
    source = (REPO_ROOT / "app" / "runtime" / "runtime.py").read_text(encoding="utf-8")

    assert "self._load_json(" not in source


def test_app_export_accepts_empty_optional_benchmark_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = AppRuntimeService(tmp_path)
    metrics_dir = tmp_path / "metricstracker"
    metrics_dir.mkdir()
    metrics_path = metrics_dir / "backtest_test_metrics.parquet"
    pd.DataFrame(
        {
            "Backtest_id": [
                "allocation:single_backtest:fixed",
                "allocation:single_backtest:fixed",
            ],
            "Time": ["2026-01-01", "2026-01-02"],
            "Equity_value": [100.0, 101.0],
            "BAH_Equity": [None, None],
        }
    ).to_parquet(metrics_path, index=False)
    canonical_path = tmp_path / "canonical_result_bundle.json"
    canonical_path.write_text(
        json.dumps(
            {
                "schema_version": "canonical_result_bundle.v1",
                "validation": {"status": "valid"},
                "result_hashes": ["a" * 64],
            }
        ),
        encoding="utf-8",
    )

    def fake_plot_bundle(request: dict, **_kwargs: object) -> dict:
        assert [item["series_id"] for item in request["series"]] == [
            "allocation:single_backtest:fixed"
        ]
        return {"schema_version": "PlotBundle.v1", **request}

    monkeypatch.setattr(
        "backtester.RustCoreBridge_backtester.run_plot_bundle_via_cli",
        fake_plot_bundle,
    )

    artifacts = runtime._write_backtest_chart_payloads(  # noqa: SLF001
        "optional_benchmark",
        [metrics_path, canonical_path],
    )

    assert len(artifacts) == 1


def test_intraday_chart_uses_execution_equity_instead_of_session_close_curve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = AppRuntimeService(tmp_path)
    run_id = "intraday_equity"
    runtime.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "completed",
            "execution_bar_spec": {
                "aggregation": "time",
                "step": 1,
                "unit": "minute",
            },
        }
    )
    metrics_dir = tmp_path / "metricstracker"
    metrics_dir.mkdir()
    metrics_path = metrics_dir / "intraday_metrics.parquet"
    pd.DataFrame(
        {
            "Backtest_id": [
                "strategy:single_backtest:fixed",
                "strategy:single_backtest:fixed",
            ],
            "Time": ["2026-01-01", "2026-01-02"],
            "Equity_value": [100.0, 102.0],
        }
    ).to_parquet(metrics_path, index=False)
    execution_path = tmp_path / "intraday_execution_equity_curve.parquet"
    pd.DataFrame(
        {
            "Backtest_id": ["strategy:single_backtest:fixed"] * 3,
            "Time": [
                "2026-01-01T00:01:00Z",
                "2026-01-01T00:02:00Z",
                "2026-01-01T00:03:00Z",
            ],
            "Session_label": ["2026-01-01"] * 3,
            "Equity_value": [100.0, 99.0, 101.0],
        }
    ).to_parquet(execution_path, index=False)
    canonical_path = tmp_path / "canonical_result_bundle.json"
    canonical_path.write_text(
        json.dumps(
            {
                "schema_version": "canonical_result_bundle.v1",
                "validation": {"status": "valid"},
                "result_hashes": ["a" * 64],
            }
        ),
        encoding="utf-8",
        )

    def fake_plot_bundle(request: dict, **_kwargs: object) -> dict:
        strategy = next(
            item
            for item in request["series"]
            if item["series_id"] == "strategy:single_backtest:fixed"
        )
        assert strategy["x"] == [
            "2026-01-01T00:01:00Z",
            "2026-01-01T00:02:00Z",
            "2026-01-01T00:03:00Z",
        ]
        assert strategy["y"] == [100.0, 99.0, 101.0]
        assert str(execution_path) in request["artifact_source_refs"]
        return {"schema_version": "PlotBundle.v1", **request}

    monkeypatch.setattr(
        "backtester.RustCoreBridge_backtester.run_plot_bundle_via_cli",
        fake_plot_bundle,
    )

    artifacts = runtime._write_backtest_chart_payloads(  # noqa: SLF001
        run_id,
        [metrics_path, execution_path, canonical_path],
    )

    assert len(artifacts) == 1


def test_canonical_execution_equity_filename_remains_classifiable(tmp_path: Path) -> None:
    runtime = AppRuntimeService(tmp_path)
    artifact_type, required_pages, optional = runtime._classify_artifact(
        Path(
            "backtest_20260729_BTCUSDT_SMA-10-20_cross_single_"
            "portfolio-execution-equity_btcusdt_execution_equity_curve_abc123.parquet"
        )
    )

    assert artifact_type == "portfolio_execution_equity_curve_parquet"
    assert required_pages == ["metrics_explorer", "results_library"]
    assert optional is False


def test_intraday_backtest_detail_selects_execution_equity_artifact(
    tmp_path: Path,
) -> None:
    runtime = AppRuntimeService(tmp_path)
    run_id = "intraday_detail"
    runtime.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "completed",
            "execution_bar_spec": {
                "aggregation": "time",
                "step": 1,
                "unit": "minute",
            },
        }
    )
    session_equity = tmp_path / "strategy_equity_curve.parquet"
    execution_equity = tmp_path / "strategy_execution_equity_curve.parquet"
    session_equity.touch()
    execution_equity.touch()

    selected = runtime._portfolio_detail_equity_artifact(  # noqa: SLF001
        run_id,
        [session_equity, execution_equity],
    )

    assert selected == execution_equity


def test_detail_metrics_matrix_carries_rust_sortino_into_portfolio_contract() -> None:
    metrics = AppRuntimeService._detail_metrics_matrix(
        {
            "Sortino": 1.25,
            "Total_return": 0.10,
            "Annualized_return (CAGR)": 0.05,
            "Sharpe": 0.75,
            "Max_drawdown": -0.08,
            "Trade_count": 4,
        },
        {
            "total_return": 0.10,
            "cagr": 0.05,
            "sharpe": 0.75,
            "max_drawdown": -0.08,
            "trade_count": 4,
        },
    )

    assert metrics["sortino"] == pytest.approx(1.25)
