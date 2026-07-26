from pathlib import Path
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable
import importlib

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.api.scheduler import JOB_WEIGHTS
from app.api.service import AppAPIService


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_missing_metrics_run_returns_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    client = TestClient(create_app(tmp_path))

    response = client.get("/api/app/metrics/missing-run/overview")

    assert response.status_code == 404
    assert "PlotBundle index not found" in response.json()["detail"]
    assert not (tmp_path / "outputs" / "app" / "chart_payloads" / "missing-run").exists()
    assert not (tmp_path / "outputs" / "app" / "run_snapshots" / "missing-run").exists()


def test_decorate_run_loads_canonical_strategy_snapshot_for_selector_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "20260713_622d003ff9ee"
    snapshot_dir = service.registry.build_run_paths(run_id)["snapshot_dir"]
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "strategy_run.json").write_text(
        json.dumps(
            {
                "schema_version": "strategy_run",
                "platform": {
                    "workflow_id": "parameter_matrix",
                    "run_type": "example",
                    "display_label": "BTC-USD | Monthly Nth Weekday Same Session | Coinbase | Example",
                },
                "data": {"provider": "coinbase"},
                "universe": {"symbols": ["BTC-USD"]},
            }
        ),
        encoding="utf-8",
    )

    payload = service._decorate_run(  # noqa: SLF001 - selector identity contract.
        {"module": "autorunner", "run_id": run_id, "config_filename": "strategy.json"}
    )

    assert payload["display_label"] == (
        "2026-07-13 | BTC-USD | Monthly Nth Weekday Same Session | Parameter Matrix | run 622d00"
    )
    assert payload["identity"]["asset"] == "BTC-USD"
    assert payload["identity"]["concept_display"] == "Monthly Nth Weekday Same Session"


def test_decorate_run_rejects_corrupt_strategy_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "corrupt_strategy_snapshot"
    snapshot_dir = service.registry.build_run_paths(run_id)["snapshot_dir"]
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "strategy_run.json").write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="strategy_run.json"):
        service._decorate_run(  # noqa: SLF001
            {
                "module": "autorunner",
                "run_id": run_id,
                "config_filename": "strategy.json",
            }
        )


def test_decorate_run_rejects_noncanonical_strategy_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "invalid_strategy_snapshot"
    snapshot_dir = service.registry.build_run_paths(run_id)["snapshot_dir"]
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "strategy_run.json").write_text(
        json.dumps({"schema_version": "legacy_strategy"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="canonical strategy_run"):
        service._decorate_run(  # noqa: SLF001
            {
                "module": "autorunner",
                "run_id": run_id,
                "config_filename": "strategy.json",
            }
        )


def test_scheduler_rejects_unreadable_config_before_queueing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    config = tmp_path / "workspace" / "runs" / "broken.json"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="broken.json"):
        service.scheduler.submit_batch("autorunner", [str(config)])


def test_scheduler_rejects_non_object_config_before_queueing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    config = tmp_path / "workspace" / "runs" / "array.json"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        service.scheduler.submit_batch("autorunner", [str(config)])


def test_app_api_routes_smoke() -> None:
    client = TestClient(create_app(REPO_ROOT))

    health = client.get("/api/app/health")
    assert health.status_code == 200
    assert health.json()["server_session_id"].startswith("app-")
    command_center = client.get("/api/app/command-center")
    assert command_center.status_code == 200
    assert "recent_runs" in command_center.json()
    assert command_center.json()["server_session_id"] == health.json()["server_session_id"]

    wfa_runs = client.get("/api/app/wfa/runs")
    assert wfa_runs.status_code == 200
    rows = wfa_runs.json()
    if rows:
        dashboard = client.get(f"/api/app/wfa/{rows[0]['run_id']}/dashboard")
        assert dashboard.status_code == 200

    configs = client.get("/api/app/run-center/configs")
    assert configs.status_code == 200
    payload = configs.json()
    assert "autorunner" in payload
    assert "wfa" in payload
    assert "statanalyser" in payload


def test_command_center_is_no_store_and_restarted_app_has_no_stale_active_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    first_app = create_app(repo)
    first_service: AppAPIService = first_app.state.app_service
    TestClient(first_app)
    runtime_started = threading.Event()
    release_runtime = threading.Event()

    def interrupted_run(
        _config_path: str,
        _emit: Callable[[str, str], None],
    ) -> dict[str, str]:
        runtime_started.set()
        release_runtime.wait(timeout=5)
        return {"run_id": "stale_batch_run", "status": "completed"}

    monkeypatch.setattr(
        first_service.runtime,
        "run_autorunner_config",
        interrupted_run,
    )
    config = repo / "workspace" / "runs" / "case.json"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text("{}", encoding="utf-8")

    batch = first_service.scheduler.submit_batch("autorunner", [str(config)])
    assert runtime_started.wait(timeout=2)
    running_job = batch["jobs"][0]
    run_id = "stale_batch_run"
    registry_payload = {
        "run_id": run_id,
        "module": "autorunner",
        "entrypoint": "app-run-center",
        "status": "running",
        "created_at": "2026-06-30T19:29:44+08:00",
        "completed_at": None,
        "error_count": 0,
    }
    first_service.registry.write_registry_entry(registry_payload)
    first_service.registry.write_stage_status(
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
    with first_service.scheduler._lock:  # noqa: SLF001 - regression harness inspects in-memory scheduler state.
        live_job = first_service.scheduler._locate_job(batch["batch_id"], running_job["job_id"])  # noqa: SLF001
        assert live_job is not None
        live_job["status"] = "running"
        live_job["stage"] = "backtester"
        live_job["run_id"] = run_id

    try:
        restarted_client = TestClient(create_app(repo))
        response = restarted_client.get("/api/app/command-center")

        assert response.status_code == 200
        assert response.headers["Cache-Control"] == "no-store, max-age=0"
        assert response.headers["Pragma"] == "no-cache"
        assert response.json()["active_batches"] == []
        assert response.json()["server_session_id"].startswith("app-")
        failed_entry = restarted_client.app.state.app_service.registry.load_registry_entry(run_id)
        assert failed_entry["status"] == "failed"
        assert failed_entry["errors"] == [
            "Interrupted because the app process stopped before this run completed."
        ]
    finally:
        release_runtime.set()


def test_registry_list_runs_prunes_missing_registry_entries(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    service = AppAPIService(repo)
    latest_runs_path = repo / "outputs" / "app" / "latest_runs.json"
    latest_runs_path.parent.mkdir(parents=True, exist_ok=True)
    latest_runs_path.write_text(
        """
        [
          {
            "run_id": "missing_registry_run",
            "module": "autorunner",
            "status": "completed",
            "created_at": "2026-07-04T19:00:00+08:00",
            "config_filename": "ghost.json"
          }
        ]
        """.strip(),
        encoding="utf-8",
    )

    rows = service.registry.list_runs()

    assert rows == []
    assert latest_runs_path.read_text(encoding="utf-8").strip() == "[]"


def test_registry_parallel_writes_rebuild_complete_latest_runs_cache(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)

    def write_run(index: int) -> None:
        service.registry.write_registry_entry(
            {
                "run_id": f"parallel-run-{index}",
                "module": "autorunner",
                "status": "completed",
                "created_at": f"2026-07-13T23:00:{index:02d}+08:00",
                "config_filename": f"strategy-{index}.json",
            }
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(write_run, range(8)))

    latest_runs_path = service.registry.app_paths["latest_runs"]
    latest_runs_path.write_text(
        json.dumps(json.loads(latest_runs_path.read_text(encoding="utf-8"))[:4]),
        encoding="utf-8",
    )

    rows = service.registry.list_runs(module="autorunner")
    cached = json.loads(latest_runs_path.read_text(encoding="utf-8"))

    assert len(rows) == 8
    assert len(cached) == 8
    assert {row["run_id"] for row in rows} == {
        f"parallel-run-{index}" for index in range(8)
    }


def test_parameter_matrix_cannot_submit_shortlist_to_wfa() -> None:
    client = TestClient(create_app(REPO_ROOT))

    response = client.post(
        "/api/app/metrics/example-run/parameter-matrix/send-to-wfa",
        json={"candidate_rows": [{"params": {"vix_max": 33}}]},
    )

    assert response.status_code in {404, 405}


def test_workspace_target_path_maps_run_center_config_folders(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)

    assert service.workspace_target_path("autorunner") == tmp_path.resolve() / "workspace" / "runs"
    assert service.workspace_target_path("wfa") == tmp_path.resolve() / "workspace" / "wfa"
    assert service.local_folder_target_path("autorunner-output") == tmp_path.resolve() / "outputs" / "app" / "run_snapshots"
    assert service.local_folder_target_path("wfa-output") == tmp_path.resolve() / "outputs" / "app" / "run_snapshots"


def test_app_service_prewarms_rust_batch_helpers(monkeypatch, tmp_path: Path) -> None:
    calls = []

    def fake_prewarm(service_names=None):
        names = list(service_names or [])
        calls.append(names)
        return {name: "ready" for name in names}

    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    monkeypatch.setattr(bridge, "prewarm_rust_batch_services", fake_prewarm)

    service = AppAPIService(tmp_path)

    assert calls == [[
        "signal_timeline_batch",
        "calendar_same_session_batch",
        "calendar_overlay_batch",
        "reset_timer_batch",
        "daily_rank_batch",
        "metrics_parquet",
    ]]
    expected_status = {
        "status": "ready",
        "error": "",
        "services": {name: "ready" for name in calls[0]},
    }
    assert service.rust_batch_services == expected_status
    assert service.command_center()["rust_batch_services"] == expected_status

    with pytest.raises(ValueError):
        service.workspace_target_path("unknown")


def test_app_service_exposes_failed_rust_prewarm(monkeypatch, tmp_path: Path) -> None:
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    monkeypatch.setattr(
        bridge,
        "prewarm_rust_batch_services",
        lambda service_names=None: {"metrics_parquet": "unavailable"},
    )

    service = AppAPIService(tmp_path)

    assert service.rust_batch_services["status"] == "failed"
    assert "missing=" in service.rust_batch_services["error"]


def test_output_target_path_opens_latest_artifact_folder(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260516_example"
    artifact_path = (
        tmp_path
        / "outputs"
        / "app"
        / "run_snapshots"
        / run_id
        / "managed_artifacts"
        / "portfolio"
        / "result.parquet"
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(b"PAR1")
    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "completed",
            "created_at": "2026-05-16T01:00:00+08:00",
            "config_filename": "example.json",
        }
    )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "artifacts": [
                {
                    "artifact_type": "portfolio_equity_curve_parquet",
                    "source_stage": "backtester",
                    "status": "ready",
                    "path": str(artifact_path),
                }
            ]
        },
    )

    assert service.local_folder_target_path("autorunner-output") == artifact_path.parent


def test_preferred_artifact_folder_rejects_corrupt_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "corrupt_manifest"
    manifest_path = service.registry.build_run_paths(run_id)["artifact_manifest"]
    manifest_path.write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact_manifests"):
        service._preferred_artifact_folder(run_id, "autorunner")  # noqa: SLF001


def test_renderable_wfa_requires_canonical_parquet_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "stale_wfa_cache"
    payload_dir = service.registry.build_run_paths(run_id)["chart_payload_dir"]
    (payload_dir / "wfa_dashboard_payload.json").write_text("{}", encoding="utf-8")

    assert service._has_renderable_wfa(run_id) is False  # noqa: SLF001


def test_heatmap_axes_require_canonical_execution_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "legacy_heatmap_inference"
    snapshot_dir = service.registry.build_run_paths(run_id)["snapshot_dir"]
    (snapshot_dir / "backtest_result_index.json").write_text(
        json.dumps(
            {
                "backtests": [
                    {"semantic_combo": {"lookback": 20, "threshold": 0.5}},
                ]
            }
        ),
        encoding="utf-8",
    )

    assert service._has_heatmap_axes(run_id) is False  # noqa: SLF001


def test_heatmap_axes_reject_corrupt_execution_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "corrupt_execution_plan"
    snapshot_dir = service.registry.build_run_paths(run_id)["snapshot_dir"]
    (snapshot_dir / "execution_plan.json").write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="execution_plan.json"):
        service._has_heatmap_axes(run_id)  # noqa: SLF001


def test_parameter_review_store_rejects_non_object_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    path = service._parameter_review_templates_path()  # noqa: SLF001
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        service._load_parameter_review_template_store()  # noqa: SLF001


def test_workspace_open_route_uses_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called: dict[str, str] = {}

    def fake_open(self: AppAPIService, target: str) -> dict[str, str]:
        called["target"] = target
        return {
            "status": "opened",
            "target": target,
            "path": str(tmp_path / "workspace" / "runs"),
            "opener": "test",
        }

    monkeypatch.setattr(AppAPIService, "open_workspace_target", fake_open)
    client = TestClient(create_app(tmp_path))

    response = client.post("/api/app/workspace/open", json={"target": "autorunner"})

    assert response.status_code == 200
    assert response.json()["opener"] == "test"
    assert called["target"] == "autorunner"


def test_folder_open_route_accepts_output_targets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    called: dict[str, str] = {}

    def fake_open(self: AppAPIService, target: str) -> dict[str, str]:
        called["target"] = target
        return {
            "status": "opened",
            "target": target,
            "path": str(tmp_path / "outputs" / "app"),
            "opener": "test",
        }

    monkeypatch.setattr(AppAPIService, "open_local_folder_target", fake_open)
    client = TestClient(create_app(tmp_path))

    response = client.post("/api/app/folders/open", json={"target": "wfa-output"})

    assert response.status_code == 200
    assert response.json()["path"].endswith(str(Path("outputs") / "app"))
    assert called["target"] == "wfa-output"


def test_batch_route_rejects_config_paths_outside_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    client = TestClient(create_app(repo))

    response = client.post(
        "/api/app/batches",
        json={"module": "autorunner", "config_paths": [str(outside)]},
    )

    assert response.status_code == 400
    assert "escaped repo workspace" in response.json()["detail"]


def test_batch_route_rejects_wrong_module_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    wfa_config = repo / "workspace" / "wfa" / "case.json"
    wfa_config.parent.mkdir(parents=True)
    wfa_config.write_text("{}", encoding="utf-8")
    client = TestClient(create_app(repo))

    response = client.post(
        "/api/app/batches",
        json={"module": "autorunner", "config_paths": [str(wfa_config)]},
    )

    assert response.status_code == 400
    assert "autorunner batch config must be under" in response.json()["detail"]


def test_batch_route_rejects_legacy_config_alias_filename(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    config = repo / "workspace" / "runs" / "strategy-run-case.json"
    legacy_alias = repo / "workspace" / "runs" / "strategy-run-v2-case.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    client = TestClient(create_app(repo))

    response = client.post(
        "/api/app/batches",
        json={"module": "autorunner", "config_paths": [str(legacy_alias)]},
    )

    assert response.status_code == 400
    assert "Legacy config alias strategy-run-v2-* is no longer accepted" in response.json()["detail"]


def test_batch_route_rejects_non_json_config(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    config = repo / "workspace" / "runs" / "case.txt"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    client = TestClient(create_app(repo))

    response = client.post(
        "/api/app/batches",
        json={"module": "autorunner", "config_paths": [str(config)]},
    )

    assert response.status_code == 400
    assert "must be a .json file" in response.json()["detail"]


def test_mixed_batch_module_uses_repo_relative_config_root(tmp_path: Path) -> None:
    repo = tmp_path / "workspace" / "wfa" / "repo"
    run_config = repo / "workspace" / "runs" / "case.json"
    run_config.parent.mkdir(parents=True)
    run_config.write_text("{}", encoding="utf-8")
    client = TestClient(create_app(repo))

    response = client.post(
        "/api/app/batches",
        json={"module": "mixed", "config_paths": [str(run_config)]},
    )

    assert response.status_code == 200
    assert response.json()["jobs"][0]["module"] == "autorunner"


def test_batch_cancel_route_stops_running_job(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    config = repo / "workspace" / "runs" / "case.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    app = create_app(repo)
    service: AppAPIService = app.state.app_service
    cleanup_paths: dict[str, Path] = {}

    def slow_run(_config_path: str, emit: Callable[[str, str], None]) -> dict[str, str]:
        run_id = "cancel-test"
        emit("backtester", "started")
        time.sleep(0.2)
        paths = service.registry.build_run_paths(run_id)
        cleanup_paths.update(paths)
        service.registry.write_registry_entry(
            {
                "run_id": run_id,
                "module": "autorunner",
                "status": "running",
                "created_at": "2026-06-27T00:00:00+08:00",
            }
        )
        service.registry.write_snapshot_file(run_id, "partial.json", {"status": "partial"})
        return {"run_id": run_id, "status": "completed"}

    monkeypatch.setattr(service.runtime, "run_autorunner_config", slow_run)
    bridge = importlib.import_module("backtester.RustCoreBridge_backtester")
    cancel_calls = {"count": 0}

    def fake_cancel_active_rust_work() -> None:
        cancel_calls["count"] += 1

    monkeypatch.setattr(bridge, "cancel_active_rust_work", fake_cancel_active_rust_work)
    client = TestClient(app)

    started = client.post(
        "/api/app/batches",
        json={"module": "autorunner", "config_paths": [str(config)]},
    )
    assert started.status_code == 200
    batch_id = started.json()["batch_id"]

    batch = started.json()
    for _ in range(20):
        batch = client.get(f"/api/app/batches/{batch_id}").json()
        if batch["jobs"][0]["status"] == "running":
            break
        time.sleep(0.05)
    else:
        pytest.fail("batch did not start running before cancellation test timeout")

    canceled = client.post(f"/api/app/batches/{batch_id}/cancel")
    assert canceled.status_code == 200
    assert canceled.json()["jobs"][0]["cancel_requested"] is True
    assert cancel_calls["count"] == 1

    for _ in range(20):
        batch = client.get(f"/api/app/batches/{batch_id}").json()
        if batch["status"] == "canceled":
            break
        time.sleep(0.05)

    assert batch["status"] == "canceled"
    assert batch["jobs"][0]["status"] == "canceled"
    assert not cleanup_paths["run_registry"].exists()
    assert not cleanup_paths["snapshot_dir"].exists()


def test_stale_cancel_requested_job_is_force_finalized(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    config = repo / "workspace" / "runs" / "case.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    app = create_app(repo)
    service: AppAPIService = app.state.app_service
    client = TestClient(app)
    release_runtime = threading.Event()

    def blocked_runtime(config_path, emit):
        release_runtime.wait(timeout=5)
        return {"status": "completed", "run_id": None}

    monkeypatch.setattr(service.runtime, "run_autorunner_config", blocked_runtime)

    batch = service.scheduler.submit_batch("autorunner", [str(config)])
    batch_id = batch["batch_id"]
    job_id = batch["jobs"][0]["job_id"]
    run_id = "stale-cancel-test"
    run_paths = service.registry.build_run_paths(run_id)
    run_paths["run_registry"].parent.mkdir(parents=True, exist_ok=True)
    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "running",
            "created_at": "2026-06-30T20:00:00+08:00",
        }
    )
    service.registry.write_snapshot_file(run_id, "partial.json", {"status": "partial"})

    with service.scheduler._lock:  # noqa: SLF001 - regression harness mutates in-memory scheduler state.
        live_job = service.scheduler._locate_job(batch_id, job_id)  # noqa: SLF001
        assert live_job is not None
        live_job["status"] = "running"
        live_job["stage"] = "backtester"
        live_job["run_id"] = run_id
        live_job["cancel_requested"] = True
        live_job["cancel_requested_at"] = "2026-06-30T20:00:00+08:00"
        live_job["updated_at"] = "2026-06-30T20:00:00+08:00"
        live_job["started_at"] = "2026-06-30T20:00:00+08:00"
        live_job["logs"] = ["[cancel_requested] Stop requested by user"]
        service.scheduler._batches[batch_id]["status"] = "running"  # noqa: SLF001

    response = client.get(f"/api/app/batches/{batch_id}")
    release_runtime.set()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "canceled"
    assert payload["jobs"][0]["status"] == "canceled"
    assert "Canceled after stop request timeout" in payload["jobs"][0]["stage_message"]
    assert not run_paths["run_registry"].exists()
    assert not run_paths["snapshot_dir"].exists()


def test_existing_metrics_overview_payload_contract_smoke() -> None:
    service = AppAPIService(REPO_ROOT)
    runs = service.metrics_runs()
    if not runs:
        pytest.skip("no completed metrics runs available in repo sample outputs")

    payload = None
    selected_run_id = ""
    for run in runs:
        selected_run_id = str(run["run_id"])
        try:
            payload = service.metrics_overview(selected_run_id)
            break
        except (FileNotFoundError, ValueError):
            continue
    if payload is None:
        pytest.skip("repo sample outputs do not contain a current PlotBundle.v1 metrics run")

    assert payload["run_id"] == selected_run_id
    assert isinstance(payload.get("rows"), list)
    assert isinstance(payload.get("series"), list)
    assert isinstance(payload.get("categories"), dict)
    assert isinstance(payload.get("available_categories"), list)
    assert "strategy_summary" in payload
    assert "benchmark_series" in payload


def test_metrics_run_discovery_rejects_legacy_parquet_without_plot_bundle_contract(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "legacy-metrics-run"
    paths = service.registry.build_run_paths(run_id)
    paths["chart_payload_dir"].mkdir(parents=True, exist_ok=True)
    paths["chart_payload_dir"].joinpath("asset_curve_compare.json").write_text(
        json.dumps({"schema_version": "chart_payload.v1", "series": []}),
        encoding="utf-8",
    )

    assert service._has_metrics_renderable_output(run_id) is False


def test_metrics_run_discovery_accepts_current_projected_contract(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "current-metrics-run"
    payload_path = (
        service.registry.build_run_paths(run_id)["chart_payload_dir"]
        / "metrics_overview_payload.json"
    )
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(
        '{"schema_version":"metrics_overview_index.v1",'
        '"contract_id":"lo2cin4bt.metrics_overview_index.v1"}',
        encoding="utf-8",
    )

    assert service._has_metrics_renderable_output(run_id) is True


def test_existing_parameter_matrix_payload_contract_smoke() -> None:
    service = AppAPIService(REPO_ROOT)
    payload = None
    for run in service.metrics_runs():
        run_id = str(run["run_id"])
        try:
            candidate = service.parameter_matrix(run_id)
        except (FileNotFoundError, ValueError):
            # Local ignored outputs may predate the current strict metric contract.
            continue
        if isinstance(candidate, dict) and candidate.get("param_axes"):
            payload = candidate
            break
    if payload is None:
        pytest.skip("no parameter-matrix payload available in repo sample outputs")

    assert isinstance(payload.get("rows"), list)
    assert isinstance(payload.get("shortlist_rows"), list)
    assert isinstance(payload.get("cluster_summary"), list)
    assert isinstance(payload.get("parameter_importance"), list)
    assert isinstance(payload.get("param_axes"), list)
    assert isinstance(payload.get("axis_values"), dict)
    assert isinstance(payload.get("search_source_options"), list)
    assert "study_summary" in payload
    assert "default_x_axis" in payload
    assert "default_y_axis" in payload
    assert isinstance(payload.get("objectives"), list)
    assert isinstance(payload.get("aggregation_modes"), list)


def test_large_parameter_matrix_scheduler_weight_leaves_one_lane(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    config = repo / "workspace" / "runs" / "matrix.json"
    config.parent.mkdir(parents=True, exist_ok=True)
    config.write_text(
        """
        {
          "platform": {"workflow_id": "parameter_matrix"},
          "parameter_domains": {
            "fast": {"values": [1, 2, 3, 4, 5, 6, 7, 8]},
            "slow": {"values": [10, 20, 30, 40]}
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    service = AppAPIService(repo)

    weight = service.scheduler._job_weight("autorunner", config)  # noqa: SLF001 - scheduler policy regression guard.
    message = service.scheduler._queue_message("autorunner", weight)  # noqa: SLF001 - scheduler policy regression guard.

    assert weight == max(1, service.scheduler.capacity - 1)
    assert weight < service.scheduler.capacity
    assert "leaving one lane available" in message


def test_scheduler_capacity_keeps_one_lane_on_two_core_hosts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.api.scheduler.os.cpu_count", lambda: 2)

    service = AppAPIService(tmp_path / "repo")

    assert service.scheduler.capacity == max(JOB_WEIGHTS.values()) + 1
    message = service.scheduler._queue_message("wfa", JOB_WEIGHTS["wfa"])  # noqa: SLF001
    assert "leaving one lane available" in message


def test_large_wfa_scheduler_weight_uses_sampled_candidate_budget_and_leaves_one_lane(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    strategy = repo / "workspace" / "runs" / "strategy.json"
    wfa = repo / "workspace" / "wfa" / "case.json"
    strategy.parent.mkdir(parents=True, exist_ok=True)
    wfa.parent.mkdir(parents=True, exist_ok=True)
    strategy.write_text(
        """
        {
          "schema_version": "strategy_run",
          "platform": {
            "workflow_id": "parameter_matrix",
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal"
          },
          "parameter_domains": {
            "fast": {"type": "range", "start": 20, "end": 100, "step": 10},
            "slow": {"type": "range", "start": 120, "end": 300, "step": 10}
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    wfa.write_text(
        """
        {
          "schema_version": "wfa_run",
          "platform": {"workflow_id": "walk_forward_analysis"},
          "strategy_run_path": "workspace/runs/strategy.json",
          "optimizer": {"candidate_limit": 60}
        }
        """.strip(),
        encoding="utf-8",
    )
    service = AppAPIService(repo)

    weight = service.scheduler._job_weight("wfa", wfa)  # noqa: SLF001 - scheduler policy regression guard.
    message = service.scheduler._queue_message("wfa", weight)  # noqa: SLF001 - scheduler policy regression guard.

    assert weight == max(2, service.scheduler.capacity - 1)
    assert weight < service.scheduler.capacity
    assert "leaving one lane available" in message


def test_wfa_scheduler_weight_strategy_run_path_routes_through_single_bridge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    strategy = repo / "workspace" / "runs" / "strategy.json"
    wfa = repo / "workspace" / "wfa" / "case.json"
    strategy.parent.mkdir(parents=True, exist_ok=True)
    wfa.parent.mkdir(parents=True, exist_ok=True)
    strategy.write_text(
        """
        {
          "schema_version": "strategy_run",
          "platform": {
            "workflow_id": "parameter_matrix",
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal"
          },
          "parameter_domains": {
            "fast": {"values": [1,2,3,4,5,6,7,8]},
            "slow": {"values": [10,20,30,40]}
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    wfa.write_text(
        """
        {
          "schema_version": "wfa_run",
          "platform": {"workflow_id": "walk_forward_analysis"},
          "strategy_run_path": "workspace/runs/strategy.json"
        }
        """.strip(),
        encoding="utf-8",
    )
    service = AppAPIService(repo)

    weight = service.scheduler._job_weight("wfa", wfa)  # noqa: SLF001 - compatibility bridge guard.

    assert weight == max(2, service.scheduler.capacity - 1)


def test_scheduler_dispatch_prioritizes_light_job_that_fits_capacity(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    light = repo / "workspace" / "runs" / "light.json"
    heavy_strategy = repo / "workspace" / "runs" / "strategy.json"
    heavy_wfa = repo / "workspace" / "wfa" / "heavy.json"
    light.parent.mkdir(parents=True, exist_ok=True)
    heavy_strategy.parent.mkdir(parents=True, exist_ok=True)
    heavy_wfa.parent.mkdir(parents=True, exist_ok=True)
    light.write_text("{}", encoding="utf-8")
    heavy_strategy.write_text(
        """
        {
          "schema_version": "strategy_run",
          "platform": {
            "workflow_id": "parameter_matrix",
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal"
          },
          "parameter_domains": {
            "fast": {"values": [1,2,3,4,5,6,7,8]},
            "slow": {"values": [10,20,30,40]}
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    heavy_wfa.write_text(
        """
        {
          "schema_version": "wfa_run",
          "platform": {"workflow_id": "walk_forward_analysis"},
          "strategy_run_path": "workspace/runs/strategy.json"
        }
        """.strip(),
        encoding="utf-8",
    )
    service = AppAPIService(repo)
    batch = service.scheduler.submit_batch("mixed", [str(heavy_wfa), str(light)])

    with service.scheduler._lock:  # noqa: SLF001 - scheduler policy regression guard.
        service.scheduler._active_weight = max(0, service.scheduler.capacity - 1)  # noqa: SLF001
        next_ref = service.scheduler._next_schedulable_job_ref()  # noqa: SLF001

    assert next_ref is not None
    assert next_ref["job_id"] == batch["jobs"][1]["job_id"]


def test_scheduler_reserves_capacity_for_aged_heavy_job(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    light = repo / "workspace" / "runs" / "light.json"
    heavy_strategy = repo / "workspace" / "runs" / "strategy.json"
    heavy_wfa = repo / "workspace" / "wfa" / "heavy.json"
    light.parent.mkdir(parents=True, exist_ok=True)
    heavy_strategy.parent.mkdir(parents=True, exist_ok=True)
    heavy_wfa.parent.mkdir(parents=True, exist_ok=True)
    light.write_text("{}", encoding="utf-8")
    heavy_strategy.write_text(
        '{"schema_version":"strategy_run","platform":{"workflow_id":"parameter_matrix",'
        '"strategy_mode_id":"multi_asset_portfolio",'
        '"strategy_profile_id":"selection_timing_portfolio",'
        '"strategy_preset_id":"single_asset_signal"},'
        '"parameter_domains":{"a":{"values":[1,2,3,4,5,6,7,8]},'
        '"b":{"values":[1,2,3,4]}}}',
        encoding="utf-8",
    )
    heavy_wfa.write_text(
        '{"schema_version":"wfa_run","platform":{"workflow_id":"walk_forward_analysis"},'
        '"strategy_run_path":"workspace/runs/strategy.json"}',
        encoding="utf-8",
    )
    service = AppAPIService(repo)
    with service.scheduler._lock:  # noqa: SLF001 - scheduler policy regression guard.
        batch_id = "fairness-test"
        jobs = [
            {
                "job_id": "light",
                "module": "autorunner",
                "weight": 1,
                "created_at": "2026-07-11T00:00:00+08:00",
                "queued_monotonic": time.monotonic(),
            },
            {
                "job_id": "heavy",
                "module": "wfa",
                "weight": 3,
                "created_at": "2026-07-11T00:00:01+08:00",
                "queued_monotonic": 0.0,
            },
        ]
        service.scheduler.capacity = 4
        service.scheduler._batches = {batch_id: {"jobs": jobs}}  # noqa: SLF001
        service.scheduler._pending = [  # noqa: SLF001
            {"batch_id": batch_id, "job_id": "light"},
            {"batch_id": batch_id, "job_id": "heavy"},
        ]
        service.scheduler._active_weight = 0  # noqa: SLF001
        next_ref = service.scheduler._next_schedulable_job_ref()  # noqa: SLF001
        service.scheduler._pending = []  # noqa: SLF001

    assert next_ref is not None
    assert next_ref["job_id"] == "heavy"


def test_existing_wfa_dashboard_payload_contract_smoke() -> None:
    service = AppAPIService(REPO_ROOT)
    runs = service.wfa_runs()
    if not runs:
        pytest.skip("no completed wfa runs available in repo sample outputs")

    payload = service.wfa_dashboard(str(runs[0]["run_id"]))

    assert payload["run_id"] == str(runs[0]["run_id"])
    assert isinstance(payload.get("rows"), list)
    assert isinstance(payload.get("combo_groups"), list)
    assert "batch_metadata" in payload
    assert "strategy_summary" in payload
    assert "portfolio_window_summary" in payload


def test_frontend_static_assets_can_appear_after_app_start(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    client = TestClient(create_app(repo))

    missing = client.get("/assets/app.js")
    assert missing.status_code == 404

    dist = repo / "plotter" / "web" / "dist"
    assets = dist / "assets"
    assets.mkdir(parents=True)
    (dist / "index.html").write_text(
        '<script type="module" src="/assets/app.js"></script>',
        encoding="utf-8",
    )
    (assets / "app.js").write_text("window.__lo2cin4bt_test = true;", encoding="utf-8")

    index = client.get("/")
    assert index.status_code == 200
    assert "/assets/app.js" in index.text

    asset = client.get("/assets/app.js")
    assert asset.status_code == 200
    assert "window.__lo2cin4bt_test" in asset.text


def test_screenshot_bundle_uses_run_scoped_output_and_requires_all_pngs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)
    run_id = "20260714_capture123"
    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": "autorunner",
            "status": "completed",
            "created_at": "2026-07-14T00:00:00+08:00",
        }
    )
    contract_dir = tmp_path / "app" / "contracts"
    contract_dir.mkdir(parents=True, exist_ok=True)
    contract_dir.joinpath("screenshot-bundle-v1.contract.json").write_text(
        REPO_ROOT.joinpath("app", "contracts", "screenshot-bundle-v1.contract.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    runner = tmp_path / "scripts" / "capture_screenshot_bundle.mjs"
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.write_text("", encoding="utf-8")
    expected_names = {
        "equity_curve.png",
        "list.png",
        "parameter_matrix.png",
        "best_summary.png",
        "metrics1.png",
        "risk1.png",
        "risk2.png",
        "metrics2.png",
        "metrics3.png",
        "metrics4.png",
    }

    def fake_run(command, **kwargs):
        output_dir = Path(command[command.index("--output-dir") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        for filename in expected_names:
            output_dir.joinpath(filename).write_bytes(b"png")
        return type("Completed", (), {"returncode": 0, "stdout": "{}", "stderr": ""})()

    monkeypatch.setattr("app.api.service.shutil.which", lambda name: "C:/node/node.exe")
    monkeypatch.setattr("app.api.service.subprocess.run", fake_run)

    result = service.capture_screenshot_bundle(
        run_id=run_id,
        backtest_id="candidate:one",
        base_url="http://127.0.0.1:2424",
        mosaic=True,
    )

    output_dir = Path(result["output_dir"])
    assert output_dir.parent.parent.name == "screenshots"
    assert output_dir.parent.name == run_id
    assert {Path(path).name for path in result["files"]} == expected_names
    manifest = json.loads(output_dir.joinpath("manifest.json").read_text(encoding="utf-8"))
    assert manifest["run_id"] == run_id
    assert manifest["backtest_id"] == "candidate:one"
    assert manifest["mosaic"] is True
    assert len(manifest["files"]) == 10


def test_screenshot_bundle_rejects_unsafe_run_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(AppAPIService, "_prewarm_rust_batch_services", lambda self: None)
    service = AppAPIService(tmp_path)

    with pytest.raises(ValueError, match="run_id"):
        service.capture_screenshot_bundle(
            run_id="../outside",
            backtest_id="candidate",
            base_url="http://127.0.0.1:2424",
            mosaic=False,
        )
