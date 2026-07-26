from __future__ import annotations

import copy
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4
from urllib.parse import urlparse

import pandas as pd
from app.runtime.registry import AppRegistry
from app.runtime.runtime import AppRuntimeService
from app.runtime.module_identity import VALIDATION_WORKFLOW_CANONICAL, module_matches

from .labels import decorate_run_label
from .backtest_detail_contract import BacktestDetailContractService
from .metrics_contract_payload import MetricsContractPayloadService
from .payloads import AppPayloadService
from .scheduler import AppBatchScheduler


LOGGER = logging.getLogger(__name__)


class AppAPIService:
    def __init__(self, repo_root: Path):
        self.repo_root = Path(repo_root).resolve()
        self.server_session_id = f"app-{uuid4().hex[:12]}"
        self.server_started_at = datetime.now().astimezone().isoformat(timespec="seconds")
        self.registry = AppRegistry(self.repo_root)
        self.runtime = AppRuntimeService(
            self.repo_root,
            close_interrupted_runs=True,
            app_server_session_id=self.server_session_id,
        )
        self.payloads = AppPayloadService(self.repo_root, self.registry)
        self.metrics_contract_payload = MetricsContractPayloadService(self.registry)
        self.backtest_detail_contract = BacktestDetailContractService(self.registry)
        self.scheduler = AppBatchScheduler(
            self.runtime,
            self.registry,
            self.payloads,
            server_session_id=self.server_session_id,
        )
        self.rust_batch_services = self._prewarm_rust_batch_services()

    def _prewarm_rust_batch_services(self) -> Dict[str, Any]:
        service_names = [
            "signal_timeline_batch",
            "calendar_same_session_batch",
            "calendar_overlay_batch",
            "reset_timer_batch",
            "daily_rank_batch",
            "metrics_parquet",
        ]
        try:
            from backtester.RustCoreBridge_backtester import prewarm_rust_batch_services

            states = prewarm_rust_batch_services(service_names)
            missing = sorted(set(service_names).difference(states))
            unhealthy = {
                name: state
                for name, state in states.items()
                if state != "ready"
            }
            if missing or unhealthy:
                raise RuntimeError(
                    f"missing={missing}; unhealthy={unhealthy}"
                )
        except Exception as exc:
            LOGGER.warning("Rust batch service prewarm failed: %s", exc)
            return {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "services": {},
            }
        return {"status": "ready", "error": "", "services": states}

    def command_center(self) -> Dict[str, Any]:
        runs = self.registry.list_runs()
        visible_runs = [
            row
            for row in runs
            if str(row.get("config_filename", "")).strip()
            or str(row.get("status", "")) in {"completed", "partial"}
        ]
        autorunner_runs = [row for row in runs if row.get("module") == "autorunner"]
        latest_metrics_run = autorunner_runs[0]["run_id"] if autorunner_runs else None
        active_batches = self.scheduler.list_active_batches()
        completed_runs = [row for row in runs if row.get("status") == "completed"]
        failed_runs = [row for row in runs if row.get("status") == "failed"]
        completed_by_module: Dict[str, int] = {}
        failed_by_module: Dict[str, int] = {}
        for row in completed_runs:
            module_name = str(row.get("module", "") or "unknown")
            completed_by_module[module_name] = completed_by_module.get(module_name, 0) + 1
        for row in failed_runs:
            module_name = str(row.get("module", "") or "unknown")
            failed_by_module[module_name] = failed_by_module.get(module_name, 0) + 1
        latest_result = visible_runs[0] if visible_runs else {}
        return {
            "server_session_id": self.server_session_id,
            "server_started_at": self.server_started_at,
            "rust_batch_services": self.rust_batch_services,
            "active_batches": active_batches,
            "recent_runs": [self._decorate_run(row) for row in visible_runs[:8]],
            "resource_snapshot": {
                "cpu_count": os.cpu_count() or 1,
                "scheduler_capacity": self.scheduler.capacity,
                "active_batch_count": len(active_batches),
                "successful_runs": len(completed_runs),
                "failed_runs": len(failed_runs),
                "completed_by_module": completed_by_module,
                "failed_by_module": failed_by_module,
                "recent_successful_runs": len(completed_runs[:8]),
                "recent_failed_runs": len(failed_runs[:8]),
                "latest_result_time": str(
                    latest_result.get("completed_at")
                    or latest_result.get("created_at")
                    or ""
                ),
            },
            "latest_metrics_run_id": latest_metrics_run,
        }

    def run_center_configs(self) -> Dict[str, Any]:
        return {
            "autorunner": self.runtime.list_run_configs(),
            "wfa": self.runtime.list_wfa_configs(),
            "statanalyser": self.runtime.list_statanalyser_configs(),
        }

    def capture_screenshot_bundle(
        self,
        *,
        run_id: str,
        backtest_id: str,
        base_url: str,
        mosaic: bool,
    ) -> Dict[str, Any]:
        run_id = str(run_id or "").strip()
        backtest_id = str(backtest_id or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
            raise ValueError("run_id contains unsupported characters")
        if not backtest_id or len(backtest_id) > 500:
            raise ValueError("backtest_id is required")
        parsed_base_url = urlparse(str(base_url or ""))
        if parsed_base_url.scheme not in {"http", "https"} or parsed_base_url.hostname not in {"127.0.0.1", "localhost", "::1", "testserver"}:
            raise ValueError("base_url must target the local app server")
        self.registry.load_registry_entry(run_id)

        contract_path = self.repo_root / "app" / "contracts" / "screenshot-bundle-v1.contract.json"
        runner_path = self.repo_root / "scripts" / "capture_screenshot_bundle.mjs"
        if not contract_path.exists() or not runner_path.exists():
            raise FileNotFoundError("Screenshot bundle contract or runner is missing")
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        expected_filenames = [
            str(item.get("filename", ""))
            for item in contract.get("captures", [])
            if isinstance(item, dict) and item.get("filename")
        ]
        if len(expected_filenames) != 10 or len(set(expected_filenames)) != 10:
            raise ValueError("Screenshot bundle contract must define exactly 10 unique PNG files")

        node_executable = shutil.which("node")
        if not node_executable:
            raise RuntimeError("Node.js is required to capture screenshot bundles")
        capture_id = f"{datetime.now().astimezone().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
        run_root = self.registry.build_run_paths(run_id)["screenshot_dir"]
        final_dir = run_root / capture_id
        staging_dir = run_root / f".{capture_id}.tmp"
        run_root.mkdir(parents=True, exist_ok=True)
        command = [
            node_executable,
            str(runner_path),
            "--base-url",
            str(base_url).rstrip("/"),
            "--run-id",
            run_id,
            "--backtest-id",
            backtest_id,
            "--output-dir",
            str(staging_dir),
            "--mosaic",
            "true" if mosaic else "false",
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "unknown screenshot runner failure").strip()
                raise RuntimeError(f"Screenshot bundle capture failed: {detail}")
            missing = [name for name in expected_filenames if not (staging_dir / name).is_file()]
            if missing:
                raise RuntimeError(f"Screenshot bundle is incomplete: {', '.join(missing)}")
            manifest = {
                "schema_version": str(contract.get("schema_version", "screenshot_bundle.v1")),
                "capture_id": capture_id,
                "run_id": run_id,
                "backtest_id": backtest_id,
                "mosaic": bool(mosaic),
                "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "files": expected_filenames,
            }
            (staging_dir / "manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            staging_dir.rename(final_dir)
        except Exception:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            raise
        return {
            **manifest,
            "output_dir": str(final_dir),
            "files": [str(final_dir / name) for name in expected_filenames],
        }

    def local_folder_target_path(self, target: str) -> Path:
        workspace_targets = {
            "autorunner": "runs",
            "backtest": "runs",
            "backtests": "runs",
            "wfa": "wfa",
            "walk-forward": "wfa",
            "statanalyser": "statanalyser",
            "factor": "statanalyser",
            "datasets": "datasets",
            "features": "features",
            "strategies": "strategies",
        }
        output_targets = {
            "output": "outputs/app",
            "outputs": "outputs/app",
            "app-output": "outputs/app",
            "app-outputs": "outputs/app",
        }
        normalized = str(target or "").strip().lower()
        if normalized in workspace_targets:
            path = (self.repo_root / "workspace" / workspace_targets[normalized]).resolve()
        elif normalized in {"autorunner-output", "backtest-output", "backtests-output"}:
            path = self._latest_run_artifact_folder("autorunner")
        elif normalized in {"wfa-output", "walk-forward-output", "validation-workflow-output"}:
            path = self._latest_run_artifact_folder(VALIDATION_WORKFLOW_CANONICAL)
        elif normalized in output_targets:
            path = (self.repo_root / output_targets[normalized]).resolve()
        else:
            raise ValueError(f"Unknown local folder target: {target}")
        try:
            path.relative_to(self.repo_root)
        except ValueError as exc:
            raise ValueError(f"Local folder target escaped repo: {target}") from exc
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _latest_run_artifact_folder(self, module: str) -> Path:
        for row in self.registry.list_runs(module=module):
            if str(row.get("status", "")).lower() not in {"completed", "partial"}:
                continue
            run_id = str(row.get("run_id", "")).strip()
            if not run_id:
                continue
            preferred = self._preferred_artifact_folder(run_id, module)
            if preferred is not None:
                return preferred
        snapshots_root = (self.repo_root / "outputs" / "app" / "run_snapshots").resolve()
        snapshots_root.mkdir(parents=True, exist_ok=True)
        return snapshots_root

    def _preferred_artifact_folder(self, run_id: str, module: str) -> Path | None:
        manifest = self.registry.load_artifact_manifest(run_id)
        if not isinstance(manifest, dict):
            raise ValueError(f"Artifact manifest must contain a JSON object: {run_id}")
        artifacts = manifest.get("artifacts", [])
        if not isinstance(artifacts, list):
            raise ValueError(f"Artifact manifest requires an artifacts list: {run_id}")

        def artifact_priority(item: Dict[str, Any]) -> int:
            artifact_type = str(item.get("artifact_type", "")).lower()
            source_stage = str(item.get("source_stage", "")).lower()
            path_suffix = Path(str(item.get("path", ""))).suffix.lower()
            if path_suffix != ".parquet":
                return 50
            if module == "autorunner":
                if artifact_type.startswith("portfolio_"):
                    return 0
                if artifact_type.startswith("backtester"):
                    return 1
                if source_stage == "backtester":
                    return 2
            if module_matches(module, VALIDATION_WORKFLOW_CANONICAL):
                if source_stage == VALIDATION_WORKFLOW_CANONICAL:
                    return 0
                if artifact_type.startswith("wfa"):
                    return 1
            return 20

        for artifact in sorted(
            [item for item in artifacts if isinstance(item, dict)],
            key=artifact_priority,
        ):
            raw_path = str(artifact.get("path", "")).strip()
            if not raw_path:
                continue
            artifact_path = Path(raw_path).resolve()
            artifact_dir = artifact_path.parent
            try:
                artifact_dir.relative_to(self.repo_root)
            except ValueError:
                continue
            if artifact_dir.exists() or artifact_path.exists():
                return artifact_dir
        return None

    def workspace_target_path(self, target: str) -> Path:
        normalized = str(target or "").strip().lower()
        workspace_aliases = {
            "autorunner",
            "backtest",
            "backtests",
            "wfa",
            "walk-forward",
            "statanalyser",
            "factor",
            "datasets",
            "features",
            "strategies",
        }
        if normalized not in workspace_aliases:
            raise ValueError(f"Unknown workspace target: {target}")
        return self.local_folder_target_path(normalized)

    def open_local_folder_target(self, target: str) -> Dict[str, Any]:
        path = self.local_folder_target_path(target)
        opener = self._open_local_path(path)
        return {
            "status": "opened",
            "target": str(target or "").strip().lower(),
            "path": str(path),
            "opener": opener,
        }

    def open_workspace_target(self, target: str) -> Dict[str, Any]:
        self.workspace_target_path(target)
        return self.open_local_folder_target(target)

    def _open_local_path(self, path: Path) -> str:
        editor = os.environ.get("LO2CIN4BT_FILE_EDITOR", "").strip()
        if editor:
            command = [*shlex.split(editor), str(path)]
            subprocess.Popen(
                command,
                cwd=str(self.repo_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return command[0]

        code_command = shutil.which("code")
        if code_command:
            subprocess.Popen(
                [code_command, str(path)],
                cwd=str(self.repo_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return "code"

        if sys.platform.startswith("win"):
            os.startfile(str(path))  # type: ignore[attr-defined]
            return "file-explorer"
        if sys.platform == "darwin":
            subprocess.Popen(
                ["open", str(path)],
                cwd=str(self.repo_root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return "finder"
        subprocess.Popen(
            ["xdg-open", str(path)],
            cwd=str(self.repo_root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return "xdg-open"

    def metrics_runs(self) -> List[Dict[str, Any]]:
        autorunner_runs = [
            self._decorate_run(row)
            for row in self.registry.list_runs(module="autorunner")
            if row.get("status") in {"completed", "partial"}
            and str(row.get("config_filename", "")).strip()
            and not self._is_hidden_sample_run(row)
            and self._has_metrics_renderable_output(str(row.get("run_id", "")))
        ]
        return self._dedupe_preferred_runs(autorunner_runs)

    def wfa_runs(self) -> List[Dict[str, Any]]:
        return [
            self._decorate_run(row)
            for row in self.registry.list_runs(module=VALIDATION_WORKFLOW_CANONICAL)
            if row.get("status") in {"completed", "partial"}
            and self._has_renderable_wfa(str(row.get("run_id", "")))
        ]

    def stat_runs(self) -> List[Dict[str, Any]]:
        return [
            self._decorate_run(row)
            for row in self.registry.list_runs(module="statanalyser")
            if row.get("status") in {"completed", "partial"}
            and (
                self._has_artifact_type(
                    str(row.get("run_id", "")), "statanalyser_summary_json"
                )
                or self.registry.resolve_run_paths(str(row.get("run_id", "")))[
                    "snapshot_dir"
                ].joinpath("statanalyser_summary.json").exists()
            )
        ]

    def metrics_overview(self, run_id: str) -> Dict[str, Any]:
        return self.metrics_contract_payload.load(run_id)

    def parameter_matrix(self, run_id: str) -> Dict[str, Any]:
        default_overrides = self._default_parameter_review_overrides()
        return self.payloads.build_parameter_matrix_payload(
            run_id,
            force=True,
            ranking_config_override=default_overrides["ranking"],
            acceptance_config_override=default_overrides["acceptance"],
        )

    def parameter_matrix_review_preview(
        self,
        run_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        defaults = self._default_parameter_review_overrides()
        ranking = copy.deepcopy(defaults["ranking"])
        acceptance = copy.deepcopy(defaults["acceptance"])
        if isinstance(payload, dict):
            incoming_ranking = payload.get("ranking", {})
            incoming_acceptance = payload.get("acceptance", {})
            if isinstance(incoming_ranking, dict):
                ranking.update(incoming_ranking)
            if isinstance(incoming_acceptance, dict):
                acceptance.update(incoming_acceptance)
        return self.payloads.build_parameter_matrix_payload(
            run_id,
            force=True,
            ranking_config_override=ranking,
            acceptance_config_override=acceptance,
        )

    def list_parameter_review_templates(self) -> Dict[str, Any]:
        store = self._load_parameter_review_template_store()
        templates = store.get("templates", []) if isinstance(store, dict) else []
        default_name = str(store.get("default_template_name", "") or "").strip()
        output_templates = []
        for item in templates:
            normalized = dict(item) if isinstance(item, dict) else {}
            normalized["is_default"] = str(normalized.get("name", "")).strip() == default_name
            output_templates.append(normalized)
        return {
            "schema_version": "1.1",
            "default_template_name": default_name,
            "templates": output_templates,
        }

    def save_parameter_review_template(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        name = str(payload.get("name", "") or "").strip()
        if not name:
            raise ValueError("template name is required")
        ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
        acceptance = payload.get("acceptance", {}) if isinstance(payload, dict) else {}
        store = self._load_parameter_review_template_store()
        templates = store.get("templates", []) if isinstance(store, dict) else []
        templates = [item for item in templates if str(item.get("name", "")).strip().lower() != name.lower()]
        templates.append(
            {
                "name": name,
                "acceptance": acceptance if isinstance(acceptance, dict) else {},
                "ranking": ranking if isinstance(ranking, dict) else {},
                "updated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            }
        )
        templates.sort(key=lambda item: str(item.get("name", "")).lower())
        store["templates"] = templates
        default_name = str(store.get("default_template_name", "") or "").strip()
        if not default_name:
            store["default_template_name"] = name
        self._write_parameter_review_template_store(store)
        return {
            "status": "saved",
            "name": name,
            "default_template_name": str(store.get("default_template_name", "") or "").strip(),
            "template_count": len(templates),
        }

    def delete_parameter_review_template(self, name: str) -> Dict[str, Any]:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("template name is required")
        store = self._load_parameter_review_template_store()
        templates = store.get("templates", []) if isinstance(store, dict) else []
        filtered = [
            item
            for item in templates
            if str(item.get("name", "")).strip().lower() != normalized_name.lower()
        ]
        if len(filtered) == len(templates):
            raise ValueError(f"template not found: {normalized_name}")
        store["templates"] = filtered
        default_name = str(store.get("default_template_name", "") or "").strip()
        if default_name and default_name.lower() == normalized_name.lower():
            store["default_template_name"] = str(filtered[0].get("name", "")) if filtered else ""
        self._write_parameter_review_template_store(store)
        return {
            "status": "deleted",
            "name": normalized_name,
            "default_template_name": str(store.get("default_template_name", "") or "").strip(),
            "template_count": len(filtered),
        }

    def set_default_parameter_review_template(self, name: str) -> Dict[str, Any]:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("template name is required")
        store = self._load_parameter_review_template_store()
        templates = store.get("templates", []) if isinstance(store, dict) else []
        matched = next(
            (item for item in templates if str(item.get("name", "")).strip().lower() == normalized_name.lower()),
            None,
        )
        if not matched:
            raise ValueError(f"template not found: {normalized_name}")
        store["default_template_name"] = str(matched.get("name", "")).strip()
        self._write_parameter_review_template_store(store)
        return {
            "status": "default_set",
            "name": str(matched.get("name", "")).strip(),
            "template_count": len(templates),
        }

    def backtest_detail(self, run_id: str, backtest_id: str) -> Dict[str, Any]:
        return self.backtest_detail_contract.load(run_id, backtest_id)

    def backtest_detail_path(self, run_id: str, backtest_id: str) -> Path:
        try:
            return self.backtest_detail_contract.path(run_id, backtest_id)
        except (FileNotFoundError, ValueError):
            self.runtime.materialize_backtest_detail(run_id, backtest_id)
            return self.backtest_detail_contract.path(run_id, backtest_id)

    def export_backtest_csv(self, run_id: str, backtest_id: str) -> tuple[pd.DataFrame, str]:
        artifact_path = self._artifact_existing_path(run_id, "backtester_parquet")
        if artifact_path is None:
            artifact_path = self._artifact_existing_path(
                run_id, "portfolio_equity_curve_parquet"
            )
        if artifact_path is None:
            raise FileNotFoundError(f"Backtest result parquet not found for run {run_id}")

        records = pd.read_parquet(
            artifact_path,
            filters=[("Backtest_id", "==", str(backtest_id))],
        )

        if records.empty:
            raise FileNotFoundError(f"Backtest {backtest_id} not found in run {run_id}")

        label = str(self.backtest_detail(run_id, backtest_id).get("label", backtest_id))
        safe_label = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in label).strip("_") or str(backtest_id)
        filename = f"{safe_label}_{backtest_id}.csv"
        return records, filename

    def wfa_dashboard(self, run_id: str) -> Dict[str, Any]:
        path = self.payloads.ensure_wfa_dashboard_payload(run_id)
        return AppPayloadService._load_json(path, {})

    def statanalyser_summary(self, run_id: str) -> Dict[str, Any]:
        path = self.payloads.ensure_statanalyser_summary_payload(run_id)
        return AppPayloadService._load_json(path, {})

    def ai_readable_output(self, run_id: str) -> Dict[str, Any]:
        path = self.payloads.ensure_ai_readable_output(run_id)
        return AppPayloadService._load_json(path, {})

    def _dedupe_preferred_runs(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            key = str(row.get("config_filename", "")).strip() or str(row.get("run_id", ""))
            grouped.setdefault(key, []).append(row)

        def score(item: Dict[str, Any]) -> tuple[int, int, str]:
            metadata_complete = 1 if item.get("metadata_complete") else 0
            semantic_complete = 1 if item.get("semantic_index_complete", True) else 0
            created_at = str(item.get("created_at", ""))
            return (metadata_complete, semantic_complete, created_at)

        selected = [max(group, key=score) for group in grouped.values()]
        selected.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        return selected

    def _decorate_run(self, row: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(row)
        run_id = str(row.get("run_id", ""))
        snapshot_dir = self.registry.resolve_run_paths(run_id)["snapshot_dir"]
        snapshot = snapshot_dir / "run_snapshot.json"
        if snapshot.exists():
            snapshot_payload = AppPayloadService._load_json(snapshot, None)
            if not isinstance(snapshot_payload, dict):
                raise ValueError(f"Run snapshot must be a JSON object: {snapshot}")
            resolved = snapshot_payload.get("resolved_configs", {})
            if not isinstance(resolved, dict):
                raise ValueError(
                    f"Run snapshot resolved_configs must be an object: {snapshot}"
                )
            payload["dataloader_config"] = resolved.get("dataloader_config", {})
            payload["backtester_config"] = resolved.get("backtester_config", {})
            payload["wfa_config"] = resolved.get("wfa_config", {})
        strategy_snapshot = snapshot_dir / "strategy_run.json"
        if strategy_snapshot.exists():
            strategy_run_config = AppPayloadService._load_json(
                strategy_snapshot,
                None,
            )
            if not isinstance(strategy_run_config, dict):
                raise ValueError(
                    f"Strategy snapshot must be a JSON object: {strategy_snapshot}"
                )
            if (
                str(strategy_run_config.get("schema_version", "")).strip().lower()
                != "strategy_run"
            ):
                raise ValueError(
                    f"Strategy snapshot must use canonical strategy_run schema: "
                    f"{strategy_snapshot}"
                )
            backtester_config = copy.deepcopy(
                payload.get("backtester_config", {})
                if isinstance(payload.get("backtester_config"), dict)
                else {}
            )
            backtester_config["strategy_run_config"] = strategy_run_config
            payload["backtester_config"] = backtester_config
        if str(row.get("module", "")) == "autorunner":
            index_path = self.registry.resolve_run_paths(run_id)["snapshot_dir"] / "backtest_result_index.json"
            payload["semantic_index_complete"] = index_path.exists()
            if not index_path.exists():
                payload["strategy_label_mode"] = "internal_id_fallback"
            payload["strategy_summary"] = self.payloads._strategy_summary(run_id)
        payload = decorate_run_label(payload)
        manifest = self.registry.load_artifact_manifest(run_id)
        artifacts = manifest.get("artifacts", []) if isinstance(manifest, dict) else []
        if str(row.get("module", "")) == "autorunner":
            for artifact in artifacts:
                if artifact.get("artifact_type") != "portfolio_metadata_json":
                    continue
                metadata_path = Path(str(artifact.get("path", "")))
                if metadata_path.exists():
                    metadata = AppPayloadService._load_json(metadata_path, {})
                    if isinstance(metadata, dict):
                        payload["strategy_summary"] = self.payloads._portfolio_strategy_summary(run_id, metadata)
                    break
        preferred_types = {
            "autorunner": "metricstracker_parquet",
            VALIDATION_WORKFLOW_CANONICAL: "wfa_parquet",
            "statanalyser": "statanalyser_summary_json",
        }
        artifact_name = None
        preferred = preferred_types.get(str(row.get("module", "")))
        preferred_candidates = [preferred] if preferred else []
        if str(row.get("module", "")) == "autorunner":
            preferred_candidates.extend(["portfolio_metadata_json", "portfolio_equity_curve_parquet"])
        if preferred_candidates:
            for artifact in artifacts:
                if artifact.get("artifact_type") not in preferred_candidates:
                    continue
                path = Path(str(artifact.get("path", "")))
                name = path.name.lower()
                if "_audit" in name or "_metadata" in name:
                    if artifact.get("artifact_type") != "portfolio_metadata_json":
                        continue
                if artifact.get("artifact_type") == "portfolio_metadata_json":
                    artifact_name = path.name
                    break
                if "_audit" in name or "_metadata" in name:
                    continue
                artifact_name = path.name
                break
        payload["primary_artifact_name"] = artifact_name
        if module_matches(str(row.get("module", "")), VALIDATION_WORKFLOW_CANONICAL):
            payload["selector_label"] = str(payload.get("display_label", "")).strip() or str(artifact_name or run_id)
        return payload

    def _has_artifact_type(self, run_id: str, artifact_type: str) -> bool:
        if not run_id:
            return False
        manifest = self.registry.load_artifact_manifest(run_id)
        artifacts = manifest.get("artifacts", []) if isinstance(manifest, dict) else []
        for artifact in artifacts:
            if artifact.get("artifact_type") != artifact_type:
                continue
            path = Path(str(artifact.get("path", "")))
            if path.exists():
                return True
        return False

    def _has_metrics_renderable_output(self, run_id: str) -> bool:
        if not run_id:
            return False
        paths = self.registry.resolve_run_paths(run_id)
        projected = paths["chart_payload_dir"] / "metrics_overview_payload.json"
        if self._json_header_has_contract(
            projected,
            '"schema_version":"metrics_overview_index.v1"',
            '"contract_id":"lo2cin4bt.metrics_overview_index.v1"',
        ):
            return True

        plot_bundle = paths["chart_payload_dir"] / "asset_curve_compare.json"
        return (
            self._json_header_has_contract(
                plot_bundle,
                '"schema_version":"plot_bundle_index.v1"',
                '"contract_id":"lo2cin4bt.plot_bundle_index.v1"',
            )
            and self._has_artifact_type(run_id, "metricstracker_parquet")
        )

    @staticmethod
    def _json_header_has_contract(path: Path, *markers: str) -> bool:
        if not path.is_file():
            return False
        try:
            with path.open("r", encoding="utf-8-sig") as handle:
                header = handle.read(8192)
        except (OSError, UnicodeError):
            return False
        return all(marker in header for marker in markers)

    @staticmethod
    def _is_hidden_sample_run(row: Dict[str, Any]) -> bool:
        config_name = str(row.get("config_filename", "")).lower()
        label = str(row.get("display_label", "")).lower()
        semantic = str(row.get("semantic_label", "")).lower()
        sample_markers = ("multi_asset_sample", "sample-monthly-top2", "aaa-bbb-ccc")
        return any(marker in value for marker in sample_markers for value in (config_name, label, semantic))

    def _artifact_existing_path(self, run_id: str, artifact_type: str) -> Path | None:
        if not run_id:
            return None
        manifest = self.registry.load_artifact_manifest(run_id)
        artifacts = manifest.get("artifacts", []) if isinstance(manifest, dict) else []
        for artifact in artifacts:
            if artifact.get("artifact_type") != artifact_type:
                continue
            path = Path(str(artifact.get("path", "")))
            if path.exists():
                return path
        return None

    @staticmethod
    def _sanitize_generated_config_block(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        sanitized = copy.deepcopy(payload)
        sanitized.pop("__config_file_path", None)
        return sanitized

    def _has_renderable_wfa(self, run_id: str) -> bool:
        return self._has_artifact_type(run_id, "wfa_parquet")

    def _has_heatmap_axes(self, run_id: str) -> bool:
        snapshot_dir = self.registry.resolve_run_paths(run_id)["snapshot_dir"]
        execution_plan_path = snapshot_dir / "execution_plan.json"
        if not execution_plan_path.exists():
            return False
        try:
            payload = json.loads(execution_plan_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read {execution_plan_path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Execution plan must contain a JSON object: {execution_plan_path}")
        param_axes = payload.get("param_axes", [])
        if not isinstance(param_axes, list):
            raise ValueError(f"Execution plan param_axes must be a list: {execution_plan_path}")
        axes = [
            axis.get("name")
            for axis in param_axes
            if isinstance(axis, dict) and axis.get("name")
        ]
        return len(axes) >= 2

    def _parameter_review_templates_path(self) -> Path:
        return self.repo_root / "workspace" / "wfa" / "parameter-review-templates.json"

    def _load_parameter_review_template_store(self) -> Dict[str, Any]:
        path = self._parameter_review_templates_path()
        payload = (
            json.loads(path.read_text(encoding="utf-8"))
            if path.exists()
            else {"schema_version": "1.1", "default_template_name": "", "templates": []}
        )
        if not isinstance(payload, dict):
            raise ValueError(f"Parameter review template store requires a JSON object: {path}")
        payload.setdefault("schema_version", "1.1")
        payload.setdefault("default_template_name", "")
        payload.setdefault("templates", [])
        return payload

    def _write_parameter_review_template_store(self, payload: Dict[str, Any]) -> None:
        path = self._parameter_review_templates_path()
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _default_parameter_review_overrides(self) -> Dict[str, Dict[str, Any]]:
        store = self._load_parameter_review_template_store()
        default_name = str(store.get("default_template_name", "") or "").strip().lower()
        templates = store.get("templates", []) if isinstance(store, dict) else []
        default_template = next(
            (
                item for item in templates
                if str(item.get("name", "")).strip().lower() == default_name
            ),
            None,
        )
        if not isinstance(default_template, dict):
            return {"acceptance": {}, "ranking": {}}
        acceptance = default_template.get("acceptance", {})
        ranking = default_template.get("ranking", {})
        return {
            "acceptance": acceptance if isinstance(acceptance, dict) else {},
            "ranking": ranking if isinstance(ranking, dict) else {},
        }
