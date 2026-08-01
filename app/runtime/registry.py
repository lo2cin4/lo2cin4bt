from __future__ import annotations

import json
import shutil
import threading
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.path_resolver import (
    build_app_run_paths,
    ensure_app_outputs_structure,
)
from .module_identity import canonical_module_id, module_matches


class AppRegistry:
    """Filesystem-backed registry for app-managed runs."""

    _latest_runs_lock = threading.RLock()

    def __init__(self, repo_root: Path):
        self.repo_root = Path(repo_root).resolve()
        self.app_paths = ensure_app_outputs_structure(self.repo_root)

    @staticmethod
    def _read_json(path: Path, default: Any) -> Any:
        if not path.exists():
            return default
        try:
            text = path.read_text(encoding="utf-8-sig").strip()
            if not text:
                raise ValueError(f"JSON artifact is empty: {path}")
            return json.loads(text)
        except (OSError, JSONDecodeError) as exc:
            raise ValueError(f"Unable to read JSON artifact {path}: {exc}") from exc

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

    def build_run_paths(self, run_id: str) -> Dict[str, Path]:
        return build_app_run_paths(self.repo_root, run_id, create=True)

    def resolve_run_paths(self, run_id: str) -> Dict[str, Path]:
        """Resolve run paths for reads without creating an empty run shell."""
        return build_app_run_paths(self.repo_root, run_id, create=False)

    def write_registry_entry(self, payload: Dict[str, Any]) -> Path:
        run_id = str(payload["run_id"])
        paths = self.build_run_paths(run_id)
        self._write_json(paths["run_registry"], payload)
        self._update_latest_runs(payload, paths["run_registry"])
        return paths["run_registry"]

    def write_stage_status(self, run_id: str, payload: Dict[str, Any]) -> Path:
        path = self.build_run_paths(run_id)["stage_status"]
        self._write_json(path, payload)
        return path

    def write_artifact_manifest(self, run_id: str, payload: Dict[str, Any]) -> Path:
        path = self.build_run_paths(run_id)["artifact_manifest"]
        self._write_json(path, payload)
        return path

    def write_snapshot_file(self, run_id: str, name: str, payload: Any) -> Path:
        path = self.build_run_paths(run_id)["snapshot_dir"] / name
        self._write_json(path, payload)
        return path

    def list_runs(
        self,
        *,
        module: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        latest_runs = self._refresh_latest_runs_cache()
        rows: List[Dict[str, Any]] = []
        for item in latest_runs:
            if not isinstance(item, dict):
                continue
            if module and not module_matches(str(item.get("module", "")), module):
                continue
            if status and str(item.get("status", "")) != status:
                continue
            payload = dict(item)
            payload["module"] = canonical_module_id(str(payload.get("module", "")))
            rows.append(payload)
        return rows

    def load_registry_entry(self, run_id: str) -> Dict[str, Any]:
        path = self.resolve_run_paths(run_id)["run_registry"]
        return self._read_json(path, {})

    def load_stage_status(self, run_id: str) -> Dict[str, Any]:
        path = self.resolve_run_paths(run_id)["stage_status"]
        return self._read_json(path, {})

    def load_artifact_manifest(self, run_id: str) -> Dict[str, Any]:
        path = self.resolve_run_paths(run_id)["artifact_manifest"]
        return self._read_json(path, {})

    def delete_run_artifacts(self, run_id: str) -> List[str]:
        """Remove app-managed files for a canceled run and unregister it."""
        run_id_text = str(run_id or "").strip()
        if not run_id_text:
            return []
        paths = self.build_run_paths(run_id_text)
        removed: List[str] = []
        for key in (
            "run_registry",
            "artifact_manifest",
            "stage_status",
            "snapshot_dir",
            "chart_payload_dir",
            "ai_review_dir",
            "screenshot_dir",
        ):
            path = paths[key]
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                    removed.append(str(path))
                elif path.exists():
                    path.unlink()
                    removed.append(str(path))
            except OSError:
                continue
        self._refresh_latest_runs_cache()
        return removed

    @staticmethod
    def _is_cleanup_candidate(
        registry_payload: Dict[str, Any],
        *,
        current_server_session_id: Optional[str],
    ) -> bool:
        entrypoint = str(registry_payload.get("entrypoint", "") or "").strip().lower()
        owner_type = str(registry_payload.get("owner_type", "") or "").strip().lower()
        run_server_session_id = str(
            registry_payload.get("server_session_id", "") or ""
        ).strip()
        if owner_type == "direct_runtime" or entrypoint == "runtime-direct":
            return False
        if owner_type == "app_server":
            return bool(run_server_session_id) and run_server_session_id != str(
                current_server_session_id or ""
            )
        return entrypoint == "app-run-center"

    def fail_interrupted_runs(
        self,
        *,
        completed_at: str,
        message: str,
        current_server_session_id: Optional[str] = None,
    ) -> int:
        closed = 0
        for item in self.list_runs():
            if not isinstance(item, dict):
                continue
            status = str(item.get("status", "")).lower()
            if status not in {"queued", "running"}:
                continue
            run_id = str(item.get("run_id", "")).strip()
            if not run_id:
                continue

            registry_payload = self.load_registry_entry(run_id)
            if not isinstance(registry_payload, dict) or not registry_payload:
                continue
            if str(registry_payload.get("status", "")).lower() not in {"queued", "running"}:
                continue
            if not self._is_cleanup_candidate(
                registry_payload,
                current_server_session_id=current_server_session_id,
            ):
                continue

            registry_payload["status"] = "failed"
            registry_payload["completed_at"] = completed_at
            try:
                prior_error_count = int(registry_payload.get("error_count") or 0)
            except (TypeError, ValueError):
                prior_error_count = 0
            registry_payload["error_count"] = max(prior_error_count, 1)
            errors = registry_payload.get("errors")
            if not isinstance(errors, list):
                errors = []
            if message not in errors:
                errors.append(message)
            registry_payload["errors"] = errors

            stage_status = self.load_stage_status(run_id)
            if isinstance(stage_status, dict) and stage_status:
                stage_status["status"] = "failed"
                stages = stage_status.get("stages")
                if isinstance(stages, list):
                    failed_stage = None
                    for stage in stages:
                        if not isinstance(stage, dict):
                            continue
                        if str(stage.get("status", "")).lower() in {"pending", "running", "queued"}:
                            failed_stage = stage
                            break
                    if failed_stage is None:
                        for stage in stages:
                            if isinstance(stage, dict) and str(stage.get("status", "")).lower() not in {"completed", "skipped"}:
                                failed_stage = stage
                                break
                    if isinstance(failed_stage, dict):
                        failed_stage["status"] = "failed"
                        failed_stage["message"] = message
                        stage_status["current_stage"] = failed_stage.get("stage", stage_status.get("current_stage"))
                self.write_stage_status(run_id, stage_status)

            self.write_registry_entry(registry_payload)
            closed += 1
        return closed

    def _update_latest_runs(self, registry_payload: Dict[str, Any], registry_path: Path) -> None:
        del registry_payload, registry_path
        self._refresh_latest_runs_cache()

    @staticmethod
    def _registry_summary(
        registry_payload: Dict[str, Any], registry_path: Path
    ) -> Dict[str, Any]:
        return {
            "run_id": registry_payload.get("run_id"),
            "module": canonical_module_id(str(registry_payload.get("module", ""))),
            "entrypoint": registry_payload.get("entrypoint"),
            "owner_type": registry_payload.get("owner_type"),
            "server_session_id": registry_payload.get("server_session_id"),
            "status": registry_payload.get("status"),
            "created_at": registry_payload.get("created_at"),
            "completed_at": registry_payload.get("completed_at"),
            "config_filename": registry_payload.get("config_filename"),
            "symbol": registry_payload.get("symbol"),
            "execution_stream_id": registry_payload.get("execution_stream_id"),
            "decision_stream_id": registry_payload.get("decision_stream_id"),
            "execution_bar_spec": registry_payload.get("execution_bar_spec"),
            "strategy_mode": registry_payload.get("strategy_mode"),
            "semantic_label": registry_payload.get("semantic_label"),
            "display_label": registry_payload.get("display_label"),
            "run_type": registry_payload.get("run_type"),
            "is_default": registry_payload.get("is_default") is True,
            "data_lineage_manifest_path": registry_payload.get("data_lineage_manifest_path"),
            "lineage_status": registry_payload.get("lineage_status"),
            "warning_count": registry_payload.get("warning_count", 0),
            "error_count": registry_payload.get("error_count", 0),
            "registry_path": str(registry_path),
        }

    def _registry_summaries(self) -> List[Dict[str, Any]]:
        summaries: List[Dict[str, Any]] = []
        registry_root = self.app_paths["run_registry"]
        for registry_path in registry_root.glob("*.json"):
            payload = self._read_json(registry_path, {})
            if not isinstance(payload, dict) or not payload.get("run_id"):
                continue
            summaries.append(self._registry_summary(payload, registry_path))
        summaries.sort(key=lambda item: str(item.get("created_at", "")), reverse=True)
        return summaries[:100]

    def _refresh_latest_runs_cache(self) -> List[Dict[str, Any]]:
        with self._latest_runs_lock:
            summaries = self._registry_summaries()
            self._write_json(self.app_paths["latest_runs"], summaries[:100])
            return summaries
