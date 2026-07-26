from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.runtime.registry import AppRegistry
from app.runtime.module_identity import VALIDATION_WORKFLOW_CANONICAL
from app.runtime.runtime import AppRuntimeService

from .labels import load_app_config_metadata
from .payloads import AppPayloadService

JOB_WEIGHTS = {"autorunner": 1, "statanalyser": 1, "wfa": 2}
_MIN_SCHEDULER_CAPACITY = max(JOB_WEIGHTS.values()) + 1
_LARGE_MATRIX_VARIANT_THRESHOLD = 32
_HEAVY_JOB_RESERVATION_SECONDS = max(
    1,
    int(str(os.getenv("LO2CIN4BT_HEAVY_JOB_RESERVATION_SECONDS", "30")).strip() or "30"),
)
_CANCEL_GRACE_SECONDS = max(
    1,
    int(str(os.getenv("LO2CIN4BT_APP_CANCEL_GRACE_SECONDS", "5")).strip() or "5"),
)
CONFIG_ROOTS = {
    "autorunner": ("workspace", "runs"),
    "wfa": ("workspace", "wfa"),
    "statanalyser": ("workspace", "statanalyser"),
}


class BatchCancellationRequested(RuntimeError):
    """Raised inside a worker at a scheduler checkpoint after user cancellation."""

    def __init__(self, message: str, *, run_id: Optional[str] = None):
        super().__init__(message)
        self.run_id = run_id


class AppBatchScheduler:
    def __init__(
        self,
        runtime: AppRuntimeService,
        registry: AppRegistry,
        payloads: AppPayloadService,
        *,
        server_session_id: str,
    ):
        self.runtime = runtime
        self.registry = registry
        self.payloads = payloads
        self.server_session_id = str(server_session_id)
        self.capacity = min(
            4,
            max(_MIN_SCHEDULER_CAPACITY, (os.cpu_count() or 4) // 2),
        )
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._active_weight = 0
        self._batches: Dict[str, Dict[str, Any]] = {}
        self._pending: List[Dict[str, str]] = []
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher.start()

    def submit_batch(self, module: str, config_paths: List[str]) -> Dict[str, Any]:
        batch_id = self._new_batch_id()
        created_at = self._now_iso()
        jobs: List[Dict[str, Any]] = []
        for index, config_path in enumerate(config_paths):
            job_module, resolved_config_path = self._resolve_job_config(module, config_path)
            metadata = load_app_config_metadata(str(resolved_config_path), job_module)
            weight = self._job_weight(job_module, resolved_config_path)
            jobs.append(
                {
                    "job_id": f"{batch_id}_{index + 1:02d}",
                    "module": job_module,
                    "config_path": str(resolved_config_path),
                    "label": resolved_config_path.name,
                    "display_label": metadata.get("display_label"),
                    "label_badges": list(metadata.get("badges", [])),
                    "weight": weight,
                    "status": "queued",
                    "stage": "queued",
                    "stage_message": self._queue_message(job_module, weight),
                    "run_id": None,
                    "created_at": created_at,
                    "queued_monotonic": time.monotonic(),
                    "started_at": None,
                    "updated_at": created_at,
                    "stage_started_at": created_at,
                    "stage_durations": {},
                    "completed_at": None,
                    "logs": [],
                    "error": None,
                    "cancel_requested": False,
                    "cancel_requested_at": None,
                    "result_refs": {},
                }
            )
        batch = {
            "batch_id": batch_id,
            "module": module,
            "status": "queued",
            "created_at": created_at,
            "updated_at": created_at,
            "completed_at": None,
            "jobs": jobs,
            "events": [],
        }
        with self._condition:
            self._batches[batch_id] = batch
            for job in jobs:
                self._pending.append({"batch_id": batch_id, "job_id": job["job_id"]})
            self._append_event(batch_id, "batch_submitted", {"job_count": len(jobs)})
            self._condition.notify_all()
        return self.get_batch(batch_id)

    def list_active_batches(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._force_finalize_stale_cancellations_locked()
            rows = [
                self._public_batch(batch, include_events=False)
                for batch in self._batches.values()
                if batch["status"] in {"queued", "running"}
            ]
        rows.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        return rows

    def get_batch(self, batch_id: str) -> Dict[str, Any]:
        with self._lock:
            self._force_finalize_stale_cancellations_locked()
            batch = self._batches.get(batch_id)
            if batch is None:
                raise KeyError(batch_id)
            return self._public_batch(batch, include_events=True)

    def get_events_since(self, batch_id: str, offset: int) -> List[Dict[str, Any]]:
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                return []
            return batch["events"][offset:]

    def cancel_batch(self, batch_id: str) -> Dict[str, Any]:
        with self._condition:
            batch = self._batches.get(batch_id)
            if batch is None:
                raise KeyError(batch_id)
            if batch["status"] not in {"queued", "running"}:
                return self._public_batch(batch, include_events=True)

            now = self._now_iso()
            canceled_jobs = 0
            cancel_requested_jobs = 0
            for job in batch["jobs"]:
                status = str(job.get("status") or "")
                if status == "queued":
                    job["status"] = "canceled"
                    job["stage"] = "canceled"
                    job["stage_message"] = "Canceled before execution"
                    job["updated_at"] = now
                    job["completed_at"] = now
                    job["logs"] = (
                        job.get("logs", []) + ["[canceled] Canceled before execution"]
                    )[-250:]
                    canceled_jobs += 1
                elif status == "running":
                    job["cancel_requested"] = True
                    job["cancel_requested_at"] = now
                    job["stage_message"] = "Cancel requested; stopping at the next safe checkpoint"
                    job["updated_at"] = now
                    job["logs"] = (
                        job.get("logs", [])
                        + ["[cancel_requested] Stop requested by user"]
                    )[-250:]
                    cancel_requested_jobs += 1

            self._pending = [ref for ref in self._pending if ref["batch_id"] != batch_id]
            self._append_event(
                batch_id,
                "batch_cancel_requested",
                {
                    "canceled_jobs": canceled_jobs,
                    "cancel_requested_jobs": cancel_requested_jobs,
                },
            )
            if cancel_requested_jobs:
                self._request_runtime_cancellation()
            self._refresh_batch_status(batch_id)
            self._condition.notify_all()
            return self._public_batch(batch, include_events=True)

    @staticmethod
    def _request_runtime_cancellation() -> None:
        # Long Rust matrix jobs run behind persistent server processes and do not
        # naturally hit Python-side cancel checkpoints until the batch returns.
        # Closing those helpers turns a user stop into a prompt failure/cancel.
        try:
            from backtester.RustCoreBridge_backtester import cancel_active_rust_work

            cancel_active_rust_work()
        except Exception:
            # Best effort only; the job still carries the cancel_requested flag.
            return

    def _dispatch_loop(self) -> None:
        while True:
            with self._condition:
                next_ref = self._next_schedulable_job_ref()
                if next_ref is None:
                    self._condition.wait(timeout=0.5)
                    continue
                job = self._locate_job(next_ref["batch_id"], next_ref["job_id"])
                if job is None:
                    continue
                self._pending = [ref for ref in self._pending if ref != next_ref]
                self._active_weight += int(job["weight"])
                now = self._now_iso()
                job["status"] = "running"
                job["stage"] = "starting"
                job["stage_message"] = (
                    f"Starting with scheduler weight {job['weight']}/{self.capacity}"
                )
                job["started_at"] = now
                job["updated_at"] = now
                job["stage_started_at"] = now
                self._batches[next_ref["batch_id"]]["status"] = "running"
                self._batches[next_ref["batch_id"]]["updated_at"] = now
                self._append_event(
                    next_ref["batch_id"],
                    "job_started",
                    {
                        "job_id": job["job_id"],
                        "label": job["label"],
                        "display_label": job.get("display_label"),
                    },
                )
                threading.Thread(
                    target=self._run_job,
                    args=(next_ref["batch_id"], job["job_id"]),
                    daemon=True,
                ).start()

    def _run_job(self, batch_id: str, job_id: str) -> None:
        job = self._locate_job(batch_id, job_id)
        if job is None:
            return
        heartbeat_stop = threading.Event()

        def heartbeat() -> None:
            while not heartbeat_stop.wait(5.0):
                with self._lock:
                    live_job = self._locate_job(batch_id, job_id)
                    if live_job is None:
                        return
                    if str(live_job.get("status") or "") != "running":
                        return
                    now = self._now_iso()
                    live_job["updated_at"] = now
                    batch = self._batches.get(batch_id)
                    if batch is not None:
                        batch["updated_at"] = now

        def emit(stage: str, message: str) -> None:
            with self._lock:
                live_job = self._locate_job(batch_id, job_id)
                if live_job is None:
                    return
                if live_job.get("cancel_requested"):
                    raise BatchCancellationRequested("Canceled by user request")
                now = self._now_iso()
                if live_job.get("stage") != stage:
                    self._record_stage_duration(live_job, now)
                    live_job["stage_started_at"] = now
                live_job["stage"] = stage
                live_job["stage_message"] = message
                live_job["updated_at"] = now
                live_job["logs"] = (
                    live_job.get("logs", []) + [f"[{stage}] {message}"]
                )[-250:]
                self._append_event(
                    batch_id,
                    "job_log",
                    {"job_id": job_id, "stage": stage, "message": message},
                )

        try:
            threading.Thread(target=heartbeat, daemon=True).start()
            with self._lock:
                live_job = self._locate_job(batch_id, job_id)
                if live_job is None or live_job.get("cancel_requested"):
                    raise BatchCancellationRequested("Canceled by user request")
            module = str(job["module"])
            config_path = str(job["config_path"])
            if module == "autorunner":
                result = self.runtime.run_autorunner_config(config_path, emit)
                payload_module = "autorunner"
            elif module == "wfa":
                result = self.runtime.run_wfa_config(config_path, emit)
                payload_module = VALIDATION_WORKFLOW_CANONICAL
            else:
                result = self.runtime.run_statanalyser_config(config_path, emit)
                payload_module = "statanalyser"
            run_id = result.get("run_id")
            status = str(result.get("status", "completed"))
            with self._lock:
                live_job = self._locate_job(batch_id, job_id)
                if live_job is not None and live_job.get("cancel_requested"):
                    raise BatchCancellationRequested(
                        "Canceled by user request",
                        run_id=str(run_id or ""),
                    )
            if run_id and status in {"completed", "partial"}:
                self.payloads.ensure_run_payloads(run_id, module=payload_module)
            registry_entry = self.registry.load_registry_entry(run_id) if run_id else {}
            with self._lock:
                live_job = self._locate_job(batch_id, job_id)
                if live_job is not None:
                    now = self._now_iso()
                    self._record_stage_duration(live_job, now)
                    live_job["status"] = status
                    live_job["stage"] = registry_entry.get("status", status)
                    live_job["stage_message"] = f"Finished with status {status}"
                    live_job["run_id"] = run_id
                    live_job["updated_at"] = now
                    live_job["completed_at"] = now
                    live_job["result_refs"] = {
                        "module": module,
                        "run_id": run_id,
                        "semantic_label": registry_entry.get("semantic_label"),
                    }
                    self._append_event(
                        batch_id,
                        "job_finished",
                        {"job_id": job_id, "run_id": run_id, "status": status},
                    )
        except BatchCancellationRequested as exc:
            with self._lock:
                live_job = self._locate_job(batch_id, job_id)
                if live_job is not None:
                    now = self._now_iso()
                    cancellation_message = self._cancellation_message(
                        live_job,
                        fallback=str(exc),
                        now=now,
                    )
                    self._record_stage_duration(live_job, now)
                    cleanup_run_id = exc.run_id or str(live_job.get("run_id") or "")
                    cleanup_log = ""
                    if cleanup_run_id:
                        removed_paths = self.registry.delete_run_artifacts(cleanup_run_id)
                        live_job["run_id"] = None
                        if removed_paths:
                            cleanup_log = (
                                f"[canceled] Removed {len(removed_paths)} partial run artifact path(s) "
                                f"for {cleanup_run_id}"
                            )
                    live_job["status"] = "canceled"
                    live_job["stage"] = "canceled"
                    live_job["stage_message"] = cancellation_message
                    live_job["error"] = None
                    live_job["updated_at"] = now
                    live_job["completed_at"] = now
                    log_lines = live_job.get("logs", []) + [
                        f"[canceled] {cancellation_message}"
                    ]
                    if cleanup_log:
                        log_lines.append(cleanup_log)
                    live_job["logs"] = log_lines[-250:]
                    self._append_event(
                        batch_id,
                        "job_canceled",
                        {"job_id": job_id, "message": cancellation_message},
                    )
        except Exception as exc:
            with self._lock:
                live_job = self._locate_job(batch_id, job_id)
                if live_job is not None:
                    now = self._now_iso()
                    self._record_stage_duration(live_job, now)
                    live_job["status"] = "failed"
                    live_job["stage"] = "failed"
                    live_job["stage_message"] = str(exc)
                    live_job["error"] = str(exc)
                    live_job["updated_at"] = now
                    live_job["completed_at"] = now
                    live_job["logs"] = (
                        live_job.get("logs", []) + [f"[failed] {exc}"]
                    )[-250:]
                    self._append_event(
                        batch_id,
                        "job_failed",
                        {"job_id": job_id, "error": str(exc)},
                    )
        finally:
            heartbeat_stop.set()
            with self._condition:
                self._active_weight = max(0, self._active_weight - int(job["weight"]))
                self._refresh_batch_status(batch_id)
                self._condition.notify_all()

    def _refresh_batch_status(self, batch_id: str) -> None:
        batch = self._batches.get(batch_id)
        if batch is None:
            return
        statuses = [job.get("status") for job in batch["jobs"]]
        if any(status in {"queued", "running"} for status in statuses):
            batch["status"] = (
                "running" if any(status == "running" for status in statuses) else "queued"
            )
            batch["updated_at"] = self._now_iso()
            return
        has_completed = any(status == "completed" for status in statuses)
        has_partial = any(status == "partial" for status in statuses)
        has_failed = any(status == "failed" for status in statuses)
        has_canceled = any(status == "canceled" for status in statuses)
        if has_failed and (has_completed or has_partial or has_canceled):
            batch["status"] = "partial"
        elif has_failed:
            batch["status"] = "failed"
        elif has_canceled and (has_completed or has_partial):
            batch["status"] = "partial"
        elif has_canceled:
            batch["status"] = "canceled"
        elif has_partial:
            batch["status"] = "partial"
        else:
            batch["status"] = "completed"
        if batch["completed_at"] is None:
            batch["completed_at"] = self._now_iso()
        batch["updated_at"] = batch["completed_at"]
        self._append_event(batch_id, "batch_status", {"status": batch["status"]})

    def _force_finalize_stale_cancellations_locked(self) -> None:
        now = self._now_iso()
        now_ts = self._parse_iso_timestamp(now)
        if now_ts is None:
            return
        dirty_batches: set[str] = set()
        for batch_id, batch in self._batches.items():
            for job in batch.get("jobs", []):
                if str(job.get("status") or "") != "running":
                    continue
                if not job.get("cancel_requested"):
                    continue
                requested_at = self._parse_iso_timestamp(
                    str(job.get("cancel_requested_at") or job.get("updated_at") or "")
                )
                if requested_at is None:
                    continue
                if (now_ts - requested_at).total_seconds() < _CANCEL_GRACE_SECONDS:
                    continue
                cleanup_run_id = str(job.get("run_id") or "")
                cleanup_log = ""
                if cleanup_run_id:
                    removed_paths = self.registry.delete_run_artifacts(cleanup_run_id)
                    job["run_id"] = None
                    if removed_paths:
                        cleanup_log = (
                            f"[canceled] Removed {len(removed_paths)} partial run artifact path(s) "
                            f"for {cleanup_run_id}"
                        )
                self._record_stage_duration(job, now)
                job["status"] = "canceled"
                job["stage"] = "canceled"
                job["stage_message"] = (
                    f"Canceled after stop request timeout ({_CANCEL_GRACE_SECONDS}s)"
                )
                job["error"] = None
                job["updated_at"] = now
                job["completed_at"] = now
                log_lines = job.get("logs", []) + [
                    f"[canceled] Forced cancel after stop request timeout ({_CANCEL_GRACE_SECONDS}s)"
                ]
                if cleanup_log:
                    log_lines.append(cleanup_log)
                job["logs"] = log_lines[-250:]
                self._append_event(
                    batch_id,
                    "job_canceled",
                    {
                        "job_id": job["job_id"],
                        "message": job["stage_message"],
                    },
                )
                dirty_batches.add(batch_id)
        for batch_id in dirty_batches:
            self._refresh_batch_status(batch_id)

    def _cancellation_message(
        self,
        job: Dict[str, Any],
        *,
        fallback: str,
        now: str,
    ) -> str:
        requested_at = self._parse_iso_timestamp(
            str(job.get("cancel_requested_at") or job.get("updated_at") or "")
        )
        now_ts = self._parse_iso_timestamp(now)
        if requested_at is None or now_ts is None:
            return fallback
        if (now_ts - requested_at).total_seconds() < _CANCEL_GRACE_SECONDS:
            return fallback
        return f"Canceled after stop request timeout ({_CANCEL_GRACE_SECONDS}s)"

    def _next_schedulable_job_ref(self) -> Optional[Dict[str, str]]:
        aged_heavy: List[tuple[float, Dict[str, str], Dict[str, Any]]] = []
        for ref in self._pending:
            job = self._locate_job(ref["batch_id"], ref["job_id"])
            if job is None or not self._is_heavy_job(job):
                continue
            queued_value = job.get("queued_monotonic")
            queued_monotonic = (
                float(queued_value)
                if isinstance(queued_value, (int, float))
                else time.monotonic()
            )
            wait_seconds = max(
                0.0,
                time.monotonic() - queued_monotonic,
            )
            if wait_seconds >= _HEAVY_JOB_RESERVATION_SECONDS:
                aged_heavy.append((queued_monotonic, ref, job))
        if aged_heavy:
            _, reserved_ref, reserved_job = min(aged_heavy, key=lambda item: item[0])
            if self._active_weight + int(reserved_job["weight"]) <= self.capacity:
                return reserved_ref
            # Stop admitting new light jobs until the reserved heavy job fits.
            return None

        schedulable: List[tuple[tuple[Any, ...], Dict[str, str]]] = []
        for index, ref in enumerate(self._pending):
            job = self._locate_job(ref["batch_id"], ref["job_id"])
            if job is None:
                continue
            if self._active_weight + int(job["weight"]) <= self.capacity:
                schedulable.append((self._job_dispatch_priority(job, pending_index=index), ref))
        if not schedulable:
            return None
        schedulable.sort(key=lambda item: item[0])
        return schedulable[0][1]

    def _locate_job(self, batch_id: str, job_id: str) -> Optional[Dict[str, Any]]:
        batch = self._batches.get(batch_id)
        if batch is None:
            return None
        for job in batch["jobs"]:
            if job["job_id"] == job_id:
                return job
        return None

    def _append_event(self, batch_id: str, event_type: str, payload: Dict[str, Any]) -> None:
        batch = self._batches.get(batch_id)
        if batch is None:
            return
        batch["events"].append(
            {
                "type": event_type,
                "timestamp": self._now_iso(),
                **payload,
            }
        )
        batch["updated_at"] = batch["events"][-1]["timestamp"]
        batch["events"] = batch["events"][-500:]

    def _public_batch(self, batch: Dict[str, Any], *, include_events: bool) -> Dict[str, Any]:
        payload = {
            "server_session_id": self.server_session_id,
            "batch_id": batch["batch_id"],
            "module": batch["module"],
            "status": batch["status"],
            "created_at": batch["created_at"],
            "updated_at": batch.get("updated_at"),
            "completed_at": batch["completed_at"],
            "jobs": [
                {
                    "job_id": job["job_id"],
                    "module": job["module"],
                    "label": job["label"],
                    "display_label": job.get("display_label"),
                    "label_badges": list(job.get("label_badges", [])),
                    "status": job["status"],
                    "stage": job["stage"],
                    "stage_message": job.get("stage_message"),
                    "weight": job.get("weight"),
                    "run_id": job["run_id"],
                    "created_at": job.get("created_at"),
                    "started_at": job.get("started_at"),
                    "updated_at": job.get("updated_at"),
                    "stage_started_at": job.get("stage_started_at"),
                    "stage_durations": dict(job.get("stage_durations", {})),
                    "completed_at": job.get("completed_at"),
                    "error": job["error"],
                    "cancel_requested": bool(job.get("cancel_requested")),
                    "cancel_requested_at": job.get("cancel_requested_at"),
                    "logs": list(job.get("logs", [])),
                    "result_refs": dict(job.get("result_refs", {})),
                }
                for job in batch["jobs"]
            ],
        }
        if include_events:
            payload["events"] = list(batch["events"])
        return payload

    def _resolve_job_config(self, module: str, config_path: str) -> tuple[str, Path]:
        raw_path = Path(str(config_path or "").strip())
        if not raw_path.name:
            raise ValueError("config_path cannot be empty")
        if raw_path.suffix.lower() != ".json":
            raise ValueError(f"Batch config must be a .json file: {config_path}")
        self._reject_legacy_config_alias(raw_path.name)
        resolved = raw_path.resolve()
        repo_root = Path(getattr(self.runtime, "repo_root", Path.cwd())).resolve()
        if not resolved.exists():
            raise ValueError(f"Batch config file does not exist: {config_path}")
        try:
            resolved.relative_to(repo_root)
        except ValueError as exc:
            raise ValueError(f"Batch config escaped repo workspace: {config_path}") from exc

        job_module = self._resolve_job_module(module, resolved, repo_root)
        expected_parts = CONFIG_ROOTS.get(job_module)
        if expected_parts is None:
            raise ValueError(f"Unsupported batch module: {job_module}")
        expected_root = repo_root.joinpath(*expected_parts).resolve()
        try:
            resolved.relative_to(expected_root)
        except ValueError as exc:
            raise ValueError(
                f"{job_module} batch config must be under {expected_root}: {config_path}"
            ) from exc
        return job_module, resolved

    def _job_weight(self, job_module: str, config_path: Path) -> int:
        base_weight = int(JOB_WEIGHTS[job_module])
        config = self._load_json_config(config_path)
        if job_module == "wfa":
            candidate_count = self._wfa_candidate_count(config, config_path)
            if candidate_count >= _LARGE_MATRIX_VARIANT_THRESHOLD:
                return max(base_weight, self.capacity - 1)
            return base_weight
        if job_module != "autorunner":
            return base_weight
        raw_platform = config.get("platform")
        platform: Dict[str, Any] = raw_platform if isinstance(raw_platform, dict) else {}
        workflow_id = str(platform.get("workflow_id") or "").strip().lower()
        raw_domains = config.get("parameter_domains")
        domains: Dict[str, Any] = raw_domains if isinstance(raw_domains, dict) else {}
        variant_count = self._parameter_variant_count(domains)
        if workflow_id == "parameter_matrix" and variant_count >= _LARGE_MATRIX_VARIANT_THRESHOLD:
            return max(base_weight, self.capacity - 1)
        return base_weight

    def _job_dispatch_priority(self, job: Dict[str, Any], *, pending_index: int) -> tuple[Any, ...]:
        return (
            int(job.get("weight") or 1),
            str(job.get("created_at") or ""),
            pending_index,
        )

    @staticmethod
    def _is_heavy_job(job: Dict[str, Any]) -> bool:
        module = str(job.get("module") or "")
        return int(job.get("weight") or 1) > int(JOB_WEIGHTS.get(module, 1))

    def _queue_message(self, job_module: str, weight: int) -> str:
        if weight > int(JOB_WEIGHTS.get(job_module, 1)):
            return (
                "Queued: large matrix job uses most worker capacity "
                f"(scheduler weight {weight}/{self.capacity}) while leaving one lane available."
            )
        return (
            f"Queued: waiting for worker capacity "
            f"(scheduler weight {weight}/{self.capacity})."
        )

    @staticmethod
    def _load_json_config(config_path: Path) -> Dict[str, Any]:
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to read config {config_path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Config {config_path} must contain a JSON object")
        return payload

    def _wfa_candidate_count(self, config: Dict[str, Any], config_path: Path) -> int:
        strategy_config = self._workflow_strategy_run_config(config, config_path)
        raw_domains = strategy_config.get("parameter_domains")
        domains: Dict[str, Any] = raw_domains if isinstance(raw_domains, dict) else {}
        raw_count = self._parameter_variant_count(domains)
        if raw_count <= 1:
            return raw_count
        raw_optimizer = config.get("optimizer")
        optimizer: Dict[str, Any] = raw_optimizer if isinstance(raw_optimizer, dict) else {}
        raw_budget = self._first_present(
            optimizer.get("max_candidates"),
            optimizer.get("candidate_limit"),
            optimizer.get("n_trials"),
            config.get("max_candidates"),
            config.get("candidate_limit"),
        )
        budget = self._positive_int(raw_budget, default=0)
        if budget <= 0:
            return raw_count
        return min(raw_count, budget)

    def _workflow_strategy_run_config(self, config: Dict[str, Any], config_path: Path) -> Dict[str, Any]:
        if not isinstance(config, dict):
            return {}
        value = config.get("strategy_run_config")
        if isinstance(value, dict):
            return value
        relative_path = self.runtime._workflow_strategy_run_path(  # noqa: SLF001 - single compatibility entrypoint.
            config,
            config_file=config_path,
        )
        if not relative_path:
            return {}
        try:
            strategy_path = (self.runtime.repo_root / relative_path).resolve()
            strategy_path.relative_to(self.runtime.repo_root)
        except (ValueError, OSError):
            return {}
        if strategy_path == config_path or not strategy_path.exists():
            return {}
        return self._load_json_config(strategy_path)

    @staticmethod
    def _record_stage_duration(job: Dict[str, Any], ended_at: str) -> None:
        stage = str(job.get("stage") or "").strip()
        started_at = str(job.get("stage_started_at") or "").strip()
        if not stage or not started_at:
            return
        try:
            start = datetime.fromisoformat(started_at)
            end = datetime.fromisoformat(ended_at)
        except ValueError:
            return
        elapsed = max(0.0, (end - start).total_seconds())
        durations = job.get("stage_durations")
        if not isinstance(durations, dict):
            durations = {}
            job["stage_durations"] = durations
        durations[stage] = round(float(durations.get(stage, 0.0)) + elapsed, 3)

    @classmethod
    def _parameter_variant_count(cls, domains: Dict[str, Any]) -> int:
        if not domains:
            return 1
        total = 1
        for spec in domains.values():
            values = cls._parameter_domain_values(spec)
            if not values:
                continue
            total *= len(values)
        return total

    @staticmethod
    def _parameter_domain_values(spec: Any) -> List[Any]:
        if isinstance(spec, list):
            return list(spec)
        if not isinstance(spec, dict):
            return []
        if isinstance(spec.get("values"), list):
            return list(spec["values"])
        if str(spec.get("type", "")).lower() == "range" or {"start", "end"}.issubset(spec.keys()):
            try:
                start_raw = spec.get("start")
                end_raw = spec.get("end")
                if start_raw is None or end_raw is None:
                    return []
                start = int(start_raw)
                end = int(end_raw)
                step = int(spec.get("step") or 1)
            except (TypeError, ValueError):
                return []
            if step == 0:
                return []
            if start <= end and step > 0:
                return list(range(start, end + 1, step))
            if start >= end and step < 0:
                return list(range(start, end - 1, step))
        return []

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _positive_int(value: Any, *, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _reject_legacy_config_alias(name: str) -> None:
        if str(name).startswith("strategy-run-v2-"):
            raise ValueError(
                "Legacy config alias strategy-run-v2-* is no longer accepted; use the canonical strategy-run-* filename."
            )
        if str(name).startswith("wfa-run-v2-"):
            raise ValueError(
                "Legacy config alias wfa-run-v2-* is no longer accepted; use the canonical wfa-run-* filename."
            )

    @staticmethod
    def _resolve_job_module(module: str, config_path: Path, repo_root: Path) -> str:
        if module != "mixed":
            return module
        resolved = Path(config_path).resolve()
        for candidate_module, parts in CONFIG_ROOTS.items():
            expected_root = Path(repo_root).joinpath(*parts).resolve()
            try:
                resolved.relative_to(expected_root)
            except ValueError:
                continue
            return candidate_module
        return "autorunner"

    @staticmethod
    def _new_batch_id() -> str:
        return datetime.now().strftime("%Y%m%d_%H%M%S_") + f"{int(time.time_ns()) % 1000000:06d}"

    @staticmethod
    def _now_iso() -> str:
        return datetime.now().astimezone().isoformat(timespec="seconds")

    @staticmethod
    def _parse_iso_timestamp(raw: str) -> Optional[datetime]:
        value = str(raw or "").strip()
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
