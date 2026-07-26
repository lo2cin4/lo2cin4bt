"""Concurrent client for the single persistent Rust engine service."""

from __future__ import annotations

import queue
import subprocess
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional


class EngineServiceError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable


class EngineServiceClient:
    protocol_version = "engine_service.v1"

    def __init__(
        self,
        process_factory: Callable[[], subprocess.Popen[str]],
        *,
        availability_check: Callable[[], bool],
    ) -> None:
        self._process_factory = process_factory
        self._availability_check = availability_check
        self._lifecycle_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._process: Optional[subprocess.Popen[str]] = None
        self._pending: Dict[str, queue.Queue[Dict[str, Any]]] = {}
        self._active_execute: set[str] = set()

    @property
    def process(self) -> Optional[subprocess.Popen[str]]:
        with self._lifecycle_lock:
            return self._process

    def start(self) -> subprocess.Popen[str]:
        if not self._availability_check():
            raise RuntimeError("Rust core is unavailable; cargo or crate directory is missing")
        with self._lifecycle_lock:
            process = self._process
            if process is not None and process.poll() is None:
                return process
            if process is not None:
                self._process = None
                self._fail_pending("service_exited", "Rust engine service exited")
            process = self._process_factory()
            if process.stdout is None or process.stdin is None:
                self._terminate_process(process)
                raise RuntimeError("Rust engine service requires stdin and stdout pipes")
            self._process = process
            threading.Thread(
                target=self._reader_loop,
                args=(process,),
                name="lo2cin4bt-engine-service-reader",
                daemon=True,
            ).start()
            return process

    def execute(
        self,
        operation: str,
        payload: Dict[str, Any],
        *,
        timeout: int,
        request_id: Optional[str] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        result = self.request(
            "execute",
            payload,
            timeout=timeout,
            operation=operation,
            request_id=request_id,
            progress_callback=progress_callback,
        )
        if not isinstance(result, dict):
            raise RuntimeError("Rust engine service result must be an object")
        return result

    def execute_engine_request(
        self,
        engine_request: Dict[str, Any],
        market_data_bundle: Dict[str, Any],
        *,
        timeout: int,
        artifact_output_dir: Optional[str] = None,
        artifact_run_id: Optional[str] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        result = self.request(
            "execute_engine_request",
            {
                "engine_request": engine_request,
                "market_data_bundle": market_data_bundle,
                "artifact_output_dir": artifact_output_dir,
                "artifact_run_id": artifact_run_id,
            },
            timeout=timeout,
            progress_callback=progress_callback,
        )
        if not isinstance(result, dict):
            raise RuntimeError("Rust EngineRequest result must be an object")
        return result

    def execute_engine_request_batch(
        self,
        engine_requests: List[Dict[str, Any]],
        market_data_bundle: Dict[str, Any],
        *,
        timeout: int,
        artifact_output_dir: Optional[str] = None,
        artifact_run_id: Optional[str] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        result = self.request(
            "execute_engine_request_batch",
            {
                "engine_requests": engine_requests,
                "market_data_bundle": market_data_bundle,
                "artifact_output_dir": artifact_output_dir,
                "artifact_run_id": artifact_run_id,
            },
            timeout=timeout,
            progress_callback=progress_callback,
        )
        if not isinstance(result, dict):
            raise RuntimeError("Rust EngineRequest batch result must be an object")
        return result

    def request(
        self,
        command: str,
        payload: Any,
        *,
        timeout: int,
        operation: Optional[str] = None,
        request_id: Optional[str] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Any:
        process = self.start()
        resolved_request_id = request_id or f"python-{uuid.uuid4().hex}"
        response_queue: queue.Queue[Dict[str, Any]] = queue.Queue()
        with self._pending_lock:
            if resolved_request_id in self._pending:
                raise ValueError(f"duplicate engine request_id: {resolved_request_id}")
            self._pending[resolved_request_id] = response_queue
            if command in {
                "execute",
                "execute_engine_request",
                "execute_engine_request_batch",
            }:
                self._active_execute.add(resolved_request_id)

        timeout_seconds = max(1, int(timeout))
        envelope = {
            "protocol_version": self.protocol_version,
            "request_id": resolved_request_id,
            "command": command,
            "operation": operation,
            "payload": payload,
            "deadline_unix_ms": int(time.time() * 1000) + timeout_seconds * 1000,
            "resource_budget": {"max_operation_ms": timeout_seconds * 1000},
        }
        try:
            self._write(process, envelope)
            deadline = time.monotonic() + timeout_seconds
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired("engine_service_cli", timeout_seconds)
                try:
                    response = response_queue.get(timeout=remaining)
                except queue.Empty as exc:
                    raise subprocess.TimeoutExpired(
                        "engine_service_cli", timeout_seconds
                    ) from exc
                status = str(response.get("status") or "")
                if status == "progress":
                    progress = response.get("result")
                    if progress_callback is not None and isinstance(progress, dict):
                        progress_callback(progress)
                    continue
                if status == "error":
                    raw_error = response.get("error")
                    error = raw_error if isinstance(raw_error, dict) else {}
                    raise EngineServiceError(
                        str(error.get("code") or "operation_failed"),
                        str(error.get("message") or "Rust engine service operation failed"),
                        retryable=bool(error.get("retryable")),
                    )
                if status not in {"ok", "shutting_down"}:
                    raise RuntimeError(f"unknown Rust engine service status: {status}")
                return response.get("result")
        finally:
            with self._pending_lock:
                self._pending.pop(resolved_request_id, None)
                self._active_execute.discard(resolved_request_id)

    def cancel_all(self, *, timeout: int = 2) -> list[str]:
        with self._pending_lock:
            targets = sorted(self._active_execute)
        accepted: list[str] = []
        for target in targets:
            try:
                self.request(
                    "cancel",
                    {"target_request_id": target},
                    timeout=timeout,
                )
                accepted.append(target)
            except Exception:
                continue
        return accepted

    def close(self, *, graceful: bool = False) -> None:
        process = self.process
        if process is None:
            return
        if graceful and process.poll() is None:
            try:
                self.request("shutdown", {}, timeout=2)
            except Exception:
                pass
        with self._lifecycle_lock:
            if self._process is process:
                self._process = None
        self._terminate_process(process)

    def _write(self, process: subprocess.Popen[str], envelope: Dict[str, Any]) -> None:
        import json

        encoded = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
        with self._write_lock:
            if process.poll() is not None or process.stdin is None:
                raise RuntimeError("Rust engine service is not running")
            process.stdin.write(encoded + "\n")
            process.stdin.flush()

    def _reader_loop(self, process: subprocess.Popen[str]) -> None:
        import json

        stdout = process.stdout
        if stdout is None:
            return
        try:
            for line in stdout:
                try:
                    response = json.loads(line)
                except json.JSONDecodeError:
                    self._fail_pending(
                        "invalid_service_response",
                        "Rust engine service emitted invalid JSON",
                    )
                    break
                if not isinstance(response, dict):
                    self._fail_pending(
                        "invalid_service_response",
                        "Rust engine service response must be a JSON object",
                    )
                    break
                request_id = str(response.get("request_id") or "")
                with self._pending_lock:
                    target = self._pending.get(request_id)
                if target is not None:
                    target.put(response)
        finally:
            owned_process = False
            with self._lifecycle_lock:
                if self._process is process:
                    self._process = None
                    owned_process = True
            if owned_process:
                self._fail_pending("service_exited", "Rust engine service exited")

    def _fail_pending(self, code: str, message: str) -> None:
        with self._pending_lock:
            targets = list(self._pending.values())
        frame = {
            "status": "error",
            "error": {"code": code, "message": message, "retryable": True},
        }
        for target in targets:
            target.put(frame)

    @staticmethod
    def _terminate_process(process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
