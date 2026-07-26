from __future__ import annotations

import asyncio
import io
import mimetypes
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .service import AppAPIService


class BatchRequest(BaseModel):
    module: str = Field(pattern="^(autorunner|wfa|statanalyser|mixed)$")
    config_paths: List[str]


class WorkspaceOpenRequest(BaseModel):
    target: str = Field(pattern="^[a-zA-Z0-9_-]+$")


class ParameterMatrixReviewPreviewRequest(BaseModel):
    acceptance: Dict[str, Any] | None = None
    ranking: Dict[str, Any] | None = None


class ParameterReviewTemplateRequest(BaseModel):
    name: str
    acceptance: Dict[str, Any] | None = None
    ranking: Dict[str, Any] | None = None

class ParameterReviewTemplateDeleteRequest(BaseModel):
    name: str


class ScreenshotBundleRequest(BaseModel):
    run_id: str
    backtest_id: str
    mosaic: bool = False


class LateBoundStaticFiles(StaticFiles):
    async def check_config(self) -> None:
        self.config_checked = True


def create_app(repo_root: Path) -> FastAPI:
    repo_root = Path(repo_root).resolve()
    mimetypes.add_type("font/ttf", ".ttf")
    mimetypes.add_type("font/woff2", ".woff2")
    service = AppAPIService(repo_root)
    app = FastAPI(title="lo2cin4bt App API", version="2.1.0")
    app.state.app_service = service
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)

    @app.middleware("http")
    async def no_store_app_responses(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/api/") or request.url.path.startswith("/assets/") or "text/html" in request.headers.get("accept", ""):
            response.headers["Cache-Control"] = "no-store, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @app.get("/api/app/health")
    def health() -> Dict[str, Any]:
        return {
            "status": "ok",
            "server_session_id": service.server_session_id,
            "server_started_at": service.server_started_at,
        }

    @app.get("/api/app/command-center")
    def command_center() -> Dict[str, Any]:
        return service.command_center()

    @app.get("/api/app/run-center/configs")
    def run_center_configs() -> Dict[str, Any]:
        return service.run_center_configs()

    @app.post("/api/app/workspace/open")
    def open_workspace(payload: WorkspaceOpenRequest) -> Dict[str, Any]:
        try:
            return service.open_workspace_target(payload.target)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Unable to open workspace: {exc}") from exc

    @app.post("/api/app/folders/open")
    def open_folder(payload: WorkspaceOpenRequest) -> Dict[str, Any]:
        try:
            return service.open_local_folder_target(payload.target)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Unable to open local folder: {exc}") from exc

    @app.post("/api/app/batches")
    def create_batch(payload: BatchRequest) -> Dict[str, Any]:
        if not payload.config_paths:
            raise HTTPException(status_code=400, detail="config_paths cannot be empty")
        try:
            return service.scheduler.submit_batch(payload.module, payload.config_paths)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/app/batches/{batch_id}")
    def get_batch(batch_id: str) -> Dict[str, Any]:
        try:
            return service.scheduler.get_batch(batch_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown batch: {batch_id}") from exc

    @app.post("/api/app/batches/{batch_id}/cancel")
    def cancel_batch(batch_id: str) -> Dict[str, Any]:
        try:
            return service.scheduler.cancel_batch(batch_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown batch: {batch_id}") from exc

    @app.websocket("/api/app/batches/{batch_id}/stream")
    async def batch_stream(websocket: WebSocket, batch_id: str) -> None:
        await websocket.accept()
        cursor = 0
        try:
            while True:
                events = service.scheduler.get_events_since(batch_id, cursor)
                if events:
                    for event in events:
                        await websocket.send_json(event)
                    cursor += len(events)
                await asyncio.sleep(0.35)
        except WebSocketDisconnect:
            return

    @app.get("/api/app/metrics/runs")
    def metrics_runs() -> List[Dict[str, Any]]:
        return service.metrics_runs()

    @app.post("/api/app/screenshots")
    def capture_screenshot_bundle(payload: ScreenshotBundleRequest, request: Request) -> Dict[str, Any]:
        try:
            return service.capture_screenshot_bundle(
                run_id=payload.run_id,
                backtest_id=payload.backtest_id,
                base_url=str(request.base_url).rstrip("/"),
                mosaic=payload.mosaic,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.get("/api/app/wfa/runs")
    def wfa_runs() -> List[Dict[str, Any]]:
        return service.wfa_runs()

    @app.get("/api/app/statanalyser/runs")
    def stat_runs() -> List[Dict[str, Any]]:
        return service.stat_runs()

    @app.get("/api/app/metrics/{run_id}/overview", response_model=None)
    def metrics_overview(run_id: str):
        try:
            return JSONResponse(service.metrics_overview(run_id))
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/app/metrics/{run_id}/parameter-matrix")
    def parameter_matrix(run_id: str) -> Dict[str, Any]:
        try:
            return service.parameter_matrix(run_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/app/metrics/{run_id}/parameter-matrix/review-preview")
    def parameter_matrix_review_preview(run_id: str, payload: ParameterMatrixReviewPreviewRequest) -> Dict[str, Any]:
        try:
            return service.parameter_matrix_review_preview(run_id, payload.model_dump())
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/app/parameter-review/templates")
    def list_parameter_review_templates() -> Dict[str, Any]:
        return service.list_parameter_review_templates()

    @app.post("/api/app/parameter-review/templates")
    def save_parameter_review_template(payload: ParameterReviewTemplateRequest) -> Dict[str, Any]:
        try:
            return service.save_parameter_review_template(payload.model_dump())
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.delete("/api/app/parameter-review/templates")
    def delete_parameter_review_template(payload: ParameterReviewTemplateDeleteRequest) -> Dict[str, Any]:
        try:
            return service.delete_parameter_review_template(payload.name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.put("/api/app/parameter-review/templates/default")
    def set_default_parameter_review_template(payload: ParameterReviewTemplateDeleteRequest) -> Dict[str, Any]:
        try:
            return service.set_default_parameter_review_template(payload.name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/app/wfa/{run_id}/dashboard")
    def wfa_dashboard(run_id: str) -> Dict[str, Any]:
        try:
            return service.wfa_dashboard(run_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/app/backtests/{run_id}/{backtest_id}")
    def backtest_detail(run_id: str, backtest_id: str):
        try:
            service.backtest_detail_path(run_id, backtest_id)
            return JSONResponse(service.backtest_detail(run_id, backtest_id))
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/app/backtests/{run_id}/{backtest_id}/export.csv")
    def export_backtest_csv(run_id: str, backtest_id: str):
        try:
            frame, filename = service.export_backtest_csv(run_id, backtest_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        buffer = io.StringIO()
        frame.to_csv(buffer, index=False)
        buffer.seek(0)
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return StreamingResponse(buffer, media_type="text/csv; charset=utf-8", headers=headers)

    @app.get("/api/app/statanalyser/{run_id}")
    def statanalyser_summary(run_id: str) -> Dict[str, Any]:
        try:
            return service.statanalyser_summary(run_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/app/ai-readable/{run_id}")
    def ai_readable_output(run_id: str) -> Dict[str, Any]:
        try:
            return service.ai_readable_output(run_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    dist_dir = repo_root / "plotter" / "web" / "dist"
    assets_dir = dist_dir / "assets"
    app.mount(
        "/assets",
        LateBoundStaticFiles(directory=assets_dir, check_dir=False),
        name="app-assets",
    )
    fonts_dir = dist_dir / "fonts"
    app.mount(
        "/fonts",
        LateBoundStaticFiles(directory=fonts_dir, check_dir=False),
        name="app-fonts",
    )

    @app.get("/{full_path:path}", response_model=None)
    async def spa(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        index_path = dist_dir / "index.html"
        if index_path.exists():
            return FileResponse(
                index_path,
                headers={
                    "Cache-Control": "no-store, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
        return JSONResponse(
            {
                "message": "App web frontend is not built yet.",
                "expected_dist": str(index_path),
                "next_steps": [
                    "cd plotter/web",
                    "npm ci",
                    "npm run build",
                    "cd ../..",
                    "python main.py",
                ],
                "note": "This is expected after a fresh GitHub clone or source checkout because dist/ is a generated build artifact.",
            },
            status_code=503,
        )

    return app
