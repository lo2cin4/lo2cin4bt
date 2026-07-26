"""Shared path resolution helpers for workspace/outputs-only runtime."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class ResolvedPath:
    """Resolved path metadata for diagnostics and deprecation warnings."""

    path: Path
    mode: str
    used_fallback: bool
    matched_legacy_root: Optional[Path] = None


def repo_root_from(anchor_file: str) -> Path:
    """Resolve repository root from a module file path."""
    return Path(anchor_file).resolve().parent.parent


def workspace_root(repo_root: Path) -> Path:
    return repo_root / "workspace"


def outputs_root(repo_root: Path) -> Path:
    """Canonical runtime output root for current versions."""
    return repo_root / "outputs"


def app_outputs_root(repo_root: Path) -> Path:
    """Canonical app-managed output root for browser-first artifacts."""
    return outputs_root(repo_root) / "app"


def legacy_records_root(repo_root: Path) -> Path:
    """Legacy output root path (deprecated; no runtime fallback)."""
    return repo_root / "records"


def app_outputs_paths(repo_root: Path) -> dict[str, Path]:
    """Return canonical app output paths without touching the filesystem."""
    app_root = app_outputs_root(repo_root)
    return {
        "root": app_root,
        "run_registry": app_root / "run_registry",
        "run_snapshots": app_root / "run_snapshots",
        "artifact_manifests": app_root / "artifact_manifests",
        "chart_payloads": app_root / "chart_payloads",
        "ai_review": app_root / "ai_review",
        "screenshots": app_root / "screenshots",
        "stage_status": app_root / "stage_status",
        "latest_runs": app_root / "latest_runs.json",
    }


def ensure_app_outputs_structure(repo_root: Path) -> dict[str, Path]:
    """Ensure canonical app output folders exist and return their paths."""
    paths = app_outputs_paths(repo_root)
    for key, path in paths.items():
        if key == "latest_runs":
            continue
        path.mkdir(parents=True, exist_ok=True)
    if not paths["latest_runs"].exists():
        paths["latest_runs"].write_text("[]\n", encoding="utf-8")
    return paths


def build_app_run_paths(
    repo_root: Path,
    run_id: str,
    *,
    create: bool = True,
) -> dict[str, Path]:
    """Build stable paths for one run, creating directories only when requested."""
    run_id_text = str(run_id or "").strip()
    if not run_id_text:
        raise ValueError("run_id is required")
    if (
        run_id_text in {".", ".."}
        or "/" in run_id_text
        or "\\" in run_id_text
        or ":" in run_id_text
        or Path(run_id_text).name != run_id_text
    ):
        raise ValueError("run_id must be a plain filename-safe identifier")

    app_paths = (
        ensure_app_outputs_structure(repo_root)
        if create
        else app_outputs_paths(repo_root)
    )
    snapshot_dir = app_paths["run_snapshots"] / run_id_text
    chart_payload_dir = app_paths["chart_payloads"] / run_id_text
    ai_review_dir = app_paths["ai_review"] / run_id_text
    screenshot_dir = app_paths["screenshots"] / run_id_text
    if create:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        chart_payload_dir.mkdir(parents=True, exist_ok=True)
        ai_review_dir.mkdir(parents=True, exist_ok=True)
        screenshot_dir.mkdir(parents=True, exist_ok=True)
    return {
        "app_root": app_paths["root"],
        "run_registry": app_paths["run_registry"] / f"{run_id_text}.json",
        "latest_runs": app_paths["latest_runs"],
        "artifact_manifest": app_paths["artifact_manifests"] / f"{run_id_text}.json",
        "stage_status": app_paths["stage_status"] / f"{run_id_text}.json",
        "snapshot_dir": snapshot_dir,
        "run_config_snapshot": snapshot_dir / "run_config.json",
        "dataloader_config_snapshot": snapshot_dir / "dataloader_config.json",
        "backtester_config_snapshot": snapshot_dir / "backtester_config.json",
        "dataloader_health": snapshot_dir / "dataloader_health.json",
        "data_lineage_manifest": snapshot_dir / "data_lineage_manifest.json",
        "execution_plan_snapshot": snapshot_dir / "execution_plan.json",
        "chart_payload_dir": chart_payload_dir,
        "shared_series_dir": chart_payload_dir / "shared_series",
        "ai_review_dir": ai_review_dir,
        "ai_readable_output": ai_review_dir / "ai_review_pack.json",
        "screenshot_dir": screenshot_dir,
    }


def ensure_workspace_structure(repo_root: Path) -> dict[str, Path]:
    """Ensure canonical workspace folders exist and return their paths."""
    ws_root = workspace_root(repo_root)
    paths = {
        "root": ws_root,
        "datasets": ws_root / "datasets",
        "features": ws_root / "features",
        "strategies": ws_root / "strategies",
        "runs": ws_root / "runs",
        "wfa": ws_root / "wfa",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def resolve_input_path(
    raw_path: str,
    *,
    repo_root: Path,
    config_file_path: Optional[str] = None,
) -> ResolvedPath:
    """Resolve an input path with stable precedence.

    Precedence:
    1) absolute path
    2) repo-root relative path
    3) config-file relative path
    """
    text = str(raw_path or "").strip()
    if not text:
        raise ValueError("empty path")

    candidate = Path(text)
    if candidate.is_absolute():
        return ResolvedPath(path=candidate, mode="absolute", used_fallback=False)

    repo_candidate = (repo_root / candidate).resolve()
    if repo_candidate.exists():
        return ResolvedPath(
            path=repo_candidate, mode="repo_relative", used_fallback=False
        )

    if config_file_path:
        config_candidate = (Path(config_file_path).resolve().parent / candidate).resolve()
        if config_candidate.exists():
            return ResolvedPath(
                path=config_candidate, mode="config_relative", used_fallback=False
            )

    # Return repo-relative target for deterministic error messaging upstream.
    return ResolvedPath(path=repo_candidate, mode="repo_relative", used_fallback=False)


def discover_configs(
    *,
    primary_dir: Path,
    legacy_dirs: Optional[Iterable[Path]] = None,
) -> list[Path]:
    """Discover runnable config files from primary workspace directory only."""
    ordered_dirs = [primary_dir]
    seen_names: set[str] = set()
    selected: list[Path] = []

    for directory in ordered_dirs:
        directory = Path(directory)
        if not directory.exists():
            continue
        for file_path in sorted(directory.glob("*.json")):
            if "template" in file_path.stem.lower():
                continue
            name_key = file_path.name.lower()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)
            selected.append(file_path.resolve())
    return selected


def detect_config_source(
    config_path: Path,
    *,
    primary_dir: Path,
    legacy_dirs: Optional[Iterable[Path]] = None,
) -> str:
    """Classify config source for precedence diagnostics."""
    resolved = config_path.resolve()
    if resolved.parent == primary_dir.resolve():
        return "workspace"
    return "other"
