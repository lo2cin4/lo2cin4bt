"""Exporter for unified portfolio WFA artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from utils.filename_utils import bounded_filename_stem
from validation_workflow.UnifiedPortfolioWFARunner_validation_workflow import UnifiedPortfolioWFAResult


class UnifiedPortfolioWFAExporter:
    """Write selected-optimum WFA artifacts with diagnostics separated."""

    def __init__(
        self,
        *,
        result: UnifiedPortfolioWFAResult,
        output_dir: Path | str,
        run_id: str,
        export_diagnostics: bool = True,
    ) -> None:
        self.result = result
        self.output_dir = Path(output_dir)
        self.run_id = bounded_filename_stem(run_id or "unified_wfa", max_length=80, fallback="unified_wfa")
        self.export_diagnostics = bool(export_diagnostics)

    def export(self) -> List[str]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        paths: List[str] = []

        selected_path = self.output_dir / f"{self.run_id}_selected_optimum.parquet"
        self.result.selected_optimum.to_parquet(selected_path)
        paths.append(str(selected_path))

        if self.export_diagnostics:
            diagnostics_path = self.output_dir / f"{self.run_id}_candidate_diagnostics.parquet"
            self.result.candidate_diagnostics.to_parquet(diagnostics_path)
            paths.append(str(diagnostics_path))

        metadata = dict(self.result.metadata)
        metadata["selected_optimum_artifact"] = selected_path.name
        metadata["candidate_diagnostics_artifact"] = (
            f"{self.run_id}_candidate_diagnostics.parquet" if self.export_diagnostics else None
        )
        metadata_path = self.output_dir / f"{self.run_id}_metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        paths.append(str(metadata_path))
        return paths


def load_unified_wfa_metadata(path: Path | str) -> Optional[dict]:
    metadata_path = Path(path)
    if not metadata_path.exists():
        return None
    return json.loads(metadata_path.read_text(encoding="utf-8"))
