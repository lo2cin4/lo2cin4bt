"""Helpers for reading machine-readable audit sidecars produced by backtester exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


class AuditReaderBacktester:
    """Load metadata and detailed audit sidecars for a primary backtest artifact."""

    @staticmethod
    def _primary_path(primary_artifact_path: str | Path) -> Path:
        return Path(primary_artifact_path).resolve()

    @staticmethod
    def _sidecar_base(primary_artifact_path: str | Path) -> Path:
        return AuditReaderBacktester._primary_path(primary_artifact_path).with_suffix("")

    @staticmethod
    def _metadata_path(primary_artifact_path: str | Path) -> Path:
        base = AuditReaderBacktester._sidecar_base(primary_artifact_path)
        return base.with_name(base.name + "_metadata.json")

    @staticmethod
    def _audit_json_path(primary_artifact_path: str | Path) -> Path:
        base = AuditReaderBacktester._sidecar_base(primary_artifact_path)
        return base.with_name(base.name + "_audit.json")

    @staticmethod
    def _audit_parquet_path(primary_artifact_path: str | Path) -> Path:
        base = AuditReaderBacktester._sidecar_base(primary_artifact_path)
        return base.with_name(base.name + "_audit.parquet")

    @staticmethod
    def load_metadata(primary_artifact_path: str | Path) -> Dict[str, Any]:
        path = AuditReaderBacktester._metadata_path(primary_artifact_path)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def load_audit_manifest(primary_artifact_path: str | Path) -> Dict[str, Any]:
        path = AuditReaderBacktester._audit_json_path(primary_artifact_path)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def load_audit_frame(primary_artifact_path: str | Path) -> pd.DataFrame:
        parquet_path = AuditReaderBacktester._audit_parquet_path(primary_artifact_path)
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)

        manifest = AuditReaderBacktester.load_audit_manifest(primary_artifact_path)
        audit_rows = manifest.get("audit_rows", [])
        if isinstance(audit_rows, list) and audit_rows:
            return pd.DataFrame(audit_rows)

        chunk_rows: List[Dict[str, Any]] = []
        for chunk in manifest.get("audit_row_chunks", []) or []:
            chunk_path = Path(str(chunk.get("path", "")))
            if not chunk_path.exists():
                continue
            with chunk_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if text:
                        chunk_rows.append(json.loads(text))
        return pd.DataFrame(chunk_rows)

    @staticmethod
    def summarize_feature_audit(
        primary_artifact_path: str | Path,
        *,
        feature_field: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        metadata = AuditReaderBacktester.load_metadata(primary_artifact_path)
        summary = metadata.get("feature_audit_summary", [])
        if not isinstance(summary, list):
            return []
        if feature_field is None:
            return summary
        return [
            item
            for item in summary
            if isinstance(item, dict) and str(item.get("feature_field", "")) == str(feature_field)
        ]

    @staticmethod
    def summarize_quality(
        primary_artifact_path: str | Path,
        *,
        stale_ratio_warn: float = 0.2,
        fill_ratio_warn: float = 0.2,
        max_age_warn: int = 5,
    ) -> Dict[str, Any]:
        metadata = AuditReaderBacktester.load_metadata(primary_artifact_path)
        summary_rows = metadata.get("feature_audit_summary", [])
        if not isinstance(summary_rows, list):
            raise ValueError("feature_audit_summary must be a list")

        primary_path = str(AuditReaderBacktester._primary_path(primary_artifact_path))
        if not summary_rows:
            return {
                "primary_artifact_path": primary_path,
                "audit_available": False,
                "feature_count": 0,
                "audit_row_count": 0,
                "total_rows": 0,
                "total_stale_rows": 0,
                "total_filled_rows": 0,
                "stale_ratio": None,
                "fill_ratio": None,
                "max_age_bars": None,
                "warnings": [],
                "features": [],
            }

        total_rows = 0
        total_stale_rows = 0
        total_filled_rows = 0
        max_age_bars = 0
        feature_summaries: List[Dict[str, Any]] = []
        warnings: List[str] = []

        for index, item in enumerate(summary_rows):
            if not isinstance(item, dict):
                raise ValueError(f"feature_audit_summary[{index}] must be an object")
            feature_field = _required_text(item, "feature_field", index)
            source_id = _required_text(item, "source_id", index)
            rows = _required_nonnegative_int(item, "rows", index)
            stale_rows = _required_nonnegative_int(item, "stale_rows", index)
            filled_rows = _required_nonnegative_int(item, "filled_rows", index)
            max_age = _required_nonnegative_int(item, "max_age_bars", index)
            if rows <= 0:
                raise ValueError(f"feature_audit_summary[{index}].rows must be positive")
            if stale_rows > rows or filled_rows > rows:
                raise ValueError(
                    f"feature_audit_summary[{index}] stale_rows/filled_rows cannot exceed rows"
                )
            total_rows += rows
            total_stale_rows += stale_rows
            total_filled_rows += filled_rows
            max_age_bars = max(max_age_bars, max_age)
            stale_ratio = (stale_rows / rows) if rows > 0 else 0.0
            fill_ratio = (filled_rows / rows) if rows > 0 else 0.0
            feature_summary = {
                "feature_field": feature_field,
                "source_id": source_id,
                "rows": rows,
                "stale_rows": stale_rows,
                "filled_rows": filled_rows,
                "max_age_bars": max_age,
                "stale_ratio": stale_ratio,
                "fill_ratio": fill_ratio,
            }
            feature_summaries.append(feature_summary)
            if stale_ratio_warn >= 0 and stale_ratio >= stale_ratio_warn:
                warnings.append(
                    f"High stale ratio for {feature_summary['feature_field']}: {stale_ratio:.1%}"
                )
            if fill_ratio_warn >= 0 and fill_ratio >= fill_ratio_warn:
                warnings.append(
                    f"High fill ratio for {feature_summary['feature_field']}: {fill_ratio:.1%}"
                )
            if max_age_warn >= 0 and max_age >= max_age_warn:
                warnings.append(
                    f"High max age for {feature_summary['feature_field']}: {max_age} bars"
                )

        return {
            "primary_artifact_path": primary_path,
            "audit_available": True,
            "feature_count": len(feature_summaries),
            "audit_row_count": _required_nonnegative_int(metadata, "audit_row_count", "metadata"),
            "total_rows": total_rows,
            "total_stale_rows": total_stale_rows,
            "total_filled_rows": total_filled_rows,
            "stale_ratio": (total_stale_rows / total_rows) if total_rows > 0 else 0.0,
            "fill_ratio": (total_filled_rows / total_rows) if total_rows > 0 else 0.0,
            "max_age_bars": max_age_bars,
            "warnings": warnings,
            "features": feature_summaries,
        }

    @staticmethod
    def load_audit_bundle(primary_artifact_path: str | Path) -> Dict[str, Any]:
        return {
            "primary_artifact_path": str(AuditReaderBacktester._primary_path(primary_artifact_path)),
            "metadata": AuditReaderBacktester.load_metadata(primary_artifact_path),
            "audit_manifest": AuditReaderBacktester.load_audit_manifest(primary_artifact_path),
            "audit_frame": AuditReaderBacktester.load_audit_frame(primary_artifact_path),
            "quality_summary": AuditReaderBacktester.summarize_quality(primary_artifact_path),
        }


def _required_text(item: Dict[str, Any], field: str, index: Any) -> str:
    value = item.get(field)
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"feature_audit_summary[{index}].{field} is required")
    return text


def _required_nonnegative_int(item: Dict[str, Any], field: str, index: Any) -> int:
    if field not in item or isinstance(item[field], bool):
        raise ValueError(f"feature_audit_summary[{index}].{field} is required")
    try:
        value = int(item[field])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"feature_audit_summary[{index}].{field} must be an integer"
        ) from exc
    if value < 0:
        raise ValueError(
            f"feature_audit_summary[{index}].{field} must be non-negative"
        )
    return value
