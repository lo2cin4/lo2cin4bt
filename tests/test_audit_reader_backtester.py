from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


from backtester.AuditReader_backtester import AuditReaderBacktester


def test_audit_reader_summarizes_quality_from_metadata(tmp_path: Path) -> None:
    artifact_path = tmp_path / "demo.parquet"
    artifact_path.write_text("placeholder", encoding="utf-8")
    metadata_path = tmp_path / "demo_metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "audit_row_count": 10,
                "feature_audit_summary": [
                    {
                        "feature_field": "feature.vix.close",
                        "source_id": "vix_daily",
                        "rows": 10,
                        "stale_rows": 3,
                        "filled_rows": 4,
                        "max_age_bars": 6,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = AuditReaderBacktester.summarize_quality(artifact_path)

    assert summary["feature_count"] == 1
    assert summary["stale_ratio"] == 0.3
    assert summary["fill_ratio"] == 0.4
    assert summary["max_age_bars"] == 6
    assert any("High stale ratio" in msg for msg in summary["warnings"])
    assert any("High fill ratio" in msg for msg in summary["warnings"])
    assert any("High max age" in msg for msg in summary["warnings"])
    assert summary["audit_available"] is True


def test_audit_reader_marks_missing_audit_as_unavailable(tmp_path: Path) -> None:
    artifact_path = tmp_path / "demo.parquet"
    artifact_path.write_text("placeholder", encoding="utf-8")

    summary = AuditReaderBacktester.summarize_quality(artifact_path)

    assert summary["audit_available"] is False
    assert summary["stale_ratio"] is None
    assert summary["fill_ratio"] is None
    assert summary["max_age_bars"] is None


def test_audit_reader_rejects_incomplete_feature_summary(tmp_path: Path) -> None:
    artifact_path = tmp_path / "demo.parquet"
    artifact_path.write_text("placeholder", encoding="utf-8")
    (tmp_path / "demo_metadata.json").write_text(
        json.dumps(
            {
                "audit_row_count": 10,
                "feature_audit_summary": [
                    {
                        "feature_field": "feature.vix.close",
                        "rows": 10,
                        "stale_rows": 0,
                        "filled_rows": 0,
                        "max_age_bars": 0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="source_id"):
        AuditReaderBacktester.summarize_quality(artifact_path)


def test_summarize_audit_sidecar_script_outputs_machine_readable_summary(tmp_path: Path) -> None:
    artifact_path = tmp_path / "demo.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(artifact_path, index=False)
    metadata_path = tmp_path / "demo_metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "audit_row_count": 4,
                "feature_audit_summary": [
                    {
                        "feature_field": "feature.market_breadth.close",
                        "source_id": "breadth_daily",
                        "rows": 4,
                        "stale_rows": 1,
                        "filled_rows": 0,
                        "max_age_bars": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "demo_audit.json").write_text(
        json.dumps({"audit_rows_inline": True, "audit_rows": []}),
        encoding="utf-8",
    )
    pd.DataFrame({"feature_field": ["feature.market_breadth.close"]}).to_parquet(
        tmp_path / "demo_audit.parquet",
        index=False,
    )

    script_path = _REPO_ROOT / "verification" / "scripts" / "summarize_audit_sidecar.py"
    completed = subprocess.run(
        [sys.executable, str(script_path), str(artifact_path)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["artifact_path"] == str(artifact_path.resolve())
    assert payload["quality_summary"]["feature_count"] == 1
    assert payload["audit_manifest_overview"]["chunk_count"] == 0
