"""Summarize audit sidecars for AI/debug workflows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backtester.AuditReader_backtester import AuditReaderBacktester


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize audit sidecars for a primary artifact.")
    parser.add_argument("artifact_path", help="Path to the primary parquet/csv/xlsx artifact")
    parser.add_argument("--stale-ratio-warn", type=float, default=0.2)
    parser.add_argument("--fill-ratio-warn", type=float, default=0.2)
    parser.add_argument("--max-age-warn", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact_path = Path(args.artifact_path).resolve()
    bundle = AuditReaderBacktester.load_audit_bundle(artifact_path)
    quality = AuditReaderBacktester.summarize_quality(
        artifact_path,
        stale_ratio_warn=args.stale_ratio_warn,
        fill_ratio_warn=args.fill_ratio_warn,
        max_age_warn=args.max_age_warn,
    )
    payload = {
        "artifact_path": str(artifact_path),
        "metadata": bundle.get("metadata", {}),
        "quality_summary": quality,
        "audit_manifest_overview": {
            "audit_rows_inline": bundle.get("audit_manifest", {}).get("audit_rows_inline"),
            "chunk_count": len(bundle.get("audit_manifest", {}).get("audit_row_chunks", []) or []),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
