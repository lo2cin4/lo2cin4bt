from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from verification.scripts.compare_utils import ensure_directory, write_json
from verification.scripts.prepare_truth_validation_fixtures import prepare_fixtures
from verification.scripts.run_project_case import build_case_workspace

VERIFICATION_ROOT = PROJECT_ROOT / "verification"
MANIFEST_ROOT = VERIFICATION_ROOT / "fixtures" / "manifests"
DEFAULT_BATCH_ID = "first_round"
DEFAULT_MANIFEST_PATH = MANIFEST_ROOT / f"{DEFAULT_BATCH_ID}_cases.json"


def resolve_manifest_path(batch_id: str = DEFAULT_BATCH_ID, path: Path | None = None) -> Path:
    if path is not None:
        return path
    return MANIFEST_ROOT / f"{batch_id}_cases.json"


def load_manifest(batch_id: str = DEFAULT_BATCH_ID, path: Path | None = None) -> Dict[str, Any]:
    manifest_path = resolve_manifest_path(batch_id, path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Truth validation manifest missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["_manifest_path"] = str(manifest_path)
    return payload


def initialize_batch(batch_id: str, manifest: Dict[str, Any], fixture_root: Path | None = None) -> Dict[str, Any]:
    prepare_fixtures(fixture_root)

    batch_cases: List[Dict[str, Any]] = []
    for case in manifest.get("cases", []):
        if case.get("enabled", True) is False:
            continue
        case_id = case.get("case_id") or case.get("id")
        case_workspace = build_case_workspace(batch_id, case_id)
        batch_cases.append(
            {
                "case_id": case_id,
                "module": case["module"],
                "scenario": case.get("scenario") or case.get("description", ""),
                "compare_mode": case.get("compare_mode", "old_vs_new"),
                "status": "TODO",
                "workspace": case_workspace,
            }
        )

    results_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id)
    summary = {
        "batch_id": batch_id,
        "manifest": manifest.get("_manifest_path", str(resolve_manifest_path(batch_id))),
        "status": "INITIALIZED",
        "case_count": len(batch_cases),
        "cases": batch_cases,
    }
    write_json(results_dir / "summary.json", summary)
    (results_dir / "summary.md").write_text(
        "\n".join(
            [
                f"# {batch_id}",
                "",
                f"- status: {summary['status']}",
                f"- case_count: {summary['case_count']}",
                "",
                "## Cases",
                *[
                    f"- `{case['case_id']}` | {case['module']} | {case['scenario']} | {case['status']}"
                    for case in batch_cases
                ],
            ]
        ),
        encoding="utf-8",
    )
    return summary


def refresh_batch_summary(batch_id: str) -> Dict[str, Any]:
    summary_path = VERIFICATION_ROOT / "results" / batch_id / "summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Truth validation summary missing: {summary_path}")

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    case_statuses = [case.get("status", "TODO") for case in payload.get("cases", [])]

    if case_statuses and all(status == "PASS" for status in case_statuses):
        payload["status"] = "COMPLETED"
    elif any(status == "FAIL" for status in case_statuses):
        payload["status"] = "FAILED"
    elif any(status == "PASS" for status in case_statuses):
        payload["status"] = "IN_PROGRESS"
    else:
        payload["status"] = "INITIALIZED"

    write_json(summary_path, payload)

    lines = [
        f"# {payload['batch_id']}",
        "",
        f"- status: {payload['status']}",
        f"- case_count: {payload.get('case_count', len(payload.get('cases', [])))}",
    ]
    for field in [
        "dataloader_status",
        "statanalyser_status",
        "backtester_status",
        "metrics_status",
        "wfa_status",
    ]:
        if field in payload:
            lines.append(f"- {field}: {payload[field]}")

    lines.extend(["", "## Cases"])
    for case in payload.get("cases", []):
        extra = ""
        if "difference_count" in case:
            extra = f" | diff={case['difference_count']}"
        lines.append(
            f"- `{case['case_id']}` | {case['module']} | {case['scenario']} | {case['status']}{extra}"
        )

    (summary_path.with_suffix(".md")).write_text("\n".join(lines), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialise a truth-validation batch.")
    parser.add_argument("--batch-id", default=DEFAULT_BATCH_ID)
    parser.add_argument("--manifest")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    if args.refresh:
        print(json.dumps(refresh_batch_summary(args.batch_id), ensure_ascii=False, indent=2))
        return
    manifest_path = Path(args.manifest).resolve() if args.manifest else None
    print(
        json.dumps(
            initialize_batch(args.batch_id, load_manifest(args.batch_id, manifest_path)),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
