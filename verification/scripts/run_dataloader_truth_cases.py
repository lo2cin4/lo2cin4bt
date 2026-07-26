from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from verification.scripts.compare_utils import compare_dataframes, ensure_directory, write_json
from verification.scripts.paths import FIXTURE_ROOT, NEW_ROOT, OLD_ROOT, VERIFICATION_ROOT, WORKER_SCRIPT
from verification.scripts.run_truth_validation_batch import load_manifest


def _iter_dataloader_cases(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [case for case in manifest.get("cases", []) if case.get("module") == "dataloader" and case.get("enabled", True)]


def _write_case_file(case: Dict[str, Any], result_dir: Path) -> Path:
    case_file = result_dir / "resolved_case_input.json"
    case_file.write_text(json.dumps(case, ensure_ascii=False, indent=2), encoding="utf-8")
    return case_file


def _run_worker(project_root: Path, case: Dict[str, Any], output_root: Path, result_dir: Path) -> Dict[str, Any]:
    ensure_directory(output_root)
    case_file = _write_case_file(case, result_dir)
    subprocess.run(
        [
            sys.executable,
            str(WORKER_SCRIPT),
            "--project-root",
            str(project_root),
            "--case-file",
            str(case_file),
            "--output-root",
            str(output_root),
            "--fixture-root",
            str(FIXTURE_ROOT),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads((output_root / "worker_payload.json").read_text(encoding="utf-8"))


def run_batch(batch_id: str = "first_round") -> Dict[str, Any]:
    manifest = load_manifest(batch_id)
    cases = _iter_dataloader_cases(manifest)
    batch_results: List[Dict[str, Any]] = []

    for case in cases:
        case_id = case.get("case_id") or case.get("id")
        result_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / case_id)
        old_output_root = ensure_directory(VERIFICATION_ROOT / "baseline_old" / batch_id / case_id)
        new_output_root = ensure_directory(VERIFICATION_ROOT / "candidate_new" / batch_id / case_id)

        old_payload = _run_worker(OLD_ROOT, case, old_output_root, result_dir)
        new_payload = _run_worker(NEW_ROOT, case, new_output_root, result_dir)

        old_df = pd.read_parquet(old_output_root / "dataloader_output.parquet")
        new_df = pd.read_parquet(new_output_root / "dataloader_output.parquet")
        compare = compare_dataframes(
            old_df,
            new_df,
            sort_by=["Time"],
            ignore_columns={"Backtest_id"},
        )

        status = "PASS" if compare["pass"] else "FAIL"
        case_result = {
            "case_id": case_id,
            "module": "dataloader",
            "status": status,
            "old_snapshot": old_payload,
            "new_snapshot": new_payload,
            "compare": compare,
        }
        write_json(result_dir / "comparison.json", case_result)
        batch_results.append(
            {
                "case_id": case_id,
                "status": status,
                "difference_count": len(compare["differences"]),
            }
        )

    summary = {
        "batch_id": batch_id,
        "module": "dataloader",
        "status": "PASS" if all(case["status"] == "PASS" for case in batch_results) else "FAIL",
        "cases": batch_results,
    }
    summary_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / "dataloader")
    write_json(summary_dir / "summary.json", summary)
    (summary_dir / "summary.md").write_text(
        "\n".join(
            [
                f"# dataloader {batch_id}",
                "",
                f"- status: {summary['status']}",
                "",
                "## Cases",
                *[
                    f"- `{case['case_id']}` | {case['status']} | diff={case['difference_count']}"
                    for case in batch_results
                ],
            ]
        ),
        encoding="utf-8",
    )
    _update_master_batch_summary(batch_id, batch_results)
    return summary


def _update_master_batch_summary(batch_id: str, dataloader_results: List[Dict[str, Any]]) -> None:
    summary_json = VERIFICATION_ROOT / "results" / batch_id / "summary.json"
    summary_md = VERIFICATION_ROOT / "results" / batch_id / "summary.md"
    if not summary_json.exists():
        return

    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    result_map = {item["case_id"]: item for item in dataloader_results}

    for case in payload.get("cases", []):
        case_id = case.get("case_id")
        if case_id in result_map:
            case["status"] = result_map[case_id]["status"]
            case["difference_count"] = result_map[case_id]["difference_count"]

    payload["dataloader_status"] = (
        "PASS" if all(item["status"] == "PASS" for item in dataloader_results) else "FAIL"
    )
    write_json(summary_json, payload)

    lines = [
        f"# {payload['batch_id']}",
        "",
        f"- status: {payload.get('status', 'INITIALIZED')}",
        f"- case_count: {payload.get('case_count', len(payload.get('cases', [])))}",
        f"- dataloader_status: {payload['dataloader_status']}",
        "",
        "## Cases",
    ]
    for case in payload.get("cases", []):
        extra = ""
        if "difference_count" in case:
            extra = f" | diff={case['difference_count']}"
        lines.append(
            f"- `{case['case_id']}` | {case['module']} | {case['scenario']} | {case['status']}{extra}"
        )
    summary_md.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run dataloader truth-validation cases.")
    parser.add_argument("--batch-id", default="first_round")
    args = parser.parse_args()
    payload = run_batch(args.batch_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
