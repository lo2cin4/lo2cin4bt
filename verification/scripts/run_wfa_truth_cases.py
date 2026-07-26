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


def _iter_wfa_cases(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [case for case in manifest.get("cases", []) if case.get("module") == "wfa" and case.get("enabled", True)]


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


def _compare_windows(left: List[Dict[str, Any]], right: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if left == right:
        return []
    return [{"field": "window_boundaries", "left": left, "right": right}]


def _collect_objective_snapshots(output_root: Path) -> Dict[str, pd.DataFrame]:
    snapshots: Dict[str, pd.DataFrame] = {}
    export_dir = output_root / "records" / "validation_workflow"
    for parquet_path in sorted(export_dir.glob("*_snapshot.parquet")):
        objective = parquet_path.stem.replace("_snapshot", "")
        snapshots[objective] = pd.read_parquet(parquet_path)
    return snapshots


def run_batch(batch_id: str = "third_round") -> Dict[str, Any]:
    manifest = load_manifest(batch_id)
    cases = _iter_wfa_cases(manifest)
    batch_results: List[Dict[str, Any]] = []

    for case in cases:
        case_id = case.get("case_id") or case.get("id")
        compare_mode = case.get("compare_mode", "old_vs_new")
        result_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / case_id)
        new_output_root = ensure_directory(VERIFICATION_ROOT / "candidate_new" / batch_id / case_id)

        if compare_mode == "old_vs_new":
            old_output_root = ensure_directory(VERIFICATION_ROOT / "baseline_old" / batch_id / case_id)
            old_payload = _run_worker(OLD_ROOT, case, old_output_root, result_dir)
            new_payload = _run_worker(NEW_ROOT, case, new_output_root, result_dir)

            scalar_differences: List[Dict[str, Any]] = []
            for field in ["resolved_mode", "window_count", "objective_names", "objective_result_counts", "csv_export_count"]:
                if old_payload.get(field) != new_payload.get(field):
                    scalar_differences.append(
                        {"field": field, "left": old_payload.get(field), "right": new_payload.get(field)}
                    )

            scalar_differences.extend(
                _compare_windows(
                    old_payload.get("window_boundaries", []),
                    new_payload.get("window_boundaries", []),
                )
            )

            objective_differences: List[Dict[str, Any]] = []
            objective_compares: Dict[str, Any] = {}
            old_snapshots = _collect_objective_snapshots(old_output_root)
            new_snapshots = _collect_objective_snapshots(new_output_root)
            all_objectives = sorted(set(old_snapshots.keys()) | set(new_snapshots.keys()))
            for objective in all_objectives:
                old_df = old_snapshots.get(objective)
                new_df = new_snapshots.get(objective)
                if old_df is None or new_df is None:
                    objective_differences.append(
                        {"field": f"objective_snapshot:{objective}", "left": old_df is not None, "right": new_df is not None}
                    )
                    continue
                compare = compare_dataframes(
                    old_df,
                    new_df,
                    sort_by=["window_id", "condition_pair_id", "param_combination_id"],
                    ignore_columns={"Trade_group_id", "Backtest_id"},
                )
                objective_compares[objective] = compare
                if not compare["pass"]:
                    objective_differences.extend(
                        [{"objective": objective, **difference} for difference in compare["differences"]]
                    )

            status = "PASS" if not scalar_differences and not objective_differences else "FAIL"
            case_result = {
                "case_id": case_id,
                "module": "wfa",
                "status": status,
                "old_snapshot": old_payload,
                "new_snapshot": new_payload,
                "scalar_differences": scalar_differences,
                "objective_compares": objective_compares,
                "objective_differences": objective_differences,
            }
            difference_count = len(scalar_differences) + len(objective_differences)
        elif compare_mode == "candidate_only_truth":
            new_payload = _run_worker(NEW_ROOT, case, new_output_root, result_dir)
            truth_compare = _validate_candidate_wfa_truth(case, new_payload)
            status = "PASS" if truth_compare["pass"] else "FAIL"
            case_result = {
                "case_id": case_id,
                "module": "wfa",
                "status": status,
                "new_snapshot": new_payload,
                "truth_compare": truth_compare,
            }
            difference_count = len(truth_compare["differences"])
        else:
            raise ValueError(f"Unsupported WFA compare_mode: {compare_mode}")

        write_json(result_dir / "comparison.json", case_result)
        batch_results.append(
            {
                "case_id": case_id,
                "status": status,
                "difference_count": difference_count,
            }
        )

    summary = {
        "batch_id": batch_id,
        "module": "wfa",
        "status": "PASS" if all(case["status"] == "PASS" for case in batch_results) else "FAIL",
        "cases": batch_results,
    }
    summary_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / "wfa")
    write_json(summary_dir / "summary.json", summary)
    (summary_dir / "summary.md").write_text(
        "\n".join(
            [
                f"# wfa {batch_id}",
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


def _validate_candidate_wfa_truth(case: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    expected = case.get("expected_truth", {})
    differences: List[Dict[str, Any]] = []
    for field in ["resolved_mode", "window_count", "objective_names", "objective_result_counts"]:
        if field in expected and payload.get(field) != expected[field]:
            differences.append({"field": field, "expected": expected[field], "actual": payload.get(field)})
    min_csv_export_count = expected.get("min_csv_export_count")
    if min_csv_export_count is not None and payload.get("csv_export_count", 0) < min_csv_export_count:
        differences.append(
            {
                "field": "csv_export_count",
                "expected": f">={min_csv_export_count}",
                "actual": payload.get("csv_export_count", 0),
            }
        )
    return {"pass": not differences, "differences": differences}


def _update_master_batch_summary(batch_id: str, wfa_results: List[Dict[str, Any]]) -> None:
    summary_json = VERIFICATION_ROOT / "results" / batch_id / "summary.json"
    summary_md = VERIFICATION_ROOT / "results" / batch_id / "summary.md"
    if not summary_json.exists():
        return
    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    result_map = {item["case_id"]: item for item in wfa_results}
    for case in payload.get("cases", []):
        case_id = case.get("case_id")
        if case_id in result_map:
            case["status"] = result_map[case_id]["status"]
            case["difference_count"] = result_map[case_id]["difference_count"]
    payload["wfa_status"] = "PASS" if all(item["status"] == "PASS" for item in wfa_results) else "FAIL"
    write_json(summary_json, payload)

    lines = [
        f"# {payload['batch_id']}",
        "",
        f"- status: {payload.get('status', 'INITIALIZED')}",
        f"- case_count: {payload.get('case_count', len(payload.get('cases', [])))}",
    ]
    for field in ["dataloader_status", "statanalyser_status", "backtester_status", "metrics_status", "wfa_status"]:
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
    summary_md.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run WFA truth-validation cases.")
    parser.add_argument("--batch-id", default="third_round")
    args = parser.parse_args()
    payload = run_batch(args.batch_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
