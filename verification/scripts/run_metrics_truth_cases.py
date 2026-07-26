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

from verification.scripts.compare_utils import compare_dataframes, ensure_directory, normalize_records, write_json
from verification.scripts.paths import FIXTURE_ROOT, NEW_ROOT, OLD_ROOT, VERIFICATION_ROOT, WORKER_SCRIPT
from verification.scripts.run_backtester_truth_cases import _validate_candidate_truth as _validate_backtester_truth
from verification.scripts.run_truth_validation_batch import load_manifest


def _dedupe_metadata_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for row in rows:
        key = json.dumps(row, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _iter_metrics_cases(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [case for case in manifest.get("cases", []) if case.get("module") == "metrics" and case.get("enabled", True)]


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
    cases = _iter_metrics_cases(manifest)
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

            old_metrics_path = Path(old_payload["metrics_parquet_path"])
            new_metrics_path = Path(new_payload["metrics_parquet_path"])
            old_df = pd.read_parquet(old_metrics_path)
            new_df = pd.read_parquet(new_metrics_path)
            compare = compare_dataframes(
                old_df,
                new_df,
                sort_by=["Time", "Backtest_id"],
                ignore_columns={"Backtest_id"},
                float_tolerance=1e-7,
            )

            old_metadata = json.loads(Path(old_payload["metadata_json_path"]).read_text(encoding="utf-8"))
            new_metadata = json.loads(Path(new_payload["metadata_json_path"]).read_text(encoding="utf-8"))
            old_meta_norm = normalize_records(old_metadata if isinstance(old_metadata, list) else [old_metadata])
            new_meta_norm = normalize_records(new_metadata if isinstance(new_metadata, list) else [new_metadata])
            old_meta_norm = _dedupe_metadata_rows(old_meta_norm)
            new_meta_norm = _dedupe_metadata_rows(new_meta_norm)
            metadata_equal = old_meta_norm == new_meta_norm

            scalar_differences = []
            if not metadata_equal:
                scalar_differences.append({"field": "metadata_payload", "left": old_meta_norm, "right": new_meta_norm})

            status = "PASS" if compare["pass"] and not scalar_differences else "FAIL"
            case_result = {
                "case_id": case_id,
                "module": "metrics",
                "status": status,
                "old_snapshot": old_payload,
                "new_snapshot": new_payload,
                "compare": compare,
                "scalar_differences": scalar_differences,
            }
            difference_count = len(compare["differences"]) + len(scalar_differences)
        elif compare_mode == "candidate_only_truth":
            new_payload = _run_worker(NEW_ROOT, case, new_output_root, result_dir)
            metrics_df = pd.read_parquet(Path(new_payload["metrics_parquet_path"]))
            backtester_df = pd.read_parquet(new_output_root / "backtester_output.parquet")
            truth_compare = _validate_candidate_metrics_truth(
                case=case,
                payload=new_payload,
                metrics_df=metrics_df,
                backtester_df=backtester_df,
            )
            status = "PASS" if truth_compare["pass"] else "FAIL"
            case_result = {
                "case_id": case_id,
                "module": "metrics",
                "status": status,
                "new_snapshot": new_payload,
                "truth_compare": truth_compare,
            }
            difference_count = len(truth_compare["differences"])
        else:
            raise ValueError(f"Unsupported metrics compare_mode: {compare_mode}")

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
        "module": "metrics",
        "status": "PASS" if all(case["status"] == "PASS" for case in batch_results) else "FAIL",
        "cases": batch_results,
    }
    summary_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / "metrics")
    write_json(summary_dir / "summary.json", summary)
    (summary_dir / "summary.md").write_text(
        "\n".join(
            [
                f"# metrics {batch_id}",
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


def _validate_candidate_metrics_truth(
    *,
    case: Dict[str, Any],
    payload: Dict[str, Any],
    metrics_df: pd.DataFrame,
    backtester_df: pd.DataFrame,
) -> Dict[str, Any]:
    expected = case.get("expected_truth", {})
    differences: List[Dict[str, Any]] = []

    if metrics_df.empty:
        differences.append({"field": "metrics_rows", "expected": "non_empty", "actual": 0})

    expected_rows = expected.get("metrics_rows")
    if expected_rows is not None and len(metrics_df) != expected_rows:
        differences.append({"field": "metrics_rows", "expected": expected_rows, "actual": int(len(metrics_df))})

    required_columns = expected.get("required_columns", [])
    if isinstance(required_columns, list) and required_columns:
        missing_columns = [str(column) for column in required_columns if str(column) not in metrics_df.columns]
        if missing_columns:
            differences.append(
                {"field": "required_columns", "expected": required_columns, "actual_missing": missing_columns}
            )

    expected_metadata_rows = expected.get("metadata_rows")
    if expected_metadata_rows is not None and payload.get("metadata_rows") != expected_metadata_rows:
        differences.append(
            {
                "field": "metadata_rows",
                "expected": expected_metadata_rows,
                "actual": payload.get("metadata_rows"),
            }
        )

    backtester_expected = expected.get("backtester")
    if isinstance(backtester_expected, dict):
        backtester_compare = _validate_backtester_truth(
            {"expected_truth": backtester_expected},
            payload.get("backtester", {}),
            backtester_df,
        )
        differences.extend(
            {"field": f"backtester.{item.get('field')}", **item}
            for item in backtester_compare["differences"]
        )

    return {"pass": not differences, "differences": differences}


def _update_master_batch_summary(batch_id: str, metric_results: List[Dict[str, Any]]) -> None:
    summary_json = VERIFICATION_ROOT / "results" / batch_id / "summary.json"
    summary_md = VERIFICATION_ROOT / "results" / batch_id / "summary.md"
    if not summary_json.exists():
        return

    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    result_map = {item["case_id"]: item for item in metric_results}
    for case in payload.get("cases", []):
        case_id = case.get("case_id")
        if case_id in result_map:
            case["status"] = result_map[case_id]["status"]
            case["difference_count"] = result_map[case_id]["difference_count"]
    payload["metrics_status"] = "PASS" if all(item["status"] == "PASS" for item in metric_results) else "FAIL"
    write_json(summary_json, payload)

    lines = [
        f"# {payload['batch_id']}",
        "",
        f"- status: {payload.get('status', 'INITIALIZED')}",
        f"- case_count: {payload.get('case_count', len(payload.get('cases', [])))}",
    ]
    for field in ["dataloader_status", "statanalyser_status", "backtester_status", "metrics_status"]:
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

    parser = argparse.ArgumentParser(description="Run metrics truth-validation cases.")
    parser.add_argument("--batch-id", default="first_round")
    args = parser.parse_args()
    payload = run_batch(args.batch_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
