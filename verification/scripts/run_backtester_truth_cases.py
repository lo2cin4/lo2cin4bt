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


def _iter_backtester_cases(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        case
        for case in manifest.get("cases", [])
        if case.get("module") == "backtester"
        and case.get("enabled", True)
    ]


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
    cases = _iter_backtester_cases(manifest)
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

            old_df = pd.read_parquet(old_output_root / "backtester_output.parquet")
            new_df = pd.read_parquet(new_output_root / "backtester_output.parquet")
            compare = compare_dataframes(
                old_df,
                new_df,
                sort_by=["Time", "Trade_group_id", "Trade_action"],
                ignore_columns={"Backtest_id", "Trade_group_id"},
            )

            scalar_differences = []
            for field in ["success", "resolved_engine_mode", "predictor_column", "frequency"]:
                if old_payload.get(field) != new_payload.get(field):
                    scalar_differences.append(
                        {"field": field, "left": old_payload.get(field), "right": new_payload.get(field)}
                    )

            status = "PASS" if compare["pass"] and not scalar_differences else "FAIL"
            case_result = {
                "case_id": case_id,
                "module": "backtester",
                "status": status,
                "old_snapshot": old_payload,
                "new_snapshot": new_payload,
                "compare": compare,
                "scalar_differences": scalar_differences,
            }
            difference_count = len(compare["differences"]) + len(scalar_differences)
        elif compare_mode == "candidate_only_truth":
            new_payload = _run_worker(NEW_ROOT, case, new_output_root, result_dir)
            new_df = pd.read_parquet(new_output_root / "backtester_output.parquet")
            truth_compare = _validate_candidate_truth(case, new_payload, new_df)
            status = "PASS" if truth_compare["pass"] else "FAIL"
            case_result = {
                "case_id": case_id,
                "module": "backtester",
                "status": status,
                "new_snapshot": new_payload,
                "truth_compare": truth_compare,
            }
            difference_count = len(truth_compare["differences"])
        else:
            raise ValueError(f"Unsupported backtester compare_mode: {compare_mode}")

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
        "module": "backtester",
        "status": "PASS" if all(case["status"] == "PASS" for case in batch_results) else "FAIL",
        "cases": batch_results,
    }
    summary_dir = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / "backtester")
    write_json(summary_dir / "summary.json", summary)
    (summary_dir / "summary.md").write_text(
        "\n".join(
            [
                f"# backtester {batch_id}",
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


def _validate_candidate_truth(case: Dict[str, Any], payload: Dict[str, Any], df: pd.DataFrame) -> Dict[str, Any]:
    expected = case.get("expected_truth", {})
    differences: List[Dict[str, Any]] = []

    resolved_engine_mode = payload.get("resolved_engine_mode")
    if expected.get("resolved_engine_mode") and resolved_engine_mode != expected["resolved_engine_mode"]:
        differences.append(
            {
                "field": "resolved_engine_mode",
                "expected": expected["resolved_engine_mode"],
                "actual": resolved_engine_mode,
            }
        )

    if expected.get("expected_rows") is not None and len(df) != expected["expected_rows"]:
        differences.append(
            {"field": "rows", "expected": expected["expected_rows"], "actual": int(len(df))}
        )

    required_columns = expected.get("required_columns", [])
    if isinstance(required_columns, list) and required_columns:
        missing_columns = [str(column) for column in required_columns if str(column) not in df.columns]
        if missing_columns:
            differences.append(
                {"field": "required_columns", "expected": required_columns, "actual_missing": missing_columns}
            )

    nonzero_trade_actions = int((df["Trade_action"] != 0).sum())
    if expected.get("expected_nonzero_trade_actions") is not None and nonzero_trade_actions != expected["expected_nonzero_trade_actions"]:
        differences.append(
            {
                "field": "nonzero_trade_actions",
                "expected": expected["expected_nonzero_trade_actions"],
                "actual": nonzero_trade_actions,
            }
        )

    for row_name in ["entry_row", "holding_row", "exit_row"]:
        row_expectation = expected.get(row_name)
        if not row_expectation:
            continue
        row_diff = _compare_expected_row(df, row_name, row_expectation)
        differences.extend(row_diff)

    return {"pass": not differences, "differences": differences}


def _compare_expected_row(df: pd.DataFrame, row_name: str, expected: Dict[str, Any]) -> List[Dict[str, Any]]:
    differences: List[Dict[str, Any]] = []
    row_df = df[df["Time"].astype(str) == str(expected["Time"])]
    if row_df.empty:
        return [{"field": row_name, "expected": expected, "actual": "missing_time_row"}]

    row = row_df.iloc[0]
    for key, expected_value in expected.items():
        if key == "Time":
            continue
        actual_value = row[key]
        if pd.isna(actual_value) and expected_value is None:
            continue
        if isinstance(expected_value, float):
            if abs(float(actual_value) - expected_value) > 1e-9:
                differences.append(
                    {"field": f"{row_name}.{key}", "expected": expected_value, "actual": float(actual_value)}
                )
        else:
            if actual_value != expected_value:
                differences.append(
                    {"field": f"{row_name}.{key}", "expected": expected_value, "actual": actual_value}
                )
    return differences


def _update_master_batch_summary(batch_id: str, backtester_results: List[Dict[str, Any]]) -> None:
    summary_json = VERIFICATION_ROOT / "results" / batch_id / "summary.json"
    summary_md = VERIFICATION_ROOT / "results" / batch_id / "summary.md"
    if not summary_json.exists():
        return

    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    result_map = {item["case_id"]: item for item in backtester_results}
    for case in payload.get("cases", []):
        case_id = case.get("case_id")
        if case_id in result_map:
            case["status"] = result_map[case_id]["status"]
            case["difference_count"] = result_map[case_id]["difference_count"]
    payload["backtester_status"] = (
        "PASS" if all(item["status"] == "PASS" for item in backtester_results) else "FAIL"
    )
    write_json(summary_json, payload)

    lines = [
        f"# {payload['batch_id']}",
        "",
        f"- status: {payload.get('status', 'INITIALIZED')}",
        f"- case_count: {payload.get('case_count', len(payload.get('cases', [])))}",
    ]
    for field in ["dataloader_status", "statanalyser_status", "backtester_status"]:
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

    parser = argparse.ArgumentParser(description="Run backtester truth-validation cases.")
    parser.add_argument("--batch-id", default="first_round")
    args = parser.parse_args()
    payload = run_batch(args.batch_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
