"""Run a small but enforceable consistency gate for the Rust-backed runtime strict zone."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
STRICT_ZONE = [
    Path("app/api/payloads.py"),
    Path("autorunner/BacktestRunner_autorunner.py"),
    Path("backtester/EngineRequest_backtester.py"),
    Path("autorunner/DataLoader_autorunner.py"),
    Path("backtester/MultiAssetPortfolioBundleExporter_backtester.py"),
    Path("backtester/RustCoreBridge_backtester.py"),
    Path("backtester/RuntimeContracts_backtester.py"),
    Path("metricstracker/MetricsArtifactWriter_metricstracker.py"),
]
REQUIRED_AUDIT_CONTRACT_FILES = [
    Path("backtester/contracts/audit-output/audit-output-contract-v1.schema.json"),
    Path("backtester/contracts/audit-output/examples/audit-output-contract-v1.example.json"),
    Path("backtester/contracts/audit-output/README.md"),
]
REQUIRED_PLATFORM_CONTRACT_FILES = [
    Path("app/contracts/run-registry-v1.schema.json"),
    Path("app/contracts/run-snapshot-v1.schema.json"),
    Path("app/contracts/artifact-manifest-v1.schema.json"),
    Path("app/contracts/dataloader-health-v1.schema.json"),
    Path("app/contracts/chart-payload-v1.schema.json"),
    Path("app/contracts/page-artifact-matrix-v1.schema.json"),
    Path("app/contracts/examples/run-registry-v1.example.json"),
    Path("app/contracts/examples/run-snapshot-v1.example.json"),
    Path("app/contracts/examples/artifact-manifest-v1.example.json"),
    Path("app/contracts/examples/dataloader-health-v1.example.json"),
    Path("app/contracts/examples/chart-payload-v1.example.json"),
    Path("app/contracts/examples/page-artifact-matrix-v1.example.json"),
    Path("app/contracts/README.md"),
    Path("docs/app-core-contracts.md"),
]
RUFF_RULES = ["E9", "F401", "F403", "F405", "F821", "I001", "W291"]
RETIRED_BACKTESTER_FILES = [
    Path("backtester/FeatureContractMaterializer_backtester.py"),
    Path("backtester/MultiAssetPortfolioEngine_backtester.py"),
]


def _absolute_paths() -> list[Path]:
    return [REPO_ROOT / path for path in STRICT_ZONE]


def _check_no_prints(paths: list[Path]) -> list[str]:
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print":
                violations.append(f"{path}: print() is not allowed in semantic-native strict zone")
    return violations


def _run_py_compile(paths: list[Path]) -> None:
    command = [sys.executable, "-m", "py_compile", *[str(path) for path in paths]]
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def _run_ruff(paths: list[Path]) -> None:
    command = [
        sys.executable,
        "-m",
        "ruff",
        "check",
        *[str(path) for path in paths],
        "--select",
        ",".join(RUFF_RULES),
    ]
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def _check_audit_contract_files() -> list[str]:
    violations: list[str] = []
    for relative_path in REQUIRED_AUDIT_CONTRACT_FILES:
        absolute_path = REPO_ROOT / relative_path
        if not absolute_path.exists():
            violations.append(f"missing required audit contract file: {relative_path}")
    return violations


def _check_app_contract_files() -> list[str]:
    violations: list[str] = []
    for relative_path in REQUIRED_PLATFORM_CONTRACT_FILES:
        absolute_path = REPO_ROOT / relative_path
        if not absolute_path.exists():
            violations.append(f"missing required app contract file: {relative_path}")
    return violations


def _check_retired_backtester_files() -> list[str]:
    return [
        f"retired Python backtester must not exist: {path}"
        for path in RETIRED_BACKTESTER_FILES
        if (REPO_ROOT / path).exists()
    ]


def _check_audit_contract_structure() -> list[str]:
    import json

    violations: list[str] = []
    schema_path = REPO_ROOT / REQUIRED_AUDIT_CONTRACT_FILES[0]
    example_path = REPO_ROOT / REQUIRED_AUDIT_CONTRACT_FILES[1]
    if not schema_path.exists() or not example_path.exists():
        return violations

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    example = json.loads(example_path.read_text(encoding="utf-8"))

    required_schema_props = {
        "schema_version",
        "contract_id",
        "placement_policy",
        "summary_index_fields",
        "detailed_feature_audit_fields",
        "ui_policy",
    }
    required_example_top = required_schema_props
    if not required_schema_props.issubset(set(schema.get("properties", {}).keys())):
        violations.append("audit-output schema is missing required top-level properties")
    if not required_example_top.issubset(set(example.keys())):
        violations.append("audit-output example is missing required top-level keys")

    placement = example.get("placement_policy", {})
    ui_policy = example.get("ui_policy", {})
    if placement.get("machine_readable_detailed_audit") is not True:
        violations.append("audit-output example must keep detailed audit machine-readable")
    if not str(placement.get("audit_parquet_compression", "")).strip():
        violations.append("audit-output example must declare audit parquet compression")
    if not isinstance(placement.get("audit_json_inline_row_limit"), int):
        violations.append("audit-output example must declare audit json inline row limit")
    if not str(placement.get("audit_json_chunk_suffix", "")).strip():
        violations.append("audit-output example must declare audit json chunk suffix")
    if ui_policy.get("hide_detailed_audit_by_default") is not True:
        violations.append("audit-output example must hide detailed audit by default")
    return violations


def _check_app_contract_structure() -> list[str]:
    import json

    violations: list[str] = []
    registry_example_path = REPO_ROOT / "app/contracts/examples/run-registry-v1.example.json"
    page_matrix_example_path = REPO_ROOT / "app/contracts/examples/page-artifact-matrix-v1.example.json"
    chart_payload_example_path = REPO_ROOT / "app/contracts/examples/chart-payload-v1.example.json"
    readme_path = REPO_ROOT / "app/contracts/README.md"
    docs_path = REPO_ROOT / "docs/app-core-contracts.md"

    if not all(
        path.exists()
        for path in (
            registry_example_path,
            page_matrix_example_path,
            chart_payload_example_path,
            readme_path,
            docs_path,
        )
    ):
        return violations

    registry = json.loads(registry_example_path.read_text(encoding="utf-8"))
    page_matrix = json.loads(page_matrix_example_path.read_text(encoding="utf-8"))
    chart_payload = json.loads(chart_payload_example_path.read_text(encoding="utf-8"))
    readme_text = readme_path.read_text(encoding="utf-8")
    docs_text = docs_path.read_text(encoding="utf-8")

    expected_status = {"queued", "running", "completed", "failed", "partial"}
    if registry.get("status") not in expected_status:
        violations.append("app run-registry example must use a valid lifecycle status")

    page_ids = {page.get("page_id") for page in page_matrix.get("pages", [])}
    required_pages = {
        "run_center",
        "results_library",
        "backtest_explorer",
        "wfa_studio",
        "statanalyser_studio",
        "metrics_explorer",
    }
    if not required_pages.issubset(page_ids):
        violations.append("app page artifact matrix example is missing required pages")

    metrics_page = next(
        (page for page in page_matrix.get("pages", []) if page.get("page_id") == "metrics_explorer"),
        None,
    )
    if metrics_page and metrics_page.get("fallback_policy", {}).get("allow_ui_rebuild") is not False:
        violations.append("metrics_explorer must not rebuild missing evidence in the UI")

    backtest_page = next(
        (page for page in page_matrix.get("pages", []) if page.get("page_id") == "backtest_explorer"),
        None,
    )
    if backtest_page and "backtester_parquet" not in backtest_page.get("required_artifacts", []):
        violations.append("backtest_explorer must require backtester_parquet")

    if not chart_payload.get("artifact_source_refs"):
        violations.append("app chart payload example must keep artifact source refs")

    if "immediate cutover" not in readme_text.lower():
        violations.append("app contracts README must document immediate cutover")
    if "statanalyser" not in docs_text.lower():
        violations.append("app core contracts doc must document statanalyser foundation policy")
    return violations


def main() -> int:
    paths = _absolute_paths()
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        for item in missing:
            print(f"MISSING: {item}")
        return 1

    _run_py_compile(paths)
    _run_ruff(paths)

    print_violations = _check_no_prints(paths)
    print_violations.extend(_check_audit_contract_files())
    print_violations.extend(_check_app_contract_files())
    print_violations.extend(_check_retired_backtester_files())
    print_violations.extend(_check_audit_contract_structure())
    print_violations.extend(_check_app_contract_structure())
    if print_violations:
        for item in print_violations:
            print(item)
        return 1

    print("consistency gate: PASS")
    for path in paths:
        print(f" - {path.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
