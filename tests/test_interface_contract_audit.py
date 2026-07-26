import ast
import json
import re
from pathlib import Path

from backtester.EngineRequest_backtester import build_engine_request
from backtester.StrategyRunConfig_backtester import (
    normalize_strategy_run_config,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_INTERFACE_ROOTS = (
    "app",
    "backtester",
    "dataloader",
    "metricstracker",
    "validation_workflow",
)


def test_interface_classes_do_not_call_undefined_self_methods() -> None:
    missing: list[str] = []
    for relative_root in PYTHON_INTERFACE_ROOTS:
        for path in (REPO_ROOT / relative_root).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8-sig"))
            for class_node in (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)):
                methods = {
                    node.name
                    for node in class_node.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                }
                assigned: set[str] = set()
                for node in ast.walk(class_node):
                    if isinstance(node, ast.Assign):
                        targets = node.targets
                    elif isinstance(node, ast.AnnAssign):
                        targets = [node.target]
                    else:
                        continue
                    for target in targets:
                        if (
                            isinstance(target, ast.Attribute)
                            and isinstance(target.value, ast.Name)
                            and target.value.id == "self"
                        ):
                            assigned.add(target.attr)
                calls = {
                    node.func.attr
                    for node in ast.walk(class_node)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "self"
                }
                for name in sorted(calls - methods - assigned):
                    missing.append(f"{path.relative_to(REPO_ROOT)}:{class_node.name}.{name}")

    assert missing == []


def test_every_workspace_strategy_config_compiles_to_engine_request() -> None:
    for path in sorted((REPO_ROOT / "workspace" / "runs").glob("strategy-run-*.json")):
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        normalized = normalize_strategy_run_config(raw, source_path=path)
        request = build_engine_request(normalized)
        assert request["schema_version"] == "engine_request.v1", path.name
        assert request["outputs"]["result_contract"] == (
            "canonical_result_bundle.v1"
        ), path.name


def test_frontend_api_paths_have_backend_routes() -> None:
    frontend = (REPO_ROOT / "plotter" / "web" / "src" / "api.ts").read_text(
        encoding="utf-8"
    )
    backend = (REPO_ROOT / "app" / "api" / "app.py").read_text(encoding="utf-8")
    backend_routes = {
        re.sub(r"\{[^}]+\}", "{}", route)
        for route in re.findall(
            r'@app\.(?:get|post|put|delete)\("([^"]+)"', backend
        )
    }
    frontend_paths = set(
        re.findall(r"(?:getJson|postJson|requestJson)<[^>]+>\(`([^`]+)`", frontend)
    )
    frontend_paths.update(
        re.findall(r"(?:getJson|postJson|requestJson)<[^>]+>\('([^']+)'", frontend)
    )
    frontend_paths.add("/backtests/${runId}/${backtestId}/export.csv")

    missing = []
    for path in sorted(frontend_paths):
        normalized = "/api/app" + re.sub(r"\$\{[^}]+\}", "{}", path)
        if normalized not in backend_routes:
            missing.append(normalized)
    assert missing == []


def test_payload_contract_docs_name_the_generated_parameter_payload() -> None:
    canonical_map = (
        REPO_ROOT
        / "skills"
        / "lo2cin4bt"
        / "references"
        / "payload-contract-map.md"
    )
    text = canonical_map.read_text(encoding="utf-8")
    assert "parameter_heatmap_payload.json" in text
    assert "parameter_matrix_payload.json" not in text

    for skill_name in ("lo2cin4bt-backtesting", "lo2cin4bt-performance-analysis"):
        skill = REPO_ROOT / "skills" / skill_name / "SKILL.md"
        assert (
            "skills/lo2cin4bt/references/payload-contract-map.md"
            in skill.read_text(encoding="utf-8")
        )
