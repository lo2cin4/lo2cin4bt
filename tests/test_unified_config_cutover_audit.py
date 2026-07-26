import json
from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]

PUBLIC_STRATEGY_CONFIG_PATHS = [
    REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples",
    REPO_ROOT / "workspace" / "runs",
]

TEMPLATE_CONFIG_PATHS = [
    REPO_ROOT / "autorunner" / "templates" / "config_template.json",
]

AUTHORING_TEMPLATE_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "strategy_authoring"
    / "templates"
    / "strategy-config-dsl-v1.template.yaml"
)

PUBLIC_WFA_CONFIG_PATHS = [
    REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples",
    REPO_ROOT / "workspace" / "wfa",
]

FORBIDDEN_PUBLIC_DIRECT_MODE_IDS = {
    "single_asset_signal",
}

CANONICAL_PROFILE_IDS = {
    "selection_timing_portfolio",
    "allocation_portfolio",
    "rotation_portfolio",
    "calendar_event_portfolio",
    "pair_spread_portfolio",
    "multi_leg_event_portfolio",
}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def test_workspace_strategy_run_copies_match_canonical_examples() -> None:
    canonical_root = REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples"
    workspace_root = REPO_ROOT / "workspace" / "runs"

    for workspace_path in workspace_root.glob("strategy-run-*.json"):
        canonical_path = canonical_root / workspace_path.name
        if canonical_path.exists():
            assert _load_json(workspace_path) == _load_json(canonical_path), (
                f"Workspace config diverged from canonical example: {workspace_path.name}"
            )


def _iter_public_strategy_runs():
    for root in PUBLIC_STRATEGY_CONFIG_PATHS:
        for path in sorted(root.glob("*.json")):
            payload = _load_json(path)
            if isinstance(payload, dict) and payload.get("schema_version") == "strategy_run":
                yield path, payload


def _iter_public_wfa_runs():
    for root in PUBLIC_WFA_CONFIG_PATHS:
        for path in sorted(root.glob("wfa-run-*.json")):
            payload = _load_json(path)
            if isinstance(payload, dict) and payload.get("schema_version") == "wfa_run":
                yield path, payload


def test_public_strategy_run_surfaces_do_not_use_legacy_direct_single_asset_mode():
    offenders = []
    for path, payload in _iter_public_strategy_runs():
        platform = payload.get("platform", {}) if isinstance(payload.get("platform"), dict) else {}
        mode_id = str(platform.get("strategy_mode_id") or "").strip()
        if mode_id in FORBIDDEN_PUBLIC_DIRECT_MODE_IDS:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_active_workspace_configs_have_creation_date_and_no_redundant_prefix():
    offenders = []
    roots = [REPO_ROOT / "workspace" / "runs", REPO_ROOT / "workspace" / "wfa"]
    for root in roots:
        for path in sorted(root.glob("*.json")):
            payload = _load_json(path)
            platform = payload.get("platform", {}) if isinstance(payload.get("platform"), dict) else {}
            display_label = str(platform.get("display_label") or "").strip()
            created_at = str(platform.get("created_at") or "").strip()
            label_parts = [
                part.strip() for part in display_label.split("|") if part.strip()
            ]
            if re.match(r"^(?:Workflow\s*\||Backtest\s*\|)", display_label, re.I):
                offenders.append(f"{path.relative_to(REPO_ROOT)}: display_label")
            if len(label_parts) != 3:
                offenders.append(
                    f"{path.relative_to(REPO_ROOT)}: display_label must have three parts"
                )
            if not re.match(r"^20\d{2}-\d{2}-\d{2}(?:T.*)?$", created_at):
                offenders.append(f"{path.relative_to(REPO_ROOT)}: platform.created_at")

    assert offenders == []


def test_public_strategy_run_surfaces_have_explicit_canonical_profile():
    offenders = []
    for path, payload in _iter_public_strategy_runs():
        platform = payload.get("platform", {}) if isinstance(payload.get("platform"), dict) else {}
        profile_id = str(platform.get("strategy_profile_id") or "").strip()
        if profile_id not in CANONICAL_PROFILE_IDS:
            offenders.append(f"{path.relative_to(REPO_ROOT)}: {profile_id or '<missing>'}")

    assert offenders == []


def test_single_asset_signal_preset_surfaces_compile_contract_shape():
    offenders = []
    for path, payload in _iter_public_strategy_runs():
        platform = payload.get("platform", {}) if isinstance(payload.get("platform"), dict) else {}
        preset_id = str(platform.get("strategy_preset_id") or "").strip()
        if preset_id != "single_asset_signal":
            continue
        if str(platform.get("strategy_mode_id") or "").strip() != "multi_asset_portfolio":
            offenders.append(f"{path.relative_to(REPO_ROOT)}: mode")
            continue
        if str(platform.get("strategy_profile_id") or "").strip() != "selection_timing_portfolio":
            offenders.append(f"{path.relative_to(REPO_ROOT)}: profile")

    assert offenders == []


def test_autorunner_template_uses_unified_preset_surface():
    path = TEMPLATE_CONFIG_PATHS[0]
    config = _load_json(path)
    assert config.get("schema_version") == "strategy_run"
    assert "backtester" not in config

    platform = config.get("platform", {}) if isinstance(config.get("platform"), dict) else {}
    assert platform.get("strategy_mode_id") == "multi_asset_portfolio"
    assert platform.get("strategy_profile_id") == "selection_timing_portfolio"
    assert platform.get("strategy_preset_id") == "single_asset_signal"


def test_authoring_dsl_template_uses_unified_profile_preset_surface():
    text = AUTHORING_TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "strategy_profile_id: selection_timing_portfolio" in text
    assert "strategy_preset_id: single_asset_signal" in text
    assert re.search(r"method:\s+signal_state\b", text) is None
    assert "op: signal.change" not in text


def test_public_wfa_surfaces_present_as_workflow_contracts_referencing_strategy_run():
    offenders = []
    for path, payload in _iter_public_wfa_runs():
        strategy_run_path = str(payload.get("strategy_run_path") or "").strip()
        if not strategy_run_path.startswith("workspace/runs/"):
            offenders.append(f"{path.relative_to(REPO_ROOT)}: strategy_run_path")
            continue
        if str(payload.get("strategy_config_path") or "").strip():
            offenders.append(f"{path.relative_to(REPO_ROOT)}: legacy_strategy_config_path_present")
            continue
        platform = payload.get("platform", {}) if isinstance(payload.get("platform"), dict) else {}
        display_label = str(platform.get("display_label") or "").strip()
        if re.match(r"^(?:Workflow\s*\||Backtest\s*\|)", display_label, re.I):
            offenders.append(f"{path.relative_to(REPO_ROOT)}: display_label")
            continue
        if not re.match(r"^20\d{2}-\d{2}-\d{2}(?:T.*)?$", str(platform.get("created_at") or "")):
            offenders.append(f"{path.relative_to(REPO_ROOT)}: platform.created_at")
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        notes = metadata.get("notes", []) if isinstance(metadata.get("notes"), list) else []
        notes_text = " ".join(str(item) for item in notes)
        if "strategy_run" not in notes_text or "workflow" not in notes_text.lower():
            offenders.append(f"{path.relative_to(REPO_ROOT)}: notes")

    assert offenders == []


def test_phase3_runtime_bridge_does_not_reimplement_legacy_strategy_config_fallbacks():
    offenders = []
    target_files = [
        REPO_ROOT / "app" / "runtime" / "runtime.py",
        REPO_ROOT / "app" / "api" / "scheduler.py",
        REPO_ROOT / "app" / "api" / "payloads.py",
    ]
    forbidden_pattern = re.compile(r'get\("strategy_config_path"\)')

    for path in target_files:
        text = path.read_text(encoding="utf-8")
        if forbidden_pattern.search(text):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_active_workflow_readers_reject_legacy_embedded_strategy_config_key():
    offenders = []
    target_files = [
        REPO_ROOT / "app" / "runtime" / "runtime.py",
        REPO_ROOT / "app" / "api" / "scheduler.py",
        REPO_ROOT / "app" / "api" / "payloads.py",
        REPO_ROOT / "validation_workflow" / "WalkForwardEngine_validation_workflow.py",
    ]
    forbidden_pattern = re.compile(r'["\']strategy_config["\']')

    for path in target_files:
        text = path.read_text(encoding="utf-8")
        if forbidden_pattern.search(text):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_active_workflow_readers_do_not_accept_portfolio_config_shell():
    offenders = []
    target_files = [
        REPO_ROOT / "app" / "runtime" / "runtime.py",
        REPO_ROOT / "app" / "api" / "scheduler.py",
        REPO_ROOT / "app" / "api" / "payloads.py",
        REPO_ROOT / "app" / "api" / "labels.py",
        REPO_ROOT / "autorunner" / "ConfigLoader_autorunner.py",
        REPO_ROOT / "autorunner" / "ConfigValidator_autorunner.py",
        REPO_ROOT / "validation_workflow" / "ConfigLoader_validation_workflow.py",
        REPO_ROOT / "validation_workflow" / "WalkForwardEngine_validation_workflow.py",
    ]
    forbidden_pattern = re.compile(r'["\']portfolio_config["\']')

    for path in target_files:
        text = path.read_text(encoding="utf-8")
        if forbidden_pattern.search(text):
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == []


def test_autorunner_config_boundary_has_no_legacy_runtime_fields():
    offenders = []
    target_files = [
        REPO_ROOT / "autorunner" / "ConfigLoader_autorunner.py",
        REPO_ROOT / "autorunner" / "ConfigSelector_autorunner.py",
        REPO_ROOT / "autorunner" / "ConfigValidator_autorunner.py",
    ]
    forbidden_patterns = {
        "condition_pairs": re.compile(r'["\']condition_pairs["\']'),
        "indicator_params": re.compile(r'["\']indicator_params["\']'),
        "trading_params": re.compile(r'["\']trading_params["\']'),
    }

    for path in target_files:
        text = path.read_text(encoding="utf-8")
        for label, pattern in forbidden_patterns.items():
            if pattern.search(text):
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {label}")

    assert offenders == []


def test_public_contracts_drop_legacy_mode_taxonomy_and_old_signal_terms():
    offenders = []
    contract_paths = [
        REPO_ROOT / "backtester" / "contracts" / "strategy" / "strategy-run.schema.json",
        REPO_ROOT / "backtester" / "contracts" / "strategy" / "mode-registry-v1.json",
        REPO_ROOT / "docs" / "contracts" / "strategy-mode-and-workflow-contract.md",
    ]
    forbidden_terms = [
        "single_asset_engine",
        "multi_asset_trigger_selection",
        "dynamic_allocation_rules",
    ]
    for path in contract_paths:
        text = path.read_text(encoding="utf-8")
        for term in forbidden_terms:
            if term in text:
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {term}")
    assert offenders == []


def test_retired_versioned_schema_alias_files_are_absent():
    retired_aliases = [
        REPO_ROOT / "backtester" / "contracts" / "strategy" / "strategy-run-v2.schema.json",
        REPO_ROOT / "backtester" / "contracts" / "strategy" / "wfa-run-v2.schema.json",
    ]

    assert [str(path.relative_to(REPO_ROOT)) for path in retired_aliases if path.exists()] == []


def test_active_runtime_does_not_accept_retired_signal_execution_terms():
    offenders = []
    target_files = [
        REPO_ROOT / "backtester" / "StrategyRunConfig_backtester.py",
        REPO_ROOT / "backtester" / "UnifiedBacktestRunner_backtester.py",
        REPO_ROOT / "backtester" / "ops" / "registry.py",
    ]
    retired_terms = ["signal_state", "signal_target_weight", "signal.change"]

    for path in target_files:
        text = path.read_text(encoding="utf-8")
        for term in retired_terms:
            if term in text:
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {term}")

    assert offenders == []
