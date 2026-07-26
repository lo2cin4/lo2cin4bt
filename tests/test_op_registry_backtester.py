import json
import re
from copy import deepcopy
from pathlib import Path

from jsonschema import Draft202012Validator

from backtester.ops.export import export_registry, stable_json
from backtester.ops.registry import (
    BUILDING_BLOCK_CATEGORY,
    EVENT_DRIVEN_CONDITION,
    EVENT_DRIVEN_FEATURE_DAG,
    MULTI_ASSET_INDICATORS,
    MULTI_ASSET_INLINE_CONDITION_FEATURE,
    build_registry,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "backtester" / "contracts" / "ops" / "op-spec-v1.schema.json"
EXPORT_PATH = REPO_ROOT / "app" / "contracts" / "generated" / "op-registry-v1.json"
STRATEGY_SCHEMA_PATH = REPO_ROOT / "backtester" / "contracts" / "strategy" / "strategy-contract-v2.schema.json"
ENGINE_REQUEST_SCHEMA_PATH = REPO_ROOT / "backtester" / "contracts" / "runtime" / "engine-request-v1.schema.json"
RUST_ENGINE_REQUEST_PATH = REPO_ROOT / "rust" / "lo2cin4bt_core" / "src" / "engine_request.rs"


EXPECTED_CORE_OPS = {
    "and",
    "calendar.event_date",
    "calendar.every_session",
    "calendar.first_session",
    "calendar.last_weekday_of_month",
    "calendar.month_end",
    "calendar.month_in",
    "calendar.month_start",
    "calendar.nth_weekday_of_month",
    "calendar.quarter_end",
    "calendar.quarter_start",
    "calendar.weekday_eq",
    "calendar.year_end",
    "calendar.year_start",
    "cross_down",
    "cross_up",
    "eq",
    "ge",
    "gt",
    "indicator.ema",
    "indicator.atr",
    "indicator.bollinger",
    "indicator.calendar_return",
    "indicator.macd",
    "indicator.momentum",
    "indicator.percentile",
    "indicator.rsi",
    "indicator.sma",
    "indicator.volatility",
    "indicator.zscore",
    "math.abs",
    "math.add",
    "math.clip",
    "math.divide",
    "math.multiply",
    "math.negate",
    "math.subtract",
    "transform.fill_missing",
    "transform.lag",
    "transform.where",
    "rolling.correlation",
    "rolling.max",
    "rolling.median",
    "rolling.min",
    "rolling.sum",
    "cross_section.percentile",
    "cross_section.rank",
    "cross_section.winsorize",
    "cross_section.zscore",
    "le",
    "lt",
    "ne",
    "not",
    "or",
    "target.change",
    "session.same_session_close",
    "template.fixed_allocation_rebalance",
    "template.momentum_rotation",
    "template.monthly_nth_weekday_same_session",
    "template.single_asset_ma_cross",
}


REQUIRED_OP_FIELDS = {
    "canonical_id",
    "aliases",
    "block_kind",
    "category",
    "spec_version",
    "status",
    "stability",
    "usage_sites",
    "params_schema",
    "optimizable_params",
    "input_shape",
    "output_type",
    "output_shape",
    "temporal_metadata",
    "data_alignment",
    "leakage_flags",
    "wfa_safety",
    "implementation",
    "evidence_paths",
    "audit",
    "cache",
    "deprecation",
    "docs",
    "unsupported_message",
    "safety_warnings",
}


EXPECTED_BLOCK_KINDS = {
    "and": "condition_logic",
    "calendar.event_date": "calendar",
    "calendar.every_session": "calendar",
    "calendar.first_session": "calendar",
    "calendar.last_weekday_of_month": "calendar",
    "calendar.month_end": "calendar",
    "calendar.month_in": "calendar",
    "calendar.month_start": "calendar",
    "calendar.nth_weekday_of_month": "calendar",
    "calendar.quarter_end": "calendar",
    "calendar.quarter_start": "calendar",
    "calendar.weekday_eq": "calendar",
    "calendar.year_end": "calendar",
    "calendar.year_start": "calendar",
    "cross_down": "cross_condition",
    "cross_up": "cross_condition",
    "eq": "condition_comparator",
    "ge": "condition_comparator",
    "gt": "condition_comparator",
    "indicator.ema": "indicator",
    "indicator.atr": "indicator",
    "indicator.bollinger": "indicator",
    "indicator.calendar_return": "indicator",
    "indicator.macd": "indicator",
    "indicator.momentum": "indicator",
    "indicator.percentile": "indicator",
    "indicator.rsi": "indicator",
    "indicator.sma": "indicator",
    "indicator.volatility": "indicator",
    "indicator.zscore": "indicator",
    "math.abs": "math",
    "math.add": "math",
    "math.clip": "math",
    "math.divide": "math",
    "math.multiply": "math",
    "math.negate": "math",
    "math.subtract": "math",
    "transform.fill_missing": "transform",
    "transform.lag": "transform",
    "transform.where": "transform",
    "rolling.correlation": "rolling",
    "rolling.max": "rolling",
    "rolling.median": "rolling",
    "rolling.min": "rolling",
    "rolling.sum": "rolling",
    "cross_section.percentile": "cross_section",
    "cross_section.rank": "cross_section",
    "cross_section.winsorize": "cross_section",
    "cross_section.zscore": "cross_section",
    "le": "condition_comparator",
    "lt": "condition_comparator",
    "ne": "condition_comparator",
    "not": "condition_logic",
    "or": "condition_logic",
    "target.change": "rebalance_trigger",
    "session.same_session_close": "execution",
    "template.fixed_allocation_rebalance": "strategy_template",
    "template.momentum_rotation": "strategy_template",
    "template.monthly_nth_weekday_same_session": "strategy_template",
    "template.single_asset_ma_cross": "strategy_template",
}


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_registry_export_validates_against_op_spec_schema() -> None:
    payload = build_registry().export_payload()
    schema = _load_json(SCHEMA_PATH)

    Draft202012Validator(schema).validate(payload)
    assert payload["registry_name"] == "Strategy Building Blocks"
    assert payload["category_name"] == "Strategy Building Blocks"
    assert payload["runtime_behavior_changed"] is True
    assert "not a profitability claim" in payload["support_meaning"]


def test_core_registry_has_required_fields_and_only_building_block_category() -> None:
    payload = build_registry().export_payload()
    ids = {op["canonical_id"] for op in payload["ops"]}

    assert ids == EXPECTED_CORE_OPS
    assert {op["category"] for op in payload["ops"]} == {BUILDING_BLOCK_CATEGORY}
    assert "feature" not in {op["category"].lower() for op in payload["ops"]}
    assert "inline feature" not in {op["category"].lower() for op in payload["ops"]}

    for op in payload["ops"]:
        assert REQUIRED_OP_FIELDS.issubset(op)
        assert op["usage_sites"]
        assert op["params_schema"]["type"] == "object"
        assert op["temporal_metadata"]["observation_time"]
        assert op["implementation"]["path"]
        assert len(op["implementation"]["source_hash"]) == 64
        assert op["implementation"]["source_hashes"]
        assert all(len(item) == 64 for item in op["implementation"]["source_hashes"].values())
        assert op["leakage_flags"]["uses_future"] is False
        assert op["leakage_flags"]["allows_tradable_bfill"] is False
        assert op["wfa_safety"]["train_only_selection_required"] is True
        assert "Strategy Building Block" in op["unsupported_message"]
        assert op["evidence_paths"]
        assert isinstance(op["safety_warnings"], list)
        assert op["docs"]["display_name"] != op["canonical_id"]


def test_registry_classifies_building_blocks_by_authoring_kind() -> None:
    payload = build_registry().export_payload()
    by_id = {op["canonical_id"]: op for op in payload["ops"]}

    assert {op_id: by_id[op_id]["block_kind"] for op_id in EXPECTED_CORE_OPS} == EXPECTED_BLOCK_KINDS

    indicator_ids = {op["canonical_id"] for op in payload["ops"] if op["block_kind"] == "indicator"}
    assert indicator_ids == {
        "indicator.ema",
        "indicator.atr",
        "indicator.bollinger",
        "indicator.calendar_return",
        "indicator.macd",
        "indicator.momentum",
        "indicator.percentile",
        "indicator.rsi",
        "indicator.sma",
        "indicator.volatility",
        "indicator.zscore",
    }

    condition_ids = {
        op["canonical_id"]
        for op in payload["ops"]
        if op["block_kind"] in {"condition_logic", "condition_comparator", "cross_condition"}
    }
    assert {"and", "or", "not", "gt", "cross_up"}.issubset(condition_ids)
    assert "indicator.rsi" not in condition_ids


def test_operation_ids_are_identical_across_registry_request_schema_and_rust() -> None:
    request_schema = _load_json(ENGINE_REQUEST_SCHEMA_PATH)
    schema_ops = set(
        request_schema["$defs"]["decisionPlan"]["properties"]["required_operations"]
        ["items"]["enum"]
    )
    rust_source = RUST_ENGINE_REQUEST_PATH.read_text(encoding="utf-8")
    operation_enum = rust_source.split("pub enum OperationId", 1)[1].split("\n}", 1)[0]
    rust_ops = set(re.findall(r'#\[serde\(rename = "([^"]+)"\)\]', operation_enum))

    assert schema_ops == EXPECTED_CORE_OPS
    assert rust_ops == EXPECTED_CORE_OPS


def test_usage_site_support_records_nodeir_and_multi_asset_differences() -> None:
    registry = build_registry()

    assert registry.is_supported("indicator.sma", MULTI_ASSET_INDICATORS)
    assert not registry.is_supported("sma", MULTI_ASSET_INDICATORS)
    assert not registry.is_supported("sma", MULTI_ASSET_INLINE_CONDITION_FEATURE)
    assert not registry.is_supported("ta.sma", EVENT_DRIVEN_FEATURE_DAG)
    assert not registry.is_supported("indicator.sma", EVENT_DRIVEN_FEATURE_DAG)

    assert not registry.is_supported("ema", MULTI_ASSET_INLINE_CONDITION_FEATURE)
    assert not registry.is_supported("ema", MULTI_ASSET_INDICATORS)
    assert not registry.is_supported("ema", EVENT_DRIVEN_FEATURE_DAG)

    assert registry.is_supported("indicator.rsi", MULTI_ASSET_INDICATORS)
    assert not registry.is_supported("indicator.rsi", MULTI_ASSET_INLINE_CONDITION_FEATURE)
    assert not registry.is_supported("indicator.rsi", EVENT_DRIVEN_FEATURE_DAG)
    assert registry.is_supported("indicator.zscore", MULTI_ASSET_INDICATORS)
    assert registry.is_supported("indicator.percentile", MULTI_ASSET_INDICATORS)
    assert registry.is_supported("indicator.bollinger", MULTI_ASSET_INDICATORS)
    assert registry.is_supported("indicator.atr", MULTI_ASSET_INDICATORS)

    assert registry.is_supported(">", "multi_asset.condition")
    assert not registry.is_supported(">", EVENT_DRIVEN_CONDITION)
    assert registry.is_supported("gt", EVENT_DRIVEN_CONDITION)


def test_public_indicator_aliases_are_not_accepted() -> None:
    registry = build_registry()

    for op_name in [
        "sma",
        "ta.sma",
        "ema",
        "ta.ema",
        "momentum",
        "return",
        "volatility",
        "rolling_volatility",
        "atr",
        "ta.atr",
        "average_true_range",
        "rsi",
        "ta.rsi",
        "macd",
        "ta.macd",
        "indicator.macd_signal",
        "macd_signal",
        "ta.macd_signal",
        "zscore",
        "ta.zscore",
        "percentile",
        "rolling_percentile",
        "bollinger",
        "bollinger_band",
        "ta.bollinger",
    ]:
        assert registry.resolve(op_name) is None
        assert not registry.is_supported(op_name, MULTI_ASSET_INDICATORS)


def test_alias_conflict_keeps_core_building_block_owner() -> None:
    core_sma = build_registry().resolve("indicator.sma")
    assert core_sma is not None
    intruder = deepcopy(core_sma)
    intruder["canonical_id"] = "workspace.fake_sma"
    intruder["aliases"] = ["indicator.sma"]
    intruder["implementation"] = {
        "path": "workspace/ops/fake_sma/op.py",
        "symbols": ["FakeSma"],
        "source_hash": None,
    }

    registry = build_registry(extra_specs=[intruder])

    assert registry.resolve("indicator.sma")["canonical_id"] == "indicator.sma"
    assert registry.resolve("workspace.fake_sma") is None
    assert registry.conflicts
    assert "already owned" in registry.conflicts[0]["reason"]


def test_workspace_extra_specs_are_namespaced_and_path_contained() -> None:
    core_sma = build_registry().resolve("indicator.sma")
    assert core_sma is not None
    intruder = deepcopy(core_sma)
    intruder["canonical_id"] = "workspace.escape"
    intruder["aliases"] = ["workspace_escape"]
    intruder["implementation"] = {
        "path": "workspace/ops/../escape.py",
        "symbols": ["Escape"],
        "source_hash": "a" * 64,
    }

    registry = build_registry(extra_specs=[intruder])

    assert registry.resolve("workspace.escape") is None
    assert registry.conflicts
    assert "workspace/ops" in registry.conflicts[0]["reason"]


def test_workspace_extra_specs_reject_escape_evidence_and_source_hashes() -> None:
    core_sma = build_registry().resolve("indicator.sma")
    assert core_sma is not None
    intruder = deepcopy(core_sma)
    intruder["canonical_id"] = "workspace.unsafe_evidence"
    intruder["aliases"] = ["workspace_unsafe_evidence"]
    intruder["implementation"] = {
        "path": "workspace/ops/unsafe_evidence/op.py",
        "symbols": ["UnsafeEvidence"],
        "source_hash": "a" * 64,
        "source_hashes": {
            "workspace/ops/unsafe_evidence/op.py": "a" * 64,
            "D:/outside.py": "b" * 64,
        },
    }
    intruder["evidence_paths"] = ["D:/private/evidence.txt"]

    registry = build_registry(extra_specs=[intruder])

    assert registry.resolve("workspace.unsafe_evidence") is None
    assert registry.conflicts
    assert "repo-relative" in registry.conflicts[0]["reason"] or "workspace/ops" in registry.conflicts[0]["reason"]


def test_workspace_extra_specs_reject_malformed_hashes() -> None:
    core_sma = build_registry().resolve("indicator.sma")
    assert core_sma is not None
    intruder = deepcopy(core_sma)
    intruder["canonical_id"] = "workspace.bad_hash"
    intruder["aliases"] = ["workspace_bad_hash"]
    intruder["implementation"] = {
        "path": "workspace/ops/bad_hash/op.py",
        "symbols": ["BadHash"],
        "source_hash": "not-a-sha256",
        "source_hashes": {
            "workspace/ops/bad_hash/op.py": "not-a-sha256",
        },
    }

    registry = build_registry(extra_specs=[intruder])

    assert registry.resolve("workspace.bad_hash") is None
    assert registry.conflicts
    assert "sha256" in registry.conflicts[0]["reason"]


def test_workspace_extra_specs_require_source_hashes_map() -> None:
    core_sma = build_registry().resolve("indicator.sma")
    assert core_sma is not None
    intruder = deepcopy(core_sma)
    intruder["canonical_id"] = "workspace.missing_source_hashes"
    intruder["aliases"] = ["workspace_missing_source_hashes"]
    intruder["implementation"] = {
        "path": "workspace/ops/missing_source_hashes/op.py",
        "symbols": ["MissingSourceHashes"],
        "source_hash": "a" * 64,
    }
    intruder["evidence_paths"] = ["tests/test_workspace_missing_source_hashes.py"]

    registry = build_registry(extra_specs=[intruder])

    assert registry.resolve("workspace.missing_source_hashes") is None
    assert registry.conflicts
    assert "source_hashes" in registry.conflicts[0]["reason"]


def test_op_schema_rejects_absolute_or_parent_escape_paths() -> None:
    payload = build_registry().export_payload()
    schema = _load_json(SCHEMA_PATH)

    bad_impl = deepcopy(payload)
    bad_impl["ops"][0]["implementation"]["path"] = "../../outside.py"
    errors = list(Draft202012Validator(schema).iter_errors(bad_impl))
    assert errors

    bad_evidence = deepcopy(payload)
    bad_evidence["ops"][0]["evidence_paths"] = ["D:/private/evidence.txt"]
    errors = list(Draft202012Validator(schema).iter_errors(bad_evidence))
    assert errors

    bad_source_hash_key = deepcopy(payload)
    source_hash = next(iter(bad_source_hash_key["ops"][0]["implementation"]["source_hashes"].values()))
    bad_source_hash_key["ops"][0]["implementation"]["source_hashes"] = {
        "D:/outside.py": source_hash,
        "../escape.py": source_hash,
    }
    errors = list(Draft202012Validator(schema).iter_errors(bad_source_hash_key))
    assert errors


def test_export_registry_path_is_contained() -> None:
    outside = REPO_ROOT / "tmp-op-registry.json"

    try:
        export_registry(outside)
    except ValueError as exc:
        assert "app" in str(exc)
        assert "generated" in str(exc)
    else:
        raise AssertionError("export_registry accepted an output path outside app/contracts/generated")


def test_calendar_param_refs_and_year_boundaries_match_strategy_schema() -> None:
    nth_weekday = build_registry().resolve("calendar.nth_weekday_of_month")
    assert nth_weekday is not None
    params = stable_json(nth_weekday["params_schema"])
    assert '"param_ref"' in params
    year_start = build_registry().resolve("calendar.year_start")
    assert year_start is not None
    assert "months" in year_start["params_schema"]["properties"]

    strategy_schema = stable_json(_load_json(STRATEGY_SCHEMA_PATH))
    assert '"calendar.year_start"' in strategy_schema
    assert '"calendar.year_end"' in strategy_schema


def test_source_hashes_cover_secondary_implementation_files() -> None:
    sma = build_registry().resolve("indicator.sma")
    same_session = build_registry().resolve("session.same_session_close")
    assert sma is not None
    assert same_session is not None

    assert "rust/lo2cin4bt_core/src/computed_fields/indicators.rs" in sma["implementation"]["source_hashes"]
    assert "rust/lo2cin4bt_core/src/engine_runtime.rs" in same_session["implementation"]["source_hashes"]
    assert not any("NodeIR" in path for path in sma["implementation"]["source_hashes"])
    assert not any("NodeIR" in path for path in same_session["implementation"]["source_hashes"])


def test_strategy_templates_are_authoring_blocks_not_runtime_claims() -> None:
    registry = build_registry()

    for op_name in [
        "template.single_asset_ma_cross",
        "template.monthly_nth_weekday_same_session",
        "template.fixed_allocation_rebalance",
        "template.momentum_rotation",
    ]:
        spec = registry.resolve(op_name)
        assert spec is not None
        assert spec["output_type"] == "strategy_run_config_template"
        assert registry.is_supported(op_name, "ai.strategy_authoring")
        assert "not a profitability claim" in " ".join(spec["safety_warnings"])
        for required_key in [
            "provider",
            "frequency",
            "calendar",
            "timezone",
            "universe",
            "universe_provenance",
            "benchmark",
            "execution_timing",
            "cost_model",
            "slippage_model",
        ]:
            assert required_key in spec["params_schema"]["required"]

    assert not registry.is_supported("template.vcp_ascending_triangle", "ai.strategy_authoring")


def test_target_change_is_the_only_registered_change_rebalance_trigger() -> None:
    registry = build_registry()
    signal_change = registry.resolve("signal.change")
    target_change = registry.resolve("target.change")

    assert signal_change is None
    assert target_change is not None
    assert target_change["canonical_id"] == "target.change"
    assert registry.is_supported("target.change", "multi_asset.rebalance_trigger")
    assert not registry.is_supported("signal.change", "multi_asset.condition")


def test_usage_site_is_required_for_support_verdicts() -> None:
    report = build_registry().support_report("indicator.sma")

    assert report["supported"] is False
    assert "usage_site is required" in report["reason"]


def test_deterministic_export_order_and_generated_snapshot_match_code() -> None:
    payload = build_registry().export_payload()
    ids = [op["canonical_id"] for op in payload["ops"]]

    assert ids == sorted(ids)
    assert stable_json(payload) == stable_json(build_registry().export_payload())
    assert _load_json(EXPORT_PATH) == payload


def test_unknown_ops_report_unsupported() -> None:
    registry = build_registry()
    report = registry.support_report("indicator.future_magic", usage_site=MULTI_ASSET_INDICATORS)

    assert report["supported"] is False
    assert report["canonical_id"] is None
    assert "Unsupported Strategy Building Blocks op" in report["reason"]


def test_op_spec_shared_constants_match_required_fields() -> None:
    from backtester.ops.spec import OP_SPEC_REQUIRED_FIELDS

    assert set(OP_SPEC_REQUIRED_FIELDS) == REQUIRED_OP_FIELDS
