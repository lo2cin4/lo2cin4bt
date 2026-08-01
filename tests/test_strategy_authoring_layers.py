import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LAYER_CONTRACT_PATH = REPO_ROOT / "backtester" / "contracts" / "strategy_authoring" / "strategy-authoring-layers-v1.json"
TEMPLATE_DIR = REPO_ROOT / "backtester" / "contracts" / "strategy_authoring" / "templates"
AUTHORING_REFERENCE_PATH = REPO_ROOT / "skills" / "lo2cin4bt" / "references" / "strategy-authoring-template.md"
CONFIG_FIELDS_REFERENCE_PATH = REPO_ROOT / "skills" / "lo2cin4bt" / "references" / "strategy-config-fields.md"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_strategy_authoring_layers_define_user_and_machine_boundaries() -> None:
    payload = _load_json(LAYER_CONTRACT_PATH)

    assert payload["schema_version"] == "strategy_authoring_layers.v1"
    layer_ids = [layer["layer_id"] for layer in payload["layers"]]
    assert layer_ids == [
        "strategy_config_dsl",
        "machine_ir",
        "building_block_registry",
        "runtime_adapter",
        "verification",
    ]

    dsl = payload["layers"][0]
    machine_ir = payload["layers"][1]

    assert dsl["audience"] == "human_ai"
    assert machine_ir["audience"] == "validator_backtester"
    assert dsl["writes_to"] == "machine_ir"
    assert machine_ir["reads_from"] == "strategy_config_dsl"

    dsl_sections = {section["section_id"]: section for section in dsl["sections"]}
    assert dsl_sections["template"]["allowed_block_kinds"] == ["strategy_template"]
    assert dsl_sections["computed_fields"]["allowed_block_kinds"] == ["indicator"]
    assert dsl_sections["signals"]["allowed_block_kinds"] == [
        "condition_logic",
        "condition_comparator",
        "cross_condition",
        "calendar",
        "execution",
    ]
    assert dsl_sections["selection"]["allowed_block_kinds"] == [
        "indicator",
        "condition_logic",
        "condition_comparator",
        "cross_condition",
    ]

    machine_columns = {column["name"] for column in machine_ir["required_columns"]}
    assert {
        "canonical_id",
        "block_kind",
        "usage_site",
        "params_schema",
        "temporal_metadata",
        "implementation_source_hash",
        "evidence_paths",
    }.issubset(machine_columns)

    for section in dsl["sections"]:
        for block_kind in section["allowed_block_kinds"]:
            matching = [item for item in payload["block_kinds"] if item["kind"] == block_kind]
            assert matching
            assert section["section_id"] in matching[0]["dsl_sections"]


def test_strategy_authoring_templates_exist_and_name_their_layer() -> None:
    expected_templates = {
        "strategy-config-dsl-v1.template.yaml": "strategy_config_dsl",
        "machine-ir-v1.template.json": "machine_ir",
        "building-block-spec-v1.template.json": "building_block_registry",
        "oracle-test-v1.template.py": "verification",
    }

    for filename, layer_id in expected_templates.items():
        text = (TEMPLATE_DIR / filename).read_text(encoding="utf-8")
        assert layer_id in text
        assert "template_only" in text
        assert "do_not_run" in text
        assert "TODO" not in text
        assert "TBD" not in text


def test_machine_ir_template_carries_registry_metadata_columns() -> None:
    payload = _load_json(TEMPLATE_DIR / "machine-ir-v1.template.json")

    for node in payload["nodes"]:
        assert node["params_schema"]
        assert node["temporal_metadata"]["observation_time"]
        assert len(node["implementation_source_hash"]) == 64
        assert node["evidence_paths"]


def test_strategy_config_dsl_template_keeps_logic_and_computed_fields_separate() -> None:
    text = (TEMPLATE_DIR / "strategy-config-dsl-v1.template.yaml").read_text(encoding="utf-8")

    assert "platform:" in text
    assert "strategy_profile_id: selection_timing_portfolio" in text
    assert "strategy_preset_id: single_asset_signal" in text
    assert "computed_fields:" in text
    assert "signals:" in text
    assert "selection:" in text
    assert "allocation:" in text
    assert "rebalance:" in text
    assert "op: indicator.rsi" in text
    assert "op: lt" in text
    assert "field: rsi_14" in text
    assert "method: signal_state" not in text
    assert "trigger: signal_change" not in text
    assert "feature:" not in text
    assert "bar_time:" in text
    assert "stream_binding:" in text
    assert "frequency:" not in text
    assert "\n  calendar:" not in text
    assert "\n  timezone:" not in text


def test_ai_skill_docs_reference_two_layer_authoring_contract() -> None:
    authoring_text = AUTHORING_REFERENCE_PATH.read_text(encoding="utf-8")
    config_text = CONFIG_FIELDS_REFERENCE_PATH.read_text(encoding="utf-8")

    assert "backtester/contracts/strategy_authoring/strategy-authoring-layers-v1.json" in authoring_text
    assert "Strategy Config DSL" in authoring_text
    assert "Machine IR" in authoring_text
    assert "block_kind" in authoring_text

    assert "condition_logic" in config_text
    assert "condition_comparator" in config_text
    assert "indicator" in config_text
    assert "strategy_template" in config_text
