import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_audit_output_contract_schema_and_example_exist() -> None:
    schema_path = (
        REPO_ROOT
        / "backtester"
        / "contracts"
        / "audit-output"
        / "audit-output-contract-v1.schema.json"
    )
    example_path = (
        REPO_ROOT
        / "backtester"
        / "contracts"
        / "audit-output"
        / "examples"
        / "audit-output-contract-v1.example.json"
    )

    assert schema_path.exists()
    assert example_path.exists()


def test_audit_output_contract_example_protects_newbie_default_surface() -> None:
    example_path = (
        REPO_ROOT
        / "backtester"
        / "contracts"
        / "audit-output"
        / "examples"
        / "audit-output-contract-v1.example.json"
    )
    payload = json.loads(example_path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "1.0"
    assert payload["placement_policy"]["machine_readable_detailed_audit"] is True
    assert payload["placement_policy"]["audit_parquet_compression"] == "zstd"
    assert payload["placement_policy"]["audit_json_inline_row_limit"] == 1000
    assert payload["placement_policy"]["audit_json_chunk_suffix"] == "_audit_rows_001.jsonl"
    assert payload["ui_policy"]["hide_detailed_audit_by_default"] is True
    assert payload["ui_policy"]["newbie_default_view"] in {
        "summary_only",
        "summary_with_warnings",
    }
    assert "source_audit_id" in payload["summary_index_fields"]
    assert "source_time" in payload["detailed_feature_audit_fields"]
    assert "value_origin" in payload["detailed_feature_audit_fields"]
