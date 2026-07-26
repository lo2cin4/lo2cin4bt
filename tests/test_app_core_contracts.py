import json
from pathlib import Path

from jsonschema import Draft202012Validator

from app.api.payloads import METRICS_OVERVIEW_SCHEMA_VERSION


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_ROOT = REPO_ROOT / "app" / "contracts"
EXAMPLES_ROOT = CONTRACT_ROOT / "examples"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _page_state(page_def: dict, available_artifacts: set[str]) -> str:
    required = set(page_def.get("required_artifacts", []))
    optional = set(page_def.get("optional_artifacts", []))
    if not required.issubset(available_artifacts):
        return "blocked"
    if optional and not optional.issubset(available_artifacts):
        return "partial"
    return "renderable"


def test_app_core_contract_files_exist() -> None:
    required_files = [
        CONTRACT_ROOT / "run-registry-v1.schema.json",
        CONTRACT_ROOT / "run-snapshot-v1.schema.json",
        CONTRACT_ROOT / "artifact-manifest-v1.schema.json",
        CONTRACT_ROOT / "dataloader-health-v1.schema.json",
        CONTRACT_ROOT / "data-lineage-manifest-v1.schema.json",
        CONTRACT_ROOT / "chart-payload-v1.schema.json",
        CONTRACT_ROOT / "ai-readable-output-v1.schema.json",
        CONTRACT_ROOT / "page-artifact-matrix-v1.schema.json",
        EXAMPLES_ROOT / "run-registry-v1.example.json",
        EXAMPLES_ROOT / "run-snapshot-v1.example.json",
        EXAMPLES_ROOT / "artifact-manifest-v1.example.json",
        EXAMPLES_ROOT / "dataloader-health-v1.example.json",
        EXAMPLES_ROOT / "data-lineage-manifest-v1.example.json",
        EXAMPLES_ROOT / "chart-payload-v1.example.json",
        EXAMPLES_ROOT / "ai-readable-output-v1.example.json",
        EXAMPLES_ROOT / "page-artifact-matrix-v1.example.json",
        CONTRACT_ROOT / "README.md",
        REPO_ROOT / "docs" / "app-core-contracts.md",
    ]

    missing = [path for path in required_files if not path.exists()]
    assert missing == []


def test_app_core_contract_examples_validate_against_schemas() -> None:
    pairs = [
        ("run-registry-v1", "run-registry-v1"),
        ("run-snapshot-v1", "run-snapshot-v1"),
        ("artifact-manifest-v1", "artifact-manifest-v1"),
        ("dataloader-health-v1", "dataloader-health-v1"),
        ("data-lineage-manifest-v1", "data-lineage-manifest-v1"),
        ("chart-payload-v1", "chart-payload-v1"),
        ("ai-readable-output-v1", "ai-readable-output-v1"),
        ("page-artifact-matrix-v1", "page-artifact-matrix-v1"),
    ]
    for schema_name, example_name in pairs:
        schema = _load_json(CONTRACT_ROOT / f"{schema_name}.schema.json")
        example = _load_json(EXAMPLES_ROOT / f"{example_name}.example.json")
        Draft202012Validator(schema).validate(example)


def test_run_registry_example_carries_required_app_fields() -> None:
    payload = _load_json(EXAMPLES_ROOT / "run-registry-v1.example.json")
    schema = _load_json(CONTRACT_ROOT / "run-registry-v1.schema.json")

    assert payload["schema_version"] == "1.0"
    assert payload["status"] == "completed"
    assert payload["config_snapshot_dir"].startswith("outputs/app/run_snapshots/")
    assert payload["artifact_manifest_path"].startswith("outputs/app/artifact_manifests/")
    assert payload["dataloader_health_path"].endswith("/dataloader_health.json")
    assert payload["data_lineage_manifest_path"].endswith("/data_lineage_manifest.json")
    assert payload["lineage_status"] in {"complete", "partial", "unknown", "pending"}
    assert "lineage_status" in schema["required"]
    assert payload["warning_count"] >= 0
    assert payload["error_count"] >= 0


def test_run_snapshot_example_locks_new_results_only_registry_mode() -> None:
    payload = _load_json(EXAMPLES_ROOT / "run-snapshot-v1.example.json")

    assert payload["app_runtime"]["server_mode"] == "browser_first"
    assert payload["app_runtime"]["registry_mode"] == "new_results_only"
    assert payload["execution_plan"]["path"].startswith("outputs/app/run_snapshots/")


def test_artifact_manifest_example_uses_finalize_aware_statuses() -> None:
    payload = _load_json(EXAMPLES_ROOT / "artifact-manifest-v1.example.json")

    statuses = {item["status"] for item in payload["artifacts"]}
    assert statuses == {"ready"}
    assert all(item["content_contract"] for item in payload["artifacts"])
    assert all(item["source_stage"] for item in payload["artifacts"])


def test_dataloader_health_example_keeps_quality_metrics_in_dataloader_layer() -> None:
    payload = _load_json(EXAMPLES_ROOT / "dataloader-health-v1.example.json")

    assert payload["audit_available"] is True
    assert payload["fill_ratio"] >= 0
    assert payload["stale_ratio"] >= 0
    assert payload["max_age_bars"] >= 0
    assert payload["primary_source"] == "price"
    assert len(payload["source_list"]) >= 1


def test_data_lineage_manifest_example_keeps_proven_inferred_unknown_claims() -> None:
    payload = _load_json(EXAMPLES_ROOT / "data-lineage-manifest-v1.example.json")

    assert payload["contract_id"] == "lo2cin4bt-app-data-lineage-manifest-v1"
    assert payload["lineage_status"] == "partial"
    assert payload["input_sources"]
    assert payload["input_sources"][0]["source_type"] == "provider"
    assert payload["input_sources"][0]["content_hash"] is None
    assert payload["universe_provenance"]["survivorship_bias_risk"] == "high"
    assert payload["universe_provenance"]["point_in_time_constituents"] is False
    assert payload["consumed_data_snapshot"]["status"] == "not_captured"
    assert payload["factor_feature_audit"]["status"] == "not_applicable"
    assert payload["validity_flags"]["lookahead_guard_verified"] is True
    assert payload["validity_flags"]["corporate_actions_known"] is False
    assert payload["lineage_claims"]["proven"]
    assert payload["lineage_claims"]["inferred"]
    assert payload["lineage_claims"]["unknown"]


def test_chart_payload_example_is_ai_readable_and_source_linked() -> None:
    payload = _load_json(EXAMPLES_ROOT / "chart-payload-v1.example.json")

    assert payload["schema_version"] == "plot_bundle.v1"
    assert payload["contract_id"] == "lo2cin4bt.plot_bundle.v1"
    assert payload["series"]
    assert payload["source_hashes"]
    assert payload["artifact_source_refs"]
    assert payload["axes"]["x"]
    assert payload["axes"]["y"]


def test_ai_readable_output_example_embeds_future_metric_catalog() -> None:
    payload = _load_json(EXAMPLES_ROOT / "ai-readable-output-v1.example.json")

    assert payload["contract_id"] == "lo2cin4bt-app-ai-readable-output-v1"
    assert "metrics_overview_payload" in payload["source_payloads"]
    assert payload["payload_index"][0]["schema_version"] == METRICS_OVERVIEW_SCHEMA_VERSION
    assert (
        payload["source_payloads"]["metrics_overview_payload"]["schema_version"]
        == METRICS_OVERVIEW_SCHEMA_VERSION
    )
    field_paths = {item["path"] for item in payload["metric_field_catalog"]["fields"]}
    assert (
        "source_payloads.metrics_overview_payload.rows[].new_future_score"
        in field_paths
    )


def test_page_artifact_matrix_covers_foundation_pages() -> None:
    payload = _load_json(EXAMPLES_ROOT / "page-artifact-matrix-v1.example.json")
    page_map = {page["page_id"]: page for page in payload["pages"]}

    expected_pages = {
        "run_center",
        "results_library",
        "backtest_explorer",
        "wfa_studio",
        "statanalyser_studio",
        "metrics_explorer",
    }
    assert expected_pages.issubset(page_map.keys())
    assert "backtester_parquet" in page_map["backtest_explorer"]["required_artifacts"]
    assert "metricstracker_parquet" in page_map["metrics_explorer"]["required_artifacts"]
    assert page_map["metrics_explorer"]["fallback_policy"]["allow_ui_rebuild"] is False
    assert page_map["run_center"]["fallback_policy"]["allow_ui_rebuild"] is False


def test_page_artifact_matrix_supports_blocked_partial_and_renderable_states() -> None:
    payload = _load_json(EXAMPLES_ROOT / "page-artifact-matrix-v1.example.json")
    page_map = {page["page_id"]: page for page in payload["pages"]}
    backtest_page = page_map["backtest_explorer"]

    assert _page_state(backtest_page, {"run_registry"}) == "blocked"
    assert (
        _page_state(backtest_page, {"run_registry", "backtester_parquet"})
        == "partial"
    )
    assert (
        _page_state(
            backtest_page,
            {
                "run_registry",
                "backtester_parquet",
                "metricstracker_parquet",
                "audit_sidecar",
                "execution_plan_json",
                "dataloader_health",
                "data_lineage_manifest_json",
            },
        )
        == "renderable"
    )


def test_app_docs_lock_immediate_cutover_boundary() -> None:
    readme = (CONTRACT_ROOT / "README.md").read_text(encoding="utf-8").lower()
    docs = (REPO_ROOT / "docs" / "app-core-contracts.md").read_text(
        encoding="utf-8"
    ).lower()

    assert "immediate cutover" in readme
    assert "new runs" in readme
    assert "statanalyser" in docs
    assert "summary json" in docs
