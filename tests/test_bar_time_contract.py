from __future__ import annotations

import copy
import json
from pathlib import Path

from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "bar-time-contract-v1.schema.json"
)
EXAMPLE_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "examples"
    / "bar-time-contract-v1.example.json"
)


def _schema() -> dict[str, object]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _example() -> dict[str, object]:
    return json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))


def _errors(payload: dict[str, object]) -> list[object]:
    return list(Draft202012Validator(_schema()).iter_errors(payload))


def test_schema_is_valid_and_direct_external_daily_example_conforms() -> None:
    schema = _schema()
    Draft202012Validator.check_schema(schema)

    example = _example()
    Draft202012Validator(schema).validate(example)

    execution = [
        stream for stream in example["streams"] if stream["role"] == "execution"
    ]
    assert len(execution) == 1
    assert example["timestamp_model"]["ordering"] == (
        "available_time_then_event_time_then_external_execution_sequence"
        "_then_lifecycle_stage_then_stream_id_then_source_sequence"
    )
    assert execution[0]["source"]["kind"] == "external"
    assert execution[0]["source"]["provider_id"] == "fixture_daily_direct"
    assert execution[0]["bar_spec"]["step"] == 1
    assert execution[0]["bar_spec"]["unit"] == "day"

    derived = [
        stream for stream in example["streams"] if stream["source"]["kind"] == "derived"
    ]
    assert derived
    assert derived[0]["source"]["parent_stream_id"] == execution[0]["stream_id"]


def test_contract_requires_exactly_one_execution_stream() -> None:
    no_execution = _example()
    for stream in no_execution["streams"]:
        stream["role"] = "decision"
    assert _errors(no_execution)

    two_executions = _example()
    two_executions["streams"][1]["role"] = "execution"
    assert _errors(two_executions)


def test_external_source_can_serve_either_role_but_derived_is_decision_only() -> None:
    payload = _example()
    external_decision = copy.deepcopy(payload["streams"][0])
    external_decision["stream_id"] = "external_monthly_regime"
    external_decision["role"] = "decision"
    external_decision["bar_spec"] = {
        "aggregation": "time",
        "step": 1,
        "unit": "month",
        "price_type": "last",
        "alignment": "calendar_period_start",
    }
    payload["streams"].append(external_decision)

    Draft202012Validator(_schema()).validate(payload)

    derived_execution = _example()
    derived_execution["streams"][0]["role"] = "decision"
    derived_execution["streams"][1]["role"] = "execution"
    assert _errors(derived_execution)


def test_source_shape_is_discriminated_and_fail_closed() -> None:
    external_with_parent = _example()
    external_with_parent["streams"][0]["source"]["parent_stream_id"] = "other"
    assert _errors(external_with_parent)

    derived_without_parent = _example()
    del derived_without_parent["streams"][1]["source"]["parent_stream_id"]
    assert _errors(derived_without_parent)

    derived_without_shared_rust = _example()
    derived_without_shared_rust["streams"][1]["source"][
        "aggregation_engine"
    ] = "python_resample"
    assert _errors(derived_without_shared_rust)

    derived_without_final_bar_policy = _example()
    del derived_without_final_bar_policy["streams"][1]["source"][
        "partial_final_bar_policy"
    ]
    assert _errors(derived_without_final_bar_policy)

    derived_emits_partial_final_bar = _example()
    derived_emits_partial_final_bar["streams"][1]["source"][
        "partial_final_bar_policy"
    ] = "emit"
    Draft202012Validator(_schema()).validate(derived_emits_partial_final_bar)

    invalid_partial_final_policy = _example()
    invalid_partial_final_policy["streams"][1]["source"][
        "partial_final_bar_policy"
    ] = "implicit"
    assert _errors(invalid_partial_final_policy)


def test_phase_zero_units_and_alignment_are_bounded() -> None:
    for unsupported_unit in ("second", "year"):
        payload = _example()
        payload["streams"][0]["bar_spec"]["unit"] = unsupported_unit
        assert _errors(payload)

    payload = _example()
    payload["streams"][0]["bar_spec"]["alignment"] = "session"
    assert _errors(payload)

    payload = _example()
    payload["streams"][1]["bar_spec"]["alignment"] = "calendar_period"
    assert _errors(payload)


def test_contract_rejects_legacy_frequency_and_interval_fields() -> None:
    payload = _example()
    payload["frequency"] = "1D"
    assert _errors(payload)

    payload = _example()
    payload["streams"][0]["bar_spec"]["interval"] = "1D"
    assert _errors(payload)


def test_timestamp_and_session_semantics_are_explicit() -> None:
    payload = _example()
    del payload["timestamp_model"]["precision"]
    assert _errors(payload)

    payload = _example()
    del payload["session_model"]["session_label_policy"]
    assert _errors(payload)

    payload = _example()
    del payload["streams"][0]["timestamp_semantics"]["available_time_column"]
    assert _errors(payload)

    payload = _example()
    payload["streams"][0]["timestamp_semantics"]["availability_policy"] = (
        "provider_guess"
    )
    assert _errors(payload)

    payload = _example()
    payload["timestamp_model"]["clock"] = "event_driven"
    assert _errors(payload)

    payload = _example()
    payload["timestamp_model"]["ordering"] = "available_time_then_stream_id"
    assert _errors(payload)


def test_price_semantics_are_one_explicit_run_level_model() -> None:
    payload = _example()
    assert payload["price_model"] == {
        "price_basis": "split_dividend_adjusted",
        "corporate_action_policy": "provider_applied",
    }

    del payload["price_model"]
    assert _errors(payload)

    payload = _example()
    payload["price_model"]["price_basis"] = "provider_default"
    assert _errors(payload)

    payload = _example()
    payload["price_model"]["corporate_action_policy"] = "silent_fallback"
    assert _errors(payload)


def test_schema_records_cross_stream_validator_invariants() -> None:
    invariants = set(_schema()["x-contract-invariants"])

    assert {
        "unique_stream_ids",
        "derived_parent_stream_exists",
        "derived_lineage_is_acyclic",
        "derived_bar_is_coarser_and_compatible_with_parent",
        "provider_supports_exact_external_bar_spec",
        "provider_supports_exact_price_model",
        "execution_stream_source_is_external",
        "derived_stream_role_is_decision",
        "same_timestamp_lifecycle_order_is_data_derived_signal_order_fill",
    }.issubset(invariants)


def test_phase_zero_rejects_ambiguous_empty_bars_and_calendar_alignment() -> None:
    payload = _example()
    payload["streams"][1]["source"]["empty_bar_policy"] = "emit"
    assert _errors(payload)

    payload = _example()
    payload["streams"][1]["bar_spec"]["alignment"] = "session_open"
    assert _errors(payload)


def test_phase_zero_session_scope_is_regular_or_24x7_and_rejects_non_session_bars() -> None:
    payload = _example()
    payload["session_model"]["session_scope"] = "extended"
    assert _errors(payload)

    payload = _example()
    payload["session_model"]["non_session_bar_policy"] = "preserve"
    assert _errors(payload)
