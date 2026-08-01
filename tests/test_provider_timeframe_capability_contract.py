from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_CONTRACT_ROOT = REPO_ROOT / "backtester" / "contracts" / "runtime"
SCHEMA_PATH = (
    RUNTIME_CONTRACT_ROOT / "provider-timeframe-capability-v1.schema.json"
)
EXAMPLE_PATH = (
    RUNTIME_CONTRACT_ROOT
    / "examples"
    / "provider-timeframe-capability-v1.example.json"
)


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validator() -> Draft202012Validator:
    schema = _load(SCHEMA_PATH)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _example() -> dict[str, object]:
    return _load(EXAMPLE_PATH)


def test_example_is_a_valid_explicit_provider_capability() -> None:
    payload = _example()

    _validator().validate(payload)

    assert payload["schema_version"] == "provider_timeframe_capability.v1"
    assert payload["contract_id"] == "lo2cin4bt.provider_timeframe_capability.v1"
    assert payload["source_kind"] == "external"
    assert payload["supported_timeframes"] == [
        {
            "aggregation": "time",
            "step": 1,
            "unit": "day",
            "price_types": ["last"],
            "alignments": ["session_open"],
            "calendar_ids": ["XNYS"],
            "history": {
                "depth": {
                    "kind": "bounded",
                    "amount": 20,
                    "unit": "year",
                },
                "pagination": {
                    "mode": "not_supported",
                    "max_bars_per_request": None,
                },
            },
            "timestamp_semantics": {
                "timestamp_convention": "bar_open",
                "precision": "second",
                "timezone": "America/New_York",
                "availability": "bar_close",
            },
            "session_scopes": ["regular"],
            "price_policy": {
                "price_basis": "split_dividend_adjusted",
                "corporate_action_policy": "provider_applied",
            },
            "quality_policy": {
                "missing_bar_policy": "preserve_gap",
                "duplicate_timestamp_policy": "fail",
                "out_of_order_policy": "sort_then_validate",
            },
        }
    ]


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("history", "depth"),
        ("history", "pagination"),
        ("timestamp_semantics", "timestamp_convention"),
        ("timestamp_semantics", "timezone"),
        ("timestamp_semantics", "availability"),
        ("price_policy", "price_basis"),
        ("price_policy", "corporate_action_policy"),
        ("quality_policy", "missing_bar_policy"),
        ("quality_policy", "duplicate_timestamp_policy"),
    ],
)
def test_each_timeframe_requires_complete_provider_semantics(
    section: str,
    field: str,
) -> None:
    payload = _example()
    timeframe = payload["supported_timeframes"][0]
    del timeframe[section][field]

    assert list(_validator().iter_errors(payload))


@pytest.mark.parametrize(
    "field",
    [
        "aggregation",
        "step",
        "unit",
        "price_types",
        "alignments",
        "calendar_ids",
        "session_scopes",
    ],
)
def test_each_timeframe_requires_an_exact_supported_bar_combination(
    field: str,
) -> None:
    payload = _example()
    del payload["supported_timeframes"][0][field]

    assert list(_validator().iter_errors(payload))


def test_supported_timeframe_requires_positive_step_and_known_unit() -> None:
    payload = _example()
    payload["supported_timeframes"][0]["step"] = 0
    assert list(_validator().iter_errors(payload))

    payload = _example()
    payload["supported_timeframes"][0]["unit"] = "1d"
    assert list(_validator().iter_errors(payload))

    payload = _example()
    payload["supported_timeframes"][0]["unit"] = "second"
    assert list(_validator().iter_errors(payload))


def test_alignment_and_availability_cannot_reintroduce_lookahead() -> None:
    payload = _example()
    payload["supported_timeframes"][0]["alignments"] = ["session"]
    assert list(_validator().iter_errors(payload))

    payload = _example()
    payload["supported_timeframes"][0]["timestamp_semantics"]["availability"] = (
        "bar_open"
    )
    assert list(_validator().iter_errors(payload))


def test_bounded_history_requires_positive_amount_and_unit() -> None:
    payload = _example()
    payload["supported_timeframes"][0]["history"]["depth"]["amount"] = None

    assert list(_validator().iter_errors(payload))


def test_unbounded_history_rejects_a_fake_limit() -> None:
    payload = _example()
    depth = payload["supported_timeframes"][0]["history"]["depth"]
    depth.update({"kind": "unbounded", "amount": 20, "unit": "year"})

    assert list(_validator().iter_errors(payload))


def test_pagination_contract_cannot_hide_a_page_limit() -> None:
    payload = _example()
    pagination = payload["supported_timeframes"][0]["history"]["pagination"]
    pagination["max_bars_per_request"] = 5000

    assert list(_validator().iter_errors(payload))


def test_provider_can_declare_multiple_exact_external_timeframes() -> None:
    payload = _example()
    intraday = copy.deepcopy(payload["supported_timeframes"][0])
    intraday.update({"step": 5, "unit": "minute"})
    intraday["history"] = {
        "depth": {
            "kind": "bounded",
            "amount": 30,
            "unit": "calendar_day",
        },
        "pagination": {
            "mode": "required",
            "max_bars_per_request": 1000,
        },
    }
    payload["supported_timeframes"].append(intraday)

    _validator().validate(payload)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("unsupported_timeframe", "nearest_supported"),
        ("unavailable_history_depth", "truncate"),
        ("unsupported_session_scope", "regular"),
        ("unsupported_price_basis", "raw"),
        ("provider_fallback", "allowed"),
        ("frequency_fallback", "allowed"),
    ],
)
def test_every_unsupported_request_path_is_fail_closed(
    field: str,
    invalid_value: str,
) -> None:
    payload = _example()
    payload["unsupported_request_policy"][field] = invalid_value

    assert list(_validator().iter_errors(payload))


def test_unknown_fields_cannot_create_an_implicit_fallback() -> None:
    payload = copy.deepcopy(_example())
    payload["fallback_provider"] = "fixture-secondary"
    payload["supported_timeframes"][0]["frequency_aliases"] = ["1D", "daily"]

    assert list(_validator().iter_errors(payload))


def test_provider_capability_cannot_claim_a_derived_stream() -> None:
    payload = _example()
    payload["source_kind"] = "derived"

    assert list(_validator().iter_errors(payload))


def test_phase_zero_provider_sessions_are_regular_or_24x7() -> None:
    payload = _example()
    payload["supported_timeframes"][0]["session_scopes"] = ["extended"]

    assert list(_validator().iter_errors(payload))
