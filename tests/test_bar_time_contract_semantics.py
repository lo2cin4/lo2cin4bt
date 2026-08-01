from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from backtester.timeframe_contracts import validate_bar_time_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_CONTRACT_ROOT = REPO_ROOT / "backtester" / "contracts" / "runtime"
BAR_TIME_EXAMPLE = (
    RUNTIME_CONTRACT_ROOT / "examples" / "bar-time-contract-v1.example.json"
)
PROVIDER_CAPABILITY_EXAMPLE = (
    RUNTIME_CONTRACT_ROOT
    / "examples"
    / "provider-timeframe-capability-v1.example.json"
)


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _bar_time() -> dict[str, object]:
    return _load(BAR_TIME_EXAMPLE)


def _capabilities() -> dict[str, dict[str, object]]:
    capability = _load(PROVIDER_CAPABILITY_EXAMPLE)
    return {str(capability["provider_id"]): capability}


def test_validates_direct_daily_and_derived_monthly_lineage() -> None:
    validate_bar_time_contract(_bar_time(), _capabilities())


def test_rejects_duplicate_stream_ids() -> None:
    payload = _bar_time()
    payload["streams"][1]["stream_id"] = payload["streams"][0]["stream_id"]

    with pytest.raises(ValueError, match="stream_id.*unique"):
        validate_bar_time_contract(payload)


def test_rejects_missing_or_cyclic_derived_parent() -> None:
    payload = _bar_time()
    payload["streams"][1]["source"]["parent_stream_id"] = "missing"
    with pytest.raises(ValueError, match="parent_stream_id.*missing"):
        validate_bar_time_contract(payload)

    payload = _bar_time()
    second = copy.deepcopy(payload["streams"][1])
    second["stream_id"] = "decision_weekly"
    second["source"]["parent_stream_id"] = "decision_monthly"
    second["bar_spec"]["unit"] = "week"
    payload["streams"][1]["source"]["parent_stream_id"] = "decision_weekly"
    payload["streams"].append(second)
    with pytest.raises(ValueError, match="cycle"):
        validate_bar_time_contract(payload)


def test_rejects_derived_stream_that_is_not_coarser_than_parent() -> None:
    payload = _bar_time()
    payload["streams"][1]["bar_spec"].update({"step": 1, "unit": "minute"})

    with pytest.raises(ValueError, match="strictly coarser"):
        validate_bar_time_contract(payload)


def test_rejects_non_divisible_fixed_duration_and_non_nesting_calendar_parent() -> None:
    payload = _bar_time()
    payload["streams"][0]["bar_spec"].update(
        {"step": 45, "unit": "minute", "alignment": "session_open"}
    )
    payload["streams"][1]["bar_spec"].update(
        {"step": 1, "unit": "hour", "alignment": "session_open"}
    )
    with pytest.raises(ValueError, match="strictly coarser"):
        validate_bar_time_contract(payload)

    payload = _bar_time()
    payload["streams"][0]["bar_spec"].update(
        {"step": 1, "unit": "week", "alignment": "calendar_period_start"}
    )
    with pytest.raises(ValueError, match="strictly coarser"):
        validate_bar_time_contract(payload)


def test_external_stream_must_exact_match_provider_capability() -> None:
    payload = _bar_time()
    payload["streams"][0]["bar_spec"]["step"] = 5

    with pytest.raises(ValueError, match="exact capability"):
        validate_bar_time_contract(payload, _capabilities())


def test_provider_native_timestamp_timezone_is_independent_from_session_timezone() -> None:
    capabilities = _capabilities()
    capability = next(iter(capabilities.values()))
    capability["supported_timeframes"][0]["timestamp_semantics"]["timezone"] = "UTC"

    validate_bar_time_contract(_bar_time(), capabilities)


def test_provider_calendar_session_price_and_availability_are_fail_closed() -> None:
    cases = [
        ("calendar", "calendar_id", "XHKG"),
        ("session", "session_scope", "24x7"),
        ("price", "price_basis", "raw"),
        ("availability", "availability_policy", "explicit_timestamp"),
    ]

    for _, field, value in cases:
        payload = _bar_time()
        if field in payload["session_model"]:
            payload["session_model"][field] = value
        elif field in payload["price_model"]:
            payload["price_model"][field] = value
        else:
            payload["streams"][0]["timestamp_semantics"][field] = value

        with pytest.raises(ValueError, match="exact capability"):
            validate_bar_time_contract(payload, _capabilities())


def test_missing_provider_capability_cannot_fallback() -> None:
    with pytest.raises(ValueError, match="provider capability.*missing"):
        validate_bar_time_contract(_bar_time(), {})
