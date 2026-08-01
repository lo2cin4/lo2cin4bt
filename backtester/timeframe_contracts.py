from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


_CONTRACT_ROOT = Path(__file__).resolve().parent / "contracts" / "runtime"
_BAR_TIME_SCHEMA = _CONTRACT_ROOT / "bar-time-contract-v1.schema.json"
_PROVIDER_CAPABILITY_SCHEMA = (
    _CONTRACT_ROOT / "provider-timeframe-capability-v1.schema.json"
)
_UNIT_ORDER = {
    "minute": 0,
    "hour": 1,
    "day": 2,
    "week": 3,
    "month": 4,
}


def _schema(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_schema(payload: Mapping[str, Any], schema_path: Path) -> None:
    validator = Draft202012Validator(_schema(schema_path))
    errors = sorted(validator.iter_errors(payload), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.path) or "<root>"
        raise ValueError(f"{schema_path.name} validation failed at {location}: {first.message}")


def _is_strictly_coarser(
    child_spec: Mapping[str, Any],
    parent_spec: Mapping[str, Any],
) -> bool:
    child_unit = str(child_spec["unit"])
    parent_unit = str(parent_spec["unit"])
    if child_unit in {"minute", "hour"} and parent_unit in {"minute", "hour"}:
        child_minutes = int(child_spec["step"]) * (60 if child_unit == "hour" else 1)
        parent_minutes = int(parent_spec["step"]) * (
            60 if parent_unit == "hour" else 1
        )
        return child_minutes > parent_minutes and child_minutes % parent_minutes == 0

    if child_unit == "month" and parent_unit == "week":
        return False

    child_order = _UNIT_ORDER[child_unit]
    parent_order = _UNIT_ORDER[parent_unit]
    if child_order != parent_order:
        return child_order > parent_order

    child_step = int(child_spec["step"])
    parent_step = int(parent_spec["step"])
    return child_step > parent_step and child_step % parent_step == 0


def _validate_stream_lineage(streams: list[Mapping[str, Any]]) -> None:
    stream_ids = [str(stream["stream_id"]) for stream in streams]
    if len(stream_ids) != len(set(stream_ids)):
        raise ValueError("bar stream_id values must be unique")

    streams_by_id = {
        str(stream["stream_id"]): stream
        for stream in streams
    }
    derived_parents: dict[str, str] = {}
    for stream in streams:
        source = stream["source"]
        if source["kind"] != "derived":
            continue
        stream_id = str(stream["stream_id"])
        parent_id = str(source["parent_stream_id"])
        if parent_id not in streams_by_id:
            raise ValueError(
                f"derived stream {stream_id!r} parent_stream_id {parent_id!r} is missing"
            )
        derived_parents[stream_id] = parent_id

    for stream_id in derived_parents:
        seen: set[str] = set()
        current = stream_id
        while current in derived_parents:
            if current in seen:
                raise ValueError(f"derived stream lineage contains a cycle at {current!r}")
            seen.add(current)
            current = derived_parents[current]

    for stream_id, parent_id in derived_parents.items():
        stream = streams_by_id[stream_id]
        parent = streams_by_id[parent_id]
        if not _is_strictly_coarser(stream["bar_spec"], parent["bar_spec"]):
            raise ValueError(
                f"derived stream {stream_id!r} must be strictly coarser than "
                f"parent {parent_id!r}"
            )


def _provider_availability_matches(
    stream_policy: str,
    provider_policy: str,
) -> bool:
    return (
        stream_policy == "bar_close"
        and provider_policy == "bar_close"
    ) or (
        stream_policy == "explicit_timestamp"
        and provider_policy == "provider_reported"
    )


def _capability_matches(
    *,
    stream: Mapping[str, Any],
    capability: Mapping[str, Any],
    session_model: Mapping[str, Any],
    price_model: Mapping[str, Any],
) -> bool:
    spec = stream["bar_spec"]
    timestamp_semantics = stream["timestamp_semantics"]
    provider_timestamp = capability["timestamp_semantics"]
    provider_price = capability["price_policy"]
    return (
        capability["aggregation"] == spec["aggregation"]
        and capability["step"] == spec["step"]
        and capability["unit"] == spec["unit"]
        and spec["price_type"] in capability["price_types"]
        and spec["alignment"] in capability["alignments"]
        and session_model["calendar_id"] in capability["calendar_ids"]
        and session_model["session_scope"] in capability["session_scopes"]
        and _provider_availability_matches(
            str(timestamp_semantics["availability_policy"]),
            str(provider_timestamp["availability"]),
        )
        and price_model["price_basis"] == provider_price["price_basis"]
        and price_model["corporate_action_policy"]
        == provider_price["corporate_action_policy"]
    )


def _validate_provider_capabilities(
    payload: Mapping[str, Any],
    provider_capabilities: Mapping[str, Mapping[str, Any]],
) -> None:
    session_model = payload["session_model"]
    price_model = payload["price_model"]
    for stream in payload["streams"]:
        source = stream["source"]
        if source["kind"] != "external":
            continue
        provider_id = str(source["provider_id"])
        capability = provider_capabilities.get(provider_id)
        if capability is None:
            raise ValueError(f"provider capability for {provider_id!r} is missing")
        _validate_schema(capability, _PROVIDER_CAPABILITY_SCHEMA)
        if capability["provider_id"] != provider_id:
            raise ValueError(
                f"provider capability key {provider_id!r} does not match payload "
                f"provider_id {capability['provider_id']!r}"
            )
        if not any(
            _capability_matches(
                stream=stream,
                capability=timeframe,
                session_model=session_model,
                price_model=price_model,
            )
            for timeframe in capability["supported_timeframes"]
        ):
            raise ValueError(
                f"external stream {stream['stream_id']!r} has no exact capability "
                f"for provider {provider_id!r}"
            )


def validate_bar_time_contract(
    payload: Mapping[str, Any],
    provider_capabilities: Mapping[str, Mapping[str, Any]] | None = None,
) -> None:
    """Validate the Phase 0 bar/time contract without changing runtime behavior."""

    _validate_schema(payload, _BAR_TIME_SCHEMA)
    streams = payload["streams"]
    _validate_stream_lineage(streams)
    if provider_capabilities is not None:
        _validate_provider_capabilities(payload, provider_capabilities)
