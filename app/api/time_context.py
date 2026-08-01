"""Project the typed bar-time contract without changing its semantics."""

from __future__ import annotations

import copy
from typing import Any, Dict


def bar_spec_label(bar_spec: Any) -> str:
    if not isinstance(bar_spec, dict):
        return ""
    step = bar_spec.get("step")
    unit = str(bar_spec.get("unit") or "")
    if not isinstance(step, int) or isinstance(step, bool) or step <= 0 or not unit:
        return ""
    unit_label = unit if step == 1 else f"{unit}s"
    return f"{step} {unit_label}"


def project_time_context(data: Any) -> Dict[str, Any]:
    """Return the bound execution/decision streams and their exact time models."""

    if not isinstance(data, dict):
        return {}
    bar_time = data.get("bar_time")
    binding = data.get("stream_binding")
    if not isinstance(bar_time, dict) or not isinstance(binding, dict):
        return {}
    streams = bar_time.get("streams")
    if not isinstance(streams, list):
        return {}
    streams_by_id = {
        str(stream.get("stream_id") or ""): stream
        for stream in streams
        if isinstance(stream, dict) and stream.get("stream_id")
    }
    execution_stream_id = str(binding.get("execution_stream_id") or "")
    decision_stream_id = str(binding.get("decision_stream_id") or "")
    execution_stream = streams_by_id.get(execution_stream_id)
    decision_stream = streams_by_id.get(decision_stream_id)
    if not isinstance(execution_stream, dict) or not isinstance(decision_stream, dict):
        return {}

    def stream_context(stream: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "stream_id": str(stream.get("stream_id") or ""),
            "role": str(stream.get("role") or ""),
            "source": copy.deepcopy(stream.get("source") or {}),
            "bar_spec": copy.deepcopy(stream.get("bar_spec") or {}),
            "timestamp_semantics": copy.deepcopy(
                stream.get("timestamp_semantics") or {}
            ),
        }

    return {
        "execution": stream_context(execution_stream),
        "decision": stream_context(decision_stream),
        "session": copy.deepcopy(bar_time.get("session_model") or {}),
        "timestamp": copy.deepcopy(bar_time.get("timestamp_model") or {}),
    }


def strategy_time_summary(data: Any) -> Dict[str, Any]:
    context = project_time_context(data)
    if not context:
        return {
            "frequency_label": "",
            "decision_frequency_label": "",
            "calendar_id": "",
            "timezone": "",
            "time_context": {},
        }
    execution = context["execution"]
    decision = context["decision"]
    session = context["session"]
    return {
        "execution_stream_id": execution["stream_id"],
        "execution_bar_spec": copy.deepcopy(execution["bar_spec"]),
        "decision_stream_id": decision["stream_id"],
        "decision_bar_spec": copy.deepcopy(decision["bar_spec"]),
        "frequency_label": bar_spec_label(execution["bar_spec"]),
        "decision_frequency_label": bar_spec_label(decision["bar_spec"]),
        "calendar_id": str(session.get("calendar_id") or ""),
        "timezone": str(session.get("timezone") or ""),
        "time_context": context,
    }
