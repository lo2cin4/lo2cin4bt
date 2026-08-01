"""Exact provider bar timestamp normalization for session-aligned bars."""

from __future__ import annotations

from typing import Any

import exchange_calendars as xcals  # type: ignore[import-untyped]
import pandas as pd


def typed_intraday_duration(bar_spec: Any) -> pd.Timedelta:
    if (
        not isinstance(bar_spec, dict)
        or bar_spec.get("aggregation") != "time"
        or bar_spec.get("unit") not in {"minute", "hour"}
    ):
        raise ValueError("Provider intraday bar_spec requires minute or hour time bars")
    step = bar_spec.get("step")
    if not isinstance(step, int) or isinstance(step, bool) or step < 1:
        raise ValueError("Provider intraday BarSpec step must be positive")
    if bar_spec["unit"] == "minute":
        return pd.Timedelta(step, unit="minute")
    return pd.Timedelta(step, unit="hour")


def normalize_native_bar_open_keys(
    row_keys: pd.DatetimeIndex,
    *,
    bar_spec: Any,
    timestamp_convention: str,
    calendar_id: str,
) -> pd.DatetimeIndex:
    """Convert native bar-open keys without extending a partial bar past close."""

    if timestamp_convention not in {"bar_open", "bar_close"}:
        raise ValueError("Provider requires timestamp_convention=bar_open or bar_close")
    if row_keys.tz is None:
        raise ValueError("Provider intraday row keys must be timezone-aware")

    duration = typed_intraday_duration(bar_spec)
    if calendar_id != "XNYS":
        return row_keys if timestamp_convention == "bar_open" else row_keys + duration

    opens, closes, _, _ = resolve_xnys_session_bar_bounds(
        row_keys,
        bar_spec=bar_spec,
        timestamp_convention="bar_open",
    )
    return opens if timestamp_convention == "bar_open" else closes


def resolve_xnys_session_bar_bounds(
    row_keys: pd.DatetimeIndex,
    *,
    bar_spec: Any,
    timestamp_convention: str,
) -> tuple[
    pd.DatetimeIndex,
    pd.DatetimeIndex,
    list[str],
    dict[str, tuple[pd.Timestamp, pd.Timestamp]],
]:
    """Resolve exact XNYS session-open-aligned bar bounds from one row-key convention."""

    if timestamp_convention not in {"bar_open", "bar_close"}:
        raise ValueError("Provider requires timestamp_convention=bar_open or bar_close")
    if row_keys.empty:
        raise ValueError("Provider intraday row keys must not be empty")
    if row_keys.tz is None:
        raise ValueError("Provider intraday row keys must be timezone-aware")

    duration = typed_intraday_duration(bar_spec)
    calendar = xcals.get_calendar("XNYS")
    utc_keys = row_keys.tz_convert("UTC")
    first = utc_keys.min().normalize() - pd.Timedelta(days=7)
    last = utc_keys.max().normalize() + pd.Timedelta(days=7)
    schedule = calendar.schedule.loc[
        first.tz_localize(None) : last.tz_localize(None)
    ]
    opens: list[pd.Timestamp] = []
    closes: list[pd.Timestamp] = []
    labels: list[str] = []
    windows: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    for row_key in utc_keys:
        if timestamp_convention == "bar_open":
            matching = schedule[
                (schedule["open"] <= row_key) & (row_key < schedule["close"])
            ]
        else:
            matching = schedule[
                (schedule["open"] < row_key) & (row_key <= schedule["close"])
            ]
        if len(matching) != 1:
            raise ValueError(
                f"Provider timestamp {row_key} is not inside one XNYS session"
            )
        session_open = pd.Timestamp(matching.iloc[0]["open"])
        session_close = pd.Timestamp(matching.iloc[0]["close"])
        if timestamp_convention == "bar_open":
            opened = row_key
            closed = min(opened + duration, session_close)
        else:
            closed = row_key
            if closed == session_close:
                elapsed = session_close - session_open
                bucket_index = int(
                    (elapsed - pd.Timedelta(1, unit="ns")) // duration
                )
                opened = session_open + bucket_index * duration
            else:
                opened = closed - duration
        if (
            opened < session_open
            or closed > session_close
            or (opened - session_open) % duration != pd.Timedelta(0)
            or min(opened + duration, session_close) != closed
        ):
            raise ValueError(
                f"Provider bar {opened}..{closed} is not an exact "
                "session-open-aligned XNYS bar"
            )
        opens.append(opened)
        closes.append(closed)
        label = pd.Timestamp(matching.index[0]).strftime("%Y-%m-%d")
        labels.append(label)
        windows.setdefault(label, (session_open, session_close))
    return (
        pd.DatetimeIndex(opens, name=row_keys.name),
        pd.DatetimeIndex(closes, name=row_keys.name),
        labels,
        windows,
    )
