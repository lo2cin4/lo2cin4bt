"""Shared timeframe helpers for session-level portfolio runtime boundaries."""

from __future__ import annotations

import re
from typing import Any


def is_subdaily_timeframe(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    compact = text.replace(" ", "")
    if compact.isdigit():
        try:
            return int(compact) < 86400
        except ValueError:
            return False
    daily_aliases = {
        "d",
        "1d",
        "day",
        "1day",
        "daily",
        "b",
        "1b",
        "businessday",
        "business_day",
        "1businessday",
        "1business_day",
        "w",
        "1w",
        "week",
        "1week",
        "weekly",
        "mth",
        "1mth",
        "month",
        "1month",
        "monthly",
    }
    if compact in daily_aliases:
        return False
    shorthand = re.fullmatch(r"(\d+)([a-z]+)", compact)
    if shorthand:
        unit = shorthand.group(2)
        if unit in {"m", "min", "mins", "minute", "minutes", "s", "sec", "secs", "second", "seconds"}:
            return True
        if unit in {"h", "hr", "hrs", "hour", "hours"}:
            return True
        if unit in {"d", "day", "days", "w", "wk", "wks", "week", "weeks", "mo", "mon", "month", "months"}:
            return False
    intraday_tokens = (
        "min",
        "minute",
        "minutes",
        "h",
        "hr",
        "hour",
        "hours",
        "sec",
        "second",
        "seconds",
    )
    return any(token in compact for token in intraday_tokens)
