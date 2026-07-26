"""Filename helpers for Windows-safe generated artifacts."""

from __future__ import annotations

import hashlib
import re
from typing import Any


def safe_filename_part(value: Any, *, fallback: str = "artifact") -> str:
    """Return a portable filename component without changing readable short names."""
    text = str(value or "").strip()
    text = text.replace("+", "-")
    part = re.sub(r"[^0-9A-Za-z._-]+", "_", text)
    part = re.sub(r"_+", "_", part).strip("_.-")
    return part or fallback


def bounded_filename_part(value: Any, *, max_length: int = 96, fallback: str = "artifact") -> str:
    """Bound one filename component and append a hash only when truncation is needed."""
    part = safe_filename_part(value, fallback=fallback)
    if max_length <= 0 or len(part) <= max_length:
        return part
    digest = hashlib.sha1(part.encode("utf-8", errors="ignore")).hexdigest()[:10]
    if max_length <= len(digest) + 1:
        return digest[:max_length]
    prefix = part[: max_length - len(digest) - 1].rstrip("_.-")
    return f"{prefix}_{digest}" if prefix else digest[:max_length]


def bounded_filename_stem(value: Any, *, max_length: int = 96, fallback: str = "artifact") -> str:
    """Alias for output stems; kept semantic at call sites."""
    return bounded_filename_part(value, max_length=max_length, fallback=fallback)
