"""Core Strategy Building Blocks mirrored from current runtime code."""

from __future__ import annotations

from typing import Any, Dict, List

from .registry import core_op_specs


CORE_OP_SPECS: List[Dict[str, Any]] = core_op_specs()

__all__ = ["CORE_OP_SPECS", "core_op_specs"]
