"""Shared OP_SPEC field names for Strategy Building Blocks."""

from __future__ import annotations


OP_SPEC_REQUIRED_FIELDS = (
    "canonical_id",
    "aliases",
    "block_kind",
    "category",
    "spec_version",
    "status",
    "stability",
    "usage_sites",
    "params_schema",
    "optimizable_params",
    "input_shape",
    "output_type",
    "output_shape",
    "temporal_metadata",
    "data_alignment",
    "leakage_flags",
    "wfa_safety",
    "implementation",
    "evidence_paths",
    "audit",
    "cache",
    "deprecation",
    "docs",
    "unsupported_message",
    "safety_warnings",
)

OP_SPEC_SCHEMA_PATH = "backtester/contracts/ops/op-spec-v1.schema.json"

__all__ = ["OP_SPEC_REQUIRED_FIELDS", "OP_SPEC_SCHEMA_PATH"]
