from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd


DEFAULT_FLOAT_TOLERANCE = 1e-9
DEFAULT_IGNORE_KEYS = {"timestamp", "uuid", "backtest_id", "Backtest_id"}


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: Any) -> Path:
    ensure_directory(path.parent)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def logicalize_path(path: Path | str, roots: Mapping[str, Path | str]) -> str:
    candidate = Path(path).resolve()
    for label, root in roots.items():
        root_path = Path(root).resolve()
        try:
            relative = candidate.relative_to(root_path)
            return f"{label}/{relative.as_posix()}"
        except ValueError:
            continue
    return candidate.as_posix()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dataframe_signature(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "dtypes": {column: str(dtype) for column, dtype in df.dtypes.items()},
    }


def normalize_records(
    records: Sequence[Mapping[str, Any]],
    *,
    ignore_keys: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    ignore = set(DEFAULT_IGNORE_KEYS)
    if ignore_keys:
        ignore.update(ignore_keys)

    normalized: List[Dict[str, Any]] = []
    for row in records:
        item: Dict[str, Any] = {}
        for key, value in row.items():
            if key in ignore:
                continue
            if pd.isna(value):
                item[key] = None
            elif isinstance(value, Path):
                item[key] = value.as_posix()
            else:
                item[key] = value
        normalized.append(item)
    return normalized


def compare_dataframes(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    sort_by: Optional[Sequence[str]] = None,
    float_tolerance: float = DEFAULT_FLOAT_TOLERANCE,
    ignore_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    ignore = set(ignore_columns or [])

    left_df = left.copy()
    right_df = right.copy()

    left_df = left_df[[column for column in left_df.columns if column not in ignore]]
    right_df = right_df[[column for column in right_df.columns if column not in ignore]]

    if sort_by:
        usable_sort = [column for column in sort_by if column in left_df.columns and column in right_df.columns]
        if usable_sort:
            left_df = left_df.sort_values(usable_sort).reset_index(drop=True)
            right_df = right_df.sort_values(usable_sort).reset_index(drop=True)

    left_signature = dataframe_signature(left_df)
    right_signature = dataframe_signature(right_df)

    result: Dict[str, Any] = {
        "pass": True,
        "left": left_signature,
        "right": right_signature,
        "differences": [],
    }

    if list(left_df.columns) != list(right_df.columns):
        result["pass"] = False
        result["differences"].append(
            {
                "kind": "columns",
                "left": list(left_df.columns),
                "right": list(right_df.columns),
            }
        )
        return result

    if len(left_df) != len(right_df):
        result["pass"] = False
        result["differences"].append(
            {
                "kind": "row_count",
                "left": int(len(left_df)),
                "right": int(len(right_df)),
            }
        )

    if left_df.empty and right_df.empty:
        return result

    sample_limit = min(len(left_df), len(right_df))
    for column in left_df.columns:
        left_series = left_df[column].iloc[:sample_limit]
        right_series = right_df[column].iloc[:sample_limit]

        if pd.api.types.is_numeric_dtype(left_series) and pd.api.types.is_numeric_dtype(right_series):
            left_numeric = left_series.astype(float)
            right_numeric = right_series.astype(float)
            matching_missing = left_numeric.isna() & right_numeric.isna()
            one_sided_missing = left_numeric.isna() ^ right_numeric.isna()
            delta = (left_numeric - right_numeric).abs()
            failing = one_sided_missing | (~matching_missing & ~delta.le(float_tolerance))
            if failing.any():
                position = next(index for index, value in enumerate(failing) if value)
                left_value = left_numeric.iloc[position]
                right_value = right_numeric.iloc[position]
                delta_value = delta.iloc[position]
                result["pass"] = False
                result["differences"].append(
                    {
                        "kind": "numeric_value",
                        "column": column,
                        "index": position,
                        "left": None if pd.isna(left_value) else float(left_value),
                        "right": None if pd.isna(right_value) else float(right_value),
                        "delta": None if pd.isna(delta_value) else float(delta_value),
                    }
                )
        else:
            left_values = normalize_records(left_series.to_frame().to_dict("records"))
            right_values = normalize_records(right_series.to_frame().to_dict("records"))
            if left_values != right_values:
                mismatch_index = next(
                    index
                    for index, (left_value, right_value) in enumerate(zip(left_values, right_values))
                    if left_value != right_value
                )
                result["pass"] = False
                result["differences"].append(
                    {
                        "kind": "value",
                        "column": column,
                        "index": mismatch_index,
                        "left": left_values[mismatch_index],
                        "right": right_values[mismatch_index],
                    }
                )

    return result
