"""Historical universe constituents validation helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from utils.path_resolver import resolve_input_path


CONSTITUENTS_PATH_KEYS = (
    "historical_constituents_path",
    "universe_constituents_path",
    "constituents_path",
    "source_path",
    "universe_path",
)
CONSTITUENTS_HASH_KEYS = (
    "historical_constituents_hash",
    "universe_constituents_hash",
    "constituents_hash",
)
CONSTITUENTS_SOURCE_TYPES = {
    "artifact_snapshot",
    "historical_constituents",
    "historical_universe_constituents",
    "point_in_time_provider_snapshot",
    "point_in_time_snapshot",
    "provider_snapshot",
    "pit_universe",
}


def constituents_source_ref(universe: Dict[str, Any]) -> Optional[str]:
    for key in CONSTITUENTS_PATH_KEYS + ("source_ref", "source"):
        value = universe.get(key)
        if value not in (None, "", []):
            return str(value)
    return None


def declared_constituents_hash(universe: Dict[str, Any]) -> Optional[str]:
    for key in CONSTITUENTS_HASH_KEYS:
        value = universe.get(key)
        if value not in (None, "", []):
            return str(value)
    return None


def constituents_path_declared(universe: Dict[str, Any]) -> bool:
    return any(universe.get(key) not in (None, "", []) for key in CONSTITUENTS_PATH_KEYS)


def validate_historical_universe_constituents(
    *,
    universe: Dict[str, Any],
    configured_symbols: Iterable[str],
    as_of_date: Optional[str],
    repo_root: Optional[Path] = None,
    config_file_path: Optional[str] = None,
) -> Dict[str, Any]:
    raw_path = next(
        (str(universe.get(key)) for key in CONSTITUENTS_PATH_KEYS if universe.get(key) not in (None, "", [])),
        "",
    )
    expected_hash = declared_constituents_hash(universe)
    expected_symbols = sorted({str(symbol) for symbol in configured_symbols if str(symbol)})
    out: Dict[str, Any] = {
        "schema_version": "historical_universe_constituents_validation.v1",
        "status": "not_applicable",
        "path": raw_path or None,
        "content_hash": None,
        "expected_hash": expected_hash,
        "hash_matches": None,
        "row_count": 0,
        "symbol_column": None,
        "date_columns": [],
        "date_semantics": None,
        "configured_symbols": expected_symbols,
        "symbols_in_file_count": 0,
        "missing_configured_symbols": [],
        "duplicate_row_count": 0,
        "as_of_date": as_of_date,
        "as_of_covered": None,
        "warnings": [],
        "errors": [],
    }
    if not raw_path:
        return out

    repo_root = repo_root or Path.cwd()
    resolved = resolve_input_path(
        raw_path,
        repo_root=repo_root,
        config_file_path=config_file_path,
    ).path
    out["path"] = str(resolved)
    if not resolved.is_file():
        out["status"] = "missing"
        out["errors"].append("historical_constituents_file_missing")
        return out

    content_hash = _hash_file(resolved)
    out["content_hash"] = content_hash
    if expected_hash:
        normalized_expected = expected_hash if expected_hash.startswith("sha256:") else f"sha256:{expected_hash}"
        out["hash_matches"] = normalized_expected == content_hash
        if not out["hash_matches"]:
            out["errors"].append("historical_constituents_hash_mismatch")

    try:
        frame = _read_constituents_frame(resolved)
    except Exception as exc:  # pragma: no cover - defensive parser guard
        out["status"] = "invalid"
        out["errors"].append(f"historical_constituents_unreadable:{type(exc).__name__}")
        return out

    out["row_count"] = int(len(frame))
    if frame.empty:
        out["status"] = "invalid"
        out["errors"].append("historical_constituents_empty")
        return out

    columns = {str(column).strip().lower(): column for column in frame.columns}
    symbol_column = _first_existing(columns, ("symbol", "ticker", "asset", "asset_symbol"))
    if symbol_column is None:
        out["status"] = "invalid"
        out["errors"].append("historical_constituents_symbol_column_missing")
        return out
    out["symbol_column"] = str(symbol_column)

    symbols_in_file = sorted(
        {
            str(value).strip()
            for value in frame[symbol_column].dropna().tolist()
            if str(value).strip()
        }
    )
    out["symbols_in_file_count"] = len(symbols_in_file)
    missing = sorted(set(expected_symbols) - set(symbols_in_file))
    out["missing_configured_symbols"] = missing
    if missing:
        out["errors"].append("historical_constituents_missing_configured_symbols")

    asof_column = _first_existing(columns, ("as_of_date", "as_of", "date", "snapshot_date", "constituents_date"))
    start_column = _first_existing(columns, ("effective_start", "start_date", "start", "valid_from", "from_date"))
    end_column = _first_existing(columns, ("effective_end", "end_date", "end", "valid_to", "to_date"))
    has_effective_interval = start_column is not None or end_column is not None
    exact_asof_required = asof_column is not None and not has_effective_interval
    date_columns = [str(column) for column in (asof_column, start_column, end_column) if column is not None]
    out["date_columns"] = date_columns
    if has_effective_interval:
        out["date_semantics"] = "effective_interval"
    elif asof_column is not None:
        out["date_semantics"] = "exact_as_of_snapshot"
    if not date_columns:
        out["errors"].append("historical_constituents_date_evidence_missing")

    duplicate_subset = [symbol_column] + [column for column in (asof_column, start_column, end_column) if column is not None]
    out["duplicate_row_count"] = int(frame.duplicated(subset=duplicate_subset).sum()) if duplicate_subset else 0
    if out["duplicate_row_count"]:
        out["warnings"].append("historical_constituents_duplicate_rows")

    if as_of_date and date_columns:
        as_of = pd.Timestamp(as_of_date)
        covered = _covered_symbols_at_as_of(
            frame=frame,
            symbol_column=symbol_column,
            as_of=as_of,
            asof_column=asof_column,
            start_column=start_column,
            end_column=end_column,
            exact_asof_required=exact_asof_required,
        )
        missing_at_as_of = sorted(set(expected_symbols) - covered)
        out["as_of_covered"] = not missing_at_as_of
        if missing_at_as_of:
            out["errors"].append("historical_constituents_as_of_coverage_missing")
            out["missing_configured_symbols_at_as_of"] = missing_at_as_of
            if exact_asof_required:
                out["errors"].append("historical_constituents_exact_as_of_snapshot_missing")
    elif date_columns:
        out["warnings"].append("historical_constituents_as_of_not_declared")

    out["status"] = "valid" if not out["errors"] else "invalid"
    return out


def _read_constituents_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, lines=suffix == ".jsonl")
    return pd.read_csv(path)


def _first_existing(columns: Dict[str, Any], candidates: tuple[str, ...]) -> Any:
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    return None


def _covered_symbols_at_as_of(
    *,
    frame: pd.DataFrame,
    symbol_column: Any,
    as_of: pd.Timestamp,
    asof_column: Any,
    start_column: Any,
    end_column: Any,
    exact_asof_required: bool = False,
) -> set[str]:
    mask = pd.Series(True, index=frame.index)
    if asof_column is not None:
        values = pd.to_datetime(frame[asof_column], errors="coerce")
        if exact_asof_required:
            mask &= values.notna() & (values.dt.normalize() == as_of.normalize())
        else:
            mask &= values.notna() & (values <= as_of)
    if start_column is not None:
        starts = pd.to_datetime(frame[start_column], errors="coerce")
        mask &= starts.notna() & (starts <= as_of)
    if end_column is not None:
        ends = pd.to_datetime(frame[end_column], errors="coerce")
        mask &= ends.isna() | (ends >= as_of)
    return {
        str(value).strip()
        for value in frame.loc[mask, symbol_column].dropna().tolist()
        if str(value).strip()
    }


def _hash_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
