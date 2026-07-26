"""Content-addressed MarketDataBundle runtime boundary.

Provider adapters may use pandas while fetching data, but the backtester only
receives this immutable manifest handle. Every table is persisted as Parquet
and verified again when it crosses into the engine.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

import pandas as pd
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]


SCHEMA_VERSION = "market_data_bundle.v1"
CONTRACT_ID = "lo2cin4bt.market_data_bundle.v1"
TIME_COLUMN = "Time"
_BARS = {"open", "high", "low", "close", "volume"}
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "backtester"
    / "contracts"
    / "runtime"
    / "market-data-bundle-v1.schema.json"
)


@dataclass(frozen=True)
class MarketDataBundle:
    """Immutable handle to one validated, file-backed market-data bundle."""

    manifest_path: Path

    @classmethod
    def open(cls, manifest_path: Path | str) -> "MarketDataBundle":
        bundle = cls(Path(manifest_path).resolve())
        bundle.read_manifest()
        return bundle

    @property
    def bundle_id(self) -> str:
        return str(self.read_manifest()["bundle_id"])

    @property
    def content_hash(self) -> str:
        return str(self.read_manifest()["content_hash"])

    def read_manifest(self) -> Dict[str, Any]:
        try:
            payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid MarketDataBundle manifest: {self.manifest_path}") from exc
        validate_market_data_bundle_manifest(payload, manifest_path=self.manifest_path)
        return payload

    def load_frames(self) -> Dict[str, pd.DataFrame]:
        manifest = self.read_manifest()
        bundle_dir = self.manifest_path.parent.resolve()
        index_kind = str(manifest["time_semantics"]["index_kind"])
        frames: Dict[str, pd.DataFrame] = {}
        for name, table in manifest["tables"].items():
            raw_path = table.get("path")
            if not isinstance(raw_path, str) or not raw_path.strip():
                raise ValueError(f"MarketDataBundle table {name} is not file-backed")
            path = Path(raw_path).resolve()
            if path.parent != bundle_dir:
                raise ValueError(f"MarketDataBundle table path escapes bundle directory: {name}")
            if not path.is_file():
                raise ValueError(f"MarketDataBundle table is missing: {name}")
            frame = _normalize_frame(pd.read_parquet(path), name=name, index_kind=index_kind)
            if _frame_content_hash(frame) != table["content_hash"]:
                raise ValueError(f"MarketDataBundle table content hash mismatch: {name}")
            if len(frame.index) != int(table["row_count"]):
                raise ValueError(f"MarketDataBundle table row_count mismatch: {name}")
            if [str(column) for column in frame.columns] != list(table["columns"]):
                raise ValueError(f"MarketDataBundle table columns mismatch: {name}")
            frames[str(name)] = frame
        if "close" not in frames:
            raise ValueError("MarketDataBundle requires a close table")
        return frames

    def primary_frame(self) -> pd.DataFrame:
        return self.load_frames()["close"]

    def validate_against_engine_request(self, engine_request: Mapping[str, Any]) -> None:
        manifest = self.read_manifest()
        requirements = dict(engine_request.get("data_requirements") or {})
        if requirements.get("bundle_schema_version") != SCHEMA_VERSION:
            raise ValueError("EngineRequest does not require MarketDataBundle.v1")
        expected_symbols = [str(item) for item in requirements.get("symbols") or []]
        if manifest["symbols"] != expected_symbols:
            raise ValueError("MarketDataBundle symbols do not match EngineRequest")
        for field in ("frequency", "calendar", "timezone"):
            expected = str(requirements.get(field) or "")
            if expected and str(manifest.get(field) or "") != expected:
                raise ValueError(f"MarketDataBundle {field} does not match EngineRequest")


def build_market_data_bundle(
    frames: Mapping[str, pd.DataFrame],
    *,
    spec: Mapping[str, Any],
    output_root: Path | str,
) -> MarketDataBundle:
    """Persist normalized provider frames and return their immutable handle."""

    if not isinstance(frames, Mapping) or not frames:
        raise ValueError("MarketDataBundle requires non-empty market frames")
    index_kind = str(spec.get("index_kind") or "session_label")
    if index_kind not in {"session_label", "event_timestamp"}:
        raise ValueError(f"Unknown MarketDataBundle index_kind: {index_kind}")
    normalized: Dict[str, pd.DataFrame] = {}
    for raw_name, raw_frame in frames.items():
        name = str(raw_name).strip().lower()
        if not _TABLE_NAME.fullmatch(name):
            raise ValueError(f"Invalid MarketDataBundle table name: {raw_name}")
        if not isinstance(raw_frame, pd.DataFrame) or raw_frame.empty:
            raise ValueError(f"MarketDataBundle table {name} must be a non-empty DataFrame")
        normalized[name] = _normalize_frame(raw_frame, name=name, index_kind=index_kind)
    if "close" not in normalized:
        raise ValueError("MarketDataBundle requires a close table")
    normalized, removed_sessions = _prepare_runtime_bar_tables(normalized)

    close = normalized["close"]
    symbols = [str(item).strip() for item in spec.get("symbols") or close.columns]
    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("MarketDataBundle symbols must be non-empty and unique")
    if [str(column) for column in close.columns] != symbols:
        raise ValueError("MarketDataBundle close columns must match configured symbols in order")

    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".mdb-staging-", dir=root))
    try:
        table_payloads: Dict[str, Dict[str, Any]] = {}
        for name in sorted(normalized):
            path = staging / f"{name}.parquet"
            frame = normalized[name].copy()
            frame.index.name = TIME_COLUMN
            frame.to_parquet(path, index=True)
            canonical = _normalize_frame(pd.read_parquet(path), name=name, index_kind=index_kind)
            table_payloads[name] = {
                "role": (
                    "bars"
                    if name in _BARS
                    else "benchmarks"
                    if name == "benchmark_close"
                    else "features"
                ),
                "transport": "parquet",
                "path": None,
                "content_hash": _frame_content_hash(canonical),
                "columns": [str(column) for column in canonical.columns],
                "row_count": len(canonical.index),
            }

        frequency = str(spec.get("frequency") or spec.get("interval") or "1D")
        provider = str(spec.get("provider") or spec.get("source") or "file")
        time_semantics = {
            "index_kind": index_kind,
            "event_time_column": TIME_COLUMN,
            "available_time_column": spec.get("available_time_column"),
            "availability_policy": str(spec.get("availability_policy") or "bar_close"),
            "ordering": "event_time_then_table_name",
        }
        quality = {
            "missing_value_policy": str(
                spec.get("missing_value_policy")
                or ("drop_rows" if removed_sessions else "preserve")
            ),
            "duplicate_time_policy": "fail",
            "out_of_order_policy": str(spec.get("out_of_order_policy") or "sort_then_validate"),
            "stale_value_policy": str(spec.get("stale_value_policy") or "preserve"),
            "warnings": [
                *[str(item) for item in spec.get("quality_warnings") or []],
                *(
                    [f"removed {removed_sessions} provider rows without any tradable prices"]
                    if removed_sessions
                    else []
                ),
            ],
        }
        lineage = {
            "provider": provider,
            "source_hashes": {
                name: table["content_hash"] for name, table in sorted(table_payloads.items())
            },
            "point_in_time": bool(spec.get("point_in_time", False)),
            "adjustment_policy": _adjustment_policy(spec, provider),
        }
        base_manifest: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "contract_id": CONTRACT_ID,
            "bundle_id": "",
            "content_hash": "",
            "symbols": symbols,
            "frequency": frequency,
            "calendar": str(spec.get("calendar") or ""),
            "timezone": str(spec.get("timezone") or ""),
            "time_range": {
                "start": _timestamp_text(close.index[0]),
                "end": _timestamp_text(close.index[-1]),
            },
            "row_count": len(close.index),
            "time_column": TIME_COLUMN,
            "time_semantics": time_semantics,
            "quality": quality,
            "tables": table_payloads,
            "lineage": lineage,
        }
        content_hash = market_data_bundle_content_hash(base_manifest)
        bundle_id = f"mdb-{content_hash[:16]}"
        final_dir = root / bundle_id
        for name, table in table_payloads.items():
            table["path"] = str((final_dir / f"{name}.parquet").resolve())
        base_manifest["bundle_id"] = bundle_id
        base_manifest["content_hash"] = content_hash
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(
            json.dumps(base_manifest, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        validate_market_data_bundle_manifest(base_manifest, manifest_path=manifest_path)

        if final_dir.exists():
            existing = MarketDataBundle.open(final_dir / "manifest.json")
            if existing.content_hash != content_hash:
                raise ValueError(f"MarketDataBundle cache collision: {bundle_id}")
            return existing
        staging.replace(final_dir)
        return MarketDataBundle.open(final_dir / "manifest.json")
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def validate_market_data_bundle_manifest(
    manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None = None,
) -> None:
    payload = dict(manifest or {})
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
    expected_hash = market_data_bundle_content_hash(payload)
    if payload.get("content_hash") != expected_hash:
        raise ValueError("MarketDataBundle content_hash does not match canonical manifest")
    if payload.get("bundle_id") != f"mdb-{expected_hash[:16]}":
        raise ValueError("MarketDataBundle bundle_id does not match content_hash")
    if manifest_path is not None and Path(manifest_path).name != "manifest.json":
        raise ValueError("MarketDataBundle manifest filename must be manifest.json")


def market_data_bundle_content_hash(manifest: Mapping[str, Any]) -> str:
    payload = dict(manifest or {})
    tables = {
        str(name): {
            key: value
            for key, value in dict(table).items()
            if key != "path"
        }
        for name, table in sorted(dict(payload.get("tables") or {}).items())
    }
    canonical = {
        key: value
        for key, value in payload.items()
        if key not in {"bundle_id", "content_hash", "tables"}
    }
    canonical["tables"] = tables
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_frame(frame: pd.DataFrame, *, name: str, index_kind: str) -> pd.DataFrame:
    out = frame.copy()
    index = pd.to_datetime(out.index, errors="coerce")
    if index.isna().any():
        raise ValueError(f"MarketDataBundle table {name} contains invalid timestamps")
    if getattr(index, "tz", None) is not None:
        index = index.tz_convert("UTC")
        if index_kind == "session_label":
            index = index.tz_localize(None)
    if index_kind == "session_label":
        index = pd.DatetimeIndex(index).normalize()
    out.index = pd.DatetimeIndex(index, name=TIME_COLUMN)
    out.columns = [str(column) for column in out.columns]
    if out.index.duplicated().any():
        raise ValueError(f"MarketDataBundle table {name} contains duplicate timestamps")
    out = out.sort_index()
    return out


def _prepare_runtime_bar_tables(
    frames: Dict[str, pd.DataFrame],
) -> tuple[Dict[str, pd.DataFrame], int]:
    """Remove provider placeholder sessions and enforce finite runtime prices."""

    prepared = {name: frame.copy() for name, frame in frames.items()}
    close = prepared["close"].apply(pd.to_numeric, errors="coerce")
    valid_close = close.gt(0.0) & close.notna()
    empty_sessions = ~valid_close.any(axis=1)
    removed_sessions = int(empty_sessions.sum())
    if removed_sessions:
        removed_index = close.index[empty_sessions]
        prepared = {
            name: frame.loc[~frame.index.isin(removed_index)].copy()
            for name, frame in prepared.items()
        }
        close = prepared["close"].apply(pd.to_numeric, errors="coerce")
        valid_close = close.gt(0.0) & close.notna()
    if close.empty:
        raise ValueError("MarketDataBundle contains no tradable close sessions")

    invalid_close = ~valid_close
    if invalid_close.any().any():
        invalid_symbols = [
            str(symbol) for symbol in close.columns if bool(invalid_close[symbol].any())
        ]
        raise ValueError(
            "MarketDataBundle close table contains partial invalid prices for: "
            + ", ".join(invalid_symbols)
        )

    close_index = prepared["close"].index
    for name in sorted(_BARS & prepared.keys()):
        frame = prepared[name]
        if not frame.index.equals(close_index):
            raise ValueError(
                f"MarketDataBundle bar table {name} timestamps do not match close"
            )
        if name == "volume":
            continue
        numeric = frame.apply(pd.to_numeric, errors="coerce")
        invalid = numeric.isna() | numeric.le(0.0)
        if invalid.any().any():
            invalid_symbols = [
                str(symbol) for symbol in numeric.columns if bool(invalid[symbol].any())
            ]
            raise ValueError(
                f"MarketDataBundle {name} table contains invalid prices for: "
                + ", ".join(invalid_symbols)
            )
    return prepared, removed_sessions


def _frame_content_hash(frame: pd.DataFrame) -> str:
    metadata = json.dumps(
        {
            "columns": [str(column) for column in frame.columns],
            "dtypes": [str(dtype) for dtype in frame.dtypes],
            "index_name": str(frame.index.name or TIME_COLUMN),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    values = pd.util.hash_pandas_object(frame, index=True, categorize=False).to_numpy(
        dtype="uint64",
        copy=False,
    )
    digest = hashlib.sha256(metadata)
    digest.update(values.tobytes())
    return digest.hexdigest()


def _adjustment_policy(spec: Mapping[str, Any], provider: str) -> str:
    explicit = str(spec.get("adjustment_policy") or "").strip()
    if explicit:
        return explicit
    if provider.lower() in {"yfinance", "yf"}:
        return "split_dividend_adjusted"
    return "unspecified"


def _timestamp_text(value: Any) -> str:
    return pd.Timestamp(value).isoformat()
