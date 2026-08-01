"""Content-addressed MarketDataBundle v2 runtime boundary.

Providers hand this module already-typed external bars.  The bundle persists
the provider's price tables and its explicit execution timeline; it does not
infer timestamps, aggregate bars, or translate legacy frequency fields.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

import pandas as pd
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]


SCHEMA_VERSION = "market_data_bundle.v2"
CONTRACT_ID = "lo2cin4bt.market_data_bundle.v2"
TIME_COLUMN = "Time"
TIMELINE_TABLE = "execution_timeline"
TIMELINE_COLUMNS = (
    "external_execution_sequence",
    "bar_open_timestamp",
    "bar_close_timestamp",
    "available_timestamp",
    "session_label",
)
_TIMESTAMP_COLUMNS = (
    "bar_open_timestamp",
    "bar_close_timestamp",
    "available_timestamp",
)
_BARS = ("open", "high", "low", "close", "volume")
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "backtester"
    / "contracts"
    / "runtime"
    / "market-data-bundle-v2.schema.json"
)


@dataclass(frozen=True)
class ExecutionStreamSpec:
    """Typed physical contract for the single external execution stream."""

    stream_id: str
    provider_id: str
    session_scope: str
    row_key_kind: str
    bar_spec: Mapping[str, Any]
    timestamp_semantics: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ExecutionStreamSpec":
        payload = dict(value or {})
        expected = {
            "stream_id",
            "role",
            "source",
            "session_scope",
            "row_key_kind",
            "bar_spec",
            "timestamp_semantics",
            "timeline_table",
            "ohlcv_tables",
        }
        if set(payload) != expected:
            missing = sorted(expected - set(payload))
            extra = sorted(set(payload) - expected)
            raise ValueError(
                "execution_stream fields must match the v2 contract exactly; "
                f"missing={missing}, extra={extra}"
            )
        if payload.get("role") != "execution":
            raise ValueError("execution_stream.role must be execution")
        source = payload.get("source")
        if not isinstance(source, Mapping) or set(source) != {"kind", "provider_id"}:
            raise ValueError("execution_stream.source must declare kind and provider_id")
        if source.get("kind") != "external":
            raise ValueError("execution_stream.source.kind must be external")
        if payload.get("timeline_table") != TIMELINE_TABLE:
            raise ValueError(f"execution_stream.timeline_table must be {TIMELINE_TABLE}")
        if payload.get("ohlcv_tables") != {name: name for name in _BARS}:
            raise ValueError("execution_stream.ohlcv_tables must bind the five canonical tables")
        bar_spec = payload.get("bar_spec")
        semantics = payload.get("timestamp_semantics")
        if not isinstance(bar_spec, Mapping) or not isinstance(semantics, Mapping):
            raise ValueError("execution_stream requires typed bar_spec and timestamp_semantics")
        spec = cls(
            stream_id=str(payload.get("stream_id") or ""),
            provider_id=str(source.get("provider_id") or ""),
            session_scope=str(payload.get("session_scope") or ""),
            row_key_kind=str(payload.get("row_key_kind") or ""),
            bar_spec=dict(bar_spec),
            timestamp_semantics=dict(semantics),
        )
        spec.validate()
        return spec

    def validate(self) -> None:
        if not _TABLE_NAME.fullmatch(self.stream_id):
            raise ValueError("execution_stream.stream_id is invalid")
        if not self.provider_id:
            raise ValueError("execution_stream.source.provider_id is required")
        if self.session_scope not in {"regular", "24x7"}:
            raise ValueError("execution_stream.session_scope must be regular or 24x7")
        if self.row_key_kind not in {"session_label", "event_timestamp"}:
            raise ValueError(
                "execution_stream.row_key_kind must be session_label or event_timestamp"
            )
        required_bar_spec = {"aggregation", "step", "unit", "price_type", "alignment"}
        if set(self.bar_spec) != required_bar_spec:
            raise ValueError("execution_stream.bar_spec fields are incomplete or unknown")
        if self.bar_spec.get("aggregation") != "time":
            raise ValueError("execution_stream.bar_spec.aggregation must be time")
        step = self.bar_spec.get("step")
        if not isinstance(step, int) or isinstance(step, bool) or step < 1:
            raise ValueError("execution_stream.bar_spec.step must be a positive integer")
        required_semantics = {
            "timestamp_convention",
            "interval_boundary",
            "external_execution_sequence_column",
            "bar_open_time_column",
            "bar_close_time_column",
            "available_time_column",
            "session_label_column",
            "availability_policy",
        }
        if set(self.timestamp_semantics) != required_semantics:
            raise ValueError(
                "execution_stream.timestamp_semantics fields are incomplete or unknown"
            )
        expected_columns = {
            "external_execution_sequence_column": "external_execution_sequence",
            "bar_open_time_column": "bar_open_timestamp",
            "bar_close_time_column": "bar_close_timestamp",
            "available_time_column": "available_timestamp",
            "session_label_column": "session_label",
        }
        for field, expected in expected_columns.items():
            if self.timestamp_semantics.get(field) != expected:
                raise ValueError(f"execution_stream.timestamp_semantics.{field} must be {expected}")

    def to_manifest(self) -> Dict[str, Any]:
        self.validate()
        return {
            "stream_id": self.stream_id,
            "role": "execution",
            "source": {"kind": "external", "provider_id": self.provider_id},
            "session_scope": self.session_scope,
            "row_key_kind": self.row_key_kind,
            "bar_spec": dict(self.bar_spec),
            "timestamp_semantics": dict(self.timestamp_semantics),
            "timeline_table": TIMELINE_TABLE,
            "ohlcv_tables": {name: name for name in _BARS},
        }


@dataclass(frozen=True)
class SessionWindow:
    """One provider-supplied concrete market session window."""

    session_label: str
    open_timestamp: str
    close_timestamp: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SessionWindow":
        payload = dict(value or {})
        expected = {"session_label", "open_timestamp", "close_timestamp"}
        if set(payload) != expected:
            raise ValueError("session_windows entries must match the v2 contract exactly")
        return cls(
            session_label=str(payload["session_label"]),
            open_timestamp=_utc_timestamp_text(payload["open_timestamp"]),
            close_timestamp=_utc_timestamp_text(payload["close_timestamp"]),
        )

    def to_manifest(self) -> Dict[str, str]:
        return {
            "session_label": self.session_label,
            "open_timestamp": self.open_timestamp,
            "close_timestamp": self.close_timestamp,
        }


@dataclass(frozen=True)
class ExternalMarketData:
    """Typed provider result consumed by the v2 bundle writer."""

    frames: Mapping[str, pd.DataFrame]
    execution_stream: ExecutionStreamSpec
    execution_timeline: pd.DataFrame
    session_windows: Sequence[SessionWindow]


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
        row_key_kind = str(manifest["execution_stream"]["row_key_kind"])
        frames: Dict[str, pd.DataFrame] = {}
        for name, table in manifest["tables"].items():
            if table["role"] == "bar_timeline":
                continue
            frames[str(name)] = self._load_table(
                str(name), table, row_key_kind=row_key_kind
            )
        if "close" not in frames:
            raise ValueError("MarketDataBundle requires a close table")
        return frames

    def load_execution_timeline(self) -> pd.DataFrame:
        manifest = self.read_manifest()
        table = manifest["tables"][manifest["execution_stream"]["timeline_table"]]
        frame = self._load_table(
            TIMELINE_TABLE,
            table,
            row_key_kind=str(manifest["execution_stream"]["row_key_kind"]),
        )
        return _normalize_execution_timeline(
            frame,
            row_key_kind=str(manifest["execution_stream"]["row_key_kind"]),
            semantics=manifest["execution_stream"]["timestamp_semantics"],
        )

    def _load_table(
        self,
        name: str,
        table: Mapping[str, Any],
        *,
        row_key_kind: str,
    ) -> pd.DataFrame:
        bundle_dir = self.manifest_path.parent.resolve()
        raw_path = table.get("path")
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ValueError(f"MarketDataBundle table {name} is not file-backed")
        path = Path(raw_path)
        if not path.is_absolute():
            path = bundle_dir / path
        path = path.resolve()
        if path.parent != bundle_dir:
            raise ValueError(f"MarketDataBundle table path escapes bundle directory: {name}")
        if not path.is_file():
            raise ValueError(f"MarketDataBundle table is missing: {name}")
        transport_frame = pd.read_parquet(path)
        _validate_transport_frame(
            transport_frame,
            name=name,
            row_key_kind=row_key_kind,
        )
        if _frame_content_hash(transport_frame) != table["content_hash"]:
            raise ValueError(f"MarketDataBundle table content hash mismatch: {name}")
        if len(transport_frame.index) != int(table["row_count"]):
            raise ValueError(f"MarketDataBundle table row_count mismatch: {name}")
        if [str(column) for column in transport_frame.columns] != list(table["columns"]):
            raise ValueError(f"MarketDataBundle table columns mismatch: {name}")
        return _normalize_frame(
            transport_frame,
            name=name,
            row_key_kind=row_key_kind,
        )

    def primary_frame(self) -> pd.DataFrame:
        return self.load_frames()["close"]

    def validate_against_engine_request(self, engine_request: Mapping[str, Any]) -> None:
        manifest = self.read_manifest()
        requirements = dict(engine_request.get("data_requirements") or {})
        if requirements.get("bundle_schema_version") != SCHEMA_VERSION:
            raise ValueError("EngineRequest does not require MarketDataBundle.v2")
        expected_symbols = [str(item) for item in requirements.get("symbols") or []]
        if manifest["symbols"] != expected_symbols:
            raise ValueError("MarketDataBundle symbols do not match EngineRequest")
        bar_time = requirements.get("bar_time")
        if not isinstance(bar_time, Mapping):
            raise ValueError("EngineRequest data_requirements.bar_time is required")
        streams = {
            str(stream.get("stream_id")): stream
            for stream in bar_time.get("streams") or []
            if isinstance(stream, Mapping)
        }
        execution = manifest["execution_stream"]
        requested = streams.get(execution["stream_id"])
        if requested is None:
            raise ValueError("MarketDataBundle execution stream is not declared by EngineRequest")
        for field in ("role", "source", "bar_spec"):
            if requested.get(field) != execution[field]:
                raise ValueError(
                    f"MarketDataBundle execution_stream.{field} does not match EngineRequest"
                )
        requested_semantics = requested.get("timestamp_semantics")
        physical_semantics = dict(execution["timestamp_semantics"])
        physical_semantics.pop("external_execution_sequence_column", None)
        if requested_semantics != physical_semantics:
            raise ValueError(
                "MarketDataBundle execution_stream.timestamp_semantics "
                "does not match EngineRequest"
            )


def build_market_data_bundle(
    data: ExternalMarketData,
    *,
    spec: Mapping[str, Any],
    output_root: Path | str,
) -> MarketDataBundle:
    """Persist one explicit external execution stream without inference."""

    if not isinstance(data, ExternalMarketData):
        raise TypeError("MarketDataBundle v2 requires ExternalMarketData")
    stream = data.execution_stream
    stream.validate()
    if not isinstance(data.frames, Mapping) or not data.frames:
        raise ValueError("MarketDataBundle requires non-empty market frames")
    if any(
        field in spec
        for field in ("frequency", "interval", "index_kind", "time_semantics")
    ):
        raise ValueError(
            "MarketDataBundle v2 rejects legacy frequency, interval, "
            "index_kind, and time_semantics"
        )
    _validate_physical_row_domain(data.frames, stream=stream)

    normalized: Dict[str, pd.DataFrame] = {}
    for raw_name, raw_frame in data.frames.items():
        name = str(raw_name).strip().lower()
        if not _TABLE_NAME.fullmatch(name):
            raise ValueError(f"Invalid MarketDataBundle table name: {raw_name}")
        if not isinstance(raw_frame, pd.DataFrame) or raw_frame.empty:
            raise ValueError(f"MarketDataBundle table {name} must be a non-empty DataFrame")
        normalized[name] = _normalize_frame(
            raw_frame, name=name, row_key_kind=stream.row_key_kind
        )
    missing_bars = [name for name in _BARS if name not in normalized]
    if missing_bars:
        raise ValueError(
            "MarketDataBundle v2 requires all OHLCV tables: " + ", ".join(missing_bars)
        )

    symbols = [str(item).strip() for item in spec.get("symbols") or []]
    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError("MarketDataBundle symbols must be non-empty and unique")
    _validate_ohlcv_tables(normalized, symbols)

    timeline = _normalize_execution_timeline(
        data.execution_timeline,
        row_key_kind=stream.row_key_kind,
        semantics=stream.timestamp_semantics,
    )
    close = normalized["close"]
    if len(timeline.index) != len(close.index) or not timeline.index.equals(close.index):
        raise ValueError(
            "MarketDataBundle execution_timeline row count and row keys must match OHLCV"
        )
    windows = _validate_session_windows(data.session_windows, timeline)
    normalized[TIMELINE_TABLE] = timeline

    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".mdb-staging-", dir=root))
    try:
        table_payloads: Dict[str, Dict[str, Any]] = {}
        for name in sorted(normalized):
            path = staging / f"{name}.parquet"
            transport_frame = _to_transport_frame(
                normalized[name],
                row_key_kind=stream.row_key_kind,
            )
            transport_frame.to_parquet(path, index=True)
            canonical = pd.read_parquet(path)
            _validate_transport_frame(
                canonical,
                name=name,
                row_key_kind=stream.row_key_kind,
            )
            table_payloads[name] = {
                "role": (
                    "bar_timeline"
                    if name == TIMELINE_TABLE
                    else "bars"
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

        provider = stream.provider_id
        quality = {
            "missing_value_policy": "fail",
            "duplicate_time_policy": "fail",
            "out_of_order_policy": "fail",
            "stale_value_policy": str(spec.get("stale_value_policy") or "preserve"),
            "warnings": [str(item) for item in spec.get("quality_warnings") or []],
        }
        lineage = {
            "provider": provider,
            "source_hashes": {
                name: table["content_hash"] for name, table in sorted(table_payloads.items())
            },
            "point_in_time": bool(spec.get("point_in_time", False)),
            "adjustment_policy": _adjustment_policy(spec),
        }
        base_manifest: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "contract_id": CONTRACT_ID,
            "bundle_id": "",
            "content_hash": "",
            "symbols": symbols,
            "calendar": str(spec.get("calendar_id") or ""),
            "timezone": str(spec.get("timezone") or ""),
            "time_range": {
                "start": _timestamp_text(close.index[0]),
                "end": _timestamp_text(close.index[-1]),
            },
            "row_count": len(close.index),
            "time_column": TIME_COLUMN,
            "execution_stream": stream.to_manifest(),
            "session_windows": [window.to_manifest() for window in windows],
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
            key: value for key, value in dict(table).items() if key != "path"
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


def _normalize_frame(
    frame: pd.DataFrame,
    *,
    name: str,
    row_key_kind: str,
) -> pd.DataFrame:
    out = frame.copy()
    index = pd.DatetimeIndex(pd.to_datetime(out.index, errors="coerce"))
    if index.isna().any():
        raise ValueError(f"MarketDataBundle table {name} contains invalid timestamps")
    if row_key_kind == "session_label":
        if index.tz is not None:
            raise ValueError(
                f"MarketDataBundle session_label table {name} requires timezone-naive row keys"
            )
        index = index.normalize()
    elif row_key_kind == "event_timestamp":
        if index.tz is None:
            raise ValueError(
                f"MarketDataBundle event_timestamp table {name} requires timezone-aware row keys"
            )
        index = index.tz_convert("UTC")
    else:
        raise ValueError(f"Unknown MarketDataBundle row_key_kind: {row_key_kind}")
    out.index = pd.DatetimeIndex(index, name=TIME_COLUMN)
    out.columns = [str(column) for column in out.columns]
    if out.index.duplicated().any():
        raise ValueError(f"MarketDataBundle table {name} contains duplicate timestamps")
    if not out.index.is_monotonic_increasing:
        raise ValueError(f"MarketDataBundle table {name} row keys must be ordered")
    return out


def _validate_physical_row_domain(
    frames: Mapping[str, pd.DataFrame],
    *,
    stream: ExecutionStreamSpec,
) -> None:
    """Reject physical rows that cannot represent the typed execution domain."""

    close = frames.get("close")
    if not isinstance(close, pd.DataFrame) or close.empty:
        return
    index = pd.DatetimeIndex(pd.to_datetime(close.index, errors="coerce"))
    if index.isna().any():
        return
    if stream.row_key_kind == "session_label":
        if index.tz is not None or not index.equals(index.normalize()):
            raise ValueError(
                "MarketDataBundle execution row index spacing is incompatible "
                "with session_label bars"
            )
    elif stream.row_key_kind == "event_timestamp" and index.tz is None:
        raise ValueError(
            "MarketDataBundle event_timestamp row index spacing requires "
            "timezone-aware provider timestamps"
        )


def _to_transport_frame(
    frame: pd.DataFrame,
    *,
    row_key_kind: str,
) -> pd.DataFrame:
    out = frame.copy()
    if row_key_kind == "session_label":
        values = pd.DatetimeIndex(out.index).strftime("%Y-%m-%d").tolist()
    elif row_key_kind == "event_timestamp":
        values = [_utc_timestamp_text(value) for value in out.index]
    else:
        raise ValueError(f"Unknown MarketDataBundle row_key_kind: {row_key_kind}")
    out.index = pd.Index(values, dtype="string", name=TIME_COLUMN)
    return out


def _validate_transport_frame(
    frame: pd.DataFrame,
    *,
    name: str,
    row_key_kind: str,
) -> None:
    if frame.index.name != TIME_COLUMN:
        raise ValueError(f"MarketDataBundle table {name} transport index must be Time")
    if not pd.api.types.is_string_dtype(frame.index.dtype):
        raise ValueError(
            f"MarketDataBundle table {name} transport Time must be a string"
        )
    values = frame.index.astype("string").tolist()
    if any(value is pd.NA or not isinstance(value, str) for value in values):
        raise ValueError(f"MarketDataBundle table {name} transport Time contains nulls")
    if row_key_kind == "session_label":
        parsed = pd.to_datetime(values, format="%Y-%m-%d", errors="coerce")
        if parsed.isna().any() or parsed.strftime("%Y-%m-%d").tolist() != values:
            raise ValueError(
                f"MarketDataBundle table {name} transport Time must use YYYY-MM-DD"
            )
    elif row_key_kind == "event_timestamp":
        try:
            canonical = [_utc_timestamp_text(value) for value in values]
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"MarketDataBundle table {name} transport Time must use UTC RFC3339"
            ) from exc
        if canonical != values or any(not value.endswith("Z") for value in values):
            raise ValueError(
                f"MarketDataBundle table {name} transport Time must be canonical UTC ...Z"
            )
    else:
        raise ValueError(f"Unknown MarketDataBundle row_key_kind: {row_key_kind}")
    if len(values) != len(set(values)):
        raise ValueError(f"MarketDataBundle table {name} transport Time contains duplicates")
    if values != sorted(values):
        raise ValueError(f"MarketDataBundle table {name} transport Time must be ordered")
    if name == TIMELINE_TABLE:
        for column in _TIMESTAMP_COLUMNS:
            if column not in frame.columns or not pd.api.types.is_string_dtype(
                frame[column].dtype
            ):
                raise ValueError(
                    f"execution_timeline transport {column} must be a string"
                )
            timestamp_values = frame[column].astype("string").tolist()
            if (
                any(value is pd.NA or not isinstance(value, str) for value in timestamp_values)
                or [_utc_timestamp_text(value) for value in timestamp_values]
                != timestamp_values
                or any(not value.endswith("Z") for value in timestamp_values)
            ):
                raise ValueError(
                    f"execution_timeline transport {column} must be canonical UTC ...Z"
                )


def _normalize_execution_timeline(
    frame: pd.DataFrame,
    *,
    row_key_kind: str,
    semantics: Mapping[str, Any],
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError("MarketDataBundle execution_timeline must be a non-empty DataFrame")
    out = _normalize_frame(
        frame, name=TIMELINE_TABLE, row_key_kind=row_key_kind
    )
    if list(out.columns) != list(TIMELINE_COLUMNS):
        raise ValueError(
            "MarketDataBundle execution_timeline columns must match the v2 contract exactly"
        )
    sequence = pd.to_numeric(out["external_execution_sequence"], errors="coerce")
    if sequence.isna().any() or (sequence < 0).any() or (sequence % 1 != 0).any():
        raise ValueError("execution_timeline sequence must contain unsigned integers")
    sequence = sequence.astype("uint64")
    if sequence.duplicated().any() or not sequence.is_monotonic_increasing:
        raise ValueError(
            "execution_timeline sequence must be unique and strictly increasing"
        )
    out["external_execution_sequence"] = sequence
    parsed_timestamps: Dict[str, pd.Series] = {}
    for column in _TIMESTAMP_COLUMNS:
        values = []
        for value in out[column]:
            try:
                timestamp = pd.Timestamp(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"execution_timeline.{column} contains invalid UTC timestamps"
                ) from exc
            if pd.isna(timestamp) or timestamp.tzinfo is None:
                raise ValueError(
                    f"execution_timeline.{column} requires timezone-aware timestamps"
                )
            values.append(timestamp.tz_convert("UTC"))
        parsed_timestamps[column] = pd.Series(values, index=out.index)
    labels = out["session_label"].astype("string")
    parsed_labels = pd.to_datetime(labels, format="%Y-%m-%d", errors="coerce")
    if (
        parsed_labels.isna().any()
        or parsed_labels.dt.strftime("%Y-%m-%d").tolist() != labels.tolist()
    ):
        raise ValueError("execution_timeline.session_label must use YYYY-MM-DD")
    out["session_label"] = labels
    if not (
        parsed_timestamps["bar_open_timestamp"]
        < parsed_timestamps["bar_close_timestamp"]
    ).all():
        raise ValueError("execution_timeline requires bar_open_timestamp < bar_close_timestamp")
    if not (
        parsed_timestamps["bar_close_timestamp"]
        <= parsed_timestamps["available_timestamp"]
    ).all():
        raise ValueError(
            "execution_timeline requires bar_close_timestamp <= available_timestamp"
        )
    if semantics.get("availability_policy") == "bar_close" and not parsed_timestamps[
        "bar_close_timestamp"
    ].equals(parsed_timestamps["available_timestamp"]):
        raise ValueError(
            "bar_close availability_policy requires available_timestamp == bar_close_timestamp"
        )
    convention = semantics.get("timestamp_convention")
    authoritative = (
        parsed_timestamps["bar_close_timestamp"]
        if convention == "bar_close"
        else parsed_timestamps["bar_open_timestamp"]
        if convention == "bar_open"
        else None
    )
    if authoritative is None:
        raise ValueError("execution_stream.timestamp_convention is invalid")
    if row_key_kind == "session_label":
        expected = pd.DatetimeIndex(parsed_labels, name=TIME_COLUMN)
    else:
        expected = pd.DatetimeIndex(authoritative, name=TIME_COLUMN)
    if not out.index.equals(expected):
        raise ValueError(
            "execution_timeline row keys do not match row_key_kind and timestamp_convention"
        )
    for column in _TIMESTAMP_COLUMNS:
        out[column] = parsed_timestamps[column].map(_utc_timestamp_text).astype("string")
    return out


def _validate_ohlcv_tables(
    frames: Mapping[str, pd.DataFrame],
    symbols: Sequence[str],
) -> None:
    close = frames["close"]
    for name in _BARS:
        frame = frames[name]
        if len(frame.index) != len(close.index) or not frame.index.equals(close.index):
            raise ValueError(
                f"MarketDataBundle OHLCV table {name} row count and row keys must match close"
            )
        if list(frame.columns) != list(symbols):
            raise ValueError(
                f"MarketDataBundle OHLCV table {name} columns must match symbols in order"
            )
        numeric = frame.apply(pd.to_numeric, errors="coerce")
        if numeric.isna().any().any():
            raise ValueError(f"MarketDataBundle {name} table contains missing values")
        invalid = numeric.lt(0.0) if name == "volume" else numeric.le(0.0)
        if invalid.any().any():
            raise ValueError(f"MarketDataBundle {name} table contains invalid values")
    open_ = frames["open"].apply(pd.to_numeric, errors="coerce")
    high = frames["high"].apply(pd.to_numeric, errors="coerce")
    low = frames["low"].apply(pd.to_numeric, errors="coerce")
    close = frames["close"].apply(pd.to_numeric, errors="coerce")
    if (
        high.lt(open_).any().any()
        or high.lt(close).any().any()
        or low.gt(open_).any().any()
        or low.gt(close).any().any()
        or high.lt(low).any().any()
    ):
        raise ValueError(
            "MarketDataBundle OHLCV price relationships are invalid"
        )


def _validate_session_windows(
    values: Sequence[SessionWindow],
    timeline: pd.DataFrame,
) -> list[SessionWindow]:
    if not values:
        raise ValueError("MarketDataBundle session_windows must not be empty")
    windows = list(values)
    if not all(isinstance(window, SessionWindow) for window in windows):
        raise TypeError("MarketDataBundle session_windows must contain SessionWindow values")
    labels = [window.session_label for window in windows]
    if labels != sorted(labels) or len(labels) != len(set(labels)):
        raise ValueError("MarketDataBundle session_windows must be unique and ordered")
    timeline_labels = list(dict.fromkeys(timeline["session_label"].astype(str).tolist()))
    if labels != timeline_labels:
        raise ValueError(
            "MarketDataBundle session_windows must exactly cover execution_timeline sessions"
        )
    by_label = {window.session_label: window for window in windows}
    for _, row in timeline.iterrows():
        window = by_label[str(row["session_label"])]
        open_time = pd.Timestamp(window.open_timestamp)
        close_time = pd.Timestamp(window.close_timestamp)
        bar_open_time = pd.Timestamp(str(row["bar_open_timestamp"]))
        bar_close_time = pd.Timestamp(str(row["bar_close_timestamp"]))
        if open_time >= close_time:
            raise ValueError("MarketDataBundle session window open must precede close")
        if bar_open_time < open_time or bar_close_time > close_time:
            raise ValueError("execution_timeline bar falls outside its session window")
    return windows


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
    values = pd.util.hash_pandas_object(
        frame, index=True, categorize=False
    ).to_numpy(dtype="uint64", copy=False)
    digest = hashlib.sha256(metadata)
    digest.update(values.tobytes())
    return digest.hexdigest()


def _adjustment_policy(spec: Mapping[str, Any]) -> str:
    explicit = str(spec.get("adjustment_policy") or "").strip()
    allowed = {"raw", "split_adjusted", "split_dividend_adjusted"}
    if explicit not in allowed:
        raise ValueError(
            "MarketDataBundle requires adjustment_policy from the typed "
            "bar_time.price_model.price_basis"
        )
    return explicit


def _timestamp_text(value: Any) -> str:
    return pd.Timestamp(value).isoformat()


def _utc_timestamp_text(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        raise ValueError("session window timestamps must be timezone-aware")
    return timestamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")
