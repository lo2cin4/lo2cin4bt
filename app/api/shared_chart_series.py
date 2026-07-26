"""Content-addressed storage for repeated chart and detail series."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from pathlib import Path
from typing import Any, Dict

from app.runtime.registry import AppRegistry


SHARED_SERIES_SCHEMA_VERSION = "chart_shared_series.v1"
SHARED_SERIES_CONTRACT_ID = "lo2cin4bt.chart_shared_series.v1"
SHARED_SERIES_REF_SCHEMA_VERSION = "chart_shared_series_ref.v1"
_CONTENT_HASH = re.compile(r"^[0-9a-f]{64}$")


class SharedChartSeriesStore:
    """Store a run's repeated arrays once and address them by content hash."""

    def __init__(self, registry: AppRegistry):
        self.registry = registry

    def put(self, run_id: str, kind: str, value: Any) -> Dict[str, str]:
        normalized_kind = str(kind or "").strip()
        if not normalized_kind:
            raise ValueError("shared series kind is required")
        value_bytes = self._canonical_bytes(value)
        content_hash = hashlib.sha256(value_bytes).hexdigest()
        ref = {
            "schema_version": SHARED_SERIES_REF_SCHEMA_VERSION,
            "series_id": content_hash,
            "kind": normalized_kind,
        }
        path = self._path(run_id, content_hash)
        if path.is_file():
            self.load(run_id, ref, expected_kind=normalized_kind)
            return ref
        envelope = {
            "schema_version": SHARED_SERIES_SCHEMA_VERSION,
            "contract_id": SHARED_SERIES_CONTRACT_ID,
            "run_id": str(run_id),
            "series_id": content_hash,
            "kind": normalized_kind,
            "value": value,
        }
        self.write_json(path, envelope)
        return ref

    def load(
        self,
        run_id: str,
        ref: Dict[str, Any],
        *,
        expected_kind: str | None = None,
    ) -> Any:
        if not isinstance(ref, dict) or ref.get("schema_version") != SHARED_SERIES_REF_SCHEMA_VERSION:
            raise ValueError("shared series reference is invalid")
        series_id = str(ref.get("series_id") or "")
        kind = str(ref.get("kind") or "")
        if not _CONTENT_HASH.fullmatch(series_id):
            raise ValueError("shared series content hash is invalid")
        if expected_kind is not None and kind != expected_kind:
            raise ValueError("shared series kind does not match its field")
        envelope = self.read_json(self._path(run_id, series_id))
        if (
            envelope.get("schema_version") != SHARED_SERIES_SCHEMA_VERSION
            or envelope.get("contract_id") != SHARED_SERIES_CONTRACT_ID
            or envelope.get("run_id") != str(run_id)
            or envelope.get("series_id") != series_id
            or envelope.get("kind") != kind
        ):
            raise ValueError("shared series envelope is invalid")
        if hashlib.sha256(self._canonical_bytes(envelope.get("value"))).hexdigest() != series_id:
            raise ValueError("shared series content hash does not match its value")
        return envelope.get("value")

    def compact_plot_bundle(self, run_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._compact_series_payload(
            run_id,
            payload,
            schema_version="plot_bundle_index.v1",
            contract_id="lo2cin4bt.plot_bundle_index.v1",
        )

    def materialize_plot_bundle(self, run_id: str, index: Dict[str, Any]) -> Dict[str, Any]:
        return self._materialize_series_payload(
            run_id,
            index,
            schema_version="plot_bundle_index.v1",
            contract_id="lo2cin4bt.plot_bundle_index.v1",
        )

    def compact_metrics_overview(
        self, run_id: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        index = self._compact_series_payload(
            run_id,
            payload,
            schema_version="metrics_overview_index.v1",
            contract_id="lo2cin4bt.metrics_overview_index.v1",
        )
        benchmark = payload.get("benchmark_series")
        index["payload"].pop("benchmark_series", None)
        index["benchmark_series"] = self._compact_one_series(run_id, benchmark)
        return index

    def materialize_metrics_overview(
        self, run_id: str, index: Dict[str, Any]
    ) -> Dict[str, Any]:
        payload = self._materialize_series_payload(
            run_id,
            index,
            schema_version="metrics_overview_index.v1",
            contract_id="lo2cin4bt.metrics_overview_index.v1",
        )
        payload["benchmark_series"] = self._materialize_one_series(
            run_id, index.get("benchmark_series")
        )
        return payload

    def compact_backtest_detail(
        self, run_id: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        compact_payload = dict(payload)
        refs: Dict[str, Dict[str, str]] = {}
        for field in ("ohlc_by_asset", "benchmark_series"):
            if field in compact_payload:
                refs[field] = self.put(run_id, f"backtest_detail.{field}", compact_payload.pop(field))
        return {
            "schema_version": "backtest_detail_index.v1",
            "contract_id": "lo2cin4bt.backtest_detail_index.v1",
            "run_id": str(run_id),
            "backtest_id": str(payload.get("backtest_id") or ""),
            "payload": compact_payload,
            "shared_series_refs": refs,
        }

    def materialize_backtest_detail(
        self, run_id: str, index: Dict[str, Any]
    ) -> Dict[str, Any]:
        self._validate_index(
            run_id,
            index,
            schema_version="backtest_detail_index.v1",
            contract_id="lo2cin4bt.backtest_detail_index.v1",
        )
        payload = dict(index["payload"])
        refs = index.get("shared_series_refs")
        if not isinstance(refs, dict):
            raise ValueError("backtest detail shared series references are invalid")
        for field, ref in refs.items():
            if field not in {"ohlc_by_asset", "benchmark_series"}:
                raise ValueError(f"unsupported backtest detail shared field: {field}")
            payload[field] = self.load(
                run_id,
                ref,
                expected_kind=f"backtest_detail.{field}",
            )
        return payload

    def _compact_series_payload(
        self,
        run_id: str,
        payload: Dict[str, Any],
        *,
        schema_version: str,
        contract_id: str,
    ) -> Dict[str, Any]:
        series = payload.get("series")
        if not isinstance(series, list):
            raise ValueError("series payload must contain a series list")
        compact_payload = dict(payload)
        compact_payload.pop("series", None)
        return {
            "schema_version": schema_version,
            "contract_id": contract_id,
            "run_id": str(run_id),
            "payload": compact_payload,
            "series": [self._compact_one_series(run_id, item) for item in series],
        }

    def _materialize_series_payload(
        self,
        run_id: str,
        index: Dict[str, Any],
        *,
        schema_version: str,
        contract_id: str,
    ) -> Dict[str, Any]:
        self._validate_index(
            run_id,
            index,
            schema_version=schema_version,
            contract_id=contract_id,
        )
        series = index.get("series")
        if not isinstance(series, list):
            raise ValueError("series index must contain a series list")
        payload = dict(index["payload"])
        payload["series"] = [self._materialize_one_series(run_id, item) for item in series]
        return payload

    def _compact_one_series(self, run_id: str, item: Any) -> Any:
        if item is None:
            return None
        if not isinstance(item, dict) or not isinstance(item.get("x"), list) or not isinstance(item.get("y"), list):
            raise ValueError("chart series must contain x and y lists")
        compact = {key: value for key, value in item.items() if key not in {"x", "y"}}
        compact["data_ref"] = self.put(
            run_id,
            "chart_xy",
            {"x": item["x"], "y": item["y"]},
        )
        return compact

    def _materialize_one_series(self, run_id: str, item: Any) -> Any:
        if item is None:
            return None
        if not isinstance(item, dict):
            raise ValueError("chart series index item is invalid")
        ref = item.get("data_ref")
        if not isinstance(ref, dict):
            raise ValueError("chart series data reference is invalid")
        data = self.load(run_id, ref, expected_kind="chart_xy")
        if not isinstance(data, dict) or not isinstance(data.get("x"), list) or not isinstance(data.get("y"), list):
            raise ValueError("shared chart series data is invalid")
        materialized = {key: value for key, value in item.items() if key != "data_ref"}
        materialized.update(data)
        return materialized

    @staticmethod
    def _validate_index(
        run_id: str,
        index: Dict[str, Any],
        *,
        schema_version: str,
        contract_id: str,
    ) -> None:
        if not isinstance(index, dict):
            raise ValueError("shared series index must be an object")
        if (
            index.get("schema_version") != schema_version
            or index.get("contract_id") != contract_id
            or index.get("run_id") != str(run_id)
            or not isinstance(index.get("payload"), dict)
        ):
            raise ValueError("shared series index contract is invalid")

    def _path(self, run_id: str, content_hash: str) -> Path:
        if not _CONTENT_HASH.fullmatch(content_hash):
            raise ValueError("shared series content hash is invalid")
        return self.registry.resolve_run_paths(run_id)["shared_series_dir"] / f"{content_hash}.json"

    @staticmethod
    def read_json(path: Path) -> Dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FileNotFoundError(f"shared chart payload is unavailable: {path}") from exc
        if not isinstance(payload, dict):
            raise ValueError("shared chart payload must be an object")
        return payload

    @staticmethod
    def write_json(path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        SharedChartSeriesStore._atomic_write(path, encoded)

    @staticmethod
    def _atomic_write(path: Path, content: bytes) -> None:
        temporary = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
        try:
            temporary.write_bytes(content)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _canonical_bytes(value: Any) -> bytes:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
