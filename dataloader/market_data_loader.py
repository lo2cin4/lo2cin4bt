"""Multi-asset market data provider entrypoint.

This module is the runtime boundary between provider-specific download logic
and the backtester. Backtester code should ask this loader for normalized
market frames instead of calling provider APIs directly.
"""

from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import exchange_calendars as xcals  # type: ignore[import-untyped]

from dataloader.market_data_bundle import (
    ExternalMarketData,
    ExecutionStreamSpec,
    MarketDataBundle,
    SessionWindow,
    build_market_data_bundle,
)
from dataloader.provider_bar_time import (
    normalize_native_bar_open_keys,
    resolve_xnys_session_bar_bounds,
    typed_intraday_duration,
)
from utils.path_resolver import resolve_input_path


_YFINANCE_DOWNLOAD_LOCK = threading.Lock()


class MarketDataContractError(ValueError):
    """Fail-closed provider/data error with one frontend and AI-readable shape."""

    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        provider: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.provider = provider
        self.details = details or {}

    def to_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": "run_failure.v1",
            "error_code": self.error_code,
            "stage": "dataloader",
            "provider": self.provider,
            "message": str(self),
            "details": self.details,
            "action": "fix_data_or_config",
        }

_PROVIDER_ALIASES = {
    "yfinance": "yfinance",
    "yf": "yfinance",
    "binance": "binance",
    "binance_spot": "binance",
    "coinbase": "coinbase",
    "coinbase_exchange": "coinbase",
    "futu": "futu",
    "futu_openapi": "futu",
    "ibkr": "ibkr",
    "interactive_brokers": "ibkr",
    "interactivebrokers": "ibkr",
}

_PROVIDER_BAR_CAPABILITIES = {
    "yfinance": {(1, "day")},
    "binance": {
        (1, "minute"),
        (3, "minute"),
        (5, "minute"),
        (15, "minute"),
        (30, "minute"),
        (1, "hour"),
        (2, "hour"),
        (4, "hour"),
        (6, "hour"),
        (8, "hour"),
        (12, "hour"),
        (1, "day"),
    },
    "coinbase": {
        (1, "minute"),
        (5, "minute"),
        (15, "minute"),
        (1, "hour"),
        (6, "hour"),
        (1, "day"),
    },
    "futu": {
        (1, "minute"),
        (5, "minute"),
        (15, "minute"),
        (30, "minute"),
        (1, "hour"),
        (1, "day"),
    },
    "ibkr": {
        (1, "minute"),
        (5, "minute"),
        (15, "minute"),
        (30, "minute"),
        (1, "hour"),
        (1, "day"),
    },
}

_IBKR_SINGLE_REQUEST_HISTORY_DAYS = {
    (1, "minute"): 1,
    (5, "minute"): 7,
    (15, "minute"): 7,
    (30, "minute"): 31,
    (1, "hour"): 31,
    (1, "day"): 365,
}

def provider_timeframe_capability(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Return the complete fail-closed capability for one requested external stream."""

    provider_raw = str(spec.get("provider") or spec.get("source") or "").strip().lower()
    provider = _PROVIDER_ALIASES.get(provider_raw)
    if provider is None:
        raise ValueError(f"Unsupported certified provider: {provider_raw or '<missing>'}")
    execution_stream = spec.get("execution_stream")
    if not isinstance(execution_stream, dict):
        raise ValueError(f"{provider} requires the typed execution_stream")
    bar_spec = execution_stream.get("bar_spec")
    if not isinstance(bar_spec, dict):
        raise ValueError(f"{provider} requires execution_stream.bar_spec")
    step = bar_spec.get("step")
    unit = str(bar_spec.get("unit") or "")
    pair = (step, unit)
    if pair not in _PROVIDER_BAR_CAPABILITIES[provider]:
        raise MarketDataContractError(
            "unsupported_timeframe",
            f"{provider} does not support execution bar_spec step={step}, unit={unit}",
            provider=provider,
            details={"bar_spec": {"step": step, "unit": unit}},
        )
    depth: Dict[str, Any]
    pagination: Dict[str, Any]
    if provider == "ibkr":
        depth = {
            "kind": "bounded",
            "amount": _IBKR_SINGLE_REQUEST_HISTORY_DAYS[pair],
            "unit": "calendar_day",
        }
        pagination = {"mode": "not_supported", "max_bars_per_request": None}
    else:
        depth = {"kind": "unbounded", "amount": None, "unit": None}
        max_bars = {"binance": 1000, "coinbase": 300, "futu": 1000}.get(provider)
        pagination = {
            "mode": "required" if max_bars is not None else "not_supported",
            "max_bars_per_request": max_bars,
        }
    timezone = (
        "UTC"
        if provider in {"binance", "coinbase", "ibkr"}
        else "America/New_York"
    )
    calendar_id = (
        "CRYPTO_24_7" if provider in {"binance", "coinbase"} else "XNYS"
    )
    session_scope = "24x7" if provider in {"binance", "coinbase"} else "regular"
    precision = "millisecond" if provider == "binance" else "second"
    price_basis = str(spec.get("adjustment_policy") or "")
    return {
        "schema_version": "provider_timeframe_capability.v1",
        "contract_id": "lo2cin4bt.provider_timeframe_capability.v1",
        "provider_id": provider,
        "market_data_kind": "bars",
        "source_kind": "external",
        "supported_timeframes": [
            {
                "aggregation": "time",
                "step": step,
                "unit": unit,
                "price_types": ["last"],
                "alignments": ["session_open"],
                "calendar_ids": [calendar_id],
                "history": {"depth": depth, "pagination": pagination},
                "timestamp_semantics": {
                    "timestamp_convention": "bar_open",
                    "precision": precision,
                    "timezone": timezone,
                    "availability": "bar_close",
                },
                "session_scopes": [session_scope],
                "price_policy": {
                    "price_basis": price_basis,
                    "corporate_action_policy": (
                        "provider_applied"
                        if (
                            provider == "yfinance"
                            and price_basis == "split_dividend_adjusted"
                        )
                        or (
                            provider == "ibkr"
                            and price_basis
                            in {"split_adjusted", "split_dividend_adjusted"}
                        )
                        else "not_available"
                    ),
                },
                "quality_policy": {
                    "missing_bar_policy": "fail",
                    "duplicate_timestamp_policy": "fail",
                    "out_of_order_policy": "fail",
                },
            }
        ],
        "unsupported_request_policy": {
            "unsupported_timeframe": "fail",
            "unavailable_history_depth": "fail",
            "unsupported_session_scope": "fail",
            "unsupported_price_basis": "fail",
            "incompatible_timestamp_semantics": "fail",
            "provider_fallback": "forbidden",
            "frequency_fallback": "forbidden",
        },
    }


def market_data_spec_from_requirements(
    requirements: Dict[str, Any],
    stream_binding: Dict[str, Any],
) -> Dict[str, Any]:
    """Compile EngineRequest data requirements into one provider-adapter spec."""

    if not isinstance(requirements, dict) or not requirements:
        raise ValueError("EngineRequest data_requirements are required")
    provider_config = requirements.get("provider_config")
    spec = dict(provider_config) if isinstance(provider_config, dict) else {}
    bar_time = requirements.get("bar_time")
    if not isinstance(bar_time, dict):
        raise ValueError("EngineRequest data_requirements.bar_time is required")
    session_model = bar_time.get("session_model")
    if not isinstance(session_model, dict):
        raise ValueError("EngineRequest bar_time.session_model is required")
    price_model = bar_time.get("price_model")
    if not isinstance(price_model, dict):
        raise ValueError("EngineRequest bar_time.price_model is required")
    execution_stream_id = str(stream_binding.get("execution_stream_id") or "")
    if not execution_stream_id:
        raise ValueError("EngineRequest strategy.stream_binding.execution_stream_id is required")
    streams = bar_time.get("streams")
    if not isinstance(streams, list):
        raise ValueError("EngineRequest bar_time.streams is required")
    bound_stream = next(
        (
            stream
            for stream in streams
            if isinstance(stream, dict)
            and stream.get("stream_id") == execution_stream_id
        ),
        None,
    )
    if not isinstance(bound_stream, dict):
        raise ValueError(
            "EngineRequest execution_stream_id must reference a bar_time stream"
        )
    source = bound_stream.get("source")
    if (
        bound_stream.get("role") != "execution"
        or not isinstance(source, dict)
        or source.get("kind") != "external"
    ):
        raise ValueError("Bound execution stream must be an external execution stream")
    provider = str(requirements.get("provider") or "")
    if source.get("provider_id") != provider:
        raise ValueError("Bound execution stream provider_id must match provider")
    bar_spec = bound_stream.get("bar_spec")
    semantics = bound_stream.get("timestamp_semantics")
    if not isinstance(bar_spec, dict) or not isinstance(semantics, dict):
        raise ValueError("Bound execution stream requires bar_spec and timestamp_semantics")
    row_key_kind = (
        "session_label"
        if bar_spec.get("unit") in {"day", "week", "month"}
        else "event_timestamp"
    )
    supported_shape = (
        (
            bar_spec.get("step") == 1
            and bar_spec.get("unit") == "day"
            and row_key_kind == "session_label"
        )
        or (
            bar_spec.get("unit") in {"minute", "hour"}
            and row_key_kind == "event_timestamp"
        )
    )
    if not supported_shape:
        raise ValueError(
            "Provider execution stream supports only one-day session_label bars "
            "or minute/hour event_timestamp bars"
        )
    execution_stream = {
        **bound_stream,
        "session_scope": session_model.get("session_scope"),
        "row_key_kind": row_key_kind,
        "timestamp_semantics": {
            **semantics,
            "external_execution_sequence_column": (
                "external_execution_sequence"
            ),
        },
        "timeline_table": "execution_timeline",
        "ohlcv_tables": {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        },
    }
    canonical = {
        "provider": provider,
        "symbols": requirements.get("symbols"),
        "calendar_id": session_model.get("calendar_id"),
        "timezone": session_model.get("timezone"),
        "session_model": session_model,
        "execution_stream": execution_stream,
        "adjustment_policy": price_model.get("price_basis"),
        "bar_time": bar_time,
        "start_date": requirements.get("start_date"),
        "end_date": requirements.get("end_date"),
        "start_policy": requirements.get("start_policy"),
        "external_features": requirements.get("external_features"),
        "benchmark": requirements.get("benchmark"),
    }
    for key, value in canonical.items():
        if value not in (None, "", []):
            spec[key] = value
    if not spec.get("symbols"):
        raise ValueError("EngineRequest data_requirements.symbols must not be empty")
    return spec


class MultiAssetMarketDataLoader:
    """Load normalized multi-asset market data frames from configured providers."""

    def __init__(self, *, repo_root: Path):
        self.repo_root = Path(repo_root)

    def load(
        self,
        spec: Any,
        *,
        config_file_path: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        if not isinstance(spec, dict) or not spec:
            raise ValueError("backtester.market_data is required for multi_asset_portfolio")
        self._validate_time_spec(spec)

        provider = str(spec.get("provider") or spec.get("source") or "").strip().lower()
        if provider in _PROVIDER_ALIASES and isinstance(
            spec.get("execution_stream"), dict
        ):
            self._validate_provider_capability(spec)
        if provider in {"yfinance", "yf"}:
            self._validate_provider_price_basis(spec, provider="yfinance")
            frames = self._download_yfinance(spec)
            self._validate_provider_frames(frames, spec=spec)
            return self._with_external_features(
                frames,
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"binance", "binance_spot"}:
            self._validate_provider_price_basis(spec, provider="binance")
            frames = self._download_binance(spec)
            self._validate_provider_frames(frames, spec=spec)
            return self._with_external_features(
                frames,
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"coinbase", "coinbase_exchange"}:
            self._validate_provider_price_basis(spec, provider="coinbase")
            frames = self._download_coinbase(spec)
            self._validate_provider_frames(frames, spec=spec)
            return self._with_external_features(
                frames,
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"futu", "futu_openapi"}:
            from dataloader.futu_loader import FutuMarketDataLoader

            frames = FutuMarketDataLoader().load_multi_asset(spec)
            self._validate_provider_frames(frames, spec=spec)
            return self._with_external_features(
                frames,
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"ibkr", "interactive_brokers", "interactivebrokers"}:
            from dataloader.ibkr_loader import IBKRMarketDataLoader

            frames = IBKRMarketDataLoader().load_multi_asset(spec)
            self._validate_provider_frames(frames, spec=spec)
            return self._with_external_features(
                frames,
                spec,
                config_file_path=config_file_path,
            )

        return self._with_external_features(
            self._load_wide_frames(spec, config_file_path=config_file_path),
            spec,
            config_file_path=config_file_path,
        )

    @classmethod
    def _validate_provider_frames(
        cls,
        frames: Dict[str, pd.DataFrame],
        *,
        spec: Dict[str, Any],
    ) -> None:
        provider_raw = str(spec.get("provider") or spec.get("source") or "").lower()
        provider = _PROVIDER_ALIASES.get(provider_raw, provider_raw or "unknown")
        required_fields = ("open", "high", "low", "close", "volume")
        missing_fields = [field for field in required_fields if field not in frames]
        if missing_fields:
            raise MarketDataContractError(
                "missing_field",
                f"{provider} response is missing required field(s): {', '.join(missing_fields)}",
                provider=provider,
                details={"missing_fields": missing_fields},
            )

        reference_index: Optional[pd.DatetimeIndex] = None
        reference_columns: Optional[List[str]] = None
        for field in required_fields:
            frame = frames[field]
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                raise MarketDataContractError(
                    "missing_data",
                    f"{provider} {field} contains no rows",
                    provider=provider,
                    details={"field": field},
                )
            index = pd.DatetimeIndex(frame.index)
            if index.has_duplicates:
                raise MarketDataContractError(
                    "duplicate_timestamp",
                    f"{provider} {field} contains duplicate timestamps",
                    provider=provider,
                    details={"field": field},
                )
            if not index.is_monotonic_increasing:
                raise MarketDataContractError(
                    "out_of_order_timestamp",
                    f"{provider} {field} timestamps are out of order",
                    provider=provider,
                    details={"field": field},
                )
            missing_value_count = int(frame.isna().sum().sum())
            if missing_value_count:
                raise MarketDataContractError(
                    "missing_value",
                    f"{provider} {field} contains {missing_value_count} missing value(s)",
                    provider=provider,
                    details={
                        "field": field,
                        "missing_value_count": missing_value_count,
                    },
                )
            columns = [str(column) for column in frame.columns]
            if reference_index is None:
                reference_index = index
                reference_columns = columns
            elif not index.equals(reference_index) or columns != reference_columns:
                raise MarketDataContractError(
                    "misaligned_provider_fields",
                    f"{provider} OHLCV fields do not share identical timestamps and symbols",
                    provider=provider,
                    details={"field": field},
                )

        assert reference_index is not None
        cls._validate_provider_bar_continuity(
            reference_index,
            spec=spec,
            provider=provider,
        )

    @classmethod
    def _validate_provider_bar_continuity(
        cls,
        row_keys: pd.DatetimeIndex,
        *,
        spec: Dict[str, Any],
        provider: str,
    ) -> None:
        execution_stream = spec.get("execution_stream")
        bar_spec = (
            execution_stream.get("bar_spec")
            if isinstance(execution_stream, dict)
            else None
        )
        session_model = spec.get("session_model")
        if not isinstance(bar_spec, dict) or not isinstance(session_model, dict):
            return
        unit = str(bar_spec.get("unit") or "")
        step = bar_spec.get("step")
        calendar_id = str(session_model.get("calendar_id") or "")
        if not isinstance(step, int) or isinstance(step, bool) or step < 1:
            return

        if unit == "day" and step == 1:
            observed = row_keys.tz_localize(None).normalize()
            if calendar_id == "XNYS":
                calendar = xcals.get_calendar("XNYS")
                expected_start = pd.Timestamp(str(observed[0]))
                expected_end = pd.Timestamp(str(observed[-1]))
                if provider == "yfinance":
                    requested_start = spec.get("start") or spec.get("start_date")
                    requested_end = spec.get("end") or spec.get("end_date")
                    if requested_start and requested_end:
                        expected_start = pd.Timestamp(str(requested_start)).tz_localize(
                            None
                        )
                        expected_end = (
                            pd.Timestamp(str(requested_end))
                            .tz_localize(None)
                            .normalize()
                            - pd.Timedelta(days=1)
                        )
                expected = calendar.sessions_in_range(expected_start, expected_end)
            elif calendar_id == "CRYPTO_24_7":
                expected = pd.date_range(
                    pd.Timestamp(str(observed[0])),
                    pd.Timestamp(str(observed[-1])),
                    freq="D",
                )
            else:
                return
        elif unit in {"minute", "hour"}:
            duration = typed_intraday_duration(bar_spec)
            observed = row_keys.tz_convert("UTC")
            if calendar_id == "CRYPTO_24_7":
                expected = pd.date_range(
                    pd.Timestamp(str(observed[0])),
                    pd.Timestamp(str(observed[-1])),
                    freq=duration,
                )
            elif calendar_id == "XNYS":
                convention = cls._timestamp_convention(spec)
                calendar = xcals.get_calendar("XNYS")
                sessions = calendar.sessions_in_range(
                    observed[0].normalize().tz_localize(None),
                    observed[-1].normalize().tz_localize(None),
                )
                expected_values: List[pd.Timestamp] = []
                for session in sessions:
                    session_open = calendar.session_open(session)
                    session_close = calendar.session_close(session)
                    opens = pd.date_range(
                        session_open,
                        session_close,
                        freq=duration,
                        inclusive="left",
                    )
                    if convention == "bar_open":
                        expected_values.extend(opens.tolist())
                    else:
                        expected_values.extend(
                            [min(opened + duration, session_close) for opened in opens]
                        )
                expected = pd.DatetimeIndex(expected_values)
                expected = expected[(expected >= observed[0]) & (expected <= observed[-1])]
            else:
                return
        else:
            return

        missing = expected.difference(observed)
        unexpected = observed.difference(expected)
        if len(missing) or len(unexpected):
            raise MarketDataContractError(
                "missing_bar",
                f"{provider} bars are incomplete for the declared calendar and BarSpec",
                provider=provider,
                details={
                    "missing_bar_count": len(missing),
                    "unexpected_bar_count": len(unexpected),
                    "first_missing_timestamp": (
                        str(missing[0]) if len(missing) else None
                    ),
                },
            )

    def load_bundle(
        self,
        spec: Any,
        *,
        output_root: Path,
        config_file_path: Optional[str] = None,
    ) -> MarketDataBundle:
        """Fetch provider data and seal it behind the canonical bundle boundary."""

        if not isinstance(spec, dict) or not spec:
            raise ValueError("MarketDataBundle requires a non-empty data specification")
        if any(
            field in spec
            for field in ("frequency", "interval", "index_kind", "time_semantics")
        ):
            raise ValueError(
                "MarketDataBundle v2 rejects legacy frequency, interval, "
                "index_kind, and time_semantics"
            )
        execution_stream_raw = spec.get("execution_stream")
        if not isinstance(execution_stream_raw, dict):
            raise ValueError("MarketDataBundle v2 requires execution_stream")
        execution_stream = ExecutionStreamSpec.from_mapping(execution_stream_raw)
        frames = self.load(spec, config_file_path=config_file_path)
        frames = self._with_benchmark_close(
            frames,
            spec,
            config_file_path=config_file_path,
        )
        has_timeline = "execution_timeline" in spec
        has_windows = "session_windows" in spec
        if has_timeline != has_windows:
            raise ValueError(
                "MarketDataBundle requires execution_timeline and session_windows together"
            )
        if has_timeline:
            timeline = self._load_execution_timeline(
                spec,
                row_key_kind=execution_stream.row_key_kind,
                config_file_path=config_file_path,
            )
            windows_raw = spec.get("session_windows")
            if not isinstance(windows_raw, list) or not windows_raw:
                raise ValueError("MarketDataBundle v2 requires concrete session_windows")
            windows = []
            for item in windows_raw:
                if not isinstance(item, dict):
                    raise ValueError("session_windows entries must be objects")
                windows.append(SessionWindow.from_mapping(item))
        else:
            provider = str(spec.get("provider") or "").strip().lower()
            canonical_provider = {
                alias: canonical
                for alias, canonical in _PROVIDER_ALIASES.items()
            }.get(provider)
            if canonical_provider is None:
                raise ValueError(
                    "Provider-generated execution_timeline is available only for "
                    "certified external providers; other providers must supply "
                    "execution_timeline and session_windows together"
                )
            stream_provider = _PROVIDER_ALIASES.get(
                execution_stream.provider_id,
                execution_stream.provider_id,
            )
            if stream_provider != canonical_provider:
                raise ValueError(
                    "execution_stream provider_id must match the data provider"
                )
            timeline, windows = self._materialize_provider_time_domain(
                frames,
                spec=spec,
                execution_stream=execution_stream,
            )
        bundle = build_market_data_bundle(
            ExternalMarketData(
                frames=frames,
                execution_stream=execution_stream,
                execution_timeline=timeline,
                session_windows=windows,
            ),
            spec=spec,
            output_root=output_root,
        )
        return bundle

    @classmethod
    def _materialize_provider_time_domain(
        cls,
        frames: Dict[str, pd.DataFrame],
        *,
        spec: Dict[str, Any],
        execution_stream: ExecutionStreamSpec,
    ) -> tuple[pd.DataFrame, List[SessionWindow]]:
        """Materialize provider rows onto the one authoritative typed time domain."""

        close = frames.get("close")
        if not isinstance(close, pd.DataFrame) or close.empty:
            raise ValueError("Provider time materializer requires non-empty close rows")
        row_keys = pd.DatetimeIndex(close.index, name="Time")
        if row_keys.has_duplicates or not row_keys.is_monotonic_increasing:
            raise ValueError("Provider execution row keys must be unique and ordered")
        session_model = spec.get("session_model")
        if not isinstance(session_model, dict):
            raise ValueError("Provider time materializer requires session_model")
        calendar_id = str(session_model.get("calendar_id") or "")
        timezone = str(session_model.get("timezone") or "")
        session_scope = str(session_model.get("session_scope") or "")
        cls._validate_provider_session_model(
            calendar_id=calendar_id,
            timezone=timezone,
            session_scope=session_scope,
        )
        bar_spec = execution_stream.bar_spec
        if execution_stream.row_key_kind == "session_label":
            if (
                bar_spec.get("aggregation") != "time"
                or bar_spec.get("step") != 1
                or bar_spec.get("unit") != "day"
            ):
                raise ValueError(
                    "Provider session-label materialization requires a one-day execution BarSpec"
                )
            if row_keys.tz is not None:
                raise ValueError("Provider daily session labels must be timezone-naive dates")
            labels = row_keys.normalize()
            if not labels.equals(row_keys):
                raise ValueError("Provider daily row keys must be normalized session labels")
            lifecycle, windows = cls._daily_session_lifecycle(
                labels,
                calendar_id=calendar_id,
            )
            authoritative = lifecycle["bar_close_timestamp"]
        else:
            if row_keys.tz is None:
                raise ValueError(
                    "Provider event_timestamp rows require timezone-aware timestamps"
                )
            row_keys = row_keys.tz_convert("UTC")
            lifecycle, windows = cls._event_session_lifecycle(
                row_keys,
                calendar_id=calendar_id,
                bar_spec=bar_spec,
                timestamp_semantics=execution_stream.timestamp_semantics,
            )
            convention = execution_stream.timestamp_semantics.get(
                "timestamp_convention"
            )
            authoritative = lifecycle[
                "bar_close_timestamp"
                if convention == "bar_close"
                else "bar_open_timestamp"
            ]
            if not pd.DatetimeIndex(authoritative).equals(row_keys):
                raise ValueError(
                    "Provider timestamps do not match timestamp_convention"
                )
        availability_policy = execution_stream.timestamp_semantics.get(
            "availability_policy"
        )
        if availability_policy != "bar_close":
            raise ValueError(
                "Provider time materializer requires availability_policy=bar_close"
            )
        timeline = pd.DataFrame(
            {
                "external_execution_sequence": range(len(row_keys)),
                "bar_open_timestamp": lifecycle["bar_open_timestamp"],
                "bar_close_timestamp": lifecycle["bar_close_timestamp"],
                "available_timestamp": lifecycle["bar_close_timestamp"],
                "session_label": lifecycle["session_label"],
            },
            index=row_keys,
        )
        timeline.index.name = "Time"
        return timeline, windows

    @staticmethod
    def _validate_provider_session_model(
        *,
        calendar_id: str,
        timezone: str,
        session_scope: str,
    ) -> None:
        supported = {
            "XNYS": ("America/New_York", "regular"),
            "CRYPTO_24_7": ("UTC", "24x7"),
        }
        expected = supported.get(calendar_id)
        if expected is None:
            raise ValueError(f"Unsupported calendar_id: {calendar_id}")
        if (timezone, session_scope) != expected:
            raise ValueError(
                f"{calendar_id} requires timezone={expected[0]} and "
                f"session_scope={expected[1]}"
            )

    @staticmethod
    def _daily_session_lifecycle(
        labels: pd.DatetimeIndex,
        *,
        calendar_id: str,
    ) -> tuple[Dict[str, List[Any]], List[SessionWindow]]:
        opens: List[pd.Timestamp] = []
        closes: List[pd.Timestamp] = []
        label_text = labels.strftime("%Y-%m-%d").tolist()
        if calendar_id == "XNYS":
            calendar = xcals.get_calendar("XNYS")
            schedule = calendar.schedule.loc[labels.min() : labels.max()]
            for label in labels:
                if label not in schedule.index:
                    raise ValueError(
                        f"Provider row {label.date()} is not an XNYS session"
                    )
                opens.append(pd.Timestamp(schedule.loc[label, "open"]))
                closes.append(pd.Timestamp(schedule.loc[label, "close"]))
        else:
            opens = [label.tz_localize("UTC") for label in labels]
            closes = [timestamp + pd.Timedelta(days=1) for timestamp in opens]
        windows = [
            SessionWindow.from_mapping(
                {
                    "session_label": label,
                    "open_timestamp": opened,
                    "close_timestamp": closed,
                }
            )
            for label, opened, closed in zip(label_text, opens, closes)
        ]
        return {
            "bar_open_timestamp": opens,
            "bar_close_timestamp": closes,
            "session_label": label_text,
        }, windows

    @staticmethod
    def _event_session_lifecycle(
        row_keys: pd.DatetimeIndex,
        *,
        calendar_id: str,
        bar_spec: Any,
        timestamp_semantics: Any,
    ) -> tuple[Dict[str, List[Any]], List[SessionWindow]]:
        duration = MultiAssetMarketDataLoader._execution_bar_duration(bar_spec)
        convention = timestamp_semantics.get("timestamp_convention")
        if convention not in {"bar_open", "bar_close"}:
            raise ValueError(
                "Provider time materializer requires bar_open or bar_close timestamps"
            )

        labels: List[str] = []
        window_by_label: Dict[str, SessionWindow] = {}
        if calendar_id == "XNYS":
            utc_keys = row_keys.tz_convert("UTC")
            open_keys, close_keys, labels, resolved_windows = (
                resolve_xnys_session_bar_bounds(
                    utc_keys,
                    bar_spec=bar_spec,
                    timestamp_convention=str(convention),
                )
            )
            opens = list(open_keys)
            closes = list(close_keys)
            for session_label, (session_open, session_close) in (
                resolved_windows.items()
            ):
                window_by_label.setdefault(
                    session_label,
                    SessionWindow.from_mapping(
                        {
                            "session_label": session_label,
                            "open_timestamp": session_open,
                            "close_timestamp": session_close,
                        }
                    ),
                )
        else:
            if convention == "bar_close":
                closes = list(row_keys)
                opens = [timestamp - duration for timestamp in row_keys]
            else:
                opens = list(row_keys)
                closes = [timestamp + duration for timestamp in row_keys]
            for opened, closed in zip(opens, closes):
                membership = closed - pd.Timedelta(1, unit="ns")
                session_label = membership.strftime("%Y-%m-%d")
                session_open = membership.normalize()
                labels.append(session_label)
                window_by_label.setdefault(
                    session_label,
                    SessionWindow.from_mapping(
                        {
                            "session_label": session_label,
                            "open_timestamp": session_open,
                            "close_timestamp": session_open + pd.Timedelta(days=1),
                        }
                    ),
                )
        windows = [window_by_label[label] for label in dict.fromkeys(labels)]
        return {
            "bar_open_timestamp": opens,
            "bar_close_timestamp": closes,
            "session_label": labels,
        }, windows

    @staticmethod
    def _execution_bar_duration(bar_spec: Any) -> pd.Timedelta:
        return typed_intraday_duration(bar_spec)

    def _with_benchmark_close(
        self,
        frames: Dict[str, pd.DataFrame],
        spec: Dict[str, Any],
        *,
        config_file_path: Optional[str],
    ) -> Dict[str, pd.DataFrame]:
        """Attach benchmark prices without expanding the trading universe."""

        benchmark = spec.get("benchmark")
        if not isinstance(benchmark, dict):
            return frames
        forbidden_time_fields = {
            "frequency",
            "interval",
            "bar_spec",
            "execution_stream",
            "index_kind",
            "time_semantics",
        }
        nested_time_fields = sorted(forbidden_time_fields & set(benchmark))
        if nested_time_fields:
            raise ValueError(
                "benchmark must use the bundle execution stream; nested time fields "
                f"are forbidden: {nested_time_fields}"
            )
        symbol = str(benchmark.get("symbol") or "").strip().upper()
        if not symbol:
            return frames
        close = frames.get("close")
        if isinstance(close, pd.DataFrame) and symbol in close.columns:
            out = dict(frames)
            out["benchmark_close"] = close[[symbol]].copy()
            return out

        benchmark_spec = {
            key: value
            for key, value in spec.items()
            if key not in {"symbols", "external_features", "features", "benchmark"}
        }
        benchmark_spec.update(
            {
                key: value
                for key, value in benchmark.items()
                if key not in {"symbol", "label"} and value not in (None, "")
            }
        )
        benchmark_spec["symbols"] = [symbol]
        benchmark_frames = self.load(
            benchmark_spec,
            config_file_path=config_file_path,
        )
        benchmark_close = benchmark_frames.get("close")
        if not isinstance(benchmark_close, pd.DataFrame) or symbol not in benchmark_close.columns:
            raise ValueError(f"configured benchmark {symbol} did not produce a close series")
        if not isinstance(close, pd.DataFrame) or not benchmark_close.index.equals(close.index):
            raise ValueError(
                "configured benchmark row keys must exactly match the execution stream"
            )
        out = dict(frames)
        out["benchmark_close"] = benchmark_close[[symbol]].copy()
        return out

    @staticmethod
    def _validate_time_spec(spec: Dict[str, Any]) -> None:
        legacy_fields = sorted(
            {"frequency", "interval", "index_kind", "time_semantics"} & set(spec)
        )
        if legacy_fields:
            raise ValueError(
                "multi-asset market data loader rejects legacy time fields: "
                + ", ".join(legacy_fields)
            )

    @staticmethod
    def _validate_provider_capability(spec: Dict[str, Any]) -> None:
        provider_raw = str(spec.get("provider") or spec.get("source") or "").strip().lower()
        provider = _PROVIDER_ALIASES.get(provider_raw)
        if provider is None:
            raise ValueError(f"Unsupported certified provider: {provider_raw or '<missing>'}")
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError(f"{provider} requires the typed execution_stream")
        source = execution_stream.get("source")
        if not isinstance(source, dict) or source.get("kind") != "external":
            raise ValueError(f"{provider} requires an external execution_stream source")
        stream_provider = _PROVIDER_ALIASES.get(
            str(source.get("provider_id") or "").strip().lower()
        )
        if stream_provider != provider:
            raise ValueError("execution_stream provider_id must match the data provider")
        bar_spec = execution_stream.get("bar_spec")
        if not isinstance(bar_spec, dict) or bar_spec.get("aggregation") != "time":
            raise ValueError(f"{provider} only accepts typed time bars")
        step = bar_spec.get("step")
        unit = bar_spec.get("unit")
        if (
            not isinstance(step, int)
            or isinstance(step, bool)
            or (step, str(unit)) not in _PROVIDER_BAR_CAPABILITIES[provider]
        ):
            raise ValueError(
                f"{provider} does not support execution bar_spec "
                f"step={step}, unit={unit}"
            )
        expected_row_key = "session_label" if unit == "day" else "event_timestamp"
        if execution_stream.get("row_key_kind") != expected_row_key:
            raise ValueError(
                f"{provider} {step}-{unit} bars require row_key_kind={expected_row_key}"
            )
        semantics = execution_stream.get("timestamp_semantics")
        if not isinstance(semantics, dict):
            raise ValueError(f"{provider} requires typed timestamp_semantics")
        if semantics.get("timestamp_convention") not in {"bar_open", "bar_close"}:
            raise ValueError(
                f"{provider} requires timestamp_convention=bar_open or bar_close"
            )
        if semantics.get("availability_policy") != "bar_close":
            raise ValueError(f"{provider} requires availability_policy=bar_close")
        session_model = spec.get("session_model")
        if not isinstance(session_model, dict):
            raise ValueError(f"{provider} requires the typed session_model")
        expected_session = (
            ("UTC", "24x7", "CRYPTO_24_7")
            if provider in {"binance", "coinbase"}
            else ("America/New_York", "regular", "XNYS")
        )
        actual_session = (
            str(session_model.get("timezone") or ""),
            str(session_model.get("session_scope") or ""),
            str(session_model.get("calendar_id") or ""),
        )
        if actual_session != expected_session:
            raise ValueError(
                f"{provider} requires calendar_id={expected_session[2]}, "
                f"timezone={expected_session[0]}, session_scope={expected_session[1]}"
            )
        if execution_stream.get("session_scope") != expected_session[1]:
            raise ValueError(
                f"{provider} execution_stream.session_scope must match session_model"
            )
        if provider == "futu":
            if str(spec.get("market") or "US").strip().upper() != "US":
                raise ValueError("futu XNYS certification requires market=US")
            native_adjustment = {"adjustment", "autype"} & set(spec)
            if native_adjustment:
                raise ValueError(
                    "futu certification forbids provider-native adjustment fields"
                )
            native_time = {"frequency", "interval", "ktype"} & set(spec)
            if native_time:
                raise ValueError(
                    "futu certification forbids provider-native time fields"
                )
        if provider == "ibkr":
            if spec.get("use_rth", True) is not True:
                raise ValueError("ibkr XNYS regular-session certification requires use_rth=true")
            native_time = {"frequency", "interval", "bar_size"} & set(spec)
            if native_time:
                raise ValueError(
                    "ibkr certification forbids provider-native time fields"
                )
            if "what_to_show" in spec:
                raise ValueError(
                    "ibkr certification derives what_to_show from adjustment_policy"
                )
        MultiAssetMarketDataLoader._validate_provider_price_basis(
            spec,
            provider=provider,
        )
        capability = provider_timeframe_capability(spec)
        MultiAssetMarketDataLoader._validate_requested_history(
            spec,
            capability=capability,
        )

    @staticmethod
    def _validate_requested_history(
        spec: Dict[str, Any],
        *,
        capability: Dict[str, Any],
    ) -> None:
        start_raw = spec.get("start") or spec.get("start_date")
        end_raw = spec.get("end") or spec.get("end_date")
        if not start_raw:
            return
        start = pd.Timestamp(str(start_raw))
        end = (
            pd.Timestamp(str(end_raw))
            if end_raw
            else pd.Timestamp.now(tz="UTC").tz_localize(None)
        )
        if start.tz is not None:
            start = start.tz_convert("UTC").tz_localize(None)
        if end.tz is not None:
            end = end.tz_convert("UTC").tz_localize(None)
        if end <= start:
            raise ValueError("provider history request requires end after start")
        timeframe = capability["supported_timeframes"][0]
        depth = timeframe["history"]["depth"]
        if depth["kind"] != "bounded":
            return
        amount = int(depth["amount"])
        requested_days = (end - start) / pd.Timedelta(days=1)
        provider = str(capability["provider_id"])
        if requested_days > amount:
            raise ValueError(
                f"{provider} requested history span {requested_days:g} calendar days "
                f"exceeds certified maximum {amount}"
            )
    @staticmethod
    def _validate_provider_price_basis(
        spec: Dict[str, Any],
        *,
        provider: str,
    ) -> None:
        price_basis = str(spec.get("adjustment_policy") or "")
        supported = {
            "yfinance": {"raw", "split_dividend_adjusted"},
            "binance": {"raw"},
            "coinbase": {"raw"},
            "futu": {"raw"},
            "ibkr": {"split_adjusted", "split_dividend_adjusted"},
        }[provider]
        if price_basis not in supported:
            raise ValueError(
                f"{provider} cannot exactly supply adjustment_policy={price_basis or '<missing>'}"
            )

    def _load_wide_frames(
        self,
        spec: Dict[str, Any],
        *,
        config_file_path: Optional[str],
    ) -> Dict[str, pd.DataFrame]:
        frames: Dict[str, pd.DataFrame] = {}
        for field_name, field_spec in spec.items():
            key = str(field_name).strip().lower()
            if key in {
                "provider",
                "source",
                "symbols",
                "start",
                "start_date",
                "end",
                "end_date",
                "start_policy",
                "calendar",
                "timezone",
                "index_kind",
                "session_label_policy",
                "availability_policy",
                "available_time_column",
                "calendar_id",
                "session_model",
                "bar_time",
                "adjustment_policy",
                "point_in_time",
                "stale_value_policy",
                "quality_warnings",
                "benchmark",
                "execution_stream",
                "execution_timeline",
                "session_windows",
                "time_semantics",
                "external_features",
                "features",
            }:
                continue
            if isinstance(field_spec, str):
                field_spec = {"path": field_spec}
            if not isinstance(field_spec, dict):
                raise ValueError(f"backtester.market_data.{field_name} must be a path or object")
            raw_path = str(field_spec.get("path") or "").strip()
            if not raw_path:
                raise ValueError(f"backtester.market_data.{field_name}.path is required")
            resolved = resolve_input_path(
                raw_path,
                repo_root=self.repo_root,
                config_file_path=config_file_path,
            )
            frames[key] = self._read_wide_market_frame(
                resolved.path,
                time_column=str(field_spec.get("time_column") or "Time"),
                index_kind=self._configured_row_key_kind(spec),
            )
        if not frames:
            raise ValueError("No file-backed market data fields were configured")
        return frames

    def _load_execution_timeline(
        self,
        spec: Dict[str, Any],
        *,
        row_key_kind: str,
        config_file_path: Optional[str],
    ) -> pd.DataFrame:
        timeline_spec = spec.get("execution_timeline")
        if not isinstance(timeline_spec, dict):
            raise ValueError(
                "MarketDataBundle v2 requires file-backed execution_timeline"
            )
        raw_path = str(timeline_spec.get("path") or "").strip()
        if not raw_path:
            raise ValueError("execution_timeline.path is required")
        resolved = resolve_input_path(
            raw_path,
            repo_root=self.repo_root,
            config_file_path=config_file_path,
        )
        suffix = resolved.path.suffix.lower()
        if suffix == ".csv":
            frame = pd.read_csv(resolved.path)
        elif suffix in {".parquet", ".pq"}:
            frame = pd.read_parquet(resolved.path)
        else:
            raise ValueError(
                f"Unsupported execution_timeline format: {resolved.path.suffix}"
            )
        time_column = str(timeline_spec.get("time_column") or "Time")
        if not isinstance(frame.index, pd.DatetimeIndex):
            if time_column not in frame.columns:
                raise ValueError(f"execution_timeline is missing {time_column}")
            frame = frame.copy()
            frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce")
            frame = frame.set_index(time_column)
        frame.index = _normalize_time_index(frame.index, index_kind=row_key_kind)
        frame.index.name = "Time"
        return frame

    @staticmethod
    def _configured_row_key_kind(spec: Dict[str, Any]) -> str:
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError("typed execution_stream is required")
        row_key_kind = execution_stream.get("row_key_kind")
        if row_key_kind in {"session_label", "event_timestamp"}:
            return str(row_key_kind)
        raise ValueError("execution_stream.row_key_kind is required")

    def _with_external_features(
        self,
        frames: Dict[str, pd.DataFrame],
        spec: Dict[str, Any],
        *,
        config_file_path: Optional[str],
    ) -> Dict[str, pd.DataFrame]:
        feature_specs = spec.get("external_features")
        if feature_specs is None:
            feature_specs = spec.get("features")
        if not isinstance(feature_specs, list) or not feature_specs:
            return frames
        out = dict(frames)
        symbols = self._symbols_for_feature_frames(spec, out)
        for feature_spec in feature_specs:
            if not isinstance(feature_spec, dict):
                raise ValueError("data.external_features[] must contain objects")
            name = str(feature_spec.get("name") or feature_spec.get("field") or "").strip().lower()
            if not name:
                raise ValueError("data.external_features[].name is required")
            close_frame = out.get("close")
            out[name] = self._load_external_feature_frame(
                feature_spec,
                feature_name=name,
                symbols=symbols,
                base_index=close_frame.index if isinstance(close_frame, pd.DataFrame) else None,
                index_kind=self._configured_row_key_kind(spec),
                config_file_path=config_file_path,
            )
        return out

    @staticmethod
    def _symbols_for_feature_frames(
        spec: Dict[str, Any],
        frames: Dict[str, pd.DataFrame],
    ) -> List[str]:
        raw_symbols = spec.get("symbols", [])
        if isinstance(raw_symbols, list) and raw_symbols:
            return [str(item).strip().upper() for item in raw_symbols if str(item).strip()]
        close = frames.get("close")
        if isinstance(close, pd.DataFrame):
            return [str(column).strip().upper() for column in close.columns if str(column).strip()]
        return []

    def _load_external_feature_frame(
        self,
        feature_spec: Dict[str, Any],
        *,
        feature_name: str,
        symbols: List[str],
        base_index: Optional[pd.Index],
        index_kind: str,
        config_file_path: Optional[str],
    ) -> pd.DataFrame:
        raw_path = str(feature_spec.get("path") or feature_spec.get("uri") or "").strip()
        if not raw_path:
            raise ValueError(f"data.external_features.{feature_name}.path is required")
        resolved = resolve_input_path(
            raw_path,
            repo_root=self.repo_root,
            config_file_path=config_file_path,
        )
        source = self._read_tabular_frame(resolved.path)
        time_column = self._resolve_column(
            source,
            str(feature_spec.get("time_column") or ""),
            ["Time", "Date", "Datetime", "timestamp", "date", "time"],
        )
        if time_column is None:
            raise ValueError(f"data.external_features.{feature_name} requires a time column")
        symbol_column = self._resolve_column(
            source,
            str(feature_spec.get("symbol_column") or feature_spec.get("asset_column") or ""),
            ["Symbol", "Asset", "Ticker", "symbol", "asset", "ticker"],
        )
        value_column = self._resolve_column(
            source,
            str(feature_spec.get("value_column") or feature_spec.get("column") or ""),
            [feature_name, feature_name.upper(), feature_name.lower(), "Value", "value"],
        )
        if value_column is None:
            raise ValueError(f"data.external_features.{feature_name} requires a value column")
        frame = source[[time_column] + ([symbol_column] if symbol_column else []) + [value_column]].copy()
        frame[time_column] = _normalize_time_index(
            frame[time_column],
            index_kind=index_kind,
        )
        frame[value_column] = pd.to_numeric(frame[value_column], errors="coerce")
        frame = frame.dropna(subset=[time_column]).copy()
        if symbol_column:
            frame[symbol_column] = frame[symbol_column].astype(str).str.strip().str.upper()
            wide = frame.pivot_table(
                index=time_column,
                columns=symbol_column,
                values=value_column,
                aggfunc="last",
            )
            if symbols:
                wide = wide.reindex(columns=symbols)
        else:
            series = frame.groupby(time_column, sort=True)[value_column].last()
            columns = symbols or [feature_name.upper()]
            wide = pd.DataFrame({symbol: series for symbol in columns})
        wide = wide.sort_index().apply(pd.to_numeric, errors="coerce")
        if base_index is not None:
            normalized_index = _normalize_time_index(
                base_index,
                index_kind=index_kind,
            )
            wide = wide.reindex(normalized_index).ffill()
            wide.index = normalized_index
        return wide

    @staticmethod
    def _read_tabular_frame(path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return pd.read_csv(path)
        if suffix in {".parquet", ".pq"}:
            return pd.read_parquet(path)
        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(path)
        raise ValueError(f"Unsupported external feature file type: {path.suffix}")

    @staticmethod
    def _resolve_column(
        frame: pd.DataFrame,
        requested: str,
        candidates: List[str],
    ) -> Optional[str]:
        if requested and requested in frame.columns:
            return requested
        lower_map = {str(column).strip().lower(): str(column) for column in frame.columns}
        if requested and requested.strip().lower() in lower_map:
            return lower_map[requested.strip().lower()]
        for candidate in candidates:
            key = str(candidate).strip().lower()
            if key in lower_map:
                return lower_map[key]
        return None

    def _download_yfinance(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        try:
            import yfinance as yf
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError("yfinance is required for provider=yfinance multi-asset data") from exc

        symbols = spec.get("symbols", [])
        if not isinstance(symbols, list) or not symbols:
            raise ValueError("backtester.market_data.symbols is required for yfinance multi-asset data")
        symbols = [str(item).strip().upper() for item in symbols if str(item).strip()]
        start_raw = spec.get("start") or spec.get("start_date")
        end = spec.get("end") or spec.get("end_date")
        if not start_raw:
            raise ValueError("yfinance certified history requires explicit start")
        start = str(start_raw)
        interval = self._provider_interval(spec, provider="yfinance")
        adjustment_policy = str(spec["adjustment_policy"])
        timeout = int(spec.get("timeout") or spec.get("download_timeout") or 30)
        def download(tickers: List[str]) -> pd.DataFrame:
            return yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=adjustment_policy == "split_dividend_adjusted",
                group_by="column",
                progress=False,
                threads=False,
                timeout=timeout,
            )

        def parse_raw(raw_frame: pd.DataFrame, requested_symbols: List[str]) -> Dict[str, pd.DataFrame]:
            frames_out: Dict[str, pd.DataFrame] = {}
            field_map = {
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
            for key, yf_field in field_map.items():
                if isinstance(raw_frame.columns, pd.MultiIndex):
                    if yf_field not in raw_frame.columns.get_level_values(0):
                        continue
                    frame = pd.DataFrame(raw_frame[yf_field]).copy()
                else:
                    if len(requested_symbols) != 1 or yf_field not in raw_frame.columns:
                        continue
                    frame = raw_frame[[yf_field]].copy()
                    frame.columns = requested_symbols
                frame.index = self._normalize_yfinance_row_keys(
                    frame.index,
                    spec=spec,
                )
                frames_out[key] = frame.sort_index().apply(pd.to_numeric, errors="coerce")
            return frames_out

        # yfinance uses process-global state internally. Concurrent multi-asset
        # downloads can return partial/mixed frames, so serialize this call.
        with _YFINANCE_DOWNLOAD_LOCK:
            raw = download(symbols)
        if raw.empty:
            raw = self._download_yfinance_symbols_individually(download, symbols)
        frames = parse_raw(raw, symbols)
        if "close" not in frames:
            raise ValueError("yfinance multi-asset data did not include close prices")
        missing_symbols = [symbol for symbol in symbols if symbol not in frames["close"].columns]
        if missing_symbols:
            retry_raw = self._download_yfinance_symbols_individually(download, missing_symbols)
            retry_frames = parse_raw(retry_raw, missing_symbols)
            for key, retry_frame in retry_frames.items():
                if key in frames:
                    frames[key] = frames[key].join(retry_frame, how="outer")
                else:
                    frames[key] = retry_frame
            missing_symbols = [symbol for symbol in symbols if symbol not in frames["close"].columns]
            if missing_symbols:
                raise ValueError(
                    "yfinance multi-asset data missing requested symbols: "
                    + ", ".join(missing_symbols)
                )
        frames = {key: frame.reindex(columns=symbols) for key, frame in frames.items()}
        start_policy = str(
            spec.get("start_policy") or spec.get("dropna_policy") or ""
        ).strip().lower()
        if start_policy in {"common_available", "first_common", "all_symbols_available"}:
            common_dates = frames["close"].dropna(how="any").index
            if common_dates.empty:
                raise ValueError(
                    f"yfinance data has no common tradable date for symbols={symbols}"
                )
            first_common = pd.Timestamp(common_dates.to_series().iloc[0]).normalize()
            frames = {
                key: frame.loc[frame.index >= first_common].copy()
                for key, frame in frames.items()
            }
        return frames

    @classmethod
    def _normalize_yfinance_row_keys(
        cls,
        values: Any,
        *,
        spec: Dict[str, Any],
    ) -> pd.DatetimeIndex:
        """Convert yfinance's native bar-open keys to the requested typed keys."""

        row_key_kind = cls._configured_row_key_kind(spec)
        row_keys = _normalize_time_index(values, index_kind=row_key_kind)
        if row_key_kind == "session_label":
            return row_keys
        convention = cls._timestamp_convention(spec)
        if convention == "bar_open":
            return row_keys
        execution_stream = spec["execution_stream"]
        session_model = spec.get("session_model")
        if not isinstance(session_model, dict):
            raise ValueError("yfinance intraday data requires the typed session_model")
        return normalize_native_bar_open_keys(
            row_keys,
            bar_spec=execution_stream["bar_spec"],
            timestamp_convention=convention,
            calendar_id=str(session_model.get("calendar_id") or ""),
        )

    def _download_coinbase(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        try:
            import requests
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError("requests is required for provider=coinbase multi-asset data") from exc

        symbols = spec.get("symbols", [])
        if not isinstance(symbols, list) or not symbols:
            raise ValueError("backtester.market_data.symbols is required for coinbase multi-asset data")
        symbols = [str(item).strip().upper() for item in symbols if str(item).strip()]
        start_raw = spec.get("start") or spec.get("start_date")
        end = spec.get("end") or spec.get("end_date")
        if not start_raw:
            raise ValueError("coinbase certified history requires explicit start")
        start = str(start_raw)
        granularity = self._coinbase_granularity(spec)
        api_base = str(spec.get("api_base") or "https://api.exchange.coinbase.com").rstrip("/")
        timeout = int(spec.get("timeout") or spec.get("download_timeout") or 30)

        field_frames: Dict[str, List[pd.Series]] = {
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
        }
        missing: List[str] = []
        for symbol in symbols:
            frame = self._download_coinbase_symbol(
                requests_module=requests,
                api_base=api_base,
                symbol=symbol,
                granularity=granularity,
                row_key_kind=self._configured_row_key_kind(spec),
                timestamp_convention=self._timestamp_convention(spec),
                start=start,
                end=end,
                timeout=timeout,
            )
            if frame.empty:
                missing.append(symbol)
                continue
            for field in field_frames:
                field_frames[field].append(frame[field].rename(symbol))

        if missing:
            raise ValueError("coinbase data missing requested symbols: " + ", ".join(missing))
        if not field_frames["close"]:
            raise ValueError("coinbase returned no close prices")

        frames = {
            field: pd.concat(series_list, axis=1).sort_index()
            for field, series_list in field_frames.items()
        }
        frames = {key: frame.reindex(columns=symbols) for key, frame in frames.items()}
        start_policy = str(
            spec.get("start_policy") or spec.get("dropna_policy") or ""
        ).strip().lower()
        if start_policy in {"common_available", "first_common", "all_symbols_available"}:
            common_dates = frames["close"].dropna(how="any").index
            if common_dates.empty:
                raise ValueError(
                    f"coinbase data has no common tradable date for symbols={symbols}"
                )
            first_common = pd.Timestamp(common_dates.to_series().iloc[0]).normalize()
            frames = {
                key: frame.loc[frame.index >= first_common].copy()
                for key, frame in frames.items()
            }
        return frames

    @staticmethod
    def _download_coinbase_symbol(
        *,
        requests_module: Any,
        api_base: str,
        symbol: str,
        granularity: int,
        row_key_kind: str,
        timestamp_convention: str,
        start: str,
        end: Any,
        timeout: int,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end) if end else pd.Timestamp.now(tz="UTC").tz_localize(None)
        if end_ts <= start_ts:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        rows: List[List[Any]] = []
        url = f"{api_base}/products/{symbol}/candles"
        max_candles = 300
        batch_delta = timedelta(seconds=max_candles * int(granularity))
        current_start = start_ts.to_pydatetime()
        final_end = end_ts.to_pydatetime()

        while current_start < final_end:
            current_end = min(current_start + batch_delta, final_end)
            response = requests_module.get(
                url,
                params={
                    "start": current_start.isoformat(),
                    "end": current_end.isoformat(),
                    "granularity": int(granularity),
                },
                timeout=timeout,
            )
            response.raise_for_status()
            batch = response.json()
            if batch:
                page_start_epoch = int(pd.Timestamp(current_start).timestamp())
                page_end_epoch = int(pd.Timestamp(current_end).timestamp())
                rows.extend(
                    row
                    for row in batch
                    if (
                        isinstance(row, list)
                        and row
                        and page_start_epoch <= int(row[0]) < page_end_epoch
                    )
                )
            current_start = current_end

        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        frame = pd.DataFrame(
            rows,
            columns=["timestamp", "low", "high", "open", "close", "volume"],
        )
        if frame["timestamp"].duplicated().any():
            raise ValueError("coinbase returned duplicate candle timestamps")
        timestamps = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
        if row_key_kind == "session_label":
            frame["Time"] = timestamps.dt.tz_localize(None).dt.normalize()
        elif timestamp_convention == "bar_open":
            frame["Time"] = timestamps
        elif timestamp_convention == "bar_close":
            frame["Time"] = timestamps + pd.to_timedelta(granularity, unit="s")
        else:
            raise ValueError(
                "coinbase requires timestamp_convention=bar_open or bar_close"
            )
        for field in ["open", "high", "low", "close", "volume"]:
            frame[field] = pd.to_numeric(frame[field], errors="coerce")
        return frame.set_index("Time")[["open", "high", "low", "close", "volume"]].sort_index()

    def _download_binance(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        try:
            import requests
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError("requests is required for provider=binance multi-asset data") from exc

        symbols = spec.get("symbols", [])
        if not isinstance(symbols, list) or not symbols:
            raise ValueError("backtester.market_data.symbols is required for binance multi-asset data")
        symbols = [str(item).strip().upper().replace("/", "") for item in symbols if str(item).strip()]
        start_raw = spec.get("start") or spec.get("start_date")
        end = spec.get("end") or spec.get("end_date")
        if not start_raw:
            raise ValueError("binance certified history requires explicit start")
        start = str(start_raw)
        interval = self._provider_interval(spec, provider="binance")
        api_base = str(
            spec.get("api_base") or "https://data-api.binance.vision"
        ).rstrip("/")
        timeout = int(spec.get("timeout") or spec.get("download_timeout") or 30)
        row_key_kind = self._configured_row_key_kind(spec)
        timestamp_convention = self._timestamp_convention(spec)
        bar_duration = (
            self._execution_bar_duration(spec["execution_stream"]["bar_spec"])
            if row_key_kind == "event_timestamp"
            else None
        )

        field_frames: Dict[str, List[pd.Series]] = {
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
        }
        missing: List[str] = []
        for symbol in symbols:
            frame = self._download_binance_symbol(
                requests_module=requests,
                api_base=api_base,
                symbol=symbol,
                interval=interval,
                row_key_kind=row_key_kind,
                timestamp_convention=timestamp_convention,
                bar_duration=bar_duration,
                start=start,
                end=end,
                timeout=timeout,
            )
            if frame.empty:
                missing.append(symbol)
                continue
            for field in field_frames:
                field_frames[field].append(frame[field].rename(symbol))

        if missing:
            raise ValueError("binance data missing requested symbols: " + ", ".join(missing))
        if not field_frames["close"]:
            raise ValueError("binance returned no close prices")

        frames = {
            field: pd.concat(series_list, axis=1).sort_index()
            for field, series_list in field_frames.items()
        }
        frames = {key: frame.reindex(columns=symbols) for key, frame in frames.items()}
        start_policy = str(
            spec.get("start_policy") or spec.get("dropna_policy") or ""
        ).strip().lower()
        if start_policy in {"common_available", "first_common", "all_symbols_available"}:
            common_dates = frames["close"].dropna(how="any").index
            if common_dates.empty:
                raise ValueError(
                    f"binance data has no common tradable date for symbols={symbols}"
                )
            first_common = pd.Timestamp(common_dates.to_series().iloc[0]).normalize()
            frames = {
                key: frame.loc[frame.index >= first_common].copy()
                for key, frame in frames.items()
            }
        return frames

    @staticmethod
    def _download_binance_symbol(
        *,
        requests_module: Any,
        api_base: str,
        symbol: str,
        interval: str,
        row_key_kind: str,
        timestamp_convention: str,
        bar_duration: Optional[pd.Timedelta],
        start: str,
        end: Any,
        timeout: int,
    ) -> pd.DataFrame:
        start_ms = int(pd.Timestamp(start).timestamp() * 1000)
        requested_start_ms = start_ms
        end_ms = int(pd.Timestamp(end).timestamp() * 1000) if end else None
        url = f"{api_base}/api/v3/klines"
        rows: List[List[Any]] = []
        while True:
            params: Dict[str, Any] = {
                "symbol": symbol,
                "interval": interval,
                "startTime": start_ms,
                "limit": 1000,
            }
            if end_ms is not None:
                params["endTime"] = end_ms - 1
            response = requests_module.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            rows.extend(batch)
            next_start = int(batch[-1][0]) + 1
            if next_start <= start_ms:
                break
            start_ms = next_start
            if end_ms is not None and start_ms >= end_ms:
                break
            if end_ms is None and len(batch) < 1000:
                break

        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        frame = pd.DataFrame(
            rows,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "ignore",
            ],
        )
        frame["open_time"] = pd.to_numeric(frame["open_time"], errors="raise")
        frame = frame.loc[frame["open_time"] >= requested_start_ms].copy()
        if end_ms is not None:
            frame = frame.loc[frame["open_time"] < end_ms].copy()
        if frame["open_time"].duplicated().any():
            raise ValueError("binance returned duplicate candle timestamps")
        open_timestamps = pd.to_datetime(frame["open_time"], unit="ms", utc=True)
        if row_key_kind == "session_label":
            frame["Time"] = open_timestamps.dt.tz_localize(None).dt.normalize()
        elif timestamp_convention == "bar_open":
            frame["Time"] = open_timestamps
        elif timestamp_convention == "bar_close":
            if bar_duration is None:
                raise ValueError(
                    "binance event_timestamp bar_close requires a typed bar duration"
                )
            frame["Time"] = open_timestamps + bar_duration
        else:
            raise ValueError(
                "binance requires timestamp_convention=bar_open or bar_close"
            )
        for field in ["open", "high", "low", "close", "volume"]:
            frame[field] = pd.to_numeric(frame[field], errors="coerce")
        return frame.set_index("Time")[["open", "high", "low", "close", "volume"]].sort_index()

    @staticmethod
    def _provider_interval(spec: Dict[str, Any], *, provider: str) -> str:
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError(f"{provider} requires the typed execution_stream")
        bar_spec = execution_stream.get("bar_spec")
        if not isinstance(bar_spec, dict):
            raise ValueError(f"{provider} requires execution_stream.bar_spec")
        if bar_spec.get("aggregation") != "time":
            raise ValueError(f"{provider} only accepts time bars")
        step = bar_spec.get("step")
        unit = bar_spec.get("unit")
        if not isinstance(step, int) or isinstance(step, bool) or step < 1:
            raise ValueError(f"{provider} requires a positive bar_spec.step")
        mappings = {
            "yfinance": {
                (1, "day"): "1d",
            },
            "coinbase": {
                (1, "minute"): "1m",
                (5, "minute"): "5m",
                (15, "minute"): "15m",
                (1, "hour"): "1h",
                (6, "hour"): "6h",
                (1, "day"): "1d",
            },
            "binance": {
                (1, "minute"): "1m",
                (3, "minute"): "3m",
                (5, "minute"): "5m",
                (15, "minute"): "15m",
                (30, "minute"): "30m",
                (1, "hour"): "1h",
                (2, "hour"): "2h",
                (4, "hour"): "4h",
                (6, "hour"): "6h",
                (8, "hour"): "8h",
                (12, "hour"): "12h",
                (1, "day"): "1d",
                (3, "day"): "3d",
                (1, "week"): "1w",
                (1, "month"): "1M",
            },
        }
        interval = mappings.get(provider, {}).get((step, str(unit)))
        if interval is None:
            raise ValueError(
                f"{provider} does not support execution bar_spec step={step}, unit={unit}"
            )
        return interval

    @staticmethod
    def _timestamp_convention(spec: Dict[str, Any]) -> str:
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError("typed execution_stream is required")
        semantics = execution_stream.get("timestamp_semantics")
        if not isinstance(semantics, dict):
            raise ValueError("execution_stream.timestamp_semantics is required")
        convention = str(semantics.get("timestamp_convention") or "")
        if convention not in {"bar_open", "bar_close"}:
            raise ValueError(
                "execution_stream.timestamp_semantics.timestamp_convention is invalid"
            )
        return convention

    @classmethod
    def _coinbase_granularity(cls, spec: Dict[str, Any]) -> int:
        provider_interval = cls._provider_interval(spec, provider="coinbase")
        exact_values = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "6h": 21600,
            "1d": 86400,
        }
        return exact_values[provider_interval]

    @staticmethod
    def _download_yfinance_symbols_individually(download: Any, symbols: List[str]) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        errors: List[str] = []
        for symbol in symbols:
            try:
                raw = download([symbol])
            except Exception as exc:  # pragma: no cover - network dependent
                errors.append(f"{symbol}: {exc}")
                continue
            if raw.empty:
                errors.append(f"{symbol}: empty response")
                continue
            if not isinstance(raw.columns, pd.MultiIndex):
                raw = pd.concat({symbol: raw}, axis=1).swaplevel(0, 1, axis=1)
            frames.append(raw)
        if not frames:
            raise ValueError(
                "yfinance returned no data for symbols="
                + str(symbols)
                + (f"; retries: {'; '.join(errors)}" if errors else "")
            )
        return pd.concat(frames, axis=1)

    @staticmethod
    def _read_wide_market_frame(
        path: Path,
        *,
        time_column: str,
        index_kind: str = "session_label",
    ) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".parquet":
            frame = pd.read_parquet(path)
        elif suffix == ".csv":
            frame = pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported multi-asset market data format: {path.suffix}")
        if isinstance(frame.index, pd.DatetimeIndex):
            out = frame.copy()
        else:
            column = time_column if time_column in frame.columns else None
            if column is None:
                candidates = [
                    col for col in frame.columns if str(col).lower() in {"time", "date", "datetime"}
                ]
                column = str(candidates[0]) if candidates else str(frame.columns[0])
            out = frame.copy()
            out[column] = pd.to_datetime(out[column], errors="coerce")
            out = out.dropna(subset=[column]).set_index(column)
        out.index = _normalize_time_index(out.index, index_kind=index_kind)
        out = out.sort_index()
        return out.apply(pd.to_numeric, errors="coerce")


def _normalize_time_index(values: Any, *, index_kind: str) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(pd.to_datetime(values, errors="coerce"))
    if index.isna().any():
        raise ValueError("market data contains invalid timestamps")
    if index_kind == "session_label":
        return index.tz_localize(None).normalize()
    if index_kind != "event_timestamp":
        raise ValueError(f"Unknown market data index_kind: {index_kind}")
    if index.tz is None:
        raise ValueError("event_timestamp market data requires timezone-aware timestamps")
    return index.tz_convert("UTC")
