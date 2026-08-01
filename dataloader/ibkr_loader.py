"""Interactive Brokers market data loader."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from dataloader.provider_bar_time import (
    normalize_native_bar_open_keys,
    typed_intraday_duration,
)


class IBKRMarketDataLoader:
    """Download OHLCV frames through TWS or IB Gateway.

    This adapter uses the optional ``ib_insync`` package as a thin Python
    wrapper around the IBKR API. Runtime still requires TWS / IB Gateway with
    API access enabled and the relevant market data entitlements.
    """

    def load_multi_asset(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        symbols = self._symbols(spec)
        if not symbols:
            raise ValueError("backtester.market_data.symbols is required for provider=ibkr")

        try:
            from ib_insync import IB, Stock, util  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError(
                "IBKR provider requires ib_insync plus a running TWS/IB Gateway API session"
            ) from exc

        host = str(spec.get("host") or "127.0.0.1")
        port = int(spec.get("port") or 7497)
        client_id = int(spec.get("client_id") or 17)
        exchange = str(spec.get("exchange") or "SMART")
        currency = str(spec.get("currency") or "USD")
        duration = self._duration_from_dates(spec)
        if duration is None:
            raise ValueError("IBKR certified history requires explicit start and end")
        bar_size = self._bar_size(spec)
        adjustment_policy = str(spec.get("adjustment_policy") or "")
        what_to_show = {
            "split_adjusted": "TRADES",
            "split_dividend_adjusted": "ADJUSTED_LAST",
        }.get(adjustment_policy)
        if what_to_show is None:
            raise ValueError(
                "IBKR requires adjustment_policy=split_adjusted or "
                "split_dividend_adjusted"
            )
        use_rth = bool(spec.get("use_rth", True))
        end_datetime = str(spec.get("end_datetime") or spec.get("end") or spec.get("end_date") or "")

        close: Dict[str, pd.Series] = {}
        open_: Dict[str, pd.Series] = {}
        high: Dict[str, pd.Series] = {}
        low: Dict[str, pd.Series] = {}
        volume: Dict[str, pd.Series] = {}

        ib = IB()
        ib.connect(host, port, clientId=client_id)
        try:
            for symbol in symbols:
                contract = Stock(symbol, exchange, currency)
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=end_datetime,
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow=what_to_show,
                    useRTH=use_rth,
                    formatDate=2,
                )
                frame = util.df(bars)
                if frame is None or frame.empty:
                    raise ValueError(f"IBKR returned no historical bars for {symbol}")
                frame = self._normalize_ibkr_frame(frame, spec=spec)
                close[symbol] = frame["close"]
                open_[symbol] = frame["open"]
                high[symbol] = frame["high"]
                low[symbol] = frame["low"]
                volume[symbol] = frame["volume"]
        finally:
            ib.disconnect()

        frames = {
            "open": pd.DataFrame(open_).reindex(columns=symbols),
            "high": pd.DataFrame(high).reindex(columns=symbols),
            "low": pd.DataFrame(low).reindex(columns=symbols),
            "close": pd.DataFrame(close).reindex(columns=symbols),
            "volume": pd.DataFrame(volume).reindex(columns=symbols),
        }
        start = spec.get("start") or spec.get("start_date")
        if start:
            start_ts = pd.Timestamp(str(start)).normalize()
            sample_index = pd.DatetimeIndex(frames["close"].index)
            if sample_index.tz is not None and start_ts.tz is None:
                start_ts = start_ts.tz_localize("UTC")
            elif sample_index.tz is None and start_ts.tz is not None:
                start_ts = start_ts.tz_localize(None)
            frames = {key: frame.loc[frame.index >= start_ts].copy() for key, frame in frames.items()}
        return frames

    @staticmethod
    def _normalize_ibkr_frame(
        frame: pd.DataFrame,
        *,
        spec: Dict[str, Any],
    ) -> pd.DataFrame:
        date_col = "date" if "date" in frame.columns else str(frame.columns[0])
        out = frame.copy()
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col])
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError("IBKR requires the typed execution_stream")
        bar_spec = execution_stream.get("bar_spec")
        semantics = execution_stream.get("timestamp_semantics")
        if not isinstance(bar_spec, dict) or not isinstance(semantics, dict):
            raise ValueError("IBKR requires typed bar_spec and timestamp_semantics")
        native = pd.DatetimeIndex(out[date_col])
        if bar_spec.get("unit") == "day":
            row_keys = native.tz_localize(None).normalize()
        else:
            if native.tz is None:
                raise ValueError(
                    "IBKR intraday data requires timezone-aware UTC timestamps"
                )
            row_keys = native.tz_convert("UTC")
            convention = semantics.get("timestamp_convention")
            session_model = spec.get("session_model")
            if not isinstance(session_model, dict):
                raise ValueError("IBKR intraday data requires the typed session_model")
            row_keys = normalize_native_bar_open_keys(
                row_keys,
                bar_spec=bar_spec,
                timestamp_convention=str(convention),
                calendar_id=str(session_model.get("calendar_id") or ""),
            )
        out = out.set_index(row_keys).sort_index()
        out.index.name = "Time"
        for col in ["open", "high", "low", "close", "volume"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        return out[["open", "high", "low", "close", "volume"]]

    @staticmethod
    def _bar_duration(bar_spec: Dict[str, Any]) -> pd.Timedelta:
        return typed_intraday_duration(bar_spec)

    @staticmethod
    def _symbols(spec: Dict[str, Any]) -> List[str]:
        raw = spec.get("symbols", [])
        if not isinstance(raw, list):
            return []
        return [str(item).strip().upper() for item in raw if str(item).strip()]

    @staticmethod
    def _bar_size(spec: Dict[str, Any]) -> str:
        legacy_fields = sorted(
            {"frequency", "interval", "bar_size"} & set(spec)
        )
        if legacy_fields:
            raise ValueError(
                "IBKR interval must derive from the typed execution_stream; "
                "legacy provider time fields are forbidden: "
                + ", ".join(legacy_fields)
            )
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError("IBKR requires the typed execution_stream")
        bar_spec = execution_stream.get("bar_spec")
        if not isinstance(bar_spec, dict) or bar_spec.get("aggregation") != "time":
            raise ValueError("IBKR requires a typed time-aggregated execution_stream.bar_spec")
        step = bar_spec.get("step")
        unit = bar_spec.get("unit")
        if not isinstance(step, int) or isinstance(step, bool) or step <= 0:
            raise ValueError("IBKR execution_stream.bar_spec.step must be a positive integer")
        if not isinstance(unit, str):
            raise ValueError("IBKR execution_stream.bar_spec.unit must be a string")
        mapping = {
            (1, "day"): "1 day",
            (1, "hour"): "1 hour",
            (30, "minute"): "30 mins",
            (15, "minute"): "15 mins",
            (5, "minute"): "5 mins",
            (1, "minute"): "1 min",
        }
        bar_size = mapping.get((step, unit))
        if bar_size is None:
            raise ValueError(
                f"IBKR does not support execution bar_spec step={step}, unit={unit}"
            )
        return bar_size

    @staticmethod
    def _duration_from_dates(spec: Dict[str, Any]) -> str | None:
        start = spec.get("start") or spec.get("start_date")
        end = spec.get("end") or spec.get("end_date")
        if not start:
            return None
        try:
            start_dt = datetime.fromisoformat(str(start))
            end_dt = (
                datetime.fromisoformat(str(end))
                if end
                else datetime.now().astimezone().replace(tzinfo=None)
            )
        except ValueError:
            return None
        seconds = (end_dt - start_dt).total_seconds()
        if seconds <= 0:
            return None
        days = max(math.ceil(seconds / 86_400), 1)
        if days <= 7:
            return f"{days} D"
        if days <= 31:
            return "1 M"
        if days <= 365:
            return "1 Y"
        return None
