"""FUTU OpenAPI market data loader."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from dataloader.provider_bar_time import (
    normalize_native_bar_open_keys,
    typed_intraday_duration,
)


class FutuMarketDataLoader:
    """Download OHLCV frames through a local FUTU OpenD quote gateway."""

    def load_multi_asset(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        symbols = self._symbols(spec)
        if not symbols:
            raise ValueError("backtester.market_data.symbols is required for provider=futu")

        try:
            from futu import (  # type: ignore[import-not-found]
                AuType,
                KLType,
                OpenQuoteContext,
                RET_OK,
            )
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError(
                "FUTU provider requires the futu Python package and a running OpenD gateway"
            ) from exc

        host = str(spec.get("host") or "127.0.0.1")
        port = int(spec.get("port") or 11111)
        start = str(spec.get("start") or spec.get("start_date") or "")
        end = str(spec.get("end") or spec.get("end_date") or "")
        if not start:
            raise ValueError("FUTU certified history requires explicit start")
        if not end:
            end = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
        ktype = self._futu_ktype(spec, KLType)
        autype = self._futu_autype(
            str(spec.get("adjustment_policy") or ""),
            AuType,
        )
        market = str(spec.get("market") or "US").upper()
        symbol_map_raw = spec.get("symbol_map")
        symbol_map: Dict[str, Any] = symbol_map_raw if isinstance(symbol_map_raw, dict) else {}

        close: Dict[str, pd.Series] = {}
        open_: Dict[str, pd.Series] = {}
        high: Dict[str, pd.Series] = {}
        low: Dict[str, pd.Series] = {}
        volume: Dict[str, pd.Series] = {}

        quote_ctx = OpenQuoteContext(host=host, port=port)
        try:
            for symbol in symbols:
                futu_code = str(symbol_map.get(symbol) or self._to_futu_code(symbol, market))
                frame = self._request_history_kline(
                    quote_ctx=quote_ctx,
                    code=futu_code,
                    start=start or None,
                    end=end or None,
                    ktype=ktype,
                    autype=autype,
                    ret_ok=RET_OK,
                    spec=spec,
                )
                frame = frame.sort_index()
                close[symbol] = frame["close"]
                open_[symbol] = frame["open"]
                high[symbol] = frame["high"]
                low[symbol] = frame["low"]
                volume[symbol] = frame["volume"]
        finally:
            quote_ctx.close()

        return {
            "open": pd.DataFrame(open_).reindex(columns=symbols),
            "high": pd.DataFrame(high).reindex(columns=symbols),
            "low": pd.DataFrame(low).reindex(columns=symbols),
            "close": pd.DataFrame(close).reindex(columns=symbols),
            "volume": pd.DataFrame(volume).reindex(columns=symbols),
        }

    @staticmethod
    def _request_history_kline(
        *,
        quote_ctx: Any,
        code: str,
        start: str | None,
        end: str | None,
        ktype: Any,
        autype: Any,
        ret_ok: Any,
        spec: Dict[str, Any],
    ) -> pd.DataFrame:
        pages: List[pd.DataFrame] = []
        page_req_key = None
        while True:
            ret, data, page_req_key = quote_ctx.request_history_kline(
                code,
                start=start,
                end=end,
                ktype=ktype,
                autype=autype,
                max_count=1000,
                page_req_key=page_req_key,
            )
            if ret != ret_ok:
                raise RuntimeError(f"FUTU request_history_kline failed for {code}: {data}")
            if isinstance(data, pd.DataFrame) and not data.empty:
                pages.append(data.copy())
            if page_req_key is None:
                break
        if not pages:
            raise ValueError(f"FUTU returned no kline data for {code}")
        frame = pd.concat(pages, ignore_index=True)
        return FutuMarketDataLoader._normalize_futu_frame(frame, spec=spec)

    @staticmethod
    def _normalize_futu_frame(
        frame: pd.DataFrame,
        *,
        spec: Dict[str, Any],
    ) -> pd.DataFrame:
        time_col = "time_key" if "time_key" in frame.columns else "date"
        frame[time_col] = pd.to_datetime(frame[time_col], errors="coerce")
        frame = frame.dropna(subset=[time_col])
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError("FUTU requires the typed execution_stream")
        bar_spec = execution_stream.get("bar_spec")
        semantics = execution_stream.get("timestamp_semantics")
        if not isinstance(bar_spec, dict) or not isinstance(semantics, dict):
            raise ValueError("FUTU requires typed bar_spec and timestamp_semantics")
        if bar_spec.get("unit") == "day":
            row_keys = pd.DatetimeIndex(frame[time_col]).tz_localize(None).normalize()
        else:
            session_model = spec.get("session_model")
            if not isinstance(session_model, dict):
                raise ValueError("FUTU intraday data requires the typed session_model")
            timezone = str(session_model.get("timezone") or "")
            native = pd.DatetimeIndex(frame[time_col])
            if native.tz is None:
                native = native.tz_localize(
                    timezone,
                    ambiguous="raise",
                    nonexistent="raise",
                )
            row_keys = native.tz_convert("UTC")
            convention = semantics.get("timestamp_convention")
            row_keys = normalize_native_bar_open_keys(
                row_keys,
                bar_spec=bar_spec,
                timestamp_convention=str(convention),
                calendar_id=str(session_model.get("calendar_id") or ""),
            )
        frame = frame.set_index(row_keys)
        frame.index.name = "Time"
        for col in ["open", "high", "low", "close", "volume"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame[["open", "high", "low", "close", "volume"]].sort_index()

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
    def _to_futu_code(symbol: str, market: str) -> str:
        symbol = str(symbol).strip().upper()
        if "." in symbol:
            return symbol
        return f"{market}.{symbol}"

    @staticmethod
    def _futu_ktype(spec: Dict[str, Any], kl_type: Any) -> Any:
        legacy_fields = sorted({"frequency", "interval", "ktype"} & set(spec))
        if legacy_fields:
            raise ValueError(
                "FUTU interval must derive from the typed execution_stream; "
                "legacy provider time fields are forbidden: "
                + ", ".join(legacy_fields)
            )
        execution_stream = spec.get("execution_stream")
        if not isinstance(execution_stream, dict):
            raise ValueError("FUTU requires the typed execution_stream")
        bar_spec = execution_stream.get("bar_spec")
        if not isinstance(bar_spec, dict) or bar_spec.get("aggregation") != "time":
            raise ValueError("FUTU requires a typed time-aggregated execution_stream.bar_spec")
        step = bar_spec.get("step")
        unit = bar_spec.get("unit")
        if not isinstance(step, int) or isinstance(step, bool) or step <= 0:
            raise ValueError("FUTU execution_stream.bar_spec.step must be a positive integer")
        if not isinstance(unit, str):
            raise ValueError("FUTU execution_stream.bar_spec.unit must be a string")
        mapping = {
            (1, "day"): "K_DAY",
            (1, "minute"): "K_1M",
            (5, "minute"): "K_5M",
            (15, "minute"): "K_15M",
            (30, "minute"): "K_30M",
            (1, "hour"): "K_60M",
        }
        ktype_name = mapping.get((step, unit))
        if ktype_name is None:
            raise ValueError(
                f"FUTU does not support execution bar_spec step={step}, unit={unit}"
            )
        return getattr(kl_type, ktype_name)

    @staticmethod
    def _futu_autype(value: str, au_type: Any) -> Any:
        normalized = value.strip().lower()
        if normalized == "raw":
            return getattr(au_type, "NONE")
        raise ValueError(
            "FUTU certification supports adjustment_policy=raw only"
        )
