"""FUTU OpenAPI market data loader."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


class FutuMarketDataLoader:
    """Download OHLCV frames through a local FUTU OpenD quote gateway."""

    def load_multi_asset(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        symbols = self._symbols(spec)
        if not symbols:
            raise ValueError("backtester.market_data.symbols is required for provider=futu")

        try:
            from futu import AuType, KLType, OpenQuoteContext, RET_OK
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError(
                "FUTU provider requires the futu Python package and a running OpenD gateway"
            ) from exc

        host = str(spec.get("host") or "127.0.0.1")
        port = int(spec.get("port") or 11111)
        start = str(spec.get("start") or spec.get("start_date") or "")
        end = str(spec.get("end") or spec.get("end_date") or "")
        ktype = self._futu_ktype(str(spec.get("interval") or spec.get("frequency") or "1d"), KLType)
        autype = self._futu_autype(str(spec.get("adjustment") or spec.get("autype") or "qfq"), AuType)
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
        time_col = "time_key" if "time_key" in frame.columns else "date"
        frame[time_col] = pd.to_datetime(frame[time_col], errors="coerce")
        frame = frame.dropna(subset=[time_col]).set_index(time_col)
        frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
        for col in ["open", "high", "low", "close", "volume"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame[["open", "high", "low", "close", "volume"]]

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
    def _futu_ktype(interval: str, kl_type: Any) -> Any:
        normalized = interval.strip().lower()
        mapping = {
            "1d": "K_DAY",
            "day": "K_DAY",
            "1wk": "K_WEEK",
            "1w": "K_WEEK",
            "week": "K_WEEK",
            "1mo": "K_MON",
            "1m": "K_1M",
            "5m": "K_5M",
            "15m": "K_15M",
            "30m": "K_30M",
            "60m": "K_60M",
        }
        return getattr(kl_type, mapping.get(normalized, "K_DAY"))

    @staticmethod
    def _futu_autype(value: str, au_type: Any) -> Any:
        normalized = value.strip().lower()
        if normalized in {"none", "raw", "no_adjust"}:
            return getattr(au_type, "NONE")
        if normalized in {"hfq", "backward"}:
            return getattr(au_type, "HFQ")
        return getattr(au_type, "QFQ")
