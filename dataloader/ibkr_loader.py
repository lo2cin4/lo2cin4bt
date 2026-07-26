"""Interactive Brokers market data loader."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


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
            from ib_insync import IB, Stock, util
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError(
                "IBKR provider requires ib_insync plus a running TWS/IB Gateway API session"
            ) from exc

        host = str(spec.get("host") or "127.0.0.1")
        port = int(spec.get("port") or 7497)
        client_id = int(spec.get("client_id") or 17)
        exchange = str(spec.get("exchange") or "SMART")
        currency = str(spec.get("currency") or "USD")
        duration = str(spec.get("duration") or self._duration_from_dates(spec) or "10 Y")
        bar_size = str(spec.get("bar_size") or self._bar_size(str(spec.get("interval") or "1d")))
        what_to_show = str(spec.get("what_to_show") or "TRADES")
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
                    formatDate=1,
                )
                frame = util.df(bars)
                if frame is None or frame.empty:
                    raise ValueError(f"IBKR returned no historical bars for {symbol}")
                frame = self._normalize_ibkr_frame(frame)
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
            frames = {key: frame.loc[frame.index >= start_ts].copy() for key, frame in frames.items()}
        return frames

    @staticmethod
    def _normalize_ibkr_frame(frame: pd.DataFrame) -> pd.DataFrame:
        date_col = "date" if "date" in frame.columns else str(frame.columns[0])
        out = frame.copy()
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        out = out.dropna(subset=[date_col]).set_index(date_col).sort_index()
        out.index = pd.to_datetime(out.index).tz_localize(None).normalize()
        for col in ["open", "high", "low", "close", "volume"]:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        return out[["open", "high", "low", "close", "volume"]]

    @staticmethod
    def _symbols(spec: Dict[str, Any]) -> List[str]:
        raw = spec.get("symbols", [])
        if not isinstance(raw, list):
            return []
        return [str(item).strip().upper() for item in raw if str(item).strip()]

    @staticmethod
    def _bar_size(interval: str) -> str:
        normalized = interval.strip().lower()
        mapping = {
            "1d": "1 day",
            "1day": "1 day",
            "day": "1 day",
            "1wk": "1 week",
            "1w": "1 week",
            "1h": "1 hour",
            "60m": "1 hour",
            "30m": "30 mins",
            "15m": "15 mins",
            "5m": "5 mins",
            "1m": "1 min",
        }
        return mapping.get(normalized, "1 day")

    @staticmethod
    def _duration_from_dates(spec: Dict[str, Any]) -> str | None:
        start = spec.get("start") or spec.get("start_date")
        end = spec.get("end") or spec.get("end_date")
        if not start or not end:
            return None
        try:
            start_dt = datetime.fromisoformat(str(start))
            end_dt = datetime.fromisoformat(str(end))
        except ValueError:
            return None
        days = max((end_dt - start_dt).days + 1, 1)
        if days < 365:
            return f"{days} D"
        years = max(round(days / 365), 1)
        return f"{years} Y"
