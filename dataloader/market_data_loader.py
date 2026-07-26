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

from backtester.timeframe_utils import is_subdaily_timeframe
from dataloader.market_data_bundle import MarketDataBundle, build_market_data_bundle
from utils.path_resolver import resolve_input_path


_YFINANCE_DOWNLOAD_LOCK = threading.Lock()


def market_data_spec_from_requirements(requirements: Dict[str, Any]) -> Dict[str, Any]:
    """Compile EngineRequest data requirements into one provider-adapter spec."""

    if not isinstance(requirements, dict) or not requirements:
        raise ValueError("EngineRequest data_requirements are required")
    provider_config = requirements.get("provider_config")
    spec = dict(provider_config) if isinstance(provider_config, dict) else {}
    canonical = {
        "provider": requirements.get("provider"),
        "symbols": requirements.get("symbols"),
        "frequency": requirements.get("frequency"),
        "calendar": requirements.get("calendar"),
        "timezone": requirements.get("timezone"),
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
        self._validate_session_level_spec(spec)

        provider = str(spec.get("provider") or spec.get("source") or "").strip().lower()
        if provider in {"yfinance", "yf"}:
            return self._with_external_features(
                self._download_yfinance(spec),
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"binance", "binance_spot"}:
            return self._with_external_features(
                self._download_binance(spec),
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"coinbase", "coinbase_exchange"}:
            return self._with_external_features(
                self._download_coinbase(spec),
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"futu", "futu_openapi"}:
            from dataloader.futu_loader import FutuMarketDataLoader

            return self._with_external_features(
                FutuMarketDataLoader().load_multi_asset(spec),
                spec,
                config_file_path=config_file_path,
            )
        if provider in {"ibkr", "interactive_brokers", "interactivebrokers"}:
            from dataloader.ibkr_loader import IBKRMarketDataLoader

            return self._with_external_features(
                IBKRMarketDataLoader().load_multi_asset(spec),
                spec,
                config_file_path=config_file_path,
            )

        return self._with_external_features(
            self._load_wide_frames(spec, config_file_path=config_file_path),
            spec,
            config_file_path=config_file_path,
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
        frames = self.load(spec, config_file_path=config_file_path)
        frames = self._with_benchmark_close(
            frames,
            spec,
            config_file_path=config_file_path,
        )
        bundle = build_market_data_bundle(frames, spec=spec, output_root=output_root)
        return bundle

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
        out = dict(frames)
        out["benchmark_close"] = benchmark_close[[symbol]].copy()
        return out

    @staticmethod
    def _validate_session_level_spec(spec: Dict[str, Any]) -> None:
        for field_name in ("frequency", "interval"):
            value = spec.get(field_name)
            if is_subdaily_timeframe(value):
                raise ValueError(
                    "multi-asset market data loader currently supports session-level bars only; "
                    f"sub-daily {field_name}={value!r} is not supported"
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
                "interval",
                "frequency",
                "start_policy",
                "calendar",
                "timezone",
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
            )
        if not frames:
            raise ValueError("No file-backed market data fields were configured")
        return frames

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
        frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce")
        frame[value_column] = pd.to_numeric(frame[value_column], errors="coerce")
        frame = frame.dropna(subset=[time_column]).copy()
        frame[time_column] = frame[time_column].dt.tz_localize(None).dt.normalize()
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
            normalized_index = pd.to_datetime(base_index, errors="coerce").tz_localize(None).normalize()
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
        start = str(spec.get("start") or spec.get("start_date") or "1990-01-01")
        end = spec.get("end") or spec.get("end_date")
        interval = str(spec.get("interval") or "1d")
        timeout = int(spec.get("timeout") or spec.get("download_timeout") or 30)
        def download(tickers: List[str]) -> pd.DataFrame:
            return yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True,
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
                frame.index = pd.to_datetime(frame.index).tz_localize(None).normalize()
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

    def _download_coinbase(self, spec: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        try:
            import requests
        except Exception as exc:  # pragma: no cover - optional dependency guard
            raise RuntimeError("requests is required for provider=coinbase multi-asset data") from exc

        symbols = spec.get("symbols", [])
        if not isinstance(symbols, list) or not symbols:
            raise ValueError("backtester.market_data.symbols is required for coinbase multi-asset data")
        symbols = [str(item).strip().upper() for item in symbols if str(item).strip()]
        start = str(spec.get("start") or spec.get("start_date") or "2017-01-01")
        end = spec.get("end") or spec.get("end_date")
        granularity = self._coinbase_granularity(str(spec.get("interval") or spec.get("frequency") or "1d"))
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
                rows.extend(batch)
            current_start = current_end

        if not rows:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        frame = pd.DataFrame(
            rows,
            columns=["timestamp", "low", "high", "open", "close", "volume"],
        ).drop_duplicates(subset=["timestamp"])
        frame["Time"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True).dt.tz_localize(None).dt.normalize()
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
        start = str(spec.get("start") or spec.get("start_date") or "2017-01-01")
        end = spec.get("end") or spec.get("end_date")
        interval = self._binance_interval(str(spec.get("interval") or spec.get("frequency") or "1d"))
        api_base = str(spec.get("api_base") or "https://api.binance.com").rstrip("/")
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
            frame = self._download_binance_symbol(
                requests_module=requests,
                api_base=api_base,
                symbol=symbol,
                interval=interval,
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
        start: str,
        end: Any,
        timeout: int,
    ) -> pd.DataFrame:
        start_ms = int(pd.Timestamp(start).timestamp() * 1000)
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
                params["endTime"] = end_ms
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
            if len(batch) < 1000:
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
        frame["Time"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True).dt.tz_localize(None).dt.normalize()
        for field in ["open", "high", "low", "close", "volume"]:
            frame[field] = pd.to_numeric(frame[field], errors="coerce")
        return frame.set_index("Time")[["open", "high", "low", "close", "volume"]].sort_index()

    @staticmethod
    def _binance_interval(interval: str) -> str:
        normalized = interval.strip()
        aliases = {
            "1D": "1d",
            "1DAY": "1d",
            "D": "1d",
            "DAILY": "1d",
            "1H": "1h",
            "1M": "1m",
        }
        return aliases.get(normalized.upper(), normalized.lower())

    @staticmethod
    def _coinbase_granularity(interval: str) -> int:
        normalized = interval.strip().lower()
        aliases = {
            "1m": 60,
            "60": 60,
            "5m": 300,
            "300": 300,
            "15m": 900,
            "900": 900,
            "1h": 3600,
            "60m": 3600,
            "3600": 3600,
            "6h": 21600,
            "21600": 21600,
            "1d": 86400,
            "1day": 86400,
            "day": 86400,
            "daily": 86400,
            "86400": 86400,
        }
        if normalized not in aliases:
            raise ValueError(f"Unsupported coinbase interval: {interval}")
        return aliases[normalized]

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
    def _read_wide_market_frame(path: Path, *, time_column: str) -> pd.DataFrame:
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
        out.index = pd.to_datetime(out.index).tz_localize(None).normalize()
        out = out.sort_index()
        return out.apply(pd.to_numeric, errors="coerce")
