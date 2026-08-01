"""Shared metric annualization and risk-free-rate config helpers."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


DEFAULT_TRADITIONAL_TIME_UNIT = 252
DEFAULT_CRYPTO_TIME_UNIT = 365
DEFAULT_RISK_FREE_RATE = 0.04

CRYPTO_PROVIDERS = {"binance", "coinbase", "crypto", "ccxt"}
CRYPTO_CALENDARS = {"crypto", "crypto_24_7", "24/7", "24x7"}
CRYPTO_SYMBOL_SUFFIXES = ("USDT", "USDC", "BTC", "ETH")


def resolve_metric_config(
    metric_config: Mapping[str, Any] | None,
    *,
    source_config: Mapping[str, Any] | None = None,
    default_enable: bool | None = None,
) -> dict[str, Any]:
    """Return a normalized metricstracker config.

    ``time_unit`` is the annual session count used after Rust projects every
    accepted equity stream to the last row of each canonical session. If the
    config omits it, crypto-like providers/calendars/symbols default to 365 and
    traditional exchange sessions default to 252. ``risk_free_rate`` accepts
    either 0.04 or 4.
    """

    config = dict(metric_config or {})
    source = dict(source_config or {})
    if default_enable is not None:
        config.setdefault("enable_metrics_analysis", bool(default_enable))
    config["time_unit"] = _parse_positive_int(
        config.get("time_unit"),
        default=infer_default_time_unit(source),
        field_name="time_unit",
    )
    config["risk_free_rate"] = _parse_rate(
        config.get("risk_free_rate"),
        default=DEFAULT_RISK_FREE_RATE,
        field_name="risk_free_rate",
    )
    return config


def infer_default_time_unit(config: Mapping[str, Any] | None) -> int:
    if _looks_like_crypto_config(config or {}):
        return DEFAULT_CRYPTO_TIME_UNIT
    return DEFAULT_TRADITIONAL_TIME_UNIT


def _looks_like_crypto_config(config: Mapping[str, Any]) -> bool:
    data = _dict(config.get("data"))
    universe = _dict(config.get("universe"))
    provider = _norm(data.get("provider") or data.get("source"))
    calendar = _norm(data.get("calendar"))
    symbols = _symbols_from_config(data, universe)

    if provider in CRYPTO_PROVIDERS:
        return True
    if calendar in CRYPTO_CALENDARS:
        return True
    return any(_looks_like_crypto_symbol(symbol) for symbol in symbols)


def _symbols_from_config(
    data: Mapping[str, Any],
    universe: Mapping[str, Any],
) -> Sequence[str]:
    values: list[str] = []
    raw_symbols = universe.get("symbols")
    if isinstance(raw_symbols, list):
        values.extend(str(item) for item in raw_symbols if str(item).strip())
    for key in ("symbol", "asset", "ticker"):
        value = data.get(key)
        if value:
            values.append(str(value))
    benchmark = _dict(data.get("benchmark"))
    if benchmark.get("symbol"):
        values.append(str(benchmark["symbol"]))
    return values


def _looks_like_crypto_symbol(symbol: str) -> bool:
    text = symbol.strip().upper().replace("-", "")
    if not text:
        return False
    return text.endswith(CRYPTO_SYMBOL_SUFFIXES) and any(
        token in text for token in ("BTC", "ETH", "USDT", "USDC")
    )


def _parse_positive_int(value: Any, *, default: int, field_name: str) -> int:
    if value is None or value == "":
        return int(default)
    try:
        parsed = int(float(value)) if isinstance(value, str) else int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return parsed


def _parse_rate(value: Any, *, default: float, field_name: str) -> float:
    if value is None or value == "":
        return float(default)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if parsed > 1:
        parsed = parsed / 100.0
    return parsed


def _dict(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()
