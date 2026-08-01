from __future__ import annotations

from types import SimpleNamespace
import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator

from dataloader.futu_loader import FutuMarketDataLoader
from dataloader.ibkr_loader import IBKRMarketDataLoader
from dataloader.market_data_loader import (
    MarketDataContractError,
    MultiAssetMarketDataLoader,
    provider_timeframe_capability,
)
from dataloader.provider_bar_time import normalize_native_bar_open_keys


REPO_ROOT = Path(__file__).resolve().parents[1]
CAPABILITY_SCHEMA = json.loads(
    (
        REPO_ROOT
        / "backtester"
        / "contracts"
        / "runtime"
        / "provider-timeframe-capability-v1.schema.json"
    ).read_text(encoding="utf-8")
)
RUN_FAILURE_SCHEMA = json.loads(
    (
        REPO_ROOT
        / "backtester"
        / "contracts"
        / "runtime"
        / "run-failure-v1.schema.json"
    ).read_text(encoding="utf-8")
)


def _stream(
    provider: str,
    *,
    step: int = 1,
    unit: str = "minute",
    timestamp_convention: str = "bar_close",
) -> dict:
    row_key_kind = "session_label" if unit == "day" else "event_timestamp"
    return {
        "stream_id": f"execution_{step}_{unit}",
        "role": "execution",
        "source": {"kind": "external", "provider_id": provider},
        "session_scope": "24x7" if provider in {"binance", "coinbase"} else "regular",
        "row_key_kind": row_key_kind,
        "bar_spec": {
            "aggregation": "time",
            "step": step,
            "unit": unit,
            "price_type": "last",
            "alignment": "session_open",
        },
        "timestamp_semantics": {
            "timestamp_convention": timestamp_convention,
            "interval_boundary": "left_open_right_closed",
            "availability_policy": "bar_close",
            "external_execution_sequence_column": "external_execution_sequence",
            "bar_open_time_column": "bar_open_timestamp",
            "bar_close_time_column": "bar_close_timestamp",
            "available_time_column": "available_timestamp",
            "session_label_column": "session_label",
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


def _spec(
    provider: str,
    *,
    step: int = 1,
    unit: str = "minute",
    timestamp_convention: str = "bar_close",
) -> dict:
    is_crypto = provider in {"binance", "coinbase"}
    price_basis = "split_adjusted" if provider == "ibkr" else "raw"
    return {
        "provider": provider,
        "symbols": ["QQQ"],
        "adjustment_policy": price_basis,
        "calendar_id": "CRYPTO_24_7" if is_crypto else "XNYS",
        "timezone": "UTC" if is_crypto else "America/New_York",
        "session_model": {
            "calendar_id": "CRYPTO_24_7" if is_crypto else "XNYS",
            "timezone": "UTC" if is_crypto else "America/New_York",
            "session_scope": "24x7" if is_crypto else "regular",
        },
        "execution_stream": _stream(
            provider,
            step=step,
            unit=unit,
            timestamp_convention=timestamp_convention,
        ),
    }


@pytest.mark.parametrize(
    ("provider", "supported", "unsupported"),
    [
        ("yfinance", (1, "day"), (1, "minute")),
        ("binance", (12, "hour"), (90, "minute")),
        ("coinbase", (6, "hour"), (30, "minute")),
        ("futu", (30, "minute"), (2, "minute")),
        ("ibkr", (15, "minute"), (2, "hour")),
    ],
)
def test_provider_capability_accepts_only_exact_typed_bar_specs(
    provider: str,
    supported: tuple[int, str],
    unsupported: tuple[int, str],
) -> None:
    MultiAssetMarketDataLoader._validate_provider_capability(
        _spec(provider, step=supported[0], unit=supported[1])
    )

    with pytest.raises(ValueError, match="does not support execution bar_spec"):
        MultiAssetMarketDataLoader._validate_provider_capability(
            _spec(provider, step=unsupported[0], unit=unsupported[1])
        )


@pytest.mark.parametrize(
    ("provider", "step", "unit", "expected_depth", "expected_pagination"),
    [
        ("yfinance", 1, "day", ("unbounded", None), ("not_supported", None)),
        ("binance", 1, "minute", ("unbounded", None), ("required", 1000)),
        ("coinbase", 1, "minute", ("unbounded", None), ("required", 300)),
        ("futu", 1, "minute", ("unbounded", None), ("required", 1000)),
        ("ibkr", 1, "minute", ("bounded", 1), ("not_supported", None)),
        ("ibkr", 1, "day", ("bounded", 365), ("not_supported", None)),
    ],
)
def test_provider_capability_contract_is_complete_and_schema_valid(
    provider: str,
    step: int,
    unit: str,
    expected_depth: tuple[str, int | None],
    expected_pagination: tuple[str, int | None],
) -> None:
    payload = provider_timeframe_capability(_spec(provider, step=step, unit=unit))

    Draft202012Validator(CAPABILITY_SCHEMA).validate(payload)
    timeframe = payload["supported_timeframes"][0]
    assert (
        timeframe["history"]["depth"]["kind"],
        timeframe["history"]["depth"]["amount"],
    ) == expected_depth
    assert (
        timeframe["history"]["pagination"]["mode"],
        timeframe["history"]["pagination"]["max_bars_per_request"],
    ) == expected_pagination
    assert timeframe["timestamp_semantics"]["availability"] == "bar_close"
    assert timeframe["quality_policy"] == {
        "missing_bar_policy": "fail",
        "duplicate_timestamp_policy": "fail",
        "out_of_order_policy": "fail",
    }


@pytest.mark.parametrize(("step", "unit"), [(1, "minute"), (1, "hour"), (1, "week")])
def test_yfinance_rejects_every_non_daily_execution_bar(
    step: int,
    unit: str,
) -> None:
    with pytest.raises(ValueError, match="does not support execution bar_spec"):
        MultiAssetMarketDataLoader._validate_provider_capability(
            _spec("yfinance", step=step, unit=unit)
        )


def test_provider_quality_gate_rejects_missing_duplicate_and_unordered_rows() -> None:
    spec = _spec("binance")
    valid = pd.date_range("2026-07-01", periods=3, freq="min", tz="UTC")

    cases = [
        (
            pd.DatetimeIndex([valid[0], valid[0], valid[2]]),
            "duplicate_timestamp",
        ),
        (
            pd.DatetimeIndex([valid[0], valid[2], valid[1]]),
            "out_of_order_timestamp",
        ),
        (
            pd.DatetimeIndex([valid[0], valid[2]]),
            "missing_bar",
        ),
    ]
    for index, error_code in cases:
        frames = {
            field: pd.DataFrame({"QQQ": [1.0] * len(index)}, index=index)
            for field in ("open", "high", "low", "close", "volume")
        }
        with pytest.raises(MarketDataContractError) as captured:
            MultiAssetMarketDataLoader._validate_provider_frames(frames, spec=spec)
        assert captured.value.to_payload()["error_code"] == error_code


def test_provider_quality_gate_rejects_null_values_with_ai_readable_payload() -> None:
    spec = _spec("yfinance", unit="day")
    index = pd.DatetimeIndex(["2026-07-01", "2026-07-02"])
    frames = {
        field: pd.DataFrame({"QQQ": [1.0, 2.0]}, index=index)
        for field in ("open", "high", "low", "close", "volume")
    }
    frames["close"].iloc[1, 0] = float("nan")

    with pytest.raises(MarketDataContractError) as captured:
        MultiAssetMarketDataLoader._validate_provider_frames(frames, spec=spec)

    payload = captured.value.to_payload()
    Draft202012Validator(RUN_FAILURE_SCHEMA).validate(payload)
    assert payload == {
        "schema_version": "run_failure.v1",
        "error_code": "missing_value",
        "stage": "dataloader",
        "provider": "yfinance",
        "message": "yfinance close contains 1 missing value(s)",
        "details": {"field": "close", "missing_value_count": 1},
        "action": "fix_data_or_config",
    }


def test_yfinance_daily_quality_gate_rejects_missing_requested_edge_session() -> None:
    spec = _spec("yfinance", unit="day")
    spec.update({"start_date": "2026-07-01", "end_date": "2026-07-07"})
    index = pd.DatetimeIndex(["2026-07-02", "2026-07-06"])
    frames = {
        field: pd.DataFrame({"QQQ": [1.0, 2.0]}, index=index)
        for field in ("open", "high", "low", "close", "volume")
    }

    with pytest.raises(MarketDataContractError) as captured:
        MultiAssetMarketDataLoader._validate_provider_frames(frames, spec=spec)

    assert captured.value.error_code == "missing_bar"
    assert captured.value.details["first_missing_timestamp"].startswith("2026-07-01")


@pytest.mark.parametrize(
    ("step", "unit", "days"),
    [
        (1, "minute", 2),
        (5, "minute", 8),
        (30, "minute", 32),
        (1, "hour", 32),
        (1, "day", 366),
    ],
)
def test_ibkr_single_request_history_span_is_rejected_upfront(
    step: int,
    unit: str,
    days: int,
) -> None:
    spec = _spec("ibkr", step=step, unit=unit)
    spec.update({"start_date": "2026-01-01", "end_date": "2027-01-02"})
    if days != 366:
        spec["end_date"] = (
            pd.Timestamp(spec["start_date"]) + pd.Timedelta(days=days)
        ).strftime("%Y-%m-%d")

    with pytest.raises(ValueError, match="exceeds certified maximum"):
        MultiAssetMarketDataLoader._validate_provider_capability(spec)


@pytest.mark.parametrize("provider", ["futu", "ibkr"])
def test_broker_capability_is_fail_closed_before_optional_runtime(
    provider: str,
) -> None:
    spec = _spec(provider, unit="day")
    spec["session_model"]["timezone"] = "UTC"

    with pytest.raises(
        ValueError,
        match="requires calendar_id=XNYS, timezone=America/New_York",
    ):
        MultiAssetMarketDataLoader._validate_provider_capability(spec)


@pytest.mark.parametrize(
    ("provider", "field", "value", "message"),
    [
        ("futu", "market", "HK", "requires market=US"),
        ("futu", "autype", "qfq", "forbids provider-native adjustment"),
        ("ibkr", "use_rth", False, "requires use_rth=true"),
        ("ibkr", "bar_size", "5 mins", "forbids provider-native time fields"),
        ("ibkr", "what_to_show", "TRADES", "derives what_to_show"),
    ],
)
def test_broker_native_options_cannot_bypass_typed_capability(
    provider: str,
    field: str,
    value: object,
    message: str,
) -> None:
    spec = _spec(provider)
    spec[field] = value

    with pytest.raises(ValueError, match=message):
        MultiAssetMarketDataLoader._validate_provider_capability(spec)


def test_futu_intraday_native_local_bar_open_becomes_exact_utc_bar_close() -> None:
    frame = pd.DataFrame(
        {
            "time_key": ["2024-03-11 09:30:00", "2024-11-29 09:30:00"],
            "open": [100, 101],
            "high": [102, 103],
            "low": [99, 100],
            "close": [101, 102],
            "volume": [10, 11],
        }
    )

    normalized = FutuMarketDataLoader._normalize_futu_frame(
        frame,
        spec=_spec("futu"),
    )

    assert normalized.index.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [
        "2024-03-11T13:31:00Z",
        "2024-11-29T14:31:00Z",
    ]


def test_futu_direct_daily_stays_as_session_labels() -> None:
    frame = pd.DataFrame(
        {
            "time_key": ["2024-03-11 00:00:00", "2024-11-29 00:00:00"],
            "open": [100, 101],
            "high": [102, 103],
            "low": [99, 100],
            "close": [101, 102],
            "volume": [10, 11],
        }
    )

    normalized = FutuMarketDataLoader._normalize_futu_frame(
        frame,
        spec=_spec("futu", unit="day"),
    )

    assert normalized.index.tz is None
    assert normalized.index.strftime("%Y-%m-%d").tolist() == [
        "2024-03-11",
        "2024-11-29",
    ]


def test_ibkr_intraday_utc_bar_open_becomes_exact_utc_bar_close() -> None:
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-03-11T13:30:00Z", "2024-11-29T14:30:00Z"],
                utc=True,
            ),
            "open": [100, 101],
            "high": [102, 103],
            "low": [99, 100],
            "close": [101, 102],
            "volume": [10, 11],
        }
    )

    normalized = IBKRMarketDataLoader._normalize_ibkr_frame(
        frame,
        spec=_spec("ibkr"),
    )

    assert normalized.index.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [
        "2024-03-11T13:31:00Z",
        "2024-11-29T14:31:00Z",
    ]


@pytest.mark.parametrize(
    ("provider", "native_opens", "expected_closes"),
    [
        (
            "futu",
            ["2024-03-11 15:30:00", "2024-11-29 12:30:00"],
            ["2024-03-11T20:00:00Z", "2024-11-29T18:00:00Z"],
        ),
        (
            "ibkr",
            pd.to_datetime(
                ["2024-03-11T19:30:00Z", "2024-11-29T17:30:00Z"],
                utc=True,
            ),
            ["2024-03-11T20:00:00Z", "2024-11-29T18:00:00Z"],
        ),
    ],
)
def test_broker_final_partial_hour_clamps_to_exact_xnys_session_close(
    provider: str,
    native_opens: object,
    expected_closes: list[str],
) -> None:
    time_column = "time_key" if provider == "futu" else "date"
    frame = pd.DataFrame(
        {
            time_column: native_opens,
            "open": [100, 101],
            "high": [102, 103],
            "low": [99, 100],
            "close": [101, 102],
            "volume": [10, 11],
        }
    )

    if provider == "futu":
        normalized = FutuMarketDataLoader._normalize_futu_frame(
            frame,
            spec=_spec("futu", unit="hour"),
        )
    else:
        normalized = IBKRMarketDataLoader._normalize_ibkr_frame(
            frame,
            spec=_spec("ibkr", unit="hour"),
        )

    assert normalized.index.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == (
        expected_closes
    )


@pytest.mark.parametrize(
    ("session_close", "expected_open"),
    [
        ("2024-03-11T20:00:00Z", "2024-03-11T19:30:00Z"),
        ("2024-11-29T18:00:00Z", "2024-11-29T17:30:00Z"),
    ],
)
def test_xnys_bar_close_reconstructs_session_anchored_final_partial_hour(
    session_close: str,
    expected_open: str,
) -> None:
    lifecycle, windows = MultiAssetMarketDataLoader._event_session_lifecycle(
        pd.DatetimeIndex([session_close]),
        calendar_id="XNYS",
        bar_spec=_stream("yfinance", unit="hour")["bar_spec"],
        timestamp_semantics=_stream("yfinance", unit="hour")[
            "timestamp_semantics"
        ],
    )

    assert pd.Timestamp(lifecycle["bar_open_timestamp"][0]).isoformat() == (
        pd.Timestamp(expected_open).isoformat()
    )
    assert pd.Timestamp(lifecycle["bar_close_timestamp"][0]).isoformat() == (
        pd.Timestamp(session_close).isoformat()
    )
    assert windows[0].session_label in {"2024-03-11", "2024-11-29"}


def test_xnys_normal_session_final_partial_90m_uses_session_open_anchor() -> None:
    stream = _stream("yfinance", step=90, unit="minute")
    lifecycle, _ = MultiAssetMarketDataLoader._event_session_lifecycle(
        pd.DatetimeIndex(["2024-03-11T20:00:00Z"]),
        calendar_id="XNYS",
        bar_spec=stream["bar_spec"],
        timestamp_semantics=stream["timestamp_semantics"],
    )

    assert pd.Timestamp(lifecycle["bar_open_timestamp"][0]).isoformat() == (
        pd.Timestamp("2024-03-11T19:30:00Z").isoformat()
    )
    assert pd.Timestamp(lifecycle["bar_close_timestamp"][0]).isoformat() == (
        pd.Timestamp("2024-03-11T20:00:00Z").isoformat()
    )


def test_xnys_native_bar_open_convention_still_rejects_misalignment() -> None:
    stream = _stream(
        "yfinance",
        unit="hour",
        timestamp_convention="bar_open",
    )

    with pytest.raises(ValueError, match="session-open-aligned"):
        normalize_native_bar_open_keys(
            pd.DatetimeIndex(["2024-03-11T14:00:00Z"]),
            bar_spec=stream["bar_spec"],
            timestamp_convention="bar_open",
            calendar_id="XNYS",
        )


def test_xnys_nonfinal_bar_close_rejects_misaligned_reconstruction() -> None:
    stream = _stream("yfinance", unit="hour")

    with pytest.raises(ValueError, match="session-open-aligned"):
        MultiAssetMarketDataLoader._event_session_lifecycle(
            pd.DatetimeIndex(["2024-03-11T16:15:00Z"]),
            calendar_id="XNYS",
            bar_spec=stream["bar_spec"],
            timestamp_semantics=stream["timestamp_semantics"],
        )


def test_ibkr_intraday_rejects_naive_provider_timestamp() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2024-03-11 09:30:00"],
            "open": [100],
            "high": [102],
            "low": [99],
            "close": [101],
            "volume": [10],
        }
    )

    with pytest.raises(
        ValueError,
        match="timezone-aware UTC timestamps",
    ):
        IBKRMarketDataLoader._normalize_ibkr_frame(
            frame,
            spec=_spec("ibkr"),
        )


def test_ibkr_direct_daily_stays_as_session_labels() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2024-03-11", "2024-11-29"],
            "open": [100, 101],
            "high": [102, 103],
            "low": [99, 100],
            "close": [101, 102],
            "volume": [10, 11],
        }
    )

    normalized = IBKRMarketDataLoader._normalize_ibkr_frame(
        frame,
        spec=_spec("ibkr", unit="day"),
    )

    assert normalized.index.tz is None
    assert normalized.index.strftime("%Y-%m-%d").tolist() == [
        "2024-03-11",
        "2024-11-29",
    ]


@pytest.mark.parametrize("provider", ["futu", "ibkr"])
def test_broker_load_bundle_materializes_authoritative_timeline(
    provider: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    index = pd.DatetimeIndex(["2024-11-29T14:31:00Z"], name="Time")
    close = pd.DataFrame({"QQQ": [101.0]}, index=index)
    frames = {
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": pd.DataFrame({"QQQ": [10.0]}, index=index),
    }
    monkeypatch.setattr(
        MultiAssetMarketDataLoader,
        "load",
        lambda *_args, **_kwargs: frames,
    )

    bundle = MultiAssetMarketDataLoader(repo_root=tmp_path).load_bundle(
        _spec(provider),
        output_root=tmp_path / "bundle",
    )

    timeline = bundle.load_execution_timeline()
    assert timeline["bar_open_timestamp"].tolist() == ["2024-11-29T14:30:00Z"]
    assert timeline["bar_close_timestamp"].tolist() == ["2024-11-29T14:31:00Z"]
    assert timeline["session_label"].tolist() == ["2024-11-29"]


def test_binance_direct_daily_preserves_utc_session_label() -> None:
    open_ms = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp() * 1000)

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return [
                [
                    open_ms,
                    "100",
                    "102",
                    "99",
                    "101",
                    "10",
                    open_ms + 86_399_999,
                    "0",
                    1,
                    "0",
                    "0",
                    "0",
                ]
            ]

    class FakeRequests:
        @staticmethod
        def get(*_args, **_kwargs):
            return FakeResponse()

    frame = MultiAssetMarketDataLoader._download_binance_symbol(
        requests_module=FakeRequests,
        api_base="https://example.test",
        symbol="BTCUSDT",
        interval="1d",
        row_key_kind="session_label",
        timestamp_convention="bar_close",
        bar_duration=None,
        start="2024-01-01T00:00:00Z",
        end=None,
        timeout=7,
    )

    assert frame.index.tz is None
    assert frame.index.strftime("%Y-%m-%d").tolist() == ["2024-01-01"]


def test_binance_default_route_uses_market_data_only_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    open_ms = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp() * 1000)

    class FakeResponse:
        def __init__(self, rows):
            self._rows = rows

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._rows

    class FakeRequests:
        @staticmethod
        def get(url, *, params, timeout):
            calls.append(url)
            if len(calls) == 1:
                return FakeResponse(
                    [
                        [
                            open_ms,
                            "100",
                            "102",
                            "99",
                            "101",
                            "10",
                            open_ms + 59_999,
                            "0",
                            1,
                            "0",
                            "0",
                            "0",
                        ]
                    ]
                )
            return FakeResponse([])

    monkeypatch.setitem(__import__("sys").modules, "requests", FakeRequests)
    spec = _spec("binance")
    spec.update(
        {
            "symbols": ["BTCUSDT"],
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": "2024-01-01T00:02:00Z",
        }
    )

    MultiAssetMarketDataLoader(repo_root=tmp_path)._download_binance(spec)  # noqa: SLF001

    assert calls
    assert set(calls) == {
        "https://data-api.binance.vision/api/v3/klines"
    }


def test_ibkr_requests_utc_aware_dates_from_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    class FakeIB:
        def connect(self, *_args, **_kwargs) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def reqHistoricalData(self, _contract, **kwargs):
            calls.append(kwargs)
            return [SimpleNamespace()]

    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-11-29T14:30:00Z"], utc=True),
            "open": [100],
            "high": [102],
            "low": [99],
            "close": [101],
            "volume": [10],
        }
    )
    fake_module = SimpleNamespace(
        IB=FakeIB,
        Stock=lambda *args: args,
        util=SimpleNamespace(df=lambda _bars: frame),
    )
    monkeypatch.setitem(__import__("sys").modules, "ib_insync", fake_module)

    spec = _spec("ibkr")
    spec.update({"start_date": "2024-11-29", "end_date": "2024-11-30"})
    IBKRMarketDataLoader().load_multi_asset(spec)

    assert calls[0]["formatDate"] == 2
    assert calls[0]["whatToShow"] == "TRADES"


def test_ibkr_dividend_adjusted_basis_selects_adjusted_last(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict] = []

    class FakeIB:
        def connect(self, *_args, **_kwargs) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def reqHistoricalData(self, _contract, **kwargs):
            calls.append(kwargs)
            return [SimpleNamespace()]

    frame = pd.DataFrame(
        {
            "date": ["2024-11-29"],
            "open": [100],
            "high": [102],
            "low": [99],
            "close": [101],
            "volume": [10],
        }
    )
    fake_module = SimpleNamespace(
        IB=FakeIB,
        Stock=lambda *args: args,
        util=SimpleNamespace(df=lambda _bars: frame),
    )
    monkeypatch.setitem(__import__("sys").modules, "ib_insync", fake_module)
    spec = _spec("ibkr", unit="day")
    spec["adjustment_policy"] = "split_dividend_adjusted"
    spec.update({"start_date": "2024-11-29", "end_date": "2024-11-30"})

    IBKRMarketDataLoader().load_multi_asset(spec)

    assert calls[0]["whatToShow"] == "ADJUSTED_LAST"


def test_futu_history_pagination_consumes_every_page() -> None:
    pages = [
        (
            0,
            pd.DataFrame(
                {
                    "time_key": ["2024-11-29 09:30:00"],
                    "open": [100],
                    "high": [102],
                    "low": [99],
                    "close": [101],
                    "volume": [10],
                }
            ),
            b"next",
        ),
        (
            0,
            pd.DataFrame(
                {
                    "time_key": ["2024-11-29 09:31:00"],
                    "open": [101],
                    "high": [103],
                    "low": [100],
                    "close": [102],
                    "volume": [11],
                }
            ),
            None,
        ),
    ]
    calls: list[dict] = []

    class FakeQuoteContext:
        def request_history_kline(self, _code, **kwargs):
            calls.append(kwargs)
            return pages.pop(0)

    frame = FutuMarketDataLoader._request_history_kline(
        quote_ctx=FakeQuoteContext(),
        code="US.QQQ",
        start="2024-11-29",
        end="2024-11-29",
        ktype="K_1M",
        autype="NONE",
        ret_ok=0,
        spec=_spec("futu"),
    )

    assert len(frame) == 2
    assert [call["max_count"] for call in calls] == [1000, 1000]
    assert calls[1]["page_req_key"] == b"next"


def test_coinbase_history_pagination_queries_every_300_bar_window() -> None:
    calls: list[dict] = []

    class FakeResponse:
        def __init__(self, rows):
            self._rows = rows

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._rows

    class FakeRequests:
        @staticmethod
        def get(_url, *, params, timeout):
            calls.append({"params": params, "timeout": timeout})
            timestamp = int(pd.Timestamp(params["start"]).timestamp())
            return FakeResponse([[timestamp, 99, 102, 100, 101, 10]])

    frame = MultiAssetMarketDataLoader._download_coinbase_symbol(
        requests_module=FakeRequests,
        api_base="https://example.test",
        symbol="BTC-USD",
        granularity=60,
        row_key_kind="event_timestamp",
        timestamp_convention="bar_close",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T05:01:00Z",
        timeout=7,
    )

    assert len(calls) == 2
    assert len(frame) == 2


def test_coinbase_pagination_filters_provider_boundary_overlap() -> None:
    calls: list[dict] = []
    boundary = int(pd.Timestamp("2024-01-01T05:00:00Z").timestamp())

    class FakeResponse:
        def __init__(self, rows):
            self._rows = rows

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._rows

    class FakeRequests:
        @staticmethod
        def get(_url, *, params, timeout):
            calls.append({"params": params, "timeout": timeout})
            start = int(pd.Timestamp(params["start"]).timestamp())
            if len(calls) == 1:
                return FakeResponse(
                    [
                        [start, 99, 102, 100, 101, 10],
                        [boundary, 100, 103, 101, 102, 11],
                    ]
                )
            return FakeResponse(
                [
                    [boundary, 100, 103, 101, 102, 11],
                    [boundary + 60, 101, 104, 102, 103, 12],
                ]
            )

    frame = MultiAssetMarketDataLoader._download_coinbase_symbol(
        requests_module=FakeRequests,
        api_base="https://example.test",
        symbol="BTC-USD",
        granularity=60,
        row_key_kind="event_timestamp",
        timestamp_convention="bar_open",
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T05:02:00Z",
        timeout=7,
    )

    assert len(calls) == 2
    assert frame.index.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [
        "2024-01-01T00:00:00Z",
        "2024-01-01T05:00:00Z",
        "2024-01-01T05:01:00Z",
    ]


def test_binance_explicit_end_keeps_paging_after_short_batch() -> None:
    calls: list[dict] = []

    class FakeResponse:
        def __init__(self, rows):
            self._rows = rows

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._rows

    class FakeRequests:
        @staticmethod
        def get(_url, *, params, timeout):
            calls.append({"params": params, "timeout": timeout})
            if len(calls) == 1:
                open_ms = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp() * 1000)
                return FakeResponse(
                    [[open_ms, "100", "102", "99", "101", "10", open_ms + 59999, "0", 1, "0", "0", "0"]]
                )
            return FakeResponse([])

    frame = MultiAssetMarketDataLoader._download_binance_symbol(
        requests_module=FakeRequests,
        api_base="https://example.test",
        symbol="BTCUSDT",
        interval="1m",
        row_key_kind="event_timestamp",
        timestamp_convention="bar_close",
        bar_duration=pd.Timedelta(minutes=1),
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:02:00Z",
        timeout=7,
    )

    assert len(calls) == 2
    assert frame.index.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [
        "2024-01-01T00:01:00Z"
    ]


def test_binance_explicit_end_is_exclusive() -> None:
    start_ms = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp() * 1000)
    end_ms = int(pd.Timestamp("2024-01-01T00:02:00Z").timestamp() * 1000)

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return [
                [
                    open_ms,
                    "100",
                    "102",
                    "99",
                    "101",
                    "10",
                    open_ms + 59_999,
                    "0",
                    1,
                    "0",
                    "0",
                    "0",
                ]
                for open_ms in (start_ms, start_ms + 60_000, end_ms)
            ]

    class FakeRequests:
        @staticmethod
        def get(*_args, **_kwargs):
            return FakeResponse()

    frame = MultiAssetMarketDataLoader._download_binance_symbol(
        requests_module=FakeRequests,
        api_base="https://example.test",
        symbol="BTCUSDT",
        interval="1m",
        row_key_kind="event_timestamp",
        timestamp_convention="bar_open",
        bar_duration=pd.Timedelta(minutes=1),
        start="2024-01-01T00:00:00Z",
        end="2024-01-01T00:02:00Z",
        timeout=7,
    )

    assert frame.index.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:01:00Z",
    ]


def test_coinbase_duplicate_candles_fail_instead_of_being_deduplicated() -> None:
    timestamp = int(pd.Timestamp("2024-01-01T00:00:00Z").timestamp())
    row = [timestamp, 99, 102, 100, 101, 10]

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return [row, row]

    class FakeRequests:
        @staticmethod
        def get(*_args, **_kwargs):
            return FakeResponse()

    with pytest.raises(ValueError, match="duplicate candle timestamps"):
        MultiAssetMarketDataLoader._download_coinbase_symbol(
            requests_module=FakeRequests,
            api_base="https://example.test",
            symbol="BTC-USD",
            granularity=60,
            row_key_kind="event_timestamp",
            timestamp_convention="bar_close",
            start="2024-01-01T00:00:00Z",
            end="2024-01-01T00:01:00Z",
            timeout=7,
        )
