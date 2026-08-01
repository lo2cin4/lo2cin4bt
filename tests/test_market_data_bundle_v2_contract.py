from __future__ import annotations

from pathlib import Path

import json
import pandas as pd
import pytest
from jsonschema import Draft202012Validator

from dataloader.market_data_bundle import (
    ExternalMarketData,
    ExecutionStreamSpec,
    MarketDataBundle,
    SessionWindow,
    build_market_data_bundle,
    market_data_bundle_content_hash,
)
from dataloader.market_data_loader import MultiAssetMarketDataLoader


def _stream(*, unit: str, row_key_kind: str) -> ExecutionStreamSpec:
    return ExecutionStreamSpec.from_mapping(
        {
            "stream_id": f"execution_{unit}",
            "role": "execution",
            "source": {"kind": "external", "provider_id": "fixture"},
            "session_scope": "regular",
            "row_key_kind": row_key_kind,
            "bar_spec": {
                "aggregation": "time",
                "step": 1,
                "unit": unit,
                "price_type": "last",
                "alignment": "session_open",
            },
            "timestamp_semantics": {
                "timestamp_convention": "bar_close",
                "interval_boundary": "left_open_right_closed",
                "external_execution_sequence_column": (
                    "external_execution_sequence"
                ),
                "bar_open_time_column": "bar_open_timestamp",
                "bar_close_time_column": "bar_close_timestamp",
                "available_time_column": "available_timestamp",
                "session_label_column": "session_label",
                "availability_policy": "bar_close",
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
    )


def _daily_data() -> ExternalMarketData:
    index = pd.DatetimeIndex(["2024-03-11", "2024-07-03"], name="Time")
    base = pd.DataFrame(
        {"QQQ": [438.0, 491.0], "SQQQ": [10.1, 8.2]},
        index=index,
    )
    frames = {
        "open": base,
        "high": base + 1.0,
        "low": base - 1.0,
        "close": base + 0.5,
        "volume": pd.DataFrame(
            {"QQQ": [1000.0, 900.0], "SQQQ": [700.0, 600.0]},
            index=index,
        ),
    }
    timeline = pd.DataFrame(
        {
            "external_execution_sequence": [0, 1],
            "bar_open_timestamp": [
                "2024-03-11T13:30:00Z",
                "2024-07-03T13:30:00Z",
            ],
            "bar_close_timestamp": [
                "2024-03-11T20:00:00Z",
                "2024-07-03T17:00:00Z",
            ],
            "available_timestamp": [
                "2024-03-11T20:00:00Z",
                "2024-07-03T17:00:00Z",
            ],
            "session_label": ["2024-03-11", "2024-07-03"],
        },
        index=index,
    )
    windows = [
        SessionWindow.from_mapping(
            {
                "session_label": "2024-03-11",
                "open_timestamp": "2024-03-11T13:30:00Z",
                "close_timestamp": "2024-03-11T20:00:00Z",
            }
        ),
        SessionWindow.from_mapping(
            {
                "session_label": "2024-07-03",
                "open_timestamp": "2024-07-03T13:30:00Z",
                "close_timestamp": "2024-07-03T17:00:00Z",
            }
        ),
    ]
    return ExternalMarketData(
        frames=frames,
        execution_stream=_stream(unit="day", row_key_kind="session_label"),
        execution_timeline=timeline,
        session_windows=windows,
    )


def _spec() -> dict[str, object]:
    return {
        "provider": "fixture",
        "symbols": ["QQQ", "SQQQ"],
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "adjustment_policy": "split_dividend_adjusted",
    }


def test_direct_daily_bundle_keeps_explicit_external_daily_stream(tmp_path: Path) -> None:
    bundle = build_market_data_bundle(
        _daily_data(),
        spec=_spec(),
        output_root=tmp_path,
    )

    manifest = bundle.read_manifest()
    assert manifest["schema_version"] == "market_data_bundle.v2"
    assert "frequency" not in manifest
    assert "time_semantics" not in manifest
    assert manifest["execution_stream"]["source"] == {
        "kind": "external",
        "provider_id": "fixture",
    }
    assert manifest["execution_stream"]["bar_spec"]["unit"] == "day"
    assert manifest["execution_stream"]["row_key_kind"] == "session_label"
    assert "aggregation_engine" not in manifest["execution_stream"]
    assert manifest["session_windows"][1]["close_timestamp"] == (
        "2024-07-03T17:00:00Z"
    )
    assert set(bundle.load_frames()) == {"open", "high", "low", "close", "volume"}
    assert list(bundle.load_execution_timeline().columns) == [
        "external_execution_sequence",
        "bar_open_timestamp",
        "bar_close_timestamp",
        "available_timestamp",
        "session_label",
    ]
    transported_close = pd.read_parquet(manifest["tables"]["close"]["path"])
    transported_timeline = pd.read_parquet(
        manifest["tables"]["execution_timeline"]["path"]
    )
    assert transported_close.index.tolist() == ["2024-03-11", "2024-07-03"]
    assert pd.api.types.is_string_dtype(transported_close.index.dtype)
    for column in (
        "bar_open_timestamp",
        "bar_close_timestamp",
        "available_timestamp",
    ):
        assert pd.api.types.is_string_dtype(transported_timeline[column].dtype)
        assert all(value.endswith("Z") for value in transported_timeline[column])


def test_daily_bundle_rejects_subdaily_physical_index_before_normalization(
    tmp_path: Path,
) -> None:
    data = _daily_data()
    intraday_index = pd.DatetimeIndex(
        ["2024-03-11T09:30:00", "2024-03-11T10:30:00"],
        name="Time",
    )
    frames = {
        name: frame.set_axis(intraday_index, axis="index")
        for name, frame in data.frames.items()
    }

    with pytest.raises(ValueError, match="index spacing"):
        build_market_data_bundle(
            ExternalMarketData(
                frames=frames,
                execution_stream=data.execution_stream,
                execution_timeline=data.execution_timeline,
                session_windows=data.session_windows,
            ),
            spec=_spec(),
            output_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("field", "related_field", "offset"),
    [
        ("open", "high", 0.01),
        ("close", "high", 0.01),
        ("open", "low", -0.01),
        ("close", "low", -0.01),
    ],
)
def test_bundle_rejects_materially_invalid_ohlcv_price_relationships(
    tmp_path: Path,
    field: str,
    related_field: str,
    offset: float,
) -> None:
    data = _daily_data()
    frames = {name: frame.copy() for name, frame in data.frames.items()}
    frames[field].iloc[0, 0] = frames[related_field].iloc[0, 0] + offset

    with pytest.raises(ValueError, match="OHLCV price relationships"):
        build_market_data_bundle(
            ExternalMarketData(
                frames=frames,
                execution_stream=data.execution_stream,
                execution_timeline=data.execution_timeline,
                session_windows=data.session_windows,
            ),
            spec=_spec(),
            output_root=tmp_path,
        )


@pytest.mark.parametrize(
    ("symbol", "row", "high_value", "close_value"),
    [
        ("QQQ", 0, 60.26078796386718, 60.26078796386719),
        ("SQQQ", 1, 255372.01562499997, 255372.015625),
    ],
)
def test_bundle_accepts_adjusted_ohlcv_one_ulp_rounding(
    tmp_path: Path,
    symbol: str,
    row: int,
    high_value: float,
    close_value: float,
) -> None:
    data = _daily_data()
    frames = {name: frame.copy() for name, frame in data.frames.items()}
    frames["open"].iloc[row, frames["open"].columns.get_loc(symbol)] = high_value - 1.0
    frames["high"].iloc[row, frames["high"].columns.get_loc(symbol)] = high_value
    frames["low"].iloc[row, frames["low"].columns.get_loc(symbol)] = high_value - 2.0
    frames["close"].iloc[row, frames["close"].columns.get_loc(symbol)] = close_value

    bundle = build_market_data_bundle(
        ExternalMarketData(
            frames=frames,
            execution_stream=data.execution_stream,
            execution_timeline=data.execution_timeline,
            session_windows=data.session_windows,
        ),
        spec=_spec(),
        output_root=tmp_path,
    )

    assert bundle.read_manifest()["row_count"] == 2


def test_bundle_rejects_non_finite_ohlcv_value(tmp_path: Path) -> None:
    data = _daily_data()
    frames = {name: frame.copy() for name, frame in data.frames.items()}
    frames["high"].iloc[0, 0] = float("inf")

    with pytest.raises(ValueError, match="high table contains invalid values"):
        build_market_data_bundle(
            ExternalMarketData(
                frames=frames,
                execution_stream=data.execution_stream,
                execution_timeline=data.execution_timeline,
                session_windows=data.session_windows,
            ),
            spec=_spec(),
            output_root=tmp_path,
        )


@pytest.mark.parametrize("value", [None, "unspecified"])
def test_bundle_rejects_missing_or_unknown_adjustment_policy(
    tmp_path: Path,
    value: str | None,
) -> None:
    spec = _spec()
    if value is None:
        spec.pop("adjustment_policy")
    else:
        spec["adjustment_policy"] = value

    with pytest.raises(ValueError, match="typed bar_time.price_model.price_basis"):
        build_market_data_bundle(
            _daily_data(),
            spec=spec,
            output_root=tmp_path,
        )


def test_intraday_bundle_uses_explicit_event_timestamp_row_keys(tmp_path: Path) -> None:
    index = pd.DatetimeIndex(
        ["2024-07-03T13:31:00Z", "2024-07-03T13:32:00Z"],
        name="Time",
    )
    price = pd.DataFrame({"QQQ": [491.0, 491.2]}, index=index)
    timeline = pd.DataFrame(
        {
            "external_execution_sequence": [10, 11],
            "bar_open_timestamp": [
                "2024-07-03T13:30:00Z",
                "2024-07-03T13:31:00Z",
            ],
            "bar_close_timestamp": [
                "2024-07-03T13:31:00Z",
                "2024-07-03T13:32:00Z",
            ],
            "available_timestamp": [
                "2024-07-03T13:31:00Z",
                "2024-07-03T13:32:00Z",
            ],
            "session_label": ["2024-07-03", "2024-07-03"],
        },
        index=index,
    )
    data = ExternalMarketData(
        frames={
            "open": price,
            "high": price + 0.2,
            "low": price - 0.2,
            "close": price + 0.1,
            "volume": pd.DataFrame({"QQQ": [100.0, 200.0]}, index=index),
        },
        execution_stream=_stream(unit="minute", row_key_kind="event_timestamp"),
        execution_timeline=timeline,
        session_windows=[
            SessionWindow.from_mapping(
                {
                    "session_label": "2024-07-03",
                    "open_timestamp": "2024-07-03T13:30:00Z",
                    "close_timestamp": "2024-07-03T17:00:00Z",
                }
            )
        ],
    )
    spec = {
        "provider": "fixture",
        "symbols": ["QQQ"],
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "adjustment_policy": "raw",
    }

    bundle = build_market_data_bundle(data, spec=spec, output_root=tmp_path)

    manifest = bundle.read_manifest()
    assert manifest["execution_stream"]["row_key_kind"] == "event_timestamp"
    assert bundle.primary_frame().index.tz is not None
    transported = pd.read_parquet(manifest["tables"]["close"]["path"])
    assert transported.index.tolist() == [
        "2024-07-03T13:31:00Z",
        "2024-07-03T13:32:00Z",
    ]
    assert pd.api.types.is_string_dtype(transported.index.dtype)


def test_bundle_rejects_any_ohlcv_timeline_row_key_mismatch(tmp_path: Path) -> None:
    data = _daily_data()
    frames = dict(data.frames)
    frames["volume"] = frames["volume"].iloc[:1]

    with pytest.raises(ValueError, match="row count and row keys"):
        build_market_data_bundle(
            ExternalMarketData(
                frames=frames,
                execution_stream=data.execution_stream,
                execution_timeline=data.execution_timeline,
                session_windows=data.session_windows,
            ),
            spec=_spec(),
            output_root=tmp_path,
        )


def test_bundle_rejects_naive_lifecycle_timestamps(tmp_path: Path) -> None:
    data = _daily_data()
    timeline = data.execution_timeline.copy()
    timeline["bar_open_timestamp"] = pd.to_datetime(
        timeline["bar_open_timestamp"]
    ).dt.tz_localize(None)

    with pytest.raises(ValueError, match="timezone-aware"):
        build_market_data_bundle(
            ExternalMarketData(
                frames=data.frames,
                execution_stream=data.execution_stream,
                execution_timeline=timeline,
                session_windows=data.session_windows,
            ),
            spec=_spec(),
            output_root=tmp_path,
        )


def test_file_loader_requires_and_preserves_explicit_execution_metadata(
    tmp_path: Path,
) -> None:
    data = _daily_data()
    spec = _spec()
    for name, frame in data.frames.items():
        path = tmp_path / f"{name}.csv"
        frame.to_csv(path)
        spec[name] = {"path": str(path), "time_column": "Time"}
    timeline_path = tmp_path / "execution_timeline.csv"
    data.execution_timeline.to_csv(timeline_path)
    spec["execution_stream"] = data.execution_stream.to_manifest()
    spec["execution_timeline"] = {
        "path": str(timeline_path),
        "time_column": "Time",
    }
    spec["session_windows"] = [
        window.to_manifest() for window in data.session_windows
    ]

    bundle = MultiAssetMarketDataLoader(repo_root=Path.cwd()).load_bundle(
        spec,
        output_root=tmp_path / "bundles",
    )

    assert bundle.read_manifest()["execution_stream"] == (
        data.execution_stream.to_manifest()
    )
    assert bundle.load_execution_timeline()[
        "external_execution_sequence"
    ].tolist() == [0, 1]


def test_file_loader_rejects_legacy_frequency_instead_of_mapping(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="rejects legacy frequency"):
        MultiAssetMarketDataLoader(repo_root=Path.cwd()).load_bundle(
            {"provider": "file", "frequency": "1D"},
            output_root=tmp_path,
        )


def test_shared_v2_schema_and_example_match_python_canonical_hash() -> None:
    root = Path(__file__).resolve().parents[1]
    schema = json.loads(
        (
            root
            / "backtester"
            / "contracts"
            / "runtime"
            / "market-data-bundle-v2.schema.json"
        ).read_text(encoding="utf-8")
    )
    example = json.loads(
        (
            root
            / "backtester"
            / "contracts"
            / "runtime"
            / "examples"
            / "market-data-bundle-v2.example.json"
        ).read_text(encoding="utf-8")
    )

    Draft202012Validator(schema).validate(example)
    assert market_data_bundle_content_hash(example) == example["content_hash"]


def test_benchmark_must_share_execution_row_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = MultiAssetMarketDataLoader(repo_root=Path.cwd())
    index = pd.DatetimeIndex(["2024-03-11", "2024-03-12"], name="Time")
    frames = {"close": pd.DataFrame({"QQQ": [438.0, 439.0]}, index=index)}
    stream = _stream(unit="day", row_key_kind="session_label").to_manifest()
    spec = {
        "execution_stream": stream,
        "benchmark": {"provider": "fixture", "symbol": "SPY"},
    }
    monkeypatch.setattr(
        loader,
        "load",
        lambda *_args, **_kwargs: {
            "close": pd.DataFrame(
                {"SPY": [500.0]},
                index=pd.DatetimeIndex(["2024-03-11"], name="Time"),
            )
        },
    )

    with pytest.raises(ValueError, match="row keys must exactly match"):
        loader._with_benchmark_close(
            frames,
            spec,
            config_file_path=None,
        )


def test_benchmark_rejects_nested_legacy_interval() -> None:
    loader = MultiAssetMarketDataLoader(repo_root=Path.cwd())
    frames = {
        "close": pd.DataFrame(
            {"QQQ": [438.0]},
            index=pd.DatetimeIndex(["2024-03-11"], name="Time"),
        )
    }

    with pytest.raises(ValueError, match="nested time fields are forbidden"):
        loader._with_benchmark_close(
            frames,
            {
                "execution_stream": _stream(
                    unit="day", row_key_kind="session_label"
                ).to_manifest(),
                "benchmark": {
                    "provider": "fixture",
                    "symbol": "SPY",
                    "interval": "1d",
                },
            },
            config_file_path=None,
        )


def test_frozen_contract_recovery_bundle_resolves_relative_table_paths() -> None:
    root = Path(__file__).resolve().parents[1]
    bundle = MarketDataBundle.open(
        root
        / "verification"
        / "fixtures"
        / "backtest_result_contract_recovery"
        / "market_data_bundle"
        / "mdb-53080332036d4ab4"
        / "manifest.json"
    )

    manifest = bundle.read_manifest()
    frames = bundle.load_frames()
    timeline = bundle.load_execution_timeline()

    assert bundle.content_hash == (
        "53080332036d4ab4e179ba15826e4b44ba495ed951ca07519b52951c579a750d"
    )
    assert manifest["time_range"] == {
        "start": "2017-08-17T00:00:00",
        "end": "2026-07-18T00:00:00",
    }
    assert len(frames["close"]) == 3258
    assert len(timeline) == 3258


def test_relative_bundle_table_path_cannot_escape_bundle_directory(
    tmp_path: Path,
) -> None:
    bundle = build_market_data_bundle(
        _daily_data(),
        spec=_spec(),
        output_root=tmp_path,
    )
    manifest = bundle.read_manifest()
    manifest["tables"]["close"]["path"] = "../close.parquet"
    bundle.manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="escapes bundle directory"):
        bundle.load_frames()
