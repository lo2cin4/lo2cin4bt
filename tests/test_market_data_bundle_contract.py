from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from jsonschema import Draft202012Validator

from dataloader.market_data_bundle import (
    ExternalMarketData,
    ExecutionStreamSpec,
    SessionWindow,
    build_market_data_bundle,
)
from dataloader.market_data_loader import MultiAssetMarketDataLoader


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "market-data-bundle-v2.schema.json"
)
EXAMPLE_PATH = (
    REPO_ROOT
    / "backtester"
    / "contracts"
    / "runtime"
    / "examples"
    / "market-data-bundle-v2.example.json"
)


def _frames() -> dict[str, pd.DataFrame]:
    index = pd.DatetimeIndex(["2024-01-02", "2024-01-03"], name="Time")
    close = pd.DataFrame(
        {"AAA": [100.0, 101.0], "BBB": [200.0, 202.0]},
        index=index,
    )
    return {
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "volume": pd.DataFrame(
            {"AAA": [1000.0, 1100.0], "BBB": [2000.0, 2100.0]},
            index=index,
        ),
        "factor": pd.DataFrame(
            {"AAA": [1.0, 1.5], "BBB": [2.0, 2.5]},
            index=index,
        ),
    }


def _stream() -> ExecutionStreamSpec:
    return ExecutionStreamSpec.from_mapping(
        {
            "stream_id": "execution_daily",
            "role": "execution",
            "source": {"kind": "external", "provider_id": "fixture"},
            "session_scope": "regular",
            "row_key_kind": "session_label",
            "bar_spec": {
                "aggregation": "time",
                "step": 1,
                "unit": "day",
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


def _data(
    frames: dict[str, pd.DataFrame] | None = None,
) -> ExternalMarketData:
    index = pd.DatetimeIndex(["2024-01-02", "2024-01-03"], name="Time")
    timeline = pd.DataFrame(
        {
            "external_execution_sequence": [0, 1],
            "bar_open_timestamp": [
                "2024-01-02T14:30:00Z",
                "2024-01-03T14:30:00Z",
            ],
            "bar_close_timestamp": [
                "2024-01-02T21:00:00Z",
                "2024-01-03T21:00:00Z",
            ],
            "available_timestamp": [
                "2024-01-02T21:00:00Z",
                "2024-01-03T21:00:00Z",
            ],
            "session_label": ["2024-01-02", "2024-01-03"],
        },
        index=index,
    )
    return ExternalMarketData(
        frames=frames or _frames(),
        execution_stream=_stream(),
        execution_timeline=timeline,
        session_windows=[
            SessionWindow.from_mapping(
                {
                    "session_label": label,
                    "open_timestamp": f"{label}T14:30:00Z",
                    "close_timestamp": f"{label}T21:00:00Z",
                }
            )
            for label in ("2024-01-02", "2024-01-03")
        ],
    )


def _spec() -> dict[str, object]:
    return {
        "provider": "fixture",
        "symbols": ["AAA", "BBB"],
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "point_in_time": True,
        "adjustment_policy": "split_dividend_adjusted",
    }


def test_builds_schema_valid_content_addressed_bundle(tmp_path: Path) -> None:
    bundle = build_market_data_bundle(_data(), spec=_spec(), output_root=tmp_path)
    manifest = bundle.read_manifest()

    Draft202012Validator(json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))).validate(
        manifest
    )
    assert manifest["bundle_id"].startswith("mdb-")
    assert bundle.content_hash == manifest["content_hash"]
    assert manifest["symbols"] == ["AAA", "BBB"]
    assert manifest["row_count"] == 2
    assert manifest["execution_stream"]["row_key_kind"] == "session_label"
    assert "frequency" not in manifest
    assert "time_semantics" not in manifest
    assert manifest["quality"]["missing_value_policy"] == "fail"
    assert manifest["tables"]["close"]["role"] == "bars"
    assert manifest["tables"]["factor"]["role"] == "features"
    assert manifest["tables"]["execution_timeline"]["role"] == "bar_timeline"

    loaded = bundle.load_frames()
    expected = _frames()
    pd.testing.assert_frame_equal(loaded["close"], expected["close"], check_freq=False)
    pd.testing.assert_frame_equal(loaded["factor"], expected["factor"], check_freq=False)


def test_benchmark_does_not_expand_trading_universe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    loader = MultiAssetMarketDataLoader(repo_root=REPO_ROOT)
    frames = _frames()

    monkeypatch.setattr(
        loader,
        "load",
        lambda *_args, **_kwargs: {
            "close": pd.DataFrame(
                {"SPY": [500.0, 501.0]},
                index=frames["close"].index,
            )
        },
    )
    with_benchmark = loader._with_benchmark_close(
        frames,
        {
            **_spec(),
            "execution_stream": _stream().to_manifest(),
            "benchmark": {"provider": "fixture", "symbol": "SPY"},
        },
        config_file_path=None,
    )
    bundle = build_market_data_bundle(
        _data(with_benchmark),
        spec=_spec(),
        output_root=tmp_path,
    )

    manifest = bundle.read_manifest()
    assert manifest["symbols"] == ["AAA", "BBB"]
    assert manifest["tables"]["benchmark_close"]["role"] == "benchmarks"
    assert manifest["tables"]["benchmark_close"]["columns"] == ["SPY"]


def test_same_content_has_same_bundle_hash_across_output_roots(tmp_path: Path) -> None:
    first = build_market_data_bundle(
        _data(), spec=_spec(), output_root=tmp_path / "first"
    )
    second = build_market_data_bundle(
        _data(), spec=_spec(), output_root=tmp_path / "second"
    )

    assert first.content_hash == second.content_hash
    assert first.bundle_id == second.bundle_id


def test_shared_manifest_example_matches_python_canonical_hash() -> None:
    mod = __import__("dataloader.market_data_bundle", fromlist=["dummy"])
    manifest = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))

    mod.validate_market_data_bundle_manifest(manifest)
    assert mod.market_data_bundle_content_hash(manifest) == manifest["content_hash"]


def test_bundle_rejects_duplicate_timestamps(tmp_path: Path) -> None:
    frames = _frames()
    frames["close"] = pd.concat([frames["close"], frames["close"].iloc[[0]]])

    with pytest.raises(ValueError, match="duplicate timestamps"):
        build_market_data_bundle(_data(frames), spec=_spec(), output_root=tmp_path)


def test_bundle_rejects_fully_missing_provider_rows_instead_of_dropping(
    tmp_path: Path,
) -> None:
    frames = _frames()
    frames["close"].iloc[1] = [float("nan"), float("nan")]

    with pytest.raises(ValueError, match="contains missing values"):
        build_market_data_bundle(_data(frames), spec=_spec(), output_root=tmp_path)


def test_bundle_rejects_partial_invalid_runtime_prices(tmp_path: Path) -> None:
    frames = _frames()
    frames["close"].iloc[1, 0] = float("nan")

    with pytest.raises(ValueError, match="contains missing values"):
        build_market_data_bundle(_data(frames), spec=_spec(), output_root=tmp_path)


def test_bundle_detects_table_tampering(tmp_path: Path) -> None:
    bundle = build_market_data_bundle(_data(), spec=_spec(), output_root=tmp_path)
    close_path = Path(bundle.read_manifest()["tables"]["close"]["path"])
    tampered = pd.read_parquet(close_path)
    tampered.iloc[0, 0] = 999.0
    tampered.to_parquet(close_path)

    with pytest.raises(ValueError, match="content hash mismatch"):
        bundle.load_frames()


def test_bundle_rejects_engine_request_symbol_mismatch(tmp_path: Path) -> None:
    request_mod = __import__("backtester.EngineRequest_backtester", fromlist=["dummy"])
    config = json.loads(
        (
            REPO_ROOT
            / "backtester"
            / "contracts"
            / "strategy"
            / "examples"
            / "strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json"
        ).read_text(encoding="utf-8")
    )
    request = request_mod.build_engine_request(config)
    bundle = build_market_data_bundle(_data(), spec=_spec(), output_root=tmp_path)

    with pytest.raises(ValueError, match="symbols do not match"):
        bundle.validate_against_engine_request(request)
