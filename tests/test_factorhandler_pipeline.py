import sys
from pathlib import Path

import pandas as pd
import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _factor_frames():
    dates = pd.date_range("2024-01-02", periods=8, freq="B")
    close = pd.DataFrame(
        {
            "AAA": [100, 101, 102, 103, 104, 105, 106, 107],
            "BBB": [100, 102, 104, 106, 108, 110, 112, 114],
            "CCC": [100, 99, 98, 97, 96, 95, 94, 93],
            "DDD": [100, 101, 101, 102, 102, 103, 103, 104],
        },
        index=dates,
    )
    market_cap = pd.DataFrame(
        {
            "AAA": [1000] * len(dates),
            "BBB": [2000] * len(dates),
            "CCC": [500] * len(dates),
            "DDD": [800] * len(dates),
        },
        index=dates,
    )
    book_value = pd.DataFrame(
        {
            "AAA": [500] * len(dates),
            "BBB": [900] * len(dates),
            "CCC": [400] * len(dates),
            "DDD": [300] * len(dates),
        },
        index=dates,
    )
    sector = pd.DataFrame(
        {
            "AAA": ["tech"] * len(dates),
            "BBB": ["tech"] * len(dates),
            "CCC": ["health"] * len(dates),
            "DDD": ["health"] * len(dates),
        },
        index=dates,
    )
    known_at = pd.DataFrame({col: dates for col in close.columns}, index=dates)
    return {
        "close": close,
        "market_cap": market_cap,
        "book_value": book_value,
        "sector": sector,
        "known_at": known_at,
    }


def _pipeline():
    return {
        "schema_version": "factor_pipeline.v1",
        "data_requirements": {
            "price_fields": ["close"],
            "fundamental_fields": ["book_value", "market_cap"],
            "classification_fields": ["sector"],
            "point_in_time_required": True,
        },
        "construction": [
            {
                "name": "momentum_2",
                "family": "momentum",
                "op": "factor.price_momentum",
                "inputs": {"close": "close", "lookback": 2},
                "known_at": "known_at",
            },
            {
                "name": "book_to_market",
                "family": "value",
                "op": "factor.book_to_market",
                "inputs": {"book_value": "book_value", "market_cap": "market_cap"},
                "known_at": "known_at",
            },
        ],
        "preprocessing": [
            {"op": "winsorize", "limits": [0.0, 1.0]},
            {"op": "standardize"},
            {"op": "neutralize", "group_by": ["sector"]},
        ],
        "composite": {
            "method": "equal_weight",
            "inputs": ["momentum_2", "book_to_market"],
            "output": "composite_factor_score",
        },
        "point_in_time": {"known_at_field": "known_at", "fail_on_lookahead": True},
        "cache": {"enabled": False, "storage": "local_parquet"},
        "outputs": {"factor_score_frame": True},
    }


def test_factorhandler_materializes_factor_clean_and_score_frames():
    from factorhandler import FactorHandler

    result = FactorHandler(_factor_frames(), _pipeline()).run()

    assert set(result.factor_frame) == {"momentum_2", "book_to_market"}
    assert set(result.clean_factor_frame) == {"momentum_2", "book_to_market"}
    assert set(result.factor_score_frame) == {"composite_factor_score"}
    score = result.factor_score_frame["composite_factor_score"]
    assert list(score.columns) == ["AAA", "BBB", "CCC", "DDD"]
    assert result.point_in_time_audit["status"] == "passed"
    assert result.factor_quality_report["missing_fields"] == []


def test_factorhandler_neutralize_group_means_are_near_zero():
    from factorhandler import FactorHandler

    result = FactorHandler(_factor_frames(), _pipeline()).run()
    clean = result.clean_factor_frame["book_to_market"].dropna()
    sector = _factor_frames()["sector"].reindex(index=clean.index)

    for date in clean.index:
        for group in ["tech", "health"]:
            members = sector.loc[date][sector.loc[date] == group].index
            assert clean.loc[date, members].mean() == pytest.approx(0.0, abs=1e-12)


def test_factorhandler_rejects_point_in_time_lookahead():
    from factorhandler import FactorHandler, FactorHandlerError

    frames = _factor_frames()
    frames["known_at"] = frames["known_at"].copy()
    frames["known_at"].iloc[2, 0] = pd.Timestamp("2024-02-01")

    with pytest.raises(FactorHandlerError, match="point-in-time lookahead detected"):
        FactorHandler(frames, _pipeline()).run()


def test_factorhandler_volatility_does_not_convert_missing_prices_to_zero_returns():
    from factorhandler import FactorHandler

    frames = _factor_frames()
    frames["close"] = frames["close"].copy()
    frames["close"].iloc[2, 0] = float("nan")
    pipeline = _pipeline()
    pipeline["construction"] = [
        {
            "name": "volatility_2",
            "op": "factor.realized_volatility",
            "inputs": {"close": "close", "lookback": 2},
        }
    ]
    pipeline["preprocessing"] = []
    pipeline["composite"] = {
        "method": "equal_weight",
        "inputs": ["volatility_2"],
        "output": "score",
    }
    pipeline["point_in_time"] = {"fail_on_lookahead": False}

    result = FactorHandler(frames, pipeline).run()

    assert pd.isna(result.factor_frame["volatility_2"].iloc[2, 0])
    assert pd.isna(result.factor_frame["volatility_2"].iloc[3, 0])


def test_factorhandler_requires_explicit_constant_for_zero_fill():
    from factorhandler import FactorHandler, FactorHandlerError

    pipeline = _pipeline()
    pipeline["preprocessing"] = [{"op": "fill_missing", "method": "zero"}]

    with pytest.raises(FactorHandlerError, match="explicit finite value"):
        FactorHandler(_factor_frames(), pipeline).run()


def test_factorhandler_manual_weight_requires_every_input_weight():
    from factorhandler import FactorHandler, FactorHandlerError

    pipeline = _pipeline()
    pipeline["composite"] = {
        "method": "manual_weight",
        "inputs": ["momentum_2", "book_to_market"],
        "weights": {"momentum_2": 1.0},
        "output": "score",
    }

    with pytest.raises(FactorHandlerError, match="every input"):
        FactorHandler(_factor_frames(), pipeline).run()


def test_factorhandler_composite_preserves_missing_input_values():
    from factorhandler import FactorHandler

    frames = _factor_frames()
    frames["book_value"] = frames["book_value"].copy()
    frames["book_value"].iloc[3, 0] = float("nan")
    pipeline = _pipeline()
    pipeline["preprocessing"] = []

    result = FactorHandler(frames, pipeline).run()

    assert pd.isna(result.factor_score_frame["composite_factor_score"].iloc[3, 0])


def test_factorhandler_disk_cache_request_is_ignored(tmp_path):
    from factorhandler import FactorHandler

    pipeline = _pipeline()
    pipeline["cache"] = {"enabled": True, "storage": "local_parquet"}

    first = FactorHandler(_factor_frames(), pipeline, cache_dir=tmp_path).run()
    second = FactorHandler(_factor_frames(), pipeline, cache_dir=tmp_path).run()

    assert first.cache_report["enabled"] is False
    assert first.cache_report["writes"] == 0
    assert second.cache_report["hits"] == 0
    assert first.cache_report["disabled_reason"] == "disk_cache_disabled"
    assert list(tmp_path.iterdir()) == []
    pd.testing.assert_frame_equal(
        first.factor_score_frame["composite_factor_score"],
        second.factor_score_frame["composite_factor_score"],
        check_freq=False,
    )


def test_factorhandler_output_is_sealed_into_market_data_bundle(tmp_path):
    from autorunner.DataLoader_autorunner import DataLoaderAutorunner
    from dataloader.market_data_bundle import build_market_data_bundle

    spec = {
        "provider": "fixture",
        "symbols": ["AAA", "BBB", "CCC", "DDD"],
        "frequency": "1D",
        "calendar": "XNYS",
        "timezone": "America/New_York",
        "point_in_time": True,
    }
    source = build_market_data_bundle(
        _factor_frames(),
        spec=spec,
        output_root=tmp_path / "source",
    )

    enriched = DataLoaderAutorunner._materialize_factor_bundle(
        source,
        factor_pipeline=_pipeline(),
        spec=spec,
        output_root=tmp_path / "enriched",
    )

    frames = enriched.load_frames()
    assert "composite_factor_score" in frames
    assert "momentum_2" in frames
    assert enriched.read_manifest()["lineage"]["point_in_time"] is True


def test_factorhandler_exports_artifacts(tmp_path):
    from factorhandler import FactorArtifactExporter, FactorHandler

    result = FactorHandler(_factor_frames(), _pipeline()).run()
    paths = FactorArtifactExporter(result, tmp_path, run_id="factor_probe").export()

    assert any(path.endswith("_factor-score-frame_composite_factor_score.parquet") for path in paths)
    assert any(path.endswith("_factorhandler-reports.json") for path in paths)
