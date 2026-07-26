from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from verification.scripts.compare_utils import ensure_directory, write_json

IMPORT_ROOT = PROJECT_ROOT / "records" / "dataloader" / "import"
FIXTURE_ROOT = PROJECT_ROOT / "verification" / "fixtures"


def _record_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _synthetic_predictor_example() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-17", periods=8, freq="D").strftime("%Y-%m-%d"),
            "X": [1.0, 1.2, 1.1, 1.4, 1.3, 1.5, 1.7, 1.6],
        }
    )


def _synthetic_etf_balance() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [
                "16/01/2024",
                "17/01/2024",
                "18/01/2024",
                "19/01/2024",
                "20/01/2024",
                "21/01/2024",
                "22/01/2024",
                "23/01/2024",
            ],
            "total": [
                651739.7735,
                660542.5417,
                660962.5562,
                649567.6502,
                649567.6502,
                649567.6502,
                661154.0,
                655692.1792,
            ],
        }
    )


def _synthetic_boxer_score() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.date_range("2021-01-03", periods=12, freq="D").strftime("%Y-%m-%d"),
            "BTC_Price": [
                33000.0,
                32050.0,
                34025.0,
                36800.0,
                39450.0,
                40500.0,
                40200.0,
                38300.0,
                35600.0,
                33750.0,
                34500.0,
                37300.0,
            ],
            "BOXER_Score": [0.52, 0.49, 0.56, 0.62, 0.68, 0.71, 0.69, 0.63, 0.58, 0.54, 0.55, 0.6],
        }
    )


def _synthetic_predicting_5min() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "time": pd.date_range("2025-09-24 00:00:00", periods=12, freq="5min").strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "Crypto Open Interest": [
                12500.0,
                12520.0,
                12535.0,
                12510.0,
                12480.0,
                12495.0,
                12540.0,
                12575.0,
                12600.0,
                12620.0,
                12610.0,
                12645.0,
            ],
        }
    )


def _read_excel_or_existing_fixture(
    source: Path,
    fixture: Path,
    fallback: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str]:
    if source.exists():
        return pd.read_excel(source), _record_path(source)
    if fixture.exists():
        return pd.read_excel(fixture), _record_path(fixture)
    if fallback is not None:
        return fallback.copy(), "synthetic_open_source_fallback"
    raise FileNotFoundError(f"Missing truth-validation source and fixture: {source} / {fixture}")


def _read_csv_or_existing_fixture(
    source: Path,
    fixture: Path,
    fallback: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str]:
    if source.exists():
        return pd.read_csv(source), _record_path(source)
    if fixture.exists():
        return pd.read_csv(fixture), _record_path(fixture)
    if fallback is not None:
        return fallback.copy(), "synthetic_open_source_fallback"
    raise FileNotFoundError(f"Missing truth-validation source and fixture: {source} / {fixture}")


def _write_excel(df: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    df.to_excel(path, index=False)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    df.to_csv(path, index=False)


def prepare_fixtures(fixture_root: Path | None = None) -> dict:
    fixture_root = fixture_root or FIXTURE_ROOT
    dataloader_dir = ensure_directory(fixture_root / "dataloader")
    statanalyser_dir = ensure_directory(fixture_root / "statanalyser")
    ensure_directory(fixture_root / "metricstracker")
    ensure_directory(fixture_root / "validation_workflow")
    ensure_directory(fixture_root / "backtester")

    source_map: dict[str, dict[str, str]] = {}

    predictor_example_src = IMPORT_ROOT / "predictor_example.xlsx"
    predictor_example_out = dataloader_dir / "predictor_example_slice.xlsx"
    predictor_example_df, predictor_example_source = _read_excel_or_existing_fixture(
        predictor_example_src,
        predictor_example_out,
        _synthetic_predictor_example(),
    )
    predictor_example_df = predictor_example_df.head(8).copy()
    _write_excel(predictor_example_df, predictor_example_out)
    source_map["predictor_example_slice"] = {
        "source": predictor_example_source,
        "output": _record_path(predictor_example_out),
    }

    etf_src = IMPORT_ROOT / "etf_balance_1d.csv"
    etf_out = dataloader_dir / "etf_balance_total_slice.csv"
    etf_df, etf_source = _read_csv_or_existing_fixture(etf_src, etf_out, _synthetic_etf_balance())
    etf_df = etf_df.head(8).copy()
    _write_csv(etf_df[["timestamp", "total"]], etf_out)
    source_map["etf_balance_total_slice"] = {
        "source": etf_source,
        "output": _record_path(etf_out),
    }

    boxer_src = IMPORT_ROOT / "Boxer_Score_20210103.xlsx"
    boxer_out = statanalyser_dir / "boxer_score_slice.xlsx"
    boxer_df, boxer_source = _read_excel_or_existing_fixture(boxer_src, boxer_out, _synthetic_boxer_score())
    boxer_df = boxer_df.head(12).copy()
    _write_excel(boxer_df, boxer_out)
    source_map["boxer_score_slice"] = {
        "source": boxer_source,
        "output": _record_path(boxer_out),
    }

    stat_corr_df = pd.DataFrame(
        {
            "Time": [
                "2024-01-16",
                "2024-01-17",
                "2024-01-18",
                "2024-01-19",
                "2024-01-20",
                "2024-01-21",
                "2024-01-22",
                "2024-01-23",
            ],
            "Open": [100.0, 102.0, 103.0, 105.0, 106.0, 106.5, 108.0, 109.0],
            "High": [101.0, 103.5, 104.5, 106.5, 107.0, 107.0, 109.5, 111.0],
            "Low": [99.0, 101.0, 102.0, 104.0, 105.0, 105.5, 107.0, 108.0],
            "Close": [100.5, 103.0, 104.0, 106.0, 106.2, 106.8, 109.0, 110.5],
            "Volume": [1000, 1100, 1200, 1300, 900, 950, 1400, 1500],
            "total": [
                651739.7735,
                660542.5417,
                660962.5562,
                649567.6502,
                649567.6502,
                649567.6502,
                661154.0,
                655692.1792,
            ],
        }
    )
    close_return = stat_corr_df["Close"].pct_change(fill_method=None)
    if close_return.iloc[1:].isna().any():
        raise ValueError("correlation truth data contains an undefined non-initial return")
    close_return.iloc[0] = 0.0
    stat_corr_df["close_return"] = close_return
    stat_corr_out = statanalyser_dir / "correlation_truth_dataset.csv"
    _write_csv(stat_corr_df, stat_corr_out)
    source_map["correlation_truth_dataset"] = {
        "source": "derived_from_etf_balance_total_and_synthetic_price",
        "output": _record_path(stat_corr_out),
    }

    predictor_5m_src = IMPORT_ROOT / "predicting_5min.csv"
    predictor_5m_out = dataloader_dir / "predicting_5min_slice.csv"
    predictor_5m_df, predictor_5m_source = _read_csv_or_existing_fixture(
        predictor_5m_src,
        predictor_5m_out,
        _synthetic_predicting_5min(),
    )
    predictor_5m_df = predictor_5m_df.head(12).copy()
    _write_csv(predictor_5m_df, predictor_5m_out)
    source_map["predicting_5min_slice"] = {
        "source": predictor_5m_source,
        "output": _record_path(predictor_5m_out),
    }

    vix_df = pd.DataFrame(
        {
            "time": [
                "2024-01-16",
                "2024-01-17",
                "2024-01-18",
                "2024-01-19",
                "2024-01-20",
                "2024-01-21",
                "2024-01-22",
                "2024-01-23",
            ],
            "open": [100.0, 102.0, 103.0, 105.0, 106.0, 106.5, 108.0, 109.0],
            "high": [101.0, 103.5, 104.5, 106.5, 107.0, 107.0, 109.5, 111.0],
            "low": [99.0, 101.0, 102.0, 104.0, 105.0, 105.5, 107.0, 108.0],
            "close": [100.5, 103.0, 104.0, 106.0, 106.2, 106.8, 109.0, 110.5],
            "volume": [1000, 1100, 1200, 1300, 900, 950, 1400, 1500],
        }
    )
    vix_out = dataloader_dir / "price_daily_overlap_2024.csv"
    _write_csv(vix_df[["time", "open", "high", "low", "close", "volume"]], vix_out)
    source_map["price_daily_overlap_2024"] = {
        "source": "synthetic_price_daily_overlap_2024",
        "output": _record_path(vix_out),
    }

    synthetic_5m = pd.DataFrame(
        {
            "time": [
                "2025-09-24 00:00:00",
                "2025-09-24 00:05:00",
                "2025-09-24 00:10:00",
                "2025-09-24 00:15:00",
                "2025-09-24 00:20:00",
                "2025-09-24 00:25:00",
                "2025-09-24 00:30:00",
                "2025-09-24 00:35:00",
                "2025-09-24 00:40:00",
                "2025-09-24 00:45:00",
                "2025-09-24 00:50:00",
                "2025-09-24 00:55:00",
            ],
            "open": [100, 101, 102, 101, 100, 99, 98, 99, 100, 101, 102, 103],
            "high": [101, 102, 103, 102, 101, 100, 99, 100, 101, 102, 103, 104],
            "low": [99, 100, 101, 100, 99, 98, 97, 98, 99, 100, 101, 102],
            "close": [101, 102, 101, 100, 99, 98, 99, 100, 101, 102, 103, 104],
            "volume": [500] * 12,
        }
    )
    synthetic_5m_out = dataloader_dir / "price_5m_overlap_20250924.csv"
    _write_csv(synthetic_5m, synthetic_5m_out)
    source_map["price_5m_overlap_20250924"] = {
        "source": "synthetic",
        "output": _record_path(synthetic_5m_out),
    }

    mini_ohlc_truth_df = pd.DataFrame(
        {
            "Time": [
                "2024-01-17",
                "2024-01-18",
                "2024-01-19",
                "2024-01-20",
                "2024-01-21",
                "2024-01-22",
                "2024-01-23",
                "2024-01-24",
            ],
            "Open": [100, 101, 102, 103, 104, 105, 106, 107],
            "High": [101, 102, 103, 104, 105, 106, 107, 108],
            "Low": [99, 100, 101, 102, 103, 104, 105, 106],
            "Close": [100, 101, 102, 103, 104, 105, 106, 107],
            "Volume": [1000] * 8,
        }
    )
    mini_ohlc_truth_out = fixture_root / "backtester" / "mini_ohlc_truth.csv"
    _write_csv(mini_ohlc_truth_df, mini_ohlc_truth_out)
    source_map["mini_ohlc_truth"] = {
        "source": "synthetic",
        "output": _record_path(mini_ohlc_truth_out),
    }

    nday_truth_df = pd.DataFrame(
        {
            "Time": [
                "2024-01-17",
                "2024-01-18",
                "2024-01-19",
                "2024-01-20",
                "2024-01-21",
                "2024-01-22",
                "2024-01-23",
                "2024-01-24",
                "2024-01-25",
            ],
            "Open": [100, 99, 98, 99, 100, 101, 102, 103, 104],
            "High": [101, 100, 99, 100, 101, 102, 103, 104, 105],
            "Low": [99, 98, 97, 98, 99, 100, 101, 102, 103],
            "Close": [100, 99, 98, 99, 100, 101, 102, 103, 104],
            "Volume": [1000] * 9,
        }
    )
    nday_truth_out = fixture_root / "backtester" / "mini_ohlc_nday_truth.csv"
    _write_csv(nday_truth_df, nday_truth_out)
    source_map["mini_ohlc_nday_truth"] = {
        "source": "synthetic",
        "output": _record_path(nday_truth_out),
    }

    strategy_truth_payload = {
        "schema_version": "2.0",
        "strategy_id": "truth.v2.mini.price.timer",
        "name": "Truth Fixture - Mini Price Timer",
        "description": "Synthetic candidate-only truth strategy for compact verification fixtures.",
        "data_context": {
            "primary_instrument": "TEST",
            "frequency": "1D",
            "timezone": "UTC",
            "calendar": "XNYS",
        },
        "parameter_domains": {
            "entry_level": {"type": "set", "values": [101]},
            "hold_days": {"type": "set", "values": [2]},
        },
        "entry": {
            "op": "gt",
            "left": {"field": "price.close"},
            "right": {"param_ref": "entry_level"},
        },
        "exit": {"op": "timer_bars", "value": {"param_ref": "hold_days"}},
        "engine_preferences": {
            "requested_mode": "auto",
            "allow_hybrid": False,
            "max_combinations": 10,
        },
    }
    strategy_truth_out = fixture_root / "backtester" / "strategy-v2-mini-price-timer.json"
    write_json(strategy_truth_out, strategy_truth_payload)
    source_map["strategy_contract_mini_price_timer"] = {
        "source": "synthetic_strategy_contract",
        "output": _record_path(strategy_truth_out),
    }

    wfa_dir = fixture_root / "wfa"
    ensure_directory(wfa_dir)
    wfa_close_df = pd.DataFrame(
        {
            "Time": pd.date_range("2024-01-01", periods=45, freq="D").strftime("%Y-%m-%d"),
            "AAA": [round(100.0 + idx * 0.3, 4) for idx in range(45)],
            "BBB": [round(120.0 - idx * 0.1 + max(0, idx - 24) * 0.5, 4) for idx in range(45)],
        }
    )
    wfa_close_out = wfa_dir / "multi_asset_close_truth.csv"
    _write_csv(wfa_close_df, wfa_close_out)
    source_map["multi_asset_close_truth"] = {
        "source": "synthetic_multi_asset_close",
        "output": _record_path(wfa_close_out),
    }

    source_map_payload = {"fixture_root": _record_path(fixture_root), "sources": source_map}
    write_json(fixture_root / "source_map.json", source_map_payload)
    return {"fixture_root": str(fixture_root), "sources": source_map}


if __name__ == "__main__":
    payload = prepare_fixtures()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
