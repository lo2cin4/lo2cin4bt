import builtins
import importlib
import json
import os
import sys
from pathlib import Path

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
sys.modules.pop("autorunner", None)


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "phase1"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_rows(records: pd.DataFrame, actions: list[int]) -> list[dict]:
    subset = records.loc[records["Trade_action"].isin(actions)].copy()
    if subset.empty:
        return []
    subset["Time"] = pd.to_datetime(subset["Time"]).dt.strftime("%Y-%m-%d")
    cols = [
        "Time",
        "Trading_instrument",
        "Position_type",
        "Trade_action",
        "Parameter_set_id",
        "Open_position_price",
        "Close_position_price",
    ]
    subset = subset[cols].reset_index(drop=True)
    for col in ["Open_position_price", "Close_position_price"]:
        subset[col] = subset[col].astype(float).round(6)
    return subset.to_dict(orient="records")


def _run_case(config_name: str, tmp_path, monkeypatch):
    os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
    monkeypatch.setattr(builtins, "input", lambda: "")

    fixtures = _fixtures_dir()
    config = _load_json(fixtures / config_name)
    config["dataloader"]["file_config"]["file_path"] = str(
        Path(__file__).resolve().parent / "fixtures" / "smoke" / "price_data_ma_cross.csv"
    )

    config_path = tmp_path / config_name
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

    ConfigValidator = importlib.import_module(
        "autorunner.ConfigValidator_autorunner"
    ).ConfigValidator
    ConfigLoader = importlib.import_module("autorunner.ConfigLoader_autorunner").ConfigLoader
    DataLoaderAutorunner = importlib.import_module(
        "autorunner.DataLoader_autorunner"
    ).DataLoaderAutorunner
    BacktestRunnerAutorunner = importlib.import_module(
        "autorunner.BacktestRunner_autorunner"
    ).BacktestRunnerAutorunner

    assert ConfigValidator().validate_config(str(config_path)) is True

    cfg_data = ConfigLoader().load_config(str(config_path))
    assert cfg_data is not None

    data = DataLoaderAutorunner().load_data(cfg_data.dataloader_config)
    assert data is not None

    exporter_mod = importlib.import_module("backtester.TradeRecordExporter_backtester")
    monkeypatch.setattr(
        exporter_mod.TradeRecordExporter_backtester,
        "export_to_parquet",
        lambda self, backtest_id=None: None,
    )
    monkeypatch.setattr(
        exporter_mod.TradeRecordExporter_backtester,
        "export_to_csv",
        lambda self, backtest_id=None: None,
    )
    monkeypatch.setattr(
        exporter_mod.TradeRecordExporter_backtester,
        "export_to_excel",
        lambda self, backtest_id=None: None,
    )

    runner = BacktestRunnerAutorunner()
    result = runner.run_backtest(
        data,
        {
            "dataloader": cfg_data.dataloader_config,
            "backtester": cfg_data.backtester_config,
            "metricstracker": cfg_data.metricstracker_config,
        },
    )
    assert result is not None
    assert result.get("success") is True

    records = result["results"][0]["records"]
    return records


def _assert_snapshot(records: pd.DataFrame, expected_name: str) -> None:
    expected = _load_json(_fixtures_dir() / expected_name)
    snapshot = {
        "records_shape": list(records.shape),
        "trade_action_counts": {
            str(k): int(v) for k, v in records["Trade_action"].value_counts().sort_index().items()
        },
        "trade_action_sequence": records["Trade_action"].astype(int).tolist(),
        "open_trade_rows": _normalize_rows(records, [1]),
        "close_trade_rows": _normalize_rows(records, [4]),
    }
    assert snapshot == expected


def test_phase1_no_trade_path(tmp_path, monkeypatch) -> None:
    records = _run_case("no_trade_autorunner_config.json", tmp_path, monkeypatch)
    _assert_snapshot(records, "expected_no_trade_snapshot.json")


def test_phase1_open_without_close_path(tmp_path, monkeypatch) -> None:
    records = _run_case("open_no_close_autorunner_config.json", tmp_path, monkeypatch)
    _assert_snapshot(records, "expected_open_no_close_snapshot.json")

