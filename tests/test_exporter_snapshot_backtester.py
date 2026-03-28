import sys
import builtins
import importlib
import json
import os
import uuid
from datetime import datetime as _real_datetime
from pathlib import Path

import pandas as pd


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
sys.modules.pop("autorunner", None)


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "smoke"


def _load_json_obj(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


class _UuidSeq:
    def __init__(self) -> None:
        self._i = 1

    def __call__(self):
        # Deterministic UUIDs; stable across runs.
        u = uuid.UUID(int=self._i)
        self._i += 1
        return u


def _canonical_trade_rows(df: pd.DataFrame) -> list[dict]:
    # Keep only stable columns; convert Time to YYYY-MM-DD.
    trades = df.loc[df["Trade_action"].isin([1, 4])].copy()
    trades["Time"] = pd.to_datetime(trades["Time"]).dt.strftime("%Y-%m-%d")
    cols = [
        "Time",
        "Trade_action",
        "Position_type",
        "Open_position_price",
        "Close_position_price",
    ]
    trades = trades[cols].reset_index(drop=True)
    # Normalize floats for stable JSON comparison.
    for c in ["Open_position_price", "Close_position_price"]:
        trades[c] = trades[c].astype(float).round(6)
    return trades.to_dict(orient="records")


def test_exporter_full_snapshot_parquet_csv_excel(tmp_path, monkeypatch) -> None:
    """
    Golden snapshot for exporter outputs.

    We run a minimal deterministic backtest and assert the exported
    parquet/csv/xlsx contain expected trade rows.
    """
    # Keep numba from compiling during tests (faster, more deterministic).
    os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
    monkeypatch.setattr(builtins, "input", lambda: "")

    fixtures = _fixtures_dir()
    config = _load_json_obj(fixtures / "config_autorunner_file_ma1_ma4.json")
    price_csv = fixtures / "price_data_ma_cross.csv"
    config["dataloader"]["file_config"]["file_path"] = str(price_csv)

    DataLoaderAutorunner = importlib.import_module(
        "autorunner.DataLoader_autorunner"
    ).DataLoaderAutorunner
    BacktestRunnerAutorunner = importlib.import_module(
        "autorunner.BacktestRunner_autorunner"
    ).BacktestRunnerAutorunner
    VectorBacktestEngine = importlib.import_module(
        "backtester.VectorBacktestEngine_backtester"
    ).VectorBacktestEngine

    data = DataLoaderAutorunner().load_data(config["dataloader"])
    assert data is not None

    # Determinize uuid usage across engine/simulator/exporter.
    uuid_seq = _UuidSeq()
    monkeypatch.setattr(uuid, "uuid4", uuid_seq)

    runner = BacktestRunnerAutorunner()
    backtester_config = runner._convert_config(config)  # pylint: disable=protected-access

    engine = VectorBacktestEngine(data, config["dataloader"]["frequency"], symbol="TEST")
    results = engine.run_backtests(backtester_config)
    assert isinstance(results, list)
    assert len(results) == 1

    exporter_mod = importlib.import_module("backtester.TradeRecordExporter_backtester")

    exporter = exporter_mod.TradeRecordExporter_backtester(
        trade_records=pd.DataFrame(),
        frequency=config["dataloader"]["frequency"],
        results=results,
        data=data,
        Backtest_id="",
        predictor_file_name=None,
        predictor_column="X",
        symbol="TEST",
        **config["backtester"]["trading_params"],
    )
    exporter.output_dir = str(tmp_path)
    Path(exporter.output_dir).mkdir(parents=True, exist_ok=True)

    exporter.export_to_parquet()
    exporter.export_to_csv()
    exporter.export_to_excel()

    parquet_files = sorted(tmp_path.glob("*.parquet"))
    csv_files = sorted(tmp_path.glob("*.csv"))
    xlsx_files = sorted(tmp_path.glob("*.xlsx"))
    assert len(parquet_files) == 1
    assert len(csv_files) == 1
    assert len(xlsx_files) == 1

    expected = _load_json_obj(fixtures / "expected_trades_ma1_ma4.json")

    parquet_df = pd.read_parquet(parquet_files[0])
    assert _canonical_trade_rows(parquet_df) == expected

    csv_df = pd.read_csv(csv_files[0])
    assert _canonical_trade_rows(csv_df) == expected

    excel_df = pd.read_excel(xlsx_files[0])
    assert _canonical_trade_rows(excel_df) == expected
