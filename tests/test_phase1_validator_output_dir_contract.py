import builtins
import importlib
import json
import os
import sys
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
sys.modules.pop("autorunner", None)

pytestmark = pytest.mark.regression


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "smoke"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_validator_accepts_bool_like_export_flags_and_custom_output_dir(
    tmp_path, monkeypatch
) -> None:
    """
    Regression test for Phase 1 contract hardening.

    Covers:
    - bool-like config values for metricstracker + export flags
    - autorunner export_config.output_dir pass-through
    - actual parquet/csv/xlsx output to a temp directory
    """
    os.environ.setdefault("NUMBA_DISABLE_JIT", "1")
    os.environ.setdefault("LO2CIN4BT_DISABLE_MULTIPROCESS", "1")
    monkeypatch.setattr(builtins, "input", lambda: "")

    fixtures = _fixtures_dir()
    price_csv = fixtures / "price_data_ma_cross.csv"
    assert price_csv.exists()

    config = _load_json(fixtures / "config_autorunner_file_ma1_ma4.json")
    config["dataloader"]["file_config"]["file_path"] = str(price_csv)
    config["metricstracker"]["enable_metrics_analysis"] = "false"
    config["backtester"]["export_config"]["export_csv"] = "true"
    config["backtester"]["export_config"]["export_excel"] = "1"
    config["backtester"]["export_config"]["output_dir"] = str(tmp_path)

    config_path = tmp_path / "config_boollike.json"
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
    assert result.get("export_config", {}).get("output_dir") == str(tmp_path)

    parquet_files = sorted(tmp_path.glob("*.parquet"))
    csv_files = sorted(tmp_path.glob("*.csv"))
    xlsx_files = sorted(tmp_path.glob("*.xlsx"))
    assert len(parquet_files) == 1
    assert len(csv_files) == 1
    assert len(xlsx_files) == 1
