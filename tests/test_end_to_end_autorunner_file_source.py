import sys
import builtins
import json
import importlib
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
sys.modules.pop("autorunner", None)


def _fixtures_dir() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "smoke"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_end_to_end_autorunner_file_source_smoke(tmp_path, monkeypatch) -> None:
    """
    End-to-end smoke test (deterministic):
    ConfigValidator -> ConfigLoader -> DataLoader(file) -> BacktestRunnerAutorunner.

    Notes:
    - We patch input() to avoid interactive prompts in loaders.
    - We stub exporter methods to avoid writing into records/ during tests.
    """
    monkeypatch.setattr(builtins, "input", lambda: "")

    fixtures = _fixtures_dir()
    price_csv = fixtures / "price_data_ma_cross.csv"
    assert price_csv.exists()

    config = _load_json(fixtures / "config_autorunner_file_ma1_ma4.json")
    # Make file path absolute so the loader is robust to cwd changes.
    config["dataloader"]["file_config"]["file_path"] = str(price_csv)

    config_path = tmp_path / "config.json"
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
    assert "Open" in data.columns
    assert "Close" in data.columns
    assert "X" in data.columns  # predictor column when skip_predictor=true

    # Avoid polluting repo output directories during tests.
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

    results = result.get("results")
    assert isinstance(results, list)
    assert len(results) == 1

    records = results[0].get("records")
    assert records is not None
    assert (records["Trade_action"] == 1).sum() == 1
    assert (records["Trade_action"] == 4).sum() == 1
