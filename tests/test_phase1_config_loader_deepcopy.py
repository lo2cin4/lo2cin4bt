import json
import sys
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
pytestmark = pytest.mark.regression


def test_config_loader_keeps_canonical_runs_isolated(tmp_path) -> None:
    source_path = (
        _REPO_ROOT
        / "workspace"
        / "runs"
        / "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"
    )
    source_config = json.loads(source_path.read_text(encoding="utf-8"))
    config_path = tmp_path / "strategy_run.json"
    config_path.write_text(json.dumps(source_config, indent=2), encoding="utf-8")

    from autorunner.ConfigLoader_autorunner import ConfigLoader

    loader = ConfigLoader()
    first = loader.load_config(str(config_path))
    assert first is not None

    first.engine_request["strategy"]["strategy_id"] = "mutated"
    first.metricstracker_config["enable_metrics_analysis"] = False

    second = loader.load_config(str(config_path))
    assert second is not None
    assert (
        second.engine_request["strategy"]["strategy_id"]
        == source_config["metadata"]["strategy_id"]
    )
    assert second.metricstracker_config["enable_metrics_analysis"] is True


def test_config_loader_rejects_legacy_runtime_shell(tmp_path) -> None:
    config_path = tmp_path / "legacy_runtime.json"
    config_path.write_text(
        json.dumps(
            {
                "dataloader": {"source": "multi_asset", "frequency": "1D"},
                "backtester": {
                    "strategy_mode": "multi_asset_portfolio",
                    "portfolio_config": {},
                },
                "metricstracker": {"enable_metrics_analysis": False},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    from autorunner.ConfigLoader_autorunner import ConfigLoader

    assert ConfigLoader().load_config(str(config_path)) is None


def test_canonical_config_readers_accept_utf8_bom(tmp_path) -> None:
    source_path = (
        _REPO_ROOT
        / "workspace"
        / "runs"
        / "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json"
    )
    config_path = tmp_path / "strategy_run_with_bom.json"
    config_path.write_text(source_path.read_text(encoding="utf-8-sig"), encoding="utf-8-sig")

    from autorunner.ConfigLoader_autorunner import ConfigLoader
    from autorunner.ConfigValidator_autorunner import ConfigValidator

    assert ConfigValidator().get_validation_errors(str(config_path)) == []
    assert ConfigLoader().load_config(str(config_path)) is not None
