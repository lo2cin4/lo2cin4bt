import json
import sys
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
pytestmark = pytest.mark.regression


def test_config_loader_merges_defaults_without_shared_mutation(tmp_path) -> None:
    config_path = tmp_path / "minimal_config.json"
    config_path.write_text(
        json.dumps({"dataloader": {"source": "yfinance"}}, indent=2),
        encoding="utf-8",
    )

    from autorunner.ConfigLoader_autorunner import ConfigLoader

    loader = ConfigLoader()
    first = loader.load_config(str(config_path))
    assert first is not None

    first.backtester_config["condition_pairs"].append({"entry": ["A"], "exit": ["B"]})

    second = loader.load_config(str(config_path))
    assert second is not None
    assert second.backtester_config["condition_pairs"] == []
    assert second.metricstracker_config["enable_metrics_analysis"] is False
