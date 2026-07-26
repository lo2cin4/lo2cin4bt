from __future__ import annotations

import importlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from jsonschema import Draft202012Validator


pytestmark = pytest.mark.smoke
_REPO_ROOT = Path(__file__).resolve().parents[1]


def test_statanalyser_autorunner_stage_runs_from_config(tmp_path) -> None:
    config_path = tmp_path / "statanalyser_config.json"

    config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal",
            "workflow_id": "statanalyser",
        },
        "data": {
            "provider": "file",
            "frequency": "1D",
            "file_path": "tests/fixtures/smoke/price_data_ma_cross.csv",
            "date_column": "Time",
            "price_column": "Close",
        },
        "universe": {"symbols": ["QQQ"]},
        "computed_fields": [
            {"name": "short_ma", "op": "indicator.sma", "source": "close", "period": 10},
            {"name": "long_ma", "op": "indicator.sma", "source": "close", "period": 20},
        ],
        "signals": {
            "entry": {"field": "short_ma", "op": "crosses_above", "right_field": "long_ma"},
            "exit": {"field": "short_ma", "op": "crosses_below", "right_field": "long_ma"},
        },
        "selection": {},
        "allocation": {},
        "rebalance": {},
        "fill_model": {
            "timing": "timeline",
            "actions": [
                {"signal": "entry", "offset_bars": 1, "price": "open", "action": "enter"},
                {"signal": "exit", "offset_bars": 1, "price": "open", "action": "exit"},
            ],
        },
        "risk": {"max_positions": 1, "max_gross_exposure": 1.0, "long_short": "long_only"},
        "parameter_domains": {},
        "metricstracker": {"enable_metrics_analysis": False},
        "outputs": {},
        "metadata": {"strategy_id": "statanalyser_smoke"},
        "statanalyser": {
            "enabled": True,
            "target": {
                "predictor_column": "X",
                "return_column": "close_return",
                "diff_mode": "none",
            },
            "tests": {
                "stationarity": {
                    "enabled": True,
                    "output": ["summary", "decision"],
                },
                "correlation": {
                    "enabled": True,
                    "output": ["matrix", "summary"],
                },
                "autocorrelation": {
                    "enabled": True,
                    "lags": [1, 2, 3, 4, 5, 6, 7, 8],
                    "output": ["acf", "pacf", "summary"],
                },
                "distribution": {
                    "enabled": True,
                    "output": ["summary", "histogram"],
                },
                "seasonality": {
                    "enabled": True,
                    "output": ["summary"],
                },
            },
            "report": {
                "formats": ["md", "json"],
                "output_dir": str(tmp_path / "statanalyser_output"),
                "include_plots": False,
                "include_raw_tables": False,
                "fail_on_error": False,
            },
        },
    }
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")

    ConfigValidator = importlib.import_module(
        "autorunner.ConfigValidator_autorunner"
    ).ConfigValidator
    ConfigLoader = importlib.import_module(
        "autorunner.ConfigLoader_autorunner"
    ).ConfigLoader
    StatAnalyserRunnerAutorunner = importlib.import_module(
        "autorunner.StatAnalyserRunner_autorunner"
    ).StatAnalyserRunnerAutorunner

    schema = json.loads(
        (_REPO_ROOT / "backtester/contracts/strategy/strategy-run.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator(schema).validate(config)
    assert ConfigValidator().validate_config(str(config_path)) is True
    loaded = ConfigLoader().load_config(str(config_path))
    assert loaded is not None
    assert loaded.statanalyser_config["enabled"] is True

    dates = pd.date_range("2020-01-01", periods=120, freq="D")
    x = np.sin(np.linspace(0, 12 * np.pi, 120)) + np.linspace(0, 1, 120) / 10
    close_return = np.cos(np.linspace(0, 8 * np.pi, 120)) / 10
    data = pd.DataFrame(
        {
            "Time": dates,
            "X": x,
            "close_return": close_return,
        }
    )

    runner = StatAnalyserRunnerAutorunner()
    summary = runner.run(data, config)

    assert summary is not None
    assert summary["enabled"] is True
    assert summary["executed"] is True
    assert summary["failed"] == 0
    assert set(summary["results"]) == {
        "StationarityTest",
        "CorrelationTest",
        "AutocorrelationTest",
        "DistributionTest",
        "SeasonalAnalysis",
    }

    output_dir = Path(summary["report_paths"][0]).parent
    assert output_dir.exists()
    assert (output_dir / "statanalyser_report.md").exists()
    assert (output_dir / "statanalyser_summary.json").exists()
