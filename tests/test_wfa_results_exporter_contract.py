from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from validation_workflow.ResultsExporter_validation_workflow import ResultsExporter


def _exporter(tmp_path: Path) -> ResultsExporter:
    strategy_run = {
        "schema_version": "strategy_run",
        "metadata": {"strategy_id": "wfa_contract_probe"},
        "universe": {"symbols": ["QQQ"]},
    }
    config_data = SimpleNamespace(
        backtester_config={"strategy_run_config": strategy_run},
        wfa_config={"output_csv": False},
    )
    return ResultsExporter(
        results={},
        output_dir=tmp_path,
        config_data=config_data,
    )


def test_wfa_ranking_does_not_replace_missing_oos_metric_with_is_metric(
    tmp_path: Path,
) -> None:
    exporter = _exporter(tmp_path)
    frame = pd.DataFrame(
        [
            {
                "semantic_combo": '{"lookback": 20}',
                "window_id": 1,
                "oos_sharpe": None,
                "is_sharpe": 3.0,
            }
        ]
    )

    exporter._export_ranking_report(  # noqa: SLF001
        "sharpe",
        frame,
        "probe_wfa_sharpe_run",
    )

    assert not list(tmp_path.glob("*ranking*.parquet"))


def test_wfa_ranking_requires_semantic_combo(tmp_path: Path) -> None:
    exporter = _exporter(tmp_path)
    frame = pd.DataFrame(
        [{"semantic_combo": None, "window_id": 1, "oos_sharpe": 1.0}]
    )

    with pytest.raises(ValueError, match="semantic_combo"):
        exporter._export_ranking_report(  # noqa: SLF001
            "sharpe",
            frame,
            "probe_wfa_sharpe_run",
        )


def test_wfa_ranking_requires_requested_oos_metric(tmp_path: Path) -> None:
    exporter = _exporter(tmp_path)
    frame = pd.DataFrame(
        [{"semantic_combo": '{"lookback": 20}', "window_id": 1}]
    )

    with pytest.raises(ValueError, match="oos_sharpe"):
        exporter._export_ranking_report(  # noqa: SLF001
            "sharpe",
            frame,
            "probe_wfa_sharpe_run",
        )


def test_wfa_semantic_combo_extractor_rejects_invalid_contract() -> None:
    with pytest.raises(ValueError, match="semantic_combo"):
        ResultsExporter._extract_semantic_combo_json(None)  # noqa: SLF001
    with pytest.raises(ValueError, match="semantic_combo"):
        ResultsExporter._extract_semantic_combo_json(
            {"semantic_combo": "lookback=20"}
        )


def test_wfa_export_propagates_objective_export_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exporter = _exporter(tmp_path)
    exporter.results = {"results_by_objective": {"sharpe": [{"window_id": 1}]}}

    def fail_export(*_args: object, **_kwargs: object) -> None:
        raise ValueError("broken WFA artifact")

    monkeypatch.setattr(exporter, "_export_objective_results", fail_export)

    with pytest.raises(RuntimeError, match="WFA result export failed"):
        exporter.export()
