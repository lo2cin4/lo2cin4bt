from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from verification.scripts.prepare_truth_validation_fixtures import prepare_fixtures
from verification.scripts.run_truth_validation_batch import initialize_batch, load_manifest


def test_truth_validation_manifest_has_expected_first_round_cases():
    manifest = load_manifest()
    assert manifest["batch_id"] == "first_round"
    assert len(manifest["cases"]) == 8
    assert {case.get("case_id") or case.get("id") for case in manifest["cases"]} >= {
        "dataloader_skip_predictor_true",
        "backtester_ma1_nday1",
        "metricstracker_from_ma_case",
    }


def test_prepare_truth_validation_fixtures_creates_expected_files(tmp_path):
    payload = prepare_fixtures(tmp_path / "fixtures")
    fixture_root = Path(payload["fixture_root"])
    assert (fixture_root / "source_map.json").exists()
    assert (fixture_root / "dataloader" / "predictor_example_slice.xlsx").exists()
    assert (fixture_root / "dataloader" / "etf_balance_total_slice.csv").exists()
    assert (fixture_root / "dataloader" / "predicting_5min_slice.csv").exists()
    assert (fixture_root / "dataloader" / "price_5m_overlap_20250924.csv").exists()
    assert (fixture_root / "statanalyser" / "boxer_score_slice.xlsx").exists()
    assert (fixture_root / "statanalyser" / "correlation_truth_dataset.csv").exists()
    assert (fixture_root / "backtester" / "mini_ohlc_nday_truth.csv").exists()
    daily_columns = pd.read_csv(fixture_root / "dataloader" / "price_daily_overlap_2024.csv").columns.tolist()
    assert daily_columns == ["time", "open", "high", "low", "close", "volume"]


def test_prepare_truth_validation_fixtures_keeps_5m_fixture_columns_lowercase(tmp_path):
    fixture_root = Path(prepare_fixtures(tmp_path / "fixtures")["fixture_root"])
    columns = pd.read_csv(fixture_root / "dataloader" / "price_5m_overlap_20250924.csv").columns.tolist()
    assert columns == ["time", "open", "high", "low", "close", "volume"]


def test_initialize_truth_validation_batch_writes_summary(tmp_path):
    manifest = load_manifest()
    enabled_case_count = sum(1 for case in manifest["cases"] if case.get("enabled", True))
    summary = initialize_batch("test_first_round", manifest, tmp_path / "fixtures")
    results_root = PROJECT_ROOT / "verification" / "results" / "test_first_round"
    assert summary["status"] == "INITIALIZED"
    assert summary["case_count"] == enabled_case_count
    assert (results_root / "summary.json").exists()
    assert (results_root / "summary.md").exists()
    payload = json.loads((results_root / "summary.json").read_text(encoding="utf-8"))
    assert payload["case_count"] == enabled_case_count
    assert payload["cases"][0]["status"] == "TODO"
