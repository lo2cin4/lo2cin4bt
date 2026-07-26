import json
from pathlib import Path

import pytest

from app.api.backtest_detail_contract import BacktestDetailContractService
from app.api.shared_chart_series import SharedChartSeriesStore
from app.runtime.registry import AppRegistry


def _write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_backtest_detail_contract_accepts_matching_validated_source(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    run_id = "run-detail"
    backtest_id = "candidate-a"
    paths = registry.build_run_paths(run_id)
    result_hash = "a" * 64
    canonical = paths["snapshot_dir"] / "canonical.json"
    _write(
        canonical,
        {
            "schema_version": "canonical_result_bundle.v1",
            "validation": {"status": "valid"},
            "result_hashes": [result_hash],
        },
    )
    detail = paths["chart_payload_dir"] / "backtest_detail_candidate-a.json"
    payload = {
            "schema_version": "backtest_detail_bundle.v3",
            "contract_id": "lo2cin4bt.backtest_detail_bundle.v3",
            "run_id": run_id,
            "backtest_id": backtest_id,
            "ohlc": [{"time": "2024-01-01", "open": 1, "high": 1, "low": 1, "close": 1}],
            "source_hashes": [result_hash],
            "artifact_source_refs": [str(canonical)],
        }
    SharedChartSeriesStore(registry).write_json(
        detail,
        SharedChartSeriesStore(registry).compact_backtest_detail(run_id, payload),
    )

    payload = BacktestDetailContractService(registry).load(run_id, backtest_id)

    assert payload["backtest_id"] == backtest_id


def test_backtest_detail_contract_rejects_identity_mismatch(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    paths = registry.build_run_paths("run-detail")
    detail = paths["chart_payload_dir"] / "backtest_detail_candidate-a.json"
    payload = {
            "schema_version": "backtest_detail_bundle.v3",
            "contract_id": "lo2cin4bt.backtest_detail_bundle.v3",
            "run_id": "wrong-run",
            "backtest_id": "candidate-a",
            "ohlc": [{}],
            "source_hashes": ["a" * 64],
            "artifact_source_refs": [],
        }
    store = SharedChartSeriesStore(registry)
    store.write_json(detail, store.compact_backtest_detail("run-detail", payload))

    with pytest.raises(ValueError, match="identity"):
        BacktestDetailContractService(registry).load("run-detail", "candidate-a")


def test_backtest_detail_contract_rejects_stale_v1_cache(tmp_path: Path) -> None:
    registry = AppRegistry(tmp_path)
    paths = registry.build_run_paths("run-detail")
    detail = paths["chart_payload_dir"] / "backtest_detail_candidate-a.json"
    payload = {
            "schema_version": "backtest_detail_bundle.v1",
            "contract_id": "lo2cin4bt.backtest_detail_bundle.v1",
            "run_id": "run-detail",
            "backtest_id": "candidate-a",
            "ohlc": [{}],
            "source_hashes": ["a" * 64],
            "artifact_source_refs": [],
        }
    store = SharedChartSeriesStore(registry)
    store.write_json(detail, store.compact_backtest_detail("run-detail", payload))

    with pytest.raises(ValueError, match="BacktestDetailBundle.v3"):
        BacktestDetailContractService(registry).load("run-detail", "candidate-a")
