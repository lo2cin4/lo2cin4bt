import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from app.api.shared_chart_series import SharedChartSeriesStore
from app.runtime.registry import AppRegistry


def test_shared_series_is_content_addressed_and_rejects_tampering(tmp_path: Path) -> None:
    store = SharedChartSeriesStore(AppRegistry(tmp_path))
    run_id = "run-shared"
    value = {"x": ["2026-01-01", "2026-01-02"], "y": [100.0, 101.0]}

    first = store.put(run_id, "chart_xy", value)
    second = store.put(run_id, "chart_xy", value)

    assert first == second
    assert store.load(run_id, first, expected_kind="chart_xy") == value
    shared_files = list(
        store.registry.build_run_paths(run_id)["shared_series_dir"].glob("*.json")
    )
    assert len(shared_files) == 1

    envelope = json.loads(shared_files[0].read_text(encoding="utf-8"))
    envelope["value"]["y"][0] = 999.0
    shared_files[0].write_text(json.dumps(envelope), encoding="utf-8")

    with pytest.raises(ValueError, match="content hash"):
        store.load(run_id, first, expected_kind="chart_xy")


def test_plot_and_overview_indexes_share_identical_curve_storage(tmp_path: Path) -> None:
    store = SharedChartSeriesStore(AppRegistry(tmp_path))
    run_id = "run-curves"
    curve = {"x": ["2026-01-01", "2026-01-02"], "y": [100.0, 102.0]}
    plot = {
        "schema_version": "plot_bundle.v1",
        "contract_id": "lo2cin4bt.plot_bundle.v1",
        "run_id": run_id,
        "series": [
            {
                "series_id": "candidate-a",
                "label": "Candidate A",
                "annotations": [],
                **curve,
            }
        ],
    }
    overview = {
        "schema_version": "2.0",
        "contract_id": "lo2cin4bt-app-metrics-overview-payload-v1",
        "run_id": run_id,
        "series": [
            {"backtest_id": "candidate-a", "label": "Candidate A", **curve}
        ],
        "benchmark_series": None,
    }

    plot_index = store.compact_plot_bundle(run_id, plot)
    overview_index = store.compact_metrics_overview(run_id, overview)

    assert plot_index["series"][0]["data_ref"] == overview_index["series"][0][
        "data_ref"
    ]
    assert len(
        list(store.registry.build_run_paths(run_id)["shared_series_dir"].glob("*.json"))
    ) == 1
    schema = json.loads(
        Path(__file__).parents[1]
        .joinpath("app", "contracts", "chart-series-storage-v1.schema.json")
        .read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(schema)
    validator.validate(plot_index)
    validator.validate(overview_index)
    validator.validate(
        json.loads(
            next(
                store.registry.build_run_paths(run_id)["shared_series_dir"].glob("*.json")
            ).read_text(encoding="utf-8")
        )
    )
    assert store.materialize_plot_bundle(run_id, plot_index) == plot
    assert store.materialize_metrics_overview(run_id, overview_index) == overview


def test_backtest_detail_index_externalizes_only_declared_shared_fields(
    tmp_path: Path,
) -> None:
    store = SharedChartSeriesStore(AppRegistry(tmp_path))
    run_id = "run-detail"
    payload = {
        "schema_version": "backtest_detail_bundle.v3",
        "contract_id": "lo2cin4bt.backtest_detail_bundle.v3",
        "run_id": run_id,
        "backtest_id": "candidate-a",
        "ohlc": [{"time": "2026-01-01", "close": 10.0}],
        "ohlc_by_asset": {"QQQ": [{"time": "2026-01-01", "close": 10.0}]},
        "benchmark_series": [{"time": "2026-01-01", "value": 100.0}],
        "equity_series": [{"time": "2026-01-01", "value": 101.0}],
    }

    index = store.compact_backtest_detail(run_id, payload)

    assert index["schema_version"] == "backtest_detail_index.v1"
    assert "ohlc_by_asset" not in index["payload"]
    assert "benchmark_series" not in index["payload"]
    assert index["payload"]["equity_series"] == payload["equity_series"]
    assert store.materialize_backtest_detail(run_id, index) == payload
