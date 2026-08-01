import json
from pathlib import Path

import pandas as pd
import pytest

from app.api.metrics_contract_payload import MetricsContractPayloadService
from app.api.shared_chart_series import SharedChartSeriesStore
from app.api.time_context import bar_spec_label
from app.runtime.registry import AppRegistry
from backtester.StrategyRunConfig_backtester import normalize_strategy_run_config


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _contract_run(tmp_path: Path) -> tuple[MetricsContractPayloadService, str, Path]:
    registry = AppRegistry(tmp_path)
    run_id = "contract-run"
    paths = registry.build_run_paths(run_id)
    result_hash = "a" * 64
    canonical_path = paths["snapshot_dir"] / "canonical.json"
    _write_json(
        canonical_path,
        {
            "schema_version": "canonical_result_bundle.v1",
            "validation": {"status": "valid"},
            "result_hashes": [result_hash],
        },
    )
    plot_path = paths["chart_payload_dir"] / "asset_curve_compare.json"
    plot_payload = {
            "schema_version": "plot_bundle.v1",
            "contract_id": "lo2cin4bt.plot_bundle.v1",
            "run_id": run_id,
            "series": [
                {
                    "series_id": "candidate_a:single_backtest:fixed",
                    "label": "Candidate A",
                    "x": ["2024-01-01", "2024-01-02"],
                    "y": [100.0, 101.0],
                }
            ],
            "source_hashes": [result_hash],
            "artifact_source_refs": [str(canonical_path)],
            "generated_at": "2026-07-11T00:00:00Z",
        }
    store = SharedChartSeriesStore(registry)
    store.write_json(plot_path, store.compact_plot_bundle(run_id, plot_payload))
    metrics_path = paths["snapshot_dir"] / "candidate_metrics.parquet"
    pd.DataFrame({"placeholder": [1]}).to_parquet(metrics_path, index=False)
    metadata_path = metrics_path.with_name("candidate_metadata.json")
    _write_json(
        metadata_path,
        [
            {
                "Backtest_id": "candidate_a:single_backtest:fixed",
                "Annualization": {
                    "schema_version": "metrics_annualization.v1",
                    "basis": "session_close_projection",
                    "projection_policy": "last_accepted_equity_per_session",
                    "periods_per_year": 252,
                    "risk_free_rate_annual": 0.04,
                },
                "Projected_session_count": 2,
                "Projected_return_interval_count": 1,
                "Total_return": 0.01,
                "Sharpe": 1.2,
                "Max_drawdown": -0.02,
                "BAH_Total_return": 0.005,
                "Excess_return": 0.005,
            }
        ],
    )
    registry.write_artifact_manifest(
        run_id,
        {
            "artifacts": [
                {"artifact_type": "metricstracker_parquet", "path": str(metrics_path)},
                {"artifact_type": "metricstracker_metadata", "path": str(metadata_path)},
            ]
        },
    )
    return MetricsContractPayloadService(registry), run_id, plot_path


@pytest.mark.parametrize(
    ("step", "unit", "expected"),
    [
        (1, "week", "1 week"),
        (2, "week", "2 weeks"),
        (1, "month", "1 month"),
        (3, "month", "3 months"),
    ],
)
def test_typed_frequency_label_supports_calendar_periods(
    step: int, unit: str, expected: str
) -> None:
    assert bar_spec_label({"step": step, "unit": unit}) == expected


def test_metrics_contract_payload_reads_json_contracts_without_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, run_id, _plot_path = _contract_run(tmp_path)

    monkeypatch.setattr(pd, "read_parquet", lambda *args, **kwargs: pytest.fail("parquet read"))
    payload = service.load(run_id)

    assert payload["projection_source"] == "validated_json_contracts"
    assert payload["rows"][0]["sharpe"] == 1.2
    assert payload["rows"][0]["excess_return"] == pytest.approx(0.005)
    assert payload["series"][0]["y"] == [100.0, 101.0]
    assert payload["annualization"]["basis"] == "session_close_projection"
    assert payload["strategy_summary"]["annualization"]["periods_per_year"] == 252
    assert (
        payload["time_context"]
        == payload["strategy_summary"]["time_context"]
    )
    assert payload["rows"][0]["projected_session_count"] == 2
    assert payload["rows"][0]["projected_return_interval_count"] == 1


def test_metrics_contract_keeps_metricstracker_values_when_matrix_metrics_are_null_or_stale(
    tmp_path: Path,
) -> None:
    service, run_id, _plot_path = _contract_run(tmp_path)
    paths = service.registry.build_run_paths(run_id)
    matrix_path = paths["snapshot_dir"] / "portfolio_matrix_summary.json"
    _write_json(
        matrix_path,
        {
            "schema_version": "portfolio_matrix_summary.v1",
            "rows": [
                {
                    "backtest_id": "candidate_a:single_backtest:fixed",
                    "strategy_id": "candidate_a:single_backtest:fixed",
                    "total_return": 99.0,
                    "cagr": None,
                    "sharpe": None,
                    "max_drawdown": None,
                    "rebalance_count": 7,
                }
            ],
        },
    )
    manifest = service.registry.load_artifact_manifest(run_id)
    manifest["artifacts"].append(
        {"artifact_type": "portfolio_matrix_summary_json", "path": str(matrix_path)}
    )
    service.registry.write_artifact_manifest(run_id, manifest)

    payload = service.load(run_id, force=True)
    row = payload["rows"][0]

    assert row["total_return"] == pytest.approx(0.01)
    assert row["sharpe"] == pytest.approx(1.2)
    assert row["max_drawdown"] == pytest.approx(-0.02)
    assert row["rebalance_count"] == 7


def test_metrics_contract_payload_rejects_unvalidated_plot_source(tmp_path: Path) -> None:
    service, run_id, plot_path = _contract_run(tmp_path)
    plot = json.loads(plot_path.read_text(encoding="utf-8"))
    plot["payload"]["source_hashes"] = ["b" * 64]
    _write_json(plot_path, plot)

    with pytest.raises(ValueError, match="matching validated canonical"):
        service.ensure(run_id)


def test_metrics_contract_payload_rejects_missing_rust_annualization(
    tmp_path: Path,
) -> None:
    service, run_id, _plot_path = _contract_run(tmp_path)
    metadata_path = service._artifact_path(run_id, "metricstracker_metadata")
    assert metadata_path is not None
    rows = json.loads(metadata_path.read_text(encoding="utf-8"))
    del rows[0]["Annualization"]
    _write_json(metadata_path, rows)

    with pytest.raises(ValueError, match="Rust Annualization"):
        service.ensure(run_id, force=True)


def test_metrics_contract_payload_exposes_complete_executable_strategy_logic(
    tmp_path: Path,
) -> None:
    service, run_id, _plot_path = _contract_run(tmp_path)
    strategy_path = service.registry.build_run_paths(run_id)["snapshot_dir"] / "strategy_run.json"
    _write_json(
        strategy_path,
        {
            "schema_version": "strategy_run",
            "universe": {"symbols": ["QQQ", "TLT", "GLD"]},
            "signals": {
                "entry": {
                    "op": "calendar.nth_weekday_of_month",
                    "ordinal": 3,
                    "weekday": "friday",
                }
            },
            "allocation": {"method": "fixed_weights"},
            "rebalance": {"trigger": {"op": "calendar.first_session"}},
            "fill_model": {
                "timing": "timeline",
                "actions": [
                    {
                        "signal": "entry",
                        "offset_bars": 0,
                        "price": "open",
                        "action": "set_target_weights",
                        "weights": {"TLT": 0.5, "GLD": 0.5},
                    },
                    {
                        "signal": "entry",
                        "offset_bars": 0,
                        "price": "close",
                        "action": "set_target_weights",
                        "weights": {"QQQ": 1.0},
                    },
                ],
                "cost": {"transaction_cost": 0.001, "slippage": 0.0005},
            },
            "risk": {"max_positions": 2, "allow_short": False},
            "parameter_domains": {},
        },
    )

    payload = service.load(run_id, force=True)
    steps = payload["strategy_summary"]["logic_steps"]

    assert [step["kind"] for step in steps] == [
        "Universe",
        "Signal",
        "Allocation",
        "Rebalance",
        "Execution",
        "Action",
        "Action",
        "Costs",
        "Risk",
    ]
    assert "ordinal: 3" in steps[1]["detail"]
    assert "calendar event known before session" in steps[5]["detail"]
    assert "TLT 50%" in steps[5]["detail"]
    assert "calendar event known before session" in steps[6]["detail"]
    assert "QQQ 100%" in steps[6]["detail"]


def test_metrics_contract_payload_exposes_exact_multitimeframe_context(
    tmp_path: Path,
) -> None:
    service, run_id, _plot_path = _contract_run(tmp_path)
    strategy_path = service.registry.build_run_paths(run_id)["snapshot_dir"] / "strategy_run.json"
    strategy = {
        "schema_version": "strategy_run",
        "data": {
            "bar_time": {
                "schema_version": "bar_time_contract.v1",
                "contract_id": "lo2cin4bt.bar_time_contract.v1",
                "session_model": {
                    "calendar_id": "XNYS",
                    "timezone": "America/New_York",
                    "session_scope": "regular",
                    "session_label_policy": "exchange_local_date",
                    "non_session_bar_policy": "reject",
                },
                "timestamp_model": {
                    "time_standard": "UTC",
                    "precision": "nanosecond",
                    "clock": "historical_available_time",
                    "ordering": "available_time_then_event_time",
                },
                "price_model": {
                    "price_basis": "split_dividend_adjusted",
                    "corporate_action_policy": "provider_applied",
                },
                "streams": [
                    {
                        "stream_id": "execution_1m",
                        "role": "execution",
                        "source": {"kind": "external", "provider_id": "fixture"},
                        "bar_spec": {
                            "aggregation": "time",
                            "step": 1,
                            "unit": "minute",
                            "price_type": "last",
                            "alignment": "session_open",
                        },
                        "timestamp_semantics": {
                            "timestamp_convention": "bar_close",
                            "interval_boundary": "left_open_right_closed",
                            "bar_open_time_column": "bar_open_timestamp",
                            "bar_close_time_column": "bar_close_timestamp",
                            "available_time_column": "available_timestamp",
                            "session_label_column": "session_label",
                            "availability_policy": "bar_close",
                        },
                    },
                    {
                        "stream_id": "decision_5m",
                        "role": "decision",
                        "source": {
                            "kind": "derived",
                            "parent_stream_id": "execution_1m",
                            "aggregation_engine": "shared_rust",
                            "empty_bar_policy": "omit",
                            "partial_first_bar_policy": "omit",
                            "partial_final_bar_policy": "omit",
                        },
                        "bar_spec": {
                            "aggregation": "time",
                            "step": 5,
                            "unit": "minute",
                            "price_type": "last",
                            "alignment": "session_open",
                        },
                        "timestamp_semantics": {
                            "timestamp_convention": "bar_close",
                            "interval_boundary": "left_open_right_closed",
                            "bar_open_time_column": "bar_open_timestamp",
                            "bar_close_time_column": "bar_close_timestamp",
                            "available_time_column": "available_timestamp",
                            "session_label_column": "session_label",
                            "availability_policy": "bar_close",
                        },
                    },
                ],
            },
            "stream_binding": {
                "execution_stream_id": "execution_1m",
                "decision_stream_id": "decision_5m",
            },
        },
        "universe": {"symbols": ["QQQ"]},
        "signals": {},
        "fill_model": {},
    }
    _write_json(strategy_path, strategy)

    payload = service.load(run_id, force=True)
    summary = payload["strategy_summary"]
    context = summary["time_context"]

    assert context["execution"]["stream_id"] == "execution_1m"
    assert context["execution"]["bar_spec"]["unit"] == "minute"
    assert context["decision"]["stream_id"] == "decision_5m"
    assert context["decision"]["bar_spec"]["step"] == 5
    assert context["decision"]["source"]["kind"] == "derived"
    assert context["decision"]["timestamp_semantics"]["availability_policy"] == "bar_close"
    assert context["session"]["timezone"] == "America/New_York"
    assert context["timestamp"]["time_standard"] == "UTC"
    assert summary["frequency_label"] == "1 minute"
    assert summary["decision_frequency_label"] == "5 minutes"


def test_every_runnable_strategy_has_complete_logic_projection() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    runnable_paths = set((repo_root / "workspace" / "runs").glob("*.json"))
    contract_paths = set(
        (repo_root / "backtester" / "contracts" / "strategy" / "examples").glob(
            "strategy-run-*.json"
        )
    )
    config_paths = sorted(runnable_paths | contract_paths)

    assert config_paths
    for config_path in config_paths:
        config = json.loads(config_path.read_text(encoding="utf-8-sig"))
        normalized = normalize_strategy_run_config(config)
        steps = MetricsContractPayloadService._strategy_logic_steps(normalized)
        kinds = {step["kind"] for step in steps}
        detail = " ".join(step["detail"] for step in steps).lower()

        assert "Universe" in kinds, config_path.name
        assert kinds.intersection({"Signal", "Selection", "Allocation"}), config_path.name
        assert "Execution" in kinds, config_path.name
        assert "Costs" in kinds, config_path.name
        assert "Risk" in kinds, config_path.name
        if normalized.get("computed_fields"):
            assert "Indicator" in kinds, config_path.name
        actions = [step for step in steps if step["kind"] == "Action"]
        assert [step["label"] for step in actions] == [
            f"Execution {index}" for index in range(1, len(actions) + 1)
        ], config_path.name
        assert "fixed strategy" not in detail, config_path.name
        assert "strategy profile" not in detail, config_path.name

    for wfa_path in sorted((repo_root / "workspace" / "wfa").glob("*.json")):
        wfa_config = json.loads(wfa_path.read_text(encoding="utf-8-sig"))
        strategy_path = repo_root / str(wfa_config["strategy_run_path"])
        assert strategy_path in runnable_paths, wfa_path.name
