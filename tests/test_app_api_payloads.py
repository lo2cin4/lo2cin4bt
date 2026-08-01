from pathlib import Path
import json

import pandas as pd
import pytest

from backtester.BacktestResult_backtester import MultiAssetBacktestResult
from backtester.MultiAssetPortfolioExporter_backtester import (
    MultiAssetPortfolioExporterBacktester,
)
from app.api.payloads import AppPayloadService
from app.api.service import AppAPIService
from app.runtime.module_identity import VALIDATION_WORKFLOW_CANONICAL
from tests.support.app_api_contract_fixtures import (
    build_portfolio_contract_run,
    build_stat_contract_run,
    build_wfa_contract_run,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _typed_daily_data(provider: str, **extra: object) -> dict[str, object]:
    stream_id = "execution_daily"
    return {
        "provider": provider,
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
                "ordering": (
                    "available_time_then_event_time_then_external_execution_sequence"
                    "_then_lifecycle_stage_then_stream_id_then_source_sequence"
                ),
            },
            "price_model": {
                "price_basis": "split_dividend_adjusted",
                "corporate_action_policy": "provider_applied",
            },
            "streams": [
                {
                    "stream_id": stream_id,
                    "role": "execution",
                    "source": {"kind": "external", "provider_id": provider},
                    "bar_spec": {
                        "aggregation": "time",
                        "step": 1,
                        "unit": "day",
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
                }
            ],
        },
        "stream_binding": {
            "execution_stream_id": stream_id,
            "decision_stream_id": stream_id,
        },
        **extra,
    }


def test_wfa_payload_parsers_reject_legacy_or_corrupt_objects() -> None:
    with pytest.raises(ValueError, match="semantic_combo"):
        AppPayloadService._parse_semantic_combo("lookback=20|threshold=5")
    with pytest.raises(ValueError, match="semantic_combo"):
        AppPayloadService._parse_semantic_combo('["lookback", 20]')
    with pytest.raises(ValueError, match="JSON object"):
        AppPayloadService._parse_json_object("{bad json")
    with pytest.raises(ValueError, match="JSON object"):
        AppPayloadService._parse_json_object("[1, 2]")


def test_event_display_preserves_intraday_instants_and_only_synthesizes_session_events() -> (
    None
):
    intraday = pd.DataFrame(
        {
            "Time": [
                "2024-07-03T13:31:00Z",
                "2024-07-03T13:35:00Z",
            ],
            "Action": ["buy", "sell"],
        }
    )

    rendered_intraday = AppPayloadService._annotate_rebalance_event_display(
        intraday,
        pd.DataFrame(),
        timezone_label="America/New_York",
        row_key_kind="event_timestamp",
    )

    assert rendered_intraday["Time"].tolist() == [
        "2024-07-03T13:31:00Z",
        "2024-07-03T13:35:00Z",
    ]
    assert rendered_intraday["Event_timestamp_local"].tolist() == [
        "2024-07-03T09:31:00-04:00",
        "2024-07-03T09:35:00-04:00",
    ]

    rendered_session = AppPayloadService._annotate_rebalance_event_display(
        pd.DataFrame({"Time": ["2024-07-03"], "Action": ["buy"]}),
        pd.DataFrame(),
        timezone_label="America/New_York",
        row_key_kind="session_label",
    )

    assert rendered_session["Event_timestamp_local"].tolist() == ["2024-07-03 09:30 ET"]


def test_render_rule_node_supports_strategy_run_field_aliases():
    assert (
        AppPayloadService._render_rule_node(
            {"field": "short_ma", "op": "crosses_above", "right_field": "long_ma"}
        )
        == "short_ma crosses above long_ma"
    )
    assert (
        AppPayloadService._render_rule_node(
            {"field": "close", "op": "gt", "right_field": "sma_filter"}
        )
        == "close > sma_filter"
    )


def test_strategy_rule_display_overrides_render_dict_rules():
    rules = AppPayloadService._strategy_rule_display_overrides(
        {
            "strategy_rules": {
                "entry": {
                    "field": "short_ma",
                    "op": "crosses_above",
                    "right_field": "long_ma",
                },
                "exit": '{"field": "short_ma", "op": "crosses_below", "right_field": "long_ma"}',
            }
        }
    )

    assert rules["entry_rule"] == "short_ma crosses above long_ma"
    assert rules["exit_rule"] == "short_ma crosses below long_ma"


def test_app_api_metrics_payloads(tmp_path: Path) -> None:
    metrics_service, _payloads, _registry, run_id = build_portfolio_contract_run(
        tmp_path
    )
    metrics_service.ensure(run_id)
    service = AppAPIService(tmp_path)
    overview = service.metrics_overview(run_id)
    assert overview["rows"]
    first_row = overview["rows"][0]
    assert first_row["date_range_start"] is not None
    assert first_row["date_range_end"] is not None
    assert first_row["label"]
    assert first_row["label_source"] != "internal_id_fallback"

    heatmap = service.parameter_matrix(run_id)
    assert heatmap["rows"]
    assert heatmap["param_axes"]
    assert heatmap["axis_values"]
    assert heatmap["default_x_axis"]
    assert heatmap["default_y_axis"]
    assert heatmap["strategy_summary"]["asset_label"]


def test_app_api_wfa_and_statanalyser_payloads(tmp_path: Path) -> None:
    wfa_service, wfa_run_id = build_wfa_contract_run(tmp_path / "wfa")
    stat_service, stat_run_id = build_stat_contract_run(tmp_path / "stat")

    wfa = wfa_service.wfa_dashboard(wfa_run_id)
    stat = stat_service.statanalyser_summary(stat_run_id)

    assert wfa["rows"]
    assert len(wfa["rows"]) == len({row["window_id"] for row in wfa["rows"]})
    assert "cluster_summary" in wfa
    assert "summary" in stat


def test_app_api_rejects_legacy_grid_wfa_artifact() -> None:
    service = AppAPIService(REPO_ROOT)
    with pytest.raises(
        (FileNotFoundError, ValueError),
        match="selected_optimum|selected-optimum|canonical",
    ):
        service.wfa_dashboard("20260415_d1c3219130cc")


def test_wfa_dashboard_preserves_unified_dates_and_rolling_workflow(
    tmp_path: Path,
) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260503_unified_roll"
    artifact_dir = (
        service.registry.build_run_paths(run_id)["snapshot_dir"]
        / "managed_artifacts"
        / VALIDATION_WORKFLOW_CANONICAL
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    wfa_path = artifact_dir / "wfa_unified_roll_selected_optimum.parquet"
    pd.DataFrame(
        {
            "window_id": [1],
            "objective": ["calmar"],
            "semantic_combo": [json.dumps({"policy": "fixed"}, sort_keys=True)],
            "train_start": [pd.Timestamp("2020-01-02")],
            "train_end": [pd.Timestamp("2022-12-30")],
            "test_start": [pd.Timestamp("2023-01-03")],
            "test_end": [pd.Timestamp("2023-12-29")],
            "is_sharpe": [0.8],
            "is_calmar": [0.4],
            "oos_sharpe": [0.6],
            "oos_calmar": [0.3],
            "oos_total_return": [0.08],
            "selection_source": ["unified_portfolio_wfa"],
            "selection_rank": [1],
            "selection_metric": ["calmar"],
            "selection_evidence": ["rank=1 by IS Calmar"],
            "candidate_count": [1],
            "oos_portfolio_json": [
                json.dumps(
                    {
                        "asset_count": 2,
                        "allocation": [
                            {
                                "asset": "AAA",
                                "avg_weight": 0.6,
                                "last_weight": 0.5,
                                "active_days": 8,
                            },
                            {
                                "asset": "BBB",
                                "avg_weight": 0.4,
                                "last_weight": 0.5,
                                "active_days": 8,
                            },
                        ],
                        "contribution": [
                            {
                                "asset": "AAA",
                                "return_contribution": 0.03,
                                "avg_weight": 0.6,
                            },
                            {
                                "asset": "BBB",
                                "return_contribution": 0.02,
                                "avg_weight": 0.4,
                            },
                        ],
                        "active_rebalance_count": 2,
                        "avg_exposure": 1.0,
                        "avg_holdings": 2.0,
                        "total_turnover": 1.2,
                        "cost_drag": 0.001,
                    },
                    sort_keys=True,
                )
            ],
            "workflow": ["rolling_validation"],
            "wfa_row_type": ["selected_optimum"],
        }
    ).to_parquet(wfa_path, index=False)
    wfa_path.with_name("wfa_unified_roll_metadata.json").write_text(
        json.dumps(
            {
                "row_contract": "selected_optimum_per_window",
                "annualization": {
                    "schema_version": "metrics_annualization.v1",
                    "basis": "session_close_projection",
                    "projection_policy": "last_accepted_equity_per_session",
                    "periods_per_year": 252.0,
                    "risk_free_rate_annual": 0.02,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (
        service.registry.build_run_paths(run_id)["snapshot_dir"] / "wfa_run.json"
    ).write_text(
        json.dumps(
            {
                "schema_version": "wfa_run",
                "optimizer": {"primary_objective": "calmar"},
            }
        ),
        encoding="utf-8",
    )

    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "entrypoint": "test",
            "status": "completed",
            "created_at": "2026-05-03T00:00:00",
            "completed_at": "2026-05-03T00:00:01",
            "config_filename": "wfa_unified_roll.json",
            "strategy_mode": "multi_asset_portfolio",
            "run_type": "test",
        }
    )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "schema_version": "1.0",
            "artifacts": [
                {
                    "artifact_type": "wfa_parquet",
                    "path": str(wfa_path),
                    "status": "ready",
                },
            ],
        },
    )

    payload = service.wfa_dashboard(run_id)

    assert payload["batch_metadata"]["workflow"] == "rolling_validation"
    assert payload["batch_metadata"]["source_workflows"] == ["rolling_validation"]
    assert payload["rows"][0]["objective"] == "calmar"
    assert payload["rows"][0]["selection_metric"] == "calmar"
    assert payload["rows"][0]["train_start_date"].startswith("2020-01-02")
    assert payload["rows"][0]["test_end_date"].startswith("2023-12-29")
    assert payload["timeline"][0]["train_start_date"].startswith("2020-01-02")
    assert payload["timeline"][0]["test_end_date"].startswith("2023-12-29")
    assert payload["portfolio_window_summary"]["is_portfolio_wfa"] is True
    assert (
        payload["portfolio_window_summary"]["allocation_by_window"][0]["weights"][0][
            "asset"
        ]
        == "AAA"
    )
    assert payload["portfolio_window_summary"]["asset_summary"][0]["asset"] == "AAA"
    assert payload["strategy_summary"]["annualization"] == {
        "schema_version": "metrics_annualization.v1",
        "basis": "session_close_projection",
        "projection_policy": "last_accepted_equity_per_session",
        "periods_per_year": 252.0,
        "risk_free_rate_annual": 0.02,
    }


def test_wfa_dashboard_prefers_configured_primary_objective_artifact(
    tmp_path: Path,
) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260730_primary_objective"
    paths = service.registry.build_run_paths(run_id)
    artifact_dir = (
        paths["snapshot_dir"] / "managed_artifacts" / VALIDATION_WORKFLOW_CANONICAL
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)

    def selected_frame(objective: str, short_ma: int) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "window_id": [1],
                "objective": [objective],
                "semantic_combo": [
                    json.dumps({"short_ma": short_ma, "long_ma": 200}, sort_keys=True)
                ],
                "train_start": [pd.Timestamp("2026-06-01")],
                "train_end": [pd.Timestamp("2026-06-18")],
                "test_start": [pd.Timestamp("2026-06-19")],
                "test_end": [pd.Timestamp("2026-06-24")],
                "is_sharpe": [-1.0],
                "is_calmar": [-2.0],
                "oos_sharpe": [-3.0],
                "oos_calmar": [-4.0],
                "oos_total_return": [-0.1],
                "selection_source": ["unified_portfolio_wfa"],
                "selection_rank": [1],
                "selection_metric": [objective],
                "selection_evidence": [f"rank=1 by IS {objective}"],
                "candidate_count": [2],
                "total_candidate_count": [2],
                "workflow": ["walk_forward_analysis"],
                "wfa_row_type": ["selected_optimum"],
            }
        )

    calmar_path = artifact_dir / "wfa_calmar.parquet"
    sharpe_path = artifact_dir / "wfa_sharpe.parquet"
    selected_frame("calmar", 10).to_parquet(calmar_path, index=False)
    selected_frame("sharpe", 90).to_parquet(sharpe_path, index=False)
    paths["snapshot_dir"].mkdir(parents=True, exist_ok=True)
    (paths["snapshot_dir"] / "wfa_run.json").write_text(
        json.dumps(
            {
                "schema_version": "wfa_run",
                "optimizer": {"primary_objective": "sharpe"},
            }
        ),
        encoding="utf-8",
    )
    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "entrypoint": "test",
            "status": "completed",
            "created_at": "2026-07-30T00:00:00",
            "completed_at": "2026-07-30T00:00:01",
            "config_filename": "wfa-primary.json",
            "strategy_mode": "multi_asset_portfolio",
            "run_type": "test",
        }
    )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "schema_version": "1.0",
            "artifacts": [
                {
                    "artifact_type": "wfa_parquet",
                    "path": str(calmar_path),
                    "status": "ready",
                },
                {
                    "artifact_type": "wfa_parquet",
                    "path": str(sharpe_path),
                    "status": "ready",
                },
            ],
        },
    )

    payload = service.wfa_dashboard(run_id)

    assert payload["rows"][0]["objective"] == "sharpe"
    assert payload["rows"][0]["semantic_combo"]["short_ma"] == 90
    assert payload["artifact_source_refs"] == [str(sharpe_path)]


def test_wfa_dashboard_rejects_missing_primary_objective(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260730_missing_primary_objective"
    paths = service.registry.build_run_paths(run_id)
    paths["snapshot_dir"].mkdir(parents=True, exist_ok=True)
    artifact = paths["snapshot_dir"] / "wfa_selected.parquet"
    pd.DataFrame(
        {
            "wfa_row_type": ["selected_optimum"],
            "objective": ["sharpe"],
        }
    ).to_parquet(artifact, index=False)
    (paths["snapshot_dir"] / "wfa_run.json").write_text(
        json.dumps({"schema_version": "wfa_run", "optimizer": {}}),
        encoding="utf-8",
    )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "schema_version": "1.0",
            "artifacts": [
                {
                    "artifact_type": "wfa_parquet",
                    "path": str(artifact),
                    "status": "ready",
                }
            ],
        },
    )

    with pytest.raises(ValueError, match="optimizer.primary_objective"):
        service.payloads._wfa_dashboard_artifact_path(run_id)


def test_wfa_dashboard_rejects_objective_artifact_mismatch(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260730_objective_mismatch"
    paths = service.registry.build_run_paths(run_id)
    artifact_dir = (
        paths["snapshot_dir"] / "managed_artifacts" / VALIDATION_WORKFLOW_CANONICAL
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact = artifact_dir / "wfa_calmar.parquet"
    pd.DataFrame(
        {
            "wfa_row_type": ["selected_optimum"],
            "objective": ["calmar"],
        }
    ).to_parquet(artifact, index=False)
    (paths["snapshot_dir"] / "wfa_run.json").write_text(
        json.dumps(
            {
                "schema_version": "wfa_run",
                "optimizer": {"primary_objective": "sharpe"},
            }
        ),
        encoding="utf-8",
    )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "schema_version": "1.0",
            "artifacts": [
                {
                    "artifact_type": "wfa_parquet",
                    "path": str(artifact),
                    "status": "ready",
                }
            ],
        },
    )

    with pytest.raises(ValueError, match="matching.*sharpe"):
        service.payloads._wfa_dashboard_artifact_path(run_id)


def test_wfa_dashboard_links_selected_window_backtests(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260514_wfa_window_links"
    backtest_id = "wfa_window_001_sharpe_lookback_20"
    candidate_id = "wfa_summary_probe:walk_forward_analysis:lookback_20"
    artifact_dir = (
        service.registry.build_run_paths(run_id)["snapshot_dir"]
        / "managed_artifacts"
        / VALIDATION_WORKFLOW_CANONICAL
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    wfa_path = artifact_dir / "wfa_window_links_selected_optimum.parquet"
    pd.DataFrame(
        {
            "window_id": [1],
            "objective": ["sharpe"],
            "semantic_combo": [json.dumps({"lookback": 20}, sort_keys=True)],
            "train_start": [pd.Timestamp("2020-01-02")],
            "train_end": [pd.Timestamp("2022-12-30")],
            "test_start": [pd.Timestamp("2023-01-03")],
            "test_end": [pd.Timestamp("2023-01-10")],
            "is_sharpe": [1.1],
            "is_calmar": [0.7],
            "oos_sharpe": [0.9],
            "oos_calmar": [0.4],
            "oos_total_return": [0.04],
            "selection_source": ["unified_portfolio_wfa"],
            "selection_rank": [1],
            "selection_metric": ["sharpe"],
            "selection_evidence": ["rank=1 by IS Sharpe"],
            "candidate_count": [3],
            "total_candidate_count": [12],
            "candidate_budget": [3],
            "candidate_budget_applied": [True],
            "candidate_budget_policy": ["seeded_random_sample"],
            "candidate_budget_method": ["seeded_random_sample"],
            "candidate_budget_seed": [17],
            "accepted": [False],
            "review_status": ["Review"],
            "acceptance_reasons": ["candidate did not pass acceptance gates"],
            "linked_backtest_run_id": [run_id],
            "linked_backtest_id": [backtest_id],
            "oos_portfolio_json": [
                json.dumps(
                    {
                        "asset_count": 1,
                        "allocation": [
                            {
                                "asset": "QQQ",
                                "avg_weight": 1.0,
                                "last_weight": 1.0,
                                "active_days": 6,
                            }
                        ],
                        "contribution": [
                            {
                                "asset": "QQQ",
                                "return_contribution": 0.04,
                                "avg_weight": 1.0,
                            }
                        ],
                        "active_rebalance_count": 1,
                        "avg_exposure": 1.0,
                        "avg_holdings": 1.0,
                        "total_turnover": 1.0,
                        "cost_drag": 0.0,
                    },
                    sort_keys=True,
                )
            ],
            "workflow": ["walk_forward_analysis"],
            "wfa_row_type": ["selected_optimum"],
        }
    ).to_parquet(wfa_path, index=False)

    dates = pd.date_range("2023-01-03", periods=6, freq="B")
    equity = pd.DataFrame(
        {
            "Time": dates,
            "Session_label": [value.strftime("%Y-%m-%d") for value in dates],
            "Equity_value": [100.0, 101.0, 102.0, 101.5, 103.0, 104.0],
            "Portfolio_return": [0.0, 0.01, 0.0099, -0.0049, 0.0148, 0.0097],
            "Weight_QQQ": [1.0] * 6,
            "Gross_exposure": [1.0] * 6,
            "Selected_count": [1] * 6,
            "Turnover": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "Trade_cost": [0.0] * 6,
        }
    )
    result = MultiAssetBacktestResult(
        strategy_id=candidate_id,
        equity_curve=equity,
        holdings=pd.DataFrame(
            {
                "Time": [dates[0]],
                "Asset": ["QQQ"],
                "Selected": [True],
                "Target_weight": [1.0],
                "Score": [1.0],
                "Eligible": [True],
            }
        ),
        rebalance_audit=pd.DataFrame(
            {
                "Time": [dates[0]],
                "Selected_count": [1],
                "Selected_assets": [["QQQ"]],
                "Turnover": [1.0],
            }
        ),
        rebalance_trades=pd.DataFrame(),
        feature_cache={},
        config={"strategy_id": candidate_id, "resolved_params": {"lookback": 20}},
        validation_report={
            "rust_timeline_accounting_summary": {
                "schema_version": "rust_timeline_accounting_summary.v1",
                "status": "executed",
                "total_return": 0.04,
            },
            "risk_gate_summary": {
                "schema_version": "risk_gate_summary.v1",
                "event_count": 0,
                "gates_triggered": [],
            },
        },
    )
    exported_paths = [
        Path(path)
        for path in MultiAssetPortfolioExporterBacktester(
            result=result,
            output_dir=artifact_dir / "window_backtests",
            run_id=backtest_id,
        ).export()
    ]

    service.registry.write_registry_entry(
        {
            "run_id": run_id,
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "entrypoint": "test",
            "status": "completed",
            "created_at": "2026-05-14T00:00:00",
            "completed_at": "2026-05-14T00:00:01",
            "config_filename": "wfa_window_links.json",
            "strategy_mode": "multi_asset_portfolio",
            "run_type": "test",
        }
    )
    (
        service.registry.build_run_paths(run_id)["snapshot_dir"] / "wfa_run.json"
    ).write_text(
        json.dumps(
            {
                "schema_version": "wfa_run",
                "optimizer": {"primary_objective": "sharpe"},
            }
        ),
        encoding="utf-8",
    )
    artifact_rows = [
        {"artifact_type": "wfa_parquet", "path": str(wfa_path), "status": "ready"}
    ]
    for path in exported_paths:
        name = path.name
        if name.endswith("_equity_curve.parquet"):
            artifact_type = "portfolio_equity_curve_parquet"
        elif name.endswith("_holdings.parquet"):
            artifact_type = "portfolio_holdings_parquet"
        elif name.endswith("_rebalance_audit.parquet"):
            artifact_type = "portfolio_rebalance_audit_parquet"
        elif name.endswith("_rebalance_trades.parquet"):
            artifact_type = "portfolio_rebalance_trades_parquet"
        elif name.endswith("_metadata.json"):
            artifact_type = "portfolio_metadata_json"
        elif name.endswith("_run_validation_report.json"):
            artifact_type = "portfolio_run_validation_json"
        else:
            continue
        artifact_rows.append(
            {"artifact_type": artifact_type, "path": str(path), "status": "ready"}
        )
    service.registry.write_artifact_manifest(
        run_id,
        {
            "schema_version": "1.0",
            "artifacts": artifact_rows,
        },
    )

    wfa_payload = service.wfa_dashboard(run_id)
    assert wfa_payload["rows"][0]["linked_backtest"] == {
        "run_id": run_id,
        "backtest_id": backtest_id,
    }
    assert wfa_payload["rows"][0]["candidate_budget_policy"] == "seeded_random_sample"
    assert wfa_payload["rows"][0]["candidate_budget_method"] == "seeded_random_sample"
    assert wfa_payload["rows"][0]["candidate_budget_seed"] == 17
    assert wfa_payload["rows"][0]["accepted"] is False
    assert wfa_payload["rows"][0]["review_status"] == "Review"
    assert (
        wfa_payload["batch_metadata"]["candidate_budget"]["candidate_budget_policy"]
        == "seeded_random_sample"
    )
    assert (
        wfa_payload["batch_metadata"]["candidate_budget"]["total_candidate_count"] == 12
    )
    assert wfa_payload["combo_groups"][0]["accepted"] is False
    assert wfa_payload["combo_groups"][0]["review_status"] == "Review"
    assert "candidate did not pass acceptance gates" in ";".join(
        wfa_payload["combo_groups"][0]["acceptance_reasons"]
    )
    assert run_id not in {row["run_id"] for row in service.metrics_runs()}
    with pytest.raises(FileNotFoundError, match="PlotBundle index not found"):
        service.metrics_overview(run_id)
    with pytest.raises(
        FileNotFoundError, match="backtest detail contract is unavailable"
    ):
        service.backtest_detail(run_id, backtest_id)


def test_future_live_search_config_uses_canonical_wfa_filenames(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path).payloads
    wfa_dir = tmp_path / "workspace" / "wfa"
    wfa_dir.mkdir(parents=True, exist_ok=True)
    run_id = "20260708_matrix_probe"
    canonical = wfa_dir / f"wfa-shortlist-{run_id}.user.json"
    canonical.write_text(
        json.dumps(
            {
                "wfa_config": {
                    "optimizer": {"mode": "optuna", "sampler": "tpe", "n_trials": 25},
                    "ranking": {"primary_metric": "sharpe"},
                    "acceptance": {"min_windows": 3},
                }
            }
        ),
        encoding="utf-8",
    )

    payload = service._load_future_live_search_config(run_id)  # noqa: SLF001 - payload contract regression guard.

    assert payload["source_filename"] == canonical.name
    assert payload["mode"] == "optuna"
    assert payload["sampler"] == "tpe"
    assert payload["n_trials"] == 25
    assert payload["ranking"] == {"primary_metric": "sharpe"}
    assert payload["acceptance"] == {"min_windows": 3}


def test_future_live_search_config_ignores_legacy_v2_wfa_filenames(
    tmp_path: Path,
) -> None:
    service = AppAPIService(tmp_path).payloads
    wfa_dir = tmp_path / "workspace" / "wfa"
    wfa_dir.mkdir(parents=True, exist_ok=True)
    run_id = "20260708_legacy_probe"
    legacy = wfa_dir / f"wfa-v2-shortlist-{run_id}.user.json"
    legacy.write_text(
        json.dumps(
            {
                "wfa_config": {
                    "optimizer": {"mode": "optuna", "sampler": "tpe", "n_trials": 10}
                }
            }
        ),
        encoding="utf-8",
    )

    payload = service._load_future_live_search_config(run_id)  # noqa: SLF001 - payload contract regression guard.

    assert payload == {}


def test_wfa_strategy_summary_uses_embedded_strategy_run(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)
    run_id = "20260506_wfa_summary"
    config_dir = tmp_path / "workspace" / "wfa"
    config_dir.mkdir(parents=True, exist_ok=True)
    wfa_config_path = config_dir / "wfa_summary.json"
    wfa_config_path.write_text(
        '{"schema_version":"wfa_run","strategy_run_path":"workspace/runs/strategy.json",'
        '"platform":{"workflow_id":"walk_forward_analysis"}}',
        encoding="utf-8",
    )
    strategy_config = {
        "schema_version": "strategy_run",
        "platform": {
            "strategy_mode_id": "multi_asset_portfolio",
            "strategy_profile_id": "selection_timing_portfolio",
            "strategy_preset_id": "single_asset_signal",
            "workflow_id": "parameter_matrix",
        },
        "data": _typed_daily_data(
            "yfinance",
            start_date="2009-01-01",
            benchmark={"symbol": "QQQ", "label": "QQQ Buy & Hold"},
        ),
        "universe": {"symbols": ["QQQ"]},
        "signals": {"target_weight": 1.0},
        "selection": {"rank_by": "close", "top_n": 1},
        "allocation": {"method": "equal_weight"},
        "fill_model": {
            "timing": "next_bar_after_signal",
            "price": "open",
            "cost": {"transaction_cost": 0.001, "slippage": 0.0005},
        },
        "parameter_domains": {
            "entry_ma": {"type": "range", "start": 10, "end": 500, "step": 10},
            "exit_ma": {"type": "range", "start": 10, "end": 500, "step": 10},
        },
        "metadata": {
            "strategy_id": "wfa_summary_probe",
            "strategy_rules": {
                "mode_label": "Single-asset signal strategy",
                "entry_rule": "Buy QQQ when close crosses above entry SMA",
                "exit_rule": "Exit when close crosses below exit SMA",
            },
        },
    }
    service.registry.write_snapshot_file(
        run_id,
        "run_snapshot.json",
        {
            "resolved_configs": {
                "run_config": {"config_path": str(wfa_config_path)},
                "dataloader_config": {
                    "source": "yfinance",
                    "provider": "yfinance",
                    "start_date": "2009-01-01",
                    "asset_symbols": ["QQQ"],
                    "bar_time": strategy_config["data"]["bar_time"],
                    "stream_binding": strategy_config["data"]["stream_binding"],
                },
                "backtester_config": {"strategy_run_config": strategy_config},
                "wfa_config": {"mode": "standard"},
            }
        },
    )

    summary = service.payloads._strategy_summary(run_id)

    assert summary["workflow_id"] == "walk_forward_analysis"
    assert summary["asset_label"] == "QQQ"
    assert summary["entry_rule"] == "Buy QQQ when close crosses above entry SMA"
    assert summary["exit_rule"] == "Exit when close crosses below exit SMA"
    assert summary["parameter_domains"]["entry_ma"]["start"] == 10
    assert summary["time_context"]["execution"]["stream_id"] == "execution_daily"
    assert summary["time_context"]["decision"]["stream_id"] == "execution_daily"
    assert summary["time_context"]["session"]["calendar_id"] == "XNYS"
    assert summary["time_context"]["timestamp"]["time_standard"] == "UTC"


def test_apply_normalized_strategy_summary_prefers_profile_label_over_legacy_mode_label(
    tmp_path: Path,
) -> None:
    service = AppAPIService(tmp_path)

    normalized = service.payloads._normalized_strategy_config(
        {
            "schema_version": "strategy_run",
            "platform": {
                "strategy_mode_id": "multi_asset_portfolio",
                "strategy_profile_id": "selection_timing_portfolio",
                "strategy_preset_id": "single_asset_signal",
                "workflow_id": "single_backtest",
            },
            "data": _typed_daily_data("fixture"),
            "universe": {"symbols": ["QQQ"]},
            "signals": {"target_weight": 1.0},
            "selection": {"rank_by": "close", "top_n": 1},
            "allocation": {"method": "equal_weight"},
            "rebalance": {"trigger": {"op": "calendar.every_session"}},
            "fill_model": {"cost": {}},
            "risk": {},
            "metadata": {"strategy_id": "qqq_profile_probe"},
            "parameter_domains": {},
            "outputs": {},
        },
        None,
    )
    summary = service.payloads._apply_normalized_strategy_summary(
        {
            "strategy_mode_id": "single_asset_signal",
            "mode_label": "Single-asset signal strategy",
        },
        normalized,
    )

    assert summary["strategy_mode_id"] == "multi_asset_portfolio"
    assert summary["strategy_profile_id"] == "selection_timing_portfolio"
    assert summary["strategy_preset_id"] == "single_asset_signal"
    assert summary["mode_label"] == "Selection Timing Portfolio"


def test_trade_style_portfolio_detection_accepts_unified_selection_profile(
    tmp_path: Path,
) -> None:
    service = AppAPIService(tmp_path)

    assert (
        service.payloads._is_trade_style_portfolio(  # pylint: disable=protected-access
            {
                "strategy_mode_id": "multi_asset_portfolio",
                "strategy_profile_id": "selection_timing_portfolio",
                "strategy_preset_id": "single_asset_signal",
            }
        )
        is True
    )

    assert (
        service.payloads._is_trade_style_portfolio(  # pylint: disable=protected-access
            {
                "strategy_mode_id": "multi_asset_portfolio",
                "strategy_profile_id": "calendar_event_portfolio",
            }
        )
        is True
    )


def test_workflow_label_uses_validation_workflow_wording(tmp_path: Path) -> None:
    service = AppAPIService(tmp_path)

    assert (
        service.payloads._workflow_label("walk_forward_analysis")
        == "Validation workflow (WFA)"
    )  # pylint: disable=protected-access
    assert (
        service.payloads._workflow_label("rolling_validation")
        == "Validation workflow (rolling)"
    )  # pylint: disable=protected-access


def test_allocation_change_display_does_not_synthesize_trade_returns(
    tmp_path: Path,
) -> None:
    service = AppAPIService(tmp_path).payloads
    frame = service._annotate_allocation_change_trade_side(  # noqa: SLF001
        pd.DataFrame(
            [
                {
                    "Asset": "QQQ",
                    "Action": "buy",
                    "Before_weight": 0.0,
                    "Target_weight": 1.0,
                    "Execution_price": 100.0,
                },
                {
                    "Asset": "QQQ",
                    "Action": "exit",
                    "Before_weight": 1.0,
                    "Target_weight": 0.0,
                    "Execution_price": 110.0,
                },
            ]
        )
    )

    assert "Trade_pnl_pct" not in frame.columns
    assert frame["Trade_side"].tolist() == ["long", "long"]
