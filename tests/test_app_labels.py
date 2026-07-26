import json
import re
from pathlib import Path

import pytest

from app.runtime.module_identity import VALIDATION_WORKFLOW_CANONICAL
from app.api.labels import (
    canonical_artifact_filename,
    config_filename,
    decorate_config_item,
    decorate_run_label,
    display_run_type,
    infer_label_badges,
    load_app_config_metadata,
    normalize_run_type,
)


def test_normalize_run_type_collapses_legacy_terms_to_test() -> None:
    assert normalize_run_type("production") == "production"
    assert normalize_run_type("test") == "test"
    assert normalize_run_type("smoke") == "test"
    assert normalize_run_type("latest") == "test"
    assert normalize_run_type("sweep") == "test"


def test_display_run_type_only_shows_production_or_test() -> None:
    assert display_run_type("production") == "Production"
    assert display_run_type("smoke") == "Test"
    assert display_run_type("latest") == "Test"
    assert display_run_type("unknown") == ""


def test_infer_label_badges_is_no_longer_filename_driven() -> None:
    assert infer_label_badges("run-v2-sweep-multigrid-smoke.user.json") == []


def test_config_filename_returns_leaf_name() -> None:
    assert (
        config_filename(r"C:\workspace\foo\run-v2-spy-breadth-vix-example.user.json")
        == "run-v2-spy-breadth-vix-example.user.json"
    )
    assert (
        config_filename("/workspace/foo/run-v2-spy-breadth-vix-example.user.json")
        == "run-v2-spy-breadth-vix-example.user.json"
    )


def test_config_label_uses_file_mtime_when_created_at_is_missing() -> None:
    item = decorate_config_item(
        {
            "label": "strategy-run-example.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "test"},
            "raw_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "workflow_id": "single_backtest",
                    "run_type": "test",
                },
                "universe": {"symbols": ["QQQ"]},
                "computed_fields": [],
                "allocation": {},
                "fill_model": {},
                "risk": {},
                "parameter_domains": {},
                "outputs": {},
            },
            "config_mtime": 1_751_328_000,
        },
        "autorunner",
    )

    assert item["display_label"].startswith("2025-07-01 |")


def test_decorate_config_item_uses_filename_and_config_run_type() -> None:
    item = decorate_config_item(
        {
            "label": r"C:\workspace\foo\run-v2-spy-breadth-vix-example.user.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "production", "is_default": True},
            "raw_config": {
                "dataloader": {"yfinance_config": {"symbol": "SPY"}},
                "backtester": {
                    "strategy_contract_path": "workspace/strategies/strategy-v2-spy-breadth-vix-example.user.json",
                    "feature_contract_path": "workspace/features/feature-contract-spy-breadth-vix-v1.user.json",
                    "selected_predictor": "X",
                },
            },
            "config_hash": "abcdef",
            "config_created_at": "2026-07-01T00:00:00+08:00",
        },
        "autorunner",
    )
    assert item["filename"].endswith("run-v2-spy-breadth-vix-example.user.json")
    assert item["display_label"] == "2026-07-01 | SPY | Breadth + VIX | Single Backtest | #abcdef"
    assert re.match(r"backtest_\d{8}_SPY_Breadth-VIX_strategy_single_abcdef\.json", item["canonical_filename"])
    assert item["badges"] == ["Default"]
    assert item["is_default"] is True
    assert item["metadata_complete"] is True


def test_wfa_config_label_removes_redundant_workflow_prefix() -> None:
    item = decorate_config_item(
        {
            "label": "wfa-run-qqq-example.json",
            "value": "x",
            "summary": {},
            "platform": {
                "workflow_id": "walk_forward_analysis",
                "run_type": "example",
                "is_default": True,
                "created_at": "2026-06-17",
                "display_label": "Workflow | WFA | QQQ | Daily SMA Cross | yfinance",
            },
            "raw_config": {"schema_version": "wfa_run"},
            "config_hash": "ee820e",
        },
        VALIDATION_WORKFLOW_CANONICAL,
    )
    assert item["display_label"] == "2026-06-17 | QQQ | Daily SMA Cross | yfinance | #ee820e"
    assert "Workflow | WFA" not in item["display_label"]
    assert item["badges"] == ["Default"]
    assert item["is_default"] is True


def test_rolling_validation_config_label_keeps_meaningful_mode() -> None:
    item = decorate_config_item(
        {
            "label": "wfa-run-voo-gld-example.json",
            "value": "x",
            "summary": {},
            "platform": {
                "workflow_id": "rolling_validation",
                "run_type": "example",
                "is_default": True,
                "created_at": "2026-06-17T22:26:47+08:00",
                "display_label": "Workflow | WFA / Rolling Validation | VOO GLD | Momentum | yfinance",
            },
            "raw_config": {"schema_version": "wfa_run"},
            "config_hash": "7ff9a1",
        },
        VALIDATION_WORKFLOW_CANONICAL,
    )
    assert item["display_label"] == (
        "2026-06-17 | VOO GLD | Momentum | yfinance | Rolling Validation | #7ff9a1"
    )
    assert "Workflow | WFA" not in item["display_label"]
    assert item["badges"] == ["Default"]
    assert item["is_default"] is True


def test_decorate_run_label_prefers_config_filename_and_module_display() -> None:
    payload = decorate_run_label(
        {
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "semantic_label": "wfa-v2-app-smoke",
            "config_filename": "wfa-v2-latest.user.json",
            "run_type": "test",
            "run_id": "20260425_5fd126d7d8c6",
            "dataloader_config": {"yfinance_config": {"symbol": "QQQ"}},
            "backtester_config": {
                "strategy_contract_path": "workspace/strategies/strategy-v2-qqq-price-ma-cross-sweep.user.json",
                "feature_contract_path": "workspace/features/feature-contract-qqq-price-only-v1.user.json",
                "selected_predictor": "X",
            },
        }
    )
    assert payload["display_label"] == (
        "2026-04-25 | QQQ | MA Cross | Rolling Windows | run 5fd126"
    )
    assert payload["selector_label"] == payload["display_label"]
    assert payload["label_badges"] == []
    assert payload["module_display"] == "Validation Workflow"


def test_statanalyser_release_label_uses_factor_analysis_display_with_predictor_slug() -> None:
    payload = decorate_run_label(
        {
            "module": "statanalyser",
            "run_id": "20260501_abc123def456",
            "dataloader_config": {"yfinance_config": {"symbol": "SPY"}},
            "backtester_config": {
                "feature_contract_path": "workspace/features/feature-contract-spy-breadth-v1.user.json",
                "selected_predictor": "X",
            },
        }
    )
    assert payload["module_display"] == "Factor Analysis"
    assert payload["display_label"].startswith("2026-05-01 | SPY | Strategy | Summary")
    assert payload["identity"]["workflow"] == "predictor"


def test_file_source_asset_uses_dataset_label_even_with_feature_metadata(tmp_path: Path, monkeypatch) -> None:
    feature_path = tmp_path / "workspace" / "features" / "feature-contract-vix-price-v1.user.json"
    feature_path.parent.mkdir(parents=True)
    feature_path.write_text(
        json.dumps({"schema_version": "1.0", "dataset_id": "qqq_price_plus_vix_daily_v1"}),
        encoding="utf-8-sig",
    )
    monkeypatch.chdir(tmp_path)
    payload = decorate_run_label(
        {
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "run_id": "20260415_d1c3219130cc",
            "dataloader_config": {
                "source": "file",
                "file_config": {"file_path": "workspace/datasets/price.csv"},
            },
            "backtester_config": {
                "feature_contract_path": "workspace/features/feature-contract-vix-price-v1.user.json",
                "strategy_contract_path": "workspace/strategies/strategy-v2-vix-regime-ma-cross.user.json",
                "selected_predictor": "X",
            },
        }
    )
    assert payload["identity"]["asset"] == "DATASET"
    assert payload["display_label"] == (
        "2026-04-15 | DATASET | MA Cross | Rolling Windows | run d1c321"
    )
    assert "LOCAL" not in payload["display_label"]


def test_file_source_without_asset_metadata_uses_dataset_placeholder() -> None:
    payload = decorate_run_label(
        {
            "module": VALIDATION_WORKFLOW_CANONICAL,
            "run_id": "20260415_d1c3219130cc",
            "dataloader_config": {
                "source": "file",
                "file_config": {"file_path": "workspace/datasets/price.csv"},
            },
            "backtester_config": {
                "feature_contract_path": "workspace/features/feature-contract-custom-price-v1.user.json",
                "selected_predictor": "X",
            },
        }
    )
    assert payload["identity"]["asset"] == "DATASET"
    assert "LOCAL" not in payload["display_label"]


def test_decorate_run_label_marks_legacy_incomplete_state_flags() -> None:
    payload = decorate_run_label(
        {
            "module": "autorunner",
            "semantic_label": "run-v2-latest",
            "config_filename": "run-v2-latest.user.json",
            "run_type": "test",
            "run_id": "20260425_abc123def456",
            "semantic_index_complete": False,
            "strategy_label_mode": "internal_id_fallback",
        }
    )
    assert payload["label_badges"] == []
    assert payload["is_legacy_result"] is True
    assert payload["has_incomplete_strategy_labels"] is True


def test_load_app_config_metadata_uses_filename_and_explicit_run_type(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "run-v2-demo.user.json"
    config_path.write_text(
        json.dumps(
            {
                "platform": {
                    "display_label": "Ignored Demo Label",
                    "created_at": "2026-07-01",
                    "run_type": "production",
                    "is_default": True,
                },
                "dataloader": {"yfinance_config": {"symbol": "SPY"}},
                "backtester": {
                    "strategy_contract_path": "workspace/strategies/strategy-v2-spy-breadth-vix-example.user.json",
                    "feature_contract_path": "workspace/features/feature-contract-spy-breadth-vix-v1.user.json",
                    "selected_predictor": "X",
                },
            }
        ),
        encoding="utf-8",
    )
    payload = load_app_config_metadata(str(config_path), "autorunner")
    assert payload["display_label"].startswith("2026-07-01 | Ignored Demo Label | #")
    assert "Backtest |" not in payload["display_label"]
    assert payload["canonical_filename"].startswith("backtest_")
    assert payload["badges"] == ["Default"]
    assert payload["is_default"] is True
    assert payload["metadata_complete"] is True


def test_load_app_config_metadata_rejects_corrupt_json(tmp_path: Path) -> None:
    config_path = tmp_path / "broken.json"
    config_path.write_text("{bad json", encoding="utf-8")

    with pytest.raises(ValueError, match="broken.json"):
        load_app_config_metadata(str(config_path), "autorunner")


def test_load_app_config_metadata_rejects_non_object_json(tmp_path: Path) -> None:
    config_path = tmp_path / "array.json"
    config_path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        load_app_config_metadata(str(config_path), "autorunner")


def test_strategy_run_multi_asset_label_uses_universe_symbols() -> None:
    item = decorate_config_item(
        {
            "label": "backtest_20260501_MULTI_VOO-GLD_price-momentum-sma-rotation_matrix_7a91c4.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "test"},
            "raw_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "workflow_id": "parameter_matrix",
                    "run_type": "test",
                },
                "universe": {"symbols": ["VOO", "GLD"]},
                "features": [
                    {"name": "return_momentum", "op": "indicator.momentum", "source": "close"},
                    {"name": "sma_filter", "op": "indicator.sma", "source": "close"},
                ],
                "allocation": {"method": "equal_weight"},
                "metadata": {"strategy_id": "voo_gld_momentum_sma_rotation"},
            },
            "config_hash": "376aa3",
        },
        "autorunner",
    )
    assert item["display_label"] == (
        "2026-05-01 | VOO-GLD | Price | Parameter Matrix | #376aa3"
    )
    assert item["canonical_filename"].startswith(
        "backtest_20260501_VOO-GLD_PRICE_momentum-sma-rotation_matrix_376aa3"
    )


def test_strategy_run_fixed_allocation_label_uses_all_assets() -> None:
    item = decorate_config_item(
        {
            "label": "backtest_20260501_MULTI_VTI-AVUV-VXUS-SGOL-DBMF_fixed-annual-rebalance_single_2b84d1.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "test"},
            "raw_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "workflow_id": "single_backtest",
                    "run_type": "test",
                },
                "universe": {"symbols": ["VTI", "AVUV", "VXUS", "SGOL", "DBMF"]},
                "features": [],
                "allocation": {"method": "fixed_weights"},
                "metadata": {"strategy_id": "vti_avuv_vxus_sgol_dbmf_fixed_annual_rebalance"},
            },
            "config_hash": "195913",
        },
        "autorunner",
    )
    assert item["display_label"] == (
        "2026-05-01 | VTI-AVUV-VXUS-SGOL-DBMF | Allocation | Single Backtest | #195913"
    )


def test_strategy_run_selection_timing_profile_uses_profile_aware_label() -> None:
    item = decorate_config_item(
        {
            "label": "backtest_20260708_MULTI_VOO-QQQ-IWM-GLD_selection-timing_single_24fd1a.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "test"},
            "raw_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "strategy_profile_id": "selection_timing_portfolio",
                    "workflow_id": "single_backtest",
                    "run_type": "test",
                },
                "universe": {"symbols": ["VOO", "QQQ", "IWM", "GLD"]},
                "computed_fields": [
                    {"name": "momentum_60", "op": "indicator.momentum", "source": "close", "period": 60}
                ],
                "selection": {"rank_by": "momentum_60", "top_n": 2},
                "allocation": {},
                "metadata": {"strategy_id": "selection_timing_profile_probe"},
            },
            "config_hash": "24fd1a",
        },
        "autorunner",
    )
    assert item["display_label"] == (
        "2026-07-08 | VOO-QQQ-IWM-GLD | Selection | Single Backtest | #24fd1a"
    )
    assert item["canonical_filename"].startswith(
        "backtest_20260708_VOO-QQQ-IWM-GLD_SELECTION_selection-timing_single_24fd1a"
    )


def test_strategy_run_pair_spread_profile_uses_profile_aware_label() -> None:
    item = decorate_config_item(
        {
            "label": "backtest_20260708_MULTI_SPY-QQQ_pair-spread_single_83ab19.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "test"},
            "raw_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "strategy_profile_id": "pair_spread_portfolio",
                    "workflow_id": "single_backtest",
                    "run_type": "test",
                },
                "universe": {"symbols": ["SPY", "QQQ"]},
                "allocation": {"method": "fixed_weights"},
                "fill_model": {"timing": "timeline"},
                "risk": {"allow_short": True},
                "metadata": {"strategy_id": "pair_spread_profile_probe"},
            },
            "config_hash": "83ab19",
        },
        "autorunner",
    )
    assert item["display_label"] == (
        "2026-07-08 | SPY-QQQ | Pair | Single Backtest | #83ab19"
    )
    assert item["canonical_filename"].startswith(
        "backtest_20260708_SPY-QQQ_PAIR_pair-spread_single_83ab19"
    )


def test_strategy_run_multi_leg_profile_uses_profile_aware_label() -> None:
    item = decorate_config_item(
        {
            "label": "backtest_20260708_MULTI_QQQ-TLT-GLD_multi-leg-event_single_ba129e.json",
            "value": "x",
            "summary": {},
            "platform": {"run_type": "test"},
            "raw_config": {
                "schema_version": "strategy_run",
                "platform": {
                    "strategy_mode_id": "multi_asset_portfolio",
                    "strategy_profile_id": "multi_leg_event_portfolio",
                    "workflow_id": "single_backtest",
                    "run_type": "test",
                },
                "universe": {"symbols": ["QQQ", "TLT", "GLD"]},
                "allocation": {"method": "fixed_weights"},
                "fill_model": {"timing": "timeline"},
                "risk": {"allow_short": False},
                "metadata": {"strategy_id": "multi_leg_profile_probe"},
            },
            "config_hash": "ba129e",
        },
        "autorunner",
    )
    assert item["display_label"] == (
        "2026-07-08 | QQQ-TLT-GLD | Multi-Leg | Single Backtest | #ba129e"
    )
    assert item["canonical_filename"].startswith(
        "backtest_20260708_QQQ-TLT-GLD_MULTI-LEG_multi-leg-event_single_ba129e"
    )


def test_strategy_run_result_label_uses_user_concept_without_generic_prefixes() -> None:
    payload = decorate_run_label(
        {
            "module": "autorunner",
            "run_id": "20260713_622d003ff9ee",
            "backtester_config": {
                "strategy_run_config": {
                    "schema_version": "strategy_run",
                    "platform": {
                        "workflow_id": "parameter_matrix",
                        "run_type": "example",
                        "display_label": "BTC-USD | Monthly Nth Weekday Same Session | Coinbase | Example",
                    },
                    "data": {"provider": "coinbase"},
                    "universe": {"symbols": ["BTC-USD"]},
                    "metadata": {"strategy_id": "btcusd_monthly_nth_weekday"},
                }
            },
        }
    )

    assert payload["display_label"] == (
        "2026-07-13 | BTC-USD | Monthly Nth Weekday Same Session | Parameter Matrix | run 622d00"
    )
    assert payload["identity"]["concept_display"] == "Monthly Nth Weekday Same Session"
    assert "Backtest" not in payload["display_label"]
    assert "MULTI" not in payload["display_label"]


def test_strategy_run_result_label_keeps_run_type_out_of_strategy_concept() -> None:
    payload = decorate_run_label(
        {
            "module": "autorunner",
            "run_id": "20260723_d7bf27feb46b",
            "run_type": "test",
            "backtester_config": {
                "strategy_run_config": {
                    "schema_version": "strategy_run",
                    "platform": {
                        "workflow_id": "single_backtest",
                        "run_type": "test",
                        "display_label": (
                            "US Sector ETF | Monthly Adjusted 12-1 Long Short Rotation"
                            " | Research Test"
                        ),
                    },
                    "data": {"provider": "yfinance"},
                    "universe": {
                        "symbols": [
                            "XLB",
                            "XLC",
                            "XLE",
                            "XLF",
                            "XLI",
                            "XLK",
                        ]
                    },
                    "metadata": {"strategy_id": "adjusted_12_1_rotation"},
                }
            },
        }
    )

    assert payload["display_label"] == (
        "2026-07-23 | 6-ASSETS | Monthly Adjusted 12-1 Long Short Rotation"
        " | Single Backtest | run d7bf27"
    )
    assert payload["identity"]["concept_display"] == (
        "Monthly Adjusted 12-1 Long Short Rotation"
    )


def test_canonical_artifact_filename_keeps_run_id_at_end() -> None:
    identity = {
        "workflow": "wfa",
        "date": "20260425",
        "asset": "SPY",
        "factor_slug": "MOMENTUM-VIX",
        "strategy_slug": "hold-reset",
        "mode": "windows",
        "short_id": "5fd126",
    }
    filename = canonical_artifact_filename(
        identity=identity,
        artifact_type="wfa_parquet",
        source_name="20260425_SPY_momentum_vix_wfa_sharpe_abc.parquet",
        suffix="",
    )
    assert filename == "wfa_20260425_SPY_MOMENTUM-VIX_hold-reset_windows_sharpe_5fd126.parquet"


def test_portfolio_canonical_artifact_filename_bounds_long_components() -> None:
    identity = {
        "workflow": "backtest",
        "date": "20260623",
        "asset": "SPY-QQQ-VOO-GLD-IEF",
        "factor_slug": "PRICE-" + "VERY-LONG-" * 12,
        "strategy_slug": "calendar-event-" + "quarterly-nth-weekday-same-session-" * 4,
        "mode": "matrix",
        "short_id": "abc123",
    }
    source_name = "20260623_portfolio_" + "_".join(f"very_long_factor_name_{idx:02d}" for idx in range(20)) + ".parquet"

    filename = canonical_artifact_filename(
        identity=identity,
        artifact_type="portfolio_equity_curve_parquet",
        source_name=source_name,
        suffix="",
    )

    assert filename.endswith("_abc123.parquet")
    assert "portfolio-equity" in filename
    assert len(filename) <= 170
