"""Export full-result portfolio matrix artifacts as compact bundle files."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from .BacktestResult_backtester import MultiAssetBacktestResult
from .result_integrity import canonical_equity_summary
from .RuntimeContracts_backtester import build_canonical_result_bundle


class MultiAssetPortfolioBundleExporterBacktester:
    """Write all portfolio matrix candidates into shared parquet/json bundles.

    This keeps every candidate's full result rows while avoiding one file set per
    candidate. Downstream readers filter bundle tables by ``Backtest_id``.
    """

    def __init__(
        self,
        *,
        results: Sequence[MultiAssetBacktestResult],
        output_dir: Optional[str | Path] = None,
        run_id: str = "",
    ) -> None:
        self.results = list(results)
        self.run_id = self._slugify(run_id or "portfolio_matrix")
        self.output_dir = Path(output_dir) if output_dir else Path(__file__).resolve().parent.parent / "outputs" / "portfolio"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(self) -> List[str]:
        paths: List[str] = []
        date_prefix = datetime.now().strftime("%Y%m%d")
        base = f"{date_prefix}_portfolio_matrix_{self.run_id[:12]}"

        table_specs = {
            "equity_curve": "equity_curve",
            "holdings": "holdings",
            "rebalance_audit": "rebalance_audit",
            "rebalance_trades": "rebalance_trades",
            "risk_gate_events": "risk_gate_events",
        }
        table_paths: Dict[str, str] = {}
        for artifact_name, attr_name in table_specs.items():
            frame = self._combined_frame(attr_name)
            if frame.empty:
                continue
            parquet_path = self.output_dir / f"{base}_{artifact_name}.parquet"
            frame.to_parquet(parquet_path, index=False, compression="zstd")
            paths.append(str(parquet_path))
            table_paths[artifact_name] = str(parquet_path)

        metadata_path = self.output_dir / f"{base}_metadata.json"
        metadata_path.write_text(
            json.dumps(
                self._metadata_payload(paths=paths, table_paths=table_paths),
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        paths.append(str(metadata_path))

        validation_path = self.output_dir / f"{base}_run_validation_report.json"
        validation_path.write_text(
            json.dumps(
                self._validation_payload(paths=paths, table_paths=table_paths),
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        paths.append(str(validation_path))
        return paths

    def _combined_frame(self, attr_name: str) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        for result in self.results:
            frame = getattr(result, attr_name, pd.DataFrame())
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            out = frame.copy()
            if "Backtest_id" in out.columns:
                out["Backtest_id"] = str(result.strategy_id)
            else:
                out.insert(0, "Backtest_id", str(result.strategy_id))
            frames.append(out)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True, sort=False)

    def _metadata_payload(self, *, paths: List[str], table_paths: Dict[str, str]) -> Dict[str, Any]:
        candidates = [self._candidate_metadata(result) for result in self.results]
        return build_canonical_result_bundle(
            run_id=self.run_id,
            candidates=candidates,
            table_paths=table_paths,
            artifact_paths=paths,
            artifact_type="multi_asset_portfolio_matrix_bundle",
        )

    def _validation_payload(self, *, paths: List[str], table_paths: Dict[str, str]) -> Dict[str, Any]:
        return {
            "schema_version": "multi_asset_portfolio_bundle_validation.v1",
            "contract_id": "lo2cin4bt-multi-asset-portfolio-bundle-validation-v1",
            "run_id": self.run_id,
            "candidate_count": len(self.results),
            "bundle_paths": dict(table_paths),
            "artifact_paths": list(paths),
            "candidates": [
                {
                    "strategy_id": result.strategy_id,
                    "run_validation": result.validation_report,
                    "artifact_consistency": {
                        "equity_rows": int(len(result.equity_curve)),
                        "holding_rows": int(len(result.holdings)),
                        "rebalance_rows": int(len(result.rebalance_audit)),
                        "rebalance_trade_rows": int(len(result.rebalance_trades)),
                        "risk_gate_event_rows": int(len(getattr(result, "risk_gate_events", pd.DataFrame()))),
                    },
                }
                for result in self.results
            ],
        }

    def _candidate_metadata(self, result: MultiAssetBacktestResult) -> Dict[str, Any]:
        equity_summary = canonical_equity_summary(result.equity_curve)
        validation = result.validation_report if isinstance(result.validation_report, dict) else {}
        return {
            "schema_version": "multi_asset_portfolio_export.v1",
            "artifact_type": "multi_asset_portfolio_backtest",
            "strategy_id": result.strategy_id,
            "run_id": result.strategy_id,
            "row_counts": {
                "equity_curve": int(len(result.equity_curve)),
                "holdings": int(len(result.holdings)),
                "rebalance_audit": int(len(result.rebalance_audit)),
                "rebalance_trades": int(len(result.rebalance_trades)),
                "risk_gate_events": int(len(getattr(result, "risk_gate_events", pd.DataFrame()))),
            },
            "summary": {
                **equity_summary,
                "rebalance_count": int(len(result.rebalance_audit)),
            },
            "feature_cache": result.feature_cache,
            "run_validation": result.validation_report,
            "universe_provenance": validation.get("universe_provenance", {}),
            "factor_feature_audit": validation.get("factor_feature_audit", {}),
            "config": result.config,
        }

    @staticmethod
    def _slugify(value: Any) -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip())
        slug = re.sub(r"_+", "_", slug).strip("_")
        return slug or "portfolio_matrix"
