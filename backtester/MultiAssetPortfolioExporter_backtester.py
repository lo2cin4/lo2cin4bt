"""Export multi-asset portfolio backtest artifacts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .BacktestResult_backtester import MultiAssetBacktestResult
from .result_integrity import canonical_equity_summary


class MultiAssetPortfolioExporterBacktester:
    def __init__(
        self,
        *,
        result: MultiAssetBacktestResult,
        output_dir: Optional[str | Path] = None,
        run_id: str = "",
        export_csv: bool = False,
    ) -> None:
        self.result = result
        self.run_id = self._slugify(run_id or result.strategy_id or "portfolio", max_length=64)
        self.export_csv = bool(export_csv)
        self.output_dir = Path(output_dir) if output_dir else Path(__file__).resolve().parent.parent / "outputs" / "portfolio"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export(self) -> List[str]:
        paths: List[str] = []
        date_prefix = datetime.now().strftime("%Y%m%d")
        strategy_slug = self._slugify(self.result.strategy_id, max_length=48)
        run_slug = self._slugify(self.run_id, max_length=12)
        base = f"{date_prefix}_portfolio_{strategy_slug}_{run_slug}"

        artifacts = {
            "equity_curve": self.result.equity_curve,
            "holdings": self.result.holdings,
            "rebalance_audit": self.result.rebalance_audit,
            "rebalance_trades": self.result.rebalance_trades,
            "risk_gate_events": getattr(self.result, "risk_gate_events", pd.DataFrame()),
        }
        for name, frame in artifacts.items():
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            parquet_path = self.output_dir / f"{base}_{name}.parquet"
            frame.to_parquet(parquet_path, index=False, compression="zstd")
            paths.append(str(parquet_path))
            if self.export_csv:
                csv_path = self.output_dir / f"{base}_{name}.csv"
                frame.to_csv(csv_path, index=False)
                paths.append(str(csv_path))

        metadata_path = self.output_dir / f"{base}_metadata.json"
        metadata_path.write_text(
            json.dumps(self._metadata_payload(paths), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        paths.append(str(metadata_path))

        validation_path = self.output_dir / f"{base}_run_validation_report.json"
        validation_path.write_text(
            json.dumps(self._validation_payload(paths), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        paths.append(str(validation_path))

        risk_summary_path = self.output_dir / f"{base}_risk_gate_summary.json"
        risk_summary_path.write_text(
            json.dumps(self._risk_gate_summary_payload(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        paths.append(str(risk_summary_path))
        return paths

    def _metadata_payload(self, artifact_paths: List[str]) -> Dict[str, Any]:
        equity_summary = canonical_equity_summary(self.result.equity_curve)
        validation = self.result.validation_report if isinstance(self.result.validation_report, dict) else {}
        return {
            "schema_version": "multi_asset_portfolio_export.v1",
            "artifact_type": "multi_asset_portfolio_backtest",
            "strategy_id": self.result.strategy_id,
            "run_id": self.run_id,
            "generated_at": datetime.now().isoformat(),
            "row_counts": {
                "equity_curve": int(len(self.result.equity_curve)),
                "holdings": int(len(self.result.holdings)),
                "rebalance_audit": int(len(self.result.rebalance_audit)),
                "rebalance_trades": int(len(self.result.rebalance_trades)),
                "risk_gate_events": int(len(getattr(self.result, "risk_gate_events", pd.DataFrame()))),
            },
            "summary": {
                **equity_summary,
                "rebalance_count": int(len(self.result.rebalance_audit)),
            },
            "feature_cache": self.result.feature_cache,
            "run_validation": self.result.validation_report,
            "universe_provenance": validation.get("universe_provenance", {}),
            "factor_feature_audit": validation.get("factor_feature_audit", {}),
            "artifact_paths": artifact_paths,
            "config": self.result.config,
        }

    def _validation_payload(self, artifact_paths: List[str]) -> Dict[str, Any]:
        payload = dict(self.result.validation_report or {})
        payload.setdefault("schema_version", "multi_asset_run_validation.v1")
        payload.setdefault("contract_id", "lo2cin4bt-multi-asset-run-validation-v1")
        payload["strategy_id"] = self.result.strategy_id
        payload["run_id"] = self.run_id
        payload["artifact_consistency"] = {
            "equity_rows": int(len(self.result.equity_curve)),
            "holding_rows": int(len(self.result.holdings)),
            "rebalance_rows": int(len(self.result.rebalance_audit)),
            "rebalance_trade_rows": int(len(self.result.rebalance_trades)),
            "risk_gate_event_rows": int(len(getattr(self.result, "risk_gate_events", pd.DataFrame()))),
            "weight_columns": [
                str(col)
                for col in self.result.equity_curve.columns
                if str(col).startswith("Weight_")
            ],
            "artifact_paths": list(artifact_paths),
        }
        return payload

    def _risk_gate_summary_payload(self) -> Dict[str, Any]:
        validation = self.result.validation_report if isinstance(self.result.validation_report, dict) else {}
        summary = validation.get("risk_gate_summary")
        if isinstance(summary, dict):
            return dict(summary)
        risk_gate_events = getattr(self.result, "risk_gate_events", pd.DataFrame())
        return {
            "schema_version": "risk_gate_summary.v1",
            "event_count": int(len(risk_gate_events)) if isinstance(risk_gate_events, pd.DataFrame) else 0,
            "gates_triggered": [],
        }

    @staticmethod
    def _slugify(value: Any, *, max_length: int = 64) -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip())
        slug = re.sub(r"_+", "_", slug).strip("_")
        slug = slug or "portfolio"
        if max_length <= 0 or len(slug) <= max_length:
            return slug
        digest = hashlib.sha1(slug.encode("utf-8")).hexdigest()[:8]
        if max_length <= len(digest) + 1:
            return digest[:max_length]
        prefix = slug[: max_length - len(digest) - 1].rstrip("_")
        return f"{prefix}_{digest}"
