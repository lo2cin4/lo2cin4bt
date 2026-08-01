"""Capture the frozen Binance BTCUSDT 1m source used by the SMA Golden Test."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dataloader.market_data_loader import MultiAssetMarketDataLoader  # noqa: E402


FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "golden"
CSV_PATH = FIXTURE_DIR / "binance_btcusdt_1m_2026-06.csv"
METADATA_PATH = FIXTURE_DIR / "binance_btcusdt_1m_2026-06.metadata.json"
START = pd.Timestamp("2026-06-01T00:00:00Z")
END = pd.Timestamp("2026-07-01T00:00:00Z")
EXPECTED_INDEX = pd.date_range(START, END, freq="min", inclusive="left", name="Time")


def main() -> None:
    frame = MultiAssetMarketDataLoader._download_binance_symbol(
        requests_module=requests,
        api_base="https://api.binance.com",
        symbol="BTCUSDT",
        interval="1m",
        row_key_kind="event_timestamp",
        timestamp_convention="bar_open",
        bar_duration=pd.Timedelta(minutes=1),
        start=START.isoformat(),
        end=END.isoformat(),
        timeout=30,
    )
    if not frame.index.equals(EXPECTED_INDEX):
        missing = EXPECTED_INDEX.difference(frame.index)
        extra = frame.index.difference(EXPECTED_INDEX)
        raise RuntimeError(
            "Binance Golden capture is not the exact complete month: "
            f"rows={len(frame)}, missing={len(missing)}, extra={len(extra)}"
        )
    if frame.index.has_duplicates:
        raise RuntimeError("Binance Golden capture contains duplicate timestamps")
    if frame.isna().any().any():
        raise RuntimeError("Binance Golden capture contains null OHLCV values")
    if not (
        (frame["low"] <= frame[["open", "close"]].min(axis=1)).all()
        and (frame["high"] >= frame[["open", "close"]].max(axis=1)).all()
    ):
        raise RuntimeError("Binance Golden capture contains invalid OHLC bounds")

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    frame.to_csv(CSV_PATH, index=True, float_format="%.8f")
    content_hash = hashlib.sha256(CSV_PATH.read_bytes()).hexdigest()
    metadata = {
        "schema_version": "binance_ohlcv_golden_fixture.v1",
        "provider": "binance",
        "endpoint": "https://api.binance.com/api/v3/klines",
        "symbol": "BTCUSDT",
        "interval": "1m",
        "timestamp_convention": "bar_open",
        "start_inclusive": START.isoformat(),
        "end_exclusive": END.isoformat(),
        "row_count": len(frame),
        "session_count": int(frame.index.normalize().nunique()),
        "sha256": content_hash,
        "captured_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    METADATA_PATH.write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
