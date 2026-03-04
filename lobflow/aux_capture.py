"""
Auxiliary data capture: funding rate and open interest.
Writes to data/funding and data/oi for enrichment to consume.
"""

import time
import urllib.request
import json
from pathlib import Path
from typing import Optional

import pandas as pd

# Binance Futures REST
FUNDING_RATE_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
OPEN_INTEREST_URL = "https://fapi.binance.com/fapi/v1/openInterest"


def fetch_funding_append(symbol: str, funding_dir: str, limit: int = 100) -> None:
    """
    Fetch last N funding rates from Binance and append to data/funding/{symbol}.parquet.
    Columns: funding_time (datetime), funding_rate (float).
    """
    url = f"{FUNDING_RATE_URL}?symbol={symbol}&limit={limit}"
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read().decode())
    if not data:
        return
    rows = [
        {"funding_time": pd.to_datetime(d["fundingTime"], unit="ms"), "funding_rate": float(d["fundingRate"])}
        for d in data
    ]
    new_df = pd.DataFrame(rows).drop_duplicates(subset=["funding_time"]).sort_values("funding_time")
    path = Path(funding_dir) / f"{symbol}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pd.read_parquet(path)
        existing["funding_time"] = pd.to_datetime(existing["funding_time"])
        combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(subset=["funding_time"]).sort_values("funding_time")
    else:
        combined = new_df
    combined.to_parquet(path, index=False)


def fetch_oi_append(symbol: str, oi_dir: str) -> None:
    """
    Fetch current open interest from Binance and append to data/oi/{symbol}.parquet.

    Columns: ts (datetime, UTC), open_interest (float).

    Guarantees:
      - ts is timezone-aware (UTC)
      - no duplicate ts values in the stored parquet (keep last)
      - stored data sorted by ts
    """
    url = f"{OPEN_INTEREST_URL}?symbol={symbol}"
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read().decode())

    ts_ms = data.get("time", int(time.time() * 1000))
    oi = float(data.get("openInterest", 0.0))

    # Make ts UTC-aware to match bars index (+00:00)
    ts = pd.to_datetime(ts_ms, unit="ms", utc=True, errors="coerce")
    row = pd.DataFrame([{"ts": ts, "open_interest": oi}])

    path = Path(oi_dir) / f"{symbol}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        existing = pd.read_parquet(path, columns=["ts", "open_interest"])
        existing["ts"] = pd.to_datetime(existing["ts"], utc=True, errors="coerce")

        combined = pd.concat([existing, row], ignore_index=True)
    else:
        combined = row

    combined = (
        combined.dropna(subset=["ts"])
        .sort_values("ts")
        .drop_duplicates(subset=["ts"], keep="last")
        .reset_index(drop=True)
    )

    combined.to_parquet(path, index=False)



def run_oi_poller(
    symbol: str,
    oi_dir: str,
    stop_event,
    interval_sec: float = 1.0,
) -> None:
    """
    Poll OI every interval_sec and append to oi_dir until stop_event is set.
    Designed to run in a background thread or as a blocking loop (e.g. in a process).
    """
    while not stop_event.is_set():
        try:
            fetch_oi_append(symbol, oi_dir)
        except Exception:
            pass
        for _ in range(int(interval_sec * 10)):
            if stop_event.is_set():
                break
            time.sleep(0.1)


def run_oi_poller_timed(
    symbol: str,
    oi_dir: str,
    duration_sec: float,
    interval_sec: float = 1.0,
) -> None:
    """
    Poll OI every interval_sec for duration_sec, then return.
    Use from a thread when starting an 8h capture so OI data exists for the block.
    """
    deadline = time.monotonic() + duration_sec
    while time.monotonic() < deadline:
        try:
            fetch_oi_append(symbol, oi_dir)
        except Exception:
            pass
        time.sleep(max(0, min(interval_sec, deadline - time.monotonic())))


def run_funding_poller(
    symbol: str,
    funding_dir: str,
    stop_event,
    interval_sec: float = 3600.0,
) -> None:
    """
    Fetch funding every interval_sec and append to funding_dir until stop_event is set.
    """
    while not stop_event.is_set():
        try:
            fetch_funding_append(symbol, funding_dir)
        except Exception:
            pass
        for _ in range(int(interval_sec * 10)):
            if stop_event.is_set():
                break
            time.sleep(0.1)
