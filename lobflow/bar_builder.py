"""
Spot / Futures Bar Builder  (streaming + memory safe)
----------------------------------------------------
Public API (unchanged):
    snapshot, events = load_clean_segment(path)
    df = build_spot_bars(snapshot, events, bucket_cfg, bar_seconds=1)
"""

from .state_engine import BucketConfig, SpotOrderBookState
import gzip, json, pathlib, pandas as pd
from typing import List, Dict, Any, Iterable, Iterator, Tuple, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Helper: find the latest capture file
# ─────────────────────────────────────────────────────────────────────────────
def latest_capture_path(symbol: str, raw_dir: pathlib.Path) -> pathlib.Path:
    paths = list(raw_dir.glob(f"{symbol}_*.jsonl.gz"))
    if not paths:
        raise FileNotFoundError(f"No capture files for {symbol} in {raw_dir}")
    return max(paths, key=lambda p: p.stat().st_mtime)


# ─────────────────────────────────────────────────────────────────────────────
# Streaming loader: snapshot + first bracketing diff + onward
# ─────────────────────────────────────────────────────────────────────────────
def load_clean_segment(path: pathlib.Path) -> Tuple[dict, Iterable[Dict[str, Any]]]:
    """
    Streaming loader that:
      1) scans the file to find the LAST snapshot (stores only that one in memory),
      2) streams forward from the FIRST diff whose [U,u] brackets lastUpdateId+1,
      3) yields normalized depth/trade events from that point onward.
    Returns:
        snapshot: dict (the LAST snapshot found)
        events:   generator of normalized events (first depth has bracket_flag=True)
    """
    snapshot: Optional[dict] = None
    need_id: Optional[int] = None

    # Pass 1: scan entire file to latch the *last* snapshot
    with gzip.open(path, "rt") as fh:
        for ln in fh:
            rec = json.loads(ln)
            if rec.get("type") == "snapshot":
                snapshot = rec["data"]
                if isinstance(snapshot, dict) and "lastUpdateId" in snapshot:
                    need_id = int(snapshot["lastUpdateId"]) + 1

    if snapshot is None or need_id is None:
        raise RuntimeError("No snapshot found in file; cannot build a clean segment.")

    # Pass 2: stream from first bracketing diff onward
    def _iter_events() -> Iterator[Dict[str, Any]]:
        found_bracket = False
        with gzip.open(path, "rt") as fh:
            for ln in fh:
                rec = json.loads(ln)
                t = rec.get("type")

                if t == "depth":
                    d = rec.get("data", {})
                    try:
                        U = int(d.get("U"))
                        u = int(d.get("u"))
                    except (TypeError, ValueError):
                        continue

                    if not found_bracket and U <= need_id <= u:
                        found_bracket = True
                        yield {
                            "etype": "depth",
                            "bracket_flag": True,
                            "U": U, "u": u,
                            "b": d.get("b", []),
                            "a": d.get("a", []),
                            "ts": d.get("E") or rec.get("ts_local", 0) // 1_000_000,
                        }
                        continue

                    if found_bracket:
                        yield {
                            "etype": "depth",
                            "U": U, "u": u,
                            "b": d.get("b", []),
                            "a": d.get("a", []),
                            "ts": d.get("E") or rec.get("ts_local", 0) // 1_000_000,
                        }

                elif found_bracket and t == "trade":
                    d = rec.get("data", {})
                    # Only emit trades after we’ve started the clean segment
                    try:
                        p = float(d["p"])
                        q = float(d["q"])
                        m = bool(d["m"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    yield {
                        "etype": "trade",
                        "ts": d.get("E") or rec.get("ts_local", 0) // 1_000_000,
                        "p": p,
                        "q": q,
                        "m": m,
                    }

    return snapshot, _iter_events()



# ─────────────────────────────────────────────────────────────────────────────
# Bar builder (unchanged API, now streaming-friendly)
# ─────────────────────────────────────────────────────────────────────────────
def build_spot_bars(
    snapshot: dict,
    events: Iterable[Dict[str, Any]],
    bucket_cfg: BucketConfig,
    bar_seconds: int = 1
) -> pd.DataFrame:
    """Build bars from snapshot + event iterable (generator or list)."""

    engine = SpotOrderBookState.from_snapshot(snapshot)

    def reset_acc():
        return dict(
            mid_open=None, mid_high=-1e99, mid_low=1e99,
            spread_open=None,
            top_bid_open=None, top_ask_open=None,
            trade_ct=0, trade_vol_buy=0.0, trade_vol_sell=0.0,
            ofi_total=0.0
        )

    bar_rows: List[dict] = []
    acc = reset_acc()

    first_bracket_ts: Optional[int] = None
    cur_bar_end: Optional[int] = None

    last_mid = engine.mid()
    last_spread = engine.spread()

    def append_bar(ts_ms: int):
        snap_row = engine.snapshot_buckets(bucket_cfg)
        bar_rows.append(dict(
            ts=ts_ms,
            mid_open=acc["mid_open"],
            mid_high=acc["mid_high"] if acc["mid_high"] != -1e99 else last_mid,
            mid_low=acc["mid_low"] if acc["mid_low"] != 1e99 else last_mid,
            mid_close=last_mid,
            spread_open=acc["spread_open"],
            spread_close=last_spread,
            top_bid_qty_open=acc["top_bid_open"],
            top_ask_qty_open=acc["top_ask_open"],
            top_bid_qty_close=snap_row["top_bid_qty"],
            top_ask_qty_close=snap_row["top_ask_qty"],
            ofi_total=acc["ofi_total"],
            trade_ct=acc["trade_ct"],
            trade_vol_buy=acc["trade_vol_buy"],
            trade_vol_sell=acc["trade_vol_sell"],
            trade_imbalance=acc["trade_vol_buy"] - acc["trade_vol_sell"],
            **{k: v for k, v in snap_row.items() if k.startswith(("b", "a", "o"))}
        ))

    for ev in events:
        # start timing on first bracket
        if first_bracket_ts is None and ev.get("bracket_flag"):
            first_bracket_ts = ev["ts"]
            cur_bar_end = ((first_bracket_ts // (bar_seconds * 1000)) + 1) * (bar_seconds * 1000)

        # roll bars forward
        while first_bracket_ts is not None and ev["ts"] >= cur_bar_end:
            append_bar(cur_bar_end)
            cur_bar_end += bar_seconds * 1000
            acc = reset_acc()

        # apply event
        if ev["etype"] == "depth":
            summary = engine.apply_depth_event(ev["U"], ev["u"], ev["b"], ev["a"])
            if not summary:
                continue
            if summary["best_bid"] is not None and summary["best_ask"] is not None:
                last_mid = (summary["best_bid"] + summary["best_ask"]) / 2
            last_spread = summary["spread"]

            if acc["mid_open"] is None:
                acc["mid_open"] = last_mid
                acc["spread_open"] = last_spread
                bb = summary["best_bid"]; ba = summary["best_ask"]
                acc["top_bid_open"] = bb and engine.bids.levels.get(bb, 0.0)
                acc["top_ask_open"] = ba and engine.asks.levels.get(ba, 0.0)

            if last_mid is not None:
                acc["mid_high"] = max(acc["mid_high"], last_mid)
                acc["mid_low"] = min(acc["mid_low"], last_mid)

            acc["ofi_total"] += summary["net_bid_qty"] - summary["net_ask_qty"]

        elif ev["etype"] == "trade":
            acc["trade_ct"] += 1
            if ev["m"]:
                acc["trade_vol_sell"] += ev["q"]
            else:
                acc["trade_vol_buy"] += ev["q"]

    # flush final bar if any
    if first_bracket_ts is not None and cur_bar_end is not None:
        append_bar(cur_bar_end)

    if not bar_rows:
        return pd.DataFrame()

    df = pd.DataFrame(bar_rows).set_index("ts").sort_index()
    df.index = pd.to_datetime(df.index, unit="ms", utc=True)

    overflow_cols = [c for c in df.columns if c.startswith("o")]
    if overflow_cols:
        df[overflow_cols] = df[overflow_cols].fillna(0.0)
    if {"mid_high", "mid_low", "mid_open"} <= set(df.columns):
        df[["mid_high", "mid_low"]] = df[["mid_high", "mid_low"]].fillna(df["mid_open"])

    return df
