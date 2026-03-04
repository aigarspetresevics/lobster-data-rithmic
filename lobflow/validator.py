"""
Validator (streaming + memory safe)
----------------------------------
Public API (unchanged):
    summary, recs = validate_capture(path, snap_choice="last", strict_gap=False, use_pu=False)
"""

import gzip, json, math, statistics as stats
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union, Iterator

# ---------- Result containers (unchanged) ------------------------------------------------
@dataclass
class DepthCheckResult:
    snap_id: int
    need_id: int
    bracket_idx: int
    bracket_U: int
    bracket_u: int
    bracket_ok: bool
    pre_snapshot_depths: int
    bad_presnapshot_depths: int

@dataclass
class ContinuityResult:
    applied_events: int
    ignored_old: int
    gaps: int
    first_gap_at: Optional[int]
    last_update_id: int

@dataclass
class BookSanity:
    n_steps: int
    crossed_count: int
    best_bid: float
    best_ask: float
    spread: float

@dataclass
class LatencyStats:
    samples: int
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float

# ---------- Streaming IO ---------------------------------------------------------------
def _stream_records(path: Path) -> Iterator[Tuple[int, Dict[str, Any]]]:
    """Stream records with their index for processing."""
    with gzip.open(path, "rt") as f:
        for idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            yield idx, json.loads(line)

def _find_snapshots_streaming(path: Path) -> List[int]:
    """Find snapshot indices without loading entire file."""
    snapshots = []
    for idx, rec in _stream_records(path):
        if rec["type"] == "snapshot":
            snapshots.append(idx)
    return snapshots

def _find_last_snapshot_streaming(path: Path) -> Tuple[int, Dict[str, Any]]:
    """Find the last snapshot and its data."""
    last_snap_idx = None
    last_snap_data = None
    
    for idx, rec in _stream_records(path):
        if rec["type"] == "snapshot":
            last_snap_idx = idx
            last_snap_data = rec
    
    if last_snap_idx is None:
        raise ValueError("No snapshot records in file.")
    
    return last_snap_idx, last_snap_data

# Fetch snapshot record by its global index (matching snap_choice)
def _get_snapshot_by_index_streaming(path: Path, target_idx: int) -> Dict[str, Any]:
    for idx, rec in _stream_records(path):
        if idx == target_idx:
            if rec["type"] != "snapshot":
                raise ValueError(f"Record at index {target_idx} is not a snapshot")
            return rec
    raise ValueError(f"Snapshot at index {target_idx} not found")

# ---------- Streaming Bracket Check ----------------------------------------------------
def _check_bracket_streaming(path: Path, snap_idx: int, snap_rec: Dict[str, Any]) -> DepthCheckResult:
    """Check bracket without loading entire file into memory."""
    snap_id = snap_rec["data"]["lastUpdateId"]
    need_id = snap_id + 1
    
    pre_snapshot_depths = 0
    bad_presnapshot = 0
    bracket_idx = None
    bracket_U = bracket_u = None
    
    for idx, rec in _stream_records(path):
        if idx <= snap_idx:
            continue
            
        if rec["type"] != "depth":
            continue
            
        d = rec["data"]
        U, u = d["U"], d["u"]
        pre_snapshot_depths += 1
        
        if U <= need_id <= u:
            bracket_idx = idx
            bracket_U = U
            bracket_u = u
            break
        else:
            if u <= snap_id:
                bad_presnapshot += 1
    
    if bracket_idx is None:
        raise ValueError(f"No bracketing depth event found after snapshot {snap_idx} (snap_id={snap_id}).")
    
    return DepthCheckResult(
        snap_id=snap_id,
        need_id=need_id,
        bracket_idx=bracket_idx,
        bracket_U=bracket_U,
        bracket_u=bracket_u,
        bracket_ok=True,
        pre_snapshot_depths=pre_snapshot_depths,
        bad_presnapshot_depths=bad_presnapshot,
    )

# ---------- Streaming Replay ----------------------------------------------------------
def _replay_depths_streaming(
    path: Path,
    start_idx: int,
    snap_rec: Dict[str, Any],
    *,
    strict_gap: bool = False,
    use_pu: bool = False
) -> Tuple[ContinuityResult, BookSanity]:
    """Replay depths without loading entire file into memory."""
    snap_id = snap_rec["data"]["lastUpdateId"]
    bids = {float(p): float(q) for p, q in snap_rec["data"]["bids"]}
    asks = {float(p): float(q) for p, q in snap_rec["data"]["asks"]}
    
    local_id = snap_id
    applied = ignored_old = gaps = crossed_count = 0
    first_gap_at: Optional[int] = None
    first_event = True
    
    for idx, rec in _stream_records(path):
        if idx < start_idx:
            continue
            
        if rec["type"] != "depth":
            continue
            
        d = rec["data"]
        U, u = d["U"], d["u"]
        
        # ---- continuity checks ---------------------------------------
        if first_event:
            first_event = False
        else:
            if use_pu and "pu" in d:
                if d["pu"] != local_id:
                    gaps += 1
                    first_gap_at = idx
                    break
            else:
                if strict_gap:
                    if U > local_id + 1:
                        gaps += 1
                        first_gap_at = idx
                        break
                else:
                    if U > local_id + 1:
                        gaps += 1
                        first_gap_at = idx
                        break
        
        # Ignore fully old events
        if u < local_id:
            ignored_old += 1
            continue
        
        # Apply bid deltas
        for price, qty in d.get("b", []):
            p = float(price)
            q = float(qty)
            if q == 0.0:
                bids.pop(p, None)
            else:
                bids[p] = q
                
        # Apply ask deltas
        for price, qty in d.get("a", []):
            p = float(price)
            q = float(qty)
            if q == 0.0:
                asks.pop(p, None)
            else:
                asks[p] = q
        
        local_id = u
        applied += 1
        
        # sanity: crossed book check
        if bids and asks and max(bids) >= min(asks):
            crossed_count += 1
    
    # final snapshot sanity
    if bids and asks:
        best_bid = max(bids)
        best_ask = min(asks)
        spread = best_ask - best_bid
    else:
        best_bid = best_ask = spread = math.nan
    
    return (
        ContinuityResult(applied, ignored_old, gaps, first_gap_at, local_id),
        BookSanity(applied, crossed_count, best_bid, best_ask, spread)
    )

# ---------- Streaming Latency ---------------------------------------------------------
def _latency_stats_streaming(path: Path, key: str = "depth") -> LatencyStats:
    """Calculate latency stats without loading entire file into memory."""
    vals = []
    for idx, rec in _stream_records(path):
        if rec["type"] != key:
            continue
        d = rec["data"]
        if "E" not in d:
            continue
        loc_ms = rec["ts_local"] / 1e6
        exch_ms = d["E"]
        vals.append(loc_ms - exch_ms)
    
    if not vals:
        return LatencyStats(0, math.nan, math.nan, math.nan, math.nan, math.nan, math.nan)
    
    vals.sort()
    def pct(p): 
        return vals[min(max(int(len(vals)*p), 0), len(vals)-1)]
    
    return LatencyStats(
        len(vals), stats.fmean(vals), pct(0.5), pct(0.95), pct(0.99), vals[0], vals[-1]
    )

# ---------- Streaming Top-level -------------------------------------------------------
def validate_capture(
    path: Union[str, Path],
    *,
    snap_choice: Union[str, int] = "last",
    strict_gap: bool = False,
    use_pu: bool = False,
):
    """Memory-safe validation using streaming approach."""
    path = Path(path)
    
    # Find snapshots without loading entire file
    snap_idxs = _find_snapshots_streaming(path)
    if not snap_idxs:
        raise ValueError("No snapshot records in file.")
    
    # Choose snapshot
    if snap_choice == "last":
        snap_idx = snap_idxs[-1]
    elif snap_choice == "first":
        snap_idx = snap_idxs[0]
    else:
        snap_idx = snap_choice
    
    # Get snapshot data that matches the chosen index for parity
    snap_rec = _get_snapshot_by_index_streaming(path, snap_idx)
    
    # Check bracket
    dchk = _check_bracket_streaming(path, snap_idx, snap_rec)
    
    # Replay depths
    cont, sanity = _replay_depths_streaming(
        path, dchk.bracket_idx, snap_rec, strict_gap=strict_gap, use_pu=use_pu
    )
    
    # Calculate latency stats
    lat_depth = _latency_stats_streaming(path, "depth")
    lat_trade = _latency_stats_streaming(path, "trade")
    
    # Count total records (one more pass)
    total_records = sum(1 for _ in _stream_records(path))
    
    summary = {
        "file": str(path),
        "records_total": total_records,
        "snapshots_found": len(snap_idxs),
        "snapshot_index_used": snap_idx,
        "snap_id": dchk.snap_id,
        "need_id": dchk.need_id,
        "bracket_index": dchk.bracket_idx,
        "bracket_event_U": dchk.bracket_U,
        "bracket_event_u": dchk.bracket_u,
        "bracket_ok": dchk.bracket_ok,
        "pre_snapshot_depths_seen": dchk.pre_snapshot_depths,
        "bad_presnapshot_depths_written": dchk.bad_presnapshot_depths,
        "applied_depth_events": cont.applied_events,
        "ignored_old_events": cont.ignored_old,
        "gaps_detected": cont.gaps,
        "first_gap_at_record": cont.first_gap_at,
        "final_update_id": cont.last_update_id,
        "crossed_books": sanity.crossed_count,
        "final_best_bid": sanity.best_bid,
        "final_best_ask": sanity.best_ask,
        "final_spread": sanity.spread,
        "latency_depth_ms_mean": lat_depth.mean_ms,
        "latency_depth_ms_p95": lat_depth.p95_ms,
        "latency_trade_ms_mean": lat_trade.mean_ms,
        "latency_trade_ms_p95": lat_trade.p95_ms,
    }
    
    # OOM-safe recs policy: return full list only for reasonably small files
    def _file_is_small(p: Path, max_bytes: int = 200 * 1024 * 1024) -> bool:
        try:
            return p.stat().st_size <= max_bytes
        except Exception:
            return False

    class _RecsProxy:
        def __init__(self, p: Path, pre_count: int | None = None):
            self._path = p
            self._count = pre_count

        def __iter__(self):
            for _, rec in _stream_records(self._path):
                yield rec

        def __len__(self):
            if self._count is None:
                self._count = sum(1 for _ in _stream_records(self._path))
            return self._count

        def __getitem__(self, idx):
            # Random access would force linear scan; disallow to prevent surprises
            raise TypeError("Random indexing not supported on streaming proxy; iterate instead")

    if _file_is_small(path):
        recs_out = [rec for _, rec in _stream_records(path)]
    else:
        recs_out = _RecsProxy(path, pre_count=total_records)

    return summary, recs_out

# ---------- Backward compatibility functions -----------------------------------------
def load_raw_records(path: Path) -> List[Dict[str, Any]]:
    """Load all records for smaller files; stream-guard for large ones to avoid OOM."""
    MAX_BYTES = 200 * 1024 * 1024
    try:
        if Path(path).stat().st_size > MAX_BYTES:
            raise MemoryError(
                "File too large to load into memory safely; consume via streaming APIs instead"
            )
    except FileNotFoundError:
        pass
    return [rec for _, rec in _stream_records(Path(path))]

def find_snapshots(recs: List[Dict[str, Any]]) -> List[int]:
    """Return indices of snapshots within an in-memory list of records (parity with validator.py)."""
    return [i for i, r in enumerate(recs) if r.get("type") == "snapshot"]

def check_bracket_from_idx(recs: List[Dict[str, Any]], snap_idx: int) -> DepthCheckResult:
    """In-memory variant for parity with original validator.py."""
    snap_rec = recs[snap_idx]
    snap_id = snap_rec["data"]["lastUpdateId"]
    need_id = snap_id + 1
    pre_snapshot_depths = 0
    bad_presnapshot = 0
    bracket_idx = None
    bracket_U = bracket_u = None
    for i in range(snap_idx + 1, len(recs)):
        r = recs[i]
        if r.get("type") != "depth":
            continue
        d = r["data"]; U, u = d["U"], d["u"]
        pre_snapshot_depths += 1
        if U <= need_id <= u:
            bracket_idx = i; bracket_U = U; bracket_u = u
            break
        else:
            if u <= snap_id:
                bad_presnapshot += 1
    if bracket_idx is None:
        raise ValueError(
            f"No bracketing depth event found after snapshot {snap_idx} (snap_id={snap_id})."
        )
    return DepthCheckResult(
        snap_id=snap_id,
        need_id=need_id,
        bracket_idx=bracket_idx,
        bracket_U=bracket_U,
        bracket_u=bracket_u,
        bracket_ok=True,
        pre_snapshot_depths=pre_snapshot_depths,
        bad_presnapshot_depths=bad_presnapshot,
    )

def replay_depths(
    recs: List[Dict[str, Any]],
    start_idx: int,
    snap_rec: Dict[str, Any],
    *,
    strict_gap: bool = False,
    use_pu: bool = False,
) -> Tuple[ContinuityResult, BookSanity]:
    """In-memory variant for parity with original validator.py."""
    snap_id = snap_rec["data"]["lastUpdateId"]
    bids = {float(p): float(q) for p, q in snap_rec["data"]["bids"]}
    asks = {float(p): float(q) for p, q in snap_rec["data"]["asks"]}
    local_id = snap_id
    applied = ignored_old = gaps = crossed_count = 0
    first_gap_at: Optional[int] = None
    first_event = True
    for i in range(start_idx, len(recs)):
        r = recs[i]
        if r.get("type") != "depth":
            continue
        d = r["data"]; U, u = d["U"], d["u"]
        if first_event:
            first_event = False
        else:
            if use_pu and "pu" in d:
                if d["pu"] != local_id:
                    gaps += 1; first_gap_at = i; break
            else:
                if strict_gap:
                    if U > local_id + 1:
                        gaps += 1; first_gap_at = i; break
                else:
                    if U > local_id + 1:
                        gaps += 1; first_gap_at = i; break
        if u < local_id:
            ignored_old += 1
            continue
        for price, qty in d.get("b", []):
            p = float(price); q = float(qty)
            if q == 0.0: bids.pop(p, None)
            else:        bids[p] = q
        for price, qty in d.get("a", []):
            p = float(price); q = float(qty)
            if q == 0.0: asks.pop(p, None)
            else:        asks[p] = q
        local_id = u
        applied += 1
        if bids and asks and max(bids) >= min(asks):
            crossed_count += 1
    if bids and asks:
        best_bid = max(bids); best_ask = min(asks); spread = best_ask - best_bid
    else:
        best_bid = best_ask = spread = math.nan
    return (
        ContinuityResult(applied, ignored_old, gaps, first_gap_at, local_id),
        BookSanity(applied, crossed_count, best_bid, best_ask, spread)
    )

def latency_stats(recs: List[Dict[str, Any]], key: str = "depth") -> LatencyStats:
    """In-memory variant for parity with original validator.py."""
    vals = []
    for r in recs:
        if r.get("type") != key:
            continue
        d = r.get("data", {})
        if "E" not in d:
            continue
        loc_ms = r["ts_local"] / 1e6
        exch_ms = d["E"]
        vals.append(loc_ms - exch_ms)
    if not vals:
        return LatencyStats(0, math.nan, math.nan, math.nan, math.nan, math.nan, math.nan)
    vals.sort()
    def pct(p):
        return vals[min(max(int(len(vals)*p), 0), len(vals)-1)]
    return LatencyStats(len(vals), stats.fmean(vals), pct(0.5), pct(0.95), pct(0.99), vals[0], vals[-1])