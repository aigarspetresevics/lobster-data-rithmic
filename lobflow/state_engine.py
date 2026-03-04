"""
Spot Order Book State Engine  (v0.3)
-----------------------------------
- Full depth tracking.
- Mid-relative *inner* buckets (default: $10 × 5 per side = ±$50).
- Optional *overflow* buckets outward in larger bands (default: $50 steps).
- Modular: no trades, no DataFrames, no bar logic.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
@dataclass
class BucketConfig:
    inner_usd: float = 30.0          # width of small inner buckets
    inner_count: int = 5             # number per side => inner_span = inner_usd * inner_count
    overflow_usd: float = 500.0       # width of overflow buckets per side
    include_overflow: bool = False 
    max_overflow_buckets: int = 5  # NEW# if False, aggregate all overflow into 1 number per side


# ----------------------------------------------------------------------
# BookSide helper
# ----------------------------------------------------------------------
class BookSide:
    def __init__(self, levels: Dict[float, float], is_bid: bool):
        self.levels: Dict[float, float] = levels
        self.is_bid = is_bid
        self.best_price: Optional[float] = None
        self._recalc_best()

    def _recalc_best(self):
        if not self.levels:
            self.best_price = None
            return
        self.best_price = max(self.levels) if self.is_bid else min(self.levels)

    def set_level(self, price: float, qty: float):
        if qty == 0.0:
            removed = self.levels.pop(price, None)
            if removed is not None and price == self.best_price:
                self._recalc_best()
            return

        # add/update
        self.levels[price] = qty
        if self.best_price is None:
            self.best_price = price
            return

        if self.is_bid:
            if price >= self.best_price:
                self.best_price = price
        else:
            if price <= self.best_price:
                self.best_price = price


# ----------------------------------------------------------------------
# Spot Order Book State Engine
# ----------------------------------------------------------------------
class SpotOrderBookState:
    def __init__(self, bids: BookSide, asks: BookSide, last_update_id: int):
        self.bids = bids
        self.asks = asks
        self.last_update_id = last_update_id

    # --- factory -----------------------------------------------------
    @classmethod
    def from_snapshot(cls, snap: dict) -> "SpotOrderBookState":
        bids_dict = {float(p): float(q) for p, q in snap["bids"]}
        asks_dict = {float(p): float(q) for p, q in snap["asks"]}
        return cls(BookSide(bids_dict, True),
                   BookSide(asks_dict, False),
                   snap["lastUpdateId"])

    # --- accessors ---------------------------------------------------
    def best_bid(self) -> Optional[float]: return self.bids.best_price
    def best_ask(self) -> Optional[float]: return self.asks.best_price

    def spread(self) -> Optional[float]:
        b = self.best_bid(); a = self.best_ask()
        if b is None or a is None: return None
        return a - b

    def mid(self) -> Optional[float]:
        b = self.best_bid(); a = self.best_ask()
        if b is None or a is None: return None
        return (a + b) / 2

    # --- apply depth -------------------------------------------------
    def apply_depth_event(
        self,
        U: int, u: int,
        bids_deltas: List[Tuple[str,str]],
        asks_deltas: List[Tuple[str,str]],
        allow_stale: bool = False,
    ) -> Dict:
        """
        Apply Binance Spot diff-depth message.
        Returns summary dict (net bid/ask qty changes, updated best, spread).
        """
        if not allow_stale and u < self.last_update_id:
            return {}

        net_bid, net_ask = 0.0, 0.0

        # bids
        for p, q in bids_deltas:
            pf, qf = float(p), float(q)
            old = self.bids.levels.get(pf, 0.0)
            self.bids.set_level(pf, qf)
            net_bid += (qf - old)

        # asks
        for p, q in asks_deltas:
            pf, qf = float(p), float(q)
            old = self.asks.levels.get(pf, 0.0)
            self.asks.set_level(pf, qf)
            net_ask += (qf - old)

        self.last_update_id = u

        return {
            "U": U, "u": u,
            "net_bid_qty": net_bid,
            "net_ask_qty": net_ask,
            "best_bid": self.best_bid(),
            "best_ask": self.best_ask(),
            "spread": self.spread(),
        }

    # --- bucket snapshot ---------------------------------------------
    def snapshot_buckets(self, cfg: BucketConfig) -> Dict:
        """
        Returns a *row-shaped* dict:
          mid, best_bid, best_ask, spread, top_bid_qty, top_ask_qty,
          b0..bN, a0..aN,
          overflow buckets (o0_bid, o1_bid, ..., o0_ask, o1_ask, ...)
        """
        mid = self.mid()
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        spread = self.spread()

        if mid is None:
            raise RuntimeError("Cannot bucket an empty book.")

        inner_span = cfg.inner_usd * cfg.inner_count

        # inner arrays
        bids_arr = [0.0] * cfg.inner_count
        asks_arr = [0.0] * cfg.inner_count

        # overflow accumulators
        bid_of = {}   # key -> qty
        ask_of = {}

        # scan bids
        for price, qty in self.bids.levels.items():
            dist = mid - price     # positive when below mid
            if dist < 0:
                continue  # crossed; ignore
            if dist < inner_span:
                idx = int(dist // cfg.inner_usd)
                bids_arr[idx] += qty
            else:
                if cfg.include_overflow:
                    overflow_dist = dist - inner_span
                    idx = int(overflow_dist // cfg.overflow_usd)
                    if idx < cfg.max_overflow_buckets:
                        key = f"o{idx}_bid"
                        bid_of[key] = bid_of.get(key, 0.0) + qty
                    else:
                        bid_of["b_overflow_tail"] = bid_of.get("b_overflow_tail", 0.0) + qty

        # scan asks
        for price, qty in self.asks.levels.items():
            dist = price - mid     # positive when above mid
            if dist < 0:
                continue
            if dist < inner_span:
                idx = int(dist // cfg.inner_usd)
                asks_arr[idx] += qty
            else:
                if cfg.include_overflow:
                    overflow_dist = dist - inner_span
                    idx = int(overflow_dist // cfg.overflow_usd)
                    if idx < cfg.max_overflow_buckets:
                        key = f"o{idx}_ask"
                        ask_of[key] = ask_of.get(key, 0.0) + qty
                    else:
                        ask_of["a_overflow_tail"] = ask_of.get("a_overflow_tail", 0.0) + qty

        row = {
            "mid": mid,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "top_bid_qty": self.bids.levels.get(best_bid, 0.0) if best_bid else 0.0,
            "top_ask_qty": self.asks.levels.get(best_ask, 0.0) if best_ask else 0.0,
        }

        # add inner bucket columns
        for i in range(cfg.inner_count):
            row[f"b{i}"] = bids_arr[i]
            row[f"a{i}"] = asks_arr[i]

        # overflow
        if cfg.include_overflow:
            # fill in *all* seen overflow bands so columns remain stable if you union across rows:
            for key, qty in bid_of.items():
                row[key] = qty
            for key, qty in ask_of.items():
                row[key] = qty
            # If no entries for a side → no row keys; DataFrame fill_value=0.0 on concat recommended.

        else:
            # aggregate everything beyond inner_span into single totals
            row["b_overflow"] = sum(qty for price, qty in self.bids.levels.items() if mid - price >= inner_span)
            row["a_overflow"] = sum(qty for price, qty in self.asks.levels.items() if price - mid >= inner_span)

        return row

    # --- full snapshot -----------------------------------------------
    def snapshot_full(self) -> Dict:
        return {
            "bids": dict(self.bids.levels),
            "asks": dict(self.asks.levels),
            "last_update_id": self.last_update_id,
        }