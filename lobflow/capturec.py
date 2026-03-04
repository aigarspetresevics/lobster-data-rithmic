# ─────────────────────────────────────────────────────────────────────────────
#   Unified Spot / Futures capture (bracket-safe, flexible duration)
# ─────────────────────────────────────────────────────────────────────────────
from .config import url, Settings
import aiohttp, websockets, asyncio, gzip, json, time, datetime as dt
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, Awaitable, Optional
from enum import Enum


class _WritePhase(Enum):
    """Invariant: new file must start with snapshot, then depth, then stream. meta_bracket is optional metadata later."""
    EXPECT_SNAPSHOT = 1
    EXPECT_BRACKET = 2
    STREAMING = 3


# Max pre-live buffer size to avoid OOM on small droplets
MAX_BUF_RECORDS = 50_000
# After this many consecutive buffer overruns, restart WebSocket tasks to get a fresh connection
MAX_CONSECUTIVE_OVERRUNS = 3

@dataclass
class RotationInfo:
    symbol: str
    market: str
    start_ts: dt.datetime
    end_ts: dt.datetime
    path: Path
        
# ── WebSocket listener --------------------------------------------------------
async def listener(uri: str, tag: str, start_event: asyncio.Event,
                   buf: list, *, market: str, last_u_ref: list, append_jsonl_func,
                   buf_overrun_ref: list, max_buf_records: int = MAX_BUF_RECORDS):
    """
    Generic reader for depth / trade streams.
    Futures gap rule: when live (`start_event.is_set`) ensure msg["pu"] == last_u_ref[0].
    On gap → clear buffer, reset live flag, wait for new bracket.
    When buffering, if buf exceeds max_buf_records, sets buf_overrun_ref[0] = True.
    """
    ws = await websockets.connect(uri, max_size=2**23)
    try:
        while True:
            rec = {
                "ts_local": time.time_ns(),
                "type": tag,
                "data": json.loads(await ws.recv()),
            }

            # ---------- live futures gap check ----------
            if (market == "futures"
                and tag == "depth"
                and start_event.is_set()
                and last_u_ref[0] is not None):
                d = rec["data"]
                if d.get("pu") != last_u_ref[0]:
                    print("⚠️  GAP in futures diff stream — resyncing...")
                    start_event.clear()   # back to buffering mode
                    buf.clear()
                    last_u_ref[0] = None  # will be set after next bracket
            # --------------------------------------------

            # after live, record last_u for next gap check
            if market == "futures" and tag == "depth" and start_event.is_set():
                last_u_ref[0] = rec["data"]["u"]

            # buffer vs direct-write; cap buffer size when buffering
            if start_event.is_set():
                append_jsonl_func(rec)
            else:
                buf.append(rec)
                if len(buf) >= max_buf_records:
                    buf_overrun_ref[0] = True
    except asyncio.CancelledError:
        await ws.close()


async def liquidation_listener(uri: str, start_event: asyncio.Event, append_jsonl_func) -> None:
    """
    Subscribes to forceOrder (liquidation) stream; appends to the same raw file only when live.
    Drops events until start_event is set.
    """
    ws = await websockets.connect(uri, max_size=2**23)
    try:
        while True:
            raw = await ws.recv()
            if start_event.is_set():
                rec = {"ts_local": time.time_ns(), "type": "liquidation", "data": json.loads(raw)}
                append_jsonl_func(rec)
    except asyncio.CancelledError:
        await ws.close()


# ── REST snapshot -------------------------------------------------------------
async def fetch_snapshot(sym: str, tmpl: str, limit: int):
    url_ = tmpl.format(sym=sym, sym_lower=sym.lower(), limit=limit)
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url_) as r:
            return await r.json()

# ── main capture coroutine ----------------------------------------------------
async def capture_one_symbol(
        sym: str,
        *,
        settings: Settings,
        hours: int = 0,
        minutes: int = 0,
        seconds: float = 0.0,
        market: str = "futures",                 # "spot" / "futures"
        bracket_timeout: float = 0.2,
        output_file: str = None,
        block_time_ref: Optional[list] = None,
        on_rotation_complete: Optional[Callable[[RotationInfo], Awaitable[None]]] = None):

    """
    Capture a single symbol into a gz file for a fixed duration.
    The implementation keeps a continuous pair of websocket listeners
    alive for the duration, and treats the whole run as a single
    time-based \"rotation\"; when it completes, an optional callback
    is invoked with RotationInfo so callers can react to file completion.
    """

    sym = sym or settings.symbols[0]

    if output_file:
        raw_file = Path(output_file)
    else:
        raw_file = Path(settings.capture.raw_dir) / f"{sym}_{dt.datetime.utcnow():%Y%m%d_%H%M%S}.jsonl.gz"

    raw_file.parent.mkdir(parents=True, exist_ok=True)

    # ---- phase-checked write: snapshot then depth then stream ----
    # meta_bracket is allowed only in STREAMING (after at least one depth post-snapshot).
    write_phase_ref = [_WritePhase.EXPECT_SNAPSHOT]

    def _write_raw(obj: dict) -> None:
        with gzip.open(raw_file, "ab") as fh:
            fh.write((json.dumps(obj) + "\n").encode())

    def append_jsonl(obj: dict) -> None:
        rec_type = obj.get("type", "")
        phase = write_phase_ref[0]
        if phase == _WritePhase.EXPECT_SNAPSHOT:
            if rec_type != "snapshot":
                raise RuntimeError(f"Invariant: first record must be snapshot, got {rec_type!r}")
            write_phase_ref[0] = _WritePhase.EXPECT_BRACKET
        elif phase == _WritePhase.EXPECT_BRACKET:
            if rec_type != "depth":
                raise RuntimeError(f"Invariant: after snapshot expect depth, got {rec_type!r}")
            write_phase_ref[0] = _WritePhase.STREAMING
        _write_raw(obj)

    # total duration
    total_secs = hours * 3600 + minutes * 60 + seconds
    if total_secs <= 0:
        raise ValueError("Capture duration must be positive.")

    # time window for this capture segment (fallback if block_time_ref not used)
    segment_start_ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    segment_end_ts = segment_start_ts + dt.timedelta(seconds=total_secs)
    block_start_ts: Optional[dt.datetime] = None
    block_end_ts: Optional[dt.datetime] = None

    # endpoint selection
    liq_uri: Optional[str] = None
    if market == "spot":
        depth_uri = url(settings.capture.depth_endpoint,  sym)
        trade_uri = url(settings.capture.trade_endpoint,  sym)
        snap_tmpl = settings.capture.snapshot_endpoint
        snap_limit = settings.capture.depth_limit
    elif market == "futures":
        depth_uri = url(settings.capture.fut_depth_endpoint,  sym)
        trade_uri = url(settings.capture.fut_trade_endpoint,  sym)
        liq_uri = url(settings.capture.fut_force_order_endpoint, sym)
        snap_tmpl = settings.capture.fut_snapshot_endpoint
        snap_limit = min(settings.capture.depth_limit, 1000)
    else:
        raise ValueError("market must be 'spot' or 'futures'")

    # mutable holder for last seen `u` (used only in futures gap check)
    last_u_ref = [None]

    start_event = asyncio.Event()
    buf: list = []
    buf_overrun_ref = [False]

    # launch WS tasks
    depth_task = asyncio.create_task(
        listener(depth_uri, "depth", start_event, buf,
                 market=market, last_u_ref=last_u_ref, append_jsonl_func=append_jsonl,
                 buf_overrun_ref=buf_overrun_ref))
    trade_task = asyncio.create_task(
        listener(trade_uri, "trade", start_event, buf,
                 market=market, last_u_ref=last_u_ref, append_jsonl_func=append_jsonl,
                 buf_overrun_ref=buf_overrun_ref))
    liq_task: Optional[asyncio.Task[None]] = None
    if liq_uri is not None:
        liq_task = asyncio.create_task(liquidation_listener(liq_uri, start_event, append_jsonl))

    await asyncio.sleep(0.05)  # tiny buffer window

    attempts = 0
    consecutive_overruns = 0
    while not start_event.is_set():
        if buf_overrun_ref[0]:
            buf_overrun_ref[0] = False
            buf.clear()
            consecutive_overruns += 1
            if consecutive_overruns >= MAX_CONSECUTIVE_OVERRUNS:
                depth_task.cancel()
                trade_task.cancel()
                if liq_task is not None:
                    liq_task.cancel()
                await asyncio.gather(
                    depth_task, trade_task,
                    *([] if liq_task is None else [liq_task]),
                    return_exceptions=True)
                consecutive_overruns = 0
                await asyncio.sleep(0.1)
                depth_task = asyncio.create_task(
                    listener(depth_uri, "depth", start_event, buf,
                             market=market, last_u_ref=last_u_ref, append_jsonl_func=append_jsonl,
                             buf_overrun_ref=buf_overrun_ref))
                trade_task = asyncio.create_task(
                    listener(trade_uri, "trade", start_event, buf,
                             market=market, last_u_ref=last_u_ref, append_jsonl_func=append_jsonl,
                             buf_overrun_ref=buf_overrun_ref))
                if liq_uri is not None:
                    liq_task = asyncio.create_task(liquidation_listener(liq_uri, start_event, append_jsonl))
        attempts += 1

        # ① fetch snapshot (each attempt starts a new segment: snapshot then bracket then stream)
        write_phase_ref[0] = _WritePhase.EXPECT_SNAPSHOT
        snap = await fetch_snapshot(sym, snap_tmpl, snap_limit)
        snap_id  = snap["lastUpdateId"]
        need_id  = snap_id + 1

        append_jsonl({"ts_local": time.time_ns(), "type": "snapshot", "data": snap})

        deadline     = time.monotonic() + bracket_timeout
        j            = 0
        bracket_idx  = None

        # ② scan buffer for first diff that brackets the snapshot
        while time.monotonic() < deadline and not start_event.is_set():
            if buf_overrun_ref[0]:
                buf_overrun_ref[0] = False
                buf.clear()
                consecutive_overruns += 1
                if consecutive_overruns >= MAX_CONSECUTIVE_OVERRUNS:
                    depth_task.cancel()
                    trade_task.cancel()
                    if liq_task is not None:
                        liq_task.cancel()
                    await asyncio.gather(
                        depth_task, trade_task,
                        *([] if liq_task is None else [liq_task]),
                        return_exceptions=True)
                    consecutive_overruns = 0
                    await asyncio.sleep(0.1)
                    depth_task = asyncio.create_task(
                        listener(depth_uri, "depth", start_event, buf,
                                 market=market, last_u_ref=last_u_ref, append_jsonl_func=append_jsonl,
                                 buf_overrun_ref=buf_overrun_ref))
                    trade_task = asyncio.create_task(
                        listener(trade_uri, "trade", start_event, buf,
                                 market=market, last_u_ref=last_u_ref, append_jsonl_func=append_jsonl,
                                 buf_overrun_ref=buf_overrun_ref))
                    if liq_uri is not None:
                        liq_task = asyncio.create_task(liquidation_listener(liq_uri, start_event, append_jsonl))
                break
            await asyncio.sleep(0)
            while j < len(buf):
                rec = buf[j]; j += 1
                if rec["type"] == "depth":
                    d = rec["data"]
                    if d["U"] <= need_id <= d["u"]:
                        bracket_idx = j - 1
                        start_event.set()
                        break

        if start_event.is_set() and bracket_idx is not None:
            dropped_depth = 0
            flushed       = 0
            last_flushed_depth_u: Optional[int] = None
            for rec_flush in buf[bracket_idx:]:
                if rec_flush["type"] == "depth" and rec_flush["data"]["u"] < need_id:
                    dropped_depth += 1
                    continue
                append_jsonl(rec_flush)
                flushed += 1
                if rec_flush["type"] == "depth":
                    last_flushed_depth_u = rec_flush["data"]["u"]
            buf.clear()

            # baseline for futures gap-check (must be last appended depth, not last record - last can be trade)
            if market == "futures":
                last_u_ref[0] = last_flushed_depth_u if last_flushed_depth_u is not None else snap_id

            # meta record
            append_jsonl({
                "ts_local": time.time_ns(),
                "type": "meta_bracket",
                "data": {
                    "market": market,
                    "attempts": attempts,
                    "snap_id": snap_id,
                    "need_id": need_id,
                    "bracket_idx": bracket_idx,
                    "dropped_depth": dropped_depth,
                    "flushed_records": flushed,
                }
            })
            block_start_ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
            if block_time_ref is not None and block_time_ref[0] is None:
                block_time_ref[0] = block_start_ts
            consecutive_overruns = 0
            print(f"[{sym} | {market}] Live after {attempts} snapshot attempt(s).")

    print(f"DEBUG: Entering live streaming phase. Sleeping for {total_secs} seconds.")

    # ③ live streaming for the requested duration
    try:
        await asyncio.sleep(total_secs)
    finally:
        depth_task.cancel()
        trade_task.cancel()
        if liq_task is not None:
            liq_task.cancel()
        await asyncio.gather(
            depth_task, trade_task,
            *([] if liq_task is None else [liq_task]),
            return_exceptions=True)

    block_end_ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    if block_time_ref is not None:
        block_time_ref[1] = block_end_ts

    # Signal rotation completion to caller (treated as a single block)
    if on_rotation_complete is not None:
        start_ts = (block_time_ref[0] if block_time_ref and block_time_ref[0] is not None else block_start_ts) or segment_start_ts
        end_ts = (block_time_ref[1] if block_time_ref and block_time_ref[1] is not None else block_end_ts) or segment_end_ts
        info = RotationInfo(symbol=sym, market=market, start_ts=start_ts, end_ts=end_ts, path=raw_file)
        await on_rotation_complete(info)

async def main_capture_loop(
    symbol: str,
    *,
    settings: Settings,
    hours: int = 0,
    minutes: int = 0,
    seconds: float = 0.0,
    market: str = "futures",
    reconnect_delay: int = 5,
    output_file: str = None,
    on_rotation_complete: Optional[Callable[[RotationInfo], Awaitable[None]]] = None
):
    """
    Main loop to run and auto-restart the capture on network failure.
    Each restart creates a new, independent data file.
    """
    total_duration_seconds = hours * 3600 + minutes * 60 + seconds
    if total_duration_seconds <= 0:
        raise ValueError("Total capture duration must be positive.")
        
    start_time = time.monotonic()
    block_time_ref: list = [None, None]

    while True:
        elapsed_time = time.monotonic() - start_time
        remaining_time = total_duration_seconds - elapsed_time

        if remaining_time <= 0:
            print("Total desired capture duration reached. Exiting.")
            break

        try:
            print("-" * 50)
            print(f"Starting new capture session for {remaining_time:.2f} seconds.")
            await capture_one_symbol(
                sym=symbol,
                settings=settings,
                seconds=remaining_time,
                market=market,
                output_file=output_file,
                block_time_ref=block_time_ref,
                on_rotation_complete=on_rotation_complete,
            )
            # If it completes without error, the total duration is met.
            print("Capture completed successfully without interruption.")
            break
        except (websockets.exceptions.ConnectionClosedError, aiohttp.ClientError) as e:
            print(f"\n--- NETWORK ERROR ---")
            print(f"  Error: {e}")
            print(f"  The current data file has been saved.")
            print(f"  Restarting capture in {reconnect_delay} seconds...")
            print("-" * 21)
            await asyncio.sleep(reconnect_delay)
        except Exception as e:
            print(f"\n--- UNEXPECTED ERROR ---")
            print(f"  Error: {e}")
            print(f"  The current data file has been saved.")
            print(f"  Restarting capture in {reconnect_delay} seconds...")
            print("-" * 24)
            await asyncio.sleep(reconnect_delay)


