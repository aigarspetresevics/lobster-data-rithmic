import asyncio
import os
import datetime as dt
import threading
from pathlib import Path
import traceback
import pandas as pd
from lobflow.capturec import main_capture_loop, RotationInfo
from lobflow.validator import validate_capture
from lobflow.bar_builder import load_clean_segment, build_spot_bars
from lobflow.enrich import enrich_bars_with_aux
from lobflow.state_engine import BucketConfig
from lobflow.config import load_settings
from lobflow import aux_capture
from lobflow.catalog_sqlite import CatalogSQLite
from lobflow.storage_spaces import SpacesUploader

# ---------- config ----------
CONFIG_PATH = os.environ.get("LOBFLOW_CONFIG", "config.yaml")
settings = load_settings(CONFIG_PATH)

VENUE = settings.source.venue
MARKET = settings.source.market

# Support multiple symbols; main loop will iterate over this list
SYMBOLS = settings.symbols
CAPTURE_DURATION_SECONDS = 8 * 60 * 60

RAW_DATA_DIR = Path(settings.capture.raw_dir)           # from YAML
CSV_OUTPUT_DIR = Path("./data/processed_csv")
PARQUET_OUTPUT_DIR = Path("./data/processed_parquet")

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CSV_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

bucket_config = BucketConfig(
    inner_usd=10.0,
    inner_count=5,
    include_overflow=True,
    overflow_usd=50.0,
    max_overflow_buckets=5
)

bar_interval_seconds = 1

# ---------- bounded processing concurrency ----------
PROCESS_SEM = asyncio.Semaphore(2)

# ---------- catalog + storage ----------
catalog = CatalogSQLite(settings.catalog.path)
uploader = SpacesUploader(
    endpoint=settings.storage.endpoint,
    region=settings.storage.region,
    bucket=settings.storage.bucket
)

def _key_for(prefix: str, venue: str, symbol: str, start_ts: dt.datetime, end_ts: dt.datetime, ext: str) -> str:
    # UTC only
    y = start_ts.strftime("%Y")
    m = start_ts.strftime("%m")
    d = start_ts.strftime("%d")
    start_hm = start_ts.strftime("%H%M")
    end_hm = end_ts.strftime("%H%M")
    return f"{prefix}/{venue}/{symbol}/{y}/{m}/{d}/{start_hm}-{end_hm}.{ext}"

async def main_controller_loop():
    print("✅ Controller starting with config:", CONFIG_PATH)
    print(f"Venue={VENUE} Market={MARKET} Symbols={', '.join(SYMBOLS)}")

    async def process_symbol_block(symbol: str, raw_path: Path, csv_path: Path, parquet_path: Path,
                                   start_ts: dt.datetime, end_ts: dt.datetime) -> None:
        """
        Full downstream pipeline for a single symbol + time block:
        validate -> build bars -> save CSV/Parquet -> upload -> catalog -> cleanup.
        Runs as a background task so capture scheduling is not blocked.
        """
        try:
            print(f"Validating {raw_path} for symbol={symbol} ...")
            summary, _ = validate_capture(path=raw_path)
            if not summary.get("bracket_ok"):
                print(f"❌ Validation failed for {symbol}: bracket not OK. Skipping processing.")
                return
            print(f"✅ Validation OK for {symbol}.")

            print(f"Building bars for {symbol} ...")
            snapshot, events = load_clean_segment(raw_path)
            df_bars = build_spot_bars(
                snapshot=snapshot,
                events=events,
                bucket_cfg=bucket_config,
                bar_seconds=bar_interval_seconds,
            )
            print(f"✅ Built {len(df_bars)} bars for {symbol}.")

            if getattr(settings.enrichment, "enabled", False):
                df_bars = enrich_bars_with_aux(
                    df_bars, symbol, start_ts, end_ts, raw_path, settings
                )
                print(f"✅ Enriched bars for {symbol}.")

            # Save CSV + Parquet locally
            df_bars = df_bars.reset_index()
            df_bars["ts"] = pd.to_datetime(df_bars["ts"], utc=True).dt.tz_convert(None)  
            df_bars.to_csv(csv_path, index=False)
            print(f"✅ Saved CSV for {symbol} → {csv_path}")

            df_bars.to_parquet(parquet_path, index=False)
            print(f"✅ Saved Parquet for {symbol} → {parquet_path}")

            csv_key = _key_for(settings.storage.csv_prefix, VENUE, symbol, start_ts, end_ts, "csv")
            pq_key = _key_for(settings.storage.parquet_prefix, VENUE, symbol, start_ts, end_ts, "parquet")

            # Upload CSV; then catalog with status uploading_csv (parquet placeholders)
            print(f"Uploading CSV for {symbol} → s3://{settings.storage.bucket}/{csv_key}")
            try:
                csv_size = uploader.upload_file(csv_path, csv_key)
            except Exception:
                catalog.upsert_block(
                    venue=VENUE, symbol=symbol,
                    start_ts=start_ts.isoformat(), end_ts=end_ts.isoformat(),
                    block_seconds=CAPTURE_DURATION_SECONDS,
                    csv_bucket=settings.storage.bucket, csv_key="", csv_size_bytes=0,
                    parquet_bucket=settings.storage.bucket, parquet_key="", parquet_size_bytes=0,
                    status="failed",
                )
                raise
            catalog.upsert_block(
                venue=VENUE,
                symbol=symbol,
                start_ts=start_ts.isoformat(),
                end_ts=end_ts.isoformat(),
                block_seconds=CAPTURE_DURATION_SECONDS,
                csv_bucket=settings.storage.bucket,
                csv_key=csv_key,
                csv_size_bytes=csv_size,
                parquet_bucket=settings.storage.bucket,
                parquet_key="",
                parquet_size_bytes=0,
                status="uploading_csv",
            )

            # Upload Parquet; then catalog ready only after both succeed
            print(f"Uploading Parquet for {symbol} → s3://{settings.storage.bucket}/{pq_key}")
            try:
                pq_size = uploader.upload_file(parquet_path, pq_key)
            except Exception:
                catalog.upsert_block(
                    venue=VENUE, symbol=symbol,
                    start_ts=start_ts.isoformat(), end_ts=end_ts.isoformat(),
                    block_seconds=CAPTURE_DURATION_SECONDS,
                    csv_bucket=settings.storage.bucket, csv_key=csv_key, csv_size_bytes=csv_size,
                    parquet_bucket=settings.storage.bucket, parquet_key="", parquet_size_bytes=0,
                    status="failed",
                )
                raise
            catalog.upsert_block(
                venue=VENUE,
                symbol=symbol,
                start_ts=start_ts.isoformat(),
                end_ts=end_ts.isoformat(),
                block_seconds=CAPTURE_DURATION_SECONDS,
                csv_bucket=settings.storage.bucket,
                csv_key=csv_key,
                csv_size_bytes=csv_size,
                parquet_bucket=settings.storage.bucket,
                parquet_key=pq_key,
                parquet_size_bytes=pq_size,
                status="ready",
            )
            print(f"✅ Catalog updated for {symbol} (SQLite).")

            # Delete local artifacts (keep raw for retention job)
            try:
                csv_path.unlink(missing_ok=True)
                parquet_path.unlink(missing_ok=True)
                print(f"🧹 Deleted local CSV/Parquet staging files for {symbol}.")
            except Exception as e:
                print(f"⚠️ Could not delete local CSV/Parquet for {symbol}: {e}")

        except Exception as e:
            print(f"❌ Error while processing symbol {symbol}: {e}")
            traceback.print_exc()
            try:
                catalog.upsert_block(
                    venue=VENUE, symbol=symbol,
                    start_ts=start_ts.isoformat(), end_ts=end_ts.isoformat(),
                    block_seconds=CAPTURE_DURATION_SECONDS,
                    csv_bucket=settings.storage.bucket, csv_key="", csv_size_bytes=0,
                    parquet_bucket=settings.storage.bucket, parquet_key="", parquet_size_bytes=0,
                    status="failed",
                )
            except Exception:
                pass

    async def run_process_symbol_block(
        symbol: str, raw_path: Path, csv_path: Path, parquet_path: Path,
        start_ts: dt.datetime, end_ts: dt.datetime,
    ) -> None:
        async with PROCESS_SEM:
            await process_symbol_block(symbol, raw_path, csv_path, parquet_path, start_ts, end_ts)

    async def on_rotation_complete(info: RotationInfo) -> None:
        """Build paths from real block timestamps, rename raw file to match coverage, spawn processing."""
        start_tag = info.start_ts.strftime("%Y%m%d_%H%M")
        end_tag = info.end_ts.strftime("%H%M")

        # Canonical raw filename based on actual block window
        base_raw = RAW_DATA_DIR / f"{info.symbol}_{start_tag}-{end_tag}.jsonl.gz"
        canonical_raw = base_raw

        # If the canonical name already exists, fall back to a suffixed variant
        if canonical_raw.exists():
            for i in range(1, 100):
                candidate = RAW_DATA_DIR / f"{info.symbol}_{start_tag}-{end_tag}_{i}.jsonl.gz"
                if not candidate.exists():
                    canonical_raw = candidate
                    break

        raw_path_for_processing = info.path
        try:
            if info.path.resolve() != canonical_raw.resolve():
                raw_path_for_processing = info.path.rename(canonical_raw)
        except Exception as e:
            # If rename fails, log and continue processing with the original path
            print(f"⚠️ Could not rename raw file {info.path} → {canonical_raw}: {e}")

        csv_path = CSV_OUTPUT_DIR / f"{info.symbol}_{start_tag}-{end_tag}_bars.csv"
        parquet_path = PARQUET_OUTPUT_DIR / f"{info.symbol}_{start_tag}-{end_tag}_bars.parquet"
        asyncio.create_task(
            run_process_symbol_block(
                symbol=info.symbol,
                raw_path=raw_path_for_processing,
                csv_path=csv_path,
                parquet_path=parquet_path,
                start_ts=info.start_ts,
                end_ts=info.end_ts,
            )
        )

    while True:
        # Raw file names use cycle start time; real block start/end come from rotation callback
        cycle_start = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        cycle_end = cycle_start + dt.timedelta(seconds=CAPTURE_DURATION_SECONDS)
        start_tag = cycle_start.strftime("%Y%m%d_%H%M")
        end_tag = cycle_end.strftime("%H%M")

        runs = []
        for symbol in SYMBOLS:
            raw_filename = f"{symbol}_{start_tag}-{end_tag}.jsonl.gz"
            runs.append({"symbol": symbol, "raw": RAW_DATA_DIR / raw_filename})

        try:
            print(f"\n--- [{dt.datetime.utcnow()}] Starting new {CAPTURE_DURATION_SECONDS/3600:.1f}-hour cycle ---")
            for run in runs:
                print(f"Capturing raw for {run['symbol']} → {run['raw']}")

            # Start OI (and optional funding) pollers in background threads when enrichment is enabled
            aux_threads = []
            if getattr(settings.enrichment, "enabled", False):
                oi_dir = getattr(settings.enrichment, "oi_dir", "./data/oi")
                for run in runs:
                    t = threading.Thread(
                        target=aux_capture.run_oi_poller_timed,
                        args=(run["symbol"], oi_dir, CAPTURE_DURATION_SECONDS + 60),
                        kwargs={"interval_sec": 1.0},
                    )
                    t.daemon = True
                    t.start()
                    aux_threads.append(t)

            capture_tasks = [
                main_capture_loop(
                    symbol=run["symbol"],
                    settings=settings,
                    seconds=CAPTURE_DURATION_SECONDS,
                    market=MARKET,
                    reconnect_delay=30,
                    output_file=run["raw"],
                    on_rotation_complete=on_rotation_complete,
                )
                for run in runs
            ]

            results = await asyncio.gather(*capture_tasks, return_exceptions=True)
            for t in aux_threads:
                t.join(timeout=5)
            for run, result in zip(runs, results):
                if isinstance(result, Exception):
                    print(f"❌ Capture failed for {run['symbol']}: {result}")
                else:
                    print(f"✅ Capture finished for {run['symbol']} → {run['raw']}")

        except Exception as e:
            print(f"❌ Error in main capture loop: {e}")
            print("Restarting after 60 seconds ...")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main_controller_loop())
