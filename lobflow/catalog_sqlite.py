import sqlite3
import time
from pathlib import Path
from typing import Optional

DDL = """
CREATE TABLE IF NOT EXISTS data_blocks (
  venue TEXT NOT NULL,
  symbol TEXT NOT NULL,
  start_ts TEXT NOT NULL,   -- ISO8601 UTC
  end_ts TEXT NOT NULL,     -- ISO8601 UTC
  block_seconds INTEGER NOT NULL,

  csv_bucket TEXT NOT NULL,
  csv_key TEXT NOT NULL,
  csv_size_bytes INTEGER NOT NULL,

  parquet_bucket TEXT NOT NULL,
  parquet_key TEXT NOT NULL,
  parquet_size_bytes INTEGER NOT NULL,

  status TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),

  UNIQUE (venue, symbol, start_ts, end_ts)
);

CREATE INDEX IF NOT EXISTS idx_blocks_vs_start
  ON data_blocks (venue, symbol, start_ts);

CREATE INDEX IF NOT EXISTS idx_blocks_vs_end
  ON data_blocks (venue, symbol, end_ts);
"""

UPSERT = """
INSERT INTO data_blocks (
  venue, symbol, start_ts, end_ts, block_seconds,
  csv_bucket, csv_key, csv_size_bytes,
  parquet_bucket, parquet_key, parquet_size_bytes,
  status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(venue, symbol, start_ts, end_ts) DO UPDATE SET
  block_seconds=excluded.block_seconds,
  csv_bucket=excluded.csv_bucket,
  csv_key=excluded.csv_key,
  csv_size_bytes=excluded.csv_size_bytes,
  parquet_bucket=excluded.parquet_bucket,
  parquet_key=excluded.parquet_key,
  parquet_size_bytes=excluded.parquet_size_bytes,
  status=excluded.status;
"""

class CatalogSQLite:
    def __init__(self, path: str):
        self.path = str(Path(path).expanduser())

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.executescript(DDL)
        return con

    def upsert_block(
        self,
        *,
        venue: str,
        symbol: str,
        start_ts: str,
        end_ts: str,
        block_seconds: int,
        csv_bucket: str,
        csv_key: str,
        csv_size_bytes: int,
        parquet_bucket: str,
        parquet_key: str,
        parquet_size_bytes: int,
        status: str
    ) -> None:
        con = self.connect()
        max_retries = 5
        retry_delay_sec = 0.05
        args = (
            venue, symbol, start_ts, end_ts, block_seconds,
            csv_bucket, csv_key, csv_size_bytes,
            parquet_bucket, parquet_key, parquet_size_bytes,
            status
        )
        try:
            for attempt in range(max_retries):
                try:
                    con.execute(UPSERT, args)
                    con.commit()
                    break
                except sqlite3.OperationalError as e:
                    if attempt == max_retries - 1:
                        raise
                    if "locked" not in str(e).lower() and "busy" not in str(e).lower():
                        raise
                    time.sleep(retry_delay_sec)
        finally:
            con.close()
