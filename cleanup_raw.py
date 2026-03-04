import os
import time
from pathlib import Path
from lobflow.config import load_settings

CONFIG_PATH = os.environ.get("LOBFLOW_CONFIG", "config.yaml")
S = load_settings(CONFIG_PATH)

RAW_DIR = Path(S.capture.raw_dir)
DAYS = int(os.environ.get("RAW_RETENTION_DAYS", "3"))
CUTOFF = time.time() - DAYS * 24 * 60 * 60

deleted = 0
kept = 0

for p in RAW_DIR.glob("*.jsonl.gz"):
    try:
        if p.stat().st_mtime < CUTOFF:
            p.unlink()
            deleted += 1
        else:
            kept += 1
    except Exception:
        pass

print(f"cleanup_raw: deleted={deleted} kept={kept} dir={RAW_DIR} days={DAYS}")
