"""
Test that capture files satisfy the invariant: first record is snapshot,
then a depth or meta_bracket that brackets the snapshot, then the rest.
"""
import gzip
import json
import tempfile
from pathlib import Path

import pytest


def validate_capture_file_shape(path: Path) -> None:
    """
    Assert file starts with: one snapshot, then a depth or meta_bracket record, then any.
    Raises AssertionError or ValueError if the invariant is violated.
    """
    with gzip.open(path, "rt") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    if not lines:
        raise ValueError("Empty file")
    records = [json.loads(ln) for ln in lines]
    first = records[0]
    assert first.get("type") == "snapshot", f"First record must be snapshot, got {first.get('type')!r}"
    snap_id = first.get("data", {}).get("lastUpdateId")
    assert snap_id is not None, "Snapshot must have lastUpdateId"
    need_id = snap_id + 1
    if len(records) < 2:
        raise ValueError("File must have at least snapshot and one bracket record")
    second = records[1]
    assert second.get("type") in ("depth", "meta_bracket"), (
        f"Second record must be depth or meta_bracket, got {second.get('type')!r}"
    )
    if second.get("type") == "depth":
        d = second.get("data", {})
        U, u = d.get("U"), d.get("u")
        assert U is not None and u is not None, "Depth must have U and u"
        assert U <= need_id <= u, f"Depth [U,u] must bracket {need_id}, got [{U},{u}]"


def test_capture_file_shape_valid():
    """Valid file: snapshot, then bracketing depth, then trade."""
    snap = {"lastUpdateId": 100, "bids": [], "asks": []}
    depth = {"U": 99, "u": 105, "b": [], "a": []}
    with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as f:
        path = Path(f.name)
    try:
        with gzip.open(path, "wt") as fh:
            fh.write(json.dumps({"type": "snapshot", "data": snap}) + "\n")
            fh.write(json.dumps({"type": "depth", "data": depth}) + "\n")
            fh.write(json.dumps({"type": "trade", "data": {}}) + "\n")
        validate_capture_file_shape(path)
    finally:
        path.unlink(missing_ok=True)


def test_capture_file_shape_snapshot_then_meta_bracket():
    """Valid file: snapshot then meta_bracket (no depth as second record)."""
    snap = {"lastUpdateId": 200, "bids": [], "asks": []}
    with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as f:
        path = Path(f.name)
    try:
        with gzip.open(path, "wt") as fh:
            fh.write(json.dumps({"type": "snapshot", "data": snap}) + "\n")
            fh.write(json.dumps({"type": "meta_bracket", "data": {"need_id": 201}}) + "\n")
        validate_capture_file_shape(path)
    finally:
        path.unlink(missing_ok=True)


def test_capture_file_shape_rejects_depth_first():
    """Invalid: depth before snapshot."""
    depth = {"U": 1, "u": 10, "b": [], "a": []}
    with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as f:
        path = Path(f.name)
    try:
        with gzip.open(path, "wt") as fh:
            fh.write(json.dumps({"type": "depth", "data": depth}) + "\n")
            fh.write(json.dumps({"type": "snapshot", "data": {"lastUpdateId": 5}}) + "\n")
        with pytest.raises(AssertionError, match="First record must be snapshot"):
            validate_capture_file_shape(path)
    finally:
        path.unlink(missing_ok=True)
