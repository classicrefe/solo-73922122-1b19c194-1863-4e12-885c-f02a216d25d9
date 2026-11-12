"""Unit tests for backup validation and the atomic database swap."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from app.hl7 import normalize_payload, parse_message
from app.recovery import MAX_BACKUP_BYTES, ValidationError, validate_backup
from app.store import Store

from tests.test_lis import make_message


def _open_store(path: Path) -> Store:
    store = Store(str(path))
    store.init()
    return store


def _apply(store: Store, control_id: str, sample: str, value: str = "5.0"):
    text = make_message(control_id, sample, [("GLU", value, "mmol/L", "F")])
    return store.apply(parse_message(normalize_payload(text.encode())))


def _make_backup(store: Store, dest: Path) -> Path:
    store.online_backup(str(dest))
    assert dest.is_file()
    # A portable backup is a single file; no WAL sidecars may be replayed.
    assert not Path(str(dest) + "-wal").exists()
    assert not Path(str(dest) + "-shm").exists()
    return dest


def test_valid_backup_passes_validation(tmp_path):
    live = _open_store(tmp_path / "live.db")
    _apply(live, "V-1", "SV-1")
    backup = _make_backup(live, tmp_path / "good.db")
    summary = validate_backup(backup)
    assert summary["size_bytes"] > 0
    assert summary["tables"] == ["messages", "results"]
    assert summary["counts"] == {"messages": 1, "results": 1}


def test_non_sqlite_file_rejected(tmp_path):
    fake = tmp_path / "fake.db"
    fake.write_bytes(b"not a database at all" * 10)
    with pytest.raises(ValidationError, match="magic header"):
        validate_backup(fake)
    # Validation never creates sidecar files next to the upload.
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fake.db"]


def test_empty_and_oversized_uploads_rejected(tmp_path):
    empty = tmp_path / "empty.db"
    empty.write_bytes(b"")
    with pytest.raises(ValidationError, match="empty"):
        validate_backup(empty)

    big = tmp_path / "big.db"
    with open(big, "wb") as out:
        out.truncate(MAX_BACKUP_BYTES + 1)
    with pytest.raises(ValidationError, match="exceeds"):
        validate_backup(big)


def test_corrupt_database_fails_integrity_check(tmp_path):
    live = _open_store(tmp_path / "live.db")
    _apply(live, "C-1", "SC-1")
    backup = _make_backup(live, tmp_path / "corrupt.db")
    raw = bytearray(backup.read_bytes())
    page_size = int.from_bytes(bytes(raw[16:18]), "big")
    # Overwrite the b-tree page header of the first interior/leaf table page
    # so SQLite cannot parse the page structure any more.
    offset = page_size
    raw[offset + 0] = 99   # invalid b-tree page type
    raw[offset + 1] = 0
    raw[offset + 2] = 0
    backup.write_bytes(bytes(raw))
    with pytest.raises(ValidationError, match="integrity_check|corrupt|open"):
        validate_backup(backup)


def test_wrong_schema_rejected(tmp_path):
    foreign = tmp_path / "foreign.db"
    conn = sqlite3.connect(foreign)
    conn.execute("CREATE TABLE messages (control_id TEXT PRIMARY KEY, x TEXT)")
    conn.execute("CREATE TABLE results (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()
    with pytest.raises(ValidationError, match="schema"):
        validate_backup(foreign)


def test_extra_table_rejected(tmp_path):
    live = _open_store(tmp_path / "live.db")
    _apply(live, "E-1", "SE-1")
    backup = _make_backup(live, tmp_path / "extra.db")
    conn = sqlite3.connect(backup)
    conn.execute("CREATE TABLE surprise (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    with pytest.raises(ValidationError, match="table set mismatch"):
        validate_backup(backup)


def test_missing_index_rejected(tmp_path):
    live = _open_store(tmp_path / "live.db")
    _apply(live, "I-1", "SI-1")
    backup = _make_backup(live, tmp_path / "noindex.db")
    conn = sqlite3.connect(backup)
    conn.execute("DROP INDEX idx_results_sample")
    conn.commit()
    conn.close()
    with pytest.raises(ValidationError, match="idx_results_sample"):
        validate_backup(backup)


def test_replace_database_restores_backup_and_old_wal_is_discarded(tmp_path):
    live_path = tmp_path / "live.db"
    live = _open_store(live_path)
    _apply(live, "OLD-1", "S-OLD", "9.9")
    backup = _make_backup(live, tmp_path / "point-in-time.db")

    # More traffic after the backup, including WAL content that must not
    # survive into the restored database.
    text = make_message("OLD-2", "S-OLD", [("GLU", "8.8", "mmol/L", "C")])
    live.apply(parse_message(normalize_payload(text.encode())))
    live.samples()  # keep a -wal file around if SQLite chose not to checkpoint
    assert live.sample_results("S-OLD")["items"][0]["value"] == "8.8"

    candidate = tmp_path / "candidate.db"
    backup.replace(candidate)
    # Plant a stale WAL next to the candidate; it must be ignored/discarded.
    Path(str(candidate) + "-wal").write_bytes(b"stale wal content")
    counts = live.replace_database(str(candidate))

    assert counts == {"messages": 1, "results": 1}
    assert live.sample_results("S-OLD")["items"][0]["value"] == "9.9"
    assert live.message("OLD-2") is None
    assert live.message("OLD-1") is not None
    # No leftover sidecars or rollback files.
    names = {p.name for p in tmp_path.iterdir()}
    assert not any("recover-old" in n or "rejected" in n for n in names)
    assert not Path(str(live_path) + "-wal").exists()


def test_replace_database_rolls_back_when_candidate_is_bad(tmp_path):
    live_path = tmp_path / "live.db"
    live = _open_store(live_path)
    _apply(live, "K-1", "S-KEEP", "5.0")

    bad = tmp_path / "bad.db"
    bad.write_bytes(b"SQLite format 3\x00" + b"\x00" * 4096)
    with pytest.raises(sqlite3.Error):
        live.replace_database(str(bad))

    # Service continues on the old database.
    assert live.sample_results("S-KEEP")["items"][0]["value"] == "5.0"
    assert live.message("K-1") is not None
    names = {p.name for p in tmp_path.iterdir()}
    assert not any("recover-old" in n for n in names)


def test_replace_database_keeps_working_after_rollback(tmp_path):
    live_path = tmp_path / "live.db"
    live = _open_store(live_path)
    _apply(live, "R-1", "S-R", "1.0")
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"SQLite format 3\x00" + b"\x00" * 4096)
    with pytest.raises(sqlite3.Error):
        live.replace_database(str(bad))

    text = make_message("R-2", "S-R", [("K", "4.1", "mmol/L", "F")])
    result = live.apply(parse_message(normalize_payload(text.encode())))
    assert result.applied
    assert {i["code"] for i in live.sample_results("S-R")["items"]} == {
        "GLU", "K"}


def test_restored_database_replays_ack_recorded_in_backup(tmp_path):
    live_path = tmp_path / "live.db"
    live = _open_store(live_path)
    first = _apply(live, "DUP-1", "S-DUP")
    original_ack = first.ack
    backup = _make_backup(live, tmp_path / "bk.db")

    # Simulate the bad write that triggers recovery on a fresh live file.
    live2_path = tmp_path / "live2.db"
    live2 = _open_store(live2_path)
    live2.replace_database(str(backup))

    text = make_message("DUP-1", "S-DUP", [("GLU", "5.0", "mmol/L", "F")])
    replay = live2.apply(parse_message(normalize_payload(text.encode())))
    assert not replay.applied
    assert replay.ack == original_ack


def test_slow_apply_hook_holds_writer_lock(tmp_path):
    live = _open_store(tmp_path / "live.db")
    live.slow_apply_delay = 0.3
    start = time.monotonic()
    _apply(live, "SLOW-1", "S-SLOW")
    assert time.monotonic() - start >= 0.3
