"""Durable result store and exactly-once message application.

All state lives in one SQLite database.  Every accepted message is applied in
a single ``BEGIN IMMEDIATE`` transaction together with the ACK that was sent
for it, so:

* an ACK is only sent after ``COMMIT`` returned (``synchronous=FULL`` + WAL);
* a crash before commit leaves no partial results and no recorded message,
  and the analyser's retransmission is applied afresh;
* re-delivery of an accepted MSH-10 with identical text returns the stored,
  byte-identical ACK without applying anything again;
* re-delivery of an accepted MSH-10 with different text is rejected (AE);
* concurrent deliveries of the same MSH-10 serialise on the SQLite writer lock,
  so exactly one applies and the others replay its ACK.

OBX-11 semantics enforced here, inside the same transaction:

* ``F`` adds a result item and fails if the item already exists for the sample;
* ``C`` corrects an existing item and fails if the item does not exist.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from .hl7 import (
    ERR_APPLICATION_ERROR,
    ERR_DUPLICATE_KEY,
    Envelope,
    ParsedMessage,
    build_ack,
)

BUSY_TIMEOUT_MS = 30000

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    control_id     TEXT PRIMARY KEY,
    raw_hash       TEXT NOT NULL,
    raw_message    TEXT NOT NULL,
    sample_no      TEXT NOT NULL,
    ack_code       TEXT NOT NULL,
    ack_message    BLOB NOT NULL,
    received_at    TEXT NOT NULL,
    applied_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    sample_no      TEXT NOT NULL,
    code           TEXT NOT NULL,
    value          TEXT NOT NULL,
    unit           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    value_type     TEXT NOT NULL DEFAULT '',
    text           TEXT NOT NULL DEFAULT '',
    obx_seq        INTEGER NOT NULL DEFAULT 0,
    control_id     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    PRIMARY KEY (sample_no, code)
);

CREATE INDEX IF NOT EXISTS idx_results_sample ON results(sample_no);
"""

# Column definitions expected by the recovery API.  Each entry is
# (name, declared type, "not null" flag as in PRAGMA table_info,
#  primary-key position as in PRAGMA table_info).
EXPECTED_TABLES: dict[str, list[tuple[str, str, int, int]]] = {
    "messages": [
        ("control_id", "TEXT", 0, 1),
        ("raw_hash", "TEXT", 1, 0),
        ("raw_message", "TEXT", 1, 0),
        ("sample_no", "TEXT", 1, 0),
        ("ack_code", "TEXT", 1, 0),
        ("ack_message", "BLOB", 1, 0),
        ("received_at", "TEXT", 1, 0),
        ("applied_at", "TEXT", 1, 0),
    ],
    "results": [
        ("sample_no", "TEXT", 1, 1),
        ("code", "TEXT", 1, 1),
        ("value", "TEXT", 1, 0),
        ("unit", "TEXT", 1, 0),
        ("status", "TEXT", 1, 0),
        ("value_type", "TEXT", 1, 0),
        ("text", "TEXT", 1, 0),
        ("obx_seq", "INTEGER", 1, 0),
        ("control_id", "TEXT", 1, 0),
        ("updated_at", "TEXT", 1, 0),
    ],
}

EXPECTED_INDEX = "idx_results_sample"


class ApplyError(Exception):
    """A validly-formed message cannot be applied; maps to an AE ACK."""

    def __init__(self, text: str, err_code: str = ERR_APPLICATION_ERROR,
                 segment: str = ""):
        super().__init__(text)
        self.text = text
        self.err_code = err_code
        self.segment = segment


class DuplicateIdConflict(ApplyError):
    """MSH-10 already accepted with different message text."""

    def __init__(self, control_id: Optional[str]):
        super().__init__(
            f"MSH-10 '{control_id or ''}' was already accepted with different "
            "message content", ERR_DUPLICATE_KEY, "MSH")
        self.control_id = control_id


@dataclass
class ApplyResult:
    ack: bytes
    applied: bool       # False means the stored ACK was replayed


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Store:
    def __init__(self, path: str, slow_apply_delay: float = 0.0):
        self.path = path
        # Serialises writer transactions in-process.  SQLite's own writer lock
        # still protects against other processes; the lock here avoids busy
        # retries under concurrent MLLP connections.  It also serialises the
        # database swap performed by :meth:`replace_database`.
        self._write_lock = threading.Lock()
        # Demo hook: hold the writer lock for this many seconds inside an
        # apply transaction, simulating a slow fsync so the recovery flow can
        # prove that in-flight work is awaited before the swap.  Set from
        # LIS_SLOW_APPLY_DELAY (or directly by tests/demo tooling).
        self.slow_apply_delay = max(0.0, float(slow_apply_delay))

    # -- connection helpers ------------------------------------------------- #

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_MS / 1000,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            # WAL + synchronous=FULL: a completed COMMIT is fsync-durable, so
            # an AA may only be sent after the transaction has committed.
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            conn.close()

    def init(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(SCHEMA)

    # -- application -------------------------------------------------------- #

    def apply(self, parsed: ParsedMessage) -> ApplyResult:
        """Apply one validated message atomically, returning the ACK to send.

        The ACK is generated and persisted in the same transaction as the
        result changes; the caller must only transmit it after this method
        returns.  Raises :class:`ApplyError` (and commits nothing) when the
        message conflicts with existing state.
        """
        with self._write_lock:
            return self._apply_locked(parsed)

    def _apply_locked(self, parsed: ParsedMessage) -> ApplyResult:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if self.slow_apply_delay:
                # Demo-only slow-path: still inside the writer transaction, so
                # a recovery swap must wait for this work to finish.
                time.sleep(self.slow_apply_delay)
            try:
                replay = conn.execute(
                    "SELECT raw_hash, ack_message FROM messages WHERE control_id=?",
                    (parsed.control_id,)).fetchone()
                if replay is not None:
                    if replay["raw_hash"] != parsed.raw_hash:
                        raise DuplicateIdConflict(parsed.control_id)
                    conn.execute("COMMIT")
                    return ApplyResult(bytes(replay["ack_message"]), applied=False)

                for item in parsed.obx_items:
                    existing = conn.execute(
                        "SELECT value, status FROM results "
                        "WHERE sample_no=? AND code=?",
                        (parsed.sample_no, item.code)).fetchone()
                    if item.status == "F":
                        if existing is not None:
                            raise ApplyError(
                                f"OBX-3.1 '{item.code}' already exists for sample "
                                f"'{parsed.sample_no}' but OBX-11 is F",
                                ERR_DUPLICATE_KEY, "OBX")
                        conn.execute(
                            "INSERT INTO results (sample_no, code, value, unit, "
                            "status, value_type, text, obx_seq, control_id, updated_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (parsed.sample_no, item.code, item.value, item.unit,
                             item.status, item.value_type, item.text, item.seq,
                             parsed.control_id, _utc_now()))
                    else:  # C
                        if existing is None:
                            raise ApplyError(
                                f"OBX-3.1 '{item.code}' does not exist for sample "
                                f"'{parsed.sample_no}' so OBX-11 C cannot correct it",
                                ERR_APPLICATION_ERROR, "OBX")
                        conn.execute(
                            "UPDATE results SET value=?, unit=?, status=?, "
                            "value_type=?, text=?, obx_seq=?, control_id=?, updated_at=? "
                            "WHERE sample_no=? AND code=?",
                            (item.value, item.unit, item.status, item.value_type,
                             item.text, item.seq, parsed.control_id, _utc_now(),
                             parsed.sample_no, item.code))

                envelope = Envelope(
                    sending_app=parsed.sending_app,
                    sending_facility=parsed.sending_facility,
                    receiving_app=parsed.receiving_app,
                    receiving_facility=parsed.receiving_facility,
                    control_id=parsed.control_id,
                    version=parsed.version,
                )
                ack = build_ack(envelope, "AA", "Message accepted")
                now = _utc_now()
                conn.execute(
                    "INSERT INTO messages (control_id, raw_hash, raw_message, "
                    "sample_no, ack_code, ack_message, received_at, applied_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (parsed.control_id, parsed.raw_hash,
                     parsed.raw_bytes.decode("ascii"), parsed.sample_no,
                     "AA", ack, now, now))
                conn.execute("COMMIT")
                return ApplyResult(ack, applied=True)
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                raise

    # -- queries ------------------------------------------------------------ #

    def sample_results(self, sample_no: str) -> dict:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT sample_no, code, value, unit, status, value_type, text, "
                "obx_seq, control_id, updated_at FROM results "
                "WHERE sample_no=? ORDER BY obx_seq, code",
                (sample_no,)).fetchall()
            messages = conn.execute(
                "SELECT control_id, ack_code, received_at, applied_at FROM messages "
                "WHERE sample_no=? ORDER BY received_at",
                (sample_no,)).fetchall()
        return {
            "sample_no": sample_no,
            "items": [dict(r) for r in rows],
            "messages": [dict(m) for m in messages],
        }

    def samples(self, limit: int = 200) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT sample_no, COUNT(*) AS item_count, MAX(updated_at) AS updated_at "
                "FROM results GROUP BY sample_no ORDER BY updated_at DESC LIMIT ?",
                (limit,)).fetchall()
        return [dict(r) for r in rows]

    def message(self, control_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT control_id, raw_hash, raw_message, sample_no, ack_code, "
                "ack_message, received_at, applied_at FROM messages WHERE control_id=?",
                (control_id,)).fetchone()
        if row is None:
            return None
        out = dict(row)
        out["ack_message"] = out["ack_message"].decode("ascii")
        return out

    # -- online backup / recovery swap ------------------------------------- #

    def online_backup(self, dest_path: str) -> None:
        """Produce a consistent backup of the live database via the SQLite
        online backup API (works while the WAL database is being written)."""
        with self._write_lock:
            source = sqlite3.connect(self.path, timeout=BUSY_TIMEOUT_MS / 1000)
            target = sqlite3.connect(dest_path, isolation_level=None)
            try:
                source.backup(target)
                target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                target.close()
                source.close()
            # A backup target may have inherited WAL sidecars; fold them in so
            # the uploaded artifact is a single self-contained database file.
            self._fold_wal(dest_path)

    @staticmethod
    def _fold_wal(path: str) -> None:
        """Checkpoint and remove *-wal/*-shm sidecars of a detached file."""
        conn = sqlite3.connect(path, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
        for suffix in ("-wal", "-shm"):
            side = Path(path + suffix)
            if side.exists():
                side.unlink()

    def replace_database(self, candidate_path: str) -> dict:
        """Atomically replace the live database with a validated backup file.

        Caller guarantees no application work is in flight (the coordinator
        quiesces MLLP/HTTP workers first).  The swap is all-or-nothing: if any
        step fails the original database is moved back and re-initialised so
        the service continues on the old database.  Returns row counts of the
        newly installed database.
        """
        candidate = Path(candidate_path)
        live = Path(self.path)
        parent = live.parent
        parent.mkdir(parents=True, exist_ok=True)
        # The candidate was validated read-only and must be a single file; a
        # stale WAL must never be replayed against it (or against the old DB).
        for suffix in ("-wal", "-shm"):
            side = Path(str(candidate) + suffix)
            if side.exists():
                side.unlink()

        with self._write_lock:
            old_paths = [live, Path(str(live) + "-wal"),
                         Path(str(live) + "-shm")]
            saved: list[tuple[Path, Path]] = []
            swapped = False
            try:
                # Move the live database (and any WAL sidecars) out of the way.
                for original in old_paths:
                    if original.exists():
                        backup = Path(str(original) + ".recover-old")
                        if backup.exists():
                            backup.unlink()
                        os.replace(original, backup)
                        saved.append((original, backup))
                os.replace(candidate, live)
                swapped = True

                # Re-initialise the replacement: WAL mode, schema idempotency.
                self.init()
                with self._connect() as conn:
                    integrity = conn.execute(
                        "PRAGMA integrity_check").fetchone()[0]
                    if integrity != "ok":
                        raise sqlite3.DatabaseError(
                            f"post-swap integrity_check failed: {integrity}")
                    counts = {
                        "messages": conn.execute(
                            "SELECT COUNT(*) FROM messages").fetchone()[0],
                        "results": conn.execute(
                            "SELECT COUNT(*) FROM results").fetchone()[0],
                    }
            except BaseException:
                # Best-effort rollback so the service never stays half-swapped.
                try:
                    if swapped and live.exists():
                        reject = Path(str(live) + ".rejected")
                        if reject.exists():
                            reject.unlink()
                        os.replace(live, reject)
                    for original, backup in saved:
                        if not original.exists() and backup.exists():
                            os.replace(backup, original)
                    self._fold_wal(str(live))
                    self.init()
                except Exception:  # pragma: no cover - defensive
                    log = __import__("logging").getLogger("lis")
                    log.exception("recovery rollback failed")
                raise
            # Success: discard the old database and its sidecars.
            for _original, backup in saved:
                for suffix in ("", "-wal", "-shm"):
                    stale = Path(str(backup) + suffix)
                    if stale.exists():
                        stale.unlink()
            return counts
