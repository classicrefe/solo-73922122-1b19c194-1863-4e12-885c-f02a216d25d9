"""Database recovery API support: backup validation and the swap coordinator.

The receiver is single-process, so all MLLP frame handling and HTTP query work
funnels through one :class:`RecoveryCoordinator`.  Recovery is a two-phase
operation:

1. **Upload / validate** — the SQLite backup produced by this service is saved
   under the staging directory and checked there (header, ``integrity_check``,
   the exact two-table schema).  Validation runs in a worker thread and never
   touches the live database, so a rejected upload cannot affect the service.
2. **Commit / swap** — new MLLP connections are paused, data queries return
   503, existing MLLP connections are closed (residual frames are discarded),
   all store work already in flight is awaited, then the validated file
   replaces the live database in one all-or-nothing move.  Failure restores
   the old database; the request survives client disconnection and the
   coordinator never gets stuck in a maintenance state.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from .store import EXPECTED_INDEX, EXPECTED_TABLES

log = logging.getLogger("lis")

MAX_BACKUP_BYTES = 16 * 1024 * 1024  # 16 MiB
SQLITE_MAGIC = b"SQLite format 3\x00"


class ValidationError(Exception):
    """The uploaded backup is unusable; the live service is untouched."""


@dataclass
class JobGate:
    """Tracks every in-flight piece of store work (apply / query / backup).

    Jobs are keyed by the future returned by ``asyncio.to_thread`` so they
    remain tracked even if the HTTP/MLLP request that started them is
    cancelled (the SQLite work in the worker thread runs to completion).
    """

    _jobs: dict[asyncio.Future, str] = field(default_factory=dict)
    _cond: asyncio.Condition = field(default_factory=asyncio.Condition)

    async def run(self, label: str, func, *args, **kwargs):
        future = asyncio.ensure_future(asyncio.to_thread(func, *args, **kwargs))
        async with self._cond:
            self._jobs[future] = label
            self._cond.notify_all()
        try:
            return await future
        except asyncio.CancelledError:
            # The request went away, but the SQLite worker thread must run to
            # completion; shielding here keeps the gate count honest.
            await asyncio.shield(future)
            raise
        finally:
            async with self._cond:
                self._jobs.pop(future, None)
                self._cond.notify_all()

    async def wait_idle(self, timeout: float = 120.0) -> list[str]:
        """Wait until every tracked job has finished; return its labels."""
        await asyncio.wait_for(
            self._cond.wait_for(lambda: not self._jobs), timeout=timeout)
        return list(self._jobs.values())

    def active_labels(self) -> list[str]:
        return list(self._jobs.values())


def _read_only_uri(path: Path) -> str:
    return "file:" + quote(str(path.resolve())) + "?immutable=1"


def validate_backup(path: str | Path) -> dict[str, Any]:
    """Validate an uploaded SQLite backup without touching the live database.

    Opens the file read-only/immutable (no WAL or sidecar files are created),
    runs ``PRAGMA integrity_check`` and verifies that exactly the two expected
    tables exist with the expected columns and the supporting index.  Raises
    :class:`ValidationError` on any deviation; returns summary counts.
    """
    path = Path(path)
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ValidationError(f"cannot read backup file: {exc}") from exc
    if size == 0:
        raise ValidationError("backup file is empty")
    if size > MAX_BACKUP_BYTES:
        raise ValidationError(
            f"backup exceeds {MAX_BACKUP_BYTES} bytes ({size} bytes)")
    with open(path, "rb") as fh:
        header = fh.read(len(SQLITE_MAGIC))
    if header != SQLITE_MAGIC:
        raise ValidationError("not a SQLite database file (bad magic header)")

    uri = _read_only_uri(path)
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=10)
    except sqlite3.Error as exc:
        raise ValidationError(f"cannot open SQLite database: {exc}") from exc
    try:
        try:
            rows = conn.execute("PRAGMA integrity_check").fetchall()
        except sqlite3.Error as exc:
            raise ValidationError(f"integrity_check failed: {exc}") from exc
        result = [r[0] for r in rows]
        if result != ["ok"]:
            raise ValidationError(
                "integrity_check reported corruption: " + "; ".join(result[:5]))

        tables = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT name, type FROM sqlite_master "
                "WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'")
        }
        user_tables = sorted(
            name for name, kind in tables.items() if kind == "table")
        expected = sorted(EXPECTED_TABLES)
        if user_tables != expected:
            raise ValidationError(
                f"table set mismatch: expected {expected}, found {user_tables}")

        for table, columns in EXPECTED_TABLES.items():
            info = conn.execute(
                f"PRAGMA table_info({table})").fetchall()
            actual = [(r[1], r[2].upper(), 1 if r[3] else 0,
                       1 if r[5] else 0) for r in info]
            wanted = [(name, kind.upper(), notnull, pk)
                      for name, kind, notnull, pk in columns]
            if actual != wanted:
                raise ValidationError(
                    f"schema of table '{table}' does not match the service "
                    f"schema; expected {wanted}, found {actual}")

        indexes = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'")
        }
        if EXPECTED_INDEX not in indexes:
            raise ValidationError(
                f"required index '{EXPECTED_INDEX}' is missing")

        counts = {
            table: conn.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in expected
        }
    finally:
        conn.close()

    return {"size_bytes": size, "tables": user_tables, "counts": counts}


class RecoveryCoordinator:
    """Single-flight recovery state machine shared by MLLP and HTTP layers."""

    def __init__(self, staging_dir: str):
        self.staging = Path(staging_dir)
        self.gate = JobGate()
        # Serialises swap execution on the event loop.
        self._commit_lock = asyncio.Lock()
        # Makes the "is a swap active?" check-and-set atomic across the HTTP
        # worker threads feeding one event loop.  Only held for two attribute
        # operations, so the loop is never meaningfully blocked.
        self._commit_guard = threading.Lock()
        # True for the whole swap window, including the detached task after a
        # requesting HTTP connection has gone away.
        self._commit_active = False
        # backup_id of the most recent committed swap; duplicate requests that
        # only get scheduled afterwards can still be recognised as 409 rather
        # than racing the deleted staging file into a 404.
        self._last_committed_id: Optional[str] = None
        self.switching = False
        self.phase: str = "idle"  # idle|pausing|closing-connections|draining|swapping
        self.last_result: Optional[dict] = None
        self.current_task: Optional[asyncio.Task] = None

    @property
    def commit_in_progress(self) -> bool:
        return self._commit_active

    # -- maintenance state -------------------------------------------------- #

    def ensure_accepting(self) -> None:
        """Raise 503 for data queries while a swap is in progress."""
        if self.switching:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=503,
                detail="database recovery in progress; retry shortly")

    # -- staging ------------------------------------------------------------ #

    def new_staging_path(self) -> Path:
        self.staging.mkdir(parents=True, exist_ok=True)
        return self.staging / f"restore-{uuid.uuid4().hex}.db"

    def validate(self, path: Path) -> dict:
        # Runs in a worker thread; no live-database access involved.
        return validate_backup(path)

    # -- the actual swap ---------------------------------------------------- #

    def try_begin_commit(self) -> bool:
        """Atomically claim the single recovery slot (thread-safe, no await).

        Called synchronously while creating the detached swap task, before
        any await point, so concurrent HTTP workers cannot be scheduled in
        between the claim and the actual swap.
        """
        with self._commit_guard:
            if self._commit_active:
                return False
            self._commit_active = True
            return True

    def end_commit(self) -> None:
        with self._commit_guard:
            self._commit_active = False

    async def commit(self, mllp_server, store, candidate: Path,
                     backup_id: str = "") -> dict:
        """Run the full recovery swap under the process-wide single-flight lock.

        Routers claim the slot via :meth:`try_begin_commit` before creating
        the detached task, then execute this coroutine.  ``finally``
        guarantees maintenance state is always cleared and the MLLP listener
        always resumed.
        """
        from fastapi import HTTPException

        if not self._commit_active:
            raise HTTPException(
                status_code=409,
                detail="another recovery operation is already in progress")
        if not candidate.is_file():
            self.end_commit()
            if backup_id == self._last_committed_id:
                # A duplicate request scheduled after the winning swap had
                # already deleted the staging file.
                raise HTTPException(
                    status_code=409,
                    detail="this backup was already restored by another "
                           "recovery operation")
            raise HTTPException(
                status_code=404,
                detail="unknown or expired backup_id; upload the backup again")
        await self._commit_lock.acquire()
        try:
            return await self._run_swap(mllp_server, store, candidate,
                                        backup_id)
        finally:
            self._commit_lock.release()
            self.end_commit()

    async def _run_swap(self, mllp_server, store, candidate: Path,
                        backup_id: str = "") -> dict:
        started = time.monotonic()
        result: dict[str, dict] = {}
        self.switching = True
        self.phase = "pausing"
        log.warning("recovery: pausing new MLLP connections")
        try:
            # 1. Stop accepting new MLLP connections.
            await mllp_server.pause()
            # 2. Close every existing MLLP connection; partial frames are
            #    discarded with the socket (never written).
            self.phase = "closing-connections"
            closed = await mllp_server.drop_connections()
            log.warning("recovery: closed %d old connection(s)", closed)
            # 3. Wait for store work that was already admitted to finish.
            self.phase = "draining"
            labels = await self.gate.wait_idle()
            if labels:  # pragma: no cover - wait_idle returns when empty
                log.warning("recovery: awaited late jobs %s", labels)
            # 4. Swap the database files inside the store's writer lock.
            self.phase = "swapping"
            counts = await asyncio.to_thread(
                store.replace_database, str(candidate))
            result = {"status": "restored", "counts": counts,
                      "closed_connections": closed, "backup_id": backup_id}
            self.last_result = {**result, "ok": True}
            self._last_committed_id = backup_id
        except BaseException as exc:
            log.exception("recovery swap failed; continuing on old database")
            self.last_result = {
                "ok": False,
                "status": "failed",
                "error": str(exc),
            }
            result = {"status": "failed", "error": str(exc)}
        finally:
            # 5. Always resume the listener and clear maintenance state.
            try:
                await mllp_server.resume()
            except Exception:  # pragma: no cover - defensive
                log.exception("recovery: failed to resume MLLP listener")
            self.phase = "idle"
            self.switching = False
            # Staging cleanup (best effort).
            for stale in self.staging.glob("restore-*.db*"):
                try:
                    if stale.resolve() != candidate.resolve():
                        stale.unlink()
                except OSError:
                    pass
            try:
                if candidate.exists():
                    candidate.unlink()
            except OSError:
                pass
            log.warning("recovery: finished as %s in %.2fs",
                        result.get("status"), time.monotonic() - started)
        if result.get("status") != "restored":
            from fastapi import HTTPException

            raise HTTPException(status_code=500, detail=result)
        return result
