"""End-to-end recovery tests: live uvicorn server, real MLLP and HTTP."""

from __future__ import annotations

import asyncio
import socket
import threading
import time

import httpx
import pytest

from app.hl7 import mllp_frame

from tests.test_lis import make_message, send_async


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture
def server(tmp_path):
    import os
    os.environ["LIS_DB_PATH"] = str(tmp_path / "live.db")
    os.environ.pop("LIS_SLOW_APPLY_DELAY", None)
    import importlib
    import app.main as main_module
    importlib.reload(main_module)

    mllp_port = _free_port()
    main_module.settings.mllp_port = mllp_port
    main_module.mllp.port = mllp_port
    main_module.store.slow_apply_delay = 0.0

    import uvicorn

    config = uvicorn.Config(main_module.app, host="127.0.0.1", port=0,
                            log_level="warning")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.05)
    assert srv.started

    class Handle:
        def __init__(self):
            self.port = mllp_port
            self.http_port = srv.servers[0].sockets[0].getsockname()[1]
            self.module = main_module
            self.srv = srv

        def url(self, path):
            return f"http://127.0.0.1:{self.http_port}{path}"

    handle = Handle()
    yield handle
    srv.should_exit = True
    thread.join(timeout=10)


def _send(server, control_id: str, sample: str, *, value: str = "5.0",
          status: str = "F") -> str:
    text = make_message(control_id, sample,
                        [("GLU", value, "mmol/L", status)])
    return asyncio.run(send_async(server.port, mllp_frame(text.encode())))


def _stage(server, client: httpx.Client, content: bytes):
    return client.post(
        server.url("/api/v1/admin/recovery/backups"),
        files={"file": ("lis-backup.db", content,
                        "application/vnd.sqlite3")})


def test_backup_download_roundtrip_and_upload_validation(server):
    _send(server, "A-1", "S-A")
    with httpx.Client(timeout=30) as client:
        backup = client.get(server.url("/api/v1/admin/recovery/backup"))
        assert backup.status_code == 200
        assert backup.content[:16] == b"SQLite format 3\x00"

        resp = _stage(server, client, backup.content)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["counts"] == {"messages": 1, "results": 1}
        assert set(body["tables"]) == {"messages", "results"}

        # Garbage upload is rejected and the live service is untouched.
        bad = client.post(
            server.url("/api/v1/admin/recovery/backups"),
            files={"file": ("bad.db", b"not a database" * 4, "text/plain")})
        assert bad.status_code == 422
        sample = client.get(server.url("/api/v1/samples/S-A")).json()
        assert sample["items"][0]["value"] == "5.0"


def test_upload_rejects_oversized_backup(server):
    oversized = b"SQLite format 3\x00" + b"\x00" * (16 * 1024 * 1024)
    with httpx.Client(timeout=30) as client:
        resp = _stage(server, client, oversized)
        assert resp.status_code == 413
        status_body = client.get(
            server.url("/api/v1/admin/recovery/status")).json()
        assert status_body["phase"] == "idle"


def test_full_recovery_restores_backup_and_replays_stored_ack(server):
    # 1. baseline data and a backup taken at that point in time.
    ack_first = _send(server, "B-1", "S-B")
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()

    # 2. mis-written data arrives after the backup.
    _send(server, "B-2", "S-B", value="99.9", status="C")
    with httpx.Client(timeout=10) as client:
        assert client.get(server.url("/api/v1/samples/S-B")).json()[
            "items"][0]["value"] == "99.9"

    # 3. commit the staged backup.
    with httpx.Client(timeout=30) as client:
        commit = client.post(
            server.url("/api/v1/admin/recovery/commit"),
            params={"backup_id": staged["backup_id"]})
        assert commit.status_code == 200, commit.text
        assert commit.json()["status"] == "restored"

    # 4. the wrong data is gone and the backup state is back.
    with httpx.Client(timeout=10) as client:
        sample = client.get(server.url("/api/v1/samples/S-B")).json()
        assert sample["items"][0]["value"] == "5.0"
        assert client.get(server.url("/api/v1/messages/B-2")).status_code == 404

    # 5. same-ID resend after recovery follows the backup's stored record.
    replay = _send(server, "B-1", "S-B")
    assert replay == ack_first
    with httpx.Client(timeout=10) as client:
        assert client.get(server.url("/api/v1/samples/S-B")).json()[
            "items"][0]["value"] == "5.0"


def test_recovery_pauses_new_mllp_connections_and_resumes(server):
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()
        main = server.module
        observed = {"paused": False, "refused": False, "resumed": False}
        original_pause = main.mllp.pause
        original_resume = main.mllp.resume

        async def instrumented_pause():
            await original_pause()
            observed["paused"] = not main.mllp._serving.is_set()
            try:
                await asyncio.open_connection("127.0.0.1", server.port)
            except ConnectionRefusedError:
                observed["refused"] = True

        async def instrumented_resume():
            await original_resume()
            observed["resumed"] = main.mllp._serving.is_set()

        main.mllp.pause = instrumented_pause
        main.mllp.resume = instrumented_resume
        try:
            resp = client.post(
                server.url("/api/v1/admin/recovery/commit"),
                params={"backup_id": staged["backup_id"]})
            assert resp.status_code == 200
        finally:
            main.mllp.pause = original_pause
            main.mllp.resume = original_resume
        assert observed == {"paused": True, "refused": True,
                            "resumed": True}

    # MLLP works again after recovery.
    assert "MSA|AA|POST-1" in _send(server, "POST-1", "S-POST")


def test_queries_return_503_during_switch_and_200_after(server):
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()
        main = server.module
        saw_503 = threading.Event()
        original_pause = main.mllp.pause

        async def instrumented_pause():
            await original_pause()

            def probe():
                deadline = time.time() + 5
                while time.time() < deadline:
                    try:
                        r = httpx.get(server.url("/api/v1/samples"),
                                      timeout=5)
                        if r.status_code == 503:
                            saw_503.set()
                            return
                    except Exception:
                        pass
                    time.sleep(0.05)

            t = threading.Thread(target=probe)
            t.start()
            await asyncio.sleep(0.5)
            t.join()

        main.mllp.pause = instrumented_pause
        try:
            resp = client.post(
                server.url("/api/v1/admin/recovery/commit"),
                params={"backup_id": staged["backup_id"]})
            assert resp.status_code == 200
        finally:
            main.mllp.pause = original_pause
        assert saw_503.is_set()
        assert client.get(server.url("/api/v1/samples")).status_code == 200


def test_swap_waits_for_slow_inflight_apply(server):
    main = server.module
    main.store.slow_apply_delay = 1.0
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()

    # Kick off an MLLP apply that holds the writer transaction for ~1 s.
    apply_started = time.monotonic()

    def slow_send():
        return _send(server, "SLOW-1", "S-SLOW")

    t = threading.Thread(target=slow_send)
    t.start()
    time.sleep(0.25)  # let the transaction enter the slow section
    commit_started = time.monotonic()
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            server.url("/api/v1/admin/recovery/commit"),
            params={"backup_id": staged["backup_id"]})
        assert resp.status_code == 200, resp.text
    commit_elapsed = time.monotonic() - commit_started
    t.join(timeout=10)

    # The swap could not complete before the slow transaction finished.
    assert commit_elapsed >= 0.7
    assert time.monotonic() - apply_started >= 1.0
    # That late write belonged to the OLD database and must not survive.
    with httpx.Client(timeout=10) as client:
        assert client.get(
            server.url("/api/v1/messages/SLOW-1")).status_code == 404


def test_client_disconnect_still_completes_switch(server):
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()

    # Abandon the HTTP request immediately; the detached task must finish.
    with httpx.Client(timeout=2) as client:
        try:
            client.post(
                server.url("/api/v1/admin/recovery/commit"),
                params={"backup_id": staged["backup_id"]})
        except httpx.HTTPError:
            pass

    deadline = time.time() + 15
    with httpx.Client(timeout=10) as client:
        while time.time() < deadline:
            body = client.get(
                server.url("/api/v1/admin/recovery/status")).json()
            if not body["commit_in_progress"] and body["phase"] == "idle":
                break
            time.sleep(0.1)
        state = client.get(
            server.url("/api/v1/admin/recovery/status")).json()
        assert state["phase"] == "idle"
        assert state["last_result"]["ok"] is True
        assert state["current_task"] is None


def test_concurrent_commits_are_single_flight(server):
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()
        bid = staged["backup_id"]

    results = []

    def commit():
        try:
            r = httpx.post(
                server.url("/api/v1/admin/recovery/commit"),
                params={"backup_id": bid}, timeout=30)
            results.append(r.status_code)
        except Exception as exc:  # pragma: no cover
            results.append(str(exc))

    threads = [threading.Thread(target=commit) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results).count(200) == 1
    assert 409 in results
    with httpx.Client(timeout=10) as client:
        state = client.get(
            server.url("/api/v1/admin/recovery/status")).json()
        assert state["phase"] == "idle"


def test_commit_of_consumed_backup_id_returns_409(server):
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()
        bid = staged["backup_id"]
        first = client.post(
            server.url("/api/v1/admin/recovery/commit"),
            params={"backup_id": bid})
        assert first.status_code == 200
        # The staging file was consumed; a repeated commit of the same id is a
        # single-flight conflict, never a 404.
        second = client.post(
            server.url("/api/v1/admin/recovery/commit"),
            params={"backup_id": bid})
        assert second.status_code == 409
        # An unrelated, never-seen id remains a plain 404.
        unknown = client.post(
            server.url("/api/v1/admin/recovery/commit"),
            params={"backup_id": "f" * 32})
        assert unknown.status_code == 404


def test_bad_backup_swap_keeps_old_database(server):
    _send(server, "GOOD-1", "S-GOOD")
    # A structurally invalid file cannot even be staged; emulate a swap
    # failure by staging a valid backup whose file is then replaced.
    with httpx.Client(timeout=30) as client:
        backup = client.get(
            server.url("/api/v1/admin/recovery/backup")).content
        staged = _stage(server, client, backup).json()
        target = server.module.recovery.staging / (
            "restore-" + staged["backup_id"] + ".db")
        target.write_bytes(b"SQLite format 3\x00" + b"\x00" * 8192)
        resp = client.post(
            server.url("/api/v1/admin/recovery/commit"),
            params={"backup_id": staged["backup_id"]})
        assert resp.status_code == 500

    with httpx.Client(timeout=10) as client:
        assert client.get(server.url("/api/v1/messages/GOOD-1")).status_code == 200
        state = client.get(
            server.url("/api/v1/admin/recovery/status")).json()
        assert state["phase"] == "idle"
        assert state["last_result"]["ok"] is False
        assert client.get(server.url("/api/v1/samples")).status_code == 200


def test_mllp_residual_frame_is_discarded_when_connections_dropped(server):
    main = server.module
    text = make_message("DROP-1", "S-DROP", [("GLU", "5.0", "mmol/L", "F")])
    frame = mllp_frame(text.encode())

    with socket.create_connection(("127.0.0.1", server.port), timeout=10) as sock:
        sock.sendall(frame[:-4])  # never send FS+CR
        with httpx.Client(timeout=30) as client:
            backup = client.get(
                server.url("/api/v1/admin/recovery/backup")).content
            staged = _stage(server, client, backup).json()
            resp = client.post(
                server.url("/api/v1/admin/recovery/commit"),
                params={"backup_id": staged["backup_id"]})
            assert resp.status_code == 200
        # socket is closed by drop_connections during the swap
    time.sleep(0.2)
    with httpx.Client(timeout=10) as client:
        assert client.get(
            server.url("/api/v1/messages/DROP-1")).status_code == 404
    assert main.mllp._conn_tasks == set()
