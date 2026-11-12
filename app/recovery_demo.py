"""End-to-end recovery demo (standard library only; runs inside the container).

Two demonstrations are provided:

1. ``backup`` (default): show the full maintenance cycle —
   send results, take an online backup, simulate mis-written data, upload the
   backup, restore it, and prove the same-MSH-10 resend then follows the
   record stored in the backup.

2. ``slow``: enable a deliberately slow writer transaction
   (``LIS_SLOW_APPLY_DELAY`` on a second compose service), start a slow
   in-flight MLLP write and trigger recovery concurrently; the swap waits for
   the admitted transaction to finish, discards it with the old database and
   never gets stuck.

Usage (container):
    python -m app.recovery_demo --base-url http://127.0.0.1:8000 \\
        --mllp-host 127.0.0.1 --mllp-port 2575
    python -m app.recovery_demo --scenario slow \\
        --base-url http://127.0.0.1:8001 --mllp-port 2576
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

VT = b"\x0b"
FS = b"\x1c"
CR = b"\x0d"


def log(step: str, detail: str = "") -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {step:<26} {detail}")


def make_message(control_id: str, sample: str, value: str,
                 status: str = "F") -> bytes:
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    msh = "|".join(["MSH", "^~\\&", "ANALYSER", "LAB", "LIS", "HOSP", now,
                    "", "ORU^R01", control_id, "P", "2.5.1"])
    obr = "|".join(["OBR", "1", "S01", f"{sample}^LAB", "", "", "", "", "",
                    "", "", "", "", "", "", "", "DR"])
    obx = "|".join(["OBX", "1", "NM", "GLU^Analyte", "", value, "mmol/L",
                    "", "", "", "", status])
    return ("\r".join([msh, obr, obx]) + "\r").encode("ascii")


def mllp_send(host: str, port: int, payload: bytes,
              timeout: float = 30.0) -> str:
    frame = VT + payload + FS + CR
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(frame)
        data = b""
        while FS not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
    return data.split(FS, 1)[0].split(VT, 1)[-1].decode("ascii", "replace")


def http(method: str, url: str, *, data: bytes | None = None,
         headers: dict | None = None, timeout: float = 60.0):
    req = urllib.request.Request(url, data=data, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, dict(resp.headers), body
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def api(base: str, method: str, path: str, **kwargs):
    status, _headers, body = http(method, base.rstrip("/") + path, **kwargs)
    try:
        parsed = json.loads(body) if body else {}
    except json.JSONDecodeError:
        parsed = {"raw": body.decode("ascii", "replace")}
    return status, parsed


def msa(ack: str) -> str:
    for segment in ack.split("\r"):
        parts = segment.split("|")
        if parts and parts[0] == "MSA":
            return parts[1]
    return "?"


def multipart_body(filename: str, content: bytes) -> tuple[bytes, str]:
    boundary = "----lisdemo" + uuid.uuid4().hex
    head = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        "Content-Type: application/vnd.sqlite3\r\n\r\n"
    ).encode("ascii")
    tail = f"\r\n--{boundary}--\r\n".encode("ascii")
    return head + content + tail, f"multipart/form-data; boundary={boundary}"


def wait_idle(base: str, deadline_s: float = 30.0) -> dict:
    deadline = time.time() + deadline_s
    while True:
        status, state = api(base, "GET", "/api/v1/admin/recovery/status")
        if state.get("phase") == "idle" and not state.get(
                "commit_in_progress"):
            return state
        if time.time() > deadline:
            raise TimeoutError(f"recovery did not finish: {state}")
        time.sleep(0.2)


def demo_backup(base: str, host: str, port: int) -> int:
    run = uuid.uuid4().hex[:6].upper()
    sample = f"S-DEMO-{run}"
    cid = f"DEMO-{run}-1"

    log("1. send baseline", f"sample={sample} MSH-10={cid} GLU=5.0")
    ack = mllp_send(host, port, make_message(cid, sample, "5.0"))
    log("   baseline ACK", f"MSA={msa(ack)}")
    first_ack = ack

    log("2. online backup", "GET /api/v1/admin/recovery/backup")
    status, _hdr, backup = http(
        "GET", base.rstrip("/") + "/api/v1/admin/recovery/backup")
    assert status == 200, f"backup download failed: {status}"
    log("   backup size", f"{len(backup)} bytes, magic={backup[:16]!r}")

    log("3. simulate bad write", "correction GLU=99.9 (F after C typo scenario)")
    bad_ack = mllp_send(host, port,
                        make_message(f"BAD-{run}", sample, "99.9", "C"))
    log("   bad-write ACK", f"MSA={msa(bad_ack)}")
    _, sample_now = api(base, "GET", f"/api/v1/samples/{sample}")
    log("   current value", sample_now["items"][0]["value"])
    assert sample_now["items"][0]["value"] == "99.9"

    log("4. upload backup", "POST /admin/recovery/backups (max 16 MiB)")
    body, content_type = multipart_body("lis-backup.db", backup)
    status, staged = api(base, "POST",
                         "/api/v1/admin/recovery/backups",
                         data=body,
                         headers={"Content-Type": content_type})
    assert status == 201, f"staging failed: {status} {staged}"
    backup_id = staged["backup_id"]
    log("   staged", f"id={backup_id} counts={staged['counts']}")

    log("5. commit restore", "POST /admin/recovery/commit")
    status, result = api(base, "POST",
                         f"/api/v1/admin/recovery/commit?backup_id={backup_id}")
    assert status == 200, f"commit failed: {status} {result}"
    log("   restored", json.dumps(result, ensure_ascii=False))

    _, sample_after = api(base, "GET", f"/api/v1/samples/{sample}")
    value = sample_after["items"][0]["value"]
    log("6. value after restore", value)
    assert value == "5.0"

    log("7. resend same MSH-10", "must replay the backup's stored ACK")
    replay = mllp_send(host, port, make_message(cid, sample, "5.0"))
    log("   replay ACK", f"MSA={msa(replay)} identical={replay == first_ack}")
    assert replay == first_ack, "resend must return the backup's original ACK"

    log("8. reject bad upload", "garbage file must be 422 and leave service up")
    bad_body, bad_ct = multipart_body("fake.db", b"not a database" * 8)
    status, rejected = api(base, "POST",
                           "/api/v1/admin/recovery/backups",
                           data=bad_body, headers={"Content-Type": bad_ct})
    log("   rejection", f"HTTP {status}: {rejected.get('detail')}")
    assert status == 422
    status, _ = api(base, "GET", "/health")
    assert status == 200

    log("OK", "recovery demo completed; service kept running throughout")
    return 0


def demo_slow(base: str, host: str, port: int) -> int:
    run = uuid.uuid4().hex[:6].upper()
    sample = f"S-SLOW-{run}"
    cid = f"SLOW-{run}"

    status, state = api(base, "GET", "/api/v1/admin/recovery/status")
    log("service status", f"phase={state['phase']}")

    log("1. take backup", "before the slow write")
    status, _hdr, backup = http(
        "GET", base.rstrip("/") + "/api/v1/admin/recovery/backup")
    assert status == 200
    body, content_type = multipart_body("lis-backup.db", backup)
    status, staged = api(base, "POST",
                         "/api/v1/admin/recovery/backups",
                         data=body,
                         headers={"Content-Type": content_type})
    assert status == 201, staged
    backup_id = staged["backup_id"]

    log("2. start slow in-flight MLLP write",
        "writer transaction holds for the configured delay")
    started = time.time()
    conn = socket.create_connection((host, port), timeout=30)
    conn.settimeout(30)
    conn.sendall(VT + make_message(cid, sample, "7.7") + FS + CR)

    # Give the server time to enter the slow writer transaction.
    time.sleep(1.0)
    log("3. trigger recovery", "while the slow write is still in flight")
    commit_started = time.time()
    status, result = api(base, "POST",
                         "/api/v1/admin/recovery/commit"
                         f"?backup_id={backup_id}",
                         timeout=60)
    commit_elapsed = time.time() - commit_started
    assert status == 200, f"commit failed: {status} {result}"
    log("   commit returned", f"after {commit_elapsed:.2f}s "
                              f"(waited for in-flight work)")

    # Drain the ACK of the old transaction; its data belongs to the old DB.
    try:
        data = b""
        while FS not in data:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        log("   old-write ACK", f"MSA={msa(data.decode('ascii', 'replace'))}")
    except OSError:
        log("   old connection", "closed by recovery (residual frame dropped)")
    finally:
        conn.close()
    log("   slow write lasted", f"{time.time() - started:.2f}s")

    state = wait_idle(base)
    log("4. final state", f"phase={state['phase']} last={state['last_result']}")
    status, message = api(base, "GET", f"/api/v1/messages/{cid}")
    log("5. late write gone", f"GET old MSH-10 -> HTTP {status}")
    assert status == 404, "the slow write against the old DB must not survive"

    log("6. MLLP accepting again", "send a fresh message")
    ack = mllp_send(host, port, make_message(f"AFTER-{run}", sample, "4.4"))
    log("   post-recovery ACK", f"MSA={msa(ack)}")
    assert msa(ack) == "AA"
    log("OK", "slow-write recovery demo completed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--mllp-host", default="127.0.0.1")
    parser.add_argument("--mllp-port", type=int, default=2575)
    parser.add_argument("--scenario", choices=["backup", "slow"],
                        default="backup")
    args = parser.parse_args()
    try:
        if args.scenario == "slow":
            return demo_slow(args.base_url, args.mllp_host, args.mllp_port)
        return demo_backup(args.base_url, args.mllp_host, args.mllp_port)
    except (AssertionError, OSError, urllib.error.URLError) as exc:
        print(f"demo failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
