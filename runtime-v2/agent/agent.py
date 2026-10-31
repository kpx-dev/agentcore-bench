#!/usr/bin/env python3
"""Probe agent for the AgentCore Runtime V2 PoC: reports how the instance answering it was started.

Serves the AgentCore HTTP contract (GET /ping, POST /invocations on port 8080) with the standard
library only, so the image is a stock python base image plus this file. Every response describes
the process that produced it:

  boot_token       random id drawn at import. V2 snapshots the process at its first healthy /ping and
                   restores that snapshot for every session of the version, so the token repeats
                   across sessions. A V1 instance boots its own process and draws its own token.
  frozen_s         wall-clock seconds that passed while the monotonic clock did not. Monotonic time
                   does not advance across a V2 restore, so this is ~0 for a booted process and about
                   the snapshot's age for a restored one.
  restores         when that jump was first seen, i.e. when this instance was restored.
  since_restore_s  how long the instance had been running restored when the request arrived. On the
                   first request this separates an on-demand restore (well under a second) from an
                   instance restored ahead of time into a pool (however long it waited).
  prestart_lead_s  how long the process had been running before its first request; a V1 instance
                   started ahead of time shows its time in the warm pool here.

Ops (payload "op"): "probe" (default), "egress" (which AWS endpoints and the internet this instance
can reach, and at which addresses), "ballast" (time a full read of /app/ballast.bin).

The bind address defaults to loopback for local runs; the image sets AGENT_HOST=0.0.0.0, which the
AgentCore contract requires inside the microVM (the runtime is reached only through AgentCore, which
authenticates every invocation with SigV4).
"""

from __future__ import annotations

import concurrent.futures
import http.client
import http.server
import ipaddress
import json
import os
import socket
import sys
import threading
import time
import uuid
from pathlib import Path

# Captured at import, deliberately: on V2 these are frozen into the snapshot.
T0_WALL, T0_MONO = time.time(), time.monotonic()
BOOT_TOKEN = uuid.uuid4().hex

APP_DIR = Path(__file__).resolve().parent
HOST = os.environ.get("AGENT_HOST", "127.0.0.1")
PORT = int(os.environ.get("AGENT_PORT", "8080"))
TICK_S = 0.05     # heartbeat period
JUMP_S = 2.0      # wall-minus-monotonic step that counts as a restore (well above any NTP slew)
SESSION_HEADER = "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id"
SAFE_ENV = ("AWS_REGION", "AWS_DEFAULT_REGION", "POC_MARKER", "POC_IMAGE_BUILD")


def read_text(path: str | Path) -> str | None:
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def read_build() -> dict:
    try:
        return json.loads((APP_DIR / "build.json").read_text())
    except (OSError, ValueError):
        return {}


BUILD_AT_IMPORT = read_build()

_lock = threading.Lock()
_restores: list[dict] = []
_pings: list[float] = []   # monotonic time of every /ping (capped)
_state = {"instance_token": None, "first_request_wall": None, "first_request_mono": None,
          "requests": 0, "ballast_reads": 0, "offset": T0_WALL - T0_MONO}


def log(event: str, **fields) -> None:
    print(json.dumps({"poc": event, "wall": round(time.time(), 3), "boot": BOOT_TOKEN, **fields},
                     default=str), flush=True)


def _instance_token_locked() -> str:
    # Drawn lazily: after a restore os.urandom returns fresh entropy, so this one is per instance.
    if _state["instance_token"] is None:
        _state["instance_token"] = os.urandom(8).hex()
    return _state["instance_token"]


def check_jump(seen_by: str) -> dict | None:
    """Record a restore if wall time stepped ahead of monotonic time since the last check.

    Both the heartbeat and the request handler call this, so a request that arrives before the
    heartbeat's next tick still sees its own restore.
    """
    wall, mono = time.time(), time.monotonic()
    with _lock:
        jump = (wall - mono) - _state["offset"]
        _state["offset"] = wall - mono
        if abs(jump) < JUMP_S:
            return None
        rec = {"wall": round(wall, 3), "mono": round(mono, 3), "jump_s": round(jump, 3), "seen_by": seen_by}
        _restores.append(rec)
        token = _instance_token_locked()
    log("RESTORE", instance=token, **rec)
    return rec


def heartbeat() -> None:
    while True:
        time.sleep(TICK_S)
        check_jump("heartbeat")


def proc_facts() -> dict:
    uptime = read_text("/proc/uptime")
    mem_total = next((int(line.split()[1]) // 1024 for line in (read_text("/proc/meminfo") or "").splitlines()
                      if line.startswith("MemTotal:")), None)
    return {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "boot_id": read_text("/proc/sys/kernel/random/boot_id"),
        "uptime_s": float(uptime.split()[0]) if uptime else None,
        "cpu_count": os.cpu_count(),
        "mem_total_mb": mem_total,
        "cgroup_memory_max": read_text("/sys/fs/cgroup/memory.max"),
        "kernel": os.uname().release,
        "python": sys.version.split()[0],
    }


def describe(recv_wall: float, recv_mono: float, session_id: str | None) -> dict:
    with _lock:
        first = _state["first_request_mono"] is None
        if first:
            _state["first_request_wall"], _state["first_request_mono"] = recv_wall, recv_mono
        _state["requests"] += 1
        token = _instance_token_locked()
        restores, pings, state = list(_restores), list(_pings), dict(_state)
    first_mono = state["first_request_mono"]
    last = restores[-1] if restores else None
    before_first = [r for r in restores if r["mono"] <= first_mono + 0.001]
    return {
        "boot_token": BOOT_TOKEN,
        "instance_token": token,
        "started": "restored" if restores else "booted",
        "frozen_s": round((recv_wall - T0_WALL) - (recv_mono - T0_MONO), 3) + 0.0,  # no "-0.0"
        "process_started_wall": round(T0_WALL, 3),
        "request_received_wall": round(recv_wall, 3),
        "first_request_wall": round(state["first_request_wall"], 3),
        "is_first_request": first,
        "requests_served": state["requests"],
        "prestart_lead_s": round(first_mono - T0_MONO, 3),
        "restore_count": len(restores),
        "restores": restores[-3:],
        "since_restore_s": max(round(recv_mono - last["mono"], 3), 0.0) if last else None,
        "first_request_since_restore_s": (max(round(first_mono - before_first[-1]["mono"], 3), 0.0)
                                          if before_first else None),
        "pings_total": len(pings),
        "pings_since_restore": sum(1 for p in pings if last and p >= last["mono"]),
        "ping_offsets_s": [round(p - T0_MONO, 3) for p in pings[:10]],   # when the platform pinged, after import
        "session_id_seen": session_id,
        "build_at_import": BUILD_AT_IMPORT.get("build_id"),
        "build_on_disk": read_build().get("build_id"),
        "env": {k: os.environ.get(k) for k in SAFE_ENV},
        **proc_facts(),
    }


def _reach(name: str, host: str, timeout_s: float) -> dict:
    out: dict = {"target": name, "host": host}
    t0 = time.perf_counter()
    try:
        ips = sorted({ai[4][0] for ai in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)})
        out.update(ips=ips, private=all(ipaddress.ip_address(ip).is_private for ip in ips),
                   dns_ms=round((time.perf_counter() - t0) * 1000, 1))
        conn = http.client.HTTPSConnection(host, 443, timeout=timeout_s)
        try:
            conn.request("GET", "/", headers={"User-Agent": "agentcore-v2-poc"})
            resp = conn.getresponse()
            body = resp.read(256)
            out.update(reachable=True, status=resp.status)
            if name == "internet":
                out["egress_ip"] = body.decode(errors="replace").strip()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - every failure mode is a result here
        out.update(reachable=False, error=f"{type(exc).__name__}: {exc}"[:200])
    out["ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return out


def egress(region: str, timeout_s: float = 3.0) -> list[dict]:
    """Which networks this instance can reach. In VPC mode with private subnets the internet should be
    unreachable, S3 should answer through the gateway endpoint, and the interface endpoints (ECR, Logs)
    should resolve to private addresses in the VPC."""
    targets = {"internet": "checkip.amazonaws.com", "s3": f"s3.{region}.amazonaws.com",
               "ecr_api": f"api.ecr.{region}.amazonaws.com", "logs": f"logs.{region}.amazonaws.com",
               "sts": f"sts.{region}.amazonaws.com"}
    with concurrent.futures.ThreadPoolExecutor(len(targets)) as pool:
        return list(pool.map(lambda kv: _reach(kv[0], kv[1], timeout_s), targets.items()))


def read_ballast() -> dict:
    path = APP_DIR / "ballast.bin"
    if not path.exists():
        return {"ballast": "missing"}
    with _lock:
        index = _state["ballast_reads"]
        _state["ballast_reads"] += 1
    t0, total = time.perf_counter(), 0
    with open(path, "rb", buffering=0) as f:
        while chunk := f.read(1 << 20):
            total += len(chunk)
    ms = (time.perf_counter() - t0) * 1000
    return {"ballast_bytes": total, "ballast_read_ms": round(ms, 1), "ballast_read_index": index,
            "ballast_mb_per_s": round(total / 1048576 / max(ms / 1000, 1e-6), 1)}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "agentcore-v2-poc"

    def log_message(self, fmt, *args):  # the default access log would drown the probe's own lines
        pass

    def _send(self, code: int, obj: dict) -> None:
        body = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.split("?", 1)[0] != "/ping":
            return self._send(404, {"error": "not found"})
        check_jump("ping")
        with _lock:
            if len(_pings) < 10_000:
                _pings.append(time.monotonic())
            n = len(_pings)
        if n <= 3:  # the pings before a V2 snapshot: when the platform first saw this process healthy
            log("PING", n=n, since_import_s=round(time.monotonic() - T0_MONO, 3))
        self._send(200, {"status": "Healthy", "time_of_last_update": int(time.time())})

    def do_POST(self):
        if self.path.split("?", 1)[0] != "/invocations":
            return self._send(404, {"error": "not found"})
        recv_wall, recv_mono = time.time(), time.monotonic()
        check_jump("request")
        length = int(self.headers.get("Content-Length") or 0)
        try:
            payload = json.loads(self.rfile.read(length) or b"{}") if length else {}
        except ValueError:
            payload = {}
        op = payload.get("op", "probe") if isinstance(payload, dict) else "probe"
        session_id = self.headers.get(SESSION_HEADER)
        out = describe(recv_wall, recv_mono, session_id)
        if op == "egress":
            region = payload.get("region") or os.environ.get("AWS_REGION") or "us-west-2"
            out["egress"] = egress(region)
        elif op == "ballast":
            out.update(read_ballast())
        out["op"] = op
        out["handler_ms"] = round((time.monotonic() - recv_mono) * 1000, 1)
        log("REQUEST", op=op, instance=out["instance_token"], session=session_id, started=out["started"],
            frozen_s=out["frozen_s"], since_restore_s=out["since_restore_s"],
            prestart_lead_s=out["prestart_lead_s"], first=out["is_first_request"])
        self._send(200, out)


def main() -> None:
    threading.Thread(target=heartbeat, name="heartbeat", daemon=True).start()
    log("BOOT", build=BUILD_AT_IMPORT, env={k: os.environ.get(k) for k in SAFE_ENV},
        env_names=sorted(os.environ), **proc_facts())
    server = http.server.ThreadingHTTPServer((HOST, PORT), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
