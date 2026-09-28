"""Run a real 10-process local demo, inject two sequential crashes, and verify logs.

Requires sibling client and LFD checkouts (or --client_dir / --lfd_dir).
All processes bind to loopback and only processes created here are terminated.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import urllib.request

from websockets.asyncio.client import connect


ROOT = Path(__file__).resolve().parents[1]


def unused_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def run(client_dir, lfd_dir, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    processes, files = {}, {}
    gfd_port, proxy_port, *replica_ports = unused_ports(5)
    gfd_url = f"ws://127.0.0.1:{gfd_port}"
    phase_results = []

    def start(name, script, *args):
        log_file = open(output_dir / f"{name}.log", "w", encoding="utf-8")
        files[name] = log_file
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONUTF8": "1"}
        processes[name] = subprocess.Popen(
            [sys.executable, str(script), *map(str, args)], cwd=script.parent,
            stdout=log_file, stderr=subprocess.STDOUT, env=env,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

    def stop(name):
        proc = processes[name]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=4)

    def read(name):
        return (output_dir / f"{name}.log").read_text(encoding="utf-8", errors="replace")

    def health():
        with urllib.request.urlopen(f"http://127.0.0.1:{gfd_port}/health", timeout=1) as r:
            return json.load(r)

    def wait(predicate, label, seconds=20):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        raise AssertionError(f"Timed out: {label}. See {output_dir}")

    def deliveries():
        return {c: read(c).count("Delivered first reply") for c in ("C1", "C2", "C3")}

    def verify_progress(label, before):
        started = time.monotonic()
        wait(lambda: all(deliveries()[c] >= before[c] + 10 for c in before), label)
        assert all(processes[c].poll() is None for c in before), "Client exited"
        phase_results.append({"phase": label, "elapsed_seconds": round(time.monotonic() - started, 3),
                              "deliveries": deliveries(), "membership": health()["members"]})

    async def missing_lfd_heartbeat():
        async with connect(gfd_url + "/ws/lfd/GHOST", proxy=None) as ws:
            await ws.send(json.dumps({"replica_id": "GHOST",
                                     "url": "ws://127.0.0.1:1"}))
            heartbeat = json.loads(await ws.recv())
            assert heartbeat["type"] == "heartbeat"
            # Deliberately do not answer the application heartbeat.
            try:
                await asyncio.wait_for(ws.recv(), 3)
            except Exception:
                pass

    try:
        start("GFD", ROOT / "gfd.py", "--port", gfd_port,
              "--heartbeat_freq", 0.2, "--timeout", 0.8)
        wait(lambda: health()["member_count"] == 0, "GFD bootstrap")
        asyncio.run(missing_lfd_heartbeat())
        wait(lambda: "GHOST" not in health()["lfds"], "GFD heartbeat timeout")
        assert "GHOST heartbeat failed" in read("GFD")

        start("REPLY-PROXY", ROOT / "tests" / "reply_proxy.py", "--port", proxy_port,
              "--target", f"ws://127.0.0.1:{replica_ports[2]}")

        for i, port in enumerate(replica_ports, 1):
            start(f"LFD{i}", lfd_dir / "lfd.py", "--lfd_id", f"LFD{i}",
                  "--replica_id", f"S{i}", "--server_url", f"ws://127.0.0.1:{port}",
                  "--advertise_url", f"ws://127.0.0.1:{proxy_port if i == 3 else port}",
                  "--gfd_url", gfd_url, "--heartbeat_freq", 0.2, "--timeout", 0.5)
        wait(lambda: len(health()["lfds"]) == 3 and health()["member_count"] == 0,
             "LFDs register before replicas")
        for i, port in enumerate(replica_ports, 1):
            start(f"S{i}", ROOT / "server.py", "--replica_id", f"S{i}",
                  "--port", port, "--mode", "active")
            wait(lambda: health()["member_count"] == i, f"Add S{i}")

        for i in range(1, 4):
            start(f"C{i}", client_dir / "client.py", "--client_id", f"C{i}",
                  "--gfd_url", gfd_url, "--interval", 0.04, "--timeout", 2)
        verify_progress("three replicas", {"C1": 0, "C2": 0, "C3": 0})
        wait(lambda: all(read(c).count("Discarded duplicate reply") >= 10
                         for c in ("C1", "C2", "C3")), "Duplicate suppression")
        wait(lambda: all("Received <" + c + ", S3, 1, reply>" in read(c)
                         for c in ("C1", "C2", "C3")), "Lost reply retried")
        for c in ("C1", "C2", "C3"):
            text = read(c)
            assert text.index("request_num 2: Delivered first reply") < text.index(
                "Received <" + c + ", S3, 1, reply>"), "Slow replica stalled the client"
        assert "Replayed cached reply" in read("S3"), "Lost replies weren't replayed"

        before = deliveries()
        stop("S1")
        wait(lambda: health()["member_count"] == 2, "S1 removal")
        verify_progress("after S1 crash", before)
        before = deliveries()
        stop("S2")
        wait(lambda: health()["member_count"] == 1, "S2 removal")
        verify_progress("after S2 crash", before)
        assert health()["members"][0]["replica_id"] == "S3"
        for c in ("C1", "C2", "C3"):
            assert re.search(r"Received GFD membership v\d+: S3\s", read(c))

        # A fresh replica cannot safely rejoin without state transfer (Milestone 4).
        start("S1-restarted", ROOT / "server.py", "--replica_id", "S1",
              "--port", replica_ports[0], "--mode", "active")
        wait(lambda: "Refusing new/restarted S1" in read("GFD"), "Restart exclusion")
        assert health()["member_count"] == 1
        before = deliveries()
        verify_progress("restart safely excluded", before)

        # Removing an LFD must also remove its replica, even while that replica lives.
        stop("LFD3")
        wait(lambda: health()["member_count"] == 0, "LFD failure removes replica")

        states = {}
        duplicate_counts = {}
        delivered_counts = {}
        for c in ("C1", "C2", "C3"):
            text = read(c)
            assert "Traceback" not in text, f"{c} traceback"
            delivered = re.findall(r"request_num (\d+): Delivered first reply", text)
            assert len(delivered) == len(set(delivered)), f"{c} delivered a duplicate"
            delivered_counts[c] = len(delivered)
            duplicate_counts[c] = text.count("Discarded duplicate reply")
            for cid, rid, req, state in re.findall(
                    r"Received <(C\d+), (S\d+), (\d+), reply> server_state=(\d+)", text):
                states.setdefault((cid, req), set()).add(int(state))
        assert states and all(len(values) == 1 for values in states.values()), "Inconsistent replica replies"
        assert all("FAULT DETECTED" in read(lfd) for lfd in ("LFD1", "LFD2"))
        for name in ("GFD", "S1", "S2", "S3", "LFD1", "LFD2", "LFD3"):
            assert "Traceback" not in read(name), f"{name} traceback"
        report = {"result": "PASS", "phases": phase_results,
                  "delivered_once": delivered_counts, "duplicates_discarded": duplicate_counts,
                  "consistent_request_keys": len(states),
                  "extra_checks": ["GFD heartbeat timeout", "LFDs before replicas",
                                   "lost reply retried without duplicate execution",
                                   "delayed duplicate does not block new requests",
                                   "fresh replica excluded after traffic", "LFD failure removes replica"],
                  "scope": "Loopback, 10 processes; not a physical-machine deployment test"}
        (output_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        for name in reversed(list(processes)):
            stop(name)
        for file in files.values():
            file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client_dir", type=Path, default=ROOT.parent / "client")
    parser.add_argument("--lfd_dir", type=Path, default=ROOT.parent / "LFD")
    parser.add_argument("--output_dir", type=Path, default=ROOT / "test-results")
    args = parser.parse_args()
    run(args.client_dir.resolve(), args.lfd_dir.resolve(), args.output_dir.resolve())
