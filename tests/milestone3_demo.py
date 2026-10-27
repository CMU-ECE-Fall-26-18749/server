"""Run and verify the complete Milestone 3 scenario on one machine."""

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


SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent


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


def run():
    output = SERVER / "test-results" / "milestone3"
    output.mkdir(parents=True, exist_ok=True)
    gfd_port, *replica_ports = unused_ports(4)
    processes = {}
    streams = []

    def start(name, script, *flags):
        stream = open(output / f"{name}.log", "w", encoding="utf-8")
        streams.append(stream)
        processes[name] = subprocess.Popen(
            [sys.executable, str(script), *map(str, flags)],
            cwd=script.parent,
            stdout=stream,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

    def stop(name):
        process = processes[name]
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    def health(port):
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/health", timeout=1) as response:
            return json.load(response)

    def wait_for(predicate, label, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except (OSError, ValueError):
                pass
            time.sleep(0.08)
        raise AssertionError(f"Timed out waiting for {label}; see {output}")

    def log(name):
        return (output / f"{name}.log").read_text(encoding="utf-8")

    def delivery_counts():
        return {
            client: log(client).count("Delivered reply")
            for client in ("C1", "C2", "C3")
        }

    def wait_for_progress(label):
        before = delivery_counts()
        wait_for(
            lambda: all(delivery_counts()[client] >= before[client] + 8
                        for client in before),
            label,
        )

    async def retry_same_request():
        gfd_url = f"ws://127.0.0.1:{gfd_port}"
        async with connect(gfd_url + "/ws/client/PROBE", proxy=None) as control:
            await control.recv()
            await control.send(json.dumps({
                "type": "order",
                "request_num": 1,
                "payload": "retry-test",
            }))
            ordered = json.loads(await control.recv())
            url = f"ws://127.0.0.1:{replica_ports[0]}/ws/client/PROBE"
            async with connect(url, proxy=None) as ws:
                await ws.send(json.dumps(ordered))
                first = json.loads(await ws.recv())
            async with connect(url, proxy=None) as ws:
                await ws.send(json.dumps(ordered))
                second = json.loads(await ws.recv())
            assert first == second

    try:
        start(
            "GFD", SERVER / "gfd.py",
            "--port", gfd_port,
            "--heartbeat_freq", 0.15,
            "--timeout", 0.7,
        )
        wait_for(lambda: health(gfd_port)["member_count"] == 0, "GFD")

        for number, port in enumerate(replica_ports, 1):
            start(
                f"LFD{number}", ROOT / "LFD" / "lfd.py",
                "--lfd_id", f"LFD{number}",
                "--replica_id", f"S{number}",
                "--server_url", f"ws://127.0.0.1:{port}",
                "--advertise_url", f"ws://127.0.0.1:{port}",
                "--gfd_url", f"ws://127.0.0.1:{gfd_port}",
                "--heartbeat_freq", 0.15,
                "--timeout", 0.5,
            )
        wait_for(lambda: len(health(gfd_port)["lfds"]) == 3, "three LFDs")

        start(
            "S1", SERVER / "passive_server.py",
            "--replica_id", "S1",
            "--port", replica_ports[0],
            "--role", "primary",
            "--checkpoint_interval", 0.3,
            "--backup_url", f"ws://127.0.0.1:{replica_ports[1]}",
            "--backup_url", f"ws://127.0.0.1:{replica_ports[2]}",
        )
        wait_for(
            lambda: [m["replica_id"] for m in health(gfd_port)["members"]]
            == ["S1"],
            "S1 to join first",
        )
        for number in (2, 3):
            start(
                f"S{number}", SERVER / "passive_server.py",
                "--replica_id", f"S{number}",
                "--port", replica_ports[number - 1],
                "--role", "backup",
            )
            wait_for(
                lambda expected=number: health(gfd_port)["member_count"]
                == expected,
                f"S{number} to join",
            )

        for number in range(1, 4):
            start(
                f"C{number}", ROOT / "client" / "client.py",
                "--client_id", f"C{number}",
                "--gfd_url", f"ws://127.0.0.1:{gfd_port}",
                "--interval", 0.06,
                "--timeout", 2,
                "--max_requests", 60,
            )

        wait_for_progress("normal operation")
        wait_for(
            lambda: all(health(port)["checkpoint_count"] >= 2
                        for port in replica_ports[1:]),
            "checkpoints on both backups",
        )
        for client in ("C1", "C2", "C3"):
            assert all(f"Connected to S{number}" in log(client)
                       for number in range(1, 4))

        stop("S2")
        wait_for(lambda: health(gfd_port)["member_count"] == 2, "S2 removal")
        wait_for_progress("client progress after S2 crash")
        old_count = health(replica_ports[2])["checkpoint_count"]
        wait_for(lambda: health(replica_ports[2])["checkpoint_count"] > old_count,
                 "continued S3 checkpoints")

        stop("S3")
        wait_for(lambda: health(gfd_port)["member_count"] == 1, "S3 removal")
        wait_for_progress("client progress after S3 crash")
        wait_for(lambda: all(processes[c].poll() == 0
                             for c in ("C1", "C2", "C3")),
                 "clients to finish")

        asyncio.run(retry_same_request())

        for client in ("C1", "C2", "C3"):
            replies = re.findall(r"Received <C\d+, (S\d+),", log(client))
            assert replies and set(replies) == {"S1"}
            delivered = re.findall(r"request_num (\d+): Delivered reply", log(client))
            assert len(delivered) == len(set(delivered))
        for backup in ("S2", "S3"):
            assert "Received checkpoint" in log(backup)
            assert "Received <C" not in log(backup)
        for path in output.glob("*.log"):
            assert "Traceback" not in path.read_text(encoding="utf-8")

        report = {
            "result": "PASS",
            "delivered_once": delivery_counts(),
            "checks": [
                "clients connected to all three replicas",
                "only S1 processed requests and replied",
                "S2 and S3 received checkpoints",
                "clients continued after S2 and S3 crashed",
                "retry did not apply a request twice",
            ],
            "scope": "Local process test; rehearse the real network separately",
        }
        (output / "report.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(json.dumps(report, indent=2))
    finally:
        for name in reversed(list(processes)):
            stop(name)
        for stream in streams:
            stream.close()


if __name__ == "__main__":
    run()
