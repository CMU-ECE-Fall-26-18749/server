"""Verify both Milestone 4 configurations with real local processes."""

import asyncio
import json
from contextlib import ExitStack
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

from websockets.asyncio.client import connect

from milestone3_demo import unused_ports


SERVER = Path(__file__).resolve().parents[1]
ROOT = SERVER.parent


def health(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
        return json.load(response)


def wait_for(predicate, label):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except OSError:
            pass
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {label}")


async def order(url, client_id, number):
    async with connect(f"{url}/ws/client/{client_id}", proxy=None) as ws:
        await ws.send(json.dumps({"type": "order", "request_num": number,
                                  "payload": f"{client_id}:{number}"}))
        while True:
            response = json.loads(await asyncio.wait_for(ws.recv(), 4))
            if response["type"] == "ordered":
                return response
            assert response["type"] == "membership", response


async def send(member, request):
    client_id = request["client_id"]
    async with connect(f"{member['url']}/ws/client/{client_id}", proxy=None) as ws:
        await ws.send(json.dumps(request))
        reply = json.loads(await asyncio.wait_for(ws.recv(), 4))
    assert reply["type"] == "reply", reply
    assert reply["client_id"] == client_id
    assert reply["request_num"] == request["request_num"]
    assert reply["replica_id"] == member["replica_id"]
    assert reply["my_state"] == request["global_seq"]
    return reply


def run(mode):
    output = SERVER / "test-results" / f"milestone4-{mode}"
    output.mkdir(parents=True, exist_ok=True)
    rm_port, gfd_port, *ports = unused_ports(5)
    gfd_url = f"ws://127.0.0.1:{gfd_port}"
    processes = {}

    with ExitStack() as cleanup:
        def stop(process):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

        def start(name, script, *flags):
            stream = cleanup.enter_context((output / f"{name}.log").open("w", encoding="utf-8"))
            process = subprocess.Popen([sys.executable, str(script), *map(str, flags)],
                                       cwd=script.parent, stdout=stream,
                                       stderr=subprocess.STDOUT)
            cleanup.callback(stop, process)
            processes[name] = process

        def members(port=gfd_port):
            return {item["replica_id"]: item for item in health(port)["members"]}

        def joined(replica_id):
            return members().get(replica_id, {}).get("ready") is True

        def launch_replica(number, suffix=""):
            start(f"S{number}{suffix}", SERVER / "replica.py",
                  "--replica_id", f"S{number}", "--port", ports[number - 1],
                  "--mode", mode, "--gfd_url", gfd_url, "--checkpoint_interval", 0.25)

        def primary_is(replica_id):
            return health(gfd_port)["primary_id"] == replica_id

        async def request_round(number, expected):
            snapshot = health(gfd_port)
            targets = [item for item in snapshot["members"]
                       if item["ready"] and (mode == "active"
                                             or item["replica_id"] == snapshot["primary_id"])]
            assert {item["replica_id"] for item in targets} == set(expected)

            async def request(client_id):
                ordered = await order(gfd_url, client_id, number)
                replies = await asyncio.gather(*(send(target, ordered) for target in targets))
                assert len({reply["my_state"] for reply in replies}) == 1

            await asyncio.gather(*(request(f"C{number}") for number in range(1, 4)))

        start("RM", SERVER / "rm.py", "--port", rm_port)
        wait_for(lambda: health(rm_port)["member_count"] == 0, "RM")
        start("GFD", SERVER / "gfd.py", "--port", gfd_port, "--mode", mode,
              "--rm_url", f"ws://127.0.0.1:{rm_port}", "--heartbeat_freq", 0.15)
        wait_for(lambda: health(gfd_port)["member_count"] == 0, "GFD")

        for number, port in enumerate(ports, 1):
            url = f"ws://127.0.0.1:{port}"
            start(f"LFD{number}", ROOT / "LFD/lfd.py",
                  "--lfd_id", f"LFD{number}", "--replica_id", f"S{number}",
                  "--server_url", url, "--advertise_url", url, "--gfd_url", gfd_url,
                  "--heartbeat_freq", 0.15, "--timeout", 0.5)
        wait_for(lambda: len(health(gfd_port)["lfds"]) == 3, "LFD registrations")

        for number in range(1, 4):
            launch_replica(number)
            wait_for(lambda: joined(f"S{number}"), f"S{number} readiness")
        wait_for(lambda: len(members(rm_port)) == 3, "RM membership")
        expected = ("S1", "S2", "S3") if mode == "active" else ("S1",)
        asyncio.run(request_round(1, expected))
        original_boot = members()["S2"]["boot_id"]

        stop(processes["S2"])
        wait_for(lambda: "S2" not in members(), "S2 removal")
        wait_for(lambda: "S2" not in members(rm_port), "RM S2 removal")
        asyncio.run(request_round(2, ("S1", "S3") if mode == "active" else ("S1",)))
        launch_replica(2, "-restarted")
        wait_for(lambda: joined("S2"), "S2 recovery")
        assert members()["S2"]["boot_id"] != original_boot
        assert "Received checkpoint" in (output / "S2-restarted.log").read_text()
        asyncio.run(request_round(3, expected))
        wait_for(lambda: health(ports[1])["my_state"] == 9, "S2 state")

        if mode == "passive":
            ordered = asyncio.run(order(gfd_url, "PROBE", 1))
            old_reply = asyncio.run(send(members()["S1"], ordered))
            stop(processes["S1"])
            wait_for(lambda: primary_is("S2"), "S2 promotion")
            retry = asyncio.run(order(gfd_url, "PROBE", 1))
            new_reply = asyncio.run(send(members()["S2"], retry))
            assert retry == ordered and old_reply["my_state"] == new_reply["my_state"]
            asyncio.run(request_round(4, ("S2",)))
            wait_for(lambda: health(ports[2])["my_state"] == 13, "new primary checkpoints")
            launch_replica(1, "-restarted")
            wait_for(lambda: joined("S1") and health(ports[0])["my_state"] == 13,
                     "former primary recovery as backup")
            assert primary_is("S2")
        for path in output.glob("*.log"):
            assert "Traceback" not in path.read_text(encoding="utf-8"), path
    print(f"Milestone 4 {mode}: PASS", flush=True)


if __name__ == "__main__":
    run("active")
    run("passive")
