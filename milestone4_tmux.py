"""Start an interactive four-machine Milestone 4 rehearsal in tmux."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import time
import urllib.request


ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / ".venv/bin/python"
SESSION = "brds-m4"
COORDINATOR = "127.0.0.1"
REPLICAS = {"S1": "127.0.0.2", "S2": "127.0.0.3", "S3": "127.0.0.4"}
SHELL = "env HISTFILE=/dev/null bash --noprofile --norc -i"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def tmux(*args, check=True):
    return subprocess.run(["tmux", *args], check=check, capture_output=True, text=True)


def health(address, port):
    with HTTP.open(f"http://{address}:{port}/health", timeout=1) as response:
        return json.load(response)


def wait_for(label, predicate):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            if predicate():
                print(f"Ready: {label}", flush=True)
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {label}")


def check_ports():
    for address, port in [(COORDINATOR, 9000), (COORDINATOR, 9001),
                          *((address, 8001) for address in REPLICAS.values())]:
        with socket.socket() as sock:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((address, port))
            except OSError as exc:
                raise RuntimeError(f"Cannot use {address}:{port}: {exc}") from exc


def launch(pane, title, script, *flags):
    tmux("select-pane", "-t", pane, "-T", title)
    command = shlex.join([str(PYTHON), script, *flags])
    tmux("send-keys", "-t", pane, "-l", command)
    tmux("send-keys", "-t", pane, "Enter")


def split(pane):
    return tmux("split-window", "-d", "-t", pane, "-c", str(ROOT),
                "-P", "-F", "#{pane_id}", SHELL).stdout.strip()


def start(mode):
    gfd_url = f"ws://{COORDINATOR}:9000"
    coordinator = tmux("new-session", "-d", "-s", SESSION, "-n", "Coordinator",
                       "-x", "160", "-y", "50", "-c", str(ROOT), "-P", "-F",
                       "#{pane_id}", SHELL).stdout.strip()
    tmux("set-option", "-t", SESSION, "pane-border-status", "top")
    tmux("set-option", "-t", SESSION, "pane-border-format", " #{pane_title} ")
    gfd = split(coordinator)
    clients = []
    for _ in range(3):
        clients.append(split(coordinator))
        tmux("select-layout", "-t", coordinator, "tiled")

    panes = {}
    for replica_id in REPLICAS:
        lfd = tmux("new-window", "-d", "-t", f"{SESSION}:", "-n", replica_id,
                   "-c", str(ROOT), "-P", "-F", "#{pane_id}", SHELL).stdout.strip()
        panes[replica_id] = lfd, split(lfd)

    launch(coordinator, "RM", "server/rm.py", "--host", COORDINATOR)
    wait_for("RM", lambda: health(COORDINATOR, 9001)["member_count"] == 0)
    launch(gfd, "GFD", "server/gfd.py", "--host", COORDINATOR, "--mode", mode,
           "--rm_url", f"ws://{COORDINATOR}:9001")
    wait_for("GFD registered with RM",
             lambda: health(COORDINATOR, 9001).get("mode") == mode)

    for replica_id, address in REPLICAS.items():
        lfd_id = "LFD" + replica_id[1:]
        url = f"ws://{address}:8001"
        launch(panes[replica_id][0], lfd_id, "LFD/lfd.py", "--lfd_id", lfd_id,
               "--replica_id", replica_id, "--server_url", url,
               "--advertise_url", url, "--gfd_url", gfd_url, "--heartbeat_freq", "1")
        wait_for(f"{lfd_id} registered",
                 lambda: lfd_id in health(COORDINATOR, 9000)["lfds"])

    for replica_id, address in REPLICAS.items():
        launch(panes[replica_id][1], replica_id, "server/replica.py",
               "--replica_id", replica_id, "--host", address,
               "--mode", mode, "--gfd_url", gfd_url)
        wait_for(replica_id, lambda: health(address, 8001)["i_am_ready"] == 1)
    if mode == "passive":
        wait_for("S1 primary", lambda: health(COORDINATOR, 9000)["primary_id"] == "S1")

    for number, pane in enumerate(clients, 1):
        client_id = f"C{number}"
        launch(pane, client_id, "client/client.py", "--client_id", client_id,
               "--gfd_url", gfd_url)
    tmux("select-window", "-t", coordinator)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["active", "passive"], nargs="?")
    parser.add_argument("--detach", action="store_true", help="Start without attaching")
    parser.add_argument("--stop", action="store_true", help="Stop this demo session")
    args = parser.parse_args()
    if not shutil.which("tmux"):
        parser.error("tmux is required")
    target = f"={SESSION}"
    exists = tmux("has-session", "-t", target, check=False).returncode == 0
    if args.stop:
        if exists:
            tmux("kill-session", "-t", target)
        return
    if not exists:
        if not args.mode:
            parser.error("Choose active or passive to start a demo")
        if not PYTHON.exists():
            parser.error("Run python3 server/demo.py setup first")
        try:
            check_ports()
            start(args.mode)
        except (RuntimeError, subprocess.CalledProcessError) as exc:
            detail = getattr(exc, "stderr", None) or str(exc)
            parser.exit(1, f"{detail.strip()}\nInspect any started panes with "
                        f"tmux attach -t {SESSION}.\n")
    elif args.mode:
        parser.error(f"{SESSION} already exists; omit the mode to attach or use --stop")
    if args.detach:
        print(f"Open the demo with tmux attach -t {SESSION}")
    elif "TMUX" in os.environ:
        tmux("switch-client", "-t", target)
    else:
        sys.exit(subprocess.run(["tmux", "attach-session", "-t", target]).returncode)


if __name__ == "__main__":
    main()
