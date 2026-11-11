"""Launch the one-machine Milestone 2 demo in ten named tmux windows."""

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
SESSION = "brds-m2"


def tmux(*args, check=True):
    return subprocess.run(["tmux", *args], check=check, capture_output=True, text=True)


def exists():
    return tmux("has-session", "-t", f"={SESSION}", check=False).returncode == 0


def health():
    with urllib.request.urlopen("http://127.0.0.1:9000/health", timeout=1) as response:
        return json.load(response)


def wait_for(predicate, label):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            if predicate(health()):
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for {label}.")


def start(role, first=False):
    command = shlex.join([sys.executable, str(ROOT / "server/demo.py"), role,
                          *(["--local"] if role.startswith(("s", "lfd")) else [])])
    if first:
        tmux("new-session", "-d", "-s", SESSION, "-n", role.upper(),
             "-c", str(ROOT), command)
    else:
        tmux("new-window", "-d", "-t", f"{SESSION}:", "-n", role.upper(),
             "-c", str(ROOT), command)
    tmux("set-window-option", "-t", f"{SESSION}:{role.upper()}", "remain-on-exit", "on")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stop", action="store_true", help="Stop the brds-m2 session and its demo processes")
    parser.add_argument("--no-attach", action="store_true", help="Start the demo without entering tmux")
    args = parser.parse_args()
    if not shutil.which("tmux"):
        parser.error("tmux is required. On Ubuntu: sudo apt install tmux")
    if args.stop:
        if exists():
            tmux("kill-session", "-t", f"={SESSION}")
            print("Stopped brds-m2.")
        else:
            print("No brds-m2 session is running.")
        return
    if not exists():
        if not (ROOT / ".venv/bin/python").exists():
            parser.error("First run: python3 server/demo.py setup")
        for port in (9000, 8001, 8002, 8003):
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    parser.error(f"Port {port} is already in use. Stop the existing manual demo first.")
        try:
            start("gfd", first=True)
            wait_for(lambda h: h["member_count"] == 0, "GFD")
            for role in ("lfd1", "lfd2", "lfd3"):
                start(role)
            wait_for(lambda h: len(h["lfds"]) == 3, "three LFD registrations")
            for count, role in enumerate(("s1", "s2", "s3"), 1):
                start(role)
                wait_for(lambda h: h["member_count"] == count, role.upper())
            for role in ("c1", "c2", "c3"):
                start(role)
            tmux("select-window", "-t", f"{SESSION}:GFD")
            print("Started GFD, three LFDs, three replicas, and three clients.", flush=True)
        except Exception as exc:
            print(f"Launch failed: {exc}\nInspect: tmux attach -t {SESSION}\n"
                  "Stop: python3 server/local_demo.py --stop", file=sys.stderr)
            sys.exit(1)
    if args.no_attach:
        print(f"Open the demo: tmux attach -t {SESSION}")
    elif "TMUX" in os.environ:
        tmux("switch-client", "-t", f"={SESSION}")
    else:
        subprocess.run(["tmux", "attach-session", "-t", f"={SESSION}"], check=True)


if __name__ == "__main__":
    main()
