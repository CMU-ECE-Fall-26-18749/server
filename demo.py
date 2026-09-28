"""Run Milestone 2 components with short, cross-platform commands from any folder."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request


SERVER = Path(__file__).resolve().parent
ROOT = SERVER.parent
VENV_PYTHON = ROOT / ".venv" / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("component", choices=["setup", "test", "check", "gfd",
                                             "s1", "s2", "s3", "lfd1", "lfd2", "lfd3",
                                             "c1", "c2", "c3"])
    parser.add_argument("--gfd-host", default="127.0.0.1", help="Coordinator laptop IPv4 address")
    parser.add_argument("--replica-host", default="127.0.0.1", help="This replica laptop IPv4 address")
    parser.add_argument("--local", action="store_true", help="Use ports 8001/8002/8003 for one-laptop practice")
    parser.add_argument("--heartbeat", type=float, default=1, help="Heartbeat interval in seconds")
    parser.add_argument("--timeout", type=float, default=3)
    parser.add_argument("--interval", type=float, default=1, help="Client request interval in seconds")
    args = parser.parse_args()
    if args.heartbeat <= 0 or args.timeout <= 0 or args.interval < 0:
        parser.error("heartbeat/timeout must be positive; interval must be nonnegative")

    if args.component == "setup":
        if sys.version_info < (3, 10):
            parser.error("Install Python 3.10 or newer first")
        if not VENV_PYTHON.exists():
            subprocess.run([sys.executable, "-m", "venv", str(ROOT / ".venv")], check=True)
        subprocess.run([str(VENV_PYTHON), "-m", "pip", "install", "-r",
                        str(SERVER / "requirements.txt")], check=True)
        print("Ready. Keep server, client, and LFD folders next to each other.")
        return

    if args.component == "check":
        for name, host, port in [("GFD", args.gfd_host, 9000), ("Replica", args.replica_host, 8001)]:
            url = f"http://{host}:{port}/health"
            try:
                with urllib.request.urlopen(url, timeout=3) as response:
                    print(f"{name} reachable: {json.load(response)}")
            except (urllib.error.URLError, TimeoutError) as exc:
                print(f"{name} unreachable at {url}: {exc}")
                print("Check that the process is running, the IP is correct, and peer access is allowed.")
                sys.exit(1)
        return

    if not VENV_PYTHON.exists():
        parser.error("First run: python server/demo.py setup")
    if args.component == "test":
        subprocess.run([str(VENV_PYTHON), "-m", "unittest", "discover", "-s",
                        str(SERVER / "tests"), "-p", "test_*.py", "-v"], check=True)
        subprocess.run([str(VENV_PYTHON), str(SERVER / "tests/integration_demo.py")], check=True)
        return

    role = args.component
    port = 8000 + int(role[-1]) if args.local and role != "gfd" else 8001
    gfd_url = f"ws://{args.gfd_host}:9000"
    if role == "gfd":
        script = SERVER / "gfd.py"
        flags = ["--host", "0.0.0.0", "--heartbeat_freq", args.heartbeat, "--timeout", args.timeout]
    elif role.startswith("lfd"):
        number = role[-1]
        script = ROOT / "LFD/lfd.py"
        flags = ["--lfd_id", role.upper(), "--replica_id", "S" + number,
                 "--server_url", f"ws://127.0.0.1:{port}", "--gfd_url", gfd_url,
                 "--advertise_url", f"ws://{args.replica_host}:{port}",
                 "--heartbeat_freq", args.heartbeat, "--timeout", args.timeout]
    elif role.startswith("s"):
        script = SERVER / "server.py"
        flags = ["--replica_id", role.upper(), "--host", "0.0.0.0", "--port", port, "--mode", "active"]
    else:
        script = ROOT / "client/client.py"
        flags = ["--client_id", role.upper(), "--gfd_url", gfd_url, "--interval", args.interval]
    if not script.exists():
        parser.error(f"Missing {script}. Extract/clone all three repositories side by side.")
    command = [str(VENV_PYTHON), str(script), *map(str, flags)]
    print("Starting:", " ".join(command), flush=True)
    process = subprocess.Popen(command, cwd=script.parent)
    try:
        sys.exit(process.wait())
    except KeyboardInterrupt:
        # Ctrl+C already reaches the child in this terminal's process group.
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=3)


if __name__ == "__main__":
    main()
