"""Start one Milestone 3 component with a short command."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.request


SERVER = Path(__file__).resolve().parent
ROOT = SERVER.parent
VENV_PYTHON = ROOT / ".venv" / (
    "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("component", choices=[
        "setup", "test", "check", "gfd",
        "lfd1", "lfd2", "lfd3", "s1", "s2", "s3", "c1", "c2", "c3",
    ])
    parser.add_argument("--gfd-host", default="127.0.0.1")
    parser.add_argument("--replica-host", default="127.0.0.1")
    parser.add_argument("--backup-host", action="append", default=[])
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--heartbeat", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=3)
    parser.add_argument("--checkpoint", type=float, default=2)
    parser.add_argument("--interval", type=float, default=1)
    args = parser.parse_args()

    if min(args.heartbeat, args.timeout, args.checkpoint) <= 0 or args.interval < 0:
        parser.error("timing values must be positive; interval may be zero")

    if args.component == "setup":
        if not VENV_PYTHON.exists():
            subprocess.run([sys.executable, "-m", "venv", str(ROOT / ".venv")],
                           check=True)
        subprocess.run([
            str(VENV_PYTHON), "-m", "pip", "install", "-r",
            str(SERVER / "requirements.txt"),
        ], check=True)
        print("Setup complete.")
        return

    if not VENV_PYTHON.exists():
        parser.error("Run: python3 server/demo.py setup")

    if args.component == "test":
        subprocess.run([
            str(VENV_PYTHON), str(SERVER / "tests" / "milestone3_demo.py"),
        ], check=True)
        return

    if args.component == "check":
        urls = [
            f"http://{args.gfd_host}:9000/health",
            f"http://{args.replica_host}:8001/health",
        ]
        for url in urls:
            try:
                with urllib.request.urlopen(url, timeout=3) as response:
                    print(url, json.load(response))
            except (urllib.error.URLError, TimeoutError) as exc:
                parser.error(f"Cannot reach {url}: {exc}")
        return

    role = args.component
    local_port = 8000 + int(role[-1]) if args.local and role != "gfd" else 8001
    gfd_url = f"ws://{args.gfd_host}:9000"

    if role == "gfd":
        script = SERVER / "gfd.py"
        flags = [
            "--host", "0.0.0.0",
            "--heartbeat_freq", args.heartbeat,
            "--timeout", args.timeout,
        ]
    elif role.startswith("lfd"):
        number = role[-1]
        script = ROOT / "LFD" / "lfd.py"
        flags = [
            "--lfd_id", f"LFD{number}",
            "--replica_id", f"S{number}",
            "--server_url", f"ws://127.0.0.1:{local_port}",
            "--advertise_url", f"ws://{args.replica_host}:{local_port}",
            "--gfd_url", gfd_url,
            "--heartbeat_freq", args.heartbeat,
            "--timeout", args.timeout,
        ]
    elif role.startswith("s"):
        number = role[-1]
        script = SERVER / "passive_server.py"
        flags = [
            "--replica_id", f"S{number}",
            "--host", "0.0.0.0",
            "--port", local_port,
            "--role", "primary" if role == "s1" else "backup",
            "--checkpoint_interval", args.checkpoint,
        ]
        if role == "s1":
            backup_hosts = (["127.0.0.1:8002", "127.0.0.1:8003"]
                            if args.local else [f"{host}:8001" for host in args.backup_host])
            if len(backup_hosts) != 2:
                parser.error("s1 needs two --backup-host values (S2_IP and S3_IP)")
            for host in backup_hosts:
                flags += ["--backup_url", f"ws://{host}"]
    else:
        script = ROOT / "client" / "client.py"
        flags = [
            "--client_id", role.upper(),
            "--gfd_url", gfd_url,
            "--interval", args.interval,
        ]

    command = [str(VENV_PYTHON), str(script), *map(str, flags)]
    print("Starting:", " ".join(command), flush=True)
    process = subprocess.Popen(command, cwd=script.parent)
    try:
        sys.exit(process.wait())
    except KeyboardInterrupt:
        process.terminate()
        process.wait(timeout=3)


if __name__ == "__main__":
    main()
