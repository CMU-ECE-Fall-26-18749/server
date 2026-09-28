"""Exercise the original single-server client/server mode over real sockets."""

import json
from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest
import urllib.request


class StandaloneTests(unittest.TestCase):
    def test_three_requests_increment_state(self):
        root = Path(__file__).resolve().parents[1]
        client = root.parent / "client/client.py"
        if not client.exists():
            self.skipTest("sibling client repository is required")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        proc = subprocess.Popen([sys.executable, str(root / "server.py"), "--port", str(port)],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        self.fail("standalone server did not start")
                    time.sleep(0.1)
            result = subprocess.run([sys.executable, str(client), "--client_id", "C1",
                                     "--server_url", f"ws://127.0.0.1:{port}",
                                     "--max_requests", "3", "--interval", "0"],
                                    capture_output=True, text=True, timeout=10,
                                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.count("Received <C1, S1,"), 3)
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as r:
                self.assertEqual(json.load(r)["my_state"], 3)
        finally:
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
