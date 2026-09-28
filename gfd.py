"""Global fault detector, membership publisher, and in-memory request sequencer."""

import argparse
import asyncio
import contextlib
import uuid
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from utils import log


class Sequence:
    def __init__(self):
        self.epoch = uuid.uuid4().hex
        self.requests = {}

    def allocate(self, client_id, request_num, payload):
        if type(request_num) is not int or request_num < 1 or not isinstance(payload, str):
            raise ValueError("request_num must be positive and payload must be text")
        key = (client_id, request_num)
        if key in self.requests:
            result = self.requests[key]
            if result["payload"] != payload:
                raise ValueError("request identity reused with different payload")
            return result
        result = {"type": "ordered", "epoch": self.epoch,
                  "global_seq": len(self.requests) + 1, "client_id": client_id,
                  "request_num": request_num, "payload": payload}
        self.requests[key] = result
        return result


def create_app(heartbeat_freq=2.0, timeout=3.0):
    app = FastAPI(title="Milestone 2 GFD")
    sequence = Sequence()
    members, known_boots, lfds, clients = {}, {}, {}, {}
    version = 0

    def membership():
        return {"type": "membership", "epoch": sequence.epoch, "version": version,
                "members": [members[r] for r in sorted(members)]}

    async def publish():
        log(f"GFD: {len(members)} members: {', '.join(sorted(members))}", "membership")
        snapshot = membership()
        for client_id, (_, queue) in list(clients.items()):
            queue.put_nowait(snapshot)
            log(f"GFD: Sending membership v{version} to {client_id}", "membership")

    async def update(replica_id, lfd_id, url, alive, boot_id):
        nonlocal version
        if alive:
            if not boot_id:
                raise ValueError("live replica must supply its boot_id")
            if sequence.requests and known_boots.get(replica_id) != boot_id:
                if replica_id in members:
                    del members[replica_id]
                    version += 1
                    await publish()
                log(f"GFD: Refusing new/restarted {replica_id} after traffic began; "
                    "state transfer is outside Milestone 2. Restart the demo.", "fault")
                return
            member = {"replica_id": replica_id, "lfd_id": lfd_id,
                      "url": url, "boot_id": boot_id}
            if members.get(replica_id) == member:
                return
            known_boots[replica_id] = boot_id
            members[replica_id] = member
        elif replica_id in members:
            del members[replica_id]
        else:
            return
        version += 1
        await publish()

    @app.get("/health")
    async def health():
        return {**membership(), "member_count": len(members),
                "last_sequence": len(sequence.requests), "lfds": sorted(lfds)}

    @app.websocket("/ws/lfd/{lfd_id}")
    async def lfd_endpoint(ws: WebSocket, lfd_id: str):
        await ws.accept()
        replica_id, heartbeat_task = None, None
        try:
            registration = await asyncio.wait_for(ws.receive_json(), timeout)
            replica_id = registration["replica_id"]
            url = registration["url"].rstrip("/")
            if urlparse(url).scheme not in ("ws", "wss") or not urlparse(url).hostname:
                raise ValueError("advertised replica URL must be ws://host:port or wss://host:port")
            if lfd_id in lfds or any(s["replica_id"] == replica_id for s in lfds.values()):
                raise ValueError("duplicate LFD or replica identity")
            session = {"ws": ws, "replica_id": replica_id, "pong": asyncio.Event(), "count": 0}
            lfds[lfd_id] = session
            log(f"GFD: Registered {lfd_id} monitoring {replica_id}", "info")

            async def heartbeat():
                try:
                    while True:
                        session["count"] += 1
                        session["pong"].clear()
                        log(f"GFD: [{session['count']}] Sending heartbeat to {lfd_id}", "heartbeat")
                        await asyncio.wait_for(ws.send_json({"type": "heartbeat",
                            "heartbeat_count": session["count"]}), timeout)
                        await asyncio.wait_for(session["pong"].wait(), timeout)
                        await asyncio.sleep(heartbeat_freq)
                except (TimeoutError, OSError, RuntimeError, WebSocketDisconnect):
                    log(f"GFD: FAULT DETECTED - {lfd_id} heartbeat failed", "fault")
                    with contextlib.suppress(Exception):
                        await ws.close(code=1011)

            heartbeat_task = asyncio.create_task(heartbeat())
            while True:
                msg = await ws.receive_json()
                if msg.get("type") == "heartbeat_reply" and msg.get("heartbeat_count") == session["count"]:
                    log(f"GFD: [{session['count']}] Received heartbeat from {lfd_id}", "heartbeat")
                    session["pong"].set()
                elif msg.get("type") == "status":
                    alive = msg.get("alive") is True
                    log(f"{lfd_id}: {'add' if alive else 'delete'} replica {replica_id}", "membership")
                    await update(replica_id, lfd_id, url, alive, msg.get("boot_id"))
        except (WebSocketDisconnect, TimeoutError, ValueError, KeyError, TypeError) as exc:
            log(f"GFD: {lfd_id} disconnected/rejected ({type(exc).__name__}: {exc})", "fault")
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat_task
            if lfd_id in lfds and lfds[lfd_id]["ws"] is ws:
                del lfds[lfd_id]
                await update(replica_id, lfd_id, "", False, None)
            with contextlib.suppress(Exception):
                await ws.close()

    @app.websocket("/ws/client/{client_id}")
    async def client_endpoint(ws: WebSocket, client_id: str):
        await ws.accept()
        if client_id in clients:
            await ws.close(code=1008, reason="client_id already connected")
            return
        queue = asyncio.Queue()
        clients[client_id] = (ws, queue)
        queue.put_nowait(membership())
        log(f"GFD: Sending initial membership to {client_id}", "membership")

        async def writer():
            try:
                while True:
                    await asyncio.wait_for(ws.send_json(await queue.get()), timeout)
            except (TimeoutError, OSError, RuntimeError, WebSocketDisconnect):
                with contextlib.suppress(Exception):
                    await ws.close(code=1011)

        task = asyncio.create_task(writer())
        try:
            while True:
                msg = await ws.receive_json()
                if msg.get("type") != "order":
                    raise ValueError("expected order request")
                if not members:
                    queue.put_nowait({"type": "unavailable", "request_num": msg.get("request_num")})
                    continue
                ordered = sequence.allocate(client_id, msg["request_num"], msg.get("payload", ""))
                log(f"GFD: Ordered <{client_id}, {ordered['request_num']}> "
                    f"global_seq={ordered['global_seq']}", "request")
                queue.put_nowait(ordered)
        except WebSocketDisconnect:
            pass
        except (ValueError, KeyError, TypeError) as exc:
            log(f"GFD: Invalid request from {client_id}: {exc}", "fault")
            await ws.close(code=1008)
        finally:
            clients.pop(client_id, None)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--heartbeat_freq", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=3)
    args = parser.parse_args()
    if args.heartbeat_freq <= 0 or args.timeout <= 0:
        parser.error("heartbeat_freq and timeout must be positive seconds")
    log("GFD: 0 members", "membership")
    uvicorn.run(create_app(args.heartbeat_freq, args.timeout), host=args.host,
                port=args.port, log_level="warning", timeout_graceful_shutdown=1)
