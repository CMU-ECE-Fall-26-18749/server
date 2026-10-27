"""Milestone 3 membership service and request sequencer."""

import argparse
import asyncio
import contextlib
import uuid
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from utils import log


class Sequencer:
    """Give every client request one global sequence number."""

    def __init__(self):
        self.epoch = uuid.uuid4().hex
        self.requests = {}

    def order(self, client_id, request_num, payload):
        key = (client_id, request_num)
        if key not in self.requests:
            self.requests[key] = {
                "type": "ordered",
                "epoch": self.epoch,
                "global_seq": len(self.requests) + 1,
                "client_id": client_id,
                "request_num": request_num,
                "payload": payload,
            }
        return self.requests[key]


def create_app(heartbeat_interval=2.0, timeout=3.0):
    app = FastAPI(title="Milestone 3 GFD")
    sequencer = Sequencer()
    members = {}
    lfds = {}
    clients = {}
    version = 0

    def membership():
        return {
            "type": "membership",
            "epoch": sequencer.epoch,
            "version": version,
            "members": [members[name] for name in sorted(members)],
            "primary_id": "S1" if "S1" in members else None,
        }

    async def publish():
        snapshot = membership()
        names = ", ".join(sorted(members))
        label = "member" if len(members) == 1 else "members"
        log(f"GFD: {len(members)} {label}: {names}", "membership")
        for client_id, queue in list(clients.items()):
            queue.put_nowait(snapshot)
            log(f"GFD: Sent membership v{version} to {client_id}", "membership")

    async def set_member(replica_id, lfd_id, url, boot_id, alive):
        nonlocal version
        if alive:
            new_member = {
                "replica_id": replica_id,
                "lfd_id": lfd_id,
                "url": url,
                "boot_id": boot_id,
            }
            if members.get(replica_id) == new_member:
                return
            members[replica_id] = new_member
            action = "add"
        elif replica_id in members:
            del members[replica_id]
            action = "delete"
        else:
            return
        log(f"{lfd_id}: {action} replica {replica_id}", "membership")
        version += 1
        await publish()

    @app.get("/health")
    async def health():
        return {
            **membership(),
            "member_count": len(members),
            "last_sequence": len(sequencer.requests),
            "lfds": sorted(lfds),
        }

    @app.websocket("/ws/lfd/{lfd_id}")
    async def lfd_endpoint(ws: WebSocket, lfd_id: str):
        await ws.accept()
        replica_id = None
        heartbeat_task = None
        try:
            registration = await asyncio.wait_for(ws.receive_json(), timeout)
            replica_id = registration["replica_id"]
            replica_url = registration["url"].rstrip("/")
            parsed = urlparse(replica_url)
            if parsed.scheme not in ("ws", "wss") or not parsed.hostname:
                raise ValueError("invalid replica URL")
            session = {"pong": asyncio.Event(), "count": 0}
            lfds[lfd_id] = session
            log(f"GFD: Registered {lfd_id} for {replica_id}", "info")

            async def heartbeat():
                try:
                    while True:
                        session["count"] += 1
                        session["pong"].clear()
                        count = session["count"]
                        log(f"GFD: [{count}] Sending heartbeat to {lfd_id}",
                            "heartbeat")
                        await asyncio.wait_for(ws.send_json({
                            "type": "heartbeat",
                            "heartbeat_count": count,
                        }), timeout)
                        await asyncio.wait_for(session["pong"].wait(), timeout)
                        await asyncio.sleep(heartbeat_interval)
                except (TimeoutError, OSError, RuntimeError):
                    log(f"GFD: FAULT DETECTED - {lfd_id}", "fault")
                    with contextlib.suppress(Exception):
                        await ws.close(code=1011)

            heartbeat_task = asyncio.create_task(heartbeat())
            while True:
                message = await ws.receive_json()
                if (message.get("type") == "heartbeat_reply" and
                        message.get("heartbeat_count") == session["count"]):
                    log(f"GFD: [{session['count']}] Received heartbeat from "
                        f"{lfd_id}", "heartbeat")
                    session["pong"].set()
                elif message.get("type") == "status":
                    alive = message.get("alive") is True
                    await set_member(
                        replica_id,
                        lfd_id,
                        replica_url,
                        message.get("boot_id"),
                        alive,
                    )
        except (WebSocketDisconnect, TimeoutError, ValueError, KeyError, OSError):
            log(f"GFD: {lfd_id} disconnected", "fault")
        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task
            lfds.pop(lfd_id, None)
            if replica_id:
                await set_member(replica_id, lfd_id, "", None, False)
            with contextlib.suppress(Exception):
                await ws.close()

    @app.websocket("/ws/client/{client_id}")
    async def client_endpoint(ws: WebSocket, client_id: str):
        await ws.accept()
        if client_id in clients:
            await ws.close(code=1008, reason="client ID already connected")
            return
        queue = asyncio.Queue()
        clients[client_id] = queue
        queue.put_nowait(membership())

        async def send_messages():
            while True:
                await ws.send_json(await queue.get())

        sender = asyncio.create_task(send_messages())
        try:
            while True:
                message = await ws.receive_json()
                if message.get("type") != "order":
                    raise ValueError("expected an order request")
                if "S1" not in members:
                    queue.put_nowait({
                        "type": "unavailable",
                        "request_num": message.get("request_num"),
                    })
                    continue
                ordered = sequencer.order(
                    client_id,
                    message["request_num"],
                    message.get("payload", ""),
                )
                log(f"GFD: Ordered <{client_id}, {message['request_num']}> "
                    f"global_seq={ordered['global_seq']}", "request")
                queue.put_nowait(ordered)
        except WebSocketDisconnect:
            pass
        except (ValueError, KeyError, TypeError):
            await ws.close(code=1008)
        finally:
            clients.pop(client_id, None)
            sender.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sender

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--heartbeat_freq", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=3)
    args = parser.parse_args()
    log("GFD: 0 members", "membership")
    uvicorn.run(
        create_app(args.heartbeat_freq, args.timeout),
        host=args.host,
        port=args.port,
        log_level="warning",
        timeout_graceful_shutdown=1,
    )
