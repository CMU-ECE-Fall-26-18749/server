"""Milestone 3 membership service and request sequencer."""

import argparse
import asyncio
import contextlib
import json
import uuid
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from utils import log


class Sequencer:
    """Give every client request one global sequence number."""

    def __init__(self):
        self.epoch = uuid.uuid4().hex
        self.requests = {}
        self.history = []

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
            self.history.append(self.requests[key])
        elif self.requests[key]["payload"] != payload:
            raise ValueError("request identity reused with different payload")
        return self.requests[key]


def create_app(heartbeat_interval=2.0, timeout=3.0, mode="passive",
               rm_url=None):
    sequencer = Sequencer()
    members = {}
    lfds = {}
    clients = {}
    version = 0
    candidate = None
    primary_id = None
    changed = asyncio.Event()

    @asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(coordinate()) if rm_url else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    app = FastAPI(title="Replica membership service", lifespan=lifespan)

    def membership():
        return {
            "type": "membership",
            "epoch": sequencer.epoch,
            "version": version,
            "members": [dict(members[name]) for name in sorted(members)],
            "primary_id": (primary_id if rm_url else
                           ("S1" if "S1" in members else None)),
            "mode": mode,
        }

    async def publish():
        nonlocal version
        version += 1
        changed.set()
        snapshot = membership()
        names = ", ".join(sorted(members))
        label = "member" if len(members) == 1 else "members"
        log(f"GFD: {len(members)} {label}: {names}", "membership")
        for client_id, queue in list(clients.items()):
            queue.put_nowait(snapshot)
            log(f"GFD: Sent membership v{version} to {client_id}", "membership")

    async def coordinate():
        nonlocal candidate
        while True:
            try:
                async with connect(rm_url.rstrip("/") + "/ws/gfd",
                                   open_timeout=timeout, proxy=None) as ws:
                    while True:
                        changed.clear()
                        snapshot = {**membership(), "candidate": candidate,
                                    "last_sequence": len(sequencer.history)}
                        await ws.send(json.dumps(snapshot))
                        reply = json.loads(await asyncio.wait_for(ws.recv(), timeout))
                        selected = reply["candidate"]
                        if (version == snapshot["version"] and mode == "passive"
                                and selected != candidate):
                            candidate = selected
                            await publish()
                        await changed.wait()
            except (OSError, TimeoutError, WebSocketException, ValueError, KeyError):
                await asyncio.sleep(0.5)

    async def set_member(replica_id, lfd_id, url, boot_id, alive):
        nonlocal candidate, primary_id
        if alive:
            previous = members.get(replica_id)
            new_member = {
                "replica_id": replica_id,
                "lfd_id": lfd_id,
                "url": url,
                "boot_id": boot_id,
            }
            if previous and all(previous[key] == value
                                for key, value in new_member.items()):
                return
            if rm_url:
                new_member["ready"] = not sequencer.history and (
                    mode == "active" or not any(m["ready"] for m in members.values())
                )
            members[replica_id] = new_member
            action = "add"
        elif replica_id in members:
            del members[replica_id]
            action = "delete"
        else:
            return
        if candidate == replica_id:
            candidate = None
        if primary_id == replica_id:
            primary_id = None
        log(f"{lfd_id}: {action} replica {replica_id}", "membership")
        await publish()

    @app.get("/health")
    async def health():
        return {
            **membership(),
            "member_count": len(members),
            "last_sequence": len(sequencer.requests),
            "lfds": sorted(lfds),
        }

    @app.websocket("/ws/replica/{replica_id}")
    async def replica_endpoint(ws: WebSocket, replica_id: str):
        nonlocal primary_id
        await ws.accept()
        try:
            while rm_url:
                message = await ws.receive_json()
                member = members.get(replica_id)
                registered = bool(member and member["boot_id"] == message["boot_id"])
                if registered:
                    cursor = message["cursor"]
                    if type(cursor) is not int or not 0 <= cursor <= len(sequencer.history):
                        raise ValueError("invalid replica cursor")
                    if (mode == "active" and not member["ready"]
                            and message.get("ready") and cursor == len(sequencer.history)):
                        member["ready"] = True
                        await publish()
                    elif mode == "passive":
                        if not member["ready"] and message.get("ready"):
                            member["ready"] = True
                            await publish()
                        if (candidate == replica_id and primary_id != replica_id
                                and message.get("primary_ready")
                                and cursor == len(sequencer.history)):
                            primary_id = replica_id
                            log(f"GFD: Primary is {replica_id}", "membership")
                            await publish()
                after = message.get("after")
                await ws.send_json({
                    **membership(),
                    "registered": registered,
                    "candidate": candidate,
                    "last_sequence": len(sequencer.history),
                    "requests": (sequencer.history[after:after + 256]
                                 if registered and type(after) is int and after >= 0 else []),
                })
        except (WebSocketDisconnect, ValueError, KeyError, TypeError):
            pass
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    @app.websocket("/ws/lfd/{lfd_id}")
    async def lfd_endpoint(ws: WebSocket, lfd_id: str):
        await ws.accept()
        replica_id = None
        session = None
        heartbeat_task = None
        try:
            registration = await asyncio.wait_for(ws.receive_json(), timeout)
            replica_id = registration["replica_id"]
            replica_url = registration["url"].rstrip("/")
            parsed = urlparse(replica_url)
            if parsed.scheme not in ("ws", "wss") or not parsed.hostname:
                raise ValueError("invalid replica URL")
            if lfd_id in lfds or any(item["replica_id"] == replica_id
                                     for item in lfds.values()):
                raise ValueError("duplicate LFD or replica identity")
            session = {"pong": asyncio.Event(), "count": 0, "replica_id": replica_id}
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
            if session is not None and lfds.get(lfd_id) is session:
                del lfds[lfd_id]
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
                snapshot = membership()
                available = (snapshot["primary_id"] if mode == "passive" else
                             any(m.get("ready") for m in members.values()))
                if not available:
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
    parser.add_argument("--mode", choices=["active", "passive"], default="passive")
    parser.add_argument("--rm_url")
    args = parser.parse_args()
    if args.mode == "active" and not args.rm_url:
        parser.error("active mode requires --rm_url")
    log("GFD: 0 members", "membership")
    uvicorn.run(
        create_app(args.heartbeat_freq, args.timeout, args.mode, args.rm_url),
        host=args.host,
        port=args.port,
        log_level="warning",
        timeout_graceful_shutdown=1,
    )
