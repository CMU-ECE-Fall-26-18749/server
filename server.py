"""Stateful replica. Use --mode active with the Milestone 2 GFD/client."""

import argparse
import asyncio
import uuid

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from utils import log


class OrderedCounter:
    """Execute the global sequence once, including after a lost reply/retry."""

    def __init__(self):
        self.value = 0
        self.epoch = None
        self.records = {}
        self.identities = {}
        self.changed = asyncio.Condition()

    async def apply(self, message):
        seq = message.get("global_seq")
        epoch = message.get("epoch")
        if type(seq) is not int or seq < 1 or not isinstance(epoch, str) or not epoch:
            raise ValueError("active requests require epoch and positive global_seq")
        identity = (message["client_id"], message["request_num"])
        signature = (*identity, message.get("payload", ""))
        async with self.changed:
            if self.epoch is None:
                self.epoch = epoch
            if epoch != self.epoch:
                raise ValueError("GFD epoch changed; restart the entire demo")
            if identity in self.identities and self.identities[identity] != seq:
                raise ValueError("request identity reused with another sequence")
            await self.changed.wait_for(lambda: seq <= self.value + 1)
            if identity in self.identities and self.identities[identity] != seq:
                raise ValueError("request identity reused with another sequence")
            if seq in self.records:
                old_signature, result = self.records[seq]
                if signature != old_signature:
                    raise ValueError("conflicting request for existing sequence")
                return result, True
            self.value += 1
            self.records[seq] = (signature, self.value)
            self.identities[identity] = seq
            self.changed.notify_all()
            return self.value, False


def create_app(replica_id="S1", mode="standalone"):
    app = FastAPI(title=f"Replica {replica_id}")
    counter = OrderedCounter()
    boot_id = uuid.uuid4().hex

    @app.get("/health")
    async def health():
        return {"replica_id": replica_id, "boot_id": boot_id,
                "my_state": counter.value, "epoch": counter.epoch, "mode": mode}

    @app.websocket("/ws/client/{client_id}")
    async def client_endpoint(ws: WebSocket, client_id: str):
        await ws.accept()
        log(f"{replica_id}: Client {client_id} connected", "info")
        try:
            while True:
                msg = await ws.receive_json()
                if (msg.get("client_id") != client_id or
                        type(msg.get("request_num")) is not int or msg["request_num"] < 1):
                    raise ValueError("client_id must match connection and request_num must be positive")
                req = msg["request_num"]
                label = f"<{client_id}, {replica_id}, {req}, request>"
                log(f"{replica_id}: Received {label}", "request")
                before = counter.value
                if mode == "active":
                    result, duplicate = await counter.apply(msg)
                    before = counter.value if duplicate else result - 1
                else:
                    counter.value += 1
                    result, duplicate = counter.value, False
                log(f"{replica_id}: my_state = {before} before processing {label}", "state")
                log(f"{replica_id}: my_state = {counter.value} after processing {label}", "state")
                if duplicate:
                    log(f"{replica_id}: Replayed cached reply for {label}; state unchanged", "duplicate")
                reply = {"type": "reply", "client_id": client_id, "replica_id": replica_id,
                         "request_num": req, "my_state": result}
                if mode == "active":
                    reply.update(epoch=msg["epoch"], global_seq=msg["global_seq"])
                await ws.send_json(reply)
                log(f"{replica_id}: Sending <{client_id}, {replica_id}, {req}, reply>", "reply")
        except WebSocketDisconnect:
            log(f"{replica_id}: Client {client_id} disconnected", "info")
        except (ValueError, KeyError, TypeError) as exc:
            await ws.send_json({"type": "error", "error": str(exc)})
            await ws.close(code=1008)

    @app.websocket("/ws/heartbeat")
    async def heartbeat_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                msg = await ws.receive_json()
                count = msg.get("heartbeat_count")
                lfd_id = msg.get("lfd_id", "LFD")
                log(f"{replica_id}: [{count}] Received heartbeat from {lfd_id}", "heartbeat")
                await ws.send_json({"replica_id": replica_id, "status": "alive",
                                    "boot_id": boot_id, "heartbeat_count": count})
                log(f"{replica_id}: [{count}] Sent heartbeat reply to {lfd_id}", "heartbeat")
        except WebSocketDisconnect:
            log(f"{replica_id}: LFD disconnected", "fault")
    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replica_id", default="S1")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--mode", choices=["standalone", "active"], default="standalone")
    args = parser.parse_args()
    log(f"{args.replica_id}: Starting {args.mode} replica on {args.host}:{args.port}")
    uvicorn.run(create_app(args.replica_id, args.mode), host=args.host, port=args.port,
                log_level="warning", timeout_graceful_shutdown=1)
