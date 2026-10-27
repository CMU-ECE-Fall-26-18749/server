"""Milestone 3 primary or backup server replica."""

import argparse
import asyncio
import contextlib
import json
import uuid
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from utils import log


class OrderedCounter:
    """Apply globally ordered requests once."""

    def __init__(self):
        self.value = 0
        self.epoch = None
        self.results = {}
        self.changed = asyncio.Condition()

    async def apply(self, message):
        sequence = message["global_seq"]
        async with self.changed:
            if self.epoch is None:
                self.epoch = message["epoch"]
            if message["epoch"] != self.epoch:
                raise ValueError("GFD changed; restart the demo")
            await self.changed.wait_for(lambda: sequence <= self.value + 1)
            if sequence in self.results:
                return self.results[sequence], True, self.value
            before = self.value
            self.value += 1
            self.results[sequence] = self.value
            self.changed.notify_all()
            return self.value, False, before


def create_app(replica_id="S1", role="backup", backup_urls=None,
               checkpoint_interval=2.0):
    backup_urls = backup_urls or []
    counter = OrderedCounter()
    boot_id = uuid.uuid4().hex
    checkpoint_count = 0

    async def checkpoint_worker(backup_id, url, queue):
        """Keep one checkpoint connection open to one backup."""
        while True:
            try:
                async with connect(url.rstrip("/") + "/ws/checkpoint", proxy=None,
                                   open_timeout=2, close_timeout=1) as ws:
                    log(f"{replica_id}: Checkpoint channel connected to "
                        f"{backup_id} at {url}", "info")
                    while True:
                        checkpoint = await queue.get()
                        await ws.send(json.dumps(checkpoint))
                        reply = json.loads(await asyncio.wait_for(ws.recv(), 3))
                        if reply.get("type") != "checkpoint_ack":
                            raise ValueError("backup rejected checkpoint")
                        log(
                            f"{replica_id}: Sent checkpoint to {backup_id}: "
                            f"my_state={checkpoint['my_state']} "
                            f"checkpoint_count={checkpoint['checkpoint_count']}",
                            "state",
                        )
            except (OSError, TimeoutError, ValueError, WebSocketException):
                await asyncio.sleep(0.5)

    async def make_checkpoints(queues):
        nonlocal checkpoint_count
        while True:
            await asyncio.sleep(checkpoint_interval)
            checkpoint = {
                "type": "checkpoint",
                "epoch": counter.epoch,
                "my_state": counter.value,
                "checkpoint_count": checkpoint_count,
            }
            for queue in queues:
                if queue.full():
                    queue.get_nowait()
                queue.put_nowait(checkpoint)
            checkpoint_count += 1

    @asynccontextmanager
    async def lifespan(app):
        tasks = []
        if role == "primary":
            queues = [asyncio.Queue(maxsize=1) for _ in backup_urls]
            tasks = [
                asyncio.create_task(checkpoint_worker(f"S{number}", url, queue))
                for number, (url, queue) in enumerate(
                    zip(backup_urls, queues), start=2
                )
            ]
            tasks.append(asyncio.create_task(make_checkpoints(queues)))
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(title=f"Milestone 3 {replica_id}", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {
            "replica_id": replica_id,
            "boot_id": boot_id,
            "role": role,
            "my_state": counter.value,
            "checkpoint_count": checkpoint_count,
        }

    @app.websocket("/ws/checkpoint")
    async def checkpoint_endpoint(ws: WebSocket):
        nonlocal checkpoint_count
        await ws.accept()
        try:
            while True:
                message = await ws.receive_json()
                if role != "backup":
                    raise ValueError("primary cannot receive checkpoints")
                counter.epoch = message.get("epoch")
                counter.value = message["my_state"]
                checkpoint_count_value = message["checkpoint_count"]
                checkpoint_count = checkpoint_count_value
                log(
                    f"{replica_id}: Received checkpoint: my_state={counter.value} "
                    f"checkpoint_count={checkpoint_count}",
                    "state",
                )
                await ws.send_json({"type": "checkpoint_ack"})
        except WebSocketDisconnect:
            pass
        except (ValueError, KeyError, TypeError) as exc:
            await ws.send_json({"type": "error", "error": str(exc)})

    @app.websocket("/ws/client/{client_id}")
    async def client_endpoint(ws: WebSocket, client_id: str):
        await ws.accept()
        log(f"{replica_id}: Client {client_id} connected ({role})", "info")
        try:
            while True:
                message = await ws.receive_json()
                if role != "primary":
                    raise ValueError("backup replicas do not process client requests")
                if (message.get("client_id") != client_id or
                        type(message.get("request_num")) is not int):
                    raise ValueError("invalid request identity")
                request_num = message["request_num"]
                label = f"<{client_id}, {replica_id}, {request_num}, request>"
                result, duplicate, before = await counter.apply(message)
                log(f"{replica_id}: Received {label}", "request")
                log(f"{replica_id}: my_state={before} before {label}", "state")
                log(f"{replica_id}: my_state={counter.value} after {label}", "state")
                if duplicate:
                    log(f"{replica_id}: Duplicate request; state unchanged", "duplicate")
                await ws.send_json({
                    "type": "reply",
                    "epoch": message["epoch"],
                    "global_seq": message["global_seq"],
                    "client_id": client_id,
                    "replica_id": replica_id,
                    "request_num": request_num,
                    "my_state": result,
                })
                log(f"{replica_id}: Sending <{client_id}, {replica_id}, "
                    f"{request_num}, reply>", "reply")
        except WebSocketDisconnect:
            pass
        except (ValueError, KeyError, TypeError) as exc:
            with contextlib.suppress(Exception):
                await ws.send_json({"type": "error", "error": str(exc)})
                await ws.close(code=1008)

    @app.websocket("/ws/heartbeat")
    async def heartbeat_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                message = await ws.receive_json()
                count = message.get("heartbeat_count")
                lfd_id = message.get("lfd_id", "LFD")
                log(f"{replica_id}: [{count}] Received heartbeat from {lfd_id}",
                    "heartbeat")
                await ws.send_json({
                    "replica_id": replica_id,
                    "boot_id": boot_id,
                    "status": "alive",
                    "heartbeat_count": count,
                })
                log(f"{replica_id}: [{count}] Sent heartbeat reply to {lfd_id}",
                    "heartbeat")
        except WebSocketDisconnect:
            pass

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replica_id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--role", choices=["primary", "backup"], required=True)
    parser.add_argument("--backup_url", action="append", default=[])
    parser.add_argument("--checkpoint_interval", type=float, default=2)
    args = parser.parse_args()
    if args.checkpoint_interval <= 0:
        parser.error("checkpoint interval must be positive")
    if args.role == "primary" and len(args.backup_url) != 2:
        parser.error("the primary requires exactly two --backup_url values")
    uvicorn.run(
        create_app(args.replica_id, args.role, args.backup_url,
                   args.checkpoint_interval),
        host=args.host,
        port=args.port,
        log_level="warning",
        timeout_graceful_shutdown=1,
    )
