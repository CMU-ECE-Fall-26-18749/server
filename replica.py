"""Run an active replica or a passive primary or backup."""

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


class Replica:
    def __init__(self, replica_id, mode, gfd_url, checkpoint_interval):
        self.replica_id = replica_id
        self.mode = mode
        self.gfd_url = gfd_url.rstrip("/")
        self.checkpoint_interval = checkpoint_interval
        self.boot_id = uuid.uuid4().hex
        self.epoch = None
        self.value = 0
        self.checkpoint_count = 0
        self.snapshot_received = False
        self.ready = False
        self.primary_id = None
        self.members = {}
        self.high_watermark_request_num = {}
        self.changed = asyncio.Condition()
        self.control_lock = asyncio.Lock()

    async def exchange(self, ws, after=None, ready=False, primary_ready=False):
        await ws.send(json.dumps({
            "boot_id": self.boot_id,
            "cursor": self.value,
            "after": after,
            "ready": ready,
            "primary_ready": primary_ready,
        }))
        return json.loads(await asyncio.wait_for(ws.recv(), 3))

    async def replay(self, ws, target):
        while self.value < target:
            snapshot = await self.exchange(ws, after=self.value)
            if not snapshot["registered"] or snapshot["epoch"] != self.epoch:
                raise ValueError("replica is no longer registered")
            if not snapshot["requests"]:
                raise ValueError("missing ordered requests")
            async with self.changed:
                for request in snapshot["requests"]:
                    sequence = request["global_seq"]
                    if sequence > target:
                        break
                    if sequence == self.value + 1:
                        self.value = sequence
                        log(f"{self.replica_id}: Replayed global_seq={sequence}", "state")
                self.changed.notify_all()

    async def update(self, snapshot):
        if self.epoch is not None and snapshot["epoch"] != self.epoch:
            raise ValueError("GFD changed; restart the demo")
        self.epoch = snapshot["epoch"]
        self.members = {member["replica_id"]: member for member in snapshot["members"]}
        async with self.changed:
            self.primary_id = snapshot["primary_id"]
            member = self.members.get(self.replica_id)
            ready = snapshot["registered"] and member["ready"]
            if ready and not self.ready:
                log(f"{self.replica_id}: i_am_ready=1 my_state={self.value}", "state")
            self.ready = ready
            self.changed.notify_all()

    def checkpoint(self):
        return {
            "type": "checkpoint",
            "epoch": self.epoch,
            "replica_id": self.replica_id,
            "boot_id": self.boot_id,
            "my_state": self.value,
            "checkpoint_count": self.checkpoint_count,
        }

    async def install(self, checkpoint):
        if (checkpoint["type"] != "checkpoint" or checkpoint["epoch"] != self.epoch
                or type(checkpoint["my_state"]) is not int or checkpoint["my_state"] < 0):
            raise ValueError("invalid checkpoint")
        async with self.changed:
            if checkpoint["my_state"] < self.value:
                return
            self.value = checkpoint["my_state"]
            self.checkpoint_count = max(self.checkpoint_count,
                                        checkpoint["checkpoint_count"])
            self.snapshot_received = True
            log(f"{self.replica_id}: Received checkpoint from "
                f"{checkpoint['replica_id']}: my_state={self.value} "
                f"checkpoint_count={self.checkpoint_count}", "state")
            self.changed.notify_all()

    async def recover(self):
        for donor in self.members.values():
            if donor["replica_id"] == self.replica_id or not donor["ready"]:
                continue
            try:
                async with connect(donor["url"] + "/ws/snapshot", proxy=None,
                                   open_timeout=1, close_timeout=1) as ws:
                    await ws.send(self.replica_id)
                    checkpoint = json.loads(await asyncio.wait_for(ws.recv(), 2))
                    if (checkpoint["replica_id"] == donor["replica_id"]
                            and checkpoint["boot_id"] == donor["boot_id"]):
                        await self.install(checkpoint)
                        return
            except (OSError, TimeoutError, WebSocketException, ValueError, KeyError):
                continue

    async def control(self):
        url = f"{self.gfd_url}/ws/replica/{self.replica_id}"
        while True:
            try:
                async with connect(url, proxy=None, open_timeout=2,
                                   close_timeout=1) as ws:
                    while True:
                        async with self.control_lock:
                            snapshot = await self.exchange(ws)
                            await self.update(snapshot)
                            if snapshot["registered"]:
                                if self.mode == "active" and not self.ready:
                                    if not self.snapshot_received:
                                        await self.recover()
                                    if self.snapshot_received:
                                        await self.replay(ws, snapshot["last_sequence"])
                                        await self.update(await self.exchange(ws, ready=True))
                                elif self.mode == "passive":
                                    if self.snapshot_received and not self.ready:
                                        await self.update(await self.exchange(ws, ready=True))
                                    if (snapshot["candidate"] == self.replica_id
                                            and self.primary_id != self.replica_id):
                                        await self.replay(ws, snapshot["last_sequence"])
                                        await self.update(await self.exchange(ws, primary_ready=True))
                        await asyncio.sleep(0.15)
            except (OSError, TimeoutError, WebSocketException, ValueError, KeyError):
                async with self.changed:
                    self.ready = False
                    self.primary_id = None
                    self.changed.notify_all()
                await asyncio.sleep(0.5)

    async def send_checkpoint(self, member, checkpoint):
        try:
            async with connect(member["url"] + "/ws/checkpoint", proxy=None,
                               open_timeout=1, close_timeout=1) as ws:
                await ws.send(json.dumps(checkpoint))
                reply = json.loads(await asyncio.wait_for(ws.recv(), 2))
                if reply["type"] == "checkpoint_ack":
                    log(f"{self.replica_id}: Sent checkpoint to {member['replica_id']}: "
                        f"my_state={checkpoint['my_state']} "
                        f"checkpoint_count={checkpoint['checkpoint_count']}", "state")
        except (OSError, TimeoutError, WebSocketException, ValueError, KeyError):
            pass

    async def checkpoints(self):
        while True:
            if self.ready and self.primary_id == self.replica_id:
                self.checkpoint_count += 1
                checkpoint = self.checkpoint()
                await asyncio.gather(*(self.send_checkpoint(member, checkpoint)
                                       for rid, member in self.members.items()
                                       if rid != self.replica_id))
            await asyncio.sleep(self.checkpoint_interval)

    def can_serve(self):
        return self.ready and (self.mode == "active" or self.primary_id == self.replica_id)

    async def apply(self, message):
        sequence = message["global_seq"]
        while True:
            async with self.changed:
                await self.changed.wait_for(self.can_serve)
                if message["epoch"] != self.epoch:
                    raise ValueError("request epoch does not match GFD")
                before = self.value
                if sequence <= before + 1:
                    if sequence > before:
                        self.value = sequence
                        self.changed.notify_all()
                    return before, sequence
            async with self.control_lock:
                if self.can_serve() and sequence > self.value + 1:
                    url = f"{self.gfd_url}/ws/replica/{self.replica_id}"
                    async with connect(url, proxy=None, open_timeout=2,
                                       close_timeout=1) as ws:
                        await self.replay(ws, sequence - 1)


def create_app(replica_id, mode, gfd_url, checkpoint_interval):
    replica = Replica(replica_id, mode, gfd_url, checkpoint_interval)

    @asynccontextmanager
    async def lifespan(app):
        log(f"{replica_id}: i_am_ready=0", "state")
        tasks = [asyncio.create_task(replica.control())]
        if mode == "passive":
            tasks.append(asyncio.create_task(replica.checkpoints()))
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(title=f"Replica {replica_id}", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {**replica.checkpoint(), "mode": mode,
                "primary_id": replica.primary_id, "i_am_ready": int(replica.ready),
                "high_watermark_request_num": replica.high_watermark_request_num}

    @app.websocket("/ws/snapshot")
    async def snapshot_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            recipient = await ws.receive_text()
            if mode == "active" and replica.ready:
                checkpoint = replica.checkpoint()
                await ws.send_json(checkpoint)
                log(f"{replica_id}: Sent checkpoint to {recipient}: "
                    f"my_state={checkpoint['my_state']}", "state")
        except WebSocketDisconnect:
            pass
        finally:
            await ws.close()

    @app.websocket("/ws/checkpoint")
    async def checkpoint_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            checkpoint = await ws.receive_json()
            primary = replica.members.get(replica.primary_id)
            if (mode != "passive" or not primary or replica.primary_id == replica_id
                    or primary["replica_id"] != checkpoint["replica_id"]
                    or primary["boot_id"] != checkpoint["boot_id"]):
                raise ValueError("checkpoint is not from the current primary")
            await replica.install(checkpoint)
            await ws.send_json({"type": "checkpoint_ack"})
        except WebSocketDisconnect:
            pass
        except (ValueError, KeyError, TypeError) as exc:
            await ws.send_json({"type": "error", "error": str(exc)})
        finally:
            with contextlib.suppress(Exception):
                await ws.close()

    @app.websocket("/ws/client/{client_id}")
    async def client_endpoint(ws: WebSocket, client_id: str):
        await ws.accept()
        queue = asyncio.Queue()

        async def receive_requests():
            while True:
                message = await ws.receive_json()
                number = message["request_num"]
                sequence = message["global_seq"]
                if (message["client_id"] != client_id or type(number) is not int
                        or number < 1 or type(sequence) is not int or sequence < 1):
                    raise ValueError("invalid request identity")
                watermarks = replica.high_watermark_request_num
                watermarks[client_id] = max(watermarks.get(client_id, 0), number)
                log(f"{replica_id}: Received <{client_id}, {replica_id}, {number}, request> "
                    f"i_am_ready={int(replica.can_serve())}", "request")
                queue.put_nowait(message)

        async def send_replies():
            while True:
                message = await queue.get()
                number = message["request_num"]
                before, result = await replica.apply(message)
                label = f"<{client_id}, {replica_id}, {number}, request>"
                log(f"{replica_id}: my_state={before} before {label}", "state")
                log(f"{replica_id}: my_state={max(before, result)} after {label}", "state")
                await ws.send_json({
                    "type": "reply",
                    "epoch": message["epoch"],
                    "global_seq": message["global_seq"],
                    "client_id": client_id,
                    "replica_id": replica_id,
                    "request_num": number,
                    "my_state": result,
                })
                log(f"{replica_id}: Sending <{client_id}, {replica_id}, "
                    f"{number}, reply>", "reply")

        tasks = [asyncio.create_task(receive_requests()), asyncio.create_task(send_replies())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except WebSocketDisconnect:
            pass
        except (OSError, TimeoutError, ValueError, KeyError, TypeError, WebSocketException):
            with contextlib.suppress(Exception):
                await ws.close(code=1011)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @app.websocket("/ws/heartbeat")
    async def heartbeat_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                message = await ws.receive_json()
                count = message.get("heartbeat_count")
                lfd_id = message.get("lfd_id", "LFD")
                log(f"{replica_id}: [{count}] Received heartbeat from {lfd_id}", "heartbeat")
                await ws.send_json({
                    "replica_id": replica_id,
                    "boot_id": replica.boot_id,
                    "status": "alive",
                    "heartbeat_count": count,
                })
                log(f"{replica_id}: [{count}] Sent heartbeat reply to {lfd_id}", "heartbeat")
        except WebSocketDisconnect:
            pass

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replica_id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--mode", choices=["active", "passive"], required=True)
    parser.add_argument("--gfd_url", required=True)
    parser.add_argument("--checkpoint_interval", type=float, default=2)
    args = parser.parse_args()
    if args.checkpoint_interval <= 0:
        parser.error("checkpoint interval must be positive")
    uvicorn.run(create_app(args.replica_id, args.mode, args.gfd_url,
                           args.checkpoint_interval),
                host=args.host, port=args.port, log_level="warning",
                timeout_graceful_shutdown=1)
