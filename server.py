"""
Server Replica (S1) — 18-749 Milestone 1

Usage:
    python server.py --replica_id S1 --host 0.0.0.0 --port 8001

WebSocket endpoints:
    /ws/client/{client_id}  — for client requests/replies
    /ws/heartbeat           — for LFD heartbeating
"""

import argparse
import asyncio
import json

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from utils import log

parser = argparse.ArgumentParser(description="Server Replica")
parser.add_argument("--replica_id", type=str, default="S1")
parser.add_argument("--host", type=str, default="0.0.0.0")
parser.add_argument("--port", type=int, default=8001)
args = parser.parse_args()

REPLICA_ID = args.replica_id

my_state: int = 0

app = FastAPI(title=f"Replica {REPLICA_ID}")


@app.websocket("/ws/client/{client_id}")
async def client_endpoint(ws: WebSocket, client_id: str):
    """
    Handle a persistent WebSocket connection from a single client.
    Each incoming JSON message must contain:
        { "client_id": "C1", "request_num": 101, "payload": "..." }
    """
    global my_state
    await ws.accept()
    log(f"{REPLICA_ID}: Client {client_id} connected", "info")

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)

            c_id = msg["client_id"]
            req_num = msg["request_num"]
            payload = msg.get("payload", "")

            #Print receipt of request

            log(
                f"{REPLICA_ID}: Received <{c_id}, {REPLICA_ID}, {req_num}, request> "
                f"payload={payload}",
                "request",
            )

             #Print state BEFORE processing

            log(
                f"{REPLICA_ID}: my_state = {my_state} before processing "
                f"<{c_id}, {REPLICA_ID}, {req_num}, request>",
                "state",
            )

            #Process the request (update state)
            my_state += 1

            #Print state AFTER processing
            log(
                f"{REPLICA_ID}: my_state = {my_state} after processing "
                f"<{c_id}, {REPLICA_ID}, {req_num}, request>",
                "state",
            )

            #Send reply
            reply = {
                "client_id": c_id,
                "replica_id": REPLICA_ID,
                "request_num": req_num,
                "my_state": my_state,
            }
            await ws.send_text(json.dumps(reply))
            log(
                f"{REPLICA_ID}: Sending <{c_id}, {REPLICA_ID}, {req_num}, reply>",
                "reply",
            )

    except WebSocketDisconnect:
        log(f"{REPLICA_ID}: Client {client_id} disconnected", "info")


@app.websocket("/ws/heartbeat")
async def heartbeat_endpoint(ws: WebSocket):
    """
    Handle heartbeat pings from the Local Fault Detector.
    On each ping, reply with a pong carrying the replica_id.
    """
    await ws.accept()
    log(f"{REPLICA_ID}: LFD connected for heartbeating", "heartbeat")

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)

            log(
                f"{REPLICA_ID}: Received heartbeat from {msg.get('lfd_id', 'LFD')}",
                "heartbeat",
            )

            # Reply to heartbeat
            reply = {"replica_id": REPLICA_ID, "status": "alive"}
            await ws.send_text(json.dumps(reply))
            log(
                f"{REPLICA_ID}: Sent heartbeat reply to {msg.get('lfd_id', 'LFD')}",
                "heartbeat",
            )

    except WebSocketDisconnect:
        log(f"{REPLICA_ID}: LFD disconnected", "fault")


if __name__ == "__main__":
    log(f"{REPLICA_ID}: Starting on {args.host}:{args.port}", "info")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")