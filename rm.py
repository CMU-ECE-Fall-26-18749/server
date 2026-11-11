"""Coordinate membership and primary selection."""

import argparse

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from utils import log


def create_app():
    app = FastAPI(title="Replication Manager")
    membership = {"members": [], "primary_id": None, "candidate": None}

    @app.get("/health")
    async def health():
        return {**membership, "member_count": len(membership["members"])}

    @app.websocket("/ws/gfd")
    async def gfd_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                snapshot = await ws.receive_json()
                members = snapshot["members"]
                ready = sorted(member["replica_id"] for member in members
                               if member["ready"])
                candidate = snapshot["candidate"]
                if snapshot["mode"] == "passive" and candidate not in ready:
                    candidate = ready[0] if ready else None
                names = ", ".join(member["replica_id"] for member in members)
                label = "member" if len(members) == 1 else "members"
                log(f"RM: {len(members)} {label}: {names}", "membership")
                if candidate and candidate != membership["candidate"]:
                    log(f"RM: Selected {candidate} as primary", "membership")
                membership.update(snapshot, candidate=candidate)
                await ws.send_json({"candidate": candidate})
        except WebSocketDisconnect:
            pass

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9001)
    args = parser.parse_args()
    log("RM: 0 members", "membership")
    uvicorn.run(create_app(), host=args.host, port=args.port,
                log_level="warning", timeout_graceful_shutdown=1)
