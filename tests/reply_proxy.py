"""Test-only proxy: lose each client's first reply, then delay its retry reply."""

import argparse
import asyncio
import contextlib

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


async def run(port, target):
    dropped = set()

    async def connection(downstream):
        path = downstream.request.path
        tasks = []
        try:
            async with connect(target + path, proxy=None) as upstream:
                async def requests():
                    async for message in downstream:
                        await upstream.send(message)

                async def replies():
                    first = True
                    async for message in upstream:
                        if path not in dropped:
                            dropped.add(path)
                            await downstream.close(code=1011, reason="test: lose first reply")
                            return
                        if first:
                            await asyncio.sleep(0.6)
                            first = False
                        await downstream.send(message)

                tasks = [asyncio.create_task(requests()), asyncio.create_task(replies())]
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
        except ConnectionClosed:
            pass
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async with serve(connection, "127.0.0.1", port):
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    asyncio.run(run(args.port, args.target))
