from __future__ import annotations

import asyncio
import threading
from multiprocessing import get_context
from typing import Any, Optional

SENTINEL = object()

# ---- MP -> Async bridge ----
def mp_to_async_queue(mpq, *, loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Queue:
    """
    Returns an asyncio.Queue. A background thread forwards items from mpq into it.
    Stops when SENTINEL is seen (and forwards it).
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    aq: asyncio.Queue = asyncio.Queue(maxsize=1)

    def _forward():
        try:
            while True:
                item = mpq.get()  # blocking in thread
                loop.call_soon_threadsafe(aq.put_nowait, item)
                if item is SENTINEL:
                    break
        except Exception as e:
            # You may want to log this
            loop.call_soon_threadsafe(aq.put_nowait, SENTINEL)

    t = threading.Thread(target=_forward, daemon=True)
    t.start()
    return aq


# ---- Async -> MP bridge ----
def async_to_mp_queue(aq: asyncio.Queue, *, ctx_method: str = "spawn"):
    """
    Returns a new MPQueue and starts an async task that forwards from aq -> mpq.
    Stops when SENTINEL is seen (and forwards it).
    """
    ctx = get_context(ctx_method)
    mpq = ctx.Queue(maxsize=1)

    async def _pump():
        try:
            while True:
                item = await aq.get()
                mpq.put(item)  # blocking, but OK in event loop since put() is quick; if worried wrap in to_thread
                if item is SENTINEL:
                    break
        except Exception:
            mpq.put(SENTINEL)

    asyncio.create_task(_pump())
    return mpq

from typing import Any

async def _multiplex_async_queues_task(queues: list[asyncio.Queue]) -> asyncio.Queue:
    if not queues:
        return asyncio.Queue(maxsize=1)
    if len(queues) == 1:
        return queues[0]

    outbound: asyncio.Queue = asyncio.Queue(maxsize=1)

    async def forward(q: asyncio.Queue):
        try:
            while True:
                item = await q.get()
                if item is SENTINEL:
                    break
                await outbound.put(item)
        finally:
            # ensure this forwarder is "counted down" even on error/cancel
            pass

    async def supervisor():
        # Run one forwarder per queue
        async with asyncio.TaskGroup() as tg:
            for q in queues:
                tg.create_task(forward(q))
        # All forwarders done => close downstream
        await outbound.put(SENTINEL)

    asyncio.create_task(supervisor())
    return outbound


def multiplex_async_queues(queues: list[asyncio.Queue]) -> asyncio.Queue:
    """
    Synchronous wrapper that schedules the multiplexer and returns the outbound queue immediately.
    """
    outbound: asyncio.Queue = asyncio.Queue(maxsize=1)

    async def _runner():
        muxed = await _multiplex_async_queues_task(queues)
        # Pipe muxed -> outbound
        while True:
            item = await muxed.get()
            await outbound.put(item)
            if item is SENTINEL:
                break

    asyncio.create_task(_runner())
    return outbound


async def make_shared_inbound_for_pool(
    upstream: asyncio.Queue,
    *,
    n_workers: int,
    maxsize: int = 1,
) -> asyncio.Queue:
    """
    Drain 'upstream' into a new shared inbound queue that multiple workers read from.
    When upstream closes (first SENTINEL), emit N SENTINELs so every worker exits.
    """
    shared: asyncio.Queue = asyncio.Queue(maxsize=maxsize)

    async def pump():
        try:
            while True:
                item = await upstream.get()
                if item is SENTINEL:
                    break
                await shared.put(item)
        finally:
            for _ in range(n_workers):
                await shared.put(SENTINEL)

    asyncio.create_task(pump())
    return shared

async def make_broadcast_inbounds(
    upstream: asyncio.Queue,
    *,
    sizes: list[int],
) -> list[asyncio.Queue]:
    """
    Create N per-worker queues. Every upstream item is put onto all N queues.
    On close, send exactly one SENTINEL to each queue.
    """
    n = len(sizes)
    outs: list[asyncio.Queue] = [asyncio.Queue(maxsize=sizes[i]) for i in range(n)]

    async def pump():
        try:
            while True:
                item = await upstream.get()
                if item is SENTINEL:
                    break
                await asyncio.gather(*(q.put(item) for q in outs))
        finally:
            await asyncio.gather(*(q.put(SENTINEL) for q in outs))

    asyncio.create_task(pump())
    return outs