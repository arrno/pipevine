from __future__ import annotations

import asyncio
import threading
from multiprocessing import get_context
import multiprocessing
from typing import Any, Optional, Callable
from enum import Enum, auto

class Marker(Enum):
    STOP = auto()
    
SENTINEL = Marker.STOP

# ---- MP -> Async bridge ----
def mp_to_async_queue(mpq: multiprocessing.Queue, *, loop: Optional[asyncio.AbstractEventLoop] = None) -> asyncio.Queue:
    """
    Returns an asyncio.Queue. A background thread forwards items from mpq into it.
    Stops when SENTINEL is seen (and forwards it).
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    aq: asyncio.Queue = asyncio.Queue()

    def _forward() -> None:
        try:
            while True:
                item = mpq.get()  # blocking in thread
                loop.call_soon_threadsafe(aq.put_nowait, item)
                if item == SENTINEL:
                    break
        except Exception as e:
            # You may want to log this
            loop.call_soon_threadsafe(aq.put_nowait, SENTINEL)

    t = threading.Thread(target=_forward, daemon=True)
    t.start()
    return aq


# ---- Async -> MP bridge ----
def async_to_mp_queue(aq: asyncio.Queue, *, ctx_method: str = "spawn") -> multiprocessing.Queue:
    """
    Returns a new MPQueue and starts an async task that forwards from aq -> mpq.
    Stops when SENTINEL is seen (and forwards it).
    
    NOTE: This function has a race condition - the returned MP queue may not
    immediately have data available. Caller should allow time for the async
    task to pump data, or better yet, use async_to_mp_queue_with_ready().
    """
    ctx = get_context(ctx_method)
    mpq = ctx.Queue()

    async def _pump() -> None:
        try:
            while True:
                item = await aq.get()
                mpq.put(item)  # blocking, but OK in event loop since put() is quick; if worried wrap in to_thread
                if item == SENTINEL:
                    break
        except Exception:
            mpq.put(SENTINEL)

    asyncio.create_task(_pump())
    return mpq

async def async_to_mp_queue_with_ready(aq: asyncio.Queue, *, ctx_method: str = "spawn") -> multiprocessing.Queue:
    """
    Returns a new MPQueue and starts an async task that forwards from aq -> mpq.
    Ensures the pump task has started before returning.
    """
    ctx = get_context(ctx_method)
    mpq = ctx.Queue()
    ready = asyncio.Event()

    async def _pump() -> None:
        ready.set()  # Signal that pump has started
        try:
            while True:
                item = await aq.get()
                mpq.put(item)
                if item == SENTINEL:
                    break
        except Exception:
            mpq.put(SENTINEL)

    asyncio.create_task(_pump())
    await ready.wait()  # Wait for pump to start
    return mpq

from typing import Any

async def _multiplex_async_queues_task(queues: list[asyncio.Queue]) -> asyncio.Queue:
    if not queues:
        return asyncio.Queue(maxsize=1)
    if len(queues) == 1:
        return queues[0]

    outbound: asyncio.Queue = asyncio.Queue(maxsize=1)

    async def forward(q: asyncio.Queue) -> None:
        try:
            while True:
                item = await q.get()
                if item is SENTINEL:
                    break
                await outbound.put(item)
        finally:
            # ensure this forwarder is "counted down" even on error/cancel
            pass

    async def supervisor() -> None:
        # Run one forwarder per queue
        async with asyncio.TaskGroup() as tg:
            for q in queues:
                tg.create_task(forward(q))
        # All forwarders done => close downstream
        await outbound.put(SENTINEL)

    asyncio.create_task(supervisor())
    return outbound

async def _multiplex_and_merge_async_queues_task(
    queues: list[asyncio.Queue],
    merge: Callable[[list[Any]], Any],
    *,
    outbound_maxsize: int = 1,
) -> asyncio.Queue:
    """
    Barrier-synchronize across all queues:
      - In each round, await exactly one item from each queue.
      - If ANY item is SENTINEL, stop (no merge emitted for that round),
        then drain the remaining queues up to their SENTINEL to avoid
        upstream backpressure / deadlocks.
      - Otherwise, merge(items) and put to outbound.

    Cohort integrity: items are taken in lockstep (one per queue per round),
    so cohorts aren't mixed across rounds.
    """
    if not queues:
        return asyncio.Queue(maxsize=1)

    outbound: asyncio.Queue = asyncio.Queue(maxsize=outbound_maxsize)

    async def drain_to_sentinel(q: asyncio.Queue) -> None:
        while True:
            x = await q.get()
            if x is SENTINEL:
                return

    async def supervisor() -> None:
        try:
            while True:
                # One "barrier" get per queue for this round
                get_tasks = [asyncio.create_task(q.get()) for q in queues]
                try:
                    items = await asyncio.gather(*get_tasks)
                except Exception:
                    # If the supervisor is being cancelled/aborting, close downstream.
                    for t in get_tasks:
                        if not t.done():
                            t.cancel()  # Safe: pending gets don't dequeue
                    raise

                # If any queue ended, we terminate the stream (no partial merge)
                if any(item is SENTINEL for item in items):
                    # Drain all *other* queues to their SENTINEL so producers can finish.
                    drains = []
                    for item, q in zip(items, queues):
                        if item is SENTINEL:
                            continue
                        drains.append(asyncio.create_task(drain_to_sentinel(q)))
                    if drains:
                        await asyncio.gather(*drains)
                    break

                # Normal path: merge the cohort and emit
                merged = merge(items)
                await outbound.put(merged)
        finally:
            await outbound.put(SENTINEL)

    asyncio.create_task(supervisor())
    return outbound

def multiplex_async_queues(queues: list[asyncio.Queue]) -> asyncio.Queue:
    """
    Synchronous wrapper that schedules the multiplexer and returns the outbound queue immediately.
    """
    outbound: asyncio.Queue = asyncio.Queue(maxsize=1)

    async def _runner() -> None:
        muxed = await _multiplex_async_queues_task(queues)
        # Pipe muxed -> outbound
        while True:
            item = await muxed.get()
            await outbound.put(item)
            if item is SENTINEL:
                break

    asyncio.create_task(_runner())
    return outbound

def multiplex_and_merge_async_queues(
    queues: list[asyncio.Queue],
    merge: Callable[[list[Any]], Any],
) -> asyncio.Queue:
    """
    Synchronous wrapper that schedules the multiplexer and returns the outbound queue immediately.
    """
    outbound: asyncio.Queue = asyncio.Queue(maxsize=1)

    async def _runner() -> None:
        muxed = await _multiplex_and_merge_async_queues_task(queues, merge)
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

    async def pump() -> None:
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

    async def pump() -> None:
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