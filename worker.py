import asyncio
from asyncio import Queue, shield
from typing import Callable, Any, TypeVar, Tuple
from util import Result, unwrap, with_retry
from multiprocessing import get_context, Queue as MPQueue
from multiprocessing.process import BaseProcess
from multiprocessing.queues import SimpleQueue
from collections import deque
from async_util import SENTINEL

T = TypeVar("T")

def worker_no_buf(
    f: Callable[[Any], Result[T]], 
    retries: int,
    inbound: Queue[Any],
) -> Queue[Any]: 
    
    outbound: Queue = Queue(1)
    handler = with_retry(retries)(f)

    async def run() -> None:
        try:
            while True:
                val = await inbound.get()
                if val is SENTINEL:
                    break
                result = handler(val)
                if asyncio.iscoroutine(result):
                    result = await result
                await outbound.put(result)
        finally:
            await shield(outbound.put(SENTINEL))

    asyncio.create_task(run())
    return outbound

# async wrapper
def worker(
    f: Callable[[Any], Result[T]], 
    buf_size: int, 
    retries: int,
    inbound: Queue[Any],
) -> Queue[Any]:
    
    if buf_size <= 0:
        return worker_no_buf(f, retries, inbound)
    
    outbound: Queue = Queue(1)
    handler = with_retry(retries)(f)
    buff_in: Queue[Any] = Queue(max(1, buf_size))

    async def buff() -> None:
        try:
            while True:
                val = await inbound.get()
                if val is SENTINEL:
                    return
                await buff_in.put(val)
        finally: 
            await shield(buff_in.put(SENTINEL))

    async def run() -> None:
        try:
            while True:
                # pull
                val = await buff_in.get()
                
                # normal close
                if val is SENTINEL:
                    break
                
                result = handler(val)
                if asyncio.iscoroutine(result):
                    result = await result
                await outbound.put(unwrap(result))

        except Exception as e:
            print(e)    
        finally: 
            await shield(outbound.put(SENTINEL))

    asyncio.create_task(buff())
    asyncio.create_task(run())
    return outbound


def _mp_task(f: Callable, inbound: MPQueue, outbound: MPQueue, retries: int, buf_size: int) -> None:
    from queue import Empty
    handler = with_retry(retries)(f)

    ring: deque[Any] = deque()
    sentinel_seen = False

    try:
        while True:
            if buf_size > 0 and not sentinel_seen:
                try:
                    while len(ring) < buf_size and not sentinel_seen:
                        item = inbound.get_nowait()
                        if item == SENTINEL:
                            sentinel_seen = True
                            break
                        ring.append(item)
                except Empty:
                    pass
            
            if not ring and sentinel_seen:
                break

            # If nothing buffered, block for the next item
            if not ring:
                val = inbound.get()
                if val == SENTINEL:
                    break  # no buffered work; terminate
                ring.append(val)

            # Process one item
            val = ring.popleft()
            result = handler(val)
            # Note: mp_worker runs in separate process, can't await async functions
            # async functions should be wrapped or converted to sync before mp_worker
            outbound.put(unwrap(result))

    except Exception as e:
        print(e)    
    finally: 
        outbound.put(SENTINEL)

def mp_worker(
    f: Callable[[Any], Result[T]], 
    buf_size: int, 
    retries: int,
    inbound: MPQueue,
) -> Tuple[MPQueue, BaseProcess]:
    
    ctx = get_context("spawn")
    outbound: MPQueue = MPQueue(1)
    buf_size = max(0, buf_size)

    proc = ctx.Process(
        target=_mp_task, 
        args=(f, inbound, outbound, retries, buf_size), 
        daemon=True,
    )
    proc.start()
    return outbound, proc