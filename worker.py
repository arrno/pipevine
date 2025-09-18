import asyncio
import pickle
from asyncio import Queue, shield
from typing import Any, TypeVar, Tuple, Sequence
from util import unwrap, with_retry, is_ok
import multiprocessing as mp
from multiprocessing import get_all_start_methods, Queue as MPQueue
from multiprocessing.process import BaseProcess
from worker_state import WorkerState, WorkerHandler
from collections import deque
from async_util import SENTINEL

T = TypeVar("T")

def worker_no_buf(
    f: WorkerHandler, 
    retries: int,
    inbound: Queue[Any],
) -> Queue[Any]: 
    
    outbound: Queue = Queue(1)
    handler = with_retry(retries)(f)
    state = WorkerState({})

    async def run() -> None:
        try:
            while True:
                val = await inbound.get()
                if val is SENTINEL:
                    break
                result = handler(val, state)
                if asyncio.iscoroutine(result):
                    result = await result
                if not is_ok(result):
                    continue
                await outbound.put(unwrap(result))
        finally:
            await shield(outbound.put(SENTINEL))

    asyncio.create_task(run())
    return outbound

# async wrapper
def worker(
    f: WorkerHandler, 
    buf_size: int, 
    retries: int,
    inbound: Queue[Any],
) -> Queue[Any]:
    
    if buf_size <= 0:
        return worker_no_buf(f, retries, inbound)
    
    outbound: Queue = Queue(1)
    handler = with_retry(retries)(f)
    buff_in: Queue[Any] = Queue(max(1, buf_size))
    state = WorkerState({})

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
                
                result = handler(val, state)
                if asyncio.iscoroutine(result):
                    result = await result
                if not is_ok(result):
                    continue
                await outbound.put(unwrap(result))

        except Exception as e:
            print(e)    
        finally: 
            await shield(outbound.put(SENTINEL))

    asyncio.create_task(buff())
    asyncio.create_task(run())
    return outbound


def _is_picklable(fn: WorkerHandler) -> bool:
    try:
        pickle.dumps(fn)
        return True
    except Exception:
        return False


def _choose_ctx_method(
    fns: Sequence[WorkerHandler],
    preferred: str | None = None,
) -> str:
    if preferred is not None:
        return preferred

    methods = get_all_start_methods()
    if "spawn" in methods and all(_is_picklable(fn) for fn in fns):
        return "spawn"
    if "fork" in methods:
        return "fork"
    return methods[0] if methods else "spawn"


def _mp_task(f: WorkerHandler, inbound: MPQueue, outbound: MPQueue, retries: int, buf_size: int) -> None:
    from queue import Empty
    handler = with_retry(retries)(f)

    ring: deque[Any] = deque()
    sentinel_seen = False
    state = WorkerState({})

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
            result = handler(val, state)
            # Note: mp_worker runs in separate process, can't await async functions
            # async functions should be wrapped or converted to sync before mp_worker
            if not is_ok(result):
                continue
            outbound.put(unwrap(result))

    except Exception as e:
        print(e)    
    finally: 
        outbound.put(SENTINEL)

def mp_worker(
    f: WorkerHandler,
    buf_size: int,
    retries: int,
    inbound: MPQueue,
    *,
    ctx_method: str | None = None,
) -> Tuple[MPQueue, BaseProcess]:

    method = _choose_ctx_method((f,), ctx_method)
    ctx = mp.get_context(method)
    outbound: MPQueue = ctx.Queue(1)
    buf_size = max(0, buf_size)

    process_factory = getattr(ctx, "Process")
    proc = process_factory(
        target=_mp_task,
        args=(f, inbound, outbound, retries, buf_size),
        daemon=True,
    )
    proc.start()
    return outbound, proc

def default_mp_ctx_method(functions: Sequence[WorkerHandler]) -> str:
    """Expose preferred ctx method so async bridges can share it."""
    return _choose_ctx_method(functions, None)
