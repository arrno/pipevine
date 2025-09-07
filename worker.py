import asyncio
from asyncio import Queue, shield
from typing import Callable, Any, TypeVar, Tuple
from util import Result, is_err, get_err, unwrap, with_retry
from multiprocessing import get_context, Queue as MPQueue
from multiprocessing.process import BaseProcess

SENTINEL = None
T = TypeVar("T")

# async wrapper
def worker(
    f: Callable[[Any], Result[T]], 
    buffer: int, 
    retries: int,
    inbound: Queue[Any],
) -> Queue[Any]:
    
    outbound: Queue = Queue(buffer)
    handler = with_retry(retries)(f)
    
    async def run():
        try:
            while True:
                # pull
                val = await inbound.get()
                
                # normal close
                if val is SENTINEL:
                    break
                
                result = handler(val)
                await outbound.put(unwrap(result))

        except Exception as e:
            print(e)    
        finally: 
            await shield(outbound.put(SENTINEL))

    asyncio.create_task(run())
    return outbound

def mp_worker(
    f: Callable[[Any], Result[T]], 
    buffer: int, 
    retries: int,
    inbound: MPQueue,
) -> Tuple[MPQueue, BaseProcess]:
    
    ctx = get_context("spawn")
    outbound: MPQueue = ctx.Queue(buffer)
    handler = with_retry(retries)(f)

    def run(inbound: MPQueue, outbound: MPQueue):
        try:
            while True:
                # pull
                val = inbound.get()
                
                # normal close
                if val is SENTINEL:
                    break
                
                result = handler(val)
                outbound.put(unwrap(result))

        except Exception as e:
            print(e)    
        finally: 
            outbound.put(SENTINEL)

    proc = ctx.Process(target=run, args=(inbound, outbound), daemon=True)
    proc.start()
    return outbound, proc