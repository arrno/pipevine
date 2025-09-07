import asyncio
from asyncio import Queue
from typing import Callable, Any, TypeVar
from util import Result, is_err, unwrap

SENTINEL = None
T = TypeVar("T")

# async wrapper
def worker(f: Callable[[Any], Result[T]], inbound: Queue[Any]) -> Queue[Any]:
    outbound: Queue = Queue(1)

    async def run():
        while True:
            # pull
            val = await inbound.get()
            
            # normal close
            if val is SENTINEL:
                await outbound.put(SENTINEL)
                return
            
            try:
                result = f(val)
            except Exception as e:
                print(e)
                await outbound.put(SENTINEL)
                return    
            
            if is_err(result):
                print(result)
                await outbound.put(SENTINEL)
                return           
            
            await outbound.put(unwrap(result))

    asyncio.create_task(run())
    return outbound