import asyncio
from asyncio import Queue, shield
from typing import Callable, Any, TypeVar
from util import Result, is_err, get_err, unwrap

SENTINEL = None
T = TypeVar("T")

# async wrapper
def worker(f: Callable[[Any], Result[T]], inbound: Queue[Any]) -> Queue[Any]:
    outbound: Queue = Queue(1)

    async def run():
        try:
            while True:
                # pull
                val = await inbound.get()
                
                # normal close
                if val is SENTINEL:
                    break
                
                result = f(val)
                if is_err(result):
                    raise RuntimeError(get_err(result)) 
                await outbound.put(unwrap(result))

        except Exception as e:
            print(e)    
        finally: 
            await shield(outbound.put(SENTINEL))



    asyncio.create_task(run())
    return outbound