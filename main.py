import asyncio
from asyncio import Queue
from typing import Callable, Any, TypeVar
from util import Result, is_err, unwrap

# handlers
def double(val: int) -> Result[int]:
    return val + val

def square(val: int) -> Result[int]:
    return val * val

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

def generator() -> Queue[Any]:
    q: Queue = Queue(1)
    
    async def run():
        for i in range(10):
            await q.put(i)
        await q.put(SENTINEL)

    asyncio.create_task(run())
    return q

async def drain(q: Queue[Any]) -> None:
    while True:
        val = await q.get()
        if val is SENTINEL:
            break
        print(val)

# boot
async def boot():
    inbound = generator()
    queue_one = worker(double, inbound)
    queue_two = worker(square, queue_one)
    await drain(queue_two)

if __name__ == "__main__":
    asyncio.run(boot())