import asyncio
from util import Result, err_as_value
from pipeline import Pipeline
from stage import work_pool, mix_pool
from typing import Callable

@work_pool(buffer=1, retries=1, num_workers=1)
async def double(val: int) -> Result[int]:
    return val + val

@work_pool(buffer=1, retries=1, num_workers=1)
async def square(val: int) -> Result[int]:
    return val * val

@mix_pool(buffer=2, retries=2, merger=lambda x: x)
def fork_stage() -> list[Callable]:
    return [
        lambda x : x + x,
        lambda x : x * x
    ]

@err_as_value
def try_add(val: int) -> int:
    if val > 10:
        raise RuntimeError("Number too high")
    return val + 2

if __name__ == "__main__":

    job = Pipeline(i for i in range(10)).\
        stage(double).\
        stage(square).\
        run()
    
    asyncio.run(job)