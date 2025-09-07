import asyncio
from asyncio import Queue
from typing import Any, Callable, Tuple, Iterator
from util import Result
from worker import worker, SENTINEL
from pipeline import Pipeline
from stage import as_stage

@as_stage
def double(val: int) -> Result[int]:
    return val + val

@as_stage
def square(val: int) -> Result[int]:
    return val * val

if __name__ == "__main__":

    job = Pipeline().\
        gen(i for i in range(10)).\
        stage(double).\
        stage(square).\
        run()
    
    asyncio.run(job)