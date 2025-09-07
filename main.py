import asyncio
from asyncio import Queue
from typing import Any, Callable, Tuple
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

def make_gen() -> Callable[[], Tuple[bool, Result]]:
    i = -1
    def run() -> Tuple[bool, Result]:
        nonlocal i
        if i >= 9:
            return False, 0
        i += 1
        return True, i
    return run


if __name__ == "__main__":
    
    job = Pipeline().\
        gen(make_gen()).\
        stage(double).\
        stage(square).\
        run()
    
    asyncio.run(job)