import asyncio
from util import Result, err_as_value, Err
from pipeline import Pipeline
from stage import as_stage, stage, StageType

@stage(buffer=1, retries=1, stage_type=StageType.Pool)
async def double(val: int) -> Result[int]:
    return val + val

@stage(buffer=1, retries=1, stage_type=StageType.Pool)
async def square(val: int) -> Result[int]:
    return val * val

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