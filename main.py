import asyncio
from util import Result
from pipeline import Pipeline
from stage import as_stage

@as_stage
def double(val: int) -> Result[int]:
    return val + val

@as_stage
def square(val: int) -> Result[int]:
    return val * val

if __name__ == "__main__":

    job = Pipeline(i for i in range(10)).\
        stage(double).\
        stage(square).\
        run()
    
    asyncio.run(job)