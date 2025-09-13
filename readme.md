# Pypline

Async iter tools for python

```python
@work_pool(buffer=1, retries=1, num_workers=1)
async def double(val: int) -> Result[int]:
    return val + val

@work_pool(buffer=1, retries=1, num_workers=1)
async def square(val: int) -> Result[int]:
    return val * val

@mix_pool(buffer=2, retries=2, merger=lambda x: x)
def dub_sqr() -> list[Callable]:
    return [
        lambda x : x + x,
        lambda x : x * x
    ]

job = Pipeline(i for i in range(10)).\
    stage(double).\
    stage(square).\
    run()

asyncio.run(job)
```

or

```python
p = Pipeline(i for i in range(10)) >> double >> square
asyncio.run(p.run())
```
