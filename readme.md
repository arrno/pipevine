# Pypline

Async iter tools for python

```python
@as_stage
def double(val: int) -> Result[int]:
    return val + val

@as_stage
def square(val: int) -> Result[int]:
    return val * val

def generator() -> Queue[Any]:
    q: Queue = Queue(1)

    async def run():
        for i in range(10):
            await q.put(i)
        await q.put(SENTINEL)

    asyncio.create_task(run())
    return q

def make_gen() -> Callable[[], Tuple[bool, Result]]:
    i = -1
    def run() -> Tuple[bool, Result]:
        nonlocal i
        if i >= 9:
            return False, 0
        i += 1
        return True, i
    return run

async def pipe():
    await Pipeline().\
        gen(make_gen()).\
        stage(double).\
        stage(square).\
        run()

if __name__ == "__main__":
    asyncio.run(pipe())
```
