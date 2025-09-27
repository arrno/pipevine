from __future__ import annotations

import queue
import asyncio
from threading import Thread
from asyncio import Queue, shield
from collections.abc import AsyncIterator as AsyncIteratorABC, Iterator as IteratorABC
from typing import Any, Iterator, AsyncIterator

from .async_util import SENTINEL
from .stage import Stage
from .util import Err, Result, is_err, unwrap

class Pipeline:
    
    '''
    async pipeline
    '''

    def __init__(self, gen: Iterator[Any] | AsyncIterator[Any] | Pipeline, log: bool = False) -> None:
        self.log = log
        self.generator: Iterator[Any] | AsyncIterator[Any] | None = None
        self.stages: list[Stage] = []
        self.result: Result = "ok"
        
        if isinstance(gen, Pipeline):
            self.gen(gen.iter())
            return
        
        self.gen(gen)

    def gen(self, gen: Iterator[Any] | AsyncIterator[Any]) -> Pipeline:
        self.generator = gen
        return self
    
    def stage(self, st: Stage | Pipeline) -> Pipeline:
        if isinstance(st, Pipeline):
            st.gen(self.iter())
            return st
        if len(st.functions) > 0:
            self.stages.append(st)
        return self
    
    def __rshift__(self, other: Stage | Pipeline) -> Pipeline:
        return self.stage(other)

    def __generate(self, gen: Iterator[Any] | AsyncIterator[Any]) -> asyncio.Queue[Any]:
        outbound: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)

        async def run() -> None:
            try:
                if isinstance(gen, AsyncIteratorABC):
                    async for result in gen:
                        if is_err(result):
                            self.__handle_err(str(result))
                            self.__handle_log(result)
                            return                        # sentinel sent in finally
                        await outbound.put(unwrap(result))
                elif isinstance(gen, IteratorABC):
                    for result in gen:
                        if is_err(result):
                            self.__handle_err(str(result))
                            self.__handle_log(result)
                            return                        # sentinel sent in finally
                        await outbound.put(unwrap(result))
                else:
                    raise TypeError("Pipeline source must be Iterator or AsyncIterator")
            except asyncio.CancelledError:
                # allow task cancellation to propagate; finally still runs
                raise
            except Exception as e:
                # real error from iterator or unwrap()
                self.__handle_err(str(e))
                self.__handle_log(e)
            finally:
                # guarantee exactly-once termination signal
                try:
                    await shield(outbound.put(SENTINEL))
                except Exception:
                    pass

        asyncio.create_task(run())
        return outbound
    
    def __handle_log(self, val: Any) -> None:
        if self.log:
            print(val)
    
    def __handle_err(self, err: str) -> None:
        self.result = Err(err)

    async def __drain(self, q: Queue[Any]) -> None:
        while True:
            val = await q.get()
            if val is SENTINEL:
                break
            self.__handle_log(val)
            
    async def run(self) -> Result:

        if not self.generator:
            err = Err("no generator")
            self.__handle_err(err.message)
            self.__handle_log(err.message)
            return err
        
        stream = self.__generate(self.generator)
        for stage in self.stages:
            stream = stage.run(stream)
        await self.__drain(stream)
        return self.result
    
    async def as_async_generator(self) -> AsyncIterator:
        if not self.generator:
            raise RuntimeError("Pipeline.as_async_generator needs a self.generator")

        stream = self.__generate(self.generator)
        for stage in self.stages:
            stream = stage.run(stream)

        while True:
            val = await stream.get()
            yield val
            if val is SENTINEL:
                break

    def iter(self, max_buffer: int = 64) -> Iterator[Any]:
        q: queue.Queue = queue.Queue(maxsize=max_buffer)
        done = object()  # end-of-iteration marker distinct from SENTINEL
        exception_holder: list[BaseException] = []

        async def _pump() -> None:
            try:
                async for item in self.as_async_generator():
                    # Push each item; if back-pressured, this awaits on the sync side.
                    if item is SENTINEL:
                        break
                    q.put(item)
            except BaseException as e:  # capture and deliver exceptions to sync side
                exception_holder.append(e)
            finally:
                q.put(done)

        def _runner() -> None:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(_pump())
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

        t = Thread(target=_runner, daemon=True)
        t.start()

        while True:
            item = q.get()
            # Propagate async exceptions on the sync side
            if exception_holder:
                raise exception_holder[0]
            if item is done:
                return
            yield item 
