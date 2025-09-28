from __future__ import annotations

import types
import queue
import asyncio
import logging
import contextlib
from threading import Thread
from asyncio import Queue, shield, Task, QueueEmpty, QueueFull
from collections.abc import AsyncIterator as AsyncIteratorABC, Iterator as IteratorABC
from typing import Any, Iterator, AsyncIterator, Awaitable, Callable, cast

from .async_util import SENTINEL
from .stage import Stage
from .util import Err, Result, is_err, unwrap

logger = logging.getLogger(__name__)


class Pipeline:
    
    '''
    async pipeline
    '''

    def __init__(self, gen: Iterator[Any] | AsyncIterator[Any] | Pipeline, log: bool = False) -> None:
        self.log = log
        self.generator: Iterator[Any] | AsyncIterator[Any] | None = None
        self.stages: list[Stage] = []
        self.result: Result = "ok"
        self.cancel_event = asyncio.Event()
        self.tasks: set[Task[Any]] = set()
        self._gen_stream: Queue | None = None
        self._parent: Pipeline | None = None
        
        if isinstance(gen, Pipeline):
            self._parent = gen
            self.gen(gen.iter())
            return
        
        self.gen(gen)


    def gen(self, gen: Iterator[Any] | AsyncIterator[Any]) -> Pipeline:
        self.generator = gen
        self.cancel_event.clear()
        self.result = "ok"
        return self
    
    def stage(self, st: Stage | Pipeline) -> Pipeline:
        st.log = self.log
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
        self._gen_stream = outbound

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

        task = asyncio.create_task(run())
        self.tasks.add(task)

        def _discard(_: Task[Any]) -> None:
            self.tasks.discard(task)

        task.add_done_callback(_discard)
        return outbound
    
    def __handle_log(self, val: Any) -> None:
        if self.log:
            logger.info(val)
    
    def __handle_err(self, err: str) -> None:
        self.result = Err(err)

    async def __drain(self, q: Queue[Any]) -> None:
        while True:
            val = await q.get()
            if val is SENTINEL:
                break
            self.__handle_log(val)
            
    async def run(self) -> Result:
        if self.cancel_event.is_set():
            return self.result
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

    async def cancel(self, reason: str | None = None) -> Result:
        if self.cancel_event.is_set():
            return self.result

        if reason is None:
            reason = "pipeline cancelled"

        self.cancel_event.set()
        self.result = Err(reason)
        self.__handle_log(reason)

        if self._parent is not None:
            await self._parent.cancel(reason)

        gen = self.generator
        if gen is not None:
            aclose = getattr(gen, "aclose", None)
            if callable(aclose):
                aclose_typed = cast(Callable[[], Awaitable[None]], aclose)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(asyncio.shield(aclose_typed()), 1.0)
            else:
                close = getattr(gen, "close", None)
                if callable(close):
                    with contextlib.suppress(Exception):
                        close()

        tasks = list(self.tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self.stages:
            await asyncio.gather(
                *(stage.close() for stage in self.stages),
                return_exceptions=True,
            )

        # generator task canceled, drain self gen_stream until empty
        # then send final sentinel for drain
        if self._gen_stream is not None:
            while True:
                try:
                    self._gen_stream.get_nowait()
                except QueueEmpty:
                    break
                except Exception:
                    break
            with contextlib.suppress(QueueFull, Exception):
                self._gen_stream.put_nowait(SENTINEL)
            self._gen_stream = None

        return self.result
