from __future__ import annotations

import time
import queue
import asyncio
import logging
import contextlib
from threading import Thread
from dataclasses import dataclass, field, replace
from asyncio import Queue, shield, Task, QueueEmpty, QueueFull
from collections.abc import AsyncIterator as AsyncIteratorABC, Iterator as IteratorABC
from typing import Any, Iterator, AsyncIterator, Awaitable, Callable, cast

from .async_util import SENTINEL
from .stage import Stage, StageMetrics
from .util import Err, Result, is_err, unwrap, aiter_from_iter

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    start: float = 0
    stop: float = 0
    duration: float = 0
    processed: int = 0
    failed: int = 0
    stages: list[StageMetrics] = field(default_factory=list)


class Pipeline:
    
    '''
    async pipeline
    '''

    def __init__(self, gen: Iterator[Any] | AsyncIterator[Any] | Pipeline, log: bool = False) -> None:
        self.log = log
        self.generator: AsyncIterator[Any] | None = None
        self.stages: list[Stage] = []
        self._metrics = PipelineMetrics()
        self._error: Err | None = None
        self.cancel_event = asyncio.Event()
        self.tasks: set[Task[Any]] = set()
        self._gen_stream: Queue | None = None
        self._parent: Pipeline | None = None
        self._log_emit = False
        
        if isinstance(gen, Pipeline):
            self._parent = gen
            self.gen(gen.as_async_generator())
            return
        
        self.gen(gen)


    def gen(self, gen: Iterator[Any] | AsyncIterator[Any]) -> Pipeline:
        if isinstance(gen, Iterator):
            self.generator = aiter_from_iter(gen)
        else:
            self.generator = gen
        self.cancel_event.clear()
        self._error = None
        return self
    
    def stage(self, st: Stage | Pipeline) -> Pipeline:
        st.log = self.log
        if isinstance(st, Pipeline):
            st.gen(self.as_async_generator())
            return st
        st._on_kill_switch = self.cancel
        if len(st.functions) > 0:
            self.stages.append(st)
        return self
    
    def __rshift__(self, other: Stage | Pipeline) -> Pipeline:
        return self.stage(other)

    def _reset_metrics(self) -> None:
        self._metrics = PipelineMetrics()
        self._error = None

    @contextlib.asynccontextmanager
    async def _metrics_scope(self) -> AsyncIterator[None]:
        self._reset_metrics()
        try:
            yield
        finally:
            self._calculate_metrics()

    def __generate(self, gen: AsyncIterator[Any]) -> asyncio.Queue[Any]:
        outbound: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)
        self._gen_stream = outbound

        async def run() -> None:
            try:
                if isinstance(gen, AsyncIteratorABC):
                    async for item in gen:
                        await outbound.put(item)
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
        
        self._metrics.start = time.time()
        
        task = asyncio.create_task(run())
        self.tasks.add(task)

        def _discard(_: Task[Any]) -> None:
            self.tasks.discard(task)

        task.add_done_callback(_discard)
        return outbound
    
    def __handle_log(self, val: Any) -> None:
        if self.log:
            logger.info(val)
    
    def __handle_err(self, err: str | Err) -> None:
        if isinstance(err, Err):
            self._error = err
        else:
            self._error = Err(err)

    async def __drain(self, q: Queue[Any]) -> None:
        while True:
            val = await q.get()
            if val is SENTINEL:
                break
            if self._log_emit:
                logger.info(val)
            
    async def run(self) -> Result:
        if self.cancel_event.is_set():
            return self.result
        if not self.generator:
            err = Err("no generator")
            self.__handle_err(err)
            self.__handle_log(err.message)
            return err

        async with self._metrics_scope():
            stream = self.__generate(self.generator)
            for stage in self.stages:
                stream = stage.run(stream)
            await self.__drain(stream)

        return self.result
    
    async def as_async_generator(self) -> AsyncIterator:
        if not self.generator:
            raise RuntimeError("Pipeline.as_async_generator needs a self.generator")

        async with self._metrics_scope():
            stream = self.__generate(self.generator)
            for stage in self.stages:
                stream = stage.run(stream)

            while True:
                val = await stream.get()
                yield val
                if val is SENTINEL:
                    break
    
    def async_iter(self) -> AsyncIterator[Any]:
        return self.as_async_generator()
    
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

    def _calculate_metrics(self) -> None:
        stage_metrics: list[StageMetrics] = []
        total_failed = 0

        for stage in self.stages:
            metrics = stage.metrics
            stage_metrics.append(replace(metrics))
            total_failed += metrics.failed

        if stage_metrics:
            self._metrics.processed = stage_metrics[-1].processed

        self._metrics.failed = total_failed
        self._metrics.stages = stage_metrics
        stop_time = time.time()
        self._metrics.stop = stop_time
        if self._metrics.start:
            self._metrics.duration = max(0.0, stop_time - self._metrics.start)
        else:
            self._metrics.duration = 0.0

    @property
    def result(self) -> Result:
        return self._error if self._error is not None else self._metrics

    @property
    def metrics(self) -> PipelineMetrics:
        return self._metrics

    async def cancel(self, reason: str | None = None) -> Result:
        if self.cancel_event.is_set():
            return self.result

        if reason is None:
            reason = "pipeline cancelled"

        self.cancel_event.set()
        self.__handle_err(reason)
        self.__handle_log(reason)

        # 1) Close our own generator ASAP to stop consuming parent.
        gen = self.generator
        if self._parent is not None:
            await self._parent.cancel(reason)
        elif gen is not None:
            aclose = getattr(gen, "aclose", None)
            if callable(aclose):
                aclose_typed = cast(Callable[[], Awaitable[None]], aclose)
                with contextlib.suppress(Exception):
                    # Keep this short & shielded; we just want to signal closure.
                    await asyncio.wait_for(asyncio.shield(aclose_typed()), 1.0)
            else:
                close = getattr(gen, "close", None)
                if callable(close):
                    with contextlib.suppress(Exception):
                        close()

        # 2) Unblock any producer/consumer by draining our own stream
        if self._gen_stream is not None:
            try:
                while True:
                    try:
                        self._gen_stream.get_nowait()
                    except QueueEmpty:
                        break
                    except Exception:
                        break
                with contextlib.suppress(QueueFull, Exception):
                    self._gen_stream.put_nowait(SENTINEL)
            finally:
                self._gen_stream = None

        # 3) Now cancel our tasks
        tasks = list(self.tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 4) Close stages
        if self.stages:
            await asyncio.gather(
                *(stage.close() for stage in self.stages),
                return_exceptions=True,
            )

        return self.result

