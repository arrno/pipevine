from __future__ import annotations

import time
import asyncio
import contextlib
import multiprocessing as mp
from enum import Enum, auto
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from multiprocessing.sharedctypes import Synchronized
from asyncio import Queue, Task, QueueFull, QueueEmpty
from typing import Any, Callable, Optional, TypeAlias, TypeVar, Awaitable

from .async_util import (
    SENTINEL,
    async_to_mp_queue_with_ready,
    make_broadcast_inbounds,
    make_shared_inbound_for_pool,
    mp_to_async_queue,
    multiplex_and_merge_async_queues,
    multiplex_async_queues,
)
from .util import Err
from .worker import default_mp_ctx_method, mp_worker, worker
from .worker_state import WorkerHandler


@dataclass
class KillSwitch():
    reason: str


T = TypeVar("T")
Result: TypeAlias = T | Err


StageFunc: TypeAlias = Callable[[Any], Any]


class PathChoice(Enum):
    One = auto() # item takes on func path in stage
    All = auto() # item is emitted on every func path in stage


@dataclass
class StageMetrics:
    start: float = 0
    stop: float = 0
    duration: float = 0
    processed: int = 0
    failed: int = 0


def _split_buffer_across(n: int, total: int) -> list[int]:
    if n <= 0: return []
    base = max(1, total // n)
    rem = max(0, total - base * n)
    sizes = [base + (1 if i < rem else 0) for i in range(n)]
    return sizes


async def dummy(x: str | None) -> None:
    return None

class Stage:
    def __init__(
            self,
            buffer: int,
            retries: int,
            multi_proc: bool,
            functions: list[WorkerHandler],
            merge: Optional[Callable[[list[Any]], Any]] = None,
            choose: PathChoice = PathChoice.One,
            log: bool = False,
        ):
        self.buffer = buffer
        self.retries = retries
        self.multi_proc = multi_proc
        self.functions = functions
        self.merge = merge
        self._choose = choose
        self.log = log
        self._tasks: set[Task[Any]] = set()
        self._processes: set[BaseProcess] = set()
        self._outbound: Queue | None = None
        self._inbound: Queue | None = None
        self._metrics = StageMetrics()
        self._on_kill_switch: Callable[[str | None], Awaitable[Any]] = dummy
        self._tally_len = False

    def _register_task(self, task: Task[Any]) -> None:
        self._tasks.add(task)
        def _cleanup(_: Task[Any]) -> None:
            self._tasks.discard(task)
        task.add_done_callback(_cleanup)

    def _count(self, item: Any) -> None:
        if self._tally_len and hasattr(item, "__len__"):
            self._metrics.processed += len(item)
        else:
            self._metrics.processed += 1
    
    def _count_err(self) -> None:
        self._metrics.failed += 1

    @property
    def metrics(self) -> StageMetrics:
        return self._metrics

    def run(self, inbound: Queue) -> Queue:
        """
        Public contract: accept and return Queue.
        """
        # return queue now; supervise in background
        outbound: Queue = Queue(maxsize=self.buffer)
        self._inbound = inbound
        self._outbound = outbound

        mp_err_counter: Synchronized[int] | None = None

        async def _run_async() -> None:
            nonlocal mp_err_counter
            merge = self.merge if self.merge != None else lambda x: x

            try:
                if not self.multi_proc:
                    # ---- ASYNC workers ----
                    if self._choose is PathChoice.One:
                        shared_in = await make_shared_inbound_for_pool(
                            inbound,
                            n_workers=len(self.functions),
                            maxsize=self.buffer,
                            register_task=self._register_task,
                        )
                        outqs = [
                            worker(
                                fn,
                                1,
                                self.retries,
                                shared_in,
                                self.log,
                                register_task=self._register_task,
                                on_err=self._count_err,
                            )
                            for fn in self.functions
                        ]

                        muxed = multiplex_async_queues(outqs, register_task=self._register_task)
                    else:  # PathType.All
                        sizes = _split_buffer_across(len(self.functions), self.buffer)
                        per_ins = await make_broadcast_inbounds(
                            inbound,
                            sizes=sizes,
                            register_task=self._register_task,
                        )
                        outqs = [
                            worker(
                                fn,
                                1,
                                self.retries,
                                q_in,
                                self.log,
                                register_task=self._register_task,
                                on_err=self._count_err,
                            )
                            for fn, q_in in zip(self.functions, per_ins)
                        ]

                        muxed = multiplex_and_merge_async_queues(
                            outqs,
                            merge,
                            register_task=self._register_task,
                        )
                else:
                    outqs_async: list[Queue] = []
                    ctx_method = default_mp_ctx_method(self.functions)
                    ctx = mp.get_context(ctx_method)
                    mp_err_counter = ctx.Value("i", 0)
                    # ---- MP workers ----
                    if self._choose is PathChoice.One:
                        shared_in = await make_shared_inbound_for_pool(
                            inbound,
                            n_workers=len(self.functions),
                            maxsize=self.buffer,
                            register_task=self._register_task,
                        )
                        mp_in = await async_to_mp_queue_with_ready(
                            shared_in,
                            ctx_method=ctx_method,
                            sentinel_count=len(self.functions),
                            register_task=self._register_task,
                        )

                        for fn in self.functions:
                            mp_out, proc = mp_worker(
                                fn,
                                1,
                                self.retries,
                                mp_in,
                                ctx_method=ctx_method,
                                log=self.log,
                                err_counter=mp_err_counter,
                            )
                            self._processes.add(proc)
                            outqs_async.append(mp_to_async_queue(mp_out))

                        muxed = multiplex_async_queues(outqs_async, register_task=self._register_task)

                    else:  # PathType.All
                        sizes = _split_buffer_across(len(self.functions), self.buffer)
                        per_ins = await make_broadcast_inbounds(
                            inbound,
                            sizes=sizes,
                            register_task=self._register_task,
                        )

                        for fn, q_in in zip(self.functions, per_ins):
                            mp_in = await async_to_mp_queue_with_ready(
                                q_in,
                                ctx_method=ctx_method,
                                register_task=self._register_task,
                            )
                            mp_out, proc = mp_worker(
                                fn,
                                1,
                                self.retries,
                                mp_in,
                                ctx_method=ctx_method,
                                log=self.log,
                                err_counter=mp_err_counter,
                            )
                            self._processes.add(proc)
                            outqs_async.append(mp_to_async_queue(mp_out))

                        muxed = multiplex_and_merge_async_queues(
                            outqs_async,
                            merge,
                            register_task=self._register_task,
                        )

                # pipe muxed -> outbound
                while True:
                    item = await muxed.get()
                    if isinstance(item, KillSwitch):

                        async def do_cancel() -> Any:
                            return await self._on_kill_switch(item.reason)
                        cancel_task: Task = asyncio.create_task(do_cancel())

                        def _finalize(task: Task[Any]) -> None:
                            with contextlib.suppress(Exception):
                                task.result()

                        cancel_task.add_done_callback(_finalize)
                        break
                    
                    await outbound.put(item)
                    if item is SENTINEL:
                        break
                    self._count(item)
            finally:
                if mp_err_counter is not None:
                    self._metrics.failed += mp_err_counter.value
                self._metrics.stop = time.time()
                self._metrics.duration = max(0.0, self._metrics.stop - self._metrics.start)
        
        self._metrics = StageMetrics()
        self._metrics.start = time.time()

        task = asyncio.create_task(_run_async())
        self._register_task(task)
        return outbound

    async def _cancel_internals(self) -> None:
        targets = set(self._tasks)

        for tracked in targets:
            if not tracked.done():
                tracked.cancel()

        if targets:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*targets, return_exceptions=True),
                    timeout=0.5,
                )
        self._tasks.clear()

        for proc in list(self._processes):
            try:
                proc.join(timeout=0.1)
            except Exception:
                pass
            if proc.is_alive():
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    proc.join(timeout=0.1)
                except Exception:
                    pass
        self._processes.clear()

    async def close(self) -> bool:
        if self._inbound is None:
            return False
        
        await self._cancel_internals()

        if self._outbound is not None:
            while True:
                try:
                    self._outbound.get_nowait()
                except QueueEmpty:
                    break
                except Exception:
                    break
            with contextlib.suppress(QueueFull, Exception):
                self._outbound.put_nowait(SENTINEL)

        return True
    
def work_pool(
    *,
    buffer: int = 1,
    retries: int = 1,
    num_workers: int = 1,
    multi_proc: bool = False,
    fork_merge: Callable[[list[Any]], Any] | None = None
) -> Callable[[WorkerHandler], Stage]:
    """
    Decorator to create stages with configurable options.
    
    Usage:
    @work_pool()  # defaults
    @work_pool(buffer=10, retries=3)  # with options
    @work_pool(stage_type=StageType.Fork, merge=lambda results: sum(results))
    """
    def decorator(f: WorkerHandler) -> Stage:
        return Stage(
            buffer, 
            retries, 
            multi_proc, 
            [f for _ in range(num_workers)], 
            fork_merge,
            PathChoice.All if fork_merge else PathChoice.One,
        )
    
    return decorator

def mix_pool(
    *,
    buffer: int = 1,
    retries: int = 1,
    multi_proc: bool = False,
    fork_merge: Callable[[list[Any]], Any] | None = None
) -> Callable[[Callable[[], list[WorkerHandler]]], Stage]:
    def decorator(fs: Callable[[], list[Callable]]) -> Stage:
        return Stage(
            buffer, 
            retries, 
            multi_proc, 
            fs(), 
            fork_merge,
            PathChoice.All if fork_merge else PathChoice.One,
        )
    
    return decorator

# Keep as_stage for backwards compatibility, but always with defaults
def as_stage(func: WorkerHandler | Stage) -> Stage:
    """Simple stage decorator with defaults."""
    if isinstance(func, Stage):
        return func
    return Stage(1, 1, False, [func], None)
