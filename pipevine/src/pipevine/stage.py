from __future__ import annotations

import asyncio
from asyncio import Queue
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeAlias, TypeVar

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

T = TypeVar("T")
Result: TypeAlias = T | Err

StageFunc: TypeAlias = Callable[[Any], Any]

class PathChoice(Enum):
    One = auto() # item takes on func path in stage
    All = auto() # item is emitted on every func path in stage

def _split_buffer_across(n: int, total: int) -> list[int]:
    if n <= 0: return []
    base = max(1, total // n)
    rem = max(0, total - base * n)
    sizes = [base + (1 if i < rem else 0) for i in range(n)]
    return sizes

@dataclass
class Stage:
    buffer: int
    retries: int
    multi_proc: bool  # True => multiprocessing
    functions: list[WorkerHandler]
    merge: Optional[Callable[[list[Any]], Any]] = None # TODO
    _choose: PathChoice = PathChoice.One

    def run(self, inbound: Queue) -> Queue:
        """
        Public contract: accept and return Queue.
        """
        # return queue now; supervise in background
        outbound: Queue = Queue(maxsize=self.buffer)
        self.inbound = inbound

        async def _run_async() -> None:
            merge = self.merge if self.merge != None else lambda x: x

            if not self.multi_proc:
                # ---- ASYNC workers ----
                if self._choose is PathChoice.One:
                    shared_in = await make_shared_inbound_for_pool(
                        inbound, n_workers=len(self.functions), maxsize=self.buffer
                    )
                    outqs = [
                        worker(fn, 1, self.retries, shared_in) 
                        for fn in self.functions
                    ]

                    muxed = multiplex_async_queues(outqs)
                else:  # PathType.All
                    sizes = _split_buffer_across(len(self.functions), self.buffer)
                    per_ins = await make_broadcast_inbounds(inbound, sizes=sizes)
                    outqs = [
                        worker(fn, 1, self.retries, q_in) 
                        for fn, q_in in zip(self.functions, per_ins)
                    ]

                    muxed = multiplex_and_merge_async_queues(outqs, merge)

            else:
                outqs_async: list[Queue] = []
                ctx_method = default_mp_ctx_method(self.functions)
                # ---- MP workers ----
                if self._choose is PathChoice.One:
                    shared_in = await make_shared_inbound_for_pool(
                        inbound, n_workers=len(self.functions), maxsize=self.buffer
                    )
                    mp_in = await async_to_mp_queue_with_ready(
                        shared_in,
                        ctx_method=ctx_method,
                        sentinel_count=len(self.functions),
                    )

                    for fn in self.functions:
                        mp_out, _proc = mp_worker(
                            fn,
                            1,
                            self.retries,
                            mp_in,
                            ctx_method=ctx_method,
                        )
                        outqs_async.append(mp_to_async_queue(mp_out))

                    muxed = multiplex_async_queues(outqs_async)

                else:  # PathType.All
                    sizes = _split_buffer_across(len(self.functions), self.buffer)
                    per_ins = await make_broadcast_inbounds(inbound, sizes=sizes)
                    
                    for fn, q_in in zip(self.functions, per_ins):
                        mp_in = await async_to_mp_queue_with_ready(
                            q_in,
                            ctx_method=ctx_method,
                        )
                        mp_out, _proc = mp_worker(
                            fn,
                            1,
                            self.retries,
                            mp_in,
                            ctx_method=ctx_method,
                        )
                        outqs_async.append(mp_to_async_queue(mp_out))

                    muxed = multiplex_and_merge_async_queues(outqs_async, merge)

            # pipe muxed -> outbound
            while True:
                item = await muxed.get()
                await outbound.put(item)
                if item is SENTINEL:
                    break

        asyncio.create_task(_run_async())
        return outbound
    
    async def close(self) -> bool:
        if hasattr(self, "inbound"):
            await self.inbound.put(SENTINEL)
            return True
        return False
    
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
