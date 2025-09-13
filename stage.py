from enum import Enum, auto
from typing import (
    Callable,
    Any,
    TypeVar,
    TypeAlias,
)
from worker import worker, mp_worker
from dataclasses import dataclass
from util import Err
import asyncio
from asyncio import Queue
from async_util import (
    mp_to_async_queue, 
    async_to_mp_queue, 
    multiplex_async_queues,
    make_broadcast_inbounds,
    make_shared_inbound_for_pool, 
    SENTINEL
)
from typing import Optional

T = TypeVar("T")
Result: TypeAlias = T | Err

type StageFunc = Callable[[Any], Any]

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
    functions: list[Callable[[Any], Any]]
    merge: Optional[Callable[[list[Any]], Any]] = None # TODO
    choose: PathChoice = PathChoice.One

    def _run(self, inbound: Queue) -> Queue:
        """
        Public contract: accept and return Queue.
        Internally, spin MP adapters if multi_proc=True.
        """

        if not self.multi_proc:
            # Async path
            outqs: list[Queue] = []
            for func in self.functions:
                out_q = worker(func, self.buffer, self.retries, inbound)  # returns Queue
                outqs.append(out_q)
            return multiplex_async_queues(outqs)

        # Multiprocessing path
        # 1) bridge inbound async -> mp
        mp_inbound = async_to_mp_queue(inbound, ctx_method="spawn")

        # 2) start mp workers, adapt each MPQueue -> Queue
        outqs_async: list[Queue] = []
        procs = []
        for func in self.functions:
            mp_out_q, proc = mp_worker(func, self.buffer, self.retries, mp_inbound)
            outqs_async.append(mp_to_async_queue(mp_out_q))
            procs.append(proc)

        # 3) multiplex results as async
        return multiplex_async_queues(outqs_async)

    def run(self, inbound: Queue) -> Queue:
        """
        Public contract: accept and return Queue.
        """
        # return queue now; supervise in background
        outbound: Queue = Queue(maxsize=self.buffer)

        async def _run_async():
            if not self.multi_proc:
                # ---- ASYNC workers ----
                if self.choose is PathChoice.One:
                    shared_in = await make_shared_inbound_for_pool(
                        inbound, n_workers=len(self.functions), maxsize=self.buffer
                    )
                    outqs = [worker(fn, 1, self.retries, shared_in)
                             for fn in self.functions]
                else:  # PathType.All
                    sizes = _split_buffer_across(len(self.functions), self.buffer)
                    per_ins = await make_broadcast_inbounds(inbound, sizes=sizes)
                    outqs = [worker(fn, 1, self.retries, q_in)
                             for fn, q_in in zip(self.functions, per_ins)]

                muxed = multiplex_async_queues(outqs)

            else:
                # ---- MP workers ----
                if self.choose is PathChoice.One:
                    shared_in = await make_shared_inbound_for_pool(
                        inbound, n_workers=len(self.functions), maxsize=self.buffer
                    )
                    mp_in = async_to_mp_queue(shared_in, ctx_method="spawn")

                    outqs_async: list[Queue] = []
                    for fn in self.functions:
                        mp_out, _proc = mp_worker(fn, 1, self.retries, mp_in)
                        outqs_async.append(mp_to_async_queue(mp_out))
                    muxed = multiplex_async_queues(outqs_async)

                else:  # PathType.All
                    sizes = _split_buffer_across(len(self.functions), self.buffer)
                    per_ins = await make_broadcast_inbounds(inbound, sizes=sizes)
                    outqs_async: list[Queue] = []
                    for fn, q_in in zip(self.functions, per_ins):
                        mp_in = async_to_mp_queue(q_in, ctx_method="spawn")
                        mp_out, _proc = mp_worker(fn, 1, self.retries, mp_in)
                        outqs_async.append(mp_to_async_queue(mp_out))
                    muxed = multiplex_async_queues(outqs_async)

            # pipe muxed -> outbound
            while True:
                item = await muxed.get()
                await outbound.put(item)
                if item is SENTINEL:
                    break

        asyncio.create_task(_run_async())
        return outbound
    
def work_pool(
    *,
    buffer: int = 1,
    retries: int = 1,
    num_workers: int = 1,
    multi_proc: bool = False,
    choose: PathChoice = PathChoice.One,
    merge: Callable[[list[Any]], Any] | None = None
) -> Callable[[StageFunc], Stage]:
    """
    Decorator to create stages with configurable options.
    
    Usage:
    @work_pool()  # defaults
    @work_pool(buffer=10, retries=3)  # with options
    @work_pool(stage_type=StageType.Fork, merge=lambda results: sum(results))
    """
    def decorator(f: StageFunc) -> Stage:
        return Stage(
            buffer, 
            retries, 
            multi_proc, 
            [f for _ in range(num_workers)], 
            merge,
            choose,
        )
    
    return decorator

def mix_pool(
    *,
    buffer: int = 1,
    retries: int = 1,
    multi_proc: bool = False,
    choose: PathChoice = PathChoice.One,
    merge: Callable[[list[Any]], Any] | None = None
) -> Callable[[Callable[[], list[StageFunc]]], Stage]:
    def decorator(fs: Callable[[], list[Callable]]) -> Stage:
        return Stage(
            buffer, 
            retries, 
            multi_proc, 
            fs(), 
            merge,
            choose,
        )
    
    return decorator

# Keep as_stage for backwards compatibility, but always with defaults
def as_stage(func: Callable[[Any], Any]) -> Stage:
    """Simple stage decorator with defaults."""
    return Stage(1, 1, False, [func], None)