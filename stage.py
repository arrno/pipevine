from enum import Enum, auto
from typing import (
    Callable,
    Any,
    TypeVar,
    TypeAlias,
)
from dataclasses import dataclass
from util import Err
import asyncio
from asyncio import Queue

T = TypeVar("T")
Result: TypeAlias = T | Err

type StageFunc = Callable[[Any], Any]

class StageType(Enum):
    Fork = auto()
    Pool = auto()

@dataclass
class Stage:
    buffer: int
    retries: int
    stage_type: StageType
    functions: list[Callable[[Any], Any]]
    merger: Callable[[list[Any]], Any] | None

    def run(self, inbound: Queue[Any]) -> Queue[Any]:
        """Run the stage with the appropriate worker pattern."""
        from worker import worker, SENTINEL
        
        # TODO handle buffer here
        # TODO spawn a worker per function

        match self.stage_type:
            case StageType.Pool:
                # Single worker processes all tasks sequentially
                return worker(
                    self.functions[0], 
                    self.buffer, 
                    self.retries,
                    inbound,
                )
            case StageType.Fork:
                # For now, implement as single worker
                # TODO: Implement true fork with multiple workers and merger
                return worker(
                    self.functions[0], 
                    self.buffer, 
                    self.retries,
                    inbound,
                )
            
        # TODO handle merger

def work_pool(
    *,
    buffer: int = 1,
    retries: int = 1,
    num_workers: int = 1,
    multi_thread: bool = False,
) -> Callable[[StageFunc], Stage]:
    """
    Decorator to create stages with configurable options.
    
    Usage:
    @work_pool()  # defaults
    @work_pool(buffer=10, retries=3)  # with options
    @work_pool(stage_type=StageType.Fork, merger=lambda results: sum(results))
    """
    def decorator(f: StageFunc) -> Stage:
        return Stage(
            buffer, 
            retries, 
            StageType.Fork if multi_thread else StageType.Pool, 
            [f for _ in range(num_workers)], 
            None,
        )
    
    return decorator

def mix_pool(
    *,
    buffer: int = 1,
    retries: int = 1,
    multi_thread: bool = False,
    merger: Callable[[list[Any]], Any] | None = None
) -> Callable[[Callable[[], list[StageFunc]]], Stage]:
    def decorator(fs: Callable[[], list[Callable]]) -> Stage:
        return Stage(
            buffer, 
            retries, 
            StageType.Fork if multi_thread else StageType.Pool, 
            fs(), 
            merger,
        )
    
    return decorator

# Keep as_stage for backwards compatibility, but always with defaults
def as_stage(func: Callable[[Any], Any]) -> Stage:
    """Simple stage decorator with defaults."""
    return Stage(1, 1, StageType.Pool, [func], None)