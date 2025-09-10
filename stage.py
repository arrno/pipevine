from enum import Enum, auto
from typing import (
    Callable,
    Any,
    TypeVar,
    TypeAlias,
    Iterator,
)
from dataclasses import dataclass
from util import Err

T = TypeVar("T")
Result: TypeAlias = T | Err

class StageType(Enum):
    Fork = auto()
    Pool = auto()

@dataclass
class Stage:
    buffer: int
    retries: int
    stage_type: StageType
    functions: list[Callable[[Any], Result]]
    merger: Callable[[Any], Any] | None

    def run(self, inbound: Iterator[Any]) -> Iterator[Any]:
        # TODO if we have a buffer, honor it here
        match self.stage_type:
            case StageType.Pool:
                '''
                span n workers, send each task to one
                '''
            case StageType.Fork:
                '''
                span n workers, send each task to all
                if we have a merger function, use it to merge results
                '''

        return iter([]) # TODO should be post merge output

# TODO we need a better decorator that allows buffer/retries/type/multi-functions/merger
def as_stage(func: Callable[[Any], Result]) -> Stage:
    return Stage(1, 1, StageType.Pool, [func], None)