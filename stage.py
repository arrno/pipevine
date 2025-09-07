from enum import Enum, auto
from typing import Callable, Any, TypeVar, TypeAlias
from dataclasses import dataclass
from util import Err

T = TypeVar("T")
Result: TypeAlias = T | Err

class StageType(Enum):
    Simple = auto()
    Fork = auto()
    Pool = auto()
    Merge = auto()

@dataclass
class Stage:
    buffer: int
    retries: int
    stage_type: StageType
    functions: list[Callable[[Any], Result]]

def as_stage(func: Callable[[Any], Result]) -> Stage:
    return Stage(1, 1, StageType.Simple, [func])