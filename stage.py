from enum import Enum, auto
from typing import Callable, Any
from dataclasses import dataclass
from util import Result

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