from enum import Enum, auto
from typing import Callable
from dataclasses import dataclass

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
    functions: list[Callable]
        