from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Protocol, Unpack

@dataclass
class WorkerState:
    values: dict[str, Any]

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)
    
    def update(self, **kwargs: Any) -> None:
        self.values.update(kwargs)

class WorkerHandler(Protocol):
    def __call__(self, val: Any, state: WorkerState, /) -> Any: ...