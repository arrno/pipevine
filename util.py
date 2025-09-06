from dataclasses import dataclass
from typing import TypeVar, Generic

T = TypeVar("T")

@dataclass
class Err:
    message: str

type Result[T] = T | Err

def is_err(res: Result[T]) -> bool:
    return isinstance(res, Err)

def is_ok(res: Result[T]) -> bool:
    return not is_err(res)

def unwrap(res: Result[T]) -> T:
    if isinstance(res, Err):
        raise RuntimeError("unwrap on Err")
    return res

def unwrap_or(res: Result[T], default: T) -> T:
    if isinstance(res, Err):
        return default
    return res
    